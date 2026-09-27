"""Bounded owned-data assessment, with no feature, price or model evaluation."""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import databento_dbn as dbn

from .boundary import OperationClassification, RepoBoundary
from .canonical import sha256_file, sha256_json
from .causal_observation_canary import _binding, _query_contract
from .errors import IntegrityError
from .foundation.decoder import _chunks, _enum_name, _char_enum_name, _tri_state
from .rolling_hour_price_census import _working_set

OPERATION = "ASSESS_OWNED_ROLLING_HOUR_REFERENCE_INPUTS_ONCE"
PLAN = Path("configs/rolling_hour_owned_inputs_assessment_v1.json")
OUTPUT = "state/unpublished_evidence/rolling_hour_research_v1/owned_inputs_assessment_v1"
DAY = 86400 * 10**9
DEPENDENCIES = ("src/futures_rebuild/rolling_hour_owned_inputs.py",
    "src/futures_rebuild/foundation/decoder.py", "src/futures_rebuild/foundation/snapshot.py",
    "src/futures_rebuild/causal_observation_canary.py", "src/futures_rebuild/source_symbology.py",
    "src/futures_rebuild/research_gateway_policy.py", "src/futures_rebuild/boundary.py")


def prepare(root):
    source = json.loads((root/"configs/source_contract.json").read_bytes())
    if sha256_json({k:v for k,v in source.items() if k != "contract_id"}) != source["contract_id"]:
        raise IntegrityError("source contract identity differs")
    ref = source["complete_inventory"]
    if sha256_file(root/ref["path"]) != ref["sha256"]:
        raise IntegrityError("source inventory binding differs")
    entries = json.loads((root/ref["path"]).read_bytes())["entries"]
    sources = []
    for market in ("ES","NQ","CL","GC"):
        for year in range(2018,2023):
            for family in ("definition","statistics","status"):
                pair = [e for e in entries if e["market"]==market and e["year"]==year and e["family"]==family]
                if len(pair)!=2 or {e["kind"] for e in pair}!={"DBN","SIDECAR"}:
                    raise IntegrityError("missing or ambiguous exact owned source pair")
                p = {e["kind"]:dict(e) for e in pair}
                p["SIDECAR"].update(paired_dbn_sha256=p["DBN"]["sha256"],paired_dbn_size_bytes=p["DBN"]["size_bytes"])
                p["query_contract"] = _query_contract(root,p["SIDECAR"])
                if (p["query_contract"]["start"]!=f"{year}-01-01" or p["query_contract"]["end"]!=f"{year+1}-01-01"
                        or p["query_contract"]["schema"]!=family or p["query_contract"]["market"]!=market):
                    raise IntegrityError("owned input period or identity differs")
                sources.append(p)
    core = {"operation":OPERATION,"purpose":"OWNED_REFERENCE_RECORD_SUPPORT_ONLY_NO_MODEL_EVALUATION",
        "source_contract_sha256":sha256_file(root/"configs/source_contract.json"),"source_contract":source,
        "sources":sources,"dbn_bytes":sum(s["DBN"]["size_bytes"] for s in sources),
        "file_count":60,"record_limit":20_000_000,"hard_stop_minutes":60,"estimated_runtime_minutes":[2,15],
        "working_memory_bytes_max":2**31,"output_bytes_max":2**26,"provider_cost_usd":0,
        "output_directory":OUTPUT,"implementation":{p:sha256_file(root/p) for p in DEPENDENCIES},
        "measurements":["record and UTC-date counts","reference/status type counts","provider timestamp ranges",
            "same-day definition presence by receive and event times","continuous-query scope limitations"],
        "preservation":"READ_ONLY_EXACT_60_DBN_AND_60_SIDECARS;NO_SOURCE_OR_POLICY_MUTATION",
        "excluded":["price research","features","labels","fitting","evaluation","provider","2023_PLUS","holdout","forward","trading"]}
    return core | {"plan_id":sha256_json(core)}


def load_plan(root):
    plan = json.loads((root/PLAN).read_bytes())
    if plan != prepare(root):
        raise IntegrityError("assessment plan or its bound inputs changed")
    return plan


def scope(root,plan):
    return {"plan_id":plan["plan_id"],"plan_sha256":sha256_file(root/PLAN),"purpose":plan["purpose"]}


def summarize(chunks, family, year, definitions, consume):
    """Count only. Definition presence is a limited join diagnostic, not selection proof."""
    result = {"records":0,"received_utc_dates":Counter(),"event_utc_dates":Counter(),
        "types":Counter(),"updates":Counter(),"states":Counter(),"exceptions":Counter(),
        "definition_presence":Counter(),"ts_event_range_ns":None,"ts_recv_range_ns":None}
    lower = int(datetime(year,1,1,tzinfo=timezone.utc).timestamp())*10**9
    upper = int(datetime(year+1,1,1,tzinfo=timezone.utc).timestamp())*10**9
    for chunk in chunks:
        consume(len(chunk))
        result["records"] += len(chunk)
        for row in chunk:
            event, received = int(row["ts_event"]), int(row["ts_recv"])
            for field, timestamp in (("ts_event",event),("ts_recv",received)):
                key = field+"_range_ns"
                bounds = result[key]
                result[key] = [timestamp,timestamp] if bounds is None else [min(bounds[0],timestamp),max(bounds[1],timestamp)]
            day = received//DAY
            key = (int(row["publisher_id"]),int(row["instrument_id"]),day)
            result["received_utc_dates"][datetime.fromtimestamp(day*86400,tz=timezone.utc).date().isoformat()] += 1
            result["event_utc_dates"][datetime.fromtimestamp(event//DAY*86400,tz=timezone.utc).date().isoformat()] += 1
            if not lower <= received < upper:
                result["exceptions"]["received_outside_requested_interval"] += 1
            if received < event:
                result["exceptions"]["received_before_event"] += 1
            if family=="definition":
                # Earliest same-day definition only; no claim about later revisions or rolls.
                definitions[key] = min(received,definitions.get(key,received))
                result["types"][_char_enum_name(dbn.InstrumentClass,row["instrument_class"],"instrument_class")] += 1
                result["updates"][_char_enum_name(dbn.SecurityUpdateAction,row["security_update_action"],"security_update_action")] += 1
            else:
                first = definitions.get(key)
                if first is None:
                    presence = "no_same_day_definition_in_assessed_file"
                elif first <= event:
                    presence = "present_by_event"
                elif first <= received:
                    presence = "present_by_receive_only"
                else:
                    presence = "same_day_definition_arrives_later"
                result["definition_presence"][presence] += 1
                if family=="statistics":
                    result["types"][_enum_name(dbn.StatType,row["stat_type"],"stat_type")] += 1
                    result["updates"][_enum_name(dbn.StatUpdateAction,row["update_action"],"update_action")] += 1
                else:
                    result["types"][_enum_name(dbn.StatusAction,row["action"],"action")] += 1
                    state = _tri_state(row['is_trading'],"is_trading")+"/"+_tri_state(row['is_quoting'],"is_quoting")
                    result["states"][state] += 1
    result["definition_instrument_days"] = len(definitions) if family=="definition" else None
    return result


def execute_once(root,receipt):
    root = Path(root).resolve(strict=True)
    plan = load_plan(root)
    boundary = RepoBoundary(root)
    required = scope(root,plan) | {"approval_command":OPERATION,"approval_plan_id":plan["plan_id"],
        "approval_plan_sha256":sha256_file(root/PLAN)}
    classification = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
    receipt.verify(boundary,operation=OPERATION,classification=classification,required_scope=required)
    out = boundary.assert_active_path(root/plan["output_directory"],purpose="owned source assessment")
    out.mkdir(parents=True,exist_ok=False)
    start, total, written = time.monotonic(),0,0

    def consume(count=0):
        nonlocal total
        total += count
        if total>plan["record_limit"] or time.monotonic()-start>=plan["hard_stop_minutes"]*60 or _working_set()>=plan["working_memory_bytes_max"]:
            raise RuntimeError("assessment resource bound exhausted; no retry")

    def save(name,payload):
        nonlocal written
        data = json.dumps(payload,sort_keys=True,indent=2).encode()+b"\n"
        if written+len(data)>plan["output_bytes_max"]:
            raise RuntimeError("assessment report bound exhausted")
        with (out/name).open("xb") as stream:
            stream.write(data)
        written += len(data)

    save("approval.json",{"user_instruction":"Do this: assessing these owned inputs before proposing acquisiti",
        "scope":"Assess the previously identified owned 60 reference DBNs for four markets in 2018-2022; no acquisition or model evaluation",
        "receipt":receipt.as_dict(),"plan_id":plan["plan_id"]})
    receipt.consume(boundary,operation=OPERATION,classification=classification,required_scope=required)
    files, failures, definitions, group = [],[],{},None
    try:
        for pair in plan["sources"]:
            consume()
            e = pair["DBN"]
            current = (e["market"],e["year"])
            if group != current:
                definitions,group = {},current
            name = f"{e['market']}_{e['year']}_{e['family']}.json"
            before = total
            try:
                receipt.assert_consumed(boundary,operation=OPERATION,classification=classification,required_scope=required)
                query = _query_contract(root,pair["SIDECAR"])
                if query != pair["query_contract"]:
                    raise IntegrityError("query contract changed")
                source = plan["source_contract"]
                binding = _binding(root=root,entry=e,release_id=source["active_canonical_source"]["release_id"],
                    release_manifest_sha256=source["active_canonical_source"]["release_manifest_sha256"],
                    files_index_sha256=source["complete_inventory"]["content_inventory_sha256"])
                chunks = _chunks(binding,schema=e["family"],market=e["market"],expected_query_contract=query,
                    batch_rows=4096,on_open=lambda path:consume())
                try:
                    result = summarize(chunks,e["family"],e["year"],definitions,consume)
                finally:
                    chunks.close()
                if sha256_file(root/pair["SIDECAR"]["path"]) != pair["SIDECAR"]["sha256"]:
                    raise IntegrityError("sidecar changed during read")
                result.update(state="COMPLETE_NONEMPTY" if result["records"] else "COMPLETE_EMPTY",
                    source_binding=pair,source_hashes_verified_before_and_after=True)
                save(name,result)
                print(f"{e['market']} {e['year']} {e['family']}: {result['records']} records",flush=True)
            except (IntegrityError,ValueError,TypeError) as exc:
                failures.append(name)
                save(name,{"state":"FAILED_NOT_RETRIED","error":str(exc),"records_counted_before_failure":total-before,"source_binding":pair})
                if e["family"]=="definition":
                    definitions.clear()
                print(f"{name}: FAILED ({type(exc).__name__})",flush=True)
            files.append(name)
        consume()
        report = {"state":"ASSESSMENT_COMPLETE_WITH_FAILURES" if failures else "ASSESSMENT_COMPLETE",
            "plan_id":plan["plan_id"],"receipt_id":receipt.receipt_id,"files_attempted":len(files),"failures":failures,
            "records_counted":total,"elapsed_seconds":time.monotonic()-start,"peak_working_set_bytes":_working_set(),
            "provider_cost_usd":0,"price_features_labels_models_evaluations_computed":False,
            "artifacts":{name:sha256_file(out/name) for name in files+["approval.json"]}}
        save("report.json",report)
        return report
    except Exception as exc:
        save("failure.json",{"state":"CONSUMED_INCOMPLETE_NO_RETRY","error":str(exc),"files_completed":files,"records_counted":total})
        raise
