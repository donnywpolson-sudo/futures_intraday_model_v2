"""Approved six-column format counts only; preserve the consumed price census."""
from collections import Counter
import json
from pathlib import Path
import re
import time

import pyarrow.parquet as pq

from . import rolling_hour_price_census as census
from .boundary import OperationClassification, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError

PLAN = Path("state/unpublished_evidence/rolling_hour_research_v1/price_readiness_v6/lineage_diagnostic_plan.json")
PLAN_ID = "432d97f755735745ac7008206c00bd7dbed1c47f2adf48b275fad4d74f701e77"
OPERATION = "DIAGNOSE_ROLLING_HOUR_PRICE_LINEAGE_FORMAT_ONCE"
LOWER = re.compile(r"[0-9a-f]+")
HEX = re.compile(r"[0-9a-fA-F]+")


def load_plan(root):
    bundle, previous = census.approved_plan(root)
    plan = json.loads((root / PLAN).read_bytes())
    if (plan["plan_id"] != PLAN_ID or sha256_json({k:v for k,v in plan.items() if k != "plan_id"}) != PLAN_ID
            or plan["exact_sources"] != previous["exact_sources"] or plan["operation"] != OPERATION):
        raise IntegrityError("lineage diagnostic frozen scope differs")
    return bundle, plan


def scope(root, plan):
    return {"plan_id": PLAN_ID, "plan_sha256": sha256_file(root / PLAN),
        "implementation_sha256": sha256_file(Path(__file__)),
        "census_implementation_sha256": sha256_file(Path(census.__file__)),
        "gateway_policy_sha256": sha256_file(root / "src/futures_rebuild/research_gateway_policy.py"),
        "purpose": plan["purpose"]}


def format_counts(values):
    """Never retain values; format categories are disjoint and exhaustive."""
    result = {k:Counter() for k in ("types", "lengths", "formats")}
    total = nulls = 0
    for value in values:
        total += 1
        result["types"][type(value).__name__] += 1
        if value is None:
            nulls += 1
            category = "null"
        elif not isinstance(value, (str, bytes)):
            category = "non_text_type"
        else:
            result["lengths"][str(len(value))] += 1
            text = value.decode("ascii", errors="replace") if isinstance(value, bytes) else value
            category = ("lowercase_hex" if LOWER.fullmatch(text) else
                "uppercase_or_mixed_hex" if HEX.fullmatch(text) else "non_hex_or_empty")
        result["formats"][category] += 1
    return {"rows":total, "nulls":nulls, **{k:dict(v) for k,v in result.items()}}


def execute_once(root, receipt):
    root = Path(root).resolve(strict=True)
    bundle, plan = load_plan(root)
    boundary = RepoBoundary(root)
    required = scope(root, plan) | {"approval_command": OPERATION,
        "approval_plan_id": PLAN_ID, "approval_plan_sha256": sha256_file(root / PLAN)}
    classification = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
    receipt.verify(boundary, operation=OPERATION, classification=classification, required_scope=required)
    out = boundary.assert_active_path(root / plan["output_directory"], purpose="lineage diagnostic reports")
    out.mkdir(parents=True, exist_ok=False)
    started, written = time.monotonic(), 0

    def save(name, payload):
        nonlocal written
        data = json.dumps(payload, indent=2, sort_keys=True).encode()+b"\n"
        if written + len(data) > plan["estimated_output_bytes_max"]:
            raise IntegrityError("diagnostic output limit exceeded")
        with (out/name).open("xb") as stream:
            stream.write(data)
        written += len(data)

    def limit():
        if time.monotonic()-started >= plan["hard_stop_minutes"]*60 or census._working_set() >= plan["working_memory_bytes_max"]:
            raise IntegrityError("diagnostic time or memory limit exceeded")

    save("approval.json", {"user_message":"Approve", "approved_plan":plan, "receipt":receipt.as_dict()})
    receipt.consume(boundary, operation=OPERATION, classification=classification, required_scope=required)
    completed, rows_total = [], 0
    try:
        if sha256_file(root/"data/causally_gated_normalized/_control/catalog.json") != bundle["readiness_preparation"]["price_catalog_sha256"]:
            raise IntegrityError("catalog changed")
        for source in plan["exact_sources"]:
            limit()
            receipt.assert_consumed(boundary, operation=OPERATION, classification=classification, required_scope=required)
            path = census.resolve(repository_root=root, market=source["market"], year=source["year"], purpose="LINEAGE_AUDIT")
            manifest = root/source["sidecar_path"]
            if (path != root/source["parquet_path"] or path.stat().st_size != source["physical_bytes"]
                    or sha256_file(path) != source["parquet_sha256"] or sha256_file(manifest) != source["sidecar_sha256"]):
                raise IntegrityError("source differs from approved binding")
            fields = {name:{"rows":0,"nulls":0,"types":Counter(),"lengths":Counter(),"formats":Counter()} for name in plan["columns"]}
            count = 0
            with pq.ParquetFile(path) as parquet:
                if parquet.metadata.num_rows != source["row_count"] or not set(fields) <= set(parquet.schema_arrow.names):
                    raise IntegrityError("source schema or count differs")
                for batch in parquet.iter_batches(batch_size=8192, columns=plan["columns"]):
                    limit()
                    count += batch.num_rows
                    if count > source["row_count"]:
                        raise IntegrityError("row budget exceeded")
                    for name in fields:
                        stats = format_counts(batch.column(name).to_pylist())
                        for key in ("rows","nulls"):
                            fields[name][key] += stats[key]
                        for key in ("types","lengths","formats"):
                            fields[name][key].update(stats[key])
            if count != source["row_count"] or sha256_file(path) != source["parquet_sha256"] or sha256_file(manifest) != source["sidecar_sha256"]:
                raise IntegrityError("count or post-read source hash differs")
            name = f"{source['market']}_{source['year']}.json"
            save(name, {"source_binding":source, "rows":count, "fields":fields, "source_hashes_verified_before_and_after":True})
            completed.append(name)
            rows_total += count
            print(f"{source['market']} {source['year']}: {count} rows counted", flush=True)
        limit()
        if rows_total != plan["row_limit"]:
            raise IntegrityError("complete diagnostic total differs")
        report = {"state":"COMPLETE_FORMAT_DIAGNOSTIC_NOT_LINEAGE_CERTIFICATION", "plan_id":PLAN_ID,
            "rows":rows_total, "files":len(completed), "receipt_id":receipt.receipt_id,
            "elapsed_seconds":time.monotonic()-started, "peak_working_set_bytes":census._working_set(),
            "observation_values_saved":False, "provider_cost_usd":0,
            "artifacts":{name:sha256_file(out/name) for name in completed+["approval.json"]}}
        save("report.json",report)
        return report
    except Exception as exc:
        save("failure.json",{"state":"CONSUMED_INCOMPLETE_NO_RETRY", "error":str(exc), "completed_files":completed})
        raise
