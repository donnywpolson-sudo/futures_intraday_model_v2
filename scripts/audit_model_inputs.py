"""Bounded, read-only historical diagnostics for the inactive input candidate.

Run only within the user-approved scope; no fitting, activation or provider use.
Outputs small JSON summaries, never a replacement observation dataset.
"""
import argparse
from collections import Counter, defaultdict
from hashlib import sha256
import json
from pathlib import Path
import time

import numpy as np
import pyarrow.parquet as pq

from futures_rebuild import active_data_view
from futures_rebuild.annual_foundation_independence import authority
from futures_rebuild.annual_foundation_release import reference_record
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_observation_canary import _query_contract
from futures_rebuild.causal_source_closure import select_preferred_standard_source_entries, validate_full_build_selection_contract
from futures_rebuild.causal_timeframe_storage import memory_peak, process_io
from futures_rebuild.foundation.decoder import _chunks
from futures_rebuild.foundation.snapshot import DbnReleaseFile
from futures_rebuild.model_input_preparation import EXCLUSIONS, IDENTITY_FIELDS, MINUTE, _physical_batches

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "state/unpublished_evidence/free_model_input_remediation_v1"


def write(name, result):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT/name).write_text(json.dumps(result, indent=2, sort_keys=True)+"\n", encoding="utf-8")


def structural():
    started, cpu = time.perf_counter(), time.process_time()
    catalog_path = ROOT / active_data_view.ACTIVE_ROOT / "catalog.json"
    binding = sha256_file(catalog_path)
    catalog = active_data_view._load_view_catalog(ROOT/active_data_view.ACTIVE_ROOT, allow_unreferenced_append_files=True)
    population=authority(ROOT)
    total, per_release, unavailable, tails = Counter(), [], [], {}
    # Engineering coverage metric, not a model/trial: 61 consecutive observations.
    # The same segment census measures complete 60-minute entry-to-exit paths.
    for entry in sorted(catalog["entries"], key=lambda e:(e["market"],e["year"])):
        if not entry.get("parquet_path"):
            unavailable.append({k:entry[k] for k in ("market","year","disposition","reason")})
            continue
        counts = Counter()
        market = entry["market"]
        previous = tails.get(market)
        reference=dict(path=entry["sidecar_path"],sha256=entry["sidecar_sha256"])
        manifest=reference_record(ROOT,reference)
        assert population["wrappers"].get(manifest["id"])==reference
        assert manifest["minute_permission"]["entry_binding"]=={k:v for k,v in entry.items() if k!="sidecar_sha256"}
        for obs, quality, masks in _physical_batches(ROOT, entry, manifest):
            n=len(obs)
            columns={k:obs[k].to_numpy(zero_copy_only=False) for k in ("bar_start_ns",*IDENTITY_FIELDS)}
            bad=np.logical_or.reduce(list(masks.values()))
            gap=np.zeros(n,dtype=bool); identity=gap.copy()
            starts=columns["bar_start_ns"]
            gap[1:]=starts[1:]!=starts[:-1]+MINUTE
            for key in IDENTITY_FIELDS:
                identity[1:] |= columns[key][1:] != columns[key][:-1]
            if previous:
                gap[0]=starts[0]!=previous["start"]+MINUTE
                identity[0]=any(columns[k][0]!=previous[k] for k in IDENTITY_FIELDS)
            else:
                gap[0]=True
            ordered=np.zeros(n,dtype=bool)
            ordered[1:]=starts[1:]<=starts[:-1]
            if previous:
                ordered[0]=starts[0]<=previous["start"]
            bad |= ordered
            masks["DUPLICATE_OR_OUT_OF_ORDER"]=ordered
            reset=gap | identity | bad
            # A bad bar cannot seed a new lookback; its successor starts fresh.
            reset[1:] |= bad[:-1]
            if previous and previous["bad"]:
                reset[0]=True
            positions=np.arange(n)
            prior_run=0 if previous is None else previous["run"]
            origin=np.where(reset,positions,-prior_run)
            run=positions-np.maximum.accumulate(origin)+1
            run[bad]=0
            valid=~bad
            counts.update(observations=n, accepted_rows=int(valid.sum()), excluded_rows=int(bad.sum()),
                          eligible_61_bar_windows=int((run>=61).sum()), refused_61_bar_windows=int((run<61).sum()),
                          gap_boundaries=int(gap.sum())-(previous is None), identity_boundaries=int(identity.sum()))
            # Overlapping reasons describe observed anchors with a 60-observation
            # tail; they are intentionally NOT summed into the unique refusal count.
            for reason, mask in {**masks,"GAP_BOUNDARY":gap,"IDENTITY_BOUNDARY":identity}.items():
                tail=[] if previous is None else previous["reason_tails"].get(reason,[])
                joined=np.concatenate((np.asarray(tail,dtype=bool),mask))
                cumulative=np.concatenate(([0],np.cumsum(joined)))
                ends=np.arange(len(tail),len(joined))+1
                width=60 if reason.endswith("BOUNDARY") else 61
                hit=cumulative[ends]-cumulative[np.maximum(0,ends-width)]>0
                counts["reason_windows:"+reason]+=int(hit.sum())
                counts["reason_rows:"+reason]+=int(mask.sum())
            reason_tails={reason:([] if previous is None else previous["reason_tails"].get(reason,[]))+mask.tolist()
                          for reason,mask in {**masks,"GAP_BOUNDARY":gap,"IDENTITY_BOUNDARY":identity}.items()}
            previous={k:columns[k][-1].item() if isinstance(columns[k][-1],np.generic) else columns[k][-1]
                      for k in IDENTITY_FIELDS}
            previous.update(start=int(starts[-1]),run=int(run[-1]),bad=bool(bad[-1]),
                            reason_tails={k:v[-60:] for k,v in reason_tails.items()})
        tails[market]=previous
        for table,column in (("missingness","state"),("roll","crossing_status"),("cadence","result")):
            file=manifest["packing"]["files"][table]
            path=(ROOT/entry["parquet_path"]).parent/file["filename"]
            if sha256_file(path)!=file["sha256"]:
                raise RuntimeError("Sidecar hash mismatch: "+str(path))
            for batch in pq.ParquetFile(path).iter_batches(batch_size=50_000,columns=[column]):
                counts.update({table+":"+str(k):v for k,v in Counter(batch[column].to_pylist()).items()})
        assert counts["observations"]==entry["row_count"]
        assert counts["accepted_rows"]+counts["excluded_rows"]==counts["observations"]
        assert counts["eligible_61_bar_windows"]+counts["refused_61_bar_windows"]==counts["observations"]
        total.update(counts)
        per_release.append(dict(market=market,year=entry["year"],cohort=entry["cohort"],
            permitted_uses=entry["permitted_uses"], selection_eligible=entry["selection_eligible"],
            observations_sha256=entry["parquet_sha256"],manifest_sha256=entry["sidecar_sha256"],counts=dict(counts)))
        if len(per_release)%25==0:
            print(json.dumps(dict(releases=len(per_release),rows=total["observations"],seconds=round(time.perf_counter()-started,1))),flush=True)
    assert sha256_file(catalog_path)==binding
    assert authority(ROOT)==population
    result=dict(status="COMPLETE_STRUCTURAL_AUDIT_NOT_RESEARCH_ADMISSION",catalog_sha256=binding,
        window_metric="61 consecutive observed minutes; all observed anchors, reset at gaps/identity/refusal; no price calculations",
        reason_counts_basis="Overlapping reasons use trailing observed records; gaps can make their elapsed span longer than 61 minutes. Exact disputed clock-window intersections are in disputed_sources.json.",
        target_metric="61 observations prove a 60-minute entry-to-exit path; incomplete anchors retained as indeterminate",
        complete_target_paths=total["eligible_61_bar_windows"],indeterminate_target_paths=total["refused_61_bar_windows"],
        totals=dict(total),releases=per_release,unavailable=unavailable,exclusions=EXCLUSIONS,
        resources=dict(elapsed_seconds=time.perf_counter()-started,cpu_seconds=time.process_time()-cpu,
                       peak_memory_bytes=memory_peak(),io=process_io()))
    write("structural_audit.json",result)
    print(json.dumps({k:v for k,v in result.items() if k not in ("releases","unavailable")}),flush=True)


def statistics():
    started,cpu=time.perf_counter(),time.process_time()
    contract_path=ROOT/"configs/source_contract.json"
    contract_hash=sha256_file(contract_path)
    contract=json.loads(contract_path.read_bytes())
    source=contract["complete_inventory"]
    assert sha256_file(ROOT/source["path"])==source["sha256"]
    inventory=json.loads((ROOT/source["path"]).read_bytes())
    validate_full_build_selection_contract(contract,inventory["entries"])
    catalog=json.loads((ROOT/"data/causally_gated_normalized/_control/catalog.json").read_bytes())
    markets=sorted({e["market"] for e in catalog["entries"]})
    entries=select_preferred_standard_source_entries(ROOT,[e for e in inventory["entries"]
        if e["market"] in markets and 2018<=e["year"]<=2022 and e["family"]=="statistics"
        and e["admitted_standard_foundation"]])
    bypath={e["path"]:e for e in entries}
    total,units,seen=Counter(),[],set()
    payloads={}; distinct=Counter(); coverage=defaultdict(Counter)
    for e in entries:
        if e["kind"]!="DBN":continue
        side=bypath[e["path"]+".manifest.json"]
        query=_query_contract(ROOT,{**side,"paired_dbn_sha256":e["sha256"],"paired_dbn_size_bytes":e["size_bytes"]})
        binding=DbnReleaseFile(e["path"],ROOT/e["path"],e["path"],e["size_bytes"],e["sha256"],
            contract["active_canonical_source"]["release_id"],contract["contract_id"],sha256_json(entries))
        counts=Counter(); widths=set(); examples={}; decoded=sha256()
        for chunk in _chunks(binding,schema="statistics",market=e["market"],expected_query_contract=query,batch_rows=50_000):
            dtype=chunk.dtype.fields["quantity"][0]
            assert dtype.kind=="i" and dtype.itemsize in (4,8)
            widths.add(dtype.itemsize*8)
            decoded.update(chunk.tobytes())
            null=(1<<(dtype.itemsize*8-1))-1
            oi=(chunk["stat_type"]==9);settle=(chunk["stat_type"]==3); new=(chunk["update_action"]==1)
            counts.update(rows=len(chunk),oi_rows=int(oi.sum()),settlement_rows=int(settle.sum()),
                oi_valid_quantity_null_price=int((oi&new&(chunk["quantity"]!=null)&(chunk["price"]==2**63-1)).sum()),
                oi_undefined_quantity=int((oi&new&(chunk["quantity"]==null)).sum()),
                oi_zero_quantity=int((oi&new&(chunk["quantity"]==0)).sum()),
                settlement_valid_price=int((settle&new&(chunk["price"]!=2**63-1)).sum()),
                settlement_unused_null_quantity=int((settle&(chunk["quantity"]==null)).sum()),
                settlement_undefined_price=int((settle&new&(chunk["price"]==2**63-1)).sum()),
                deletes=int(((oi|settle)&(chunk["update_action"]==2)).sum()))
            for name,mask in (("oi",oi&new&(chunk["quantity"]!=null)),("settlement",settle&new&(chunk["price"]!=2**63-1))):
                if name not in examples and mask.any():
                    r=chunk[np.flatnonzero(mask)[0]]
                    examples[name]={k:int(r[k]) for k in ("ts_recv","ts_ref","publisher_id","instrument_id","price","quantity","stat_flags")}
        seen.add((e["market"],e["year"]))
        coverage[(e["market"],e["year"])].update(counts)
        total.update(counts)
        payload_key=(e["market"],tuple(sorted(widths)),decoded.hexdigest())
        duplicate=payloads.get(payload_key)
        if duplicate is None:
            payloads[payload_key]=e["path"]
            distinct.update(counts)
        units.append(dict(market=e["market"],year=e["year"],path=e["path"],sha256=e["sha256"],
                          decoded_payload_sha256=decoded.hexdigest(),identical_payload_of=duplicate,
                          quantity_widths=sorted(widths),counts=dict(counts),examples=examples))
        if len(units)%25==0:print(json.dumps(dict(statistics_files=len(units),rows=total["rows"])),flush=True)
    assert sha256_file(contract_path)==contract_hash
    result=dict(status="COMPLETE_FIELD_SEMANTICS_SCAN_NOT_FEATURE_ADMISSION",source_contract_sha256=contract_hash,
        physical_record_totals=dict(total),distinct_complete_payload_totals=dict(distinct),files=units,
        missing_market_years=[(m,y) for m in markets for y in range(2018,2023) if (m,y) not in seen],
        signal_absent=[dict(market=m,year=y,signal=k) for (m,y),counts in coverage.items()
                       for k in ("oi_rows","settlement_rows") if not counts[k]],
        resources=dict(elapsed_seconds=time.perf_counter()-started,cpu_seconds=time.process_time()-cpu,
                       peak_memory_bytes=memory_peak(),io=process_io()))
    write("statistics_audit.json",result)
    print(json.dumps({k:v for k,v in result.items() if k!="files"}),flush=True)


def integration():
    """Small real-data integration; all sources stay within explicitly approved history."""
    from hashlib import sha256
    from futures_rebuild.canonical import canonical_bytes
    from futures_rebuild.foundation.decoder import iter_definitions, iter_statistics
    from futures_rebuild.foundation.economics import EconomicsRuleBook
    from futures_rebuild.foundation.market_state import StatisticsRolePolicy, _statistics_ledger_event
    from futures_rebuild.foundation.statistics_v2 import AsOfStatisticsV2, iter_statistics_v2, TYPES
    from futures_rebuild.model_input_preparation import WindowRequest, prepare_inputs
    started=time.perf_counter()
    contract=json.loads((ROOT/"configs/source_contract.json").read_bytes())
    source=contract["complete_inventory"]
    assert sha256_file(ROOT/source["path"])==source["sha256"]
    inventory=json.loads((ROOT/source["path"]).read_bytes())
    entries=select_preferred_standard_source_entries(ROOT,[e for e in inventory["entries"]
        if e["market"]=="ES" and e["year"]==2020 and e["family"] in ("statistics","definition")
        and e["admitted_standard_foundation"]])
    bypath={e["path"]:e for e in entries}
    sources={}
    for e in entries:
        if e["kind"]!="DBN":continue
        query=_query_contract(ROOT,{**bypath[e["path"]+".manifest.json"],
            "paired_dbn_sha256":e["sha256"],"paired_dbn_size_bytes":e["size_bytes"]})
        binding=DbnReleaseFile(e["path"],ROOT/e["path"],e["path"],e["size_bytes"],e["sha256"],
            contract["active_canonical_source"]["release_id"],contract["contract_id"],sha256_json(entries))
        sources[e["family"]]=(binding,dict(market="ES",expected_query_contract=query))
    binding,kwargs=sources["statistics"]
    roles=StatisticsRolePolicy.from_file(ROOT/"configs/statistics_foundation_roles.json")
    raw=[]; count=0; serialized=sha256()
    for value,old in zip(iter_statistics_v2(binding,**kwargs),iter_statistics(binding,**kwargs),strict=True):
        assert value.record==old
        serialized.update(canonical_bytes(_statistics_ledger_event(old,roles=roles)))
        count+=1
        if old.stat_type in TYPES:raw.append(value)
    binding,kwargs=sources["definition"]
    definitions=list(iter_definitions(binding,**kwargs))
    store=AsOfStatisticsV2(raw,definitions=definitions,economics=EconomicsRuleBook.from_file(ROOT/"configs/contract_economics_rules.json"))
    # Two eligible windows on Jan 2/6, after the January 1 statistics examples.
    windows=[WindowRequest(t,t+2*MINUTE,t+2*MINUTE+5_000_000_000)
             for t in (1577977200000000000,1578322800000000000)]
    prepared=prepare_inputs(ROOT,market="ES",year=2020,requests=windows,statistics=store,
                            max_age_ns=4*86_400_000_000_000,settlement_flags=7)
    summaries=[]
    for r in prepared:
        assert r["status"]=="ELIGIBLE"
        summaries.append(dict(start_ns=r["request"].start_ns,status=r["status"],rows=len(r["observations"]),
            statistics={k:{f:v for f,v in p.items() if f not in ("record","conflicting_records")}
                        for k,p in r["statistics"].items()}))
    disputed=[]
    for market,start,end in EXCLUSIONS:
        year=2015 if market=="6N" else 2020
        requests=[WindowRequest(start,end,end+5_000_000_000),WindowRequest(end,end+2*MINUTE,end+2*MINUTE+5_000_000_000)]
        results=prepare_inputs(ROOT,market=market,year=year,requests=requests)
        assert results[0]["status"]=="REFUSED" and "DISPUTED_INTERVAL" in results[0]["reasons"]
        # A dispute ending does not promise observations in the following minutes.
        assert "DISPUTED_INTERVAL" not in results[1]["reasons"]
        assert results[1]["status"] == ("ELIGIBLE" if len(results[1]["observations"])==2 else "REFUSED")
        disputed.append(dict(market=market,excluded_observations=len(results[0]["observations"]),
                              reasons=results[0]["reasons"],post_exclusion_state=results[1]["status"],
                              post_exclusion_reasons=results[1]["reasons"]))
    result=dict(status="PASS_LOCAL_INTEGRATION",legacy_decoded_records_identical=count,
        legacy_event_stream_sha256=serialized.hexdigest(),source_bindings=[dict(path=e["path"],sha256=e["sha256"]) for e in entries],
        es_windows=summaries,disputed=disputed,definition_records=len(definitions),
        unresolved_statistics_identities=sum(map(len,store.unresolved.values())),
        resources=dict(elapsed_seconds=time.perf_counter()-started,peak_memory_bytes=memory_peak(),io=process_io()))
    write("integration.json",result)
    print(json.dumps(result),flush=True)


def disputes():
    started=time.perf_counter()
    contract=json.loads((ROOT/"configs/source_contract.json").read_bytes())
    source=contract["complete_inventory"]
    assert sha256_file(ROOT/source["path"])==source["sha256"]
    inventory=json.loads((ROOT/source["path"]).read_bytes())
    catalog=json.loads((ROOT/"data/causally_gated_normalized/_control/catalog.json").read_bytes())
    comparisons=[]; bindings=[]; window_exclusions=[]
    for market,start,end in EXCLUSIONS:
        year=2015 if market=="6N" else 2020
        entries=select_preferred_standard_source_entries(ROOT,[e for e in inventory["entries"]
            if e["market"]==market and e["year"]==year and e["family"] in ("ohlcv_1m","ohlcv_1h","ohlcv_1s")
            and e["admitted_standard_foundation"]])
        bypath={e["path"]:e for e in entries}; selected={}
        for e in entries:
            if e["kind"]!="DBN":continue
            query=_query_contract(ROOT,{**bypath[e["path"]+".manifest.json"],
                "paired_dbn_sha256":e["sha256"],"paired_dbn_size_bytes":e["size_bytes"]})
            binding=DbnReleaseFile(e["path"],ROOT/e["path"],e["path"],e["size_bytes"],e["sha256"],
                contract["active_canonical_source"]["release_id"],contract["contract_id"],sha256_json(entries))
            rows=[]
            for chunk in _chunks(binding,schema=query["schema"],market=market,expected_query_contract=query,batch_rows=50_000):
                for r in chunk[(chunk["ts_event"]>=start)&(chunk["ts_event"]<end)]:
                    rows.append({k:int(r[k]) for k in ("ts_event","instrument_id","open","high","low","close","volume")})
            selected[e["family"]]=rows
            bindings.append(dict(path=e["path"],sha256=e["sha256"]))
        entry=next(e for e in catalog["entries"] if (e["market"],e["year"])==(market,year))
        assert sha256_file(ROOT/entry["parquet_path"])==entry["parquet_sha256"]
        columns=["bar_start_ns","instrument_id","open_nano","high_nano","low_nano","close_nano","volume"]
        normalized=[]; feature_windows=target_windows=0
        for batch in pq.ParquetFile(ROOT/entry["parquet_path"]).iter_batches(batch_size=50_000,columns=columns):
            t=batch["bar_start_ns"].to_numpy()
            feature_windows+=int(((t>=start)&(t<end+60*MINUTE)).sum())
            target_windows+=int(((t>=start-60*MINUTE)&(t<end)).sum())
            normalized.extend(batch.take(np.flatnonzero((t>=start)&(t<end))).to_pylist())
        window_exclusions.append(dict(market=market,start_ns=start,end_ns=end,
            feature_61_minute_intersections=feature_windows,target_61_observation_intersections=target_windows))
        assert len(normalized)==len(selected["ohlcv_1m"])
        for n,r in zip(normalized,selected["ohlcv_1m"],strict=True):
            assert [n[k] for k in columns]==[r[k] for k in ("ts_event","instrument_id","open","high","low","close","volume")]
        for hour in range(start,end,60*MINUTE):
            aggregates={}
            for family,rows in selected.items():
                rows=[r for r in rows if hour<=r["ts_event"]<hour+60*MINUTE]
                assert rows
                aggregates[family]=dict(rows=len(rows),observed_minutes=len({r["ts_event"]//MINUTE for r in rows}),
                    ohlcv=[rows[0]["open"],max(r["high"] for r in rows),min(r["low"] for r in rows),rows[-1]["close"],sum(r["volume"] for r in rows)])
            assert aggregates["ohlcv_1s"]["ohlcv"]==aggregates["ohlcv_1h"]["ohlcv"]
            assert aggregates["ohlcv_1m"]["ohlcv"]!=aggregates["ohlcv_1h"]["ohlcv"]
            comparisons.append(dict(market=market,hour_start_ns=hour,normalized_matches_raw_minute=True,aggregates=aggregates))
    result=dict(status="PASS_THREE_DISPUTES_REPRODUCED_NO_REPLACEMENT_PRICES",comparisons=comparisons,
        window_exclusions=window_exclusions,source_bindings=bindings,
        resources=dict(elapsed_seconds=time.perf_counter()-started,peak_memory_bytes=memory_peak(),io=process_io()))
    write("disputed_sources.json",result)
    print(json.dumps(result),flush=True)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scan",choices=("structural","statistics","integration","disputes"))
    args=parser.parse_args()
    {"structural":structural,"statistics":statistics,"integration":integration,"disputes":disputes}[args.scan]()
