"""Generated-only checks for the complete V10 foundation audit."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild import v10_foundation_audit as a
from futures_rebuild.canonical import sha256_json
from futures_rebuild.causal_observation_parquet import write_bundle, write_partitioned_bundle, FILENAMES
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_causal_observation_parquet import _tables

pytestmark = pytest.mark.current
START = a.timestamp("2023-12-31T23:59:00Z")


def sample(start=START, contract="ESH5", previous=None, digest=None, multiplier=50):
    t = _tables()
    if digest:
        t = json.loads(json.dumps(t).replace("a"*64, digest))
    row = t["observations"][0]
    row.update(bar_start_ns=start, bar_end_ns=start+a.MINUTE, source_timestamp_ns=start,
               available_at_ns=start+a.MINUTE+5_000_000_000,
               decision_eligible_at_ns=start+a.MINUTE+5_000_000_000,
               actual_contract=contract, raw_symbol=contract, multiplier_nano=multiplier)
    row["row_id"] = sha256_json({k:v for k,v in row.items() if k != "row_id"})
    rid = row["row_id"]
    m = t["missingness"][0]
    m.update(observation_row_id=rid, interval_start_ns=start, interval_end_ns=start+a.MINUTE)
    m["evidence_sha256"] = sha256_json({"market":"ES", "source_row_sha256":row["source_row_sha256"],
        "interval_start_ns":start,"interval_end_ns":start+a.MINUTE,"authority":m["authority"]})
    m["evidence_id"] = sha256_json({k:v for k,v in m.items() if k != "evidence_id"})
    before = previous["actual_contract"] if previous else contract
    changed = before != contract
    roll = t["roll"][0]
    roll.update(row_id=rid, actual_contract_before=before, actual_contract_after=contract,
        roll_flag=changed, effective_time_ns=start if changed else None,
        price_discontinuity_flag=bool(changed and previous["close_nano"] != row["open_nano"]),
        crossing_status="ROLL_BOUNDARY_UNADJUSTED" if changed else "NO_CROSSING")
    roll["causal_selection_evidence_sha256"] = sha256_json({"definition_row_sha256":row["definition_row_sha256"],
        "definition_received_at_ns":row["definition_received_at_ns"],"prior_contract":before})
    t["quality"][0].update(row_id=rid, row_identity_sha256=rid)
    t["quality"][0]["quality_flags"].append("ECONOMICS_RULEBOOK_SHA256_"+a.ECONOMICS_RULEBOOK_SHA256)
    c=t["cadence"][0];c["row_id"]=rid
    c["comparison_id"]=sha256_json({k:v for k,v in c.items() if k != "comparison_id"})
    return t


def part(start=START, end=START+3*a.MINUTE):
    return dict(market="ES",start_ns=start,end_ns=end,source_contract_id="a"*64,source_release_id="a"*64)


def sources():
    row=sample()["observations"][0]
    return {row[p+"_file_path"]:dict(market="ES",family=f,sha256="a"*64,start_ns=0,end_ns=a.CUTOFF)
            for p,f in (("source","ohlcv_1m"),("definition_source","definition"))}


def test_gap_accounting_across_year_boundary_preserves_roll_and_no_schedule_claim():
    first=sample();events=[]
    counts,prev=a.inspect_group(first,part(end=START+a.MINUTE),sources(),50,None,events.append)
    a.finish_partition(prev,part(end=START+a.MINUTE),counts,events.append)
    second=sample(START+2*a.MINUTE,"ESM5",prev)
    p=part(START+a.MINUTE,START+4*a.MINUTE)
    c,last=a.inspect_group(second,p,sources(),50,prev,events.append)
    a.finish_partition(last,p,c,events.append)
    assert c["contract_changes"]==1 and c["negative_price_rows"]==1
    assert c["observed_ns"]+c["uncovered_ns"]==p["end_ns"]-p["start_ns"]
    gaps=[e for e in events if e["kind"]=="UNCOVERED_INTERVAL"]
    assert [(e["start_ns"],e["end_ns"]) for e in gaps]==[(START+a.MINUTE,START+2*a.MINUTE),(START+3*a.MINUTE,START+4*a.MINUTE)]
    assert all(e["state"]=="UNKNOWN" for e in gaps)


@pytest.mark.parametrize("field,value,match", [
    ("definition_received_at_ns",START+1,"definition"),
    ("bar_start_ns",START+1,"timing"),
    ("available_at_ns",START,"timing"),
    ("decision_eligible_at_ns",a.CUTOFF,"timing"),
    ("multiplier_nano",0,"economics"),
    ("min_price_increment_nano",0,"economics"),
    ("volume",-1,"economics"),
    ("low_nano",100,"OHLC"),
    ("source_file_sha256","b"*64,"lineage"),
])
def test_bad_observation_is_not_silently_admitted(field,value,match):
    t=sample();t["observations"][0][field]=value
    with pytest.raises(IntegrityError,match=match):
        a.inspect_group(t,part(),sources(),50,None,lambda _:None)


def test_duplicate_roll_and_supporting_identity_mismatch_rejected():
    t=sample();r=t["observations"][0]
    with pytest.raises(IntegrityError,match="overlapping"):
        a.inspect_group(t,part(),sources(),50,r,lambda _:None)
    t["quality"][0]["row_id"]="b"*64
    with pytest.raises(IntegrityError,match="quality coverage"):
        a.inspect_group(t,part(),sources(),50,None,lambda _:None)


def test_optional_cadence_and_group_local_ordinals(tmp_path):
    first=sample();second=sample(START+a.MINUTE)
    # Both groups use ordinal zero; the decoder must resolve a different row ID.
    write_partitioned_bundle(tmp_path,table_row_groups={n:[first[n],second[n]] for n in FILENAMES})
    got=list(a.groups(tmp_path))
    assert [g["quality"][0]["row_id"] for g in got]==[first["observations"][0]["row_id"],second["observations"][0]["row_id"]]
    empty=tmp_path/"empty";first["cadence"]=[];write_bundle(empty,tables=first)
    assert list(a.groups(empty))[0]["cadence"]==[]


def test_invalid_physical_ordinal_and_missing_table_fail(tmp_path):
    write_bundle(tmp_path,tables=sample())
    p=tmp_path/"quality.parquet";t=pq.read_table(p)
    t=t.set_column(0,t.schema.field(0),pa.array([7],type=pa.uint32()))
    pq.write_table(t,p)
    with pytest.raises(IntegrityError,match="ordinal"):
        list(a.groups(tmp_path))
    p.unlink()
    with pytest.raises(IntegrityError,match="unreadable"):
        list(a.groups(tmp_path))


def test_annual_equality_ignores_encoding_but_rejects_row_loss_or_change(tmp_path):
    monthly=[]
    for i in range(2):
        p=tmp_path/str(i);write_bundle(p,tables=sample(START+i*a.MINUTE));monthly.append(p/"observations.parquet")
    table=pa.concat_tables([pq.read_table(p) for p in monthly])
    annual=tmp_path/"annual.parquet";pq.write_table(table,annual,compression="snappy",row_group_size=2)
    assert a.compare_annual(annual,monthly,2)["row_count"]==2
    pq.write_table(table.slice(0,1),annual)
    with pytest.raises(IntegrityError,match="annual logical"):
        a.compare_annual(annual,monthly,2)
    changed=table.set_column(table.schema.get_field_index("volume"),table.schema.field("volume"),pa.array([999,5],type=pa.uint64()))
    pq.write_table(changed,annual)
    with pytest.raises(IntegrityError,match="annual logical"):
        a.compare_annual(annual,monthly,2)


def test_noncomparable_is_reported_not_dropped():
    t=sample();t["cadence"][0]["result"]="NOT_COMPARABLE";events=[]
    c,_=a.inspect_group(t,part(),sources(),50,None,events.append)
    assert c["observations"]==1 and c["cadence_1s_NOT_COMPARABLE"]==1
    assert events[0]["state"]=="NOT_COMPARABLE"


def test_execution_refuses_unapproved_scope_before_payload_access(tmp_path,monkeypatch):
    p=a.signed({"scope":"test"},"plan_id")
    (tmp_path/"authorization.json").write_text(json.dumps({"plan_id":p["plan_id"],"scope":"test","status":"PREPARED"}))
    monkeypatch.setattr(a,"groups",lambda *_:pytest.fail("payload reader reached"))
    with pytest.raises(UnauthorizedOperation,match="owner-approved"):
        a.execute(tmp_path,p,"authorization.json")
    assert not (tmp_path/"state").exists()


def test_binding_drift_rejected(tmp_path):
    path=tmp_path/"evidence.json";path.write_bytes(b"original")
    plan={"metadata":{"evidence.json":{"sha256":a.sha256_file(path)}},"implementation":{},"environment":a.environment()}
    budget=type("HashOnly",(),{"hash":staticmethod(a.sha256_file)})()
    a.verify_bindings(tmp_path,plan,budget)
    path.write_bytes(b"changed")
    with pytest.raises(IntegrityError,match="metadata drift"):
        a.verify_bindings(tmp_path,plan,budget)


def execution_fixture(root):
    raw=root/"synthetic";raw.mkdir()
    for name in ("ohlcv_1m.dbn.zst","definition.dbn.zst"):
        (raw/name).write_bytes(b"generated fixture; no vendor data")
    digest=a.sha256_file(raw/"ohlcv_1m.dbn.zst")
    tables=sample(digest=digest,multiplier=50*10**9)
    month=raw/"month";write_bundle(month,tables=tables)
    annual=raw/"annual.parquet";shutil.copyfile(month/"observations.parquet",annual)
    rule=root/a.ECONOMICS_RULEBOOK_PATH;rule.parent.mkdir(parents=True)
    shutil.copyfile(Path(__file__).resolve().parents[1]/a.ECONOMICS_RULEBOOK_PATH,rule)
    source=[]
    for name,family in (("ohlcv_1m.dbn.zst","ohlcv_1m"),("definition.dbn.zst","definition")):
        source.append(dict(path="synthetic/"+name,sha256=digest,size=(raw/name).stat().st_size,
            market="ES",family=family,kind="DBN",start_ns=0,end_ns=a.CUTOFF))
    payloads=[dict(path=p.relative_to(root).as_posix(),sha256=a.sha256_file(p),size=p.stat().st_size)
              for p in [*month.iterdir(),annual]]
    p=part(end=START+a.MINUTE)
    p.update(source_contract_id=digest,source_release_id=digest,directory="synthetic/month",
             year=2023,interval="synthetic",certificate_id="generated")
    plan=a.signed(dict(repository_root=str(root.resolve()),limits=a.asdict(a.LIMITS),scope="GENERATED_TEST_ONLY",
        environment=a.environment(),metadata={},implementation={},sources=source,payloads=payloads,
        months=[p],expected_rows=1,annual=[dict(market="ES",year=2023,parquet_path="synthetic/annual.parquet",
        source_bindings=[dict(path="synthetic/month/observations.parquet")],row_count=1,permitted_uses=["SYNTHETIC_TEST"])]),"plan_id")
    directory=root/a.HERE/plan["plan_id"];directory.mkdir(parents=True)
    (directory/"plan.json").write_bytes(a.canonical_bytes(plan)+b"\n")
    (directory/"authorization.json").write_text(json.dumps(dict(status="OWNER_APPROVED_IN_THIS_TASK",
        plan_id=plan["plan_id"],scope=plan["scope"],owner_message="GENERATED_TEST_ONLY")))
    return plan,(directory/"authorization.json").relative_to(root).as_posix()


def test_complete_generated_execution_and_interval_accounting(tmp_path):
    plan,auth=execution_fixture(tmp_path)
    result=a.execute(tmp_path,plan,auth)
    assert result["status"]=="PASS_FOUNDATION_INTEGRITY_WITH_COVERAGE_LIMITATIONS",result
    assert result["months_completed"]==result["annual_completed"]==1
    assert result["totals"]["observed_ns"]==a.MINUTE
    assert result["model_specific_training_readiness"]=="NOT_DECLARED"


def test_interrupted_scan_never_reports_success(tmp_path,monkeypatch):
    plan,auth=execution_fixture(tmp_path)
    def fail(*args):
        raise IntegrityError("injected resource limit")
    monkeypatch.setattr(a,"groups",fail)
    result=a.execute(tmp_path,plan,auth)
    assert result["status"]=="INCOMPLETE" and result["months_completed"]==0
    assert not (tmp_path/a.HERE/plan["plan_id"]/"supervision.json").exists()


@pytest.mark.skipif(a.sys.platform!="win32",reason="Windows native containment")
def test_generated_worker_is_supervised_to_completion(tmp_path):
    plan,auth=execution_fixture(tmp_path)
    record=a.supervise(tmp_path,a.HERE+"/"+plan["plan_id"]+"/plan.json",auth)
    assert record["status"]=="PASS_SUPERVISED_FOUNDATION_AUDIT",record
    assert record["workers_stopped"] is True
