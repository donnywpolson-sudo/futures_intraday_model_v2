"""Generated hourly releases only; no historical inputs or research admission."""
from copy import deepcopy
from dataclasses import replace
import gzip
import json
import os
import shutil

import pytest

from futures_rebuild import hourly_foundation_audit as audit
from futures_rebuild import causal_timeframe_publication as publication
from futures_rebuild.canonical import sha256_file, canonical_bytes
from futures_rebuild.causal_timeframe_storage import Budget, signed, contained
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.causal_1h import produce_day
from tests.test_causal_1h import sample, spec, RULEBOOK, LIMITS, BASE, DAY
from tests.test_causal_timeframe_publication import candidate, publication_receipt

pytestmark = pytest.mark.current


def synthetic_reader(root):
    """Use real window mechanics over generated releases, without historical evidence."""
    from futures_rebuild.hourly_quality_reader import HourlyReader, POINTER
    from futures_rebuild.causal_observation_parquet import read_table
    reader = object.__new__(HourlyReader)
    reader.root = root
    reader.bindings = {}
    reader.derived = []
    index = audit.read_record(contained(root, publication.INDEX))
    reader.entries = {(e['market'], e['month']): e for e in index['entries']}
    def native_month(market, month):
        entry = reader.entries.get((market, month))
        if not entry or entry['disposition'] != 'CERTIFIED':
            raise UnauthorizedOperation('Native hourly month is unavailable')
        return [dict(r, origin='NATIVE_1H', quality_disposition='CORROBORATED_BY_FULL_MINUTES')
                for r in read_table(contained(root, entry['directory'] + '/observations.parquet'), name='observations')]
    reader._native_month = native_month
    pointer = contained(root, POINTER)
    pointer.parent.mkdir(exist_ok=True)
    pointer.write_text('generated route guard only')
    return reader


@pytest.fixture(autouse=True)
def generated_reader_only(monkeypatch):
    monkeypatch.setattr(audit, 'HourlyReader', synthetic_reader)


def source_map(days):
    result = {}
    for day in days:
        for row, family in [(r, "ohlcv_1h") for r in day.bars] + [(r, "definition") for r in day.definitions] + [(r, "ohlcv_1m") for r in day.references["1m"]]:
            result[row.source_file_path] = {"path": row.source_file_path, "sha256": row.source_file_sha256,
                "market": "ES", "family": family, "kind": "DBN", "start_ns": BASE - 10*DAY, "end_ns": BASE + 10*DAY}
    return result


def inspect(day=None):
    day = sample() if day is None else day
    tables, _ = produce_day(day, spec(), RULEBOOK, None)
    entry = {"market": "ES", "coverage_start_ns": BASE, "coverage_end_ns": BASE + DAY}
    return tables, entry, {"spec": spec().identity["spec"]}, source_map([day])


def run_inspect(args, events=None, previous=None):
    return audit.inspect_group(*args, int(RULEBOOK.rules["ES"].expected_unit_qty * 10**9), previous,
                               (events if events is not None else []).append)


@pytest.mark.parametrize("kind,expected", [("match", "MATCH"), ("gap", "UNKNOWN_CAUSE"),
    ("identity", "UNKNOWN_CAUSE"), ("both", "UNKNOWN_CAUSE"), ("missing", "MISSING_MINUTE_REFERENCE"),
    ("disagree", "RECORDED_DISAGREEMENT")])
def test_comparisons_preserve_bars_and_do_not_infer_causes(kind, expected):
    day = sample(n=1)
    refs = day.references["1m"]
    if kind in ("gap", "both"):
        refs = refs[:-1]
    if kind in ("identity", "both"):
        refs = (replace(refs[0], instrument_id=999),) + refs[1:]
    if kind == "missing":
        refs = ()
    if kind == "disagree":
        refs = (replace(refs[0], volume=99),) + refs[1:]
    args = inspect(replace(day, references={"1m": refs}))
    before = deepcopy(args[0])
    events = []
    counts, _ = run_inspect(args, events)
    assert counts["cause_" + expected] == 1
    assert args[0] == before and args[0]["observations"][0]["volume"] == 600
    assert all(e.get("training_eligibility", "NOT_DECLARED") == "NOT_DECLARED" for e in events)


@pytest.mark.parametrize("kind", ["premature", "late_definition", "units", "ohlc", "cadence", "quality_reference", "missingness", "duplicate", "roll", "protected"])
def test_corrupt_mechanics_rejected(kind):
    args = inspect()
    tables = args[0]
    row = tables["observations"][0]
    if kind == "premature": row["available_at_ns"] = row["bar_start_ns"]
    elif kind == "late_definition": row["definition_received_at_ns"] = row["bar_start_ns"] + 1
    elif kind == "units": row["multiplier_nano"] = 0
    elif kind == "ohlc": row["high_nano"] = row["low_nano"] - 1
    elif kind == "cadence": tables["cadence"][0]["comparison_cadence"] = "1d"
    elif kind == "quality_reference": tables["quality"][0]["row_id"] = "other"
    elif kind == "missingness": tables["missingness"][0]["interval_end_ns"] += 1
    elif kind == "duplicate": tables["observations"].append(deepcopy(row))
    elif kind == "roll": tables["roll"][0]["roll_flag"] = True
    elif kind == "protected": args[2]["spec"]["end_ns"] = row["bar_end_ns"]
    with pytest.raises(IntegrityError):
        run_inspect(args)


@pytest.mark.parametrize("price", [0, -1_000_000_000])
def test_valid_zero_and_negative_prices(price):
    day = sample(n=1)
    bars = tuple(replace(b, open_nano=price, high_nano=price, low_nano=price, close_nano=price) for b in day.bars)
    counts, _ = run_inspect(inspect(replace(day, bars=bars)))
    assert counts["negative_price_rows"] == (price < 0)
    assert counts["zero_price_rows"] == (price == 0)


def test_definition_event_clock_does_not_override_received_by_start_contract():
    day = sample(n=1)
    definitions = (replace(day.definitions[0], ts_event_ns=day.bars[0].event_at_ns + 1),)
    counts, _ = run_inspect(inspect(replace(day, definitions=definitions)))
    assert counts["definition_event_after_bar_start"] == 1


def published(root, start=BASE, end=BASE+DAY):
    relative = candidate(root, start=start, end=end)
    plan = publication.prepare_publication(root, candidates=[relative])
    index = publication.publish(root, plan, publication_receipt(root, plan), LIMITS)
    economics = contained(root, audit.ECONOMICS_RULEBOOK_PATH)
    economics.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(audit.Path(__file__).resolve().parents[1] / audit.ECONOMICS_RULEBOOK_PATH, economics)
    days = [sample(d) for d in range(start, end, DAY)]
    sources = source_map(days)
    for s in sources.values():
        s.update(start_ns=start, end_ns=end)
    parts = []
    for e in index["entries"]:
        if e["disposition"] == "UNAVAILABLE":
            anchor = next(x for x in index["entries"] if x["disposition"] == "CERTIFIED")
            parts.append({**e, "rows": 0, "manifest": anchor["manifest"], "evidence_entry": anchor,
                "seal_path": relative.rsplit("/", 1)[0] + "/" + e["month"] + ".seal.json"})
            continue
        release = json.loads(contained(root, e["manifest"]).read_bytes())
        parts.append({**e, "rows": sum(d["observations"] for d in release["seal"]["coverage"])})
    output = root / "audit"
    output.mkdir()
    return {"parts": parts, "sources": list(sources.values()), "rows": sum(p["rows"] for p in parts)}, Budget(output, LIMITS)


def test_complete_scan_reproduces_replay_fingerprints_and_accounts_for_gaps(tmp_path):
    plan, budget = published(tmp_path)
    before = {p: sha256_file(p) for p in tmp_path.rglob("*.parquet")}
    result = audit.scan(tmp_path, plan, budget)
    assert result["totals"]["observations"] == 2
    assert result["totals"]["observed_ns"] + result["totals"]["uncovered_ns"] == DAY
    assert result["model_specific_eligibility"] == "NOT_DECLARED"
    assert result['totals']['native_accepted'] == 2
    assert any(case['result'] == 'ACCEPTED' for case in result['actual_window_reads'])
    assert before == {p: sha256_file(p) for p in before}
    with gzip.open(budget.directory / "exceptions.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    assert any(e["kind"] == "UNCOVERED_INTERVAL" for e in events)


def test_year_boundary_scan(tmp_path):
    start = audit.source_time("2023-12-31")
    plan, budget = published(tmp_path, start=start, end=start+2*DAY)
    result = audit.scan(tmp_path, plan, budget)
    assert result["totals"]["partitions"] == 2
    assert result['totals']['native_accepted'] == 4
    assert result["totals"]["observed_ns"] + result["totals"]["uncovered_ns"] == 2*DAY


@pytest.mark.parametrize("altered", [False, True])
def test_empty_month_refused_without_disabling_other_months(tmp_path, monkeypatch, altered):
    from tests import test_causal_timeframe_publication as fixtures
    start = audit.source_time("2024-01-31")
    monkeypatch.setattr(fixtures, "sample", lambda day: sample(day, n=0 if day == start else 2))
    plan, budget = published(tmp_path, start=start, end=start+2*DAY)
    assert plan["parts"][0]["disposition"] == "UNAVAILABLE"
    if altered:
        path = contained(tmp_path, plan["parts"][0]["seal_path"])
        seal = json.loads(path.read_bytes())
        seal["coverage"][0]["observations"] = 1
        path.write_bytes(canonical_bytes(signed({k: v for k, v in seal.items() if k != "id"})))
        with pytest.raises(IntegrityError, match="empty-month evidence"):
            audit.scan(tmp_path, plan, budget)
    else:
        result = audit.scan(tmp_path, plan, budget)
        assert result["totals"]["certified_empty_months"] == 1
        assert result["totals"]["partitions"] == 2
        assert result["totals"]["observed_ns"] + result["totals"]["uncovered_ns"] == 2*DAY
        assert result['unavailable_months'] == [['ES', '2024-01']]
        assert any(case['result'] == 'ACCEPTED' for case in result['actual_window_reads'])
        assert any(case['case'] == 'unavailable_ES' and case['result'] == 'REFUSED' for case in result['actual_window_reads'])
        assert result["model_specific_eligibility"] == "NOT_DECLARED"


@pytest.mark.parametrize("kind", ["payload", "support_missing", "row_count", "selection", "fingerprint", "accounting"])
def test_scan_fails_on_lost_or_altered_evidence(tmp_path, monkeypatch, kind):
    plan, budget = published(tmp_path)
    entry = plan["parts"][0]
    if kind == "payload":
        path = contained(tmp_path, entry["directory"] + "/" + audit.FILENAMES["observations"])
        path.write_bytes(path.read_bytes() + b"altered")
    elif kind == "support_missing":
        contained(tmp_path, entry["directory"] + "/" + audit.FILENAMES["quality"]).unlink()
    elif kind == "row_count": plan["rows"] += 1
    elif kind == "selection":
        def wrong_reader(root):
            reader = synthetic_reader(root)
            reader.read_window = lambda **kw: []
            return reader
        monkeypatch.setattr(audit, 'HourlyReader', wrong_reader)
    else:
        original = publication.validate_release
        def altered(root, entry):
            release = deepcopy(original(root, entry))
            if kind == "fingerprint":
                for p in release["certificate"]["passes"]: p["logical_sha256"] = "0"*64
            else: release["seal"]["coverage"][0]["source_primary_rows"] += 1
            return release
        monkeypatch.setattr(publication, "validate_release", altered)
    with pytest.raises((IntegrityError, FileNotFoundError)):
        audit.scan(tmp_path, plan, budget)


def test_resource_limit_is_not_a_pass(tmp_path):
    plan, budget = published(tmp_path)
    budget.limits = replace(LIMITS, maximum_written_bytes=1)
    with pytest.raises(IntegrityError, match="resource"):
        audit.scan(tmp_path, plan, budget)


def test_incomplete_campaign_and_missing_approval_rejected(tmp_path):
    core = {"repository_root": str(tmp_path.resolve()), "limits": audit.asdict(audit.LIMITS),
        "status": "BLOCKED_RELEASE_INCOMPLETE", "blockers": ["campaign running"],
        "expected_markets": list(audit.V10_CHECKPOINT_MARKETS), "parts": [{"market": m} for m in audit.V10_CHECKPOINT_MARKETS]}
    with pytest.raises(UnauthorizedOperation, match="complete 41"):
        audit.authorize(tmp_path, signed(core), {})
    core.update(status="PREPARED_REQUIRES_OWNER_APPROVAL", blockers=[])
    with pytest.raises(UnauthorizedOperation, match="owner approval"):
        audit.authorize(tmp_path, signed(core), {})


def test_stale_binding_rejected_before_data_open(tmp_path, monkeypatch):
    p = tmp_path / "metadata.json"
    p.write_text("old")
    plan = {"environment": {}, "metadata": {"metadata.json": {"sha256": sha256_file(p)}}}
    from futures_rebuild import causal_1h
    monkeypatch.setattr(causal_1h, "ENVIRONMENT", {})
    p.write_text("changed")
    with pytest.raises(IntegrityError, match="prepared input changed"):
        audit.verify_bindings(tmp_path, plan, Budget(tmp_path, LIMITS))


def generated_worker():
    # Narrow the all-market assertion only inside this generated-data child process.
    audit.V10_CHECKPOINT_MARKETS = ("ES",)
    audit.HourlyReader = synthetic_reader
    raise SystemExit(audit.worker())


def test_generated_worker_and_supervisor_complete_without_history(tmp_path, monkeypatch):
    from dataclasses import asdict
    from futures_rebuild.causal_1h import build_candidate, certify_candidate, ENVIRONMENT
    from futures_rebuild import causal_1h_native as native
    repo = audit.Path(__file__).resolve().parents[1]
    monkeypatch.setenv("PYTHONPATH", str(repo) + os.pathsep + str(repo / "src"))
    day = sample()
    for relative in source_map([day]):
        path = contained(tmp_path, relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"generated source only")
    def bound(row):
        return replace(row, source_file_sha256=sha256_file(contained(tmp_path, row.source_file_path)))
    day = replace(day, bars=tuple(map(bound, day.bars)), definitions=tuple(map(bound, day.definitions)),
                  references={"1m": tuple(map(bound, day.references["1m"]))})
    relative = "state/unpublished_evidence/generated_hourly/candidate.json"
    directory = contained(tmp_path, "state/unpublished_evidence/generated_hourly")
    build_candidate(directory, spec(), lambda: iter([day]), RULEBOOK, LIMITS)
    certify_candidate(directory, spec(), lambda: iter([day]), RULEBOOK, LIMITS)
    catalog = contained(tmp_path, "data/active/catalog.json")
    catalog.parent.mkdir(parents=True)
    catalog.write_text("{}")
    pub_plan = publication.prepare_publication(tmp_path, candidates=[relative])
    index = publication.publish(tmp_path, pub_plan, publication_receipt(tmp_path, pub_plan), LIMITS)
    economics = contained(tmp_path, audit.ECONOMICS_RULEBOOK_PATH)
    economics.parent.mkdir(parents=True)
    shutil.copyfile(audit.Path(__file__).resolve().parents[1] / audit.ECONOMICS_RULEBOOK_PATH, economics)
    sources = source_map([day])
    for source in sources.values(): source["size_bytes"] = contained(tmp_path, source["path"]).stat().st_size
    core = {"repository_root": str(tmp_path.resolve()), "status": "PREPARED_REQUIRES_OWNER_APPROVAL",
        "blockers": [], "expected_markets": ["ES"], "limits": asdict(audit.LIMITS), "metadata": {},
        "environment": ENVIRONMENT, "parts": [{**e, "rows": 2} for e in index["entries"]],
        "sources": list(sources.values()), "rows": 2}
    plan = signed(core)
    relative_dir = audit.HERE + "/" + plan["id"]
    output = contained(tmp_path, relative_dir)
    output.mkdir(parents=True)
    (output / "plan.json").write_bytes(canonical_bytes(plan))
    (output / "authorization.json").write_text(json.dumps({"plan_id": plan["id"],
        "status": "OWNER_APPROVED_IN_THIS_TASK", "owner_message": "GENERATED_TEST_ONLY"}))
    monkeypatch.setattr(audit, "V10_CHECKPOINT_MARKETS", ("ES",))
    original = native.supervise
    def supervised(command, root, request, limits):
        return original([audit.sys.executable, "-B", "-c",
            "from tests.test_hourly_foundation_audit import generated_worker; generated_worker()"], root, request, limits)
    monkeypatch.setattr(native, "supervise", supervised)
    record = audit.supervise(tmp_path, relative_dir + "/plan.json", relative_dir + "/authorization.json")
    assert record["status"] == "PASS_SUPERVISED_FOUNDATION_AUDIT", record
    assert record["conservative_aggregate_peak_bytes"] <= audit.LIMITS.maximum_memory_bytes
    assert record["workers_stopped"]
    assert (output / "execution" / "REPORT.md").exists()


def test_supervisor_failure_cannot_produce_success_receipt(tmp_path, monkeypatch):
    from futures_rebuild import causal_1h_native as native
    core = {"repository_root": str(tmp_path.resolve()), "status": "PREPARED_REQUIRES_OWNER_APPROVAL",
        "blockers": [], "expected_markets": list(audit.V10_CHECKPOINT_MARKETS), "limits": audit.asdict(audit.LIMITS),
        "parts": [{"market": m} for m in audit.V10_CHECKPOINT_MARKETS]}
    plan = signed(core)
    relative = audit.HERE + "/" + plan["id"]
    directory = contained(tmp_path, relative)
    directory.mkdir(parents=True)
    (directory / "plan.json").write_bytes(canonical_bytes(plan))
    (directory / "authorization.json").write_text(json.dumps({"plan_id": plan["id"],
        "status": "OWNER_APPROVED_IN_THIS_TASK", "owner_message": "GENERATED_TEST_ONLY"}))
    def fail(*args):
        raise IntegrityError("injected whole-attempt timeout")
    monkeypatch.setattr(native, "supervise", fail)
    result = audit.supervise(tmp_path, relative + "/plan.json", relative + "/authorization.json")
    assert result["status"] == "INCOMPLETE" and "timeout" in result["error"]
    with pytest.raises(IntegrityError, match="already used"):
        audit.supervise(tmp_path, relative + "/plan.json", relative + "/authorization.json")
