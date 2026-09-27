"""Generated-data checks of the actual inactive ES census path.

All prices, contracts, calendars, certificates and scope sizes below are synthetic.
Policy/receipt/resolver doubles are confined to each generated root; there is no
production test switch or historical authorization. The Arrow reader/decoder,
metadata preparation, date classification and output accounting remain real.
"""
from copy import deepcopy
from datetime import date, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import pyarrow as pa

from futures_rebuild import es_close_source_readiness as m
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.causal_observation_parquet import write_table
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation

H = sha256_json("SYNTHETIC_CENSUS_PROVENANCE")
PRICE_SENTINEL = -9876543210987654
REHEARSAL = {}


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value)+b"\n")


def bind(value, name):
    return {**value, name: sha256_json(value)}


def generated_row(day, clock, **changes):
    t = m._stamp(day, clock)
    row = {n: H for n in m.SCHEMAS["observations"].names if n.endswith("sha256") or n in
           ("source_contract_id", "source_release_id")}
    row.update(market="ES", source_file_path="SYNTHETIC_UNOPENED_SOURCE.parquet",
               source_cadence="1m", bar_start_ns=t, bar_end_ns=t+60*m.NS,
               source_timestamp_ns=t, available_at_ns=t+65*m.NS,
               decision_eligible_at_ns=t+65*m.NS, publisher_id=1, instrument_id=7,
               raw_symbol="SYNTHETIC_ES", actual_contract="SYNTHETIC_ES_M4",
               definition_source_file_path="SYNTHETIC_UNOPENED_DEFINITIONS.parquet",
               definition_event_at_ns=m._ns("2024-01-01"),
               definition_received_at_ns=m._ns("2024-01-01"),
               listing_activation_ns=m._ns("2024-01-01"), expiration_ns=m._ns("2025-01-01"),
               open_nano=PRICE_SENTINEL, high_nano=PRICE_SENTINEL,
               low_nano=PRICE_SENTINEL, close_nano=PRICE_SENTINEL, volume=123456789,
               currency="USD", min_price_increment_nano=250000000, multiplier_nano=50000000000,
               project_session_id="SYNTHETIC-"+day, project_trade_date=day,
               project_grouping_start_ns=t-3600*m.NS, project_grouping_end_ns=t+3600*m.NS,
               project_timezone="America/Chicago", official_schedule_state="UNKNOWN")
    row["source_row_sha256"] = sha256_json(["SYNTHETIC_ROW", t])
    row.update(changes)
    return row


@pytest.fixture
def fixture_factory(tmp_path, monkeypatch):
    """Scope/calendar/receipt/resolver are explicit later-adoption test doubles."""
    created = []

    def make(*, mutate=None, calendar=None, large=False, count_offset=0, wrong_schema=False):
        root = m.io_path(tmp_path/str(len(created)))
        root.mkdir()
        created.append(root)
        monkeypatch.setattr(m, "SOURCE_YEARS", (2024,))
        monkeypatch.setattr(m, "START", "2024-01-01" if large else "2024-03-08")
        monkeypatch.setattr(m, "END", "2024-03-12T22:00:00Z")
        days = tuple(m._dates())
        known = m._ns("2024-01-01")
        cash = {d: {"cash_state": "CLOSED" if date.fromisoformat(d).weekday() >= 5 else "FULL",
                    "available_at_ns": known, "evidence_refs": ["SYNTHETIC_CALENDAR_EVIDENCE"]}
                for d in days}
        if calendar:
            calendar(cash)
        if large:
            # 100,000 complete generated one-minute rows; not market observations.
            start = m._stamp("2024-01-01", "00:00")
            rows = []
            for i in range(100000):
                t = start+i*60*m.NS
                day = m.datetime.fromtimestamp(t/m.NS, m.NY).date().isoformat()
                clock = m.datetime.fromtimestamp(t/m.NS, m.NY).strftime("%H:%M")
                rows.append(generated_row(day, clock))
        else:
            rows = [generated_row(d, clock) for d in days for clock in m.CLOCKS]
        if mutate:
            mutate(rows)
        rel = "data/active/causally_gated_normalized/ES/2024/2024.parquet"
        payload = root/rel
        if wrong_schema:
            from futures_rebuild.causal_observation_parquet import _encode_physical
            payload.parent.mkdir(parents=True, exist_ok=True)
            schema = m.SCHEMAS["observations"].with_metadata({"SYNTHETIC": "WRONG_SCHEMA"})
            m.pq.write_table(pa.Table.from_pylist([_encode_physical("observations", r) for r in rows],
                                                schema=schema), payload)
        else:
            write_table(payload, name="observations",
                        row_groups=(rows[i:i+4096] for i in range(0, len(rows), 4096)))
        receipt = bind({"schema_version": m.active_data_view.CONTENT_RECEIPT_SCHEMA,
                        "state": m.active_data_view.CERTIFICATION_STATE,
                        "checks": {"SYNTHETIC_ONLY": "PASS"}}, "content_validation_receipt_id")
        access = m.active_data_view.build_access_policy_binding(
            market="ES", year=2024, universe_contract_sha256=H, active_view_id=H,
            content_validation_receipt_id=receipt["content_validation_receipt_id"],
            coverage_start=m.START, coverage_end=m.END)
        entry = dict(market="ES", year=2024, parquet_path=rel, parquet_sha256=sha256_file(payload),
                     sidecar_path=rel+".manifest.json", row_count=len(rows)+count_offset, schema_fingerprint=H,
                     coverage_start=m.START, coverage_end=m.END, cohort=access["cohort"],
                     permitted_uses=access["permitted_uses"], selection_eligible=access["selection_eligible"],
                     access_policy_binding_id=access["access_policy_binding_id"],
                     content_validation_receipt_id=receipt["content_validation_receipt_id"])
        side = {"entry_binding": deepcopy(entry), "access_policy_binding": access,
                "content_validation_receipt": receipt}
        put(root/entry["sidecar_path"], side)
        entry["sidecar_sha256"] = sha256_file(root/entry["sidecar_path"])
        put(root/"data/active/catalog.json", {"entries": [entry]})
        (root/m.DECLARATION).parent.mkdir(parents=True, exist_ok=True)
        (root/m.DECLARATION).write_text("SYNTHETIC_DECLARATION_NO_RESEARCH_AUTHORITY", encoding="utf-8")
        monkeypatch.setattr(m, "DECLARATION_SHA", sha256_file(root/m.DECLARATION))
        put(root/m.CALENDAR, {"cash_days": cash, "source_files": {}})
        proposal = {"execution_enabled": False, "authority": {"historical_read": False},
                    "proposed_operation": m.OPERATION, "purpose": "LINEAGE_AUDIT",
                    "projected_fields": list(m.FIELDS),
                    "scope": {"markets": ["ES"], "calendar_start": m.START,
                              "available_at_exclusive_end": m.END, "candidate_clock_minutes_ny": list(m.CLOCKS)},
                    "declaration_path": m.DECLARATION.as_posix(), "declaration_sha256": m.DECLARATION_SHA,
                    "catalog_sha256": sha256_file(root/"data/active/catalog.json"),
                    "source_files": [{**entry, "payload_bytes_by_stat": payload.stat().st_size}],
                    "caps_proposed_not_measured": {**m.LIMITS, "source_files": 1, "declared_rows": len(rows)+count_offset,
                                                   "source_file_bytes_by_stat": payload.stat().st_size}}
        put(root/m.PROPOSAL, proposal)
        monkeypatch.setattr(m, "PROPOSAL_SHA", sha256_file(root/m.PROPOSAL))
        op = m.prepare(root)
        operation_path = m.BASE/"synthetic_operation.json"
        put(root/operation_path, op)
        adoption = bind({"state": "ADOPTED_INACTIVE_RECEIPT_REQUIRED", "operation": m.OPERATION,
                         "scope": m.scope(op), "operation_path": operation_path.as_posix(),
                         "operation_sha256": sha256_file(root/operation_path)}, "adoption_id")
        put(root/m.APPROVED, adoption)
        calls = {"resolver": 0, "footer": 0, "projections": []}
        original_reader = m.pq.ParquetFile

        class ArrowSpy:
            def __init__(self, path):
                actual = Path(str(path).removeprefix("\\\\?\\")).resolve()
                assert actual == Path(str(payload).removeprefix("\\\\?\\")).resolve(), "NATIVE_ARROW_REAL_SOURCE_ACCESS_FORBIDDEN"
                calls["footer"] += 1
                self.reader = original_reader(path)

            def __getattr__(self, name):
                return getattr(self.reader, name)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.reader.close()

            def iter_batches(self, **kwargs):
                assert kwargs["columns"] == list(m.FIELDS)
                assert set(kwargs["columns"]).isdisjoint(m.EXCLUDED)
                assert kwargs["use_threads"] is False
                calls["projections"].append(kwargs["columns"])
                return self.reader.iter_batches(**kwargs)

        def resolver(*, repository_root, market, year, purpose):
            assert Path(repository_root) == root and (market, year, purpose) == ("ES", 2024, "LINEAGE_AUDIT")
            calls["resolver"] += 1
            assert sha256_file(payload) == entry["parquet_sha256"]  # Only generated fixture bytes.
            return payload

        def policy(operation, requested):
            assert operation == m.OPERATION and requested == m.scope(op)
            assert root != m.CODE_ROOT and Path(str(root).removeprefix("\\\\?\\")).is_relative_to(tmp_path)

        class ReceiptDouble:
            """One-use fixture-only double; no external approval is issued."""
            def __init__(self):
                self.scope = tuple(sorted(m.scope(op).items()))
                self.used = False
                self.valid = True
                self.path = root/"state/authorization_uses/SYNTHETIC.json"

            def verify(self, boundary, *, operation, classification, required_scope):
                if (boundary.active_root != root or operation != m.OPERATION
                        or classification is not m.OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
                        or dict(self.scope) != required_scope or not self.valid):
                    raise UnauthorizedOperation("synthetic receipt rejected")

            def consume(self, boundary, **kwargs):
                self.verify(boundary, **kwargs)
                if self.used:
                    raise UnauthorizedOperation("synthetic receipt consumed")
                self.used = True
                put(self.path, {"SYNTHETIC_RECEIPT_DOUBLE": True})
                return self.path

            def assert_consumed(self, boundary, **kwargs):
                self.verify(boundary, **kwargs)
                if not self.used or not self.path.exists():
                    raise UnauthorizedOperation("synthetic receipt is not consumed")
                return self.path

        monkeypatch.setattr(m.pq, "ParquetFile", ArrowSpy)
        monkeypatch.setattr(m.active_data_view, "resolve", resolver)
        monkeypatch.setattr(m, "require_current_real_history_operation", policy)
        monkeypatch.setattr(m, "OperationReceipt", ReceiptDouble)
        return SimpleNamespace(root=root, op=op, rows=rows, calls=calls, payload=payload, proposal=proposal,
                               receipt=ReceiptDouble(), boundary=m.RepoBoundary(root),
                               output=root/m.DESTINATION/op["operation_id"][:16])
    return make


def run(f):
    return m.execute_once(f.boundary, f.receipt, f.op)


def record(f, day):
    return json.loads((f.output/"dates"/(day+".json")).read_text())


def test_actual_projection_clock_calendar_and_exact_accounting(fixture_factory):
    f = fixture_factory()
    result = run(f)
    monday = record(f, "2024-03-11")
    assert monday["previous_cash_date"] == "2024-03-08"
    assert monday["input_state"] == "REFERENCES_PRESENT_SELECTION_UNVERIFIED"
    assert monday["outcome_references"] == "REFERENCES_PRESENT"
    assert record(f, "2024-03-08")["decision_utc_ns"] == m._ns("2024-03-08T20:29:05Z")
    assert monday["decision_utc_ns"] == m._ns("2024-03-11T19:29:05Z")
    assert monday["decision_pacific"] == "2024-03-11T12:29:05-07:00"
    assert result["totals"]["dates"] == 5 and result["totals"]["rows"] == 20
    assert f.calls["resolver"] == f.calls["footer"] == 1
    actual = sum(p.stat().st_size for p in f.output.rglob("*") if p.is_file())+f.receipt.path.stat().st_size
    assert result["resources"]["retained_bytes"] == actual
    assert result["resources"]["fits"] == 0 and result["qualification"] == "UNAVAILABLE"
    emitted = "".join(p.read_text() for p in f.output.rglob("*.json"))
    assert str(PRICE_SENTINEL) not in emitted and "123456789" not in emitted
    assert all(k not in emitted for k in m.EXCLUDED)


def test_metadata_prepare_never_reads_payload(fixture_factory, monkeypatch):
    f = fixture_factory()
    monkeypatch.setattr(m.active_data_view, "resolve", lambda **kw: pytest.fail("payload resolver called"))
    monkeypatch.setattr(m.pq, "ParquetFile", lambda *a, **kw: pytest.fail("footer opened"))
    assert m.prepare(f.root) == f.op
    path = f.root/m.BASE/"prepared.json"
    m.prepare(f.root, path)
    assert path.exists()
    with pytest.raises(UnauthorizedOperation):
        m.prepare(f.root, f.root/"active.json")
    with pytest.raises(UnauthorizedOperation):
        m.prepare(f.root, f.root/m.BASE/".."/".."/"active.json")
    assert not (f.root/"active.json").exists()


@pytest.mark.parametrize("bad", ["missing", "wrong_scope", "extra_scope", "expired", "consumed", "unadopted"])
def test_access_rejections_precede_source_reads(fixture_factory, monkeypatch, bad):
    f = fixture_factory()
    receipt = f.receipt
    if bad == "missing":
        receipt = None
    elif bad == "wrong_scope":
        receipt.scope = (("operation_id", H),)
    elif bad == "extra_scope":
        receipt.scope += (("price_access", "yes"),)
    elif bad == "expired":
        receipt.valid = False
    elif bad == "consumed":
        receipt.used = True
    else:
        from futures_rebuild.research_gateway_policy import require_current_real_history_operation
        monkeypatch.setattr(m, "require_current_real_history_operation", require_current_real_history_operation)
    with pytest.raises(UnauthorizedOperation):
        m.execute_once(f.boundary, receipt, f.op)
    assert f.calls["resolver"] == f.calls["footer"] == 0


@pytest.mark.parametrize("bad", ["catalog", "sidecar", "declaration", "code", "projection", "extra_market", "boundary"])
def test_stale_or_widened_scope_never_reaches_resolver(fixture_factory, monkeypatch, bad):
    f = fixture_factory()
    if bad in ("catalog", "sidecar", "declaration"):
        rel = {"catalog": "data/active/catalog.json",
               "sidecar": f.op["sources"][0]["sidecar_path"], "declaration": m.DECLARATION.as_posix()}[bad]
        with (f.root/rel).open("ab") as out:
            out.write(b" ")
    elif bad == "code":
        f.op["implementation_bindings"][m.CODE_FILES[0]] = H
    elif bad == "projection":
        f.op["fields"].append("open_nano")
    elif bad == "extra_market":
        f.op["sources"][0]["market"] = "NQ"
    else:
        f.op["sources"][0]["coverage_end"] = "2026-01-01"
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        run(f)
    assert f.calls["resolver"] == f.calls["footer"] == 0


@pytest.mark.parametrize("change,expected", [
    ("shifted_ns", "MISSING_OR_INVALID_INPUT_REFERENCE"),
    ("late", "INPUT_LATE"),
    ("contract", "PREVIOUS_CONTRACT_OR_SPEC_MISMATCH"),
    ("invalid_definition", "MISSING_OR_INVALID_INPUT_REFERENCE"),
    ("invalid_tick", "MISSING_OR_INVALID_INPUT_REFERENCE"),
    ("duplicate", "MISSING_OR_INVALID_INPUT_REFERENCE"),
])
def test_reference_defects_are_explicit(fixture_factory, change, expected):
    def mutate(rows):
        r = next(r for r in rows if r["bar_start_ns"] == m._stamp("2024-03-11", "15:28"))
        if change == "shifted_ns":
            for k in ("bar_start_ns", "bar_end_ns", "source_timestamp_ns", "available_at_ns", "decision_eligible_at_ns"):
                r[k] += 1
        elif change == "late":
            r["decision_eligible_at_ns"] += m.NS
        elif change == "contract":
            r = next(r for r in rows if r["bar_start_ns"] == m._stamp("2024-03-08", "15:59"))
            r["actual_contract"] = "SYNTHETIC_OTHER_CONTRACT"
        elif change == "invalid_definition":
            r["definition_received_at_ns"] = r["available_at_ns"]+m.NS
        elif change == "invalid_tick":
            r["min_price_increment_nano"] = 0
        else:
            rows.insert(rows.index(r), deepcopy(r))
    f = fixture_factory(mutate=mutate)
    run(f)
    assert record(f, "2024-03-11")["input_state"] == expected


@pytest.mark.parametrize("change,expected", [
    ("short", "SHORT_CURRENT_OR_PRECEDING"), ("unknown", "CALENDAR_UNKNOWN"),
    ("later_known", "REFERENCES_PRESENT_SELECTION_UNVERIFIED")])
def test_calendar_knowledge_and_no_skipping_short_sessions(fixture_factory, change, expected):
    def calendar(c):
        if change == "short":
            c["2024-03-08"]["cash_state"] = "SHORT"
        elif change == "unknown":
            c["2024-03-10"] = {"cash_state": "UNKNOWN", "available_at_ns": None, "evidence_refs": []}
        else:
            c["2024-03-08"]["available_at_ns"] = m._stamp("2024-03-11", "14:00")
    f = fixture_factory(calendar=calendar)
    run(f)
    assert record(f, "2024-03-11")["input_state"] == expected


def test_missing_future_exit_cannot_remove_input_or_date(fixture_factory):
    def mutate(rows):
        rows[:] = [r for r in rows if r["bar_start_ns"] != m._stamp("2024-03-11", "16:00")]
    f = fixture_factory(mutate=mutate)
    result = run(f)
    monday = record(f, "2024-03-11")
    assert monday["input_state"] == "REFERENCES_PRESENT_SELECTION_UNVERIFIED"
    assert monday["outcome_references"] == "UNOBSERVABLE"
    assert result["totals"]["dates"] == 5


def test_failed_partial_attempt_persists_and_cannot_resume(fixture_factory):
    def mutate(rows):
        rows[-2], rows[-1] = rows[-1], rows[-2]
    f = fixture_factory(mutate=mutate)
    with pytest.raises(IntegrityError):
        run(f)
    assert (f.output/"failure.json").exists() and not (f.output/"complete.json").exists()
    assert (f.output/"dates/2024-03-08.json").exists()
    before = {p.relative_to(f.output).as_posix(): sha256_file(p) for p in f.output.rglob("*") if p.is_file()}
    with pytest.raises(UnauthorizedOperation):
        run(f)
    assert before == {p.relative_to(f.output).as_posix(): sha256_file(p) for p in f.output.rglob("*") if p.is_file()}


@pytest.mark.parametrize("limit", ["working_set", "commit", "wall", "output"])
def test_resource_stop_before_source_read(fixture_factory, monkeypatch, limit):
    f = fixture_factory()
    if limit in ("working_set", "commit"):
        monkeypatch.setattr(m, "_memory", lambda: (m.LIMITS["peak_working_set_bytes"], 0)
                            if limit == "working_set" else (0, m.LIMITS["peak_commit_bytes"]))
    elif limit == "wall":
        old = m._Budget.__init__
        def expired(self, *a, **kw):
            old(self, *a, **kw)
            self.started -= m.LIMITS["wall_seconds"]+1
        monkeypatch.setattr(m._Budget, "__init__", expired)
    else:
        monkeypatch.setitem(m.LIMITS, "total_output_bytes", 10)
        # Reprepare to exercise the actual preflight allowance, not identity rejection.
        put(f.root/m.PROPOSAL, {**f.proposal, "caps_proposed_not_measured":
            {**f.proposal["caps_proposed_not_measured"], "total_output_bytes": 10}})
        monkeypatch.setattr(m, "PROPOSAL_SHA", sha256_file(f.root/m.PROPOSAL))
        f.op = m.prepare(f.root)
        f.receipt.scope = tuple(sorted(m.scope(f.op).items()))
        monkeypatch.setattr(m, "require_scope", lambda *a: None)  # Fixture adoption double only.
        monkeypatch.setattr(m, "require_current_real_history_operation", lambda *a: None)
    with pytest.raises(IntegrityError):
        run(f)
    assert f.calls["resolver"] == f.calls["footer"] == 0


def test_row_group_cap_precedes_decoding(fixture_factory, monkeypatch):
    f = fixture_factory()
    monkeypatch.setitem(m.LIMITS, "row_group_uncompressed_bytes", 1)
    # Permit unchanged fixture adoption; the real reader still checks row-group size.
    monkeypatch.setattr(m, "require_scope", lambda *a: None)
    with pytest.raises(IntegrityError, match="row group"):
        run(f)
    assert f.calls["projections"] == []


def test_100000_generated_rows_measure_real_decoder(fixture_factory):
    f = fixture_factory(large=True)
    started = m.time.perf_counter()
    result = run(f)
    elapsed = m.time.perf_counter()-started
    assert result["totals"]["rows"] == 100000
    assert len(f.calls["projections"]) == 1  # One iter_batches invocation, not one physical batch.
    REHEARSAL.update(rows=100000, full_call_seconds=elapsed, rows_per_second=100000/elapsed,
                     input_file_bytes=f.payload.stat().st_size, **result["resources"])


@pytest.mark.parametrize("kind", ["schema", "count"])
def test_actual_footer_schema_count_rejection(fixture_factory, kind):
    f = fixture_factory(wrong_schema=kind == "schema", count_offset=1 if kind == "count" else 0)
    with pytest.raises(IntegrityError, match="schema or count"):
        run(f)
    assert f.calls["footer"] == 1 and f.calls["projections"] == []


@pytest.mark.parametrize("kind", ["fields", "market", "boundary"])
def test_metadata_scope_rejects_rebound_invalid_proposal(fixture_factory, monkeypatch, kind):
    f = fixture_factory()
    p = deepcopy(f.proposal)
    if kind == "fields":
        p["projected_fields"].append("open_nano")
    elif kind == "market":
        p["source_files"][0]["market"] = "NQ"
    else:
        p["source_files"][0]["coverage_end"] = "2026-01-01"
    put(f.root/m.PROPOSAL, p)
    monkeypatch.setattr(m, "PROPOSAL_SHA", sha256_file(f.root/m.PROPOSAL))
    with pytest.raises(UnauthorizedOperation):
        m.prepare(f.root)
    assert f.calls["resolver"] == f.calls["footer"] == 0


def test_oversized_date_record_preserves_failure_only(fixture_factory, monkeypatch):
    f = fixture_factory()
    monkeypatch.setitem(m.LIMITS, "date_record_bytes", 16)
    monkeypatch.setattr(m, "require_scope", lambda *a: None)  # Generated adoption double only.
    with pytest.raises(IntegrityError, match="record/control"):
        run(f)
    assert (f.output/"failure.json").exists()
    assert not (f.output/"dates").exists()
    assert not (f.output/"complete.json").exists()


def test_module_has_no_predictive_calls_or_price_projection():
    import ast
    tree = ast.parse(Path(m.__file__).read_text())
    banned = {"close_inputs", "close_fit", "close_select", "close_score", "run_close_pilot", "fit", "predict"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            assert name not in banned
        if isinstance(node, ast.ImportFrom):
            assert node.module != "research_pilot"
    assert set(m.FIELDS).isdisjoint(m.EXCLUDED)
