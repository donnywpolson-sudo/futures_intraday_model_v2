from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild import l0_alpha_candidate as c
from futures_rebuild import l0_alpha_preparation as p
from futures_rebuild import l0_alpha_source_readiness as s
from futures_rebuild.canonical import write_new_canonical_authority as write
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation

pytestmark = pytest.mark.current


def test_metadata_preparation_never_opens_price_payloads(tmp_path, monkeypatch):
    entries = []
    for market in c.MARKETS:
        for year in range(2018, 2023):
            name = f"data/active/causally_gated_normalized/{market}/{year}/{year}.parquet"
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"synthetic payload; not opened")
            entries.append({"market": market, "year": year, "parquet_path": name, "sidecar_path": name + ".manifest.json",
                "coverage_start": f"{year}-01-01", "coverage_end": f"{year+1}-01-01", "disposition": "RESEARCH_READY_CAUSAL_PRICE",
                "permitted_uses": ["FEATURE_GENERATION"], "selection_eligible": True, "parquet_sha256": "a" * 64,
                "sidecar_sha256": "b" * 64, "row_count": 100, "content_validation_receipt_id": "c" * 64,
                "access_policy_binding_id": "d" * 64})
    write(tmp_path / "data/active/catalog.json", {"entries": entries})
    original = Path.read_bytes
    def guarded(path):
        assert path.suffix != ".parquet", "metadata preparation opened prices"
        return original(path)
    monkeypatch.setattr(Path, "read_bytes", guarded)
    plan = s.prepare(tmp_path)
    assert len(plan["exact_sources"]) == 20 and plan["row_limit"] == 2000
    assert plan["provider_cost_usd"] == 0 and plan["hard_stop_minutes"] == 20
    assert plan["source_admission"] == "EXISTING_ACTIVE_VIEW_CONTENT_CERTIFICATE_AND_ACCESS_POLICY_VALIDATION"


def test_complete_weekday_stream_retains_empty_dates_and_fixed_time_scope():
    day, rows, _ = p.synthetic_day("ES", "2018-01-01", 1, dense=True)
    outside = {**rows[0], "bar_start_ns": rows[0]["bar_start_ns"] - c.MINUTE}
    daily = list(s.daily_inputs([outside, *rows]))
    assert [d for d, _, _ in daily] == list(c.weekday_dates())
    assert daily[0][1] == rows and all(not values for _, values, _ in daily[1:])
    with pytest.raises(IntegrityError, match="ordering"):
        list(s.daily_inputs([*rows, {**rows[0], "project_trade_date": "2017-12-29"}]))


def test_readers_reject_duck_typed_receipts_before_any_source_access(tmp_path):
    fake = SimpleNamespace(assert_consumed=lambda *a, **k: None)
    with pytest.raises(UnauthorizedOperation, match="real receipt"):
        list(s._verified_rows(tmp_path, [{}], fake, fake, {}, lambda: None, {}))
    with pytest.raises(UnauthorizedOperation, match="fresh readiness"):
        s.execute_once(fake, fake)


def test_real_receipt_tuple_scope_reaches_source_preflight(tmp_path, monkeypatch):
    from futures_rebuild.boundary import OperationReceipt, RepoBoundary
    from futures_rebuild.certified_research_gateway import CertifiedResearchGateway
    boundary = RepoBoundary(tmp_path)
    gateway = CertifiedResearchGateway(tmp_path, boundary)
    bundle = {"bundle_id": "a" * 64, "historical_readiness_proposal": {
        "purpose": "TEST_GENERATED_ONLY", "readiness_plan_id": "b" * 64, "catalog_sha256": "c" * 64}}
    monkeypatch.setattr(p, "validate_bundle", lambda root: bundle)
    def preflight(root):
        raise RuntimeError("source preflight reached without any data read")
    monkeypatch.setattr(s, "prepare", preflight)
    for mismatch in (False, True):
        scope = s.scope(bundle)
        if mismatch:
            scope["purpose"] = "UNAPPROVED_SCOPE"
        receipt = OperationReceipt.issue_user_approved(boundary, operation=s.OPERATION,
            classification=s.CLASSIFICATION, scope=scope, approval_command=s.OPERATION,
            approval_plan_id="d" * 64, approval_plan_sha256="e" * 64,
            approval_line=f"APPROVE {s.OPERATION} PLAN {'d' * 64} SHA256 {'e' * 64}")
        assert type(receipt.scope) is tuple
        with pytest.raises(UnauthorizedOperation if mismatch else RuntimeError,
                           match="bound to this successor" if mismatch else "source preflight reached"):
            s.execute_once(gateway, receipt)
