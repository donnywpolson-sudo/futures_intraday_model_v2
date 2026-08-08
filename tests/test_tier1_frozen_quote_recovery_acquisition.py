from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_frozen_quote_recovery_acquisition import (
    MAXIMUM_EXTERNAL_COST_USD,
    STAGING_ROOT,
    build_file_credential_get_range,
    load_acquisition_plan,
    load_cost_record,
    provider_request,
    stage_query_store,
    validate_cost_record_queries,
    validate_fresh_cost_quote,
    verify_store_metadata,
)
from futures_rebuild.tier1_frozen_quote_recovery_cost import (
    build_quote_cost_queries,
    load_diagnostic_record,
)


ROOT = Path(__file__).resolve().parents[1]


def _queries():
    return build_quote_cost_queries(
        diagnostic_record=load_diagnostic_record(root=ROOT),
    )


def _metadata(query):
    from futures_rebuild.tier1_frozen_quote_recovery_acquisition import _iso_to_ns

    return SimpleNamespace(
        dataset="GLBX.MDP3",
        schema="bbo-1s",
        stype_in="continuous",
        stype_out="instrument_id",
        ts_out=False,
        limit=None,
        start=_iso_to_ns(query.start),
        end=_iso_to_ns(query.end),
        symbols=list(query.symbols),
    )


class _FakeStore:
    def __init__(self, query):
        self.metadata = _metadata(query)

    def to_file(self, path, *, mode, compression):
        assert mode == "x"
        assert compression == "zstd"
        Path(path).open("xb").write(b"synthetic-dbn-bytes")


def test_acquisition_cost_ledger_is_exact_and_below_the_hard_cap() -> None:
    queries = _queries()
    costs = validate_cost_record_queries(
        cost_record=load_cost_record(root=ROOT), queries=queries,
    )
    assert len(queries) == len(costs) == 30
    assert sum(costs.values(), Decimal("0")) == Decimal("1.252167820932")
    assert sum(costs.values(), Decimal("0")) <= MAXIMUM_EXTERNAL_COST_USD


def test_fresh_cost_quote_fails_before_download_when_cap_drifts() -> None:
    queries = _queries()
    with pytest.raises(UnauthorizedOperation, match="cost ceiling"):
        validate_fresh_cost_quote(
            queries=queries,
            get_cost=lambda **kwargs: "1.31" if kwargs["start"] == queries[0].start else "0",
        )
    costs, estimates = validate_fresh_cost_quote(
        queries=queries, get_cost=lambda **kwargs: "0.01",
    )
    assert len(estimates) == len(costs) == 30
    assert sum(costs.values(), Decimal("0")) == Decimal("0.30")


def test_fresh_cost_checks_share_the_whole_operation_runtime_limit() -> None:
    queries = _queries()
    ticks = iter([0.0, 601.0])
    with pytest.raises(IntegrityError, match="host runtime"):
        validate_fresh_cost_quote(
            queries=queries, get_cost=lambda **kwargs: "0", clock=lambda: next(ticks),
        )


def test_market_row_request_is_exact_and_explicit_about_output_identity() -> None:
    request = provider_request(_queries()[0])
    assert set(request) == {
        "dataset", "schema", "stype_in", "stype_out", "symbols", "start", "end",
    }
    assert request["dataset"] == "GLBX.MDP3"
    assert request["schema"] == "bbo-1s"
    assert request["stype_in"] == "continuous"
    assert request["stype_out"] == "instrument_id"


def test_acquisition_client_uses_only_file_api_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "db-failed-environment-key")
    (tmp_path / "api.env").write_text(
        "DATABENTO_API_KEY=db-synthetic-file-key\n", encoding="utf-8",
    )
    observed = {}

    class _Historical:
        def __init__(self, *, key):
            observed["key"] = key
            self.metadata = SimpleNamespace(get_cost=lambda **kwargs: "0")
            self.timeseries = SimpleNamespace(get_range=lambda **kwargs: kwargs)

    get_range = build_file_credential_get_range(
        root=tmp_path, historical_factory=_Historical,
    )
    assert observed == {"key": "db-synthetic-file-key"}
    assert get_range(test=True) == {"test": True}


def test_download_metadata_mismatch_fails_closed() -> None:
    query = _queries()[0]
    store = _FakeStore(query)
    store.metadata.end += 1
    with pytest.raises(IntegrityError, match="metadata differs"):
        verify_store_metadata(store=store, query=query)


def test_staging_writes_create_only_dbn_and_hash_bound_sidecar(tmp_path: Path) -> None:
    query = _queries()[0]
    attempt = tmp_path / STAGING_ROOT / ("a" * 64)
    result = stage_query_store(
        root=tmp_path,
        attempt_root=attempt,
        query=query,
        store=_FakeStore(query),
        estimated_cost=Decimal("0.01"),
    )
    raw = tmp_path / result.relative_path
    sidecar = tmp_path / result.sidecar_relative_path
    manifest = json.loads(sidecar.read_text(encoding="utf-8"))
    core = dict(manifest)
    manifest_id = core.pop("manifest_id")
    assert raw.read_bytes() == b"synthetic-dbn-bytes"
    assert result.sha256 == sha256_file(raw)
    assert result.sidecar_sha256 == sha256_file(sidecar)
    assert manifest_id == sha256_json(core)
    assert manifest["query"] == query.as_dict()
    assert manifest["successor_source_activated"] is False
    with pytest.raises(FileExistsError):
        stage_query_store(
            root=tmp_path,
            attempt_root=attempt,
            query=query,
            store=_FakeStore(query),
            estimated_cost=Decimal("0.01"),
        )


def test_acquisition_plan_is_hash_bound_staging_only_and_non_authorizing() -> None:
    plan = load_acquisition_plan(root=ROOT)
    assert plan["plan_id"] == (
        "ebfb3338ef1fb5fdc71ec074ee682d2767675c630c8ae1cb6e434d19cacc8c82"
    )
    assert plan["estimated_external_cost_usd"] == "1.252167820932"
    assert plan["maximum_external_cost_usd"] == "1.30"
    assert plan["fresh_metadata_cost_calls_before_download"] == 30
    assert plan["fresh_quote_must_not_exceed_maximum_before_any_download"] is True
    assert plan["credential_source"] == "file api.env"
    assert plan["raw_market_rows_downloaded"] is True
    assert plan["staging"]["source_activation"] is False
    assert set(plan["forbidden_actions"].values()) == {True}


def test_failed_funding_attempt_is_preserved_as_unpublished_operational_evidence() -> None:
    path = ROOT / "configs/tier1_frozen_bbo_acquisition_failed_attempt_0bb14ebd.json"
    artifact = json.loads(path.read_text(encoding="utf-8"))
    core = dict(artifact)
    artifact_id = core.pop("artifact_id")
    claim = ROOT / artifact["authorization_claim_path"]
    assert artifact_id == sha256_json(core)
    assert sha256_file(claim) == artifact["authorization_claim_sha256"]
    assert artifact["disposition"] == "FAILED_BEFORE_FIRST_MARKET_ROW_DELIVERY"
    assert artifact["provider_failure"] == {
        "error_class": "BentoClientError",
        "provider_code": "account_insufficient_funds",
        "status_code": 402,
    }
    assert artifact["market_row_files_created"] == 0
    assert artifact["historical_rows_decoded"] is False
    assert artifact["publication"] is False
