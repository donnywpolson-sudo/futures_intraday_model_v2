from __future__ import annotations

import json
from pathlib import Path

import pytest

import futures_rebuild.ohlcv_historical_backfill_v3 as v3
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


pytestmark = pytest.mark.current


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def test_authoritative_universe_is_derived_from_both_contracts(tmp_path: Path) -> None:
    full = [f"F{index:02d}" for index in range(41)]
    micro = [f"M{index:02d}" for index in range(17)]
    _write(
        tmp_path / "configs/research_universe_contract.json",
        {"tiers": [{"tier_id": 3, "symbols": full[:38]}, {"tier_id": 4, "symbols": full[38:]}]},
    )
    _write(
        tmp_path / "configs/micro_contract_universe_v1.json",
        {"tiers": {"tier_3": micro}},
    )

    result = v3.authoritative_universe(tmp_path)

    assert result["full_size_roots"] == full
    assert result["micro_roots"] == micro
    assert len(result["roots"]) == 58
    assert result["bindings"]["configs/research_universe_contract.json"] == sha256_file(
        tmp_path / "configs/research_universe_contract.json"
    )


def test_authoritative_universe_rejects_overlap(tmp_path: Path) -> None:
    full = [f"F{index:02d}" for index in range(41)]
    micro = [full[0], *(f"M{index:02d}" for index in range(16))]
    _write(
        tmp_path / "configs/research_universe_contract.json",
        {"tiers": [{"tier_id": 3, "symbols": full[:38]}, {"tier_id": 4, "symbols": full[38:]}]},
    )
    _write(tmp_path / "configs/micro_contract_universe_v1.json", {"tiers": {"tier_3": micro}})

    with pytest.raises(IntegrityError, match="disjoint 41 full-size plus 17 micro"):
        v3.authoritative_universe(tmp_path)


def _bound_plan(root: Path) -> tuple[Path, dict[str, object], str]:
    core: dict[str, object] = {
        "authority": {
            "active_data_mutation": False,
            "credential_access": False,
            "provider_network_access": False,
            "publication": False,
            "status": v3.PLAN_STATUS,
        },
        "end_exclusive": v3.END_EXCLUSIVE,
        "estimates": {"current_free_bytes": 1, "required_free_bytes": 0},
        "schema_version": v3.PLAN_SCHEMA,
    }
    document = {**core, "plan_id": sha256_json(core)}
    path = root / v3.PLAN_ROOT / "synthetic" / "plan.json"
    _write(path, document)
    return path, document, sha256_file(path)


def test_bound_plan_revalidates_live_delta_without_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document, digest = _bound_plan(tmp_path)
    fresh = json.loads(json.dumps(document))
    fresh.pop("plan_id")
    fresh["estimates"]["current_free_bytes"] = 999  # type: ignore[index]
    fresh["plan_id"] = sha256_json(fresh)
    monkeypatch.setattr(v3, "build_completion_plan", lambda *_args, **_kwargs: fresh)

    observed = v3.load_bound_completion_plan(
        tmp_path,
        path,
        expected_plan_id=str(document["plan_id"]),
        expected_sha256=digest,
    )

    assert observed == document
    assert observed["authority"]["provider_network_access"] is False  # type: ignore[index]


def test_bound_plan_rejects_live_delta_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document, digest = _bound_plan(tmp_path)
    fresh = json.loads(json.dumps(document))
    fresh.pop("plan_id")
    fresh["end_exclusive"] = "2026-07-15T00:00:00Z"
    fresh["plan_id"] = sha256_json(fresh)
    monkeypatch.setattr(v3, "build_completion_plan", lambda *_args, **_kwargs: fresh)

    with pytest.raises(IntegrityError, match="live delta"):
        v3.load_bound_completion_plan(
            tmp_path,
            path,
            expected_plan_id=str(document["plan_id"]),
            expected_sha256=digest,
        )


def test_bound_plan_rejects_forged_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, document, _ = _bound_plan(tmp_path)
    document["authority"]["provider_network_access"] = True  # type: ignore[index]
    core = dict(document)
    core.pop("plan_id")
    document["plan_id"] = sha256_json(core)
    _write(path, document)
    digest = sha256_file(path)
    monkeypatch.setattr(v3, "build_completion_plan", lambda *_args, **_kwargs: document)

    with pytest.raises(UnauthorizedOperation, match="authority"):
        v3.load_bound_completion_plan(
            tmp_path,
            path,
            expected_plan_id=str(document["plan_id"]),
            expected_sha256=digest,
        )


def test_quote_is_metadata_only_and_zero_cost_bound() -> None:
    calls: list[dict[str, object]] = []
    request = {
        "dataset": v3.DATASET,
        "end": v3.END_EXCLUSIVE,
        "market": "ZQ",
        "schema": "ohlcv-1d",
        "start": "2010-06-06T00:00:00Z",
        "stype_in": "continuous",
        "symbols": ["ZQ.v.0"],
    }
    plan = {
        "authority": {
            "active_data_mutation": False,
            "credential_access": False,
            "provider_network_access": False,
            "publication": False,
        },
        "execution_limits": {"provider_cost_cap_usd": "0.0", "provider_request_count": 1},
        "plan_id": "a" * 64,
        "requests": [request],
        "schema_version": v3.PLAN_SCHEMA,
    }

    def get_cost(**kwargs: object) -> str:
        calls.append(kwargs)
        return "0"

    quote = v3.quote_completion_plan(
        plan,
        plan_sha256="b" * 64,
        get_cost=get_cost,
        get_billable_size=lambda **_: 123,
        get_record_count=lambda **_: 45,
    )

    assert quote["status"] == "PASS_WITHIN_APPROVED_ZERO_COST_CAP"
    assert quote["provider_call_count"] == 1
    assert quote["authority"] == {
        "download": False,
        "provider_row_read": False,
        "publication": False,
        "submission": False,
    }
    assert calls == [{key: request[key] for key in ("dataset", "schema", "symbols", "start", "end", "stype_in")}]
    assert quote["quotes"][0]["api_billable_uncompressed_bytes"] == 123
    assert quote["quotes"][0]["provider_record_count"] == 45


def test_live_dual_resolution_completion_has_zero_remaining_batch() -> None:
    root = Path(__file__).resolve().parents[1]

    plan = v3.build_completion_plan(root)

    assert plan["coverage"]["ohlcv-1d"]["present_root_count"] == 58
    assert plan["coverage"]["ohlcv-1h"]["present_root_count"] == 58
    assert plan["coverage"]["ohlcv-1d"]["missing_roots"] == []
    assert plan["coverage"]["ohlcv-1h"]["missing_roots"] == []
    assert plan["intervals"] == []
    assert plan["requests"] == []
    assert plan["execution_limits"]["target_dbn_file_count_maximum"] == 0


def test_batch_derivation_rejects_market_outside_live_delta() -> None:
    root = Path(__file__).resolve().parents[1]
    plan = v3.build_completion_plan(root)

    with pytest.raises(v3.ContractError, match="outside the live delta"):
        v3.derive_completion_batch(
            plan,
            selection={"MSF": ["ohlcv-1d"]},
        )


def test_quote_reports_cost_above_zero_cap_without_submission() -> None:
    plan = {
        "authority": {
            "active_data_mutation": False,
            "credential_access": False,
            "provider_network_access": False,
            "publication": False,
        },
        "execution_limits": {"provider_cost_cap_usd": "0", "provider_request_count": 1},
        "plan_id": "a" * 64,
        "requests": [{
            "dataset": v3.DATASET,
            "end": v3.END_EXCLUSIVE,
            "market": "ZQ",
            "schema": "ohlcv-1d",
            "start": "2010-06-06T00:00:00Z",
            "stype_in": "continuous",
            "symbols": ["ZQ.v.0"],
        }],
        "schema_version": v3.PLAN_SCHEMA,
    }

    quote = v3.quote_completion_plan(plan, plan_sha256="b" * 64, get_cost=lambda **_: "0.01")

    assert quote["status"] == "BLOCKED_COST_EXCEEDS_APPROVED_CAP"
    assert quote["estimated_data_cost_usd"] == "0.01"


def test_staged_manifest_reuses_existing_annual_contract() -> None:
    interval = {
        "end_exclusive": v3.END_EXCLUSIVE,
        "estimated_final_bytes_high": 1000,
        "market": "ZQ",
        "schemas": list(v3.SCHEMAS),
        "start_inclusive": "2025-01-01T00:00:00Z",
    }
    requests = [
        {
            "compression": "zstd",
            "dataset": v3.DATASET,
            "encoding": "dbn",
            "end": v3.END_EXCLUSIVE,
            "map_symbols": False,
            "market": "ZQ",
            "schema": schema,
            "split_duration": "year",
            "split_symbols": False,
            "start": "2025-01-01T00:00:00Z",
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "symbols": ["ZQ.v.0"],
        }
        for schema in v3.SCHEMAS
    ]
    rows = v3.build_staged_execution_manifest(
        {"intervals": [interval], "plan_id": "a" * 64, "requests": requests},
        markets=["ZQ"],
        provider_metadata_sha256="b" * 64,
    )

    assert len(rows) == 4
    assert {(row["schema"], row["year"]) for row in rows} == {
        ("ohlcv-1d", 2025),
        ("ohlcv-1d", 2026),
        ("ohlcv-1h", 2025),
        ("ohlcv-1h", 2026),
    }
    assert all(row["execution_action"] == "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY" for row in rows)
    assert all(str(row["final_path"]).startswith("data/dbn/") for row in rows)
