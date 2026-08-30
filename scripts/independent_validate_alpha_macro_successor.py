"""Fresh-process readback certificate for the sealed macro successor."""

from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]
CATALOG = Path("data/active/catalog.json")
MARKETS = ("ES", "NQ", "CL", "GC", "ZN", "6E", "SR3", "ZB", "6J")
NEW = ("SR3", "ZB", "6J")


def read(relative: Path) -> dict[str, object]:
    path = ROOT / relative
    raw = path.read_bytes()
    value = json.loads(raw)
    assert isinstance(value, dict) and raw == canonical_bytes(value) + b"\n", relative
    return value


def bound(relative: str, expected: str, size: int | None = None) -> None:
    pure = PurePosixPath(relative)
    assert not pure.is_absolute() and ".." not in pure.parts and pure.as_posix() == relative
    path = ROOT.joinpath(*pure.parts).resolve(strict=True)
    path.relative_to(ROOT)
    if size is not None:
        assert path.stat().st_size == size, relative
    assert sha256_file(path, reject_hardlinks=False) == expected, relative


def one(pattern: str) -> Path:
    values = list(ROOT.glob(pattern))
    assert len(values) == 1, (pattern, values)
    return values[0].relative_to(ROOT)


def self_hash(value: dict[str, object], field: str) -> None:
    assert value[field] == sha256_json({key: item for key, item in value.items() if key != field})


def main() -> None:
    closure = read(Path("state/alpha_ladder_macro_selection_closure/zn_6e_terminal_v1.json"))
    self_hash(closure, "closure_id")
    assert closure["outcome"] == "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    for item in closure["artifact_bindings"]:  # type: ignore[index]
        bound(str(item["path"]), str(item["sha256"]))

    freeze = read(Path("configs/alpha_ladder_macro_successor_contract_v1.json"))
    self_hash(freeze, "contract_id")
    assert freeze["candidate_order"] == ["SR3", "ZN", "ZB", "6E", "6J"]
    assert freeze["new_candidates"] == list(NEW)
    assert set(freeze["cost_model"]["adverse_ticks_round_trip"].values()) == {4}  # type: ignore[index]
    assert freeze["cost_model"]["fee_round_trip_usd"] == "10.00"  # type: ignore[index]
    assert freeze["measurement_schedule"]["original_schedule_sha256"] == "d30c7da4f38c2098fa1352335c5612fe1d4d583bef51036056c395314954fd63"  # type: ignore[index]

    catalog = read(CATALOG)
    self_hash(catalog, "catalog_sha256")
    entries = catalog["entries"]
    assert isinstance(entries, list) and len(entries) == 144
    assert {item["market"] for item in entries} == set(MARKETS)
    assert {(item["market"], item["year"]) for item in entries} == {
        (market, year) for market in MARKETS for year in range(2010, 2026)
    }
    for item in entries:
        year = int(item["year"])
        pre_effective = item["market"] == "SR3" and year < 2018
        assert item["selection_eligible"] is (2018 <= year <= 2022 and not pre_effective)
        assert ("SELECTION" in item["permitted_uses"]) is (2018 <= year <= 2022 and not pre_effective)
        assert year <= 2025
        if pre_effective:
            assert item["disposition"] == "QUARANTINED_NOT_MATERIALIZED"
            assert item["reason"] == "PRODUCT_NOT_LISTED_NO_ROWS"
            assert item["parquet_path"] is None and item["sidecar_path"] is None
        else:
            bound(str(item["parquet_path"]), str(item["parquet_sha256"]))
            bound(str(item["sidecar_path"]), str(item["sidecar_sha256"]))

    catalog_terminal_path = one("reports/causal_nine_market_active_release/*/terminal.json")
    catalog_terminal = read(catalog_terminal_path)
    self_hash(catalog_terminal, "terminal_id")
    assert catalog_terminal["status"] == "ACTIVE_NINE_MARKET_RESEARCH_READY_CAUSAL_PRICE"
    assert catalog_terminal["active_view_id"] == catalog["active_view_id"]
    predecessor = read(catalog_terminal_path.parent / "predecessor_catalog.json")
    assert len(predecessor["entries"]) == 96  # type: ignore[arg-type]
    previous_by_key = {(item["market"], item["year"]): item for item in predecessor["entries"]}  # type: ignore[index]
    current_by_key = {(item["market"], item["year"]): item for item in entries}
    for key, item in previous_by_key.items():
        assert current_by_key[key] == item
        bound(str(item["parquet_path"]), str(item["parquet_sha256"]))
        bound(str(item["sidecar_path"]), str(item["sidecar_sha256"]))

    identities = {}
    for name, relative in (
        ("ladder", Path("configs/active_alpha_research_ladder_macro_successor.json")),
        ("execution", Path("configs/active_alpha_ladder_execution_design_macro_successor.json")),
        ("rank1", Path("configs/active_alpha_historical_research_design.json")),
    ):
        identities[name] = read(relative)
        self_hash(identities[name], "pointer_id")
        assert {"MARKET_ROW_READ", "MACRO_SELECTION"} <= set(
            identities[name].get("does_not_authorize", ())
        )
    assert identities["rank1"]["successor_ladder"]["pointer_id"] == identities["ladder"]["pointer_id"]  # type: ignore[index]
    assert identities["rank1"]["successor_execution_design"]["pointer_id"] == identities["execution"]["pointer_id"]  # type: ignore[index]

    plan = read(Path("configs/alpha_ladder_macro_successor_execution_proxy_plan_v1.json"))
    self_hash(plan, "plan_id")
    assert plan["markets"] == list(NEW) and plan["attempts"] == 1 and plan["retries"] == 0
    files = plan["bindings"]["files"]  # type: ignore[index]
    assert len(files) == plan["limits"]["files"]  # type: ignore[index]
    assert sum(plan["limits"]["by_market"][market]["files"] for market in NEW) == len(files)  # type: ignore[index]
    for item in files:
        bound(str(item["path"]), str(item["sha256"]), int(item["size"]))

    terminal_path = one("state/unpublished_evidence/alpha_ladder_macro_successor_execution_proxy/*/terminal.json")
    terminal = read(terminal_path)
    self_hash(terminal, "terminal_id")
    evidence = read(terminal_path.parent / "evidence.json")
    self_hash(evidence, "evidence_id")
    assert evidence["raw_rows_included"] is False
    assert set(evidence["aggregate_evidence"]) == set(NEW)  # type: ignore[arg-type]
    assert set(evidence["selector_result"]["candidate_results"]) == {"SR3", "ZN", "ZB", "6E", "6J"}  # type: ignore[index]
    selected = terminal["selected_market"]
    if selected is None:
        assert terminal["outcome"] in {"NO_ELIGIBLE_MACRO_DIVERSIFIER", "UNKNOWN_FAIL_CLOSED"}
        assert not (ROOT / "configs/active_alpha_ladder_macro_market.json").exists()
    else:
        pointer = read(Path("configs/active_alpha_ladder_macro_market.json"))
        self_hash(pointer, "pointer_id")
        assert pointer["market"] == selected and pointer["state"] == "ACTIVE_SEALED_MACRO_SELECTION"

    checks = {
        "active_catalog_144_market_years": "PASS", "all_active_files_read_back": "PASS",
        "predecessor_six_unchanged": "PASS", "rank1_bound_non_authorizing": "PASS",
        "selector_inventory_read_back": "PASS", "terminal_single_use_outcome": "PASS",
        "final_252_or_forward_access": "ABSENT",
    }
    core = {
        "schema_version": "alpha_ladder_macro_successor_independent_validation/1.0.0",
        "status": "PASS_INDEPENDENT_FRESH_PROCESS_TERMINAL_VALIDATION",
        "catalog_terminal_id": catalog_terminal["terminal_id"], "selector_terminal_id": terminal["terminal_id"],
        "checks": checks, "provider_calls": 0, "holdout_or_forward_rows": 0,
        "mechanism_or_trial_created": False,
    }
    certificate = {**core, "certificate_id": sha256_json(core)}
    output = ROOT / "reports/alpha_ladder_macro_successor_independent_validation" / str(certificate["certificate_id"]) / "certificate.json"
    output.parent.mkdir(parents=True, exist_ok=False)
    descriptor = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, canonical_bytes(certificate) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    print(json.dumps(certificate, sort_keys=True))


if __name__ == "__main__":
    main()
