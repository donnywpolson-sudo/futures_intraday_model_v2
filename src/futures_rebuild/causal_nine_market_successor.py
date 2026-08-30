"""Certify and atomically activate the exact nine-market causal view."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path, PurePosixPath

from .active_data_certification import (
    canonical_parquet_fingerprint,
    canonical_parquet_sequence_fingerprint,
)
from .active_data_view import (
    ACTIVE_ROOT, CERTIFICATION_STATE, CatalogEntry, UpdateMode,
    _write_new_or_exact, build_content_validation_receipt, build_plan,
    cohort_for_coverage, cohort_for_year, publish_append_only, stage_view,
    verify_plan_bindings,
    verify_view,
)
from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .causal_incremental_release import (
    ACTIVE_RELEASE_MARKETS, CHECKPOINT_SET_ID, DEVELOPMENT_END_EXCLUSIVE,
    _approved_plan, _coverage, _validated_certificate, build_certificate_package,
    load_contract as load_six_market_contract,
)
from .errors import ContractError, IntegrityError


NEW_MARKETS = ("SR3", "ZB", "6J")
ALL_MARKETS = (*ACTIVE_RELEASE_MARKETS, *NEW_MARKETS)
CONTRACT_PATH = Path("configs/causal_nine_market_successor_contract_v1.json")
ARTIFACT_ROOT = Path("reports/causal_nine_market_active_release")


def _read_canonical(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"canonical input is unreadable: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"input is not canonical JSON: {path}")
    return value


def _entry(raw: Mapping[str, object]) -> CatalogEntry:
    return CatalogEntry(
        market=str(raw["market"]), year=int(raw["year"]),
        coverage_start=str(raw["coverage_start"]), coverage_end=str(raw["coverage_end"]),
        coverage_kind=str(raw["coverage_kind"]), cohort=str(raw["cohort"]),
        disposition=str(raw["disposition"]),
        selection_eligible=bool(raw["selection_eligible"]),
        permitted_uses=tuple(str(item) for item in raw["permitted_uses"]),  # type: ignore[union-attr]
        source_bindings=tuple(dict(item) for item in raw["source_bindings"]),  # type: ignore[union-attr]
        content_validation_receipt_id=str(raw["content_validation_receipt_id"]),
        access_policy_binding_id=str(raw["access_policy_binding_id"]),
        parquet_sha256=str(raw["parquet_sha256"]), sidecar_sha256=str(raw["sidecar_sha256"]),
        row_count=int(raw["row_count"]), schema_fingerprint=str(raw["schema_fingerprint"]),
    )


def build_contract(
    *, repository_root: Path, certificate_paths: Mapping[str, str]
) -> dict[str, object]:
    """Build the metadata-only nine-market transition contract."""

    root = repository_root.resolve(strict=True)
    if tuple(certificate_paths) != NEW_MARKETS:
        raise ContractError("certificate paths must be ordered SR3, ZB, 6J")
    current = verify_view(root / ACTIVE_ROOT)
    entries = current.get("entries")
    if not isinstance(entries, list) or len(entries) != 96 or {
        str(item["market"]) for item in entries if isinstance(item, Mapping)
    } != set(ACTIVE_RELEASE_MARKETS):
        raise IntegrityError("current active view is not the exact six-market predecessor")
    six_contract = load_six_market_contract(root)
    six_package = build_certificate_package(
        repository_root=root,
        certificate_paths={
            market: str(six_contract["certificate_paths"][market])  # type: ignore[index]
            for market in ACTIVE_RELEASE_MARKETS
        },
    )
    shared = {
        name: str(six_package[name])
        for name in ("checkpoint_set_id", "source_contract_id", "source_release_id", "causal_contract_id")
    }
    certificates = []
    for market in NEW_MARKETS:
        relative = certificate_paths[market]
        certificate = _validated_certificate(
            root, relative_path=relative, expected_market=market,
            shared_bindings=shared, allowed_markets=NEW_MARKETS,
        )
        certificates.append({
            "attempt_id": certificate["attempt_id"],
            "certificate_id": certificate["certificate_id"],
            "market": market,
            "path": relative,
            "sha256": sha256_file(root / PurePosixPath(relative)),
        })
    core: dict[str, object] = {
        "authority": {
            "activation": False, "feature_or_outcome_construction": False,
            "final_252_or_forward_access": False, "model_fit_or_prediction": False,
            "provider_or_network": False, "selector_execution": False,
            "trading": False, "wfa_or_alpha_evaluation": False,
        },
        "certificates": certificates,
        "checkpoint_set_id": CHECKPOINT_SET_ID,
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "markets": list(ALL_MARKETS),
        "catalog_market_years": 144,
        "materialized_market_years": 136,
        "protected_pre_effective_market_years": 8,
        "predecessor_catalog": {
            "active_view_id": current["active_view_id"],
            "catalog_sha256": current["catalog_sha256"],
            "file_sha256": sha256_file(root / ACTIVE_ROOT / "catalog.json"),
            "path": "data/active/catalog.json",
        },
        "preserve_predecessor_market_files_exactly": True,
        "schema_version": "causal_nine_market_successor_contract/1.0.0",
        "source_bindings": shared,
        "state": "PREPARED_REQUIRES_EXACT_EXECUTION",
    }
    return {**core, "contract_id": sha256_json(core)}


def load_contract(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    contract = _read_canonical(root / CONTRACT_PATH)
    paths = {
        str(item["market"]): str(item["path"])
        for item in contract.get("certificates", ())
        if isinstance(item, Mapping)
    }
    rebuilt = build_contract(repository_root=root, certificate_paths=paths)
    if contract != rebuilt:
        raise IntegrityError("nine-market successor contract differs")
    return contract


def _year_sources(
    certificate: Mapping[str, object], *, market: str
) -> dict[int, list[dict[str, object]]]:
    inventory = certificate["replay_evidence"]["output_inventory"]  # type: ignore[index]
    result: dict[int, list[dict[str, object]]] = {}
    for raw in inventory:  # type: ignore[union-attr]
        if not isinstance(raw, Mapping) or PurePosixPath(str(raw.get("path"))).name != "observations.parquet":
            continue
        item = dict(raw)
        parts = PurePosixPath(str(item["path"])).parts
        try:
            sealed = parts.index("sealed")
            year = int(parts[sealed + 1])
        except (ValueError, IndexError) as exc:
            raise IntegrityError("certified observation path lacks a sealed year") from exc
        result.setdefault(year, []).append(item)
    expected = set(range(2018 if market == "SR3" else 2010, 2026))
    if set(result) != expected:
        raise IntegrityError("new market certificate lacks the complete development span")
    return {year: sorted(items, key=lambda item: str(item["path"])) for year, items in result.items()}


def _protected_sr3_entries(contract_id: str) -> list[CatalogEntry]:
    return [
        CatalogEntry(
            market="SR3", year=year, coverage_start=f"{year}-01-01",
            coverage_end=f"{year + 1}-01-01", coverage_kind="PRODUCT_PRE_EFFECTIVE_NO_ROWS",
            cohort=cohort_for_year(year), disposition="QUARANTINED_NOT_MATERIALIZED",
            selection_eligible=False, permitted_uses=(),
            source_bindings=({"successor_contract_id": contract_id},),
            reason="PRODUCT_NOT_LISTED_NO_ROWS",
        )
        for year in range(2010, 2018)
    ]


def execute_nine_market_successor(
    *, repository_root: Path, user_authorization_id: str, approved_at: str
) -> dict[str, object]:
    """Materialize three additions, verify all nine, and atomically activate."""

    root = repository_root.resolve(strict=True)
    if len(user_authorization_id) != 64:
        raise ContractError("user authorization ID is invalid")
    datetime.strptime(approved_at, "%Y-%m-%dT%H:%M:%SZ")
    contract = load_contract(root)
    current = verify_view(root / ACTIVE_ROOT)
    predecessor = contract["predecessor_catalog"]  # type: ignore[index]
    if (
        current["active_view_id"] != predecessor["active_view_id"]
        or current["catalog_sha256"] != predecessor["catalog_sha256"]
        or sha256_file(root / ACTIVE_ROOT / "catalog.json") != predecessor["file_sha256"]
    ):
        raise IntegrityError("active six-market predecessor drifted")
    receipt_semantic_bindings = {
        path: sha256_file(root / path)
        for path in (
            CONTRACT_PATH.as_posix(),
            "configs/active_data_view_contract.json",
            "configs/alpha_ladder_macro_successor_contract_v1.json",
            "configs/source_contract.json",
        )
    }
    implementation_bindings = {
        path: sha256_file(root / path)
        for path in (
            "src/futures_rebuild/active_data_certification.py",
            "src/futures_rebuild/active_data_view.py",
            "src/futures_rebuild/causal_nine_market_successor.py",
        )
    }
    environment_bindings = {
        path: sha256_file(root / path)
        for path in ("configs/environment.lock.json", "requirements.lock")
    }
    catalog_semantic_bindings = dict(current["semantic_bindings"])
    materializations = []
    source_bytes = 0
    for binding in contract["certificates"]:  # type: ignore[index]
        market = str(binding["market"])
        certificate = _read_canonical(root / PurePosixPath(str(binding["path"])))
        for year, items in _year_sources(certificate, market=market).items():
            coverage_start, coverage_end = _coverage(items)
            if year == 2025 and coverage_end != DEVELOPMENT_END_EXCLUSIVE.replace(":", ""):
                raise IntegrityError("new 2025 source crosses the development cutoff")
            sources = [io_path(root / PurePosixPath(str(item["path"]))) for item in items]
            fingerprint = canonical_parquet_sequence_fingerprint(sources)
            source_bindings = [
                {"path": str(item["path"]), "sha256": str(item["sha256"]), "size": int(item["size"])}
                for item in items
            ]
            source_bytes += sum(int(item["size"]) for item in items)
            receipt = build_content_validation_receipt(
                market=market, year=year, coverage_start=coverage_start,
                coverage_end=coverage_end, source_bindings=source_bindings,
                semantic_bindings=receipt_semantic_bindings,
                implementation_bindings=implementation_bindings,
                environment_bindings=environment_bindings,
                canonical_schema_fingerprint=str(fingerprint["schema_fingerprint"]),
                canonical_row_hash=str(fingerprint["canonical_row_hash"]),
                row_count=int(fingerprint["row_count"]),
                checks={"certificate_revalidated": "PASS", "source_sequence_fully_read": "PASS"},
                limitations=("NO_HOLDOUT_OR_FORWARD_ACCESS", "NO_MODEL_OR_SELECTOR_AUTHORITY"),
            )
            materializations.append({
                "content_validation_receipt": receipt,
                "content_validation_receipt_id": receipt["content_validation_receipt_id"],
                "coverage_end": coverage_end,
                "coverage_kind": "CERTIFIED_V10_PARTIAL_2025_DEVELOPMENT_SOURCE" if year == 2025 else "CERTIFIED_V10_DEVELOPMENT_SOURCE",
                "coverage_start": coverage_start,
                "market": market,
                "row_count": int(fingerprint["row_count"]),
                "schema_fingerprint": str(fingerprint["schema_fingerprint"]),
                "source_bindings": source_bindings,
                "source_paths": [str(item["path"]) for item in items],
                "source_sha256s": [str(item["sha256"]) for item in items],
                "universe_contract_sha256": receipt_semantic_bindings["configs/alpha_ladder_macro_successor_contract_v1.json"],
                "year": year,
            })
    materializations.sort(key=lambda item: (str(item["market"]), int(item["year"])))
    if len(materializations) != 40:
        raise IntegrityError("nine-market successor requires exactly 40 materialized additions")
    active_files = [path for path in (root / ACTIVE_ROOT).rglob("*") if path.is_file()]
    active_bytes = sum(path.stat().st_size for path in active_files)
    if shutil.disk_usage(root).free < active_bytes + source_bytes + 1024**3:
        raise IntegrityError("nine-market staging lacks bounded free space")
    preserved = [_entry(item) for item in current["entries"]]  # type: ignore[index]
    protected = _protected_sr3_entries(str(contract["contract_id"]))
    preserved.extend(protected)
    entries = [dict(item) for item in current["entries"]]  # type: ignore[index]
    entries.extend(item.as_dict() for item in protected)
    entries.extend({
        "cohort": cohort_for_coverage(year=int(item["year"]), coverage_start=str(item["coverage_start"]), coverage_end=str(item["coverage_end"])),
        "coverage_end": item["coverage_end"], "coverage_kind": item["coverage_kind"],
        "coverage_start": item["coverage_start"], "disposition": CERTIFICATION_STATE,
        "market": item["market"], "selection_eligible": 2018 <= int(item["year"]) <= 2022,
        "year": item["year"],
    } for item in materializations)
    entries.sort(key=lambda item: (str(item["market"]), int(item["year"])))
    plan = build_plan(
        operation="MATERIALIZE_CAUSAL_ACTIVE_VIEW", mode=UpdateMode.FULL_SUCCESSOR,
        foundation_release_id=str(contract["source_bindings"]["source_release_id"]),  # type: ignore[index]
        foundation_manifest_sha256=receipt_semantic_bindings["configs/source_contract.json"],
        semantic_bindings=catalog_semantic_bindings, entries=entries,
        limits={
            "maximum_materializations": 48,
            "maximum_output_files": len(active_files) + 2 * len(materializations),
            "maximum_rows": sum(int(item["row_count"]) for item in materializations),
            "maximum_source_bytes": source_bytes,
            "maximum_source_files": sum(len(item["source_paths"]) for item in materializations),
            "maximum_staging_bytes": active_bytes + source_bytes,
        },
        forbidden_actions=("HOLDOUT_OR_FORWARD_ACCESS", "PROVIDER_ACCESS", "SOURCE_V10_MUTATION", "TRADING"),
        outputs=("state/active_data_view_staging",),
        implementation_bindings=implementation_bindings,
        environment_bindings=environment_bindings,
        recovery_boundary="CREATE_ONLY_NINE_MARKET_STAGE_CURRENT_ACTIVE_UNCHANGED",
    )
    release_id = sha256_json({"contract_id": contract["contract_id"], "plan_id": plan["plan_id"]})
    artifact_root = root / ARTIFACT_ROOT / release_id
    approval = _approved_plan(plan, user_authorization_id=user_authorization_id, approved_at=approved_at)
    _write_new_or_exact(artifact_root / "materialization_plan.json", plan)
    _write_new_or_exact(artifact_root / "materialization_approval.json", approval)
    verify_plan_bindings(root, plan)
    stage, catalog = stage_view(
        repository_root=root, plan=plan,
        foundation_release_id=str(contract["source_bindings"]["source_release_id"]),  # type: ignore[index]
        foundation_manifest_sha256=receipt_semantic_bindings["configs/source_contract.json"],
        semantic_bindings=catalog_semantic_bindings, materializations=materializations,
        non_materialized_entries=preserved, seed_view=root / ACTIVE_ROOT,
    )
    verified = verify_view(stage)
    if len(verified["entries"]) != 144:
        raise IntegrityError("staged successor does not contain 144 market-years")
    for item in materializations:
        market, year = str(item["market"]), int(item["year"])
        fingerprint = canonical_parquet_fingerprint(
            stage / "causally_gated_normalized" / market / str(year) / f"{year}.parquet"
        )
        receipt = item["content_validation_receipt"]
        if (
            fingerprint["canonical_row_hash"] != receipt["canonical_row_hash"]
            or fingerprint["row_count"] != receipt["row_count"]
        ):
            raise IntegrityError("independent new-market readback differs")
    files = [path for path in stage.rglob("*") if path.is_file()]
    publication = build_plan(
        operation="PUBLISH_CAUSAL_ACTIVE_VIEW", mode=UpdateMode.APPEND_ONLY,
        foundation_release_id=str(contract["source_bindings"]["source_release_id"]),  # type: ignore[index]
        foundation_manifest_sha256=receipt_semantic_bindings["configs/source_contract.json"],
        semantic_bindings=catalog_semantic_bindings, entries=entries,
        limits={
            "maximum_active_files": len(files), "maximum_catalog_entries": 144,
            "maximum_input_bytes": sum(path.stat().st_size for path in files),
            "maximum_input_files": len(files), "maximum_publications": 1,
            "maximum_retries": 0, "maximum_workers": 1,
        },
        forbidden_actions=("HOLDOUT_OR_FORWARD_ACCESS", "PROVIDER_ACCESS", "SOURCE_V10_MUTATION", "TRADING"),
        outputs=("data/active", "state/active_data_view_publication"),
        implementation_bindings=implementation_bindings,
        environment_bindings=environment_bindings,
        recovery_boundary="FILES_FIRST_CATALOG_LAST_PREDECESSOR_CATALOG_SNAPSHOTTED",
    )
    publication_approval = _approved_plan(
        publication, user_authorization_id=user_authorization_id, approved_at=approved_at
    )
    _write_new_or_exact(artifact_root / "predecessor_catalog.json", current)
    _write_new_or_exact(artifact_root / "publication_plan.json", publication)
    _write_new_or_exact(artifact_root / "publication_approval.json", publication_approval)
    publication_stage = root / "state/active_data_view_staging" / str(publication["plan_id"])
    if publication_stage.exists():
        raise IntegrityError("append-only publication stage already exists")
    os.replace(stage, publication_stage)
    receipt = publish_append_only(
        repository_root=root, staging=publication_stage, plan=publication, approval=publication_approval
    )
    active = verify_view(root / ACTIVE_ROOT)
    final_core = {
        "active_view_id": active["active_view_id"], "catalog_sha256": active["catalog_sha256"],
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "markets": list(ALL_MARKETS), "catalog_market_years": 144,
        "materialized_market_years": 136, "protected_pre_effective_market_years": 8,
        "provider_calls": 0, "holdout_or_forward_rows": 0,
        "publication_receipt_id": receipt["publication_receipt_id"],
        "schema_version": "causal_nine_market_active_release_terminal/1.0.0",
        "source_v10_mutated": False,
        "status": "ACTIVE_NINE_MARKET_RESEARCH_READY_CAUSAL_PRICE",
    }
    final = {**final_core, "terminal_id": sha256_json(final_core)}
    _write_new_or_exact(artifact_root / "terminal.json", final)
    return final
