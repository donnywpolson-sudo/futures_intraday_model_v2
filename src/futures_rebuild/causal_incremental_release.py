"""Prepare the exact six-market active Alpha-ladder causal release.

The preparation surface is metadata-only. It revalidates immutable V10
certificates and their file inventories, but it does not open market rows,
materialize annual files, mutate ``data/active``, or grant publication.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .causal_observation_foundation import V10_CHECKPOINT_MARKETS
from .causal_observation_market_certification import CERTIFICATE_SCHEMA
from .errors import ContractError, IntegrityError, UnauthorizedOperation


CONTRACT_PATH = Path("configs/causal_incremental_release_contract_v1.json")
CONTRACT_SCHEMA = "causal_alpha_ladder_active_release_contract/1.0.0"
PACKAGE_SCHEMA = "causal_alpha_ladder_certificate_package/1.0.0"
MARKET_PLAN_SCHEMA = "causal_alpha_ladder_market_publication_plan/1.0.0"
SNAPSHOT_SCHEMA = "causal_alpha_ladder_snapshot/1.0.0"
ACTIVE_RELEASE_MARKETS = ("ES", "NQ", "CL", "GC", "ZN", "6E")
TIER1_MARKETS = ACTIVE_RELEASE_MARKETS
COMPLETION_PRIORITY_ORDER = ("ES", "GC", "6E", "CL", "ZN", "NQ")
SUCCESSOR_MARKET_ORDER = (
    *COMPLETION_PRIORITY_ORDER,
    *(market for market in V10_CHECKPOINT_MARKETS if market not in COMPLETION_PRIORITY_ORDER),
)
DEVELOPMENT_END_EXCLUSIVE = "2025-07-13T22:00:00Z"
CHECKPOINT_SET_ID = "c10ee5951a386916d1aacdc4105ac7513db32d79349a8a0a91f48e90f42fb476"
PASS_MARKET_STATUS = "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
CERTIFICATE_ROOT = Path("state/causal_observation_market_certificates")


def _load_object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not a JSON object")
    return value


def _plain_relative(value: object, description: str) -> PurePosixPath:
    if type(value) is not str or not value:
        raise ContractError(f"{description} is absent")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ContractError(f"{description} is not a plain repository-relative path")
    return path


def _contained(root: Path, value: object, description: str) -> Path:
    relative = _plain_relative(value, description)
    path = root.joinpath(*relative.parts).resolve(strict=False)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation(f"{description} escapes the repository") from exc
    return path


def load_contract(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    path = root / CONTRACT_PATH
    contract = _load_object(path, "Tier 1 release contract")
    if (
        contract.get("schema_version") != CONTRACT_SCHEMA
        or contract.get("campaign_kind") != "SIX_MARKET_ACTIVE_ALPHA_LADDER_RELEASE"
        or contract.get("completion_priority_order") != list(COMPLETION_PRIORITY_ORDER)
        or contract.get("active_release_markets") != list(ACTIVE_RELEASE_MARKETS)
        or contract.get("alpha_tier1_semantics") != {
            "carried_forward": ["ES"],
            "fixed_additions": ["NQ", "CL", "GC"],
            "macro_candidates": ["ZN", "6E"],
            "macro_selection_status": "PENDING_PRE_RESULT_EXECUTION_GATE",
            "selected_macro_count": 1,
        }
        or contract.get("materialized_year_range") != [2010, 2025]
        or contract.get("protected_years") != []
        or contract.get("checkpoint_set_id") != CHECKPOINT_SET_ID
        or contract.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or contract.get("source_v10_evidence_must_remain_immutable") is not True
        or contract.get("publication_authorized") is not False
        or contract.get("activation_authorized") is not False
    ):
        raise IntegrityError("Tier 1 release contract is not exact")
    certificate_paths = contract.get("certificate_paths")
    if (
        not isinstance(certificate_paths, Mapping)
        or tuple(certificate_paths) != tuple(sorted(ACTIVE_RELEASE_MARKETS))
        or any(
            not str(certificate_paths[market]).startswith(
                f"{CERTIFICATE_ROOT.as_posix()}/{CHECKPOINT_SET_ID}/{market}/"
            )
            or not str(certificate_paths[market]).endswith(".json")
            for market in ACTIVE_RELEASE_MARKETS
        )
    ):
        raise IntegrityError("active-release certificate paths are not exact")
    controls = contract.get("bias_controls")
    if not isinstance(controls, Mapping) or controls != {
        "catalog_snapshot_per_research_run": True,
        "market_selection_may_not_use_performance": True,
        "mixed_release_fallback_allowed": False,
        "priority_order_frozen_before_remaining_market_results": True,
        "results_may_claim_full_41_market_coverage": False,
    }:
        raise IntegrityError("Tier 1 bias controls differ")
    return {**contract, "contract_sha256": sha256_file(path)}


def _certificate_core(certificate: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in certificate.items() if key != "certificate_id"}


def _validated_certificate(
    root: Path,
    *,
    relative_path: str,
    expected_market: str,
    shared_bindings: Mapping[str, str] | None = None,
    allowed_markets: Sequence[str] = ACTIVE_RELEASE_MARKETS,
) -> dict[str, object]:
    path = _contained(root, relative_path, "certificate path")
    certificate = _load_object(path, "market maximum-robustness certificate")
    evidence = certificate.get("replay_evidence")
    inventory = evidence.get("output_inventory") if isinstance(evidence, Mapping) else None
    attempt_id = certificate.get("attempt_id")
    checkpoint_set_id = certificate.get("checkpoint_set_id")
    expected_path = (
        CERTIFICATE_ROOT
        / str(checkpoint_set_id)
        / expected_market
        / f"{attempt_id}.json"
    ).as_posix()
    if (
        expected_market not in allowed_markets
        or expected_market not in V10_CHECKPOINT_MARKETS
        or relative_path != expected_path
        or certificate.get("schema_version") != CERTIFICATE_SCHEMA
        or certificate.get("status") != PASS_MARKET_STATUS
        or certificate.get("market") != expected_market
        or certificate.get("certificate_id") != sha256_json(_certificate_core(certificate))
        or certificate.get("replay_passes") != 2
        or certificate.get("provider_calls") != 0
        or certificate.get("holdout_rows") != 0
        or certificate.get("forward_rows") != 0
        or any(
            certificate.get(name) != 0
            for name in ("outcomes", "features", "fitting", "predictions", "evaluations")
        )
        or certificate.get("publication_authorized") is not False
        or certificate.get("activation_authorized") is not False
        or not isinstance(evidence, Mapping)
        or not isinstance(inventory, list)
        or not inventory
        or evidence.get("output_inventory_sha256") != sha256_json(inventory)
    ):
        raise IntegrityError("market maximum-robustness certificate is incompatible")
    if shared_bindings and any(
        certificate.get(name) != value for name, value in shared_bindings.items()
    ):
        raise IntegrityError("Tier 1 certificates do not share one source contract")
    seen: set[str] = set()
    for item in inventory:
        if not isinstance(item, Mapping):
            raise IntegrityError("certified output inventory entry is invalid")
        rendered = _plain_relative(item.get("path"), "certified output").as_posix()
        output = _contained(root, rendered, "certified output")
        if (
            rendered in seen
            or not io_path(output).is_file()
            or io_path(output).stat().st_size != item.get("size")
            or sha256_file(io_path(output)) != item.get("sha256")
        ):
            raise IntegrityError("certified output changed after market certification")
        seen.add(rendered)
    return certificate


def build_certificate_package(
    *, repository_root: Path, certificate_paths: Mapping[str, str]
) -> dict[str, object]:
    """Revalidate all and only the six active Alpha-ladder certificates."""

    root = repository_root.resolve(strict=True)
    contract = load_contract(root)
    if tuple(certificate_paths) != ACTIVE_RELEASE_MARKETS:
        raise ContractError(
            "active release requires ordered ES, NQ, CL, GC, ZN, and 6E certificates"
        )
    certificates: list[dict[str, object]] = []
    shared: dict[str, str] = {}
    all_paths: list[str] = []
    for market in ACTIVE_RELEASE_MARKETS:
        relative = _plain_relative(certificate_paths[market], "certificate path").as_posix()
        certificate = _validated_certificate(
            root,
            relative_path=relative,
            expected_market=market,
            shared_bindings=shared or None,
        )
        if not shared:
            shared = {
                name: str(certificate[name])
                for name in (
                    "checkpoint_set_id",
                    "source_contract_id",
                    "source_release_id",
                    "causal_contract_id",
                )
            }
        inventory = certificate["replay_evidence"]["output_inventory"]  # type: ignore[index]
        paths = sorted(str(item["path"]) for item in inventory)  # type: ignore[index]
        if any(f"/{market}/" not in path for path in paths):
            raise IntegrityError("certificate inventory crosses a market boundary")
        all_paths.extend(paths)
        certificates.append(
            {
                "market": market,
                "certificate_path": relative,
                "certificate_sha256": sha256_file(root / PurePosixPath(relative)),
                "certificate_id": certificate["certificate_id"],
                "attempt_id": certificate["attempt_id"],
                "checkpoint_result_id": certificate["checkpoint_result_id"],
                "checkpoint_sha256": certificate["checkpoint_sha256"],
                "output_file_count": len(paths),
                "output_paths_sha256": sha256_json(paths),
            }
        )
    if len(all_paths) != len(set(all_paths)):
        raise IntegrityError("Tier 1 certified outputs overlap")
    core: dict[str, object] = {
        "schema_version": PACKAGE_SCHEMA,
        "status": "PASS_SIX_MARKET_ALPHA_LADDER_CERTIFICATES_REVALIDATED_INACTIVE",
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": contract["contract_sha256"],
        "markets": list(ACTIVE_RELEASE_MARKETS),
        "completion_priority_order": list(COMPLETION_PRIORITY_ORDER),
        "successor_market_order": list(SUCCESSOR_MARKET_ORDER),
        "certificates": certificates,
        **shared,
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "output_file_count": len(all_paths),
        "output_paths_sha256": sha256_json(sorted(all_paths)),
        "rows_opened": 0,
        "provider_calls": 0,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "package_id": sha256_json(core)}


def validate_certificate_package(
    *, repository_root: Path, package: Mapping[str, object]
) -> None:
    certificate_paths = {
        str(item["market"]): str(item["certificate_path"])
        for item in package.get("certificates", ())
        if isinstance(item, Mapping)
    }
    rebuilt = build_certificate_package(
        repository_root=repository_root, certificate_paths=certificate_paths
    )
    if dict(package) != rebuilt:
        raise IntegrityError("Tier 1 certificate package differs")


def build_market_publication_plan(
    *, repository_root: Path, package: Mapping[str, object], market: str
) -> dict[str, object]:
    """Prepare one annual materialization plan without opening market rows."""

    root = repository_root.resolve(strict=True)
    validate_certificate_package(repository_root=root, package=package)
    if market not in ACTIVE_RELEASE_MARKETS:
        raise UnauthorizedOperation("market is outside the active Alpha-ladder release")
    binding = next(
        item for item in package["certificates"] if item["market"] == market  # type: ignore[index]
    )
    certificate = _load_object(root / PurePosixPath(str(binding["certificate_path"])), "certificate")
    inventory = certificate["replay_evidence"]["output_inventory"]  # type: ignore[index]
    observations = [
        dict(item)
        for item in inventory  # type: ignore[union-attr]
        if isinstance(item, Mapping)
        and PurePosixPath(str(item.get("path"))).name == "observations.parquet"
    ]
    by_year: dict[int, list[dict[str, object]]] = {}
    for item in observations:
        parts = PurePosixPath(str(item["path"])).parts
        try:
            sealed_index = parts.index("sealed")
            year = int(parts[sealed_index + 1])
        except (ValueError, IndexError) as exc:
            raise IntegrityError("certified observation path lacks a sealed year") from exc
        by_year.setdefault(year, []).append(item)
    if set(by_year) != set(range(2010, 2026)):
        raise IntegrityError("certified market does not cover all development years")
    years = []
    for year in sorted(by_year):
        sources = sorted(by_year[year], key=lambda item: str(item["path"]))
        years.append(
            {
                "market": market,
                "year": year,
                "source_observation_partitions": sources,
                "source_partitions_sha256": sha256_json(sources),
                "target_parquet_path": (
                    f"data/active/causally_gated_normalized/{market}/{year}/{year}.parquet"
                ),
                "target_manifest_path": (
                    f"data/active/causally_gated_normalized/{market}/{year}/"
                    f"{year}.parquet.manifest.json"
                ),
            }
        )
    core = {
        "schema_version": MARKET_PLAN_SCHEMA,
        "status": "PREPARED_TIER1_MARKET_ANNUAL_MATERIALIZATION",
        "package_id": package["package_id"],
        "market": market,
        "certificate_id": binding["certificate_id"],
        "certificate_sha256": binding["certificate_sha256"],
        "years": years,
        "year_count": len(years),
        "source_observation_file_count": len(observations),
        "source_observation_files_sha256": sha256_json(
            sorted(observations, key=lambda item: str(item["path"]))
        ),
        "preparation_market_rows_opened": 0,
        "provider_calls": 0,
        "publication_authorized": False,
        "activation_authorized": False,
        "source_v10_evidence_mutation_authorized": False,
    }
    return {**core, "market_plan_id": sha256_json(core)}


def build_snapshot(
    *, package: Mapping[str, object], market_plans: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    by_market = {str(plan.get("market")): plan for plan in market_plans}
    if set(by_market) != set(ACTIVE_RELEASE_MARKETS) or len(by_market) != len(market_plans):
        raise UnauthorizedOperation("active Alpha-ladder snapshot requires exactly six markets")
    if any(
        plan.get("schema_version") != MARKET_PLAN_SCHEMA
        or plan.get("package_id") != package.get("package_id")
        or plan.get("year_count") != 16
        or plan.get("publication_authorized") is not False
        or plan.get("activation_authorized") is not False
        for plan in by_market.values()
    ):
        raise IntegrityError("Tier 1 market publication plan is incompatible")
    ordered_ids = [
        str(by_market[market]["market_plan_id"])
        for market in ACTIVE_RELEASE_MARKETS
    ]
    core = {
        "schema_version": SNAPSHOT_SCHEMA,
        "status": "PREPARED_EXACT_SIX_MARKET_ALPHA_LADDER_PUBLICATION_REQUIRED",
        "package_id": package["package_id"],
        "markets": list(ACTIVE_RELEASE_MARKETS),
        "completion_priority_order": list(COMPLETION_PRIORITY_ORDER),
        "market_plan_ids": ordered_ids,
        "market_plan_ids_sha256": sha256_json(ordered_ids),
        "year_range": [2010, 2025],
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "mixed_release_fallback_allowed": False,
        "results_may_claim_full_41_market_coverage": False,
        "historical_row_read_authorized": False,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "snapshot_id": sha256_json(core)}


def canonical_artifact(value: Mapping[str, object]) -> bytes:
    return canonical_bytes(dict(value)) + b"\n"


def next_priority_market(completed_markets: Sequence[str]) -> str | None:
    """Return the next frozen priority market, rejecting skips or reordering."""

    completed = tuple(completed_markets)
    if completed != COMPLETION_PRIORITY_ORDER[: len(completed)]:
        raise ContractError("completed markets are not an exact priority prefix")
    if len(completed) > len(COMPLETION_PRIORITY_ORDER):
        raise ContractError("completed priority market count is invalid")
    if len(completed) == len(COMPLETION_PRIORITY_ORDER):
        return None
    return COMPLETION_PRIORITY_ORDER[len(completed)]


def _approved_plan(
    plan: Mapping[str, object], *, user_authorization_id: str, approved_at: str
) -> dict[str, object]:
    core = {
        "approved_at": approved_at,
        "operation": plan["operation"],
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_json(dict(plan)),
        "schema_version": "causal_active_view_approval/1.0.0",
        "status": "APPROVED",
        "user_authorization_id": user_authorization_id,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _coverage(source_items: Sequence[Mapping[str, object]]) -> tuple[str, str]:
    intervals: list[tuple[str, str]] = []
    for item in source_items:
        parts = PurePosixPath(str(item["path"])).parts
        try:
            sealed = parts.index("sealed")
            start, end = parts[sealed + 2].split("_", 1)
        except (ValueError, IndexError) as exc:
            raise IntegrityError("certified observation partition interval is invalid") from exc
        intervals.append((start, end))
    intervals.sort()
    return intervals[0][0], intervals[-1][1]


def execute_active_release(
    *, repository_root: Path, user_authorization_id: str, approved_at: str
) -> dict[str, object]:
    """Materialize, independently verify, and atomically activate six markets."""

    from .active_data_certification import (
        canonical_parquet_fingerprint,
        canonical_parquet_sequence_fingerprint,
    )
    from .active_data_view import (
        ACTIVE_ROOT,
        CERTIFICATION_STATE,
        UpdateMode,
        _write_new_or_exact,
        build_content_validation_receipt,
        build_plan,
        cohort_for_coverage,
        publish_full_successor,
        resolve,
        stage_view,
        verify_approval,
        verify_plan_bindings,
        verify_view,
    )

    root = repository_root.resolve(strict=True)
    current_catalog = verify_view(root / ACTIVE_ROOT)
    if len(user_authorization_id) != 64 or any(
        character not in "0123456789abcdef" for character in user_authorization_id
    ):
        raise ContractError("user authorization ID is invalid")
    try:
        datetime.strptime(approved_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ContractError("approval time must be a UTC second") from exc

    contract = load_contract(root)
    certificate_paths = {
        market: str(contract["certificate_paths"][market])  # type: ignore[index]
        for market in ACTIVE_RELEASE_MARKETS
    }
    package = build_certificate_package(
        repository_root=root, certificate_paths=certificate_paths
    )
    market_plans = [
        build_market_publication_plan(
            repository_root=root, package=package, market=market
        )
        for market in ACTIVE_RELEASE_MARKETS
    ]
    snapshot = build_snapshot(package=package, market_plans=market_plans)
    semantic_bindings = {
        path: sha256_file(root / path)
        for path in (
            CONTRACT_PATH.as_posix(),
            "configs/active_alpha_research_ladder.json",
            "configs/active_data_view_contract.json",
            "configs/source_contract.json",
        )
    }
    implementation_bindings = {
        path: sha256_file(root / path)
        for path in (
            "src/futures_rebuild/active_data_certification.py",
            "src/futures_rebuild/active_data_view.py",
            "src/futures_rebuild/causal_incremental_release.py",
        )
    }
    environment_bindings = {
        path: sha256_file(root / path)
        for path in ("configs/environment.lock.json", "requirements.lock")
    }
    release_id = sha256_json(
        {
            "snapshot_id": snapshot["snapshot_id"],
            "semantic_bindings": semantic_bindings,
            "implementation_bindings": implementation_bindings,
            "environment_bindings": environment_bindings,
        }
    )
    artifact_root = root / "reports" / "causal_alpha_ladder_active_release" / release_id
    active_ladder_sha256 = semantic_bindings["configs/active_alpha_research_ladder.json"]

    materializations: list[dict[str, object]] = []
    protected = []
    planned_entries: list[dict[str, object]] = []
    for market_plan in market_plans:
        market = str(market_plan["market"])
        for year_plan in market_plan["years"]:  # type: ignore[index]
            year = int(year_plan["year"])
            items = list(year_plan["source_observation_partitions"])
            coverage_start, coverage_end = _coverage(items)
            if year == 2025 and (
                coverage_start != "2025-01-01"
                or coverage_end != DEVELOPMENT_END_EXCLUSIVE.replace(":", "")
            ):
                raise IntegrityError("2025 source is not the exact development prefix")
            sources = [
                io_path(root / PurePosixPath(str(item["path"]))) for item in items
            ]
            fingerprint = canonical_parquet_sequence_fingerprint(sources)
            source_bindings = [
                {
                    "path": str(item["path"]),
                    "sha256": str(item["sha256"]),
                    "size": int(item["size"]),
                }
                for item in items
            ]
            receipt = build_content_validation_receipt(
                market=market,
                year=year,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                source_bindings=source_bindings,
                semantic_bindings=semantic_bindings,
                implementation_bindings=implementation_bindings,
                environment_bindings=environment_bindings,
                canonical_schema_fingerprint=str(fingerprint["schema_fingerprint"]),
                canonical_row_hash=str(fingerprint["canonical_row_hash"]),
                row_count=int(fingerprint["row_count"]),
                checks={
                    "certificate_revalidated": "PASS",
                    "source_sequence_fully_read": "PASS",
                    "source_sequence_nonempty": "PASS",
                },
                limitations=(
                    "NO_HOLDOUT_OR_FORWARD_ACCESS",
                    "NO_MODEL_FIT_EVALUATION_OR_TRADING_AUTHORITY",
                ),
            )
            materializations.append(
                {
                    "market": market,
                    "year": year,
                    "coverage_start": coverage_start,
                    "coverage_end": coverage_end,
                    "coverage_kind": (
                        "CERTIFIED_V10_PARTIAL_2025_DEVELOPMENT_SOURCE"
                        if year == 2025
                        else "CERTIFIED_V10_DEVELOPMENT_SOURCE"
                    ),
                    "source_paths": [str(item["path"]) for item in items],
                    "source_sha256s": [str(item["sha256"]) for item in items],
                    "source_bindings": source_bindings,
                    "row_count": int(fingerprint["row_count"]),
                    "schema_fingerprint": str(fingerprint["schema_fingerprint"]),
                    "content_validation_receipt": receipt,
                    "content_validation_receipt_id": receipt[
                        "content_validation_receipt_id"
                    ],
                    "universe_contract_sha256": active_ladder_sha256,
                }
            )
            planned_entries.append(
                {
                    "cohort": cohort_for_coverage(
                        year=year,
                        coverage_start=coverage_start,
                        coverage_end=coverage_end,
                    ),
                    "coverage_end": coverage_end,
                    "coverage_kind": (
                        "CERTIFIED_V10_PARTIAL_2025_DEVELOPMENT_SOURCE"
                        if year == 2025
                        else "CERTIFIED_V10_DEVELOPMENT_SOURCE"
                    ),
                    "coverage_start": coverage_start,
                    "disposition": CERTIFICATION_STATE,
                    "market": market,
                    "selection_eligible": 2018 <= year <= 2022,
                    "year": year,
                }
            )

    materializations.sort(key=lambda item: (str(item["market"]), int(item["year"])))
    planned_entries.sort(key=lambda item: (str(item["market"]), int(item["year"])))
    input_core: dict[str, object] = {
        "schema_version": "causal_alpha_ladder_materialization_input/1.0.0",
        "package": package,
        "snapshot": snapshot,
        "materializations": materializations,
        "protected_entries": protected,
        "summary": {
            "market_count": len(ACTIVE_RELEASE_MARKETS),
            "materialization_count": len(materializations),
            "protected_count": len(protected),
            "source_bytes": sum(
                int(binding["size"])
                for item in materializations
                for binding in item["source_bindings"]  # type: ignore[index]
            ),
            "source_files": sum(
                len(item["source_paths"]) for item in materializations  # type: ignore[arg-type]
            ),
            "row_count": sum(int(item["row_count"]) for item in materializations),
        },
    }
    materialization_input = {
        **input_core,
        "materialization_input_id": sha256_json(input_core),
    }
    input_path = artifact_root / "materialization_input.json"
    _write_new_or_exact(artifact_root / "certificate_package.json", package)
    for plan in market_plans:
        _write_new_or_exact(
            artifact_root / f"{plan['market']}_annual_plan.json", plan
        )
    _write_new_or_exact(artifact_root / "snapshot.json", snapshot)
    _write_new_or_exact(input_path, materialization_input)

    materialization_plan = build_plan(
        operation="MATERIALIZE_CAUSAL_ACTIVE_VIEW",
        mode=UpdateMode.FULL_SUCCESSOR,
        foundation_release_id=str(package["source_release_id"]),
        foundation_manifest_sha256=semantic_bindings["configs/source_contract.json"],
        semantic_bindings=semantic_bindings,
        entries=planned_entries,
        limits={
            "maximum_materializations": len(materializations),
            "maximum_output_files": len(materializations) * 2 + 1,
            "maximum_rows": int(input_core["summary"]["row_count"]),  # type: ignore[index]
            "maximum_source_bytes": int(input_core["summary"]["source_bytes"]),  # type: ignore[index]
            "maximum_source_files": int(input_core["summary"]["source_files"]),  # type: ignore[index]
        },
        forbidden_actions=(
            "HOLDOUT_OR_FORWARD_ACCESS",
            "PROVIDER_ACCESS",
            "SOURCE_V10_MUTATION",
            "TRADING",
        ),
        outputs=("state/active_data_view_staging",),
        implementation_bindings=implementation_bindings,
        environment_bindings=environment_bindings,
        recovery_boundary="CREATE_ONLY_STAGING_CURRENT_ACTIVE_UNCHANGED",
    )
    materialization_core = {
        key: value for key, value in materialization_plan.items() if key != "plan_id"
    }
    materialization_core["materialization_input_binding"] = {
        "materialization_input_id": materialization_input["materialization_input_id"],
        "path": input_path.relative_to(root).as_posix(),
        "sha256": sha256_file(input_path),
    }
    materialization_plan = {
        **materialization_core,
        "plan_id": sha256_json(materialization_core),
    }
    materialization_approval = _approved_plan(
        materialization_plan,
        user_authorization_id=user_authorization_id,
        approved_at=approved_at,
    )
    materialization_plan_path = artifact_root / "materialization_plan.json"
    materialization_approval_path = artifact_root / "materialization_approval.json"
    _write_new_or_exact(materialization_plan_path, materialization_plan)
    _write_new_or_exact(materialization_approval_path, materialization_approval)
    verify_plan_bindings(root, materialization_plan)
    verify_approval(
        materialization_approval,
        materialization_plan,
        expected_operation="MATERIALIZE_CAUSAL_ACTIVE_VIEW",
    )

    stage, catalog = stage_view(
        repository_root=root,
        plan=materialization_plan,
        foundation_release_id=str(package["source_release_id"]),
        foundation_manifest_sha256=semantic_bindings["configs/source_contract.json"],
        semantic_bindings=semantic_bindings,
        materializations=materializations,
        non_materialized_entries=protected,
    )
    verification_rows = []
    receipt_by_key = {
        (str(item["market"]), int(item["year"])): item[
            "content_validation_receipt"
        ]
        for item in materializations
    }
    for item in materializations:
        market, year = str(item["market"]), int(item["year"])
        output = stage / "causally_gated_normalized" / market / str(year) / f"{year}.parquet"
        fingerprint = canonical_parquet_fingerprint(io_path(output))
        receipt = receipt_by_key[(market, year)]
        if (
            fingerprint["canonical_row_hash"] != receipt["canonical_row_hash"]
            or fingerprint["schema_fingerprint"]
            != receipt["canonical_schema_fingerprint"]
            or fingerprint["row_count"] != receipt["row_count"]
        ):
            raise IntegrityError("independent annual readback differs from certified source")
        verification_rows.append(
            {
                "market": market,
                "year": year,
                "canonical_row_hash": fingerprint["canonical_row_hash"],
                "row_count": fingerprint["row_count"],
                "status": "PASS",
            }
        )
    verification_core = {
        "schema_version": "causal_alpha_ladder_active_readback/1.0.0",
        "status": "PASS_FULL_ANNUAL_READBACK",
        "active_view_id": catalog["active_view_id"],
        "market_count": len(ACTIVE_RELEASE_MARKETS),
        "materialized_year_count": len(verification_rows),
        "protected_year_count": len(protected),
        "results": verification_rows,
    }
    verification = {
        **verification_core,
        "verification_id": sha256_json(verification_core),
    }
    verification_path = artifact_root / "independent_readback.json"
    _write_new_or_exact(verification_path, verification)
    verify_view(stage)

    files = [path for path in stage.rglob("*") if path.is_file()]
    publication_binding = {
        "active_view_id": catalog["active_view_id"],
        "catalog_file_sha256": sha256_file(stage / "catalog.json"),
        "catalog_sha256": catalog["catalog_sha256"],
        "entry_count": len(catalog["entries"]),
        "materialization_approval_path": materialization_approval_path.relative_to(root).as_posix(),
        "materialization_approval_receipt_id": materialization_approval["approval_receipt_id"],
        "materialization_approval_sha256": sha256_file(materialization_approval_path),
        "materialization_input_id": materialization_input["materialization_input_id"],
        "materialization_input_path": input_path.relative_to(root).as_posix(),
        "materialization_input_sha256": sha256_file(input_path),
        "materialization_plan_file_sha256": sha256_file(materialization_plan_path),
        "materialization_plan_id": materialization_plan["plan_id"],
        "materialization_plan_path": materialization_plan_path.relative_to(root).as_posix(),
        "materialization_plan_sha256": sha256_json(materialization_plan),
        "materialized_count": len(materializations),
        "non_materialized_count": len(protected),
        "parquet_count": sum(path.suffix == ".parquet" for path in files),
        "sidecar_count": sum(
            path.name.endswith(".parquet.manifest.json") for path in files
        ),
        "staging_bytes": sum(path.stat().st_size for path in files),
        "staging_file_count": len(files),
        "staging_path": stage.relative_to(root).as_posix(),
    }
    publication_plan = build_plan(
        operation="PUBLISH_CAUSAL_ACTIVE_VIEW",
        mode=UpdateMode.FULL_SUCCESSOR,
        foundation_release_id=str(package["source_release_id"]),
        foundation_manifest_sha256=semantic_bindings["configs/source_contract.json"],
        semantic_bindings=semantic_bindings,
        entries=planned_entries,
        limits={
            "maximum_active_files": len(files),
            "maximum_catalog_entries": len(catalog["entries"]),
            "maximum_input_bytes": publication_binding["staging_bytes"],
            "maximum_input_files": len(files),
            "maximum_publications": 1,
            "maximum_retries": 0,
            "maximum_workers": 1,
        },
        forbidden_actions=(
            "HOLDOUT_OR_FORWARD_ACCESS",
            "PROVIDER_ACCESS",
            "SOURCE_V10_MUTATION",
            "TRADING",
        ),
        outputs=("data/active", "state/active_data_view_publication"),
        implementation_bindings=implementation_bindings,
        environment_bindings=environment_bindings,
        recovery_boundary="ATOMIC_FULL_SUCCESSOR_WITH_PREDECESSOR_ROLLBACK",
    )
    publication_core = {
        key: value for key, value in publication_plan.items() if key != "plan_id"
    }
    publication_core["publication_input_binding"] = publication_binding
    publication_core["publication_input_binding"]["predecessor_active_view_id"] = (
        current_catalog["active_view_id"]
    )
    publication_core["publication_input_binding"]["predecessor_catalog_sha256"] = (
        current_catalog["catalog_sha256"]
    )
    publication_plan = {
        **publication_core,
        "plan_id": sha256_json(publication_core),
    }
    publication_approval = _approved_plan(
        publication_plan,
        user_authorization_id=user_authorization_id,
        approved_at=approved_at,
    )
    publication_plan_path = artifact_root / "publication_plan.json"
    publication_approval_path = artifact_root / "publication_approval.json"
    _write_new_or_exact(publication_plan_path, publication_plan)
    _write_new_or_exact(publication_approval_path, publication_approval)
    verify_plan_bindings(root, publication_plan)
    receipt = publish_full_successor(
        repository_root=root,
        staging=stage,
        plan=publication_plan,
        approval=publication_approval,
    )
    _write_new_or_exact(artifact_root / "publication_receipt.json", receipt)
    active_catalog = verify_view(root / ACTIVE_ROOT)
    resolved = {
        market: resolve(
            repository_root=root,
            market=market,
            year=2025,
            purpose="FEATURE_GENERATION",
        ).relative_to(root).as_posix()
        for market in ACTIVE_RELEASE_MARKETS
    }
    final_core = {
        "schema_version": "causal_alpha_ladder_active_release_terminal/1.0.0",
        "status": "ACTIVE_RELEASED_RESEARCH_READY_CAUSAL_PRICE",
        "active_view_id": active_catalog["active_view_id"],
        "catalog_sha256": active_catalog["catalog_sha256"],
        "markets": list(ACTIVE_RELEASE_MARKETS),
        "materialized_year_range": [2010, 2025],
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "protected_years": [],
        "materialized_market_years": len(materializations),
        "resolved_feature_generation_paths": resolved,
        "publication_receipt_id": receipt["publication_receipt_id"],
        "provider_calls": 0,
        "holdout_or_forward_rows": 0,
        "source_v10_mutated": False,
    }
    final = {**final_core, "terminal_id": sha256_json(final_core)}
    _write_new_or_exact(artifact_root / "terminal.json", final)
    return final


def main() -> int:
    authorization = sha256_json(
        {
            "scope": (
                "User requested a full successor active release of ES NQ CL GC ZN 6E "
                "through the 2025-07-13T22:00:00Z development cutoff, excluding "
                "holdout and forward rows and preserving the predecessor rollback."
            )
        }
    )
    result = execute_active_release(
        repository_root=Path.cwd(),
        user_authorization_id=authorization,
        approved_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
