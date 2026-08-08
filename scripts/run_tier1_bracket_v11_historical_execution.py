"""Execute the approved V11 historical screen without publishing evidence."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.boundary import (
    OperationClassification, OperationReceipt, RepoBoundary,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.data_layout import manifest_relative_path, verify_data_release_manifest
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild import tier1_bracket_v5 as v5
from futures_rebuild.tier1_bracket_v11_execution import (
    EXECUTION_OPERATION_V11, execute_authorized_v11,
)


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "configs/tier1_bracket_successor_v11_historical_execution_plan.json"


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V11 launcher artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("V11 launcher artifact is not an object")
    return value


def _load_plan() -> tuple[dict[str, object], Path]:
    plan = _object(PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    trial_id = str(plan.get("trial_id"))
    registry_path = ROOT / (
        f"state/trial_registry/tier1_bracket_successor_v11/{trial_id}.json"
    )
    forbidden = plan.get("forbidden_actions")
    bindings = plan.get("registered_execution_bindings")
    scope = plan.get("source_scope")
    if (
        plan_id != sha256_json(core)
        or plan.get("operation") != EXECUTION_OPERATION_V11
        or plan.get("execution_mode") != "IN_MEMORY_UNPUBLISHED_RESULT"
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("output_root") != "state/tier1_bracket_successor_v11_unpublished"
        or not isinstance(forbidden, dict)
        or not forbidden
        or not all(value is True for value in forbidden.values())
        or not isinstance(bindings, dict)
        or any(sha256_file(ROOT / path) != digest for path, digest in bindings.items())
        or not isinstance(scope, dict)
        or scope.get("markets") != ["6E", "CL", "ES", "ZN"]
        or scope.get("years") != [2018, 2019, 2020, 2021, 2022]
        or scope.get("registered_source_count") != 20
        or plan.get("trial_registry_sha256") != sha256_file(registry_path)
        or plan.get("runner_sha256") != sha256_file(Path(__file__))
    ):
        raise UnauthorizedOperation("V11 historical execution plan drifted")
    return plan, registry_path


def _registered_sources(
    *, boundary: RepoBoundary, registry: dict[str, object],
) -> dict[tuple[str, int], Path]:
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("V11 registered source bindings are absent")
    if v5.source_binding_id_from_metadata_v5(raw) != registry.get("source_binding_id"):
        raise IntegrityError("V11 registered source binding identity drifted")
    paths: dict[tuple[str, int], Path] = {}
    for item in raw:
        market, year = str(item["market"]), int(item["year"])
        if year == 2025 or year not in range(2018, 2023):
            raise UnauthorizedOperation("holdout or forward source rejected before open")
        release_id = str(item["causal_release_id"])
        manifest = verify_data_release_manifest(
            ROOT / manifest_relative_path("causally_gated_normalized", release_id),
            boundary,
        )
        matches = [
            entry for entry in manifest.files
            if Path(entry.logical_path).name == "bars.parquet"
        ]
        if (
            manifest.release_id != release_id
            or manifest.metadata.get("market") != market
            or manifest.metadata.get("year") != year
            or len(matches) != 1
            or matches[0].sha256 != item["source_parquet_sha256"]
        ):
            raise IntegrityError("V11 registered causal source manifest drifted")
        paths[(market, year)] = ROOT / manifest.physical_relative_path(matches[0])
    expected = {(market, year) for market in v5.MARKETS for year in range(2018, 2023)}
    if set(paths) != expected:
        raise IntegrityError("V11 registered source map is incomplete")
    return paths


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan, registry_path = _load_plan()
    registry = _object(registry_path)
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != plan.get("trial_id")
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("source_row_access") is not False
        or registry.get("holdout_or_forward_access") is not False
        or registry.get("source_binding_id") != plan.get("source_binding_id")
        or registry.get("calendar_release_id") != plan.get("calendar_release_id")
        or registry.get("dependency_lock_receipt_id")
        != plan.get("dependency_lock_receipt_id")
        or not isinstance(bindings, dict)
        or any(sha256_file(ROOT / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V11 declaration is unavailable or drifted")
    source_paths = _registered_sources(boundary=boundary, registry=registry)
    plan_id = str(plan["plan_id"])
    plan_sha = sha256_file(PLAN_PATH)
    output_root = ROOT / str(plan["output_root"])
    scope = {
        "trial_id": str(plan["trial_id"]),
        "source_binding_id": str(plan["source_binding_id"]),
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
    }
    receipt = OperationReceipt.issue_user_approved(
        boundary, operation=EXECUTION_OPERATION_V11,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=EXECUTION_OPERATION_V11,
        approval_plan_id=plan_id, approval_plan_sha256=plan_sha,
        approval_line=(
            f"APPROVE {EXECUTION_OPERATION_V11} PLAN {plan_id} SHA256 {plan_sha}"
        ),
    )
    result = execute_authorized_v11(
        root=ROOT, boundary=boundary, receipt=receipt,
        trial_id=str(plan["trial_id"]), source_paths=source_paths,
        output_root=output_root, plan_id=plan_id, plan_sha256=plan_sha,
    )
    stress = result.base.evaluation["stress"]
    print(json.dumps({
        "status": "COMPLETED_IN_MEMORY_UNPUBLISHED",
        "trial_id": plan["trial_id"], "plan_id": plan_id,
        "authorization_receipt_id": receipt.receipt_id,
        "decision": result.base.decision,
        "coverage": result.base.coverage,
        "stress_paths": {
            strategy: {
                "admitted_trades": len(path.admitted),
                "net_pnl_usd": str(path.ending_equity_usd - 100000),
                "maximum_continuous_drawdown_usd": str(
                    path.maximum_continuous_drawdown_usd
                ),
                "complete": path.complete,
            }
            for strategy, path in stress.items()
        },
        "source_integrity_audit": result.source_integrity_audit,
        "publication": False, "holdout_or_forward_access": False,
        "provider_access": False, "trading": False,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
