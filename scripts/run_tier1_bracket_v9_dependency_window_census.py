"""Run the approved V9 dependency-window census without modeling or publication."""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import (
    manifest_relative_path,
    verify_data_release_manifest,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.runtime_environment import require_locked_repository_environment
from futures_rebuild import tier1_bracket_v5 as v5
from futures_rebuild import tier1_bracket_v10 as v10


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "configs/tier1_bracket_v9_dependency_window_census_plan.json"
REGISTRY_PATH = ROOT / (
    "state/trial_registry/tier1_bracket_successor_v9/"
    "fed4cc30c3f01e4f5b15eacfecdc50fe3a45bf671c0306d568f013f02c91dcd8.json"
)
APPROVAL_COMMAND = "AUDIT_V9_REGISTERED_SOURCE_DEPENDENCY_WINDOWS_READ_ONLY"


def _object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise IntegrityError(f"JSON artifact is not an object: {path.as_posix()}")
    return value


def _load_and_verify_plan() -> dict[str, object]:
    plan = _object(PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    source_scope = plan.get("source_scope")
    if (
        plan_id != sha256_json(core)
        or plan.get("operation") != APPROVAL_COMMAND
        or plan.get("execution_mode") != "IN_MEMORY_UNPUBLISHED_COUNTS_ONLY"
        or plan.get("maximum_host_runtime_seconds") != 300
        or not isinstance(forbidden, dict)
        or not all(value is True for value in forbidden.values())
        or not isinstance(source_scope, dict)
        or source_scope.get("markets") != ["6E", "CL", "ES", "ZN"]
        or source_scope.get("years") != [2018, 2019, 2020, 2021, 2022]
        or source_scope.get("registered_source_count") != 20
        or plan.get("trial_registry_sha256") != sha256_file(REGISTRY_PATH)
        or plan.get("v10_dependency_census_code_sha256")
        != sha256_file(ROOT / "src/futures_rebuild/tier1_bracket_v10.py")
    ):
        raise UnauthorizedOperation("dependency-window census plan drifted")
    return plan


def _claim_approval(
    *, boundary: RepoBoundary, plan: dict[str, object], registry: dict[str, object],
) -> Path:
    plan_id = str(plan["plan_id"])
    plan_sha = sha256_file(PLAN_PATH)
    scope = {
        "trial_id": str(plan["trial_id"]),
        "source_binding_id": str(plan["source_binding_id"]),
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022",
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "historical_evaluation": "false",
        "publication": "false",
    }
    approval_line = f"APPROVE {APPROVAL_COMMAND} PLAN {plan_id} SHA256 {plan_sha}"
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=APPROVAL_COMMAND,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=APPROVAL_COMMAND,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha,
        approval_line=approval_line,
    )
    receipt.verify(
        boundary,
        operation=APPROVAL_COMMAND,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    if not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("dependency-window census receipt is not single-use")
    claim = ROOT / "state/authorization_uses" / f"{receipt.receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V9 dependency-window census authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v9_dependency_window_census_authorization_use/1.0.0",
                "receipt_id": receipt.receipt_id,
                "trial_id": registry["trial_id"],
                "source_binding_id": registry["source_binding_id"],
                "plan_id": plan_id,
                "plan_sha256": plan_sha,
                "runner_sha256": sha256_file(Path(__file__)),
                "v10_dependency_census_code_sha256": sha256_file(
                    ROOT / "src/futures_rebuild/tier1_bracket_v10.py"
                ),
                "holdout_or_forward_access": False,
                "model_fit": False,
                "prediction_generation": False,
                "historical_evaluation": False,
                "publication": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("dependency-window census receipt was already consumed") from exc
    return claim


def _registered_sources(
    *, boundary: RepoBoundary, registry: dict[str, object],
) -> dict[tuple[str, int], Path]:
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("V9 registered source bindings are absent")
    if v5.source_binding_id_from_metadata_v5(raw) != registry.get("source_binding_id"):
        raise IntegrityError("V9 registered source binding identity drifted")
    paths: dict[tuple[str, int], Path] = {}
    for item in raw:
        market = str(item["market"])
        year = int(item["year"])
        if year == 2025 or year not in range(2018, 2023):
            raise UnauthorizedOperation("holdout or forward source rejected before open")
        release_id = str(item["causal_release_id"])
        manifest_path = ROOT / manifest_relative_path(
            "causally_gated_normalized", release_id,
        )
        manifest = verify_data_release_manifest(manifest_path, boundary)
        matches = [entry for entry in manifest.files if Path(entry.logical_path).name == "bars.parquet"]
        if (
            manifest.release_id != release_id
            or manifest.metadata.get("market") != market
            or manifest.metadata.get("year") != year
            or len(matches) != 1
            or matches[0].sha256 != item["source_parquet_sha256"]
        ):
            raise IntegrityError("registered causal source manifest drifted")
        paths[(market, year)] = ROOT / manifest.physical_relative_path(matches[0])
    expected = {(market, year) for market in v5.MARKETS for year in range(2018, 2023)}
    if set(paths) != expected:
        raise IntegrityError("registered source map is incomplete")
    return paths


def _add_counts(target: dict[str, int], source: v10.DependencyWindowCensusV10) -> None:
    for field in fields(v10.DependencyWindowCensusV10):
        target[field.name] += int(getattr(source, field.name))


def _census_one(
    *, market: str, year: int, path: Path,
    expected_by_session: dict[str, tuple[v5.CensusCheckpoint, ...]],
) -> tuple[dict[str, int], dict[str, object]]:
    counts = {field.name: 0 for field in fields(v10.DependencyWindowCensusV10)}
    audit = v10.SourceIntegrityAuditV10(market)
    stream = v10.iter_source_records_from_parquet_v10(
        market=market, path=path, audit=audit,
    )
    active_session: str | None = None
    active_rows: list[v5.V5SourceRecord] = []
    processed: set[str] = set()

    def flush() -> None:
        nonlocal active_rows, active_session
        if active_session is None:
            return
        if active_session in processed:
            raise IntegrityError("source session is not contiguous in the stream")
        processed.add(active_session)
        expected = expected_by_session.get(active_session, ())
        if expected:
            _add_counts(
                counts,
                v10.audit_checkpoint_dependencies_v10(
                    source_rows=tuple(active_rows), census=expected,
                ),
            )
        active_rows = []

    for row in stream:
        if active_session is None:
            active_session = row.exchange_session_date
        elif row.exchange_session_date != active_session:
            flush()
            active_session = row.exchange_session_date
        active_rows.append(row)
        if len(active_rows) > 2_000:
            raise IntegrityError("dependency census session buffer exceeded 2,000 rows")
    flush()
    for session, expected in expected_by_session.items():
        if session not in processed:
            _add_counts(
                counts,
                v10.audit_checkpoint_dependencies_v10(source_rows=(), census=expected),
            )
    audit_summary = audit.as_dict()
    audit_summary["sessions_with_observed_discontinuities"] = len(
        audit.sessions_with_observed_discontinuities
    )
    return counts, audit_summary


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan = _load_and_verify_plan()
    registry = _object(REGISTRY_PATH)
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != plan.get("trial_id")
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or registry.get("source_binding_id") != plan.get("source_binding_id")
        or registry.get("calendar_release_id") != plan.get("calendar_release_id")
        or not isinstance(bindings, dict)
        or any(sha256_file(ROOT / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V9 declaration is unavailable or drifted")
    require_locked_repository_environment(ROOT)
    _claim_approval(boundary=boundary, plan=plan, registry=registry)
    paths = _registered_sources(boundary=boundary, registry=registry)
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    census = v5.build_expected_census_from_calendar(sessions=sessions)
    by_market_year_session: dict[
        tuple[str, int], dict[str, tuple[v5.CensusCheckpoint, ...]]
    ] = {}
    mutable: dict[tuple[str, int, str], list[v5.CensusCheckpoint]] = {}
    for checkpoint in census:
        expected = checkpoint.expected
        mutable.setdefault(
            (expected.market, expected.year, expected.exchange_session_date), []
        ).append(checkpoint)
    for (market, year, session), values in mutable.items():
        by_market_year_session.setdefault((market, year), {})[session] = tuple(values)

    overall = {field.name: 0 for field in fields(v10.DependencyWindowCensusV10)}
    breakdown: dict[str, object] = {}
    for (market, year), path in sorted(paths.items()):
        counts, audit = _census_one(
            market=market, year=year, path=path,
            expected_by_session=by_market_year_session.get((market, year), {}),
        )
        for name, value in counts.items():
            overall[name] += value
        breakdown[f"{market}/{year}"] = {
            "dependency_windows": counts,
            "source_integrity": audit,
        }
    print(json.dumps({
        "status": "COMPLETED_IN_MEMORY_UNPUBLISHED",
        "operation": APPROVAL_COMMAND,
        "plan_id": plan["plan_id"],
        "trial_id": registry["trial_id"],
        "overall": overall,
        "market_year": breakdown,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
