"""Coverage-only census of immutable local causal releases for V12."""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

from futures_rebuild.boundary import (
    OperationClassification, OperationReceipt, RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import verify_data_release_manifest
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.runtime_environment import require_locked_repository_environment
from futures_rebuild import tier1_bracket_v5 as v5
from futures_rebuild import tier1_bracket_v10 as v10


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "configs/tier1_bracket_v12_local_source_alternative_census_plan.json"
MANIFEST_ROOT = ROOT / "manifests/data_releases/causally_gated_normalized"
OPERATION = "AUDIT_V12_LOCAL_CAUSAL_RELEASE_ALTERNATIVES_READ_ONLY"


def _object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise IntegrityError(f"artifact is not an object: {path.as_posix()}")
    return value


def _catalog() -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for path in MANIFEST_ROOT.glob("*.json"):
        value = _object(path)
        metadata = value.get("metadata")
        if not isinstance(metadata, dict):
            continue
        market, year = metadata.get("market"), metadata.get("year")
        if market not in set(v5.MARKETS) or type(year) is not int or year not in range(2018, 2023):
            continue
        bars = [
            item for item in value.get("files", [])
            if isinstance(item, dict)
            and Path(str(item.get("logical_path", ""))).name == "bars.parquet"
        ]
        if len(bars) != 1:
            raise IntegrityError("candidate causal manifest has ambiguous bars payload")
        items.append({
            "market": market, "year": year,
            "release_id": value["release_id"],
            "payload_sha256": bars[0]["sha256"],
            "manifest_sha256": sha256_file(path),
        })
    return sorted(
        items,
        key=lambda item: (str(item["market"]), int(item["year"]), str(item["release_id"])),
    )


def _load_plan() -> dict[str, object]:
    plan = _object(PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("operation") != OPERATION
        or plan.get("execution_mode") != "IN_MEMORY_UNPUBLISHED_COVERAGE_COUNTS_ONLY"
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("candidate_release_count") != 61
        or plan.get("candidate_manifest_catalog_id") != sha256_json(_catalog())
        or plan.get("v12_contract_sha256")
        != sha256_file(ROOT / "configs/tier1_bracket_successor_v12.json")
        or plan.get("dependency_census_code_sha256")
        != sha256_file(ROOT / "src/futures_rebuild/tier1_bracket_v10.py")
        or plan.get("runner_sha256") != sha256_file(Path(__file__))
        or not isinstance(forbidden, dict) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("V12 local-source census plan drifted")
    return plan


def _claim(*, boundary: RepoBoundary, plan: dict[str, object]) -> Path:
    plan_id, plan_sha = str(plan["plan_id"]), sha256_file(PLAN_PATH)
    scope = {
        "candidate_manifest_catalog_id": str(plan["candidate_manifest_catalog_id"]),
        "candidate_release_count": str(plan["candidate_release_count"]),
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022",
        "holdout_or_forward_access": "false", "provider_access": "false",
        "model_fit": "false", "prediction_generation": "false",
        "historical_evaluation": "false", "publication": "false",
    }
    receipt = OperationReceipt.issue_user_approved(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=OPERATION,
        approval_plan_id=plan_id, approval_plan_sha256=plan_sha,
        approval_line=f"APPROVE {OPERATION} PLAN {plan_id} SHA256 {plan_sha}",
    )
    claim = ROOT / "state/authorization_uses" / f"{receipt.receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V12 local-source census authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v12_local_source_census_authorization_use/1.0.0",
                "receipt_id": receipt.receipt_id, "plan_id": plan_id,
                "plan_sha256": plan_sha,
                "candidate_manifest_catalog_id": plan["candidate_manifest_catalog_id"],
                "candidate_release_count": plan["candidate_release_count"],
                "holdout_or_forward_access": False, "provider_access": False,
                "model_fit": False, "prediction_generation": False,
                "historical_evaluation": False, "publication": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V12 local-source census receipt was already consumed") from exc
    return claim


def _candidate_path(
    *, boundary: RepoBoundary, item: dict[str, object],
) -> Path:
    manifest_path = MANIFEST_ROOT / f"{item['release_id']}.json"
    if sha256_file(manifest_path) != item["manifest_sha256"]:
        raise IntegrityError("candidate causal manifest changed")
    manifest = verify_data_release_manifest(manifest_path, boundary)
    matches = [
        entry for entry in manifest.files
        if Path(entry.logical_path).name == "bars.parquet"
    ]
    if (
        len(matches) != 1 or matches[0].sha256 != item["payload_sha256"]
        or manifest.metadata.get("market") != item["market"]
        or manifest.metadata.get("year") != item["year"]
    ):
        raise IntegrityError("candidate causal release identity drifted")
    return ROOT / manifest.physical_relative_path(matches[0])


def _add(target: dict[str, int], value: v10.DependencyWindowCensusV10) -> None:
    for field in fields(v10.DependencyWindowCensusV10):
        target[field.name] += int(getattr(value, field.name))


def _census(
    *, market: str, path: Path,
    expected_by_session: dict[str, tuple[v5.CensusCheckpoint, ...]],
) -> tuple[dict[str, int], dict[str, object]]:
    counts = {field.name: 0 for field in fields(v10.DependencyWindowCensusV10)}
    audit = v10.SourceIntegrityAuditV10(market)
    stream = v10.iter_source_records_from_parquet_v10(
        market=market, path=path, audit=audit,
    )
    active: str | None = None
    rows: list[v5.V5SourceRecord] = []
    processed: set[str] = set()

    def flush() -> None:
        nonlocal rows, active
        if active is None:
            return
        if active in processed:
            raise IntegrityError("candidate source session is not contiguous")
        processed.add(active)
        expected = expected_by_session.get(active, ())
        if expected:
            _add(counts, v10.audit_checkpoint_dependencies_v10(
                source_rows=tuple(rows), census=expected,
            ))
        rows = []

    for row in stream:
        if active is None:
            active = row.exchange_session_date
        elif row.exchange_session_date != active:
            flush()
            active = row.exchange_session_date
        rows.append(row)
        if len(rows) > 2_000:
            raise IntegrityError("candidate source session buffer exceeded 2,000 rows")
    flush()
    for session, expected in expected_by_session.items():
        if session not in processed:
            _add(counts, v10.audit_checkpoint_dependencies_v10(
                source_rows=(), census=expected,
            ))
    return counts, audit.as_dict()


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan = _load_plan()
    require_locked_repository_environment(ROOT)
    claim = _claim(boundary=boundary, plan=plan)
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    census = v5.build_expected_census_from_calendar(sessions=sessions)
    expected: dict[tuple[str, int], dict[str, list[v5.CensusCheckpoint]]] = {}
    for checkpoint in census:
        item = checkpoint.expected
        expected.setdefault((item.market, item.year), {}).setdefault(
            item.exchange_session_date, []
        ).append(checkpoint)
    results: dict[str, list[dict[str, object]]] = {}
    for item in _catalog():
        market, year = str(item["market"]), int(item["year"])
        path = _candidate_path(boundary=boundary, item=item)
        counts, audit = _census(
            market=market, path=path,
            expected_by_session={
                session: tuple(values)
                for session, values in expected[(market, year)].items()
            },
        )
        results.setdefault(f"{market}/{year}", []).append({
            "release_id": item["release_id"],
            "payload_sha256": item["payload_sha256"],
            "dependency_windows": counts, "source_integrity": audit,
        })
    selected: dict[str, object] = {}
    for key, values in results.items():
        ranked = sorted(values, key=lambda item: (
            -int(item["dependency_windows"]["complete_both_windows"]),
            -int(item["dependency_windows"]["complete_execution_windows"]),
            -int(item["dependency_windows"]["complete_feature_windows"]),
            int(item["dependency_windows"]["missing_source_sessions"]),
            int(item["dependency_windows"]["ambiguous_source_sessions"]),
            str(item["release_id"]),
        ))
        selected[key] = ranked[0]
    print(json.dumps({
        "status": "COMPLETED_IN_MEMORY_UNPUBLISHED_COVERAGE_COUNTS_ONLY",
        "plan_id": plan["plan_id"],
        "authorization_claim_sha256": sha256_file(claim),
        "selection_rule": plan["selection_rule"],
        "selected": selected, "all_candidates": results,
        "model_fit": False, "prediction_generation": False,
        "historical_evaluation": False, "publication": False,
        "holdout_or_forward_access": False, "provider_access": False,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
