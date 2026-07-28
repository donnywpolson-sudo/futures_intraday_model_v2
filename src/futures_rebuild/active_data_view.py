"""Fail-closed planning, publication, and resolution for certified causal data.

The active view is derived and disposable.  Immutable DBN, Phase 1B, Phase 2,
and foundation releases remain the only provenance authority.  This module
never selects a release from directory dates, globs, or modification times.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any

from .boundary import RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .runtime_environment import require_locked_repository_environment


CONTRACT_PATH = Path("configs/active_data_view_contract.json")
ACTIVE_ROOT = Path("data/active")
ACTIVE_PAYLOAD_ROOT = ACTIVE_ROOT / "causally_gated_normalized"
CATALOG_PATH = ACTIVE_ROOT / "catalog.json"
STAGING_ROOT = Path("state/active_data_view_staging")
CERTIFICATION_ROOT = Path("state/active_data_view_certification")
PUBLICATION_LOCK = Path("state/locks/active_data_view.lock")
PUBLICATION_JOURNAL_ROOT = Path("state/active_data_view_publication")
ROLLBACK_ROOT = Path("state/active_data_view_rollback")
FAILED_PUBLICATION_ROOT = Path("state/active_data_view_failed_publication")

CONTRACT_SCHEMA = "causal_active_view_contract/1.0.0"
PLAN_SCHEMA = "causal_active_view_plan/1.0.0"
APPROVAL_SCHEMA = "causal_active_view_approval/1.0.0"
CATALOG_SCHEMA = "causal_active_catalog/1.0.0"
CONTENT_RECEIPT_SCHEMA = "causal_content_validation_receipt/1.0.0"
ACCESS_BINDING_SCHEMA = "causal_access_policy_binding/1.0.0"
SIDECAR_SCHEMA = "causal_active_market_year_manifest/1.0.0"
JOURNAL_SCHEMA = "causal_active_publication_journal/1.0.0"
PUBLICATION_RECEIPT_SCHEMA = "causal_active_publication_receipt/1.0.0"
CERTIFICATION_STATE = "RESEARCH_READY_CAUSAL_PRICE"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MARKET = re.compile(r"^[0-9A-Z]{1,16}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_YEAR = range(2000, 2201)

PROTECTED_DISPOSITIONS = {
    "LOCKED_HOLDOUT_NOT_MATERIALIZED",
    "FORWARD_ONLY_NOT_MATERIALIZED",
}
NON_MATERIALIZED_DISPOSITIONS = PROTECTED_DISPOSITIONS | {
    "QUARANTINED_NOT_MATERIALIZED"
}
COHORT_PERMISSIONS: Mapping[str, tuple[str, ...]] = {
    "DATA_QUALITY_ONLY": ("DATA_QUALITY", "LINEAGE_AUDIT"),
    "FORMATION_CONTEXT": ("FORMATION_CONTEXT", "LINEAGE_AUDIT"),
    "LEGACY_FEED_STRESS": ("FEED_STRESS", "LINEAGE_AUDIT"),
    "FEED_TRANSITION_STRESS": ("FEED_STRESS", "LINEAGE_AUDIT"),
    "DISCOVERY_SELECTION": (
        "DISCOVERY_RESEARCH",
        "FEATURE_GENERATION",
        "LINEAGE_AUDIT",
        "SELECTION",
    ),
    "NON_PRISTINE_RESEARCH": (
        "FEATURE_GENERATION",
        "LINEAGE_AUDIT",
        "NON_SELECTION_RESEARCH",
    ),
    "LOCKED_HOLDOUT": (),
    "FORWARD_ONLY": (),
}


class UpdateMode(str, Enum):
    INITIAL = "INITIAL"
    APPEND_ONLY = "APPEND_ONLY"
    FULL_SUCCESSOR = "FULL_SUCCESSOR"


def _plain_sha256(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ContractError(f"{name} must be one lowercase SHA-256")
    return value


def _plain_market(value: object) -> str:
    if type(value) is not str or _MARKET.fullmatch(value) is None:
        raise ContractError("market is not a canonical symbol")
    return value


def _plain_year(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value not in _YEAR:
        raise ContractError("year is outside the canonical range")
    return value


def _load_canonical(path: Path, description: str) -> dict[str, object]:
    assert_plain_file(path)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not valid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    encoded = canonical_bytes(dict(payload)) + b"\n"
    assert_no_linklike_ancestors(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path,
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0),
        )
    except FileExistsError:
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"existing artifact differs: {path}")
        return
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _resolve_certification_workspace(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    run_id: str,
    market: str,
    year: int,
) -> tuple[Path, Path]:
    root = repository_root.resolve(strict=True)
    _plain_sha256(run_id, "certification run ID")
    _plain_market(market)
    _plain_year(year)
    outputs = plan.get("outputs")
    if (
        not isinstance(outputs, list)
        or not outputs
        or any(type(value) is not str for value in outputs)
    ):
        raise IntegrityError("certification plan outputs are invalid")
    state_prefix = f"{CERTIFICATION_ROOT.as_posix()}/"
    state_outputs = [
        value for value in outputs if str(value).startswith(state_prefix)
    ]
    if len(state_outputs) != 1:
        raise IntegrityError(
            "certification plan must declare exactly one temporary state root"
        )
    state_relative = str(state_outputs[0])
    state_pure = PurePosixPath(state_relative)
    if (
        state_pure.is_absolute()
        or state_pure.as_posix() != state_relative
        or "\\" in state_relative
        or any(part in {"", ".", ".."} for part in state_pure.parts)
    ):
        raise IntegrityError("certification temporary state root is not canonical")

    pilot_run_ids = plan.get("pilot_run_ids")
    run_component = run_id
    if pilot_run_ids is not None:
        if (
            not isinstance(pilot_run_ids, list)
            or len(pilot_run_ids) != 2
            or len(set(pilot_run_ids)) != 2
            or any(
                type(value) is not str or _SHA256.fullmatch(value) is None
                for value in pilot_run_ids
            )
            or run_id not in pilot_run_ids
        ):
            raise UnauthorizedOperation(
                "certification run ID is outside the pilot plan"
            )
        scope_id = _plain_sha256(plan.get("pilot_scope_id"), "pilot scope ID")
        expected_outputs = sorted(
            [
                f"reports/active_data_view/pilot/{scope_id}/run-1",
                f"reports/active_data_view/pilot/{scope_id}/run-2",
                f"state/active_data_view_certification/pilot/{scope_id}",
            ]
        )
        if outputs != expected_outputs:
            raise IntegrityError(
                "pilot certification outputs differ from the declared scope"
            )
        run_component = f"run-{pilot_run_ids.index(run_id) + 1}"

    state_root = root / state_pure
    workspace = state_root / run_component / market / str(year)
    assert_no_linklike_ancestors(workspace)
    resolved_state = state_root.resolve(strict=False)
    resolved_workspace = workspace.resolve(strict=False)
    try:
        resolved_workspace.relative_to(resolved_state)
        resolved_state.relative_to(root)
    except ValueError as exc:
        raise IntegrityError(
            "certification workspace escapes the plan-declared state root"
        ) from exc
    return state_root, workspace


def verify_contract(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    payload = _load_canonical(root / CONTRACT_PATH, "active-view contract")
    expected = {
        "active_root",
        "catalog_schema",
        "certification_root",
        "content_validation_receipt_schema",
        "does_not_authorize",
        "forbidden_authority_sources",
        "historical_calendar_claim",
        "historical_evidence_basis",
        "historical_row_admission",
        "historical_session_roll_role",
        "historical_uncertainty_rule",
        "manifest_layout",
        "parquet_layout",
        "plan_schema",
        "policy_binding_schema",
        "publication_journal_schema",
        "publication_lock",
        "publication_receipt_schema",
        "research_capability",
        "resolver_rules",
        "schema_version",
        "staging_root",
        "update_modes",
    }
    if (
        set(payload) != expected
        or payload["schema_version"] != CONTRACT_SCHEMA
        or payload["active_root"] != ACTIVE_ROOT.as_posix()
        or payload["catalog_schema"] != CATALOG_SCHEMA
        or payload["content_validation_receipt_schema"] != CONTENT_RECEIPT_SCHEMA
        or payload["policy_binding_schema"] != ACCESS_BINDING_SCHEMA
        or payload["plan_schema"] != PLAN_SCHEMA
        or payload["publication_journal_schema"] != JOURNAL_SCHEMA
        or payload["publication_receipt_schema"] != PUBLICATION_RECEIPT_SCHEMA
        or payload["update_modes"] != sorted(item.value for item in UpdateMode)
        or payload["forbidden_authority_sources"]
        != ["FOLDER_DATE", "FALLBACK_RELEASE", "FILESYSTEM_GLOB", "NEWEST_HASH"]
        or payload["historical_calendar_claim"]
        != "NOT_OFFICIAL_HISTORICAL_CME_SESSION_AUTHORITY"
        or payload["historical_evidence_basis"]
        != "IMMUTABLE_ACCEPTED_DATABENTO_DBN_OBSERVABILITY"
        or payload["historical_row_admission"]
        != (
            "ACTUAL_DECODED_SOURCE_ROWS_ONLY_NO_FILL_INTERPOLATION_"
            "SYNTHETIC_OPEN_OR_SYNTHETIC_CLOSE"
        )
        or payload["historical_session_roll_role"]
        != "TRADE_DATE_GROUPING_ONLY_NOT_TRADING_HOURS_AUTHORITY"
        or payload["historical_uncertainty_rule"]
        != "UNOBSERVED_TIME_IS_MISSING_NOT_CLOSED"
    ):
        raise IntegrityError("active-view contract differs from the implementation")
    return payload


def cohort_for_year(year: int) -> str:
    _plain_year(year)
    if year == 2010:
        return "DATA_QUALITY_ONLY"
    if year == 2011:
        return "FORMATION_CONTEXT"
    if 2012 <= year <= 2016:
        return "LEGACY_FEED_STRESS"
    if year == 2017:
        return "FEED_TRANSITION_STRESS"
    if 2018 <= year <= 2022:
        return "DISCOVERY_SELECTION"
    if 2023 <= year <= 2024:
        return "NON_PRISTINE_RESEARCH"
    if year == 2025:
        return "LOCKED_HOLDOUT"
    if year == 2026:
        return "FORWARD_ONLY"
    raise ContractError("market-year lacks an explicit cohort assignment")


def disposition_for(*, year: int, research_admissible: bool) -> str:
    cohort = cohort_for_year(year)
    if cohort == "LOCKED_HOLDOUT":
        return "LOCKED_HOLDOUT_NOT_MATERIALIZED"
    if cohort == "FORWARD_ONLY":
        return "FORWARD_ONLY_NOT_MATERIALIZED"
    if type(research_admissible) is not bool:
        raise ContractError("research-admissible flag must be boolean")
    return (
        CERTIFICATION_STATE
        if research_admissible
        else "QUARANTINED_NOT_MATERIALIZED"
    )


def selection_eligible(*, cohort: str, disposition: str) -> bool:
    return cohort == "DISCOVERY_SELECTION" and disposition == CERTIFICATION_STATE


def _canonical_target(market: str, year: int) -> tuple[str, str]:
    _plain_market(market)
    _plain_year(year)
    base = f"data/active/causally_gated_normalized/{market}/{year}/{year}.parquet"
    return base, f"{base}.manifest.json"


@dataclass(frozen=True)
class CatalogEntry:
    market: str
    year: int
    coverage_start: str
    coverage_end: str
    coverage_kind: str
    cohort: str
    disposition: str
    selection_eligible: bool
    permitted_uses: tuple[str, ...]
    source_bindings: tuple[Mapping[str, object], ...]
    content_validation_receipt_id: str | None = None
    access_policy_binding_id: str | None = None
    parquet_sha256: str | None = None
    sidecar_sha256: str | None = None
    row_count: int | None = None
    schema_fingerprint: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        _plain_market(self.market)
        _plain_year(self.year)
        expected_cohort = cohort_for_year(self.year)
        if self.cohort != expected_cohort:
            raise ContractError("catalog cohort differs from explicit year policy")
        expected_selection = selection_eligible(
            cohort=self.cohort, disposition=self.disposition
        )
        if self.selection_eligible is not expected_selection:
            raise ContractError("catalog selection eligibility is invalid")
        if not self.coverage_start or not self.coverage_end:
            raise ContractError("catalog coverage is absent")
        materialized = self.disposition == CERTIFICATION_STATE
        bound = (
            self.content_validation_receipt_id,
            self.access_policy_binding_id,
            self.parquet_sha256,
            self.sidecar_sha256,
            self.schema_fingerprint,
        )
        if materialized:
            if (
                self.permitted_uses != tuple(COHORT_PERMISSIONS[self.cohort])
                or
                any(value is None for value in bound)
                or isinstance(self.row_count, bool)
                or not isinstance(self.row_count, int)
                or self.row_count <= 0
                or self.reason is not None
            ):
                raise ContractError("research-ready entry lacks certification bindings")
            for value in bound:
                _plain_sha256(value, "catalog binding")
        elif (
            self.disposition not in NON_MATERIALIZED_DISPOSITIONS
            or any(value is not None for value in (*bound, self.row_count))
            or not self.reason
            or self.permitted_uses
        ):
            raise ContractError("non-materialized catalog entry is invalid")

    @property
    def key(self) -> str:
        return f"{self.market}/{self.year}"

    def as_dict(self) -> dict[str, object]:
        parquet_path, sidecar_path = _canonical_target(self.market, self.year)
        payload: dict[str, object] = {
            "access_policy_binding_id": self.access_policy_binding_id,
            "cohort": self.cohort,
            "content_validation_receipt_id": self.content_validation_receipt_id,
            "coverage_end": self.coverage_end,
            "coverage_kind": self.coverage_kind,
            "coverage_start": self.coverage_start,
            "disposition": self.disposition,
            "market": self.market,
            "parquet_path": parquet_path if self.disposition == CERTIFICATION_STATE else None,
            "parquet_sha256": self.parquet_sha256,
            "permitted_uses": list(self.permitted_uses),
            "reason": self.reason,
            "row_count": self.row_count,
            "schema_fingerprint": self.schema_fingerprint,
            "selection_eligible": self.selection_eligible,
            "sidecar_path": sidecar_path if self.disposition == CERTIFICATION_STATE else None,
            "sidecar_sha256": self.sidecar_sha256,
            "source_bindings": [dict(item) for item in self.source_bindings],
            "year": self.year,
        }
        return payload


def classify_update(
    *,
    current_catalog: Mapping[str, object] | None,
    proposed_entries: Sequence[Mapping[str, object]],
    current_semantic_bindings: Mapping[str, str] | None = None,
    proposed_semantic_bindings: Mapping[str, str],
) -> UpdateMode:
    if current_catalog is None:
        return UpdateMode.INITIAL
    validate_catalog(current_catalog, verify_self_hash=True)
    current_bindings = current_catalog.get("semantic_bindings")
    if (
        not isinstance(current_bindings, dict)
        or current_semantic_bindings is None
        or dict(current_bindings) != dict(current_semantic_bindings)
        or dict(current_bindings) != dict(proposed_semantic_bindings)
    ):
        return UpdateMode.FULL_SUCCESSOR
    current_entries = current_catalog["entries"]
    assert isinstance(current_entries, list)
    old = {
        f"{item['market']}/{item['year']}": item
        for item in current_entries
        if isinstance(item, dict)
    }
    new = {
        f"{item['market']}/{item['year']}": item
        for item in proposed_entries
        if isinstance(item, dict)
    }
    if len(old) != len(current_entries) or len(new) != len(proposed_entries):
        raise IntegrityError("successor entries are not exact objects")
    if not set(old).issubset(new):
        return UpdateMode.FULL_SUCCESSOR
    if any(old[key] != new[key] for key in old):
        return UpdateMode.FULL_SUCCESSOR
    if set(new) == set(old):
        raise ContractError("successor contains no change")
    return UpdateMode.APPEND_ONLY


def build_plan(
    *,
    operation: str,
    mode: UpdateMode,
    foundation_release_id: str,
    foundation_manifest_sha256: str,
    semantic_bindings: Mapping[str, str],
    entries: Sequence[Mapping[str, object]],
    limits: Mapping[str, int],
    forbidden_actions: Sequence[str],
    outputs: Sequence[str],
    implementation_bindings: Mapping[str, str],
    environment_bindings: Mapping[str, str],
    recovery_boundary: str,
) -> dict[str, object]:
    if not operation or mode not in UpdateMode:
        raise ContractError("active-view operation or update mode is invalid")
    _plain_sha256(foundation_release_id, "foundation release ID")
    _plain_sha256(foundation_manifest_sha256, "foundation manifest hash")
    if not entries or not forbidden_actions or not outputs or not recovery_boundary:
        raise ContractError("active-view plan is incompletely bounded")
    for group_name, group in (
        ("semantic", semantic_bindings),
        ("implementation", implementation_bindings),
        ("environment", environment_bindings),
    ):
        if not group:
            raise ContractError(f"{group_name} bindings are absent")
        for name, value in group.items():
            if not name:
                raise ContractError(f"{group_name} binding name is absent")
            _plain_sha256(value, f"{group_name} binding")
    for name, value in limits.items():
        if not name or isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ContractError("plan limit is invalid")
    core: dict[str, object] = {
        "entries": list(entries),
        "environment_bindings": dict(sorted(environment_bindings.items())),
        "forbidden_actions": sorted(set(forbidden_actions)),
        "foundation_manifest_sha256": foundation_manifest_sha256,
        "foundation_release_id": foundation_release_id,
        "implementation_bindings": dict(sorted(implementation_bindings.items())),
        "limits": dict(sorted(limits.items())),
        "operation": operation,
        "outputs": sorted(set(outputs)),
        "recovery_boundary": recovery_boundary,
        "schema_version": PLAN_SCHEMA,
        "semantic_bindings": dict(sorted(semantic_bindings.items())),
        "target_root": ACTIVE_PAYLOAD_ROOT.as_posix(),
        "update_mode": mode.value,
    }
    return {**core, "plan_id": sha256_json(core)}


def build_pending_approval(plan: Mapping[str, object]) -> dict[str, object]:
    plan_id = _plain_sha256(plan.get("plan_id"), "plan ID")
    if plan_id != sha256_json({k: v for k, v in plan.items() if k != "plan_id"}):
        raise IntegrityError("active-view plan self-hash is invalid")
    return {
        "approval_receipt_id": None,
        "approved_at": None,
        "operation": plan["operation"],
        "plan_id": plan_id,
        "plan_sha256": sha256_json(plan),
        "schema_version": APPROVAL_SCHEMA,
        "status": "PENDING",
        "user_authorization_id": None,
    }


def verify_approval(
    approval: Mapping[str, object],
    plan: Mapping[str, object],
    *,
    expected_operation: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != expected_operation
        or approval.get("operation") != plan.get("operation")
        or approval.get("plan_id") != plan.get("plan_id")
        or approval.get("plan_sha256") != sha256_json(plan)
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
    ):
        raise UnauthorizedOperation("operation lacks exact hash-bound approval")
    _plain_sha256(approval.get("user_authorization_id"), "user authorization ID")
    return str(approval["approval_receipt_id"])


def build_content_validation_receipt(
    *,
    market: str,
    year: int,
    coverage_start: str,
    coverage_end: str,
    source_bindings: Sequence[Mapping[str, object]],
    semantic_bindings: Mapping[str, str],
    implementation_bindings: Mapping[str, str],
    environment_bindings: Mapping[str, str],
    canonical_schema_fingerprint: str,
    canonical_row_hash: str,
    row_count: int,
    checks: Mapping[str, str],
    limitations: Sequence[str],
) -> dict[str, object]:
    _plain_market(market)
    _plain_year(year)
    _plain_sha256(canonical_schema_fingerprint, "canonical schema fingerprint")
    _plain_sha256(canonical_row_hash, "canonical row hash")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise ContractError("certification row count is invalid")
    if not checks or any(value != "PASS" for value in checks.values()):
        raise IntegrityError("content validation contains a non-pass result")
    core: dict[str, object] = {
        "canonical_row_hash": canonical_row_hash,
        "canonical_schema_fingerprint": canonical_schema_fingerprint,
        "checks": dict(sorted(checks.items())),
        "coverage_end": coverage_end,
        "coverage_start": coverage_start,
        "environment_bindings": dict(sorted(environment_bindings.items())),
        "implementation_bindings": dict(sorted(implementation_bindings.items())),
        "limitations": sorted(set(limitations)),
        "market": market,
        "row_count": row_count,
        "schema_version": CONTENT_RECEIPT_SCHEMA,
        "semantic_bindings": dict(sorted(semantic_bindings.items())),
        "source_bindings": [dict(item) for item in source_bindings],
        "state": CERTIFICATION_STATE,
        "year": year,
    }
    return {**core, "content_validation_receipt_id": sha256_json(core)}


def validate_content_validation_receipt(
    receipt: Mapping[str, object],
) -> str:
    receipt_id = _plain_sha256(
        receipt.get("content_validation_receipt_id"), "content validation receipt ID"
    )
    core = {key: value for key, value in receipt.items() if key != "content_validation_receipt_id"}
    if (
        receipt.get("schema_version") != CONTENT_RECEIPT_SCHEMA
        or receipt.get("state") != CERTIFICATION_STATE
        or receipt_id != sha256_json(core)
        or not isinstance(receipt.get("checks"), dict)
        or any(value != "PASS" for value in receipt["checks"].values())
    ):
        raise IntegrityError("content validation receipt is invalid")
    return receipt_id


def content_receipt_reusable(
    receipt: Mapping[str, object],
    *,
    source_bindings: Sequence[Mapping[str, object]],
    semantic_bindings: Mapping[str, str],
    implementation_bindings: Mapping[str, str],
    environment_bindings: Mapping[str, str],
) -> bool:
    """Return true only for an exactly unchanged data-affecting dependency closure."""

    validate_content_validation_receipt(receipt)
    return (
        receipt.get("source_bindings") == [dict(item) for item in source_bindings]
        and receipt.get("semantic_bindings")
        == dict(sorted(semantic_bindings.items()))
        and receipt.get("implementation_bindings")
        == dict(sorted(implementation_bindings.items()))
        and receipt.get("environment_bindings")
        == dict(sorted(environment_bindings.items()))
    )


def build_access_policy_binding(
    *,
    market: str,
    year: int,
    universe_contract_sha256: str,
    active_view_id: str,
    content_validation_receipt_id: str,
) -> dict[str, object]:
    _plain_market(market)
    cohort = cohort_for_year(_plain_year(year))
    if cohort in {"LOCKED_HOLDOUT", "FORWARD_ONLY"}:
        raise UnauthorizedOperation("protected market-year cannot receive an access binding")
    for value, name in (
        (universe_contract_sha256, "universe contract hash"),
        (active_view_id, "active view ID"),
        (content_validation_receipt_id, "content validation receipt ID"),
    ):
        _plain_sha256(value, name)
    core: dict[str, object] = {
        "active_view_id": active_view_id,
        "capability": CERTIFICATION_STATE,
        "cohort": cohort,
        "content_validation_receipt_id": content_validation_receipt_id,
        "forbidden_uses": [
            "HOLDOUT_OR_FORWARD_ACCESS",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PRE_2025_STATUS_DEPENDENT_USE",
            "TRADING",
        ],
        "market": market,
        "permitted_uses": list(COHORT_PERMISSIONS[cohort]),
        "schema_version": ACCESS_BINDING_SCHEMA,
        "selection_eligible": cohort == "DISCOVERY_SELECTION",
        "universe_contract_sha256": universe_contract_sha256,
        "year": year,
    }
    return {**core, "access_policy_binding_id": sha256_json(core)}


def validate_access_policy_binding(binding: Mapping[str, object]) -> str:
    binding_id = _plain_sha256(
        binding.get("access_policy_binding_id"), "access policy binding ID"
    )
    core = {key: value for key, value in binding.items() if key != "access_policy_binding_id"}
    market = _plain_market(binding.get("market"))
    year = _plain_year(binding.get("year"))
    cohort = cohort_for_year(year)
    if (
        binding.get("schema_version") != ACCESS_BINDING_SCHEMA
        or binding_id != sha256_json(core)
        or binding.get("cohort") != cohort
        or binding.get("capability") != CERTIFICATION_STATE
        or binding.get("selection_eligible") is not (cohort == "DISCOVERY_SELECTION")
        or binding.get("permitted_uses") != list(COHORT_PERMISSIONS[cohort])
        or market != binding["market"]
        or year < 2025
        and "PRE_2025_STATUS_DEPENDENT_USE"
        not in binding.get("forbidden_uses", ())
    ):
        raise IntegrityError("access policy binding is invalid")
    return binding_id


def build_sidecar(
    *,
    entry: CatalogEntry,
    content_receipt: Mapping[str, object],
    access_binding: Mapping[str, object],
) -> dict[str, object]:
    content_id = validate_content_validation_receipt(content_receipt)
    access_id = validate_access_policy_binding(access_binding)
    if (
        content_id != entry.content_validation_receipt_id
        or access_id != entry.access_policy_binding_id
    ):
        raise IntegrityError("market-year sidecar bindings differ from catalog entry")
    entry_binding = entry.as_dict()
    entry_binding.pop("sidecar_sha256")
    core: dict[str, object] = {
        "access_policy_binding": dict(access_binding),
        "content_validation_receipt": dict(content_receipt),
        "entry_binding": entry_binding,
        "schema_version": SIDECAR_SCHEMA,
    }
    return {**core, "sidecar_id": sha256_json(core)}


def build_catalog(
    *,
    active_view_id: str,
    plan_id: str,
    foundation_release_id: str,
    foundation_manifest_sha256: str,
    semantic_bindings: Mapping[str, str],
    entries: Sequence[CatalogEntry],
) -> dict[str, object]:
    for value, name in (
        (active_view_id, "active view ID"),
        (plan_id, "plan ID"),
        (foundation_release_id, "foundation release ID"),
        (foundation_manifest_sha256, "foundation manifest hash"),
    ):
        _plain_sha256(value, name)
    if not entries:
        raise ContractError("active catalog cannot be empty")
    ordered = sorted(entries, key=lambda item: (item.market, item.year))
    if list(entries) != ordered or len({item.key for item in entries}) != len(entries):
        raise ContractError("active catalog entries are not unique and ordered")
    dispositions: dict[str, int] = {}
    for entry in entries:
        dispositions[entry.disposition] = dispositions.get(entry.disposition, 0) + 1
    core: dict[str, object] = {
        "active_view_id": active_view_id,
        "disposition_counts": dict(sorted(dispositions.items())),
        "entries": [item.as_dict() for item in entries],
        "foundation_manifest_sha256": foundation_manifest_sha256,
        "foundation_release_id": foundation_release_id,
        "plan_id": plan_id,
        "schema_version": CATALOG_SCHEMA,
        "semantic_bindings": dict(sorted(semantic_bindings.items())),
    }
    return {**core, "catalog_sha256": sha256_json(core)}


def validate_catalog(
    catalog: Mapping[str, object], *, verify_self_hash: bool = True
) -> str:
    expected = {
        "active_view_id",
        "catalog_sha256",
        "disposition_counts",
        "entries",
        "foundation_manifest_sha256",
        "foundation_release_id",
        "plan_id",
        "schema_version",
        "semantic_bindings",
    }
    if (
        set(catalog) != expected
        or catalog.get("schema_version") != CATALOG_SCHEMA
        or not isinstance(catalog.get("entries"), list)
        or not catalog["entries"]
        or not isinstance(catalog.get("semantic_bindings"), dict)
        or not isinstance(catalog.get("disposition_counts"), dict)
    ):
        raise IntegrityError("active catalog schema is invalid")
    for key in (
        "active_view_id",
        "catalog_sha256",
        "foundation_manifest_sha256",
        "foundation_release_id",
        "plan_id",
    ):
        _plain_sha256(catalog.get(key), f"catalog {key}")
    if verify_self_hash:
        core = {key: value for key, value in catalog.items() if key != "catalog_sha256"}
        if catalog["catalog_sha256"] != sha256_json(core):
            raise IntegrityError("active catalog self-hash is invalid")
    entries = catalog["entries"]
    keys: list[str] = []
    counts: dict[str, int] = {}
    for raw in entries:
        if not isinstance(raw, dict):
            raise IntegrityError("active catalog entry is not an object")
        market = _plain_market(raw.get("market"))
        year = _plain_year(raw.get("year"))
        key = f"{market}/{year}"
        keys.append(key)
        expected_parquet, expected_sidecar = _canonical_target(market, year)
        disposition = raw.get("disposition")
        cohort = cohort_for_year(year)
        if (
            raw.get("cohort") != cohort
            or raw.get("selection_eligible")
            is not selection_eligible(cohort=cohort, disposition=str(disposition))
        ):
            raise IntegrityError("active catalog cohort or permission is invalid")
        if disposition == CERTIFICATION_STATE:
            for name in (
                "access_policy_binding_id",
                "content_validation_receipt_id",
                "parquet_sha256",
                "schema_fingerprint",
                "sidecar_sha256",
            ):
                _plain_sha256(raw.get(name), f"catalog entry {name}")
            if (
                raw.get("parquet_path") != expected_parquet
                or raw.get("sidecar_path") != expected_sidecar
                or raw.get("reason") is not None
                or raw.get("permitted_uses") != list(COHORT_PERMISSIONS[cohort])
                or isinstance(raw.get("row_count"), bool)
                or not isinstance(raw.get("row_count"), int)
                or raw["row_count"] <= 0
            ):
                raise IntegrityError("materialized catalog entry is incomplete")
        elif (
            disposition not in NON_MATERIALIZED_DISPOSITIONS
            or raw.get("parquet_path") is not None
            or raw.get("sidecar_path") is not None
            or not raw.get("reason")
            or raw.get("permitted_uses") != []
        ):
            raise IntegrityError("non-materialized catalog entry is invalid")
        counts[str(disposition)] = counts.get(str(disposition), 0) + 1
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise IntegrityError("active catalog entries are not unique and ordered")
    if dict(sorted(counts.items())) != catalog["disposition_counts"]:
        raise IntegrityError("active catalog disposition census differs")
    return str(catalog["catalog_sha256"])


def _assert_exact_tree(root: Path, expected_files: set[str]) -> None:
    if not root.is_dir() or is_linklike(root):
        raise IntegrityError("active-view tree is absent or link-like")
    actual: set[str] = set()
    for path in root.rglob("*"):
        if is_linklike(path):
            raise IntegrityError(f"active-view tree contains a link: {path}")
        if path.is_file():
            assert_plain_file(path)
            actual.add(path.relative_to(root).as_posix())
        elif not path.is_dir():
            raise IntegrityError(f"active-view tree contains an unknown object: {path}")
    if actual != expected_files:
        raise IntegrityError("active-view tree contains missing or extra files")


def materialize_parquet(
    *,
    sources: Sequence[Path],
    source_sha256s: Sequence[str],
    destination: Path,
    expected_row_count: int,
    expected_schema_fingerprint: str,
    batch_rows: int = 100_000,
) -> tuple[str, str, int]:
    """Create one certified market-year Parquet without semantic transformation."""

    if (
        not sources
        or len(sources) != len(source_sha256s)
        or isinstance(expected_row_count, bool)
        or not isinstance(expected_row_count, int)
        or expected_row_count <= 0
        or isinstance(batch_rows, bool)
        or not isinstance(batch_rows, int)
        or not 1 <= batch_rows <= 1_000_000
    ):
        raise ContractError("materialization bounds or sources are invalid")
    _plain_sha256(expected_schema_fingerprint, "expected schema fingerprint")
    for path, expected_hash in zip(sources, source_sha256s):
        _plain_sha256(expected_hash, "source Parquet hash")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("selected causal source changed before materialization")
    assert_no_linklike_ancestors(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise IntegrityError("materialization destination already exists")
    required_bytes = sum(path.stat().st_size for path in sources)
    available_bytes = shutil.disk_usage(destination.parent).free
    if available_bytes < required_bytes + 67_108_864:
        raise IntegrityError("insufficient disk for bounded materialization")
    if len(sources) == 1:
        descriptor = os.open(
            destination,
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0),
        )
        try:
            with sources[0].open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    os.write(descriptor, chunk)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq

        first = pq.ParquetFile(sources[0])
        schema = first.schema_arrow
        writer = pq.ParquetWriter(
            destination,
            schema,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )
        try:
            previous_key: tuple[int, int, int] | None = None
            observed_rows = 0
            for source in sources:
                parquet = pq.ParquetFile(source)
                if not parquet.schema_arrow.equals(schema, check_metadata=True):
                    raise IntegrityError("split causal source schemas differ")
                for batch in parquet.iter_batches(batch_size=batch_rows):
                    names = set(batch.schema.names)
                    required = {"event_at_ns", "publisher_id", "instrument_id"}
                    if not required.issubset(names):
                        raise IntegrityError("causal primary-key columns are absent")
                    rows = batch.select(
                        ["event_at_ns", "publisher_id", "instrument_id"]
                    ).to_pylist()
                    for raw in rows:
                        key = (
                            int(raw["event_at_ns"]),
                            int(raw["publisher_id"]),
                            int(raw["instrument_id"]),
                        )
                        if previous_key is not None and key <= previous_key:
                            raise IntegrityError(
                                "split causal inputs overlap, duplicate, or reverse order"
                            )
                        previous_key = key
                    writer.write_batch(batch, row_group_size=batch.num_rows)
                    observed_rows += batch.num_rows
        finally:
            writer.close()
        if observed_rows != expected_row_count:
            raise IntegrityError("split materialization row count differs")
    from .active_data_certification import canonical_parquet_fingerprint

    fingerprint = canonical_parquet_fingerprint(destination, batch_rows=batch_rows)
    if (
        fingerprint["row_count"] != expected_row_count
        or fingerprint["schema_fingerprint"] != expected_schema_fingerprint
    ):
        raise IntegrityError("materialized Parquet differs from certification")
    with destination.open("r+b") as handle:
        os.fsync(handle.fileno())
    fsync_directory(destination.parent)
    return (
        sha256_file(destination),
        str(fingerprint["canonical_row_hash"]),
        int(fingerprint["row_count"]),
    )


def stage_view(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    foundation_release_id: str,
    foundation_manifest_sha256: str,
    semantic_bindings: Mapping[str, str],
    materializations: Sequence[Mapping[str, object]],
    non_materialized_entries: Sequence[CatalogEntry],
) -> tuple[Path, dict[str, object]]:
    """Create one complete, create-only successor under the exact staging root."""

    root = repository_root.resolve(strict=True)
    plan_id = _plain_sha256(plan.get("plan_id"), "staging plan ID")
    if plan.get("operation") != "MATERIALIZE_CAUSAL_ACTIVE_VIEW":
        raise ContractError("staging plan operation is invalid")
    stage = root / STAGING_ROOT / plan_id
    if stage.exists():
        raise IntegrityError("active-view staging destination already exists")
    stage.mkdir(parents=True)
    active_view_id = sha256_json(
        {
            "content_validation_receipt_ids": sorted(
                _plain_sha256(
                    item.get("content_validation_receipt_id"),
                    "content validation receipt ID",
                )
                for item in materializations
            ),
            "foundation_release_id": foundation_release_id,
            "plan_id": plan_id,
            "semantic_bindings": dict(sorted(semantic_bindings.items())),
        }
    )
    entries: list[CatalogEntry] = list(non_materialized_entries)
    try:
        for raw in sorted(
            materializations, key=lambda item: (str(item.get("market")), int(item.get("year", 0)))
        ):
            market = _plain_market(raw.get("market"))
            year = _plain_year(raw.get("year"))
            content = raw.get("content_validation_receipt")
            if not isinstance(content, dict):
                raise IntegrityError("materialization content receipt is absent")
            content_id = validate_content_validation_receipt(content)
            if content_id != raw.get("content_validation_receipt_id"):
                raise IntegrityError("materialization content receipt binding differs")
            access = build_access_policy_binding(
                market=market,
                year=year,
                universe_contract_sha256=_plain_sha256(
                    raw.get("universe_contract_sha256"), "universe contract hash"
                ),
                active_view_id=active_view_id,
                content_validation_receipt_id=content_id,
            )
            access_id = validate_access_policy_binding(access)
            sources_raw = raw.get("source_paths")
            hashes_raw = raw.get("source_sha256s")
            if (
                not isinstance(sources_raw, list)
                or not isinstance(hashes_raw, list)
                or any(type(value) is not str for value in sources_raw)
                or any(type(value) is not str for value in hashes_raw)
            ):
                raise ContractError("materialization source list is invalid")
            sources = [root / PurePosixPath(value) for value in sources_raw]
            relative = (
                Path("causally_gated_normalized")
                / market
                / str(year)
                / f"{year}.parquet"
            )
            destination = stage / relative
            parquet_sha256, canonical_row_hash, row_count = materialize_parquet(
                sources=sources,
                source_sha256s=hashes_raw,
                destination=destination,
                expected_row_count=int(raw["row_count"]),
                expected_schema_fingerprint=_plain_sha256(
                    raw.get("schema_fingerprint"), "schema fingerprint"
                ),
            )
            if canonical_row_hash != content.get("canonical_row_hash"):
                raise IntegrityError("materialized canonical rows differ from certification")
            cohort = cohort_for_year(year)
            provisional = CatalogEntry(
                market=market,
                year=year,
                coverage_start=str(raw["coverage_start"]),
                coverage_end=str(raw["coverage_end"]),
                coverage_kind=str(raw["coverage_kind"]),
                cohort=cohort,
                disposition=CERTIFICATION_STATE,
                selection_eligible=cohort == "DISCOVERY_SELECTION",
                permitted_uses=COHORT_PERMISSIONS[cohort],
                source_bindings=tuple(
                    dict(item)
                    for item in raw.get("source_bindings", ())
                    if isinstance(item, dict)
                ),
                content_validation_receipt_id=content_id,
                access_policy_binding_id=access_id,
                parquet_sha256=parquet_sha256,
                sidecar_sha256="0" * 64,
                row_count=row_count,
                schema_fingerprint=str(raw["schema_fingerprint"]),
            )
            sidecar = build_sidecar(
                entry=provisional,
                content_receipt=content,
                access_binding=access,
            )
            sidecar_path = destination.with_suffix(".parquet.manifest.json")
            _write_new_or_exact(sidecar_path, sidecar)
            entries.append(
                CatalogEntry(
                    **{
                        **provisional.__dict__,
                        "sidecar_sha256": sha256_file(sidecar_path),
                    }
                )
            )
        entries.sort(key=lambda item: (item.market, item.year))
        catalog = build_catalog(
            active_view_id=active_view_id,
            plan_id=plan_id,
            foundation_release_id=foundation_release_id,
            foundation_manifest_sha256=foundation_manifest_sha256,
            semantic_bindings=semantic_bindings,
            entries=entries,
        )
        _write_new_or_exact(stage / "catalog.json", catalog)
        verify_view(stage)
        return stage, catalog
    except Exception:
        # Keep the failed create-only stage as evidence.  It is inert because
        # data/active and its catalog commit marker were never changed.
        raise


def _load_view_catalog(
    root: Path, *, allow_unreferenced_append_files: bool
) -> dict[str, object]:
    active = root.resolve(strict=True)
    catalog = _load_canonical(active / "catalog.json", "active catalog")
    validate_catalog(catalog)
    expected = {"catalog.json"}
    for raw in catalog["entries"]:
        assert isinstance(raw, dict)
        if raw["disposition"] != CERTIFICATION_STATE:
            continue
        parquet_relative = str(raw["parquet_path"]).removeprefix("data/active/")
        sidecar_relative = str(raw["sidecar_path"]).removeprefix("data/active/")
        if parquet_relative.startswith("/") or sidecar_relative.startswith("/"):
            raise IntegrityError("catalog paths are outside the active root")
        expected.update({parquet_relative, sidecar_relative})
    if allow_unreferenced_append_files:
        actual: set[str] = set()
        for path in active.rglob("*"):
            if is_linklike(path):
                raise IntegrityError(f"active-view tree contains a link: {path}")
            if path.is_file():
                assert_plain_file(path)
                actual.add(path.relative_to(active).as_posix())
            elif not path.is_dir():
                raise IntegrityError(f"active-view tree contains an unknown object: {path}")
        extra = actual - expected
        for relative in extra:
            parts = PurePosixPath(relative).parts
            if (
                len(parts) != 4
                or parts[0] != "causally_gated_normalized"
                or _MARKET.fullmatch(parts[1]) is None
                or not parts[2].isdigit()
                or int(parts[2]) not in _YEAR
                or parts[3]
                not in {
                    f"{parts[2]}.parquet",
                    f"{parts[2]}.parquet.manifest.json",
                }
            ):
                raise IntegrityError("active view contains a noncanonical unreferenced file")
        if not expected.issubset(actual):
            raise IntegrityError("active view lacks a catalog-referenced file")
    else:
        _assert_exact_tree(active, expected)
    return catalog


def verify_view(
    root: Path, *, allow_unreferenced_append_files: bool = False
) -> dict[str, object]:
    active = root.resolve(strict=True)
    catalog = _load_view_catalog(
        active,
        allow_unreferenced_append_files=allow_unreferenced_append_files,
    )
    for raw in catalog["entries"]:
        assert isinstance(raw, dict)
        if raw["disposition"] != CERTIFICATION_STATE:
            continue
        parquet_relative = str(raw["parquet_path"]).removeprefix("data/active/")
        sidecar_relative = str(raw["sidecar_path"]).removeprefix("data/active/")
        parquet = active / PurePosixPath(parquet_relative)
        sidecar = active / PurePosixPath(sidecar_relative)
        if sha256_file(parquet) != raw["parquet_sha256"]:
            raise IntegrityError(f"active Parquet hash differs: {raw['market']}/{raw['year']}")
        if sha256_file(sidecar) != raw["sidecar_sha256"]:
            raise IntegrityError(f"active sidecar hash differs: {raw['market']}/{raw['year']}")
        sidecar_payload = _load_canonical(sidecar, "active market-year sidecar")
        catalog_entry_binding = dict(raw)
        catalog_entry_binding.pop("sidecar_sha256")
        if (
            sidecar_payload.get("schema_version") != SIDECAR_SCHEMA
            or sidecar_payload.get("sidecar_id")
            != sha256_json(
                {
                    key: value
                    for key, value in sidecar_payload.items()
                    if key != "sidecar_id"
                }
            )
            or sidecar_payload.get("entry_binding") != catalog_entry_binding
        ):
            raise IntegrityError("active sidecar differs from the catalog")
        validate_content_validation_receipt(
            sidecar_payload.get("content_validation_receipt", {})
        )
        validate_access_policy_binding(sidecar_payload.get("access_policy_binding", {}))
    return catalog


def _journal_path(repository_root: Path, plan_id: str) -> Path:
    _plain_sha256(plan_id, "publication plan ID")
    return repository_root / PUBLICATION_JOURNAL_ROOT / plan_id / "journal.json"


def _require_same_volume(*paths: Path) -> None:
    devices = {path.stat().st_dev for path in paths}
    if len(devices) != 1:
        raise IntegrityError("transactional publication paths are not on one volume")


def _write_journal(
    path: Path,
    *,
    plan_id: str,
    approval_receipt_id: str,
    mode: UpdateMode,
    state: str,
    active_view_id: str,
    staging_path: str,
    rollback_path: str | None,
) -> dict[str, object]:
    core: dict[str, object] = {
        "active_view_id": active_view_id,
        "approval_receipt_id": approval_receipt_id,
        "plan_id": plan_id,
        "rollback_path": rollback_path,
        "schema_version": JOURNAL_SCHEMA,
        "staging_path": staging_path,
        "state": state,
        "update_mode": mode.value,
    }
    payload = {**core, "journal_id": sha256_json(core)}
    if path.exists():
        current = _load_canonical(path, "publication journal")
        allowed = {
            "INTENT": {"ACTIVE_PROMOTED", "COMMITTED", "ROLLED_BACK"},
            "ACTIVE_PROMOTED": {"COMMITTED", "ROLLED_BACK"},
            "COMMITTED": set(),
            "ROLLED_BACK": set(),
        }
        if (
            current.get("schema_version") != JOURNAL_SCHEMA
            or current.get("journal_id")
            != sha256_json({k: v for k, v in current.items() if k != "journal_id"})
            or state not in allowed.get(str(current.get("state")), set())
        ):
            raise IntegrityError("publication journal transition is invalid")
        temporary = path.with_suffix(".next")
        _write_new_or_exact(temporary, payload)
        os.replace(temporary, path)
        fsync_directory(path.parent)
    else:
        _write_new_or_exact(path, payload)
    return payload


def publish_initial(
    *,
    repository_root: Path,
    staging: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    boundary.assert_active_path(root / ACTIVE_ROOT / "_probe", purpose="active publication")
    plan_id = _plain_sha256(plan.get("plan_id"), "publication plan ID")
    approval_id = verify_approval(
        approval, plan, expected_operation="PUBLISH_CAUSAL_ACTIVE_VIEW"
    )
    if plan.get("update_mode") != UpdateMode.INITIAL.value:
        raise ContractError("initial publisher requires INITIAL mode")
    active = root / ACTIVE_ROOT
    if active.exists():
        raise IntegrityError("initial active root already exists")
    stage = staging.resolve(strict=True)
    expected_stage_parent = (root / STAGING_ROOT / plan_id).resolve(strict=False)
    if stage != expected_stage_parent or is_linklike(stage):
        raise ContractError("publication stage differs from the exact plan stage")
    staged_catalog = verify_view(stage)
    active_view_id = _plain_sha256(
        staged_catalog.get("active_view_id"), "active view ID"
    )
    journal = _journal_path(root, plan_id)
    active.parent.mkdir(parents=True, exist_ok=True)
    _require_same_volume(stage, active.parent)
    with FileLease(root / PUBLICATION_LOCK):
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.INITIAL,
            state="INTENT",
            active_view_id=active_view_id,
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=None,
        )
        os.replace(stage, active)
        fsync_directory(active.parent)
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.INITIAL,
            state="ACTIVE_PROMOTED",
            active_view_id=active_view_id,
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=None,
        )
        try:
            catalog = verify_view(active)
        except Exception:
            failed = root / FAILED_PUBLICATION_ROOT / plan_id
            failed.parent.mkdir(parents=True, exist_ok=True)
            if failed.exists():
                raise IntegrityError("failed-publication quarantine already exists")
            os.replace(active, failed)
            fsync_directory(active.parent)
            _write_journal(
                journal,
                plan_id=plan_id,
                approval_receipt_id=approval_id,
                mode=UpdateMode.INITIAL,
                state="ROLLED_BACK",
                active_view_id=active_view_id,
                staging_path=stage.relative_to(root).as_posix(),
                rollback_path=failed.relative_to(root).as_posix(),
            )
            raise
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.INITIAL,
            state="COMMITTED",
            active_view_id=active_view_id,
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=None,
        )
    core: dict[str, object] = {
        "active_view_id": active_view_id,
        "approval_receipt_id": approval_id,
        "catalog_sha256": catalog["catalog_sha256"],
        "journal_path": journal.relative_to(root).as_posix(),
        "plan_id": plan_id,
        "schema_version": PUBLICATION_RECEIPT_SCHEMA,
        "state": "PUBLISHED_VERIFIED",
    }
    return {**core, "publication_receipt_id": sha256_json(core)}


def publish_full_successor(
    *,
    repository_root: Path,
    staging: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan_id = _plain_sha256(plan.get("plan_id"), "publication plan ID")
    approval_id = verify_approval(
        approval, plan, expected_operation="PUBLISH_CAUSAL_ACTIVE_VIEW"
    )
    if plan.get("update_mode") != UpdateMode.FULL_SUCCESSOR.value:
        raise ContractError("full successor publisher requires FULL_SUCCESSOR mode")
    active = root / ACTIVE_ROOT
    current = verify_view(active)
    stage = staging.resolve(strict=True)
    if stage != (root / STAGING_ROOT / plan_id).resolve(strict=False):
        raise ContractError("successor stage differs from the exact plan stage")
    successor = verify_view(stage)
    rollback = root / ROLLBACK_ROOT / str(current["active_view_id"])
    if rollback.exists():
        raise IntegrityError("full-successor rollback destination already exists")
    journal = _journal_path(root, plan_id)
    rollback.parent.mkdir(parents=True, exist_ok=True)
    _require_same_volume(active, stage, rollback.parent)
    with FileLease(root / PUBLICATION_LOCK):
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.FULL_SUCCESSOR,
            state="INTENT",
            active_view_id=str(successor["active_view_id"]),
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=rollback.relative_to(root).as_posix(),
        )
        os.replace(active, rollback)
        try:
            os.replace(stage, active)
            verify_view(active)
        except Exception:
            if active.exists():
                failed = root / FAILED_PUBLICATION_ROOT / plan_id
                failed.parent.mkdir(parents=True, exist_ok=True)
                if failed.exists():
                    raise IntegrityError("failed successor quarantine already exists")
                os.replace(active, failed)
            os.replace(rollback, active)
            verify_view(active)
            _write_journal(
                journal,
                plan_id=plan_id,
                approval_receipt_id=approval_id,
                mode=UpdateMode.FULL_SUCCESSOR,
                state="ROLLED_BACK",
                active_view_id=str(successor["active_view_id"]),
                staging_path=stage.relative_to(root).as_posix(),
                rollback_path=rollback.relative_to(root).as_posix(),
            )
            raise
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.FULL_SUCCESSOR,
            state="COMMITTED",
            active_view_id=str(successor["active_view_id"]),
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=rollback.relative_to(root).as_posix(),
        )
    core: dict[str, object] = {
        "active_view_id": successor["active_view_id"],
        "approval_receipt_id": approval_id,
        "catalog_sha256": successor["catalog_sha256"],
        "journal_path": journal.relative_to(root).as_posix(),
        "plan_id": plan_id,
        "rollback_path": rollback.relative_to(root).as_posix(),
        "schema_version": PUBLICATION_RECEIPT_SCHEMA,
        "state": "PUBLISHED_VERIFIED",
    }
    return {**core, "publication_receipt_id": sha256_json(core)}


def publish_append_only(
    *,
    repository_root: Path,
    staging: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan_id = _plain_sha256(plan.get("plan_id"), "publication plan ID")
    approval_id = verify_approval(
        approval, plan, expected_operation="PUBLISH_CAUSAL_ACTIVE_VIEW"
    )
    if plan.get("update_mode") != UpdateMode.APPEND_ONLY.value:
        raise ContractError("append publisher requires APPEND_ONLY mode")
    active = root / ACTIVE_ROOT
    current = verify_view(active)
    stage = staging.resolve(strict=True)
    successor = verify_view(stage)
    if stage != (root / STAGING_ROOT / plan_id).resolve(strict=False):
        raise ContractError("append stage differs from the exact plan stage")
    mode = classify_update(
        current_catalog=current,
        proposed_entries=successor["entries"],
        current_semantic_bindings=current["semantic_bindings"],
        proposed_semantic_bindings=successor["semantic_bindings"],
    )
    if mode is not UpdateMode.APPEND_ONLY:
        raise IntegrityError("staged successor is not append-only")
    current_keys = {
        f"{item['market']}/{item['year']}" for item in current["entries"]
    }
    new_entries = [
        item
        for item in successor["entries"]
        if f"{item['market']}/{item['year']}" not in current_keys
        and item["disposition"] == CERTIFICATION_STATE
    ]
    promoted: list[Path] = []
    journal = _journal_path(root, plan_id)
    _require_same_volume(active, stage)
    with FileLease(root / PUBLICATION_LOCK):
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.APPEND_ONLY,
            state="INTENT",
            active_view_id=str(successor["active_view_id"]),
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=None,
        )
        for entry in new_entries:
            relative = (
                PurePosixPath(str(entry["parquet_path"]))
                .parent.relative_to(PurePosixPath("data/active"))
            )
            source_dir = stage / relative
            target_dir = active / relative
            if target_dir.exists():
                raise IntegrityError("append-only destination already exists")
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source_dir, target_dir)
            promoted.append(target_dir)
        catalog_next = active / "catalog.json.next"
        _write_new_or_exact(catalog_next, successor)
        os.replace(catalog_next, active / "catalog.json")
        fsync_directory(active)
        verify_view(active)
        _write_journal(
            journal,
            plan_id=plan_id,
            approval_receipt_id=approval_id,
            mode=UpdateMode.APPEND_ONLY,
            state="COMMITTED",
            active_view_id=str(successor["active_view_id"]),
            staging_path=stage.relative_to(root).as_posix(),
            rollback_path=None,
        )
    core: dict[str, object] = {
        "active_view_id": successor["active_view_id"],
        "approval_receipt_id": approval_id,
        "catalog_sha256": successor["catalog_sha256"],
        "journal_path": journal.relative_to(root).as_posix(),
        "plan_id": plan_id,
        "promoted_market_years": len(promoted),
        "schema_version": PUBLICATION_RECEIPT_SCHEMA,
        "state": "PUBLISHED_VERIFIED",
    }
    return {**core, "publication_receipt_id": sha256_json(core)}


def recover_publication(repository_root: Path, plan_id: str) -> str:
    root = repository_root.resolve(strict=True)
    journal_path = _journal_path(root, plan_id)
    journal = _load_canonical(journal_path, "publication journal")
    if journal.get("state") == "COMMITTED":
        verify_view(root / ACTIVE_ROOT)
        return "LAST_KNOWN_GOOD_VERIFIED"
    if journal.get("state") == "ROLLED_BACK":
        if (root / ACTIVE_ROOT).exists():
            verify_view(root / ACTIVE_ROOT)
            return "ROLLBACK_VERIFIED"
        return "AUTHORITATIVE_ABSENT_VERIFIED"
    raise UnauthorizedOperation(
        "incomplete publication requires exact recovery approval and reviewed paths"
    )


def verify_plan_bindings(
    repository_root: Path, plan: Mapping[str, object]
) -> None:
    root = repository_root.resolve(strict=True)
    plan_id = _plain_sha256(plan.get("plan_id"), "active-view plan ID")
    if plan_id != sha256_json({key: value for key, value in plan.items() if key != "plan_id"}):
        raise IntegrityError("active-view plan self-hash is invalid")
    for group_name in (
        "environment_bindings",
        "implementation_bindings",
        "semantic_bindings",
    ):
        group = plan.get(group_name)
        if not isinstance(group, dict) or not group:
            raise IntegrityError(f"active-view {group_name} are absent")
        for relative, expected in group.items():
            _plain_sha256(expected, f"{group_name} hash")
            if not isinstance(relative, str) or not relative:
                raise IntegrityError(f"active-view {group_name} path is invalid")
            candidate = root / PurePosixPath(relative)
            if candidate.is_file() and sha256_file(candidate) != expected:
                raise IntegrityError(f"active-view binding changed: {relative}")
            if (
                "/" in relative
                or relative.endswith((".json", ".lock", ".py", ".toml", ".yaml"))
            ) and not candidate.is_file():
                raise IntegrityError(f"active-view bound file is absent: {relative}")


def resolve(
    *,
    repository_root: Path,
    market: str,
    year: int,
    purpose: str,
    require_status: bool = False,
) -> Path:
    root = repository_root.resolve(strict=True)
    market = _plain_market(market)
    year = _plain_year(year)
    if not purpose:
        raise ContractError("resolver purpose is absent")
    if year < 2025 and require_status:
        raise UnauthorizedOperation("pre-2025 status-dependent use is forbidden")
    active = root / ACTIVE_ROOT
    catalog = _load_view_catalog(
        active, allow_unreferenced_append_files=True
    )
    matches = [
        item
        for item in catalog["entries"]
        if item["market"] == market and item["year"] == year
    ]
    if len(matches) != 1:
        raise IntegrityError("market-year is absent or duplicated in the active catalog")
    entry = matches[0]
    if entry["disposition"] in PROTECTED_DISPOSITIONS:
        raise UnauthorizedOperation("protected market-year payload is not materialized")
    if entry["disposition"] == "QUARANTINED_NOT_MATERIALIZED":
        raise IntegrityError("market-year is quarantined")
    if purpose not in entry["permitted_uses"]:
        raise UnauthorizedOperation("requested purpose is not permitted for this cohort")
    if purpose == "SELECTION" and entry["selection_eligible"] is not True:
        raise UnauthorizedOperation("only the discovery cohort permits selection")
    relative = PurePosixPath(str(entry["parquet_path"])).relative_to(
        PurePosixPath("data/active")
    )
    path = active / relative
    sidecar_relative = PurePosixPath(str(entry["sidecar_path"])).relative_to(
        PurePosixPath("data/active")
    )
    sidecar = active / sidecar_relative
    if sha256_file(sidecar) != entry["sidecar_sha256"]:
        raise IntegrityError("resolved sidecar changed after catalog publication")
    sidecar_payload = _load_canonical(sidecar, "resolved market-year sidecar")
    catalog_entry_binding = dict(entry)
    catalog_entry_binding.pop("sidecar_sha256")
    if (
        sidecar_payload.get("schema_version") != SIDECAR_SCHEMA
        or sidecar_payload.get("sidecar_id")
        != sha256_json(
            {
                key: value
                for key, value in sidecar_payload.items()
                if key != "sidecar_id"
            }
        )
        or sidecar_payload.get("entry_binding") != catalog_entry_binding
        or validate_content_validation_receipt(
            sidecar_payload.get("content_validation_receipt", {})
        )
        != entry["content_validation_receipt_id"]
        or validate_access_policy_binding(
            sidecar_payload.get("access_policy_binding", {})
        )
        != entry["access_policy_binding_id"]
    ):
        raise IntegrityError("resolved sidecar certification bindings differ")
    if sha256_file(path) != entry["parquet_sha256"]:
        raise IntegrityError("resolved Parquet changed after active-view verification")
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    policy_plan_command = commands.add_parser("policy-plan")
    policy_plan_command.add_argument("--foundation-release-id", required=True)
    policy_plan_command.add_argument("--output", type=Path, required=True)
    policy_plan_command.add_argument("--approval-output", type=Path, required=True)
    policy_publish_command = commands.add_parser("policy-publish")
    policy_publish_command.add_argument("--plan", type=Path, required=True)
    policy_publish_command.add_argument("--approval", type=Path, required=True)
    plan_command = commands.add_parser("plan")
    plan_command.add_argument("--foundation-release-id", required=True)
    plan_command.add_argument("--accepted-policy-release-id", required=True)
    plan_command.add_argument("--policy-acceptance-receipt-id", required=True)
    plan_command.add_argument("--output", type=Path, required=True)
    plan_command.add_argument("--approval-output", type=Path, required=True)
    plan_command.add_argument("--supersession-output", type=Path, required=True)
    pilot_plan_command = commands.add_parser("pilot-plan")
    pilot_plan_command.add_argument("--foundation-release-id", required=True)
    pilot_plan_command.add_argument("--accepted-policy-release-id", required=True)
    pilot_plan_command.add_argument(
        "--policy-acceptance-receipt-id", required=True
    )
    pilot_plan_command.add_argument("--output", type=Path, required=True)
    pilot_plan_command.add_argument("--approval-output", type=Path, required=True)
    supersede_command = commands.add_parser("supersede-plan")
    supersede_command.add_argument("--predecessor", type=Path, required=True)
    supersede_command.add_argument("--successor", type=Path, required=True)
    supersede_command.add_argument("--output", type=Path, required=True)
    certify_command = commands.add_parser("certify")
    certify_command.add_argument("--plan", type=Path, required=True)
    certify_command.add_argument("--approval", type=Path, required=True)
    certify_command.add_argument("--market", required=True)
    certify_command.add_argument("--year", type=int, required=True)
    certify_command.add_argument("--run-id", required=True)
    certify_command.add_argument("--batch-rows", type=int, default=100_000)
    materialize_command = commands.add_parser("materialize")
    materialize_command.add_argument("--plan", type=Path, required=True)
    materialize_command.add_argument("--approval", type=Path, required=True)
    materialize_command.add_argument(
        "--materialization-input", type=Path, required=True
    )
    publish_command = commands.add_parser("publish")
    publish_command.add_argument("--plan", type=Path, required=True)
    publish_command.add_argument("--approval", type=Path, required=True)
    publish_command.add_argument("--staging", type=Path, required=True)
    verify_command = commands.add_parser("verify")
    verify_command.add_argument(
        "--root", type=Path, default=ACTIVE_ROOT
    )
    resolve_command = commands.add_parser("resolve")
    resolve_command.add_argument("--market", required=True)
    resolve_command.add_argument("--year", type=int, required=True)
    resolve_command.add_argument("--purpose", required=True)
    resolve_command.add_argument("--require-status", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    require_locked_repository_environment(root)
    verify_contract(root)
    if args.command == "policy-plan":
        from .active_data_plan import (
            build_policy_pending_approval,
            build_policy_successor_plan,
        )

        plan = build_policy_successor_plan(
            repository_root=root,
            foundation_release_id=args.foundation_release_id,
        )
        approval = build_policy_pending_approval(plan)
        output = args.output if args.output.is_absolute() else root / args.output
        approval_output = (
            args.approval_output
            if args.approval_output.is_absolute()
            else root / args.approval_output
        )
        _write_new_or_exact(output, plan)
        _write_new_or_exact(approval_output, approval)
        print(canonical_bytes({"plan_id": plan["plan_id"], "status": "PENDING"}).decode())
        return 0
    if args.command == "policy-publish":
        from .active_data_plan import publish_policy_successor

        plan_path = args.plan if args.plan.is_absolute() else root / args.plan
        approval_path = (
            args.approval if args.approval.is_absolute() else root / args.approval
        )
        plan = _load_canonical(plan_path, "price-policy successor plan")
        approval = _load_canonical(approval_path, "price-policy acceptance")
        manifest_path, receipt = publish_policy_successor(
            repository_root=root,
            plan=plan,
            approval=approval,
        )
        print(
            canonical_bytes(
                {
                    "manifest_path": manifest_path.relative_to(root).as_posix(),
                    "policy_acceptance_receipt_id": receipt[
                        "policy_acceptance_receipt_id"
                    ],
                    "policy_release_id": receipt["policy_release_id"],
                    "status": "ACCEPTED_NON_AUTHORIZING",
                }
            ).decode()
        )
        return 0
    if args.command == "plan":
        from .active_data_plan import (
            build_dry_run_plan,
            build_supersession_record,
        )

        plan, approval = build_dry_run_plan(
            repository_root=root,
            foundation_release_id=args.foundation_release_id,
            accepted_policy_release_id=args.accepted_policy_release_id,
            policy_acceptance_receipt_id=args.policy_acceptance_receipt_id,
        )
        supersession = build_supersession_record(
            repository_root=root,
            predecessor_plan_path="configs/causal_market_year_materialization_plan.json",
            successor_plan=plan,
        )
        output = args.output if args.output.is_absolute() else root / args.output
        approval_output = (
            args.approval_output
            if args.approval_output.is_absolute()
            else root / args.approval_output
        )
        supersession_output = (
            args.supersession_output
            if args.supersession_output.is_absolute()
            else root / args.supersession_output
        )
        _write_new_or_exact(output, plan)
        _write_new_or_exact(approval_output, approval)
        _write_new_or_exact(supersession_output, supersession)
        print(canonical_bytes({"plan_id": plan["plan_id"], "status": "PENDING"}).decode())
        return 0
    if args.command == "pilot-plan":
        from .active_data_plan import build_pilot_plan

        plan, approval = build_pilot_plan(
            repository_root=root,
            foundation_release_id=args.foundation_release_id,
            accepted_policy_release_id=args.accepted_policy_release_id,
            policy_acceptance_receipt_id=args.policy_acceptance_receipt_id,
        )
        output = args.output if args.output.is_absolute() else root / args.output
        approval_output = (
            args.approval_output
            if args.approval_output.is_absolute()
            else root / args.approval_output
        )
        _write_new_or_exact(output, plan)
        _write_new_or_exact(approval_output, approval)
        print(
            canonical_bytes(
                {
                    "pilot_scope_id": plan["pilot_scope_id"],
                    "plan_id": plan["plan_id"],
                    "status": "PENDING",
                }
            ).decode()
        )
        return 0
    if args.command == "supersede-plan":
        from .active_data_plan import build_supersession_record

        predecessor = (
            args.predecessor
            if args.predecessor.is_absolute()
            else args.predecessor.as_posix()
        )
        if isinstance(predecessor, Path):
            predecessor = predecessor.resolve(strict=True).relative_to(root).as_posix()
        successor_path = (
            args.successor
            if args.successor.is_absolute()
            else root / args.successor
        )
        successor = _load_canonical(successor_path, "successor active-view plan")
        record = build_supersession_record(
            repository_root=root,
            predecessor_plan_path=str(predecessor),
            successor_plan=successor,
        )
        output = args.output if args.output.is_absolute() else root / args.output
        _write_new_or_exact(output, record)
        print(
            canonical_bytes(
                {
                    "status": record["state"],
                    "supersession_id": record["supersession_id"],
                }
            ).decode()
        )
        return 0
    if args.command == "certify":
        from .active_data_certification import certify_market_year

        plan_path = args.plan if args.plan.is_absolute() else root / args.plan
        approval_path = (
            args.approval if args.approval.is_absolute() else root / args.approval
        )
        plan = _load_canonical(plan_path, "certification plan")
        approval = _load_canonical(approval_path, "certification approval")
        verify_plan_bindings(root, plan)
        verify_approval(
            approval, plan, expected_operation="CERTIFY_CAUSAL_ACTIVE_VIEW"
        )
        market = _plain_market(args.market)
        year = _plain_year(args.year)
        _plain_sha256(args.run_id, "certification run ID")
        if (root / ACTIVE_ROOT).exists():
            raise IntegrityError("certification requires the active root to remain absent")
        certification_state_root, workspace = _resolve_certification_workspace(
            repository_root=root,
            plan=plan,
            run_id=args.run_id,
            market=market,
            year=year,
        )
        source_objects = plan.get("source_objects")
        if source_objects is not None:
            if not isinstance(source_objects, list):
                raise IntegrityError("certification source-object inventory is invalid")
            observed_bytes = 0
            for item in source_objects:
                if (
                    not isinstance(item, dict)
                    or type(item.get("path")) is not str
                    or type(item.get("size")) is not int
                ):
                    raise IntegrityError("certification source object is invalid")
                path = root / PurePosixPath(str(item["path"]))
                assert_plain_file(path)
                if (
                    path.stat().st_size != item["size"]
                    or sha256_file(path) != item.get("sha256")
                ):
                    raise IntegrityError(
                        f"certification source object changed: {item['path']}"
                    )
                observed_bytes += int(item["size"])
            limits = plan.get("limits")
            if (
                not isinstance(limits, dict)
                or observed_bytes != limits.get("maximum_source_bytes")
                or len(source_objects) != limits.get("maximum_source_files")
            ):
                raise IntegrityError("certification source ceilings differ from the plan")
            certification_state_root.mkdir(parents=True, exist_ok=True)
            if shutil.disk_usage(certification_state_root).free < int(
                limits["maximum_temporary_bytes"]
            ):
                raise IntegrityError("insufficient disk for approved certification")
        matches = [
            item
            for item in plan.get("entries", ())
            if isinstance(item, dict)
            and item.get("market") == market
            and item.get("year") == year
        ]
        if len(matches) != 1 or matches[0].get("disposition") != CERTIFICATION_STATE:
            raise UnauthorizedOperation("market-year is not an approved certification candidate")
        report, receipt = certify_market_year(
            boundary=RepoBoundary(active_root=root),
            foundation_release_id=str(plan["foundation_release_id"]),
            foundation_manifest_sha256=str(plan["foundation_manifest_sha256"]),
            foundation_intervals=matches[0]["intervals"],
            workspace=workspace,
            semantic_bindings=plan["semantic_bindings"],
            implementation_bindings=plan["implementation_bindings"],
            environment_bindings=plan["environment_bindings"],
            batch_rows=args.batch_rows,
        )
        _write_new_or_exact(workspace / "certification_report.json", report)
        _write_new_or_exact(workspace / "content_validation_receipt.json", receipt)
        print(
            canonical_bytes(
                {
                    "certification_report_id": report["certification_report_id"],
                    "content_validation_receipt_id": receipt[
                        "content_validation_receipt_id"
                    ],
                    "status": "PASS",
                }
            ).decode()
        )
        return 0
    if args.command == "materialize":
        plan_path = args.plan if args.plan.is_absolute() else root / args.plan
        approval_path = (
            args.approval if args.approval.is_absolute() else root / args.approval
        )
        input_path = (
            args.materialization_input
            if args.materialization_input.is_absolute()
            else root / args.materialization_input
        )
        plan = _load_canonical(plan_path, "materialization plan")
        approval = _load_canonical(approval_path, "materialization approval")
        materialization_input = _load_canonical(
            input_path, "materialization input"
        )
        verify_plan_bindings(root, plan)
        verify_approval(
            approval, plan, expected_operation="MATERIALIZE_CAUSAL_ACTIVE_VIEW"
        )
        non_materialized_raw = materialization_input.get("non_materialized_entries")
        materializations = materialization_input.get("materializations")
        if not isinstance(non_materialized_raw, list) or not isinstance(
            materializations, list
        ):
            raise IntegrityError("materialization input collections are invalid")
        non_materialized = tuple(
            CatalogEntry(
                market=str(item["market"]),
                year=int(item["year"]),
                coverage_start=str(item["coverage_start"]),
                coverage_end=str(item["coverage_end"]),
                coverage_kind=str(item["coverage_kind"]),
                cohort=str(item["cohort"]),
                disposition=str(item["disposition"]),
                selection_eligible=False,
                permitted_uses=(),
                source_bindings=tuple(item.get("source_bindings", ())),
                reason=str(item["reason"]),
            )
            for item in non_materialized_raw
            if isinstance(item, dict)
        )
        stage, catalog = stage_view(
            repository_root=root,
            plan=plan,
            foundation_release_id=str(plan["foundation_release_id"]),
            foundation_manifest_sha256=str(plan["foundation_manifest_sha256"]),
            semantic_bindings=plan["semantic_bindings"],
            materializations=materializations,
            non_materialized_entries=non_materialized,
        )
        print(
            canonical_bytes(
                {
                    "catalog_sha256": catalog["catalog_sha256"],
                    "staging": stage.relative_to(root).as_posix(),
                    "status": "PASS",
                }
            ).decode()
        )
        return 0
    if args.command == "publish":
        plan_path = args.plan if args.plan.is_absolute() else root / args.plan
        approval_path = (
            args.approval if args.approval.is_absolute() else root / args.approval
        )
        stage = args.staging if args.staging.is_absolute() else root / args.staging
        plan = _load_canonical(plan_path, "publication plan")
        approval = _load_canonical(approval_path, "publication approval")
        verify_plan_bindings(root, plan)
        mode = UpdateMode(str(plan.get("update_mode")))
        if mode is UpdateMode.INITIAL:
            receipt = publish_initial(
                repository_root=root,
                staging=stage,
                plan=plan,
                approval=approval,
            )
        elif mode is UpdateMode.APPEND_ONLY:
            receipt = publish_append_only(
                repository_root=root,
                staging=stage,
                plan=plan,
                approval=approval,
            )
        else:
            receipt = publish_full_successor(
                repository_root=root,
                staging=stage,
                plan=plan,
                approval=approval,
            )
        print(canonical_bytes(receipt).decode())
        return 0
    if args.command == "verify":
        target = args.root
        if not target.is_absolute():
            target = root / target
        catalog = verify_view(target)
        print(canonical_bytes({"catalog_sha256": catalog["catalog_sha256"], "status": "PASS"}).decode())
        return 0
    path = resolve(
        repository_root=root,
        market=args.market,
        year=args.year,
        purpose=args.purpose,
        require_status=args.require_status,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
