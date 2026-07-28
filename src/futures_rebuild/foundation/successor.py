"""Bounded, restart-safe finalization of the 41-market foundation successor.

The finalizer reuses the immutable 33-market component under its historical
policy receipt and rebuilds exactly the dependency-closed eight-market scope.
It performs no provider, alpha, prediction, WFA, label, or holdout operation.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping

from ..boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from ..canonical import canonical_bytes, sha256_json
from ..data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
)
from ..errors import ContractError, IntegrityError
from ..locking import FileLease
from ..producer_bridge import (
    CausalFeatureSpec,
    load_versioned_session_policy,
    publish_versioned_session_policy,
)
from .coverage import StatusResearchScopePolicy
from .market_state import (
    FoundationCoveragePolicy,
    LoadedMarketStateFoundation,
    StatisticsRolePolicy,
    load_market_state_foundation,
    load_status_eligibility,
)
from .materialize import load_causal_interval, load_raw_interval
from .orchestrator import (
    FOUNDATION_SET_RELEASE_KIND,
    FOUNDATION_SUCCESSOR_SCHEMA_VERSION,
    OUTCOME_DEFERRED_UNTIL,
    OUTCOME_SOURCE_ROLE,
    _atomic_checkpoint,
    _checkpoint_core,
    _checkpoint_payload,
    _interval_key,
    _read_canonical_object,
    _receipt,
    _source_family_coverage_passes,
    FoundationOrchestrator,
    load_feature_source_input,
    load_foundation_set,
    load_outcome_source_input,
)
from .selection import load_source_selection_with_resolution
from .snapshot import PublishedDbnRelease as PublishedSourceSnapshot
from .successor_contract import (
    REBUILT_MARKETS,
    build_foundation_successor_provenance,
    build_policy_successor_contract,
    historical_policy_binding,
)
from .support import VerifiedFoundationPolicies, publish_foundation_policies


SUCCESSOR_CHECKPOINT_VERSION = "foundation_successor_checkpoint/1.0.0"
SUCCESSOR_RUN_CONTRACT_VERSION = "foundation_successor_run/1.0.0"
_INTERVAL_PHASES = {
    "raw",
    "definitions",
    "causal",
    "status_eligibility",
    "economics",
    "feature_input",
    "outcome_source_input",
}


@dataclass(frozen=True)
class FoundationSuccessorResult:
    run_id: str
    checkpoint_path: Path
    rebuilt_interval_count: int
    reused_interval_count: int
    foundation_set_receipt: VerifiedReleaseReceipt | None

    def as_dict(self) -> dict[str, object]:
        status = (
            "COMPLETE_BOUNDED_41_MARKET_FOUNDATION_SUCCESSOR"
            if self.foundation_set_receipt is not None
            else "PAUSED_AT_DURABLE_INTERVAL_BOUNDARY"
        )
        return {
            "checkpoint_path": str(self.checkpoint_path),
            "foundation_set_receipt": (
                self.foundation_set_receipt.as_dict()
                if self.foundation_set_receipt is not None
                else None
            ),
            "provider_call_count": 0,
            "rebuilt_interval_count": self.rebuilt_interval_count,
            "reused_interval_count": self.reused_interval_count,
            "run_id": self.run_id,
            "status": status,
        }


class FoundationSuccessorFinalizer:
    """Finalize one exact 33+8 component foundation with durable checkpoints."""

    def __init__(
        self,
        *,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
        batch_rows: int = 100_000,
    ) -> None:
        if type(batch_rows) is not int or batch_rows <= 0:
            raise ContractError("foundation successor batch_rows must be positive")
        operation_receipt.verify(boundary, operation="PUBLISH_RELEASE")
        if operation_receipt.classification not in {
            OperationClassification.SYNTHETIC_MECHANICS_ONLY,
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        }:
            raise ContractError("foundation successor requires a non-alpha receipt")
        self.boundary = boundary
        self.base = FoundationOrchestrator(
            boundary=boundary,
            operation_receipt=operation_receipt,
            batch_rows=batch_rows,
            allow_legacy_calendar_unbound=True,
        )
        self.publisher = self.base.publisher
        self.batch_rows = batch_rows

    def run(
        self,
        *,
        source_dbn_manifest: Path,
        source_selection_receipt: VerifiedReleaseReceipt,
        predecessor_checkpoint_path: Path,
        expected_predecessor_checkpoint_id: str,
        expected_predecessor_run_id: str,
        feature_spec: CausalFeatureSpec,
        maximum_rebuild_intervals: int,
        invocation_rebuild_interval_budget: int | None = None,
        invocation_assembly_interval_budget: int | None = None,
        after_checkpoint: Callable[[str], None] | None = None,
    ) -> FoundationSuccessorResult:
        global_lock = self.boundary.assert_active_path(
            self.boundary.active_root / "state" / "locks" / "foundation-build.lock",
            purpose="global foundation successor lock",
            subtree="state/locks",
        )
        with FileLease(global_lock):
            return self._run_exclusive(
                source_dbn_manifest=source_dbn_manifest,
                source_selection_receipt=source_selection_receipt,
                predecessor_checkpoint_path=predecessor_checkpoint_path,
                expected_predecessor_checkpoint_id=(
                    expected_predecessor_checkpoint_id
                ),
                expected_predecessor_run_id=expected_predecessor_run_id,
                feature_spec=feature_spec,
                maximum_rebuild_intervals=maximum_rebuild_intervals,
                invocation_rebuild_interval_budget=(
                    invocation_rebuild_interval_budget
                ),
                invocation_assembly_interval_budget=(
                    invocation_assembly_interval_budget
                ),
                after_checkpoint=after_checkpoint,
            )

    def _run_exclusive(
        self,
        *,
        source_dbn_manifest: Path,
        source_selection_receipt: VerifiedReleaseReceipt,
        predecessor_checkpoint_path: Path,
        expected_predecessor_checkpoint_id: str,
        expected_predecessor_run_id: str,
        feature_spec: CausalFeatureSpec,
        maximum_rebuild_intervals: int,
        invocation_rebuild_interval_budget: int | None,
        invocation_assembly_interval_budget: int | None,
        after_checkpoint: Callable[[str], None] | None,
    ) -> FoundationSuccessorResult:
        predecessor_path = self.boundary.assert_active_path(
            predecessor_checkpoint_path,
            purpose="foundation predecessor checkpoint",
            subtree="state/foundation_runs_v2",
        )
        predecessor_payload = _read_canonical_object(
            predecessor_path, description="foundation predecessor checkpoint"
        )
        predecessor = _checkpoint_core(predecessor_payload)
        if (
            predecessor_payload.get("checkpoint_id")
            != expected_predecessor_checkpoint_id
            or predecessor.get("run_id") != expected_predecessor_run_id
            or predecessor.get("status") != "RUNNING"
        ):
            raise IntegrityError("foundation predecessor identity/status is invalid")

        snapshot = PublishedSourceSnapshot.open(
            source_dbn_manifest, boundary=self.boundary
        )
        selection, resolved = load_source_selection_with_resolution(
            source_selection_receipt,
            snapshot=snapshot,
            boundary=self.boundary,
        )
        intervals = resolved.intervals
        markets = frozenset(item.market for item in intervals)
        reused_markets = markets - REBUILT_MARKETS
        rebuild_intervals = tuple(
            item for item in intervals if item.market in REBUILT_MARKETS
        )
        reused_intervals = tuple(
            item for item in intervals if item.market in reused_markets
        )
        if (
            len(markets) != 41
            or len(reused_markets) != 33
            or len(rebuild_intervals) != 118
            or len(reused_intervals) != 565
            or type(maximum_rebuild_intervals) is not int
            or maximum_rebuild_intervals != len(rebuild_intervals)
            or (
                invocation_rebuild_interval_budget is not None
                and (
                    type(invocation_rebuild_interval_budget) is not int
                    or not 1
                    <= invocation_rebuild_interval_budget
                    <= maximum_rebuild_intervals
                )
            )
            or (
                invocation_assembly_interval_budget is not None
                and (
                    type(invocation_assembly_interval_budget) is not int
                    or not 1 <= invocation_assembly_interval_budget <= len(intervals)
                )
            )
        ):
            raise IntegrityError(
                "foundation successor exceeds or differs from its exact 118-interval bound"
            )
        predecessor_completed = predecessor.get("completed")
        if not isinstance(predecessor_completed, dict):
            raise IntegrityError("foundation predecessor completed map is invalid")
        predecessor_interval_states = predecessor_completed.get("intervals")
        if (
            not isinstance(predecessor_interval_states, dict)
            or len(predecessor_interval_states) != len(intervals)
            or any(
                not isinstance(state, dict) or set(state) != _INTERVAL_PHASES
                for state in predecessor_interval_states.values()
            )
        ):
            raise IntegrityError("foundation predecessor interval closure is invalid")

        tracked_feature_spec = CausalFeatureSpec.from_dict(
            _read_canonical_object(
                self.boundary.active_root
                / "configs"
                / "mechanical_feature_spec.json",
                description="foundation feature specification",
            )
        )
        if feature_spec != tracked_feature_spec:
            raise IntegrityError(
                "foundation successor feature specification is not tracked"
            )
        base_contract = self.base._run_contract(
            snapshot=snapshot,
            selection_receipt=source_selection_receipt,
            selection=selection,
            intervals=intervals,
            resolved_selection=resolved,
            feature_spec=feature_spec,
        )
        run_contract = {
            **base_contract,
            "maximum_rebuild_intervals": maximum_rebuild_intervals,
            "predecessor_checkpoint_id": expected_predecessor_checkpoint_id,
            "predecessor_run_id": expected_predecessor_run_id,
            "rebuild_interval_keys": [
                _interval_key(item.market, item.year, item.start, item.end)
                for item in rebuild_intervals
            ],
            "rebuilt_markets": sorted(REBUILT_MARKETS),
            "reused_markets": sorted(reused_markets),
            "run_contract_version": SUCCESSOR_RUN_CONTRACT_VERSION,
        }
        run_id = sha256_json(run_contract)
        run_lock = self.boundary.assert_active_path(
            self.boundary.active_root
            / "state"
            / "locks"
            / f"foundation-successor-{run_id}.lock",
            purpose="foundation successor run lock",
            subtree="state/locks",
        )
        with FileLease(run_lock):
            checkpoint_path, core = self._load_or_initialize_checkpoint(
                run_id=run_id, run_contract=run_contract
            )
            return self._run_locked(
                snapshot=snapshot,
                selection=selection,
                intervals=intervals,
                resolved=resolved,
                source_selection_receipt=source_selection_receipt,
                predecessor_path=predecessor_path,
                predecessor_payload=predecessor_payload,
                predecessor=predecessor,
                predecessor_interval_states=predecessor_interval_states,
                reused_markets=reused_markets,
                feature_spec=feature_spec,
                checkpoint_path=checkpoint_path,
                core=core,
                invocation_rebuild_interval_budget=(
                    invocation_rebuild_interval_budget
                    or maximum_rebuild_intervals
                ),
                invocation_assembly_interval_budget=(
                    invocation_assembly_interval_budget or len(intervals)
                ),
                after_checkpoint=after_checkpoint,
            )

    def _load_or_initialize_checkpoint(
        self, *, run_id: str, run_contract: Mapping[str, object]
    ) -> tuple[Path, dict[str, object]]:
        path = self.boundary.assert_active_path(
            self.boundary.active_root
            / "state"
            / "foundation_successors"
            / run_id
            / "checkpoint.json",
            purpose="foundation successor checkpoint",
            subtree="state/foundation_successors",
        )
        expected = {
            "checkpoint_version": SUCCESSOR_CHECKPOINT_VERSION,
            "completed": {},
            "layout_version": "2.0.0",
            "run_contract": dict(run_contract),
            "run_id": run_id,
            "status": "RUNNING",
        }
        if not path.exists():
            _atomic_checkpoint(path, _checkpoint_payload(expected))
            return path, expected
        payload = _read_canonical_object(
            path, description="foundation successor checkpoint"
        )
        core = _checkpoint_core(payload)
        if (
            core.get("checkpoint_version") != SUCCESSOR_CHECKPOINT_VERSION
            or core.get("layout_version") != "2.0.0"
            or core.get("run_id") != run_id
            or core.get("run_contract") != dict(run_contract)
            or core.get("status") not in {"RUNNING", "COMPLETE"}
            or not isinstance(core.get("completed"), dict)
        ):
            raise IntegrityError(
                "foundation successor checkpoint differs from the exact run"
            )
        return path, core

    def _persist(
        self,
        checkpoint_path: Path,
        core: dict[str, object],
        *,
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> None:
        self.base._persist(
            checkpoint_path,
            core,
            phase=phase,
            after_checkpoint=after_checkpoint,
        )

    def _run_locked(
        self,
        *,
        snapshot: PublishedSourceSnapshot,
        selection: Mapping[str, object],
        intervals: tuple[object, ...],
        resolved: object,
        source_selection_receipt: VerifiedReleaseReceipt,
        predecessor_path: Path,
        predecessor_payload: Mapping[str, object],
        predecessor: Mapping[str, object],
        predecessor_interval_states: Mapping[str, object],
        reused_markets: frozenset[str],
        feature_spec: CausalFeatureSpec,
        checkpoint_path: Path,
        core: dict[str, object],
        invocation_rebuild_interval_budget: int,
        invocation_assembly_interval_budget: int,
        after_checkpoint: Callable[[str], None] | None,
    ) -> FoundationSuccessorResult:
        completed = core["completed"]
        if not isinstance(completed, dict):
            raise IntegrityError("foundation successor completed map is invalid")
        allowed = {
            "source_bound",
            "predecessor_bound",
            "foundation_policy",
            "session_policy",
            "successor_provenance",
            "intervals",
            "assembled_intervals",
            "foundation_set",
        }
        if not set(completed).issubset(allowed):
            raise IntegrityError("foundation successor checkpoint phase is invalid")
        source_bound = {
            "selection_manifest_id": selection["selection_manifest_id"],
            "source_selection_receipt_id": source_selection_receipt.receipt_id,
            "source_dbn_release_id": snapshot.source_release_id,
        }
        predecessor_bound = {
            "checkpoint_id": predecessor_payload["checkpoint_id"],
            "run_id": predecessor["run_id"],
        }
        for name, value in (
            ("source_bound", source_bound),
            ("predecessor_bound", predecessor_bound),
        ):
            if name in completed:
                if completed[name] != value:
                    raise IntegrityError(f"foundation successor {name} changed")
            else:
                completed[name] = value
                self._persist(
                    checkpoint_path,
                    core,
                    phase=name,
                    after_checkpoint=after_checkpoint,
                )

        if "foundation_policy" in completed:
            successor_policy_receipt = _receipt(
                completed["foundation_policy"], name="successor foundation policy"
            )
        else:
            successor_policy_receipt = publish_foundation_policies(
                boundary=self.boundary,
                publisher=self.publisher,
                config_root=self.boundary.active_root / "configs",
            )
            completed["foundation_policy"] = successor_policy_receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase="foundation_policy",
                after_checkpoint=after_checkpoint,
            )
        successor_policies = VerifiedFoundationPolicies.from_release(
            successor_policy_receipt, boundary=self.boundary
        )
        if "session_policy" in completed:
            successor_session_receipt = _receipt(
                completed["session_policy"], name="successor session policy"
            )
        else:
            successor_session_receipt = publish_versioned_session_policy(
                policies=successor_policies,
                boundary=self.boundary,
                publisher=self.publisher,
            )
            completed["session_policy"] = successor_session_receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase="session_policy",
                after_checkpoint=after_checkpoint,
            )
        successor_session = load_versioned_session_policy(
            successor_session_receipt,
            policies=successor_policies,
            boundary=self.boundary,
        )

        predecessor_completed = predecessor["completed"]
        assert isinstance(predecessor_completed, dict)
        predecessor_policy_receipt = _receipt(
            predecessor_completed["foundation_policy"],
            name="predecessor foundation policy",
        )
        predecessor_session_receipt = _receipt(
            predecessor_completed["session_policy"],
            name="predecessor session policy",
        )
        market_state_receipt = _receipt(
            predecessor_completed["market_state"], name="predecessor market state"
        )
        historical_policy = historical_policy_binding(
            predecessor_policy_receipt, boundary=self.boundary
        )
        coverage_policy = FoundationCoveragePolicy.from_file(
            self.boundary.active_root
            / "configs"
            / "foundation_coverage_policy.json"
        )
        scope_policy = StatusResearchScopePolicy.from_file(
            self.boundary.active_root
            / "configs"
            / "status_research_scope_policy.json"
        )
        statistics_roles = StatisticsRolePolicy.from_file(
            self.boundary.active_root
            / "configs"
            / "statistics_foundation_roles.json"
        )
        market_state = load_market_state_foundation(
            market_state_receipt,
            boundary=self.boundary,
            expected_selection=resolved,
            expected_source_selection_receipt=source_selection_receipt,
            expected_policies=historical_policy,
            expected_coverage_policy=coverage_policy,
            expected_scope_policy=scope_policy,
            expected_statistics_roles=statistics_roles,
            trusted_checkpoint_mtime_ns=predecessor_path.stat().st_mtime_ns,
        )
        adapted_contract = dict(market_state.contract)
        adapted_contract["foundation_policy_set_id"] = successor_policies.policy_set_id
        adapted_market_state = LoadedMarketStateFoundation(
            receipt=market_state.receipt,
            contract=MappingProxyType(adapted_contract),
            coverage_matrix=market_state.coverage_matrix,
            boundary=market_state.boundary,
            release_paths=market_state.release_paths,
            status_outputs=market_state.status_outputs,
            statistics_outputs=market_state.statistics_outputs,
        )
        policy_contract = build_policy_successor_contract(
            boundary=self.boundary,
            predecessor_policy_receipt=predecessor_policy_receipt,
            successor_policy_receipt=successor_policy_receipt,
            selection_receipt=source_selection_receipt,
            reused_markets=sorted(reused_markets),
            rebuilt_markets=sorted(REBUILT_MARKETS),
        )
        predecessor_relative = predecessor_path.relative_to(
            self.boundary.active_root
        ).as_posix()
        successor_provenance = build_foundation_successor_provenance(
            boundary=self.boundary,
            predecessor_checkpoint_path=predecessor_relative,
            predecessor_policy_receipt=predecessor_policy_receipt,
            predecessor_session_receipt=predecessor_session_receipt,
            successor_policy_receipt=successor_policy_receipt,
            successor_session_receipt=successor_session_receipt,
            market_state_receipt=market_state_receipt,
            selection_receipt=source_selection_receipt,
            policy_successor_contract=policy_contract,
        )
        if "successor_provenance" in completed:
            if completed["successor_provenance"] != successor_provenance:
                raise IntegrityError("foundation successor provenance changed")
        else:
            completed["successor_provenance"] = successor_provenance
            self._persist(
                checkpoint_path,
                core,
                phase="successor_provenance",
                after_checkpoint=after_checkpoint,
            )

        interval_states = completed.setdefault("intervals", {})
        if not isinstance(interval_states, dict):
            raise IntegrityError("foundation successor interval map is invalid")
        expected_rebuild_keys = {
            _interval_key(item.market, item.year, item.start, item.end)
            for item in intervals
            if item.market in REBUILT_MARKETS
        }
        if not set(interval_states).issubset(expected_rebuild_keys):
            raise IntegrityError("foundation successor contains an unexpected interval")
        newly_completed_intervals = 0
        for interval in intervals:
            if interval.market not in REBUILT_MARKETS:
                continue
            key = _interval_key(
                interval.market, interval.year, interval.start, interval.end
            )
            state = interval_states.setdefault(key, {})
            if not isinstance(state, dict) or not set(state).issubset(
                _INTERVAL_PHASES
            ):
                raise IntegrityError("foundation successor interval phase map is invalid")
            was_complete = set(state) == _INTERVAL_PHASES
            if (
                not was_complete
                and newly_completed_intervals
                >= invocation_rebuild_interval_budget
            ):
                return FoundationSuccessorResult(
                    run_id=str(core["run_id"]),
                    checkpoint_path=checkpoint_path,
                    rebuilt_interval_count=sum(
                        isinstance(value, dict)
                        and set(value) == _INTERVAL_PHASES
                        for value in interval_states.values()
                    ),
                    reused_interval_count=565,
                    foundation_set_receipt=None,
                )
            predecessor_state = predecessor_interval_states.get(key)
            if not isinstance(predecessor_state, dict):
                raise IntegrityError("foundation predecessor interval is absent")
            if "raw" not in state:
                if state:
                    raise IntegrityError("foundation successor skips reused raw phase")
                state["raw"] = predecessor_state["raw"]
                self._persist(
                    checkpoint_path,
                    core,
                    phase=f"{key}:raw_reused",
                    after_checkpoint=after_checkpoint,
                )
            raw_receipt = self.base._ensure_raw(
                state,
                interval=interval,
                selection_receipt=source_selection_receipt,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:raw",
                after_checkpoint=after_checkpoint,
            )
            definitions = self.base._ensure_definitions(
                state,
                raw_receipt=raw_receipt,
                policies=successor_policies,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:definitions",
                after_checkpoint=after_checkpoint,
            )
            causal_receipt = self.base._ensure_causal(
                state,
                raw_receipt=raw_receipt,
                policies=successor_policies,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:causal",
                after_checkpoint=after_checkpoint,
            )
            status_receipt = self.base._ensure_status_eligibility(
                state,
                causal_receipt=causal_receipt,
                market_state=adapted_market_state,
                market=interval.market,
                year=interval.year,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:status_eligibility",
                after_checkpoint=after_checkpoint,
            )
            economics = self.base._ensure_economics(
                state,
                causal_receipt=causal_receipt,
                definitions=definitions,
                policies=successor_policies,
                session_policy=successor_session,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:economics",
                after_checkpoint=after_checkpoint,
            )
            self.base._ensure_feature_input(
                state,
                causal_receipt=causal_receipt,
                definitions=definitions,
                economics=economics,
                policies=successor_policies,
                session_policy=successor_session,
                feature_spec=feature_spec,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:feature_input",
                after_checkpoint=after_checkpoint,
            )
            self.base._ensure_outcome_source_input(
                state,
                causal_receipt=causal_receipt,
                definitions=definitions,
                economics=economics,
                policies=successor_policies,
                session_policy=successor_session,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:outcome_source_input",
                after_checkpoint=after_checkpoint,
            )
            if set(state) != _INTERVAL_PHASES:
                raise IntegrityError(
                    "foundation successor rebuilt interval is incomplete"
                )
            if not was_complete:
                newly_completed_intervals += 1
        if set(interval_states) != expected_rebuild_keys:
            raise IntegrityError("foundation successor rebuild closure is incomplete")

        assembled = completed.setdefault("assembled_intervals", {})
        if not isinstance(assembled, dict):
            raise IntegrityError("foundation successor assembled map is invalid")
        expected_all_keys = {
            _interval_key(item.market, item.year, item.start, item.end)
            for item in intervals
        }
        if not set(assembled).issubset(expected_all_keys):
            raise IntegrityError("foundation successor assembled interval is unknown")
        verified_intervals: list[dict[str, object]] = []
        newly_assembled_intervals = 0
        for interval in intervals:
            key = _interval_key(
                interval.market, interval.year, interval.start, interval.end
            )
            state = (
                interval_states[key]
                if interval.market in REBUILT_MARKETS
                else predecessor_interval_states[key]
            )
            if key in assembled:
                record = assembled[key]
                if not isinstance(record, dict):
                    raise IntegrityError(
                        "foundation successor assembled record is invalid"
                    )
            else:
                if (
                    newly_assembled_intervals
                    >= invocation_assembly_interval_budget
                ):
                    return FoundationSuccessorResult(
                        run_id=str(core["run_id"]),
                        checkpoint_path=checkpoint_path,
                        rebuilt_interval_count=118,
                        reused_interval_count=565,
                        foundation_set_receipt=None,
                    )
                record = self._assemble_interval(
                    interval=interval,
                    state=state,
                    market_state_receipt=market_state_receipt,
                    feature_spec=feature_spec,
                    coverage_policy=coverage_policy,
                    scope_policy=scope_policy,
                    expected_policy_receipt=(
                        successor_policy_receipt
                        if interval.market in REBUILT_MARKETS
                        else predecessor_policy_receipt
                    ),
                    expected_session_receipt=(
                        successor_session_receipt
                        if interval.market in REBUILT_MARKETS
                        else predecessor_session_receipt
                    ),
                )
                assembled[key] = record
                self._persist(
                    checkpoint_path,
                    core,
                    phase=f"{key}:assembled",
                    after_checkpoint=after_checkpoint,
                )
                newly_assembled_intervals += 1
            verified_intervals.append(record)
        if set(assembled) != expected_all_keys:
            raise IntegrityError("foundation successor assembly is incomplete")
        verified_intervals.sort(key=lambda item: str(item["interval_key"]))

        coverage_gate = self._coverage_gate(
            intervals=verified_intervals,
            resolved=resolved,
            market_state=market_state,
            coverage_policy=coverage_policy,
            scope_policy=scope_policy,
        )
        foundation_set_core = {
            "alpha_evidence": False,
            "candidate_eligible": False,
            "dependency_closure_complete": True,
            "coverage_gate": coverage_gate,
            "coverage_matrix": list(resolved.coverage_matrix),
            "coverage_matrix_id": resolved.coverage_matrix_id,
            "feature_spec": feature_spec.as_dict(),
            "feature_spec_hash": feature_spec.spec_hash,
            "foundation_policy_receipt": successor_policy_receipt.as_dict(),
            "historical_outcome_or_label_execution": False,
            "interval_count": len(verified_intervals),
            "intervals": verified_intervals,
            "learned_or_outcome_informed_transform_count": 0,
            "model_fit_count": 0,
            "market_state_release_receipt": market_state_receipt.as_dict(),
            "query_manifest": list(resolved.query_manifest),
            "query_manifest_id": resolved.query_manifest_id,
            "query_mode_census": list(resolved.query_mode_census),
            "feature_role_contract": {
                "eligibility_authority": "STATUS_AS_OF_ELIGIBILITY_RELEASE",
                "mechanical_feature_values_are_not_standalone_research_eligibility": True,
                "statistics_feature_use": False,
            },
            "outcome_contract": {
                "deferred_until": OUTCOME_DEFERRED_UNTIL,
                "labels_materialized": False,
                "prediction_ledger_read": False,
                "role": OUTCOME_SOURCE_ROLE,
            },
            "provider_call_count": 0,
            "run_contract": dict(core["run_contract"]),
            "run_id": core["run_id"],
            "schema_version": FOUNDATION_SUCCESSOR_SCHEMA_VERSION,
            "session_policy_receipt": successor_session_receipt.as_dict(),
            "source_selection_receipt": source_selection_receipt.as_dict(),
            "source_dbn_release_id": snapshot.source_release_id,
            "successor_provenance": successor_provenance,
            "wfa_execution_count": 0,
        }
        foundation_set = {
            **foundation_set_core,
            "foundation_set_id": sha256_json(foundation_set_core),
        }
        if "foundation_set" in completed:
            foundation_set_receipt = _receipt(
                completed["foundation_set"], name="foundation successor set"
            )
            if (
                load_foundation_set(
                    foundation_set_receipt, boundary=self.boundary
                )
                != foundation_set
            ):
                raise IntegrityError("completed foundation successor changed")
        else:
            stage = self.publisher.create_stage("foundation_successor")
            dependency_ids = {
                source_selection_receipt.release_id,
                predecessor_policy_receipt.release_id,
                predecessor_session_receipt.release_id,
                successor_policy_receipt.release_id,
                successor_session_receipt.release_id,
                market_state_receipt.release_id,
            }
            for item in verified_intervals:
                for name in (
                    "raw_release_receipt",
                    "definition_release_receipt",
                    "causal_release_receipt",
                    "status_eligibility_release_receipt",
                    "economics_release_receipt",
                    "feature_input_release_receipt",
                    "outcome_source_input_release_receipt",
                ):
                    dependency_ids.add(str(item[name]["release_id"]))
            manifest = ReleaseManifest.build(
                stage,
                phase="foundation",
                release_kind=FOUNDATION_SET_RELEASE_KIND,
                schema_version=FOUNDATION_SUCCESSOR_SCHEMA_VERSION,
                logical_paths={},
                source_release_ids=tuple(sorted(dependency_ids)),
                embedded_documents={"foundation_set.json": foundation_set},
                metadata={
                    "coverage_matrix_id": resolved.coverage_matrix_id,
                    "feature_spec_hash": feature_spec.spec_hash,
                    "foundation_set_id": foundation_set["foundation_set_id"],
                    "interval_count": len(verified_intervals),
                    "query_manifest_id": resolved.query_manifest_id,
                    "run_id": core["run_id"],
                    "source_dbn_release_id": snapshot.source_release_id,
                    "successor_provenance_id": successor_provenance[
                        "successor_provenance_id"
                    ],
                },
            )
            manifest_path = self.publisher.publish(stage, manifest)
            foundation_set_receipt = VerifiedReleaseReceipt.from_manifest(
                manifest_path, self.boundary
            )
            if (
                load_foundation_set(
                    foundation_set_receipt, boundary=self.boundary
                )
                != foundation_set
            ):
                raise IntegrityError("foundation successor failed exact readback")
            completed["foundation_set"] = foundation_set_receipt.as_dict()
            core["status"] = "COMPLETE"
            self._persist(
                checkpoint_path,
                core,
                phase="foundation_set",
                after_checkpoint=after_checkpoint,
            )
        if core["status"] != "COMPLETE":
            core["status"] = "COMPLETE"
            self._persist(
                checkpoint_path,
                core,
                phase="complete_repair",
                after_checkpoint=after_checkpoint,
            )
        return FoundationSuccessorResult(
            run_id=str(core["run_id"]),
            checkpoint_path=checkpoint_path,
            rebuilt_interval_count=118,
            reused_interval_count=565,
            foundation_set_receipt=foundation_set_receipt,
        )

    def _assemble_interval(
        self,
        *,
        interval: object,
        state: Mapping[str, object],
        market_state_receipt: VerifiedReleaseReceipt,
        feature_spec: CausalFeatureSpec,
        coverage_policy: FoundationCoveragePolicy,
        scope_policy: StatusResearchScopePolicy,
        expected_policy_receipt: VerifiedReleaseReceipt,
        expected_session_receipt: VerifiedReleaseReceipt,
    ) -> dict[str, object]:
        if set(state) != _INTERVAL_PHASES:
            raise IntegrityError("foundation successor interval state is incomplete")
        key = _interval_key(
            interval.market, interval.year, interval.start, interval.end
        )
        raw_receipt = _receipt(state["raw"], name=f"{key}:raw")
        definitions = _receipt(state["definitions"], name=f"{key}:definitions")
        causal_receipt = _receipt(state["causal"], name=f"{key}:causal")
        status_receipt = _receipt(
            state["status_eligibility"], name=f"{key}:status"
        )
        economics = _receipt(state["economics"], name=f"{key}:economics")
        feature_receipt = _receipt(
            state["feature_input"], name=f"{key}:feature"
        )
        outcome_receipt = _receipt(
            state["outcome_source_input"], name=f"{key}:outcome"
        )
        raw = load_raw_interval(raw_receipt, boundary=self.boundary).interval_receipt
        _, causal_report = load_causal_interval(
            causal_receipt, boundary=self.boundary
        )
        status = load_status_eligibility(
            status_receipt,
            causal_receipt=causal_receipt,
            market_state_receipt=market_state_receipt,
            boundary=self.boundary,
        )
        feature = load_feature_source_input(
            feature_receipt, boundary=self.boundary
        )
        outcome = load_outcome_source_input(
            outcome_receipt, boundary=self.boundary
        )
        expected_dependencies = {
            "causal_release_receipt": causal_receipt.as_dict(),
            "definition_release_receipt": definitions.as_dict(),
            "economics_release_receipt": economics.as_dict(),
            "foundation_policy_receipt": expected_policy_receipt.as_dict(),
            "session_policy_receipt": expected_session_receipt.as_dict(),
        }
        if (
            raw.get("market") != interval.market
            or raw.get("year") != interval.year
            or raw.get("source_bar_file_sha256") != interval.bars.binding.sha256
            or raw.get("source_definition_file_sha256")
            != interval.definition.binding.sha256
            or causal_report.get("source_raw_release_id") != raw_receipt.release_id
            or causal_report.get("foundation_policy_release_id")
            != expected_policy_receipt.release_id
            or any(
                feature[name] != value
                for name, value in expected_dependencies.items()
            )
            or any(
                outcome[name] != value
                for name, value in expected_dependencies.items()
            )
            or feature.get("feature_spec_hash") != feature_spec.spec_hash
        ):
            raise IntegrityError(
                "foundation successor interval dependency binding is invalid"
            )
        bar_rows = int(status["total_rows"])
        feature_ready_rows = int(feature["feature_ready_rows"])
        eligible_rows = int(status["eligible_rows"])
        resolved_rows = int(status["resolved_status_rows"])
        unresolved_rows = int(status["unresolved_status_rows"])
        resolved_fraction = (
            Decimal(resolved_rows) / Decimal(bar_rows)
            if bar_rows
            else Decimal(0)
        )
        gated_fraction = (
            Decimal(eligible_rows) / Decimal(feature_ready_rows)
            if feature_ready_rows
            else Decimal(0)
        )
        coverage_passed = (
            resolved_fraction
            >= coverage_policy.minimum_status_resolved_decision_fraction
            and gated_fraction
            >= coverage_policy.minimum_status_gated_feature_ready_fraction
        )
        in_scope = scope_policy.includes_interval(
            start=interval.start, end=interval.end
        )
        disposition = scope_policy.disposition(
            start=interval.start,
            end=interval.end,
            coverage_passed=coverage_passed,
        )
        if (
            bar_rows <= 0
            or eligible_rows > feature_ready_rows
            or (
                interval.market in REBUILT_MARKETS
                and in_scope
                and feature_ready_rows <= 0
            )
        ):
            raise IntegrityError(
                "foundation successor rebuilt research interval remains unresolved"
            )
        gate_core = {
            "bar_rows": bar_rows,
            "feature_ready_rows": feature_ready_rows,
            "in_research_scope": in_scope,
            "interval_key": key,
            "research_disposition": disposition,
            "research_scope_policy_hash": scope_policy.policy_hash,
            "status_gated_feature_ready_fraction": str(gated_fraction),
            "status_gated_feature_ready_rows": eligible_rows,
            "status_eligible_rows": eligible_rows,
            "status_query_contract_ids": sorted(
                item.query_contract_id for item in interval.status
            ),
            "status_query_mode_ids": sorted(
                {item.query_mode_id for item in interval.status}
            ),
            "status_resolved_decision_fraction": str(resolved_fraction),
            "status_resolved_rows": resolved_rows,
            "status_source_present": bool(interval.status),
            "status_unresolved_rows": unresolved_rows,
        }
        gate = {**gate_core, "status_epoch_gate_id": sha256_json(gate_core)}
        return {
            "bar_query_contract_id": interval.bars.query_contract_id,
            "bar_query_mode_id": interval.bars.query_mode_id,
            "bar_source_path": interval.bars.binding.relative_path,
            "bar_source_sha256": interval.bars.binding.sha256,
            "causal_release_receipt": causal_receipt.as_dict(),
            "coverage_disposition": interval.coverage_disposition,
            "definition_release_receipt": definitions.as_dict(),
            "definition_query_contract_id": interval.definition.query_contract_id,
            "definition_query_mode_id": interval.definition.query_mode_id,
            "definition_source_path": interval.definition.binding.relative_path,
            "definition_source_sha256": interval.definition.binding.sha256,
            "economics_release_receipt": economics.as_dict(),
            "end": interval.end,
            "feature_input_release_receipt": feature_receipt.as_dict(),
            "feature_ready_rows": feature_ready_rows,
            "interval_key": key,
            "market": interval.market,
            "outcome_source_input_release_receipt": outcome_receipt.as_dict(),
            "raw_release_receipt": raw_receipt.as_dict(),
            "status_eligibility_release_receipt": status_receipt.as_dict(),
            "status_epoch_gate": gate,
            "status_eligible_rows": eligible_rows,
            "status_gated_feature_ready_rows": eligible_rows,
            "status_resolved_rows": resolved_rows,
            "status_unresolved_rows": unresolved_rows,
            "start": interval.start,
            "year": interval.year,
        }

    @staticmethod
    def _coverage_gate(
        *,
        intervals: list[dict[str, object]],
        resolved: object,
        market_state: LoadedMarketStateFoundation,
        coverage_policy: FoundationCoveragePolicy,
        scope_policy: StatusResearchScopePolicy,
    ) -> dict[str, object]:
        total_bar_rows = sum(
            int(item["status_resolved_rows"])
            + int(item["status_unresolved_rows"])
            for item in intervals
        )
        total_feature_rows = sum(
            int(item["feature_ready_rows"]) for item in intervals
        )
        total_eligible = sum(int(item["status_eligible_rows"]) for item in intervals)
        total_gated = sum(int(item["status_gated_feature_ready_rows"]) for item in intervals)
        total_resolved = sum(int(item["status_resolved_rows"]) for item in intervals)
        total_unresolved = sum(int(item["status_unresolved_rows"]) for item in intervals)
        in_scope = [
            item
            for item in intervals
            if item["status_epoch_gate"]["in_research_scope"] is True
        ]
        research_bar_rows = sum(
            int(item["status_resolved_rows"]) + int(item["status_unresolved_rows"])
            for item in in_scope
        )
        research_feature_rows = sum(
            int(item["feature_ready_rows"]) for item in in_scope
        )
        research_eligible = sum(
            int(item["status_eligible_rows"]) for item in in_scope
        )
        research_gated = sum(
            int(item["status_gated_feature_ready_rows"]) for item in in_scope
        )
        research_resolved = sum(
            int(item["status_resolved_rows"]) for item in in_scope
        )
        research_unresolved = sum(
            int(item["status_unresolved_rows"]) for item in in_scope
        )
        resolved_fraction = (
            Decimal(research_resolved) / Decimal(research_bar_rows)
            if research_bar_rows
            else Decimal(0)
        )
        gated_fraction = (
            Decimal(research_gated) / Decimal(research_feature_rows)
            if research_feature_rows
            else Decimal(0)
        )
        archive_resolved_fraction = (
            Decimal(total_resolved) / Decimal(total_bar_rows)
            if total_bar_rows
            else Decimal(0)
        )
        archive_gated_fraction = (
            Decimal(total_gated) / Decimal(total_feature_rows)
            if total_feature_rows
            else Decimal(0)
        )
        gates = [dict(item["status_epoch_gate"]) for item in intervals]
        if (
            research_bar_rows < coverage_policy.minimum_bar_rows
            or research_eligible < coverage_policy.minimum_status_eligible_rows
            or research_gated
            < coverage_policy.minimum_status_gated_feature_ready_rows
            or resolved_fraction
            < coverage_policy.minimum_status_resolved_decision_fraction
            or gated_fraction
            < coverage_policy.minimum_status_gated_feature_ready_fraction
            or not in_scope
            or any(
                item["status_epoch_gate"]["research_disposition"] != "ELIGIBLE"
                for item in in_scope
            )
            or not _source_family_coverage_passes(
                market_state.contract, coverage_policy=coverage_policy
            )
        ):
            raise IntegrityError("foundation successor coverage gate failed")
        archive_core = {
            "bar_rows": total_bar_rows,
            "feature_ready_rows": total_feature_rows,
            "missing_status_rows_remain_in_denominator": True,
            "status_eligible_rows": total_eligible,
            "status_gated_feature_ready_fraction": str(archive_gated_fraction),
            "status_gated_feature_ready_rows": total_gated,
            "status_resolved_decision_fraction": str(archive_resolved_fraction),
            "status_resolved_rows": total_resolved,
            "status_unresolved_rows": total_unresolved,
        }
        return {
            "archive_census": {
                **archive_core,
                "archive_census_id": sha256_json(archive_core),
            },
            "bar_rows": research_bar_rows,
            "coverage_matrix_id": resolved.coverage_matrix_id,
            "coverage_policy": coverage_policy.as_dict(),
            "coverage_policy_hash": coverage_policy.policy_hash,
            "feature_ready_rows": research_feature_rows,
            "missing_status_rows_remain_in_denominator": True,
            "research_abstained_interval_count": sum(
                gate["in_research_scope"] is False for gate in gates
            ),
            "research_eligible_interval_count": sum(
                gate["research_disposition"] == "ELIGIBLE" for gate in gates
            ),
            "research_failed_interval_count": sum(
                gate["research_disposition"] == "FAIL_STATUS_COVERAGE"
                for gate in gates
            ),
            "research_scope_interval_count": len(in_scope),
            "research_scope_policy": scope_policy.as_dict(),
            "research_scope_policy_hash": scope_policy.policy_hash,
            "research_scope_status_source_market_year_fraction": (
                market_state.contract[
                    "research_scope_status_source_market_year_fraction"
                ]
            ),
            "statistics_feature_use": False,
            "statistics_source_market_year_fraction": market_state.contract[
                "statistics_source_market_year_fraction"
            ],
            "status_eligible_rows": research_eligible,
            "status_epoch_gates": gates,
            "status_epoch_gates_id": sha256_json(gates),
            "status_gated_feature_ready_fraction": str(gated_fraction),
            "status_gated_feature_ready_rows": research_gated,
            "status_resolved_decision_fraction": str(resolved_fraction),
            "status_resolved_rows": research_resolved,
            "status_source_market_year_fraction": market_state.contract[
                "status_source_market_year_fraction"
            ],
            "status_unresolved_rows": research_unresolved,
        }


def main(argv: list[str] | None = None) -> int:
    from .orchestrator import _boundary_from_contract, _load_feature_spec

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--source-dbn-manifest", type=Path, required=True)
    parser.add_argument("--source-selection-manifest", type=Path, required=True)
    parser.add_argument("--predecessor-checkpoint", type=Path, required=True)
    parser.add_argument("--expected-predecessor-checkpoint-id", required=True)
    parser.add_argument("--expected-predecessor-run-id", required=True)
    parser.add_argument("--feature-spec", type=Path, required=True)
    parser.add_argument("--maximum-rebuild-intervals", type=int, required=True)
    parser.add_argument(
        "--invocation-rebuild-interval-budget", type=int, default=20
    )
    parser.add_argument(
        "--invocation-assembly-interval-budget", type=int, default=50
    )
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("foundation successor publication requires explicit --execute")
    boundary = _boundary_from_contract(args.repository_root, args.source_contract)
    feature_spec = _load_feature_spec(args.feature_spec, boundary=boundary)
    selection_receipt = VerifiedReleaseReceipt.from_manifest(
        args.source_selection_manifest, boundary
    )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "expected_predecessor_checkpoint_id": (
                args.expected_predecessor_checkpoint_id
            ),
            "expected_predecessor_run_id": args.expected_predecessor_run_id,
            "maximum_rebuild_intervals": str(args.maximum_rebuild_intervals),
            "invocation_rebuild_interval_budget": str(
                args.invocation_rebuild_interval_budget
            ),
            "invocation_assembly_interval_budget": str(
                args.invocation_assembly_interval_budget
            ),
            "rebuilt_markets_id": sha256_json(sorted(REBUILT_MARKETS)),
            "source_selection_release_id": selection_receipt.release_id,
        },
    )

    def progress(phase: str) -> None:
        print(
            canonical_bytes(
                {
                    "phase": phase,
                    "provider_call_count": 0,
                    "status": "CHECKPOINTED",
                }
            ).decode("utf-8"),
            flush=True,
        )

    result = FoundationSuccessorFinalizer(
        boundary=boundary,
        operation_receipt=operation,
        batch_rows=args.batch_rows,
    ).run(
        source_dbn_manifest=args.source_dbn_manifest,
        source_selection_receipt=selection_receipt,
        predecessor_checkpoint_path=args.predecessor_checkpoint,
        expected_predecessor_checkpoint_id=args.expected_predecessor_checkpoint_id,
        expected_predecessor_run_id=args.expected_predecessor_run_id,
        feature_spec=feature_spec,
        maximum_rebuild_intervals=args.maximum_rebuild_intervals,
        invocation_rebuild_interval_budget=(
            args.invocation_rebuild_interval_budget
        ),
        invocation_assembly_interval_budget=(
            args.invocation_assembly_interval_budget
        ),
        after_checkpoint=progress,
    )
    print(canonical_bytes(result.as_dict()).decode("utf-8"), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
