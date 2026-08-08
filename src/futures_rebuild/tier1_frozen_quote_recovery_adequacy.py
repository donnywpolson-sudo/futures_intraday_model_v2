"""Predeclared causal source gate for staged Tier 1 BBO recovery rows.

This module contains only deterministic mechanics.  It does not open staged
DBN files, publish a source, or expose prices.  A later separately authorized
census may feed decoded observations into these rules.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from databento_dbn import UNDEF_PRICE

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .tier1_bracket_v5 import MARKETS, NS_PER_MINUTE
from .tier1_frozen_quote_recovery_acquisition import (
    PLAN_PATH as ACQUISITION_PLAN_PATH,
)
from .tier1_frozen_quote_recovery_cost import (
    DIAGNOSTIC_RECORD_ID,
    DIAGNOSTIC_RECORD_SHA256,
    ENTRY_POST_ARRIVAL_SECONDS,
    PROTOCOL_ID,
    PROTOCOL_PATH,
    PROTOCOL_SHA256,
    QUOTE_STALENESS_SECONDS,
    build_quote_cost_queries,
)
from .tier1_frozen_successor_source_semantics import (
    ENTRY_DELAY_MINUTES,
    MAXIMUM_HOLD_MINUTES,
    MAXIMUM_LIQUIDATION_DELAY_MINUTES,
)


PLAN_PATH = Path("configs/tier1_frozen_quote_recovery_adequacy_plan.json")
ACQUISITION_PLAN_ID = "ebfb3338ef1fb5fdc71ec074ee682d2767675c630c8ae1cb6e434d19cacc8c82"
ACQUISITION_PLAN_SHA256 = "044fdbe91dd9e99471e406d172aeba23906e85b3bcec913b87b253232db03351"
OPERATION = "CENSUS_FROZEN_TIER1_BBO_RECOVERY_ADEQUACY_AND_PUBLISH"
EXPECTED_TARGET_COUNTS = {"ENTRY": 27, "LIQUIDATION": 6}
MAXIMUM_HOST_RUNTIME_SECONDS = 900


@dataclass(frozen=True)
class FrozenQuoteTargetSpec:
    opportunity_id: str
    query_id: str
    market: str
    category: str
    decision_at_ns: int

    def bind_causal_identity(self, instrument_id: int) -> QuoteRecoveryTarget:
        return QuoteRecoveryTarget(
            self.opportunity_id, self.query_id, self.market,
            self.category, self.decision_at_ns, instrument_id,
        )


def build_frozen_quote_target_specs(
    *, diagnostic_record: Mapping[str, object],
) -> tuple[FrozenQuoteTargetSpec, ...]:
    """Bind each unresolved diagnostic target to exactly one frozen query."""

    recovery = diagnostic_record.get("recovery_map")
    if not isinstance(recovery, list):
        raise IntegrityError("BBO recovery diagnostic map is absent")
    unresolved = {
        str(item.get("opportunity_id")): item
        for item in recovery
        if isinstance(item, Mapping)
        and item.get("disposition") == "NOT_OBSERVED_IN_BOUND_DIAGNOSTIC_SOURCES"
    }
    queries = build_quote_cost_queries(diagnostic_record=diagnostic_record)
    query_by_opportunity: dict[str, str] = {}
    for query in queries:
        for opportunity_id in query.opportunity_ids:
            if opportunity_id in query_by_opportunity:
                raise IntegrityError("BBO recovery opportunity is assigned to multiple queries")
            query_by_opportunity[opportunity_id] = query.query_id
    if set(unresolved) != set(query_by_opportunity) or len(unresolved) != 33:
        raise IntegrityError("BBO recovery target-to-query binding is incomplete")
    output: list[FrozenQuoteTargetSpec] = []
    for opportunity_id, item in unresolved.items():
        market = item.get("market")
        category = item.get("category")
        decision = item.get("decision_at_ns")
        if (
            market not in MARKETS or category not in EXPECTED_TARGET_COUNTS
            or type(decision) is not int or decision < 0
        ):
            raise IntegrityError("BBO recovery diagnostic target is invalid")
        output.append(FrozenQuoteTargetSpec(
            opportunity_id, query_by_opportunity[opportunity_id],
            str(market), str(category), decision,
        ))
    output.sort(key=lambda item: (item.decision_at_ns, item.market, item.opportunity_id))
    counts = {
        category: sum(item.category == category for item in output)
        for category in EXPECTED_TARGET_COUNTS
    }
    if counts != EXPECTED_TARGET_COUNTS or len({item.query_id for item in output}) != 30:
        raise IntegrityError("BBO recovery target composition or query count changed")
    return tuple(output)


@dataclass(frozen=True)
class QuoteRecoveryTarget:
    opportunity_id: str
    query_id: str
    market: str
    category: str
    decision_at_ns: int
    expected_instrument_id: int

    def window(self) -> tuple[int, int]:
        if (
            len(self.opportunity_id) != 64 or len(self.query_id) != 64
            or self.market not in MARKETS
            or self.category not in EXPECTED_TARGET_COUNTS
            or type(self.decision_at_ns) is not int or self.decision_at_ns < 0
            or type(self.expected_instrument_id) is not int
            or self.expected_instrument_id <= 0
        ):
            raise IntegrityError("BBO recovery target is invalid")
        second = 1_000_000_000
        entry_at = self.decision_at_ns + ENTRY_DELAY_MINUTES * NS_PER_MINUTE
        if self.category == "ENTRY":
            return entry_at, entry_at + ENTRY_POST_ARRIVAL_SECONDS * second
        timeout = entry_at + MAXIMUM_HOLD_MINUTES * NS_PER_MINUTE
        return timeout, timeout + MAXIMUM_LIQUIDATION_DELAY_MINUTES * NS_PER_MINUTE


@dataclass(frozen=True)
class CausalBboObservation:
    query_id: str
    market: str
    instrument_id: int
    ts_event_ns: int
    available_at_ns: int
    bid_price_nano: int
    ask_price_nano: int
    bid_size: int
    ask_size: int
    row_ordinal: int

    def validate(self) -> None:
        if (
            len(self.query_id) != 64 or self.market not in MARKETS
            or type(self.instrument_id) is not int or self.instrument_id <= 0
            or type(self.ts_event_ns) is not int or self.ts_event_ns < 0
            or type(self.available_at_ns) is not int
            or self.available_at_ns < self.ts_event_ns
            or type(self.bid_price_nano) is not int
            or type(self.ask_price_nano) is not int
            or self.bid_price_nano in {UNDEF_PRICE, 0}
            or self.ask_price_nano in {UNDEF_PRICE, 0}
            or self.bid_price_nano < 0
            or self.ask_price_nano <= self.bid_price_nano
            or type(self.bid_size) is not int or self.bid_size <= 0
            or type(self.ask_size) is not int or self.ask_size <= 0
            or type(self.row_ordinal) is not int or self.row_ordinal < 0
        ):
            raise IntegrityError("BBO observation is not a valid causal two-sided book")


@dataclass(frozen=True)
class QuoteCoverage:
    opportunity_id: str
    query_id: str
    market: str
    category: str
    status: str
    reason: str | None
    selected_available_at_ns: int | None
    selected_row_ordinal: int | None

    def as_price_free_dict(self) -> dict[str, object]:
        return {
            "opportunity_id": self.opportunity_id,
            "query_id": self.query_id,
            "market": self.market,
            "category": self.category,
            "status": self.status,
            "reason": self.reason,
            "selected_available_at_ns": self.selected_available_at_ns,
            "selected_row_ordinal": self.selected_row_ordinal,
        }


def classify_quote_target(
    *, target: QuoteRecoveryTarget,
    observations: Sequence[CausalBboObservation],
) -> tuple[QuoteCoverage, CausalBboObservation | None]:
    """Select the first causal quote; never backfill from a pre-action quote."""

    start, deadline = target.window()
    ordinals: set[int] = set()
    scoped: list[CausalBboObservation] = []
    for observation in observations:
        observation.validate()
        if observation.row_ordinal in ordinals:
            raise IntegrityError("BBO observation ordinal is duplicated")
        ordinals.add(observation.row_ordinal)
        if observation.query_id != target.query_id or observation.market != target.market:
            raise IntegrityError("BBO observation differs from its frozen target query")
        if start <= observation.available_at_ns <= deadline:
            scoped.append(observation)
    if any(item.instrument_id != target.expected_instrument_id for item in scoped):
        return QuoteCoverage(
            target.opportunity_id, target.query_id, target.market, target.category,
            "EXPLICIT_UNAVAILABLE", "ambiguous or foreign causal instrument identity",
            None, None,
        ), None
    eligible = sorted(scoped, key=lambda item: (item.available_at_ns, item.row_ordinal))
    if not eligible:
        return QuoteCoverage(
            target.opportunity_id, target.query_id, target.market, target.category,
            "EXPLICIT_UNAVAILABLE", "no valid causal two-sided quote within deadline",
            None, None,
        ), None
    selected = eligible[0]
    return QuoteCoverage(
        target.opportunity_id, target.query_id, target.market, target.category,
        "COMPLETE", None, selected.available_at_ns, selected.row_ordinal,
    ), selected


def adjudicate_quote_coverage(
    *, coverage: Sequence[QuoteCoverage], expected_ids: Sequence[str],
) -> dict[str, object]:
    ids = [item.opportunity_id for item in coverage]
    if (
        not expected_ids or len(expected_ids) != len(set(expected_ids))
        or len(ids) != len(set(ids)) or set(ids) != set(expected_ids)
        or any(item.status not in {"COMPLETE", "EXPLICIT_UNAVAILABLE"} for item in coverage)
    ):
        raise IntegrityError("BBO recovery coverage ledger does not reconcile")
    counts = {
        category: sum(item.category == category for item in coverage)
        for category in EXPECTED_TARGET_COUNTS
    }
    complete = sum(item.status == "COMPLETE" for item in coverage)
    checks = {
        "all_33_targets_have_terminal_status": len(coverage) == 33,
        "target_composition_is_27_entry_and_6_liquidation": counts == EXPECTED_TARGET_COUNTS,
        "every_target_has_valid_causal_side_specific_quote": complete == len(coverage),
        "pre_action_quotes_never_used_as_fills": True,
        "prices_not_published_in_source_quality_record": True,
        "source_not_activated_by_census": True,
    }
    return {
        "decision": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "target_count": len(coverage),
        "complete_count": complete,
        "explicit_unavailable_count": len(coverage) - complete,
        "category_counts": counts,
    }


def load_quote_recovery_adequacy_plan(*, root: Path) -> dict[str, object]:
    try:
        plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise UnauthorizedOperation("BBO adequacy plan is unavailable") from exc
    if not isinstance(plan, dict):
        raise UnauthorizedOperation("BBO adequacy plan is not an object")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_frozen_quote_recovery_adequacy_plan/1.0.0"
        or plan.get("state") != "PREPARED_AWAITING_ACQUISITION_AND_SEPARATE_ROW_READ_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("diagnostic_record_id") != DIAGNOSTIC_RECORD_ID
        or plan.get("diagnostic_record_sha256") != DIAGNOSTIC_RECORD_SHA256
        or plan.get("protocol_id") != PROTOCOL_ID
        or plan.get("protocol_sha256") != PROTOCOL_SHA256
        or plan.get("acquisition_plan_id") != ACQUISITION_PLAN_ID
        or plan.get("acquisition_plan_sha256") != ACQUISITION_PLAN_SHA256
        or plan.get("expected_target_counts") != EXPECTED_TARGET_COUNTS
        or plan.get("maximum_host_runtime_seconds") != MAXIMUM_HOST_RUNTIME_SECONDS
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or sha256_file(root / PROTOCOL_PATH) != PROTOCOL_SHA256
        or sha256_file(root / ACQUISITION_PLAN_PATH) != ACQUISITION_PLAN_SHA256
        or not isinstance(forbidden, Mapping) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("BBO adequacy plan is absent or drifted")
    return plan
