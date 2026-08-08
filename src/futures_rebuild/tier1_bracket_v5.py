"""V5 pre-data successor controls for the audited Tier 1 bracket trial.

The registered V4 files are evidence and are never imported as an execution
engine here.  This module supplies the corrected, synthetic-testable control
surface that a V5 registration binds before historical rows may be opened.
"""

from __future__ import annotations

import json
import math
import platform
import sys
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from .research.contracts import ResearchContractError
from .research.hac import newey_west_mean
from .research.multiple_testing import romano_wolf_from_differentials
from .tier1_bracket_post_audit import CausalBar, cost_ticks, latest_causal_feature_bar
from .tier1_bracket_v4 import (
    BracketFill,
    DirectionOutcomes,
    ExpectedCheckpoint,
    FoldSpec,
    FrozenPrediction,
    MarketSpec,
    MaterializedRow,
    ModelFitResult,
    SourceMinute,
    _features,
    _strategy_signal,
    build_v4_folds_from_census,
    fit_predict_v4,
    simulate_v4_bracket_fill,
)


MARKETS = ("ES", "CL", "ZN", "6E")
CHECKPOINTS = ("08:30", "10:30", "13:30")
CHICAGO = ZoneInfo("America/Chicago")
NS_PER_MINUTE = 60_000_000_000
V4_TRIAL_ID = "427237dc760699831f0998421e6718b3166c5a03dfc1e336bdd3bf7b901c2349"
V4_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v4") / f"{V4_TRIAL_ID}.json"
V4_EVENT = Path("state/trial_events/tier1_bracket_successor_v4") / f"{V4_TRIAL_ID}.json"
V4_BOUND_PATHS = (
    Path("configs/tier1_bracket_successor_v4.json"),
    Path("src/futures_rebuild/tier1_bracket_v4.py"),
    Path("tests/test_tier1_bracket_v4.py"),
    V4_REGISTRY,
    V4_EVENT,
)
V5_CONTRACT = Path("configs/tier1_bracket_successor_v5.json")
V4_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v4_retirement_preparation.json")
V5_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v5")
V5_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v5")
V4_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v4_retirement")
V4_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v4_retirement")
TRADABLE_DISPOSITIONS = frozenset(
    {
        "ELIGIBLE",
        "AUTHORITATIVE_INTERVAL",
        "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK",
    }
)


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid JSON artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"artifact is not an object: {path.as_posix()}")
    return value


def _hex64(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= set("0123456789abcdef")


def load_v5_contract(*, root: Path) -> dict[str, object]:
    payload = _load_object(root / V5_CONTRACT)
    risk = payload.get("risk")
    inference = payload.get("inference")
    coverage = payload.get("coverage")
    authority = payload.get("authority")
    census = payload.get("opportunity_census")
    if (
        payload.get("schema_version") != "tier1_bracket_successor_v5_contract/1.0.0"
        or payload.get("state") != "PREPARED_NOT_REGISTERED"
        or payload.get("supersedes_v4_trial_id") != V4_TRIAL_ID
        or not isinstance(census, dict)
        or census.get("source")
        != "HASH_BOUND_CME_AUTHORED_HISTORICAL_CHECKPOINT_CALENDAR_INDEX_INDEPENDENT_OF_PRICE_ROWS"
        or census.get("active_calendar_pointer_path")
        != "configs/tier1_historical_checkpoint_calendar_v5.json"
        or census.get("certification_scope")
        != "ONLY_08_30_10_30_13_30_AMERICA_CHICAGO_NOT_A_FULL_SESSION_TAPE"
        or census.get("required_history")
        != "GAPLESS_EVERY_CALENDAR_DATE_2018-01-01_THROUGH_2022-12-31_FOR_ES_CL_ZN_6E"
        or census.get("forward_only_calendar_binding") != "INVALID_REGISTRATION"
        or census.get("prepublication_metadata_span_check") is not True
        or census.get("authorized_row_level_market_date_completeness_check") is not True
        or not isinstance(risk, dict)
        or risk.get("maximum_planned_initial_loss_usd") != "250"
        or risk.get("continuous_drawdown_threshold_usd") != "1500"
        or not isinstance(inference, dict)
        or inference.get("block_sensitivity_sessions") != [5, 10, 20]
        or inference.get("portfolio_power_alternative_usd_per_complete_session") != "30"
        or inference.get("sleeve_power_alternative_usd_per_complete_session") != "1.25"
        or inference.get("training_power_resamples") != 5000
        or not isinstance(coverage, dict)
        or coverage.get("terminal_ledger_rate_required") != "1.0"
        or not isinstance(authority, dict)
        or authority.get("caller_boolean_authorization_forbidden") is not True
        or authority.get("holdout_or_forward_access") is not False
    ):
        raise IntegrityError("V5 contract is incomplete or has drifted")
    return payload


@dataclass(frozen=True)
class CalendarSessionSpec:
    market: str
    exchange_session_date: str
    open_at_ns: int
    close_at_ns: int
    calendar_release_id: str
    checkpoint_states: Mapping[str, bool] | None = None

    def validate(self) -> None:
        try:
            session = date.fromisoformat(self.exchange_session_date)
        except ValueError as exc:
            raise IntegrityError("calendar session date is invalid") from exc
        if (
            self.market not in MARKETS
            or session.year not in range(2018, 2023)
            or type(self.open_at_ns) is not int
            or type(self.close_at_ns) is not int
            or self.open_at_ns >= self.close_at_ns
            or not _hex64(self.calendar_release_id)
            or self.checkpoint_states is not None
            and (
                set(self.checkpoint_states) != set(CHECKPOINTS)
                or any(type(value) is not bool for value in self.checkpoint_states.values())
            )
        ):
            raise IntegrityError("calendar session specification is invalid")


@dataclass(frozen=True)
class CensusCheckpoint:
    expected: ExpectedCheckpoint
    calendar_open: bool
    calendar_release_id: str


def _checkpoint_ns(session_date: str, checkpoint: str) -> int:
    hour, minute = (int(item) for item in checkpoint.split(":"))
    local = datetime.combine(date.fromisoformat(session_date), time(hour, minute), CHICAGO)
    return int(local.timestamp() * 1_000_000_000)


def build_expected_census_from_calendar(
    *, sessions: Sequence[CalendarSessionSpec],
) -> tuple[CensusCheckpoint, ...]:
    """Build all market/session/checkpoint rows without consulting price rows."""

    for item in sessions:
        item.validate()
    keys = [(item.market, item.exchange_session_date) for item in sessions]
    if not keys or len(keys) != len(set(keys)):
        raise IntegrityError("calendar session census is empty or duplicated")
    result: list[CensusCheckpoint] = []
    for session in sorted(sessions, key=lambda item: (item.exchange_session_date, item.market)):
        year = date.fromisoformat(session.exchange_session_date).year
        for checkpoint in CHECKPOINTS:
            decision = _checkpoint_ns(session.exchange_session_date, checkpoint)
            core = {
                "calendar_release_id": session.calendar_release_id,
                "checkpoint": checkpoint,
                "decision_at_ns": decision,
                "market": session.market,
                "session": session.exchange_session_date,
                "year": year,
            }
            expected = ExpectedCheckpoint(
                sha256_json(core), session.market, year,
                session.exchange_session_date, checkpoint, decision,
            )
            result.append(
                CensusCheckpoint(
                    expected,
                    (
                        bool(session.checkpoint_states[checkpoint])
                        if session.checkpoint_states is not None
                        else session.open_at_ns <= decision < session.close_at_ns
                    ),
                    session.calendar_release_id,
                )
            )
    return tuple(result)


def load_registered_calendar_sessions_v5(
    *, boundary: RepoBoundary, registered_calendar_index_release_id: str,
) -> tuple[CalendarSessionSpec, ...]:
    """Load the hash-bound calendar universe after historical authorization."""

    from .historical_checkpoint_calendar import load_historical_checkpoint_calendar

    loaded = load_historical_checkpoint_calendar(boundary=boundary)
    if loaded.index_receipt.release_id != registered_calendar_index_release_id:
        raise IntegrityError("historical checkpoint calendar index differs from V5 registration")
    result: list[CalendarSessionSpec] = []
    for market in MARKETS:
        trade_date = date(2018, 1, 1)
        while trade_date <= date(2022, 12, 31):
            states = loaded.sessions.get((market, trade_date.isoformat()))
            if states is None:
                raise IntegrityError("registered checkpoint calendar omits a market trade date")
            start = datetime.combine(trade_date - timedelta(days=1), time(17), CHICAGO).astimezone(timezone.utc)
            end = datetime.combine(trade_date, time(17), CHICAGO).astimezone(timezone.utc)
            result.append(
                CalendarSessionSpec(
                    market, trade_date.isoformat(),
                    int(start.timestamp() * 1_000_000_000),
                    int(end.timestamp() * 1_000_000_000),
                    loaded.calendar_receipt.release_id, states,
                )
            )
            trade_date += timedelta(days=1)
    return tuple(result)


def require_historical_calendar_index_span_v5(
    *, manifest: Mapping[str, object], expected_release_id: str,
) -> None:
    """Reject a calendar-index binding that cannot cover the V5 history.

    This metadata-only preregistration check prevents a current/forward active
    calendar from being mislabeled as 2018-2022 evidence.  Row-level universe
    and session checks remain mandatory when historical calendar access is
    separately authorized.
    """

    embedded = manifest.get("embedded_documents")
    if manifest.get("release_kind") == "historical_checkpoint_calendar_index":
        document = embedded.get("historical_checkpoint_calendar_index.json") if isinstance(embedded, dict) else None
        if (
            manifest.get("release_id") != expected_release_id
            or not isinstance(document, dict)
            or document.get("coverage_start") != "2018-01-01"
            or document.get("coverage_end") != "2022-12-31"
            or document.get("markets") != ["6E", "CL", "ES", "ZN"]
        ):
            raise IntegrityError("QUALIFIED_HISTORICAL_CALENDAR_SOURCE_NOT_ESTABLISHED")
        return
    document = embedded.get("exchange_calendar_index.json") if isinstance(embedded, dict) else None
    segments = document.get("segments") if isinstance(document, dict) else None
    if manifest.get("release_kind") != "exchange_calendar_index" or manifest.get("release_id") != expected_release_id or not isinstance(segments, list) or not segments:
        raise IntegrityError("historical calendar index manifest is invalid")
    cursor = date(2018, 1, 1)
    required_end = date(2022, 12, 31)
    for raw in segments:
        if not isinstance(raw, dict):
            raise IntegrityError("historical calendar index segment is invalid")
        try:
            start = date.fromisoformat(str(raw["effective_from_trade_date"]))
            end = date.fromisoformat(str(raw["effective_through_trade_date"]))
        except (KeyError, ValueError) as exc:
            raise IntegrityError("historical calendar index segment is invalid") from exc
        if end < cursor:
            continue
        if start > cursor:
            raise IntegrityError(
                "QUALIFIED_HISTORICAL_CALENDAR_SOURCE_NOT_ESTABLISHED"
            )
        cursor = end + timedelta(days=1)
        if cursor > required_end:
            return
    raise IntegrityError("QUALIFIED_HISTORICAL_CALENDAR_SOURCE_NOT_ESTABLISHED")


@dataclass(frozen=True)
class V5SourceRecord:
    market: str
    exchange_session_date: str
    disposition: str | None
    bar: CausalBar | None
    volume: float | None
    actual_identity_hash: str | None
    source_row_sha256: str
    market_spec: MarketSpec | None = None

    @property
    def executable(self) -> bool:
        return self.disposition in TRADABLE_DISPOSITIONS and self.bar is not None

    def validate(self) -> None:
        if self.market not in MARKETS or not _hex64(self.source_row_sha256):
            raise IntegrityError("V5 source record identity is invalid")
        try:
            date.fromisoformat(self.exchange_session_date)
        except ValueError as exc:
            raise IntegrityError("V5 source session is invalid") from exc
        if self.executable:
            assert self.bar is not None
            self.bar.validate()
            if (
                self.bar.executable is not True
                or self.volume is None
                or not math.isfinite(self.volume)
                or self.volume < 0
                or not _hex64(self.actual_identity_hash)
                or self.market_spec is None
            ):
                raise IntegrityError("tradable V5 source record is incomplete")
            self.market_spec.validate()
        elif self.bar is not None and self.bar.executable:
            raise IntegrityError("non-tradable V5 source record cannot be executable")


def source_record_from_mapping(*, market: str, row: Mapping[str, object]) -> V5SourceRecord:
    """Fail closed on missing/unknown disposition; never manufacture eligibility."""

    disposition_raw = row.get("disposition")
    disposition = disposition_raw if isinstance(disposition_raw, str) else None
    executable = disposition in TRADABLE_DISPOSITIONS
    event_raw = row.get("event_at_ns")
    session_raw = row.get("exchange_session_date")
    source_hash = row.get("source_row_sha256")
    if type(event_raw) is not int or not isinstance(session_raw, str) or not _hex64(source_hash):
        raise IntegrityError("source row lacks required census identity")
    bar: CausalBar | None = None
    volume: float | None = None
    identity = row.get("actual_identity_hash")
    if executable:
        try:
            volume = float(row["volume"])
            prices = [Decimal(int(row[name])) / Decimal("1000000000") for name in (
                "open_nano", "high_nano", "low_nano", "close_nano"
            )]
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise IntegrityError("tradable source row lacks executable values") from exc
        bar = CausalBar(
            event_raw, event_raw + NS_PER_MINUTE,
            event_raw + NS_PER_MINUTE + 5_000_000_000,
            *prices, True,
        )
    elif all(type(row.get(name)) is int for name in ("open_nano", "high_nano", "low_nano", "close_nano")):
        prices = [Decimal(int(row[name])) / Decimal("1000000000") for name in (
            "open_nano", "high_nano", "low_nano", "close_nano"
        )]
        bar = CausalBar(
            event_raw, event_raw + NS_PER_MINUTE,
            event_raw + NS_PER_MINUTE + 5_000_000_000,
            *prices, False,
        )
    market_spec: MarketSpec | None = None
    try:
        market_spec = MarketSpec(
            Decimal(str(row["tick_size"])), Decimal(str(row["tick_value"])),
            Decimal(str(row["point_value"])),
        )
        market_spec.validate()
    except (KeyError, TypeError, ArithmeticError, IntegrityError):
        if executable:
            raise IntegrityError("tradable source row lacks market economics")
    record = V5SourceRecord(
        market, session_raw, disposition, bar, volume,
        identity if isinstance(identity, str) else None, str(source_hash), market_spec,
    )
    record.validate()
    return record


V5_PRE_PREDICTION = frozenset(
    {
        "CALENDAR_CLOSED",
        "MISSING_SOURCE_SESSION",
        "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY",
        "INSUFFICIENT_CAUSAL_HISTORY",
        "TRAINING_OR_PREDICTION_INELIGIBLE",
    }
)
V5_POST_PREDICTION = frozenset(
    {
        "FLAT_NO_TRADE", "MISSING_PRICE_PATH", "RISK_CAP_REJECTION",
        "HURDLE_FAILURE", "CROSS_MARKET_RANKING_LOSS",
        "OVERLAP_ABSTENTION", "DAILY_STOP_ABSTENTION", "ENTRY_CAP_ABSTENTION",
        "DRAWDOWN_ABSTENTION", "INCOMPLETE_RISK_LIQUIDATION_PATH", "ADMITTED_TRADE",
    }
)
V5_TERMINAL_DISPOSITIONS = V5_PRE_PREDICTION | V5_POST_PREDICTION | {"PREDICTION_PRODUCED"}


@dataclass(frozen=True)
class OpportunityRecordV5:
    opportunity_id: str
    market: str
    exchange_session_date: str
    checkpoint: str
    decision_at_ns: int
    terminal_disposition: str
    prediction_produced: bool
    feature_event_at_ns: int | None = None
    feature_available_at_ns: int | None = None
    order_submitted_at_ns: int | None = None
    fill_at_ns: int | None = None
    outcome_coverage: str = "NOT_APPLICABLE"

    def validate(self) -> None:
        if (
            not self.opportunity_id or self.market not in MARKETS
            or self.checkpoint not in CHECKPOINTS
            or self.terminal_disposition not in V5_TERMINAL_DISPOSITIONS
            or (self.terminal_disposition in V5_PRE_PREDICTION and self.prediction_produced)
            or (self.terminal_disposition in V5_POST_PREDICTION and not self.prediction_produced)
            or (self.terminal_disposition == "PREDICTION_PRODUCED" and not self.prediction_produced)
        ):
            raise IntegrityError("V5 opportunity record is inconsistent")
        if self.prediction_produced and (
            type(self.feature_event_at_ns) is not int
            or type(self.feature_available_at_ns) is not int
            or self.feature_event_at_ns > self.feature_available_at_ns
            or self.feature_available_at_ns > self.decision_at_ns
        ):
            raise IntegrityError("V5 prediction lacks a causal feature timestamp")
        if self.terminal_disposition == "ADMITTED_TRADE" and (
            type(self.order_submitted_at_ns) is not int
            or type(self.fill_at_ns) is not int
            or not self.decision_at_ns < self.order_submitted_at_ns <= self.fill_at_ns
            or self.outcome_coverage not in {
                "COMPLETE", "STRESS_COMPLETE_PARTIAL_DIAGNOSTICS"
            }
        ):
            raise IntegrityError("V5 admitted trade lacks causal order/fill evidence")


@dataclass(frozen=True)
class MaterializedRowV5:
    expected: ExpectedCheckpoint
    ledger: OpportunityRecordV5
    features: Mapping[str, float] | None
    atr: Decimal | None
    source_row_sha256: str | None
    outcomes: Mapping[str, DirectionOutcomes] | None
    execution_path: tuple[CausalBar, ...] = ()
    market_spec: MarketSpec | None = None
    risk_eligible: bool = True


def reconcile_v5_opportunity_ledger(
    *, expected_ids: Sequence[str], records: Sequence[OpportunityRecordV5],
) -> dict[str, int]:
    for record in records:
        record.validate()
    if len(expected_ids) != len(set(expected_ids)) or len(records) != len(expected_ids):
        raise IntegrityError("V5 opportunity ledger count does not reconcile")
    if {record.opportunity_id for record in records} != set(expected_ids):
        raise IntegrityError("V5 opportunity ledger identity does not reconcile")
    return {
        "expected": len(expected_ids),
        "predictions": sum(record.prediction_produced for record in records),
        "pre_prediction_abstentions": sum(
            record.terminal_disposition in V5_PRE_PREDICTION for record in records
        ),
    }


REQUIRED_PARQUET_COLUMNS = frozenset(
    {
        "actual_identity_hash", "close_nano", "disposition",
        "event_at_ns", "exchange_session_date", "high_nano", "low_nano",
        "open_nano", "point_value", "source_row_sha256", "tick_size",
        "tick_value", "volume",
    }
)


def iter_source_records_from_parquet(
    *, market: str, path: Path, batch_size: int = 65_536,
) -> Iterator[V5SourceRecord]:
    """Bounded-memory reader. Authorization must be verified before calling it."""

    if market not in MARKETS or batch_size < 1 or batch_size > 65_536:
        raise IntegrityError("parquet stream request is outside V5 bounds")
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    if not REQUIRED_PARQUET_COLUMNS.issubset(parquet.schema_arrow.names):
        raise IntegrityError("V5 source schema lacks required columns")
    for batch in parquet.iter_batches(
        batch_size=batch_size, columns=sorted(REQUIRED_PARQUET_COLUMNS),
    ):
        columns = batch.to_pydict()
        for index in range(batch.num_rows):
            yield source_record_from_mapping(
                market=market,
                row={name: values[index] for name, values in columns.items()},
            )


def materialize_v5_rows(
    *, source_rows: Sequence[V5SourceRecord], census: Sequence[CensusCheckpoint],
    market_specs: Mapping[str, MarketSpec], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str],
) -> tuple[MaterializedRowV5, ...]:
    """Retain the calendar universe and make every unusable input an abstention."""

    for row in source_rows:
        row.validate()
    required_markets = {item.expected.market for item in census}
    if not required_markets or not required_markets <= set(market_specs):
        raise IntegrityError("V5 market specifications are incomplete")
    for spec in market_specs.values():
        spec.validate()
    expected_ids = [item.expected.opportunity_id for item in census]
    if not expected_ids or len(expected_ids) != len(set(expected_ids)):
        raise IntegrityError("V5 census is empty or duplicated")
    grouped: dict[tuple[str, str], list[V5SourceRecord]] = {}
    for row in source_rows:
        grouped.setdefault((row.market, row.exchange_session_date), []).append(row)
    prediction_scope = set(prediction_scope_sessions)
    fee = Decimal(str(contract["costs"]["fee_per_side_usd"]))  # type: ignore[index]
    output: list[MaterializedRowV5] = []
    for checkpoint in census:
        expected = checkpoint.expected
        if not checkpoint.calendar_open:
            output.append(MaterializedRowV5(
                expected,
                OpportunityRecordV5(
                    expected.opportunity_id, expected.market,
                    expected.exchange_session_date, expected.checkpoint,
                    expected.decision_at_ns, "CALENDAR_CLOSED", False,
                ),
                None, None, None, None,
            ))
            continue
        raw = sorted(
            grouped.get((expected.market, expected.exchange_session_date), ()),
            key=lambda item: item.bar.event_at_ns if item.bar is not None else -1,
        )
        if not raw:
            output.append(MaterializedRowV5(
                expected,
                OpportunityRecordV5(
                    expected.opportunity_id, expected.market,
                    expected.exchange_session_date, expected.checkpoint,
                    expected.decision_at_ns, "MISSING_SOURCE_SESSION", False,
                ),
                None, None, None, None,
            ))
            continue
        event_values = [item.bar.event_at_ns for item in raw if item.bar is not None]
        if len(event_values) != len(set(event_values)):
            output.append(MaterializedRowV5(
                expected,
                OpportunityRecordV5(
                    expected.opportunity_id, expected.market,
                    expected.exchange_session_date, expected.checkpoint,
                    expected.decision_at_ns,
                    "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY", False,
                ),
                None, None, None, None,
            ))
            continue
        executable = [item for item in raw if item.executable]
        converted = [
            SourceMinute(
                item.market, item.exchange_session_date, item.bar,
                float(item.volume), str(item.actual_identity_hash), item.source_row_sha256,
            )
            for item in executable
            if item.bar is not None and item.volume is not None
        ]
        try:
            current_bar = latest_causal_feature_bar(
                bars=[item.bar for item in converted],
                decision_at_ns=expected.decision_at_ns,
            )
            current_index = next(
                index for index, item in enumerate(converted) if item.bar is current_bar
            )
            features, atr = _features(
                converted[current_index - 60 : current_index + 1],
                decision_at_ns=expected.decision_at_ns,
            )
        except (IntegrityError, IndexError, StopIteration):
            output.append(MaterializedRowV5(
                expected,
                OpportunityRecordV5(
                    expected.opportunity_id, expected.market,
                    expected.exchange_session_date, expected.checkpoint,
                    expected.decision_at_ns, "INSUFFICIENT_CAUSAL_HISTORY", False,
                ),
                None, None, None, None,
            ))
            continue
        spec = market_specs[expected.market]
        stress_ticks = cost_ticks(contract=contract, scenario="stress", market=expected.market)
        from .tier1_bracket_post_audit import planned_initial_loss_usd
        planned = planned_initial_loss_usd(
            atr=atr, tick_size=spec.tick_size, tick_value=spec.tick_value,
            round_trip_cost_ticks=stress_ticks, fee_per_side_usd=fee,
        )
        if planned > Decimal("250"):
            predict = expected.exchange_session_date in prediction_scope
            output.append(MaterializedRowV5(
                expected,
                OpportunityRecordV5(
                    expected.opportunity_id, expected.market,
                    expected.exchange_session_date, expected.checkpoint,
                    expected.decision_at_ns,
                    "PREDICTION_PRODUCED" if predict else "TRAINING_OR_PREDICTION_INELIGIBLE",
                    predict, current_bar.event_at_ns, current_bar.available_at_ns,
                    outcome_coverage="NOT_APPLICABLE_RISK_INELIGIBLE",
                ),
                features, atr, converted[current_index].source_row_sha256, None,
                (), spec, False,
            ))
            continue
        future = [
            item for item in converted
            if expected.decision_at_ns + NS_PER_MINUTE <= item.bar.event_at_ns
            <= expected.decision_at_ns + 61 * NS_PER_MINUTE
        ]
        future_contiguous = bool(future) and all(
            future[index].bar.event_at_ns - future[index - 1].bar.event_at_ns == NS_PER_MINUTE
            for index in range(1, len(future))
        )
        outcomes: dict[str, DirectionOutcomes] = {}
        entry_bar = (
            future[0].bar
            if future_contiguous
            and future[0].bar.event_at_ns == expected.decision_at_ns + NS_PER_MINUTE
            else None
        )
        if entry_bar is not None:
            for scenario in ("base", "stress", "extreme"):
                try:
                    ticks = cost_ticks(contract=contract, scenario=scenario, market=expected.market)
                    outcomes[scenario] = DirectionOutcomes(
                        simulate_v4_bracket_fill(
                            direction="long", decision_at_ns=expected.decision_at_ns,
                            entry_bar=entry_bar, path_bars=[item.bar for item in future],
                            atr=atr, tick_size=spec.tick_size, tick_value=spec.tick_value,
                            point_value=spec.point_value, fee_per_side_usd=fee,
                            round_trip_cost_ticks=ticks,
                            maximum_planned_loss_usd=Decimal("250"),
                        ),
                        simulate_v4_bracket_fill(
                            direction="short", decision_at_ns=expected.decision_at_ns,
                            entry_bar=entry_bar, path_bars=[item.bar for item in future],
                            atr=atr, tick_size=spec.tick_size, tick_value=spec.tick_value,
                            point_value=spec.point_value, fee_per_side_usd=fee,
                            round_trip_cost_ticks=ticks,
                            maximum_planned_loss_usd=Decimal("250"),
                        ),
                    )
                except IntegrityError:
                    continue
        predict = expected.exchange_session_date in prediction_scope
        ledger = OpportunityRecordV5(
            expected.opportunity_id, expected.market,
            expected.exchange_session_date, expected.checkpoint,
            expected.decision_at_ns,
            "PREDICTION_PRODUCED" if predict else "TRAINING_OR_PREDICTION_INELIGIBLE",
            predict,
            feature_event_at_ns=current_bar.event_at_ns,
            feature_available_at_ns=current_bar.available_at_ns,
            outcome_coverage=(
                "COMPLETE" if len(outcomes) == 3
                else "STRESS_COMPLETE_PARTIAL_DIAGNOSTICS" if "stress" in outcomes
                else "MISSING"
            ),
        )
        output.append(MaterializedRowV5(
            expected, ledger, features, atr, converted[current_index].source_row_sha256,
            outcomes if "stress" in outcomes else None,
            tuple(item.bar for item in future), spec,
        ))
    reconcile_v5_opportunity_ledger(
        expected_ids=expected_ids, records=[item.ledger for item in output],
    )
    return tuple(output)


def materialize_v5_streams(
    *, streams: Mapping[tuple[str, int], Iterable[V5SourceRecord]],
    census: Sequence[CensusCheckpoint], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str], maximum_session_rows: int = 2_000,
) -> tuple[MaterializedRowV5, ...]:
    """Bound memory to one market session while retaining absent calendar sessions."""

    expected_keys = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    if set(streams) != expected_keys or maximum_session_rows != 2_000:
        raise IntegrityError("V5 source stream coverage or memory bound is invalid")
    census_by_key: dict[tuple[str, int], list[CensusCheckpoint]] = {}
    for checkpoint in census:
        census_by_key.setdefault(
            (checkpoint.expected.market, checkpoint.expected.year), []
        ).append(checkpoint)
    if set(census_by_key) != expected_keys:
        raise IntegrityError("calendar census lacks a market-year")
    output: list[MaterializedRowV5] = []
    for key in sorted(expected_keys):
        market, year = key
        expected = census_by_key[key]
        expected_by_session: dict[str, list[CensusCheckpoint]] = {}
        for checkpoint in expected:
            expected_by_session.setdefault(
                checkpoint.expected.exchange_session_date, []
            ).append(checkpoint)
        seen_sessions: set[str] = set()
        current_session: str | None = None
        buffer: list[V5SourceRecord] = []
        spec: MarketSpec | None = None
        prior_event = -1

        def flush() -> None:
            nonlocal buffer, current_session
            if current_session is None:
                return
            seen_sessions.add(current_session)
            session_census = expected_by_session.get(current_session)
            if session_census is not None:
                if spec is None:
                    raise IntegrityError("V5 source stream lacks market economics")
                output.extend(materialize_v5_rows(
                    source_rows=tuple(buffer), census=tuple(session_census),
                    market_specs={market: spec}, contract=contract,
                    prediction_scope_sessions=prediction_scope_sessions,
                ))
            buffer = []

        for record in streams[key]:
            record.validate()
            if record.market != market or date.fromisoformat(record.exchange_session_date).year != year:
                raise IntegrityError("V5 source stream row is outside its binding")
            if record.bar is not None:
                if record.bar.event_at_ns < prior_event:
                    raise IntegrityError("V5 source stream is not chronological")
                prior_event = record.bar.event_at_ns
            if record.market_spec is not None:
                if spec is None:
                    spec = record.market_spec
                elif spec != record.market_spec:
                    raise IntegrityError("V5 market economics vary inside a source stream")
            if current_session is None:
                current_session = record.exchange_session_date
            elif record.exchange_session_date != current_session:
                if record.exchange_session_date < current_session:
                    raise IntegrityError("V5 source session labels are not chronological")
                flush()
                current_session = record.exchange_session_date
            buffer.append(record)
            if len(buffer) > maximum_session_rows:
                raise IntegrityError("V5 source session exceeds the registered memory bound")
        flush()
        if spec is None:
            raise IntegrityError("V5 source market-year contains no valid economics")
        for session in sorted(set(expected_by_session) - seen_sessions):
            output.extend(materialize_v5_rows(
                source_rows=(), census=tuple(expected_by_session[session]),
                market_specs={market: spec}, contract=contract,
                prediction_scope_sessions=prediction_scope_sessions,
            ))
    output.sort(
        key=lambda row: (
            row.expected.exchange_session_date, row.expected.checkpoint,
            MARKETS.index(row.expected.market),
        )
    )
    reconcile_v5_opportunity_ledger(
        expected_ids=[item.expected.opportunity_id for item in census],
        records=[item.ledger for item in output],
    )
    return tuple(output)


def build_v5_folds_from_census(census: Sequence[CensusCheckpoint]) -> tuple[FoldSpec, ...]:
    return build_v4_folds_from_census([item.expected for item in census])


def fit_predict_v5(
    *, rows: Sequence[MaterializedRowV5], folds: Sequence[FoldSpec],
) -> ModelFitResult:
    """Reuse the audited market-specific ridge math and give it a V5 identity."""

    result = fit_predict_v4(rows=rows, folds=folds)  # type: ignore[arg-type]
    core = {
        **dict(result.canonical_model_payload),
        "schema_version": "tier1_bracket_successor_v5_models/1.0.0",
        "pre_prediction_risk_cap_usd": "250",
        "source_eligibility": "DISPOSITION_GATED",
        "training_statistic_lineage": "NESTED_CROSSFIT_REQUIRED_FOR_POWER",
    }
    core.pop("model_bundle_id", None)
    return ModelFitResult(
        {**core, "model_bundle_id": sha256_json(core)},
        result.predictions, result.training_outcome_exclusions,
    )


@dataclass(frozen=True)
class PlannedStrategyV5:
    strategy: str
    trades: tuple[PlannedTradeV5, ...]
    preliminary_terminals: Mapping[str, str]


def plan_strategy_v5(
    *, strategy: str, predictions: Sequence[FrozenPrediction],
    rows: Sequence[MaterializedRowV5], scenario: str,
) -> PlannedStrategyV5:
    if scenario not in {"base", "stress", "extreme"}:
        raise IntegrityError("V5 scenario is invalid")
    rows_by_id = {row.expected.opportunity_id: row for row in rows}
    if len(rows_by_id) != len(rows):
        raise IntegrityError("V5 materialized rows are duplicated")
    terminals: dict[str, str] = {}
    grouped: dict[tuple[str, str], list[PlannedTradeV5]] = {}
    if strategy == "flat_no_trade":
        return PlannedStrategyV5(
            strategy, (), {prediction.opportunity_id: "FLAT_NO_TRADE" for prediction in predictions},
        )
    for prediction in predictions:
        signal = _strategy_signal(prediction, strategy)
        if signal is None:
            terminals[prediction.opportunity_id] = "HURDLE_FAILURE"
            continue
        row = rows_by_id.get(prediction.opportunity_id)
        if row is None:
            raise IntegrityError("V5 prediction lacks its materialized row")
        if not row.risk_eligible:
            terminals[prediction.opportunity_id] = "RISK_CAP_REJECTION"
            continue
        if (
            row.outcomes is None or scenario not in row.outcomes
            or row.market_spec is None or not row.execution_path
        ):
            terminals[prediction.opportunity_id] = "MISSING_PRICE_PATH"
            continue
        direction, score = signal
        fill = getattr(row.outcomes[scenario], direction)
        if fill.planned_initial_loss_usd > Decimal("250"):
            raise IntegrityError("post-prediction fill escaped the pre-prediction risk cap")
        trade = PlannedTradeV5(
            prediction.opportunity_id, prediction.market, prediction.year,
            prediction.session, prediction.checkpoint, direction,
            Decimal(str(score)), fill, row.execution_path, row.market_spec,
        )
        grouped.setdefault((prediction.session, prediction.checkpoint), []).append(trade)
    selected: list[PlannedTradeV5] = []
    for key in sorted(grouped):
        ordered = sorted(
            grouped[key],
            key=lambda item: (-item.ranking_score, MARKETS.index(item.market)),
        )
        selected.append(ordered[0])
        for loser in ordered[1:]:
            terminals[loser.opportunity_id] = "CROSS_MARKET_RANKING_LOSS"
    return PlannedStrategyV5(strategy, tuple(selected), dict(sorted(terminals.items())))


def evaluate_strategies_v5(
    *, predictions: Sequence[FrozenPrediction], rows: Sequence[MaterializedRowV5],
    strategies: Sequence[str],
) -> Mapping[str, Mapping[str, AccountPathV5]]:
    prediction_ids = tuple(prediction.opportunity_id for prediction in predictions)
    if len(prediction_ids) != len(set(prediction_ids)):
        raise IntegrityError("V5 frozen predictions are duplicated")
    result: dict[str, Mapping[str, AccountPathV5]] = {}
    for scenario in ("base", "stress", "extreme"):
        plans = {
            strategy: plan_strategy_v5(
                strategy=strategy, predictions=predictions, rows=rows,
                scenario=scenario,
            )
            for strategy in strategies
        }
        paths = simulate_independent_strategy_paths_v5(
            plans_by_strategy={name: plan.trades for name, plan in plans.items()},
            opportunity_ids_by_strategy={name: prediction_ids for name in plans},
        )
        reconciled: dict[str, AccountPathV5] = {}
        for name, path in paths.items():
            terminals = dict(path.terminal_dispositions)
            for opportunity_id, disposition in plans[name].preliminary_terminals.items():
                if terminals[opportunity_id] == "NO_SIGNAL":
                    terminals[opportunity_id] = disposition
            if set(terminals) != set(prediction_ids):
                raise IntegrityError("V5 strategy terminal ledger does not reconcile")
            reconciled[name] = AccountPathV5(
                path.strategy, path.admitted, dict(sorted(terminals.items())),
                path.equity_marks, path.session_net_pnl_usd,
                path.ending_equity_usd, path.maximum_continuous_drawdown_usd,
                path.complete,
            )
        result[scenario] = reconciled
    return result


REQUIRED_ACTIVE_STRATEGIES_V5 = (
    "candidate",
    "flat_no_trade",
    "fold_local_unconditional_return_by_market_session",
    "previous_bar_sign_momentum",
    "previous_bar_sign_reversal",
    "risk_matched_always_long_intraday",
    "candidate_signal_market_order_ranking_ablation",
)


@dataclass(frozen=True)
class CrossfitEvidenceBundleV5:
    sessions: tuple[str, ...]
    fold_ids: tuple[int, ...]
    differential_returns: Mapping[str, tuple[float, ...]]
    sleeve_returns: Mapping[str, tuple[float, ...]]


def build_nested_crossfit_evidence_v5(
    *, rows: Sequence[MaterializedRowV5],
) -> CrossfitEvidenceBundleV5:
    """Generate training power evidence with the same realized policy statistic."""

    sessions = sorted(
        {
            row.expected.exchange_session_date for row in rows
            if row.expected.year in {2018, 2019}
        }
    )
    seed_size = max(30, math.ceil(len(sessions) * 0.40))
    evaluation_sessions = sessions[seed_size:]
    if seed_size >= len(sessions) or len(evaluation_sessions) < 8:
        raise IntegrityError("training history cannot support nested crossfit")
    quotient, remainder = divmod(len(evaluation_sessions), 8)
    folds: list[FoldSpec] = []
    owners: dict[str, int] = {}
    start = 0
    for index in range(8):
        size = quotient + (1 if index < remainder else 0)
        test = evaluation_sessions[start : start + size]
        first = sessions.index(test[0])
        training = sessions[: first - 1]
        if not training:
            raise IntegrityError("nested crossfit embargo leaves no training history")
        folds.append(FoldSpec(index, tuple(training), tuple(test)))
        owners.update({session: index for session in test})
        start += size
    crossfit_rows: list[MaterializedRowV5] = []
    for row in rows:
        session = row.expected.exchange_session_date
        predict = session in owners and row.features is not None and row.outcomes is not None
        if predict:
            if (
                row.ledger.feature_event_at_ns is None
                or row.ledger.feature_available_at_ns is None
            ):
                raise IntegrityError("nested crossfit row lacks causal feature lineage")
            ledger = OpportunityRecordV5(
                row.expected.opportunity_id, row.expected.market,
                row.expected.exchange_session_date, row.expected.checkpoint,
                row.expected.decision_at_ns, "PREDICTION_PRODUCED", True,
                row.ledger.feature_event_at_ns,
                row.ledger.feature_available_at_ns,
                outcome_coverage=row.ledger.outcome_coverage,
            )
            crossfit_rows.append(replace(row, ledger=ledger))
        else:
            crossfit_rows.append(row)
    model = fit_predict_v5(rows=crossfit_rows, folds=folds)
    evaluation = evaluate_strategies_v5(
        predictions=model.predictions, rows=crossfit_rows,
        strategies=REQUIRED_ACTIVE_STRATEGIES_V5,
    )["stress"]
    ordered_sessions = tuple(evaluation_sessions)
    candidate = evaluation["candidate"]
    differential: dict[str, tuple[float, ...]] = {}
    for baseline in REQUIRED_ACTIVE_STRATEGIES_V5[1:]:
        differential[baseline] = tuple(
            float(
                (
                    candidate.session_net_pnl_usd.get(session, Decimal("0"))
                    - evaluation[baseline].session_net_pnl_usd.get(session, Decimal("0"))
                ) / Decimal("100000")
            )
            for session in ordered_sessions
        )
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in MARKETS for checkpoint in CHECKPOINTS
        for direction in ("long", "short")
    )
    contributions = {
        sleeve: {session: Decimal("0") for session in ordered_sessions}
        for sleeve in sleeve_ids
    }
    for trade in candidate.admitted:
        sleeve = f"{trade.market}/{trade.checkpoint}/{trade.direction}"
        contributions[sleeve][trade.session] += trade.fill.net_pnl_usd
    sleeve_returns = {
        sleeve: tuple(
            float(contributions[sleeve][session] / Decimal("100000"))
            for session in ordered_sessions
        )
        for sleeve in sleeve_ids
    }
    return CrossfitEvidenceBundleV5(
        ordered_sessions, tuple(owners[session] for session in ordered_sessions),
        differential, sleeve_returns,
    )


@dataclass(frozen=True)
class ContinuousRiskResult:
    fill: BracketFill | None
    equity_marks: tuple[tuple[int, str, Decimal], ...]
    ending_peak_equity: Decimal
    maximum_drawdown_usd: Decimal
    risk_breach: str | None
    complete: bool


def apply_continuous_risk_v5(
    *, fill: BracketFill, direction: str, path: Sequence[CausalBar],
    spec: MarketSpec, realized_equity: Decimal, prior_peak_equity: Decimal,
    session_start_equity: Decimal, daily_limit: Decimal = Decimal("1000"),
    drawdown_limit: Decimal = Decimal("1500"),
) -> ContinuousRiskResult:
    """Conservative bar-extreme marks, including the ordinary exit bar."""

    if direction not in {"long", "short"} or daily_limit <= 0 or drawdown_limit <= 0:
        raise IntegrityError("continuous risk request is invalid")
    spec.validate()
    executable = sorted((bar for bar in path if bar.executable), key=lambda bar: bar.event_at_ns)
    if not executable:
        return ContinuousRiskResult(None, (), prior_peak_equity, Decimal("0"), None, False)
    sign = Decimal("1") if direction == "long" else Decimal("-1")
    peak = max(prior_peak_equity, realized_equity)
    maximum_drawdown = peak - realized_equity
    marks: list[tuple[int, str, Decimal]] = []
    round_trip_fee = Decimal("10")
    if fill.costs_usd < round_trip_fee:
        raise IntegrityError("fill costs are below the locked round-trip fee")
    slip_ticks = (fill.costs_usd - round_trip_fee) / spec.tick_value
    exit_half_slip = slip_ticks * spec.tick_size / Decimal("2")

    def marked_equity(price: Decimal) -> Decimal:
        executable_exit = price - sign * exit_half_slip
        return (
            realized_equity
            + sign * (executable_exit - fill.entry_price) * spec.point_value
            - round_trip_fee
        )

    for index, bar in enumerate(executable):
        if bar.event_at_ns < fill.entry_at_ns:
            continue
        if bar.event_at_ns > fill.exit_at_ns:
            break
        favorable_price = bar.high_price if direction == "long" else bar.low_price
        adverse_price = bar.low_price if direction == "long" else bar.high_price
        favorable_equity = marked_equity(favorable_price)
        peak = max(peak, favorable_equity)
        marks.append((bar.event_at_ns, "FAVORABLE_EXTREME", favorable_equity))
        adverse_equity = marked_equity(adverse_price)
        marks.append((bar.event_at_ns, "ADVERSE_EXTREME", adverse_equity))
        maximum_drawdown = max(maximum_drawdown, peak - adverse_equity)
        daily_breach = session_start_equity - adverse_equity >= daily_limit
        drawdown_breach = peak - adverse_equity >= drawdown_limit
        if daily_breach or drawdown_breach:
            later = [item for item in executable[index + 1 :] if item.event_at_ns > bar.event_at_ns]
            if not later:
                return ContinuousRiskResult(
                    None, tuple(marks), peak, maximum_drawdown,
                    "DRAWDOWN" if drawdown_breach else "DAILY", False,
                )
            liquidation = later[0]
            liquidation_equity = marked_equity(liquidation.open_price)
            liquidation_exit_price = liquidation.open_price - sign * exit_half_slip
            marks.append((liquidation.event_at_ns, "RISK_LIQUIDATION", liquidation_equity))
            maximum_drawdown = max(maximum_drawdown, peak - liquidation_equity)
            gross = liquidation_equity - realized_equity + fill.costs_usd
            adjusted = BracketFill(
                fill.entry_at_ns, liquidation.event_at_ns, fill.entry_price,
                liquidation_exit_price, fill.stop_price, fill.target_price,
                "RISK_LIQUIDATION_DRAWDOWN" if drawdown_breach else "RISK_LIQUIDATION_DAILY",
                gross, fill.costs_usd, liquidation_equity - realized_equity,
                fill.planned_initial_loss_usd,
            )
            return ContinuousRiskResult(
                adjusted, tuple(marks), peak, maximum_drawdown,
                "DRAWDOWN" if drawdown_breach else "DAILY", True,
            )
    ending_equity = realized_equity + fill.net_pnl_usd
    marks.append((fill.exit_at_ns, "REALIZED_EXIT", ending_equity))
    peak = max(peak, ending_equity)
    maximum_drawdown = max(maximum_drawdown, peak - ending_equity)
    return ContinuousRiskResult(fill, tuple(marks), peak, maximum_drawdown, None, True)


def entries_overlap_v5(
    *, candidate_entry_at_ns: int, prior_exit_at_ns: int,
    prior_exit_proven_at_bar_open: bool = False,
) -> bool:
    if candidate_entry_at_ns < prior_exit_at_ns:
        return True
    if candidate_entry_at_ns == prior_exit_at_ns:
        return not prior_exit_proven_at_bar_open
    return False


@dataclass(frozen=True)
class PlannedTradeV5:
    opportunity_id: str
    market: str
    year: int
    session: str
    checkpoint: str
    direction: str
    ranking_score: Decimal
    fill: BracketFill
    path: tuple[CausalBar, ...]
    market_spec: MarketSpec

    def validate(self) -> None:
        if (
            not self.opportunity_id
            or self.market not in MARKETS
            or self.year not in range(2018, 2023)
            or self.checkpoint not in CHECKPOINTS
            or self.direction not in {"long", "short"}
            or not self.ranking_score.is_finite()
            or self.fill.planned_initial_loss_usd <= 0
        ):
            raise IntegrityError("planned V5 trade is invalid")
        self.market_spec.validate()
        for bar in self.path:
            bar.validate()


@dataclass(frozen=True)
class AccountPathV5:
    strategy: str
    admitted: tuple[PlannedTradeV5, ...]
    terminal_dispositions: Mapping[str, str]
    equity_marks: tuple[tuple[int, str, str, Decimal], ...]
    session_net_pnl_usd: Mapping[str, Decimal]
    ending_equity_usd: Decimal
    maximum_continuous_drawdown_usd: Decimal
    complete: bool


def simulate_account_path_v5(
    *, strategy: str, planned_trades: Sequence[PlannedTradeV5],
    all_opportunity_ids: Sequence[str], starting_capital: Decimal = Decimal("100000"),
) -> AccountPathV5:
    """One strategy, one independent schedule, and one independent risk state."""

    if not strategy or starting_capital <= 0:
        raise IntegrityError("account path identity or capital is invalid")
    all_ids = tuple(all_opportunity_ids)
    if len(all_ids) != len(set(all_ids)):
        raise IntegrityError("account opportunity universe is duplicated")
    for trade in planned_trades:
        trade.validate()
    if not {trade.opportunity_id for trade in planned_trades} <= set(all_ids):
        raise IntegrityError("planned strategy trade is outside its opportunity universe")
    terminals = {item: "NO_SIGNAL" for item in all_ids}
    admitted: list[PlannedTradeV5] = []
    marks: list[tuple[int, str, str, Decimal]] = []
    session_pnl: dict[str, Decimal] = {}
    entries: dict[str, int] = {}
    session_start: dict[str, Decimal] = {}
    daily_blocked: set[str] = set()
    equity = peak = starting_capital
    maximum_drawdown = Decimal("0")
    open_until = -1
    global_drawdown_blocked = False
    complete = True
    ordered = sorted(
        planned_trades,
        key=lambda item: (item.fill.entry_at_ns, -item.ranking_score, MARKETS.index(item.market)),
    )
    for trade in ordered:
        session_pnl.setdefault(trade.session, Decimal("0"))
        entries.setdefault(trade.session, 0)
        session_start.setdefault(trade.session, equity)
        if trade.fill.planned_initial_loss_usd > Decimal("250"):
            terminals[trade.opportunity_id] = "RISK_CAP_REJECTION"
            continue
        if entries_overlap_v5(
            candidate_entry_at_ns=trade.fill.entry_at_ns,
            prior_exit_at_ns=open_until,
        ):
            terminals[trade.opportunity_id] = "OVERLAP_ABSTENTION"
            continue
        if trade.session in daily_blocked:
            terminals[trade.opportunity_id] = "DAILY_STOP_ABSTENTION"
            continue
        if global_drawdown_blocked:
            terminals[trade.opportunity_id] = "DRAWDOWN_ABSTENTION"
            continue
        if entries[trade.session] >= 3:
            terminals[trade.opportunity_id] = "ENTRY_CAP_ABSTENTION"
            continue
        risk = apply_continuous_risk_v5(
            fill=trade.fill, direction=trade.direction, path=trade.path,
            spec=trade.market_spec, realized_equity=equity,
            prior_peak_equity=peak,
            session_start_equity=session_start[trade.session],
        )
        for at_ns, kind, marked_equity in risk.equity_marks:
            marks.append((at_ns, trade.opportunity_id, kind, marked_equity))
        peak = max(peak, risk.ending_peak_equity)
        maximum_drawdown = max(maximum_drawdown, risk.maximum_drawdown_usd)
        if not risk.complete or risk.fill is None:
            terminals[trade.opportunity_id] = "INCOMPLETE_RISK_LIQUIDATION_PATH"
            complete = False
            continue
        accepted = PlannedTradeV5(
            trade.opportunity_id, trade.market, trade.year, trade.session,
            trade.checkpoint, trade.direction, trade.ranking_score,
            risk.fill, trade.path, trade.market_spec,
        )
        admitted.append(accepted)
        terminals[trade.opportunity_id] = "ADMITTED_TRADE"
        entries[trade.session] += 1
        equity += risk.fill.net_pnl_usd
        session_pnl[trade.session] += risk.fill.net_pnl_usd
        open_until = risk.fill.exit_at_ns
        if risk.risk_breach == "DAILY":
            daily_blocked.add(trade.session)
        if risk.risk_breach == "DRAWDOWN" or peak - equity >= Decimal("1500"):
            global_drawdown_blocked = True
    if set(terminals) != set(all_ids):
        raise IntegrityError("account path terminal ledger does not reconcile")
    return AccountPathV5(
        strategy, tuple(admitted), dict(sorted(terminals.items())), tuple(marks),
        dict(sorted(session_pnl.items())), equity, maximum_drawdown, complete,
    )


def simulate_independent_strategy_paths_v5(
    *, plans_by_strategy: Mapping[str, Sequence[PlannedTradeV5]],
    opportunity_ids_by_strategy: Mapping[str, Sequence[str]],
) -> Mapping[str, AccountPathV5]:
    if set(plans_by_strategy) != set(opportunity_ids_by_strategy):
        raise IntegrityError("independent strategy universes are incomplete")
    return {
        strategy: simulate_account_path_v5(
            strategy=strategy, planned_trades=tuple(plans_by_strategy[strategy]),
            all_opportunity_ids=tuple(opportunity_ids_by_strategy[strategy]),
        )
        for strategy in sorted(plans_by_strategy)
    }


def segmented_account_views_v5(
    *, strategy: str, planned_trades: Sequence[PlannedTradeV5],
    opportunity_market_year: Mapping[str, tuple[str, int]],
) -> Mapping[str, AccountPathV5]:
    """Reset each market-year diagnostic so earlier drawdown cannot censor it."""

    by_id = {trade.opportunity_id: trade for trade in planned_trades}
    if not set(by_id) <= set(opportunity_market_year):
        raise IntegrityError("segmented plan is outside its opportunity metadata")
    result: dict[str, AccountPathV5] = {}
    for market in MARKETS:
        for year in range(2020, 2023):
            key = f"{market}/{year}"
            ids = tuple(
                item for item, identity in opportunity_market_year.items()
                if identity == (market, year)
            )
            trades = tuple(by_id[item] for item in ids if item in by_id)
            if ids:
                result[key] = simulate_account_path_v5(
                    strategy=f"{strategy}:{key}", planned_trades=trades,
                    all_opportunity_ids=ids,
                )
    return result


@dataclass(frozen=True)
class CoverageEvidence:
    expected: int
    terminal: int
    causal_feature_expected: int
    causal_feature_eligible: int
    predictions: int
    market_year_expected: Mapping[str, int]
    market_year_feature_eligible: Mapping[str, int]


def evaluate_coverage_gate(evidence: CoverageEvidence) -> dict[str, object]:
    keys = set(evidence.market_year_expected)
    required_keys = {
        f"{market}/{year}" for market in MARKETS for year in range(2020, 2023)
    }
    if (
        evidence.expected <= 0
        or evidence.causal_feature_expected <= 0
        or evidence.terminal < 0
        or evidence.causal_feature_eligible < 0
        or evidence.predictions < 0
        or evidence.terminal > evidence.expected
        or evidence.causal_feature_eligible > evidence.causal_feature_expected
        or evidence.predictions > evidence.causal_feature_eligible
        or keys != required_keys
        or keys != set(evidence.market_year_feature_eligible)
        or any(evidence.market_year_expected[key] <= 0 for key in keys)
        or sum(evidence.market_year_expected.values()) != evidence.causal_feature_expected
        or sum(evidence.market_year_feature_eligible.values()) != evidence.causal_feature_eligible
    ):
        return {"status": "INVALID", "passed": False}
    terminal_rate = evidence.terminal / evidence.expected
    feature_rate = evidence.causal_feature_eligible / evidence.causal_feature_expected
    prediction_rate = (
        evidence.predictions / evidence.causal_feature_eligible
        if evidence.causal_feature_eligible else 0.0
    )
    market_year_rates = {
        key: evidence.market_year_feature_eligible[key] / evidence.market_year_expected[key]
        for key in sorted(keys)
    }
    passed = (
        terminal_rate == 1.0
        and feature_rate >= 0.95
        and prediction_rate >= 0.99
        and min(market_year_rates.values()) >= 0.90
    )
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_COVERAGE",
        "passed": passed,
        "terminal_rate": terminal_rate,
        "causal_feature_rate": feature_rate,
        "prediction_rate": prediction_rate,
        "market_year_feature_rates": market_year_rates,
    }


@dataclass(frozen=True)
class PowerEvidenceV5:
    resamples: int
    alternative_mean: float
    estimated_power: float
    adequately_powered: bool


@dataclass(frozen=True)
class CrossfitStatisticSeriesV5:
    statistic_name: str
    partition_role: str
    values: tuple[float, ...]
    fold_ids: tuple[int, ...]

    def validate(self, *, expected_statistic: str) -> None:
        if (
            self.statistic_name != expected_statistic
            or self.partition_role != "NESTED_CHRONOLOGICAL_CROSSFIT_TRAIN"
            or len(self.values) < 30
            or len(self.values) != len(self.fold_ids)
            or len(set(self.fold_ids)) < 2
            or not all(math.isfinite(item) for item in self.values)
        ):
            raise IntegrityError("training power statistic does not match evaluation")


def training_bootstrap_power_v5(
    training_differential_returns: Sequence[float], *,
    planned_evaluation_observations: int, alternative_mean: float,
    resamples: int = 5000, mean_block_length: int = 10, seed: int = 0,
) -> PowerEvidenceV5:
    """Use all declared resamples on the same candidate-minus-baseline statistic."""

    values = np.asarray(training_differential_returns, dtype=np.float64)
    if (
        len(values) < 30 or planned_evaluation_observations < 30
        or resamples != 5000 or mean_block_length <= 0
        or not np.all(np.isfinite(values)) or alternative_mean <= 0
    ):
        raise IntegrityError("training-only power inputs violate the V5 contract")
    centered = values - float(np.mean(values))
    rng = np.random.default_rng(seed)
    reject = 0
    probability = 1.0 / float(mean_block_length)
    chunk_size = 500
    for chunk_start in range(0, resamples, chunk_size):
        count = min(chunk_size, resamples - chunk_start)
        random_starts = rng.integers(
            0, len(centered), size=(count, planned_evaluation_observations),
            dtype=np.int64,
        )
        restarts = rng.random((count, planned_evaluation_observations)) < probability
        restarts[:, 0] = True
        indices = np.empty_like(random_starts)
        indices[:, 0] = random_starts[:, 0]
        for position in range(1, planned_evaluation_observations):
            indices[:, position] = np.where(
                restarts[:, position], random_starts[:, position],
                (indices[:, position - 1] + 1) % len(centered),
            )
        samples = centered[indices] + alternative_mean
        standard_errors = np.std(samples, axis=1, ddof=1) / math.sqrt(
            planned_evaluation_observations
        )
        means = np.mean(samples, axis=1)
        statistics = np.divide(
            means, standard_errors,
            out=np.full_like(means, -np.inf), where=standard_errors > 0,
        )
        reject += int(np.count_nonzero(statistics > 1.6448536269514722))
    power = reject / resamples
    return PowerEvidenceV5(resamples, alternative_mean, power, power >= 0.80)


def portfolio_training_power_v5(
    series: CrossfitStatisticSeriesV5, *, planned_evaluation_observations: int,
    seed: int,
) -> PowerEvidenceV5:
    series.validate(expected_statistic="CANDIDATE_MINUS_REQUIRED_BASELINE_SESSION_RETURN")
    return training_bootstrap_power_v5(
        series.values, planned_evaluation_observations=planned_evaluation_observations,
        alternative_mean=30.0 / 100_000.0, resamples=5000,
        mean_block_length=10, seed=seed,
    )


def sleeve_training_power_v5(
    series: CrossfitStatisticSeriesV5, *, planned_evaluation_observations: int,
    seed: int,
) -> PowerEvidenceV5:
    series.validate(expected_statistic="REALIZED_POLICY_SLEEVE_SESSION_RETURN")
    return training_bootstrap_power_v5(
        series.values, planned_evaluation_observations=planned_evaluation_observations,
        alternative_mean=1.25 / 100_000.0, resamples=5000,
        mean_block_length=10, seed=seed,
    )


@dataclass(frozen=True)
class BootstrapSensitivityV5:
    mean_block_length: int
    resamples: int
    lower_one_sided_95: float
    lower_two_sided_95: float
    upper_two_sided_95: float


def bootstrap_sensitivity_v5(
    values: Sequence[float], *, seed: int, resamples: int = 10_000,
) -> tuple[BootstrapSensitivityV5, ...]:
    array = np.asarray(values, dtype=np.float64)
    if len(array) < 30 or not np.all(np.isfinite(array)) or resamples != 10_000:
        raise IntegrityError("bootstrap sensitivity inputs violate registration")
    results: list[BootstrapSensitivityV5] = []
    for block_length in block_sensitivity_plan_v5():
        rng = np.random.default_rng(seed + block_length)
        means = np.empty(resamples, dtype=np.float64)
        probability = 1.0 / block_length
        chunk_size = 500
        for chunk_start in range(0, resamples, chunk_size):
            count = min(chunk_size, resamples - chunk_start)
            random_starts = rng.integers(
                0, len(array), size=(count, len(array)), dtype=np.int64,
            )
            restarts = rng.random((count, len(array))) < probability
            restarts[:, 0] = True
            indices = np.empty_like(random_starts)
            indices[:, 0] = random_starts[:, 0]
            for position in range(1, len(array)):
                indices[:, position] = np.where(
                    restarts[:, position], random_starts[:, position],
                    (indices[:, position - 1] + 1) % len(array),
                )
            means[chunk_start : chunk_start + count] = np.mean(
                array[indices], axis=1
            )
        results.append(
            BootstrapSensitivityV5(
                block_length, resamples,
                float(np.quantile(means, 0.05)),
                float(np.quantile(means, 0.025)),
                float(np.quantile(means, 0.975)),
            )
        )
    return tuple(results)


def classify_power_and_effect_v5(
    *, power_adequate: bool, complete_clusters: int,
    effect_mean_usd: Decimal, confidence_upper_usd: Decimal,
    confidence_lower_usd: Decimal, mees_usd: Decimal,
) -> str:
    if confidence_upper_usd <= 0 or effect_mean_usd <= 0:
        return "FAIL_NO_EDGE"
    if confidence_upper_usd <= mees_usd or effect_mean_usd <= mees_usd:
        return "FAIL_NOT_ECONOMIC"
    if complete_clusters < 30 or not power_adequate:
        return "INCONCLUSIVE_DATA_OR_POWER"
    if confidence_lower_usd <= mees_usd:
        return "INCONCLUSIVE_EFFECT"
    return "PASS_EFFECT_GATE"


def _hac_lag_v5(observations: int) -> int:
    if observations < 3:
        raise IntegrityError("too few observations for HAC")
    return max(1, min(10, int(math.floor(4 * (observations / 100) ** (2 / 9)))))


def derive_v5_decision(
    *, evaluation: Mapping[str, Mapping[str, AccountPathV5]],
    evaluation_sessions: Sequence[str], coverage: CoverageEvidence,
    crossfit: CrossfitEvidenceBundleV5, seed: int,
) -> dict[str, object]:
    """One ordered promotion decision with power, effect, and controls separated."""

    coverage_result = evaluate_coverage_gate(coverage)
    if coverage_result["status"] == "INVALID":
        core = {
            "schema_version": "tier1_bracket_successor_v5_decision/1.0.0",
            "classification": "INVALID", "coverage": coverage_result,
        }
        return {**core, "decision_id": sha256_json(core)}
    if set(evaluation) != {"base", "stress", "extreme"}:
        raise IntegrityError("V5 scenario evaluation set is incomplete")
    stress = evaluation["stress"]
    if set(stress) != set(REQUIRED_ACTIVE_STRATEGIES_V5):
        raise IntegrityError("V5 required strategy set is incomplete")
    sessions = tuple(sorted(set(evaluation_sessions)))
    if len(sessions) < 30 or len(sessions) != len(evaluation_sessions):
        raise IntegrityError("V5 evaluation sessions are insufficient or duplicated")
    candidate_path = stress["candidate"]
    candidate = np.asarray(
        [
            float(candidate_path.session_net_pnl_usd.get(session, Decimal("0")) / Decimal("100000"))
            for session in sessions
        ],
        dtype=np.float64,
    )
    sensitivity = bootstrap_sensitivity_v5(candidate, seed=seed, resamples=10_000)
    conservative_lower = min(item.lower_one_sided_95 for item in sensitivity)
    conservative_upper = max(item.upper_two_sided_95 for item in sensitivity)
    candidate_mean_usd = Decimal(str(float(np.mean(candidate)) * 100000))
    baseline_ids = REQUIRED_ACTIVE_STRATEGIES_V5[1:]
    differentials = np.column_stack(
        [
            candidate - np.asarray(
                [
                    float(stress[name].session_net_pnl_usd.get(session, Decimal("0")) / Decimal("100000"))
                    for session in sessions
                ], dtype=np.float64,
            )
            for name in baseline_ids
        ]
    )
    lag = _hac_lag_v5(len(sessions))
    try:
        baseline_rw = romano_wolf_from_differentials(
            differentials, hypothesis_ids=baseline_ids, hac_lag=lag,
            mean_block_length=10.0, n_resamples=10_000, seed=seed + 101,
            minimum_resamples=10_000,
        )
    except ResearchContractError:
        baseline_rw_status = "DEGENERATE_OR_CONTRACT_FAILURE_FAIL_CLOSED"
        baseline_adjusted_p = np.ones(len(baseline_ids), dtype=np.float64)
    else:
        baseline_rw_status = "OK"
        baseline_adjusted_p = baseline_rw.adjusted_p_values
    portfolio_power: dict[str, PowerEvidenceV5] = {}
    for offset, baseline in enumerate(baseline_ids):
        series = CrossfitStatisticSeriesV5(
            "CANDIDATE_MINUS_REQUIRED_BASELINE_SESSION_RETURN",
            "NESTED_CHRONOLOGICAL_CROSSFIT_TRAIN",
            tuple(crossfit.differential_returns[baseline]), crossfit.fold_ids,
        )
        portfolio_power[baseline] = portfolio_training_power_v5(
            series, planned_evaluation_observations=len(sessions),
            seed=seed + 200 + offset,
        )
    portfolio_power_adequate = all(item.adequately_powered for item in portfolio_power.values())
    effect_classification = classify_power_and_effect_v5(
        power_adequate=portfolio_power_adequate,
        complete_clusters=len(sessions), effect_mean_usd=candidate_mean_usd,
        confidence_upper_usd=Decimal(str(conservative_upper * 100000)),
        confidence_lower_usd=Decimal(str(conservative_lower * 100000)),
        mees_usd=Decimal("20"),
    )
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in MARKETS for checkpoint in CHECKPOINTS for direction in ("long", "short")
    )
    evaluation_contributions = {
        sleeve: {session: Decimal("0") for session in sessions} for sleeve in sleeve_ids
    }
    for trade in candidate_path.admitted:
        sleeve = f"{trade.market}/{trade.checkpoint}/{trade.direction}"
        evaluation_contributions[sleeve][trade.session] += trade.fill.net_pnl_usd
    sleeve_matrix = np.column_stack(
        [
            np.asarray(
                [
                    float(evaluation_contributions[sleeve][session] / Decimal("100000"))
                    - 0.8333333333333333 / 100000.0
                    for session in sessions
                ], dtype=np.float64,
            )
            for sleeve in sleeve_ids
        ]
    )
    try:
        sleeve_rw = romano_wolf_from_differentials(
            sleeve_matrix, hypothesis_ids=sleeve_ids, hac_lag=lag,
            mean_block_length=10.0, n_resamples=10_000, seed=seed + 303,
            minimum_resamples=10_000,
        )
    except ResearchContractError:
        sleeve_rw_status = "DEGENERATE_OR_CONTRACT_FAILURE_FAIL_CLOSED"
        sleeve_adjusted_p = np.ones(len(sleeve_ids), dtype=np.float64)
    else:
        sleeve_rw_status = "OK"
        sleeve_adjusted_p = sleeve_rw.adjusted_p_values
    sleeve_results: dict[str, object] = {}
    sleeves_pass = True
    sleeve_effect_classifications: list[str] = []
    for offset, sleeve in enumerate(sleeve_ids):
        training = CrossfitStatisticSeriesV5(
            "REALIZED_POLICY_SLEEVE_SESSION_RETURN",
            "NESTED_CHRONOLOGICAL_CROSSFIT_TRAIN",
            tuple(crossfit.sleeve_returns[sleeve]), crossfit.fold_ids,
        )
        power = sleeve_training_power_v5(
            training, planned_evaluation_observations=len(sessions),
            seed=seed + 400 + offset,
        )
        values = sleeve_matrix[:, offset] + 0.8333333333333333 / 100000.0
        try:
            hac = newey_west_mean(values, lag=lag)
        except ResearchContractError:
            hac = None
            effect = "INCONCLUSIVE_DATA_OR_POWER"
        else:
            effect = classify_power_and_effect_v5(
                power_adequate=power.adequately_powered and hac.status == "OK",
                complete_clusters=len(values),
                effect_mean_usd=Decimal(str(hac.mean * 100000)),
                confidence_upper_usd=Decimal(str((hac.mean + 1.96 * hac.standard_error) * 100000)),
                confidence_lower_usd=Decimal(str((hac.mean - 1.96 * hac.standard_error) * 100000)),
                mees_usd=Decimal("0.8333333333333333333333333333"),
            )
        adjusted_p = float(sleeve_adjusted_p[offset])
        passed = effect == "PASS_EFFECT_GATE" and adjusted_p <= 0.05
        sleeve_effect_classifications.append(effect)
        sleeves_pass = sleeves_pass and passed
        sleeve_results[sleeve] = {
            "power": power.estimated_power, "effect_classification": effect,
            "adjusted_p": adjusted_p, "passed": passed,
            "hac_status": hac.status if hac is not None else "CONTRACT_FAILURE",
        }
    candidate_net = candidate_path.ending_equity_usd - Decimal("100000")
    baseline_nets = {
        name: stress[name].ending_equity_usd - Decimal("100000") for name in baseline_ids
    }
    baseline_pass = (
        candidate_net > 0
        and all(candidate_net > value for value in baseline_nets.values())
        and bool(np.all(baseline_adjusted_p <= 0.05))
    )
    by_market_year: dict[tuple[str, int], Decimal] = {}
    by_year: dict[int, Decimal] = {}
    for trade in candidate_path.admitted:
        key = (trade.market, trade.year)
        by_market_year[key] = by_market_year.get(key, Decimal("0")) + trade.fill.net_pnl_usd
        by_year[trade.year] = by_year.get(trade.year, Decimal("0")) + trade.fill.net_pnl_usd
    distribution_pass = (
        sum(value > 0 for value in by_year.values()) >= 2
        and sum(value > 0 for value in by_market_year.values()) >= 6
        and {market for (market, _), value in by_market_year.items() if value > 0} == set(MARKETS)
    )
    drawdown_pass = (
        candidate_path.complete
        and candidate_path.maximum_continuous_drawdown_usd <= Decimal("1500")
    )
    if coverage_result["status"] != "PASS":
        classification = "INCONCLUSIVE_DATA_OR_COVERAGE"
    elif effect_classification != "PASS_EFFECT_GATE":
        classification = effect_classification
    elif "FAIL_NO_EDGE" in sleeve_effect_classifications:
        classification = "FAIL_NO_EDGE"
    elif "FAIL_NOT_ECONOMIC" in sleeve_effect_classifications:
        classification = "FAIL_NOT_ECONOMIC"
    elif "INCONCLUSIVE_DATA_OR_POWER" in sleeve_effect_classifications:
        classification = "INCONCLUSIVE_DATA_OR_POWER"
    elif "INCONCLUSIVE_EFFECT" in sleeve_effect_classifications:
        classification = "INCONCLUSIVE_EFFECT"
    elif not baseline_pass or not sleeves_pass:
        classification = "FAIL_MULTIPLICITY_OR_CONTROL"
    elif not distribution_pass or not drawdown_pass:
        classification = "FAIL_PROMOTION_GATE"
    else:
        classification = "PASS_HISTORICAL_SCREEN"
    core = {
        "schema_version": "tier1_bracket_successor_v5_decision/1.0.0",
        "classification": classification,
        "coverage": coverage_result,
        "candidate_effect_classification": effect_classification,
        "candidate_mean_usd_per_session": str(candidate_mean_usd),
        "bootstrap_sensitivity": [asdict(item) for item in sensitivity],
        "portfolio_power": {name: asdict(value) for name, value in portfolio_power.items()},
        "baseline_romano_wolf_adjusted_p": {
            name: float(baseline_adjusted_p[index])
            for index, name in enumerate(baseline_ids)
        },
        "baseline_romano_wolf_status": baseline_rw_status,
        "sleeve_romano_wolf_status": sleeve_rw_status,
        "sleeves": sleeve_results,
        "continuous_drawdown_usd": str(candidate_path.maximum_continuous_drawdown_usd),
        "distribution_passed": distribution_pass,
        "drawdown_passed": drawdown_pass,
        "stress_and_baselines_passed": baseline_pass,
        "dsr_status": conventional_dsr_status(observed_trial_sharpes=None),
        "legacy_normal_quantile_proxy_used_for_promotion": False,
        "live_readiness": False,
    }
    return {**core, "decision_id": sha256_json(core)}


def block_sensitivity_plan_v5() -> tuple[int, ...]:
    return (5, 10, 20)


def risk_matched_always_long_eligible(*, planned_loss_usd: Decimal) -> bool:
    """The baseline uses its own signal path and the identical hard loss cap."""

    return planned_loss_usd.is_finite() and Decimal("0") < planned_loss_usd <= Decimal("250")


def conventional_dsr_status(*, observed_trial_sharpes: Sequence[float] | None) -> str:
    if observed_trial_sharpes is None or len(observed_trial_sharpes) < 2:
        return "NOT_CLAIMED_MISSING_OBSERVED_HASH_BOUND_TRIAL_SHARPE_CENSUS"
    if not all(math.isfinite(item) for item in observed_trial_sharpes):
        raise IntegrityError("observed trial Sharpe census is invalid")
    return "ELIGIBLE_FOR_CONVENTIONAL_DSR"


def verify_historical_operation_receipt_v5(
    *, boundary: RepoBoundary, receipt: OperationReceipt, trial_id: str,
    source_binding_id: str, output_root: Path,
) -> str:
    if not _hex64(trial_id) or not _hex64(source_binding_id):
        raise UnauthorizedOperation("V5 historical receipt scope is invalid")
    boundary.assert_active_path(
        output_root.absolute(), purpose="V5 historical output root"
    )
    required_scope = {
        "trial_id": trial_id,
        "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
    }
    receipt.verify(
        boundary,
        operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V5_HISTORICAL_SCREEN",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    observed_scope = dict(receipt.scope)
    approval_keys = {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    if (
        set(observed_scope) != set(required_scope) | approval_keys
        or any(observed_scope.get(key) != value for key, value in required_scope.items())
    ):
        raise UnauthorizedOperation("V5 receipt does not grant the exact historical scope")
    if not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("V5 historical operation requires a single-use external receipt")
    return receipt.receipt_id


def claim_historical_operation_receipt_v5(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
) -> Path:
    """Consume the single-use receipt create-only before any source file opens."""

    boundary.assert_active_root(root)
    receipt_id = verify_historical_operation_receipt_v5(
        boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=source_binding_id, output_root=output_root,
    )
    claim = root / "state" / "authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V5 authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "tier1_bracket_v5_authorization_use/1.0.0",
        "receipt_id": receipt_id,
        "trial_id": trial_id,
        "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": False,
    }
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes(payload) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("historical authorization receipt was already consumed") from exc
    return claim


def source_binding_id_v5(source_paths: Mapping[tuple[str, int], Path]) -> str:
    expected = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    if set(source_paths) != expected:
        raise IntegrityError("V5 requires exactly twenty 2018-2022 source paths")
    core = [
        {"market": market, "year": year, "sha256": sha256_file(source_paths[(market, year)])}
        for market, year in sorted(expected)
    ]
    return sha256_json({"sources": core})


def source_binding_id_from_metadata_v5(bindings: Sequence[Mapping[str, object]]) -> str:
    normalized: list[dict[str, object]] = []
    for item in bindings:
        market, year, digest = item.get("market"), item.get("year"), item.get("source_parquet_sha256")
        if market not in MARKETS or type(year) is not int or year not in range(2018, 2023) or not _hex64(digest):
            raise IntegrityError("registered V5 source binding is malformed")
        normalized.append({"market": market, "year": year, "sha256": digest})
    expected = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    if {(str(item["market"]), int(item["year"])) for item in normalized} != expected:
        raise IntegrityError("registered V5 source binding coverage is incomplete")
    return sha256_json({"sources": sorted(normalized, key=lambda item: (str(item["market"]), int(item["year"])))})


def authorized_source_streams_v5(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path],
    output_root: Path,
) -> Mapping[tuple[str, int], Iterable[V5SourceRecord]]:
    """Gate runtime, registration, hashes, and receipt before returning row streams."""

    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before open")
    registry_path = root / V5_REGISTRY_ROOT / f"{trial_id}.json"
    registry = _load_object(registry_path)
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
    ):
        raise UnauthorizedOperation("registered V5 declaration is unavailable")
    require_locked_repository_environment(root)
    raw_bindings = registry.get("source_bindings")
    if not isinstance(raw_bindings, list) or not all(isinstance(item, dict) for item in raw_bindings):
        raise IntegrityError("registered V5 source bindings are absent")
    binding_id = source_binding_id_from_metadata_v5(raw_bindings)
    registered_binding = registry.get("source_binding_id")
    if registered_binding != binding_id:
        raise IntegrityError("source bytes differ from the registered V5 binding")
    claim_historical_operation_receipt_v5(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
    )
    expected_hashes = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw_bindings
    }
    if set(source_paths) != set(expected_hashes):
        raise IntegrityError("authorized source path map differs from registration")
    for key, path in source_paths.items():
        if sha256_file(path) != expected_hashes[key]:
            raise IntegrityError("authorized source bytes differ from registration")
    return {
        key: iter_source_records_from_parquet(market=key[0], path=source_paths[key])
        for key in sorted(source_paths)
    }


def execute_authorized_v5(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path],
    output_root: Path,
) -> V5PipelineResult:
    """Execute in memory after exact authorization; never publish automatically."""

    streams = authorized_source_streams_v5(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
    )
    registry = _load_object(root / V5_REGISTRY_ROOT / f"{trial_id}.json")
    calendar_release_id = registry.get("calendar_release_id")
    if not _hex64(calendar_release_id):
        raise IntegrityError("V5 registration lacks its calendar binding")
    sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(calendar_release_id),
    )
    census = build_expected_census_from_calendar(sessions=sessions)
    runtime = prepare_runtime_receipt_v5(root=root, trial_id=trial_id)
    return run_v5_pipeline(
        streams=streams, census=census, contract=load_v5_contract(root=root),
        trial_id=trial_id, runtime_receipt=runtime,
    )


@dataclass(frozen=True)
class EvidenceArtifactsV5:
    model: Mapping[str, object]
    predictions: Sequence[Mapping[str, object]]
    opportunity_ledger: Sequence[Mapping[str, object]]
    fills: Sequence[Mapping[str, object]]
    continuous_equity_marks: Sequence[Mapping[str, object]]
    segmented_metrics: Mapping[str, object]
    inference: Mapping[str, object]
    decision: Mapping[str, object]
    runtime_receipt: Mapping[str, object]


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


@dataclass(frozen=True)
class V5PipelineResult:
    materialized_rows: tuple[MaterializedRowV5, ...]
    model_fit: ModelFitResult
    evaluation: Mapping[str, Mapping[str, AccountPathV5]]
    segmented: Mapping[str, Mapping[str, AccountPathV5]]
    crossfit: CrossfitEvidenceBundleV5
    coverage: Mapping[str, object]
    decision: Mapping[str, object]
    evidence: EvidenceArtifactsV5


def finalize_candidate_ledger_v5(
    *, rows: Sequence[MaterializedRowV5], candidate_path: AccountPathV5,
) -> tuple[OpportunityRecordV5, ...]:
    admitted = {trade.opportunity_id: trade for trade in candidate_path.admitted}
    records: list[OpportunityRecordV5] = []
    for row in rows:
        record = row.ledger
        if record.prediction_produced:
            disposition = candidate_path.terminal_dispositions.get(
                row.expected.opportunity_id
            )
            if disposition is None:
                raise IntegrityError("candidate evaluation omitted a frozen prediction")
            trade = admitted.get(row.expected.opportunity_id)
            record = replace(
                record, terminal_disposition=disposition,
                order_submitted_at_ns=(
                    row.expected.decision_at_ns + 1 if trade is not None else None
                ),
                fill_at_ns=(trade.fill.entry_at_ns if trade is not None else None),
            )
        record.validate()
        records.append(record)
    reconcile_v5_opportunity_ledger(
        expected_ids=[row.expected.opportunity_id for row in rows],
        records=records,
    )
    return tuple(records)


def run_v5_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[V5SourceRecord]],
    census: Sequence[CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> V5PipelineResult:
    if not _hex64(trial_id):
        raise IntegrityError("V5 pipeline requires a registered trial identity")
    folds = build_v5_folds_from_census(census)
    prediction_sessions = tuple(session for fold in folds for session in fold.test_sessions)
    rows = materialize_v5_streams(
        streams=streams, census=census, contract=contract,
        prediction_scope_sessions=prediction_sessions,
    )
    model = fit_predict_v5(rows=rows, folds=folds)
    evaluation = evaluate_strategies_v5(
        predictions=model.predictions, rows=rows,
        strategies=REQUIRED_ACTIVE_STRATEGIES_V5,
    )
    opportunity_metadata = {
        prediction.opportunity_id: (prediction.market, prediction.year)
        for prediction in model.predictions
    }
    segmented: dict[str, Mapping[str, AccountPathV5]] = {}
    for strategy in REQUIRED_ACTIVE_STRATEGIES_V5:
        plan = plan_strategy_v5(
            strategy=strategy, predictions=model.predictions, rows=rows,
            scenario="stress",
        )
        segmented[strategy] = segmented_account_views_v5(
            strategy=strategy, planned_trades=plan.trades,
            opportunity_market_year=opportunity_metadata,
        )
    prediction_scope = set(prediction_sessions)
    calendar_open_ids = {
        item.expected.opportunity_id for item in census if item.calendar_open
    }
    evaluation_rows = [
        row for row in rows
        if row.expected.exchange_session_date in prediction_scope
    ]
    market_year_expected: dict[str, int] = {}
    market_year_features: dict[str, int] = {}
    for row in evaluation_rows:
        if row.expected.opportunity_id not in calendar_open_ids:
            continue
        key = f"{row.expected.market}/{row.expected.year}"
        market_year_expected[key] = market_year_expected.get(key, 0) + 1
        market_year_features[key] = market_year_features.get(key, 0) + int(row.features is not None)
    coverage_evidence = CoverageEvidence(
        expected=len(evaluation_rows), terminal=len(evaluation_rows),
        causal_feature_expected=sum(
            row.expected.opportunity_id in calendar_open_ids for row in evaluation_rows
        ),
        causal_feature_eligible=sum(row.features is not None for row in evaluation_rows),
        predictions=len(model.predictions),
        market_year_expected=market_year_expected,
        market_year_feature_eligible=market_year_features,
    )
    coverage_result = evaluate_coverage_gate(coverage_evidence)
    crossfit = build_nested_crossfit_evidence_v5(rows=rows)
    seed = int(trial_id[:16], 16)
    decision = derive_v5_decision(
        evaluation=evaluation, evaluation_sessions=prediction_sessions,
        coverage=coverage_evidence, crossfit=crossfit, seed=seed,
    )
    opportunity_ledger = [
        asdict(record) for record in finalize_candidate_ledger_v5(
            rows=rows, candidate_path=evaluation["stress"]["candidate"],
        )
    ]
    fills: list[dict[str, object]] = []
    marks: list[dict[str, object]] = []
    for scenario, paths in evaluation.items():
        for strategy, path in paths.items():
            for trade in path.admitted:
                fills.append({
                    "scenario": scenario, "strategy": strategy,
                    "opportunity_id": trade.opportunity_id,
                    "market": trade.market, "year": trade.year,
                    "session": trade.session, "checkpoint": trade.checkpoint,
                    "direction": trade.direction, "fill": asdict(trade.fill),
                })
            for at_ns, opportunity_id, kind, equity in path.equity_marks:
                marks.append({
                    "scenario": scenario, "strategy": strategy,
                    "at_ns": at_ns, "opportunity_id": opportunity_id,
                    "kind": kind, "equity_usd": str(equity),
                })
    segmented_payload = {
        strategy: {
            key: {
                "ending_equity_usd": str(path.ending_equity_usd),
                "maximum_continuous_drawdown_usd": str(path.maximum_continuous_drawdown_usd),
                "complete": path.complete,
                "terminal_dispositions": dict(path.terminal_dispositions),
                "admitted_fills": [
                    {
                        "opportunity_id": trade.opportunity_id,
                        "market": trade.market, "year": trade.year,
                        "session": trade.session, "checkpoint": trade.checkpoint,
                        "direction": trade.direction, "fill": asdict(trade.fill),
                    }
                    for trade in path.admitted
                ],
                "continuous_equity_marks": [
                    {
                        "at_ns": at_ns, "opportunity_id": opportunity_id,
                        "kind": kind, "equity_usd": str(equity),
                    }
                    for at_ns, opportunity_id, kind, equity in path.equity_marks
                ],
            }
            for key, path in views.items()
        }
        for strategy, views in segmented.items()
    }
    artifacts = EvidenceArtifactsV5(
        model=model.canonical_model_payload,
        predictions=tuple(asdict(item) for item in model.predictions),
        opportunity_ledger=tuple(opportunity_ledger), fills=tuple(fills),
        continuous_equity_marks=tuple(marks),
        segmented_metrics=segmented_payload,
        inference={
            "crossfit_sessions": list(crossfit.sessions),
            "crossfit_fold_ids": list(crossfit.fold_ids),
            "coverage": coverage_result,
        },
        decision=decision, runtime_receipt=runtime_receipt,
    )
    build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    return V5PipelineResult(
        rows, model, evaluation, segmented, crossfit, coverage_result,
        decision, artifacts,
    )


def build_evidence_manifest_v5(
    *, trial_id: str, artifacts: EvidenceArtifactsV5,
) -> dict[str, object]:
    if not _hex64(trial_id) or not artifacts.predictions or not artifacts.opportunity_ledger:
        raise IntegrityError("V5 evidence lacks frozen predictions or opportunity rows")
    payloads = _json_safe(asdict(artifacts))
    assert isinstance(payloads, dict)
    required = {
        "model", "predictions", "opportunity_ledger", "fills",
        "continuous_equity_marks", "segmented_metrics", "inference",
        "decision", "runtime_receipt",
    }
    if set(payloads) != required:
        raise IntegrityError("V5 evidence artifact set is incomplete")
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payloads[name]}) + b"\n")
        for name in sorted(payloads)
    }
    core = {
        "schema_version": "tier1_bracket_successor_v5_evidence_manifest/1.0.0",
        "trial_id": trial_id,
        "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def persist_evidence_bundle_v5(
    *, boundary: RepoBoundary, output_root: Path, trial_id: str,
    artifacts: EvidenceArtifactsV5,
) -> dict[str, str]:
    """Write the complete evidence set create-only; publication approval is external."""

    manifest = build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    boundary.assert_active_path(
        output_root.absolute(), purpose="V5 evidence output root"
    )
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V5 evidence publication is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{manifest['manifest_id']}-",
        dir=destination.parent,
    ))
    payloads = _json_safe(asdict(artifacts))
    assert isinstance(payloads, dict)
    for filename, expected_hash in manifest["files"].items():
        name = filename.removesuffix(".json")
        path = staging / filename
        body = {"payload": payloads[name]}
        with path.open("xb") as stream:
            stream.write(canonical_bytes(body) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("persisted V5 evidence hash mismatch")
    staging_manifest = staging / "manifest.json"
    with staging_manifest.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V5 evidence destination appeared during publication")
    staging.replace(destination)
    manifest_path = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": manifest_path.as_posix(),
        "manifest_sha256": sha256_file(manifest_path),
    }


def prepare_runtime_receipt_v5(*, root: Path, trial_id: str) -> dict[str, object]:
    dependency_id = require_locked_repository_environment(root)
    core = {
        "schema_version": "tier1_bracket_successor_v5_runtime_receipt/1.0.0",
        "trial_id": trial_id,
        "dependency_lock_receipt_id": dependency_id,
        "dependency_lock_receipt_sha256": sha256_file(root / "configs/dependency_lock_receipt.json"),
        "python_executable": str(Path(sys.executable).resolve(strict=True)),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "platform": sys.platform,
    }
    return {**core, "runtime_receipt_id": sha256_json(core)}


@dataclass(frozen=True)
class PreparedV4Retirement:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV5Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


def prepare_v4_retirement_v5(*, root: Path) -> PreparedV4Retirement:
    preparation = _load_object(root / V4_RETIREMENT_PREPARATION)
    registry = _load_object(root / V4_REGISTRY)
    event = _load_object(root / V4_EVENT)
    registered_bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V4_TRIAL_ID
        or preparation.get("disposition") != "INCOMPLETE_PRE_DATA_IMPLEMENTATION_DEFECTS"
        or preparation.get("research_evidence_contaminated") is not False
        or registry.get("trial_id") != V4_TRIAL_ID
        or event.get("trial_id") != V4_TRIAL_ID
        or not isinstance(registered_bindings, dict)
    ):
        raise IntegrityError("V4 retirement preparation or preserved evidence is invalid")
    for path in V4_BOUND_PATHS[:3]:
        if registered_bindings.get(path.as_posix()) != sha256_file(root / path):
            raise IntegrityError("a registered V4 implementation binding changed")
    core = {
        **preparation,
        "preserved_v4_sha256": {
            path.as_posix(): sha256_file(root / path) for path in V4_BOUND_PATHS
        },
    }
    return PreparedV4Retirement(sha256_json(core), core)


def prepare_v5_registration(*, root: Path) -> PreparedV5Registration:
    contract = load_v5_contract(root=root)
    retirement = prepare_v4_retirement_v5(root=root)
    census_contract = contract["opportunity_census"]
    assert isinstance(census_contract, dict)
    pointer_path = Path(str(census_contract["active_calendar_pointer_path"]))
    active_calendar = _load_object(root / pointer_path)
    calendar_receipt = active_calendar.get("calendar_index_receipt")
    if not isinstance(calendar_receipt, dict) or not _hex64(calendar_receipt.get("release_id")):
        raise IntegrityError("historical checkpoint calendar binding is invalid")
    dependency = _load_object(root / "configs/dependency_lock_receipt.json")
    if not _hex64(dependency.get("receipt_id")):
        raise IntegrityError("dependency lock binding is invalid")
    calendar_manifest_text = calendar_receipt.get("manifest_path")
    if not isinstance(calendar_manifest_text, str):
        raise IntegrityError("historical calendar manifest binding is absent")
    calendar_manifest = Path(calendar_manifest_text)
    if sha256_file(root / calendar_manifest) != calendar_receipt.get("manifest_sha256"):
        raise IntegrityError("historical calendar manifest differs from its pointer")
    calendar_manifest_payload = _load_object(root / calendar_manifest)
    require_historical_calendar_index_span_v5(
        manifest=calendar_manifest_payload,
        expected_release_id=str(calendar_receipt["release_id"]),
    )
    from .historical_checkpoint_calendar import load_historical_checkpoint_calendar

    verified_calendar = load_historical_checkpoint_calendar(
        boundary=RepoBoundary(root.resolve()), pointer_path=root / pointer_path,
    )
    if verified_calendar.index_receipt.release_id != calendar_receipt["release_id"]:
        raise IntegrityError("verified historical calendar differs from V5 pointer")
    calendar_lineage_manifests = tuple(
        Path(receipt.manifest_path)
        for receipt in (
            verified_calendar.calendar_receipt,
            verified_calendar.capture_receipt,
        )
    )
    v4_registry = _load_object(root / V4_REGISTRY)
    raw_sources = v4_registry.get("source_bindings")
    if not isinstance(raw_sources, list) or not all(isinstance(item, dict) for item in raw_sources):
        raise IntegrityError("preserved V4 source bindings are absent")
    source_binding_id = source_binding_id_from_metadata_v5(raw_sources)
    bound_paths = (
        V5_CONTRACT,
        V4_RETIREMENT_PREPARATION,
        pointer_path,
        Path("configs/dependency_lock_receipt.json"),
        calendar_manifest,
        Path("src/futures_rebuild/boundary.py"),
        Path("src/futures_rebuild/canonical.py"),
        Path("src/futures_rebuild/data_layout.py"),
        Path("src/futures_rebuild/errors.py"),
        Path("src/futures_rebuild/exchange_calendar.py"),
        Path("src/futures_rebuild/historical_checkpoint_calendar.py"),
        Path("src/futures_rebuild/locking.py"),
        Path("src/futures_rebuild/runtime_environment.py"),
        Path("src/futures_rebuild/research/bootstrap.py"),
        Path("src/futures_rebuild/research/contracts.py"),
        Path("src/futures_rebuild/research/dsr.py"),
        Path("src/futures_rebuild/research/hac.py"),
        Path("src/futures_rebuild/research/multiple_testing.py"),
        Path("src/futures_rebuild/research/power.py"),
        Path("src/futures_rebuild/tier1_bracket_post_audit.py"),
        Path("src/futures_rebuild/tier1_bracket_v5.py"),
        Path("tests/test_tier1_bracket_v5.py"),
        Path("tests/test_historical_checkpoint_calendar.py"),
        *calendar_lineage_manifests,
        *V4_BOUND_PATHS,
    )
    core = {
        "schema_version": "tier1_bracket_successor_v5_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": contract["classification"],
        "supersedes_v4_trial_id": V4_TRIAL_ID,
        "v4_retirement_record_id": retirement.record_id,
        "bindings": {path.as_posix(): sha256_file(root / path) for path in bound_paths},
        "calendar_release_id": calendar_receipt["release_id"],
        "dependency_lock_receipt_id": dependency["receipt_id"],
        "source_bindings": sorted(
            (dict(item) for item in raw_sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_binding_id": source_binding_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return PreparedV5Registration(sha256_json(core), core)


def persist_v4_retirement_v5(*, root: Path, prepared: PreparedV4Retirement) -> dict[str, str]:
    """Create-only publication surface. Caller must hold separate publication approval."""

    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V4 retirement identity is invalid")
    registry = root / V4_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = root / V4_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("V4 retirement publication is create-only")
    registry.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    try:
        with registry.open("xb") as stream:
            stream.write(canonical_bytes({**prepared.canonical_payload, "state": "RETIRED_WITHOUT_DATA_ACCESS"}) + b"\n")
        with event.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v4_retirement_event/1.0.0",
                "event_type": "RETIRED", "trial_id": V4_TRIAL_ID,
                "record_id": prepared.record_id,
            }) + b"\n")
    except FileExistsError as exc:
        raise IntegrityError("V4 retirement publication raced another writer") from exc
    return {"record_id": prepared.record_id, "registry_path": registry.as_posix(), "event_path": event.as_posix()}


def persist_v5_registration(*, root: Path, prepared: PreparedV5Registration) -> dict[str, str]:
    """Create-only publication surface. Caller must hold separate publication approval."""

    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V5 registration identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(sha256_file(root / path) != digest for path, digest in bindings.items()):
        raise IntegrityError("V5 registration bindings changed after preparation")
    registry = root / V5_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = root / V5_EVENT_ROOT / f"{prepared.trial_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("V5 registration publication is create-only")
    registry.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    try:
        with registry.open("xb") as stream:
            stream.write(canonical_bytes({**prepared.canonical_payload, "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS", "trial_id": prepared.trial_id}) + b"\n")
        with event.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_successor_v5_event/1.0.0",
                "event_type": "DECLARED", "trial_id": prepared.trial_id,
                "source_row_access": False, "model_fit": False,
                "prediction_generation": False, "historical_evaluation": False,
                "holdout_or_forward_access": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise IntegrityError("V5 registration publication raced another writer") from exc
    return {"trial_id": prepared.trial_id, "registry_path": registry.as_posix(), "event_path": event.as_posix()}
