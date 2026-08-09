"""Fail-closed source routing for the Apex integer-micro Alpha lane.

This module is deliberately source-safe.  It describes and validates paths and
phase contracts, but it does not contact Databento, open DBN payloads, decode
rows, publish a catalog, or grant research authority.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Final

from .canonical import sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .source_symbology import require_allowed_query_symbology


LANE_ID: Final = "apex_integer_micro_11"
DATASET: Final = "GLBX.MDP3"
TIER_0_MARKETS: Final = ("MES",)
TIER_1_MARKETS: Final = ("MES", "MCL", "MGC", "M6E")
TIER_2_ADDITIONS: Final = ("MNQ", "MYM", "M2K", "M6A", "SIL")
TIER_2_MARKETS: Final = (*TIER_1_MARKETS, *TIER_2_ADDITIONS)
SATELLITES: Final = ("MBT", "MET")
TIER_3_MARKETS: Final = (*TIER_2_MARKETS, *SATELLITES)
CURRENT_ACQUISITION_MARKETS: Final = TIER_1_MARKETS
SCHEMAS: Final = ("definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s")
FORBIDDEN_SCHEMAS: Final = frozenset({"trades", "bbo-1s", "mbp-1", "mbp-10"})
RESEARCH_END_YEAR: Final = 2024
SEALED_HOLDOUT_YEAR: Final = 2025
ACQUISITION_START_DATE: Final = date(2018, 1, 1)
LATEST_PREPARED_YEAR: Final = 2026
STANDARD_LANE_TERM: Final = "standard/full-contract lane"
MICRO_LANE_TERM: Final = "Apex integer-micro lane"
SOURCE_SCHEMA_TERM: Final = "required Databento Standard historical schemas"

PRODUCT_REFERENCE_REQUIREMENTS: Final = {
    "MES": {
        "parent_product": "ES",
        "cme_schedule_family": "CME_EQUITY_INDEX_US",
        "tick_size": "0.25",
        "tick_value_usd": "1.25",
        "point_value_usd": "5",
        "currency": "USD",
        "underlying_contract_size": "5 USD x S&P 500 Index",
    },
    "MCL": {
        "parent_product": "CL",
        "cme_schedule_family": "NYMEX_ENERGY_CRUDE_OIL",
        "tick_size": "0.01",
        "tick_value_usd": "1",
        "point_value_usd": "100",
        "currency": "USD",
        "underlying_contract_size": "100 barrels",
    },
    "MGC": {
        "parent_product": "GC",
        "cme_schedule_family": "COMEX_METALS_GOLD",
        "tick_size": "0.10",
        "tick_value_usd": "1",
        "point_value_usd": "10",
        "currency": "USD",
        "underlying_contract_size": "10 troy ounces",
    },
    "M6E": {
        "parent_product": "6E",
        "cme_schedule_family": "CME_FX_EUR_USD",
        "tick_size": "0.0001",
        "tick_value_usd": "1.25",
        "point_value_usd": "12500",
        "currency": "USD",
        "underlying_contract_size": "12500 euro",
    },
}


def _market(value: str) -> str:
    if value not in TIER_3_MARKETS:
        raise ContractError("market is not in the Apex micro ladder")
    return value


def _schema(value: str) -> str:
    if value in FORBIDDEN_SCHEMAS or value not in SCHEMAS:
        raise ContractError("schema is not permitted for the Apex micro lane")
    return value


def annual_market_year_intervals(
    *, start: str, end_exclusive: str,
) -> tuple[dict[str, object], ...]:
    """Split one bounded product range into explicit calendar-year intervals."""

    try:
        first = date.fromisoformat(start)
        end = date.fromisoformat(end_exclusive)
    except (TypeError, ValueError) as exc:
        raise ContractError("micro Phase 1A date bound is invalid") from exc
    if first < ACQUISITION_START_DATE or first >= end:
        raise ContractError("micro Phase 1A date range is outside the prepared scope")
    if end.year > LATEST_PREPARED_YEAR + 1:
        raise ContractError("micro Phase 1A end exceeds the prepared year scope")
    intervals: list[dict[str, object]] = []
    cursor = first
    while cursor < end:
        next_year = date(cursor.year + 1, 1, 1)
        interval_end = min(next_year, end)
        intervals.append(
            {
                "year": cursor.year,
                "start": cursor.isoformat(),
                "end_exclusive": interval_end.isoformat(),
                "interval": f"{cursor.isoformat()}_{interval_end.isoformat()}",
                "partial_launch_year": cursor.month != 1 or cursor.day != 1,
                "partial_latest_year": interval_end != next_year,
            }
        )
        cursor = interval_end
    return tuple(intervals)


def validate_annual_market_year_interval(
    *, year: int, interval: str,
) -> tuple[str, str]:
    """Require an interval to be positive and contained in its calendar year."""

    if type(year) is not int or year < ACQUISITION_START_DATE.year or year > LATEST_PREPARED_YEAR:
        raise ContractError("micro Phase 1A year is invalid")
    parts = interval.split("_") if type(interval) is str else []
    if len(parts) != 2:
        raise ContractError("micro Phase 1A interval must contain exact date bounds")
    try:
        start = date.fromisoformat(parts[0])
        end = date.fromisoformat(parts[1])
    except ValueError as exc:
        raise ContractError("micro Phase 1A interval date is invalid") from exc
    maximum_end = date(year + 1, 1, 1)
    if start.year != year or start >= end or end > maximum_end:
        raise ContractError("micro Phase 1A interval must stay within one market-year")
    return start.isoformat(), end.isoformat()


def phase1a_paths(*, market: str, schema: str, year: int, interval: str) -> dict[str, str]:
    """Return exact inactive DBN and sidecar destinations."""

    _market(market)
    _schema(schema)
    validate_annual_market_year_interval(year=year, interval=interval)
    schema_folder = schema.replace("-", "_")
    base = Path("data/dbn") / schema_folder / market / str(year)
    return {
        "dbn": (base / f"{interval}.dbn.zst").as_posix(),
        "sidecar": (base / f"{interval}.dbn.zst.manifest.json").as_posix(),
    }


def build_product_reference_requirements() -> dict[str, object]:
    """Build explicit acquisition references without claiming row certification."""

    products: dict[str, object] = {}
    for market in CURRENT_ACQUISITION_MARKETS:
        reference = dict(PRODUCT_REFERENCE_REQUIREMENTS[market])
        products[market] = {
            "market": market,
            **reference,
            "integer_contract_size": 1,
            "product_effective_date": "PROVIDER_METADATA_PREFLIGHT_REQUIRED",
            "actual_instrument_identity": "DEFINITION_INSTRUMENT_ID_REQUIRED_PER_INTERVAL",
            "roll_continuity": "CONTINUOUS_RANK_0_SELECTOR_MUST_RESOLVE_TO_ACTUAL_INSTRUMENT_ID",
            "calendar_mapping": "EXPLICIT_CME_SCHEDULE_FAMILY_REQUIRED_NO_PARENT_INHERITANCE",
            "economics_mapping": "EXPLICIT_MICRO_VALUES_REQUIRED_NO_PARENT_INHERITANCE",
            "prelaunch_disposition": "PRODUCT_NOT_YET_EFFECTIVE_NO_EMPTY_DBN",
            "unavailable_source_disposition": "FAIL_CLOSED_NO_AUTOMATIC_SUBSTITUTE",
        }
    core: dict[str, object] = {
        "schema_version": "apex_micro_product_reference_requirements/1.0.0",
        "lane_id": LANE_ID,
        "markets": products,
        "selection_policy": {
            "returns_predictions_or_strategy_performance_used": False,
            "invented_zn_micro_proxy_forbidden": True,
            "future_micro_rates_admission": (
                "REQUIRES_PREOUTCOME_APEX_ELIGIBILITY_PROVIDER_AVAILABILITY_AND_ECONOMICS"
            ),
        },
    }
    return {**core, "requirements_id": sha256_json(core)}


def validate_product_reference_requirements(value: Mapping[str, object]) -> None:
    if dict(value) != build_product_reference_requirements():
        raise IntegrityError("Apex micro product reference requirements drifted")


def validate_product_effective_date(value: object) -> str:
    if type(value) is not str or len(value) != 10:
        raise IntegrityError("provider-confirmed product effective date is invalid")
    try:
        year, month, day = (int(part) for part in value.split("-"))
    except (ValueError, TypeError) as exc:
        raise IntegrityError("provider-confirmed product effective date is invalid") from exc
    if not (2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        raise IntegrityError("provider-confirmed product effective date is invalid")
    return value


def validate_economics_reference(market: str, reference: Mapping[str, object]) -> None:
    """Reject missing, fractional, cross-market, or numerically invalid economics."""

    _market(market)
    expected = PRODUCT_REFERENCE_REQUIREMENTS.get(market)
    if expected is None or any(reference.get(key) != value for key, value in expected.items()):
        raise IntegrityError("micro economics or schedule reference drifted")
    if reference.get("integer_contract_size") != 1:
        raise IntegrityError("Apex micro contract size must be exactly one integer contract")
    for key in ("tick_size", "tick_value_usd", "point_value_usd"):
        try:
            amount = Decimal(str(reference[key]))
        except (InvalidOperation, KeyError, ValueError) as exc:
            raise IntegrityError("micro economics value is invalid") from exc
        if not amount.is_finite() or amount <= 0:
            raise IntegrityError("micro economics value is invalid")


def phase1b_role(schema: str) -> str:
    """Map each allowed source schema to its immutable Phase 1B role."""

    return {
        "definition": "CONTRACT_IDENTITY_REFERENCE",
        "status": "MARKET_STATE_DIAGNOSTIC",
        "statistics": "MARKET_STATE_DIAGNOSTIC",
        "ohlcv-1m": "CAUSAL_FEATURE_FOUNDATION_INPUT",
        "ohlcv-1s": "CAUSAL_EXECUTION_EVIDENCE_INPUT",
    }[_schema(schema)]


def phase1b_destination(
    *, market: str, schema: str, year: int, interval: str, release_id: str,
) -> str:
    """Return the schema-specific immutable Phase 1B output root."""

    _market(market)
    role = phase1b_role(schema)
    validate_annual_market_year_interval(year=year, interval=interval)
    if len(release_id) < 12:
        raise ContractError("micro Phase 1B release identity is invalid")
    if schema in {"definition", "ohlcv-1m"}:
        root = Path("data/raw") / market / str(year) / interval / release_id
    elif schema in {"status", "statistics"}:
        root = Path("data/market_state") / schema / market / str(year) / interval / release_id
    else:
        root = Path("data/outcome_sources") / market / str(year) / interval / release_id
    return root.as_posix()


def require_decode_authority(
    *, year: int, mechanism_frozen_at: str | None,
    source_interval_start: str | None = None,
) -> None:
    """Block sealed holdout and pre-freeze forward decoding."""

    if year == SEALED_HOLDOUT_YEAR:
        raise UnauthorizedOperation("2025 micro data is sealed holdout custody only")
    if year > SEALED_HOLDOUT_YEAR:
        if not mechanism_frozen_at:
            raise UnauthorizedOperation("forward micro rows require a prior frozen mechanism")
        if source_interval_start is None or source_interval_start < mechanism_frozen_at:
            raise UnauthorizedOperation("pre-freeze 2026 micro rows cannot be decoded as forward data")
    if year < 2018 or year > 2026:
        raise UnauthorizedOperation("micro decode year is outside the prepared source scope")


def classify_product_session(
    *, session_id: str, product_effective_date: str | None,
) -> str:
    """Classify prelaunch sessions without inventing product history."""

    if product_effective_date is None:
        return "PRODUCT_EFFECTIVE_DATE_UNVERIFIED"
    return (
        "PRODUCT_NOT_YET_EFFECTIVE"
        if session_id < product_effective_date
        else "PRODUCT_EFFECTIVE"
    )


def build_phase2_contract(
    *, market: str, year: int, source_bindings: Mapping[str, str],
    product_effective_date: str, mechanism_frozen_at: str | None = None,
    source_interval_start: str | None = None,
) -> dict[str, object]:
    """Build a non-authorizing Phase 2 routing contract from exact bindings."""

    _market(market)
    require_decode_authority(
        year=year, mechanism_frozen_at=mechanism_frozen_at,
        source_interval_start=source_interval_start,
    )
    validate_product_effective_date(product_effective_date)
    if set(source_bindings) != set(SCHEMAS):
        raise IntegrityError("micro Phase 2 requires all five schema bindings")
    if any(len(value) != 64 for value in source_bindings.values()):
        raise IntegrityError("micro source bindings must be SHA-256 digests")
    core: dict[str, object] = {
        "schema_version": "micro_alpha_phase2_contract/1.0.0",
        "lane_id": LANE_ID,
        "market": market,
        "year": year,
        "product_effective_date": product_effective_date,
        "source_bindings": dict(sorted(source_bindings.items())),
        "feature_foundation": {
            "schema": "ohlcv-1m",
            "selector": f"{market}.v.0",
            "availability_required": True,
            "training_transformations": "FOLD_LOCAL_ONLY",
        },
        "execution_foundation": {
            "schema": "ohlcv-1s",
            "selector": f"{market}.v.0",
            "actual_instrument_id_required": True,
            "missing_sparse_roll_states_preserved": True,
            "feature_eligibility": False,
            "evidence_semantics": "REPORTED_TRADE_BARS_ONLY",
            "cannot_prove": [
                "BBO_AVAILABILITY", "QUEUE_PRIORITY", "GUARANTEED_MARKET_ORDER_EXECUTION",
                "PRECISE_WITHIN_SECOND_TICK_ORDERING",
            ],
            "entry_after_decision_and_causal_availability": True,
            "same_bar_ambiguity": "CONSERVATIVE_ADVERSE_OR_UNFILLED",
            "explicit_states": ["UNFILLED", "NO_TRIGGER"],
        },
        "diagnostics": {
            "status": "DIAGNOSTIC_ONLY_PRE_2025_CAPABILITY_EPOCH",
            "statistics": "DIAGNOSTIC_ONLY_NEVER_FEATURE_ELIGIBLE",
        },
        "authority": {
            "row_read": False,
            "catalog_activation": False,
            "research_registration": False,
        },
        "coverage_policy": {
            "missing_sparse_duplicate_prelaunch_ambiguous_are_explicit": True,
            "missing_or_sparse_checkpoints_never_silently_removed": True,
            "baselines_use_independent_schedules": True,
            "stress_costs_locked_before_outcomes": True,
        },
    }
    return {**core, "phase2_contract_id": sha256_json(core)}


def validate_phase1a_request(request: Mapping[str, object]) -> dict[str, object]:
    """Validate one metadata-preflight request without contacting a provider."""

    market = _market(str(request.get("market", "")))
    schema = _schema(str(request.get("schema", "")))
    expected_stype = "parent" if schema == "definition" else "continuous"
    expected_symbol = f"{market}.FUT" if schema == "definition" else f"{market}.v.0"
    require_allowed_query_symbology(
        schema=schema,
        market=market,
        stype_in=request.get("stype_in"),
        symbols=request.get("symbols"),
    )
    if (
        request.get("dataset") != DATASET
        or request.get("stype_in") != expected_stype
        or request.get("symbols") != [expected_symbol]
        or request.get("stype_out") != "instrument_id"
        or request.get("start") != "2018-01-01"
        or request.get("end_rule") != "LATEST_COMPLETE_DAY_END_EXCLUSIVE_PROVIDER_CONFIRMED"
        or request.get("maximum_cost_usd") != 0
    ):
        raise IntegrityError("micro metadata-preflight request drifted")
    return dict(request)


def require_lane_catalog_entry(entry: Mapping[str, object], *, lane_id: str = LANE_ID) -> None:
    """Prevent a micro catalog from resolving a standard-contract source."""

    if entry.get("lane_id") != lane_id or entry.get("market") not in TIER_3_MARKETS:
        raise UnauthorizedOperation("source belongs to a different research lane")
    if entry.get("contract_scale") != "MICRO_INTEGER_ONLY":
        raise UnauthorizedOperation("micro lane cannot resolve standard or fractional contracts")
