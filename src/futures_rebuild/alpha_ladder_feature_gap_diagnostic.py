"""Exact price-free diagnosis of the sealed Alpha Tier 1 feature gaps."""

from __future__ import annotations

import hashlib
import multiprocessing
import os
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from time import monotonic

from .active_data_view import resolve
from .alpha_ladder_combined_readiness import (
    ACTIVE_CATALOG_PATH,
    CHECKPOINT,
    CORE,
    SCENARIOS,
    YEARS,
    _active_calendar,
    _read_canonical,
    _write_once,
)
from .alpha_ladder_combined_readiness_v3 import (
    EMBARGO_SESSIONS,
    EVALUATION_SESSIONS,
    OUTER_FOLDS,
    PURGE_MINUTES,
    TRAINING_SESSIONS,
    _outer_folds,
)
from .alpha_ladder_limit_readiness import (
    CT,
    REQUIRED_COLUMNS,
    LimitBar,
    _dependency_clock,
)
from .alpha_ladder_reported_trade_exit_readiness import (
    PLAN_PATH as READINESS_PLAN_PATH,
    _selected_sources,
    classify_session,
)
from .alpha_ladder_reported_trade_exit_tier0 import (
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_source_compatibility import source_row_from_mapping
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


PLAN_PATH = Path("configs/alpha_ladder_feature_gap_diagnostic_plan.json")
OUTPUT_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_feature_gap_diagnostic"
)
MODULE_PATH = Path("src/futures_rebuild/alpha_ladder_feature_gap_diagnostic.py")
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_feature_gap_diagnostic_plan.py"
)
RUNNER_PATH = Path("scripts/run_alpha_ladder_feature_gap_diagnostic.py")
TEST_PATH = Path("tests/test_alpha_ladder_feature_gap_diagnostic.py")
READINESS_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_readiness"
)
READINESS_REPORT_PATH = READINESS_ROOT / "readiness_report.json"
READINESS_CERTIFICATE_PATH = READINESS_ROOT / "tier1_readiness_certificate.json"
READINESS_MANIFEST_PATH = READINESS_ROOT / "tier1_session_manifest.json"
READINESS_AUTHORIZATION_PATH = Path(
    "state/authorization_uses/"
    "b03fefb269c16ad4afd87c369cb5c545f40fe1a969e3c63aff19f92b93cbf0d4.json"
)

READINESS_PLAN_ID = "8faeb6d8b51ca4358d2824e92e240e120f360d8f6e36a042d4f0a99d0e9c20f2"
READINESS_PLAN_SHA256 = "a336b80a26cfd9707078be92ff277232d884851a8604a698147d24cde3db197a"
READINESS_REPORT_ID = "aba1c5f17fb8f3806c14dc36a1ce79c8130944eaa5a4dfe56dea5884ef7dbc93"
READINESS_REPORT_SHA256 = "c9f12bb8e84ac9acd3bdc2c3e31f25a3bba0d032aa3becba2e6e193e162d5d8a"
READINESS_CERTIFICATE_ID = (
    "a3189e7e1333a93e733af9e03a79e65b043871c304c704e068d0e11ebf5ab41b"
)
READINESS_CERTIFICATE_SHA256 = (
    "54558b64da8d762ea11bda8aa359e831ad8bd615acf4baa97661ecf79fa762eb"
)
READINESS_MANIFEST_ID = "6538199b6b767e0c81f78b19c36b70506ffb37cb47c57d91f025760ebac49023"
READINESS_MANIFEST_SHA256 = "f478f962052f9cd8709fe4b481b6ba6f7204fd85b5978fa1fced7151ed5209ea"
READINESS_AUTHORIZATION_SHA256 = (
    "b2ec0b49219541018f26744995b02a611e78b7eb0c3f4d0a803ec94a780b5df2"
)

EXPECTED_TARGETS = (
    ("ES", "fold-0"),
    ("ES", "fold-2"),
    ("CL", "fold-0"),
    ("CL", "fold-2"),
    ("ZN", "fold-0"),
    ("ZN", "fold-2"),
    ("ZN", "fold-3"),
    ("ZN", "fold-7"),
    ("6E", "fold-2"),
)
GENERIC_FEATURE_ABSTENTION = "EXPLICIT_CAUSAL_FEATURE_ABSTENTION"
ALLOWED_FAILED_GATES = frozenset({
    "MINIMUM_COMPLETE_TRAINING_SESSIONS",
    "MINIMUM_FEATURE_COMPLETE_TRAINING_SESSIONS",
    "MINIMUM_TRANSFORMATION_READY_TRAINING_SESSIONS",
    "MINIMUM_FEATURE_COMPLETE_EVALUATION_SESSIONS",
})
CLASSIFICATIONS = (
    "SOURCE_SESSION_ABSENT",
    "DUPLICATE_EVENT_TIMESTAMP",
    "LATE_AVAILABILITY",
    "INSUFFICIENT_REPORTED_BAR_HISTORY",
    "WINDOW_START_TOO_LATE",
    "WINDOW_END_TOO_EARLY",
    "IDENTITY_ROLL_DISCONTINUITY",
    "INVALID_FIELDS",
    "INVALID_ECONOMICS",
    "ECONOMICS_DRIFT",
    "FEATURE_STOP_GEOMETRY_INVALID",
    "UNCLASSIFIED_FEATURE_GATE_MISMATCH",
    "FEATURE_COMPLETE",
)
DIRECT_DEPENDENCIES = frozenset({
    MODULE_PATH.as_posix(),
    PREPARE_SCRIPT_PATH.as_posix(),
    RUNNER_PATH.as_posix(),
    TEST_PATH.as_posix(),
    "src/futures_rebuild/active_data_view.py",
    "src/futures_rebuild/alpha_ladder_combined_readiness.py",
    "src/futures_rebuild/alpha_ladder_combined_readiness_v3.py",
    "src/futures_rebuild/alpha_ladder_limit_readiness.py",
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_readiness.py",
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_tier0.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/cash_open_source_compatibility.py",
    "src/futures_rebuild/research_gateway_policy.py",
})


@dataclass(frozen=True)
class FeatureRow:
    event_at: datetime
    available_at: datetime
    executable: bool
    identity: str | None
    source_row_sha256: str
    disposition: str
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None
    tick_size: Decimal | None
    tick_value: Decimal | None

    def as_limit_bar(self) -> LimitBar | None:
        values = (
            self.open, self.high, self.low, self.close,
            self.volume, self.tick_size, self.tick_value,
        )
        if not self.executable or self.identity is None or any(
            value is None for value in values
        ):
            return None
        return LimitBar(
            event_at=self.event_at,
            available_at=self.available_at,
            identity=self.identity,
            open=self.open,  # type: ignore[arg-type]
            high=self.high,  # type: ignore[arg-type]
            low=self.low,  # type: ignore[arg-type]
            close=self.close,  # type: ignore[arg-type]
            volume=self.volume,  # type: ignore[arg-type]
            tick_size=self.tick_size,  # type: ignore[arg-type]
            tick_value=self.tick_value,  # type: ignore[arg-type]
        )


def _economics_fingerprint(row: FeatureRow) -> str | None:
    if row.tick_size is None or row.tick_value is None:
        return None
    return sha256_json({
        "tick_size": str(row.tick_size),
        "tick_value": str(row.tick_value),
    })


def _minute_iso(value: datetime) -> str:
    return value.replace(second=0, microsecond=0).isoformat()


def _invalid_counts(rows: Sequence[FeatureRow]) -> Counter:
    counts = Counter()
    for row in rows:
        if row.as_limit_bar() is None:
            counts["unparseable_executable_row"] += 1
            continue
        assert row.high is not None and row.low is not None and row.close is not None
        assert row.volume is not None and row.tick_size is not None
        assert row.tick_value is not None
        if row.high <= 0:
            counts["nonpositive_high"] += 1
        if row.low <= 0:
            counts["nonpositive_low"] += 1
        if row.close <= 0:
            counts["nonpositive_close"] += 1
        if row.high < row.low:
            counts["high_below_low"] += 1
        if row.volume < 0:
            counts["negative_volume"] += 1
        if row.tick_size <= 0:
            counts["nonpositive_tick_size"] += 1
        if row.tick_value <= 0:
            counts["nonpositive_tick_value"] += 1
    return counts


def diagnose_feature_session(
    *, session: str, rows: Sequence[FeatureRow], cost_ticks: Mapping[str, int],
) -> dict[str, object]:
    """Return exact causal metadata and flags without exposing price values."""

    session_date = date.fromisoformat(session)
    checkpoint = datetime.combine(session_date, time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=5)
    ordered = tuple(sorted(rows, key=lambda row: (row.event_at, row.source_row_sha256)))
    window = tuple(
        row for row in ordered
        if time(9, 30) <= row.event_at.timetz().replace(tzinfo=None) < time(10, 0)
    )
    executable_window = tuple(row for row in window if row.executable)
    parseable_bars = tuple(
        bar for row in ordered if (bar := row.as_limit_bar()) is not None
    )
    causal_rows = tuple(
        row for row in executable_window
        if row.as_limit_bar() is not None and row.available_at <= decision
    )
    causal_bars = tuple(row.as_limit_bar() for row in causal_rows)
    assert all(isinstance(bar, LimitBar) for bar in causal_bars)
    feature = tuple(causal_bars[-21:])
    duplicate_count = len(parseable_bars) - len({bar.event_at for bar in parseable_bars})
    invalid_counts = _invalid_counts(executable_window)
    feature_invalid_counts = _invalid_counts(causal_rows[-21:])

    expected_minutes = tuple(
        checkpoint - timedelta(minutes=30 - index) for index in range(30)
    )
    observed_minutes = {_minute_iso(row.event_at) for row in executable_window}
    missing_minutes = tuple(
        value.isoformat() for value in expected_minutes
        if _minute_iso(value) not in observed_minutes
    )
    late_rows = tuple(row for row in executable_window if row.available_at > decision)
    identities = tuple(sorted({
        row.identity for row in feature if isinstance(row, LimitBar)
    }))
    economics = tuple(sorted({
        fingerprint for raw in causal_rows[-21:]
        if (fingerprint := _economics_fingerprint(raw)) is not None
    }))
    nonexecutable = Counter(row.disposition for row in window if not row.executable)

    production = classify_session(
        session=session,
        bars=parseable_bars,
        cost_ticks=cost_ticks,
    )
    if production.feature_complete:
        classification = "FEATURE_COMPLETE"
    elif production.dispositions == ("AMBIGUOUS_DUPLICATE_SOURCE_TIMESTAMP",):
        classification = "DUPLICATE_EVENT_TIMESTAMP"
    elif not window:
        classification = "SOURCE_SESSION_ABSENT"
    elif len(causal_bars) < 21:
        classification = (
            "LATE_AVAILABILITY"
            if len(executable_window) >= 21 and late_rows
            else "INSUFFICIENT_REPORTED_BAR_HISTORY"
        )
    elif causal_bars[0].event_at.timetz().replace(tzinfo=None) > time(9, 35):
        classification = "WINDOW_START_TOO_LATE"
    elif causal_bars[-1].event_at.timetz().replace(tzinfo=None) < time(9, 58):
        classification = "WINDOW_END_TOO_EARLY"
    elif len(identities) != 1:
        classification = "IDENTITY_ROLL_DISCONTINUITY"
    elif any(
        feature_invalid_counts[name] for name in (
            "unparseable_executable_row", "nonpositive_high", "nonpositive_low",
            "nonpositive_close", "high_below_low", "negative_volume",
        )
    ):
        classification = "INVALID_FIELDS"
    elif any(
        feature_invalid_counts[name] for name in (
            "nonpositive_tick_size", "nonpositive_tick_value",
        )
    ):
        classification = "INVALID_ECONOMICS"
    elif len(economics) != 1:
        classification = "ECONOMICS_DRIFT"
    else:
        true_ranges = [
            max(
                bar.high - bar.low,
                abs(bar.high - previous.close),
                abs(bar.low - previous.close),
            )
            for previous, bar in zip(feature, feature[1:])
        ]
        stop_ticks = int((
            Decimal("1.5") * (sum(true_ranges, Decimal(0)) / Decimal(20))
            / feature[-1].tick_size
        ).to_integral_value(rounding=ROUND_CEILING))
        classification = (
            "FEATURE_STOP_GEOMETRY_INVALID"
            if stop_ticks <= 0
            else "UNCLASSIFIED_FEATURE_GATE_MISMATCH"
        )

    return {
        "session": session,
        "classification": classification,
        "production_feature_complete": production.feature_complete,
        "production_dispositions": list(production.dispositions),
        "raw_window_row_count": len(window),
        "executable_window_row_count": len(executable_window),
        "causal_window_row_count": len(causal_bars),
        "late_availability_row_count": len(late_rows),
        "duplicate_event_timestamp_count": duplicate_count,
        "earliest_executable_event_at": (
            executable_window[0].event_at.isoformat() if executable_window else None
        ),
        "latest_executable_event_at": (
            executable_window[-1].event_at.isoformat() if executable_window else None
        ),
        "missing_minute_timestamps": list(missing_minutes),
        "identity_hashes": list(identities),
        "economics_fingerprints": list(economics),
        "nonexecutable_disposition_counts": dict(sorted(nonexecutable.items())),
        "invalid_field_counts": dict(sorted(invalid_counts.items())),
        "feature_window_invalid_field_counts": dict(
            sorted(feature_invalid_counts.items())
        ),
        "source_row_hashes": sorted(row.source_row_sha256 for row in window),
        "price_values_included": False,
    }


def _sealed_inputs(*, root: Path):
    readiness_plan = _read_canonical(root / READINESS_PLAN_PATH, name="readiness plan")
    report = _read_canonical(root / READINESS_REPORT_PATH, name="readiness report")
    certificate = _read_canonical(
        root / READINESS_CERTIFICATE_PATH, name="Tier 1 readiness certificate",
    )
    manifest = _read_canonical(root / READINESS_MANIFEST_PATH, name="Tier 1 manifest")
    if (
        readiness_plan.get("plan_id") != READINESS_PLAN_ID
        or sha256_file(root / READINESS_PLAN_PATH) != READINESS_PLAN_SHA256
        or report.get("report_id") != READINESS_REPORT_ID
        or sha256_file(root / READINESS_REPORT_PATH) != READINESS_REPORT_SHA256
        or report.get("state") != "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS"
        or report.get("pilot_decision") != "PASS"
        or report.get("tier1_decision") != "FAIL"
        or report.get("combined_registration_ready") is not False
        or report.get("tier1_certificate_id") != READINESS_CERTIFICATE_ID
        or certificate.get("certificate_id") != READINESS_CERTIFICATE_ID
        or sha256_file(root / READINESS_CERTIFICATE_PATH)
        != READINESS_CERTIFICATE_SHA256
        or certificate.get("overall_decision") != "FAIL"
        or certificate.get("registration_allowed") is not False
        or certificate.get("protocol_id") != MECHANISM_ID
        or manifest.get("manifest_id") != READINESS_MANIFEST_ID
        or sha256_file(root / READINESS_MANIFEST_PATH) != READINESS_MANIFEST_SHA256
        or sha256_file(root / READINESS_AUTHORIZATION_PATH)
        != READINESS_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("sealed reported-trade-exit readiness evidence changed")
    return readiness_plan, report, certificate, manifest


def reconstruct_targets(
    *, calendar: Mapping[str, object], certificate: Mapping[str, object],
    manifest: Mapping[str, object],
) -> tuple[dict[str, object], ...]:
    rows = certificate.get("fold_market_results")
    exclusions = manifest.get("excluded_pilot_evaluation_session_ids")
    if not isinstance(rows, list) or not isinstance(exclusions, list):
        raise IntegrityError("sealed target evidence is malformed")
    failed = {
        (str(item["market"]), str(item["fold_id"])): item
        for item in rows
        if isinstance(item, Mapping) and item.get("status") == "FAIL"
    }
    if set(failed) != set(EXPECTED_TARGETS):
        raise IntegrityError("sealed readiness failures are not the exact nine targets")
    calendar_rows = calendar.get("calendar_rows")
    if not isinstance(calendar_rows, list):
        raise IntegrityError("active calendar rows are absent")
    excluded = {str(item) for item in exclusions}
    folds_by_market = {}
    for market in CORE:
        sessions = tuple(
            str(item["trade_date"])
            for item in calendar_rows
            if isinstance(item, Mapping)
            and item.get("market") == market
            and isinstance(item.get("checkpoint_open"), Mapping)
            and item["checkpoint_open"].get(CHECKPOINT) is True
            and str(item["trade_date"]) not in excluded
        )
        folds_by_market[market] = {
            str(fold["fold_id"]): fold for fold in _outer_folds(sessions)
        }

    targets = []
    for market, fold_id in EXPECTED_TARGETS:
        sealed = failed[(market, fold_id)]
        fold = folds_by_market[market][fold_id]
        failed_gates = tuple(str(item) for item in sealed["failed_gates"])
        if not failed_gates or not set(failed_gates).issubset(ALLOWED_FAILED_GATES):
            raise IntegrityError("target failed for a non-feature-readiness reason")
        baseline_evidence = sealed.get("baseline_universe_readiness")
        if not isinstance(baseline_evidence, Mapping) or any(
            item.get("selected_sessions") != item.get("selected_path_complete_sessions")
            for item in baseline_evidence.values() if isinstance(item, Mapping)
        ):
            raise IntegrityError("target contains an unresolved baseline path")
        counts = sealed.get("counts")
        reasons = sealed.get("exclusion_reasons")
        years = sealed.get("market_year_breakdown")
        if not isinstance(counts, Mapping) or not isinstance(reasons, Mapping) \
                or not isinstance(years, Mapping):
            raise IntegrityError("target counts are malformed")
        allowed_reasons = {
            f"TRAINING__CANDIDATE__{GENERIC_FEATURE_ABSTENTION}",
            f"EVALUATION__CANDIDATE__{GENERIC_FEATURE_ABSTENTION}",
        }
        if not set(reasons).issubset(allowed_reasons):
            raise IntegrityError("target contains a non-feature exclusion")
        training = tuple(str(item) for item in fold["training_sessions"])
        evaluation = tuple(str(item) for item in fold["evaluation_sessions"])
        if (
            len(training) != int(counts["expected_training_sessions"])
            or len(evaluation) != int(counts["expected_evaluation_sessions"])
        ):
            raise IntegrityError("reconstructed fold count differs from sealed evidence")
        by_year = {}
        for year, raw in years.items():
            if not isinstance(raw, Mapping):
                raise IntegrityError("target market-year evidence is malformed")
            year_reasons = raw.get("exclusion_reasons")
            if not isinstance(year_reasons, Mapping):
                raise IntegrityError("target market-year exclusions are malformed")
            if (
                sum(item.startswith(str(year)) for item in training)
                != int(raw["expected_training_sessions"])
                or sum(item.startswith(str(year)) for item in evaluation)
                != int(raw["expected_evaluation_sessions"])
            ):
                raise IntegrityError("reconstructed market-year count changed")
            by_year[str(year)] = {
                "training_feature_gaps": int(year_reasons.get(
                    f"TRAINING__CANDIDATE__{GENERIC_FEATURE_ABSTENTION}", 0,
                )),
                "evaluation_feature_gaps": int(year_reasons.get(
                    f"EVALUATION__CANDIDATE__{GENERIC_FEATURE_ABSTENTION}", 0,
                )),
            }
        targets.append({
            "market": market,
            "fold_id": fold_id,
            "failed_gates": list(failed_gates),
            "training_session_ids": list(training),
            "embargo_session_ids": list(fold["embargo_sessions"]),
            "evaluation_session_ids": list(evaluation),
            "expected_training_feature_gaps": int(reasons.get(
                f"TRAINING__CANDIDATE__{GENERIC_FEATURE_ABSTENTION}", 0,
            )),
            "expected_evaluation_feature_gaps": int(reasons.get(
                f"EVALUATION__CANDIDATE__{GENERIC_FEATURE_ABSTENTION}", 0,
            )),
            "expected_feature_gaps_by_market_year": by_year,
        })
    return tuple(targets)


def reconcile_diagnostics(
    *, targets: Sequence[Mapping[str, object]],
    diagnostics: Mapping[str, Mapping[str, Mapping[str, object]]],
) -> dict[str, object]:
    target_results = []
    roles: dict[tuple[str, str], list[str]] = defaultdict(list)
    for target in targets:
        market = str(target["market"])
        fold_id = str(target["fold_id"])
        market_rows = diagnostics.get(market)
        if not isinstance(market_rows, Mapping):
            raise IntegrityError(f"diagnostic omitted target market {market}")
        role_gaps = {}
        for role, key, expected_key in (
            ("training", "training_session_ids", "expected_training_feature_gaps"),
            ("evaluation", "evaluation_session_ids", "expected_evaluation_feature_gaps"),
        ):
            sessions = tuple(str(item) for item in target[key])
            missing = set(sessions) - set(market_rows)
            if missing:
                raise IntegrityError("diagnostic session accounting is incomplete")
            gaps = tuple(
                session for session in sessions
                if market_rows[session].get("production_feature_complete") is False
            )
            if len(gaps) != int(target[expected_key]):
                raise IntegrityError("diagnostic does not reconcile to sealed fold counts")
            for session in gaps:
                record = market_rows[session]
                if record.get("production_dispositions") != [GENERIC_FEATURE_ABSTENTION]:
                    raise IntegrityError("diagnostic changed the sealed feature disposition")
                if record.get("classification") not in CLASSIFICATIONS[:-1]:
                    raise IntegrityError("feature gap lacks an allowed root-cause class")
                roles[(market, session)].append(f"{fold_id}:{role}")
            role_gaps[role] = list(gaps)
        by_year_observed = {}
        sealed_years = target["expected_feature_gaps_by_market_year"]
        assert isinstance(sealed_years, Mapping)
        for year, sealed in sealed_years.items():
            assert isinstance(sealed, Mapping)
            training_count = sum(item.startswith(str(year)) for item in role_gaps["training"])
            evaluation_count = sum(
                item.startswith(str(year)) for item in role_gaps["evaluation"]
            )
            if (
                training_count != int(sealed["training_feature_gaps"])
                or evaluation_count != int(sealed["evaluation_feature_gaps"])
            ):
                raise IntegrityError("diagnostic market-year counts do not reconcile")
            by_year_observed[str(year)] = {
                "training_feature_gaps": training_count,
                "evaluation_feature_gaps": evaluation_count,
            }
        target_results.append({
            "market": market,
            "fold_id": fold_id,
            "training_feature_gap_session_ids": role_gaps["training"],
            "evaluation_feature_gap_session_ids": role_gaps["evaluation"],
            "market_year_reconciliation": by_year_observed,
            "status": "EXACT_RECONCILIATION",
        })

    gap_records = []
    for (market, session), fold_roles in sorted(roles.items()):
        record = diagnostics[market][session]
        gap_records.append({
            **record,
            "market": market,
            "fold_roles": sorted(fold_roles),
        })
    return {
        "status": "EXACT_RECONCILIATION",
        "target_count": len(target_results),
        "unique_feature_gap_session_count": len(gap_records),
        "classification_counts": dict(sorted(Counter(
            str(item["classification"]) for item in gap_records
        ).items())),
        "target_reconciliation": target_results,
        "feature_gap_records": gap_records,
    }


def build_plan(*, root: Path) -> dict[str, object]:
    readiness_plan, report, certificate, manifest = _sealed_inputs(root=root)
    pointer, calendar = _active_calendar(root)
    targets = reconstruct_targets(
        calendar=calendar, certificate=certificate, manifest=manifest,
    )
    selected, _by_key = _selected_sources(root=root)
    if any(sha256_file(root / path) != digest for path, digest in selected.items()):
        raise IntegrityError("active diagnostic source bytes differ from the catalog")
    predecessor_bindings = readiness_plan.get("bindings")
    if not isinstance(predecessor_bindings, Mapping) or any(
        predecessor_bindings.get(path) != digest for path, digest in selected.items()
    ):
        raise IntegrityError("diagnostic source set differs from sealed readiness")
    bindings = {
        READINESS_PLAN_PATH.as_posix(): READINESS_PLAN_SHA256,
        READINESS_REPORT_PATH.as_posix(): READINESS_REPORT_SHA256,
        READINESS_CERTIFICATE_PATH.as_posix(): READINESS_CERTIFICATE_SHA256,
        READINESS_MANIFEST_PATH.as_posix(): READINESS_MANIFEST_SHA256,
        READINESS_AUTHORIZATION_PATH.as_posix(): READINESS_AUTHORIZATION_SHA256,
        MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
        ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
        "configs/active_cash_open_impulse_historical_calendar.json": sha256_file(
            root / "configs/active_cash_open_impulse_historical_calendar.json"
        ),
        str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
        **selected,
        **{path: sha256_file(root / path) for path in DIRECT_DEPENDENCIES},
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_feature_gap_diagnostic_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "purpose": "EXACT_PRICE_FREE_DIAGNOSIS_OF_NINE_SEALED_TIER1_FEATURE_GAPS",
        "mechanism_id": MECHANISM_ID,
        "mechanism_sha256": MECHANISM_SHA256,
        "readiness_plan_id": READINESS_PLAN_ID,
        "readiness_report_id": READINESS_REPORT_ID,
        "readiness_certificate_id": READINESS_CERTIFICATE_ID,
        "readiness_manifest_id": READINESS_MANIFEST_ID,
        "markets": list(CORE),
        "years": list(YEARS),
        "checkpoint": CHECKPOINT,
        "fold_construction": {
            "outer_folds": OUTER_FOLDS,
            "initial_training_sessions": TRAINING_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "embargo_sessions": EMBARGO_SESSIONS,
            "purge_minutes": PURGE_MINUTES,
            "calendar_basis": "PER_MARKET_CHECKPOINT_ELIGIBLE_SESSIONS",
            "pilot_evaluation_sessions_excluded_from_every_market": True,
        },
        "target_failed_fold_market_count": len(targets),
        "targets": list(targets),
        "classifications": list(CLASSIFICATIONS),
        "diagnostic_fields": [
            "exact_session_id",
            "missing_minute_timestamps",
            "causal_and_late_row_counts",
            "duplicate_timestamp_count",
            "identity_hashes",
            "economics_fingerprints_only",
            "invalid_field_counts_without_values",
            "feature_and_stop_geometry_status",
            "source_row_hashes",
        ],
        "reconciliation_requirements": {
            "exact_target_set": True,
            "exact_training_and_evaluation_session_ids": True,
            "exact_fold_role_feature_gap_counts": True,
            "exact_market_year_feature_gap_counts": True,
            "production_disposition_must_remain_generic_feature_abstention": True,
            "price_values_forbidden": True,
            "returns_forbidden": True,
        },
        "required_outputs": ["diagnostic_report.json"],
        "output_root": OUTPUT_ROOT.as_posix(),
        "execution_limits": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_workers": 4,
            "worker_deadline_seconds": 3300,
            "maximum_runtime_seconds": 3600,
            "maximum_external_cost_usd": "0",
            "windows_host_required": True,
        },
        "authority": {
            "historical_row_read": True,
            "returns": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "registration": False,
            "trial_execution": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "active_data_mutation": False,
            "trading": False,
        },
        "price_free_output": True,
        "calendar_id": calendar["calendar_id"],
        "bindings": dict(sorted(bindings.items())),
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("Alpha feature-gap diagnostic plan drifted")
    return dict(plan)


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="Alpha feature-gap diagnostic plan")
    return validate_plan(plan, root=root)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": MECHANISM_ID,
        "readiness_report_id": READINESS_REPORT_ID,
        "readiness_certificate_id": READINESS_CERTIFICATE_ID,
        "period": "2018,2019,2020,2021,2022",
        "markets": ",".join(CORE),
        "checkpoint": CHECKPOINT,
        "target_failed_fold_market_count": "9",
        "purpose": str(plan["purpose"]),
        "output_root": OUTPUT_ROOT.as_posix(),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "price_free_output": "true",
        "returns": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "performance_evaluation": "false",
        "registration": "false",
        "trial_execution": "false",
        "provider_network_access": "false",
        "holdout_2025_access": "false",
        "active_data_mutation": "false",
        "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _decimal(value: object, *, nano: bool = False) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not result.is_finite():
        return None
    return result / Decimal(1_000_000_000) if nano else result


def _scan_market(task):
    import pyarrow.parquet as pq

    market, sources, target_sessions, cost_ticks = task
    target_set = set(target_sessions)
    rows: dict[str, list[FeatureRow]] = defaultdict(list)
    audits = {}
    for year, raw_path in sources:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        if not REQUIRED_COLUMNS.issubset(parquet.schema_arrow.names):
            raise IntegrityError(f"feature diagnostic schema is incomplete for {market} {year}")
        total = dependency = retained = sessionless = 0
        for batch in parquet.iter_batches(
            batch_size=65_536, columns=sorted(REQUIRED_COLUMNS),
        ):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                total += 1
                raw = {name: values[index] for name, values in columns.items()}
                event_ns = raw.get("event_at_ns")
                if type(event_ns) is not int or not _dependency_clock(event_ns):
                    continue
                dependency += 1
                session = raw.get("exchange_session_date")
                if not isinstance(session, str):
                    sessionless += 1
                    continue
                if session not in target_set:
                    continue
                normalized = source_row_from_mapping(market=market, row=raw)
                event_at = datetime.fromtimestamp(
                    event_ns / 1_000_000_000, timezone.utc,
                ).astimezone(CT)
                available_at = datetime.fromtimestamp(
                    normalized.available_at_ns / 1_000_000_000, timezone.utc,
                ).astimezone(CT)
                rows[session].append(FeatureRow(
                    event_at=event_at,
                    available_at=available_at,
                    executable=normalized.executable,
                    identity=normalized.actual_identity_hash,
                    source_row_sha256=normalized.source_row_sha256,
                    disposition=str(raw.get("disposition")),
                    open=_decimal(raw.get("open_nano"), nano=True),
                    high=_decimal(raw.get("high_nano"), nano=True),
                    low=_decimal(raw.get("low_nano"), nano=True),
                    close=_decimal(raw.get("close_nano"), nano=True),
                    volume=_decimal(raw.get("volume")),
                    tick_size=_decimal(raw.get("tick_size")),
                    tick_value=_decimal(raw.get("tick_value")),
                ))
                retained += 1
        audits[f"{market}/{year}"] = {
            "source_path": path.as_posix(),
            "source_sha256": sha256_file(path),
            "total_rows_scanned": total,
            "dependency_rows_scanned": dependency,
            "target_session_rows_retained": retained,
            "sessionless_dependency_rows": sessionless,
        }
    diagnostics = {
        session: diagnose_feature_session(
            session=session,
            rows=rows.get(session, ()),
            cost_ticks=cost_ticks,
        )
        for session in target_sessions
    }
    return market, diagnostics, audits


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> Mapping[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("feature-gap diagnostic requires Windows main process")
    if (root / OUTPUT_ROOT).exists():
        raise UnauthorizedOperation("feature-gap diagnostic output already exists")
    use_path = receipt.consume(
        boundary,
        operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    selected, by_key = _selected_sources(root=root)
    mechanism = _read_canonical(root / MECHANISM_PATH, name="reported-trade mechanism")
    costs = mechanism["costs"]["round_trip_adverse_ticks"]
    targets = plan["targets"]
    assert isinstance(targets, list)
    sessions_by_market = {
        market: sorted({
            str(session)
            for target in targets if target["market"] == market
            for key in ("training_session_ids", "evaluation_session_ids")
            for session in target[key]
        })
        for market in CORE
    }
    tasks = []
    for market in CORE:
        sources = []
        for year in YEARS:
            item = by_key[(market, year)]
            assert isinstance(item, Mapping)
            path = resolve(repository_root=root, market=market, year=year, purpose="SELECTION")
            relative = path.relative_to(root).as_posix()
            if selected.get(relative) != sha256_file(path):
                raise IntegrityError(f"active source changed for {market} {year}")
            sources.append((year, str(path)))
        market_costs = {scenario: int(costs[scenario][market]) for scenario in SCENARIOS}
        tasks.append((market, tuple(sources), tuple(sessions_by_market[market]), market_costs))
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        worker_results = pool.map_async(_scan_market, tasks, chunksize=1).get(
            timeout=int(plan["execution_limits"]["worker_deadline_seconds"])
        )
        pool.close()
        pool.join()
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    diagnostics = {market: rows for market, rows, _audits in worker_results}
    audits = {
        market: audit for market, _rows, audit in worker_results
    }
    reconciliation = reconcile_diagnostics(targets=targets, diagnostics=diagnostics)
    if monotonic() - started > int(plan["execution_limits"]["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("feature-gap diagnostic exceeded total runtime")
    core = {
        "schema_version": "alpha_ladder_feature_gap_diagnostic/1.0.0",
        "state": "SEALED_UNPUBLISHED_PRICE_FREE_EXACT_RECONCILIATION",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        "readiness_report_id": READINESS_REPORT_ID,
        "readiness_certificate_id": READINESS_CERTIFICATE_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "price_free_output": True,
        "source_audits": audits,
        "reconciliation": reconciliation,
        "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    _write_once(root / OUTPUT_ROOT / "diagnostic_report.json", report)
    return report
