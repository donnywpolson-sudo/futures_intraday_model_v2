"""Fail-closed, one-use economic execution for the registered Alpha ES pilot.

This module intentionally has no command-line entry point.  Preparing its immutable
plan is source-safe; opening the bound Parquet files is possible only after the
CertifiedResearchGateway consumes an exact external authorization receipt.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
from time import monotonic
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from statistics import mean, stdev
from typing import Any

import numpy as np

from .alpha_ladder_limit_readiness import CT, LimitBar, _read_market
from .alpha_ladder_reported_trade_exit_successor import classify_reported_trade_exit
from .boundary import OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .certified_research_gateway import CertifiedResearchGateway
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import CERTIFIED_TRIAL_EXECUTION_OPERATION


TRIAL_ID = "a6ae7b8394906c3661b9f1456f30cf513d5a1df43a072c9e8a601bc8989c82bc"
REGISTRATION_SHA256 = "b2b78123080cf4eb9a09778f0815f12a0d7b1839e4e39d4c147566f6af2e8e44"
REGISTRATION_PATH = Path(
    "state/trial_registry/alpha_ladder_es_pilot/"
    f"{TRIAL_ID}.json"
)
PREDECESSOR_PLAN_PATH = Path("configs/alpha_ladder_es_pilot_execution_plan.json")
PREDECESSOR_PLAN_ID = "ab6d2557b0253ddc9f977c17146b286cb4ee5921a427a26e759bd995a0bd817e"
PREDECESSOR_PLAN_SHA256 = "dcc5f45dc335eca49e9ac31ecadcbcd396a4c6c619de2c3ca667cfe34e16d7bb"
PLAN_PATH = Path("configs/alpha_ladder_es_pilot_execution_plan_v2.json")
MODULE_PATH = Path("src/futures_rebuild/alpha_ladder_es_pilot_execution.py")
PREPARE_SCRIPT_PATH = Path("scripts/prepare_alpha_ladder_es_pilot_execution_plan.py")
TEST_PATH = Path("tests/test_alpha_ladder_es_pilot_execution.py")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_es_pilot_execution") / TRIAL_ID / "attempt-1"
FAILURE_ROOT = Path("state/unpublished_evidence/alpha_ladder_es_pilot_execution_failures") / TRIAL_ID / "attempt-1"
PUBLISHED_ROOT = Path(
    "state/preexecution_certificates/alpha_ladder_full_regular_source_observable/"
    "cf727f9a2955a9909f74201050f2dfd8ccd11d4b78878feb40ad718c22a98f44"
)
PILOT_CERTIFICATE_PATH = PUBLISHED_ROOT / "pilot_readiness_certificate.json"
TIER1_CERTIFICATE_PATH = PUBLISHED_ROOT / "tier1_readiness_certificate.json"
PILOT_SESSION_MANIFEST_PATH = PUBLISHED_ROOT / "pilot_session_manifest.json"
PUBLICATION_MANIFEST_PATH = PUBLISHED_ROOT / "publication_manifest.json"
MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_full_regular_source_observable_successor/"
    "cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3/"
    "mechanism.json"
)
TIER0_CERTIFICATE_PATH = MECHANISM_PATH.with_name("tier0_certificate.json")
TIER0_DECISION_PATH = MECHANISM_PATH.with_name("tier0_decision.json")
ACTIVE_LADDER_POINTER = Path("configs/active_alpha_research_ladder.json")
ACTIVE_CALENDAR_POINTER = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")
FEATURE_NAMES = (
    "log_return_1",
    "log_return_5",
    "log_return_10",
    "intrabar_range_fraction",
    "atr_10_fraction",
    "range_to_atr_10",
    "realized_volatility_10",
    "log1p_volume",
    "volume_zscore_10",
)
TARGET_DIRECTIONS = ("LONG", "SHORT")
SCENARIOS = ("base", "stress", "extreme")
MANDATORY_BASELINES = (
    "flat_no_trade",
    "fold_local_unconditional_direction",
    "previous_reported_bar_sign_momentum",
    "previous_reported_bar_sign_reversal",
    "risk_matched_always_long",
    "risk_matched_always_short",
)
DIRECT_DEPENDENCIES = (
    MODULE_PATH,
    PREPARE_SCRIPT_PATH,
    TEST_PATH,
    Path("src/futures_rebuild/alpha_ladder_limit_readiness.py"),
    Path("src/futures_rebuild/alpha_ladder_reported_trade_exit_successor.py"),
    Path("src/futures_rebuild/alpha_research_ladder.py"),
    Path("src/futures_rebuild/boundary.py"),
    Path("src/futures_rebuild/canonical.py"),
    Path("src/futures_rebuild/certified_research_gateway.py"),
    Path("src/futures_rebuild/preexecution_fold_certification.py"),
    Path("src/futures_rebuild/research_gateway_policy.py"),
    Path("pyproject.toml"),
    Path("requirements.lock"),
    Path("requirements.sha256.lock"),
)


class DataCoverageError(IntegrityError):
    """A sealed readiness claim contradicted the rows opened after authorization."""


@dataclass(frozen=True)
class FeatureContext:
    session: str
    values: tuple[float, ...]
    bars: tuple[LimitBar, ...]
    stop_ticks: int
    tick_size: Decimal
    tick_value: Decimal
    previous_delta: Decimal


@dataclass(frozen=True)
class PathResult:
    disposition: str
    complete: bool
    filled: bool
    direction: str
    scenario: str
    entry_at: datetime | None = None
    exit_at: datetime | None = None
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    stop_price: Decimal | None = None
    gross_pnl_usd: Decimal = Decimal("0")
    fees_usd: Decimal = Decimal("0")
    slippage_usd: Decimal = Decimal("0")
    net_pnl_usd: Decimal = Decimal("0")
    planned_loss_usd: Decimal = Decimal("0")
    net_r: Decimal | None = None
    mark_net_pnls: tuple[Decimal, ...] = ()


@dataclass(frozen=True)
class RidgeBundle:
    feature_mean: tuple[float, ...]
    feature_scale: tuple[float, ...]
    coefficients: Mapping[str, tuple[float, ...]]
    target_counts: Mapping[str, int]
    unconditional_direction: str
    unconditional_means: Mapping[str, float]
    transformation_session_count: int


@dataclass(frozen=True)
class PilotExecutionResult:
    plan_id: str
    authorization_receipt_id: str
    authorization_use_path: str
    output_root: str
    decision: str
    terminal_artifact_id: str


def _object(path: Path, *, name: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"{name} is not canonical single-line JSON")
    return value


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode:
        raise IntegrityError(f"git {' '.join(args)} failed closed")
    return completed.stdout.strip()


def _money(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _float_text(value: float) -> str:
    if not math.isfinite(value):
        raise IntegrityError("non-finite model value")
    return format(value, ".17g")


def _artifact(core: Mapping[str, object], *, identity_key: str) -> dict[str, object]:
    payload = dict(core)
    payload[identity_key] = sha256_json(payload)
    return payload


def _write_exclusive(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(dict(payload)) + b"\n"
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, raw)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if path.read_bytes() != raw:
        raise IntegrityError(f"create-only artifact readback failed: {path}")


def _registered_context(root: Path) -> dict[str, object]:
    registration = _object(root / REGISTRATION_PATH, name="ES pilot registration")
    if (
        sha256_file(root / REGISTRATION_PATH) != REGISTRATION_SHA256
        or registration.get("trial_id") != TRIAL_ID
        or registration.get("state") != "REGISTERED_NOT_CLAIMED_NOT_EXECUTED"
        or registration.get("protocol_id")
        != "cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3"
    ):
        raise IntegrityError("ES pilot registration changed")
    return registration


def _source_bindings_from_certificate(root: Path) -> dict[str, str]:
    certificate = _object(root / PILOT_CERTIFICATE_PATH, name="pilot certificate")
    bindings = certificate.get("source_bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("pilot certificate lost source bindings")
    selected: dict[str, str] = {}
    for year in (2018, 2019, 2020):
        path = f"data/active/causally_gated_normalized/ES/{year}/{year}.parquet"
        digest = bindings.get(path)
        if not isinstance(digest, str) or len(digest) != 64:
            raise IntegrityError(f"pilot certificate lacks ES {year} source")
        selected[path] = digest
    return selected


def _catalog_matches_sources(root: Path, sources: Mapping[str, str]) -> None:
    catalog = _object(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are invalid")
    observed: dict[str, str] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            continue
        path = item.get("parquet_path")
        if path in sources:
            if (
                item.get("market") != "ES"
                or item.get("year") not in {2018, 2019, 2020}
                or item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE"
                or item.get("parquet_sha256") != sources[path]
            ):
                raise IntegrityError(f"active catalog source binding changed: {path}")
            observed[str(path)] = str(item["parquet_sha256"])
    if observed != dict(sources):
        raise IntegrityError("active catalog does not resolve the three pilot sources exactly")


def _immutable_bindings(root: Path) -> dict[str, str]:
    ladder = _object(root / ACTIVE_LADDER_POINTER, name="active Alpha ladder")
    calendar = _object(root / ACTIVE_CALENDAR_POINTER, name="active calendar")
    resolved = []
    for container, keys in (
        (ladder, ("contract_path", "profile_path")),
        (calendar, ("calendar_path", "event_path", "registration_path")),
    ):
        for key in keys:
            value = container.get(key)
            if not isinstance(value, str) or not value:
                raise IntegrityError(f"active pointer lacks {key}")
            resolved.append(Path(value))
    paths = (
        REGISTRATION_PATH,
        PUBLICATION_MANIFEST_PATH,
        PILOT_CERTIFICATE_PATH,
        TIER1_CERTIFICATE_PATH,
        PILOT_SESSION_MANIFEST_PATH,
        MECHANISM_PATH,
        TIER0_CERTIFICATE_PATH,
        TIER0_DECISION_PATH,
        ACTIVE_LADDER_POINTER,
        ACTIVE_CALENDAR_POINTER,
        ACTIVE_CATALOG_PATH,
        *resolved,
        PREDECESSOR_PLAN_PATH,
        *DIRECT_DEPENDENCIES,
    )
    return {path.as_posix(): sha256_file(root / path) for path in paths}


def _plan_core(root: Path) -> dict[str, object]:
    registration = _registered_context(root)
    publication = _object(root / PUBLICATION_MANIFEST_PATH, name="readiness publication")
    pilot = _object(root / PILOT_CERTIFICATE_PATH, name="pilot readiness certificate")
    tier1 = _object(root / TIER1_CERTIFICATE_PATH, name="Tier 1 readiness certificate")
    manifest = _object(root / PILOT_SESSION_MANIFEST_PATH, name="pilot session manifest")
    mechanism = _object(root / MECHANISM_PATH, name="frozen mechanism")
    sources = _source_bindings_from_certificate(root)
    _catalog_matches_sources(root, sources)
    if (
        publication.get("publication_id")
        != "280a7c2954e10a47cfd0e5bfe88e709087c5d0d4073b7b1a781dcf5b4422163a"
        or pilot.get("overall_decision") != "PASS"
        or pilot.get("certificate_id")
        != "ebec0f6cb9db0ab8765975276320a006924504664b24505ff0ce1fa94e1f929b"
        or tier1.get("overall_decision") != "PASS"
        or mechanism.get("mechanism_id") != registration.get("protocol_id")
        or len(manifest.get("training_session_ids", [])) != 504
        or len(manifest.get("evaluation_session_ids", [])) != 63
    ):
        raise IntegrityError("registered pilot prerequisites changed")
    evaluation = tuple(str(item) for item in manifest["evaluation_session_ids"])
    training = tuple(str(item) for item in manifest["training_session_ids"])
    if training[-1] != "2020-01-10" or evaluation[0] != "2020-01-14":
        raise IntegrityError("pilot session boundary changed")
    return {
        "schema_version": "alpha_ladder_es_pilot_execution_plan/1.0.0",
        "state": "PREPARED_NOT_AUTHORIZED_NOT_EXECUTED",
        "predecessor": {
            "plan_path": PREDECESSOR_PLAN_PATH.as_posix(),
            "plan_id": PREDECESSOR_PLAN_ID,
            "plan_sha256": PREDECESSOR_PLAN_SHA256,
            "classification": "INVALID_PREPARATION_SYNTHETIC_ROW_LOADER_INJECTION_SURFACE",
            "executable": False,
        },
        "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "trial_id": TRIAL_ID,
        "trial_family": registration["trial_family"],
        "registration_path": REGISTRATION_PATH.as_posix(),
        "registration_sha256": REGISTRATION_SHA256,
        "publication_id": publication["publication_id"],
        "mechanism_id": mechanism["mechanism_id"],
        "mechanism_sha256": sha256_file(root / MECHANISM_PATH),
        "tier0_decision_sha256": sha256_file(root / TIER0_DECISION_PATH),
        "pilot_readiness_certificate_id": pilot["certificate_id"],
        "tier1_readiness_certificate_id": tier1["certificate_id"],
        "pilot_session_manifest_id": manifest["manifest_id"],
        "session_scope": {
            "market": "ES",
            "training_session_ids": list(training),
            "embargo_session_ids": ["2020-01-13"],
            "evaluation_session_ids": list(evaluation),
            "training_count": 504,
            "embargo_count": 1,
            "evaluation_count": 63,
            "purge_minutes": 40,
        },
        "source_bindings": dict(sorted(sources.items())),
        "immutable_bindings": dict(sorted(_immutable_bindings(root).items())),
        "model": {
            "feature_names": list(FEATURE_NAMES),
            "family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
            "ridge_penalty": "1.0",
            "unpenalized_intercept": True,
            "hyperparameter_search": False,
            "standardization": "TRAINING_ELIGIBLE_ROWS_ONLY_POPULATION_STD",
            "zero_training_std": "STANDARDIZED_VALUE_ZERO",
            "ordered_targets": list(TARGET_DIRECTIONS),
            "exact_tie": "ORDERED_ARGMAX_FIRST_TARGET_LONG",
            "hurdle_stress_net_r": "0.25",
            "hurdle_comparison": "GREATER_THAN_OR_EQUAL",
        },
        "execution": {
            "checkpoint": "10:00_AMERICA_CHICAGO",
            "decision_latency_seconds": 5,
            "trigger_timeout_seconds": 120,
            "entry_limit_timeout_minutes": 5,
            "entry_penetration_ticks": 1,
            "entry_fill": "LIMIT_PRICE_ON_FIRST_LATER_PENETRATING_REPORTED_BAR",
            "same_bar_ordering": "ENTRY_THEN_PROTECTIVE_STOP_CONSERVATIVE",
            "stop": "1.5_ATR20_CEILING_FULL_TICK",
            "scheduled_exit_minutes_after_fill": 30,
            "exit_order_latency_seconds": 5,
            "exit_resolution_minutes": 15,
            "exit_fill": "FIRST_LATER_SAME_IDENTITY_REPORTED_BAR_OPEN",
            "protective_stop_precedence": True,
            "cost_scenarios": list(SCENARIOS),
            "fee_per_side_usd": "5.00",
            "stress_risk_cap_usd": "250",
            "daily_loss_limit_usd": "500",
            "continuous_drawdown_limit_usd": "1500",
            "maximum_entries_per_session": 1,
            "maximum_positions": 1,
            "continuous_bar_marking": (
                "ENTRY_BAR_ADVERSE_ONLY_THEN_LATER_BAR_FAVORABLE_BEFORE_ADVERSE_"
                "WITH_STOP_PRECEDENCE_AND_ENTRY_COST_ALLOCATED_IMMEDIATELY"
            ),
        },
        "baselines": {
            "mandatory": list(MANDATORY_BASELINES),
            "independent_source_schedule_and_risk_state": True,
            "candidate_schedule_reuse": False,
        },
        "metrics": {
            "daily_series_includes_all_63_sessions": True,
            "sharpe": "SQRT_252_X_DAILY_MEAN_DIV_SAMPLE_STD",
            "sortino": "SQRT_252_X_DAILY_MEAN_DIV_ZERO_TARGET_DOWNSIDE_RMS",
            "formal_significance_required": False,
        },
        "promotion_gate": {
            "minimum_trades": 8,
            "stress_net_pnl_positive": True,
            "beat_true_zero_and_all_mandatory_baselines": True,
            "maximum_continuous_drawdown_usd": "1500",
            "complete_coverage_and_metrics": True,
            "live_readiness_claim": False,
        },
        "authority": {
            "attempts": 1,
            "retries": 0,
            "maximum_runtime_seconds": 900,
            "maximum_external_cost_usd": "0",
            "historical_claim_required_before_source_hash_or_open": True,
            "output_root": OUTPUT_ROOT.as_posix(),
            "failure_root": FAILURE_ROOT.as_posix(),
            "terminal_evidence_written_last": True,
            "raw_source_rows_copied_to_evidence": False,
            "required_clean_pushed_head_bound_at_authorization": True,
        },
    }


def build_plan(*, root: Path) -> dict[str, object]:
    core = _plan_core(root.resolve(strict=False))
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(
    plan: Mapping[str, object], *, root: Path, verify_protected: bool = False,
) -> dict[str, object]:
    root = root.resolve(strict=False)
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("ES pilot execution plan changed")
    if plan.get("plan_id") != sha256_json({k: v for k, v in plan.items() if k != "plan_id"}):
        raise IntegrityError("ES pilot execution plan identity changed")
    authority = plan.get("authority")
    if not isinstance(authority, Mapping):
        raise IntegrityError("ES pilot plan authority is invalid")
    for key in ("output_root", "failure_root"):
        path = contained_path(root, str(authority[key]))
        if path.exists():
            raise UnauthorizedOperation(f"ES pilot {key} already exists")
    if verify_protected:
        sources = plan.get("source_bindings")
        if not isinstance(sources, Mapping):
            raise IntegrityError("ES pilot source bindings are invalid")
        for relative, expected_sha in sources.items():
            if sha256_file(contained_path(root, str(relative))) != expected_sha:
                raise IntegrityError(f"protected pilot source changed: {relative}")
    return expected


def load_plan(*, root: Path, verify_protected: bool = False) -> dict[str, object]:
    plan = _object(root.resolve(strict=False) / PLAN_PATH, name="ES pilot execution plan")
    return validate_plan(plan, root=root, verify_protected=verify_protected)


def additional_execution_scope(
    *, root: Path, plan: Mapping[str, object], pushed_git_head: str,
) -> dict[str, str]:
    root = root.resolve(strict=False)
    validate_plan(plan, root=root, verify_protected=False)
    if len(pushed_git_head) != 40 or any(ch not in "0123456789abcdef" for ch in pushed_git_head):
        raise IntegrityError("pushed Git HEAD is invalid")
    sources = plan["source_bindings"]
    assert isinstance(sources, Mapping)
    return {
        "execution_plan_id": str(plan["plan_id"]),
        "execution_plan_sha256": sha256_file(root / PLAN_PATH),
        "execution_source_manifest_sha256": sha256_json(dict(sorted(sources.items()))),
        "execution_output_root": str(plan["authority"]["output_root"]),
        "execution_failure_root": str(plan["authority"]["failure_root"]),
        "execution_maximum_runtime_seconds": str(plan["authority"]["maximum_runtime_seconds"]),
        "execution_maximum_external_cost_usd": str(plan["authority"]["maximum_external_cost_usd"]),
        "execution_pushed_git_head": pushed_git_head,
    }


def required_scope(
    *, root: Path, plan: Mapping[str, object], pushed_git_head: str,
) -> dict[str, str]:
    root = root.resolve(strict=False)
    gateway = CertifiedResearchGateway(root=root, boundary=RepoBoundary(root))
    return gateway.execution_scope(
        registration_path=root / REGISTRATION_PATH,
        expected_registration_sha256=REGISTRATION_SHA256,
        additional_scope=additional_execution_scope(
            root=root, plan=plan, pushed_git_head=pushed_git_head,
        ),
    )


def compute_feature_context(*, session: str, bars: Sequence[LimitBar]) -> FeatureContext:
    ordered = tuple(sorted(bars, key=lambda item: item.event_at))
    if len({bar.event_at for bar in ordered}) != len(ordered):
        raise DataCoverageError(f"{session} contains duplicate source timestamps")
    checkpoint = datetime.combine(date.fromisoformat(session), time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=5)
    causal = tuple(
        bar
        for bar in ordered
        if time(9, 30) <= bar.event_at.time() < time(10, 0)
        and bar.available_at <= decision
    )
    if len(causal) < 21:
        raise DataCoverageError(f"{session} lacks the 21 causal feature bars")
    feature = causal[-21:]
    if (
        len({bar.identity for bar in feature}) != 1
        or len({(bar.tick_size, bar.tick_value) for bar in feature}) != 1
        or any(
            current.event_at - previous.event_at != timedelta(minutes=1)
            for previous, current in zip(feature, feature[1:])
        )
        or any(
            bar.open <= 0
            or bar.high <= 0
            or bar.low <= 0
            or bar.close <= 0
            or bar.volume < 0
            or bar.high < bar.low
            or not bar.low <= bar.open <= bar.high
            or not bar.low <= bar.close <= bar.high
            for bar in feature
        )
    ):
        raise DataCoverageError(f"{session} causal feature bars are invalid")
    closes = [float(bar.close) for bar in feature]
    volumes = [float(bar.volume) for bar in feature]
    one_bar_returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, 21)]
    true_ranges = [
        max(
            feature[index].high - feature[index].low,
            abs(feature[index].high - feature[index - 1].close),
            abs(feature[index].low - feature[index - 1].close),
        )
        for index in range(1, 21)
    ]
    atr10 = sum(true_ranges[-10:], Decimal("0")) / Decimal(10)
    atr20 = sum(true_ranges, Decimal("0")) / Decimal(20)
    if atr10 <= 0 or atr20 <= 0 or feature[-1].tick_size <= 0 or feature[-1].tick_value <= 0:
        raise DataCoverageError(f"{session} feature or stop geometry is invalid")
    recent_volumes = volumes[-10:]
    volume_mean = mean(recent_volumes)
    volume_std = math.sqrt(mean((item - volume_mean) ** 2 for item in recent_volumes))
    realized_mean = mean(one_bar_returns[-10:])
    realized_volatility = math.sqrt(
        mean((item - realized_mean) ** 2 for item in one_bar_returns[-10:])
    )
    previous_close = feature[-2].close
    values = (
        math.log(closes[-1] / closes[-2]),
        math.log(closes[-1] / closes[-6]),
        math.log(closes[-1] / closes[-11]),
        float((feature[-1].high - feature[-1].low) / previous_close),
        float(atr10 / previous_close),
        float((feature[-1].high - feature[-1].low) / atr10),
        realized_volatility,
        math.log1p(volumes[-1]),
        0.0 if volume_std == 0 else (volumes[-1] - volume_mean) / volume_std,
    )
    if len(values) != len(FEATURE_NAMES) or not all(math.isfinite(item) for item in values):
        raise DataCoverageError(f"{session} produced a non-finite feature")
    stop_ticks = int(
        (Decimal("1.5") * atr20 / feature[-1].tick_size).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    if stop_ticks <= 0:
        raise DataCoverageError(f"{session} produced a nonpositive stop")
    return FeatureContext(
        session=session,
        values=values,
        bars=feature,
        stop_ticks=stop_ticks,
        tick_size=feature[-1].tick_size,
        tick_value=feature[-1].tick_value,
        previous_delta=feature[-1].close - feature[-2].close,
    )


def _cost_ticks(mechanism: Mapping[str, object], *, scenario: str) -> int:
    try:
        value = mechanism["costs"]["round_trip_adverse_ticks"][scenario]["ES"]
    except (KeyError, TypeError) as exc:
        raise IntegrityError("mechanism lost ES scenario costs") from exc
    if type(value) is not int or value < 0:
        raise IntegrityError("mechanism ES scenario costs are invalid")
    return value


def _trigger(*, session: str, bars: Sequence[LimitBar]) -> tuple[LimitBar | None, str]:
    checkpoint = datetime.combine(date.fromisoformat(session), time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=5)
    candidates = sorted(
        (
            bar
            for bar in bars
            if bar.event_at >= checkpoint
            and decision < bar.available_at <= decision + timedelta(seconds=120)
        ),
        key=lambda bar: (bar.available_at, bar.event_at),
    )
    if not candidates:
        return None, "EXPLICIT_CAUSAL_NO_TRIGGER_TIMEOUT"
    first = candidates[0]
    if sum(
        (item.available_at, item.event_at) == (first.available_at, first.event_at)
        for item in candidates
    ) != 1:
        raise DataCoverageError(f"{session} trigger evidence is ambiguous")
    return first, "TRIGGER_OBSERVED"


def _penetrates(bar: LimitBar, *, direction: str, limit: Decimal) -> bool:
    if direction == "LONG":
        return bar.low <= limit - bar.tick_size
    if direction == "SHORT":
        return bar.high >= limit + bar.tick_size
    raise IntegrityError("direction is invalid")


def simulate_direction(
    *,
    context: FeatureContext,
    bars: Sequence[LimitBar],
    direction: str,
    scenario: str,
    mechanism: Mapping[str, object],
) -> PathResult:
    if direction not in TARGET_DIRECTIONS or scenario not in SCENARIOS:
        raise IntegrityError("direction or scenario left the frozen domain")
    adverse_ticks = _cost_ticks(mechanism, scenario=scenario)
    fees = Decimal("10")
    slippage = Decimal(adverse_ticks) * context.tick_value
    planned = Decimal(context.stop_ticks) * context.tick_value + fees + slippage
    if planned > Decimal("250"):
        return PathResult(
            "RISK_ABSTENTION", True, False, direction, scenario,
            planned_loss_usd=planned,
        )
    ordered = tuple(sorted(bars, key=lambda item: item.event_at))
    trigger, trigger_disposition = _trigger(session=context.session, bars=ordered)
    if trigger is None:
        return PathResult(
            trigger_disposition, True, False, direction, scenario,
            planned_loss_usd=planned,
        )
    expected_economics = (context.tick_size, context.tick_value)
    if trigger.identity != context.bars[-1].identity or (
        trigger.tick_size,
        trigger.tick_value,
    ) != expected_economics:
        raise DataCoverageError(f"{context.session} trigger identity or economics changed")
    if trigger.available_at < trigger.event_at + timedelta(minutes=1, seconds=5):
        raise DataCoverageError(f"{context.session} trigger availability is too early")
    order_time = trigger.available_at
    entries = sorted(
        (
            bar
            for bar in ordered
            if order_time < bar.event_at <= order_time + timedelta(minutes=5)
            and _penetrates(bar, direction=direction, limit=trigger.close)
        ),
        key=lambda item: item.event_at,
    )
    if not entries:
        return PathResult(
            "EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT",
            True,
            False,
            direction,
            scenario,
            planned_loss_usd=planned,
        )
    entry = entries[0]
    if sum(item.event_at == entry.event_at for item in entries) != 1:
        raise DataCoverageError(f"{context.session} entry evidence is ambiguous")
    if entry.identity != trigger.identity or (entry.tick_size, entry.tick_value) != expected_economics:
        raise DataCoverageError(f"{context.session} entry identity or economics changed")
    if entry.available_at < entry.event_at + timedelta(minutes=1, seconds=5):
        raise DataCoverageError(f"{context.session} entry availability is too early")
    entry_price = trigger.close
    fill_time = entry.event_at + timedelta(minutes=1)
    stop_price = (
        entry_price - Decimal(context.stop_ticks) * context.tick_size
        if direction == "LONG"
        else entry_price + Decimal(context.stop_ticks) * context.tick_size
    )
    scheduled_exit = fill_time + timedelta(minutes=30)
    exit_result = classify_reported_trade_exit(
        bars=ordered,
        scheduled_exit_intent=scheduled_exit,
        identity=entry.identity,
        resolution_minutes=15,
    )
    if not exit_result.complete or exit_result.evidence_bar is None or exit_result.fill_time is None:
        raise DataCoverageError(
            f"{context.session} filled entry lacks its sealed reported-trade exit path"
        )
    exit_bar = exit_result.evidence_bar
    if (exit_bar.tick_size, exit_bar.tick_value) != expected_economics:
        raise DataCoverageError(f"{context.session} exit economics changed")
    entry_cost_ticks = int(math.ceil(adverse_ticks / 2))
    entry_cost = Decimal("5") + Decimal(entry_cost_ticks) * context.tick_value
    mark_pnls: list[Decimal] = [-entry_cost]
    exit_price: Decimal | None = None
    exit_at: datetime | None = None
    disposition = "VERIFIED_CAUSAL_REPORTED_TRADE_EXIT_PROXY"
    path_bars = [
        bar
        for bar in ordered
        if entry.event_at <= bar.event_at <= exit_bar.event_at
    ]
    for index, bar in enumerate(path_bars):
        if bar.identity != entry.identity or (bar.tick_size, bar.tick_value) != expected_economics:
            raise DataCoverageError(f"{context.session} hold identity or economics changed")
        favorable = bar.high if direction == "LONG" else bar.low
        adverse = bar.low if direction == "LONG" else bar.high
        if index > 0:
            favorable_gross = (
                (favorable - entry_price) / context.tick_size * context.tick_value
                if direction == "LONG"
                else (entry_price - favorable) / context.tick_size * context.tick_value
            )
            mark_pnls.append(favorable_gross - entry_cost)
        adverse_gross = (
            (adverse - entry_price) / context.tick_size * context.tick_value
            if direction == "LONG"
            else (entry_price - adverse) / context.tick_size * context.tick_value
        )
        mark_pnls.append(adverse_gross - entry_cost)
        stopped = bar.low <= stop_price if direction == "LONG" else bar.high >= stop_price
        if stopped:
            gap = bar.open <= stop_price if direction == "LONG" else bar.open >= stop_price
            exit_price = bar.open if gap else stop_price
            exit_at = fill_time if index == 0 else bar.event_at
            disposition = "VERIFIED_PROTECTIVE_STOP"
            break
        if bar.event_at == exit_bar.event_at:
            exit_price = exit_bar.open
            exit_at = exit_result.fill_time
            break
    if exit_price is None or exit_at is None:
        raise DataCoverageError(f"{context.session} execution path did not terminalize")
    gross = (
        (exit_price - entry_price) / context.tick_size * context.tick_value
        if direction == "LONG"
        else (entry_price - exit_price) / context.tick_size * context.tick_value
    )
    net = gross - fees - slippage
    mark_pnls.append(net)
    return PathResult(
        disposition=disposition,
        complete=True,
        filled=True,
        direction=direction,
        scenario=scenario,
        entry_at=fill_time,
        exit_at=exit_at,
        entry_price=entry_price,
        exit_price=exit_price,
        stop_price=stop_price,
        gross_pnl_usd=gross,
        fees_usd=fees,
        slippage_usd=slippage,
        net_pnl_usd=net,
        planned_loss_usd=planned,
        net_r=net / planned,
        mark_net_pnls=tuple(mark_pnls),
    )


def fit_models(
    *,
    training_sessions: Sequence[str],
    bars_by_session: Mapping[str, Sequence[LimitBar]],
    mechanism: Mapping[str, object],
) -> tuple[RidgeBundle, dict[str, FeatureContext]]:
    contexts: dict[str, FeatureContext] = {}
    targets: dict[str, dict[str, float]] = {direction: {} for direction in TARGET_DIRECTIONS}
    eligible_sessions: set[str] = set()
    for session in training_sessions:
        context = compute_feature_context(session=session, bars=bars_by_session.get(session, ()))
        contexts[session] = context
        for direction in TARGET_DIRECTIONS:
            result = simulate_direction(
                context=context,
                bars=bars_by_session.get(session, ()),
                direction=direction,
                scenario="stress",
                mechanism=mechanism,
            )
            if not result.complete:
                raise DataCoverageError(f"{session} training target path is incomplete")
            if result.filled:
                assert result.net_r is not None
                targets[direction][session] = float(result.net_r)
                eligible_sessions.add(session)
    ordered = tuple(session for session in training_sessions if session in eligible_sessions)
    if not ordered or any(not targets[direction] for direction in TARGET_DIRECTIONS):
        raise DataCoverageError("pilot training rows cannot fit both directional targets")
    x = np.asarray([contexts[session].values for session in ordered], dtype=np.float64)
    feature_mean = x.mean(axis=0)
    feature_scale = x.std(axis=0, ddof=0)
    if not np.isfinite(x).all() or not np.isfinite(feature_mean).all() or not np.isfinite(feature_scale).all():
        raise DataCoverageError("training-only transformation is non-finite")
    safe_scale = np.where(feature_scale == 0.0, 1.0, feature_scale)
    coefficients: dict[str, tuple[float, ...]] = {}
    target_means: dict[str, float] = {}
    for direction in TARGET_DIRECTIONS:
        sessions = tuple(session for session in ordered if session in targets[direction])
        rows = np.asarray([contexts[session].values for session in sessions], dtype=np.float64)
        standardized = (rows - feature_mean) / safe_scale
        standardized[:, feature_scale == 0.0] = 0.0
        design = np.column_stack((np.ones(len(standardized)), standardized))
        response = np.asarray([targets[direction][session] for session in sessions], dtype=np.float64)
        penalty = np.eye(design.shape[1], dtype=np.float64)
        penalty[0, 0] = 0.0
        try:
            solved = np.linalg.solve(design.T @ design + penalty, design.T @ response)
        except np.linalg.LinAlgError as exc:
            raise DataCoverageError("fixed Ridge system is singular") from exc
        if not np.isfinite(solved).all():
            raise DataCoverageError("fixed Ridge coefficients are non-finite")
        coefficients[direction] = tuple(float(item) for item in solved)
        target_means[direction] = float(response.mean())
    unconditional = max(TARGET_DIRECTIONS, key=lambda item: target_means[item])
    return (
        RidgeBundle(
            feature_mean=tuple(float(item) for item in feature_mean),
            feature_scale=tuple(float(item) for item in feature_scale),
            coefficients=coefficients,
            target_counts={key: len(value) for key, value in targets.items()},
            unconditional_direction=unconditional,
            unconditional_means=target_means,
            transformation_session_count=len(ordered),
        ),
        contexts,
    )


def predict(bundle: RidgeBundle, context: FeatureContext) -> tuple[dict[str, float], str, float]:
    vector = np.asarray(context.values, dtype=np.float64)
    mean_vector = np.asarray(bundle.feature_mean, dtype=np.float64)
    scale = np.asarray(bundle.feature_scale, dtype=np.float64)
    standardized = np.zeros_like(vector)
    nonzero = scale != 0.0
    standardized[nonzero] = (vector[nonzero] - mean_vector[nonzero]) / scale[nonzero]
    design = np.concatenate(([1.0], standardized))
    predictions = {
        direction: float(design @ np.asarray(bundle.coefficients[direction], dtype=np.float64))
        for direction in TARGET_DIRECTIONS
    }
    if not all(math.isfinite(value) for value in predictions.values()):
        raise DataCoverageError("evaluation prediction is non-finite")
    selected, score, _qualifies = select_candidate(predictions)
    return predictions, selected, score


def select_candidate(predictions: Mapping[str, float]) -> tuple[str, float, bool]:
    """Apply ordered-target argmax and the inclusive locked +0.25R hurdle."""

    if set(predictions) != set(TARGET_DIRECTIONS) or any(
        not math.isfinite(float(predictions[key])) for key in TARGET_DIRECTIONS
    ):
        raise IntegrityError("directional predictions are incomplete or non-finite")
    # max() retains the first item for exact ties, so LONG wins the locked order.
    selected = max(TARGET_DIRECTIONS, key=lambda item: float(predictions[item]))
    score = float(predictions[selected])
    return selected, score, score >= 0.25


def _serialize_path(path: PathResult) -> dict[str, object]:
    return {
        "disposition": path.disposition,
        "complete": path.complete,
        "filled": path.filled,
        "direction": path.direction,
        "scenario": path.scenario,
        "entry_at": path.entry_at.isoformat() if path.entry_at else None,
        "exit_at": path.exit_at.isoformat() if path.exit_at else None,
        "entry_price": _money(path.entry_price) if path.entry_price is not None else None,
        "exit_price": _money(path.exit_price) if path.exit_price is not None else None,
        "stop_price": _money(path.stop_price) if path.stop_price is not None else None,
        "gross_pnl_usd": _money(path.gross_pnl_usd),
        "fees_usd": _money(path.fees_usd),
        "slippage_usd": _money(path.slippage_usd),
        "net_pnl_usd": _money(path.net_pnl_usd),
        "planned_loss_usd": _money(path.planned_loss_usd),
        "net_r": _money(path.net_r) if path.net_r is not None else None,
    }


def _account_path(
    *, sessions: Sequence[str], actions: Mapping[str, PathResult],
) -> dict[str, object]:
    equity = Decimal("0")
    peak = Decimal("0")
    maximum_drawdown = Decimal("0")
    permanently_halted = False
    daily: list[dict[str, object]] = []
    admitted: list[dict[str, object]] = []
    abstentions: Counter[str] = Counter()
    for session in sessions:
        action = actions[session]
        if not action.complete:
            raise DataCoverageError(f"{session} action did not terminalize")
        if action.filled and permanently_halted:
            abstentions["DRAWDOWN_BLOCKED"] += 1
            daily.append({"session": session, "net_pnl_usd": "0", "disposition": "DRAWDOWN_BLOCKED"})
            continue
        if not action.filled:
            abstentions[action.disposition] += 1
            daily.append({"session": session, "net_pnl_usd": "0", "disposition": action.disposition})
            continue
        start_equity = equity
        for relative in action.mark_net_pnls:
            marked = start_equity + relative
            peak = max(peak, marked)
            maximum_drawdown = max(maximum_drawdown, peak - marked)
        equity = start_equity + action.net_pnl_usd
        peak = max(peak, equity)
        maximum_drawdown = max(maximum_drawdown, peak - equity)
        if peak - equity >= Decimal("1500"):
            permanently_halted = True
        admitted.append({"session": session, **_serialize_path(action)})
        daily.append(
            {"session": session, "net_pnl_usd": _money(action.net_pnl_usd), "disposition": action.disposition}
        )
    return {
        "daily": daily,
        "admitted": admitted,
        "abstentions": dict(sorted(abstentions.items())),
        "ending_equity_usd": _money(equity),
        "maximum_continuous_drawdown_usd": _money(maximum_drawdown),
        "permanently_halted": permanently_halted,
    }


def _metrics(account: Mapping[str, object], *, expected_sessions: int) -> dict[str, object]:
    daily = account["daily"]
    admitted = account["admitted"]
    if not isinstance(daily, list) or not isinstance(admitted, list) or len(daily) != expected_sessions:
        raise DataCoverageError("daily accounting is incomplete")
    daily_values = [float(item["net_pnl_usd"]) for item in daily]
    daily_mean = mean(daily_values)
    daily_std = stdev(daily_values) if len(daily_values) >= 2 else 0.0
    downside = math.sqrt(mean(min(item, 0.0) ** 2 for item in daily_values))
    sharpe = None if daily_std == 0 else math.sqrt(252.0) * daily_mean / daily_std
    sortino = None if downside == 0 else math.sqrt(252.0) * daily_mean / downside
    gross = sum((Decimal(str(item["gross_pnl_usd"])) for item in admitted), Decimal("0"))
    fees = sum((Decimal(str(item["fees_usd"])) for item in admitted), Decimal("0"))
    slippage = sum((Decimal(str(item["slippage_usd"])) for item in admitted), Decimal("0"))
    net = sum((Decimal(str(item["net_pnl_usd"])) for item in admitted), Decimal("0"))
    winners = sum(Decimal(str(item["net_pnl_usd"])) > 0 for item in admitted)
    gross_minutes = Decimal("0")
    net_minutes = Decimal("0")
    exits: Counter[str] = Counter()
    for item in admitted:
        entry = datetime.fromisoformat(str(item["entry_at"]))
        exit_at = datetime.fromisoformat(str(item["exit_at"]))
        minutes = Decimal(str((exit_at - entry).total_seconds())) / Decimal("60")
        gross_minutes += abs(minutes)
        net_minutes += minutes if item["direction"] == "LONG" else -minutes
        exits[str(item["disposition"])] += 1
    return {
        "gross_pnl_usd": _money(gross),
        "fees_usd": _money(fees),
        "slippage_usd": _money(slippage),
        "net_pnl_usd": _money(net),
        "daily_annualized_sharpe_252": None if sharpe is None else _float_text(sharpe),
        "daily_annualized_sortino_252": None if sortino is None else _float_text(sortino),
        "maximum_continuous_drawdown_usd": account["maximum_continuous_drawdown_usd"],
        "turnover_contract_round_trips": len(admitted),
        "trade_count": len(admitted),
        "hit_rate": None if not admitted else _float_text(winners / len(admitted)),
        "gross_exposure_contract_minutes": _money(gross_minutes),
        "net_exposure_contract_minutes": _money(net_minutes),
        "exit_dispositions": dict(sorted(exits.items())),
        "abstention_dispositions": account["abstentions"],
        "coverage": {
            "expected_sessions": expected_sessions,
            "terminal_sessions": len(daily),
            "complete": len(daily) == expected_sessions,
        },
    }


def classify_pilot_gate(strategies: Mapping[str, object]) -> tuple[str, list[str]]:
    """Reconstruct the exact pilot gate from complete strategy metrics."""

    try:
        candidate_stress = strategies["candidate"]["stress"]["metrics"]
    except (KeyError, TypeError) as exc:
        raise DataCoverageError("candidate stress metrics are missing") from exc
    if not isinstance(candidate_stress, Mapping):
        raise DataCoverageError("candidate stress metrics are invalid")
    failed: list[str] = []
    if int(candidate_stress["trade_count"]) < 8:
        failed.append("MINIMUM_EIGHT_TRADES")
    if Decimal(str(candidate_stress["net_pnl_usd"])) <= 0:
        failed.append("POSITIVE_STRESS_NET_PNL")
    for baseline in MANDATORY_BASELINES:
        try:
            baseline_metrics = strategies[baseline]["stress"]["metrics"]
        except (KeyError, TypeError) as exc:
            raise DataCoverageError(f"mandatory baseline metrics are missing: {baseline}") from exc
        if not isinstance(baseline_metrics, Mapping):
            raise DataCoverageError(f"mandatory baseline metrics are invalid: {baseline}")
        if Decimal(str(candidate_stress["net_pnl_usd"])) <= Decimal(
            str(baseline_metrics["net_pnl_usd"])
        ):
            failed.append(f"BEAT_BASELINE__{baseline}")
    if Decimal(str(candidate_stress["maximum_continuous_drawdown_usd"])) > Decimal("1500"):
        failed.append("MAXIMUM_CONTINUOUS_DRAWDOWN_1500")
    if candidate_stress.get("coverage") != {
        "expected_sessions": 63,
        "terminal_sessions": 63,
        "complete": True,
    }:
        failed.append("COMPLETE_COVERAGE_AND_METRICS")
    return ("PASS" if not failed else "FAIL"), failed


def evaluate_loaded_rows(
    *,
    plan: Mapping[str, object],
    mechanism: Mapping[str, object],
    bars_by_session: Mapping[str, Sequence[LimitBar]],
) -> dict[str, object]:
    session_scope = plan["session_scope"]
    assert isinstance(session_scope, Mapping)
    training = tuple(str(item) for item in session_scope["training_session_ids"])
    evaluation = tuple(str(item) for item in session_scope["evaluation_session_ids"])
    bundle, _training_contexts = fit_models(
        training_sessions=training,
        bars_by_session=bars_by_session,
        mechanism=mechanism,
    )
    contexts = {
        session: compute_feature_context(session=session, bars=bars_by_session.get(session, ()))
        for session in evaluation
    }
    predictions: list[dict[str, object]] = []
    candidate_directions: dict[str, str | None] = {}
    for session in evaluation:
        values, selected, score = predict(bundle, contexts[session])
        _selected_check, _score_check, qualifies = select_candidate(values)
        if (_selected_check, _score_check) != (selected, score):
            raise IntegrityError("prediction selection replay changed")
        candidate_directions[session] = selected if qualifies else None
        predictions.append(
            {
                "session": session,
                "long_predicted_stress_net_r": _float_text(values["LONG"]),
                "short_predicted_stress_net_r": _float_text(values["SHORT"]),
                "selected_direction": selected,
                "selected_predicted_stress_net_r": _float_text(score),
                "hurdle_passed": qualifies,
            }
        )
    model = {
        "feature_names": list(FEATURE_NAMES),
        "training_only_mean": [_float_text(item) for item in bundle.feature_mean],
        "training_only_population_std": [_float_text(item) for item in bundle.feature_scale],
        "coefficients": {
            key: [_float_text(item) for item in bundle.coefficients[key]]
            for key in TARGET_DIRECTIONS
        },
        "target_counts": dict(bundle.target_counts),
        "transformation_session_count": bundle.transformation_session_count,
        "unconditional_direction": bundle.unconditional_direction,
        "unconditional_stress_net_r_means": {
            key: _float_text(bundle.unconditional_means[key]) for key in TARGET_DIRECTIONS
        },
        "ridge_penalty": "1.0",
        "intercept_penalized": False,
        "parameter_search": False,
    }
    strategies: dict[str, dict[str, object]] = {}
    for strategy in ("candidate", *MANDATORY_BASELINES):
        scenarios: dict[str, object] = {}
        for scenario in SCENARIOS:
            actions: dict[str, PathResult] = {}
            for session in evaluation:
                context = contexts[session]
                direction: str | None
                if strategy == "candidate":
                    direction = candidate_directions[session]
                    if direction is None:
                        actions[session] = PathResult(
                            "BELOW_PREDICTED_STRESS_NET_HURDLE",
                            True,
                            False,
                            "NONE",
                            scenario,
                        )
                        continue
                elif strategy == "flat_no_trade":
                    actions[session] = PathResult(
                        "TRUE_ZERO_NO_TRADE", True, False, "NONE", scenario,
                    )
                    continue
                elif strategy == "fold_local_unconditional_direction":
                    direction = bundle.unconditional_direction
                elif strategy == "risk_matched_always_long":
                    direction = "LONG"
                elif strategy == "risk_matched_always_short":
                    direction = "SHORT"
                elif strategy in {
                    "previous_reported_bar_sign_momentum",
                    "previous_reported_bar_sign_reversal",
                }:
                    if context.previous_delta == 0:
                        actions[session] = PathResult(
                            "EXPLICIT_CAUSAL_ZERO_SIGN_ABSTENTION",
                            True,
                            False,
                            "NONE",
                            scenario,
                        )
                        continue
                    momentum = "LONG" if context.previous_delta > 0 else "SHORT"
                    direction = (
                        momentum
                        if strategy == "previous_reported_bar_sign_momentum"
                        else ("SHORT" if momentum == "LONG" else "LONG")
                    )
                else:
                    raise IntegrityError(f"unknown strategy: {strategy}")
                actions[session] = simulate_direction(
                    context=context,
                    bars=bars_by_session.get(session, ()),
                    direction=direction,
                    scenario=scenario,
                    mechanism=mechanism,
                )
            account = _account_path(sessions=evaluation, actions=actions)
            scenarios[scenario] = {
                "account": account,
                "metrics": _metrics(account, expected_sessions=len(evaluation)),
            }
        strategies[strategy] = scenarios
    decision, failed = classify_pilot_gate(strategies)
    return {
        "model": model,
        "predictions": predictions,
        "strategies": strategies,
        "decision": decision,
        "failed_gates": failed,
        "pilot_role": "GO_NO_GO_SCREEN_ONLY",
        "formal_significance_required": False,
        "live_readiness_claim": False,
    }


def _preclaim_state(
    *, root: Path, plan: Mapping[str, object], receipt: OperationReceipt,
) -> str:
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise UnauthorizedOperation("ES pilot requires an exactly clean worktree before claim")
    head = _git(root, "rev-parse", "HEAD")
    origin = _git(root, "rev-parse", "origin/main")
    scope = dict(receipt.scope)
    expected_head = scope.get("execution_pushed_git_head")
    if head != expected_head or origin != expected_head:
        raise UnauthorizedOperation("ES pilot HEAD is not the approved pushed origin/main")
    expected_scope = required_scope(root=root, plan=plan, pushed_git_head=head)
    observed = {
        key: value
        for key, value in scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    if observed != expected_scope:
        raise UnauthorizedOperation("ES pilot receipt scope changed")
    if (root / OUTPUT_ROOT).exists() or (root / FAILURE_ROOT).exists():
        raise UnauthorizedOperation("ES pilot output or failure root already exists")
    return head


def _load_authorized_rows(
    *, root: Path, plan: Mapping[str, object], mechanism: Mapping[str, object],
) -> tuple[dict[str, Sequence[LimitBar]], dict[str, object]]:
    validate_plan(plan, root=root, verify_protected=True)
    source_bindings = plan["source_bindings"]
    assert isinstance(source_bindings, Mapping)
    sources = tuple(
        (int(Path(relative).stem), str(contained_path(root, str(relative))))
        for relative in sorted(source_bindings)
    )
    cost_ticks = {
        scenario: _cost_ticks(mechanism, scenario=scenario) for scenario in SCENARIOS
    }
    market, bars_by_session, _risk, audits = _read_market(("ES", sources, cost_ticks))
    if market != "ES":
        raise IntegrityError("authorized source loader changed market")
    return dict(bars_by_session), dict(audits)


def _sealed_outputs(
    *,
    root: Path,
    plan: Mapping[str, object],
    receipt: OperationReceipt,
    use_path: Path,
    source_audit: Mapping[str, object],
    evaluation: Mapping[str, object],
) -> list[tuple[str, dict[str, object], str]]:
    try:
        relative_use_path = use_path.relative_to(root).as_posix()
    except ValueError:
        relative_use_path = use_path.as_posix()
    common = {
        "trial_id": TRIAL_ID,
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": relative_use_path,
    }
    input_audit = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_input_audit/1.0.0",
            **common,
            "source_audit": dict(source_audit),
            "source_bindings": plan["source_bindings"],
            "training_sessions": 504,
            "embargo_sessions": 1,
            "evaluation_sessions": 63,
            "raw_source_rows_copied": False,
        },
        identity_key="input_audit_id",
    )
    model = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_model/1.0.0",
            **common,
            **dict(evaluation["model"]),
        },
        identity_key="model_artifact_id",
    )
    predictions = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_predictions/1.0.0",
            **common,
            "rows": evaluation["predictions"],
            "expected_rows": 63,
        },
        identity_key="prediction_artifact_id",
    )
    candidate = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_candidate_execution/1.0.0",
            **common,
            "scenarios": evaluation["strategies"]["candidate"],
        },
        identity_key="candidate_execution_id",
    )
    baselines = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_baseline_executions/1.0.0",
            **common,
            "independently_scheduled": True,
            "candidate_schedule_reused": False,
            "strategies": {
                key: value
                for key, value in evaluation["strategies"].items()
                if key != "candidate"
            },
        },
        identity_key="baseline_execution_id",
    )
    metrics = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_metrics/1.0.0",
            **common,
            "strategies": {
                strategy: {
                    scenario: result["metrics"] for scenario, result in scenarios.items()
                }
                for strategy, scenarios in evaluation["strategies"].items()
            },
            "daily_series_include_zero_no_trade_sessions": True,
        },
        identity_key="metrics_artifact_id",
    )
    terminal = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_terminal_report/1.0.0",
            **common,
            "state": "SEALED_UNPUBLISHED_ECONOMIC_SCREEN_COMPLETE",
            "decision": evaluation["decision"],
            "failed_gates": evaluation["failed_gates"],
            "economic_evaluation_occurred": True,
            "retry_authorized": False,
            "tier1_registered_or_executed": False,
            "holdout_2025_accessed": False,
            "live_readiness_claim": False,
        },
        identity_key="terminal_report_id",
    )
    decision = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_decision/1.0.0",
            **common,
            "decision": evaluation["decision"],
            "failed_gates": evaluation["failed_gates"],
            "role": "GO_NO_GO_SCREEN_ONLY",
            "tier1_preparation_allowed": evaluation["decision"] == "PASS",
            "automatic_tuning_or_successor": False,
            "formal_significance_claim": False,
            "live_readiness_claim": False,
            "terminal_report_id": terminal["terminal_report_id"],
        },
        identity_key="pilot_decision_id",
    )
    return [
        ("input_audit.json", input_audit, "input_audit_id"),
        ("model.json", model, "model_artifact_id"),
        ("predictions.json", predictions, "prediction_artifact_id"),
        ("candidate_execution.json", candidate, "candidate_execution_id"),
        ("baseline_executions.json", baselines, "baseline_execution_id"),
        ("metrics.json", metrics, "metrics_artifact_id"),
        ("terminal_report.json", terminal, "terminal_report_id"),
        ("pilot_decision.json", decision, "pilot_decision_id"),
    ]


def _seal_failure(
    *, root: Path, plan: Mapping[str, object], receipt: OperationReceipt,
    use_path: Path, classification: str, exc: BaseException,
) -> None:
    try:
        relative_use_path = use_path.relative_to(root).as_posix()
    except ValueError:
        relative_use_path = use_path.as_posix()
    failure = _artifact(
        {
            "schema_version": "alpha_ladder_es_pilot_execution_failure/1.0.0",
            "trial_id": TRIAL_ID,
            "plan_id": plan["plan_id"],
            "authorization_receipt_id": receipt.receipt_id,
            "authorization_use_path": relative_use_path,
            "classification": classification,
            "exception_type": type(exc).__name__,
            "message": str(exc),
            "attempt_consumed": True,
            "retry_authorized": False,
            "economic_result_produced": False,
        },
        identity_key="failure_id",
    )
    _write_exclusive(root / FAILURE_ROOT / "execution_failure.json", failure)


def execute_once(
    *,
    root: Path,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
) -> PilotExecutionResult:
    """Consume one certified claim, then execute and seal exactly one pilot result."""

    started = monotonic()
    root = root.resolve(strict=False)
    boundary.assert_active_root(root)
    plan = load_plan(root=root, verify_protected=False)
    _preclaim_state(root=root, plan=plan, receipt=receipt)
    gateway = CertifiedResearchGateway(root=root, boundary=boundary)
    scope = dict(receipt.scope)
    use_path = gateway.claim_historical_execution(
        registration_path=root / REGISTRATION_PATH,
        expected_registration_sha256=REGISTRATION_SHA256,
        receipt=receipt,
        additional_scope=additional_execution_scope(
            root=root,
            plan=plan,
            pushed_git_head=str(scope["execution_pushed_git_head"]),
        ),
    )
    try:
        mechanism = _object(root / MECHANISM_PATH, name="frozen mechanism")
        bars_by_session, source_audit = _load_authorized_rows(
            root=root, plan=plan, mechanism=mechanism,
        )
        if monotonic() - started > 900:
            raise TimeoutError("ES pilot exceeded its 900-second runtime after source load")
        first = evaluate_loaded_rows(
            plan=plan, mechanism=mechanism, bars_by_session=bars_by_session,
        )
        second = evaluate_loaded_rows(
            plan=plan, mechanism=mechanism, bars_by_session=bars_by_session,
        )
        if canonical_bytes(first) != canonical_bytes(second):
            raise IntegrityError("same-attempt deterministic replay changed")
        if monotonic() - started > 900:
            raise TimeoutError("ES pilot exceeded its 900-second runtime after replay")
        outputs = _sealed_outputs(
            root=root,
            plan=plan,
            receipt=receipt,
            use_path=use_path,
            source_audit=source_audit,
            evaluation=first,
        )
        # pilot_decision.json is deliberately last.
        for name, payload, _identity in outputs:
            _write_exclusive(root / OUTPUT_ROOT / name, payload)
        terminal = outputs[-1][1]
        return PilotExecutionResult(
            plan_id=str(plan["plan_id"]),
            authorization_receipt_id=receipt.receipt_id,
            authorization_use_path=use_path.relative_to(root).as_posix(),
            output_root=OUTPUT_ROOT.as_posix(),
            decision=str(first["decision"]),
            terminal_artifact_id=str(terminal["pilot_decision_id"]),
        )
    except BaseException as exc:
        classification = (
            "INCONCLUSIVE_DATA_OR_COVERAGE"
            if isinstance(exc, DataCoverageError)
            else "IMPLEMENTATION_INVALID_TERMINAL_FAILURE"
        )
        try:
            _seal_failure(
                root=root,
                plan=plan,
                receipt=receipt,
                use_path=use_path,
                classification=classification,
                exc=exc,
            )
        except BaseException:
            pass
        raise
