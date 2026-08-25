"""Observation-only prediction sources for the cockpit display contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol, Sequence

from .protocol import direction_entropy, timestamp_seconds
from .model_runtime import ModelSpec


@dataclass(frozen=True)
class PredictionContext:
    market: str
    contract: str
    instrument_id: int | None
    timeframe: str
    generation: int
    bars: Sequence[Mapping[str, Any]]
    prediction_time: datetime


class PredictionSource(Protocol):
    def build(self, context: PredictionContext) -> dict[str, Any]: ...


def _model_metadata() -> dict[str, str]:
    return {
        "id": "synthetic-shadow-demo",
        "version": "1",
        "strategy": "direction-probability-ui",
    }


def _completed_input_bar_time(context: PredictionContext) -> int | None:
    if len(context.bars) < 2:
        return None
    return timestamp_seconds(context.bars[-2]["time"])


def _base_payload(
    context: PredictionContext,
    *,
    state: str,
    source: str,
    synthetic: bool,
    input_bar_time: int | None,
    reason_codes: list[str],
    forecast: Mapping[str, Any] | None,
    model: Mapping[str, str] | None,
) -> dict[str, Any]:
    prediction_time = timestamp_seconds(context.prediction_time)
    identity_time = input_bar_time if input_bar_time is not None else prediction_time
    return {
        "market": context.market,
        "contract": context.contract,
        "instrument_id": context.instrument_id,
        "timeframe": context.timeframe,
        "generation": context.generation,
        "prediction_id": (
            f"{source.lower()}:{context.market}:{context.timeframe}:"
            f"{identity_time}:{state.lower()}"
        ),
        "prediction_time": prediction_time,
        "input_bar_time": input_bar_time,
        "state": state,
        "source": source,
        "synthetic": synthetic,
        "observation_only": True,
        "model": dict(model) if model is not None else None,
        "forecast": dict(forecast) if forecast is not None else None,
        "reason_codes": list(reason_codes),
    }


class NullPredictionSource:
    """Fail-closed live-mode source: it never produces a forecast."""

    def build(self, context: PredictionContext) -> dict[str, Any]:
        return _base_payload(
            context,
            state="OFFLINE",
            source="NONE",
            synthetic=False,
            input_bar_time=None,
            reason_codes=["MODEL_NOT_AUTHORIZED"],
            forecast=None,
            model=None,
        )


class SyntheticPredictionSource:
    """Deterministic provider-free scenarios for visual and contract testing."""

    _READY = {
        "ES": {
            "direction": "LONG",
            "probabilities": {"long": 0.64, "flat": 0.21, "short": 0.15},
            "expected_return": 0.00045,
        },
        "NQ": {
            "direction": "SHORT",
            "probabilities": {"long": 0.20, "flat": 0.18, "short": 0.62},
            "expected_return": -0.00055,
        },
    }
    _NON_READY = {
        "RTY": ("ABSTAIN", "MODEL_ABSTAINED"),
        "YM": ("WARMING_UP", "FEATURE_WARMUP_INCOMPLETE"),
        "CL": ("STALE", "DATA_STALE"),
        "NG": ("ERROR", "SYNTHETIC_DEMO_ERROR"),
    }

    def build(self, context: PredictionContext) -> dict[str, Any]:
        input_bar_time = _completed_input_bar_time(context)
        ready = self._READY.get(context.market)
        if ready is not None and input_bar_time is not None:
            probabilities = ready["probabilities"]
            forecast = {
                "direction": ready["direction"],
                "horizon_seconds": 15 * 60,
                "probabilities": dict(probabilities),
                "expected_return": ready["expected_return"],
                "direction_entropy": direction_entropy(probabilities),
            }
            return _base_payload(
                context,
                state="READY",
                source="SYNTHETIC_DEMO",
                synthetic=True,
                input_bar_time=input_bar_time,
                reason_codes=[],
                forecast=forecast,
                model=_model_metadata(),
            )

        if ready is not None:
            state, reason = "WARMING_UP", "FEATURE_WARMUP_INCOMPLETE"
        else:
            state, reason = self._NON_READY.get(
                context.market, ("ABSTAIN", "OUTSIDE_DEMO_SCENARIO")
            )
        return _base_payload(
            context,
            state=state,
            source="SYNTHETIC_DEMO",
            synthetic=True,
            input_bar_time=input_bar_time,
            reason_codes=[reason],
            forecast=None,
            model=_model_metadata(),
        )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def repository_model_payload(
    *,
    spec: ModelSpec,
    market: str,
    contract: str,
    instrument_id: int | None,
    generation: int,
    display_timeframe: str,
    prediction_time: datetime,
    state: str,
    reason_code: str,
    decision: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Translate validated worker output into the existing observation-only UI contract."""

    input_time = int(decision["input_bar_time"]) if decision is not None else None
    forecast = None
    reasons = [reason_code]
    if state == "READY" and decision is not None:
        probabilities = dict(decision["probabilities"])
        forecast = {
            "direction": decision["decision"],
            "confidence": float(decision["confidence"]),
            "horizon_seconds": int(decision["horizon_seconds"]),
            "probabilities": probabilities,
            "expected_return": float(decision["expected_return"]),
            "direction_entropy": direction_entropy(probabilities),
        }
    identity_time = input_time if input_time is not None else timestamp_seconds(prediction_time)
    return {
        "market": market,
        "contract": contract,
        "instrument_id": instrument_id,
        "timeframe": display_timeframe,
        "input_schema": spec.schema,
        "generation": generation,
        "prediction_id": f"repository-model:{spec.model_id}:{spec.version}:{market}:{identity_time}:{state.lower()}",
        "prediction_time": timestamp_seconds(prediction_time),
        "input_bar_time": input_time,
        "state": state,
        "source": "REPOSITORY_MODEL",
        "synthetic": False,
        "observation_only": True,
        "model": {"id": spec.model_id, "version": spec.version, "strategy": spec.strategy},
        "forecast": forecast,
        "reason_codes": reasons,
    }
