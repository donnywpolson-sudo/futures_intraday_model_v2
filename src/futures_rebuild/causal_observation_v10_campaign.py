"""Provider-free V10 campaign state machine and immutable event journal."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Sequence

from .canonical import canonical_bytes, io_path, sha256_json
from .causal_observation_market_checkpoint import MARKET_ORDER
from .errors import IntegrityError, UnauthorizedOperation


SCHEMA_VERSION = "causal_observation_v10_campaign_state/1.0.0"
JOURNAL_SCHEMA = "causal_observation_v10_campaign_event/1.0.0"
PHASES = (
    "PREFLIGHT",
    "ES_2025_CANARY",
    "NORMALIZATION",
    "CERTIFICATION_PASS_1",
    "CERTIFICATION_PASS_2",
    "CERTIFICATE_FINALIZATION",
    "FINAL_41_MARKET_AUDIT",
    "INACTIVE_COMPLETE",
    "RECOVERABLE_STOP",
    "TERMINAL_STOP",
)


@dataclass(frozen=True, slots=True)
class CampaignState:
    phase: str = "PREFLIGHT"
    market_index: int = 0
    certified_markets: tuple[str, ...] = ()
    recovery_count: int = 0
    resume_phase: str | None = None
    stop_class: str | None = None

    @property
    def market(self) -> str | None:
        if 0 <= self.market_index < len(MARKET_ORDER):
            return MARKET_ORDER[self.market_index]
        return None

    def as_dict(self) -> dict[str, object]:
        core = {
            "schema_version": SCHEMA_VERSION,
            "phase": self.phase,
            "market_index": self.market_index,
            "market": self.market,
            "certified_markets": list(self.certified_markets),
            "recovery_count": self.recovery_count,
            "resume_phase": self.resume_phase,
            "stop_class": self.stop_class,
        }
        return {**core, "state_id": sha256_json(core)}


def _validate(state: CampaignState) -> None:
    expected = tuple(MARKET_ORDER[: len(state.certified_markets)])
    if (
        state.phase not in PHASES
        or state.certified_markets != expected
        or state.market_index != len(state.certified_markets)
        or state.recovery_count not in {0, 1}
        or (state.phase == "RECOVERABLE_STOP" and state.resume_phase not in PHASES)
        or (state.phase != "RECOVERABLE_STOP" and state.resume_phase is not None)
        or state.market_index > len(MARKET_ORDER)
    ):
        raise IntegrityError("V10 campaign state is invalid or skipped a market")


def transition(state: CampaignState, event: str) -> CampaignState:
    """Apply one exact event; no unknown outcome can advance the campaign."""

    _validate(state)
    if state.phase in {"INACTIVE_COMPLETE", "TERMINAL_STOP"}:
        raise UnauthorizedOperation("terminal V10 campaign state cannot advance")
    if event in {
        "CORRECTNESS_FAILURE",
        "AUTHORITY_FAILURE",
        "RESOURCE_FAILURE",
        "UNEXPECTED_FAILURE",
    }:
        stopped = replace(
            state,
            phase="TERMINAL_STOP",
            resume_phase=None,
            stop_class=event,
        )
        _validate(stopped)
        return stopped
    if event == "INFRASTRUCTURE_FAILURE":
        if state.recovery_count >= 1:
            stopped = replace(
                state,
                phase="TERMINAL_STOP",
                resume_phase=None,
                stop_class="REPEATED_INFRASTRUCTURE_FAILURE",
            )
            _validate(stopped)
            return stopped
        recoverable = replace(
            state,
            phase="RECOVERABLE_STOP",
            recovery_count=1,
            resume_phase=state.phase,
            stop_class="INFRASTRUCTURE_RECOVERABLE",
        )
        _validate(recoverable)
        return recoverable
    if state.phase == "RECOVERABLE_STOP":
        if event != "RESUME":
            raise UnauthorizedOperation("recoverable V10 campaign requires exact resume")
        resumed = replace(
            state,
            phase=str(state.resume_phase),
            resume_phase=None,
            stop_class=None,
        )
        _validate(resumed)
        return resumed

    expected: dict[str, tuple[str, str]] = {
        "PREFLIGHT": ("PASS", "ES_2025_CANARY"),
        "ES_2025_CANARY": ("PASS", "NORMALIZATION"),
        "NORMALIZATION": ("PASS", "CERTIFICATION_PASS_1"),
        "CERTIFICATION_PASS_1": ("PASS", "CERTIFICATION_PASS_2"),
        "CERTIFICATION_PASS_2": ("PASS", "CERTIFICATE_FINALIZATION"),
    }
    if state.phase in expected:
        required, destination = expected[state.phase]
        if event != required:
            raise UnauthorizedOperation("V10 campaign event is invalid for its phase")
        advanced = replace(state, phase=destination, recovery_count=0)
        _validate(advanced)
        return advanced
    if state.phase == "CERTIFICATE_FINALIZATION":
        if event != "PASS" or state.market is None:
            raise UnauthorizedOperation("V10 market certificate cannot advance")
        certified = state.certified_markets + (state.market,)
        if len(certified) == len(MARKET_ORDER):
            advanced = CampaignState(
                phase="FINAL_41_MARKET_AUDIT",
                market_index=len(certified),
                certified_markets=certified,
            )
        else:
            advanced = CampaignState(
                phase="NORMALIZATION",
                market_index=len(certified),
                certified_markets=certified,
            )
        _validate(advanced)
        return advanced
    if state.phase == "FINAL_41_MARKET_AUDIT":
        if event != "PASS":
            raise UnauthorizedOperation("V10 final audit requires exact pass")
        advanced = replace(state, phase="INACTIVE_COMPLETE")
        _validate(advanced)
        return advanced
    raise UnauthorizedOperation("V10 campaign phase has no transition")


def simulate_complete_campaign(
    *, fault: tuple[str, str] | None = None
) -> CampaignState:
    """Run all provider-free state transitions, optionally injecting one fault."""

    state = CampaignState()
    state = transition(state, "PASS")
    state = transition(state, "PASS")
    phases = (
        "NORMALIZATION",
        "CERTIFICATION_PASS_1",
        "CERTIFICATION_PASS_2",
        "CERTIFICATE_FINALIZATION",
    )
    while state.phase != "FINAL_41_MARKET_AUDIT":
        market = state.market
        if fault == (str(market), state.phase):
            return transition(state, "UNEXPECTED_FAILURE")
        if state.phase not in phases:
            raise IntegrityError("V10 simulation reached an unexpected phase")
        state = transition(state, "PASS")
    return transition(state, "PASS")


def append_journal_event(
    path: Path,
    *,
    sequence: int,
    previous_event_id: str | None,
    event: str,
    state: CampaignState,
    details: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Write one immutable hash-chained campaign event."""

    if sequence < 1 or (sequence == 1) != (previous_event_id is None):
        raise IntegrityError("V10 journal sequence or predecessor is invalid")
    core = {
        "schema_version": JOURNAL_SCHEMA,
        "sequence": sequence,
        "previous_event_id": previous_event_id,
        "event": event,
        "state": state.as_dict(),
        "details": dict(details or {}),
    }
    payload = {**core, "event_id": sha256_json(core)}
    physical = io_path(path)
    physical.parent.mkdir(parents=True, exist_ok=True)
    with physical.open("xb") as handle:
        handle.write(canonical_bytes(payload) + b"\n")
        handle.flush()
    return payload


def validate_journal(paths: Sequence[Path]) -> CampaignState:
    previous: str | None = None
    final: CampaignState | None = None
    for sequence, path in enumerate(paths, start=1):
        payload = json.loads(io_path(path).read_text(encoding="utf-8"))
        core = {key: value for key, value in payload.items() if key != "event_id"}
        state_value = payload.get("state")
        if (
            payload.get("sequence") != sequence
            or payload.get("previous_event_id") != previous
            or payload.get("event_id") != sha256_json(core)
            or not isinstance(state_value, Mapping)
        ):
            raise IntegrityError("V10 campaign journal chain is invalid")
        final = CampaignState(
            phase=str(state_value["phase"]),
            market_index=int(state_value["market_index"]),
            certified_markets=tuple(state_value["certified_markets"]),
            recovery_count=int(state_value["recovery_count"]),
            resume_phase=state_value.get("resume_phase"),
            stop_class=state_value.get("stop_class"),
        )
        _validate(final)
        if state_value.get("state_id") != final.as_dict()["state_id"]:
            raise IntegrityError("V10 journal state identity differs")
        previous = str(payload["event_id"])
    if final is None:
        raise IntegrityError("V10 campaign journal is empty")
    return final
