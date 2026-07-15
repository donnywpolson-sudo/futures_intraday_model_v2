"""Actual-contract/session eligibility before any synthetic inference mechanic."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re

from .contracts import ResearchContractError, explicit_int
from .economics import Direction, EconomicsBinding, _market_id


_SHA = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class FuturesInferenceUnit:
    market_id: str
    direction: Direction
    expected_session_ordinal: int
    observed_session_ordinal: int | None
    actual_contract_id: str | None
    economics_record_id: str | None
    session_complete: bool
    is_roll_session: bool
    sessions_to_expiry: int | None


@dataclass(frozen=True)
class InferenceEligibility:
    inference_unit_id: str
    eligible: bool
    abstention_reasons: tuple[str, ...]


def assess_inference_unit(
    unit: FuturesInferenceUnit,
    *,
    economics: EconomicsBinding | None,
    minimum_sessions_to_expiry: int,
) -> InferenceEligibility:
    market = _market_id(unit.market_id)
    if not isinstance(unit.direction, Direction):
        raise ResearchContractError("direction must be LONG or SHORT")
    expected = explicit_int(
        unit.expected_session_ordinal, name="expected_session_ordinal"
    )
    minimum_expiry = explicit_int(
        minimum_sessions_to_expiry, name="minimum_sessions_to_expiry"
    )
    if minimum_expiry < 0:
        raise ResearchContractError("minimum_sessions_to_expiry cannot be negative")
    for name in ("session_complete", "is_roll_session"):
        if type(getattr(unit, name)) is not bool:
            raise ResearchContractError(f"{name} must be an exact bool")
    reasons: list[str] = []
    observed: int | None
    if unit.observed_session_ordinal is None:
        observed = None
        reasons.append("MISSING_OR_INCOMPLETE_SESSION")
    else:
        observed = explicit_int(
            unit.observed_session_ordinal, name="observed_session_ordinal"
        )
        if observed != expected or not unit.session_complete:
            reasons.append("MISSING_OR_INCOMPLETE_SESSION")
    if unit.observed_session_ordinal is not None and not unit.session_complete:
        reasons.append("MISSING_OR_INCOMPLETE_SESSION")
    if not isinstance(unit.actual_contract_id, str) or _SHA.fullmatch(
        unit.actual_contract_id
    ) is None:
        reasons.append("UNVERIFIED_ACTUAL_CONTRACT")
    if not isinstance(unit.economics_record_id, str) or _SHA.fullmatch(
        unit.economics_record_id
    ) is None:
        reasons.append("UNVERIFIED_ECONOMICS")
    if economics is None:
        reasons.append("UNVERIFIED_ECONOMICS")
    else:
        try:
            economics.validate()
        except ResearchContractError:
            reasons.append("UNVERIFIED_ECONOMICS")
        else:
            if (
                economics.actual_contract_id != unit.actual_contract_id
                or economics.economics_record_id != unit.economics_record_id
            ):
                reasons.append("ECONOMICS_ACTUAL_CONTRACT_MISMATCH")
    if unit.is_roll_session:
        reasons.append("ROLL_TRANSITION")
    if unit.sessions_to_expiry is None:
        expiry: int | None = None
        reasons.append("EXPIRY_GUARD")
    else:
        expiry = explicit_int(unit.sessions_to_expiry, name="sessions_to_expiry")
        if expiry <= minimum_expiry:
            reasons.append("EXPIRY_GUARD")
    core = {
        "actual_contract_id": unit.actual_contract_id,
        "direction": unit.direction.value,
        "economics_record_id": unit.economics_record_id,
        "expected_session_ordinal": expected,
        "market_id": market,
        "observed_session_ordinal": observed,
        "sessions_to_expiry": expiry,
    }
    unit_id = hashlib.sha256(
        json.dumps(core, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    unique_reasons = tuple(sorted(set(reasons)))
    return InferenceEligibility(unit_id, not unique_reasons, unique_reasons)
