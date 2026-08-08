from __future__ import annotations

from datetime import date, timedelta

import pytest

from futures_rebuild.alpha_ladder_es_training_diagnostic import summarize_windows
from futures_rebuild.alpha_ladder_limit_readiness import SessionReadiness
from futures_rebuild.errors import IntegrityError


def _sessions() -> tuple[str, ...]:
    return tuple((date(2018, 1, 1) + timedelta(days=index)).isoformat()
                 for index in range(568))


def _complete() -> SessionReadiness:
    return SessionReadiness(True, True, True, ("VERIFIED_LIMIT_EXIT",),
                            {"base": "FEASIBLE", "stress": "FEASIBLE",
                             "extreme": "RISK_ABSTENTION"})


def test_exact_window_shortfall_and_one_exclusion_per_session() -> None:
    sessions = _sessions(); results = {session: _complete() for session in sessions}
    results[sessions[10]] = SessionReadiness(
        True, True, False,
        ("LONG__base__HOLD_IDENTITY_CHANGING", "SHORT__base__HOLD_IDENTITY_CHANGING",
         "LONG__stress__HOLD_IDENTITY_CHANGING", "SHORT__stress__HOLD_IDENTITY_CHANGING"),
        {"base": "FEASIBLE", "stress": "FEASIBLE", "extreme": "RISK_ABSTENTION"},
    )
    summary = summarize_windows(sessions=sessions, results=results)
    first = summary["windows"][0]
    assert first["complete_training_sessions"] == 503
    assert first["training_shortfall_sessions"] == 1
    assert sum(first["training_exclusion_reasons"].values()) == 1
    assert summary["candidate_window_count"] == 1


def test_feature_failure_is_exact_and_price_free() -> None:
    sessions = _sessions(); results = {session: _complete() for session in sessions}
    results[sessions[0]] = SessionReadiness(
        False, False, True, ("EXPLICIT_CAUSAL_FEATURE_ABSTENTION",), {})
    summary = summarize_windows(sessions=sessions, results=results)
    first = summary["windows"][0]
    assert first["feature_complete_training_sessions"] == 503
    assert first["training_exclusion_reasons"] == {
        "EXPLICIT_CAUSAL_FEATURE_ABSTENTION": 1}
    assert not any("price" in key.lower() for key in first)


def test_missing_session_accounting_fails_closed() -> None:
    sessions = _sessions(); results = {session: _complete() for session in sessions[:-1]}
    with pytest.raises(IntegrityError, match="accounting"):
        summarize_windows(sessions=sessions, results=results)
