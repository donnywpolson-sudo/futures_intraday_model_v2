"""True-zero flat-baseline coverage independence for V12."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from .canonical import sha256_file
from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v11 as v11


V11_TRIAL_ID = "4583409eb5443c89306f118b912bea1fabd6f20f437e30294395ac7786c3719b"
V12_CONTRACT = Path("configs/tier1_bracket_successor_v12.json")


def load_v12_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    try:
        delta = json.loads((root / V12_CONTRACT).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("invalid V12 contract JSON") from exc
    rule = delta.get("flat_baseline_coverage_successor")
    authority = delta.get("authority")
    if (
        not isinstance(delta, dict)
        or delta.get("schema_version") != "tier1_bracket_successor_v12_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v11_trial_id") != V11_TRIAL_ID
        or delta.get("inherited_v11_contract_path")
        != "configs/tier1_bracket_successor_v11.json"
        or sha256_file(root / "configs/tier1_bracket_successor_v11.json")
        != delta.get("inherited_v11_contract_sha256")
        or not isinstance(rule, dict)
        or rule.get("eligible")
        != "EVERY_CALENDAR_OPEN_CHECKPOINT_REGARDLESS_OF_FEATURE_MODEL_SIGNAL_RISK_OR_OUTCOME_AVAILABILITY"
        or rule.get("required_coverage_rate") != "1.0"
        or rule.get("feature_or_model_dependency") is not False
        or rule.get("outcome_dependency") is not False
        or not isinstance(authority, dict)
        or authority.get("publication_requires_separate_approval") is not True
        or authority.get("holdout_or_forward_access") is not False
    ):
        raise IntegrityError("V12 flat-baseline contract is incomplete or drifted")
    inherited, _ = v11.load_v11_contract(root=root)
    return inherited, delta


def evaluate_required_baseline_coverage_v12(
    *, rows: Sequence[v5.MaterializedRowV5], folds: Sequence[object],
    universes: v11.StrategyPredictionUniversesV11,
) -> dict[str, object]:
    """Make flat coverage exactly complete without weakening any nonflat gate."""

    inherited = v11.evaluate_required_baseline_coverage_v11(
        rows=rows, folds=folds, universes=universes,
    )
    strategies = inherited.get("strategies")
    if not isinstance(strategies, dict):
        return {"status": "INVALID", "passed": False}
    owner_sessions = {
        str(session) for fold in folds for session in getattr(fold, "test_sessions")
    }
    expected_rows = [
        row for row in rows
        if row.expected.exchange_session_date in owner_sessions
        and row.ledger.terminal_disposition != "CALENDAR_CLOSED"
    ]
    flat = universes.predictions.get("flat_no_trade")
    if flat is None:
        return {"status": "INVALID", "passed": False}
    flat_ids = [item.opportunity_id for item in flat]
    expected_ids = {row.expected.opportunity_id for row in expected_rows}
    if (
        not expected_rows or len(flat_ids) != len(set(flat_ids))
        or not expected_ids <= set(flat_ids)
    ):
        return {"status": "INVALID", "passed": False}
    expected_by: dict[str, int] = {}
    for row in expected_rows:
        key = f"{row.expected.market}/{row.expected.year}"
        expected_by[key] = expected_by.get(key, 0) + 1
    rates = {key: 1.0 for key in sorted(expected_by)}
    corrected = dict(strategies)
    corrected["flat_no_trade"] = {
        "status": "PASS", "expected": len(expected_ids),
        "eligible": len(expected_ids), "overall_rate": 1.0,
        "market_year_rates": rates,
        "feature_or_model_dependency": False,
        "outcome_dependency": False,
    }
    passed = all(
        isinstance(value, dict) and value.get("status") == "PASS"
        for value in corrected.values()
    )
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_COVERAGE",
        "passed": passed, "strategies": dict(sorted(corrected.items())),
    }
