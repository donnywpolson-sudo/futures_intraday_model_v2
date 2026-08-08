"""Prepare-only execution boundary for the Tier 1 bracket artifact pipeline.

The public surface binds metadata and returns a plain-language summary.  It
never opens source Parquet rows or writes labels, features, splits, or frozen
predictions; those actions remain an approved Codex orchestration task.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .canonical import sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .tier1_bracket_pipeline import build_tier1_bracket_signal_contract
from .tier1_bracket_trial import load_registered_tier1_bracket_trial


MARKETS = ("ES", "CL", "ZN", "6E")
YEARS = tuple(range(2018, 2023))
MODEL_CONTRACT_ROOT = Path("state/trial_registry/tier1_bracket_model_contract")


@dataclass(frozen=True)
class BracketMaterializationPreparation:
    """Exact no-row-open binding for the future bracket artifact run."""

    trial_id: str
    model_contract_id: str
    signal_contract_id: str
    source_pairs: tuple[tuple[str, int], ...]
    confirmation_required: Mapping[str, object]


def _load_model_contract(*, root: Path, trial_id: str) -> tuple[str, Mapping[str, object]]:
    paths = tuple(sorted((root / MODEL_CONTRACT_ROOT).glob("*.json")))
    matching = []
    for path in paths:
        try:
            candidate = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("bracket directional model contract is unreadable") from exc
        if isinstance(candidate, dict) and candidate.get("parent_trial_id") == trial_id:
            matching.append(candidate)
    if len(matching) != 1:
        raise IntegrityError("bracket execution requires exactly one model contract for its current trial")
    payload = matching[0]
    if not isinstance(payload, dict):
        raise IntegrityError("bracket directional model contract is invalid")
    contract_id = payload.get("model_contract_id")
    core = {key: value for key, value in payload.items() if key not in {"model_contract_id", "locked_at_utc"}}
    if (
        not isinstance(contract_id, str)
        or sha256_json(core) != contract_id
        or payload.get("parent_trial_id") != trial_id
        or payload.get("state") != "LOCKED_BEFORE_BRACKET_SOURCE_ROW_OPEN"
    ):
        raise IntegrityError("bracket directional model contract is inconsistent")
    return contract_id, payload


def prepare_tier1_bracket_materialization(*, root: Path) -> BracketMaterializationPreparation:
    """Bind scope and describe execution without opening a source row."""

    registered = load_registered_tier1_bracket_trial(root=root)
    if registered is None:
        raise IntegrityError("bracket materialization requires the registered bracket trial")
    trial_id = registered["trial_id"]
    if not isinstance(trial_id, str):
        raise IntegrityError("registered bracket trial ID is invalid")
    model_id, _ = _load_model_contract(root=root, trial_id=trial_id)
    signal = build_tier1_bracket_signal_contract(parent_trial_id=trial_id, model_contract_id=model_id)
    pairs = tuple((market, year) for market in MARKETS for year in YEARS)
    return BracketMaterializationPreparation(
        trial_id=trial_id,
        model_contract_id=model_id,
        signal_contract_id=signal.trial_id,
        source_pairs=pairs,
        confirmation_required={
            "operation": "create fresh Tier 1 bracket labels, features, chronological splits, and frozen predictions",
            "scope": "20 pinned local ES/CL/ZN/6E market-years for 2018-2022 only; 2025 excluded",
            "outputs": "one immutable signal contract plus fresh bracket-only research artifacts",
            "limits": "no provider/network, evaluation, trading, Git action, installation, or holdout access",
            "preservation": "existing Phase 3-8 releases and the five-minute pipeline remain unchanged",
        },
    )


def execute_tier1_bracket_materialization(*, preparation: BracketMaterializationPreparation) -> None:
    """Fail closed outside approved Codex orchestration."""

    del preparation
    raise UnauthorizedOperation(
        "Codex confirmation required before bracket source-row reads or immutable artifact publication"
    )
