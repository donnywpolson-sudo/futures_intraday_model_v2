"""Activate catalog-bound identities for the frozen macro successor."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .active_data_view import ACTIVE_ROOT, verify_view
from .alpha_ladder_macro_successor import (
    CANDIDATES, CLOSURE_PATH, CONTRACT_PATH, load_predecessor_closure,
    load_successor_contract,
)
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


LADDER_POINTER_PATH = Path("configs/active_alpha_research_ladder_macro_successor.json")
EXECUTION_POINTER_PATH = Path("configs/active_alpha_ladder_execution_design_macro_successor.json")
RANK1_POINTER_PATH = Path("configs/active_alpha_historical_research_design.json")
REGISTRY_ROOT = Path("state/alpha_ladder_macro_successor_identity")
NINE_MARKETS = ("ES", "NQ", "CL", "GC", "ZN", "6E", "SR3", "ZB", "6J")
DOES_NOT_AUTHORIZE = (
    "FEATURE_OR_OUTCOME_CONSTRUCTION", "FINAL_252_OR_FORWARD_ACCESS",
    "MACRO_SELECTION", "MARKET_ROW_READ", "MODEL_FIT_OR_PREDICTION",
    "PROVIDER_OR_NETWORK", "TRADING", "WFA_OR_ALPHA_EVALUATION",
)


def _read(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"identity input is not canonical: {path}")
    return value


def _binding(root: Path, path: Path) -> dict[str, object]:
    value = _read(root / path)
    return {"path": path.as_posix(), "sha256": sha256_file(root / path), **{
        key: value[key] for key in ("pointer_id", "contract_id", "closure_id") if key in value
    }}


def _identity(schema: str, **fields: object) -> dict[str, object]:
    core = {"schema_version": schema, **fields}
    return {**core, "pointer_id": sha256_json(core)}


def build_identities(repository_root: Path) -> dict[str, dict[str, object]]:
    """Build the three non-authorizing identities from the exact active catalog."""

    root = repository_root.resolve(strict=True)
    closure = load_predecessor_closure(root)
    freeze = load_successor_contract(root)
    catalog = verify_view(root / ACTIVE_ROOT)
    entries = catalog.get("entries")
    markets = {str(item["market"]) for item in entries}  # type: ignore[index]
    counts = {market: 0 for market in NINE_MARKETS}
    for item in entries:  # type: ignore[union-attr]
        counts[str(item["market"])] = counts.get(str(item["market"]), 0) + 1
    if markets != set(NINE_MARKETS) or set(counts.values()) != {16} or len(entries) != 144:  # type: ignore[arg-type]
        raise IntegrityError("macro successor identities require the exact 144-row catalog")
    catalog_binding = {
        "active_view_id": catalog["active_view_id"],
        "catalog_sha256": catalog["catalog_sha256"],
        "file_sha256": sha256_file(root / ACTIVE_ROOT / "catalog.json"),
        "markets": list(NINE_MARKETS), "materialized_market_years": 136,
        "protected_pre_effective_market_years": 8,
        "path": "data/active/catalog.json",
    }
    predecessor = _binding(root, Path("configs/active_alpha_research_ladder.json"))
    common = {
        "catalog_binding": catalog_binding,
        "candidate_order": list(CANDIDATES),
        "predecessor_closure": {
            "closure_id": closure["closure_id"], "path": CLOSURE_PATH.as_posix(),
            "sha256": sha256_file(root / CLOSURE_PATH),
        },
        "selector_freeze": {
            "contract_id": freeze["contract_id"], "path": CONTRACT_PATH.as_posix(),
            "sha256": sha256_file(root / CONTRACT_PATH),
        },
    }
    ladder = _identity(
        "active_alpha_research_ladder_macro_successor/1.0.0",
        state="ACTIVE_SUCCESSOR_IDENTITY_ONLY", target_path=LADDER_POINTER_PATH.as_posix(),
        predecessor_ladder=predecessor, macro_selection_status="PENDING_ONE_USE_SELECTOR",
        applies_to="FUTURE_COUNTED_MECHANISMS_ONLY", does_not_authorize=list(DOES_NOT_AUTHORIZE),
        **common,
    )
    execution = _identity(
        "active_alpha_ladder_execution_design_macro_successor/1.0.0",
        state="ACTIVE_EXECUTION_DESIGN_IDENTITY_ONLY",
        target_path=EXECUTION_POINTER_PATH.as_posix(),
        successor_ladder={"path": LADDER_POINTER_PATH.as_posix(), "pointer_id": ladder["pointer_id"]},
        holding_horizon_minutes=30,
        cost_model={"adverse_ticks_round_trip": {market: 4 for market in CANDIDATES}, "fee_round_trip_usd": "10.00"},
        does_not_authorize=list(DOES_NOT_AUTHORIZE), **common,
    )
    prior_rank1 = _read(root / RANK1_POINTER_PATH)
    rank1_core = {key: value for key, value in prior_rank1.items() if key != "pointer_id"}
    rank1_core.update({
        "schema_version": "active_alpha_historical_research_design/2.0.0",
        "successor_ladder": {"path": LADDER_POINTER_PATH.as_posix(), "pointer_id": ladder["pointer_id"]},
        "successor_execution_design": {"path": EXECUTION_POINTER_PATH.as_posix(), "pointer_id": execution["pointer_id"]},
        "active_data_binding": catalog_binding,
        "final_252": {
            "only_independent_historical_confirmation": True,
            "status_source": {
                "path": "configs/active_alpha_research_ladder.json",
                "stage": "final_historical_evaluation",
            },
        },
    })
    rank1_core.pop("active_alpha_ladder_binding", None)
    rank1_core.pop("active_execution_design_binding", None)
    rank1 = {**rank1_core, "pointer_id": sha256_json(rank1_core)}
    return {"ladder": ladder, "execution": execution, "rank1": rank1}


def activate_identities(repository_root: Path) -> dict[str, object]:
    """Create immutable copies, then replace identity pointers with Rank 1 last."""

    root = repository_root.resolve(strict=True)
    identities = build_identities(root)
    activation_id = sha256_json(identities)
    registry = root / REGISTRY_ROOT / activation_id
    registry.mkdir(parents=True, exist_ok=False)
    paths = {
        "ladder": LADDER_POINTER_PATH, "execution": EXECUTION_POINTER_PATH,
        "rank1": RANK1_POINTER_PATH,
    }
    previous = {}
    for name, path in paths.items():
        target = root / path
        if target.exists():
            previous[name] = {"path": path.as_posix(), "sha256": sha256_file(target)}
            (registry / f"previous_{name}.json").write_bytes(target.read_bytes())
        (registry / f"{name}.json").write_bytes(canonical_bytes(identities[name]) + b"\n")
    for name in ("ladder", "execution", "rank1"):
        target = root / paths[name]
        temporary = target.with_suffix(target.suffix + ".successor-new")
        temporary.write_bytes(canonical_bytes(identities[name]) + b"\n")
        os.replace(temporary, target)
    receipt_core = {
        "activation_id": activation_id, "identities": {
            name: {"path": paths[name].as_posix(), "pointer_id": identities[name]["pointer_id"],
                   "sha256": sha256_file(root / paths[name])} for name in paths
        },
        "previous": previous,
        "schema_version": "alpha_ladder_macro_successor_identity_activation/1.0.0",
        "status": "ACTIVE_NON_AUTHORIZING_IDENTITIES",
    }
    receipt = {**receipt_core, "receipt_id": sha256_json(receipt_core)}
    (registry / "activation_receipt.json").write_bytes(canonical_bytes(receipt) + b"\n")
    return receipt
