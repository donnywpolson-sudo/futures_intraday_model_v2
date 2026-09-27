from __future__ import annotations

import json
import shutil
from pathlib import Path

from futures_rebuild.annual_dbn_causal_campaign import (
    ACTIVE_CATALOG,
    MARKET_ORDER,
    SOURCE_SUCCESSOR_ROOT,
    STATE_ROOT,
    _certificate_prefix,
    _market_summaries,
    _source,
    validate_campaign,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.causal_v10_policy_compatibility import validate_market_campaign_storage


ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = ROOT / "state/annual_dbn_gate_b_market_campaign_v1/53a0f674ed2282115d0469fb5237a004277db887db1c5cc6c4b0f0cdfd760fea/campaign_plan.json"


def load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(path)
    return value


def write_once(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8")) != value:
            raise RuntimeError(f"successor artifact differs: {path}")
        return
    path.write_bytes(canonical_bytes(value) + b"\n")


def main() -> None:
    predecessor = load(PREDECESSOR)
    source, entries = _source(ROOT)
    checkpoint_set_id = str(predecessor["checkpoint_set_id"])
    prefix = _certificate_prefix(
        ROOT,
        checkpoint_set_id,
        str(source["source_contract_id"]),
        str(source["canonical_release_id"]),
    )
    remaining = list(MARKET_ORDER[len(prefix) :])
    if not prefix or remaining[:1] != ["6N"]:
        raise RuntimeError("the expected certified prefix or resume market is absent")
    free_bytes = shutil.disk_usage(ROOT).free
    headroom = validate_market_campaign_storage(
        free_bytes=free_bytes,
        remaining_market_count=len(remaining),
    )
    core = {
        key: value
        for key, value in predecessor.items()
        if key not in {"campaign_id", "plan_id", "checkpoint_set_id", "checkpoint_set", "certified_prefix", "remaining_markets", "source", "market_summaries", "query_contract_preflight", "controller_host", "storage", "preparation_access"}
    }
    core.update(
        {
            "status": "PREPARED_PREFLIGHT_PASS_FRESH_APPROVAL_REQUIRED",
            "checkpoint_set": predecessor["checkpoint_set"],
            "checkpoint_set_id": checkpoint_set_id,
            "certified_prefix": prefix,
            "remaining_markets": remaining,
            "source": source,
            "market_summaries": _market_summaries(ROOT, entries),
            "query_contract_preflight": source["preferred_query_contract_preflight"],
            "controller_host": {
                **dict(predecessor["controller_host"]),
                "task_name": "PENDING_CAMPAIGN_ID",
            },
            "storage": {
                "observed_free_bytes": free_bytes,
                "remaining_market_count": len(remaining),
                "maximum_output_bytes_per_market": 800_000_000,
                "minimum_free_after_peak_bytes": 64 * 1024**3,
                "headroom_after_full_reservation_bytes": headroom,
            },
            "preparation_access": {
                "dbn_payload_files_opened": 0,
                "sidecar_contracts_validated": source["preferred_query_contract_preflight"]["query_contract_count"],
                "provider_calls": 0,
            },
            "stale_lock_remediation": {
                "action_at_approved_launch": "NO_BOUND_LOCKS_PRESENT",
                "locks": [],
                "source_or_output_deletions": 0,
            },
        }
    )
    implementation_bindings = dict(core["implementation_bindings"])
    implementation_bindings[
        "src/futures_rebuild/annual_dbn_causal_campaign.py"
    ] = sha256_file(ROOT / "src/futures_rebuild/annual_dbn_causal_campaign.py")
    implementation_bindings[
        "src/futures_rebuild/causal_observation_market_checkpoint.py"
    ] = sha256_file(
        ROOT / "src/futures_rebuild/causal_observation_market_checkpoint.py"
    )
    core["implementation_bindings"] = implementation_bindings
    provisional = sha256_json(core)
    core["controller_host"]["task_name"] = f"FIMV2-Annual-DBN-B-{provisional[:8]}"  # type: ignore[index]
    campaign_id = sha256_json(core)
    plan = {**core, "campaign_id": campaign_id, "plan_id": campaign_id}
    path = ROOT / STATE_ROOT / campaign_id / "campaign_plan.json"
    write_once(path, plan)
    validation = validate_campaign(ROOT, campaign_id)
    print(json.dumps({
        "campaign_id": campaign_id,
        "checkpoint_set_id": checkpoint_set_id,
        "certified_markets": len(prefix),
        "next_market": remaining[0],
        "preferred_dbn_file_count": source["preferred_dbn_file_count"],
        "preferred_sidecar_file_count": source["preferred_sidecar_file_count"],
        "source_successor_root": SOURCE_SUCCESSOR_ROOT.as_posix(),
        "validation": validation,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
