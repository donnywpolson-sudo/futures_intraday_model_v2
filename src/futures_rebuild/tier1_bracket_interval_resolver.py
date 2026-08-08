"""Fail-closed selection of the indexed economics registry for one source interval."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .data_layout import DataReleaseReceipt
from .economics import VerifiedEconomicsRegistry
from .errors import IntegrityError


@dataclass(frozen=True)
class BracketIntervalBinding:
    phase8_index_release_id: str
    causal_release_id: str
    economics_release_id: str
    interval_key: str


def resolve_bracket_interval_binding(
    *, phase8_index_release_id: str, sidecar: Mapping[str, object], economics_by_interval: Sequence[Mapping[str, object]],
) -> BracketIntervalBinding:
    """Select exactly one index entry using the source sidecar's causal ID."""

    binding = sidecar.get("entry_binding")
    if not isinstance(binding, Mapping):
        raise IntegrityError("bracket source sidecar lacks its entry binding")
    sources = binding.get("source_bindings")
    if not isinstance(sources, list) or len(sources) != 1 or not isinstance(sources[0], Mapping):
        raise IntegrityError("bracket source sidecar must bind exactly one causal interval")
    causal = sources[0].get("causal_release_id")
    if not isinstance(causal, str) or len(causal) != 64:
        raise IntegrityError("bracket source sidecar causal release is invalid")
    matches = [entry for entry in economics_by_interval if entry.get("causal_release_id") == causal]
    if len(matches) != 1:
        raise IntegrityError("bracket source causal interval has no unique Phase 8 index entry")
    entry = matches[0]
    economics, interval = entry.get("economics_release_id"), entry.get("interval_key")
    if not all(isinstance(value, str) and value for value in (phase8_index_release_id, economics, interval)):
        raise IntegrityError("Phase 8 index entry is incomplete")
    return BracketIntervalBinding(phase8_index_release_id, causal, economics, interval)


def classify_source_disposition(value: object) -> bool:
    """Only documented tradable source rows may produce a bracket candidate."""

    if value == "ELIGIBLE":
        return True
    if value in {"MISSING", "NON_TRADABLE", "ROLL_BOUNDARY", "UNRESOLVED_FAIL_CLOSED"}:
        return False
    raise IntegrityError("bracket source disposition is unknown")


def load_verified_interval_economics(
    *, boundary: RepoBoundary, binding: BracketIntervalBinding,
) -> VerifiedEconomicsRegistry:
    """Load only the economics registry named by the selected index entry."""

    manifest = boundary.active_root / "manifests" / "data_releases" / "reference" / f"{binding.economics_release_id}.json"
    receipt = DataReleaseReceipt.from_manifest(manifest, boundary)
    if receipt.release_id != binding.economics_release_id or receipt.release_kind != "actual_contract_economics":
        raise IntegrityError("selected Phase 8 interval economics receipt is invalid")
    registry = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    return registry


def checkpoint_context(*, binding: BracketIntervalBinding, source_parquet_sha256: str, signal_contract_id: str) -> dict[str, str]:
    """Stable non-authoritative context for a resumable market-year checkpoint."""

    if not all(isinstance(value, str) and len(value) == 64 for value in (binding.phase8_index_release_id, binding.causal_release_id, binding.economics_release_id, source_parquet_sha256, signal_contract_id)):
        raise IntegrityError("bracket checkpoint provenance is invalid")
    return {
        "phase8_index_release_id": binding.phase8_index_release_id,
        "causal_release_id": binding.causal_release_id,
        "economics_release_id": binding.economics_release_id,
        "source_parquet_sha256": source_parquet_sha256,
        "signal_contract_id": signal_contract_id,
    }
