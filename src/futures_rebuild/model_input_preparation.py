"""Inactive, audit-only preparation over existing certified minute releases.

No default reader, calendar exception, permission, or accepted byte is changed.
The strict path never traverses a gap, including a calendar-qualified closure.
"""
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from . import active_data_view
from .annual_foundation_release import verify_minute
from .canonical import sha256_file
from .causal_observation_parquet import _decode_group, _decode_physical
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.statistics_v2 import observation_identity
from .hourly_quality_reader import CUTOFF, IDENTITY

MINUTE = 60_000_000_000
DELAY = 5_000_000_000
POLICY = "model_input_preparation/1"
EXCLUSIONS = (("6N", 1440421200000000000, 1440424800000000000),
              ("6S", 1593518400000000000, 1593525600000000000))
IDENTITY_FIELDS = (*IDENTITY, "listing_activation_ns", "expiration_ns")


@dataclass(frozen=True)
class WindowRequest:
    start_ns: int
    end_ns: int
    decision_at_ns: int
    kind: str = "feature"
    evaluated_at_ns: int | None = None
    signal_identity: tuple | None = None
    signal_available_at_ns: int | None = None

    def __post_init__(self):
        if (any(type(t) is not int for t in (self.start_ns, self.end_ns, self.decision_at_ns))
                or not 0 < self.start_ns < self.end_ns <= CUTOFF
                or self.start_ns % MINUTE or self.end_ns % MINUTE
                or self.end_ns - self.start_ns > 100_000 * MINUTE
                or not 0 <= self.decision_at_ns <= CUTOFF or self.kind not in ("feature", "target")):
            raise ContractError("Require a bounded aligned development-minute window and explicit decision")
        if self.kind == "target" and (
                type(self.evaluated_at_ns) is not int or not self.decision_at_ns <= self.evaluated_at_ns <= CUTOFF
                or type(self.signal_available_at_ns) is not int
                or not 0 <= self.signal_available_at_ns <= self.decision_at_ns
                or type(self.signal_identity) is not tuple or len(self.signal_identity) != len(IDENTITY_FIELDS)):
            raise ContractError("Target requires signal identity/availability and evaluation time")


def structural_masks(observations, quality):
    """Shared vector audit/window checks. Quality remains owned by the source."""
    c = {k: observations[k].to_numpy(zero_copy_only=False) for k in observations.column_names}
    q = {k: quality[k].to_numpy(zero_copy_only=False) for k in quality.column_names}
    n = len(observations)
    if len(quality) != n:
        raise IntegrityError("Quality and observation rows differ")
    bad_quality = ~(q["ohlc_valid"] & q["volume_valid"] & q["timestamp_order_valid"])
    bad_quality |= q["duplicate_state"] != "UNIQUE"
    disputed = np.zeros(n, dtype=bool)
    for market, start, end in EXCLUSIONS:
        disputed |= (c["market"] == market) & (c["bar_start_ns"] < end) & (c["bar_end_ns"] > start)
    bad_timing = ((c["bar_start_ns"] % MINUTE != 0)
                  | (c["bar_end_ns"] != c["bar_start_ns"] + MINUTE)
                  | (c["source_timestamp_ns"] != c["bar_start_ns"])
                  | (c["source_cadence"] != "1m")
                  | (c["available_at_ns"] < c["bar_end_ns"] + DELAY)
                  | (c["decision_eligible_at_ns"] < c["available_at_ns"])
                  | (c["definition_received_at_ns"] > c["bar_start_ns"])
                  | (c["bar_end_ns"] > CUTOFF))
    bad_identity = ((c["min_price_increment_nano"] <= 0) | (c["multiplier_nano"] <= 0)
                    | (c["publisher_id"] <= 0) | (c["instrument_id"] <= 0)
                    | (c["actual_contract"] == "") | (c["currency"] == "")
                    | (c["listing_activation_ns"] > c["bar_start_ns"])
                    | (c["expiration_ns"] <= c["bar_start_ns"]))
    masks = dict(QUALITY_REFUSAL=bad_quality, DISPUTED_INTERVAL=disputed,
                 TIMING_REFUSAL=bad_timing, IDENTITY_UNRESOLVED=bad_identity)
    # Nulls cannot acquire eligibility through NumPy comparisons.
    if any(observations[k].null_count for k in c) or any(quality[k].null_count for k in q):
        raise IntegrityError("Null structural input")
    return masks


def window_result(rows, quality_ok, request, *, market):
    """Pure check used after verified reads; unchanged rows remain in the result."""
    reasons = []
    expected = list(range(request.start_ns, request.end_ns, MINUTE))
    starts = [r["bar_start_ns"] for r in rows]
    if starts != expected:
        reasons.append("GAP_DUPLICATE_OR_OUT_OF_ORDER")
    if not quality_ok:
        reasons.append("STRUCTURAL_REFUSAL")
    if any(m == market and request.start_ns < end and request.end_ns > start for m, start, end in EXCLUSIONS):
        reasons.append("DISPUTED_INTERVAL")
    identities = {observation_identity(r) for r in rows}
    if len(identities) != 1 or any(r["market"] != market for r in rows):
        reasons.append("IDENTITY_CHANGE_OR_UNRESOLVED")
    as_of = request.decision_at_ns if request.kind == "feature" else request.evaluated_at_ns
    if any(r["decision_eligible_at_ns"] > as_of for r in rows):
        reasons.append("PREMATURE_INPUT")
    if request.kind == "target":
        if identities != {request.signal_identity}:
            reasons.append("TARGET_CROSSES_IDENTITY")
        if request.start_ns <= max(request.decision_at_ns, request.signal_available_at_ns):
            reasons.append("ENTRY_NOT_LATER_THAN_SIGNAL")
    return dict(policy=POLICY, request=request, observations=rows, reasons=tuple(reasons),
                status=("ELIGIBLE" if not reasons else "REFUSED") if request.kind == "feature"
                else ("COMPLETE" if not reasons else "INDETERMINATE"),
                availability_assumption="BAR_END_PLUS_5_SECONDS_NOT_MEASURED_LATENCY",
                research_authority=False, statistics=None)


def verified_batches(root, entry):
    """Existing annual permission/hash verification, bounded physical row groups."""
    if "LINEAGE_AUDIT" not in entry["permitted_uses"] or not entry.get("parquet_path"):
        raise UnauthorizedOperation("Annual release is unavailable for lineage audit")
    verify_minute(root, entry)
    import json
    manifest = json.loads((root / entry["sidecar_path"]).read_bytes())
    yield from _physical_batches(root, entry, manifest)


def _physical_batches(root, entry, manifest):
    """Physical checks shared by the reader and the already-bound population audit."""
    from .causal_timeframe_storage import contained
    path = contained(root, entry["parquet_path"])
    if sha256_file(path) != entry["parquet_sha256"]:
        raise IntegrityError("Annual observation bytes changed")
    files = manifest["packing"]["files"]
    quality_path = path.parent / files["quality"]["filename"]
    if sha256_file(quality_path) != files["quality"]["sha256"]:
        raise IntegrityError("Annual quality bytes changed")
    observations, quality = pq.ParquetFile(path), pq.ParquetFile(quality_path)
    for group in manifest["packing"]["groups"]:
        obs_group, q_group = group["tables"]["observations"], group["tables"]["quality"]
        offset = 0
        for obs, q in zip(observations.iter_batches(batch_size=50_000, row_groups=[obs_group["group"]]),
                          quality.iter_batches(batch_size=50_000, row_groups=[q_group["group"]]), strict=True):
            if q["observation_ordinal"].to_pylist() != list(range(offset, offset + len(obs))):
                raise IntegrityError("Quality local ordinals differ from physical observations")
            offset += len(obs)
            yield obs, q, structural_masks(obs, q)
        if offset != obs_group["rows"] or offset != q_group["rows"]:
            raise IntegrityError("Annual group counts differ")
    if sha256_file(path) != entry["parquet_sha256"] or sha256_file(quality_path) != files["quality"]["sha256"]:
        raise IntegrityError("Annual input changed during preparation")


def prepare_inputs(repository_root, *, market, year, requests, statistics=None,
                   max_age_ns=None, settlement_flags=None):
    """One opt-in interface; returns audit results without changing feature permissions.

    Batches are confined to one annual release. Windows spanning annual files
    refuse here; callers may request a later complete lookback. Targets must
    include every required outcome observation, including an exit reference.
    Optional statistics is an AsOfStatisticsV2 built from verified source data.
    """
    root = Path(repository_root).resolve(strict=True)
    if statistics is not None and year not in range(2018, 2023):
        raise UnauthorizedOperation("Optional-statistics candidate is qualified only for 2018-2022")
    requests = tuple(requests)
    if not 1 <= len(requests) <= 1_000 or any(type(r) is not WindowRequest for r in requests):
        raise ContractError("Require 1..1000 explicit window requests")
    if sum((r.end_ns-r.start_ns)//MINUTE for r in requests) > 100_000:
        raise ContractError("Requested output exceeds the bounded observation budget")
    # Preserve all existing catalog/cohort/status rules. This interface has no status override.
    active_data_view.resolve(repository_root=root, market=market, year=year, purpose="LINEAGE_AUDIT")
    catalog = active_data_view._load_view_catalog(root / active_data_view.ACTIVE_ROOT,
                                                 allow_unreferenced_append_files=True)
    entry = next(e for e in catalog["entries"] if (e["market"], e["year"]) == (market, year))
    retained, good, segments = [[] for _ in requests], [True]*len(requests), {}
    previous, segment_start = None, None
    for obs, q, masks in verified_batches(root, entry):
        bad = np.logical_or.reduce(list(masks.values()))
        starts = obs["bar_start_ns"].to_numpy(zero_copy_only=False)
        columns = {k:obs[k].to_numpy(zero_copy_only=False) for k in IDENTITY_FIELDS}
        changes = np.zeros(len(obs), dtype=bool)
        changes[0] = previous is None or any(columns[k][0] != previous[k] for k in IDENTITY_FIELDS)
        for key in IDENTITY_FIELDS:
            changes[1:] |= columns[key][1:] != columns[key][:-1]
        segment_starts = np.maximum.accumulate(np.where(changes, starts, segment_start or 0))
        for i, request in enumerate(requests):
            selected = np.flatnonzero((starts >= request.start_ns) & (starts < request.end_ns))
            if len(selected):
                retained[i].extend(_decode_group("observations", [
                    _decode_physical("observations", row) for row in obs.take(selected).to_pylist()], ()))
                good[i] &= not bool(bad[selected].any())
                segments[i] = int(segment_starts[selected[-1]])
        previous = {k:columns[k][-1] for k in IDENTITY_FIELDS}
        segment_start = int(segment_starts[-1])
    results = [window_result(rows, ok, request, market=market)
               for rows, ok, request in zip(retained, good, requests, strict=True)]
    indices = [i for i, r in enumerate(results) if r["status"] == "ELIGIBLE"]
    if statistics is not None:
        projections = statistics.batch_asof([dict(observation=retained[i][-1],
            decision_at_ns=requests[i].decision_at_ns, segment_start_ns=segments[i],
            max_age_ns=max_age_ns, settlement_flags=settlement_flags) for i in indices])
        for i, projection in zip(indices, projections, strict=True):
            results[i]["statistics"] = projection
    for result in results:
        result["cohort"] = entry["cohort"]
        result["permitted_uses"] = tuple(entry["permitted_uses"])
        result["selection_eligible"] = entry["selection_eligible"]
    return results
