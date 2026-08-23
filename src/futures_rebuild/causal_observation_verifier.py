"""Independent verifier for staged causal-observation candidates.

The verifier deliberately does not import the producer implementation or
accept a producer success flag.  It recomputes file, row, timing, lineage, and
evidence invariants directly from candidate bytes and their manifest.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Mapping

from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest
from .errors import IntegrityError
from .foundation.economics import EconomicsRuleBook
from .foundation.records import NANO
from .causal_observation_foundation import ECONOMICS_RULEBOOK_ID, ECONOMICS_RULEBOOK_SHA256
from .causal_observation_parquet import (
    FILENAMES as PARQUET_FILENAMES,
    FORMAT_VERSION as PARQUET_FORMAT_VERSION,
    read_bundle,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MISSINGNESS = {
    "OBSERVED_VALID", "NO_TRADE_EXPECTED", "MARKET_CLOSED", "HALTED_OR_PAUSED",
    "NOT_YET_LISTED", "ROLL_EXCLUDED", "SOURCE_UNAVAILABLE", "UNEXPECTED_GAP",
    "CORRUPT_OR_CONFLICTING", "UNKNOWN_FAIL_CLOSED",
}
_FORBIDDEN = {
    "outcome", "target", "label", "feature", "fold", "prediction", "evaluation",
    "pnl", "return", "model", "promotion", "mechanism",
}
_JSONL_FILENAMES = {
    "observations.jsonl", "missingness.jsonl", "roll.jsonl", "quality.jsonl",
    "cadence.jsonl",
}
_PARQUET_FILENAMES = frozenset(PARQUET_FILENAMES.values())


def _read_canonical_lines(path: Path) -> list[dict[str, object]]:
    raw = path.read_bytes()
    rows: list[dict[str, object]] = []
    for line in raw.splitlines(keepends=True):
        if not line.endswith(b"\n"):
            raise IntegrityError("candidate evidence line lacks canonical termination")
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("candidate evidence line is invalid JSON") from exc
        if not isinstance(value, dict) or line != canonical_bytes(value) + b"\n":
            raise IntegrityError("candidate evidence line is not canonical")
        rows.append(value)
    return rows


def _index_by_filename(
    stage: Path, manifest: DataReleaseManifest, expected: frozenset[str] | set[str]
) -> dict[str, tuple[Path, object]]:
    result: dict[str, tuple[Path, object]] = {}
    for entry in manifest.files:
        filename = Path(entry.logical_path).name
        if filename in result or filename not in expected:
            raise IntegrityError("candidate manifest file set is unexpected or duplicate")
        candidates = [
            path for path in stage.rglob(filename)
            if path.is_file() and path.stat().st_size == entry.size and sha256_file(path) == entry.sha256
        ]
        if len(candidates) != 1:
            raise IntegrityError("candidate file cannot be independently matched to its manifest")
        result[filename] = (candidates[0], entry)
    if set(result) != set(expected):
        raise IntegrityError("candidate manifest omits a required evidence file")
    observed = {path.resolve() for path in stage.rglob("*") if path.is_file()}
    matched = {value[0].resolve() for value in result.values()}
    if observed != matched:
        raise IntegrityError("candidate stage contains an unmanifested file")
    return result


def verify_observation_candidate(
    *,
    stage: Path,
    manifest: DataReleaseManifest,
    economics_rulebook: EconomicsRuleBook,
) -> dict[str, object]:
    parquet_format = manifest.schema_version == "causal_observation_partition/1.1.0"
    if (
        manifest.phase != "causally_gated_normalized"
        or manifest.release_kind != "development_only_causal_observation_partition"
        or manifest.schema_version
        not in {
            "causal_observation_partition/1.0.0",
            "causal_observation_partition/1.1.0",
        }
        or manifest.release_id != sha256_json(manifest.core_dict())
    ):
        raise IntegrityError("candidate manifest identity or capability is invalid")
    metadata = manifest.metadata
    if (
        metadata.get("schema_version")
        != (
            "causal_observation_evidence/1.1.0"
            if parquet_format
            else "causal_observation_evidence/1.0.0"
        )
        or (
            parquet_format
            and (
                metadata.get("storage_format") != PARQUET_FORMAT_VERSION
                or metadata.get("compression") != "zstd-9"
                or metadata.get("deterministic_identity_columns_reconstructed") is not True
            )
        )
        or metadata.get("publication_authorized") is not False
        or metadata.get("activation_authorized") is not False
        or metadata.get("economics_rulebook_sha256") != ECONOMICS_RULEBOOK_SHA256
        or metadata.get("economics_rulebook_id") != ECONOMICS_RULEBOOK_ID
        or economics_rulebook.rulebook_hash != ECONOMICS_RULEBOOK_ID
        or any(metadata.get(name) != 0 for name in ("outcome_count", "feature_count", "prediction_count", "evaluation_count"))
        or any(_SHA256.fullmatch(str(metadata.get(name, ""))) is None for name in (
            "causal_contract_id", "source_contract_id", "source_release_id", "plan_id",
            "plan_sha256", "exact_source_entries_sha256",
        ))
    ):
        raise IntegrityError("candidate manifest metadata grants or omits capability")

    if parquet_format:
        files = _index_by_filename(stage, manifest, _PARQUET_FILENAMES)
        tables = read_bundle(files[PARQUET_FILENAMES["observations"]][0].parent)
        observations = tables["observations"]
        missingness = tables["missingness"]
        rolls = tables["roll"]
        quality = tables["quality"]
        cadence = tables["cadence"]
    else:
        files = _index_by_filename(stage, manifest, _JSONL_FILENAMES)
        observations = _read_canonical_lines(files["observations.jsonl"][0])
        missingness = _read_canonical_lines(files["missingness.jsonl"][0])
        rolls = _read_canonical_lines(files["roll.jsonl"][0])
        quality = _read_canonical_lines(files["quality.jsonl"][0])
        cadence = _read_canonical_lines(files["cadence.jsonl"][0])
    if not observations:
        raise IntegrityError("candidate has no observations")
    row_ids: list[str] = []
    order: list[tuple[str, int, str]] = []
    observation_by_id: dict[str, dict[str, object]] = {}
    for row in observations:
        if _FORBIDDEN & set(row):
            raise IntegrityError("candidate contains a non-observation capability field")
        try:
            row_id = str(row["row_id"])
            source_contract = str(row["source_contract_id"])
            source_release = str(row["source_release_id"])
            source_file = str(row["source_file_sha256"])
            source_row = str(row["source_row_sha256"])
            definition_file = str(row["definition_source_file_sha256"])
            definition_row = str(row["definition_row_sha256"])
            market = str(row["market"])
            start, end = int(row["bar_start_ns"]), int(row["bar_end_ns"])
            source_at = int(row["source_timestamp_ns"])
            available, eligible = int(row["available_at_ns"]), int(row["decision_eligible_at_ns"])
            definition_event = int(row["definition_event_at_ns"])
            definition_received = int(row["definition_received_at_ns"])
            activation, expiration = int(row["listing_activation_ns"]), int(row["expiration_ns"])
            opening, high = int(row["open_nano"]), int(row["high_nano"])
            low, closing, volume = int(row["low_nano"]), int(row["close_nano"]), int(row["volume"])
            expected_multiplier = economics_rulebook.rules[market].expected_unit_qty * NANO
        except (KeyError, TypeError, ValueError) as exc:
            raise IntegrityError("candidate observation fields are malformed") from exc
        if (
            any(_SHA256.fullmatch(value) is None for value in (row_id, source_contract, source_release, source_file, source_row, definition_file, definition_row))
            or source_contract != metadata["source_contract_id"]
            or source_release != metadata["source_release_id"]
            or not 0 < start < end <= available <= eligible
            or not start <= source_at <= end
            or definition_event > available
            or definition_received > available
            or (activation not in {0, 2**64 - 1} and activation > source_at)
            or (expiration not in {0, 2**64 - 1} and expiration <= source_at)
            or volume < 0
            or high < max(opening, closing)
            or low > min(opening, closing)
            or high < low
            or int(row["min_price_increment_nano"]) <= 0
            or int(row["multiplier_nano"]) <= 0
            or expected_multiplier != expected_multiplier.to_integral_value()
            or int(row["multiplier_nano"]) != int(expected_multiplier)
            or not int(row["project_grouping_start_ns"]) <= start < end <= int(row["project_grouping_end_ns"])
            or row.get("project_timezone") != "America/Chicago"
            or row.get("official_schedule_state")
            not in {"AUTHORITATIVE_APPLICABLE", "AUTHORITATIVE_CLOSED", "UNKNOWN_FAIL_CLOSED"}
        ):
            raise IntegrityError("candidate observation invariant failed")
        row_ids.append(row_id)
        observation_by_id[row_id] = row
        order.append((market, start, row_id))
    if len(set(row_ids)) != len(row_ids) or order != sorted(order):
        raise IntegrityError("candidate row identities are duplicate or unordered")
    required_ids = set(row_ids)

    def exact_coverage(rows: list[dict[str, object]], name: str) -> None:
        ids = [str(row.get("row_id")) for row in rows]
        if len(ids) != len(required_ids) or set(ids) != required_ids:
            raise IntegrityError(f"candidate {name} ledger coverage differs")

    exact_coverage(rolls, "roll")
    exact_coverage(quality, "quality")
    missing_observation_ids = [
        str(row.get("observation_row_id"))
        for row in missingness
        if row.get("observation_row_id") is not None
    ]
    missing_evidence_ids = [str(row.get("evidence_id")) for row in missingness]
    if (
        set(missing_observation_ids) != required_ids
        or len(missing_observation_ids) != len(required_ids)
        or len(missing_evidence_ids) != len(set(missing_evidence_ids))
    ):
        raise IntegrityError("candidate missingness ledger coverage differs")
    for row in missingness:
        if (
            row.get("state") not in _MISSINGNESS
            or _SHA256.fullmatch(str(row.get("evidence_id", ""))) is None
            or _SHA256.fullmatch(str(row.get("evidence_sha256", ""))) is None
            or not int(row.get("interval_start_ns", 0)) < int(row.get("interval_end_ns", 0))
            or (row.get("state") == "OBSERVED_VALID")
            != (row.get("observation_row_id") is not None)
        ):
            raise IntegrityError("candidate missingness state is invalid")
        if row.get("state") in {"MARKET_CLOSED", "NO_TRADE_EXPECTED"} and row.get("authority") in {"NONE", "UNKNOWN", "OBSERVED_ABSENCE"}:
            raise IntegrityError("candidate infers closure from missing observation")
    for row in rolls:
        if (
            _SHA256.fullmatch(str(row.get("causal_selection_evidence_sha256", ""))) is None
            or type(row.get("roll_flag")) is not bool
            or type(row.get("price_discontinuity_flag")) is not bool
            or (
                row.get("roll_flag") is False
                and (row.get("actual_contract_before") != row.get("actual_contract_after") or row.get("price_discontinuity_flag") is True)
            )
        ):
            raise IntegrityError("candidate roll evidence is invalid")
    for row in quality:
        flags = row.get("quality_flags")
        multiplier_flags = (
            set(flags) & {
                "MULTIPLIER_PROVIDER_DEFINITION_CROSSCHECK_MATCH",
                "MULTIPLIER_RULEBOOK_VALUE_PROVIDER_UNIT_QTY_UNAVAILABLE",
            }
            if isinstance(flags, list)
            else set()
        )
        if (
            row.get("source_contract_id") != metadata["source_contract_id"]
            or row.get("source_release_id") != metadata["source_release_id"]
            or any(type(row.get(name)) is not bool for name in ("ohlc_valid", "volume_valid", "timestamp_order_valid"))
            or row.get("duplicate_state") not in {"UNIQUE", "DUPLICATE_IDENTICAL", "DUPLICATE_CONFLICT"}
            or len(multiplier_flags) != 1
            or f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}" not in flags
            or str(row.get("row_id")) not in observation_by_id
        ):
            raise IntegrityError("candidate quality evidence is invalid")
    for row in cadence:
        if str(row.get("row_id")) not in required_ids:
            raise IntegrityError("candidate cadence evidence references an unknown row")
        if row.get("result") not in {"MATCH", "DISAGREEMENT", "NOT_COMPARABLE", "SOURCE_MISSING"}:
            raise IntegrityError("candidate cadence result is invalid")
        if row.get("result") != "MATCH" and row.get("exception_state") == "NONE":
            raise IntegrityError("candidate cadence mismatch lacks an exception")

    counts = {
        "observations": len(observations),
        "missingness": len(missingness),
        "roll": len(rolls),
        "quality": len(quality),
        "cadence": len(cadence),
    }
    if any(
        metadata.get(field) != counts[name]
        for name, field in (
            ("observations", "observation_count"),
            ("missingness", "missingness_count"),
            ("roll", "roll_count"),
            ("quality", "quality_count"),
            ("cadence", "cadence_comparison_count"),
        )
    ):
        raise IntegrityError("candidate manifest counts differ from independent rows")
    core: dict[str, object] = {
        "schema_version": "causal_observation_candidate_certificate/1.0.0",
        "status": "PASS_SYNTHETIC_OR_AUTHORIZED_CANDIDATE_ONLY_NOT_PUBLISHED",
        "release_id": manifest.release_id,
        "manifest_identity": sha256_json(manifest.as_dict()),
        "file_inventory_sha256": sha256_json([entry.as_dict() for entry in manifest.files]),
        "ordered_row_ids_sha256": sha256_json(row_ids),
        "counts": counts,
        "source_contract_id": metadata["source_contract_id"],
        "source_release_id": metadata["source_release_id"],
        "causal_contract_id": metadata["causal_contract_id"],
        "producer_success_flag_accepted": False,
        "publication_authorized": False,
        "activation_authorized": False,
        "outcome_count": 0,
        "feature_count": 0,
        "prediction_count": 0,
        "evaluation_count": 0,
    }
    return {**core, "certificate_id": sha256_json(core)}
