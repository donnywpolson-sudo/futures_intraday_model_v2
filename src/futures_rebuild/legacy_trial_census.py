"""Canonical unresolved census of outcome-informed legacy research attempts.

The census is intentionally evidence-only.  It consumes files through a
re-verified :class:`PublishedSourceSnapshot`, never reads the legacy repository,
and never converts the observed floor into an executable multiplicity penalty.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Iterable, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_plain_file,
    canonical_bytes,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError
from .foundation.snapshot import PublishedSourceSnapshot, SnapshotFile
from .migration import (
    AUTHORIZED_MIGRATION_MANIFEST_RELATIVE,
    AUTHORIZED_MIGRATION_MANIFEST_SHA256,
    load_manifest,
)
from .release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt


LEGACY_CENSUS_RELEASE_KIND = "legacy_trial_census"
LEGACY_CENSUS_SCHEMA_VERSION = "2.0.0"
LEGACY_CENSUS_FILENAME = "legacy_census.json"
LEGACY_EVIDENCE_DISPOSITION = "legacy_trial_census_evidence_only"
LEGACY_EVIDENCE_PREFIX = "evidence/legacy_research/"
PRESCRIBED_EVIDENCE_FILE_COUNT = 24

TARGET_REGISTRY_PATH = "manifests/target_hypotheses/registry.json"
TARGET_STATUSES_PATH = "manifests/target_hypotheses/trial_statuses.jsonl"
FEATURE_REGISTRY_PATH = "manifests/feature_hypotheses/registry.json"
FEATURE_STATUSES_PATH = "manifests/feature_hypotheses/trial_statuses.jsonl"
EXPERIMENT_LEDGER_PATH = "reports/experiments/ledger.jsonl"
MUTATION_PACKAGE_PATH = (
    "reports/master_audit/"
    "master_audit_canonical_trial_search_append_only_mutation_package_20260710/"
    "master_audit_canonical_trial_search_append_only_mutation_package.json"
)
PHASE6_STATISTICAL_SUMMARY_PATH = (
    "reports/statistical_validity/"
    "tier1_core_phase6_full_predictions_20260706/"
    "statistical_validity_summary.json"
)
ORAC_FAILURE_ANALYSIS_PATH = (
    "docs/opening_range_acceptance_continuation_30m_v1_failure_analysis.md"
)
ORAC_FAILURE_AUTOPSY_PATH = (
    "docs/opening_range_acceptance_continuation_30m_v1_failure_autopsy.md"
)
TERMINAL_DISTRIBUTIONAL_PROGRAM_ID = (
    "distributional_30m_probability_magnitude_v1"
)
TERMINAL_DISTRIBUTIONAL_WFA_PATH = (
    "reports/wfa_distributional/"
    "distributional_30m_probability_magnitude_v1_5929e9e/"
    "predictions_manifest.json"
)
TERMINAL_DISTRIBUTIONAL_AUDIT_PATH = (
    "reports/prediction_audit/"
    "distributional_30m_probability_magnitude_v1_5929e9e/"
    "distributional_prediction_audit.json"
)
TERMINAL_DISTRIBUTIONAL_ALPHA_PATH = (
    "reports/model_selection/"
    "distributional_30m_probability_magnitude_v1_5929e9e/"
    "distributional_alpha_evaluation.json"
)
TERMINAL_DISTRIBUTIONAL_PATHS = frozenset(
    {
        TERMINAL_DISTRIBUTIONAL_WFA_PATH,
        TERMINAL_DISTRIBUTIONAL_AUDIT_PATH,
        TERMINAL_DISTRIBUTIONAL_ALPHA_PATH,
    }
)
CORE_LEDGER_PATHS = frozenset(
    {
        TARGET_REGISTRY_PATH,
        TARGET_STATUSES_PATH,
        FEATURE_REGISTRY_PATH,
        FEATURE_STATUSES_PATH,
        EXPERIMENT_LEDGER_PATH,
        MUTATION_PACKAGE_PATH,
        PHASE6_STATISTICAL_SUMMARY_PATH,
        ORAC_FAILURE_ANALYSIS_PATH,
        ORAC_FAILURE_AUTOPSY_PATH,
        *TERMINAL_DISTRIBUTIONAL_PATHS,
    }
)

UNRESOLVED_STATUS = "INVALID_TRIAL_CENSUS_UNRESOLVED"
INDETERMINATE_COUNT_STATE = "INDETERMINATE"

_HASH = re.compile(r"[0-9a-f]{64}")
_REFERENCE = re.compile(
    r"^(?:evidence/legacy_research/)?(?:configs|docs|manifests|reports)/"
    r"[^\s]+\.(?:csv|json|jsonl|md|parquet|toml|yaml|yml)$",
    re.IGNORECASE,
)
_PROVENANCE_CATEGORIES = frozenset(
    {
        "FEATURE_STATUS_TRIAL",
        "EXPERIMENT_LEDGER_RUN",
        "TARGET_REGISTRY_ONLY_HYPOTHESIS",
        "TARGET_STATUS_TRIAL",
        "TERMINAL_DISTRIBUTIONAL_PROGRAM",
        "TERMINAL_ORAC_PROGRAM",
        "TERMINAL_PHASE6_PROGRAM",
    }
)
_CENSUS_KEYS = frozenset(
    {
        "census_sha256",
        "counting_rule",
        "exact_count_state",
        "observed_attempt_floor",
        "preregistered_penalty_count",
        "provenance",
        "rationale_sha256",
        "schema_version",
        "source_evidence",
        "source_evidence_sha256",
        "source_snapshot_id",
        "status",
        "trusted_gate",
        "unresolved_references",
    }
)


def _reopen_snapshot(
    snapshot: PublishedSourceSnapshot, *, boundary: RepoBoundary
) -> PublishedSourceSnapshot:
    if type(snapshot) is not PublishedSourceSnapshot:
        raise ContractError("legacy census requires a PublishedSourceSnapshot")
    verified = PublishedSourceSnapshot.open(snapshot.root, boundary=boundary)
    if (
        verified.source_snapshot_id != snapshot.source_snapshot_id
        or dict(verified.receipt) != dict(snapshot.receipt)
        or set(verified.files) != set(snapshot.files)
        or any(verified.files[path] != snapshot.files[path] for path in verified.files)
    ):
        raise IntegrityError("published source snapshot changed after verification")
    return verified


def _evidence_contract(
    snapshot: PublishedSourceSnapshot, *, boundary: RepoBoundary
) -> tuple[
    PublishedSourceSnapshot,
    tuple[dict[str, object], ...],
    Mapping[str, SnapshotFile],
    str,
]:
    verified = _reopen_snapshot(snapshot, boundary=boundary)
    manifest_path = boundary.assert_active_path(
        boundary.active_root / AUTHORIZED_MIGRATION_MANIFEST_RELATIVE,
        purpose="legacy census migration evidence contract",
        subtree="configs",
    )
    manifest, manifest_sha256 = load_manifest(manifest_path)
    if (
        manifest_sha256 != AUTHORIZED_MIGRATION_MANIFEST_SHA256
        or verified.receipt.get("manifest_sha256") != manifest_sha256
    ):
        raise IntegrityError(
            "legacy census evidence differs from the authorized snapshot manifest"
        )
    raw_entries = manifest.get("entries")
    if not isinstance(raw_entries, list):
        raise IntegrityError("authorized migration evidence entries are invalid")
    raw_source_root = manifest.get("source_root")
    if type(raw_source_root) is not str or not raw_source_root:
        raise IntegrityError("authorized migration source root is invalid")
    source_root = raw_source_root.replace("\\", "/").rstrip("/")
    source_root_posix = PurePosixPath(source_root)
    source_root_windows = PureWindowsPath(source_root)
    if (
        not source_root
        or not (
            source_root_posix.is_absolute()
            or source_root_windows.is_absolute()
        )
        or ".." in source_root_posix.parts
        or ".." in source_root_windows.parts
    ):
        raise IntegrityError("authorized migration source root is unsafe")
    selected = [
        item
        for item in raw_entries
        if isinstance(item, dict)
        and item.get("disposition") == LEGACY_EVIDENCE_DISPOSITION
    ]
    if len(selected) != PRESCRIBED_EVIDENCE_FILE_COUNT:
        raise IntegrityError("legacy census requires exactly 24 prescribed evidence files")

    records: list[dict[str, object]] = []
    bindings: dict[str, SnapshotFile] = {}
    seen_families: set[str] = set()
    seen_sources: set[str] = set()
    seen_destinations: set[str] = set()
    for entry in selected:
        expected_keys = {
            "destination",
            "disposition",
            "expected_bytes",
            "expected_files",
            "expected_sha256",
            "family",
            "kind",
            "source",
        }
        if set(entry) != expected_keys:
            raise IntegrityError("legacy census evidence manifest entry is not exact")
        family = entry.get("family")
        source = entry.get("source")
        destination = entry.get("destination")
        size = entry.get("expected_bytes")
        digest = entry.get("expected_sha256")
        if (
            type(family) is not str
            or not family.startswith("legacy_research_")
            or type(source) is not str
            or type(destination) is not str
            or entry.get("kind") != "file"
            or entry.get("expected_files") != 1
            or type(size) is not int
            or isinstance(size, bool)
            or size < 0
            or type(digest) is not str
            or _HASH.fullmatch(digest) is None
            or destination
            != (
                f"{LEGACY_EVIDENCE_PREFIX}by_family/{family}"
                f"{PurePosixPath(source).suffix}"
            )
        ):
            raise IntegrityError("legacy census evidence manifest binding is invalid")
        source_path = PurePosixPath(source)
        destination_path = PurePosixPath(destination)
        if (
            source_path.is_absolute()
            or destination_path.is_absolute()
            or ".." in source_path.parts
            or ".." in destination_path.parts
            or source_path.as_posix() != source
            or destination_path.as_posix() != destination
        ):
            raise IntegrityError("legacy census evidence path is unsafe")
        normalized = destination.casefold()
        if (
            family in seen_families
            or source.casefold() in seen_sources
            or normalized in seen_destinations
        ):
            raise IntegrityError("legacy census evidence contains a duplicate provenance")
        seen_families.add(family)
        seen_sources.add(source.casefold())
        seen_destinations.add(normalized)
        try:
            binding = verified.file(destination)
        except IntegrityError as exc:
            raise IntegrityError(
                f"prescribed legacy census evidence is absent: {destination}"
            ) from exc
        if binding.size != size or binding.sha256 != digest:
            raise IntegrityError("legacy census evidence was substituted or repinned")
        binding.verify()
        bindings[source] = binding
        records.append(
            {
                "family": family,
                "path": destination,
                "sha256": digest,
                "size": size,
                "source": source,
            }
        )

    prescribed_paths = {str(item["path"]) for item in records}
    prescribed_sources = {str(item["source"]) for item in records}
    observed_paths = {
        path for path in verified.files if path.startswith(LEGACY_EVIDENCE_PREFIX)
    }
    if prescribed_paths != observed_paths or not CORE_LEDGER_PATHS.issubset(
        prescribed_sources
    ):
        raise IntegrityError(
            "published snapshot legacy evidence is missing, duplicated, or unexpected"
        )
    return (
        verified,
        tuple(sorted(records, key=lambda item: str(item["source"]))),
        dict(sorted(bindings.items())),
        source_root,
    )


def _binding(
    bindings: Mapping[str, SnapshotFile], source_path: str
) -> SnapshotFile:
    try:
        binding = bindings[source_path]
    except KeyError as exc:
        raise IntegrityError(
            f"prescribed legacy census evidence is absent: {source_path}"
        ) from exc
    binding.verify()
    return binding


def _read_json(bindings: Mapping[str, SnapshotFile], path: str) -> object:
    try:
        raw = _binding(bindings, path).path.read_bytes()
        return json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"legacy census JSON is invalid: {path}") from exc


def _read_text(bindings: Mapping[str, SnapshotFile], path: str) -> str:
    try:
        return _binding(bindings, path).path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise IntegrityError(f"legacy census text evidence is invalid: {path}") from exc


@dataclass(frozen=True)
class _JsonlRow:
    payload: Mapping[str, object]
    raw_line_sha256: str
    ordinal: int


def _read_jsonl(
    bindings: Mapping[str, SnapshotFile], path: str
) -> tuple[_JsonlRow, ...]:
    try:
        raw = _binding(bindings, path).path.read_bytes()
    except OSError as exc:
        raise IntegrityError(f"legacy census JSONL is invalid: {path}") from exc
    raw_lines = raw.splitlines()
    if not raw_lines or any(not line.strip() for line in raw_lines):
        raise IntegrityError(f"legacy census JSONL is empty or contains blank rows: {path}")
    result: list[_JsonlRow] = []
    seen_rows: set[str] = set()
    for line_number, raw_line in enumerate(raw_lines, start=1):
        try:
            item = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError(
                f"legacy census JSONL row is invalid: {path}:{line_number}"
            ) from exc
        if not isinstance(item, dict):
            raise IntegrityError(
                f"legacy census JSONL row is not an object: {path}:{line_number}"
            )
        row_hash = sha256_bytes(raw_line)
        if row_hash in seen_rows:
            raise IntegrityError("legacy census ledger contains duplicate provenance rows")
        seen_rows.add(row_hash)
        result.append(_JsonlRow(item, row_hash, line_number))
    return tuple(result)


def _registry_records(payload: object, *, description: str) -> tuple[dict[str, object], ...]:
    collection: object = payload
    if isinstance(payload, dict):
        candidates: list[object] = []
        for key in ("hypotheses", "registry", "entries", "records"):
            if key in payload:
                candidates.append(payload[key])
        if len(candidates) != 1:
            inferred = [
                value
                for value in payload.values()
                if isinstance(value, list)
                and all(isinstance(item, dict) for item in value)
            ]
            if len(candidates) == 0 and len(inferred) == 1:
                candidates = inferred
        if len(candidates) != 1:
            raise IntegrityError(f"{description} collection is ambiguous")
        collection = candidates[0]
    if not isinstance(collection, list) or not collection or any(
        not isinstance(item, dict) for item in collection
    ):
        raise IntegrityError(f"{description} records are invalid")
    return tuple(collection)  # type: ignore[return-value]


def _nested_values(record: Mapping[str, object], field: str) -> tuple[object, ...]:
    result: list[object] = []
    for key, value in record.items():
        if key == field:
            result.append(value)
        if isinstance(value, dict):
            result.extend(_nested_values(value, field))
    return tuple(result)


def _optional_string(record: Mapping[str, object], *fields: str) -> tuple[str | None, str | None]:
    for field in fields:
        direct = record.get(field)
        if direct is not None:
            if type(direct) is not str or not direct.strip():
                raise IntegrityError(f"legacy census {field} is not a nonempty string")
            return direct, field
    for field in fields:
        values = _nested_values(record, field)
        normalized = {value for value in values if type(value) is str and value.strip()}
        invalid = [value for value in values if value is not None and type(value) is not str]
        if invalid or len(normalized) > 1:
            raise IntegrityError(f"legacy census {field} provenance is ambiguous")
        if normalized:
            return next(iter(normalized)), field
    return None, None


def _required_hypothesis_id(record: Mapping[str, object]) -> str:
    value, _ = _optional_string(
        record,
        "hypothesis_id",
        "target_hypothesis_id",
        "feature_hypothesis_id",
    )
    if value is None:
        raise IntegrityError("legacy census record has no hypothesis provenance")
    return value


def _registry_index(
    records: Sequence[Mapping[str, object]], *, description: str
) -> dict[str, tuple[Mapping[str, object], str]]:
    result: dict[str, tuple[Mapping[str, object], str]] = {}
    for record in records:
        hypothesis_id = _required_hypothesis_id(record)
        if hypothesis_id in result:
            raise IntegrityError(f"{description} repeats hypothesis provenance")
        result[hypothesis_id] = (record, sha256_json(dict(record)))
    return result


def _provenance_record(
    *,
    category: str,
    hypothesis_id: str,
    identifier: str,
    identifier_field: str,
    source_paths: Iterable[str],
    source_record_sha256s: Iterable[str],
) -> dict[str, object]:
    core: dict[str, object] = {
        "category": category,
        "hypothesis_id": hypothesis_id,
        "identifier": identifier,
        "identifier_field": identifier_field,
        "source_paths": sorted(set(source_paths)),
        "source_record_sha256s": sorted(set(source_record_sha256s)),
    }
    if (
        category not in _PROVENANCE_CATEGORIES
        or not all(type(core[key]) is str and core[key] for key in (
            "hypothesis_id",
            "identifier",
            "identifier_field",
        ))
        or not core["source_paths"]
        or not core["source_record_sha256s"]
    ):
        raise IntegrityError("legacy census provenance is incomplete")
    return {**core, "provenance_id": sha256_json(core)}


def _status_provenance(
    records: Sequence[_JsonlRow],
    *,
    registry: Mapping[str, tuple[Mapping[str, object], str]],
    category: str,
    source_path: str,
) -> tuple[tuple[dict[str, object], ...], frozenset[str]]:
    normalized: list[tuple[_JsonlRow, str, str, str | None, str]] = []
    aliases: dict[str, str] = {}
    for row in records:
        record = row.payload
        source_trial_id, source_field = _optional_string(record, "source_trial_id")
        trial_id, _ = _optional_string(record, "trial_id")
        if source_trial_id is None and trial_id is None:
            raise IntegrityError("legacy census status row has no trial provenance")
        if source_trial_id is not None and trial_id is not None:
            observed = aliases.get(trial_id)
            if observed is not None and observed != source_trial_id:
                raise IntegrityError("one trial ID maps to duplicate source provenance")
            aliases[trial_id] = source_trial_id
        identifier = source_trial_id or trial_id
        assert identifier is not None
        hypothesis_id = _required_hypothesis_id(record)
        if hypothesis_id not in registry:
            raise IntegrityError("legacy status provenance is absent from its registry")
        normalized.append(
            (row, identifier, source_field or "trial_id", trial_id, hypothesis_id)
        )

    grouped: dict[str, list[tuple[_JsonlRow, str, str]]] = defaultdict(list)
    for row, identifier, identifier_field, trial_id, hypothesis_id in normalized:
        canonical_identifier = aliases.get(trial_id, identifier) if trial_id else identifier
        canonical_field = (
            "source_trial_id"
            if canonical_identifier != trial_id or identifier_field == "source_trial_id"
            else "trial_id"
        )
        grouped[canonical_identifier].append((row, canonical_field, hypothesis_id))

    result: list[dict[str, object]] = []
    covered: set[str] = set()
    for identifier, group in grouped.items():
        hypotheses = {item[2] for item in group}
        fields = {item[1] for item in group}
        if len(hypotheses) != 1 or not fields.issubset(
            {"source_trial_id", "trial_id"}
        ):
            raise IntegrityError("deduplicated trial provenance is contradictory")
        hypothesis_id = next(iter(hypotheses))
        canonical_field = (
            "source_trial_id" if "source_trial_id" in fields else "trial_id"
        )
        covered.add(hypothesis_id)
        result.append(
            _provenance_record(
                category=category,
                hypothesis_id=hypothesis_id,
                identifier=identifier,
                identifier_field=canonical_field,
                source_paths=(source_path,),
                source_record_sha256s=(item[0].raw_line_sha256 for item in group),
            )
        )
    return tuple(result), frozenset(covered)


def _evidence_basename(value: object) -> str:
    if type(value) is not str or not value:
        raise IntegrityError("legacy exclusion evidence path is invalid")
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != normalized:
        raise IntegrityError("legacy exclusion evidence path is unsafe")
    return path.name


def _candidate_evidence_basenames(candidate: Mapping[str, object]) -> set[str]:
    raw = candidate.get("evidence_paths")
    if not isinstance(raw, list) or not raw:
        raise IntegrityError("legacy exclusion candidate lacks evidence paths")
    return {_evidence_basename(value) for value in raw}


def _validate_exclusion_candidate(
    candidate: Mapping[str, object],
    *,
    row_origin: str,
    row_id: str,
    trial_id: str,
) -> None:
    if (
        candidate.get("row_origin") != row_origin
        or candidate.get("row_id") != row_id
        or candidate.get("trial_id") != trial_id
        or candidate.get("disposition")
        != "EXCLUDE_FROM_CANONICAL_TRIAL_SEARCH_LEDGER"
        or candidate.get("canonical_mutation_executed") is not False
        or candidate.get("append_to_experiment_ledger_allowed") is not False
        or candidate.get("append_to_trial_statuses_allowed") is not False
    ):
        raise IntegrityError("legacy exclusion candidate semantics are invalid")


def _excluded_run_provenance(
    *,
    mutation_package: object,
    experiment_rows: Sequence[_JsonlRow],
    statistical_summary: object,
    bindings: Mapping[str, SnapshotFile],
) -> tuple[tuple[dict[str, object], ...], dict[str, object]]:
    if not isinstance(mutation_package, dict):
        raise IntegrityError("canonical mutation package evidence is invalid")
    package = mutation_package.get("canonical_mutation_package")
    if not isinstance(package, dict):
        raise IntegrityError("canonical mutation package body is absent")
    candidates = package.get("exclusion_disposition_candidates")
    if (
        not isinstance(candidates, list)
        or len(candidates) != 5
        or any(not isinstance(candidate, dict) for candidate in candidates)
        or len(experiment_rows) != 4
    ):
        raise IntegrityError("legacy exclusion provenance is incomplete")

    expected_audits = (
        "anti_overfit_audit.json",
        "anti_overfit_audit_with_drilldown.json",
        "anti_overfit_audit_refreshed.json",
        "anti_overfit_audit_data_audit_guard_tier1_smoke.json",
    )
    result: list[dict[str, object]] = []
    for ordinal, (raw_candidate, row, expected_audit) in enumerate(
        zip(candidates[:4], experiment_rows, expected_audits, strict=True),
        start=1,
    ):
        assert isinstance(raw_candidate, dict)
        expected_row_id = f"experiment_ledger_{ordinal:03d}"
        expected_trial_id = f"experiment_ledger_row_{ordinal:03d}"
        _validate_exclusion_candidate(
            raw_candidate,
            row_origin="experiment_ledger",
            row_id=expected_row_id,
            trial_id=expected_trial_id,
        )
        if expected_audit not in _candidate_evidence_basenames(raw_candidate):
            raise IntegrityError("experiment exclusion candidate evidence is substituted")
        row_audit = row.payload.get("audit_report_path")
        if _evidence_basename(row_audit) != expected_audit or row.ordinal != ordinal:
            raise IntegrityError("experiment exclusion candidate does not bind its JSONL row")
        result.append(
            _provenance_record(
                category="EXPERIMENT_LEDGER_RUN",
                hypothesis_id=expected_trial_id,
                identifier=expected_trial_id,
                identifier_field="trial_id",
                source_paths=(EXPERIMENT_LEDGER_PATH, MUTATION_PACKAGE_PATH),
                source_record_sha256s=(
                    row.raw_line_sha256,
                    sha256_json(raw_candidate),
                ),
            )
        )

    phase6_candidate = candidates[4]
    assert isinstance(phase6_candidate, dict)
    phase6_trial_id = "tier1_core_phase6_full_predictions_20260706_current_line"
    _validate_exclusion_candidate(
        phase6_candidate,
        row_origin="current_wfa_phase8_statistical_run",
        row_id="current_wfa_phase8_statistical_run_001",
        trial_id=phase6_trial_id,
    )
    if PHASE6_STATISTICAL_SUMMARY_PATH.split("/")[-1] not in (
        _candidate_evidence_basenames(phase6_candidate)
    ):
        raise IntegrityError("Phase 6 exclusion candidate evidence is substituted")
    if not isinstance(statistical_summary, dict) or (
        statistical_summary.get("run")
        != "tier1_core_phase6_full_predictions_20260706"
        or statistical_summary.get("diagnostic_type")
        != "phase9_statistical_validity"
        or statistical_summary.get("status") != "FAIL"
        or statistical_summary.get("statistical_validity_ready") is not False
        or statistical_summary.get("research_only") is not True
        or statistical_summary.get("model_promotion_allowed") is not False
        or statistical_summary.get("failure_count") != 5
    ):
        raise IntegrityError("terminal Phase 6 statistical evidence is invalid")
    phase6 = _provenance_record(
        category="TERMINAL_PHASE6_PROGRAM",
        hypothesis_id="tier1_core_phase6_full_predictions_20260706",
        identifier=phase6_trial_id,
        identifier_field="trial_id",
        source_paths=(MUTATION_PACKAGE_PATH, PHASE6_STATISTICAL_SUMMARY_PATH),
        source_record_sha256s=(
            sha256_json(phase6_candidate),
            _binding(bindings, PHASE6_STATISTICAL_SUMMARY_PATH).sha256,
        ),
    )
    return tuple(result), phase6


def _compact_number_text(value: str) -> str:
    return re.sub(r"[\s,$`]", "", value.casefold())


def _states_prohibition(value: str, term: str) -> bool:
    lowered = value.casefold()
    escaped = re.escape(term.casefold())
    return any(
        re.search(pattern, lowered, flags=re.DOTALL) is not None
        for pattern in (
            rf"\bno\b.{{0,100}}\b{escaped}",
            rf"\bdo\s+not\b.{{0,100}}\b{escaped}",
            rf"\bdoes\s+not\s+(?:recommend|approve)\b.{{0,160}}\b{escaped}",
            rf"\b{escaped}\w*\b.{{0,100}}\b(?:not\s+allowed|forbidden)\b",
        )
    )


def _terminal_orac_provenance(
    *, bindings: Mapping[str, SnapshotFile]
) -> dict[str, object]:
    analysis = _read_text(bindings, ORAC_FAILURE_ANALYSIS_PATH)
    autopsy = _read_text(bindings, ORAC_FAILURE_AUTOPSY_PATH)
    program_id = "opening_range_acceptance_continuation_30m_v1"
    analysis_lower = analysis.casefold()
    autopsy_lower = autopsy.casefold()
    analysis_numbers = _compact_number_text(analysis)
    autopsy_numbers = _compact_number_text(autopsy)
    if (
        program_id not in analysis
        or "phase 6" not in analysis_lower
        or "wfa" not in analysis_lower
        or "expansion" not in analysis_lower
        or "prediction_count=72539" not in analysis_numbers
        or "fold_count=4" not in analysis_numbers
        or "net_return_dollars=-80468.5" not in analysis_numbers
        or re.search(
            r"all\s+(?:4|four)\s+folds.{0,80}negative",
            analysis_lower,
            re.DOTALL,
        )
        is None
        or not _states_prohibition(analysis, "tuning")
        or not _states_prohibition(analysis, "rerun")
        or not _states_prohibition(analysis, "promotion")
    ):
        raise IntegrityError("ORAC terminal failure analysis is incomplete")
    if (
        program_id not in autopsy
        or "72539" not in autopsy_numbers
        or "-80468.50" not in autopsy_numbers
        or "first_touch_feasibility_no_go" not in autopsy_lower
        or "0/36" not in autopsy_numbers
        or not (
            "diagnostic-only" in autopsy_lower
            or "diagnostic only" in autopsy_lower
        )
        or not _states_prohibition(autopsy, "rescue")
        or not _states_prohibition(autopsy, "promotion")
    ):
        raise IntegrityError("ORAC terminal failure autopsy is incomplete")
    return _provenance_record(
        category="TERMINAL_ORAC_PROGRAM",
        hypothesis_id=program_id,
        identifier=f"{program_id}_terminal_failure",
        identifier_field="program_id",
        source_paths=(ORAC_FAILURE_ANALYSIS_PATH, ORAC_FAILURE_AUTOPSY_PATH),
        source_record_sha256s=(
            _binding(bindings, ORAC_FAILURE_ANALYSIS_PATH).sha256,
            _binding(bindings, ORAC_FAILURE_AUTOPSY_PATH).sha256,
        ),
    )


def _normalized_reference_path(value: object, *, source_root: str) -> str:
    if type(value) is not str or not value:
        raise IntegrityError("terminal distributional artifact path is invalid")
    result = value.replace("\\", "/")
    if result.startswith(LEGACY_EVIDENCE_PREFIX):
        result = result[len(LEGACY_EVIDENCE_PREFIX) :]
    authorized_prefix = f"{source_root}/"
    if result.startswith(authorized_prefix):
        result = result[len(authorized_prefix) :]
    path = PurePosixPath(result)
    windows_path = PureWindowsPath(result)
    if (
        path.is_absolute()
        or windows_path.is_absolute()
        or ".." in path.parts
        or ".." in windows_path.parts
        or path.as_posix() != result
    ):
        raise IntegrityError("terminal distributional artifact path is unsafe")
    return result


def _terminal_distributional_provenance(
    bindings: Mapping[str, SnapshotFile],
    *,
    source_root: str,
) -> tuple[dict[str, object], tuple[tuple[str, object], ...]]:
    wfa = _read_json(bindings, TERMINAL_DISTRIBUTIONAL_WFA_PATH)
    audit = _read_json(bindings, TERMINAL_DISTRIBUTIONAL_AUDIT_PATH)
    alpha = _read_json(bindings, TERMINAL_DISTRIBUTIONAL_ALPHA_PATH)
    if not all(isinstance(item, dict) for item in (wfa, audit, alpha)):
        raise IntegrityError("terminal distributional evidence is not JSON objects")
    assert isinstance(wfa, dict) and isinstance(audit, dict) and isinstance(alpha, dict)

    required_wfa_hashes = (
        "source_sha256",
        "split_plan_sha256",
        "projection_manifest_sha256",
        "models_config_sha256",
        "phase6_policy_sha256",
        "evaluation_policy_sha256",
    )
    if (
        wfa.get("program_id") != TERMINAL_DISTRIBUTIONAL_PROGRAM_ID
        or wfa.get("status") != "PASS"
        or wfa.get("research_only") is not True
        or wfa.get("promotion_allowed") is not False
        or any(
            type(wfa.get(field)) is not str
            or _HASH.fullmatch(str(wfa.get(field))) is None
            for field in required_wfa_hashes
        )
    ):
        raise IntegrityError("distributional WFA evidence is invalid or nonterminal")

    prediction_manifest = audit.get("prediction_manifest")
    prediction_artifact = audit.get("prediction_artifact")
    wfa_binding = _binding(bindings, TERMINAL_DISTRIBUTIONAL_WFA_PATH)
    if (
        audit.get("program_id") != TERMINAL_DISTRIBUTIONAL_PROGRAM_ID
        or audit.get("status") != "FAIL"
        or audit.get("failure_count") != 223
        or not isinstance(prediction_manifest, dict)
        or not isinstance(prediction_artifact, dict)
        or _normalized_reference_path(
            prediction_manifest.get("path"), source_root=source_root
        )
        != TERMINAL_DISTRIBUTIONAL_WFA_PATH
        or prediction_manifest.get("sha256") != wfa_binding.sha256
        or _normalized_reference_path(
            prediction_artifact.get("path"), source_root=source_root
        )
        == TERMINAL_DISTRIBUTIONAL_WFA_PATH
        or type(prediction_artifact.get("sha256")) is not str
        or _HASH.fullmatch(str(prediction_artifact.get("sha256"))) is None
    ):
        raise IntegrityError("distributional prediction-audit evidence is invalid")

    if (
        alpha.get("program_id") != TERMINAL_DISTRIBUTIONAL_PROGRAM_ID
        or alpha.get("decision") != "REJECT"
        or alpha.get("audit_status") != "FAIL"
        or alpha.get("audit_failure_count") != 223
        or alpha.get("promotion_allowed") is not False
        or alpha.get("model_selection_allowed") is not False
        or any(
            type(alpha.get(field)) is not str
            or not alpha.get(field)
            for field in ("policy_id", "policy_sha256", "costs_sha256")
        )
        or _HASH.fullmatch(str(alpha.get("policy_sha256"))) is None
        or _HASH.fullmatch(str(alpha.get("costs_sha256"))) is None
    ):
        raise IntegrityError("distributional alpha-evaluation evidence is invalid")

    audit_binding = _binding(bindings, TERMINAL_DISTRIBUTIONAL_AUDIT_PATH)
    alpha_binding = _binding(bindings, TERMINAL_DISTRIBUTIONAL_ALPHA_PATH)
    record = _provenance_record(
        category="TERMINAL_DISTRIBUTIONAL_PROGRAM",
        hypothesis_id=TERMINAL_DISTRIBUTIONAL_PROGRAM_ID,
        identifier=TERMINAL_DISTRIBUTIONAL_PROGRAM_ID,
        identifier_field="program_id",
        source_paths=TERMINAL_DISTRIBUTIONAL_PATHS,
        source_record_sha256s=(
            wfa_binding.sha256,
            audit_binding.sha256,
            alpha_binding.sha256,
        ),
    )
    return record, (
        (TERMINAL_DISTRIBUTIONAL_WFA_PATH, wfa),
        (TERMINAL_DISTRIBUTIONAL_AUDIT_PATH, audit),
        (TERMINAL_DISTRIBUTIONAL_ALPHA_PATH, alpha),
    )


def _walk_references(value: object, pointer: str = "") -> Iterable[tuple[str, str]]:
    if isinstance(value, dict):
        for key in sorted(value):
            escaped = str(key).replace("~", "~0").replace("/", "~1")
            yield from _walk_references(value[key], f"{pointer}/{escaped}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_references(item, f"{pointer}/{index}")
    elif isinstance(value, str):
        normalized = value.replace("\\", "/")
        if _REFERENCE.fullmatch(normalized):
            if normalized.casefold().startswith(LEGACY_EVIDENCE_PREFIX.casefold()):
                normalized = normalized[len(LEGACY_EVIDENCE_PREFIX) :]
            logical = PurePosixPath(normalized)
            if logical.is_absolute() or ".." in logical.parts or logical.as_posix() != normalized:
                raise IntegrityError("legacy evidence reference path is unsafe")
            yield normalized, pointer or "/"


def _unresolved_references(
    *,
    bindings: Mapping[str, SnapshotFile],
    sources: Sequence[tuple[str, object]],
) -> list[dict[str, object]]:
    references: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for source_path, payload in sources:
        for reference, pointer in _walk_references(payload):
            references[reference].add((source_path, pointer))
    unresolved: list[dict[str, object]] = []
    for reference in sorted(references):
        if reference in bindings:
            bindings[reference].verify()
            continue
        unresolved.append(
            {
                "path": reference,
                "reason": "ABSENT_FROM_PRESCRIBED_VERIFIED_EVIDENCE",
                "referenced_by": [
                    {"json_pointer": pointer, "path": source_path}
                    for source_path, pointer in sorted(references[reference])
                ],
            }
        )
    return unresolved


def _derive_legacy_trial_census(
    snapshot: PublishedSourceSnapshot, *, boundary: RepoBoundary
) -> dict[str, object]:
    (
        verified,
        source_evidence_tuple,
        evidence_bindings,
        source_root,
    ) = _evidence_contract(snapshot, boundary=boundary)
    source_evidence = list(source_evidence_tuple)

    target_registry_payload = _read_json(evidence_bindings, TARGET_REGISTRY_PATH)
    feature_registry_payload = _read_json(evidence_bindings, FEATURE_REGISTRY_PATH)
    target_statuses = _read_jsonl(evidence_bindings, TARGET_STATUSES_PATH)
    feature_statuses = _read_jsonl(evidence_bindings, FEATURE_STATUSES_PATH)
    experiment_ledger = _read_jsonl(evidence_bindings, EXPERIMENT_LEDGER_PATH)
    mutation_package = _read_json(evidence_bindings, MUTATION_PACKAGE_PATH)
    statistical_summary = _read_json(
        evidence_bindings, PHASE6_STATISTICAL_SUMMARY_PATH
    )

    target_registry_records = _registry_records(
        target_registry_payload, description="target hypothesis registry"
    )
    feature_registry_records = _registry_records(
        feature_registry_payload, description="feature hypothesis registry"
    )
    target_registry = _registry_index(
        target_registry_records, description="target hypothesis registry"
    )
    feature_registry = _registry_index(
        feature_registry_records, description="feature hypothesis registry"
    )

    target_provenance, covered_target_hypotheses = _status_provenance(
        target_statuses,
        registry=target_registry,
        category="TARGET_STATUS_TRIAL",
        source_path=TARGET_STATUSES_PATH,
    )
    feature_provenance, _ = _status_provenance(
        feature_statuses,
        registry=feature_registry,
        category="FEATURE_STATUS_TRIAL",
        source_path=FEATURE_STATUSES_PATH,
    )
    registry_only = [
        _provenance_record(
            category="TARGET_REGISTRY_ONLY_HYPOTHESIS",
            hypothesis_id=hypothesis_id,
            identifier=hypothesis_id,
            identifier_field="hypothesis_id",
            source_paths=(TARGET_REGISTRY_PATH,),
            source_record_sha256s=(target_registry[hypothesis_id][1],),
        )
        for hypothesis_id in sorted(set(target_registry) - set(covered_target_hypotheses))
    ]
    terminal_distributional, terminal_reference_sources = (
        _terminal_distributional_provenance(
            evidence_bindings, source_root=source_root
        )
    )
    experiment_provenance, terminal_phase6 = _excluded_run_provenance(
        mutation_package=mutation_package,
        experiment_rows=experiment_ledger,
        statistical_summary=statistical_summary,
        bindings=evidence_bindings,
    )
    terminal_orac = _terminal_orac_provenance(bindings=evidence_bindings)

    provenance = [
        *target_provenance,
        *registry_only,
        *feature_provenance,
        *experiment_provenance,
        terminal_distributional,
        terminal_phase6,
        terminal_orac,
    ]
    provenance.sort(
        key=lambda item: (
            str(item["category"]),
            str(item["identifier"]),
            str(item["hypothesis_id"]),
        )
    )
    identifiers = [str(item["identifier"]) for item in provenance]
    provenance_ids = [str(item["provenance_id"]) for item in provenance]
    if len(set(identifiers)) != len(identifiers) or len(set(provenance_ids)) != len(
        provenance_ids
    ):
        raise IntegrityError("legacy census contains duplicate counted provenance")

    reference_sources: list[tuple[str, object]] = [
        (TARGET_REGISTRY_PATH, target_registry_payload),
        (TARGET_STATUSES_PATH, [row.payload for row in target_statuses]),
        (FEATURE_REGISTRY_PATH, feature_registry_payload),
        (FEATURE_STATUSES_PATH, [row.payload for row in feature_statuses]),
        (EXPERIMENT_LEDGER_PATH, [row.payload for row in experiment_ledger]),
        (MUTATION_PACKAGE_PATH, mutation_package),
        (PHASE6_STATISTICAL_SUMMARY_PATH, statistical_summary),
        *terminal_reference_sources,
    ]
    unresolved = _unresolved_references(
        bindings=evidence_bindings, sources=reference_sources
    )
    category_counts = dict(
        sorted(Counter(str(item["category"]) for item in provenance).items())
    )
    counting_rule = {
        "category_counts": category_counts,
        "deduplication": "SOURCE_TRIAL_ID_FALLBACK_TRIAL_ID",
        "exact_count_state": INDETERMINATE_COUNT_STATE,
        "excluded_experiment_ledger_runs_counted": True,
        "manual_plot_or_report_exposure_complete": False,
        "registry_only_target_hypotheses_counted": True,
        "terminal_distributional_evidence_counted": True,
        "terminal_orac_evidence_counted": True,
        "terminal_phase6_evidence_counted": True,
        "unresolved_reference_count": len(unresolved),
    }
    source_evidence_sha256 = sha256_json(source_evidence)
    rationale_sha256 = sha256_json(counting_rule)
    core: dict[str, object] = {
        "counting_rule": counting_rule,
        "exact_count_state": INDETERMINATE_COUNT_STATE,
        "observed_attempt_floor": len(provenance),
        "preregistered_penalty_count": 0,
        "provenance": provenance,
        "rationale_sha256": rationale_sha256,
        "schema_version": LEGACY_CENSUS_SCHEMA_VERSION,
        "source_evidence": source_evidence,
        "source_evidence_sha256": source_evidence_sha256,
        "source_snapshot_id": verified.source_snapshot_id,
        "status": UNRESOLVED_STATUS,
        "trusted_gate": False,
        "unresolved_references": unresolved,
    }
    payload = {**core, "census_sha256": sha256_json(core)}
    validate_legacy_trial_census_payload(payload)
    return payload


def _valid_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def validate_legacy_trial_census_payload(
    payload: object,
) -> dict[str, object]:
    """Validate the exact schema and internal content addresses of census v2."""

    if not isinstance(payload, dict) or set(payload) != _CENSUS_KEYS:
        raise IntegrityError("canonical legacy census schema is invalid")
    if (
        payload.get("schema_version") != LEGACY_CENSUS_SCHEMA_VERSION
        or payload.get("status") != UNRESOLVED_STATUS
        or payload.get("exact_count_state") != INDETERMINATE_COUNT_STATE
        or payload.get("preregistered_penalty_count") != 0
        or payload.get("trusted_gate") is not False
        or not _valid_nonnegative_int(payload.get("observed_attempt_floor"))
        or _HASH.fullmatch(str(payload.get("source_snapshot_id"))) is None
        or _HASH.fullmatch(str(payload.get("source_evidence_sha256"))) is None
        or _HASH.fullmatch(str(payload.get("rationale_sha256"))) is None
        or _HASH.fullmatch(str(payload.get("census_sha256"))) is None
    ):
        raise IntegrityError("canonical legacy census fixed fields are invalid")

    evidence = payload.get("source_evidence")
    if (
        not isinstance(evidence, list)
        or len(evidence) != PRESCRIBED_EVIDENCE_FILE_COUNT
        or evidence != sorted(evidence, key=lambda item: str(item.get("source", "")))
        or any(
            not isinstance(item, dict)
            or set(item) != {"family", "path", "sha256", "size", "source"}
            or type(item["family"]) is not str
            or type(item["path"]) is not str
            or not item["path"].startswith(LEGACY_EVIDENCE_PREFIX)
            or type(item["source"]) is not str
            or not item["source"]
            or item["path"]
            != (
                f"{LEGACY_EVIDENCE_PREFIX}by_family/{item['family']}"
                f"{PurePosixPath(item['source']).suffix}"
            )
            or type(item["sha256"]) is not str
            or _HASH.fullmatch(item["sha256"]) is None
            or not _valid_nonnegative_int(item["size"])
            for item in evidence
        )
        or len({str(item["path"]).casefold() for item in evidence}) != len(evidence)
        or len({str(item["source"]).casefold() for item in evidence}) != len(evidence)
        or sha256_json(evidence) != payload["source_evidence_sha256"]
    ):
        raise IntegrityError("canonical legacy census source evidence is invalid")

    provenance = payload.get("provenance")
    if not isinstance(provenance, list) or len(provenance) != payload[
        "observed_attempt_floor"
    ]:
        raise IntegrityError("canonical legacy census observed floor is invalid")
    expected_provenance_keys = {
        "category",
        "hypothesis_id",
        "identifier",
        "identifier_field",
        "provenance_id",
        "source_paths",
        "source_record_sha256s",
    }
    expected_order = sorted(
        provenance,
        key=lambda item: (
            str(item.get("category", "")),
            str(item.get("identifier", "")),
            str(item.get("hypothesis_id", "")),
        ),
    )
    if provenance != expected_order:
        raise IntegrityError("canonical legacy census provenance is not sorted")
    identifiers: set[str] = set()
    provenance_ids: set[str] = set()
    for item in provenance:
        if not isinstance(item, dict) or set(item) != expected_provenance_keys:
            raise IntegrityError("canonical legacy census provenance schema is invalid")
        strings = (
            item["category"],
            item["hypothesis_id"],
            item["identifier"],
            item["identifier_field"],
            item["provenance_id"],
        )
        source_paths = item["source_paths"]
        row_hashes = item["source_record_sha256s"]
        core = {key: item[key] for key in item if key != "provenance_id"}
        if (
            any(type(value) is not str or not value for value in strings)
            or item["category"] not in _PROVENANCE_CATEGORIES
            or _HASH.fullmatch(item["provenance_id"]) is None
            or not isinstance(source_paths, list)
            or not source_paths
            or source_paths != sorted(set(source_paths))
            or any(type(value) is not str or not value for value in source_paths)
            or not isinstance(row_hashes, list)
            or not row_hashes
            or row_hashes != sorted(set(row_hashes))
            or any(type(value) is not str or _HASH.fullmatch(value) is None for value in row_hashes)
            or sha256_json(core) != item["provenance_id"]
            or item["identifier"] in identifiers
            or item["provenance_id"] in provenance_ids
        ):
            raise IntegrityError("canonical legacy census provenance is invalid")
        identifiers.add(item["identifier"])
        provenance_ids.add(item["provenance_id"])

    unresolved = payload.get("unresolved_references")
    if not isinstance(unresolved, list) or unresolved != sorted(
        unresolved, key=lambda item: str(item.get("path", ""))
    ):
        raise IntegrityError("canonical legacy census unresolved references are invalid")
    seen_unresolved: set[str] = set()
    for item in unresolved:
        if (
            not isinstance(item, dict)
            or set(item) != {"path", "reason", "referenced_by"}
            or type(item["path"]) is not str
            or not item["path"]
            or item["reason"] != "ABSENT_FROM_PRESCRIBED_VERIFIED_EVIDENCE"
            or item["path"] in seen_unresolved
            or not isinstance(item["referenced_by"], list)
            or not item["referenced_by"]
            or item["referenced_by"]
            != sorted(
                item["referenced_by"],
                key=lambda value: (str(value.get("path", "")), str(value.get("json_pointer", ""))),
            )
            or any(
                not isinstance(reference, dict)
                or set(reference) != {"json_pointer", "path"}
                or type(reference["json_pointer"]) is not str
                or type(reference["path"]) is not str
                for reference in item["referenced_by"]
            )
        ):
            raise IntegrityError("canonical legacy census unresolved reference is invalid")
        seen_unresolved.add(item["path"])

    rule = payload.get("counting_rule")
    expected_rule_keys = {
        "category_counts",
        "deduplication",
        "exact_count_state",
        "excluded_experiment_ledger_runs_counted",
        "manual_plot_or_report_exposure_complete",
        "registry_only_target_hypotheses_counted",
        "terminal_distributional_evidence_counted",
        "terminal_orac_evidence_counted",
        "terminal_phase6_evidence_counted",
        "unresolved_reference_count",
    }
    category_counts = Counter(str(item["category"]) for item in provenance)
    if (
        not isinstance(rule, dict)
        or set(rule) != expected_rule_keys
        or rule["category_counts"] != dict(sorted(category_counts.items()))
        or rule["deduplication"] != "SOURCE_TRIAL_ID_FALLBACK_TRIAL_ID"
        or rule["exact_count_state"] != INDETERMINATE_COUNT_STATE
        or rule["excluded_experiment_ledger_runs_counted"] is not True
        or rule["manual_plot_or_report_exposure_complete"] is not False
        or rule["registry_only_target_hypotheses_counted"] is not True
        or rule["terminal_distributional_evidence_counted"] is not True
        or rule["terminal_orac_evidence_counted"] is not True
        or rule["terminal_phase6_evidence_counted"] is not True
        or rule["unresolved_reference_count"] != len(unresolved)
        or sha256_json(rule) != payload["rationale_sha256"]
    ):
        raise IntegrityError("canonical legacy census counting rationale is invalid")
    core = {key: payload[key] for key in payload if key != "census_sha256"}
    if sha256_json(core) != payload["census_sha256"]:
        raise IntegrityError("canonical legacy census content address is invalid")
    return dict(payload)


def publish_legacy_trial_census(
    *,
    snapshot: PublishedSourceSnapshot,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Derive and atomically publish the one fail-closed legacy census."""

    if publisher.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("legacy census publisher belongs to another repository")
    payload = _derive_legacy_trial_census(snapshot, boundary=boundary)
    stage = publisher.create_stage("legacy_trial_census")
    (stage / LEGACY_CENSUS_FILENAME).write_bytes(canonical_bytes(payload) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        release_kind=LEGACY_CENSUS_RELEASE_KIND,
        schema_version=LEGACY_CENSUS_SCHEMA_VERSION,
        source_release_ids=(snapshot.source_snapshot_id,),
        metadata={
            "census_sha256": payload["census_sha256"],
            "exact_count_state": payload["exact_count_state"],
            "source_evidence_sha256": payload["source_evidence_sha256"],
            "source_snapshot_id": payload["source_snapshot_id"],
            "status": payload["status"],
            "trusted_gate": payload["trusted_gate"],
        },
    )
    release = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_release(release, boundary)
    load_legacy_trial_census(receipt, snapshot=snapshot, boundary=boundary)
    return receipt


def load_legacy_trial_census(
    receipt: VerifiedReleaseReceipt,
    *,
    snapshot: PublishedSourceSnapshot,
    boundary: RepoBoundary,
) -> dict[str, object]:
    """Load a census only after re-deriving it from the verified source snapshot."""

    verified = _reopen_snapshot(snapshot, boundary=boundary)
    manifest = receipt.verify(boundary)
    expected_metadata = {
        "census_sha256",
        "exact_count_state",
        "source_evidence_sha256",
        "source_snapshot_id",
        "status",
        "trusted_gate",
    }
    if (
        manifest.release_kind != LEGACY_CENSUS_RELEASE_KIND
        or manifest.schema_version != LEGACY_CENSUS_SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {LEGACY_CENSUS_FILENAME}
        or set(manifest.metadata) != expected_metadata
        or manifest.source_release_ids != (verified.source_snapshot_id,)
    ):
        raise IntegrityError("canonical legacy census release contract is invalid")
    path = boundary.active_root / receipt.relative_root / LEGACY_CENSUS_FILENAME
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("canonical legacy census release JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("canonical legacy census release is not canonical JSON")
    validated = validate_legacy_trial_census_payload(payload)
    if (
        manifest.metadata["census_sha256"] != validated["census_sha256"]
        or manifest.metadata["exact_count_state"] != validated["exact_count_state"]
        or manifest.metadata["source_evidence_sha256"]
        != validated["source_evidence_sha256"]
        or manifest.metadata["source_snapshot_id"] != verified.source_snapshot_id
        or manifest.metadata["status"] != validated["status"]
        or manifest.metadata["trusted_gate"] is not False
    ):
        raise IntegrityError("canonical legacy census release metadata was substituted")
    rebuilt = _derive_legacy_trial_census(verified, boundary=boundary)
    if rebuilt != validated:
        raise IntegrityError("canonical legacy census diverges from source evidence")
    return validated


def _cli_boundary(repository_root: Path, source_contract: Path) -> RepoBoundary:
    root = repository_root.resolve(strict=False)
    contract_path = source_contract.resolve(strict=False)
    expected_contract = (root / "configs" / "source_contract.json").resolve(
        strict=False
    )
    if contract_path != expected_contract:
        raise ContractError(
            "legacy census CLI requires the canonical configs/source_contract.json"
        )
    try:
        assert_plain_file(contract_path)
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError, ContractError) as exc:
        raise ContractError("source contract JSON is invalid") from exc
    if not isinstance(contract, dict):
        raise ContractError("source contract must be an object")
    active = contract.get("active_repository")
    legacy = contract.get("legacy_repository")
    provider = contract.get("provider")
    if (
        type(active) is not str
        or not active
        or type(legacy) is not str
        or not legacy
        or not isinstance(provider, dict)
        or provider.get("paid_calls_authorized") is not False
        or provider.get("downloads_authorized") is not False
        or contract.get("discovery_policy") != "manifest_only"
        or contract.get("recursive_fallbacks_allowed") is not False
        or contract.get("links_allowed") is not False
    ):
        raise ContractError("source contract does not preserve offline census safety")
    boundary = RepoBoundary(
        Path(active),
        legacy_roots=(Path(legacy),),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(root)
    boundary.assert_active_path(
        contract_path, purpose="legacy census source contract", subtree="configs"
    )
    return boundary


def _cli_summary(
    payload: Mapping[str, object],
    *,
    release_receipt: VerifiedReleaseReceipt | None,
) -> dict[str, object]:
    unresolved = payload.get("unresolved_references")
    if not isinstance(unresolved, list):
        raise IntegrityError("legacy census unresolved-reference summary is invalid")
    return {
        "census_sha256": payload["census_sha256"],
        "exact_count_state": payload["exact_count_state"],
        "historical_execution_authorized": False,
        "mode": (
            "PUBLISHED_UNRESOLVED_CENSUS"
            if release_receipt is not None
            else "READ_ONLY_ASSESSMENT"
        ),
        "observed_attempt_floor": payload["observed_attempt_floor"],
        "paid_provider_call_count": 0,
        "preregistered_penalty_count": payload["preregistered_penalty_count"],
        "published": release_receipt is not None,
        "real_history_trust_granted": False,
        "release_receipt": (
            release_receipt.as_dict() if release_receipt is not None else None
        ),
        "source_evidence_sha256": payload["source_evidence_sha256"],
        "source_snapshot_id": payload["source_snapshot_id"],
        "status": payload["status"],
        "trusted_gate": payload["trusted_gate"],
        "unresolved_reference_count": len(unresolved),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Assess or publish the immutable unresolved legacy-trial census "
            "without provider, real-history, alpha, candidate, or trust authority"
        )
    )
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--source-snapshot-root", type=Path, required=True)
    parser.add_argument(
        "--publish",
        action="store_true",
        help="atomically publish only the unresolved non-trusted census",
    )
    args = parser.parse_args(argv)

    boundary = _cli_boundary(args.repository_root, args.source_contract)
    snapshot = PublishedSourceSnapshot.open(
        args.source_snapshot_root, boundary=boundary
    )
    payload = _derive_legacy_trial_census(snapshot, boundary=boundary)
    if not args.publish:
        print(
            canonical_bytes(
                _cli_summary(payload, release_receipt=None)
            ).decode("utf-8")
        )
        return 0

    if (
        payload["status"] != UNRESOLVED_STATUS
        or payload["exact_count_state"] != INDETERMINATE_COUNT_STATE
        or payload["preregistered_penalty_count"] != 0
        or payload["trusted_gate"] is not False
    ):
        raise IntegrityError("legacy census CLI refuses a trusted or executable census")
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "census_sha256": str(payload["census_sha256"]),
            "exact_count_state": INDETERMINATE_COUNT_STATE,
            "historical_execution_authorized": "false",
            "preregistered_penalty_count": "0",
            "source_contract_sha256": sha256_file(args.source_contract),
            "source_snapshot_id": snapshot.source_snapshot_id,
            "status": UNRESOLVED_STATUS,
            "trusted_gate": "false",
        },
    )
    publisher = AtomicPublisher(
        boundary.active_root
        / "data"
        / "vault"
        / ".staging"
        / "releases"
        / "legacy_trial_census",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "legacy-trial-census.lock",
        boundary=boundary,
        operation_receipt=operation,
    )
    receipt = publish_legacy_trial_census(
        snapshot=snapshot, boundary=boundary, publisher=publisher
    )
    print(
        canonical_bytes(
            _cli_summary(payload, release_receipt=receipt)
        ).decode("utf-8")
    )
    return 0


__all__ = [
    "CORE_LEDGER_PATHS",
    "EXPERIMENT_LEDGER_PATH",
    "FEATURE_REGISTRY_PATH",
    "FEATURE_STATUSES_PATH",
    "INDETERMINATE_COUNT_STATE",
    "LEGACY_CENSUS_RELEASE_KIND",
    "LEGACY_CENSUS_SCHEMA_VERSION",
    "PRESCRIBED_EVIDENCE_FILE_COUNT",
    "TARGET_REGISTRY_PATH",
    "TARGET_STATUSES_PATH",
    "TERMINAL_DISTRIBUTIONAL_ALPHA_PATH",
    "TERMINAL_DISTRIBUTIONAL_AUDIT_PATH",
    "TERMINAL_DISTRIBUTIONAL_PATHS",
    "TERMINAL_DISTRIBUTIONAL_PROGRAM_ID",
    "TERMINAL_DISTRIBUTIONAL_WFA_PATH",
    "UNRESOLVED_STATUS",
    "load_legacy_trial_census",
    "main",
    "publish_legacy_trial_census",
    "validate_legacy_trial_census_payload",
]


if __name__ == "__main__":
    raise SystemExit(main())
