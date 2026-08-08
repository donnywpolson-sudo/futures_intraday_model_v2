"""Checkpoint-scoped source-integrity adapter for the V9 successor.

V6 through V9 treated every adjacent timestamp discontinuity inside an
exchange-session label as proof that the entire session was ambiguous.  The
registered checkpoint calendar does not define a complete expected-minute
grid, so that rule could not distinguish a missing required bar from a normal
or irrelevant absence elsewhere in the session.

V10 keeps every source disposition and never manufactures executability.  It
records adjacent timestamp discontinuities as diagnostics only.  Completeness
is decided where it can be proved: the inherited materializer requires an
exact, causal, minute-contiguous 61-bar feature window and an exact,
minute-contiguous post-decision entry/outcome path for each checkpoint.  A
missing whole session is still retained by the independent calendar census.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from .canonical import sha256_file
from .errors import IntegrityError
from . import tier1_bracket_v9 as v9
from .tier1_bracket_v5 import (
    NS_PER_MINUTE,
    REQUIRED_PARQUET_COLUMNS,
    TRADABLE_DISPOSITIONS,
    CensusCheckpoint,
    V5SourceRecord,
    _hex64,
    source_record_from_mapping,
)


V10_AUDIT_COLUMNS = frozenset(
    set(REQUIRED_PARQUET_COLUMNS)
    | {
        "failure_code",
        "failure_detail_sha256",
        "prediction_in_coverage_denominator",
    }
)
V9_TRIAL_ID = "fed4cc30c3f01e4f5b15eacfecdc50fe3a45bf671c0306d568f013f02c91dcd8"
V10_CONTRACT = Path("configs/tier1_bracket_successor_v10.json")


def load_v10_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    """Verify the prepared delta and return the unchanged V9 strategy contract."""

    try:
        delta = json.loads((root / V10_CONTRACT).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("invalid V10 contract JSON") from exc
    if not isinstance(delta, dict):
        raise IntegrityError("V10 contract is not an object")
    continuity = delta.get("source_continuity_successor")
    anti_tuning = delta.get("anti_tuning")
    authority = delta.get("authority")
    inherited_path = delta.get("inherited_v9_contract_path")
    inherited_hash = delta.get("inherited_v9_contract_sha256")
    if (
        delta.get("schema_version") != "tier1_bracket_successor_v10_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v9_trial_id") != V9_TRIAL_ID
        or inherited_path != "configs/tier1_bracket_successor_v9.json"
        or not _hex64(inherited_hash)
        or sha256_file(root / str(inherited_path)) != inherited_hash
        or not isinstance(continuity, dict)
        or continuity.get("adjacent_timestamp_discontinuity")
        != "DIAGNOSTIC_ONLY_NOT_PROOF_OF_A_MISSING_REQUIRED_BAR"
        or continuity.get("feature_completeness")
        != "EXACT_61_BAR_CAUSAL_MINUTE_CONTIGUOUS_CHECKPOINT_WINDOW"
        or continuity.get("missing_required_dependency")
        != "CHECKPOINT_SPECIFIC_ABSTENTION_NO_IMPUTATION_OR_SHORTENING"
        or not isinstance(anti_tuning, dict)
        or set(anti_tuning.values()) != {False}
        or not isinstance(authority, dict)
        or authority.get("publication_requires_separate_approval") is not True
        or authority.get("holdout_or_forward_access") is not False
        or authority.get("provider_access") is not False
        or authority.get("trading") is not False
    ):
        raise IntegrityError("V10 source-continuity contract is incomplete or drifted")
    inherited, _ = v9.load_v9_contract(root=root)
    return inherited, delta


@dataclass
class SourceIntegrityAuditV10:
    market: str
    total_rows: int = 0
    tradable_rows: int = 0
    nontradable_rows: int = 0
    sessionless_nontradable_rows: int = 0
    observed_adjacent_timestamp_discontinuities: int = 0
    sessions_with_observed_discontinuities: set[str] = field(default_factory=set)
    failure_codes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "total_rows": self.total_rows,
            "tradable_rows": self.tradable_rows,
            "nontradable_rows": self.nontradable_rows,
            "sessionless_nontradable_rows": self.sessionless_nontradable_rows,
            "observed_adjacent_timestamp_discontinuities": (
                self.observed_adjacent_timestamp_discontinuities
            ),
            "sessions_with_observed_discontinuities": sorted(
                self.sessions_with_observed_discontinuities
            ),
            "failure_codes": dict(sorted(self.failure_codes.items())),
        }


@dataclass(frozen=True)
class DependencyWindowCensusV10:
    expected_open_checkpoints: int
    missing_source_sessions: int
    ambiguous_source_sessions: int
    complete_feature_windows: int
    incomplete_feature_windows: int
    complete_execution_windows: int
    incomplete_execution_windows: int
    complete_both_windows: int

    def as_dict(self) -> dict[str, int]:
        return {
            "expected_open_checkpoints": self.expected_open_checkpoints,
            "missing_source_sessions": self.missing_source_sessions,
            "ambiguous_source_sessions": self.ambiguous_source_sessions,
            "complete_feature_windows": self.complete_feature_windows,
            "incomplete_feature_windows": self.incomplete_feature_windows,
            "complete_execution_windows": self.complete_execution_windows,
            "incomplete_execution_windows": self.incomplete_execution_windows,
            "complete_both_windows": self.complete_both_windows,
        }


def audit_checkpoint_dependencies_v10(
    *, source_rows: Sequence[V5SourceRecord], census: Sequence[CensusCheckpoint],
) -> DependencyWindowCensusV10:
    """Count exact checkpoint dependencies without fitting or predicting."""

    for row in source_rows:
        row.validate()
    grouped: dict[tuple[str, str], list[V5SourceRecord]] = {}
    for row in source_rows:
        grouped.setdefault((row.market, row.exchange_session_date), []).append(row)
    expected_open = missing_sessions = ambiguous_sessions = 0
    complete_feature = incomplete_feature = 0
    complete_execution = incomplete_execution = complete_both = 0
    for checkpoint in census:
        if not checkpoint.calendar_open:
            continue
        expected_open += 1
        expected = checkpoint.expected
        raw = grouped.get((expected.market, expected.exchange_session_date), [])
        if not raw:
            missing_sessions += 1
            incomplete_feature += 1
            incomplete_execution += 1
            continue
        event_values = [item.bar.event_at_ns for item in raw if item.bar is not None]
        if len(event_values) != len(set(event_values)):
            ambiguous_sessions += 1
            incomplete_feature += 1
            incomplete_execution += 1
            continue
        executable = sorted(
            (item for item in raw if item.executable and item.bar is not None),
            key=lambda item: item.bar.event_at_ns,  # type: ignore[union-attr]
        )
        causal = [
            item for item in executable
            if item.bar is not None
            and item.bar.available_at_ns <= expected.decision_at_ns
        ]
        feature_ok = False
        if causal:
            current = causal[-1]
            assert current.bar is not None
            required = {
                current.bar.event_at_ns - offset * NS_PER_MINUTE
                for offset in range(61)
            }
            window = [
                item for item in executable
                if item.bar is not None and item.bar.event_at_ns in required
            ]
            feature_ok = (
                len(window) == 61
                and {item.bar.event_at_ns for item in window if item.bar is not None}
                == required
                and len({item.actual_identity_hash for item in window}) == 1
                and all(
                    item.bar is not None
                    and item.bar.available_at_ns <= expected.decision_at_ns
                    for item in window
                )
            )
        required_path = {
            expected.decision_at_ns + offset * NS_PER_MINUTE
            for offset in range(1, 62)
        }
        observed_path = {
            item.bar.event_at_ns
            for item in executable
            if item.bar is not None and item.bar.event_at_ns in required_path
        }
        execution_ok = observed_path == required_path
        complete_feature += int(feature_ok)
        incomplete_feature += int(not feature_ok)
        complete_execution += int(execution_ok)
        incomplete_execution += int(not execution_ok)
        complete_both += int(feature_ok and execution_ok)
    return DependencyWindowCensusV10(
        expected_open, missing_sessions, ambiguous_sessions,
        complete_feature, incomplete_feature,
        complete_execution, incomplete_execution, complete_both,
    )


def _event(row: Mapping[str, object]) -> int:
    value = row.get("event_at_ns")
    if type(value) is not int:
        raise IntegrityError("V10 source row lacks an event identity")
    return value


def _record_failure(
    audit: SourceIntegrityAuditV10, row: Mapping[str, object],
) -> None:
    code = row.get("failure_code")
    if not isinstance(code, str) or not code:
        code = "MISSING_FAILURE_CODE"
    audit.failure_codes[code] = audit.failure_codes.get(code, 0) + 1


def _normalize_orphan(
    *, market: str, row: Mapping[str, object], session: str,
) -> V5SourceRecord:
    if row.get("disposition") in TRADABLE_DISPOSITIONS:
        raise IntegrityError("V10 cannot assign a session to a tradable orphan")
    if row.get("prediction_in_coverage_denominator") is not True:
        raise IntegrityError("V10 orphan is absent from the declared coverage universe")
    failure = row.get("failure_code")
    detail = row.get("failure_detail_sha256")
    if not isinstance(failure, str) or not failure or not _hex64(detail):
        raise IntegrityError("V10 orphan lacks fail-closed provenance")
    normalized = dict(row)
    normalized["exchange_session_date"] = session
    record = source_record_from_mapping(market=market, row=normalized)
    if record.executable:
        raise IntegrityError("V10 orphan normalization manufactured eligibility")
    return record


def normalize_source_mappings_v10(
    *, market: str, rows: Iterator[Mapping[str, object]],
    audit: SourceIntegrityAuditV10,
) -> Iterator[V5SourceRecord]:
    """Retain source rows while deferring completeness to exact dependencies."""

    if audit.market != market:
        raise IntegrityError("V10 source audit market does not match the stream")
    previous: V5SourceRecord | None = None
    pending_orphans: list[Mapping[str, object]] = []
    for row in rows:
        audit.total_rows += 1
        disposition = row.get("disposition")
        tradable = disposition in TRADABLE_DISPOSITIONS
        if tradable:
            audit.tradable_rows += 1
        else:
            audit.nontradable_rows += 1
            _record_failure(audit, row)
        session = row.get("exchange_session_date")
        if not isinstance(session, str):
            if tradable:
                raise IntegrityError("tradable V10 source row lacks a session identity")
            if not _hex64(row.get("source_row_sha256")):
                raise IntegrityError("sessionless V10 source row lacks a source identity")
            audit.sessionless_nontradable_rows += 1
            pending_orphans.append(dict(row))
            continue

        current = source_record_from_mapping(market=market, row=row)
        bridged_orphans = False
        if pending_orphans:
            if previous is None or previous.exchange_session_date != current.exchange_session_date:
                raise IntegrityError("V10 cannot unambiguously locate a sessionless source defect")
            expected_event = (
                previous.bar.event_at_ns + NS_PER_MINUTE
                if previous.bar is not None else None
            )
            for orphan in pending_orphans:
                if expected_event is None or _event(orphan) != expected_event:
                    raise IntegrityError("V10 sessionless source defect is not minute-contiguous")
                yield _normalize_orphan(
                    market=market, row=orphan,
                    session=current.exchange_session_date,
                )
                expected_event += NS_PER_MINUTE
            if current.bar is None or current.bar.event_at_ns != expected_event:
                raise IntegrityError("V10 source defect lacks matching causal neighbors")
            pending_orphans.clear()
            bridged_orphans = True

        if (
            not bridged_orphans
            and previous is not None
            and previous.exchange_session_date == current.exchange_session_date
            and previous.bar is not None
            and current.bar is not None
            and current.bar.event_at_ns - previous.bar.event_at_ns != NS_PER_MINUTE
        ):
            audit.observed_adjacent_timestamp_discontinuities += 1
            audit.sessions_with_observed_discontinuities.add(
                current.exchange_session_date
            )
        yield current
        previous = current
    if pending_orphans:
        raise IntegrityError("V10 source stream ends with an unresolved sessionless defect")


def iter_source_records_from_parquet_v10(
    *, market: str, path: Path, audit: SourceIntegrityAuditV10,
    batch_size: int = 65_536,
) -> Iterator[V5SourceRecord]:
    """Batch-stream one source without loading the full Parquet file."""

    if audit.market != market or batch_size < 1 or batch_size > 65_536:
        raise IntegrityError("V10 parquet stream request is outside its bounds")
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    if not V10_AUDIT_COLUMNS.issubset(parquet.schema_arrow.names):
        raise IntegrityError("V10 source schema lacks audit columns")

    def mappings() -> Iterator[Mapping[str, object]]:
        for batch in parquet.iter_batches(
            batch_size=batch_size, columns=sorted(V10_AUDIT_COLUMNS),
        ):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                yield {name: values[index] for name, values in columns.items()}

    yield from normalize_source_mappings_v10(
        market=market, rows=mappings(), audit=audit,
    )
