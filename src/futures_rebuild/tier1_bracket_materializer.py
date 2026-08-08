"""Verified-source to bracket-row conversion, with no repository I/O.

The high-risk runner will be a thin caller of this module.  Keeping conversion
pure makes the actual release boundary small: it must validate source hashes,
provide only the approved 20 market-years, and persist the returned rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from decimal import Decimal, InvalidOperation
import os
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .economics import VerifiedEconomicsRegistry
from .tier1_bracket_directional import DirectionalBracketRows, materialize_directional_row
from .tier1_bracket_checkpoint import append_chunk, finalize_checkpoint, load_checkpoint
from .tier1_bracket_trial import BracketBar
from .tier1_bracket_interval_resolver import classify_source_disposition


PRICE_NANO_SCALE = Decimal("1000000000")
STREAM_WINDOW_ROWS = 81
STREAM_CARRY_ROWS = STREAM_WINDOW_ROWS - 1
DEFAULT_CHUNK_ROWS = 50_000


@dataclass(frozen=True)
class IndexedBracketEconomics:
    """The exact Phase 8-authorized economics for one actual identity."""

    actual_identity_hash: str
    tick_size: Decimal
    tick_value: Decimal
    point_value: Decimal
    currency: str
    quote_convention_id: str
    economics_release_receipt_id: str

    def validate(self) -> None:
        if (
            not isinstance(self.actual_identity_hash, str)
            or len(self.actual_identity_hash) != 64
            or any(not value.is_finite() or value <= 0 for value in (self.tick_size, self.tick_value, self.point_value))
            or self.tick_size * self.point_value != self.tick_value
            or self.currency != "USD"
            or not self.quote_convention_id
            or not isinstance(self.economics_release_receipt_id, str)
        ):
            raise IntegrityError("indexed bracket economics are invalid")


def indexed_bracket_economics_from_registry(
    registry: VerifiedEconomicsRegistry,
) -> dict[str, IndexedBracketEconomics]:
    """Adapt only a previously verified Phase 8 registry for bracket labels."""

    result = {
        identity: IndexedBracketEconomics(
            actual_identity_hash=record.actual_identity_hash,
            tick_size=record.tick_size,
            tick_value=record.tick_value,
            point_value=record.point_value,
            currency=record.currency,
            quote_convention_id=record.quote_convention_id,
            economics_release_receipt_id=record.economics_release_receipt_id,
        )
        for identity, record in registry.records.items()
    }
    if not result or set(result) != set(registry.records):
        raise IntegrityError("Phase 8 economics registry cannot provide bracket identity coverage")
    for identity, item in result.items():
        if item.actual_identity_hash != identity:
            raise IntegrityError("Phase 8 economics registry identity is inconsistent")
        item.validate()
    return result


def _required_string(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value:
        raise IntegrityError(f"bracket source row lacks {name}")
    return value


def _required_int(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if type(value) is not int:
        raise IntegrityError(f"bracket source row lacks integer {name}")
    return value


def _decimal(row: Mapping[str, object], name: str) -> Decimal:
    value = row.get(name)
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError(f"bracket source row has invalid {name}") from exc
    if not result.is_finite() or result <= 0:
        raise IntegrityError(f"bracket source row has invalid {name}")
    return result


@dataclass(frozen=True)
class VerifiedSourceBar:
    """Only point-in-time fields permitted to drive a bracket decision."""

    bar: BracketBar
    volume: float
    source_row_sha256: str
    tick_size_nano: int
    tick_value_usd: Decimal

    @classmethod
    def from_mapping(
        cls, row: Mapping[str, object], *, indexed_economics: Mapping[str, IndexedBracketEconomics]
    ) -> "VerifiedSourceBar":
        try:
            volume = float(row.get("volume"))
        except (TypeError, ValueError) as exc:
            raise IntegrityError("bracket source row has invalid volume") from exc
        source_hash = _required_string(row, "source_row_sha256")
        eligible = classify_source_disposition(row.get("disposition"))
        raw_identity = row.get("actual_identity_hash")
        if isinstance(raw_identity, str) and raw_identity:
            identity = raw_identity
        elif eligible:
            raise IntegrityError("bracket source row lacks actual_identity_hash")
        else:
            # An explicitly non-tradable source row has no actual contract to
            # resolve.  Keep it as a deterministic abstention without making
            # a sentinel usable as an economics lookup key.
            identity = sha256_json({"non_tradable_source_row": source_hash})
        economics = indexed_economics.get(identity)
        if economics is None:
            if eligible:
                raise IntegrityError("bracket source identity has no indexed Phase 8 economics")
            # An explicitly non-tradable row is preserved as an abstention.  It
            # cannot calculate a bracket, so sentinel tick fields are never an
            # economics fallback or a usable execution input.
            tick_size_nano, tick_value_usd = 1, Decimal("1")
        else:
            economics.validate()
            for field, expected in (("tick_size", economics.tick_size), ("tick_value", economics.tick_value), ("point_value", economics.point_value)):
                observed = row.get(field)
                if observed is not None and str(observed) not in {"", "None"} and _decimal(row, field) != expected:
                    raise IntegrityError("bracket source economics disagree with the indexed Phase 8 record")
            source_currency = row.get("currency")
            if source_currency is not None and str(source_currency) not in {"", "None"} and source_currency != economics.currency:
                raise IntegrityError("bracket source currency disagrees with indexed Phase 8 economics")
            tick_size_nano = int(economics.tick_size * PRICE_NANO_SCALE)
            tick_value_usd = economics.tick_value
        if tick_size_nano <= 0:
            raise IntegrityError("bracket source row tick size cannot map to nano price")
        raw_session = row.get("exchange_session_date")
        if isinstance(raw_session, str) and raw_session:
            session = raw_session
        elif eligible:
            raise IntegrityError("bracket source row lacks exchange_session_date")
        else:
            session = "UNRESOLVED_SESSION"
        return cls(
            bar=BracketBar(
                event_at_ns=_required_int(row, "event_at_ns"),
                open_nano=_required_int(row, "open_nano"),
                high_nano=_required_int(row, "high_nano"),
                low_nano=_required_int(row, "low_nano"),
                close_nano=_required_int(row, "close_nano"),
                session=session,
                actual_identity_hash=identity,
                eligible=eligible,
            ),
            volume=volume,
            source_row_sha256=source_hash,
            tick_size_nano=tick_size_nano,
            tick_value_usd=tick_value_usd,
        )


@dataclass(frozen=True)
class MaterializedDirectionalRow:
    """Fresh outputs that retain exact source and economics identity."""

    source_row_sha256: str
    actual_identity_hash: str
    exchange_session_date: str
    decision_at_ns: int
    label_unlock_at_ns: int
    row: DirectionalBracketRows

    def feature_record(self) -> dict[str, object]:
        return {
            "status": "FEATURE_READY" if self.row.long_net_r is not None and self.row.short_net_r is not None else "ABSTAINED",
            "actual_identity_hash": self.actual_identity_hash,
            "exchange_session_date": self.exchange_session_date,
            "decision_at_ns": self.decision_at_ns,
            "label_unlock_at_ns": self.label_unlock_at_ns,
            "upstream_source_row_sha256": self.source_row_sha256,
            **self.row.features,
        }

    def outcome_record(self) -> dict[str, object]:
        return {
            "status": "MATURED" if self.row.long_net_r is not None and self.row.short_net_r is not None else "ABSTAINED",
            "actual_identity_hash": self.actual_identity_hash,
            "exchange_session_date": self.exchange_session_date,
            "decision_at_ns": self.decision_at_ns,
            "label_unlock_at_ns": self.label_unlock_at_ns,
            "upstream_source_row_sha256": self.source_row_sha256,
            "long_realized_net_r": None if self.row.long_net_r is None else str(self.row.long_net_r),
            "short_realized_net_r": None if self.row.short_net_r is None else str(self.row.short_net_r),
            "long_planned_all_in_risk_usd": None if self.row.long_planned_all_in_risk_usd is None else str(self.row.long_planned_all_in_risk_usd),
            "short_planned_all_in_risk_usd": None if self.row.short_planned_all_in_risk_usd is None else str(self.row.short_planned_all_in_risk_usd),
            "long_realized_gross_pnl_usd": None if self.row.long_realized_gross_pnl_usd is None else str(self.row.long_realized_gross_pnl_usd),
            "short_realized_gross_pnl_usd": None if self.row.short_realized_gross_pnl_usd is None else str(self.row.short_realized_gross_pnl_usd),
            "long_exit_at_ns": self.row.long_exit_at_ns,
            "short_exit_at_ns": self.row.short_exit_at_ns,
            "long_triple_barrier_class": self.row.long_diagnostic,
            "short_triple_barrier_class": self.row.short_diagnostic,
            "long_exit_reason": self.row.long_exit_reason,
            "short_exit_reason": self.row.short_exit_reason,
        }



def materialize_verified_source_rows(
    *, rows: Sequence[Mapping[str, object]], stress_round_trip_cost_usd: Decimal,
    indexed_economics: Mapping[str, IndexedBracketEconomics],
) -> tuple[MaterializedDirectionalRow, ...]:
    """Create every fresh directional row from one verified market-year path.

    The caller must have verified the one input payload hash and restricted the
    input to a single approved market-year.  Every decision retains its source
    hash and may only look at the next 60 bars through the locked mechanics.
    """

    sources = tuple(VerifiedSourceBar.from_mapping(row, indexed_economics=indexed_economics) for row in rows)
    if len(sources) < 22:
        raise IntegrityError("bracket materialization requires enough verified source bars")
    if len({item.source_row_sha256 for item in sources}) != len(sources):
        raise IntegrityError("bracket materialization source row hashes are ambiguous")
    bars = tuple(item.bar for item in sources)
    output: list[MaterializedDirectionalRow] = []
    for index, source in enumerate(sources):
        # The mechanic returns a clear abstention for early or incomplete paths;
        # preserve it rather than inventing a sample.
        converted = materialize_directional_row(
            bars=bars, decision_index=index, tick_size_nano=source.tick_size_nano,
            tick_value_usd=source.tick_value_usd, stress_round_trip_cost_usd=stress_round_trip_cost_usd,
            volume=source.volume,
        )
        output.append(MaterializedDirectionalRow(
            source_row_sha256=source.source_row_sha256,
            actual_identity_hash=source.bar.actual_identity_hash,
            exchange_session_date=source.bar.session,
            decision_at_ns=source.bar.event_at_ns,
            label_unlock_at_ns=converted.label_unlock_at_ns,
            row=converted,
        ))
    return tuple(output)


def stream_materialize_verified_source_batches(
    *, batches: Sequence[Sequence[Mapping[str, object]]], stress_round_trip_cost_usd: Decimal,
    indexed_economics: Mapping[str, IndexedBracketEconomics],
) -> tuple[MaterializedDirectionalRow, ...]:
    """Materialize ordered batches with an 81-bar causal/look-ahead window.

    This pure helper keeps 80 trailing bars across batch boundaries.  Leading
    and trailing rows are retained as explicit abstentions by the existing
    mechanics; no partial future window is treated as a matured label.
    """

    pending: deque[Mapping[str, object]] = deque()
    output: list[MaterializedDirectionalRow] = []
    previous_time = -1
    hashes: set[str] = set()
    for batch in batches:
        for row in batch:
            event = _required_int(row, "event_at_ns")
            source_hash = _required_string(row, "source_row_sha256")
            if event <= previous_time or source_hash in hashes:
                raise IntegrityError("bracket streamed source order or hash is invalid")
            previous_time, hashes = event, hashes | {source_hash}
            pending.append(row)
            while len(pending) >= 81:
                window = tuple(pending)
                decision = materialize_verified_source_rows(
                    rows=window, stress_round_trip_cost_usd=stress_round_trip_cost_usd,
                    indexed_economics=indexed_economics,
                )[20]
                output.append(decision)
                pending.popleft()
    return tuple(output)


def _abstained_row(*, source: VerifiedSourceBar, reason: str) -> MaterializedDirectionalRow:
    features = {
        **{
            "bar_body_fraction": (source.bar.close_nano - source.bar.open_nano) / float(source.bar.open_nano),
            "bar_return": source.bar.close_nano / float(source.bar.open_nano) - 1.0,
            "intrabar_range_fraction": (source.bar.high_nano - source.bar.low_nano) / float(source.bar.open_nano),
        },
        "volume": source.volume,
    }
    row = DirectionalBracketRows(
        decision_at_ns=source.bar.event_at_ns,
        label_unlock_at_ns=source.bar.event_at_ns + 60 * 60_000_000_000,
        features=features,
        long_net_r=None, short_net_r=None,
        long_planned_all_in_risk_usd=None, short_planned_all_in_risk_usd=None,
        long_realized_gross_pnl_usd=None, short_realized_gross_pnl_usd=None,
        long_exit_at_ns=None, short_exit_at_ns=None,
        long_diagnostic="UNAVAILABLE", short_diagnostic="UNAVAILABLE",
        long_exit_reason=reason, short_exit_reason=reason,
    )
    return MaterializedDirectionalRow(
        source_row_sha256=source.source_row_sha256,
        actual_identity_hash=source.bar.actual_identity_hash,
        exchange_session_date=source.bar.session,
        decision_at_ns=source.bar.event_at_ns,
        label_unlock_at_ns=row.label_unlock_at_ns,
        row=row,
    )


def _window_decision(
    *, rows: Sequence[Mapping[str, object]], stress_round_trip_cost_usd: Decimal,
    indexed_economics: Mapping[str, IndexedBracketEconomics],
) -> MaterializedDirectionalRow:
    if len(rows) != STREAM_WINDOW_ROWS:
        raise IntegrityError("bracket streamed decision window is incomplete")
    sources = tuple(VerifiedSourceBar.from_mapping(row, indexed_economics=indexed_economics) for row in rows)
    decision = sources[20]
    converted = materialize_directional_row(
        bars=tuple(item.bar for item in sources), decision_index=20,
        tick_size_nano=decision.tick_size_nano, tick_value_usd=decision.tick_value_usd,
        stress_round_trip_cost_usd=stress_round_trip_cost_usd, volume=decision.volume,
    )
    return MaterializedDirectionalRow(
        source_row_sha256=decision.source_row_sha256,
        actual_identity_hash=decision.bar.actual_identity_hash,
        exchange_session_date=decision.bar.session,
        decision_at_ns=decision.bar.event_at_ns,
        label_unlock_at_ns=converted.label_unlock_at_ns,
        row=converted,
    )


def _chunk_tables(rows: Sequence[MaterializedDirectionalRow]):
    """Use fixed schemas so every chunk can be concatenated safely."""

    try:
        import pyarrow as pa
    except Exception as exc:  # pragma: no cover - environment boundary
        raise IntegrityError("bracket chunk writer requires pyarrow") from exc
    common = [
        ("status", pa.string()), ("actual_identity_hash", pa.string()),
        ("exchange_session_date", pa.string()), ("decision_at_ns", pa.int64()),
        ("label_unlock_at_ns", pa.int64()), ("upstream_source_row_sha256", pa.string()),
    ]
    feature_schema = pa.schema(common + [
        ("bar_body_fraction", pa.float64()), ("bar_return", pa.float64()),
        ("intrabar_range_fraction", pa.float64()), ("volume", pa.float64()),
    ])
    outcome_schema = pa.schema(common + [
        ("long_realized_net_r", pa.string()), ("short_realized_net_r", pa.string()),
        ("long_planned_all_in_risk_usd", pa.string()), ("short_planned_all_in_risk_usd", pa.string()),
        ("long_realized_gross_pnl_usd", pa.string()), ("short_realized_gross_pnl_usd", pa.string()),
        ("long_exit_at_ns", pa.int64()), ("short_exit_at_ns", pa.int64()),
        ("long_triple_barrier_class", pa.string()), ("short_triple_barrier_class", pa.string()),
        ("long_exit_reason", pa.string()), ("short_exit_reason", pa.string()),
    ])
    return (
        pa.Table.from_pylist([item.feature_record() for item in rows], schema=feature_schema),
        pa.Table.from_pylist([item.outcome_record() for item in rows], schema=outcome_schema),
    )


def write_streamed_bracket_chunks(
    *, batches: Iterable[Sequence[Mapping[str, object]]], stress_round_trip_cost_usd: Decimal,
    indexed_economics: Mapping[str, IndexedBracketEconomics], stage: Path, checkpoint: Path,
    root: Path, context: Mapping[str, str], chunk_rows: int = DEFAULT_CHUNK_ROWS,
    stop_after_chunks: int | None = None,
) -> dict[str, object]:
    """Write verified bracket rows as resumable, hash-bound local chunks.

    This is deliberately a local staging primitive.  It opens no repository
    release by itself and returns only checkpoint metadata, never a frozen
    research artifact.
    """

    if type(chunk_rows) is not int or chunk_rows <= 0:
        raise IntegrityError("bracket chunk size is invalid")
    if stop_after_chunks is not None and (type(stop_after_chunks) is not int or stop_after_chunks <= 0):
        raise IntegrityError("bracket chunk stop limit is invalid")
    loaded = load_checkpoint(path=checkpoint, context=context, root=root)
    if loaded is not None and loaded["complete"]:
        return loaded
    stage.mkdir(parents=True, exist_ok=True)
    chunk_directory = stage / "chunks"
    chunk_directory.mkdir(parents=True, exist_ok=True)
    pending: deque[Mapping[str, object]] = deque() if loaded is None else deque(loaded["carry_rows"])
    input_rows = 0 if loaded is None else int(loaded["input_rows"])
    output_rows = 0 if loaded is None else int(loaded["output_rows"])
    identities = set() if loaded is None else set(loaded["resolved_identity_hashes"])
    cursor = None if loaded is None else loaded["cursor"]
    sequence = 0 if loaded is None else len(loaded["chunks"])
    emitted: list[MaterializedDirectionalRow] = []
    previous_time = -1 if cursor is None else int(cursor["event_at_ns"])
    skip = input_rows
    stopped = False

    def flush() -> None:
        nonlocal sequence, output_rows, emitted, loaded
        if not emitted:
            return
        try:
            import pyarrow.parquet as pq
            feature_table, outcome_table = _chunk_tables(emitted)
            feature_final = chunk_directory / f"{sequence:06d}.features.parquet"
            outcome_final = chunk_directory / f"{sequence:06d}.outcomes.parquet"
            if feature_final.exists() or outcome_final.exists():
                raise IntegrityError("bracket chunk path already exists")
            feature_temp = feature_final.with_suffix(feature_final.suffix + ".tmp")
            outcome_temp = outcome_final.with_suffix(outcome_final.suffix + ".tmp")
            pq.write_table(feature_table, feature_temp, compression="zstd")
            pq.write_table(outcome_table, outcome_temp, compression="zstd")
            feature_hash, outcome_hash = sha256_file(feature_temp), sha256_file(outcome_temp)
            os.replace(feature_temp, feature_final)
            os.replace(outcome_temp, outcome_final)
        except IntegrityError:
            raise
        except Exception as exc:  # pragma: no cover - pyarrow exception details vary
            raise IntegrityError("bracket chunk payload write failed") from exc
        chunk = {
            "sequence": sequence,
            "row_count": len(emitted),
            "feature_payload": feature_final.relative_to(root).as_posix(),
            "feature_payload_sha256": feature_hash,
            "outcome_payload": outcome_final.relative_to(root).as_posix(),
            "outcome_payload_sha256": outcome_hash,
            "first_source_row_sha256": emitted[0].source_row_sha256,
            "last_source_row_sha256": emitted[-1].source_row_sha256,
            "first_decision_at_ns": emitted[0].decision_at_ns,
            "last_decision_at_ns": emitted[-1].decision_at_ns,
        }
        if cursor is None:
            raise IntegrityError("bracket chunk lacks an input cursor")
        loaded = append_chunk(
            path=checkpoint, root=root, context=context, chunk=chunk,
            input_rows=input_rows, cursor=cursor, carry_rows=tuple(pending),
            resolved_identity_hashes=sorted(identities),
        )
        output_rows += len(emitted)
        sequence += 1
        emitted = []

    for batch in batches:
        for row in batch:
            if skip:
                skip -= 1
                continue
            event = _required_int(row, "event_at_ns")
            source_hash = _required_string(row, "source_row_sha256")
            if event <= previous_time:
                raise IntegrityError("bracket streamed source order is invalid")
            previous_time = event
            source = VerifiedSourceBar.from_mapping(row, indexed_economics=indexed_economics)
            pending.append(dict(row))
            input_rows += 1
            cursor = {"ordinal": input_rows - 1, "event_at_ns": event, "source_row_sha256": source_hash}
            identities.add(source.bar.actual_identity_hash)
            if input_rows <= 20:
                emitted.append(_abstained_row(source=source, reason="INSUFFICIENT_CAUSAL_HISTORY"))
            while len(pending) >= STREAM_WINDOW_ROWS:
                decision = _window_decision(
                    rows=tuple(pending), stress_round_trip_cost_usd=stress_round_trip_cost_usd,
                    indexed_economics=indexed_economics,
                )
                emitted.append(decision)
                pending.popleft()
            if len(pending) > STREAM_CARRY_ROWS:
                raise IntegrityError("bracket writer retained too many carry rows")
            if len(emitted) >= chunk_rows:
                flush()
                if stop_after_chunks is not None and sequence >= stop_after_chunks:
                    stopped = True
                    break
        if stopped:
            break
    if skip:
        raise IntegrityError("bracket checkpoint cursor exceeds the supplied source stream")
    if stopped:
        return loaded if loaded is not None else {"schema_version": "tier1_bracket_checkpoint/2.0.0", "complete": False}

    # The remaining first 20 bars are history already emitted.  Everything
    # after that is an explicit incomplete-horizon abstention at end-of-file.
    for row in tuple(pending)[20:]:
        emitted.append(_abstained_row(
            source=VerifiedSourceBar.from_mapping(row, indexed_economics=indexed_economics),
            reason="INSUFFICIENT_FUTURE_HORIZON",
        ))
        if len(emitted) >= chunk_rows:
            flush()
    pending.clear()
    flush()
    return finalize_checkpoint(
        path=checkpoint, root=root, context=context, input_rows=input_rows,
        cursor=cursor, carry_rows=(), resolved_identity_hashes=sorted(identities),
    )


def write_bracket_market_year_stage(
    *, rows: Sequence[Mapping[str, object]], stress_round_trip_cost_usd: Decimal, stage: Path,
    indexed_economics: Mapping[str, IndexedBracketEconomics],
) -> dict[str, object]:
    """Write fresh bracket-only payloads into a caller-owned staging directory.

    This does not select a source file, create a manifest, or promote anything
    immutable.  Those responsibilities stay at the approved orchestration
    boundary.  It is intentionally useful for synthetic tests and for a later
    verified, per-market-year checkpoint writer.
    """

    if stage.exists():
        raise IntegrityError("bracket staging target must not already exist")
    materialized = materialize_verified_source_rows(
        rows=rows, stress_round_trip_cost_usd=stress_round_trip_cost_usd, indexed_economics=indexed_economics,
    )
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        stage.mkdir(parents=True)
        feature_path = stage / "features.parquet"
        outcome_path = stage / "outcomes.parquet"
        pq.write_table(pa.Table.from_pylist([item.feature_record() for item in materialized]), feature_path, compression="zstd")
        pq.write_table(pa.Table.from_pylist([item.outcome_record() for item in materialized]), outcome_path, compression="zstd")
    except Exception as exc:
        raise IntegrityError("bracket staging payload write failed") from exc
    return {
        "row_count": len(materialized),
        "feature_payload": feature_path,
        "outcome_payload": outcome_path,
        "matured_pair_count": sum(item.row.long_net_r is not None and item.row.short_net_r is not None for item in materialized),
    }
