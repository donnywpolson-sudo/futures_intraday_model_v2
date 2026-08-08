"""Guarded, local-release adapter for the first real Phase 8 evaluation.

The public surface deliberately prepares and validates metadata only.  Opening
prediction, outcome, or bar payload rows is an orchestration action after a
plain-language Codex approval; no command-line runner exists here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from math import sqrt
from pathlib import Path, PurePosixPath
from typing import Iterable, Mapping

from .canonical import sha256_file
from .errors import IntegrityError, UnauthorizedOperation
from .tier1_phase8_evaluator import Phase8SyntheticTrade


_PHASE6_SCHEMA = "tier1_phase6_prediction_release/1.0.0"
_READ_SCOPE_SECRET = object()
_NS_PER_DAY = 86_400 * 1_000_000_000


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"Phase 8 adapter cannot read {path.name}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Phase 8 adapter metadata must be an object")
    return value


def _decimal(value: object, *, name: str, positive: bool = False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError(f"Phase 8 {name} is not decimal") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise IntegrityError(f"Phase 8 {name} is invalid")
    return result


@dataclass(frozen=True)
class PinnedPhase8Inputs:
    """Metadata-only binding for one frozen prediction release and its inputs."""

    prediction_release_id: str
    prediction_payload: Path
    trial_id: str
    input_pairs: tuple[dict[str, object], ...]
    feature_payloads: tuple[Path, ...]
    outcome_payloads: tuple[Path, ...]
    source_bars: tuple["PinnedSourceBars", ...]


@dataclass(frozen=True)
class PinnedSourceBars:
    """One exact local causal source-bar payload, bound without opening rows."""

    market: str
    year: int
    parquet_path: Path
    parquet_sha256: str
    sidecar_path: Path


@dataclass(frozen=True)
class _ApprovedCodexRealRead:
    """Opaque marker minted only by the approved-task orchestration seam."""

    _secret: object


@dataclass(frozen=True)
class SourceBarConversion:
    """Pure conversion result for already-opened, pinned local rows.

    The converter deliberately receives rows from its caller.  It does not
    choose a release, open a Parquet file, or provide a route around the Codex
    real-read gate.
    """

    execution_rows: tuple[Mapping[str, object], ...]
    excluded_roll_count: int
    fold_local_fallback_keys: tuple[tuple[str, int, int], ...] = ()


@dataclass(frozen=True)
class FoldLocalDirections:
    """Training-only fold-local directions and explicit coarser fallbacks."""

    directions: Mapping[tuple[str, int, int], int]
    fallback_keys: frozenset[tuple[str, int, int]]


@dataclass(frozen=True)
class ScheduledExecutionRows:
    """One-contract chronological selection before risk and cost simulation."""

    execution_rows: tuple[Mapping[str, object], ...]
    candidate_count: int
    simultaneous_selection_abstentions: int
    position_overlap_abstentions: int


@dataclass(frozen=True)
class OpenedPhase8Rows:
    """Rows opened only by approved Codex orchestration."""

    predictions: tuple[Mapping[str, object], ...]
    features: tuple[Mapping[str, object], ...]
    outcomes: tuple[Mapping[str, object], ...]
    source_bars: tuple[Mapping[str, object], ...]


def pin_phase8_prediction_release(*, root: Path, prediction_release_id: str, trial_id: str) -> PinnedPhase8Inputs:
    """Verify a frozen Phase 6 manifest without opening its Parquet payload."""

    if len(prediction_release_id) != 64 or any(char not in "0123456789abcdef" for char in prediction_release_id):
        raise IntegrityError("Phase 8 prediction release ID is invalid")
    manifest_path = root / "manifests" / "data_releases" / "predictions" / f"{prediction_release_id}.json"
    manifest = _object(manifest_path)
    if (
        manifest.get("schema_version") != _PHASE6_SCHEMA
        or manifest.get("release_id") != prediction_release_id
        or manifest.get("trial_id") != trial_id
        or manifest.get("prediction_only") is not True
    ):
        raise IntegrityError("Phase 8 requires the frozen output of the registered Phase 6 trial")
    pairs = manifest.get("input_pairs")
    payload = manifest.get("payload")
    payload_hash = manifest.get("payload_sha256")
    if not isinstance(pairs, list) or len(pairs) != 20 or not isinstance(payload, str) or not isinstance(payload_hash, str):
        raise IntegrityError("Phase 6 prediction manifest is incomplete")
    payload_path = (root / payload).resolve()
    try:
        payload_path.relative_to(root.resolve())
    except ValueError as exc:
        raise IntegrityError("Phase 6 prediction payload escapes the repository") from exc
    # Hashing preserves the immutable binding but never opens market rows.
    if not payload_path.is_file() or sha256_file(payload_path) != payload_hash:
        raise IntegrityError("Phase 6 prediction payload hash does not match its manifest")
    pinned_pairs = tuple(dict(pair) for pair in pairs if isinstance(pair, dict))
    if len(pinned_pairs) != 20:
        raise IntegrityError("Phase 6 prediction manifest has invalid input pairs")
    feature_payloads = []
    outcome_payloads = []
    source_bars = []
    for pair in pinned_pairs:
        market, year = pair.get("market"), pair.get("year")
        feature_id, outcome_id = pair.get("feature_release_id"), pair.get("outcome_release_id")
        if not isinstance(market, str) or type(year) is not int or not isinstance(feature_id, str) or not isinstance(outcome_id, str):
            raise IntegrityError("Phase 6 prediction input pair is invalid")
        feature_payloads.append(_pin_pair_payload(root, "features", feature_id, "features.parquet"))
        outcome_payloads.append(_pin_pair_payload(root, "outcomes", outcome_id, "outcomes.parquet"))
        source_bars.append(_pin_source_bars(root=root, market=market, year=year, expected_sha256=pair.get("source_parquet_sha256")))
    return PinnedPhase8Inputs(
        prediction_release_id=prediction_release_id,
        prediction_payload=payload_path,
        trial_id=trial_id,
        input_pairs=pinned_pairs,
        feature_payloads=tuple(feature_payloads),
        outcome_payloads=tuple(outcome_payloads),
        source_bars=tuple(source_bars),
    )


def _pin_source_bars(*, root: Path, market: str, year: int, expected_sha256: object) -> PinnedSourceBars:
    """Bind a source-bar payload through its exact local causal sidecar only."""

    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise IntegrityError("Phase 8 source-bar hash is invalid")
    relative = Path("data") / "active" / "causally_gated_normalized" / market / str(year) / f"{year}.parquet"
    sidecar = root / relative.with_suffix(".parquet.manifest.json")
    payload = _object(sidecar)
    entry = payload.get("entry_binding")
    if not isinstance(entry, dict) or entry.get("market") != market or entry.get("year") != year:
        raise IntegrityError("Phase 8 source-bar sidecar scope is invalid")
    if entry.get("parquet_path") != relative.as_posix() or entry.get("parquet_sha256") != expected_sha256:
        raise IntegrityError("Phase 8 source-bar sidecar differs from its frozen input pair")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise IntegrityError("Phase 8 source-bar payload escapes the repository") from exc
    if not path.is_file():
        raise IntegrityError("Phase 8 source-bar payload is missing")
    return PinnedSourceBars(market, year, path, expected_sha256, sidecar)


def _pin_pair_payload(root: Path, family: str, release_id: str, filename: str) -> Path:
    """Resolve one exact feature/outcome payload and verify its immutable bytes."""

    if len(release_id) != 64 or any(char not in "0123456789abcdef" for char in release_id):
        raise IntegrityError(f"Phase 8 {family} release ID is invalid")
    manifest = _object(root / "manifests" / "data_releases" / family / f"{release_id}.json")
    files = manifest.get("files")
    if not isinstance(files, list):
        raise IntegrityError(f"Phase 8 {family} manifest is incomplete")
    matches = [item for item in files if isinstance(item, dict) and str(item.get("logical_path", "")).endswith(f"/{filename}")]
    if len(matches) != 1 or not isinstance(matches[0].get("logical_path"), str) or not isinstance(matches[0].get("sha256"), str):
        raise IntegrityError(f"Phase 8 {family} manifest has no unique payload")
    logical_path = PurePosixPath(str(matches[0]["logical_path"]))
    if logical_path.is_absolute() or ".." in logical_path.parts or logical_path.name != filename:
        raise IntegrityError(f"Phase 8 {family} logical payload path is invalid")
    # Manifests name the stable logical location.  Immutable payload bytes live
    # beneath an additional release-ID directory and must never be discovered
    # by a latest-file search.
    path = (root / Path(*logical_path.parent.parts) / release_id / filename).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise IntegrityError(f"Phase 8 {family} payload escapes the repository") from exc
    if not path.is_file() or sha256_file(path) != matches[0]["sha256"]:
        raise IntegrityError(f"Phase 8 {family} payload hash does not match its manifest")
    return path


def read_pinned_phase8_rows(*, pinned: PinnedPhase8Inputs) -> None:
    """Reject direct data access; real reads belong to approved Codex orchestration."""

    del pinned
    raise UnauthorizedOperation("Codex confirmation required before Phase 8 real-release row access")


def _approved_real_read_for_codex_task() -> _ApprovedCodexRealRead:
    """Retired opaque-token factory retained only for historical imports."""

    from .current_research_surface import reject_retired_real_history_surface

    reject_retired_real_history_surface("legacy Tier 1 Phase 8 opaque read token")
    raise AssertionError("unreachable")


def _read_pinned_rows_after_approval(
    *, approved_read: _ApprovedCodexRealRead, pinned: PinnedPhase8Inputs
) -> tuple[Mapping[str, object], ...]:
    """Open only pinned local releases after conversational approval."""

    if type(approved_read) is not _ApprovedCodexRealRead or approved_read._secret is not _READ_SCOPE_SECRET:
        raise UnauthorizedOperation("Codex confirmation required before Phase 8 real-release row access")
    try:
        import pyarrow.parquet as pq

        reader = pq.ParquetFile(pinned.prediction_payload)
        required = {
            "market", "year", "exchange_session_date", "actual_identity_hash",
            "decision_at_ns", "outer_fold", "upstream_source_row_sha256", "prediction",
        }
        if not required.issubset(set(reader.schema_arrow.names)):
            raise IntegrityError("frozen Phase 6 predictions omit required Phase 8 fields")
        prediction_rows = tuple(
            row
            for batch in reader.iter_batches(batch_size=65_536, columns=sorted(required))
            for row in batch.to_pylist()
        )
        for path, required in (
            (path, {"status", "actual_identity_hash", "decision_at_ns", "upstream_source_row_sha256"})
            for path in (*pinned.feature_payloads, *pinned.outcome_payloads)
        ):
            reader = pq.ParquetFile(path)
            if not required.issubset(set(reader.schema_arrow.names)):
                raise IntegrityError("pinned Phase 8 feature/outcome payload omits identity fields")
        return prediction_rows
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("Phase 8 cannot open the pinned prediction payload") from exc


def _read_all_pinned_phase8_rows_after_approval(
    *, approved_read: _ApprovedCodexRealRead, pinned: PinnedPhase8Inputs
) -> OpenedPhase8Rows:
    """Open exactly the verified local payload set after approval, never a latest file."""

    if type(approved_read) is not _ApprovedCodexRealRead or approved_read._secret is not _READ_SCOPE_SECRET:
        raise UnauthorizedOperation("Codex confirmation required before Phase 8 real-release row access")
    predictions = _read_pinned_rows_after_approval(approved_read=approved_read, pinned=pinned)
    if not all(type(row.get("outer_fold")) is int for row in predictions):
        raise IntegrityError("frozen Phase 6 predictions omit outer-fold identity")
    features: list[Mapping[str, object]] = []
    outcomes: list[Mapping[str, object]] = []
    source_bars: list[Mapping[str, object]] = []
    feature_columns = {"status", "exchange_session_date", "actual_identity_hash", "decision_at_ns", "planned_entry_at_ns", "upstream_source_row_sha256", "bar_return"}
    outcome_columns = {"status", "exchange_session_date", "actual_identity_hash", "decision_at_ns", "entry_at_ns", "label_unlock_at_ns", "price_return", "source_bar_event_at_ns", "upstream_source_row_sha256"}
    source_columns = {"event_at_ns", "open_nano", "disposition", "actual_identity_hash", "exchange_session_date", "tick_size", "point_value", "tick_value", "currency", "source_row_sha256"}
    try:
        import pyarrow.parquet as pq
        for pair, feature_path, outcome_path, source in zip(pinned.input_pairs, pinned.feature_payloads, pinned.outcome_payloads, pinned.source_bars, strict=True):
            market, year = pair["market"], pair["year"]
            if sha256_file(source.parquet_path) != source.parquet_sha256:
                raise IntegrityError("pinned Phase 8 source-bar payload hash does not match its binding")
            for path, required, target, source_alias in (
                (feature_path, feature_columns, features, False),
                (outcome_path, outcome_columns, outcomes, False),
                (source.parquet_path, source_columns, source_bars, True),
            ):
                reader = pq.ParquetFile(path)
                if not required.issubset(set(reader.schema_arrow.names)):
                    raise IntegrityError("pinned Phase 8 payload omits required execution fields")
                for batch in reader.iter_batches(batch_size=65_536, columns=sorted(required)):
                    for row in batch.to_pylist():
                        tagged = {**row, "market": market, "year": year}
                        if source_alias:
                            tagged["upstream_source_row_sha256"] = tagged.pop("source_row_sha256")
                        target.append(tagged)
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("Phase 8 cannot open the pinned execution payloads") from exc
    return OpenedPhase8Rows(predictions, tuple(features), tuple(outcomes), tuple(source_bars))


def normalize_phase8_execution_rows(
    *,
    prediction_rows: Iterable[Mapping[str, object]],
    execution_rows: Iterable[Mapping[str, object]],
) -> tuple[Phase8SyntheticTrade, ...]:
    """Join approved rows and turn exact contract P&L inputs into evaluator trades.

    ``execution_rows`` comes from pinned causal/bar/economics releases.  Each
    row supplies explicit entry/exit actual identities, tick economics and all
    baseline gross P&Ls.  A changed identity is an excluded roll boundary.
    """

    predictions: dict[str, Mapping[str, object]] = {}
    for row in prediction_rows:
        key = row.get("upstream_source_row_sha256")
        if not isinstance(key, str) or key in predictions:
            raise IntegrityError("Phase 8 predictions have duplicate or invalid source rows")
        if not isinstance(row.get("actual_identity_hash"), str) or type(row.get("decision_at_ns")) is not int:
            raise IntegrityError("Phase 8 prediction identity or timing is invalid")
        predictions[key] = row

    normalized: list[Phase8SyntheticTrade] = []
    for row in execution_rows:
        key = row.get("upstream_source_row_sha256")
        prediction = predictions.get(key) if isinstance(key, str) else None
        if prediction is None:
            raise IntegrityError("Phase 8 execution row is not pinned to a prediction")
        if row.get("actual_identity_hash") != prediction.get("actual_identity_hash") or row.get("decision_at_ns") != prediction.get("decision_at_ns"):
            raise IntegrityError("Phase 8 execution identity or timing differs from its prediction")
        entry_identity = row.get("entry_actual_identity_hash")
        exit_identity = row.get("exit_actual_identity_hash")
        if not isinstance(entry_identity, str) or not isinstance(exit_identity, str):
            raise IntegrityError("Phase 8 execution row omits actual contract identities")
        if entry_identity != exit_identity:
            # Continuous-symbol rolls are boundaries, not price returns.
            continue
        direction = 1 if _decimal(prediction.get("prediction"), name="prediction") > 0 else -1
        quantity = row.get("quantity")
        if type(quantity) is not int or quantity <= 0:
            raise IntegrityError("Phase 8 execution quantity is invalid")
        tick_size = _decimal(row.get("tick_size"), name="tick_size", positive=True)
        tick_value = _decimal(row.get("tick_value_usd"), name="tick_value_usd", positive=True)
        point_value = _decimal(row.get("point_value"), name="point_value", positive=True)
        if tick_size * point_value != tick_value:
            raise IntegrityError("Phase 8 tick math is inconsistent with indexed economics")
        entry = _decimal(row.get("entry_price"), name="entry_price", positive=True)
        exit = _decimal(row.get("exit_price"), name="exit_price", positive=True)
        entry_ticks, exit_ticks = entry / tick_size, exit / tick_size
        if entry_ticks != entry_ticks.to_integral_value() or exit_ticks != exit_ticks.to_integral_value():
            raise IntegrityError("Phase 8 execution price is off the indexed tick grid")
        baselines = row.get("baseline_gross_pnl_usd")
        if not isinstance(baselines, dict):
            raise IntegrityError("Phase 8 execution row omits baseline outcomes")
        market, year, session = row.get("market"), row.get("market_year"), row.get("session")
        if not isinstance(market, str) or type(year) is not int or type(session) is not int:
            raise IntegrityError("Phase 8 execution scope is invalid")
        gross = Decimal(direction * quantity) * (exit_ticks - entry_ticks) * tick_value
        normalized.append(Phase8SyntheticTrade(
            market=market,
            market_year=year,
            session=session,
            signed_quantity=direction * quantity,
            risk_at_entry_usd=_decimal(row.get("risk_at_entry_usd"), name="risk_at_entry_usd", positive=True),
            gross_pnl_usd=gross,
            tick_value_usd=tick_value,
            baseline_gross_pnl_usd={name: _decimal(value, name="baseline_gross_pnl_usd") for name, value in baselines.items()},
            entry_at_ns=row.get("entry_at_ns", 0),
            exit_at_ns=row.get("exit_at_ns", 0),
        ))
    if not normalized:
        raise IntegrityError("Phase 8 has no non-roll normalized execution rows")
    return tuple(normalized)


def convert_pinned_source_bars_to_execution_rows(
    *,
    prediction_rows: Iterable[Mapping[str, object]],
    feature_rows: Iterable[Mapping[str, object]],
    outcome_rows: Iterable[Mapping[str, object]],
    source_bar_rows: Iterable[Mapping[str, object]],
    fold_local_directions: Mapping[tuple[str, int, int], int] | FoldLocalDirections,
) -> SourceBarConversion:
    """Build evaluator inputs from pre-opened pinned source bars.

    ``fold_local_directions`` is an explicit, training-only input keyed by
    ``(market, outer_fold, UTC-minute-of-day)``.  This keeps the fold-local baseline from
    silently using the evaluation row or any future data.  A roll is reported
    as an excluded boundary; missing or inconsistent rows fail closed.
    """

    predictions = _rows_by_source(prediction_rows, "prediction")
    features = _rows_by_source(feature_rows, "feature")
    outcomes = _rows_by_source(outcome_rows, "outcome")
    bars_by_source = _rows_by_source(source_bar_rows, "source bar")
    bars_by_time: dict[tuple[str, int], Mapping[str, object]] = {}
    for row in bars_by_source.values():
        market, event_at_ns = row.get("market"), row.get("event_at_ns")
        if not isinstance(market, str) or type(event_at_ns) is not int:
            raise IntegrityError("Phase 8 source bars have duplicate or invalid event times")
        key = (market, event_at_ns)
        if key in bars_by_time:
            raise IntegrityError("Phase 8 source bars have duplicate or invalid event times")
        bars_by_time[key] = row

    directions = fold_local_directions.directions if isinstance(fold_local_directions, FoldLocalDirections) else fold_local_directions
    fallback_keys = fold_local_directions.fallback_keys if isinstance(fold_local_directions, FoldLocalDirections) else frozenset()
    rows: list[Mapping[str, object]] = []
    excluded_rolls = 0
    used_fallbacks: set[tuple[str, int, int]] = set()
    for source_hash, prediction in predictions.items():
        feature = features.get(source_hash)
        outcome = outcomes.get(source_hash)
        source = bars_by_source.get(source_hash)
        if feature is None or outcome is None or source is None:
            raise IntegrityError("Phase 8 prediction lacks a pinned feature, outcome, or source bar")
        _validate_prediction_binding(prediction=prediction, feature=feature, outcome=outcome, source=source)
        entry_at_ns, exit_at_ns = outcome.get("entry_at_ns"), outcome.get("label_unlock_at_ns")
        if type(entry_at_ns) is not int or type(exit_at_ns) is not int or entry_at_ns >= exit_at_ns:
            raise IntegrityError("Phase 8 outcome timing is invalid")
        if feature.get("planned_entry_at_ns") != entry_at_ns:
            raise IntegrityError("Phase 8 feature and outcome entry times differ")
        predicted_market = prediction.get("market")
        if not isinstance(predicted_market, str):
            raise IntegrityError("Phase 8 prediction market is invalid")
        entry_bar, exit_bar = bars_by_time.get((predicted_market, entry_at_ns)), bars_by_time.get((predicted_market, exit_at_ns))
        if entry_bar is None or exit_bar is None:
            raise IntegrityError("Phase 8 source bars do not cover the pinned execution window")
        _validate_execution_bars(source=source, entry=entry_bar, exit=exit_bar)
        entry_identity = entry_bar.get("actual_identity_hash")
        exit_identity = exit_bar.get("actual_identity_hash")
        if entry_identity != exit_identity:
            excluded_rolls += 1
            continue
        market, year, fold = prediction.get("market"), prediction.get("year"), prediction.get("outer_fold")
        if not isinstance(market, str) or type(year) is not int or type(fold) is not int:
            raise IntegrityError("Phase 8 prediction scope omits market, year, or outer fold")
        fold_key = (market, fold, _session_bucket(prediction["decision_at_ns"]))
        fold_direction = directions.get(fold_key)
        if fold_direction not in (-1, 1):
            raise IntegrityError("Phase 8 fold-local baseline lacks a training-only direction")
        if fold_key in fallback_keys:
            used_fallbacks.add(fold_key)
        candidate_direction = 1 if _decimal(prediction.get("prediction"), name="prediction") > 0 else -1
        momentum_direction = _sign(_decimal(feature.get("bar_return"), name="feature bar_return"))
        entry_price = _price(entry_bar.get("open_nano"), name="entry open_nano")
        exit_price = _price(exit_bar.get("open_nano"), name="exit open_nano")
        tick_size = _decimal(entry_bar.get("tick_size"), name="tick_size", positive=True)
        tick_value = _decimal(entry_bar.get("tick_value"), name="tick_value", positive=True)
        point_value = _decimal(entry_bar.get("point_value"), name="point_value", positive=True)
        if tick_size * point_value != tick_value:
            raise IntegrityError("Phase 8 source-bar tick math is inconsistent")
        if any(price / tick_size != (price / tick_size).to_integral_value() for price in (entry_price, exit_price)):
            raise IntegrityError("Phase 8 source-bar open is off the tick grid")
        gross = lambda direction: Decimal(direction) * ((exit_price - entry_price) / tick_size) * tick_value
        rows.append({
            "upstream_source_row_sha256": source_hash,
            "actual_identity_hash": prediction["actual_identity_hash"],
            "decision_at_ns": prediction["decision_at_ns"],
            "entry_at_ns": entry_at_ns,
            "exit_at_ns": exit_at_ns,
            "outer_fold": fold,
            "entry_actual_identity_hash": entry_identity,
            "exit_actual_identity_hash": exit_identity,
            "tick_size": tick_size,
            "tick_value_usd": tick_value,
            "point_value": point_value,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "quantity": 1,
            "risk_at_entry_usd": Decimal("125"),
            "market": market,
            "market_year": year,
            "session": _session_number(prediction.get("exchange_session_date")),
            "baseline_gross_pnl_usd": {
                "fold_local_unconditional_return_by_market_session": gross(fold_direction),
                "previous_bar_sign_momentum": gross(momentum_direction),
                "previous_bar_sign_reversal": gross(-momentum_direction),
                "risk_matched_always_long_intraday": gross(1),
                "equal_risk_version_of_candidate_signal": gross(candidate_direction),
            },
        })
    return SourceBarConversion(tuple(rows), excluded_rolls, tuple(sorted(used_fallbacks)))


def _rows_by_source(rows: Iterable[Mapping[str, object]], name: str) -> dict[str, Mapping[str, object]]:
    result: dict[str, Mapping[str, object]] = {}
    for row in rows:
        key = row.get("upstream_source_row_sha256")
        if not isinstance(key, str) or key in result:
            raise IntegrityError(f"Phase 8 {name} rows have duplicate or invalid source hashes")
        result[key] = row
    return result


def _validate_prediction_binding(*, prediction: Mapping[str, object], feature: Mapping[str, object], outcome: Mapping[str, object], source: Mapping[str, object]) -> None:
    identity = prediction.get("actual_identity_hash")
    decision = prediction.get("decision_at_ns")
    if not isinstance(identity, str) or type(decision) is not int:
        raise IntegrityError("Phase 8 prediction identity or timing is invalid")
    if feature.get("status") != "FEATURE_READY" or outcome.get("status") != "MATURED":
        raise IntegrityError("Phase 8 feature or outcome is not eligible for evaluation")
    for row in (feature, outcome, source):
        if row.get("actual_identity_hash") != identity:
            raise IntegrityError("Phase 8 feature, outcome, or source identity differs from prediction")
    if feature.get("decision_at_ns") != decision or outcome.get("decision_at_ns") != decision:
        raise IntegrityError("Phase 8 feature or outcome timing differs from prediction")


def _validate_execution_bars(*, source: Mapping[str, object], entry: Mapping[str, object], exit: Mapping[str, object]) -> None:
    for row in (entry, exit):
        if row.get("disposition") != "ELIGIBLE":
            raise IntegrityError("Phase 8 execution bar is not eligible")
        if row.get("currency") != source.get("currency") or row.get("currency") != "USD":
            raise IntegrityError("Phase 8 execution currency is inconsistent or unsupported")
        for field in ("tick_size", "tick_value", "point_value"):
            if row.get(field) != source.get(field):
                raise IntegrityError("Phase 8 execution economics differ within the pinned window")


def _price(value: object, *, name: str) -> Decimal:
    if type(value) is not int:
        raise IntegrityError(f"Phase 8 {name} is invalid")
    return Decimal(value) / Decimal("1000000000")


def _sign(value: Decimal) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _session_number(value: object) -> int:
    if not isinstance(value, str):
        raise IntegrityError("Phase 8 exchange session date is invalid")
    try:
        return date.fromisoformat(value).toordinal()
    except ValueError as exc:
        raise IntegrityError("Phase 8 exchange session date is invalid") from exc


def _session_bucket(value: object) -> int:
    if type(value) is not int or value < 0:
        raise IntegrityError("Phase 8 decision time is invalid")
    return (value % _NS_PER_DAY) // (60 * 1_000_000_000)


def derive_fold_local_directions(
    *, prediction_rows: Iterable[Mapping[str, object]], outcome_rows: Iterable[Mapping[str, object]], outer_folds: Iterable[Mapping[str, object]]
) -> FoldLocalDirections:
    """Derive exact market/session directions with a documented market fallback."""

    folds = tuple(outer_folds)
    training_ranges: dict[int, tuple[str, str]] = {}
    for index, fold in enumerate(folds):
        value = fold.get("outer_fit_session_range") if isinstance(fold, Mapping) else None
        if not isinstance(value, list) or len(value) != 2 or not all(isinstance(item, str) for item in value):
            raise IntegrityError("Phase 8 outer-fold training range is invalid")
        training_ranges[index] = (value[0], value[1])
    required = {(row.get("market"), row.get("outer_fold"), _session_bucket(row.get("decision_at_ns"))) for row in prediction_rows}
    required_keys: set[tuple[str, int, int]] = set()
    for market, fold, bucket in required:
        if not isinstance(market, str) or type(fold) is not int or fold not in training_ranges:
            raise IntegrityError("Phase 8 prediction has an invalid fold-local baseline key")
        required_keys.add((market, fold, bucket))
    totals: dict[tuple[str, int, int], Decimal] = {key: Decimal("0") for key in required_keys}
    counts: dict[tuple[str, int, int], int] = {key: 0 for key in required_keys}
    market_totals: dict[tuple[str, int], Decimal] = {(market, fold): Decimal("0") for market, fold, _ in required_keys}
    market_counts: dict[tuple[str, int], int] = {(market, fold): 0 for market, fold, _ in required_keys}
    # Aggregate once.  The prior straightforward implementation rescanned all
    # training rows for every minute bucket, which is infeasible for real data.
    for row in outcome_rows:
        if row.get("status") != "MATURED" or not isinstance(row.get("market"), str) or not isinstance(row.get("exchange_session_date"), str):
            continue
        market, session_date = row["market"], row["exchange_session_date"]
        bucket = _session_bucket(row.get("decision_at_ns"))
        value = _decimal(row.get("price_return"), name="training price_return")
        for fold, (start, end) in training_ranges.items():
            key = (market, fold, bucket)
            market_key = (market, fold)
            if market_key in market_totals and start <= session_date <= end:
                market_totals[market_key] += value
                market_counts[market_key] += 1
                if key in totals:
                    totals[key] += value
                    counts[key] += 1
    directions: dict[tuple[str, int, int], int] = {}
    fallback_keys: set[tuple[str, int, int]] = set()
    for key in required_keys:
        if counts[key] == 0:
            market_key = (key[0], key[1])
            if market_counts[market_key] == 0:
                raise IntegrityError("Phase 8 fold-local baseline has no market training observations")
            direction = _sign(market_totals[market_key])
            fallback_keys.add(key)
        else:
            direction = _sign(totals[key])
        if direction == 0:
            raise IntegrityError("Phase 8 fold-local baseline direction is ambiguous")
        directions[key] = direction
    return FoldLocalDirections(directions, frozenset(fallback_keys))


def derive_training_outcome_volatilities(
    *, outcome_rows: Iterable[Mapping[str, object]], outer_folds: Iterable[Mapping[str, object]]
) -> dict[tuple[str, int], Decimal]:
    """Return each market/fold's training-only five-minute outcome volatility."""

    folds = tuple(outer_folds)
    ranges = {
        index: tuple(fold.get("outer_fit_session_range", ()))
        for index, fold in enumerate(folds) if isinstance(fold, Mapping)
    }
    if any(len(value) != 2 or not all(isinstance(item, str) for item in value) for value in ranges.values()):
        raise IntegrityError("Phase 8 outer-fold training range is invalid")
    values: dict[tuple[str, int], list[float]] = {}
    for row in outcome_rows:
        market, session = row.get("market"), row.get("exchange_session_date")
        if row.get("status") != "MATURED" or not isinstance(market, str) or not isinstance(session, str):
            continue
        value = float(_decimal(row.get("price_return"), name="training price_return"))
        for fold, (start, end) in ranges.items():
            if start <= session <= end:
                values.setdefault((market, fold), []).append(value)
    result: dict[tuple[str, int], Decimal] = {}
    for key, samples in values.items():
        if len(samples) < 2:
            continue
        mean = sum(samples) / len(samples)
        volatility = sqrt(sum((sample - mean) ** 2 for sample in samples) / len(samples))
        if volatility > 0:
            result[key] = Decimal(str(volatility))
    return result


def schedule_one_contract_execution_rows(
    *, prediction_rows: Iterable[Mapping[str, object]], execution_rows: Iterable[Mapping[str, object]],
    training_volatilities: Mapping[tuple[str, int], Decimal],
) -> ScheduledExecutionRows:
    """Select one risk-adjusted candidate at a time, with no overlapping positions."""

    predictions = _rows_by_source(prediction_rows, "prediction")
    grouped: dict[int, list[Mapping[str, object]]] = {}
    candidate_count = 0
    for row in execution_rows:
        key = row.get("upstream_source_row_sha256")
        prediction = predictions.get(key) if isinstance(key, str) else None
        entry, exit_at, market, fold = row.get("entry_at_ns"), row.get("exit_at_ns"), row.get("market"), row.get("outer_fold")
        if prediction is None or type(entry) is not int or type(exit_at) is not int or entry >= exit_at or not isinstance(market, str) or type(fold) is not int:
            raise IntegrityError("Phase 8 scheduled execution candidate is invalid")
        volatility = training_volatilities.get((market, fold))
        if volatility is None or volatility <= 0:
            raise IntegrityError("Phase 8 candidate lacks training-only outcome volatility")
        candidate_count += 1
        grouped.setdefault(entry, []).append({**row, "_selection_score": abs(_decimal(prediction.get("prediction"), name="prediction")) / volatility})
    selected: list[Mapping[str, object]] = []
    simultaneous = 0
    overlap = 0
    open_until = -1
    market_order = {market: index for index, market in enumerate(("ES", "CL", "ZN", "6E"))}
    for entry in sorted(grouped):
        candidates = grouped[entry]
        if entry < open_until:
            overlap += len(candidates)
            continue
        ranked = sorted(candidates, key=lambda row: (-row["_selection_score"], market_order.get(row["market"], len(market_order)), row["upstream_source_row_sha256"]))
        selected.append({key: value for key, value in ranked[0].items() if key != "_selection_score"})
        simultaneous += len(ranked) - 1
        open_until = ranked[0]["exit_at_ns"]
    return ScheduledExecutionRows(tuple(selected), candidate_count, simultaneous, overlap)
