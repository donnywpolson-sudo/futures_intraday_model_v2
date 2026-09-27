"""Sole authority-bearing route for v1.2 intraday historical research."""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json, write_new_canonical_authority
from .certified_research_gateway import CertifiedResearchGateway
from .errors import IntegrityError
from .intraday_alpha_pipeline_v12 import (
    PROGRAM_SCHEMA,
    ES_FLOW_TRIAL_ROOT,
    SOURCE_REGISTRATION_SCHEMA,
    CausalBar,
    CausalSecondBar,
    HISTORICAL_CALENDAR_POINTER,
    ROW_COMPLETENESS_SCHEMA,
    ES_FLOW_SCREEN_STAGE,
    ES_FLOW_SCREEN_YEARS,
    _es_active_source_binding,
    assert_authority_not_blocked,
    build_es_flow_screen_program,
    evaluate_es_flow_screen,
    evaluate_es_flow_wfa,
    evaluate_program_decision,
    evaluate_portfolio_decision,
    materialize_program_inputs,
)
from .alpha_research_ladder import load_active_ladder
from .research_gateway_policy import (
    CERTIFIED_GATEWAY_SCHEMA,
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
    scientific_governance_binding,
)
from .causal_observation_canary import _binding as _dbn_binding, _query_contract
from .foundation.decoder import iter_bars, iter_definitions
from .foundation.economics import EconomicsRuleBook
from .foundation.identity import DefinitionIndex


def _wall(value: datetime) -> tuple[int, int, int]:
    return value.hour, value.minute, value.second


def _write_scan_checkpoint(path: Path, core: dict[str, object]) -> None:
    checkpoint = {**core, "checkpoint_sha256": sha256_json(core)}
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as stream:
        stream.write(canonical_bytes(checkpoint) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def _source_scope(registration: Mapping[str, object]) -> dict[str, object]:
    return {
        key: registration[key]
        for key in ("source_release", "historical_interval", "markets", "source_pairs", "dbn_count", "sidecar_count", "dbn_size_bytes", "bindings")
    }


def _load_scan_checkpoint(
    root: Path,
    registration: Mapping[str, object],
    checkpoint_path: Path,
    *,
    expected_sha256: str | None = None,
    require_complete: bool,
) -> dict[str, object]:
    if checkpoint_path.is_absolute():
        raise IntegrityError("source scan checkpoint path must be repository-relative")
    absolute = (root / checkpoint_path).resolve(strict=True)
    try:
        absolute.relative_to((root / "state/intraday_research_source_scan_checkpoints").resolve(strict=True))
    except ValueError as exc:
        raise IntegrityError("source scan checkpoint escapes its registry") from exc
    if expected_sha256 is not None and sha256_file(absolute) != expected_sha256:
        raise IntegrityError("source scan checkpoint bytes changed")
    checkpoint = json.loads(absolute.read_bytes())
    checkpoint_core = {key: value for key, value in checkpoint.items() if key != "checkpoint_sha256"}
    pairs = registration.get("source_pairs")
    if not isinstance(pairs, list):
        raise IntegrityError("source scan checkpoint registration has no pairs")
    pair_ids = [sha256_json(pair) for pair in pairs]
    completed = checkpoint_core.get("completed")
    if (
        checkpoint.get("checkpoint_sha256") != sha256_json(checkpoint_core)
        or checkpoint_core.get("schema_version") != "intraday_research_source_scan_checkpoint/1.0.0"
        or checkpoint_core.get("source_registration_id") != registration.get("registration_id")
        or not isinstance(completed, dict)
        or (set(completed) != set(pair_ids) if require_complete else not set(completed).issubset(pair_ids))
    ):
        raise IntegrityError("source scan checkpoint is invalid")
    for pair, pair_id in zip(pairs, pair_ids, strict=True):
        if pair_id not in completed:
            continue
        item = completed[pair_id]
        if (
            not isinstance(pair, dict)
            or not isinstance(item, dict)
            or item.get("pair_id") != pair_id
            or any(item.get(key) != pair.get(key) for key in ("family", "market", "year"))
            or not isinstance(item.get("summary"), dict)
        ):
            raise IntegrityError("source scan checkpoint pair evidence is invalid")
    return checkpoint_core


def _finalize_registered_source_checkpoint(
    root: Path,
    registration: Mapping[str, object],
    completed: Mapping[str, object],
    scan_evidence: Mapping[str, object],
) -> dict[str, object]:
    pairs = registration.get("source_pairs")
    if not isinstance(pairs, list) or len(pairs) != 144:
        raise IntegrityError("exact registered source scope is missing")
    counts: Counter[tuple[str, str, str]] = Counter()
    spans: dict[tuple[str, str, str], list[datetime]] = {}
    definition_counts: Counter[str] = Counter()
    for pair in pairs:
        if not isinstance(pair, dict):
            raise IntegrityError("registered source pair is malformed")
        pair_id = sha256_json(pair)
        item = completed.get(pair_id)
        if not isinstance(item, dict) or item.get("pair_id") != pair_id:
            raise IntegrityError("source scan checkpoint is missing a registered pair")
        family = str(pair["family"])
        market = str(pair["market"])
        summary = item.get("summary")
        if not isinstance(summary, dict):
            raise IntegrityError("source scan checkpoint summary is malformed")
        if family == "definition":
            value = summary.get("definition_count")
            if not isinstance(value, int) or value < 0:
                raise IntegrityError("registered definition summary is malformed")
            definition_counts[market] += value
            continue
        file_counts = summary.get("counts")
        file_spans = summary.get("spans")
        if not isinstance(file_counts, dict) or not isinstance(file_spans, dict) or set(file_counts) != set(file_spans):
            raise IntegrityError("registered bar summary is malformed")
        for raw_date, value in file_counts.items():
            key = (family, str(raw_date), market)
            if key in counts or not isinstance(value, int) or value <= 0:
                raise IntegrityError("registered source files overlap an exchange session")
            raw_span = file_spans[raw_date]
            if not isinstance(raw_span, list) or len(raw_span) != 2:
                raise IntegrityError("registered source span is malformed")
            try:
                parsed = [datetime.fromisoformat(str(item)) for item in raw_span]
            except ValueError as exc:
                raise IntegrityError("registered source span is malformed") from exc
            if any(value.tzinfo is None for value in parsed) or parsed[0] > parsed[1]:
                raise IntegrityError("registered source span is malformed")
            counts[key] = value
            spans[key] = parsed
    markets = tuple(registration.get("markets", ()))
    if set(markets) != {"CL", "ES", "GC", "NQ", "ZB", "ZN"} or set(definition_counts) != set(markets) or any(definition_counts[market] < 1 for market in markets):
        raise IntegrityError("registered definition coverage is incomplete")
    common: list[str] = []
    excluded: list[dict[str, object]] = []
    current = date(2018, 1, 2)
    cutoff = date(2025, 7, 13)
    while current <= cutoff:
        raw_date = current.isoformat()
        observed = {(family, market): counts[(family, raw_date, market)] for family in ("ohlcv_1m", "ohlcv_1s") for market in markets}
        complete = all(
            observed[("ohlcv_1m", market)] > 0
            and observed[("ohlcv_1s", market)] > 0
            and tuple(_wall(value) for value in spans[("ohlcv_1m", raw_date, market)]) == ((8, 30, 0), (15, 0, 0))
            and _wall(spans[("ohlcv_1s", raw_date, market)][0]) <= (8, 30, 59)
            and _wall(spans[("ohlcv_1s", raw_date, market)][1]) >= (14, 59, 0)
            for market in markets
        )
        if complete:
            common.append(raw_date)
        elif not any(observed.values()):
            excluded.append({"exchange_session_date": raw_date, "reason": "SOURCE_UNOBSERVABLE_NO_REGISTERED_SESSION_ROWS"})
        else:
            excluded.append({"exchange_session_date": raw_date, "reason": "INCOMPLETE_REGISTERED_SESSION_BOUNDARY_COVERAGE", "counts": {f"{family}:{market}": value for (family, market), value in sorted(observed.items())}})
        current += timedelta(days=1)
    requirements = registration.get("requirements")
    if not isinstance(requirements, dict) or requirements.get("incomplete_session_policy") != "EXCLUDE_BEFORE_FOLDS_NEVER_ZERO_RETURN":
        raise IntegrityError("complete-session exclusion policy is not frozen")
    minimum = int(requirements.get("minimum_common_sessions", 0))
    if len(common) < minimum:
        return {"status": "BLOCKED_INSUFFICIENT_COMPLETE_SESSIONS_NO_CERTIFICATE", "source_registration_id": registration["registration_id"], "complete_session_count": len(common), "excluded_dates": excluded}
    predecessor = json.loads((root / HISTORICAL_CALENDAR_POINTER).read_bytes())
    incomplete_count = sum(item["reason"] == "INCOMPLETE_REGISTERED_SESSION_BOUNDARY_COVERAGE" for item in excluded)
    calendar_core = {"schema_version": "intraday_research_empirical_calendar/1.2.3", "state": "INACTIVE_SOURCE_OBSERVABILITY_SUCCESSOR", "source_registration_id": registration["registration_id"], "predecessor": {"calendar_id": predecessor["calendar_id"], "path": predecessor["calendar_path"], "sha256": predecessor["calendar_sha256"]}, "interval": registration["historical_interval"], "markets": list(markets), "selection_policy": "COMPLETE_COMMON_SESSIONS_ONLY_EXCLUDE_INCOMPLETE_NEVER_ZERO_RETURN", "common_exchange_session_dates": common, "excluded_dates": excluded}
    calendar = {**calendar_core, "calendar_id": sha256_json(calendar_core)}
    calendar_path = Path("state/calendar_registry/intraday_alpha_v12") / str(calendar["calendar_id"]) / "calendar.json"
    certificate_core = {"schema_version": ROW_COMPLETENESS_SCHEMA, "status": "PASS_COMPLETE", "source_registration_id": registration["registration_id"], "calendar_id": calendar["calendar_id"], "common_exchange_session_dates_sha256": sha256_json(common), "complete_session_count": len(common), "incomplete_session_count": incomplete_count, "excluded_date_count": len(excluded), "requirements": requirements, "source_scan_evidence": dict(scan_evidence), "dbn_count_scanned": 144, "fallback_used": False}
    certificate = {**certificate_core, "certificate_id": sha256_json(certificate_core)}
    certificate_path = Path("state/intraday_research_row_completeness_registry") / str(certificate["certificate_id"]) / "certificate.json"
    write_new_canonical_authority(root / calendar_path, calendar)
    write_new_canonical_authority(root / certificate_path, certificate)
    return {"status": "PASS_COMPLETE", "calendar_path": calendar_path.as_posix(), "calendar_id": calendar["calendar_id"], "certificate_path": certificate_path.as_posix(), "certificate_id": certificate["certificate_id"], "complete_session_count": len(common), "incomplete_session_count": incomplete_count, "excluded_date_count": len(excluded)}


def audit_registered_source_rows(root: Path, registration: dict[str, object]) -> dict[str, object]:
    """Approval-gated offline scan of only the 144 metadata-registered DBNs."""

    if registration.get("schema_version") != SOURCE_REGISTRATION_SCHEMA or registration.get("state") != "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED" or registration.get("registration_id") != sha256_json({key: value for key, value in registration.items() if key != "registration_id"}):
        raise IntegrityError("metadata-only source registration is invalid")
    release = registration.get("source_release")
    pairs = registration.get("source_pairs")
    if not isinstance(release, dict) or not isinstance(pairs, list) or len(pairs) != 144:
        raise IntegrityError("exact registered source scope is missing")
    pair_ids = [sha256_json(pair) for pair in pairs]
    checkpoint_path = root / "state/intraday_research_source_scan_checkpoints" / str(registration["registration_id"]) / "checkpoint.json"
    if checkpoint_path.exists():
        checkpoint_core = _load_scan_checkpoint(root, registration, checkpoint_path.relative_to(root), require_complete=False)
    else:
        checkpoint_core = {
            "schema_version": "intraday_research_source_scan_checkpoint/1.0.0",
            "source_registration_id": registration["registration_id"],
            "completed": {},
        }
    completed = checkpoint_core["completed"]
    for position, (pair, pair_id) in enumerate(zip(pairs, pair_ids, strict=True), start=1):
        if not isinstance(pair, dict):
            raise IntegrityError("registered source pair is malformed")
        if pair_id in completed:
            print(f"[source-scan] {position}/144 resumed {pair['family']} {pair['market']} {pair['year']}", flush=True)
            continue
        dbn = pair["dbn"]
        sidecar = dict(pair["sidecar"])
        sidecar["paired_dbn_sha256"] = dbn["sha256"]
        sidecar["paired_dbn_size_bytes"] = dbn["size_bytes"]
        binding = _dbn_binding(root=root, entry=dbn, release_id=str(release["release_id"]), release_manifest_sha256=str(release["release_manifest_sha256"]), files_index_sha256=str(release["inventory_sha256"]), allow_registered_hardlinks=int(pair["year"]) == 2025)
        query = _query_contract(root, sidecar)
        family = str(pair["family"])
        market = str(pair["market"])
        if family == "definition":
            summary: dict[str, object] = {"definition_count": sum(1 for _ in iter_definitions(binding, market=market, expected_query_contract=query))}
        else:
            interval = timedelta(minutes=1) if family == "ohlcv_1m" else timedelta(seconds=1)
            file_counts: Counter[str] = Counter()
            file_previous: dict[str, datetime] = {}
            file_spans: dict[str, list[datetime]] = {}
            for bar in iter_bars(binding, market=market, expected_query_contract=query, schema=family.replace("_", "-")):
                chicago = bar.event_at.astimezone(ZoneInfo("America/Chicago"))
                raw_date = chicago.date().isoformat()
                wall = (chicago.hour, chicago.minute, chicago.second)
                in_grid = (8, 30, 0) <= wall <= (15, 0, 0) if family == "ohlcv_1m" else (8, 30, 0) <= wall < (15, 0, 0)
                if raw_date < "2018-01-02" or raw_date > "2025-07-13" or not in_grid:
                    continue
                prior = file_previous.get(raw_date)
                if prior is not None and (chicago <= prior or (chicago - prior) % interval):
                    raise IntegrityError("registered source contains a duplicate, reversed, or off-grid interval")
                file_previous[raw_date] = chicago
                file_spans.setdefault(raw_date, [chicago, chicago])[1] = chicago
                file_counts[raw_date] += 1
            summary = {
                "counts": dict(sorted(file_counts.items())),
                "spans": {raw_date: [values[0].isoformat(), values[1].isoformat()] for raw_date, values in sorted(file_spans.items())},
            }
        completed[pair_id] = {"pair_id": pair_id, "family": family, "market": market, "year": pair["year"], "summary": summary}
        _write_scan_checkpoint(checkpoint_path, checkpoint_core)
        print(f"[source-scan] {position}/144 scanned {family} {market} {pair['year']}", flush=True)
    relative_checkpoint = checkpoint_path.relative_to(root)
    return _finalize_registered_source_checkpoint(root, registration, completed, {"source_registration_id": registration["registration_id"], "checkpoint": {"path": relative_checkpoint.as_posix(), "sha256": sha256_file(checkpoint_path)}, "source_scope_sha256": sha256_json(_source_scope(registration)), "completed_pair_count": len(completed), "payload_reread": True})


def certify_registered_source_checkpoint(
    root: Path,
    predecessor_registration: Mapping[str, object],
    successor_registration: Mapping[str, object],
    checkpoint_path: Path,
    checkpoint_sha256: str,
) -> dict[str, object]:
    """Reclassify one exact completed checkpoint without reopening DBN payloads."""

    root = root.resolve(strict=True)
    for registration, schemas in (
        (predecessor_registration, {"intraday_research_source_registration/1.2.2"}),
        (successor_registration, {SOURCE_REGISTRATION_SCHEMA}),
    ):
        if registration.get("schema_version") not in schemas or registration.get("state") != "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED" or registration.get("registration_id") != sha256_json({key: value for key, value in registration.items() if key != "registration_id"}):
            raise IntegrityError("source registration identity is invalid")
    if canonical_bytes(_source_scope(predecessor_registration)) != canonical_bytes(_source_scope(successor_registration)):
        raise IntegrityError("source registration scope drifted after the completed scan")
    checkpoint = _load_scan_checkpoint(root, predecessor_registration, checkpoint_path, expected_sha256=checkpoint_sha256, require_complete=True)
    return _finalize_registered_source_checkpoint(
        root,
        successor_registration,
        checkpoint["completed"],
        {"predecessor_source_registration_id": predecessor_registration["registration_id"], "checkpoint": {"path": checkpoint_path.as_posix(), "sha256": checkpoint_sha256}, "source_scope_sha256": sha256_json(_source_scope(successor_registration)), "completed_pair_count": len(checkpoint["completed"]), "payload_reread": False},
    )


def _load_registration_bound_inputs(
    root: Path, registration: dict[str, object]
) -> dict[str, object]:
    """Open only a row-certified immutable registration; never discover sources."""

    if registration.get("schema_version") != SOURCE_REGISTRATION_SCHEMA or registration.get("state") != "ROW_COMPLETE_CERTIFIED_NOT_ACTIVE":
        raise IntegrityError("certified one-minute and one-second source bindings are required")
    registration_core = {key: value for key, value in registration.items() if key != "registration_id"}
    if registration.get("registration_id") != sha256_json(registration_core):
        raise IntegrityError("source registration identity changed")

    def contained(relative: object) -> Path:
        path = Path(str(relative))
        if path.is_absolute():
            raise IntegrityError("registered source path must be repository-relative")
        target = (root / path).resolve(strict=True)
        try:
            target.relative_to(root.resolve(strict=True))
        except ValueError as exc:
            raise IntegrityError("registered source path escapes the repository") from exc
        return target

    bindings = registration.get("bindings")
    if not isinstance(bindings, dict):
        raise IntegrityError("source registration bindings are missing")
    for relative, binding in bindings.items():
        if not isinstance(binding, dict) or binding.get("path") != relative or sha256_file(contained(relative)) != binding.get("sha256"):
            raise IntegrityError("source registration binding drifted")
    completeness = registration.get("row_completeness")
    if not isinstance(completeness, dict) or completeness.get("status") != "COMPLETE" or not isinstance(completeness.get("certificate"), dict) or not isinstance(completeness.get("calendar_successor"), dict):
        raise IntegrityError("row-completeness certificate and calendar successor are required")
    certificate_path = contained(completeness["certificate"]["path"])
    calendar_path = contained(completeness["calendar_successor"]["path"])
    if sha256_file(certificate_path) != completeness["certificate"].get("sha256") or sha256_file(calendar_path) != completeness["calendar_successor"].get("sha256"):
        raise IntegrityError("row-completeness or calendar bytes changed")
    certificate = json.loads(certificate_path.read_bytes())
    calendar = json.loads(calendar_path.read_bytes())
    sessions = tuple(calendar.get("common_exchange_session_dates", ()))
    if certificate.get("status") != "PASS_COMPLETE" or certificate.get("source_registration_id") != registration.get("predecessor_registration_id") or certificate.get("common_exchange_session_dates_sha256") != sha256_json(list(sessions)) or len(sessions) < 1010:
        raise IntegrityError("row-completeness certificate is incomplete")
    session_scope = set(sessions)
    release = registration.get("source_release")
    pairs = registration.get("source_pairs")
    if not isinstance(release, dict) or not isinstance(pairs, list) or len(pairs) != 144:
        raise IntegrityError("exact registered source scope is missing")
    definitions: dict[str, list[object]] = defaultdict(list)
    bars_by_family: dict[str, list[object]] = {"ohlcv_1m": [], "ohlcv_1s": []}
    for pair in pairs:
        if not isinstance(pair, dict) or set(pair) != {"market", "family", "year", "dbn", "sidecar"}:
            raise IntegrityError("registered source pair is malformed")
        dbn = pair["dbn"]
        sidecar = dict(pair["sidecar"])
        if not isinstance(dbn, dict) or not isinstance(sidecar, dict):
            raise IntegrityError("registered source entries are malformed")
        sidecar["paired_dbn_sha256"] = dbn["sha256"]
        sidecar["paired_dbn_size_bytes"] = dbn["size_bytes"]
        binding = _dbn_binding(
            root=root,
            entry=dbn,
            release_id=str(release["release_id"]),
            release_manifest_sha256=str(release["release_manifest_sha256"]),
            files_index_sha256=str(release["inventory_sha256"]),
            allow_registered_hardlinks=int(pair["year"]) == 2025,
        )
        query = _query_contract(root, sidecar)
        family = str(pair["family"])
        market = str(pair["market"])
        if family == "definition":
            definitions[market].extend(iter_definitions(binding, market=market, expected_query_contract=query))
        elif family in bars_by_family:
            bars_by_family[family].extend(iter_bars(binding, market=market, expected_query_contract=query, schema=family.replace("_", "-")))
        else:
            raise IntegrityError("unregistered source family reached the row loader")
    rulebook = EconomicsRuleBook.from_file(contained("configs/contract_economics_rules.json"))
    definition_indexes = {market: DefinitionIndex(values) for market, values in definitions.items()}
    minute_rows: list[CausalBar] = []
    second_rows: list[CausalSecondBar] = []
    counts: Counter[tuple[str, str, str]] = Counter()
    spans: dict[tuple[str, str, str], list[datetime]] = {}
    for family, target in (("ohlcv_1m", minute_rows), ("ohlcv_1s", second_rows)):
        interval = timedelta(minutes=1) if family == "ohlcv_1m" else timedelta(seconds=1)
        for bar in bars_by_family[family]:
            event = bar.event_at
            chicago = event.astimezone(ZoneInfo("America/Chicago"))
            session = chicago.date().isoformat()
            if session not in session_scope:
                continue
            end = (15, 0, 0) if family == "ohlcv_1s" else (15, 0, 1)
            if (chicago.hour, chicago.minute, chicago.second) < (8, 30, 0) or (chicago.hour, chicago.minute, chicago.second) >= end:
                continue
            definition = definition_indexes[bar.market].resolve(bar, decision_at=event + interval)
            economics = rulebook.resolve(bar.market, definition)
            identity = f"{bar.publisher_id}:{bar.instrument_id}:{definition.raw_symbol}:{definition.row_sha256}"
            prices = tuple(float(value) for value in bar.prices)
            counts[(family, session, bar.market)] += 1
            spans.setdefault((family, session, bar.market), [chicago, chicago])[1] = chicago
            if family == "ohlcv_1m":
                target.append(CausalBar(bar.market, session, event, event + interval, identity, *prices, float(bar.volume), float(economics.tick_size), float(economics.tick_value)))
            else:
                target.append(CausalSecondBar(bar.market, session, event, event + interval, identity, *prices))
    markets = tuple(registration.get("markets", ()))
    for session in sessions:
        for market in markets:
            minute_span = spans.get(("ohlcv_1m", session, market))
            second_span = spans.get(("ohlcv_1s", session, market))
            if (
                not minute_span or not second_span
                or tuple(_wall(value) for value in minute_span) != ((8, 30, 0), (15, 0, 0))
                or _wall(second_span[0]) > (8, 30, 59)
                or _wall(second_span[1]) < (14, 59, 0)
            ):
                raise IntegrityError("registered session boundary coverage is incomplete")
    program_path = contained(Path("state/intraday_research_program_registry") / str(registration["program_id"]) / "program.json")
    specification = json.loads(program_path.read_bytes())
    if specification.get("program_id") != registration.get("program_id"):
        raise IntegrityError("registered program identity changed")
    return {
        "specification": specification,
        "minute_rows": minute_rows,
        "second_rows": second_rows,
    }


def _active_es_row_to_bar(row: Mapping[str, object], year: int) -> CausalBar | None:
    session_date = str(row.get("project_trade_date", ""))
    if row.get("market") != "ES" or session_date[:4] != str(year) or row.get("source_cadence") != "1m":
        raise IntegrityError("ES flow screen input row escapes its bound partition")
    if not isinstance(row.get("actual_contract"), str) or not row["actual_contract"]:
        raise IntegrityError("ES flow screen input lacks actual contract identity")
    event_ns = int(row["bar_start_ns"])
    available_ns = int(row["available_at_ns"])
    event = datetime.fromtimestamp(event_ns // 1_000_000_000, tz=timezone.utc) + timedelta(microseconds=(event_ns % 1_000_000_000) // 1_000)
    available = datetime.fromtimestamp(available_ns // 1_000_000_000, tz=timezone.utc) + timedelta(microseconds=(available_ns % 1_000_000_000) // 1_000)
    if not (8, 30, 0) <= _wall(event.astimezone(ZoneInfo("America/Chicago"))) <= (15, 0, 0):
        return None
    tick_size = float(row["min_price_increment_nano"]) / 1_000_000_000
    multiplier = float(row["multiplier_nano"]) / 1_000_000_000
    return CausalBar(
        "ES", session_date, event, available, str(row["actual_contract"]),
        *(float(row[name]) / 1_000_000_000 for name in ("open_nano", "high_nano", "low_nano", "close_nano")),
        float(row["volume"]), tick_size, tick_size * multiplier,
    )


def _load_es_flow_screen_bars(root: Path, boundary: RepoBoundary, specification: Mapping[str, object]) -> list[CausalBar]:
    """Open only the five hash-bound active ES Parquets after authority is consumed."""

    sources = specification.get("sources")
    if not isinstance(sources, list) or len(sources) != len(ES_FLOW_SCREEN_YEARS):
        raise IntegrityError("ES flow screen source binding is incomplete")
    bars: list[CausalBar] = []
    required = (
        "market", "project_trade_date", "bar_start_ns", "available_at_ns",
        "actual_contract", "open_nano", "high_nano", "low_nano", "close_nano",
        "volume", "min_price_increment_nano", "multiplier_nano", "source_cadence",
    )
    try:
        import pyarrow.parquet as pq

        for source, year in zip(sources, ES_FLOW_SCREEN_YEARS, strict=True):
            if not isinstance(source, dict):
                raise IntegrityError("ES flow screen source binding is malformed")
            current = _es_active_source_binding(root, boundary, year)
            if source != current:
                raise IntegrityError("ES flow screen active input binding changed")
            path = boundary.assert_active_path(root / str(current["parquet_path"]), purpose="ES flow screen input", subtree="data/active")
            reader = pq.ParquetFile(path)
            if not set(required).issubset(reader.schema_arrow.names):
                raise IntegrityError("ES flow screen input is missing required columns")
            for batch in reader.iter_batches(batch_size=65_536, columns=list(required)):
                for row in batch.to_pylist():
                    bar = _active_es_row_to_bar(row, year)
                    if bar is not None:
                        bars.append(bar)
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("ES flow screen inputs could not be read") from exc
    if not bars:
        raise IntegrityError("ES flow screen inputs contain no eligible rows")
    return bars


def es_flow_execution_scope(
    root: Path, registration_path: Path, registration_sha256: str
) -> dict[str, str]:
    """Derive the exact single-use receipt scope from the frozen rescue registration."""

    root = root.resolve(strict=True)
    registration_path = registration_path.resolve(strict=True)
    try:
        relative = registration_path.relative_to(root)
    except ValueError as exc:
        raise IntegrityError("ES flow registration leaves the active repository") from exc
    if (
        relative.parent != ES_FLOW_TRIAL_ROOT
        or sha256_file(registration_path) != registration_sha256
    ):
        raise IntegrityError("ES flow registration binding mismatch")
    raw = registration_path.read_bytes()
    registration = json.loads(raw)
    core = {key: value for key, value in registration.items() if key != "trial_id"}
    program_binding = registration.get("program")
    readiness = registration.get("readiness")
    ladder = registration.get("alpha_ladder_binding")
    governance = registration.get("scientific_governance_binding")
    if (
        not isinstance(registration, dict)
        or raw != canonical_bytes(registration) + b"\n"
        or registration.get("trial_id") != sha256_json(core)
        or registration_path.stem != registration.get("trial_id")
        or registration.get("schema_version") != "es_flow_rescue_trial_registration/1.0.0"
        or registration.get("state") != "REGISTERED_NOT_CLAIMED_NOT_EXECUTED"
        or not isinstance(program_binding, dict)
        or not isinstance(readiness, dict)
        or not isinstance(ladder, dict)
        or not isinstance(governance, dict)
        or any(registration.get("authority", {}).values())
    ):
        raise IntegrityError("ES flow registration is invalid")
    program_path = root / str(program_binding.get("path", ""))
    specification = json.loads(program_path.read_bytes())
    if (
        program_binding.get("program_id") != specification.get("program_id")
        or program_binding.get("sha256") != sha256_file(program_path)
        or specification.get("program_id") != registration.get("protocol_id")
        or specification != build_es_flow_screen_program(root)
    ):
        raise IntegrityError("ES flow frozen program binding changed")
    readiness_core = {key: value for key, value in readiness.items() if key != "readiness_id"}
    if readiness.get("readiness_id") != sha256_json(readiness_core) or readiness_core != {
        "program_id": specification["program_id"],
        "sources_sha256": sha256_json(specification["sources"]),
        "implementation_sha256": sha256_json(specification["bindings"]["implementation"]),
        "historical_interval_sha256": sha256_json(specification["historical_interval"]),
    }:
        raise IntegrityError("ES flow source readiness binding changed")
    contract, profile = load_active_ladder(root)
    current_governance = scientific_governance_binding(root)
    if (
        ladder != {"contract_id": contract["contract_id"], "profile_id": profile["profile_id"], "stage": "pilot"}
        or governance.get("scientific_policy") != current_governance["scientific_policy"]
        or governance.get("legacy_attempts") != current_governance["legacy_attempts"]
    ):
        raise IntegrityError("ES flow scientific governance binding changed")
    years = specification["historical_interval"]
    return {
        "gateway_schema": CERTIFIED_GATEWAY_SCHEMA,
        "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
        "trial_id": str(registration["trial_id"]),
        "trial_family": str(registration["trial_family"]),
        "protocol_id": str(registration["protocol_id"]),
        "registration_path": relative.as_posix(),
        "registration_sha256": registration_sha256,
        "readiness_certificate_id": str(readiness["readiness_id"]),
        "readiness_evidence_sha256": registration_sha256,
        "alpha_ladder_contract_id": str(contract["contract_id"]),
        "alpha_ladder_profile_id": str(profile["profile_id"]),
        "alpha_ladder_stage": "pilot",
        "mechanism_sha256": str(program_binding["sha256"]),
        "predecessor_decision_sha256": str(registration["predecessor"]["sha256"]),
        "session_manifest_sha256": str(readiness["sources_sha256"]),
        "pilot_evaluation_session_ids_sha256": sha256_json({
            "screen_years": years["screen_years"],
            "wfa_years": years["wfa_years"],
            "outer_folds": specification["wfa"]["outer_folds"],
            "audit_sessions_per_fold": specification["wfa"]["audit_sessions_per_fold"],
        }),
        "scientific_policy_sha256": str(governance["scientific_policy"]["sha256"]),
        "global_trial_ledger_head": str(governance["global_trial_ledger"]["head"]),
        "conservative_legacy_attempt_count": str(governance["legacy_attempts"]["conservative_count"]),
        "intraday_authority_id": str(registration["trial_id"]),
        "markets": "ES",
        "years": ",".join(str(year) for year in years["source_years"]),
        "provider_access": "false",
        "holdout_or_forward_access": "false",
    }


def execute_es_flow_screen_once(
    root: Path,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    registration_path: Path,
    registration_sha256: str,
) -> dict[str, object]:
    """Consume one certified receipt, screen cheaply, then WFA only after a pass."""

    if type(receipt) is not OperationReceipt:
        raise IntegrityError("an actual OperationReceipt is required")
    root = boundary.assert_active_root(root)
    expected_scope = es_flow_execution_scope(root, registration_path, registration_sha256)
    registration = json.loads(registration_path.read_bytes())
    authority_id = str(registration["trial_id"])
    assert_authority_not_blocked(root, authority_id)
    observed_scope = dict(receipt.scope)
    if {
        key: value for key, value in observed_scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    } != expected_scope:
        raise IntegrityError("historical execution receipt is not bound to the frozen ES rescue")
    use_path = receipt.consume(
        boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=observed_scope,
    )
    use = json.loads(use_path.read_bytes())
    if not isinstance(use, dict) or use.get("receipt_id") != receipt.receipt_id:
        raise IntegrityError("consumed-use record failed verification")
    program_path = root / str(registration["program"]["path"])
    specification = json.loads(program_path.read_bytes())
    if specification.get("stage") != ES_FLOW_SCREEN_STAGE or specification.get("program_id") != registration.get("protocol_id"):
        raise IntegrityError("registration did not load the frozen ES flow screen")
    bars = _load_es_flow_screen_bars(root, boundary, specification)
    development_bars = [bar for bar in bars if int(bar.exchange_session_date[:4]) <= 2020]
    screen = evaluate_es_flow_screen(specification, development_bars)
    wfa = evaluate_es_flow_wfa(specification, bars, screen) if screen["decision"] == "CONTINUE_TO_HISTORICAL_WFA" else None
    official = {
        "schema_version": "intraday_research_official_historical/1.2.3",
        "authority_id": authority_id,
        "registration": {"path": str(registration_path), "sha256": registration_sha256},
        "consumed_use": {"path": str(use_path), "sha256": sha256_file(use_path)},
        "calculations": [screen] if wfa is None else [screen, wfa],
        "portfolio": None,
        "evidence_label": "READY_FOR_FORWARD_PAPER_TEST" if wfa is not None and wfa["decision"] == "READY_FOR_FORWARD_PAPER_TEST" else "NO_EDGE",
        "next_step_authorized": False,
    }
    official["official_result_id"] = sha256_json(official)
    output = root / "state/intraday_research_official_results" / str(official["official_result_id"]) / "result.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    expected = canonical_bytes(official) + b"\n"
    try:
        with output.open("xb") as stream:
            stream.write(expected)
    except FileExistsError:
        if output.read_bytes() != expected:
            raise IntegrityError("official result identity collision")
    return official


def execute_intraday_research_once(
    root: Path,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    registration_path: Path,
    registration_sha256: str,
) -> dict[str, object]:
    """Consume a real receipt before opening only registration-bound sources."""

    if type(receipt) is not OperationReceipt:
        raise IntegrityError("an actual OperationReceipt is required")
    root = boundary.assert_active_root(root)
    registration = json.loads(registration_path.read_bytes())
    if not isinstance(registration, dict) or sha256_file(registration_path) != registration_sha256:
        raise IntegrityError("registration binding mismatch")
    authority_id = str(registration.get("intraday_authority_id", ""))
    assert_authority_not_blocked(root, authority_id)
    gateway = CertifiedResearchGateway(root=root, boundary=boundary)
    use_path = gateway.claim_historical_execution(
        registration_path=registration_path,
        expected_registration_sha256=registration_sha256,
        receipt=receipt,
        additional_scope={"intraday_authority_id": authority_id},
    )
    use = json.loads(use_path.read_bytes())
    if not isinstance(use, dict) or use.get("receipt_id") != receipt.receipt_id:
        raise IntegrityError("consumed-use record failed verification")
    loaded = _load_registration_bound_inputs(root, registration)
    specification = loaded["specification"]
    if specification.get("schema_version") != PROGRAM_SCHEMA:
        raise IntegrityError("registration did not load the frozen program schema")
    calculations = []
    for program in specification["programs"]:
        name = str(program["program_name"])
        output_markets = set(program.get("markets", ())) or {"NQ", "ZB"}
        source_markets = output_markets if name != "CROSS_MARKET_INFORMATION_TRANSFER" else {"ES", "NQ", "ZB", "ZN"}
        materialized = materialize_program_inputs(
            specification,
            name,
            bars=[row for row in loaded["minute_rows"] if row.market in source_markets],
            one_second_bars=[row for row in loaded["second_rows"] if row.market in output_markets],
            stress_cost_usd=5.0,
            extreme_cost_usd=10.0,
            campaign_ids=("REGISTERED_HISTORY",),
            resamples=10_000,
        )
        calculations.append(evaluate_program_decision(specification, materialized))
    passing = [value for value in calculations if value["outcome"] == "PASS"]
    portfolio = evaluate_portfolio_decision(specification, calculations) if len(passing) >= 2 else None
    official = {
        "schema_version": "intraday_research_official_historical/1.2.3",
        "authority_id": authority_id,
        "registration": {"path": str(registration_path), "sha256": registration_sha256},
        "consumed_use": {"path": str(use_path), "sha256": sha256_file(use_path)},
        "calculations": calculations,
        "portfolio": portfolio,
        "evidence_label": "HISTORICAL_RESEARCH_EDGE" if passing or (portfolio is not None and portfolio["outcome"] == "PASS") else "NO_EDGE",
    }
    official["official_result_id"] = sha256_json(official)
    output = root / "state/intraday_research_official_results" / str(official["official_result_id"]) / "result.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    expected = canonical_bytes(official) + b"\n"
    try:
        with output.open("xb") as stream:
            stream.write(expected)
    except FileExistsError:
        if output.read_bytes() != expected:
            raise IntegrityError("official result identity collision")
    return official
