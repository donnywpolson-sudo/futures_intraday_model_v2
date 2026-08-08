"""Price-free exact dependency forensics for the cash-open readiness census.

This module reads only timestamp, disposition, identity, row-hash, and market-
spec columns.  It cannot fit a model, construct an outcome, or calculate P&L.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import verify_data_release_manifest
from .errors import IntegrityError, UnauthorizedOperation
from .historical_checkpoint_calendar import load_historical_checkpoint_calendar
from .runtime_environment import require_locked_repository_environment
from .tier1_bracket_v5 import (
    NS_PER_MINUTE,
    TRADABLE_DISPOSITIONS,
    load_registered_calendar_sessions_v5,
)


MARKETS = ("ES", "CL", "ZN", "6E")
YEARS = tuple(range(2018, 2023))
CHECKPOINTS = (time(9, 0), time(10, 30))
FEATURE_MINUTES = 30
EXECUTION_RECORDS = 31
CHICAGO = ZoneInfo("America/Chicago")
OPERATION = "AUDIT_CASH_OPEN_EXACT_LOCAL_DEPENDENCIES_ONCE"
PLAN_PATH = Path("configs/cash_open_impulse_dependency_forensics_plan.json")
MANIFEST_ROOT = Path("manifests/data_releases/causally_gated_normalized")
SPLIT_PATH = Path(
    "manifests/split_plans/tier1_core/"
    "1ef3d7de365833cb46e1a239759b018b1f85b5bfc7d342b291064f1efe66399b.json"
)
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/cash_open_impulse_dependency_forensics")
FORENSIC_COLUMNS = frozenset(
    {
        "actual_identity_hash",
        "disposition",
        "event_at_ns",
        "exchange_session_date",
        "point_value",
        "source_row_sha256",
        "tick_size",
        "tick_value",
    }
)


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid JSON artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"artifact is not an object: {path.as_posix()}")
    return value


def _hex64(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(character in "0123456789abcdef" for character in value)


@dataclass(frozen=True)
class ForensicRow:
    session: str
    event_at_ns: int
    disposition: str | None
    identity: str | None
    row_sha256: str
    market_spec: tuple[str, str, str] | None

    @property
    def executable(self) -> bool:
        return self.disposition in TRADABLE_DISPOSITIONS


def _row_from_columns(columns: Mapping[str, list[object]], index: int) -> ForensicRow:
    session = columns["exchange_session_date"][index]
    event = columns["event_at_ns"][index]
    disposition = columns["disposition"][index]
    identity = columns["actual_identity_hash"][index]
    row_hash = columns["source_row_sha256"][index]
    if (
        not isinstance(session, str)
        or not session.startswith(tuple(str(year) for year in YEARS))
        or type(event) is not int
        or not _hex64(row_hash)
    ):
        raise IntegrityError("forensic source row leaves the authorized scope")
    spec_values = tuple(
        columns[name][index] for name in ("tick_size", "tick_value", "point_value")
    )
    spec = None if any(value is None for value in spec_values) else tuple(
        str(value) for value in spec_values
    )
    return ForensicRow(
        session=session,
        event_at_ns=event,
        disposition=disposition if isinstance(disposition, str) else None,
        identity=identity if isinstance(identity, str) else None,
        row_sha256=str(row_hash),
        market_spec=spec,
    )


def iter_forensic_rows(path: Path) -> Iterable[ForensicRow]:
    """Read no OHLCV values; only dependency identity and timing columns."""

    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    if not FORENSIC_COLUMNS.issubset(parquet.schema_arrow.names):
        raise IntegrityError("forensic source lacks required dependency columns")
    for batch in parquet.iter_batches(
        batch_size=65_536, columns=sorted(FORENSIC_COLUMNS)
    ):
        columns = batch.to_pydict()
        for index in range(batch.num_rows):
            yield _row_from_columns(columns, index)


def _clock(event_at_ns: int) -> time:
    seconds, remainder = divmod(event_at_ns, 1_000_000_000)
    value = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(CHICAGO)
    return value.time().replace(microsecond=remainder // 1_000)


def _expected_clocks(checkpoint: time) -> tuple[tuple[time, ...], tuple[time, ...]]:
    minute = checkpoint.hour * 60 + checkpoint.minute
    feature = tuple(time(*divmod(minute - offset, 60)) for offset in range(30, 0, -1))
    execution = tuple(
        time(*divmod(minute + offset, 60)) for offset in range(1, 32)
    )
    return feature, execution


def _clock_text(value: time) -> str:
    return f"{value.hour:02d}:{value.minute:02d}"


def classify_checkpoint_exact(
    *, market: str, session: str, checkpoint: time, rows: Sequence[ForensicRow]
) -> dict[str, object]:
    """Return every exact terminal dependency reason for one checkpoint."""

    if market not in MARKETS or not session.startswith(tuple(str(y) for y in YEARS)):
        raise UnauthorizedOperation("forensic checkpoint leaves authorized scope")
    by_clock: dict[time, list[ForensicRow]] = {}
    for row in rows:
        by_clock.setdefault(_clock(row.event_at_ns), []).append(row)
    feature_clocks, execution_clocks = _expected_clocks(checkpoint)
    failures: list[dict[str, object]] = []
    selected: dict[time, ForensicRow] = {}
    for role, clocks in (("FEATURE", feature_clocks), ("EXECUTION", execution_clocks)):
        for expected_clock in clocks:
            observed = by_clock.get(expected_clock, [])
            executable = [row for row in observed if row.executable]
            base = {"role": role, "clock_chicago": _clock_text(expected_clock)}
            if not observed:
                failures.append({**base, "reason": "MISSING_MINUTE"})
            elif len(executable) > 1:
                failures.append({
                    **base,
                    "reason": "DUPLICATE_EXECUTABLE_MINUTE",
                    "row_hashes": sorted(row.row_sha256 for row in executable),
                })
            elif not executable:
                failures.append({
                    **base,
                    "reason": "NON_EXECUTABLE_DISPOSITION",
                    "dispositions": sorted({row.disposition or "MISSING" for row in observed}),
                    "row_hashes": sorted(row.row_sha256 for row in observed),
                })
            else:
                selected[expected_clock] = executable[0]

    feature_rows = [selected[item] for item in feature_clocks if item in selected]
    execution_rows = [selected[item] for item in execution_clocks if item in selected]
    decision_ns: int | None = None
    if len(feature_rows) == FEATURE_MINUTES:
        decision_ns = feature_rows[-1].event_at_ns + NS_PER_MINUTE + 5_000_000_000
        late = [
            row for row in feature_rows
            if row.event_at_ns + NS_PER_MINUTE + 5_000_000_000 > decision_ns
        ]
        if late:
            failures.append({
                "role": "FEATURE",
                "reason": "LATE_AVAILABILITY",
                "row_hashes": sorted(row.row_sha256 for row in late),
            })
        identities = {row.identity for row in feature_rows}
        if None in identities:
            failures.append({"role": "FEATURE", "reason": "MISSING_IDENTITY"})
        elif len(identities) != 1:
            failures.append({
                "role": "FEATURE",
                "reason": "IDENTITY_CHANGE",
                "identity_hashes": sorted(str(value) for value in identities),
            })
    if len(execution_rows) == EXECUTION_RECORDS:
        if decision_ns is None or decision_ns >= execution_rows[0].event_at_ns:
            failures.append({"role": "EXECUTION", "reason": "ENTRY_NOT_AFTER_DECISION"})
        identities = {row.identity for row in execution_rows}
        if None in identities:
            failures.append({"role": "EXECUTION", "reason": "MISSING_IDENTITY"})
        elif len(identities) != 1:
            failures.append({
                "role": "EXECUTION",
                "reason": "IDENTITY_CHANGE",
                "identity_hashes": sorted(str(value) for value in identities),
            })
        specs = {row.market_spec for row in execution_rows}
        if None in specs:
            failures.append({"role": "EXECUTION", "reason": "MISSING_MARKET_SPEC"})
        elif len(specs) != 1:
            failures.append({
                "role": "EXECUTION",
                "reason": "MARKET_SPEC_CHANGE",
                "market_specs": sorted("|".join(value) for value in specs if value is not None),
            })
    if len(feature_rows) == FEATURE_MINUTES and len(execution_rows) == EXECUTION_RECORDS:
        identities = {row.identity for row in (*feature_rows, *execution_rows)}
        if None not in identities and len(identities) != 1:
            failures.append({
                "role": "COMBINED",
                "reason": "ROLL_OR_IDENTITY_CHANGE_BETWEEN_FEATURE_AND_EXECUTION",
                "identity_hashes": sorted(str(value) for value in identities),
            })
    return {
        "market": market,
        "session": session,
        "checkpoint": _clock_text(checkpoint),
        "complete": not failures,
        "failures": failures,
    }


def _scan_release(
    *, market: str, path: Path, expected_sessions: Sequence[str],
    only_keys: set[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], dict[str, object]]:
    expected = set(expected_sessions)
    output: dict[tuple[str, str], dict[str, object]] = {}
    active_session: str | None = None
    rows: list[ForensicRow] = []
    observed: set[str] = set()

    def flush() -> None:
        nonlocal rows
        if active_session is None:
            return
        if active_session in observed:
            raise IntegrityError("forensic source session is not contiguous")
        observed.add(active_session)
        if active_session in expected:
            for checkpoint in CHECKPOINTS:
                key = (active_session, _clock_text(checkpoint))
                if only_keys is None or key in only_keys:
                    output[key] = classify_checkpoint_exact(
                        market=market, session=active_session,
                        checkpoint=checkpoint, rows=tuple(rows),
                    )
        rows = []

    for row in iter_forensic_rows(path):
        if active_session is None:
            active_session = row.session
        elif row.session != active_session:
            flush()
            active_session = row.session
        rows.append(row)
        if len(rows) > 2_000:
            raise IntegrityError("forensic session buffer exceeded 2,000 rows")
    flush()
    requested = (
        {(session, _clock_text(checkpoint)) for session in expected for checkpoint in CHECKPOINTS}
        if only_keys is None else only_keys
    )
    for session, checkpoint_text in sorted(requested):
        if (session, checkpoint_text) not in output:
            output[(session, checkpoint_text)] = {
                "market": market,
                "session": session,
                "checkpoint": checkpoint_text,
                "complete": False,
                "failures": [{"role": "SESSION", "reason": "MISSING_SOURCE_SESSION"}],
            }
    return output


def _candidate_catalog(root: Path, boundary: RepoBoundary) -> dict[tuple[str, int], list[dict[str, str]]]:
    output: dict[tuple[str, int], list[dict[str, str]]] = {}
    for manifest_path in sorted((root / MANIFEST_ROOT).glob("*.json")):
        raw = _object(manifest_path)
        metadata = raw.get("metadata")
        if not isinstance(metadata, Mapping):
            continue
        market, year = metadata.get("market"), metadata.get("year")
        if market not in MARKETS or type(year) is not int or year not in YEARS:
            continue
        manifest = verify_data_release_manifest(manifest_path, boundary)
        bars = [entry for entry in manifest.files if Path(entry.logical_path).name == "bars.parquet"]
        if len(bars) != 1:
            raise IntegrityError("causal alternative has ambiguous bars payload")
        path = root / manifest.physical_relative_path(bars[0])
        if sha256_file(path) != bars[0].sha256:
            raise IntegrityError("causal alternative payload hash changed")
        output.setdefault((str(market), int(year)), []).append({
            "release_id": manifest.release_id,
            "manifest_path": manifest_path.relative_to(root).as_posix(),
            "manifest_sha256": sha256_file(manifest_path),
            "payload_path": path.relative_to(root).as_posix(),
            "payload_sha256": bars[0].sha256,
        })
    if set(output) != {(market, year) for market in MARKETS for year in YEARS}:
        raise IntegrityError("local alternative catalog does not cover all 20 pairs")
    return output


def _active_bindings(root: Path) -> dict[tuple[str, int], dict[str, str]]:
    split = _object(root / SPLIT_PATH)
    catalog = _object(root / ACTIVE_CATALOG_PATH)
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    active_entries = {
        (str(item["market"]), int(item["year"])): item
        for item in entries if isinstance(item, Mapping)
        and item.get("market") in MARKETS and item.get("year") in YEARS
    }
    pairs = split.get("input_pairs")
    if not isinstance(pairs, list):
        raise IntegrityError("split source pairs are absent")
    output: dict[tuple[str, int], dict[str, str]] = {}
    for item in pairs:
        if not isinstance(item, Mapping):
            raise IntegrityError("split source pair is invalid")
        key = (str(item["market"]), int(item["year"]))
        active = active_entries.get(key)
        if not isinstance(active, Mapping):
            raise IntegrityError("active source binding is absent")
        path = root / str(active["parquet_path"])
        digest = str(item["source_parquet_sha256"])
        if active.get("parquet_sha256") != digest or sha256_file(path) != digest:
            raise IntegrityError("active source differs from split and catalog")
        source_bindings = active.get("source_bindings")
        if not isinstance(source_bindings, list) or len(source_bindings) != 1:
            raise IntegrityError("active causal release binding is ambiguous")
        output[key] = {
            "release_id": str(source_bindings[0]["causal_release_id"]),
            "payload_path": path.relative_to(root).as_posix(),
            "payload_sha256": digest,
        }
    if set(output) != {(market, year) for market in MARKETS for year in YEARS}:
        raise IntegrityError("active binding scope is incomplete")
    return output


def _market_worker(payload: Mapping[str, object]) -> dict[str, object]:
    root = Path(str(payload["root"]))
    market = str(payload["market"])
    expected_by_year = payload["expected_by_year"]
    active = payload["active"]
    alternatives = payload["alternatives"]
    if not isinstance(expected_by_year, Mapping) or not isinstance(active, Mapping) or not isinstance(alternatives, Mapping):
        raise IntegrityError("forensic worker payload is invalid")
    active_failures: list[dict[str, object]] = []
    comparisons: list[dict[str, object]] = []
    release_summaries: list[dict[str, object]] = []
    for year in YEARS:
        sessions = tuple(str(item) for item in expected_by_year[str(year)])
        active_item = active[str(year)]
        active_results = _scan_release(
            market=market, path=root / str(active_item["payload_path"]),
            expected_sessions=sessions,
        )
        failed = {key: value for key, value in active_results.items() if not value["complete"]}
        for value in failed.values():
            active_failures.append({"year": year, **value})
        reason_counts = Counter(
            str(failure["reason"])
            for value in failed.values() for failure in value["failures"]
        )
        for candidate in alternatives[str(year)]:
            candidate_results = _scan_release(
                market=market, path=root / str(candidate["payload_path"]),
                expected_sessions=sessions, only_keys=set(failed),
            )
            resolved = 0
            candidate_reason_counts: Counter[str] = Counter()
            exact: list[dict[str, object]] = []
            for key, active_value in sorted(failed.items()):
                candidate_value = candidate_results[key]
                resolved += bool(candidate_value["complete"])
                candidate_reason_counts.update(
                    str(item["reason"]) for item in candidate_value["failures"]
                )
                exact.append({
                    "session": key[0], "checkpoint": key[1],
                    "active_failures": active_value["failures"],
                    "candidate_complete": candidate_value["complete"],
                    "candidate_failures": candidate_value["failures"],
                })
            comparisons.append({
                "market": market, "year": year,
                "active_release_id": active_item["release_id"],
                "candidate_release_id": candidate["release_id"],
                "candidate_payload_sha256": candidate["payload_sha256"],
                "active_failed_checkpoints": len(failed),
                "resolved_checkpoints": resolved,
                "candidate_failure_reason_counts": dict(sorted(candidate_reason_counts.items())),
                "exact_checkpoint_comparison": exact,
            })
        release_summaries.append({
            "market": market, "year": year,
            "active_release_id": active_item["release_id"],
            "active_payload_sha256": active_item["payload_sha256"],
            "expected_sessions": len(sessions),
            "expected_checkpoints": len(sessions) * len(CHECKPOINTS),
            "failed_checkpoints": len(failed),
            "failure_reason_counts": dict(sorted(reason_counts.items())),
            "local_alternative_count": len(alternatives[str(year)]),
        })
    return {
        "market": market,
        "active_failures": active_failures,
        "release_summaries": release_summaries,
        "alternative_comparisons": comparisons,
    }


def load_plan(root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    bindings = plan.get("bindings")
    if (
        plan_id != sha256_json(core)
        or plan.get("operation") != OPERATION
        or plan.get("maximum_runtime_seconds") != 900
        or plan.get("maximum_workers") != 4
        or plan.get("external_cost_usd") != "0"
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("cash-open dependency-forensics plan drifted")
    return plan


def required_scope(root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "source_scope": "ES,CL,ZN,6E|2018,2019,2020,2021,2022",
        "local_alternatives_only": "true",
        "price_values_in_output": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "historical_evaluation": "false",
        "publication": "false",
        "provider_access": "false",
        "holdout_2025_access": "false",
        "maximum_runtime_seconds": "900",
        "external_cost_usd": "0",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _calendar_mismatch(root: Path) -> list[dict[str, object]]:
    split = _object(root / SPLIT_PATH)
    boundary = RepoBoundary(root)
    sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(
            _object(root / "configs/tier1_historical_checkpoint_calendar_v5.json")[
                "calendar_index_receipt"
            ]["release_id"]
        ),
    )
    by_market = {
        market: {
            item.exchange_session_date
            for item in sessions if item.market == market
            and item.checkpoint_states is not None
            and item.checkpoint_states.get("08:30") is True
            and item.checkpoint_states.get("10:30") is True
        }
        for market in MARKETS
    }
    schedule = tuple(str(value) for value in split["session_dates"])
    output: list[dict[str, object]] = []
    for fold_index, fold in enumerate(split["outer_folds"]):
        start, end = fold["outer_test_session_dates"]
        scheduled = [item for item in schedule if start <= item <= end]
        for market in MARKETS:
            excluded = [item for item in scheduled if item not in by_market[market]]
            if excluded:
                output.append({
                    "fold_id": f"fold-{fold_index}", "market": market,
                    "scheduled_sessions": len(scheduled),
                    "mechanism_eligible_sessions": len(scheduled) - len(excluded),
                    "calendar_ineligible_dates": excluded,
                })
    return output


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan(root)
    output = root / OUTPUT_ROOT / str(plan["plan_id"]) / "dependency_forensics.json"
    if output.exists():
        raise UnauthorizedOperation("dependency-forensics evidence already exists")
    use_path = receipt.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root, plan),
    )
    active = _active_bindings(root)
    alternatives = _candidate_catalog(root, boundary)
    calendar_sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    expected: dict[str, dict[str, list[str]]] = {
        market: {str(year): [] for year in YEARS} for market in MARKETS
    }
    for item in calendar_sessions:
        if (
            item.market in MARKETS
            and item.checkpoint_states is not None
            and item.checkpoint_states.get("08:30") is True
            and item.checkpoint_states.get("10:30") is True
            and int(item.exchange_session_date[:4]) in YEARS
        ):
            expected[item.market][item.exchange_session_date[:4]].append(
                item.exchange_session_date
            )
    payloads = []
    for market in MARKETS:
        payloads.append({
            "root": str(root), "market": market,
            "expected_by_year": expected[market],
            "active": {str(year): active[(market, year)] for year in YEARS},
            "alternatives": {
                str(year): alternatives[(market, year)] for year in YEARS
            },
        })
    market_results: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_market_worker, item): item["market"] for item in payloads}
        for future in as_completed(futures, timeout=780):
            market_results.append(future.result())
    if len(market_results) != len(MARKETS):
        raise IntegrityError("forensic worker result is incomplete")
    if monotonic() - started > 895:
        raise UnauthorizedOperation("forensic runtime exhausted before evidence sealing")
    reason_counts: Counter[str] = Counter()
    total_failed = 0
    total_resolved = 0
    for result in market_results:
        for failure in result["active_failures"]:
            total_failed += 1
            reason_counts.update(str(item["reason"]) for item in failure["failures"])
        total_resolved += sum(
            int(item["resolved_checkpoints"])
            for item in result["alternative_comparisons"]
        )
    core: dict[str, object] = {
        "schema_version": "cash_open_impulse_dependency_forensics/1.0.0",
        "state": "UNPUBLISHED_READ_ONLY_SOURCE_FORENSICS",
        "plan_id": plan["plan_id"],
        "protocol_id": plan["protocol_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "source_selection": {
            "active_contract_path": ACTIVE_CATALOG_PATH.as_posix(),
            "active_view_intentionally_used": True,
            "immutable_alternative_manifest_root": MANIFEST_ROOT.as_posix(),
            "active_pair_count": len(active),
            "alternative_release_count": sum(len(value) for value in alternatives.values()),
        },
        "calendar_fold_mismatches": _calendar_mismatch(root),
        "active_failure_checkpoint_count": total_failed,
        "active_failure_reason_counts": dict(sorted(reason_counts.items())),
        "alternative_resolution_instances": total_resolved,
        "market_results": sorted(market_results, key=lambda item: str(item["market"])),
        "output_contains_price_values": False,
        "model_fit": False,
        "prediction_generation": False,
        "returns_or_performance_computed": False,
        "historical_evaluation": False,
        "holdout_2025_access": False,
        "provider_network_credentials": False,
        "publication": False,
        "active_data_mutation": False,
        "trading": False,
    }
    report = {**core, "report_id": sha256_json(core)}
    output.parent.mkdir(parents=True, exist_ok=False)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    return report
