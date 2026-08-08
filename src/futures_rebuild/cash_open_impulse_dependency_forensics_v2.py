"""Sessionless-row-safe successor for cash-open dependency forensics."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from time import monotonic

from . import cash_open_impulse_dependency_forensics as v1
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .tier1_bracket_v5 import NS_PER_MINUTE, TRADABLE_DISPOSITIONS, load_registered_calendar_sessions_v5


OPERATION = "AUDIT_CASH_OPEN_EXACT_LOCAL_DEPENDENCIES_SESSIONLESS_SAFE_ONCE"
PLAN_PATH = Path("configs/cash_open_impulse_dependency_forensics_v2_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/cash_open_impulse_dependency_forensics_v2")


def _raw_row(columns: Mapping[str, list[object]], index: int) -> dict[str, object]:
    row = {name: values[index] for name, values in columns.items()}
    session, event, row_hash = (
        row["exchange_session_date"], row["event_at_ns"], row["source_row_sha256"]
    )
    if (
        session is not None and (
            not isinstance(session, str)
            or not session.startswith(tuple(str(year) for year in v1.YEARS))
        )
    ) or type(event) is not int or not v1._hex64(row_hash):
        raise IntegrityError("V2 forensic source row leaves the authorized scope")
    return row


def _normalized(row: Mapping[str, object], session: str) -> v1.ForensicRow:
    disposition = row["disposition"]
    identity = row["actual_identity_hash"]
    spec_values = tuple(row[name] for name in ("tick_size", "tick_value", "point_value"))
    spec = None if any(value is None for value in spec_values) else tuple(
        str(value) for value in spec_values
    )
    return v1.ForensicRow(
        session=session,
        event_at_ns=int(row["event_at_ns"]),
        disposition=disposition if isinstance(disposition, str) else None,
        identity=identity if isinstance(identity, str) else None,
        row_sha256=str(row["source_row_sha256"]),
        market_spec=spec,
    )


def iter_forensic_rows_v2(path: Path) -> Iterable[v1.ForensicRow]:
    """Causally attach sessionless nontradable defects using exact neighbors."""

    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    if not v1.FORENSIC_COLUMNS.issubset(parquet.schema_arrow.names):
        raise IntegrityError("V2 forensic source lacks required dependency columns")
    previous: v1.ForensicRow | None = None
    pending: list[dict[str, object]] = []
    for batch in parquet.iter_batches(
        batch_size=65_536, columns=sorted(v1.FORENSIC_COLUMNS)
    ):
        columns = batch.to_pydict()
        for index in range(batch.num_rows):
            raw = _raw_row(columns, index)
            session = raw["exchange_session_date"]
            disposition = raw["disposition"]
            if session is None:
                if disposition in TRADABLE_DISPOSITIONS:
                    raise IntegrityError("tradable V2 forensic row lacks session identity")
                pending.append(raw)
                continue
            assert isinstance(session, str)
            current = _normalized(raw, session)
            if pending:
                if previous is None or previous.session != current.session:
                    raise IntegrityError("V2 cannot causally attach sessionless source defect")
                expected_event = previous.event_at_ns + NS_PER_MINUTE
                for orphan in pending:
                    if int(orphan["event_at_ns"]) != expected_event:
                        raise IntegrityError("V2 sessionless defect is not minute-contiguous")
                    normalized = _normalized(orphan, current.session)
                    yield normalized
                    previous = normalized
                    expected_event += NS_PER_MINUTE
                if current.event_at_ns != expected_event:
                    raise IntegrityError("V2 sessionless defect lacks matching causal neighbors")
                pending.clear()
            yield current
            previous = current
    if pending:
        raise IntegrityError("V2 forensic stream ends with unresolved sessionless defect")


def _scan_release_v2(
    *, market: str, path: Path, expected_sessions: Sequence[str],
    only_keys: set[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], dict[str, object]]:
    expected = set(expected_sessions)
    output: dict[tuple[str, str], dict[str, object]] = {}
    active_session: str | None = None
    rows: list[v1.ForensicRow] = []
    observed: set[str] = set()

    def flush() -> None:
        nonlocal rows
        if active_session is None:
            return
        if active_session in observed:
            raise IntegrityError("V2 forensic source session is not contiguous")
        observed.add(active_session)
        if active_session in expected:
            for checkpoint in v1.CHECKPOINTS:
                key = (active_session, v1._clock_text(checkpoint))
                if only_keys is None or key in only_keys:
                    output[key] = v1.classify_checkpoint_exact(
                        market=market, session=active_session,
                        checkpoint=checkpoint, rows=tuple(rows),
                    )
        rows = []

    for row in iter_forensic_rows_v2(path):
        if active_session is None:
            active_session = row.session
        elif row.session != active_session:
            flush()
            active_session = row.session
        rows.append(row)
        if len(rows) > 2_000:
            raise IntegrityError("V2 forensic session buffer exceeded 2,000 rows")
    flush()
    requested = (
        {(session, v1._clock_text(checkpoint)) for session in expected for checkpoint in v1.CHECKPOINTS}
        if only_keys is None else only_keys
    )
    for session, checkpoint_text in sorted(requested):
        if (session, checkpoint_text) not in output:
            output[(session, checkpoint_text)] = {
                "market": market, "session": session, "checkpoint": checkpoint_text,
                "complete": False,
                "failures": [{"role": "SESSION", "reason": "MISSING_SOURCE_SESSION"}],
            }
    return output


def _market_worker_v2(payload: Mapping[str, object]) -> dict[str, object]:
    root, market = Path(str(payload["root"])), str(payload["market"])
    expected_by_year, active, alternatives = (
        payload["expected_by_year"], payload["active"], payload["alternatives"]
    )
    if not all(isinstance(value, Mapping) for value in (expected_by_year, active, alternatives)):
        raise IntegrityError("V2 forensic worker payload is invalid")
    active_failures: list[dict[str, object]] = []
    comparisons: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []
    for year in v1.YEARS:
        sessions = tuple(str(item) for item in expected_by_year[str(year)])
        active_item = active[str(year)]
        active_results = _scan_release_v2(
            market=market, path=root / str(active_item["payload_path"]),
            expected_sessions=sessions,
        )
        failed = {key: value for key, value in active_results.items() if not value["complete"]}
        active_failures.extend({"year": year, **value} for value in failed.values())
        active_counts = Counter(
            str(failure["reason"]) for value in failed.values() for failure in value["failures"]
        )
        for candidate in alternatives[str(year)]:
            candidate_results = _scan_release_v2(
                market=market, path=root / str(candidate["payload_path"]),
                expected_sessions=sessions, only_keys=set(failed),
            )
            candidate_counts: Counter[str] = Counter()
            exact = []
            resolved = 0
            for key, active_value in sorted(failed.items()):
                candidate_value = candidate_results[key]
                resolved += bool(candidate_value["complete"])
                candidate_counts.update(str(item["reason"]) for item in candidate_value["failures"])
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
                "candidate_failure_reason_counts": dict(sorted(candidate_counts.items())),
                "exact_checkpoint_comparison": exact,
            })
        summaries.append({
            "market": market, "year": year,
            "active_release_id": active_item["release_id"],
            "active_payload_sha256": active_item["payload_sha256"],
            "expected_sessions": len(sessions),
            "expected_checkpoints": len(sessions) * len(v1.CHECKPOINTS),
            "failed_checkpoints": len(failed),
            "failure_reason_counts": dict(sorted(active_counts.items())),
            "local_alternative_count": len(alternatives[str(year)]),
        })
    return {
        "market": market, "active_failures": active_failures,
        "release_summaries": summaries, "alternative_comparisons": comparisons,
    }


def load_plan_v2(root: Path) -> dict[str, object]:
    plan = v1._object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    bindings, predecessor = plan.get("bindings"), plan.get("consumed_predecessor")
    if (
        plan_id != sha256_json(core) or plan.get("operation") != OPERATION
        or plan.get("maximum_runtime_seconds") != 900
        or plan.get("maximum_workers") != 4 or plan.get("external_cost_usd") != "0"
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
        or not isinstance(predecessor, Mapping)
        or predecessor.get("authorization_consumed") is not True
        or predecessor.get("report_created") is not False
        or predecessor.get("retry_authorized") is not False
    ):
        raise UnauthorizedOperation("V2 dependency-forensics plan drifted")
    return plan


def required_scope_v2(root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    scope = v1.required_scope(root, plan)
    scope["approval_command"] = OPERATION
    scope["approval_plan_id"] = str(plan["plan_id"])
    scope["approval_plan_sha256"] = sha256_file(root / PLAN_PATH)
    return scope


def execute_once_v2(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan_v2(root)
    output = root / OUTPUT_ROOT / str(plan["plan_id"]) / "dependency_forensics.json"
    if output.exists():
        raise UnauthorizedOperation("V2 dependency-forensics evidence already exists")
    use_path = receipt.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope_v2(root, plan),
    )
    active = v1._active_bindings(root)
    alternatives = v1._candidate_catalog(root, boundary)
    calendar_sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    expected = {market: {str(year): [] for year in v1.YEARS} for market in v1.MARKETS}
    for item in calendar_sessions:
        if (
            item.market in v1.MARKETS and item.checkpoint_states is not None
            and item.checkpoint_states.get("08:30") is True
            and item.checkpoint_states.get("10:30") is True
            and int(item.exchange_session_date[:4]) in v1.YEARS
        ):
            expected[item.market][item.exchange_session_date[:4]].append(item.exchange_session_date)
    payloads = [{
        "root": str(root), "market": market, "expected_by_year": expected[market],
        "active": {str(year): active[(market, year)] for year in v1.YEARS},
        "alternatives": {str(year): alternatives[(market, year)] for year in v1.YEARS},
    } for market in v1.MARKETS]
    results = []
    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_market_worker_v2, payload) for payload in payloads]
        for future in as_completed(futures, timeout=780):
            results.append(future.result())
    if len(results) != len(v1.MARKETS):
        raise IntegrityError("V2 forensic worker result is incomplete")
    if monotonic() - started > 895:
        raise UnauthorizedOperation("V2 forensic runtime exhausted before sealing")
    counts: Counter[str] = Counter()
    failures = 0
    for result in results:
        for failure in result["active_failures"]:
            failures += 1
            counts.update(str(item["reason"]) for item in failure["failures"])
    core = {
        "schema_version": "cash_open_impulse_dependency_forensics/2.0.0",
        "state": "UNPUBLISHED_READ_ONLY_SOURCE_FORENSICS",
        "plan_id": plan["plan_id"], "protocol_id": plan["protocol_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "consumed_predecessor": plan["consumed_predecessor"],
        "source_selection": {
            "active_contract_path": v1.ACTIVE_CATALOG_PATH.as_posix(),
            "active_view_intentionally_used": True,
            "immutable_alternative_manifest_root": v1.MANIFEST_ROOT.as_posix(),
            "active_pair_count": len(active),
            "alternative_release_count": sum(len(value) for value in alternatives.values()),
        },
        "calendar_fold_mismatches": v1._calendar_mismatch(root),
        "active_failure_checkpoint_count": failures,
        "active_failure_reason_counts": dict(sorted(counts.items())),
        "alternative_resolution_instances": sum(
            int(item["resolved_checkpoints"])
            for result in results for item in result["alternative_comparisons"]
        ),
        "market_results": sorted(results, key=lambda item: str(item["market"])),
        "sessionless_nontradable_rows_causally_normalized": True,
        "output_contains_price_values": False,
        "model_fit": False, "prediction_generation": False,
        "returns_or_performance_computed": False, "historical_evaluation": False,
        "holdout_2025_access": False, "provider_network_credentials": False,
        "publication": False, "active_data_mutation": False, "trading": False,
    }
    report = {**core, "report_id": sha256_json(core)}
    output.parent.mkdir(parents=True, exist_ok=False)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    return report
