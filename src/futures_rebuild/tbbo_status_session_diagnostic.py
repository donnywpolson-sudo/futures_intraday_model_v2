"""Diagnose false TBBO session evidence without exposing market values."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from time import monotonic
from typing import Final

import numpy as np

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment
from . import tbbo_quality_certification as quality


OPERATION: Final = "DIAGNOSE_TBBO_ES_STATUS_SESSION_EVIDENCE_V1_ONCE"
MARKET: Final = "ES"
OUTPUT_ROOT: Final = Path("state/tbbo_status_session_diagnostics")
LOCK_PATH: Final = Path("state/locks/tbbo_es_status_session_diagnostic_v1.lock")
MAXIMUM_OUTPUT_BYTES: Final = 1_000_000


def _load_failure(*, root: Path, path: Path, preparation_id: str) -> dict[str, object]:
    failure = quality.json_object(path, "TBBO quality failure")
    core = dict(failure)
    observed_id = core.pop("failure_id", None)
    results = failure.get("market_results")
    if (
        observed_id != sha256_json(core)
        or failure.get("state") != "QUALITY_CERTIFICATION_FAILED_NOT_CATALOG_ELIGIBLE"
        or failure.get("preparation_id") != preparation_id
        or not isinstance(results, list)
    ):
        raise IntegrityError("TBBO quality failure identity is invalid")
    return failure


def _missing_sessions(failure: Mapping[str, object]) -> tuple[str, ...]:
    results = failure.get("market_results")
    if not isinstance(results, list):
        raise IntegrityError("TBBO quality failure has no market results")
    for item in results:
        if isinstance(item, Mapping) and item.get("market") == MARKET:
            missing = item.get("missing_empirical_sessions")
            if not isinstance(missing, list) or not missing:
                raise IntegrityError("ES has no missing empirical sessions to diagnose")
            return tuple(sorted(str(value) for value in missing))
    raise IntegrityError("TBBO quality failure has no ES result")


def required_scope(
    *, root: Path, preparation: Mapping[str, object], failure_path: Path,
) -> dict[str, str]:
    return {
        "preparation_id": str(preparation["preparation_id"]),
        "preparation_sha256": sha256_file(root / str(preparation["preparation_path"])),
        "failure_path": failure_path.relative_to(root).as_posix(),
        "failure_sha256": sha256_file(failure_path),
        "market": MARKET,
        "schemas": "status",
        "provider_calls": "0",
        "decoded_dataset_creation": "false",
        "raw_market_value_output": "false",
        "implementation_sha256": sha256_file(Path(__file__)),
        "approval_command": OPERATION,
        "approval_plan_id": str(preparation["preparation_id"]),
        "approval_plan_sha256": sha256_file(failure_path),
    }


def _window(received: int) -> str:
    offset = received % quality.UTC_DAY_NS
    if offset == 0:
        return "EXACT_UTC_MIDNIGHT"
    if offset < 1_000_000_000:
        return "FIRST_UTC_SECOND"
    if offset < 60_000_000_000:
        return "FIRST_UTC_MINUTE"
    if offset < 3_600_000_000_000:
        return "FIRST_UTC_HOUR"
    return "OTHER_UTC_TIME"


def _age(received: int, previous_received: int | None) -> str:
    if previous_received is None:
        return "NO_PRIOR_RECORD"
    value = received - previous_received
    if value < 60_000_000_000:
        return "UNDER_ONE_MINUTE"
    if value < 3_600_000_000_000:
        return "UNDER_ONE_HOUR"
    if value < quality.UTC_DAY_NS:
        return "UNDER_ONE_DAY"
    return "AT_LEAST_ONE_DAY"


def execute_authorized_diagnostic(
    *,
    root: Path,
    preparation_id: str,
    failure_path: Path,
    authorization: OperationReceipt,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    chunk_loader: Callable[[Path], Iterable[np.ndarray]] = quality._default_chunk_loader,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    failure_path = failure_path.resolve(strict=False)
    if not io_path(failure_path).is_file():
        raise IntegrityError("TBBO quality failure path is unavailable")
    preparation = quality.load_preparation(root=root, preparation_id=preparation_id)
    failure = _load_failure(
        root=root, path=failure_path, preparation_id=preparation_id,
    )
    targets = frozenset(_missing_sessions(failure))
    environment_check(root)
    full_scope = required_scope(
        root=root, preparation=preparation, failure_path=failure_path,
    )
    lock = SingleInstance(root / LOCK_PATH)
    if not lock.acquire():
        raise UnauthorizedOperation("another TBBO status diagnostic is running")
    try:
        authorization.consume(
            RepoBoundary(root),
            operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=full_scope,
        )
        counts: dict[str, dict[str, object]] = {
            session: {
                "session": session,
                "row_count": 0,
                "trading_or_quoting_row_count": 0,
                "receive_window_counts": {},
                "event_relation_counts": {},
                "prior_state_relation_counts": {},
                "prior_record_age_counts": {},
            }
            for session in sorted(targets)
        }
        previous: dict[int, tuple[int, tuple[str, ...]]] = {}
        rows_scanned = 0
        prior_timestamp: int | None = None
        for path, expected_sha in quality._support_sources(
            preparation, root=root, market=MARKET, schema="status",
        ):
            for raw_chunk in quality._bound_file(
                path, expected_sha256=expected_sha, chunk_loader=chunk_loader,
            ):
                chunk = quality._verify_chunk(raw_chunk, {quality.STATUS_DTYPE}, "status")
                prior_timestamp = quality._source_order(
                    chunk, prior=prior_timestamp, label="status diagnostic",
                )
                for row in chunk:
                    received = int(row["ts_recv"])
                    event = int(row["ts_event"])
                    instrument_id = int(row["instrument_id"])
                    state = tuple(
                        quality._char(row[name])
                        for name in (
                            "action", "reason", "trading_event", "is_trading",
                            "is_quoting", "is_short_sell_restricted", "_reserved",
                        )
                    )
                    prior = previous.get(instrument_id)
                    session = quality._trade_session(received)
                    if session in targets:
                        item = counts[session]
                        item["row_count"] = int(item["row_count"]) + 1
                        if quality._char(row["is_trading"]) == "Y" or quality._char(
                            row["is_quoting"]
                        ) == "Y":
                            item["trading_or_quoting_row_count"] = (
                                int(item["trading_or_quoting_row_count"]) + 1
                            )
                        day_start = received - received % quality.UTC_DAY_NS
                        relation = (
                            "EVENT_BEFORE_RECEIVE_UTC_DAY"
                            if event < day_start
                            else "EVENT_EQUALS_RECEIVE"
                            if event == received
                            else "EVENT_SAME_RECEIVE_UTC_DAY"
                        )
                        prior_relation = (
                            "NO_PRIOR_STATE"
                            if prior is None
                            else "STATE_EQUALS_PRIOR"
                            if state == prior[1]
                            else "STATE_DIFFERS_FROM_PRIOR"
                        )
                        for key, value in (
                            ("receive_window_counts", _window(received)),
                            ("event_relation_counts", relation),
                            ("prior_state_relation_counts", prior_relation),
                            ("prior_record_age_counts", _age(received, None if prior is None else prior[0])),
                        ):
                            bucket = item[key]
                            assert isinstance(bucket, dict)
                            bucket[value] = int(bucket.get(value, 0)) + 1
                    previous[instrument_id] = (received, state)
                    rows_scanned += 1
        evidence = [counts[key] for key in sorted(counts)]
        core: dict[str, object] = {
            "schema_version": "tbbo_status_session_diagnostic/1.0.0",
            "state": "STATUS_SESSION_EVIDENCE_DIAGNOSED_NOT_CERTIFIED",
            "operation": OPERATION,
            "preparation_id": preparation_id,
            "failure_sha256": sha256_file(failure_path),
            "market": MARKET,
            "target_session_count": len(targets),
            "status_rows_scanned": rows_scanned,
            "target_session_rows": sum(int(item["row_count"]) for item in evidence),
            "session_evidence": evidence,
            "provider_calls": 0,
            "decoded_datasets_created": 0,
            "raw_market_values_output": False,
            "active_catalog_mutated": False,
        }
        diagnostic_id = sha256_json(core)
        report = {**core, "diagnostic_id": diagnostic_id}
        if len(canonical_bytes(report)) > MAXIMUM_OUTPUT_BYTES:
            raise UnauthorizedOperation("TBBO status diagnostic output ceiling exceeded")
        path = root / OUTPUT_ROOT / f"{diagnostic_id}.json"
        quality.write_or_verify(path, report, "TBBO status diagnostic")
        return {
            "diagnostic_id": diagnostic_id,
            "diagnostic_path": path.relative_to(root).as_posix(),
            "state": report["state"],
            "status_rows_scanned": rows_scanned,
            "target_session_rows": report["target_session_rows"],
            "provider_calls": 0,
            "decoded_datasets_created": 0,
        }
    finally:
        lock.release()
