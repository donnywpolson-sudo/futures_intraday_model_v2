"""Price-free provenance audit for the eight sealed Alpha feature gaps.

This is a preparatory source audit, not a strategy execution.  It traces the
exact 09:30-10:00 America/Chicago feature window through immutable provider,
raw, causal, active-view, calendar, and locally captured CME evidence without
emitting price values.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timezone
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import DataFileEntry, DataReleaseManifest, verify_data_release_manifest
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


CT = ZoneInfo("America/Chicago")
WINDOW_START = time(9, 30)
WINDOW_END = time(10, 0)

PREDECESSOR_PLAN_PATH = Path(
    "configs/alpha_ladder_missing_session_provenance_audit_plan.json"
)
PREDECESSOR_PLAN_ID = (
    "05ed696d1cfc560045c53af74dd7406154d7959ad731aa6d8e2b86d839d755b4"
)
PREDECESSOR_PLAN_SHA256 = (
    "c35d70c7ebd3d2daaad3583598dcb52b7cb21b6f2d7a5c76e4b31bf63446be34"
)
PREDECESSOR_FAILURE_ID = (
    "5668b12f1ec6e0516a911580c721b1a312178ddf4643b62de65ddf823987be90"
)
PREDECESSOR_FAILURE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_missing_session_provenance_audit_attempts/"
    f"{PREDECESSOR_FAILURE_ID}/failure_record.json"
)
PLAN_PATH = Path(
    "configs/alpha_ladder_missing_session_provenance_audit_v2_plan.json"
)
OUTPUT_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_missing_session_provenance_audit"
)
REPORT_PATH = OUTPUT_ROOT / "provenance_report.json"
MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_missing_session_provenance.py"
)
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_missing_session_provenance_audit_plan.py"
)
RUNNER_PATH = Path(
    "scripts/run_alpha_ladder_missing_session_provenance_audit.py"
)
TEST_PATH = Path("tests/test_alpha_ladder_missing_session_provenance.py")

DIAGNOSTIC_PATH = Path(
    "state/unpublished_evidence/"
    "alpha_ladder_reported_trade_exit_feature_gap_diagnostic/diagnostic_report.json"
)
DIAGNOSTIC_REPORT_ID = (
    "afef7a5849352a57b60d8daa197c0bc325892045f2158a689ec5d9daf4914235"
)
DIAGNOSTIC_SHA256 = (
    "77e70f7ceb8965d6a0cbf9984eabb249026aed99d04965791b328269d5b4f71b"
)
MECHANISM_ID = (
    "50dfc52cb5b4145dcbd6a761b3c626dae28c0aa974f6db35a1b60099297034e5"
)
MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_successor/"
    f"{MECHANISM_ID}/mechanism.json"
)
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")
CALENDAR_POINTER_PATH = Path(
    "configs/active_cash_open_impulse_historical_calendar.json"
)

REFERENCE_RELEASE_ID = (
    "44d2607b386a00d8c2ff65b9acab0503d42e1105bd4d8b4f91d89b348e1e9f88"
)
REFERENCE_MANIFEST_PATH = Path(
    f"manifests/data_releases/reference/{REFERENCE_RELEASE_ID}.json"
)
REFERENCE_LOGICAL_PATH = (
    "data/reference/exchange_calendars/attachment-0020-0dc3b7199d2e.pdf"
)
REFERENCE_FILE_SHA256 = (
    "d87c249c6e3df221bc748884b04a619f1bc2a4dc395b71e1c02148f0014f5f9e"
)
REFERENCE_EXTRACTION_PATH = Path(
    "reports/exchange_calendar/cme_historical_globex_evidence_extraction_b75ecc0f.json"
)
REFERENCE_EVIDENCE_ID = (
    "2b412f0fd916250f03c408755e1cb04a7aeda80ae74b87ac796e0ddbabdef96f"
)
REFERENCE_PASSAGE_SHA256 = (
    "397ad52ab98cd832a7e26e450850971b28fe4f2b5605cccba16bdadf162b32a4"
)
REFERENCE_REQUEST_ID = "attachment-0020-0dc3b7199d2e"
REFERENCE_URL = (
    "https://www.cmegroup.com/notices/clearing/2018/12/Chadv18-474.pdf"
)

TARGETS = (
    ("ES", "2018-12-05"),
    ("ZN", "2018-12-05"),
    ("CL", "2020-02-28"),
    ("ZN", "2020-02-28"),
    ("6E", "2020-06-30"),
    ("CL", "2020-06-30"),
    ("ES", "2020-06-30"),
    ("ZN", "2020-06-30"),
)
TARGET_SET = frozenset(TARGETS)
TARGET_MARKET_YEARS = frozenset((market, int(session[:4])) for market, session in TARGETS)

CLASSIFICATIONS = (
    "CALENDAR_CLOSURE",
    "RAW_SOURCE_ABSENCE",
    "NORMALIZATION_LOSS",
    "VERIFIED_NO_TRADE",
    "UNRESOLVED_EVIDENCE_CONFLICT",
)

DIRECT_DEPENDENCIES = frozenset(
    {
        MODULE_PATH.as_posix(),
        PREPARE_SCRIPT_PATH.as_posix(),
        RUNNER_PATH.as_posix(),
        TEST_PATH.as_posix(),
        "src/futures_rebuild/boundary.py",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/data_layout.py",
        "src/futures_rebuild/research_gateway_policy.py",
    }
)

FORBIDDEN_REPORT_FIELDS = frozenset(
    {
        "open",
        "high",
        "low",
        "close",
        "open_nano",
        "high_nano",
        "low_nano",
        "close_nano",
        "price",
        "prices",
        "return",
        "returns",
        "pnl",
        "profit",
        "loss_dollars",
    }
)


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical JSON")
    return payload


def _write_once(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(dict(payload)) + b"\n"
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
    except FileExistsError as exc:
        raise UnauthorizedOperation("provenance output is immutable and already exists") from exc
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _target_key(market: str, session: str) -> str:
    return f"{market}|{session}"


def _is_window(event_at_ns: int) -> tuple[str, bool, str]:
    event = datetime.fromtimestamp(event_at_ns / 1_000_000_000, timezone.utc).astimezone(CT)
    clock = event.timetz().replace(tzinfo=None)
    return event.date().isoformat(), WINDOW_START <= clock < WINDOW_END, event.isoformat()


def _find_file(manifest: DataReleaseManifest, suffix: str) -> DataFileEntry:
    matches = tuple(item for item in manifest.files if item.logical_path.endswith(suffix))
    if len(matches) != 1:
        raise IntegrityError(
            f"release {manifest.release_id} lacks one exact {suffix} file"
        )
    return matches[0]


def _physical_record(
    *, root: Path, manifest_path: Path, manifest: DataReleaseManifest,
    entry: DataFileEntry,
) -> dict[str, object]:
    physical = manifest.physical_relative_path(entry).as_posix()
    path = root / physical
    if not path.is_file() or path.stat().st_size != entry.size:
        raise IntegrityError(f"manifested source is absent or size-drifted: {physical}")
    return {
        "release_id": manifest.release_id,
        "manifest_path": manifest_path.as_posix(),
        "manifest_sha256": sha256_file(root / manifest_path),
        "logical_path": entry.logical_path,
        "physical_path": physical,
        "sha256": entry.sha256,
        "size": entry.size,
    }


def _diagnostic_targets(root: Path) -> tuple[dict[str, str], ...]:
    if sha256_file(root / DIAGNOSTIC_PATH) != DIAGNOSTIC_SHA256:
        raise IntegrityError("sealed feature-gap diagnostic bytes changed")
    report = _read_canonical(root / DIAGNOSTIC_PATH, name="feature-gap diagnostic")
    reconciliation = report.get("reconciliation")
    if (
        report.get("report_id") != DIAGNOSTIC_REPORT_ID
        or report.get("state") != "SEALED_UNPUBLISHED_PRICE_FREE_EXACT_RECONCILIATION"
        or not isinstance(reconciliation, Mapping)
        or reconciliation.get("status") != "EXACT_RECONCILIATION"
    ):
        raise IntegrityError("feature-gap diagnostic is not the sealed predecessor")
    records = reconciliation.get("feature_gap_records")
    if not isinstance(records, list):
        raise IntegrityError("sealed feature-gap records are absent")
    observed = {
        (str(item.get("market")), str(item.get("session")))
        for item in records
        if isinstance(item, Mapping)
        and item.get("classification") == "SOURCE_SESSION_ABSENT"
    }
    if observed != TARGET_SET:
        raise IntegrityError("sealed source-absent target set changed")
    return tuple(
        {"market": market, "session": session} for market, session in TARGETS
    )


def _calendar_evidence(root: Path, boundary: RepoBoundary) -> dict[str, object]:
    pointer = _read_canonical(root / CALENDAR_POINTER_PATH, name="calendar pointer")
    calendar_path_raw = pointer.get("calendar_path")
    if not isinstance(calendar_path_raw, str):
        raise IntegrityError("active calendar pointer is malformed")
    calendar_path = Path(calendar_path_raw)
    calendar = _read_canonical(root / calendar_path, name="active calendar")
    if (
        pointer.get("calendar_id") != calendar.get("calendar_id")
        or pointer.get("calendar_sha256") != sha256_file(root / calendar_path)
    ):
        raise IntegrityError("active calendar pointer binding changed")
    rows = calendar.get("calendar_rows")
    if not isinstance(rows, list):
        raise IntegrityError("active calendar rows are absent")
    active_rows = {}
    for market, session in TARGETS:
        matches = [
            item for item in rows
            if isinstance(item, Mapping)
            and item.get("market") == market
            and item.get("trade_date") == session
        ]
        if len(matches) != 1:
            raise IntegrityError("active calendar does not contain one exact target row")
        checkpoint_open = matches[0].get("checkpoint_open")
        disposition = matches[0].get("disposition")
        if (
            not isinstance(checkpoint_open, Mapping)
            or checkpoint_open.get("10:00") is not True
            or not isinstance(disposition, Mapping)
            or disposition.get("10:00") != "REGULAR_WEEKDAY_REFERENCE_RULE"
        ):
            raise IntegrityError("target no longer has the sealed generic calendar admission")
        active_rows[_target_key(market, session)] = {
            "calendar_admitted_10_00": True,
            "calendar_disposition": "REGULAR_WEEKDAY_REFERENCE_RULE",
            "schedule_family": matches[0].get("schedule_family"),
        }

    reference_manifest = verify_data_release_manifest(
        root / REFERENCE_MANIFEST_PATH, boundary, verify_files=False,
    )
    if reference_manifest.release_id != REFERENCE_RELEASE_ID:
        raise IntegrityError("CME reference release identity changed")
    reference_entry = _find_file(reference_manifest, REFERENCE_LOGICAL_PATH)
    reference_record = _physical_record(
        root=root,
        manifest_path=REFERENCE_MANIFEST_PATH,
        manifest=reference_manifest,
        entry=reference_entry,
    )
    if reference_entry.sha256 != REFERENCE_FILE_SHA256:
        raise IntegrityError("CME national-mourning notice bytes changed")

    extraction = _read_canonical(
        root / REFERENCE_EXTRACTION_PATH, name="CME evidence extraction",
    )
    passages = extraction.get("candidate_passages")
    if not isinstance(passages, list):
        raise IntegrityError("CME evidence extraction lacks candidate passages")
    matches = [
        item for item in passages
        if isinstance(item, Mapping)
        and item.get("evidence_id") == REFERENCE_EVIDENCE_ID
    ]
    if len(matches) != 1:
        raise IntegrityError("CME national-mourning passage is not unique")
    passage = matches[0]
    text = str(passage.get("passage", "")).lower()
    if (
        passage.get("passage_sha256") != REFERENCE_PASSAGE_SHA256
        or passage.get("request_id") != REFERENCE_REQUEST_ID
        or passage.get("source_release_id") != REFERENCE_RELEASE_ID
        or passage.get("source_sha256") != REFERENCE_FILE_SHA256
        or passage.get("source_url") != REFERENCE_URL
        or "abbreviated session, closing after overnight trading at 8:30 a.m. central time"
        not in text
        or "interest rate products will close" not in text
        or "trade date of dec. 6" not in text
    ):
        raise IntegrityError("CME national-mourning semantics changed")

    closure = {
        _target_key("ES", "2018-12-05"): {
            "authoritative_checkpoint_state": "CLOSED_BY_08_30_CT",
            "semantic_basis": "EQUITY_PRODUCTS_ABBREVIATED_SESSION_ENDED_08_30_CT",
        },
        _target_key("ZN", "2018-12-05"): {
            "authoritative_checkpoint_state": "CLOSED",
            "semantic_basis": "INTEREST_RATE_PRODUCTS_DID_NOT_REOPEN_UNTIL_TRADE_DATE_2018_12_06",
        },
    }
    return {
        "calendar_id": calendar["calendar_id"],
        "calendar_path": calendar_path.as_posix(),
        "calendar_sha256": pointer["calendar_sha256"],
        "active_target_rows": active_rows,
        "authoritative_closures": closure,
        "reference": {
            **reference_record,
            "evidence_id": REFERENCE_EVIDENCE_ID,
            "passage_sha256": REFERENCE_PASSAGE_SHA256,
            "request_id": REFERENCE_REQUEST_ID,
            "source_url": REFERENCE_URL,
            "extraction_path": REFERENCE_EXTRACTION_PATH.as_posix(),
            "extraction_sha256": sha256_file(root / REFERENCE_EXTRACTION_PATH),
        },
    }


def _source_topology(root: Path, boundary: RepoBoundary) -> dict[str, object]:
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    active_by_pair = {}
    for market, year in TARGET_MARKET_YEARS:
        matches = [
            item for item in entries
            if isinstance(item, Mapping)
            and item.get("market") == market
            and item.get("year") == year
        ]
        if len(matches) != 1:
            raise IntegrityError("active catalog target binding is not unique")
        item = matches[0]
        source_bindings = item.get("source_bindings")
        if not isinstance(source_bindings, list) or len(source_bindings) != 1:
            raise IntegrityError("active target lacks one exact source binding")
        source = source_bindings[0]
        if not isinstance(source, Mapping):
            raise IntegrityError("active target source binding is malformed")
        active_by_pair[(market, year)] = {
            "market": market,
            "year": year,
            "active_path": str(item["parquet_path"]),
            "active_sha256": str(item["parquet_sha256"]),
            "active_causal_release_id": str(source["causal_release_id"]),
            "active_raw_release_id": str(source["raw_release_id"]),
            "active_dbn_release_id": str(source["dbn_release_id"]),
        }

    causal_records: dict[tuple[str, int], list[dict[str, object]]] = {
        pair: [] for pair in TARGET_MARKET_YEARS
    }
    raw_ids: set[str] = set()
    causal_root = root / "manifests/data_releases/causally_gated_normalized"
    for manifest_path_absolute in sorted(causal_root.glob("*.json")):
        relative = manifest_path_absolute.relative_to(root)
        manifest = verify_data_release_manifest(
            manifest_path_absolute, boundary, verify_files=False,
        )
        pair = (str(manifest.metadata.get("market")), int(manifest.metadata.get("year", -1)))
        if pair not in TARGET_MARKET_YEARS:
            continue
        entry = _find_file(manifest, "/bars.parquet")
        record = _physical_record(
            root=root, manifest_path=relative, manifest=manifest, entry=entry,
        )
        sources = sorted(
            source_id for source_id in manifest.source_release_ids
            if (root / f"manifests/data_releases/raw/{source_id}.json").is_file()
        )
        if len(sources) != 1:
            raise IntegrityError("causal candidate lacks one exact raw parent")
        record["raw_release_id"] = sources[0]
        causal_records[pair].append(record)
        raw_ids.add(sources[0])

    if any(not records for records in causal_records.values()):
        raise IntegrityError("one target market-year lacks causal alternatives")
    for pair, active in active_by_pair.items():
        ids = {str(item["release_id"]) for item in causal_records[pair]}
        if active["active_causal_release_id"] not in ids:
            raise IntegrityError("active causal release is outside immutable candidates")

    raw_records: dict[tuple[str, int], list[dict[str, object]]] = {
        pair: [] for pair in TARGET_MARKET_YEARS
    }
    dbn_ids: set[str] = set()
    for release_id in sorted(raw_ids):
        relative = Path(f"manifests/data_releases/raw/{release_id}.json")
        manifest = verify_data_release_manifest(
            root / relative, boundary, verify_files=False,
        )
        pair = (str(manifest.metadata.get("market")), int(manifest.metadata.get("year", -1)))
        if pair not in TARGET_MARKET_YEARS:
            raise IntegrityError("target causal lineage escaped its market-year")
        entry = _find_file(manifest, "/bars.parquet")
        record = _physical_record(
            root=root, manifest_path=relative, manifest=manifest, entry=entry,
        )
        providers = sorted(
            source_id for source_id in manifest.source_release_ids
            if (root / f"manifests/data_releases/dbn/{source_id}.json").is_file()
        )
        if len(providers) != 1:
            raise IntegrityError("raw candidate lacks one exact DBN parent")
        record["dbn_release_id"] = providers[0]
        raw_records[pair].append(record)
        dbn_ids.add(providers[0])
    if any(not records for records in raw_records.values()):
        raise IntegrityError("one target market-year lacks raw candidates")

    dbn_by_pair: dict[tuple[str, int], dict[str, dict[str, object]]] = {
        pair: {} for pair in TARGET_MARKET_YEARS
    }
    dbn_release_manifests = []
    for release_id in sorted(dbn_ids):
        relative = Path(f"manifests/data_releases/dbn/{release_id}.json")
        manifest = verify_data_release_manifest(
            root / relative, boundary, verify_files=False,
        )
        dbn_release_manifests.append(
            {
                "release_id": release_id,
                "manifest_path": relative.as_posix(),
                "manifest_sha256": sha256_file(root / relative),
            }
        )
        for market, year in TARGET_MARKET_YEARS:
            for schema in ("ohlcv_1m", "ohlcv_1s"):
                logical = (
                    f"data/dbn/{schema}/{market}/{year}/"
                    f"{year}-01-01_{year + 1}-01-01.dbn.zst"
                )
                matches = [item for item in manifest.files if item.logical_path == logical]
                sidecars = [
                    item for item in manifest.files
                    if item.logical_path == f"{logical}.manifest.json"
                ]
                if len(matches) != 1 or len(sidecars) != 1:
                    raise IntegrityError("DBN target file or sidecar is absent")
                record = _physical_record(
                    root=root,
                    manifest_path=relative,
                    manifest=manifest,
                    entry=matches[0],
                )
                sidecar = _physical_record(
                    root=root,
                    manifest_path=relative,
                    manifest=manifest,
                    entry=sidecars[0],
                )
                existing = dbn_by_pair[(market, year)].get(schema)
                candidate = {**record, "schema": schema, "sidecar": sidecar}
                if existing is None:
                    dbn_by_pair[(market, year)][schema] = candidate
                elif (
                    existing["physical_path"] != candidate["physical_path"]
                    or existing["sha256"] != candidate["sha256"]
                    or existing["sidecar"]["sha256"] != sidecar["sha256"]
                ):
                    raise IntegrityError("DBN parent releases disagree on target bytes")

    target_topology = {}
    for market, session in TARGETS:
        pair = (market, int(session[:4]))
        target_topology[_target_key(market, session)] = {
            "market": market,
            "session": session,
            "active": active_by_pair[pair],
            "causal_candidates": sorted(
                causal_records[pair], key=lambda item: str(item["release_id"]),
            ),
            "raw_candidates": sorted(
                raw_records[pair], key=lambda item: str(item["release_id"]),
            ),
            "dbn_sources": dbn_by_pair[pair],
        }
    return {
        "active_catalog_id": catalog.get("active_view_id"),
        "active_catalog_sha256": sha256_file(root / ACTIVE_CATALOG_PATH),
        "dbn_release_manifests": dbn_release_manifests,
        "targets": target_topology,
    }


def _collect_bindings(
    *, root: Path, topology: Mapping[str, object], calendar: Mapping[str, object],
) -> dict[str, str]:
    bindings = {
        DIAGNOSTIC_PATH.as_posix(): DIAGNOSTIC_SHA256,
        MECHANISM_PATH.as_posix(): sha256_file(root / MECHANISM_PATH),
        ACTIVE_CATALOG_PATH.as_posix(): str(topology["active_catalog_sha256"]),
        CALENDAR_POINTER_PATH.as_posix(): sha256_file(root / CALENDAR_POINTER_PATH),
        str(calendar["calendar_path"]): str(calendar["calendar_sha256"]),
        REFERENCE_EXTRACTION_PATH.as_posix(): str(
            calendar["reference"]["extraction_sha256"]
        ),
    }
    for path in DIRECT_DEPENDENCIES:
        bindings[path] = sha256_file(root / path)
    target_topology = topology["targets"]
    assert isinstance(target_topology, Mapping)
    for item in target_topology.values():
        assert isinstance(item, Mapping)
        active = item["active"]
        assert isinstance(active, Mapping)
        bindings[str(active["active_path"])] = str(active["active_sha256"])
        for collection in (item["causal_candidates"], item["raw_candidates"]):
            assert isinstance(collection, list)
            for source in collection:
                assert isinstance(source, Mapping)
                bindings[str(source["manifest_path"])] = str(source["manifest_sha256"])
                bindings[str(source["physical_path"])] = str(source["sha256"])
        dbn_sources = item["dbn_sources"]
        assert isinstance(dbn_sources, Mapping)
        for source in dbn_sources.values():
            assert isinstance(source, Mapping)
            sidecar = source["sidecar"]
            assert isinstance(sidecar, Mapping)
            bindings[str(source["physical_path"])] = str(source["sha256"])
            bindings[str(sidecar["physical_path"])] = str(sidecar["sha256"])
    for source in topology["dbn_release_manifests"]:
        assert isinstance(source, Mapping)
        bindings[str(source["manifest_path"])] = str(source["manifest_sha256"])
    reference = calendar["reference"]
    assert isinstance(reference, Mapping)
    bindings[str(reference["manifest_path"])] = str(reference["manifest_sha256"])
    bindings[str(reference["physical_path"])] = str(reference["sha256"])
    return dict(sorted(bindings.items()))


def build_plan(*, root: Path) -> dict[str, object]:
    if (
        sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256
        or _read_canonical(
            root / PREDECESSOR_PLAN_PATH, name="predecessor provenance plan",
        ).get("plan_id") != PREDECESSOR_PLAN_ID
    ):
        raise IntegrityError("consumed predecessor provenance plan changed")
    failure = _read_canonical(
        root / PREDECESSOR_FAILURE_PATH, name="predecessor provenance failure",
    )
    if (
        failure.get("failure_id") != PREDECESSOR_FAILURE_ID
        or failure.get("plan_id") != PREDECESSOR_PLAN_ID
        or failure.get("attempt_consumed") is not True
        or failure.get("report_created") is not False
        or failure.get("retry_authorized") is not False
    ):
        raise IntegrityError("consumed predecessor failure evidence changed")
    boundary = RepoBoundary(root)
    targets = _diagnostic_targets(root)
    calendar = _calendar_evidence(root, boundary)
    topology = _source_topology(root, boundary)
    bindings = _collect_bindings(root=root, topology=topology, calendar=calendar)
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_missing_session_provenance_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "purpose": "PRICE_FREE_PROVENANCE_AUDIT_OF_EIGHT_SEALED_SOURCE_ABSENCES",
        "mechanism_id": MECHANISM_ID,
        "diagnostic_report_id": DIAGNOSTIC_REPORT_ID,
        "predecessor_attempt": {
            "plan_id": PREDECESSOR_PLAN_ID,
            "plan_path": PREDECESSOR_PLAN_PATH.as_posix(),
            "plan_sha256": PREDECESSOR_PLAN_SHA256,
            "failure_id": PREDECESSOR_FAILURE_ID,
            "failure_path": PREDECESSOR_FAILURE_PATH.as_posix(),
            "failure_sha256": sha256_file(root / PREDECESSOR_FAILURE_PATH),
            "disposition": "CONSUMED_IMPLEMENTATION_FAILURE_NO_REPORT",
        },
        "targets": list(targets),
        "target_count": len(targets),
        "window": {
            "timezone": "America/Chicago",
            "start_inclusive": "09:30:00",
            "end_exclusive": "10:00:00",
        },
        "classification_precedence": list(CLASSIFICATIONS),
        "classification_rules": {
            "calendar_closure": "LOCAL_HASH_BOUND_AUTHORITATIVE_CME_NOTICE_PROVES_WINDOW_CLOSED",
            "normalization_loss": "UPSTREAM_PROVIDER_OR_RAW_EVENT_EXISTS_BUT_DOWNSTREAM_TARGET_WINDOW_IS_ABSENT",
            "raw_source_absence": "REQUIRED_PROVIDER_REPORTED_BAR_IS_ABSENT_WITHOUT_INDEPENDENT_COMPLETE_NO_TRADE_PROOF",
            "verified_no_trade": "INDEPENDENT_COMPLETE_TRADE_STREAM_REQUIRED_AND_ZERO_TRADES_OBSERVED",
            "unresolved": "CONFLICTING_OR_INCOMPLETE_PROVENANCE_FAILS_CLOSED",
            "absence_alone_never_proves_no_trade": True,
        },
        "source_topology": topology,
        "calendar_evidence": calendar,
        "required_outputs": [REPORT_PATH.as_posix()],
        "output_root": OUTPUT_ROOT.as_posix(),
        "execution_limits": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": 900,
            "maximum_external_cost_usd": "0",
        },
        "authority": {
            "historical_row_read": True,
            "returns": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "registration": False,
            "trial_execution": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "active_data_mutation": False,
            "trading": False,
        },
        "price_free_output": True,
        "bindings": bindings,
    }
    core["bindings"][PREDECESSOR_PLAN_PATH.as_posix()] = PREDECESSOR_PLAN_SHA256
    core["bindings"][PREDECESSOR_FAILURE_PATH.as_posix()] = sha256_file(
        root / PREDECESSOR_FAILURE_PATH
    )
    core["bindings"] = dict(sorted(core["bindings"].items()))
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("missing-session provenance audit plan drifted")
    return dict(plan)


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="missing-session provenance plan")
    return validate_plan(plan, root=root)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan.get("execution_limits")
    if not isinstance(limits, Mapping):
        raise IntegrityError("provenance plan limits are malformed")
    return {
        "mechanism_id": MECHANISM_ID,
        "diagnostic_report_id": DIAGNOSTIC_REPORT_ID,
        "period": "2018,2020",
        "markets": "6E,CL,ES,ZN",
        "target_market_session_count": "8",
        "window": "09:30-10:00 America/Chicago",
        "purpose": str(plan["purpose"]),
        "output_root": OUTPUT_ROOT.as_posix(),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "price_free_output": "true",
        "returns": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "performance_evaluation": "false",
        "registration": "false",
        "trial_execution": "false",
        "provider_network_access": "false",
        "holdout_2025_access": "false",
        "active_data_mutation": "false",
        "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def classify_provenance(evidence: Mapping[str, object]) -> tuple[str, str]:
    """Apply the frozen conservative provenance precedence."""

    if evidence.get("calendar_closed") is True:
        return "CALENDAR_CLOSURE", "AUTHORITATIVE_CME_SCHEDULE_CORRECTION"
    active = int(evidence.get("active_causal_count", 0))
    alternate = int(evidence.get("alternate_causal_count", 0))
    causal_any = int(evidence.get("any_causal_count", 0))
    causal_mislabeled = int(evidence.get("causal_mislabeled_count", 0))
    raw = int(evidence.get("raw_count", 0))
    dbn_1m = int(evidence.get("dbn_1m_count", 0))
    dbn_1s = int(evidence.get("dbn_1s_count", 0))
    trade_complete = evidence.get("independent_trade_stream_complete") is True
    trade_count = evidence.get("independent_trade_count")

    if active > 0:
        return "UNRESOLVED_EVIDENCE_CONFLICT", "SEALED_ACTIVE_ABSENCE_CONTRADICTED"
    if causal_mislabeled > 0:
        return "NORMALIZATION_LOSS", "CAUSAL_SESSION_LABEL_LOSS"
    if alternate > 0:
        return "NORMALIZATION_LOSS", "ACTIVE_RELEASE_SELECTION_LOSS"
    if raw > 0 and causal_any == 0:
        return "NORMALIZATION_LOSS", "RAW_TO_CAUSAL_LOSS"
    if dbn_1m > 0 and raw == 0:
        return "NORMALIZATION_LOSS", "DBN_TO_RAW_LOSS"
    if dbn_1m == 0 and (raw > 0 or causal_any > 0):
        return "UNRESOLVED_EVIDENCE_CONFLICT", "DOWNSTREAM_ROWS_WITHOUT_PROVIDER_BAR"
    if dbn_1m == 0 and dbn_1s > 0:
        return "RAW_SOURCE_ABSENCE", "OHLCV_1M_ABSENT_WHILE_OHLCV_1S_PRESENT"
    if dbn_1m == 0 and dbn_1s == 0:
        if trade_complete and trade_count == 0:
            return "VERIFIED_NO_TRADE", "COMPLETE_INDEPENDENT_TRADE_STREAM_ZERO_EVENTS"
        return "RAW_SOURCE_ABSENCE", "NO_PROVIDER_REPORTED_BARS_AND_NO_NO_TRADE_PROOF"
    return "UNRESOLVED_EVIDENCE_CONFLICT", "PROVENANCE_TOPOLOGY_NOT_CLASSIFIABLE"


def _blank_scan() -> dict[str, object]:
    return {
        "event_date_window_count": 0,
        "session_label_window_count": 0,
        "mislabeled_window_count": 0,
        "event_timestamps": [],
        "row_identity_hashes": [],
        "session_labels": Counter(),
    }


def _finalize_scan(raw: Mapping[str, object]) -> dict[str, object]:
    timestamps = sorted(set(str(item) for item in raw["event_timestamps"]))
    row_hashes = sorted(set(str(item) for item in raw["row_identity_hashes"]))
    labels = raw["session_labels"]
    assert isinstance(labels, Counter)
    return {
        "event_date_window_count": int(raw["event_date_window_count"]),
        "session_label_window_count": int(raw["session_label_window_count"]),
        "mislabeled_window_count": int(raw["mislabeled_window_count"]),
        "earliest_event_at": timestamps[0] if timestamps else None,
        "latest_event_at": timestamps[-1] if timestamps else None,
        "event_timestamp_set_sha256": sha256_json(timestamps),
        "row_identity_set_sha256": sha256_json(row_hashes),
        "observed_session_label_counts": dict(sorted(labels.items())),
        "price_values_included": False,
    }


def _scan_parquet(
    *, path: Path, kind: str, sessions: Sequence[str],
) -> dict[str, dict[str, object]]:
    import pyarrow.parquet as pq

    if kind not in {"causal", "raw"}:
        raise IntegrityError("unsupported provenance parquet kind")
    columns = (
        ["event_at_ns", "exchange_session_date", "source_row_sha256"]
        if kind == "causal"
        else ["event_at_ns", "row_sha256"]
    )
    parquet = pq.ParquetFile(path)
    if not set(columns).issubset(parquet.schema_arrow.names):
        raise IntegrityError(f"{kind} provenance schema is incomplete")
    results = {session: _blank_scan() for session in sessions}
    session_set = set(sessions)
    for batch in parquet.iter_batches(batch_size=65_536, columns=columns):
        values = batch.to_pydict()
        for index in range(batch.num_rows):
            event_raw = values["event_at_ns"][index]
            if type(event_raw) is not int:
                raise IntegrityError("provenance event timestamp is not an integer")
            local_date, in_window, event_iso = _is_window(event_raw)
            if not in_window:
                continue
            label = (
                values["exchange_session_date"][index] if kind == "causal" else None
            )
            row_hash = values[
                "source_row_sha256" if kind == "causal" else "row_sha256"
            ][index]
            if not isinstance(row_hash, str):
                raise IntegrityError("provenance source row lacks its identity hash")
            for session in session_set:
                target = results[session]
                if local_date == session:
                    target["event_date_window_count"] += 1
                    target["event_timestamps"].append(event_iso)
                    target["row_identity_hashes"].append(row_hash)
                    if kind == "causal" and label != session:
                        target["mislabeled_window_count"] += 1
                        target["session_labels"][str(label)] += 1
                if kind == "causal" and label == session:
                    target["session_label_window_count"] += 1
                    if local_date != session:
                        target["mislabeled_window_count"] += 1
                        target["session_labels"][local_date] += 1
    return {session: _finalize_scan(item) for session, item in results.items()}


def _validate_dbn_sidecar(
    *, path: Path, source_path: Path, expected_sha256: str,
    market: str, year: int, schema: str, session: str,
) -> dict[str, object]:
    sidecar = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(sidecar, dict):
        raise IntegrityError("DBN sidecar is malformed")
    complete = (
        sidecar.get("vendor") == "databento"
        and sidecar.get("dataset") == "GLBX.MDP3"
        and sidecar.get("schema") == schema.replace("_", "-")
        and sidecar.get("market") == market
        and sidecar.get("start") == f"{year}-01-01"
        and sidecar.get("end") == f"{year + 1}-01-01"
        and sidecar.get("request_status") == "ok"
        and sidecar.get("file_sha256") == expected_sha256
        and sidecar.get("file_size_bytes") == source_path.stat().st_size
        and date.fromisoformat(str(sidecar.get("start")))
        <= date.fromisoformat(session)
        < date.fromisoformat(str(sidecar.get("end")))
    )
    return {
        "request_status": sidecar.get("request_status"),
        "coverage_start": sidecar.get("start"),
        "coverage_end": sidecar.get("end"),
        "schema": sidecar.get("schema"),
        "symbol": (sidecar.get("symbols_requested") or [None])[0],
        "complete_requested_interval": complete,
        "sidecar_sha256": sha256_file(path),
    }


def _scan_dbn(
    *, path: Path, sessions: Sequence[str], market: str,
) -> dict[str, dict[str, object]]:
    import databento

    results = {session: _blank_scan() for session in sessions}
    session_set = set(sessions)
    store = databento.DBNStore.from_file(path)
    try:
        for record in store:
            event_raw = getattr(record, "ts_event", None)
            publisher = getattr(record, "publisher_id", None)
            instrument = getattr(record, "instrument_id", None)
            if not isinstance(event_raw, int):
                raise IntegrityError("DBN provenance record lacks an event timestamp")
            local_date, in_window, event_iso = _is_window(event_raw)
            if not in_window or local_date not in session_set:
                continue
            target = results[local_date]
            target["event_date_window_count"] += 1
            target["event_timestamps"].append(event_iso)
            target["row_identity_hashes"].append(
                sha256_json(
                    {
                        "event_at_ns": event_raw,
                        "instrument_id": int(instrument),
                        "market": market,
                        "publisher_id": int(publisher),
                    }
                )
            )
    finally:
        del store
    return {session: _finalize_scan(item) for session, item in results.items()}


def _assert_price_free(value: object, *, path: str = "report") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).lower() in FORBIDDEN_REPORT_FIELDS:
                if str(key).lower() == "returns" and child is False:
                    continue
                raise IntegrityError(f"price/economic field leaked at {path}.{key}")
            _assert_price_free(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_price_free(child, path=f"{path}[{index}]")


def _verify_bindings(root: Path, plan: Mapping[str, object]) -> None:
    bindings = plan.get("bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("provenance plan bindings are absent")
    for raw_path, expected in bindings.items():
        path = root / str(raw_path)
        if not path.is_file() or sha256_file(path) != expected:
            raise IntegrityError(f"provenance binding changed: {raw_path}")


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> Mapping[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if (root / OUTPUT_ROOT).exists():
        raise UnauthorizedOperation("provenance audit output already exists")
    use_path = receipt.consume(
        boundary,
        operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    _verify_bindings(root, plan)

    topology = plan["source_topology"]
    target_topology = topology["targets"]
    assert isinstance(target_topology, Mapping)
    sessions_by_pair: dict[tuple[str, int], list[str]] = {}
    for market, session in TARGETS:
        sessions_by_pair.setdefault((market, int(session[:4])), []).append(session)

    parquet_cache: dict[tuple[str, str], dict[str, dict[str, object]]] = {}
    dbn_cache: dict[str, dict[str, dict[str, object]]] = {}
    for market, year in sorted(TARGET_MARKET_YEARS):
        sample_key = _target_key(market, sessions_by_pair[(market, year)][0])
        item = target_topology[sample_key]
        assert isinstance(item, Mapping)
        sessions = tuple(sorted(sessions_by_pair[(market, year)]))
        for kind, sources in (
            ("causal", item["causal_candidates"]),
            ("raw", item["raw_candidates"]),
        ):
            assert isinstance(sources, list)
            for source in sources:
                assert isinstance(source, Mapping)
                path = str(source["physical_path"])
                cache_key = (kind, path)
                if cache_key not in parquet_cache:
                    parquet_cache[cache_key] = _scan_parquet(
                        path=root / path, kind=kind, sessions=sessions,
                    )
        dbn_sources = item["dbn_sources"]
        assert isinstance(dbn_sources, Mapping)
        for schema, source in dbn_sources.items():
            assert isinstance(source, Mapping)
            path = str(source["physical_path"])
            if path not in dbn_cache:
                dbn_cache[path] = _scan_dbn(
                    path=root / path, sessions=sessions, market=market,
                )

    calendar_evidence = plan["calendar_evidence"]
    closures = calendar_evidence["authoritative_closures"]
    active_calendar_rows = calendar_evidence["active_target_rows"]
    assert isinstance(closures, Mapping) and isinstance(active_calendar_rows, Mapping)
    results = []
    for market, session in TARGETS:
        key = _target_key(market, session)
        item = target_topology[key]
        assert isinstance(item, Mapping)
        active_binding = item["active"]
        assert isinstance(active_binding, Mapping)
        causal_evidence = []
        active_scan = None
        alternate_count = 0
        causal_any = 0
        causal_mislabeled = 0
        for source in item["causal_candidates"]:
            path = str(source["physical_path"])
            scan = parquet_cache[("causal", path)][session]
            record = {
                "release_id": source["release_id"],
                "physical_path": path,
                "sha256": source["sha256"],
                "raw_release_id": source["raw_release_id"],
                "scan": scan,
            }
            causal_evidence.append(record)
            count = int(scan["session_label_window_count"])
            causal_any = max(causal_any, count)
            causal_mislabeled = max(
                causal_mislabeled, int(scan["mislabeled_window_count"]),
            )
            if source["release_id"] == active_binding["active_causal_release_id"]:
                active_scan = scan
            else:
                alternate_count = max(alternate_count, count)
        if active_scan is None:
            raise IntegrityError("active causal source was not scanned")
        active_count = int(active_scan["session_label_window_count"])
        if active_count != 0:
            raise IntegrityError("sealed SOURCE_SESSION_ABSENT result no longer reconciles")

        raw_evidence = []
        raw_count = 0
        for source in item["raw_candidates"]:
            path = str(source["physical_path"])
            scan = parquet_cache[("raw", path)][session]
            raw_count = max(raw_count, int(scan["event_date_window_count"]))
            raw_evidence.append(
                {
                    "release_id": source["release_id"],
                    "physical_path": path,
                    "sha256": source["sha256"],
                    "dbn_release_id": source["dbn_release_id"],
                    "scan": scan,
                }
            )

        dbn_evidence = {}
        dbn_counts = {}
        for schema, source in item["dbn_sources"].items():
            path = str(source["physical_path"])
            scan = dbn_cache[path][session]
            sidecar = source["sidecar"]
            sidecar_evidence = _validate_dbn_sidecar(
                path=root / str(sidecar["physical_path"]),
                source_path=root / path,
                expected_sha256=str(source["sha256"]),
                market=market,
                year=int(session[:4]),
                schema=str(schema),
                session=session,
            )
            dbn_counts[str(schema)] = int(scan["event_date_window_count"])
            dbn_evidence[str(schema)] = {
                "physical_path": path,
                "sha256": source["sha256"],
                "scan": scan,
                "acquisition": sidecar_evidence,
            }

        evidence = {
            "calendar_closed": key in closures,
            "active_causal_count": active_count,
            "alternate_causal_count": alternate_count,
            "any_causal_count": causal_any,
            "causal_mislabeled_count": causal_mislabeled,
            "raw_count": raw_count,
            "dbn_1m_count": dbn_counts["ohlcv_1m"],
            "dbn_1s_count": dbn_counts["ohlcv_1s"],
            "independent_trade_stream_complete": False,
            "independent_trade_count": None,
        }
        classification, detail = classify_provenance(evidence)
        results.append(
            {
                "market": market,
                "session": session,
                "classification": classification,
                "classification_detail": detail,
                "sealed_diagnostic_reconciled": active_count == 0,
                "active_calendar": active_calendar_rows[key],
                "authoritative_calendar_correction": closures.get(key),
                "causal_candidates": causal_evidence,
                "raw_candidates": raw_evidence,
                "provider_sources": dbn_evidence,
                "classification_inputs": evidence,
                "price_values_included": False,
            }
        )

    elapsed = monotonic() - started
    maximum_runtime = int(plan["execution_limits"]["maximum_runtime_seconds"])
    if elapsed > maximum_runtime:
        raise UnauthorizedOperation("provenance audit exceeded its maximum runtime")
    counts = Counter(str(item["classification"]) for item in results)
    core = {
        "schema_version": "alpha_ladder_missing_session_provenance_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_PRICE_FREE_PROVENANCE_AUDIT",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        "diagnostic_report_id": DIAGNOSTIC_REPORT_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "target_count": len(results),
        "classification_counts": dict(sorted(counts.items())),
        "all_targets_classified": len(results) == len(TARGETS),
        "calendar_successor_required": any(
            item["classification"] == "CALENDAR_CLOSURE" for item in results
        ),
        "source_successor_required": any(
            item["classification"] in {
                "RAW_SOURCE_ABSENCE", "NORMALIZATION_LOSS",
                "UNRESOLVED_EVIDENCE_CONFLICT",
            }
            for item in results
        ),
        "verified_no_trade_requires_independent_trade_stream": True,
        "results": results,
        "price_free_output": True,
        "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    _assert_price_free(report)
    _write_once(root / REPORT_PATH, report)
    if (root / REPORT_PATH).read_bytes() != canonical_bytes(report) + b"\n":
        raise IntegrityError("provenance report byte verification failed")
    return report
