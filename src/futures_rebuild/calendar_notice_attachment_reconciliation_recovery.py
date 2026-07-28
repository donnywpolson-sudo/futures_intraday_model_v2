"""Recover an interrupted availability-tolerant CME attachment reconciliation."""

from __future__ import annotations

import json
import os
import re
import shutil
import time as monotonic_time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_notice_attachment_capture import (
    RELEASE_KIND,
    NoticeAttachmentRequestError,
    _failure_evidence as attachment_failure_evidence,
    _fetch as fetch_attachment,
    validate_attachment_capture_plan,
)
from .calendar_notice_attachment_reconciliation import (
    _existing_release_for_plan as predecessor_release_for_plan,
    _failure_path as predecessor_failure_path,
    _network_exclusion,
    build_reconciliation_plan,
    reconciliation_authority,
    validate_reconciliation_approval,
    validate_reconciliation_plan,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


INTERRUPTION_SCHEMA = (
    "cme_historical_notice_attachment_reconciliation_interruption/1.0.0"
)
PLAN_SCHEMA = (
    "cme_historical_notice_attachment_reconciliation_recovery_plan/1.0.0"
)
APPROVAL_SCHEMA = (
    "cme_historical_notice_attachment_reconciliation_recovery_approval/1.0.0"
)
CAPTURE_SCHEMA = "cme_historical_notice_attachment_capture/4.0.0"
FAILURE_SCHEMA = (
    "cme_historical_notice_attachment_reconciliation_recovery_failure/1.0.0"
)
OPERATION = (
    "RECOVER_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_ATTACHMENT_RECONCILIATION"
)
TOTAL_CANDIDATES = 797
REUSED_RESPONSES = 54
KNOWN_EXCLUSIONS = 2
KNOWN_COMPLETED_NETWORK_RESPONSES = 40
POSSIBLY_IN_FLIGHT_REQUESTS = 2
NETWORK_REQUESTS = 741
FIRST_NETWORK_ORDINAL = 57
LAST_NETWORK_ORDINAL = 797
KNOWN_EXCLUSION_ORDINALS = (14, 15)
POSSIBLY_IN_FLIGHT_ORDINALS = (57, 58)
PRESERVED_ORDINALS = (
    *range(1, 14),
    16,
    *range(17, 57),
)
MAX_RESPONSE_BYTES = 16_777_216
MAX_NETWORK_BYTES = 4_294_967_296
MAX_TOTAL_BYTES = 4_294_967_296
MAX_DURATION_SECONDS = 5_400
REQUEST_TIMEOUT_SECONDS = 45
WORKERS = 2
REUSE_VERIFICATION_WORKERS = 16
WRAPPER_EXIT_CODE = 124
WRAPPER_TIMEOUT_SECONDS = 10
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_attachment_capture.py",
    "src/futures_rebuild/calendar_notice_attachment_diagnostic.py",
    "src/futures_rebuild/calendar_notice_attachment_reconciliation.py",
    "src/futures_rebuild/calendar_notice_attachment_reconciliation_recovery.py",
    "src/futures_rebuild/calendar_notice_attachment_recovery.py",
    "src/futures_rebuild/calendar_notice_attachments.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/source_contract.py",
)
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_UNLISTED_ATTACHMENT",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_EDIT_MOVE_OR_OVERWRITE_PREDECESSOR_EVIDENCE",
    "EXECUTE_DOCUMENT_CONTENT_OR_FOLLOW_EMBEDDED_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "REQUEST_KNOWN_COMPLETED_OR_EXCLUDED_ATTACHMENT",
    "RETRY_WITHIN_THIS_APPROVAL_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "HTTP_STATUS_OTHER_THAN_200_OR_404",
    "IMPLEMENTATION_HASH_DRIFT",
    "INTERRUPTION_EVIDENCE_OR_STAGE_DRIFT",
    "MIME_OR_RESPONSE_URL_MISMATCH",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PRIOR_PLAN_OUTCOME_EXISTS",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_REQUEST_KEYS = {
    "accept",
    "discovery_reasons",
    "expected_content_types",
    "extension",
    "link_texts",
    "logical_path",
    "ordinal",
    "request_id",
    "request_kind",
    "source_notice_request_ids",
    "source_notice_urls",
    "source_titles",
    "url",
}


class NoticeAttachmentReconciliationRecoveryError(UnauthorizedOperation):
    """Raised before or during the bounded reconciliation recovery."""


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def implementation_hashes(repository_root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise IntegrityError(
                "CME reconciliation-recovery implementation input is "
                f"missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _validate_request(
    request: Mapping[str, object],
    *,
    expected_ordinal: int,
) -> dict[str, object]:
    if (
        set(request) != _REQUEST_KEYS
        or request.get("ordinal") != expected_ordinal
        or type(request.get("request_id")) is not str
        or type(request.get("url")) is not str
        or request.get("request_kind")
        != "HISTORICAL_NOTICE_ATTACHMENT_CAPTURE"
        or request.get("extension") not in {".csv", ".pdf", ".xls"}
        or type(request.get("logical_path")) is not str
        or Path(str(request["logical_path"])).name
        != f"{request['request_id']}{request['extension']}"
        or not isinstance(request.get("expected_content_types"), list)
        or not request["expected_content_types"]
    ):
        raise ContractError(
            "CME reconciliation-recovery request schema is invalid"
        )
    parsed = urllib.parse.urlparse(str(request["url"]))
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ContractError(
            "CME reconciliation-recovery request URL is invalid"
        )
    for key in (
        "discovery_reasons",
        "link_texts",
        "source_notice_request_ids",
        "source_notice_urls",
        "source_titles",
    ):
        if (
            not isinstance(request.get(key), list)
            or any(type(item) is not str for item in request[key])
        ):
            raise ContractError(
                "CME reconciliation-recovery request metadata is invalid"
            )
    return dict(request)


def _validate_remaining_requests(
    requests: Sequence[object],
) -> list[dict[str, object]]:
    if len(requests) != NETWORK_REQUESTS:
        raise ContractError(
            "CME reconciliation-recovery request count is invalid"
        )
    normalized: list[dict[str, object]] = []
    previous_url = ""
    for ordinal, raw in enumerate(requests, start=FIRST_NETWORK_ORDINAL):
        if not isinstance(raw, dict):
            raise ContractError(
                "CME reconciliation-recovery request is invalid"
            )
        request = _validate_request(raw, expected_ordinal=ordinal)
        if str(request["url"]) <= previous_url:
            raise ContractError(
                "CME reconciliation-recovery request ordering is invalid"
            )
        previous_url = str(request["url"])
        normalized.append(request)
    if normalized[-1]["ordinal"] != LAST_NETWORK_ORDINAL:
        raise ContractError(
            "CME reconciliation-recovery final ordinal is invalid"
        )
    return normalized


def _validate_exclusion(exclusion: Mapping[str, object]) -> dict[str, object]:
    expected_keys = {
        "classification",
        "evidence_id",
        "evidence_kind",
        "evidence_path",
        "evidence_sha256",
        "ordinal",
        "request_id",
        "status",
        "url",
    }
    classification = exclusion.get("classification")
    if (
        set(exclusion) != expected_keys
        or type(exclusion.get("evidence_id")) is not str
        or _SHA256.fullmatch(str(exclusion["evidence_id"])) is None
        or exclusion.get("evidence_kind")
        not in {
            "DIAGNOSTIC_RESULT",
            "RECONCILIATION_NETWORK_404",
            "RECOVERY_FAILURE",
        }
        or type(exclusion.get("evidence_path")) is not str
        or not exclusion["evidence_path"]
        or type(exclusion.get("evidence_sha256")) is not str
        or _SHA256.fullmatch(str(exclusion["evidence_sha256"])) is None
        or type(exclusion.get("ordinal")) is not int
        or type(exclusion.get("request_id")) is not str
        or type(exclusion.get("url")) is not str
        or exclusion.get("status") != "EXCLUDED_AUTHORITATIVE_HTTP_404"
        or not isinstance(classification, dict)
        or classification.get("failure_code") != "HTTP_STATUS_REJECTED"
        or not isinstance(classification.get("safe_details"), dict)
        or classification["safe_details"].get("http_status") != 404
    ):
        raise ContractError(
            "CME reconciliation-recovery exclusion is invalid"
        )
    return dict(exclusion)


def _validate_known_exclusions(
    exclusions: Sequence[object],
) -> list[dict[str, object]]:
    normalized = [
        _validate_exclusion(item)
        for item in exclusions
        if isinstance(item, dict)
    ]
    if (
        len(normalized) != KNOWN_EXCLUSIONS
        or [item["ordinal"] for item in normalized]
        != list(KNOWN_EXCLUSION_ORDINALS)
        or len({item["url"] for item in normalized}) != KNOWN_EXCLUSIONS
    ):
        raise ContractError(
            "CME reconciliation-recovery exclusion set is invalid"
        )
    return normalized


def _payload_signature(path: Path, extension: str) -> str:
    with path.open("rb") as handle:
        head = handle.read(8)
    if extension == ".pdf" and head.startswith(b"%PDF-"):
        return "PDF_SIGNATURE_VERIFIED"
    if extension == ".csv" and head:
        return "NONEMPTY_CSV_BYTES"
    if extension == ".xls" and (
        head.startswith(bytes.fromhex("D0CF11E0A1B11AE1"))
        or head.startswith(b"PK")
    ):
        return "XLS_CONTAINER_SIGNATURE_VERIFIED"
    raise IntegrityError(
        "CME reconciliation-recovery preserved payload signature is invalid"
    )


def _source_context(
    *,
    plan_path: Path,
    approval_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    plan = validate_reconciliation_plan(
        _canonical_object(
            plan_path,
            description="CME interrupted reconciliation plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME interrupted reconciliation approval",
    )
    validate_reconciliation_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, remaining, reused, exclusions = reconciliation_authority(
        predecessor_plan_path=boundary.active_root
        / str(authority["predecessor_plan_path"]),
        predecessor_approval_path=boundary.active_root
        / str(authority["predecessor_approval_path"]),
        predecessor_failure_path=boundary.active_root
        / str(authority["predecessor_failure_path"]),
        boundary=boundary,
    )
    expected = build_reconciliation_plan(
        authority=derived,
        remaining_requests=remaining,
        known_exclusions=exclusions,
        implementation_sha256=scope["implementation_sha256"],  # type: ignore[arg-type]
    )
    if plan != expected or authority != derived:
        raise IntegrityError(
            "CME interrupted reconciliation source evidence changed"
        )
    return plan, approval, remaining, reused, exclusions


def _original_requests(
    plan: Mapping[str, object],
    *,
    boundary: RepoBoundary,
) -> list[dict[str, object]]:
    scope = plan["scope"]
    assert isinstance(scope, dict)
    authority = scope["authority"]
    assert isinstance(authority, dict)
    predecessor_plan = _canonical_object(
        boundary.active_root / str(authority["predecessor_plan_path"]),
        description="CME attachment recovery predecessor plan",
    )
    predecessor_scope = predecessor_plan.get("scope")
    if not isinstance(predecessor_scope, dict):
        raise IntegrityError(
            "CME attachment recovery predecessor scope is invalid"
        )
    predecessor_authority = predecessor_scope.get("authority")
    if not isinstance(predecessor_authority, dict):
        raise IntegrityError(
            "CME attachment recovery predecessor authority is invalid"
        )
    original_plan = validate_attachment_capture_plan(
        _canonical_object(
            boundary.active_root
            / str(predecessor_authority["predecessor_plan_path"]),
            description="CME original attachment plan",
        )
    )
    original_scope = original_plan["scope"]
    assert isinstance(original_scope, dict)
    requests = original_scope["requests"]
    if (
        not isinstance(requests, list)
        or len(requests) != TOTAL_CANDIDATES
        or any(not isinstance(item, dict) for item in requests)
    ):
        raise IntegrityError(
            "CME original attachment request set is invalid"
        )
    return [dict(item) for item in requests if isinstance(item, dict)]


def build_interruption_evidence(
    *,
    plan_path: Path,
    approval_path: Path,
    stage_path: Path,
    observed_at_utc: str,
    wrapper_exit_code: int,
    wrapper_timeout_seconds: int,
    boundary: RepoBoundary,
) -> dict[str, object]:
    for path, purpose, subtree in (
        (
            plan_path,
            "CME interrupted reconciliation plan",
            "reports/exchange_calendar",
        ),
        (
            approval_path,
            "CME interrupted reconciliation approval",
            "configs",
        ),
        (
            stage_path,
            "CME interrupted reconciliation stage",
            "state/data_publication_staging",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    if (
        _UTC_SECOND.fullmatch(observed_at_utc) is None
        or wrapper_exit_code != WRAPPER_EXIT_CODE
        or wrapper_timeout_seconds != WRAPPER_TIMEOUT_SECONDS
    ):
        raise ContractError(
            "CME reconciliation interruption observation is invalid"
        )
    plan, approval, remaining, reused, exclusions = _source_context(
        plan_path=plan_path,
        approval_path=approval_path,
        boundary=boundary,
    )
    plan_id = str(plan["plan_id"])
    if (
        predecessor_failure_path(boundary.active_root, plan_id).exists()
        or predecessor_release_for_plan(boundary.active_root, plan_id)
        is not None
        or (boundary.active_root / "state/locks/data-publication.lock").exists()
    ):
        raise IntegrityError(
            "CME interrupted reconciliation has a conflicting outcome"
        )
    if not stage_path.is_dir() or stage_path.is_symlink():
        raise IntegrityError(
            "CME interrupted reconciliation stage is invalid"
        )
    original = _original_requests(plan, boundary=boundary)
    request_by_ordinal = {int(item["ordinal"]): item for item in original}
    expected_ordinals = set(PRESERVED_ORDINALS)
    files = sorted(path for path in stage_path.iterdir() if path.is_file())
    if (
        len(files) != REUSED_RESPONSES
        or any(path.is_symlink() for path in files)
    ):
        raise IntegrityError(
            "CME interrupted reconciliation file count is invalid"
        )
    expected_names = {
        Path(str(request_by_ordinal[ordinal]["logical_path"])).name
        for ordinal in expected_ordinals
    }
    if {path.name for path in files} != expected_names:
        raise IntegrityError(
            "CME interrupted reconciliation file set is invalid"
        )
    reused_by_ordinal = {int(item["ordinal"]): item for item in reused}
    files_by_name = {path.name: path for path in files}
    responses: list[dict[str, object]] = []
    file_set: list[dict[str, object]] = []
    total_bytes = 0
    with ThreadPoolExecutor(
        max_workers=REUSE_VERIFICATION_WORKERS
    ) as executor:
        verification = []
        for ordinal in PRESERVED_ORDINALS:
            spec = request_by_ordinal[ordinal]
            name = Path(str(spec["logical_path"])).name
            physical = files_by_name[name]
            verification.append(
                (
                    ordinal,
                    spec,
                    name,
                    physical,
                    executor.submit(
                        lambda path: (
                            path.stat().st_size,
                            sha256_file(path),
                        ),
                        physical,
                    ),
                )
            )
        for ordinal, spec, name, physical, future in verification:
            size, digest = future.result()
            if size < 1 or size > MAX_RESPONSE_BYTES:
                raise IntegrityError(
                    "CME interrupted reconciliation payload size is invalid"
                )
            prior = reused_by_ordinal.get(ordinal)
            if prior is not None and (
                prior.get("size") != size
                or prior.get("sha256") != digest
                or prior.get("request_id") != spec["request_id"]
                or prior.get("url") != spec["url"]
            ):
                raise IntegrityError(
                    "CME interrupted reconciliation reused bytes changed"
                )
            signature = _payload_signature(physical, str(spec["extension"]))
            total_bytes += size
            file_set.append(
                {"name": name, "sha256": digest, "size": size}
            )
            responses.append(
                {
                    "acquisition": "INTERRUPTED_STAGE_HASH_VERIFIED",
                    "discovery_reasons": spec["discovery_reasons"],
                    "extension": spec["extension"],
                    "logical_path": spec["logical_path"],
                    "ordinal": ordinal,
                    "payload_signature": signature,
                    "request_id": spec["request_id"],
                    "request_kind": spec["request_kind"],
                    "response_metadata_status": (
                        "TRANSPORT_METADATA_NOT_PRESERVED_BY_WRAPPER_"
                        "INTERRUPTION"
                    ),
                    "sha256": digest,
                    "size": size,
                    "source_notice_request_ids": spec[
                        "source_notice_request_ids"
                    ],
                    "source_titles": spec["source_titles"],
                    "url": spec["url"],
                    "writer_contract_status": (
                        "PAYLOAD_WRITTEN_ONLY_AFTER_ACCEPTED_HTTP_200_"
                        "CONTENT_TYPE_AND_URL"
                    ),
                }
            )
    responses.sort(key=lambda item: int(item["ordinal"]))
    file_set.sort(key=lambda item: str(item["name"]))
    known_exclusions = _validate_known_exclusions(exclusions)
    possible_source = [
        item
        for item in remaining
        if int(item["ordinal"]) >= FIRST_NETWORK_ORDINAL
    ]
    possible = [
        {
            "ordinal": item["ordinal"],
            "prior_attempt_status": "POSSIBLY_IN_FLIGHT_NOT_ESTABLISHED",
            "request_id": item["request_id"],
            "url": item["url"],
        }
        for item in possible_source[:POSSIBLY_IN_FLIGHT_REQUESTS]
    ]
    if [item["ordinal"] for item in possible] != list(
        POSSIBLY_IN_FLIGHT_ORDINALS
    ):
        raise IntegrityError(
            "CME interruption possible in-flight set is invalid"
        )
    core: dict[str, object] = {
        "approval_path": approval_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "approval_receipt_id": approval["approval_receipt_id"],
        "approval_sha256": sha256_file(approval_path),
        "completed_network_first_ordinal": 17,
        "completed_network_last_ordinal": 56,
        "completed_network_response_count": (
            KNOWN_COMPLETED_NETWORK_RESPONSES
        ),
        "known_exclusions": known_exclusions,
        "known_exclusion_set_id": sha256_json(known_exclusions),
        "network_attempt_count_lower_bound": (
            KNOWN_COMPLETED_NETWORK_RESPONSES
        ),
        "network_attempt_count_status": "BOUNDED_NOT_EXACT",
        "network_attempt_count_upper_bound": (
            KNOWN_COMPLETED_NETWORK_RESPONSES
            + POSSIBLY_IN_FLIGHT_REQUESTS
        ),
        "observed_at_utc": observed_at_utc,
        "plan_id": plan_id,
        "plan_path": plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "plan_sha256": sha256_file(plan_path),
        "possibly_in_flight_request_count": (
            POSSIBLY_IN_FLIGHT_REQUESTS
        ),
        "possibly_in_flight_request_set_id": sha256_json(possible),
        "possibly_in_flight_requests": possible,
        "preserved_response_count": len(responses),
        "preserved_response_set_id": sha256_json(responses),
        "preserved_responses": responses,
        "preserved_stage_file_set_id": sha256_json(file_set),
        "preserved_stage_relative_path": stage_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "preserved_total_bytes": total_bytes,
        "publication_occurred": False,
        "schema_version": INTERRUPTION_SCHEMA,
        "source_union_release_id": plan["scope"]["authority"][  # type: ignore[index]
            "source_union_release_id"
        ],
        "status": "WRAPPER_INTERRUPTED_BEFORE_PROJECT_OUTCOME",
        "wrapper_exit_code": wrapper_exit_code,
        "wrapper_timeout_seconds": wrapper_timeout_seconds,
    }
    return {**core, "interruption_id": sha256_json(core)}


def validate_interruption_evidence(
    payload: Mapping[str, object],
) -> dict[str, object]:
    core = dict(payload)
    interruption_id = core.pop("interruption_id", None)
    responses = payload.get("preserved_responses")
    exclusions = payload.get("known_exclusions")
    possible = payload.get("possibly_in_flight_requests")
    if (
        type(interruption_id) is not str
        or interruption_id != sha256_json(core)
        or payload.get("schema_version") != INTERRUPTION_SCHEMA
        or payload.get("status")
        != "WRAPPER_INTERRUPTED_BEFORE_PROJECT_OUTCOME"
        or payload.get("publication_occurred") is not False
        or payload.get("wrapper_exit_code") != WRAPPER_EXIT_CODE
        or payload.get("wrapper_timeout_seconds")
        != WRAPPER_TIMEOUT_SECONDS
        or payload.get("preserved_response_count") != REUSED_RESPONSES
        or payload.get("completed_network_response_count")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        or payload.get("network_attempt_count_lower_bound")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        or payload.get("network_attempt_count_upper_bound")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        + POSSIBLY_IN_FLIGHT_REQUESTS
        or payload.get("network_attempt_count_status")
        != "BOUNDED_NOT_EXACT"
        or payload.get("possibly_in_flight_request_count")
        != POSSIBLY_IN_FLIGHT_REQUESTS
        or not isinstance(responses, list)
        or len(responses) != REUSED_RESPONSES
        or [item.get("ordinal") for item in responses if isinstance(item, dict)]
        != list(PRESERVED_ORDINALS)
        or payload.get("preserved_response_set_id")
        != sha256_json(responses)
        or not isinstance(exclusions, list)
        or payload.get("known_exclusion_set_id") != sha256_json(exclusions)
        or not isinstance(possible, list)
        or payload.get("possibly_in_flight_request_set_id")
        != sha256_json(possible)
        or [item.get("ordinal") for item in possible if isinstance(item, dict)]
        != list(POSSIBLY_IN_FLIGHT_ORDINALS)
    ):
        raise IntegrityError(
            "CME reconciliation interruption evidence is invalid"
        )
    _validate_known_exclusions(exclusions)
    return dict(payload)


def recovery_authority(
    *,
    interruption_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    boundary.assert_active_path(
        interruption_path,
        purpose="CME reconciliation interruption evidence",
        subtree="reports/exchange_calendar",
    )
    interruption = validate_interruption_evidence(
        _canonical_object(
            interruption_path,
            description="CME reconciliation interruption evidence",
        )
    )
    expected = build_interruption_evidence(
        plan_path=boundary.active_root / str(interruption["plan_path"]),
        approval_path=boundary.active_root
        / str(interruption["approval_path"]),
        stage_path=boundary.active_root
        / str(interruption["preserved_stage_relative_path"]),
        observed_at_utc=str(interruption["observed_at_utc"]),
        wrapper_exit_code=int(interruption["wrapper_exit_code"]),
        wrapper_timeout_seconds=int(
            interruption["wrapper_timeout_seconds"]
        ),
        boundary=boundary,
    )
    if interruption != expected:
        raise IntegrityError(
            "CME reconciliation interruption evidence changed"
        )
    plan = _canonical_object(
        boundary.active_root / str(interruption["plan_path"]),
        description="CME interrupted reconciliation plan",
    )
    original = _original_requests(plan, boundary=boundary)
    remaining = [
        dict(item)
        for item in original
        if int(item["ordinal"]) >= FIRST_NETWORK_ORDINAL
    ]
    remaining = _validate_remaining_requests(remaining)
    responses = [
        dict(item)
        for item in interruption["preserved_responses"]  # type: ignore[union-attr]
        if isinstance(item, dict)
    ]
    exclusions = _validate_known_exclusions(
        interruption["known_exclusions"]  # type: ignore[arg-type]
    )
    possible = [
        dict(item)
        for item in interruption["possibly_in_flight_requests"]  # type: ignore[union-attr]
        if isinstance(item, dict)
    ]
    if any(
        possible[index]["request_id"] != remaining[index]["request_id"]
        or possible[index]["url"] != remaining[index]["url"]
        for index in range(POSSIBLY_IN_FLIGHT_REQUESTS)
    ):
        raise IntegrityError(
            "CME reconciliation possible in-flight requests changed"
        )
    authority: dict[str, object] = {
        "completed_network_first_ordinal": interruption[
            "completed_network_first_ordinal"
        ],
        "completed_network_last_ordinal": interruption[
            "completed_network_last_ordinal"
        ],
        "completed_network_response_count": interruption[
            "completed_network_response_count"
        ],
        "first_remaining_request_id": remaining[0]["request_id"],
        "interruption_id": interruption["interruption_id"],
        "interruption_path": interruption_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "interruption_sha256": sha256_file(interruption_path),
        "known_exclusion_count": KNOWN_EXCLUSIONS,
        "known_exclusion_set_id": interruption[
            "known_exclusion_set_id"
        ],
        "last_remaining_request_id": remaining[-1]["request_id"],
        "network_attempt_count_lower_bound": interruption[
            "network_attempt_count_lower_bound"
        ],
        "network_attempt_count_upper_bound": interruption[
            "network_attempt_count_upper_bound"
        ],
        "plan_id": interruption["plan_id"],
        "plan_sha256": interruption["plan_sha256"],
        "possibly_in_flight_request_count": interruption[
            "possibly_in_flight_request_count"
        ],
        "possibly_in_flight_request_set_id": interruption[
            "possibly_in_flight_request_set_id"
        ],
        "preserved_response_count": interruption[
            "preserved_response_count"
        ],
        "preserved_response_set_id": interruption[
            "preserved_response_set_id"
        ],
        "preserved_stage_file_set_id": interruption[
            "preserved_stage_file_set_id"
        ],
        "preserved_stage_relative_path": interruption[
            "preserved_stage_relative_path"
        ],
        "preserved_total_bytes": interruption[
            "preserved_total_bytes"
        ],
        "remaining_request_count": len(remaining),
        "source_approval_receipt_id": interruption[
            "approval_receipt_id"
        ],
        "source_approval_sha256": interruption["approval_sha256"],
        "source_union_release_id": interruption[
            "source_union_release_id"
        ],
    }
    _validate_authority(authority)
    return authority, remaining, responses, exclusions, possible


def _validate_authority(authority: Mapping[str, object]) -> None:
    expected_keys = {
        "completed_network_first_ordinal",
        "completed_network_last_ordinal",
        "completed_network_response_count",
        "first_remaining_request_id",
        "interruption_id",
        "interruption_path",
        "interruption_sha256",
        "known_exclusion_count",
        "known_exclusion_set_id",
        "last_remaining_request_id",
        "network_attempt_count_lower_bound",
        "network_attempt_count_upper_bound",
        "plan_id",
        "plan_sha256",
        "possibly_in_flight_request_count",
        "possibly_in_flight_request_set_id",
        "preserved_response_count",
        "preserved_response_set_id",
        "preserved_stage_file_set_id",
        "preserved_stage_relative_path",
        "preserved_total_bytes",
        "remaining_request_count",
        "source_approval_receipt_id",
        "source_approval_sha256",
        "source_union_release_id",
    }
    hash_keys = {
        "interruption_id",
        "interruption_sha256",
        "known_exclusion_set_id",
        "plan_id",
        "plan_sha256",
        "possibly_in_flight_request_set_id",
        "preserved_response_set_id",
        "preserved_stage_file_set_id",
        "source_approval_receipt_id",
        "source_approval_sha256",
        "source_union_release_id",
    }
    if (
        set(authority) != expected_keys
        or any(
            type(authority.get(key)) is not str
            or _SHA256.fullmatch(str(authority[key])) is None
            for key in hash_keys
        )
        or authority.get("completed_network_first_ordinal") != 17
        or authority.get("completed_network_last_ordinal") != 56
        or authority.get("completed_network_response_count")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        or authority.get("known_exclusion_count") != KNOWN_EXCLUSIONS
        or authority.get("network_attempt_count_lower_bound")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        or authority.get("network_attempt_count_upper_bound")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        + POSSIBLY_IN_FLIGHT_REQUESTS
        or authority.get("possibly_in_flight_request_count")
        != POSSIBLY_IN_FLIGHT_REQUESTS
        or authority.get("preserved_response_count") != REUSED_RESPONSES
        or authority.get("remaining_request_count") != NETWORK_REQUESTS
        or authority.get("preserved_total_bytes", 0) < REUSED_RESPONSES
        or authority.get("preserved_total_bytes", 0) > MAX_TOTAL_BYTES
    ):
        raise ContractError(
            "CME reconciliation-recovery authority is invalid"
        )
    for key in (
        "first_remaining_request_id",
        "interruption_path",
        "last_remaining_request_id",
        "preserved_stage_relative_path",
    ):
        if type(authority.get(key)) is not str or not authority[key]:
            raise ContractError(
                "CME reconciliation-recovery authority path is invalid"
            )


def build_recovery_plan(
    *,
    authority: Mapping[str, object],
    remaining_requests: Sequence[object],
    known_exclusions: Sequence[object],
    possibly_in_flight_requests: Sequence[object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority(authority)
    requests = _validate_remaining_requests(remaining_requests)
    exclusions = _validate_known_exclusions(known_exclusions)
    possible = [
        dict(item)
        for item in possibly_in_flight_requests
        if isinstance(item, dict)
    ]
    if (
        len(possible) != POSSIBLY_IN_FLIGHT_REQUESTS
        or sha256_json(possible)
        != authority["possibly_in_flight_request_set_id"]
        or any(
            possible[index].get("request_id")
            != requests[index]["request_id"]
            or possible[index].get("url") != requests[index]["url"]
            for index in range(POSSIBLY_IN_FLIGHT_REQUESTS)
        )
        or {item["url"] for item in exclusions}
        & {item["url"] for item in requests}
    ):
        raise ContractError(
            "CME reconciliation-recovery prior-attempt scope is invalid"
        )
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "CME reconciliation-recovery implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_http_404_exclusion_and_continue": True,
        "allow_redirects": False,
        "authority": dict(authority),
        "execution_mode": "HIDDEN_BACKGROUND_MONITORED_NO_WRAPPER_TIMEOUT",
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "known_exclusions": exclusions,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_network_bytes": MAX_NETWORK_BYTES,
        "max_network_requests": NETWORK_REQUESTS,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": {
            "data_template": (
                "data/reference/exchange_calendars/{release_id}/"
                "{request_id}{extension}"
            ),
            "failure_report": (
                "reports/exchange_calendar/"
                "cme_historical_notice_attachment_reconciliation_"
                "recovery_failure_{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "runtime_stderr": (
                "state/run_logs/cme_attachment_reconciliation_recovery_"
                "{plan_prefix}.stderr.log"
            ),
            "runtime_stdout": (
                "state/run_logs/cme_attachment_reconciliation_recovery_"
                "{plan_prefix}.stdout.log"
            ),
            "staging_root": "state/data_publication_staging",
        },
        "possibly_repeated_requests": possible,
        "purpose": (
            "REUSE_54_HASH_VERIFIED_PAYLOADS_BIND_TWO_KNOWN_404S_"
            "ACKNOWLEDGE_TWO_POSSIBLE_PRIOR_ATTEMPTS_AND_RECONCILE_"
            "ONLY_ORDINALS_57_THROUGH_797"
        ),
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": requests,
        "retries": 0,
        "reused_response_count": REUSED_RESPONSES,
        "reuse_verification_workers": REUSE_VERIFICATION_WORKERS,
        "stop_conditions": list(STOP_CONDITIONS),
        "workers": WORKERS,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": OPERATION,
        "schema_version": PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_recovery_plan(
    payload: Mapping[str, object],
) -> dict[str, object]:
    core = {key: value for key, value in payload.items() if key != "plan_id"}
    scope = payload.get("scope")
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version") != PLAN_SCHEMA
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("authority"), dict)
        or not isinstance(scope.get("requests"), list)
        or not isinstance(scope.get("known_exclusions"), list)
        or not isinstance(scope.get("possibly_repeated_requests"), list)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError(
            "CME reconciliation-recovery plan identity is invalid"
        )
    expected = build_recovery_plan(
        authority=scope["authority"],
        remaining_requests=scope["requests"],
        known_exclusions=scope["known_exclusions"],
        possibly_in_flight_requests=scope[
            "possibly_repeated_requests"
        ],
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME reconciliation-recovery plan differs from implementation"
        )
    return dict(payload)


def validate_recovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("operation") != OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation recovery lacks exact approval"
        )
    return str(approval["approval_receipt_id"])


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_attachment_reconciliation_"
            f"recovery_failure_{plan_id[:8]}.json"
        )
    )


def _existing_release_for_plan(root: Path, plan_id: str) -> Path | None:
    manifests = root / "manifests" / "data_releases" / "reference"
    if not manifests.is_dir():
        return None
    for path in sorted(manifests.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("release_kind") == RELEASE_KIND
            and payload.get("schema_version") == CAPTURE_SCHEMA
            and isinstance(payload.get("metadata"), dict)
            and payload["metadata"].get("plan_id") == plan_id
        ):
            return path
    return None


def _write_create_only(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(dict(payload)) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _failure_report(
    *,
    plan: Mapping[str, object],
    plan_path: Path,
    approval_id: str,
    stage: Path,
    attempted: int,
    responses: Sequence[Mapping[str, object]],
    exclusions: Sequence[Mapping[str, object]],
    failures: Sequence[Mapping[str, object]],
    elapsed_milliseconds: int,
    boundary: RepoBoundary,
) -> dict[str, object]:
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "elapsed_milliseconds": elapsed_milliseconds,
        "exclusions_preserved": list(exclusions),
        "exclusions_preserved_count": len(exclusions),
        "failed_requests": list(failures),
        "network_requests_attempted": attempted,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "publication_occurred": False,
        "responses_preserved": list(responses),
        "responses_preserved_count": len(responses),
        "retries_performed": 0,
        "schema_version": FAILURE_SCHEMA,
        "stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
        "status": "STOPPED",
    }
    return {**core, "failure_id": sha256_json(core)}


def capture_reconciliation_recovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_recovery_plan(
        _canonical_object(
            plan_path,
            description="CME reconciliation-recovery plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME reconciliation-recovery approval",
    )
    approval_id = validate_recovery_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation-recovery implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, remaining, descriptors, known_exclusions, possible = (
        recovery_authority(
            interruption_path=root / str(authority["interruption_path"]),
            boundary=publisher.boundary,
        )
    )
    expected = build_recovery_plan(
        authority=derived,
        remaining_requests=remaining,
        known_exclusions=known_exclusions,
        possibly_in_flight_requests=possible,
        implementation_sha256=implementation_hashes(root),
    )
    if authority != derived or plan != expected:
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation-recovery evidence changed"
        )
    failure_path = _failure_path(root, str(plan["plan_id"]))
    prior_release = _existing_release_for_plan(root, str(plan["plan_id"]))
    if failure_path.exists() or prior_release is not None:
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation-recovery already has an outcome"
        )
    source_stage = root / str(authority["preserved_stage_relative_path"])
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {
        str(item["url"]) for item in requests if isinstance(item, dict)
    }
    protected_urls = {
        str(item["url"]) for item in known_exclusions
    } | {str(item["url"]) for item in descriptors}
    if len(allowed) != NETWORK_REQUESTS or allowed & protected_urls:
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation-recovery allowlist is invalid"
        )
    stage = publisher.create_stage(
        "cme_notice_attachment_reconciliation_recovery"
    )
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    responses: list[dict[str, object]] = []
    exclusions = [dict(item) for item in known_exclusions]
    total_bytes = 0
    for raw in descriptors:
        name = Path(str(raw["logical_path"])).name
        source = source_stage / name
        target = stage / name
        shutil.copyfile(source, target)
        if (
            target.stat().st_size != raw["size"]
            or sha256_file(target) != raw["sha256"]
            or _payload_signature(target, str(raw["extension"]))
            != raw["payload_signature"]
        ):
            raise NoticeAttachmentReconciliationRecoveryError(
                "CME reconciliation-recovery copy verification failed"
            )
        logical = str(raw["logical_path"])
        logical_paths[name] = logical
        staged_paths[logical] = name
        total_bytes += target.stat().st_size
        responses.append(
            {**dict(raw), "acquisition": "RECOVERY_REUSED_HASH_VERIFIED"}
        )
    if (
        len(responses) != REUSED_RESPONSES
        or total_bytes != authority["preserved_total_bytes"]
    ):
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation-recovery reuse set changed"
        )
    started = monotonic_time.monotonic()
    network_bytes = 0
    attempted = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for offset in range(0, len(requests), WORKERS):
            elapsed = int((monotonic_time.monotonic() - started) * 1_000)
            if elapsed >= MAX_DURATION_SECONDS * 1_000:
                failure = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    exclusions=exclusions,
                    failures=[
                        {
                            "error_class": "DURATION_CEILING_REACHED",
                            "failure_code": "DURATION_CEILING_REACHED",
                            "request_id": requests[offset]["request_id"],  # type: ignore[index]
                            "safe_details": {},
                        }
                    ],
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, failure)
                raise NoticeAttachmentReconciliationRecoveryError(
                    "CME reconciliation-recovery duration limit reached"
                )
            batch = requests[offset : offset + WORKERS]
            futures = [
                (
                    spec,
                    executor.submit(fetch_attachment, spec, allowed=allowed),
                )
                for spec in batch
                if isinstance(spec, dict)
            ]
            attempted += len(futures)
            completed: list[
                tuple[
                    Mapping[str, object],
                    bytes,
                    str,
                    dict[str, str],
                    str,
                ]
            ] = []
            failures: list[dict[str, object]] = []
            for spec, future in futures:
                try:
                    body, content_type, safe_headers, received_at = (
                        future.result()
                    )
                    completed.append(
                        (
                            spec,
                            body,
                            content_type,
                            safe_headers,
                            received_at,
                        )
                    )
                except NoticeAttachmentRequestError as exc:
                    exclusion = _network_exclusion(spec=spec, exc=exc)
                    if exclusion is not None:
                        exclusions.append(exclusion)
                    else:
                        failures.append(
                            attachment_failure_evidence(exc, spec=spec)
                        )
                except Exception as exc:
                    failures.append(
                        attachment_failure_evidence(exc, spec=spec)
                    )
            for spec, body, content_type, safe_headers, received_at in sorted(
                completed,
                key=lambda item: int(item[0]["ordinal"]),
            ):
                request_id = str(spec["request_id"])
                extension = str(spec["extension"])
                name = f"{request_id}{extension}"
                staged = stage / name
                staged.write_bytes(body)
                logical = str(spec["logical_path"])
                logical_paths[name] = logical
                staged_paths[logical] = name
                network_bytes += len(body)
                total_bytes += len(body)
                responses.append(
                    {
                        "acquisition": "NETWORK",
                        "content_type": content_type,
                        "discovery_reasons": spec["discovery_reasons"],
                        "extension": extension,
                        "logical_path": logical,
                        "ordinal": spec["ordinal"],
                        "received_at_utc": received_at,
                        "request_id": request_id,
                        "request_kind": spec["request_kind"],
                        "safe_headers": safe_headers,
                        "sha256": sha256_file(staged),
                        "size": len(body),
                        "source_notice_request_ids": spec[
                            "source_notice_request_ids"
                        ],
                        "source_titles": spec["source_titles"],
                        "status_code": 200,
                        "url": spec["url"],
                    }
                )
            if network_bytes > MAX_NETWORK_BYTES or total_bytes > MAX_TOTAL_BYTES:
                failures.append(
                    {
                        "error_class": "TOTAL_BYTE_CEILING_REACHED",
                        "failure_code": "TOTAL_BYTE_CEILING_REACHED",
                        "request_id": (
                            completed[-1][0]["request_id"]
                            if completed
                            else batch[0]["request_id"]  # type: ignore[index]
                        ),
                        "safe_details": {},
                    }
                )
            if failures:
                elapsed = int((monotonic_time.monotonic() - started) * 1_000)
                failure = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    exclusions=exclusions,
                    failures=failures,
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, failure)
                raise NoticeAttachmentReconciliationRecoveryError(
                    "CME reconciliation recovery stopped on failure"
                )
    responses.sort(key=lambda item: int(item["ordinal"]))
    exclusions.sort(key=lambda item: int(item["ordinal"]))
    elapsed = int((monotonic_time.monotonic() - started) * 1_000)
    resolved_ordinals = {
        int(item["ordinal"]) for item in responses
    } | {int(item["ordinal"]) for item in exclusions}
    if (
        elapsed > MAX_DURATION_SECONDS * 1_000
        or attempted != NETWORK_REQUESTS
        or len(responses) + len(exclusions) != TOTAL_CANDIDATES
        or resolved_ordinals != set(range(1, TOTAL_CANDIDATES + 1))
        or network_bytes > MAX_NETWORK_BYTES
        or total_bytes > MAX_TOTAL_BYTES
    ):
        failure = _failure_report(
            plan=plan,
            plan_path=plan_path,
            approval_id=approval_id,
            stage=stage,
            attempted=attempted,
            responses=responses,
            exclusions=exclusions,
            failures=[
                {
                    "error_class": "FINAL_RECONCILIATION_BOUND_FAILED",
                    "failure_code": "FINAL_RECONCILIATION_BOUND_FAILED",
                    "safe_details": {},
                }
            ],
            elapsed_milliseconds=elapsed,
            boundary=publisher.boundary,
        )
        _write_create_only(failure_path, failure)
        raise NoticeAttachmentReconciliationRecoveryError(
            "CME reconciliation recovery is incomplete"
        )
    capture_core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_http_404_exclusion_and_continue": True,
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_network_bytes": MAX_NETWORK_BYTES,
            "max_network_requests": NETWORK_REQUESTS,
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "reuse_verification_workers": REUSE_VERIFICATION_WORKERS,
            "workers": WORKERS,
        },
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed,
        "exclusions": exclusions,
        "exclusions_count": len(exclusions),
        "known_completed_network_response_count": (
            KNOWN_COMPLETED_NETWORK_RESPONSES
        ),
        "network_bytes": network_bytes,
        "network_request_count": NETWORK_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "possibly_repeated_request_count": (
            POSSIBLY_IN_FLIGHT_REQUESTS
        ),
        "resolved_candidate_count": TOTAL_CANDIDATES,
        "responses": responses,
        "responses_count": len(responses),
        "reused_response_count": REUSED_RESPONSES,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": total_bytes,
        "unresolved_candidate_count": 0,
    }
    capture = {**capture_core, "capture_id": sha256_json(capture_core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(authority["source_union_release_id"]),),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "interruption_id": authority["interruption_id"],
            "plan_id": plan["plan_id"],
            "unresolved_candidate_count": 0,
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths=staged_paths,
    )
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path,
        publisher.boundary,
    )
    load_reconciliation_recovery_capture(
        receipt,
        boundary=publisher.boundary,
    )
    return receipt


def load_reconciliation_recovery_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
    verify_payload_files: bool = True,
) -> dict[str, object]:
    DataReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError(
            "CME reconciliation-recovery receipt belongs elsewhere"
        )
    manifest_path = boundary.active_root / receipt.manifest_path
    manifest = verify_data_release_manifest(
        manifest_path,
        boundary,
        verify_files=verify_payload_files,
    )
    if (
        receipt.phase != "reference"
        or manifest.release_id != receipt.release_id
        or manifest.release_kind != RELEASE_KIND
        or manifest.release_kind != receipt.release_kind
        or manifest.schema_version != CAPTURE_SCHEMA
        or manifest.schema_version != receipt.schema_version
        or sha256_file(manifest_path) != receipt.manifest_sha256
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
    ):
        raise IntegrityError(
            "CME reconciliation-recovery release is invalid"
        )
    raw = manifest.embedded_documents["capture_receipt.json"]
    if not isinstance(raw, dict):
        raise IntegrityError(
            "CME reconciliation-recovery receipt is invalid"
        )
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    responses = raw.get("responses")
    exclusions = raw.get("exclusions")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("network_request_count") != NETWORK_REQUESTS
        or raw.get("reused_response_count") != REUSED_RESPONSES
        or raw.get("known_completed_network_response_count")
        != KNOWN_COMPLETED_NETWORK_RESPONSES
        or raw.get("possibly_repeated_request_count")
        != POSSIBLY_IN_FLIGHT_REQUESTS
        or raw.get("resolved_candidate_count") != TOTAL_CANDIDATES
        or raw.get("unresolved_candidate_count") != 0
        or not isinstance(responses, list)
        or not isinstance(exclusions, list)
        or len(responses) != raw.get("responses_count")
        or len(exclusions) != raw.get("exclusions_count")
        or len(responses) + len(exclusions) != TOTAL_CANDIDATES
        or len(manifest.files) != len(responses)
        or any(not isinstance(item, dict) for item in responses)
        or any(not isinstance(item, dict) for item in exclusions)
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("unresolved_candidate_count") != 0
    ):
        raise IntegrityError(
            "CME reconciliation-recovery receipt contract is invalid"
        )
    for exclusion in exclusions:
        _validate_exclusion(exclusion)
    ordinals = {
        int(item["ordinal"])
        for item in responses + exclusions
        if isinstance(item, dict) and type(item.get("ordinal")) is int
    }
    if ordinals != set(range(1, TOTAL_CANDIDATES + 1)):
        raise IntegrityError(
            "CME reconciliation-recovery ordinal set is invalid"
        )
    return dict(raw)
