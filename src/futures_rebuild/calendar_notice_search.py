"""Offline derivation and approval-gated capture of CME Notices search."""

from __future__ import annotations

import html
import json
import re
import time as monotonic_time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .boundary import RepoBoundary
from .calendar_historical_discovery import (
    load_historical_source_discovery_capture,
)
from .calendar_notice_client import load_notice_client_capture
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation


ASSESSMENT_SCHEMA = "cme_historical_notice_search_endpoint_assessment/1.0.0"
PLAN_SCHEMA = "cme_historical_notice_search_capability_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_search_capability_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_search_capability_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_SEARCH_CAPABILITY"
RELEASE_KIND = "cme_historical_notice_search_capability_capture"
NOTICES_URL = "https://www.cmegroup.com/notices.html"
COMMON_URL = (
    "https://www.cmegroup.com/etc.clientlibs/cmegroupaem/clientlibs/"
    "common.d90a30652b9c2aa8ecaf4205681ea2f3.js"
)
COMPONENT_PATH = (
    "/content/cmegroup/en/notices/jcr:content/main-content-section/section/"
    "section-elements/search_sort_filter_d"
)
REQUIRED_START = "2010-06-06"
REQUIRED_END = "2026-07-13"
CAPABILITY_URL = (
    "https://www.cmegroup.com"
    f"{COMPONENT_PATH}.ssfajax.0.0.{REQUIRED_START}.{REQUIRED_END}.json"
)
MAX_REQUESTS = 1
MAX_TOTAL_BYTES = 8_388_608
MAX_DURATION_SECONDS = 30
REQUEST_TIMEOUT_SECONDS = 30
COMMON_RELEASE_KIND = "cme_trading_hours_client_dependency_capture"
COMMON_CAPTURE_SCHEMA = "cme_calendar_client_dependency_capture/1.0.0"
COMMON_DOCUMENT = "client_dependency_capture_receipt.json"
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_historical_discovery.py",
    "src/futures_rebuild/calendar_notice_client.py",
    "src/futures_rebuild/calendar_notice_search.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/exchange_calendar.py",
    "src/futures_rebuild/source_contract.py",
)
OUTPUT_PATHS = {
    "data_template": (
        "data/reference/exchange_calendars/{release_id}/"
        "001-historical-notice-search-capability.json"
    ),
    "manifest_template": "manifests/data_releases/reference/{release_id}.json",
    "publication_lock": "state/locks/data-publication.lock",
    "staging_root": "state/data_publication_staging",
}
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_RESULT_LINK",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "DOWNLOAD_NOTICE_DOCUMENT_OR_ARCHIVE_ATTACHMENT",
    "EVALUATE_OR_EXECUTE_CAPTURED_JAVASCRIPT",
    "FOLLOW_PAGINATION_OR_RESULT_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CLIENT_OR_ENDPOINT_EVIDENCE_DRIFT",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "client_capture_id",
    "client_manifest_path",
    "client_manifest_sha256",
    "client_release_id",
    "client_search_asset_sha256",
    "common_capture_id",
    "common_manifest_path",
    "common_manifest_sha256",
    "common_release_id",
    "common_response_sha256",
    "notices_capture_id",
    "notices_manifest_path",
    "notices_manifest_sha256",
    "notices_release_id",
    "notices_response_sha256",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class NoticeSearchCaptureError(UnauthorizedOperation):
    """Raised before or during the one-request Notices capability capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeSearchCaptureError(
            "CME historical Notices capture rejected an HTTP redirect"
        )


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
                f"CME historical Notices implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _load_common_capture(
    receipt: DataReleaseReceipt, *, boundary: RepoBoundary
) -> tuple[dict[str, object], dict[str, object]]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != COMMON_RELEASE_KIND
        or manifest.schema_version != COMMON_CAPTURE_SCHEMA
        or len(manifest.files) != 1
        or set(manifest.embedded_documents) != {COMMON_DOCUMENT}
    ):
        raise IntegrityError("accepted CME common-client release is invalid")
    raw = receipt.embedded_document(COMMON_DOCUMENT, boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("accepted CME common-client receipt is invalid")
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    response = raw.get("response")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != COMMON_CAPTURE_SCHEMA
        or not isinstance(response, dict)
        or response.get("url") != COMMON_URL
        or response.get("content_type")
        not in {"application/javascript", "text/javascript"}
        or response.get("status_code") != 200
        or type(response.get("logical_path")) is not str
    ):
        raise IntegrityError("accepted CME common-client contract is invalid")
    physical = receipt.resolve_file(str(response["logical_path"]), boundary)
    if (
        physical.stat().st_size != response.get("size")
        or sha256_file(physical) != response.get("sha256")
    ):
        raise IntegrityError("accepted CME common-client bytes changed")
    return dict(raw), response


def _component_tag(source: str) -> str:
    match = re.search(
        r'<div class="component react search-sort-filter-dynamic"[^>]*>',
        source,
        flags=re.IGNORECASE,
    )
    if match is None:
        raise IntegrityError("CME Notices search component is absent")
    return html.unescape(match.group(0))


def _attribute(tag: str, name: str) -> str:
    match = re.search(
        rf'\b{re.escape(name)}="([^"]*)"', tag, flags=re.IGNORECASE
    )
    if match is None:
        raise IntegrityError(f"CME Notices component attribute is absent: {name}")
    return html.unescape(match.group(1))


def _read_utf8(path: Path, *, description: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise IntegrityError(f"{description} is not readable UTF-8") from exc


def derive_notice_endpoint_evidence(
    *,
    notices_manifest_path: Path,
    client_manifest_path: Path,
    common_manifest_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    notices_path = boundary.assert_active_path(
        notices_manifest_path,
        purpose="CME Notices source manifest",
        subtree="manifests/data_releases/reference",
    )
    client_path = boundary.assert_active_path(
        client_manifest_path,
        purpose="CME Notices client manifest",
        subtree="manifests/data_releases/reference",
    )
    common_path = boundary.assert_active_path(
        common_manifest_path,
        purpose="accepted CME common-client manifest",
        subtree="manifests/data_releases/reference",
    )
    notices_receipt = DataReleaseReceipt.from_manifest(notices_path, boundary)
    client_receipt = DataReleaseReceipt.from_manifest(client_path, boundary)
    common_receipt = DataReleaseReceipt.from_manifest(common_path, boundary)
    notices_capture = load_historical_source_discovery_capture(
        notices_receipt, boundary=boundary
    )
    client_capture = load_notice_client_capture(
        client_receipt, boundary=boundary
    )
    common_capture, common_response = _load_common_capture(
        common_receipt, boundary=boundary
    )
    notices_response = notices_capture.get("response")
    client_responses = client_capture.get("responses")
    if (
        not isinstance(notices_response, dict)
        or not isinstance(client_responses, list)
    ):
        raise IntegrityError("CME Notices source/client evidence is invalid")
    notices_logical = notices_response.get("logical_path")
    if type(notices_logical) is not str:
        raise IntegrityError("CME Notices source path is invalid")
    notices_source = _read_utf8(
        notices_receipt.resolve_file(notices_logical, boundary),
        description="CME Notices source",
    )
    tag = _component_tag(notices_source)
    if (
        _attribute(tag, "data-path") != COMPONENT_PATH
        or _attribute(tag, "data-show-date-filters") != "true"
        or _attribute(tag, "data-sorting-mode") != "userSelected"
    ):
        raise IntegrityError("CME Notices search component contract drifted")
    common_references = {
        urllib.parse.urljoin(NOTICES_URL, value)
        for value in re.findall(
            r"""(?:src|href)=["']([^"']*common\.[0-9a-f]{32}\.js)["']""",
            notices_source,
            flags=re.IGNORECASE,
        )
    }
    if common_references != {COMMON_URL}:
        raise IntegrityError("CME Notices page binds another common client")
    search_response = next(
        (
            item
            for item in client_responses
            if isinstance(item, dict)
            and item.get("request_id") == "search-sort-filter-dynamic"
        ),
        None,
    )
    if not isinstance(search_response, dict) or type(
        search_response.get("logical_path")
    ) is not str:
        raise IntegrityError("CME Notices search client response is absent")
    search_source = _read_utf8(
        client_receipt.resolve_file(
            str(search_response["logical_path"]), boundary
        ),
        description="CME Notices search client",
    )
    common_source = _read_utf8(
        common_receipt.resolve_file(
            str(common_response["logical_path"]), boundary
        ),
        description="accepted CME common client",
    )
    search_fragments = (
        "C=r(26088)",
        (
            "(0,C.getSearchSortFilterResults)"
            '(r,V,ve,null,de,R,Ne,De,"userSelected"===Y,Je)'
        ),
        '(0,p.useState)("0")',
        'e.format("YYYY-MM-DD")',
    )
    common_fragments = (
        "var oe=r(20237)",
        "t.getSearchSortFilterResults=function",
        'p=".".concat(l,".").concat(u).concat(p)',
        'p=".".concat(f).concat(p)',
        'h="".concat(t,".ssfajax.").concat(r).concat(p,".json")',
    )
    if any(fragment not in search_source for fragment in search_fragments):
        raise IntegrityError(
            "CME Notices search-client call contract is incomplete"
        )
    if any(fragment not in common_source for fragment in common_fragments):
        raise IntegrityError(
            "accepted CME common-client endpoint contract is incomplete"
        )
    return {
        "capability_url": CAPABILITY_URL,
        "client_capture_id": client_capture["capture_id"],
        "client_manifest_path": client_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "client_manifest_sha256": sha256_file(client_path),
        "client_release_id": client_receipt.release_id,
        "client_search_asset_sha256": search_response["sha256"],
        "common_capture_id": common_capture["capture_id"],
        "common_manifest_path": common_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "common_manifest_sha256": sha256_file(common_path),
        "common_release_id": common_receipt.release_id,
        "common_response_sha256": common_response["sha256"],
        "component_path": COMPONENT_PATH,
        "date_format": "YYYY-MM-DD",
        "facade_module_id": 26088,
        "notices_capture_id": notices_capture["capture_id"],
        "notices_manifest_path": notices_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "notices_manifest_sha256": sha256_file(notices_path),
        "notices_release_id": notices_receipt.release_id,
        "notices_response_sha256": notices_response["sha256"],
        "page_index": 0,
        "required_coverage_end_trade_date": REQUIRED_END,
        "required_coverage_start_trade_date": REQUIRED_START,
        "service_module_id": 20237,
        "sort_index": "0",
    }


def build_notice_endpoint_assessment(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    if (
        evidence.get("capability_url") != CAPABILITY_URL
        or evidence.get("component_path") != COMPONENT_PATH
        or evidence.get("date_format") != "YYYY-MM-DD"
        or evidence.get("facade_module_id") != 26088
        or evidence.get("service_module_id") != 20237
        or evidence.get("page_index") != 0
        or evidence.get("sort_index") != "0"
        or evidence.get("required_coverage_start_trade_date") != REQUIRED_START
        or evidence.get("required_coverage_end_trade_date") != REQUIRED_END
    ):
        raise ContractError("CME historical Notices endpoint evidence is invalid")
    core: dict[str, object] = {
        **dict(evidence),
        "classification": (
            "EXACT_CME_NOTICES_SSF_AJAX_CAPABILITY_ENDPOINT_DERIVED"
        ),
        "forbidden_interpretations": [
            "CLIENT_CONTRACT_DOES_NOT_PROVE_HISTORICAL_RESULTS_EXIST",
            "CAPABILITY_RESPONSE_DOES_NOT_AUTHORIZE_PAGINATION_OR_RESULT_LINKS",
            "NOTICE_RESULTS_ARE_NOT_YET_EXCHANGE_SESSION_SEGMENTS",
        ],
        "next_authority": (
            "HASH_BOUND_CME_HISTORICAL_NOTICE_SEARCH_CAPABILITY_"
            "CAPTURE_APPROVAL_REQUIRED"
        ),
        "schema_version": ASSESSMENT_SCHEMA,
        "status": "ONE_EXACT_REQUIRED_RANGE_CAPABILITY_REQUEST",
    }
    return {**core, "assessment_id": sha256_json(core)}


def _validate_assessment(
    payload: Mapping[str, object], *, evidence: Mapping[str, object]
) -> str:
    expected = build_notice_endpoint_assessment(evidence)
    if dict(payload) != expected:
        raise IntegrityError(
            "CME historical Notices endpoint assessment is invalid"
        )
    return str(expected["assessment_id"])


def notice_search_authority(
    *,
    notices_manifest_path: Path,
    client_manifest_path: Path,
    common_manifest_path: Path,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    report_path = boundary.assert_active_path(
        assessment_path,
        purpose="CME historical Notices endpoint assessment",
        subtree="reports/exchange_calendar",
    )
    evidence = derive_notice_endpoint_evidence(
        notices_manifest_path=notices_manifest_path,
        client_manifest_path=client_manifest_path,
        common_manifest_path=common_manifest_path,
        boundary=boundary,
    )
    assessment = _canonical_object(
        report_path, description="CME historical Notices endpoint assessment"
    )
    assessment_id = _validate_assessment(assessment, evidence=evidence)
    return {
        "assessment_id": assessment_id,
        "assessment_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(report_path),
        "client_capture_id": evidence["client_capture_id"],
        "client_manifest_path": evidence["client_manifest_path"],
        "client_manifest_sha256": evidence["client_manifest_sha256"],
        "client_release_id": evidence["client_release_id"],
        "client_search_asset_sha256": evidence["client_search_asset_sha256"],
        "common_capture_id": evidence["common_capture_id"],
        "common_manifest_path": evidence["common_manifest_path"],
        "common_manifest_sha256": evidence["common_manifest_sha256"],
        "common_release_id": evidence["common_release_id"],
        "common_response_sha256": evidence["common_response_sha256"],
        "notices_capture_id": evidence["notices_capture_id"],
        "notices_manifest_path": evidence["notices_manifest_path"],
        "notices_manifest_sha256": evidence["notices_manifest_sha256"],
        "notices_release_id": evidence["notices_release_id"],
        "notices_response_sha256": evidence["notices_response_sha256"],
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError(
            "CME historical Notices capability authority schema is invalid"
        )
    for key in _AUTHORITY_KEYS - {
        "assessment_path",
        "client_manifest_path",
        "common_manifest_path",
        "notices_manifest_path",
    }:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError(
                "CME historical Notices capability authority hash is invalid"
            )
    for key in (
        "assessment_path",
        "client_manifest_path",
        "common_manifest_path",
        "notices_manifest_path",
    ):
        if type(authority.get(key)) is not str:
            raise ContractError(
                "CME historical Notices capability authority path is invalid"
            )


def build_notice_search_capability_plan(
    *,
    authority: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority_shape(authority)
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "CME historical Notices implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": dict(OUTPUT_PATHS),
        "purpose": (
            "PROBE_EXACT_CME_HISTORICAL_NOTICE_RANGE_AND_PAGINATION_ONLY"
        ),
        "request": {
            "accept": "application/json",
            "request_id": "historical-notice-range-capability",
            "request_kind": "HISTORICAL_NOTICE_SEARCH_CAPABILITY",
            "url": CAPABILITY_URL,
        },
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "required_coverage_end_trade_date": REQUIRED_END,
        "required_coverage_start_trade_date": REQUIRED_START,
        "retries": 0,
        "stop_conditions": list(STOP_CONDITIONS),
        "workers": 1,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": OPERATION,
        "schema_version": PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_notice_search_capability_plan(
    payload: Mapping[str, object],
) -> dict[str, object]:
    if set(payload) != {
        "classification",
        "execution_authorized",
        "operation",
        "plan_id",
        "schema_version",
        "scope",
    }:
        raise IntegrityError(
            "CME historical Notices capability plan schema is invalid"
        )
    core = {key: value for key, value in payload.items() if key != "plan_id"}
    scope = payload.get("scope")
    if (
        payload.get("schema_version") != PLAN_SCHEMA
        or payload.get("classification") != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
    ):
        raise IntegrityError(
            "CME historical Notices capability plan identity is invalid"
        )
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    if not isinstance(authority, dict) or not isinstance(implementation, dict):
        raise IntegrityError(
            "CME historical Notices capability plan scope is invalid"
        )
    expected = build_notice_search_capability_plan(
        authority=authority,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME historical Notices capability plan differs from implementation"
        )
    return dict(payload)


def validate_notice_search_capability_approval(
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
        raise NoticeSearchCaptureError(
            "CME historical Notices capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        url != CAPABILITY_URL
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        != f"{COMPONENT_PATH}.ssfajax.0.0.{REQUIRED_START}.{REQUIRED_END}.json"
        or parsed.query
        or parsed.fragment
    ):
        raise NoticeSearchCaptureError(
            "CME historical Notices URL is outside the exact allowlist"
        )


def capture_notice_search_capability(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_notice_search_capability_plan(
        _canonical_object(
            plan_path, description="CME historical Notices capability plan"
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME historical Notices capability approval",
    )
    approval_id = validate_notice_search_capability_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise NoticeSearchCaptureError(
            "CME historical Notices implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived_authority = notice_search_authority(
        notices_manifest_path=publisher.boundary.active_root
        / str(authority["notices_manifest_path"]),
        client_manifest_path=publisher.boundary.active_root
        / str(authority["client_manifest_path"]),
        common_manifest_path=publisher.boundary.active_root
        / str(authority["common_manifest_path"]),
        assessment_path=publisher.boundary.active_root
        / str(authority["assessment_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived_authority:
        raise NoticeSearchCaptureError(
            "CME historical Notices capability authority changed"
        )
    request_spec = scope["request"]
    assert isinstance(request_spec, dict)
    url = str(request_spec["url"])
    _safe_url(url)
    stage = publisher.create_stage("cme_historical_notice_capability")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != 200 or response.geturl() != url:
                raise NoticeSearchCaptureError(
                    "CME historical Notices response is not exact HTTP 200"
                )
            if response.headers.get_content_type() != "application/json":
                raise NoticeSearchCaptureError(
                    "CME historical Notices content type is unexpected"
                )
            body = response.read(MAX_TOTAL_BYTES + 1)
            if len(body) > MAX_TOTAL_BYTES:
                raise NoticeSearchCaptureError(
                    "CME historical Notices byte ceiling is exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except NoticeSearchCaptureError:
        raise
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
    ) as exc:
        raise NoticeSearchCaptureError(
            "CME historical Notices request failed before publication"
        ) from exc
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if elapsed_milliseconds > MAX_DURATION_SECONDS * 1000:
        raise NoticeSearchCaptureError(
            "CME historical Notices duration ceiling is exceeded"
        )
    staged_name = "001-historical-notice-search-capability.json"
    staged = stage / staged_name
    staged.write_bytes(body)
    logical_path = (
        "data/reference/exchange_calendars/"
        "001-historical-notice-search-capability.json"
    )
    response_record = {
        "content_type": "application/json",
        "logical_path": logical_path,
        "received_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "request_id": "historical-notice-range-capability",
        "request_kind": "HISTORICAL_NOTICE_SEARCH_CAPABILITY",
        "safe_headers": dict(sorted(safe_headers.items())),
        "sha256": sha256_file(staged),
        "size": len(body),
        "status_code": 200,
        "url": url,
    }
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_requests": MAX_REQUESTS,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "workers": 1,
        },
        "capture_approval": dict(approval),
        "captured_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "elapsed_milliseconds": elapsed_milliseconds,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "request_count": 1,
        "response": response_record,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": len(body),
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths={staged_name: logical_path},
        source_release_ids=(
            str(authority["notices_release_id"]),
            str(authority["client_release_id"]),
            str(authority["common_release_id"]),
        ),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "assessment_id": authority["assessment_id"],
            "capture_id": capture_receipt["capture_id"],
            "captured_at_utc": core["captured_at_utc"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical_path: staged_name},
    )
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_notice_search_capability_capture(
        receipt, boundary=publisher.boundary
    )
    return receipt


def load_notice_search_capability_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != RELEASE_KIND
        or manifest.schema_version != CAPTURE_SCHEMA
        or len(manifest.files) != 1
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
        or set(manifest.metadata)
        != {
            "approval_receipt_id",
            "assessment_id",
            "capture_id",
            "captured_at_utc",
            "plan_id",
        }
    ):
        raise IntegrityError(
            "CME historical Notices capability release is invalid"
        )
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError(
            "CME historical Notices capability receipt is invalid"
        )
    payload = dict(raw)
    capture_id = payload.pop("capture_id", None)
    authority = payload.get("authority")
    response = payload.get("response")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(payload)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("request_count") != 1
        or not isinstance(authority, dict)
        or not isinstance(response, dict)
        or response.get("url") != CAPABILITY_URL
        or response.get("request_kind")
        != "HISTORICAL_NOTICE_SEARCH_CAPABILITY"
        or response.get("content_type") != "application/json"
        or response.get("status_code") != 200
        or type(response.get("logical_path")) is not str
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("approval_receipt_id")
        != raw.get("approval_receipt_id")
        or manifest.source_release_ids
        != tuple(
            sorted(
                (
                    str(authority.get("notices_release_id")),
                    str(authority.get("client_release_id")),
                    str(authority.get("common_release_id")),
                )
            )
        )
    ):
        raise IntegrityError(
            "CME historical Notices capability contract is invalid"
        )
    physical = receipt.resolve_file(str(response["logical_path"]), boundary)
    if (
        physical.stat().st_size != response.get("size")
        or sha256_file(physical) != response.get("sha256")
        or physical.stat().st_size != raw.get("total_bytes")
    ):
        raise IntegrityError(
            "CME historical Notices capability response bytes changed"
        )
    return dict(raw)
