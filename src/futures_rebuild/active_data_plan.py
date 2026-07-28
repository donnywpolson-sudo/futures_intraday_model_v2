"""Manifest-only planning for the certified causal active view."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path

from .active_data_view import (
    CERTIFICATION_STATE,
    UpdateMode,
    build_pending_approval,
    build_plan,
    cohort_for_year,
    disposition_for,
    selection_eligible,
    verify_contract,
)
from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import (
    LAYOUT_VERSION,
    MANIFEST_VERSION,
    DataReleaseManifest,
    manifest_relative_path,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease


FOUNDATION_SCHEMA_VERSION = "7.0.0"
FOUNDATION_RELEASE_KIND = "futures_mechanical_foundation_set"
POLICY_PLAN_SCHEMA = "causal_active_price_policy_plan/1.0.0"
POLICY_APPROVAL_SCHEMA = "causal_active_price_policy_approval/1.0.0"
POLICY_RELEASE_SCHEMA = "causal_active_price_research_policy/1.0.0"
POLICY_OPERATION = "ACCEPT_CAUSAL_ACTIVE_PRICE_RESEARCH_POLICY"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

SEMANTIC_PATHS = (
    "configs/active_data_view_contract.json",
    "configs/causal_price_research_policy.json",
    "configs/contract_economics_rules.json",
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/foundation_policy.json",
    "configs/historical_observability_policy.json",
    "configs/known_anomalies.json",
    "configs/provider_data_epochs.json",
    "configs/research_universe_contract.json",
    "configs/session_policy.json",
    "configs/source_contract.json",
    "configs/status_research_scope_policy.json",
)
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/active_data_certification.py",
    "src/futures_rebuild/active_data_plan.py",
    "src/futures_rebuild/active_data_view.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/foundation/decoder.py",
    "src/futures_rebuild/foundation/economics.py",
    "src/futures_rebuild/foundation/materialize.py",
    "src/futures_rebuild/foundation/parquet.py",
    "src/futures_rebuild/foundation/policy.py",
    "src/futures_rebuild/foundation/snapshot.py",
    "src/futures_rebuild/foundation/support.py",
    "src/futures_rebuild/locking.py",
)
ENVIRONMENT_PATHS = (
    "configs/dependency_lock_receipt.json",
    "configs/environment.lock.json",
    "configs/offline_vault_environment.lock.json",
    "requirements.lock",
    "requirements.sha256.lock",
)


def _canonical_object(path: Path, description: str) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _json_object(path: Path, description: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{description} is not a JSON object")
    return payload


def _bindings(root: Path, paths: Sequence[str]) -> dict[str, str]:
    return {path: sha256_file(root / path) for path in paths}


def _universe_markets(root: Path) -> tuple[str, ...]:
    universe = _canonical_object(
        root / "configs/research_universe_contract.json",
        "research-universe contract",
    )
    tiers = universe.get("tiers")
    if (
        universe.get("status") != "APPROVED"
        or universe.get("schema_version") != "glbx_research_universe/1.0.0"
        or not isinstance(tiers, list)
    ):
        raise IntegrityError("research-universe contract is not accepted")
    markets: set[str] = set()
    for tier in tiers:
        if isinstance(tier, dict) and tier.get("tier_id") in (3, 4):
            symbols = tier.get("symbols")
            if not isinstance(symbols, list) or any(
                not isinstance(symbol, str) for symbol in symbols
            ):
                raise IntegrityError("research-universe market list is invalid")
            markets.update(symbols)
    if len(markets) != 41:
        raise IntegrityError("research-universe contract does not contain 41 markets")
    return tuple(sorted(markets))


def _foundation(
    root: Path, release_id: str
) -> tuple[Path, DataReleaseManifest, dict[str, object]]:
    boundary = RepoBoundary(active_root=root)
    path = root / manifest_relative_path("foundation", release_id)
    manifest = verify_data_release_manifest(path, boundary, verify_files=False)
    document = manifest.embedded_documents.get("foundation_set.json")
    if (
        manifest.phase != "foundation"
        or manifest.release_kind != FOUNDATION_RELEASE_KIND
        or manifest.schema_version != FOUNDATION_SCHEMA_VERSION
        or not isinstance(document, dict)
        or document.get("schema_version") != FOUNDATION_SCHEMA_VERSION
        or document.get("interval_count") != len(document.get("intervals", ()))
        or document.get("historical_outcome_or_label_execution") is not False
        or document.get("model_fit_count") != 0
        or document.get("provider_call_count") != 0
        or document.get("wfa_execution_count") != 0
    ):
        raise IntegrityError("accepted foundation is not the required schema-7 release")
    return path, manifest, dict(document)


def _sanitized_interval(
    detailed: Mapping[str, object],
    observed: Mapping[str, object],
) -> dict[str, object]:
    required_receipts = (
        "causal_release_receipt",
        "definition_release_receipt",
        "economics_release_receipt",
        "raw_release_receipt",
    )
    if any(not isinstance(detailed.get(name), dict) for name in required_receipts):
        raise IntegrityError("foundation interval lacks required price-lineage receipts")
    expected_equal = (
        "bar_query_contract_id",
        "bar_source_path",
        "bar_source_sha256",
        "coverage_disposition",
        "end",
        "interval_key",
        "market",
        "start",
        "year",
    )
    if any(detailed.get(name) != observed.get(name) for name in expected_equal):
        raise IntegrityError("foundation interval observability and lineage differ")
    return {
        "bar_query_contract_id": detailed["bar_query_contract_id"],
        "bar_source_path": detailed["bar_source_path"],
        "bar_source_sha256": detailed["bar_source_sha256"],
        "calendar_claim": observed["calendar_claim"],
        "causal_release_receipt": dict(detailed["causal_release_receipt"]),
        "coverage_disposition": detailed["coverage_disposition"],
        "definition_query_contract_id": detailed["definition_query_contract_id"],
        "definition_release_receipt": dict(detailed["definition_release_receipt"]),
        "definition_source_path": detailed["definition_source_path"],
        "definition_source_sha256": detailed["definition_source_sha256"],
        "economics_release_receipt": dict(detailed["economics_release_receipt"]),
        "end": detailed["end"],
        "evidence_basis": observed["evidence_basis"],
        "interval_key": detailed["interval_key"],
        "market": detailed["market"],
        "observed_bar_rows": observed["observed_bar_rows"],
        "raw_release_receipt": dict(detailed["raw_release_receipt"]),
        "research_admissible": observed["research_admissible"],
        "session_roll_role": observed["session_roll_role"],
        "source_dbn_release_id": observed["source_dbn_release_id"],
        "start": detailed["start"],
        "uncertainty_rule": observed["uncertainty_rule"],
        "year": detailed["year"],
    }


def derive_inventory(
    *, repository_root: Path, foundation_release_id: str
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    verify_contract(root)
    foundation_path, manifest, document = _foundation(root, foundation_release_id)
    markets = _universe_markets(root)
    coverage = document.get("historical_observability_coverage")
    detailed_raw = document.get("intervals")
    if (
        not isinstance(coverage, dict)
        or coverage.get("calendar_claim")
        != "NOT_OFFICIAL_HISTORICAL_CME_SESSION_AUTHORITY"
        or coverage.get("evidence_basis")
        != "IMMUTABLE_ACCEPTED_DATABENTO_DBN_OBSERVABILITY"
        or coverage.get("row_admission")
        != (
            "ACTUAL_DECODED_SOURCE_ROWS_ONLY_NO_FILL_INTERPOLATION_"
            "SYNTHETIC_OPEN_OR_SYNTHETIC_CLOSE"
        )
        or coverage.get("uncertainty_rule")
        != "UNOBSERVED_TIME_IS_MISSING_NOT_CLOSED"
        or not isinstance(coverage.get("intervals"), list)
        or not isinstance(detailed_raw, list)
    ):
        raise IntegrityError("foundation historical-observability boundary is invalid")
    detailed = {
        str(item["interval_key"]): item
        for item in detailed_raw
        if isinstance(item, dict) and isinstance(item.get("interval_key"), str)
    }
    observed = {
        str(item["interval_key"]): item
        for item in coverage["intervals"]
        if isinstance(item, dict) and isinstance(item.get("interval_key"), str)
    }
    if (
        len(detailed) != len(detailed_raw)
        or len(observed) != len(coverage["intervals"])
        or set(detailed) != set(observed)
    ):
        raise IntegrityError("foundation interval indexes are incomplete or ambiguous")
    grouped: dict[tuple[str, int], list[dict[str, object]]] = {}
    for key in sorted(detailed):
        interval = _sanitized_interval(detailed[key], observed[key])
        market = str(interval["market"])
        year = int(interval["year"])
        if market not in markets:
            raise IntegrityError("foundation contains a market outside the universe")
        grouped.setdefault((market, year), []).append(interval)
    entries: list[dict[str, object]] = []
    dispositions: dict[str, int] = {}
    split_market_years = 0
    selected_interval_count = 0
    selected_row_count = 0
    for (market, year), intervals in sorted(grouped.items()):
        intervals.sort(key=lambda item: (str(item["start"]), str(item["end"])))
        for previous, current in zip(intervals, intervals[1:]):
            if previous["end"] != current["start"]:
                raise IntegrityError("foundation market-year intervals are not contiguous")
        admissible = all(bool(item["research_admissible"]) for item in intervals)
        disposition = disposition_for(year=year, research_admissible=admissible)
        cohort = cohort_for_year(year)
        entry = {
            "cohort": cohort,
            "coverage_end": intervals[-1]["end"],
            "coverage_kind": (
                "FULL_YEAR"
                if intervals[0]["start"] == f"{year}-01-01"
                and intervals[-1]["end"] == f"{year + 1}-01-01"
                else "PARTIAL_YEAR"
            ),
            "coverage_start": intervals[0]["start"],
            "disposition": disposition,
            "intervals": intervals,
            "market": market,
            "selection_eligible": selection_eligible(
                cohort=cohort, disposition=disposition
            ),
            "year": year,
        }
        entries.append(entry)
        dispositions[disposition] = dispositions.get(disposition, 0) + 1
        if disposition == CERTIFICATION_STATE:
            selected_interval_count += len(intervals)
            selected_row_count += sum(int(item["observed_bar_rows"]) for item in intervals)
            split_market_years += int(len(intervals) > 1)
    counts = {
        "certification_candidates": dispositions.get(CERTIFICATION_STATE, 0),
        "discovery_selection_eligible": sum(
            int(bool(item["selection_eligible"])) for item in entries
        ),
        "forward_only": dispositions.get("FORWARD_ONLY_NOT_MATERIALIZED", 0),
        "holdout": dispositions.get("LOCKED_HOLDOUT_NOT_MATERIALIZED", 0),
        "market_count": len({str(item["market"]) for item in entries}),
        "market_year_count": len(entries),
        "quarantined": dispositions.get("QUARANTINED_NOT_MATERIALIZED", 0),
        "selected_interval_count": selected_interval_count,
        "selected_row_count": selected_row_count,
        "split_market_years": split_market_years,
    }
    expected = {
        "certification_candidates": 562,
        "discovery_selection_eligible": 198,
        "forward_only": 41,
        "holdout": 41,
        "market_count": 41,
        "market_year_count": 650,
        "quarantined": 6,
    }
    if any(counts[name] != value for name, value in expected.items()):
        raise IntegrityError(
            f"live foundation counts differ from the reviewed snapshot: {counts}"
        )
    return {
        "counts": counts,
        "disposition_counts": dict(sorted(dispositions.items())),
        "entries": entries,
        "foundation_manifest_path": foundation_path.relative_to(root).as_posix(),
        "foundation_manifest_sha256": sha256_file(foundation_path),
        "foundation_release_id": manifest.release_id,
        "foundation_schema_version": manifest.schema_version,
        "inventory_id": sha256_json(entries),
        "markets": list(markets),
    }


def build_policy_successor_plan(
    *, repository_root: Path, foundation_release_id: str
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    inventory = derive_inventory(
        repository_root=root, foundation_release_id=foundation_release_id
    )
    policy = {
        "capability": "RESEARCH_READY_CAUSAL_PRICE",
        "cohort_roles": {
            "2010": "DATA_QUALITY_ONLY",
            "2011": "FORMATION_CONTEXT",
            "2012-2016": "LEGACY_FEED_STRESS",
            "2017": "FEED_TRANSITION_STRESS",
            "2018-2022": "DISCOVERY_SELECTION",
            "2023-2024": "NON_PRISTINE_RESEARCH",
            "2025": "LOCKED_HOLDOUT",
            "2026": "FORWARD_ONLY",
        },
        "does_not_authorize": [
            "ACTIVE_VIEW_PUBLICATION",
            "EVALUATION_OR_PREDICTION",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MODEL_FIT",
            "OUTCOME_OR_LABEL_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "TRADING",
        ],
        "historical_calendar_claim": (
            "NOT_OFFICIAL_HISTORICAL_CME_SESSION_AUTHORITY"
        ),
        "historical_evidence_basis": (
            "IMMUTABLE_ACCEPTED_DATABENTO_DBN_OBSERVABILITY"
        ),
        "new_year_rule": "EXPLICIT_COHORT_ASSIGNMENT_REQUIRED_FAIL_CLOSED",
        "pre_2025_status_dependent_use": "FORBIDDEN",
        "schema_version": POLICY_RELEASE_SCHEMA,
        "selection_rule": "DISCOVERY_SELECTION_AND_CERTIFIED_ONLY",
        "uncertainty_rule": "UNOBSERVED_TIME_IS_MISSING_NOT_CLOSED",
    }
    core: dict[str, object] = {
        "environment_bindings": _bindings(root, ENVIRONMENT_PATHS),
        "foundation_manifest_sha256": inventory["foundation_manifest_sha256"],
        "foundation_release_id": inventory["foundation_release_id"],
        "implementation_bindings": _bindings(root, IMPLEMENTATION_PATHS),
        "operation": POLICY_OPERATION,
        "policy": policy,
        "predecessor_policy_path": "configs/causal_price_research_policy.json",
        "predecessor_policy_sha256": sha256_file(
            root / "configs/causal_price_research_policy.json"
        ),
        "schema_version": POLICY_PLAN_SCHEMA,
        "semantic_bindings": _bindings(root, SEMANTIC_PATHS),
        "snapshot_counts": inventory["counts"],
    }
    return {**core, "plan_id": sha256_json(core)}


def build_policy_pending_approval(plan: Mapping[str, object]) -> dict[str, object]:
    plan_id = plan.get("plan_id")
    if (
        not isinstance(plan_id, str)
        or plan_id != sha256_json({key: value for key, value in plan.items() if key != "plan_id"})
        or plan.get("operation") != POLICY_OPERATION
    ):
        raise IntegrityError("price-policy successor plan is invalid")
    return {
        "approval_receipt_id": None,
        "approved_at": None,
        "operation": POLICY_OPERATION,
        "plan_id": plan_id,
        "plan_sha256": sha256_json(plan),
        "schema_version": POLICY_APPROVAL_SCHEMA,
        "status": "PENDING",
        "user_authorization_id": None,
    }


def validate_policy_approval(
    approval: Mapping[str, object], plan: Mapping[str, object]
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
        or approval.get("schema_version") != POLICY_APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != POLICY_OPERATION
        or approval.get("plan_id") != plan.get("plan_id")
        or approval.get("plan_sha256") != sha256_json(plan)
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise UnauthorizedOperation(
            "price-policy successor lacks exact hash-bound acceptance"
        )
    return str(approval["approval_receipt_id"])


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    encoded = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path,
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0),
        )
    except FileExistsError:
        if not path.is_file() or path.read_bytes() != encoded:
            raise IntegrityError(f"existing policy artifact differs: {path}")
        return
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_policy_successor(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> tuple[Path, dict[str, object]]:
    root = repository_root.resolve(strict=True)
    approval_id = validate_policy_approval(approval, plan)
    rebuilt = build_policy_successor_plan(
        repository_root=root,
        foundation_release_id=str(plan["foundation_release_id"]),
    )
    if rebuilt != dict(plan):
        raise IntegrityError("price-policy successor inputs changed after planning")
    manifest_core: dict[str, object] = {
        "embedded_documents": {
            "causal_active_price_research_policy.json": dict(plan["policy"])
        },
        "files": [],
        "layout_version": LAYOUT_VERSION,
        "manifest_version": MANIFEST_VERSION,
        "metadata": {
            "approval_receipt_id": approval_id,
            "foundation_release_id": plan["foundation_release_id"],
            "plan_id": plan["plan_id"],
            "predecessor_policy_sha256": plan["predecessor_policy_sha256"],
        },
        "phase": "controls",
        "release_kind": "causal_active_price_research_policy",
        "schema_version": POLICY_RELEASE_SCHEMA,
        "source_release_ids": [],
    }
    manifest = DataReleaseManifest(
        release_id=sha256_json(manifest_core),
        phase="controls",
        release_kind="causal_active_price_research_policy",
        schema_version=POLICY_RELEASE_SCHEMA,
        source_release_ids=(),
        files=(),
        embedded_documents=manifest_core["embedded_documents"],
        metadata=manifest_core["metadata"],
    )
    manifest_path = root / manifest_relative_path("controls", manifest.release_id)
    lock = root / "state/locks/active_data_policy.lock"
    with FileLease(lock):
        _write_new_or_exact(manifest_path, manifest.as_dict())
        verified = verify_data_release_manifest(
            manifest_path, RepoBoundary(active_root=root), verify_files=False
        )
        if verified.as_dict() != manifest.as_dict():
            raise IntegrityError("published price-policy manifest differs")
        receipt_core: dict[str, object] = {
            "approval_receipt_id": approval_id,
            "manifest_path": manifest_path.relative_to(root).as_posix(),
            "manifest_sha256": sha256_file(manifest_path),
            "plan_id": plan["plan_id"],
            "policy_release_id": manifest.release_id,
            "schema_version": "causal_active_price_policy_acceptance/1.0.0",
            "state": "ACCEPTED_NON_AUTHORIZING",
        }
        receipt = {
            **receipt_core,
            "policy_acceptance_receipt_id": sha256_json(receipt_core),
        }
        receipt_path = (
            root
            / "manifests/active_data_view/policy_acceptance"
            / f"{receipt['policy_acceptance_receipt_id']}.json"
        )
        _write_new_or_exact(receipt_path, receipt)
    return manifest_path, receipt


def verify_policy_acceptance(
    *,
    repository_root: Path,
    policy_release_id: str,
    policy_acceptance_receipt_id: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    if (
        _SHA256.fullmatch(policy_release_id) is None
        or _SHA256.fullmatch(policy_acceptance_receipt_id) is None
    ):
        raise ContractError("accepted price-policy identities are invalid")
    manifest_path = root / manifest_relative_path("controls", policy_release_id)
    manifest = verify_data_release_manifest(
        manifest_path, RepoBoundary(active_root=root), verify_files=False
    )
    receipt_path = (
        root
        / "manifests/active_data_view/policy_acceptance"
        / f"{policy_acceptance_receipt_id}.json"
    )
    receipt = _canonical_object(
        receipt_path, "active price-policy acceptance receipt"
    )
    receipt_core = {
        key: value
        for key, value in receipt.items()
        if key != "policy_acceptance_receipt_id"
    }
    if (
        manifest.release_kind != "causal_active_price_research_policy"
        or manifest.schema_version != POLICY_RELEASE_SCHEMA
        or set(manifest.embedded_documents)
        != {"causal_active_price_research_policy.json"}
        or receipt.get("policy_acceptance_receipt_id")
        != sha256_json(receipt_core)
        or receipt.get("policy_acceptance_receipt_id")
        != policy_acceptance_receipt_id
        or receipt.get("policy_release_id") != policy_release_id
        or receipt.get("manifest_path")
        != manifest_path.relative_to(root).as_posix()
        or receipt.get("manifest_sha256") != sha256_file(manifest_path)
        or receipt.get("state") != "ACCEPTED_NON_AUTHORIZING"
    ):
        raise IntegrityError("active price-policy acceptance is invalid")
    return receipt


def build_supersession_record(
    *,
    repository_root: Path,
    predecessor_plan_path: str,
    successor_plan: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    predecessor_path = root / predecessor_plan_path
    predecessor = _canonical_object(
        predecessor_path, "predecessor causal materialization plan"
    )
    predecessor_id = predecessor.get("materialization_plan_id") or predecessor.get(
        "plan_id"
    )
    successor_id = successor_plan.get("plan_id")
    if (
        not isinstance(predecessor_id, str)
        or _SHA256.fullmatch(predecessor_id) is None
        or not isinstance(successor_id, str)
        or _SHA256.fullmatch(successor_id) is None
    ):
        raise IntegrityError("causal plan supersession identities are invalid")
    core: dict[str, object] = {
        "predecessor_plan_id": predecessor_id,
        "predecessor_plan_path": predecessor_plan_path,
        "predecessor_plan_sha256": sha256_file(predecessor_path),
        "preservation_rule": "PREDECESSOR_BYTES_UNCHANGED_NO_APPROVAL_REUSE",
        "schema_version": "causal_active_plan_supersession/1.0.0",
        "state": "SUPERSEDED_NONDESTRUCTIVE_NON_AUTHORIZING",
        "successor_plan_id": successor_id,
        "successor_plan_sha256": sha256_json(successor_plan),
    }
    return {**core, "supersession_id": sha256_json(core)}


def _manifest_source_objects(
    *,
    root: Path,
    manifest: DataReleaseManifest,
    manifest_path: Path,
    object_class: str,
) -> list[dict[str, object]]:
    objects = [
        {
            "object_class": f"{object_class}_MANIFEST",
            "path": manifest_path.relative_to(root).as_posix(),
            "sha256": sha256_file(manifest_path),
            "size": manifest_path.stat().st_size,
        }
    ]
    for entry in manifest.files:
        physical = root / manifest.physical_relative_path(entry)
        objects.append(
            {
                "logical_path": entry.logical_path,
                "object_class": object_class,
                "path": physical.relative_to(root).as_posix(),
                "sha256": entry.sha256,
                "size": entry.size,
            }
        )
    return objects


def _aggregation_sources(
    *,
    root: Path,
    dbn_manifest: DataReleaseManifest,
    interval: Mapping[str, object],
) -> list[dict[str, object]]:
    indexed = {entry.logical_path: entry for entry in dbn_manifest.files}
    market = str(interval["market"])
    year = int(interval["year"])
    start = str(interval["start"])
    end = str(interval["end"])
    result: list[dict[str, object]] = []
    for schema, directory in (("ohlcv-1h", "ohlcv_1h"), ("ohlcv-1d", "ohlcv_1d")):
        logical = (
            f"data/dbn/{directory}/{market}/{year}/{start}_{end}.dbn.zst"
        )
        sidecar_logical = f"{logical}.manifest.json"
        source = indexed.get(logical)
        sidecar = indexed.get(sidecar_logical)
        if source is None or sidecar is None:
            continue
        query_sidecar = _json_object(
            root / dbn_manifest.physical_relative_path(sidecar),
            "provider aggregate download sidecar",
        )
        # The DBN release is flat, but retain the manifest-resolved path rather
        # than discovering it from the filesystem.
        query = {
            "schema": schema,
            "market": market,
            "start": start,
            "end": end,
            "stype_in": query_sidecar["stype_in"],
            "symbols": query_sidecar["symbols_requested"],
        }
        from .source_symbology import build_query_contract

        query_contract = build_query_contract(**query)
        result.append(
            {
                "query_contract_id": query_contract["query_contract_id"],
                "relative_path": logical.removeprefix("data/"),
                "schema": schema,
                "sha256": source.sha256,
                "sidecar_relative_path": sidecar_logical.removeprefix("data/"),
                "sidecar_sha256": sidecar.sha256,
                "sidecar_size": sidecar.size,
                "size": source.size,
            }
        )
    return result


def build_pilot_plan(
    *,
    repository_root: Path,
    foundation_release_id: str,
    accepted_policy_release_id: str,
    policy_acceptance_receipt_id: str,
) -> tuple[dict[str, object], dict[str, object]]:
    root = repository_root.resolve(strict=True)
    verify_policy_acceptance(
        repository_root=root,
        policy_release_id=accepted_policy_release_id,
        policy_acceptance_receipt_id=policy_acceptance_receipt_id,
    )
    inventory = derive_inventory(
        repository_root=root, foundation_release_id=foundation_release_id
    )
    requested = {("6A", 2010), ("ES", 2022)}
    entries = [
        item
        for item in inventory["entries"]
        if (str(item["market"]), int(item["year"])) in requested
    ]
    if (
        {(str(item["market"]), int(item["year"])) for item in entries} != requested
        or any(item["disposition"] != CERTIFICATION_STATE for item in entries)
        or next(item for item in entries if item["market"] == "6A")[
            "coverage_kind"
        ]
        != "PARTIAL_YEAR"
        or next(item for item in entries if item["market"] == "ES")[
            "coverage_kind"
        ]
        != "FULL_YEAR"
    ):
        raise IntegrityError("exact pilot market-year scope is unavailable")
    source_objects: dict[str, dict[str, object]] = {}
    planned_entries: list[dict[str, object]] = []
    for entry in entries:
        intervals: list[dict[str, object]] = []
        entry_rows = 0
        entry_files: set[str] = set()
        for raw_interval in entry["intervals"]:
            interval = dict(raw_interval)
            dbn_id = str(interval["source_dbn_release_id"])
            dbn_manifest_path = root / manifest_relative_path("dbn", dbn_id)
            dbn_manifest = verify_data_release_manifest(
                dbn_manifest_path,
                RepoBoundary(active_root=root),
                verify_files=False,
            )
            interval["aggregation_sources"] = _aggregation_sources(
                root=root,
                dbn_manifest=dbn_manifest,
                interval=interval,
            )
            if len(interval["aggregation_sources"]) != 2:
                raise IntegrityError(
                    "pilot requires exact overlapping hourly and daily DBNs"
                )
            receipt_groups = (
                ("causal_release_receipt", "CAUSAL"),
                ("raw_release_receipt", "RAW"),
                ("definition_release_receipt", "REFERENCE_DEFINITION"),
                ("economics_release_receipt", "REFERENCE_ECONOMICS"),
            )
            for receipt_name, object_class in receipt_groups:
                receipt = interval[receipt_name]
                manifest_path = root / str(receipt["manifest_path"])
                manifest = verify_data_release_manifest(
                    manifest_path,
                    RepoBoundary(active_root=root),
                    verify_files=False,
                )
                if (
                    manifest.release_id != receipt["release_id"]
                    or sha256_file(manifest_path) != receipt["manifest_sha256"]
                ):
                    raise IntegrityError("pilot release receipt differs from its manifest")
                for item in _manifest_source_objects(
                    root=root,
                    manifest=manifest,
                    manifest_path=manifest_path,
                    object_class=object_class,
                ):
                    source_objects.setdefault(str(item["path"]), item)
                    entry_files.add(str(item["path"]))
                if object_class == "RAW":
                    receipt_entries = [
                        item
                        for item in manifest.files
                        if Path(item.logical_path).name == "interval_receipt.json"
                    ]
                    if len(receipt_entries) != 1:
                        raise IntegrityError("pilot raw receipt file is ambiguous")
                    receipt_payload = _canonical_object(
                        root / manifest.physical_relative_path(receipt_entries[0]),
                        "pilot raw interval receipt",
                    )
                    entry_rows += int(receipt_payload["bar_rows"])
                    entry_rows += int(receipt_payload["definition_rows_scanned"])
            indexed = {item.logical_path: item for item in dbn_manifest.files}
            for relative in (
                str(interval["bar_source_path"]),
                f"{interval['bar_source_path']}.manifest.json",
                str(interval["definition_source_path"]),
                f"{interval['definition_source_path']}.manifest.json",
            ):
                logical = f"data/{relative}"
                item = indexed.get(logical)
                if item is None:
                    raise IntegrityError("pilot DBN source is absent from its release")
                physical = root / dbn_manifest.physical_relative_path(item)
                source_objects.setdefault(
                    physical.relative_to(root).as_posix(),
                    {
                        "logical_path": logical,
                        "object_class": "DBN_OR_DOWNLOAD_SIDECAR",
                        "path": physical.relative_to(root).as_posix(),
                        "sha256": item.sha256,
                        "size": item.size,
                    },
                )
                entry_files.add(physical.relative_to(root).as_posix())
            source_objects.setdefault(
                dbn_manifest_path.relative_to(root).as_posix(),
                {
                    "object_class": "DBN_MANIFEST",
                    "path": dbn_manifest_path.relative_to(root).as_posix(),
                    "sha256": sha256_file(dbn_manifest_path),
                    "size": dbn_manifest_path.stat().st_size,
                },
            )
            entry_files.add(dbn_manifest_path.relative_to(root).as_posix())
            for aggregate in interval["aggregation_sources"]:
                for relative, sha_key, size_key in (
                    (
                        str(aggregate["relative_path"]),
                        "sha256",
                        "size",
                    ),
                    (
                        str(aggregate["sidecar_relative_path"]),
                        "sidecar_sha256",
                        "sidecar_size",
                    ),
                ):
                    path = root / "data" / Path(relative)
                    source_objects.setdefault(
                        path.relative_to(root).as_posix(),
                        {
                            "object_class": "AGGREGATION_DBN_OR_SIDECAR",
                            "path": path.relative_to(root).as_posix(),
                            "sha256": aggregate[sha_key],
                            "size": aggregate[size_key],
                        },
                    )
                    entry_files.add(path.relative_to(root).as_posix())
                # DBN records have a positive byte width.  File size is a
                # conservative manifest-derived upper bound when the sidecar
                # does not declare an exact record count.
                entry_rows += int(aggregate["size"])
            intervals.append(interval)
        entry_bytes = sum(
            int(source_objects[path]["size"]) for path in entry_files
        )
        planned = dict(entry)
        planned["intervals"] = intervals
        planned["source_ceiling"] = {
            "maximum_rows": entry_rows,
            "maximum_source_bytes": entry_bytes,
            "maximum_source_files": len(entry_files),
        }
        planned_entries.append(planned)
    objects = sorted(source_objects.values(), key=lambda item: str(item["path"]))
    source_bytes = sum(int(item["size"]) for item in objects)
    source_rows = sum(
        int(entry["source_ceiling"]["maximum_rows"])
        for entry in planned_entries
    )
    semantic = _bindings(root, SEMANTIC_PATHS)
    semantic["accepted_active_price_policy_release_id"] = (
        accepted_policy_release_id
    )
    semantic["accepted_active_price_policy_receipt_id"] = (
        policy_acceptance_receipt_id
    )
    implementation = _bindings(root, IMPLEMENTATION_PATHS)
    environment = _bindings(root, ENVIRONMENT_PATHS)
    scope_id = sha256_json(
        {
            "entries": planned_entries,
            "environment_bindings": environment,
            "foundation_release_id": foundation_release_id,
            "implementation_bindings": implementation,
            "semantic_bindings": semantic,
            "source_objects": objects,
        }
    )
    run_ids = [
        sha256_json({"pilot_scope_id": scope_id, "run_number": run_number})
        for run_number in (1, 2)
    ]
    plan = build_plan(
        operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
        mode=UpdateMode.INITIAL,
        foundation_release_id=foundation_release_id,
        foundation_manifest_sha256=str(inventory["foundation_manifest_sha256"]),
        semantic_bindings=semantic,
        entries=planned_entries,
        limits={
            "maximum_candidates": 2,
            "maximum_duration_seconds": 14_400,
            "maximum_memory_bytes": 4_294_967_296,
            "maximum_rows": source_rows,
            "maximum_processed_rows": source_rows * 10,
            "maximum_source_bytes": source_bytes,
            "maximum_source_files": len(objects),
            "maximum_temporary_bytes": max(source_bytes * 3, 2_147_483_648),
            "maximum_workers": 1,
        },
        forbidden_actions=[
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "TRADING",
        ],
        outputs=[
            f"reports/active_data_view/pilot/{scope_id}/run-1",
            f"reports/active_data_view/pilot/{scope_id}/run-2",
            f"state/active_data_view_certification/pilot/{scope_id}",
        ],
        implementation_bindings=implementation,
        environment_bindings=environment,
        recovery_boundary=(
            "PILOT_STATE_ONLY_TRANSIENT_OUTPUT_PRESERVED_OR_SEPARATELY_"
            "QUARANTINED_ACTIVE_ROOT_ABSENT"
        ),
    )
    plan["pilot_scope_id"] = scope_id
    plan["pilot_run_ids"] = run_ids
    plan["source_objects"] = objects
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    plan["plan_id"] = sha256_json(core)
    return plan, build_pending_approval(plan)


def build_dry_run_plan(
    *,
    repository_root: Path,
    foundation_release_id: str,
    accepted_policy_release_id: str,
    policy_acceptance_receipt_id: str,
) -> tuple[dict[str, object], dict[str, object]]:
    root = repository_root.resolve(strict=True)
    verify_policy_acceptance(
        repository_root=root,
        policy_release_id=accepted_policy_release_id,
        policy_acceptance_receipt_id=policy_acceptance_receipt_id,
    )
    inventory = derive_inventory(
        repository_root=root, foundation_release_id=foundation_release_id
    )
    semantic = _bindings(root, SEMANTIC_PATHS)
    semantic["accepted_active_price_policy_release_id"] = accepted_policy_release_id
    semantic["accepted_active_price_policy_receipt_id"] = (
        policy_acceptance_receipt_id
    )
    implementation = _bindings(root, IMPLEMENTATION_PATHS)
    environment = _bindings(root, ENVIRONMENT_PATHS)
    selected = [
        item for item in inventory["entries"] if item["disposition"] == CERTIFICATION_STATE
    ]
    maximum_source_files = sum(len(item["intervals"]) * 8 for item in selected)
    plan = build_plan(
        operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
        mode=UpdateMode.INITIAL,
        foundation_release_id=str(inventory["foundation_release_id"]),
        foundation_manifest_sha256=str(inventory["foundation_manifest_sha256"]),
        semantic_bindings=semantic,
        entries=inventory["entries"],
        limits={
            "maximum_candidates": int(inventory["counts"]["certification_candidates"]),
            "maximum_duration_seconds": 604_800,
            "maximum_memory_bytes": 8_589_934_592,
            "maximum_rows": int(inventory["counts"]["selected_row_count"]),
            "maximum_source_files": maximum_source_files,
            "maximum_temporary_bytes": 30_000_000_000,
            "maximum_workers": 1,
        },
        forbidden_actions=[
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "TRADING",
        ],
        outputs=[
            "manifests/active_data_view/full_certification_pending_plan.json",
            "reports/active_data_view/full_certification_dry_run.json",
        ],
        implementation_bindings=implementation,
        environment_bindings=environment,
        recovery_boundary="CERTIFICATION_STATE_ONLY_ACTIVE_ROOT_ABSENT",
    )
    return plan, build_pending_approval(plan)
