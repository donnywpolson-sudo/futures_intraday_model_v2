"""Explicit, immutable Phase 8 economics selection and aggregate index.

This module deliberately has no command-line entry point.  It prepares and
validates the small reference objects that connect the completed all-market
Databento audit to one economics registry per causal interval.  Publication is
left to an approved Codex high-risk task.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_market_year_materialization import IntervalSource, MarketYearTarget, resolve_selection
from .contract_economics_audit import require_phase8_passing_contract_economics_audit
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher,
)
from .economics import VerifiedEconomicsRegistry
from .errors import ContractError, IntegrityError
from .foundation.economics import EconomicsRuleBook
from .high_risk import confirmation_required
from .producer_bridge import (
    LoadedActualContractDefinitions,
    VerifiedFoundationPolicies,
    VerifiedSessionPolicy,
    derive_actual_contract_economics_records,
    publish_actual_contract_economics,
    verify_actual_contract_economics_context,
)


SELECTION_RELEASE_KIND = "phase8_interval_selection"
INDEX_RELEASE_KIND = "phase8_actual_contract_economics_index"
SELECTION_SCHEMA_VERSION = "1.0.0"
INDEX_SCHEMA_VERSION = "1.1.0"
LEGACY_INDEX_SCHEMA_VERSION = "1.0.0"
SELECTION_FILENAME = "phase8_interval_selection.json"
INDEX_FILENAME = "phase8_actual_contract_economics_index.json"


class Phase8EconomicsContext:
    """Exact verified inputs required to revalidate one indexed interval."""

    def __init__(self, *, causal_receipt: VerifiedReleaseReceipt,
                 definitions: LoadedActualContractDefinitions,
                 policies: VerifiedFoundationPolicies,
                 session_policy: VerifiedSessionPolicy) -> None:
        self.causal_receipt = causal_receipt
        self.definitions = definitions
        self.policies = policies
        self.session_policy = session_policy


def prepare_phase8_economics_publication(
    *, audit_receipt_id: str, foundation_release_id: str, rulebook_hash: str
) -> dict[str, object]:
    """Describe the bounded create-only Phase 8 publication; never execute it."""

    if any(len(value) != 64 for value in (audit_receipt_id, foundation_release_id, rulebook_hash)):
        raise ContractError("Phase 8 preparation requires pinned receipt and hash IDs")
    return confirmation_required(
        "Publish the Phase 8 economics selection and aggregate index",
        scope={
            "audit_receipt_id": audit_receipt_id,
            "foundation_release_id": foundation_release_id,
            "maximum_selected_intervals": "677",
            "maximum_logical_market_years": "644",
            "provider_calls": "0",
            "evaluation": "0",
        },
        outputs=(
            "one immutable phase8_interval_selection release",
            "only material actual_contract_economics successors",
            "one immutable phase8_actual_contract_economics_index release",
        ),
        preservation=(
            "Historic manifests and releases are read-only.  This task creates only "
            "successors where current numeric economics differ; it does not trade, "
            "evaluate, stage, commit, push, install, or alter active data."
        ),
    )


def _interval_dict(source: IntervalSource) -> dict[str, object]:
    return {
        "end": source.end,
        "interval_key": source.interval_key,
        "market": source.market,
        "release_id": source.release_id,
        "start": source.start,
        "year": source.year,
    }


def _target_dict(target: MarketYearTarget) -> dict[str, object]:
    return {
        "coverage_end": target.coverage_end,
        "coverage_start": target.coverage_start,
        "market": target.market,
        "source_release_ids": [source.release_id for source in target.sources],
        "year": target.year,
    }


def build_phase8_interval_selection(
    *, repository_root: Path, foundation_release_id: str
) -> dict[str, object]:
    """Build selection directly from the verified foundation manifest.

    The payload contains exact causal release IDs and physical boundaries.  It
    intentionally does not discover releases using filesystem order, timestamps,
    or a count-only assertion.
    """

    foundation_path, _, _, sources, targets, _excluded = resolve_selection(
        repository_root=repository_root, foundation_release_id=foundation_release_id
    )
    intervals = [_interval_dict(source) for source in sources]
    market_years = [_target_dict(target) for target in targets]
    if (
        len(intervals) != 677
        or len(market_years) != 644
        or len({item["interval_key"] for item in intervals}) != 677
        or len({item["release_id"] for item in intervals}) != 677
    ):
        raise IntegrityError("Phase 8 selection requires exactly 677 intervals in 644 market-years")
    payload = {
        "foundation_manifest_sha256": sha256_file(foundation_path),
        "foundation_release_id": foundation_release_id,
        "intervals": intervals,
        "market_years": market_years,
        "schema_version": SELECTION_SCHEMA_VERSION,
    }
    _validate_selection_payload(payload)
    return payload


def _validate_selection_payload(payload: Mapping[str, object]) -> None:
    if set(payload) != {
        "foundation_manifest_sha256", "foundation_release_id", "intervals", "market_years", "schema_version"
    } or payload.get("schema_version") != SELECTION_SCHEMA_VERSION:
        raise IntegrityError("Phase 8 selection payload schema is invalid")
    intervals = payload.get("intervals")
    market_years = payload.get("market_years")
    if not isinstance(intervals, list) or not isinstance(market_years, list):
        raise IntegrityError("Phase 8 selection payload collections are invalid")
    if len(intervals) != 677 or len(market_years) != 644:
        raise IntegrityError("Phase 8 selection scope is incomplete")
    expected_interval_keys: set[str] = set()
    expected_release_ids: set[str] = set()
    for item in intervals:
        if not isinstance(item, dict) or set(item) != {"end", "interval_key", "market", "release_id", "start", "year"}:
            raise IntegrityError("Phase 8 interval selection entry is invalid")
        market, year, start, end, key, receipt = (
            item["market"], item["year"], item["start"], item["end"], item["interval_key"], item["release_id"]
        )
        if not (type(market) is str and type(year) is int and type(start) is str and type(end) is str and type(key) is str and type(receipt) is str):
            raise IntegrityError("Phase 8 interval selection entry types are invalid")
        if key != f"{market}/{year}/{start}_{end}" or len(receipt) != 64:
            raise IntegrityError("Phase 8 interval selection identity is invalid")
        expected_interval_keys.add(key)
        expected_release_ids.add(receipt)
    if len(expected_interval_keys) != 677 or len(expected_release_ids) != 677:
        raise IntegrityError("Phase 8 selection repeats an interval or causal receipt")
    grouped: set[tuple[str, int]] = set()
    selected_by_group: dict[tuple[str, int], list[str]] = {}
    for item in intervals:
        selected_by_group.setdefault((item["market"], item["year"]), []).append(item["release_id"])
    for item in market_years:
        if not isinstance(item, dict) or set(item) != {"coverage_end", "coverage_start", "market", "source_release_ids", "year"}:
            raise IntegrityError("Phase 8 market-year selection entry is invalid")
        market, year, release_ids = item["market"], item["year"], item["source_release_ids"]
        if not (type(market) is str and type(year) is int and isinstance(release_ids, list) and all(type(value) is str for value in release_ids)):
            raise IntegrityError("Phase 8 market-year selection entry types are invalid")
        key = (market, year)
        if key in grouped or release_ids != selected_by_group.get(key):
            raise IntegrityError("Phase 8 market-year membership is invalid")
        grouped.add(key)
    if len(grouped) != 644 or set(grouped) != set(selected_by_group):
        raise IntegrityError("Phase 8 market-year grouping is incomplete")


def publish_phase8_interval_selection(
    *, payload: Mapping[str, object], boundary: RepoBoundary, publisher: PhasePublisher
) -> VerifiedReleaseReceipt:
    _validate_selection_payload(payload)
    if publisher.boundary != boundary:
        raise IntegrityError("Phase 8 selection publisher belongs to another repository")
    stage = publisher.create_stage("phase8_interval_selection")
    (stage / SELECTION_FILENAME).write_bytes(canonical_bytes(dict(payload)) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=SELECTION_RELEASE_KIND,
        schema_version=SELECTION_SCHEMA_VERSION,
        logical_paths={SELECTION_FILENAME: "data/reference/economics/phase8_interval_selection.json"},
        source_release_ids=tuple(sorted([payload["foundation_release_id"], *[item["release_id"] for item in payload["intervals"]]])),  # type: ignore[index,list-item]
        metadata={"interval_count": 677, "market_year_count": 644},
    )
    receipt = VerifiedReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    load_phase8_interval_selection(receipt, boundary=boundary)
    return receipt


def load_phase8_interval_selection(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> Mapping[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference" or manifest.release_kind != SELECTION_RELEASE_KIND
        or manifest.schema_version != SELECTION_SCHEMA_VERSION
        or {entry.logical_path for entry in manifest.files} != {"data/reference/economics/phase8_interval_selection.json"}
    ):
        raise IntegrityError("Phase 8 interval selection receipt has the wrong contract")
    path = receipt.resolve_file("data/reference/economics/phase8_interval_selection.json", boundary)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 8 interval selection payload is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("Phase 8 interval selection payload is not canonical")
    _validate_selection_payload(payload)
    expected_sources = tuple(sorted([payload["foundation_release_id"], *[item["release_id"] for item in payload["intervals"]]]))
    if manifest.source_release_ids != expected_sources or dict(manifest.metadata) != {"interval_count": 677, "market_year_count": 644}:
        raise IntegrityError("Phase 8 interval selection provenance is invalid")
    return payload


def reconcile_interval_economics(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    candidate_receipts: Sequence[VerifiedReleaseReceipt],
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    audit_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    publisher: PhasePublisher | None = None,
) -> tuple[VerifiedReleaseReceipt, str]:
    """Retain an identical historic registry or create one necessary successor.

    Candidate provenance is explicit: each candidate must name this causal
    receipt.  Compatibility compares canonical economics records, rather than
    treating a changed rulebook hash as evidence that every interval changed.
    A caller that supplies no matching candidate must also supply an approved
    publisher; there is no fallback discovery or implicit publication.
    """

    require_phase8_passing_contract_economics_audit(
        audit_receipt, boundary=boundary, rulebook=policies.economics
    )
    expected = list(
        derive_actual_contract_economics_records(
            causal_receipt=causal_receipt,
            definitions=definitions,
            policies=policies,
            session_policy=session_policy,
            boundary=boundary,
        )
    )
    for candidate in sorted(candidate_receipts, key=lambda item: item.release_id):
        manifest = candidate.verify(boundary)
        if causal_receipt.release_id not in manifest.source_release_ids:
            continue
        try:
            VerifiedEconomicsRegistry.from_release(candidate, boundary)
            raw = candidate.resolve_file(
                "data/reference/economics/contract_economics.json", boundary
            ).read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError, IntegrityError):
            continue
        if (
            isinstance(payload, dict)
            and raw == canonical_bytes(payload) + b"\n"
            and payload == {"records": expected, "schema_version": "1.1.0"}
        ):
            return candidate, "RETAINED_NUMERICALLY_COMPATIBLE"
    if publisher is None:
        raise IntegrityError(
            "no numerically compatible economics receipt; a create-only successor is required"
        )
    replacement = publish_actual_contract_economics(
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
        publisher=publisher,
    )
    return replacement, "SUCCESSOR_MATERIAL_ECONOMICS_CHANGE"


def publish_phase8_actual_contract_economics_index(
    *, selection_receipt: VerifiedReleaseReceipt, audit_receipt: VerifiedReleaseReceipt,
    economics_by_causal_release: Mapping[str, VerifiedReleaseReceipt], rulebook: EconomicsRuleBook,
    contexts_by_causal_release: Mapping[str, Phase8EconomicsContext] | None = None,
    boundary: RepoBoundary, publisher: PhasePublisher,
) -> VerifiedReleaseReceipt:
    """Publish one aggregate index after every selected causal interval is covered."""

    selection = load_phase8_interval_selection(selection_receipt, boundary=boundary)
    causal_ids = [item["release_id"] for item in selection["intervals"]]  # type: ignore[index]
    if set(economics_by_causal_release) != set(causal_ids):
        raise IntegrityError("Phase 8 economics index requires exactly one selected causal receipt per interval")
    if contexts_by_causal_release is None or set(contexts_by_causal_release) != set(causal_ids):
        raise IntegrityError("Phase 8 economics index requires explicit provenance contexts")
    require_phase8_passing_contract_economics_audit(audit_receipt, boundary=boundary, rulebook=rulebook)
    entries: list[dict[str, str]] = []
    for item in selection["intervals"]:  # type: ignore[index]
        causal_id = item["release_id"]
        economics_receipt = economics_by_causal_release[causal_id]
        context = contexts_by_causal_release[causal_id]
        if context.causal_receipt.release_id != causal_id:
            raise IntegrityError("Phase 8 context causal receipt differs from selection")
        economics_manifest = economics_receipt.verify(boundary)
        if causal_id not in economics_manifest.source_release_ids:
            raise IntegrityError("selected economics release is not bound to its causal interval")
        registry = verify_actual_contract_economics_context(
            economics_receipt, causal_receipt=context.causal_receipt,
            definitions=context.definitions, policies=context.policies,
            session_policy=context.session_policy, boundary=boundary,
        )
        entries.append({
            "causal_release_id": causal_id,
            "causal_receipt_id": context.causal_receipt.receipt_id,
            "definition_receipt_id": context.definitions.receipt.receipt_id,
            "economics_release_id": economics_receipt.release_id,
            "economics_receipt_id": economics_receipt.receipt_id,
            "foundation_policy_receipt_id": context.policies.receipt.receipt_id,
            "foundation_policy_set_id": context.policies.policy_set_id,
            "interval_key": item["interval_key"],
            "actual_identity_count": str(len(registry.records)),
            "session_policy_receipt_id": context.session_policy.receipt.receipt_id,
        })
    if len({entry["economics_release_id"] for entry in entries}) != len(entries):
        raise IntegrityError("one economics release cannot cover multiple Phase 8 intervals")
    payload = {
        "audit_receipt_id": audit_receipt.release_id,
        "economics_by_interval": entries,
        "rulebook_hash": rulebook.rulebook_hash,
        "schema_version": INDEX_SCHEMA_VERSION,
        "selection_receipt_id": selection_receipt.release_id,
    }
    stage = publisher.create_stage("phase8_actual_contract_economics_index")
    (stage / INDEX_FILENAME).write_bytes(canonical_bytes(payload) + b"\n")
    manifest = ReleaseManifest.build(
        stage, phase="reference", release_kind=INDEX_RELEASE_KIND, schema_version=INDEX_SCHEMA_VERSION,
        logical_paths={INDEX_FILENAME: "data/reference/economics/phase8_actual_contract_economics_index.json"},
        source_release_ids=tuple(sorted([selection_receipt.release_id, audit_receipt.release_id, *[entry["economics_release_id"] for entry in entries]])),
        metadata={"interval_count": 677, "market_year_count": 644, "rulebook_hash": rulebook.rulebook_hash},
    )
    receipt = VerifiedReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    load_phase8_actual_contract_economics_index(receipt, boundary=boundary, rulebook=rulebook)
    return receipt


def load_phase8_actual_contract_economics_index(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary, rulebook: EconomicsRuleBook
) -> Mapping[str, object]:
    manifest = receipt.verify(boundary)
    if receipt.phase != "reference" or manifest.release_kind != INDEX_RELEASE_KIND or manifest.schema_version not in {LEGACY_INDEX_SCHEMA_VERSION, INDEX_SCHEMA_VERSION}:
        raise IntegrityError("Phase 8 economics index receipt has the wrong contract")
    path = receipt.resolve_file("data/reference/economics/phase8_actual_contract_economics_index.json", boundary)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 8 economics index payload is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("Phase 8 economics index payload is invalid")
    entries = payload["economics_by_interval"]
    expected_keys = {"audit_receipt_id", "economics_by_interval", "rulebook_hash", "schema_version", "selection_receipt_id"}
    if set(payload) != expected_keys or payload["schema_version"] != manifest.schema_version or payload["rulebook_hash"] != rulebook.rulebook_hash or not isinstance(entries, list) or len(entries) != 677:
        raise IntegrityError("Phase 8 economics index scope or rulebook is invalid")
    entry_keys = ({"causal_release_id", "economics_release_id", "interval_key"} if payload["schema_version"] == LEGACY_INDEX_SCHEMA_VERSION else {"actual_identity_count", "causal_receipt_id", "causal_release_id", "definition_receipt_id", "economics_receipt_id", "economics_release_id", "foundation_policy_receipt_id", "foundation_policy_set_id", "interval_key", "session_policy_receipt_id"})
    if any(not isinstance(item, dict) or set(item) != entry_keys for item in entries):
        raise IntegrityError("Phase 8 economics index entry is invalid")
    if len({item["causal_release_id"] for item in entries}) != 677 or len({item["interval_key"] for item in entries}) != 677 or len({item["economics_release_id"] for item in entries}) != 677:
        raise IntegrityError("Phase 8 economics index has duplicate coverage")
    expected_sources = tuple(sorted([payload["audit_receipt_id"], payload["selection_receipt_id"], *[item["economics_release_id"] for item in entries]]))
    if manifest.source_release_ids != expected_sources or dict(manifest.metadata) != {"interval_count": 677, "market_year_count": 644, "rulebook_hash": rulebook.rulebook_hash}:
        raise IntegrityError("Phase 8 economics index provenance is invalid")
    return payload


def verify_phase8_actual_contract_economics_index_context(
    receipt: VerifiedReleaseReceipt, *, audit_receipt: VerifiedReleaseReceipt,
    contexts_by_causal_release: Mapping[str, Phase8EconomicsContext],
    boundary: RepoBoundary,
) -> Mapping[str, object]:
    """Fully revalidate every v1.1 indexed registry against explicit inputs."""

    rulebook = next(iter(contexts_by_causal_release.values())).policies.economics
    payload = load_phase8_actual_contract_economics_index(
        receipt, boundary=boundary, rulebook=rulebook
    )
    if payload["schema_version"] != INDEX_SCHEMA_VERSION or payload["audit_receipt_id"] != audit_receipt.release_id:
        raise IntegrityError("current Phase 8 gate requires a v1.1 index and matching audit")
    if set(contexts_by_causal_release) != {entry["causal_release_id"] for entry in payload["economics_by_interval"]}:
        raise IntegrityError("Phase 8 index contexts are incomplete")
    require_phase8_passing_contract_economics_audit(audit_receipt, boundary=boundary, rulebook=rulebook)
    for entry in payload["economics_by_interval"]:
        context = contexts_by_causal_release[entry["causal_release_id"]]
        if (context.causal_receipt.receipt_id != entry["causal_receipt_id"] or context.definitions.receipt.receipt_id != entry["definition_receipt_id"] or context.policies.receipt.receipt_id != entry["foundation_policy_receipt_id"] or context.policies.policy_set_id != entry["foundation_policy_set_id"] or context.session_policy.receipt.receipt_id != entry["session_policy_receipt_id"]):
            raise IntegrityError("Phase 8 index entry provenance differs from supplied context")
        economics_receipt = VerifiedReleaseReceipt.from_manifest(
            boundary.active_root / "manifests" / "data_releases" / "reference" / f"{entry['economics_release_id']}.json", boundary
        )
        registry = verify_actual_contract_economics_context(
            economics_receipt, causal_receipt=context.causal_receipt,
            definitions=context.definitions, policies=context.policies,
            session_policy=context.session_policy, boundary=boundary,
        )
        if economics_receipt.receipt_id != entry["economics_receipt_id"] or str(len(registry.records)) != entry["actual_identity_count"]:
            raise IntegrityError("Phase 8 index economics receipt differs from its entry")
    return payload


def resolve_indexed_phase8_economics(
    receipt: VerifiedReleaseReceipt, *, audit_receipt: VerifiedReleaseReceipt,
    context: Phase8EconomicsContext, boundary: RepoBoundary,
) -> VerifiedEconomicsRegistry:
    """Resolve one current Phase 8 registry; standalone receipts are insufficient."""

    require_phase8_passing_contract_economics_audit(audit_receipt, boundary=boundary, rulebook=context.policies.economics)
    payload = load_phase8_actual_contract_economics_index(receipt, boundary=boundary, rulebook=context.policies.economics)
    if payload["schema_version"] != INDEX_SCHEMA_VERSION or payload["audit_receipt_id"] != audit_receipt.release_id:
        raise IntegrityError("current Phase 8 gate requires a v1.1 index and matching audit")
    entry = next(item for item in payload["economics_by_interval"] if item["causal_release_id"] == context.causal_receipt.release_id)
    if (context.causal_receipt.receipt_id != entry["causal_receipt_id"] or context.definitions.receipt.receipt_id != entry["definition_receipt_id"] or context.policies.receipt.receipt_id != entry["foundation_policy_receipt_id"] or context.policies.policy_set_id != entry["foundation_policy_set_id"] or context.session_policy.receipt.receipt_id != entry["session_policy_receipt_id"]):
        raise IntegrityError("Phase 8 indexed economics provenance differs from supplied context")
    economics_receipt = VerifiedReleaseReceipt.from_manifest(
        boundary.active_root / "manifests" / "data_releases" / "reference" / f"{entry['economics_release_id']}.json", boundary
    )
    return verify_actual_contract_economics_context(economics_receipt, causal_receipt=context.causal_receipt, definitions=context.definitions, policies=context.policies, session_policy=context.session_policy, boundary=boundary)


def selection_fingerprint(payload: Mapping[str, object]) -> str:
    """Stable identifier for checkpoints in a future resumable publication task."""

    _validate_selection_payload(payload)
    return sha256_json(payload)
