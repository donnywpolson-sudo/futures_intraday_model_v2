"""Approval-gated staging of authoritative causal history for 41 markets.

This module does not mutate ``data/causally_gated_normalized``.  It resolves
the exact interval releases selected by one completed foundation manifest,
excludes quarantined market-years, and labels each included market-year as
causal-price-only or status-gated research capable.  Historical cohort,
selection, holdout, and forward-use controls remain unchanged.  A later,
separately approved cutover is required to archive the immutable predecessors
and change the active data-layout contract.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .boundary import RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .data_layout import (
    DataFileEntry,
    DataReleaseManifest,
    manifest_relative_path,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease


PLAN_VERSION = "3.0.0"
APPROVAL_VERSION = "1.0.0"
SIDECAR_VERSION = "1.0.0"
RECEIPT_VERSION = "1.0.0"
OPERATION = "MATERIALIZE_41_MARKET_AUTHORITATIVE_CAUSAL_HISTORY_FLAT_SUCCESSOR_V1"
FOUNDATION_RELEASE_ID = (
    "78806ef01714c72f6da537c1b6e6f8b2e903b14728822b0daa31b4c6c75a8909"
)
FOUNDATION_RELEASE_KIND = "futures_mechanical_foundation_set"
CAUSAL_RELEASE_KIND = "futures_phase2_causal_interval"
ANOMALY_ACCEPTANCE_RELEASE_KIND = (
    "futures_dbn_anomaly_materialization_acceptance"
)
ANOMALY_ACCEPTANCE_STATUS = (
    "ACCEPTED_FOR_MATERIALIZATION_ONLY_CAUSAL_QUARANTINE_RETAINED"
)
AUTHORITATIVE_COVERAGE_DISPOSITIONS = frozenset(
    {
        "AUTHORITATIVE_INTERVAL",
        "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK",
    }
)
QUARANTINED_COVERAGE_DISPOSITIONS = frozenset(
    {
        "QUARANTINED_PENDING_REVALIDATION",
        "QUARANTINED_PENDING_REVALIDATION_WITH_EXACT_REDUNDANT_CROSSCHECK",
    }
)
KNOWN_COVERAGE_DISPOSITIONS = frozenset(
    AUTHORITATIVE_COVERAGE_DISPOSITIONS
    | QUARANTINED_COVERAGE_DISPOSITIONS
)
PRICE_RESEARCH_POLICY_PATH = Path("configs/causal_price_research_policy.json")
UNIVERSE_CONTRACT_PATH = Path("configs/research_universe_contract.json")
STATUS_SCOPE_POLICY_PATH = Path("configs/status_research_scope_policy.json")
PLAN_PATH = Path("configs/causal_market_year_materialization_plan.json")
APPROVAL_PATH = Path("configs/causal_market_year_materialization_approval.json")
STAGING_ROOT = Path("state/causal_market_year_materialization")
LOCK_PATH = Path("state/locks/causal-market-year-materialization.lock")
OUTPUT_ROOT_NAME = "outputs"
RECEIPT_NAME = "materialization_receipt.json"
MAXIMUM_DURATION_SECONDS = 12 * 60 * 60
PARQUET_NAME_TEMPLATE = "{year}.parquet"
SIDECAR_SUFFIX = ".manifest.json"
TARGET_LAYOUT = "data/causally_gated_normalized/{market}/{year}/{year}.parquet"
TARGET_SIDECAR_LAYOUT = TARGET_LAYOUT + SIDECAR_SUFFIX
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MARKET = re.compile(r"^[0-9A-Z]{1,16}$")
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/causal_market_year_materialization.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/locking.py",
)


@dataclass(frozen=True)
class IntervalSource:
    market: str
    year: int
    start: str
    end: str
    coverage_disposition: str
    release_id: str
    manifest_path: str
    manifest_sha256: str
    bars_logical_path: str
    bars_physical_path: Path
    bars_sha256: str
    bars_size: int
    receipt_logical_path: str
    receipt_physical_path: Path
    receipt_sha256: str
    receipt_size: int
    row_count: int
    schema_fingerprint: str
    research_in_scope: bool
    research_disposition: str
    research_scope_policy_hash: str
    status_epoch_gate_id: str
    status_gated_feature_ready_rows: int

    @property
    def interval_key(self) -> str:
        return f"{self.market}/{self.year}/{self.start}_{self.end}"

    @property
    def price_research_eligible(self) -> bool:
        return (
            self.coverage_disposition
            in AUTHORITATIVE_COVERAGE_DISPOSITIONS
        )

    @property
    def status_research_eligible(self) -> bool:
        return (
            self.price_research_eligible
            and self.research_in_scope
            and self.research_disposition == "ELIGIBLE"
            and self.status_gated_feature_ready_rows > 0
        )

    @property
    def research_capability(self) -> str:
        if not self.price_research_eligible:
            return "EXCLUDED_QUARANTINED"
        if self.status_research_eligible:
            return "CAUSAL_PRICE_PLUS_STATUS_GATED"
        return "CAUSAL_PRICE_ONLY"

    def inventory_dict(self, root: Path) -> dict[str, object]:
        return {
            "bars_logical_path": self.bars_logical_path,
            "bars_physical_path": self.bars_physical_path.relative_to(root).as_posix(),
            "bars_sha256": self.bars_sha256,
            "bars_size": self.bars_size,
            "end": self.end,
            "coverage_disposition": self.coverage_disposition,
            "interval_key": self.interval_key,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "market": self.market,
            "receipt_logical_path": self.receipt_logical_path,
            "receipt_physical_path": self.receipt_physical_path.relative_to(root).as_posix(),
            "receipt_sha256": self.receipt_sha256,
            "receipt_size": self.receipt_size,
            "release_id": self.release_id,
            "research_disposition": self.research_disposition,
            "research_capability": self.research_capability,
            "research_in_scope": self.research_in_scope,
            "research_scope_policy_hash": self.research_scope_policy_hash,
            "row_count": self.row_count,
            "schema_fingerprint": self.schema_fingerprint,
            "start": self.start,
            "status_epoch_gate_id": self.status_epoch_gate_id,
            "status_gated_feature_ready_rows": self.status_gated_feature_ready_rows,
            "year": self.year,
        }


@dataclass(frozen=True)
class MarketYearTarget:
    market: str
    year: int
    sources: tuple[IntervalSource, ...]

    @property
    def coverage_start(self) -> str:
        return self.sources[0].start

    @property
    def coverage_end(self) -> str:
        return self.sources[-1].end

    @property
    def row_count(self) -> int:
        return sum(item.row_count for item in self.sources)

    @property
    def price_research_eligible(self) -> bool:
        return all(item.price_research_eligible for item in self.sources)

    @property
    def status_research_eligible(self) -> bool:
        return all(item.status_research_eligible for item in self.sources)

    @property
    def research_capability(self) -> str:
        if not self.price_research_eligible:
            return "EXCLUDED_QUARANTINED"
        if self.status_research_eligible:
            return "CAUSAL_PRICE_PLUS_STATUS_GATED"
        return "CAUSAL_PRICE_ONLY"

    @property
    def relative_parquet_path(self) -> Path:
        return Path(self.market) / str(self.year) / PARQUET_NAME_TEMPLATE.format(year=self.year)

    @property
    def logical_path(self) -> str:
        return (
            f"data/causally_gated_normalized/{self.market}/{self.year}/"
            f"{PARQUET_NAME_TEMPLATE.format(year=self.year)}"
        )

    def inventory_dict(self) -> dict[str, object]:
        return {
            "coverage_end": self.coverage_end,
            "coverage_start": self.coverage_start,
            "coverage_dispositions": sorted(
                {item.coverage_disposition for item in self.sources}
            ),
            "logical_path": self.logical_path,
            "market": self.market,
            "price_research_eligible": self.price_research_eligible,
            "research_capability": self.research_capability,
            "status_research_eligible": self.status_research_eligible,
            "row_count": self.row_count,
            "source_release_ids": [item.release_id for item in self.sources],
            "year": self.year,
        }


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"JSON object required: {path}")
    return payload


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    encoded = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    assert_no_linklike_ancestors(path.parent)
    if path.exists():
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"existing artifact conflicts: {path}")
        return
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _plain_sha256(value: object, *, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise IntegrityError(f"{name} is not a SHA-256 value")
    return value


def _plain_positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or type(value) is not int or value <= 0:
        raise IntegrityError(f"{name} must be a positive integer")
    return value


def _plain_nonnegative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or type(value) is not int or value < 0:
        raise IntegrityError(f"{name} must be a nonnegative integer")
    return value


def _entry_named(manifest: DataReleaseManifest, name: str) -> DataFileEntry:
    matches = [item for item in manifest.files if Path(item.logical_path).name == name]
    if len(matches) != 1:
        raise IntegrityError(f"causal manifest must contain exactly one {name}")
    return matches[0]


def _schema_fingerprint(path: Path) -> str:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - dependency contract
        raise ContractError("pyarrow is required for causal Parquet planning") from exc
    schema = pq.read_schema(path)
    return sha256_json({"arrow_schema": str(schema)})


def _foundation_manifest(
    boundary: RepoBoundary, release_id: str
) -> tuple[Path, DataReleaseManifest, Mapping[str, object]]:
    path = boundary.active_root / manifest_relative_path("foundation", release_id)
    manifest = verify_data_release_manifest(path, boundary, verify_files=False)
    if (
        manifest.release_id != release_id
        or manifest.phase != "foundation"
        or manifest.release_kind != FOUNDATION_RELEASE_KIND
        or manifest.schema_version != "5.0.0"
    ):
        raise IntegrityError("foundation selection manifest identity is not accepted")
    foundation = manifest.embedded_documents.get("foundation_set.json")
    if not isinstance(foundation, dict):
        raise IntegrityError("foundation manifest lacks foundation_set.json")
    intervals = foundation.get("intervals")
    coverage_gate = foundation.get("coverage_gate")
    if (
        foundation.get("dependency_closure_complete") is not True
        or foundation.get("learned_or_outcome_informed_transform_count") != 0
        or foundation.get("model_fit_count") != 0
        or foundation.get("provider_call_count") != 0
        or not isinstance(intervals, list)
        or foundation.get("interval_count") != len(intervals)
        or not isinstance(coverage_gate, dict)
        or coverage_gate.get("research_failed_interval_count") != 0
    ):
        raise IntegrityError("foundation selection has not passed its mechanical data gates")
    return path, manifest, foundation


def _anomaly_acceptance_release_ids(
    boundary: RepoBoundary, foundation: Mapping[str, object]
) -> tuple[str, ...]:
    intervals = foundation.get("intervals")
    assert isinstance(intervals, list)
    quarantined = [
        item
        for item in intervals
        if isinstance(item, dict)
        and str(item.get("coverage_disposition", "")).startswith("QUARANTINED_")
    ]
    selection_receipt = foundation.get("source_selection_receipt")
    if not isinstance(selection_receipt, dict):
        raise IntegrityError("foundation source-selection receipt is missing")
    selection_release_id = _plain_sha256(
        selection_receipt.get("release_id"), name="source-selection release ID"
    )
    selection_manifest_path = boundary.active_root / manifest_relative_path(
        "controls", selection_release_id
    )
    if (
        selection_receipt.get("manifest_path")
        != selection_manifest_path.relative_to(boundary.active_root).as_posix()
        or selection_receipt.get("manifest_sha256")
        != sha256_file(selection_manifest_path)
    ):
        raise IntegrityError("foundation source-selection receipt differs")
    selection_manifest = verify_data_release_manifest(
        selection_manifest_path, boundary, verify_files=False
    )
    raw_receipts = selection_manifest.embedded_documents.get(
        "anomaly_acceptance_receipts.json"
    )
    if not isinstance(raw_receipts, list):
        raise IntegrityError("source selection anomaly-acceptance closure is invalid")
    if not quarantined:
        if raw_receipts:
            raise IntegrityError("anomaly acceptance exists without quarantined intervals")
        return ()
    if len(raw_receipts) != 1 or not isinstance(raw_receipts[0], dict):
        raise IntegrityError("quarantined intervals require one exact acceptance release")
    receipt = raw_receipts[0]
    release_id = _plain_sha256(
        receipt.get("release_id"), name="anomaly-acceptance release ID"
    )
    evidence_path = boundary.active_root / manifest_relative_path("evidence", release_id)
    if (
        receipt.get("manifest_path")
        != evidence_path.relative_to(boundary.active_root).as_posix()
        or receipt.get("manifest_sha256") != sha256_file(evidence_path)
        or receipt.get("release_kind") != ANOMALY_ACCEPTANCE_RELEASE_KIND
    ):
        raise IntegrityError("anomaly-acceptance receipt differs")
    evidence = verify_data_release_manifest(
        evidence_path, boundary, verify_files=False
    )
    document = evidence.embedded_documents.get(
        "anomaly_materialization_acceptance.json"
    )
    if (
        evidence.release_kind != ANOMALY_ACCEPTANCE_RELEASE_KIND
        or not isinstance(document, dict)
        or document.get("status") != ANOMALY_ACCEPTANCE_STATUS
        or document.get("causal_quarantine_retained") is not True
        or document.get("research_eligibility_granted") is not False
        or document.get("source_dbn_release_id")
        != foundation.get("source_dbn_release_id")
    ):
        raise IntegrityError("anomaly acceptance does not preserve causal quarantine")
    return (release_id,)


def _interval_source(
    *,
    boundary: RepoBoundary,
    payload: Mapping[str, object],
) -> IntervalSource:
    market = payload.get("market")
    year = payload.get("year")
    start = payload.get("start")
    end = payload.get("end")
    receipt = payload.get("causal_release_receipt")
    coverage_disposition = payload.get("coverage_disposition")
    status_epoch_gate = payload.get("status_epoch_gate")
    if (
        type(market) is not str
        or _MARKET.fullmatch(market) is None
        or isinstance(year, bool)
        or type(year) is not int
        or not isinstance(start, str)
        or not isinstance(end, str)
        or not isinstance(receipt, dict)
        or coverage_disposition not in KNOWN_COVERAGE_DISPOSITIONS
        or not isinstance(status_epoch_gate, dict)
    ):
        raise IntegrityError("foundation interval selection is invalid")
    try:
        parsed_start = date.fromisoformat(start)
        parsed_end = date.fromisoformat(end)
    except ValueError as exc:
        raise IntegrityError("foundation interval dates are invalid") from exc
    if parsed_start >= parsed_end or parsed_start.year != year:
        raise IntegrityError("foundation interval is outside its market-year")
    interval_key = f"{market}/{year}/{start}_{end}"
    research_in_scope = status_epoch_gate.get("in_research_scope")
    research_disposition = status_epoch_gate.get("research_disposition")
    if (
        type(research_in_scope) is not bool
        or type(research_disposition) is not str
        or (research_in_scope and research_disposition != "ELIGIBLE")
        or (
            not research_in_scope
            and not research_disposition.startswith("ABSTAIN_")
        )
        or status_epoch_gate.get("interval_key") != interval_key
    ):
        raise IntegrityError("foundation status-epoch research gate is invalid")
    status_epoch_gate_id = _plain_sha256(
        status_epoch_gate.get("status_epoch_gate_id"),
        name="status epoch gate ID",
    )
    research_scope_policy_hash = _plain_sha256(
        status_epoch_gate.get("research_scope_policy_hash"),
        name="research-scope policy hash",
    )
    status_gated_feature_ready_rows = _plain_nonnegative_int(
        status_epoch_gate.get("status_gated_feature_ready_rows"),
        name="status-gated feature-ready rows",
    )
    if (
        payload.get("status_gated_feature_ready_rows")
        != status_gated_feature_ready_rows
    ):
        raise IntegrityError("foundation status-epoch row count differs")
    release_id = _plain_sha256(receipt.get("release_id"), name="causal release ID")
    manifest_sha256 = _plain_sha256(
        receipt.get("manifest_sha256"), name="causal manifest SHA-256"
    )
    expected_manifest = manifest_relative_path("causally_gated_normalized", release_id)
    if (
        receipt.get("phase") != "causally_gated_normalized"
        or receipt.get("release_kind") != CAUSAL_RELEASE_KIND
        or receipt.get("manifest_path") != expected_manifest.as_posix()
    ):
        raise IntegrityError("foundation causal receipt identity is invalid")
    manifest_path = boundary.active_root / expected_manifest
    if sha256_file(manifest_path) != manifest_sha256:
        raise IntegrityError("foundation-selected causal manifest hash differs")
    manifest = verify_data_release_manifest(manifest_path, boundary, verify_files=False)
    if (
        manifest.release_id != release_id
        or manifest.phase != "causally_gated_normalized"
        or manifest.release_kind != CAUSAL_RELEASE_KIND
    ):
        raise IntegrityError("foundation-selected causal manifest is invalid")
    expected_root = (
        f"data/causally_gated_normalized/{market}/{year}/{start}_{end}"
    )
    if manifest.metadata.get("logical_root") != expected_root:
        raise IntegrityError("causal manifest logical root differs from foundation interval")
    bars = _entry_named(manifest, "bars.parquet")
    causal_receipt = _entry_named(manifest, "causal_interval_receipt.json")
    if (
        Path(bars.logical_path).parent.as_posix() != expected_root
        or Path(causal_receipt.logical_path).parent.as_posix() != expected_root
    ):
        raise IntegrityError("causal manifest files differ from the selected interval")
    bars_path = boundary.active_root / manifest.physical_relative_path(bars)
    receipt_path = boundary.active_root / manifest.physical_relative_path(causal_receipt)
    assert_plain_file(bars_path)
    assert_plain_file(receipt_path)
    if bars_path.stat().st_size != bars.size:
        raise IntegrityError("foundation-selected causal Parquet size differs")
    if (
        receipt_path.stat().st_size != causal_receipt.size
        or sha256_file(receipt_path) != causal_receipt.sha256
    ):
        raise IntegrityError("foundation-selected causal receipt differs")
    interval_receipt = _load_json(receipt_path)
    row_count = _plain_positive_int(
        interval_receipt.get("row_count"), name="causal row count"
    )
    if (
        status_epoch_gate.get("bar_rows") != row_count
        or interval_receipt.get("market") != market
        or interval_receipt.get("year") != year
        or interval_receipt.get("logical_root") != expected_root
        or interval_receipt.get("causal_parquet_sha256") != bars.sha256
        or interval_receipt.get("causal_schema") != "FUTURES_PHASE2_CAUSAL_BARS_V2"
    ):
        raise IntegrityError("causal interval receipt differs from its manifest")
    return IntervalSource(
        market=market,
        year=year,
        start=start,
        end=end,
        coverage_disposition=str(coverage_disposition),
        release_id=release_id,
        manifest_path=expected_manifest.as_posix(),
        manifest_sha256=manifest_sha256,
        bars_logical_path=bars.logical_path,
        bars_physical_path=bars_path,
        bars_sha256=bars.sha256,
        bars_size=bars.size,
        receipt_logical_path=causal_receipt.logical_path,
        receipt_physical_path=receipt_path,
        receipt_sha256=causal_receipt.sha256,
        receipt_size=causal_receipt.size,
        row_count=row_count,
        schema_fingerprint=_schema_fingerprint(bars_path),
        research_in_scope=research_in_scope,
        research_disposition=research_disposition,
        research_scope_policy_hash=research_scope_policy_hash,
        status_epoch_gate_id=status_epoch_gate_id,
        status_gated_feature_ready_rows=status_gated_feature_ready_rows,
    )


def group_market_year_sources(
    sources: Iterable[IntervalSource],
) -> tuple[MarketYearTarget, ...]:
    grouped: dict[tuple[str, int], list[IntervalSource]] = {}
    for source in sources:
        grouped.setdefault((source.market, source.year), []).append(source)
    targets: list[MarketYearTarget] = []
    for (market, year), items in sorted(grouped.items()):
        ordered = sorted(items, key=lambda item: (item.start, item.end, item.release_id))
        if len({item.release_id for item in ordered}) != len(ordered):
            raise IntegrityError("foundation selection repeats a causal release")
        if len({item.schema_fingerprint for item in ordered}) != 1:
            raise IntegrityError("market-year causal schemas differ")
        for previous, current in zip(ordered, ordered[1:]):
            if previous.end != current.start:
                raise IntegrityError(
                    f"market-year intervals are not exactly contiguous: {market}/{year}"
                )
        targets.append(MarketYearTarget(market, year, tuple(ordered)))
    if not targets:
        raise IntegrityError("foundation selection contains no market-year targets")
    return tuple(targets)


def partition_price_research_targets(
    targets: Iterable[MarketYearTarget],
) -> tuple[tuple[MarketYearTarget, ...], tuple[MarketYearTarget, ...]]:
    eligible: list[MarketYearTarget] = []
    excluded: list[MarketYearTarget] = []
    for target in targets:
        (eligible if target.price_research_eligible else excluded).append(target)
    if not eligible:
        raise IntegrityError(
            "foundation selection contains no authoritative causal-price targets"
        )
    return tuple(eligible), tuple(excluded)


def resolve_selection(
    *, repository_root: Path, foundation_release_id: str = FOUNDATION_RELEASE_ID
) -> tuple[
    Path,
    DataReleaseManifest,
    Mapping[str, object],
    tuple[IntervalSource, ...],
    tuple[MarketYearTarget, ...],
    tuple[MarketYearTarget, ...],
]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    foundation_path, foundation_manifest, foundation = _foundation_manifest(
        boundary, foundation_release_id
    )
    raw_intervals = foundation["intervals"]
    assert isinstance(raw_intervals, list)
    all_sources = tuple(
        _interval_source(boundary=boundary, payload=item)
        for item in raw_intervals
        if isinstance(item, dict)
    )
    if len(all_sources) != len(raw_intervals):
        raise IntegrityError("foundation interval list contains invalid entries")
    if len({item.interval_key for item in all_sources}) != len(all_sources):
        raise IntegrityError("foundation selection repeats an interval key")
    all_targets = group_market_year_sources(all_sources)
    targets, excluded_targets = partition_price_research_targets(all_targets)
    sources = tuple(source for target in targets for source in target.sources)
    return (
        foundation_path,
        foundation_manifest,
        foundation,
        sources,
        targets,
        excluded_targets,
    )


def _implementation_inventory(root: Path) -> list[dict[str, object]]:
    inventory: list[dict[str, object]] = []
    for relative in IMPLEMENTATION_PATHS:
        path = root / relative
        assert_plain_file(path)
        inventory.append({"path": relative, "sha256": sha256_file(path)})
    return inventory


def _load_canonical_config(root: Path, relative_path: Path) -> dict[str, object]:
    path = root / relative_path
    payload = _load_json(path)
    if path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"config is not canonical JSON: {relative_path}")
    return payload


def _price_research_contract(
    root: Path,
) -> tuple[Mapping[str, object], tuple[str, ...], str]:
    policy = _load_canonical_config(root, PRICE_RESEARCH_POLICY_PATH)
    expected_policy_keys = {
        "allowed_coverage_dispositions",
        "capability_labels",
        "does_not_authorize",
        "excluded_coverage_dispositions",
        "market_scope",
        "policy_version",
        "pre_status_forbidden_capabilities",
        "research_use_classification",
        "selection_rule",
        "status_scope_policy_path",
    }
    market_scope = policy.get("market_scope")
    capability_labels = policy.get("capability_labels")
    if (
        set(policy) != expected_policy_keys
        or policy.get("policy_version") != "1.0.0"
        or policy.get("allowed_coverage_dispositions")
        != sorted(AUTHORITATIVE_COVERAGE_DISPOSITIONS)
        or policy.get("excluded_coverage_dispositions")
        != sorted(QUARANTINED_COVERAGE_DISPOSITIONS)
        or policy.get("selection_rule")
        != "ALL_AVAILABLE_FOUNDATION_INTERVALS_WITH_AUTHORITATIVE_COVERAGE"
        or policy.get("research_use_classification")
        != "NON_AUTHORIZING_DATA_CAPABILITY"
        or policy.get("pre_status_forbidden_capabilities")
        != ["STATUS_DEPENDENT_FEATURES", "STATUS_ELIGIBILITY_ASSERTIONS"]
        or capability_labels
        != {
            "pre_status_epoch": "CAUSAL_PRICE_ONLY",
            "status_eligible": "CAUSAL_PRICE_PLUS_STATUS_GATED",
        }
        or not isinstance(market_scope, dict)
        or market_scope
        != {
            "contract_path": UNIVERSE_CONTRACT_PATH.as_posix(),
            "expected_market_count": 41,
            "tier_ids": [3, 4],
        }
        or policy.get("status_scope_policy_path")
        != STATUS_SCOPE_POLICY_PATH.as_posix()
    ):
        raise IntegrityError("causal price-research policy is invalid")
    forbidden_authorities = policy.get("does_not_authorize")
    if forbidden_authorities != [
        "CANDIDATE_SELECTION",
        "HOLDOUT_OR_FORWARD_ACCESS",
        "MODEL_FIT",
        "PROVIDER_CALL",
        "REAL_HISTORY_EVALUATION",
        "WFA_OR_OOS",
    ]:
        raise IntegrityError("causal price-research policy authority boundary differs")

    universe = _load_canonical_config(root, UNIVERSE_CONTRACT_PATH)
    tiers = universe.get("tiers")
    approval_receipt_id = _plain_sha256(
        universe.get("approval_receipt_id"),
        name="research-universe approval receipt ID",
    )
    if (
        universe.get("status") != "APPROVED"
        or universe.get("schema_version") != "glbx_research_universe/1.0.0"
        or not isinstance(tiers, list)
    ):
        raise IntegrityError("research-universe contract is not approved")
    selected_tiers = [
        tier
        for tier in tiers
        if isinstance(tier, dict) and tier.get("tier_id") in (3, 4)
    ]
    if (
        len(selected_tiers) != 2
        or sorted(int(tier["tier_id"]) for tier in selected_tiers) != [3, 4]
    ):
        raise IntegrityError("research-universe 41-market tiers differ")
    markets: list[str] = []
    for tier in selected_tiers:
        symbols = tier.get("symbols")
        if (
            not isinstance(symbols, list)
            or any(
                type(symbol) is not str or _MARKET.fullmatch(symbol) is None
                for symbol in symbols
            )
        ):
            raise IntegrityError("research-universe market symbols are invalid")
        markets.extend(symbols)
    if len(markets) != 41 or len(set(markets)) != 41:
        raise IntegrityError("research-universe contract does not contain 41 markets")

    status_scope_policy = _load_canonical_config(root, STATUS_SCOPE_POLICY_PATH)
    if (
        status_scope_policy.get("research_interval_start") != "2025-01-01"
        or status_scope_policy.get("pre_scope_disposition")
        != "ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH"
    ):
        raise IntegrityError("status research-scope policy differs")
    return policy, tuple(sorted(markets)), approval_receipt_id


def build_scope(
    *, repository_root: Path, foundation_release_id: str = FOUNDATION_RELEASE_ID
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    (
        foundation_path,
        foundation_manifest,
        foundation,
        sources,
        targets,
        excluded_targets,
    ) = resolve_selection(
        repository_root=root, foundation_release_id=foundation_release_id
    )
    source_inventory = [item.inventory_dict(root) for item in sources]
    excluded_sources = tuple(
        source for target in excluded_targets for source in target.sources
    )
    excluded_source_inventory = [
        item.inventory_dict(root) for item in excluded_sources
    ]
    target_inventory = [item.inventory_dict() for item in targets]
    selected_bar_bytes = sum(item.bars_size for item in sources)
    selected_receipt_bytes = sum(item.receipt_size for item in sources)
    source_rows = sum(item.row_count for item in sources)
    split_targets = sum(len(item.sources) > 1 for item in targets)
    implementation = _implementation_inventory(root)
    anomaly_acceptance_release_ids = _anomaly_acceptance_release_ids(
        RepoBoundary(active_root=root), foundation
    )
    selected_quarantined_market_years = sum(
        any(
            item.coverage_disposition.startswith("QUARANTINED_")
            for item in target.sources
        )
        for target in targets
    )
    excluded_quarantined_market_years = sum(
        any(
            item.coverage_disposition.startswith("QUARANTINED_")
            for item in target.sources
        )
        for target in excluded_targets
    )
    if selected_quarantined_market_years:
        raise IntegrityError("research materialization selected quarantined data")
    excluded_market_years = [
        f"{target.market}/{target.year}" for target in excluded_targets
    ]
    excluded_dispositions: dict[str, int] = {}
    for source in excluded_sources:
        excluded_dispositions[source.research_disposition] = (
            excluded_dispositions.get(source.research_disposition, 0) + 1
        )
    price_research_policy, expected_markets, universe_approval_id = (
        _price_research_contract(root)
    )
    observed_markets = tuple(
        sorted({target.market for target in (*targets, *excluded_targets)})
    )
    if observed_markets != expected_markets:
        raise IntegrityError("foundation selection differs from the 41-market universe")
    if any(not target.price_research_eligible for target in targets):
        raise IntegrityError("materialization selected non-authoritative causal data")
    if any(
        source.coverage_disposition not in QUARANTINED_COVERAGE_DISPOSITIONS
        for target in excluded_targets
        for source in target.sources
    ):
        raise IntegrityError("materialization excluded authoritative causal data")
    capability_counts: dict[str, int] = {}
    for target in targets:
        capability_counts[target.research_capability] = (
            capability_counts.get(target.research_capability, 0) + 1
        )
    contract_path = root / "configs/data_layout_contract.json"
    source_contract_path = root / "configs/source_contract.json"
    return {
        "archive_required_before_cutover": True,
        "anomaly_acceptance_release_ids": list(anomaly_acceptance_release_ids),
        "causal_price_research_policy": price_research_policy,
        "causal_price_research_policy_sha256": sha256_file(
            root / PRICE_RESEARCH_POLICY_PATH
        ),
        "cohort_role_changes": 0,
        "cutover_authorized": False,
        "data_layout_contract_sha256": sha256_file(contract_path),
        "destination_staging_root": STAGING_ROOT.as_posix(),
        "excluded_interval_count": len(excluded_sources),
        "excluded_market_year_count": len(excluded_targets),
        "excluded_market_years": excluded_market_years,
        "excluded_quarantined_market_years": excluded_quarantined_market_years,
        "excluded_research_disposition_counts": dict(
            sorted(excluded_dispositions.items())
        ),
        "excluded_source_inventory_sha256": sha256_json(
            excluded_source_inventory
        ),
        "expected_market_years": len(targets),
        "expected_markets": list(expected_markets),
        "expected_market_count": len(expected_markets),
        "expected_output_files": len(targets) * 2 + 1,
        "expected_parquet_files": len(targets),
        "expected_sidecar_files": len(targets),
        "foundation_dependency_closure_complete": foundation.get(
            "dependency_closure_complete"
        ),
        "foundation_manifest_path": foundation_path.relative_to(root).as_posix(),
        "foundation_manifest_sha256": sha256_file(foundation_path),
        "foundation_release_id": foundation_manifest.release_id,
        "foundation_schema_version": foundation_manifest.schema_version,
        "implementation_files": implementation,
        "implementation_sha256": sha256_json(implementation),
        "maximum_duration_seconds": MAXIMUM_DURATION_SECONDS,
        "maximum_output_bytes": selected_bar_bytes * 2,
        "maximum_source_files_read": (
            len(sources) + len(excluded_sources)
        ) * 3,
        "operation": OPERATION,
        "provider_calls": 0,
        "quarantine_preserved": True,
        "research_capability_counts": dict(sorted(capability_counts.items())),
        "research_eligibility_changes": 0,
        "research_universe_approval_receipt_id": universe_approval_id,
        "research_universe_contract_sha256": sha256_file(
            root / UNIVERSE_CONTRACT_PATH
        ),
        "rollback_boundary": "STAGING_ONLY_NO_ACTIVE_DATA_MUTATION",
        "selected_interval_count": len(sources),
        "selected_quarantined_market_years": 0,
        "selected_parquet_bytes": selected_bar_bytes,
        "selected_receipt_bytes": selected_receipt_bytes,
        "selected_row_count": source_rows,
        "single_interval_market_years": len(targets) - split_targets,
        "source_contract_sha256": sha256_file(source_contract_path),
        "source_inventory_sha256": sha256_json(source_inventory),
        "selection_eligibility_changes": 0,
        "split_interval_market_years": split_targets,
        "status_scope_policy_sha256": sha256_file(
            root / STATUS_SCOPE_POLICY_PATH
        ),
        "target_inventory_sha256": sha256_json(target_inventory),
        "target_layout": TARGET_LAYOUT,
        "target_sidecar_layout": TARGET_SIDECAR_LAYOUT,
        "tracking_policy": "ONE_DURABLE_SIDECAR_PER_MARKET_YEAR_PLUS_AGGREGATE_RECEIPT",
        "holdout_or_forward_access_changes": 0,
        "forbidden_actions": [
            "ACTIVE_DATA_ROOT_MUTATION",
            "DELETE_OR_MOVE_ACCEPTED_RELEASES",
            "HARDLINK_SYMLINK_OR_JUNCTION",
            "MODEL_FIT_OR_OUTCOME_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION_OR_CUTOVER",
        ],
    }


def build_plan(scope: Mapping[str, object]) -> dict[str, object]:
    core = {
        "materialization_plan_version": PLAN_VERSION,
        "operation": OPERATION,
        "scope": dict(scope),
    }
    return {**core, "materialization_plan_id": sha256_json(core)}


def build_approval_draft(plan: Mapping[str, object]) -> dict[str, object]:
    return {
        "approval_receipt_id": None,
        "approval_version": APPROVAL_VERSION,
        "approved_at": None,
        "materialization_plan_id": plan.get("materialization_plan_id"),
        "operation": OPERATION,
        "scope": plan.get("scope"),
        "status": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "user_authorization_id": None,
    }


def _plan_identity(plan: Mapping[str, object]) -> str:
    core = {
        "materialization_plan_version": PLAN_VERSION,
        "operation": OPERATION,
        "scope": plan.get("scope"),
    }
    plan_id = plan.get("materialization_plan_id")
    expected = {**core, "materialization_plan_id": sha256_json(core)}
    if dict(plan) != expected or type(plan_id) is not str:
        raise UnauthorizedOperation("causal market-year materialization plan is invalid")
    return plan_id


def verify_approval(
    approval: Mapping[str, object], plan: Mapping[str, object]
) -> str:
    plan_id = _plan_identity(plan)
    core = {
        "approval_version": APPROVAL_VERSION,
        "approved_at": approval.get("approved_at"),
        "materialization_plan_id": plan_id,
        "operation": OPERATION,
        "scope": plan.get("scope"),
        "status": "APPROVED",
        "user_authorization_id": approval.get("user_authorization_id"),
    }
    expected = {**core, "approval_receipt_id": approval.get("approval_receipt_id")}
    if (
        dict(approval) != expected
        or type(approval.get("approved_at")) is not str
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval.get("user_authorization_id"))) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise UnauthorizedOperation(
            "causal market-year materialization lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _verify_source_file(source: IntervalSource) -> None:
    assert_plain_file(source.bars_physical_path)
    if (
        source.bars_physical_path.stat().st_size != source.bars_size
        or sha256_file(source.bars_physical_path) != source.bars_sha256
    ):
        raise IntegrityError(
            f"foundation-selected causal Parquet differs: {source.interval_key}"
        )


def _event_bounds(path: Path) -> tuple[int, int]:
    try:
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - dependency contract
        raise ContractError("pyarrow is required for causal Parquet validation") from exc
    first: int | None = None
    last: int | None = None
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=262_144, columns=["event_at_ns"]
    ):
        values = batch.column(0)
        if len(values) == 0:
            continue
        if len(values) > 1:
            ordered = pc.all(
                pc.greater_equal(values.slice(1), values.slice(0, len(values) - 1))
            ).as_py()
            if ordered is not True:
                raise IntegrityError(f"causal Parquet is not event-time ordered: {path}")
        batch_first = values[0].as_py()
        batch_last = values[-1].as_py()
        if first is None:
            first = batch_first
        if last is not None and batch_first < last:
            raise IntegrityError(f"causal Parquet batches overlap: {path}")
        last = batch_last
    if first is None or last is None:
        raise IntegrityError(f"causal Parquet contains no rows: {path}")
    return int(first), int(last)


def _materialize_parquet(target: MarketYearTarget, destination: Path) -> None:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - dependency contract
        raise ContractError("pyarrow is required for causal materialization") from exc
    for source in target.sources:
        _verify_source_file(source)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    destination.parent.mkdir(parents=True, exist_ok=True)
    assert_no_linklike_ancestors(destination.parent)
    if destination.exists():
        raise IntegrityError(f"materialized destination already exists: {destination}")
    try:
        if len(target.sources) == 1:
            shutil.copyfile(target.sources[0].bars_physical_path, temporary)
            with temporary.open("rb+") as stream:
                os.fsync(stream.fileno())
        else:
            previous_last: int | None = None
            writer: Any | None = None
            try:
                for source in target.sources:
                    first, last = _event_bounds(source.bars_physical_path)
                    if previous_last is not None and first <= previous_last:
                        raise IntegrityError(
                            f"market-year source rows overlap: {target.market}/{target.year}"
                        )
                    previous_last = last
                    parquet = pq.ParquetFile(source.bars_physical_path)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            temporary,
                            parquet.schema_arrow,
                            compression="zstd",
                            use_dictionary=True,
                            write_statistics=True,
                            version="2.6",
                        )
                    elif not writer.schema.equals(parquet.schema_arrow):
                        raise IntegrityError("market-year merge schema changed during execution")
                    for batch in parquet.iter_batches(batch_size=262_144):
                        writer.write_batch(batch)
            finally:
                if writer is not None:
                    writer.close()
            with temporary.open("rb+") as stream:
                os.fsync(stream.fileno())
        observed = pq.ParquetFile(temporary)
        try:
            observed_row_count = observed.metadata.num_rows
        finally:
            observed.close()
        if observed_row_count != target.row_count:
            raise IntegrityError("materialized market-year row count differs")
        if _schema_fingerprint(temporary) != target.sources[0].schema_fingerprint:
            raise IntegrityError("materialized market-year schema differs")
        os.replace(temporary, destination)
        fsync_directory(destination.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _sidecar_core(
    *,
    target: MarketYearTarget,
    destination: Path,
    foundation_release_id: str,
    plan_id: str,
) -> dict[str, object]:
    return {
        "coverage_end": target.coverage_end,
        "coverage_start": target.coverage_start,
        "foundation_release_id": foundation_release_id,
        "logical_path": target.logical_path,
        "market": target.market,
        "materialization_plan_id": plan_id,
        "merge_mode": (
            "VERIFIED_COPY" if len(target.sources) == 1 else "CONTIGUOUS_INTERVAL_CONCAT"
        ),
        "parquet_sha256": sha256_file(destination),
        "parquet_size": destination.stat().st_size,
        "research_capability": target.research_capability,
        "row_count": target.row_count,
        "schema_fingerprint": target.sources[0].schema_fingerprint,
        "sidecar_version": SIDECAR_VERSION,
        "source_intervals": [
            {
                "bars_sha256": source.bars_sha256,
                "coverage_disposition": source.coverage_disposition,
                "end": source.end,
                "release_id": source.release_id,
                "research_disposition": source.research_disposition,
                "research_in_scope": source.research_in_scope,
                "research_scope_policy_hash": source.research_scope_policy_hash,
                "row_count": source.row_count,
                "start": source.start,
                "status_epoch_gate_id": source.status_epoch_gate_id,
                "status_gated_feature_ready_rows": (
                    source.status_gated_feature_ready_rows
                ),
            }
            for source in target.sources
        ],
        "year": target.year,
    }


def _verify_or_materialize_target(
    *,
    target: MarketYearTarget,
    output_root: Path,
    foundation_release_id: str,
    plan_id: str,
) -> dict[str, object]:
    destination = output_root / target.relative_parquet_path
    sidecar_path = destination.with_name(destination.name + SIDECAR_SUFFIX)
    if destination.exists() or sidecar_path.exists():
        assert_plain_file(destination)
        assert_plain_file(sidecar_path)
        sidecar = _load_json(sidecar_path)
        core = dict(sidecar)
        sidecar_id = core.pop("sidecar_id", None)
        if sidecar_id != sha256_json(core):
            raise IntegrityError("existing market-year sidecar identity differs")
        expected = _sidecar_core(
            target=target,
            destination=destination,
            foundation_release_id=foundation_release_id,
            plan_id=plan_id,
        )
        if core != expected:
            raise IntegrityError("existing market-year materialization differs")
    else:
        _materialize_parquet(target, destination)
        core = _sidecar_core(
            target=target,
            destination=destination,
            foundation_release_id=foundation_release_id,
            plan_id=plan_id,
        )
        _write_new_or_exact(
            sidecar_path, {**core, "sidecar_id": sha256_json(core)}
        )
    return {
        "logical_path": target.logical_path,
        "parquet_path": destination.relative_to(output_root.parent).as_posix(),
        "parquet_sha256": sha256_file(destination),
        "parquet_size": destination.stat().st_size,
        "row_count": target.row_count,
        "sidecar_path": sidecar_path.relative_to(output_root.parent).as_posix(),
        "sidecar_sha256": sha256_file(sidecar_path),
        "source_release_ids": [source.release_id for source in target.sources],
    }


def _receipt_core(
    *,
    approval_id: str,
    outputs: Sequence[Mapping[str, object]],
    plan: Mapping[str, object],
) -> dict[str, object]:
    scope = dict(plan["scope"])
    return {
        "approval_receipt_id": approval_id,
        "cutover_authorized": False,
        "foundation_release_id": scope["foundation_release_id"],
        "excluded_market_year_count": scope["excluded_market_year_count"],
        "holdout_or_forward_access_changes": 0,
        "materialization_plan_id": plan["materialization_plan_id"],
        "operation": OPERATION,
        "output_bytes": sum(int(item["parquet_size"]) for item in outputs),
        "output_inventory_sha256": sha256_json(list(outputs)),
        "outputs": list(outputs),
        "parquet_files": len(outputs),
        "receipt_version": RECEIPT_VERSION,
        "research_capability_counts": scope["research_capability_counts"],
        "research_use_classification": "NON_AUTHORIZING_DATA_CAPABILITY",
        "selection_eligibility_changes": 0,
        "row_count": sum(int(item["row_count"]) for item in outputs),
        "status": "COMPLETE_VERIFIED_STAGING_ONLY",
    }


def execute(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    plan_id = _plan_identity(plan)
    approval_id = verify_approval(approval, plan)
    live_scope = build_scope(
        repository_root=root,
        foundation_release_id=str(dict(plan["scope"])["foundation_release_id"]),
    )
    if plan.get("scope") != live_scope:
        raise UnauthorizedOperation(
            "causal market-year materialization plan differs from live inputs"
        )
    _, _, _, _, targets, _ = resolve_selection(
        repository_root=root,
        foundation_release_id=str(live_scope["foundation_release_id"]),
    )
    stage = boundary.active_root / STAGING_ROOT / plan_id
    output_root = stage / OUTPUT_ROOT_NAME
    receipt_path = stage / RECEIPT_NAME
    if receipt_path.exists():
        return verify_staging(
            repository_root=root,
            plan=plan,
            approval=approval,
            receipt=_load_json(receipt_path),
        )
    with FileLease(boundary.active_root / LOCK_PATH):
        output_root.mkdir(parents=True, exist_ok=True)
        started_at = time.monotonic()
        outputs: list[dict[str, object]] = []
        output_bytes = 0
        for target in targets:
            if time.monotonic() - started_at > live_scope["maximum_duration_seconds"]:
                raise IntegrityError("materialization exceeded its duration ceiling")
            output = _verify_or_materialize_target(
                target=target,
                output_root=output_root,
                foundation_release_id=str(live_scope["foundation_release_id"]),
                plan_id=plan_id,
            )
            outputs.append(output)
            output_bytes += int(output["parquet_size"])
            if output_bytes > live_scope["maximum_output_bytes"]:
                raise IntegrityError("materialized output exceeds its byte ceiling")
        if (
            len(outputs) != live_scope["expected_parquet_files"]
            or output_bytes > live_scope["maximum_output_bytes"]
        ):
            raise IntegrityError("materialized output exceeds its bounded plan")
        core = _receipt_core(
            approval_id=approval_id, outputs=outputs, plan=plan
        )
        receipt = {**core, "receipt_id": sha256_json(core)}
        _write_new_or_exact(receipt_path, receipt)
    return verify_staging(
        repository_root=root,
        plan=plan,
        approval=approval,
        receipt=_load_json(receipt_path),
    )


def verify_staging(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    receipt: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan_id = _plan_identity(plan)
    approval_id = verify_approval(approval, plan)
    stage = root / STAGING_ROOT / plan_id
    outputs = receipt.get("outputs")
    if not isinstance(outputs, list) or any(not isinstance(item, dict) for item in outputs):
        raise IntegrityError("materialization receipt output inventory is invalid")
    core = dict(receipt)
    receipt_id = core.pop("receipt_id", None)
    if (
        receipt_id != sha256_json(core)
        or core
        != _receipt_core(
            approval_id=approval_id,
            outputs=outputs,
            plan=plan,
        )
    ):
        raise IntegrityError("materialization receipt identity differs")
    observed_paths: set[Path] = set()
    for item in outputs:
        parquet = stage / str(item["parquet_path"])
        sidecar = stage / str(item["sidecar_path"])
        assert_plain_file(parquet)
        assert_plain_file(sidecar)
        if (
            parquet.stat().st_size != item["parquet_size"]
            or sha256_file(parquet) != item["parquet_sha256"]
            or sha256_file(sidecar) != item["sidecar_sha256"]
        ):
            raise IntegrityError("staged market-year output differs from its receipt")
        observed_paths.update((parquet, sidecar))
    actual_paths = {
        path
        for path in stage.rglob("*")
        if path.is_file() and path != stage / RECEIPT_NAME
    }
    if actual_paths != observed_paths:
        raise IntegrityError("staging tree contains undeclared files")
    return dict(receipt)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "materialize", "verify"):
        item = commands.add_parser(command)
        item.add_argument("--repository-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    if args.command == "plan":
        plan = build_plan(build_scope(repository_root=root))
        print(
            json.dumps(
                {"approval_draft": build_approval_draft(plan), "plan": plan},
                sort_keys=True,
            )
        )
        return 0
    plan = _load_json(root / PLAN_PATH)
    approval = _load_json(root / APPROVAL_PATH)
    if args.command == "materialize":
        receipt = execute(repository_root=root, plan=plan, approval=approval)
    else:
        plan_id = _plan_identity(plan)
        receipt = verify_staging(
            repository_root=root,
            plan=plan,
            approval=approval,
            receipt=_load_json(root / STAGING_ROOT / plan_id / RECEIPT_NAME),
        )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
