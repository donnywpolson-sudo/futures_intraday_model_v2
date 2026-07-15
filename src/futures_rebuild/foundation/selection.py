"""Immutable publication and exact interval pairing for DBN source selection."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping

from ..boundary import OperationClassification, OperationReceipt, RepoBoundary
from ..canonical import assert_plain_file, canonical_bytes, sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt
from ..source_symbology import require_query_contract
from .snapshot import PublishedSourceSnapshot, SnapshotFile


AUTHORITATIVE_COVERAGE_DISPOSITIONS = frozenset(
    {
        "AUTHORITATIVE_INTERVAL",
        "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK",
        "QUARANTINED_PENDING_REVALIDATION",
        "QUARANTINED_PENDING_REVALIDATION_WITH_EXACT_REDUNDANT_CROSSCHECK",
    }
)
REDUNDANT_COVERAGE_DISPOSITIONS = frozenset(
    {
        "REDUNDANT_EXACT_CROSSCHECK_ONLY",
        "QUARANTINED_REDUNDANT_EXACT_CROSSCHECK_ONLY",
    }
)
ALLOWED_COVERAGE_DISPOSITIONS = (
    AUTHORITATIVE_COVERAGE_DISPOSITIONS | REDUNDANT_COVERAGE_DISPOSITIONS
)


SELECTION_RELEASE_KIND = "futures_dbn_source_selection"
SELECTION_SCHEMA_VERSION = "2.0.0"


@dataclass(frozen=True)
class SelectedInterval:
    market: str
    year: int
    start: str
    end: str
    definition: "SelectedFamilyFile"
    bars: "SelectedFamilyFile"
    coverage_disposition: str
    statistics: tuple["SelectedFamilyFile", ...] = ()
    status: tuple["SelectedFamilyFile", ...] = ()


@dataclass(frozen=True)
class SelectedFamilyFile:
    family: str
    schema: str
    market: str
    year: int
    start: str
    end: str
    coverage_disposition: str
    binding: SnapshotFile
    sidecar_binding: SnapshotFile
    query_contract: Mapping[str, object]
    query_contract_id: str
    query_mode_id: str

    def as_coverage_binding(self) -> dict[str, object]:
        return {
            "coverage_disposition": self.coverage_disposition,
            "end": self.end,
            "family": self.family,
            "market": self.market,
            "path": self.binding.relative_path,
            "query_contract": dict(self.query_contract),
            "query_contract_id": self.query_contract_id,
            "query_mode_id": self.query_mode_id,
            "schema": self.schema,
            "sha256": self.binding.sha256,
            "size": self.binding.size,
            "sidecar_path": self.sidecar_binding.relative_path,
            "sidecar_sha256": self.sidecar_binding.sha256,
            "sidecar_size": self.sidecar_binding.size,
            "start": self.start,
            "year": self.year,
        }


@dataclass(frozen=True)
class ResolvedFoundationSelection:
    intervals: tuple[SelectedInterval, ...]
    status_files: tuple[SelectedFamilyFile, ...]
    statistics_files: tuple[SelectedFamilyFile, ...]
    coverage_matrix: tuple[dict[str, object], ...]
    coverage_matrix_id: str
    query_manifest: tuple[dict[str, object], ...]
    query_manifest_id: str
    query_mode_census: tuple[dict[str, object], ...]
    selected_file_count: int

    @property
    def required_market_year_count(self) -> int:
        return len(self.intervals)


def publish_source_selection(
    selection: dict[str, object],
    *,
    snapshot: PublishedSourceSnapshot,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    if (
        selection.get("catalog_contract_version") != "2.0.0"
        or
        selection.get("source_scope") != "VERIFIED_PUBLISHED_SOURCE_SNAPSHOT"
        or selection.get("source_snapshot_id") != snapshot.source_snapshot_id
        or selection.get("dataset") != "GLBX.MDP3"
        or selection.get("selection_policy")
        != "EXACT_CONTRACT_ALL_FILES_NO_RECURSIVE_NEWEST"
        or selection.get("selection_scope") != "FILTERED"
        or not isinstance(selection.get("files"), list)
    ):
        raise IntegrityError("source selection is not bound to the verified DBN snapshot")
    selection_id = selection.get("selection_manifest_id")
    core = {key: value for key, value in selection.items() if key != "selection_manifest_id"}
    if selection_id != sha256_json(core):
        raise IntegrityError("source selection content address is invalid")
    selected_families = {str(item.get("family")) for item in selection.get("families", [])}
    required = {"dbn_definition", "dbn_ohlcv_1m", "dbn_statistics", "dbn_status"}
    if selected_families != required:
        raise IntegrityError("source selection lacks an exact canonical research family")
    raw_files = selection.get("files")
    assert isinstance(raw_files, list)
    file_families = {
        str(item.get("family")) for item in raw_files if isinstance(item, dict)
    }
    if file_families != required:
        raise IntegrityError(
            "source selection declares but does not contain every canonical family"
        )
    resolved = resolve_foundation_selection(selection, snapshot=snapshot)
    stage = publisher.create_stage("source_selection")
    (stage / "source_selection.json").write_bytes(canonical_bytes(selection) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        release_kind=SELECTION_RELEASE_KIND,
        schema_version=SELECTION_SCHEMA_VERSION,
        source_release_ids=(snapshot.source_snapshot_id,),
        metadata={
            "selection_manifest_id": selection_id,
            "source_snapshot_id": snapshot.source_snapshot_id,
            "query_manifest_id": resolved.query_manifest_id,
        },
    )
    release = publisher.publish(stage, manifest)
    return VerifiedReleaseReceipt.from_release(release, publisher.boundary)


def publish_catalog_selection(
    catalog_path: Path,
    *,
    snapshot: PublishedSourceSnapshot,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Promote one verified loose catalog into an immutable selection release."""

    if publisher.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("selection publisher belongs to another repository")
    catalog = boundary.assert_active_path(
        catalog_path,
        purpose="verified DBN catalog",
        subtree="state/source_selection",
    )
    expected_parent = (
        boundary.active_root / "state" / "source_selection"
    ).resolve(strict=False)
    if catalog.parent != expected_parent or catalog.suffix != ".json":
        raise ContractError("verified DBN catalog must be one direct JSON child")
    try:
        assert_plain_file(catalog)
        raw = catalog.read_bytes()
        selection = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("verified DBN catalog JSON is invalid") from exc
    if not isinstance(selection, dict) or raw != canonical_bytes(selection) + b"\n":
        raise IntegrityError("verified DBN catalog is not canonical JSON")
    return publish_source_selection(
        selection, snapshot=snapshot, publisher=publisher
    )


def load_source_selection(
    receipt: VerifiedReleaseReceipt,
    *,
    snapshot: PublishedSourceSnapshot,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != SELECTION_RELEASE_KIND
        or manifest.schema_version != SELECTION_SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {"source_selection.json"}
        or set(manifest.metadata)
        != {"query_manifest_id", "selection_manifest_id", "source_snapshot_id"}
        or manifest.metadata["source_snapshot_id"] != snapshot.source_snapshot_id
        or manifest.source_release_ids != (snapshot.source_snapshot_id,)
    ):
        raise IntegrityError("source selection release contract is invalid")
    path = boundary.active_root / receipt.relative_root / "source_selection.json"
    try:
        raw = path.read_bytes()
        selection = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("source selection release JSON is invalid") from exc
    if not isinstance(selection, dict) or raw != canonical_bytes(selection) + b"\n":
        raise IntegrityError("source selection release is not canonical JSON")
    selection_id = selection.pop("selection_manifest_id", None)
    if (
        selection_id != sha256_json(selection)
        or selection_id != manifest.metadata["selection_manifest_id"]
        or selection.get("source_snapshot_id") != snapshot.source_snapshot_id
    ):
        raise IntegrityError("source selection release content address is invalid")
    selection["selection_manifest_id"] = selection_id
    resolved = resolve_foundation_selection(selection, snapshot=snapshot)
    if resolved.query_manifest_id != manifest.metadata["query_manifest_id"]:
        raise IntegrityError("source selection query manifest identity is invalid")
    return selection


def _binding(snapshot: PublishedSourceSnapshot, declared_path: object) -> SnapshotFile:
    logical = Path(str(declared_path))
    try:
        relative = logical.relative_to(Path("data") / "dbn")
    except ValueError as exc:
        raise IntegrityError("selected DBN path is outside logical data/dbn") from exc
    return snapshot.file((Path("dbn") / relative).as_posix())


def _verify_all_selected_bindings(
    selection: Mapping[str, object], *, snapshot: PublishedSourceSnapshot
) -> None:
    raw_files = selection.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise IntegrityError("source selection has no selected files")
    seen: set[str] = set()
    for raw in raw_files:
        if not isinstance(raw, dict):
            raise IntegrityError("source selection file entry is invalid")
        raw_core = {key: value for key, value in raw.items() if key != "validation_sha256"}
        if raw.get("validation_sha256") != sha256_json(raw_core):
            raise IntegrityError("source selection file validation hash is invalid")
        binding = _binding(snapshot, raw.get("path"))
        if binding.relative_path in seen:
            raise IntegrityError("source selection contains a duplicate file path")
        seen.add(binding.relative_path)
        if raw.get("sha256") != binding.sha256 or raw.get("size") != binding.size:
            raise IntegrityError("source selection file differs from source snapshot")
        binding.verify()
        sidecar_path = raw.get("sidecar_path")
        if type(sidecar_path) is not str:
            raise IntegrityError("source selection lacks an exact DBN sidecar binding")
        sidecar = _binding(snapshot, sidecar_path)
        if (
            sidecar.relative_path != f"{binding.relative_path}.manifest.json"
            or raw.get("sidecar_sha256") != sidecar.sha256
            or raw.get("sidecar_size") != sidecar.size
        ):
            raise IntegrityError("source selection sidecar differs from source snapshot")
        sidecar.verify()


def _selected_family_file(
    raw: Mapping[str, object], *, snapshot: PublishedSourceSnapshot
) -> SelectedFamilyFile:
    try:
        schema = str(raw["schema"])
        family = str(raw["family"])
        market = str(raw["market"])
        year = raw["year"]
        start = str(raw["start"])
        end = str(raw["end"])
        disposition = str(raw["coverage_disposition"])
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except (KeyError, TypeError, ValueError) as exc:
        raise IntegrityError("source selection coverage fields are invalid") from exc
    if (
        type(year) is not int
        or start_date.year != year
        or end_date <= start_date
        or schema not in {"definition", "ohlcv-1m", "statistics", "status"}
        or family
        != {
            "definition": "dbn_definition",
            "ohlcv-1m": "dbn_ohlcv_1m",
            "statistics": "dbn_statistics",
            "status": "dbn_status",
        }[schema]
        or disposition not in ALLOWED_COVERAGE_DISPOSITIONS
    ):
        raise IntegrityError("source selection family/schema/coverage is invalid")
    try:
        query_contract = require_query_contract(raw["query_contract"])
    except (KeyError, ContractError, IntegrityError) as exc:
        raise IntegrityError("source selection query symbology is invalid") from exc
    if (
        query_contract["schema"] != schema
        or query_contract["market"] != market
        or raw.get("query_stype_in") != query_contract["stype_in"]
        or raw.get("query_symbols") != query_contract["symbols"]
        or raw.get("query_contract_id") != query_contract["query_contract_id"]
        or raw.get("query_mode_id") != query_contract["query_mode_id"]
    ):
        raise IntegrityError("source selection query contract binding is invalid")
    binding = _binding(snapshot, raw["path"])
    sidecar = _binding(snapshot, raw["sidecar_path"])
    return SelectedFamilyFile(
        family=family,
        schema=schema,
        market=market,
        year=year,
        start=start,
        end=end,
        coverage_disposition=disposition,
        binding=binding,
        sidecar_binding=sidecar,
        query_contract=query_contract,
        query_contract_id=str(query_contract["query_contract_id"]),
        query_mode_id=str(query_contract["query_mode_id"]),
    )


def resolve_foundation_selection(
    selection: dict[str, object], *, snapshot: PublishedSourceSnapshot
) -> ResolvedFoundationSelection:
    """Resolve all four canonical families and make every gap/extra explicit."""

    _verify_all_selected_bindings(selection, snapshot=snapshot)
    raw_files = selection.get("files")
    assert isinstance(raw_files, list)
    all_selected: list[SelectedFamilyFile] = []
    authoritative: list[SelectedFamilyFile] = []
    for raw in raw_files:
        assert isinstance(raw, dict)
        selected = _selected_family_file(raw, snapshot=snapshot)
        all_selected.append(selected)
        if selected.coverage_disposition in REDUNDANT_COVERAGE_DISPOSITIONS:
            continue
        authoritative.append(selected)

    by_schema: dict[str, list[SelectedFamilyFile]] = {
        schema: [] for schema in ("definition", "ohlcv-1m", "statistics", "status")
    }
    for item in authoritative:
        by_schema[item.schema].append(item)
    for schema, values in by_schema.items():
        values.sort(key=lambda item: (item.market, item.start, item.end, item.binding.sha256))
        previous: dict[str, SelectedFamilyFile] = {}
        for item in values:
            prior = previous.get(item.market)
            if prior is not None and date.fromisoformat(item.start) < date.fromisoformat(
                prior.end
            ):
                raise IntegrityError(
                    f"source selection has unresolved overlapping {schema} coverage"
                )
            previous[item.market] = item

    definitions = {
        (item.market, item.start, item.end): item
        for item in by_schema["definition"]
    }
    bars = {
        (item.market, item.start, item.end): item
        for item in by_schema["ohlcv-1m"]
    }
    if len(definitions) != len(by_schema["definition"]) or len(bars) != len(
        by_schema["ohlcv-1m"]
    ):
        raise IntegrityError("source selection contains duplicate authoritative intervals")
    if set(definitions) != set(bars) or not bars:
        raise IntegrityError("definition/bar interval pairing is incomplete")

    statuses_by_market_year: dict[tuple[str, int], list[SelectedFamilyFile]] = {}
    statistics_by_market_year: dict[tuple[str, int], list[SelectedFamilyFile]] = {}
    for item in by_schema["status"]:
        statuses_by_market_year.setdefault((item.market, item.year), []).append(item)
    for item in by_schema["statistics"]:
        statistics_by_market_year.setdefault((item.market, item.year), []).append(item)

    intervals: list[SelectedInterval] = []
    for market, start, end in sorted(bars):
        bar = bars[(market, start, end)]
        definition = definitions[(market, start, end)]
        if bar.year != definition.year:
            raise IntegrityError("selected definition/bar year differs")
        key = (market, bar.year)
        intervals.append(
            SelectedInterval(
                market=market,
                year=bar.year,
                start=start,
                end=end,
                definition=definition,
                bars=bar,
                coverage_disposition=bar.coverage_disposition,
                statistics=tuple(statistics_by_market_year.get(key, ())),
                status=tuple(statuses_by_market_year.get(key, ())),
            )
        )

    required_keys = {(item.market, item.year) for item in intervals}
    all_keys = (
        required_keys
        | set(statuses_by_market_year)
        | set(statistics_by_market_year)
        | {(item.market, item.year) for item in all_selected}
    )
    coverage_matrix: list[dict[str, object]] = []
    for market, year in sorted(all_keys):
        definition_count = sum(
            item.market == market and item.year == year
            for item in by_schema["definition"]
        )
        bar_count = sum(
            item.market == market and item.year == year
            for item in by_schema["ohlcv-1m"]
        )
        status_files = statuses_by_market_year.get((market, year), [])
        statistics_files = statistics_by_market_year.get((market, year), [])
        required = (market, year) in required_keys
        definition_files = [
            item
            for item in by_schema["definition"]
            if item.market == market and item.year == year
        ]
        bar_files = [
            item
            for item in by_schema["ohlcv-1m"]
            if item.market == market and item.year == year
        ]
        redundant_files = [
            item
            for item in all_selected
            if item.market == market
            and item.year == year
            and item.coverage_disposition in REDUNDANT_COVERAGE_DISPOSITIONS
        ]
        coverage_matrix.append(
            {
                "bar_file_count": bar_count,
                "bar_file_sha256s": sorted(item.binding.sha256 for item in bar_files),
                "bar_query_bindings": sorted(
                    (
                        {
                            "file_sha256": item.binding.sha256,
                            "query_contract_id": item.query_contract_id,
                        }
                        for item in bar_files
                    ),
                    key=lambda item: (item["file_sha256"], item["query_contract_id"]),
                ),
                "bar_query_mode_ids": sorted({item.query_mode_id for item in bar_files}),
                "definition_file_count": definition_count,
                "definition_file_sha256s": sorted(
                    item.binding.sha256 for item in definition_files
                ),
                "definition_query_bindings": sorted(
                    (
                        {
                            "file_sha256": item.binding.sha256,
                            "query_contract_id": item.query_contract_id,
                        }
                        for item in definition_files
                    ),
                    key=lambda item: (item["file_sha256"], item["query_contract_id"]),
                ),
                "definition_query_mode_ids": sorted(
                    {item.query_mode_id for item in definition_files}
                ),
                "market": market,
                "redundant_crosscheck_file_count": len(redundant_files),
                "redundant_crosscheck_file_sha256s": sorted(
                    item.binding.sha256 for item in redundant_files
                ),
                "redundant_query_bindings": sorted(
                    (
                        {
                            "file_sha256": item.binding.sha256,
                            "query_contract_id": item.query_contract_id,
                        }
                        for item in redundant_files
                    ),
                    key=lambda item: (item["file_sha256"], item["query_contract_id"]),
                ),
                "required_for_bar_foundation": required,
                "statistics_disposition": (
                    "AVAILABLE_NON_ALPHA_FOUNDATION_ONLY"
                    if statistics_files
                    else "STATISTICS_UNRESOLVED"
                ),
                "statistics_file_count": len(statistics_files),
                "statistics_file_sha256s": sorted(
                    item.binding.sha256 for item in statistics_files
                ),
                "statistics_query_bindings": sorted(
                    (
                        {
                            "file_sha256": item.binding.sha256,
                            "query_contract_id": item.query_contract_id,
                        }
                        for item in statistics_files
                    ),
                    key=lambda item: (item["file_sha256"], item["query_contract_id"]),
                ),
                "statistics_query_mode_ids": sorted(
                    {item.query_mode_id for item in statistics_files}
                ),
                "status_disposition": (
                    "AVAILABLE_AS_OF_ELIGIBILITY_INPUT"
                    if status_files
                    else "STATUS_UNRESOLVED"
                ),
                "status_file_count": len(status_files),
                "status_file_sha256s": sorted(item.binding.sha256 for item in status_files),
                "status_query_bindings": sorted(
                    (
                        {
                            "file_sha256": item.binding.sha256,
                            "query_contract_id": item.query_contract_id,
                        }
                        for item in status_files
                    ),
                    key=lambda item: (item["file_sha256"], item["query_contract_id"]),
                ),
                "status_query_mode_ids": sorted(
                    {item.query_mode_id for item in status_files}
                ),
                "year": year,
            }
        )
    matrix_id = sha256_json(coverage_matrix)
    query_manifest = sorted(
        (
            {
                **item.as_coverage_binding(),
                "validation_sha256": next(
                    str(raw["validation_sha256"])
                    for raw in raw_files
                    if isinstance(raw, dict)
                    and raw.get("path") == f"data/{item.binding.relative_path}"
                ),
            }
            for item in all_selected
        ),
        key=lambda item: str(item["path"]),
    )
    query_manifest_id = sha256_json(query_manifest)
    mode_counts: dict[tuple[str, str, str], int] = {}
    for item in all_selected:
        key = (item.schema, str(item.query_contract["stype_in"]), item.query_mode_id)
        mode_counts[key] = mode_counts.get(key, 0) + 1
    query_mode_census = [
        {
            "file_count": count,
            "query_mode_id": mode_id,
            "schema": schema,
            "stype_in": stype_in,
        }
        for (schema, stype_in, mode_id), count in sorted(mode_counts.items())
    ]
    return ResolvedFoundationSelection(
        intervals=tuple(intervals),
        status_files=tuple(by_schema["status"]),
        statistics_files=tuple(by_schema["statistics"]),
        coverage_matrix=tuple(coverage_matrix),
        coverage_matrix_id=matrix_id,
        query_manifest=tuple(query_manifest),
        query_manifest_id=query_manifest_id,
        query_mode_census=tuple(query_mode_census),
        selected_file_count=len(raw_files),
    )


def pair_selected_intervals(
    selection: dict[str, object], *, snapshot: PublishedSourceSnapshot
) -> tuple[SelectedInterval, ...]:
    return resolve_foundation_selection(selection, snapshot=snapshot).intervals


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish one verified offline DBN catalog as an immutable selection"
    )
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--source-snapshot-root", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("selection publication requires explicit --execute")
    try:
        contract = json.loads(args.source_contract.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError("source contract JSON is invalid") from exc
    if not isinstance(contract, dict):
        raise ContractError("source contract must be an object")
    boundary = RepoBoundary(
        Path(str(contract["active_repository"])),
        legacy_roots=(Path(str(contract["legacy_repository"])),),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(args.repository_root)
    boundary.assert_active_path(
        args.source_contract, purpose="source contract", subtree="configs"
    )
    snapshot = PublishedSourceSnapshot.open(
        args.source_snapshot_root, boundary=boundary
    )
    catalog = boundary.assert_active_path(
        args.catalog,
        purpose="verified DBN catalog",
        subtree="state/source_selection",
    )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "catalog_path": catalog.name,
            "catalog_sha256": sha256_file(catalog),
            "source_snapshot_id": snapshot.source_snapshot_id,
        },
    )
    publisher = AtomicPublisher(
        boundary.active_root
        / "data"
        / "vault"
        / ".staging"
        / "releases"
        / "source_selection",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "source-selection.lock",
        boundary=boundary,
        operation_receipt=operation,
    )
    receipt = publish_catalog_selection(
        catalog,
        snapshot=snapshot,
        boundary=boundary,
        publisher=publisher,
    )
    print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
