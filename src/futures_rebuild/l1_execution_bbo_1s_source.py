"""Fail-closed access to the sealed, non-Alpha BBO-1s eligible partition set."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Final

from .boundary import RepoBoundary
from .canonical import assert_plain_file, read_canonical_authority, sha256_file
from .data_layout import manifest_relative_path, verify_data_release_manifest
from .errors import IntegrityError


CATALOG_ID: Final = "6ef726aaf448e12c3f2a89a882e1873159ab09481ebe6b910f4b51e5d186b628"
CATALOG_SHA256: Final = "8dbc194eb87cd4b427596ca5719281498f4ffabe57345848660f7efbb5eedba6"
RELEASE_ID: Final = "991b45e7b5833f547863550ae64008192a6599ffab270b28dbbcb1453d54c96f"
RELEASE_MANIFEST_SHA256: Final = "28a86de15f549c3881c120de401b34d5e34d7e5b4f1731ff4ff33d6f6f4d161d"
CATALOG_PATH: Final = Path(
    f"state/l1_bbo_1s_eligibility_catalog_registry/{CATALOG_ID}/catalog.json"
)
ENTRY_COUNT: Final = 4_698
ELIGIBLE_COUNT: Final = 4_486
EXCLUDED_COUNT: Final = 212
RELEASE_FILE_COUNT: Final = 9_323
ELIGIBLE_DISPOSITION: Final = "CERTIFIED_SOURCE_ELIGIBLE_NOT_ALPHA_ACTIVE"
ENTRY_KEYS: Final = frozenset(
    {
        "alpha_source_authorized",
        "disposition",
        "logical_path",
        "market",
        "provider_condition",
        "request_id",
        "row_quality_certified",
        "source_eligible",
        "structural_scan_completed",
        "trade_date",
    }
)


def eligible_bbo_1s_paths(*, root: Path) -> tuple[Path, ...]:
    """Return only the 4,486 hash-verified canonical eligible DBNs."""

    boundary = RepoBoundary(root)
    catalog_path = boundary.assert_active_path(
        boundary.active_root / CATALOG_PATH,
        purpose="BBO-1s eligibility catalog",
        subtree="state/l1_bbo_1s_eligibility_catalog_registry",
    )
    assert_plain_file(catalog_path)
    catalog = read_canonical_authority(
        catalog_path,
        expected_sha256=CATALOG_SHA256,
        identity_field="catalog_id",
        expected_identity=CATALOG_ID,
        expected_schema="l1_bbo_1s_eligibility_catalog/1.0.0",
    )
    entries = catalog.get("entries")
    if (
        catalog.get("state") != "SEALED_SOURCE_ELIGIBILITY_CATALOG_NOT_ALPHA_ACTIVE"
        or catalog.get("entry_count") != ENTRY_COUNT
        or catalog.get("eligible_count") != ELIGIBLE_COUNT
        or catalog.get("excluded_count") != EXCLUDED_COUNT
        or catalog.get("alpha_source_authorized") is not False
        or not isinstance(entries, list)
        or len(entries) != ENTRY_COUNT
    ):
        raise IntegrityError("BBO-1s eligibility catalog contract differs")

    manifest_path = boundary.assert_active_path(
        boundary.active_root / manifest_relative_path("dbn", RELEASE_ID),
        purpose="BBO-1s canonical release manifest",
        subtree="manifests/data_releases/dbn",
    )
    if sha256_file(manifest_path) != RELEASE_MANIFEST_SHA256:
        raise IntegrityError("BBO-1s canonical release manifest changed")
    manifest = verify_data_release_manifest(manifest_path, boundary)
    catalog_binding = manifest.embedded_documents.get("eligibility_catalog")
    if (
        manifest.release_id != RELEASE_ID
        or manifest.release_kind != "l1_bbo_1s_market_partition_canonical_custody"
        or len(manifest.files) != RELEASE_FILE_COUNT
        or manifest.metadata.get("eligible_count") != ELIGIBLE_COUNT
        or manifest.metadata.get("alpha_source_authorized") is not False
        or catalog_binding != {"id": CATALOG_ID, "sha256": CATALOG_SHA256}
    ):
        raise IntegrityError("BBO-1s canonical release binding differs")

    released = {entry.logical_path: entry for entry in manifest.files}
    selected: list[Path] = []
    requests: set[str] = set()
    logical_paths: set[str] = set()
    for item in entries:
        if not isinstance(item, Mapping) or set(item) != ENTRY_KEYS:
            raise IntegrityError("BBO-1s eligibility entry schema differs")
        request_id = item["request_id"]
        logical = item["logical_path"]
        market = item["market"]
        trade_date = item["trade_date"]
        if (
            type(request_id) is not str
            or type(logical) is not str
            or type(market) is not str
            or type(trade_date) is not str
            or logical
            != f"data/dbn/bbo_1s/{market}/{trade_date[:4]}/{trade_date}.dbn.zst"
            or item["alpha_source_authorized"] is not False
            or request_id in requests
            or logical in logical_paths
        ):
            raise IntegrityError("BBO-1s eligibility entry binding differs")
        requests.add(request_id)
        logical_paths.add(logical)
        eligible = item["disposition"] == ELIGIBLE_DISPOSITION
        if (
            item["row_quality_certified"] is not eligible
            or item["source_eligible"] is not eligible
            or (eligible and item["structural_scan_completed"] is not True)
        ):
            raise IntegrityError("BBO-1s eligibility disposition differs")
        if eligible:
            entry = released.get(logical)
            if entry is None or f"{logical}.manifest.json" not in released:
                raise IntegrityError("eligible BBO-1s partition is absent from the release")
            selected.append(boundary.active_root / manifest.physical_relative_path(entry))

    if len(selected) != ELIGIBLE_COUNT or len(set(selected)) != ELIGIBLE_COUNT:
        raise IntegrityError("BBO-1s eligible partition count differs")
    return tuple(selected)


__all__ = ["eligible_bbo_1s_paths"]
