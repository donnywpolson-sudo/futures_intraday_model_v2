from __future__ import annotations

import inspect
import json
import shutil
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild import micro_alpha_product_effective_dates as dates


ROOT = Path(__file__).resolve().parents[1]


def _write_report(path: Path, core: dict[str, object]) -> dict[str, object]:
    report = {**core, "report_id": sha256_json(core)}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _copy_m6e(root: Path) -> None:
    destination = root / dates.M6E_REPORT_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / dates.M6E_REPORT_PATH, destination)


def _source(document_id: str) -> dict[str, object]:
    return {
        "document_id": document_id,
        "url": f"https://www.cmegroup.com/notices/{document_id}.html",
        "verified_claims": ["EXACT_PRODUCT_AND_EFFECTIVE_DATE_VERIFIED"],
    }


def _remaining_core() -> dict[str, object]:
    product_records = {
        "MES": ("ES", "MICRO_E_MINI_SP_500", "2019-05-05", "2019-05-06"),
        "MCL": ("CL", "MICRO_WTI_CRUDE_OIL", "2021-07-11", "2021-07-12"),
        "MGC": ("GC", "MICRO_GOLD", "2010-10-03", "2010-10-04"),
    }
    markets: dict[str, object] = {}
    for market, (parent, family, effective, first_trade) in product_records.items():
        markets[market] = {
            "parent_product": parent,
            "product_family": family,
            "product_listing_effective_date": effective,
            "first_trade_date": first_trade,
            "semantic_basis": (
                "CME_EFFECTIVE_AND_LISTING_DATE_WITH_SEPARATE_TRADE_DATE"
            ),
            "databento_mapping_role": (
                "AVAILABILITY_AND_CONTINUITY_ONLY_NOT_PRODUCT_LAUNCH"
            ),
            "databento_mapping_date_used_as_product_effective_date": False,
            "official_sources": [_source(f"{market.lower()}-source")],
        }
    return {
        "schema_version": dates.REMAINING_SCHEMA,
        "state": "PASS_OFFICIAL_PRIMARY_SOURCE_METADATA_ONLY",
        "source_authority": "CME Group",
        "source_domain": "www.cmegroup.com",
        "source_lookup": {
            "approved_maximum_network_requests": 18,
            "observed_network_requests": 9,
            "maximum_external_cost_usd": "0",
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
            "databento_calls": 0,
        },
        "markets": markets,
        "effects": {
            "market_data_downloaded": False,
            "dbn_accessed": False,
            "historical_rows_read": False,
            "catalog_activated": False,
            "published": False,
        },
    }


def _root_with_all_reports(tmp_path: Path) -> Path:
    _copy_m6e(tmp_path)
    _write_report(tmp_path / dates.REMAINING_REPORT_PATH, _remaining_core())
    return tmp_path


def test_sealed_m6e_official_source_report_validates() -> None:
    assert dates.load_m6e_product_effective_date(root=ROOT) == "2009-03-22"


def test_full_official_product_date_scope_validates(tmp_path: Path) -> None:
    root = _root_with_all_reports(tmp_path)
    assert dates.load_official_product_effective_dates(root=root) == {
        "M6E": "2009-03-22",
        "MCL": "2021-07-11",
        "MES": "2019-05-05",
        "MGC": "2010-10-03",
    }


def test_missing_remaining_report_fails_closed(tmp_path: Path) -> None:
    _copy_m6e(tmp_path)
    with pytest.raises(IntegrityError, match="unavailable or invalid"):
        dates.load_official_product_effective_dates(root=tmp_path)


def test_databento_mapping_promoted_to_launch_date_is_rejected(tmp_path: Path) -> None:
    _copy_m6e(tmp_path)
    core = _remaining_core()
    core["markets"]["MES"][  # type: ignore[index]
        "databento_mapping_date_used_as_product_effective_date"
    ] = True
    _write_report(tmp_path / dates.REMAINING_REPORT_PATH, core)
    with pytest.raises(IntegrityError, match="MES official product-date semantics"):
        dates.load_remaining_product_effective_dates(root=tmp_path)


def test_non_cme_source_host_is_rejected(tmp_path: Path) -> None:
    _copy_m6e(tmp_path)
    core = _remaining_core()
    core["markets"]["MCL"]["official_sources"][0][  # type: ignore[index]
        "url"
    ] = "https://example.com/mcl.html"
    _write_report(tmp_path / dates.REMAINING_REPORT_PATH, core)
    with pytest.raises(IntegrityError, match="exact official CME URL"):
        dates.load_remaining_product_effective_dates(root=tmp_path)


def test_lookup_request_ceiling_is_enforced(tmp_path: Path) -> None:
    _copy_m6e(tmp_path)
    core = _remaining_core()
    core["source_lookup"]["observed_network_requests"] = 19  # type: ignore[index]
    _write_report(tmp_path / dates.REMAINING_REPORT_PATH, core)
    with pytest.raises(IntegrityError, match="lookup authority or effects"):
        dates.load_remaining_product_effective_dates(root=tmp_path)


def test_extra_market_is_rejected(tmp_path: Path) -> None:
    _copy_m6e(tmp_path)
    core = _remaining_core()
    core["markets"]["MNQ"] = core["markets"]["MES"]  # type: ignore[index]
    _write_report(tmp_path / dates.REMAINING_REPORT_PATH, core)
    with pytest.raises(IntegrityError, match="evidence drifted"):
        dates.load_remaining_product_effective_dates(root=tmp_path)


def test_effective_date_after_first_trade_is_rejected(tmp_path: Path) -> None:
    _copy_m6e(tmp_path)
    core = _remaining_core()
    core["markets"]["MGC"][  # type: ignore[index]
        "product_listing_effective_date"
    ] = "2010-10-05"
    _write_report(tmp_path / dates.REMAINING_REPORT_PATH, core)
    with pytest.raises(IntegrityError, match="MGC official product-date semantics"):
        dates.load_remaining_product_effective_dates(root=tmp_path)


def test_report_identity_drift_is_rejected(tmp_path: Path) -> None:
    root = _root_with_all_reports(tmp_path)
    path = root / dates.REMAINING_REPORT_PATH
    report = json.loads(path.read_text(encoding="utf-8"))
    report["source_domain"] = "drifted.example"
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match="identity drifted"):
        dates.load_remaining_product_effective_dates(root=root)


def test_loader_has_no_network_or_data_surface() -> None:
    source = inspect.getsource(dates)
    assert "urllib.request" not in source
    assert "requests." not in source
    assert "import databento" not in source.lower()
    assert "from databento" not in source.lower()
    assert "data/dbn" not in source
    assert "dbn.decode" not in source.lower()
    assert "read_dbn" not in source.lower()
