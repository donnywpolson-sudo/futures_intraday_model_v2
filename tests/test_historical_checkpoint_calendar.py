from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.errors import IntegrityError
from futures_rebuild.historical_checkpoint_calendar import (
    CHECKPOINTS,
    END,
    MARKETS,
    POINTER_PATH,
    POINTER_SCHEMA,
    START,
    authoritative_sources,
    build_checkpoint_sessions,
    load_historical_checkpoint_calendar,
)


ROOT = Path(__file__).parents[1]


def _row(market: str, trade_date: str) -> dict[str, object]:
    return next(
        item
        for item in build_checkpoint_sessions()
        if item["market"] == market and item["trade_date"] == trade_date
    )


def test_authoritative_source_inventory_is_frozen_free_and_cme_authored() -> None:
    sources = authoritative_sources()
    assert len(sources) == 30
    assert len({item.key for item in sources}) == len(sources)
    assert {item.key[:4] for item in sources} == {"2018", "2019", "2020", "2021", "2022"}
    assert all("cmegroup.com" in item.original_url for item in sources)
    assert all(item.replay_url.startswith("https://web.archive.org/web/") for item in sources)
    assert all(item.suffix in {".xls", ".zip"} for item in sources)


def test_checkpoint_census_is_gapless_and_does_not_claim_full_sessions() -> None:
    rows = build_checkpoint_sessions()
    assert len(rows) == len(MARKETS) * 1826
    assert rows[0]["trade_date"] == START.isoformat()
    assert rows[-1]["trade_date"] == END.isoformat()
    assert len({(row["market"], row["trade_date"]) for row in rows}) == len(rows)
    assert all(set(row["checkpoint_open"]) == set(CHECKPOINTS) for row in rows)


def test_regular_weekend_full_close_and_early_close_states() -> None:
    assert _row("ES", "2022-03-15")["checkpoint_open"] == {
        "08:30": True, "10:30": True, "13:30": True,
    }
    assert _row("CL", "2022-03-13")["checkpoint_open"] == {
        "08:30": False, "10:30": False, "13:30": False,
    }
    assert _row("6E", "2021-12-24")["checkpoint_open"] == {
        "08:30": False, "10:30": False, "13:30": False,
    }
    assert _row("ZN", "2022-11-25")["checkpoint_open"] == {
        "08:30": True, "10:30": True, "13:30": False,
    }


def test_year_end_edge_cases_match_cme_workbooks() -> None:
    assert _row("ES", "2019-12-31")["checkpoint_open"]["13:30"] is True
    assert _row("ES", "2020-12-24")["checkpoint_open"]["13:30"] is False
    assert _row("ES", "2021-12-31")["checkpoint_open"]["13:30"] is True
    assert _row("ES", "2022-12-26")["checkpoint_open"]["08:30"] is False


def test_market_specific_cme_rows_override_generic_holiday_labels() -> None:
    assert _row("ES", "2018-07-03")["checkpoint_open"]["13:30"] is False
    assert _row("CL", "2018-07-03")["checkpoint_open"]["13:30"] is True
    assert _row("ES", "2021-04-02")["checkpoint_open"]["08:30"] is False
    assert _row("ZN", "2021-04-02")["checkpoint_open"] == {
        "08:30": True, "10:30": False, "13:30": False,
    }
    assert _row("6E", "2022-01-17")["checkpoint_open"]["13:30"] is True
    assert _row("CL", "2022-01-17")["checkpoint_open"]["13:30"] is False
    assert _row("6E", "2022-11-24")["checkpoint_open"]["13:30"] is True
    assert _row("6E", "2022-11-25")["checkpoint_open"]["13:30"] is False


def test_published_pointer_and_dependency_closure_verify_when_present(
    local_evidence_root: Path,
) -> None:
    pointer = local_evidence_root / POINTER_PATH
    if not pointer.exists():
        return
    loaded = load_historical_checkpoint_calendar(
        boundary=RepoBoundary(local_evidence_root.resolve()), pointer_path=pointer,
    )
    assert len(loaded.sessions) == len(MARKETS) * 1826
    assert loaded.index_receipt.release_kind == "historical_checkpoint_calendar_index"
    assert loaded.calendar_receipt.release_kind == "verified_historical_checkpoint_calendar"
    assert loaded.capture_receipt.release_kind == "cme_historical_checkpoint_calendar_capture"


def test_missing_or_unbound_calendar_pointer_fails_closed(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path.resolve())
    pointer = tmp_path / POINTER_PATH
    with pytest.raises(IntegrityError):
        load_historical_checkpoint_calendar(boundary=boundary, pointer_path=pointer)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_bytes(
        canonical_bytes(
            {
                "calendar_index_receipt": {"release_id": "0" * 64},
                "schema_version": POINTER_SCHEMA,
            }
        )
        + b"\n"
    )
    with pytest.raises(IntegrityError, match="calendar index receipt"):
        load_historical_checkpoint_calendar(boundary=boundary, pointer_path=pointer)
