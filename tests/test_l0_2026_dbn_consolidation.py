from __future__ import annotations

from pathlib import Path
import pytest

from futures_rebuild import l0_2026_dbn_consolidation as consolidation
from futures_rebuild.errors import IntegrityError


def test_stable_union_preserves_first_appearance() -> None:
    assert consolidation._stable_union(["a", "b"], ["b", "c"]) == ["a", "b", "c"]


def test_mapping_merge_preserves_order_and_rejects_overlap() -> None:
    one = {"ES.v.0": [{"start_date": 1, "end_date": 2, "symbol": "A"}]}
    two = {"ES.v.0": [{"start_date": 2, "end_date": 3, "symbol": "A"}]}
    merged = consolidation._merged_mappings(one, two)
    assert [(row.start_date, row.end_date) for row in merged[0].intervals] == [(1, 2), (2, 3)]
    with pytest.raises(IntegrityError, match="overlap"):
        consolidation._merged_mappings(one, {"ES.v.0": [{"start_date": 1, "end_date": 3, "symbol": "B"}]})

    repeated = consolidation._merged_mappings(
        one, {"ES.v.0": [{"start_date": 1, "end_date": 3, "symbol": "A"}]}
    )
    assert [(row.start_date, row.end_date) for row in repeated[0].intervals] == [(1, 3)]


def test_metadata_gate_allows_v1_v3_but_rejects_identity_drift() -> None:
    base = {
        "dataset": "GLBX.MDP3", "start": 1_767_225_600_000_000_000,
        "end": 1_783_987_200_000_000_000, "stype_in": "continuous",
        "stype_out": "instrument_id", "schema": "ohlcv-1s", "symbols": ["ES.v.0"],
        "ts_out": False, "limit": None, "version": 1,
    }
    latter = {**base, "start": base["end"], "end": 1_788_220_800_000_000_000, "version": 3}
    consolidation._require_compatible(base, latter)
    with pytest.raises(IntegrityError, match="schema"):
        consolidation._require_compatible(base, {**latter, "schema": "ohlcv-1m"})

    with pytest.raises(IntegrityError, match="contiguous"):
        consolidation._require_compatible(base, {**latter, "start": base["end"] + 1})
    consolidation._require_compatible(
        base,
        {**latter, "start": base["end"] + 1},
        require_contiguous=False,
    )


def test_publication_scope_is_exactly_nonresearch() -> None:
    scope = consolidation.publication_scope(
        {"run_id": "run", "successor_release_id": "a" * 64, "publication_plan_id": "b" * 64},
        "c" * 64,
    )
    assert scope["successor_file_count"] == "11474"
    assert scope["research_activation"] == "false"
    assert scope["archived_source_artifact_count"] == "1624"


def test_long_archive_path_uses_windows_safe_io(tmp_path: Path) -> None:
    path = tmp_path.joinpath(*(["segment_1234567890"] * 20), "artifact.dbn.zst")
    consolidation.io_path(path).parent.mkdir(parents=True)
    consolidation.io_path(path).write_bytes(b"dbn")
    assert consolidation._exists(path)
    assert consolidation._stat(path).st_size == 3


def test_active_publication_is_a_verifying_noop(tmp_path: Path, monkeypatch) -> None:
    plan = {"successor_release_id": "release"}
    plan_path = tmp_path / "plan.json"
    plan_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(consolidation, "_load_publication_plan", lambda *_args, **_kwargs: plan)
    monkeypatch.setattr(consolidation, "_json", lambda *_args: {"release_id": "release"})
    monkeypatch.setattr(consolidation, "verify_publication", lambda *_args: {"status": "PASS"})
    result = consolidation.publish_authorized(tmp_path, plan_path, object())
    assert result == {
        "status": "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE",
        "verification": {"status": "PASS"},
    }


def test_stream_rejects_nonmonotonic_records(monkeypatch, tmp_path: Path) -> None:
    class Store:
        schema = "ohlcv-1s"
        def __iter__(self):
            return iter([_Record(2), _Record(1)])

    class _Record:
        def __init__(self, ts: int): self.ts_event = ts
        def __bytes__(self): return self.ts_event.to_bytes(8, "little")

    monkeypatch.setattr(consolidation.databento.DBNStore, "from_file", lambda _path: Store())
    with pytest.raises(IntegrityError, match="nondecreasing"):
        consolidation._stream_records(tmp_path / "x.dbn.zst")


def test_bbo_stream_uses_receive_time_for_ordering(monkeypatch, tmp_path: Path) -> None:
    class Store:
        schema = "bbo-1s"
        def __iter__(self):
            return iter([_Record(2, 1), _Record(1, 2)])

    class _Record:
        def __init__(self, ts_event: int, ts_recv: int):
            self.ts_event = ts_event
            self.ts_recv = ts_recv

        def __bytes__(self):
            return self.ts_event.to_bytes(8, "little") + self.ts_recv.to_bytes(8, "little")

    monkeypatch.setattr(consolidation.databento.DBNStore, "from_file", lambda _path: Store())
    result = consolidation._stream_records(tmp_path / "x.dbn.zst")
    assert (result["first_timestamp_ns"], result["last_timestamp_ns"]) == (1, 2)


def test_record_boundary_rejects_new_cross_shard_duplicate() -> None:
    prior = {
        "last_timestamp_ns": 2,
        "last_record_sha256": "same",
    }
    with pytest.raises(IntegrityError, match="duplicate"):
        consolidation._require_record_boundary(
            prior,
            {"first_timestamp_ns": 2, "first_record_sha256": "same"},
        )
    with pytest.raises(IntegrityError, match="out of order"):
        consolidation._require_record_boundary(
            prior,
            {"first_timestamp_ns": 1, "first_record_sha256": "different"},
        )
