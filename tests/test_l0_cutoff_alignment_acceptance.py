from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import databento_dbn as dbn
import pytest
import zstandard

import futures_rebuild.l0_cutoff_alignment_acceptance as acceptance
from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def _entry() -> dict[str, object]:
    return {
        "query": {
            "schema": "ohlcv-1m",
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "symbols": ["ES.v.0"],
        }
    }


def _write_ohlcv(
    path: Path,
    timestamps: list[int],
    *,
    metadata_start: int = acceptance.START_NS,
    metadata_end: int = acceptance.END_NS,
) -> None:
    metadata = dbn.Metadata(
        "GLBX.MDP3",
        metadata_start,
        dbn.SType.CONTINUOUS,
        dbn.SType.INSTRUMENT_ID,
        dbn.Schema.OHLCV_1M,
        symbols=["ES.v.0"],
        end=metadata_end,
    )
    records = [
        dbn.OHLCVMsg(
            dbn.RType.OHLCV_1M,
            1,
            100 + index,
            timestamp,
            1_000_000_000,
            1_100_000_000,
            900_000_000,
            1_050_000_000,
            10,
        )
        for index, timestamp in enumerate(timestamps)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        zstandard.ZstdCompressor().compress(
            metadata.encode() + b"".join(bytes(record) for record in records)
        )
    )


def test_scan_dbn_reaches_eof_and_rejects_corruption_metadata_and_time(tmp_path: Path) -> None:
    valid = tmp_path / "valid.dbn.zst"
    timestamps = [acceptance.START_NS + 60_000_000_000 * index for index in range(3)]
    _write_ohlcv(valid, timestamps)
    result = acceptance._scan_dbn(valid, _entry())
    assert result["record_count"] == 3
    assert result["coverage_timestamp_field"] == "ts_event"

    wrong_metadata = tmp_path / "wrong-metadata.dbn.zst"
    _write_ohlcv(wrong_metadata, timestamps, metadata_start=acceptance.START_NS + 1)
    with pytest.raises(IntegrityError, match="metadata differs"):
        acceptance._scan_dbn(wrong_metadata, _entry())

    out_of_range = tmp_path / "out-of-range.dbn.zst"
    _write_ohlcv(out_of_range, [acceptance.START_NS - 1])
    with pytest.raises(IntegrityError, match="outside"):
        acceptance._scan_dbn(out_of_range, _entry())

    nonmonotonic = tmp_path / "nonmonotonic.dbn.zst"
    _write_ohlcv(nonmonotonic, [acceptance.START_NS + 2, acceptance.START_NS + 1])
    with pytest.raises(IntegrityError, match="nondecreasing"):
        acceptance._scan_dbn(nonmonotonic, _entry())

    truncated = tmp_path / "truncated.dbn.zst"
    truncated.write_bytes(valid.read_bytes()[:-8])
    with pytest.raises(Exception):
        acceptance._scan_dbn(truncated, _entry())


def test_date_warning_partial_collision_and_space_gates(tmp_path: Path, monkeypatch) -> None:
    results = [
        {
            "market": "ES",
            "family": family,
            "utc_date_counts": {"2026-07-14": 1},
            "project_trade_date_counts": {"2026-07-14": 1},
        }
        for family in acceptance.OHLCV_FAMILIES
    ]
    assert acceptance._validate_ohlcv_dates(results) == []
    results[-1]["utc_date_counts"] = {"2026-07-15": 1}
    assert (
        acceptance._validate_ohlcv_dates(results)[0]["reason"]
        == "utc_observation_date_set_mismatch"
    )

    assert acceptance._warning_count(
        {"provider_warnings": [{"count": 1} for _ in range(406)]}
    ) == 406
    partial_root = tmp_path / acceptance.ACQUISITION_ROOT / "raw" / "ohlcv_1s"
    partial_root.mkdir(parents=True)
    for index in range(25):
        (partial_root / f"file.dbn.zst.p.{index:08x}").write_bytes(b"partial")
    assert len(acceptance._partial_inventory(tmp_path)) == 25

    source, destination = tmp_path / "source", tmp_path / "destination"
    source.write_bytes(b"source")
    destination.write_bytes(b"collision")
    with pytest.raises(IntegrityError, match="destination exists"):
        acceptance._copy_plain(source, destination)

    monkeypatch.setattr(
        acceptance.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=999_999_999),
    )
    with pytest.raises(IntegrityError, match="one gigabyte"):
        acceptance._require_publication_space(tmp_path)


def test_new_inventory_entries_are_nonresearch_dbn_sidecar_pairs(tmp_path: Path) -> None:
    payload = tmp_path / "payload"
    dbn_path = "data/dbn/ohlcv_1s/ES/2026/file.dbn.zst"
    sidecar_path = f"{dbn_path}.manifest.json"
    for relative, content in ((dbn_path, b"dbn"), (sidecar_path, b"sidecar")):
        target = payload / Path(relative).relative_to(acceptance.DBN_ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    rows = acceptance._new_inventory_entries(
        {
            "requests": [
                {
                    "destination_dbn": dbn_path,
                    "destination_sidecar": sidecar_path,
                    "market": "ES",
                    "request_id": "request",
                }
            ]
        },
        {"results": [{"request_id": "request", "record_count": 1}]},
        payload,
    )
    assert [row["kind"] for row in rows] == ["DBN", "SIDECAR"]
    assert all(row["admitted_standard_foundation"] is False for row in rows)
    assert all(row["lane"] == "STANDARD_41" for row in rows)


def test_publication_plan_can_be_verified_after_candidate_move(tmp_path: Path) -> None:
    core: dict[str, object] = {
        "candidate_payload_root": "missing-candidate",
        "candidate_summary": {"file_count": 0, "total_bytes": 0, "inventory_sha256": "x"},
    }
    for name in (
        "release_manifest",
        "inventory_candidate",
        "wrapper_candidate",
        "pointer_candidate",
        "source_contract_candidate",
        "rollback_plan",
    ):
        path = tmp_path / f"{name}.json"
        path.write_bytes(canonical_bytes({"name": name}) + b"\n")
        core[f"{name}_path"] = path.name
        core[f"{name}_sha256"] = sha256_file(path)
    plan = {**core, "publication_plan_id": sha256_json(core)}
    path = tmp_path / "publication_plan.json"
    path.write_bytes(canonical_bytes(plan) + b"\n")
    loaded = acceptance._load_publication_plan(
        tmp_path, path, candidate_required=False
    )
    assert loaded["publication_plan_id"] == plan["publication_plan_id"]


def test_local_receipt_cannot_authorize_forward_row_audit() -> None:
    receipt = OperationReceipt.issue_local(
        RepoBoundary(ROOT),
        operation="SYNTHETIC_ONLY",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    scope = acceptance.audit_scope(
        {"receipt_id": "r", "plan_id": "a" * 64, "counts": {"requests": 406}},
        "b" * 64,
    )
    with pytest.raises(UnauthorizedOperation):
        receipt.verify(
            RepoBoundary(ROOT),
            operation=acceptance.AUDIT_OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )


class _Authorization:
    receipt_id = "receipt"

    def __init__(self) -> None:
        self.consumed = False

    def consume(self, *_args, **_kwargs) -> None:
        self.consumed = True

    def assert_consumed(self, *_args, **_kwargs) -> None:
        assert self.consumed


def _publication_fixture(root: Path) -> tuple[Path, dict[str, object]]:
    controls = root / "configs"
    controls.mkdir(parents=True)
    pointer = controls / "active_dbn_congruence_release_v1.json"
    source = controls / "source_contract.json"
    predecessor = controls / "source_contract_v4.json"
    pointer.write_bytes(canonical_bytes({"release_id": "old"}) + b"\n")
    source.write_bytes(canonical_bytes({"contract": "old"}) + b"\n")
    predecessor.write_bytes(canonical_bytes({"version": 4}) + b"\n")

    report = root / "report"
    report.mkdir()
    publication_path = report / "publication_plan.json"
    publication_path.write_bytes(b"{}\n")
    candidate_dbn = root / "candidate" / "file.dbn.zst"
    candidate_sidecar = Path(f"{candidate_dbn}.manifest.json")
    candidate_dbn.parent.mkdir()
    candidate_dbn.write_bytes(b"dbn")
    candidate_sidecar.write_bytes(b"sidecar")
    destination_dbn = root / "data/dbn/ohlcv_1s/ES/2026/file.dbn.zst"
    destination_dbn.parent.mkdir(parents=True)
    destination_sidecar = Path(f"{destination_dbn}.manifest.json")

    for name, payload in (
        ("wrapper.json", {"wrapper": 1}),
        ("inventory.json", {"inventory": 1}),
        ("successor-source.json", {"contract": "new"}),
        ("successor-pointer.json", {"release_id": "new"}),
    ):
        (report / name).write_bytes(canonical_bytes(payload) + b"\n")
    plan: dict[str, object] = {
        "run_id": "run",
        "successor_release_id": "new",
        "publication_plan_id": "a" * 64,
        "prior_active_pointer_sha256": sha256_file(pointer),
        "prior_source_contract_sha256": sha256_file(source),
        "predecessor_source_contract_sha256": sha256_file(predecessor),
        "wrapper_candidate_path": "report/wrapper.json",
        "inventory_candidate_path": "report/inventory.json",
        "source_contract_candidate_path": "report/successor-source.json",
        "pointer_candidate_path": "report/successor-pointer.json",
        "targets": [
            {
                "candidate_dbn": candidate_dbn.relative_to(root).as_posix(),
                "candidate_sidecar": candidate_sidecar.relative_to(root).as_posix(),
                "destination_dbn": destination_dbn.relative_to(root).as_posix(),
                "destination_sidecar": destination_sidecar.relative_to(root).as_posix(),
                "dbn_sha256": sha256_file(candidate_dbn),
                "sidecar_sha256": sha256_file(candidate_sidecar),
            }
        ],
    }
    return publication_path, plan


def test_publication_rolls_back_an_injected_failure(tmp_path: Path, monkeypatch) -> None:
    publication_path, plan = _publication_fixture(tmp_path)
    prior_pointer = (tmp_path / acceptance.ACTIVE_POINTER).read_bytes()
    prior_source = (tmp_path / acceptance.ACTIVE_SOURCE_CONTRACT).read_bytes()
    monkeypatch.setattr(
        acceptance, "_load_publication_plan", lambda *_args, **_kwargs: plan
    )
    monkeypatch.setattr(acceptance, "verify_publication", lambda *_args: {"status": "PASS"})
    authorization = _Authorization()
    with pytest.raises(IntegrityError, match="after pointer"):
        acceptance.publish_authorized(
            tmp_path, publication_path, authorization, failpoint="after_pointer_replace"
        )
    target = plan["targets"][0]
    assert (tmp_path / target["candidate_dbn"]).is_file()
    assert not (tmp_path / target["destination_dbn"]).exists()
    assert (tmp_path / acceptance.ACTIVE_POINTER).read_bytes() == prior_pointer
    assert (tmp_path / acceptance.ACTIVE_SOURCE_CONTRACT).read_bytes() == prior_source
    assert (publication_path.parent / "publication_failure.json").is_file()


def test_publication_second_invocation_is_an_idempotent_noop(tmp_path: Path, monkeypatch) -> None:
    publication_path, plan = _publication_fixture(tmp_path)
    monkeypatch.setattr(
        acceptance, "_load_publication_plan", lambda *_args, **_kwargs: plan
    )
    monkeypatch.setattr(acceptance, "verify_publication", lambda *_args: {"status": "PASS"})
    authorization = _Authorization()
    result = acceptance.publish_authorized(tmp_path, publication_path, authorization)
    assert result["status"] == "ACTIVATED_CANONICAL_CUSTODY_AND_VERIFIED"
    second = acceptance.publish_authorized(tmp_path, publication_path, authorization)
    assert second == {
        "status": "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE",
        "verification": {"status": "PASS"},
    }
