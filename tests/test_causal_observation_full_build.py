from __future__ import annotations

import json
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_observation_canary import (
    DecodedMarket,
    _build_market_candidate_with_state,
)
from futures_rebuild.causal_observation_foundation import (
    ACTIVE_CANONICAL_RELEASE_ID,
    ACTIVE_SOURCE_CONTRACT_ID,
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_SHA256,
    authorize_full_build_row_read,
    issue_synthetic_observation_context,
    required_full_build_scope,
)
from futures_rebuild.causal_observation_full_build import (
    DEFERRED_MICRO_COUNT,
    EXPECTED_DBN_COUNT,
    EXPECTED_ENTRY_COUNT,
    EXPECTED_PAYLOAD_BYTES,
    EXPECTED_PRIMARY_1M_DBN_COUNT,
    EXPECTED_SIDECAR_COUNT,
    EXPECTED_SOURCE_BYTES,
    EXPECTED_WORK_UNIT_COUNT,
    MAXIMUM_OUTPUT_BYTES,
    MAXIMUM_PARTITION_COUNT,
    MAXIMUM_PEAK_ADDITIONAL_BYTES,
    MAXIMUM_RUNTIME_SECONDS,
    MINIMUM_FREE_AFTER_PEAK_BYTES,
    PLAN_SCHEMA,
    STANDARD_ROOT_COUNT,
    _month_windows,
    _slice_decoded,
    run_authorized_full_build,
    validate_complete_development_boundary_metadata,
    validate_reusable_partition,
)
from futures_rebuild.causal_observation_parquet import read_bundle
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.foundation.records import ProviderBar, ProviderDefinition
from futures_rebuild.foundation.economics import EconomicsRuleBook
from futures_rebuild.research_gateway_policy import (
    CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
    PREPARATORY_REAL_HISTORY_OPERATIONS,
)


ROOT = Path(__file__).resolve().parents[1]
H = "a" * 64
RULEBOOK = EconomicsRuleBook.from_file(ROOT / "configs/contract_economics_rules.json")
def _plan(*, inventory_path: str = "inventory.json", inventory_sha256: str = H) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "operation": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": ACTIVE_SOURCE_CONTRACT_ID,
            "canonical_release_id": ACTIVE_CANONICAL_RELEASE_ID,
            "inventory_path": inventory_path,
            "inventory_sha256": inventory_sha256,
            "exact_source_entries_sha256": "b" * 64,
            "exact_dbn_entries_sha256": "c" * 64,
            "exact_source_entry_count": EXPECTED_ENTRY_COUNT,
            "exact_dbn_file_count": EXPECTED_DBN_COUNT,
            "exact_sidecar_file_count": EXPECTED_SIDECAR_COUNT,
            "total_source_bytes": EXPECTED_SOURCE_BYTES,
            "maximum_payload_bytes": EXPECTED_PAYLOAD_BYTES,
            "primary_1m_dbn_count": EXPECTED_PRIMARY_1M_DBN_COUNT,
            "work_unit_count": EXPECTED_WORK_UNIT_COUNT,
            "standard_root_count": STANDARD_ROOT_COUNT,
            "deferred_micro_count": DEFERRED_MICRO_COUNT,
            "minimum_year": 2010,
            "maximum_year": 2025,
        },
        "output_staging_path": "state/data_publication_staging/full-build-exact",
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "authority": {
            "activation": False,
            "evaluation": False,
            "features": False,
            "fitting": False,
            "forward": False,
            "holdout": False,
            "mechanism": False,
            "outcomes": False,
            "prediction": False,
            "provider": False,
            "publication": False,
            "wfa": False,
        },
        "limits": {
            "maximum_payload_bytes": EXPECTED_PAYLOAD_BYTES,
            "maximum_decoded_records": 1_000_000_000,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_partition_count": MAXIMUM_PARTITION_COUNT,
            "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
        },
        "execution": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
        },
        "storage": {
            "required_free_after_peak_bytes": MINIMUM_FREE_AFTER_PEAK_BYTES,
            "publication_authorized": False,
            "activation_authorized": False,
            "partitioning": "market/year/month",
            "empty_partitions": False,
            "full_1s_duplication": False,
        },
        "economics": {
            "rulebook_path": "configs/contract_economics_rules.json",
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "reuse_canary_candidates": False,
        "reuse_prior_partitions": False,
    }
    return {**core, "plan_id": sha256_json(core)}


def _receipt(tmp_path: Path, plan: dict[str, object], plan_sha256: str) -> OperationReceipt:
    boundary = RepoBoundary(tmp_path)
    required = required_full_build_scope(
        plan=plan,
        plan_sha256=plan_sha256,
        source_contract_id=ACTIVE_SOURCE_CONTRACT_ID,
        canonical_release_id=ACTIVE_CANONICAL_RELEASE_ID,
    )
    scope = {key: value for key, value in required.items() if not key.startswith("approval_")}
    return OperationReceipt.issue_user_approved(
        boundary,
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha256,
        approval_line=_personal_approval_line(
            CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
            str(plan["plan_id"]),
            plan_sha256,
        ),
    )


def _ns(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1_000_000_000)


def _definition(*, instrument: int, received: str, symbol: str, ordinal: int) -> ProviderDefinition:
    received_ns = _ns(received)
    return ProviderDefinition(
        dataset="GLBX.MDP3",
        market="ES",
        publisher_id=1,
        instrument_id=instrument,
        instrument_id_date_utc=received[:10],
        ts_event_ns=received_ns - 2_000_000_000,
        ts_recv_ns=received_ns,
        activation_ns=received_ns - 86_400_000_000_000,
        expiration_ns=received_ns + 365 * 86_400_000_000_000,
        security_update_action="ADD",
        instrument_class="FUTURE",
        security_type="FUT",
        raw_symbol=symbol,
        exchange="XCME",
        currency="USD",
        min_price_increment_nano=250_000_000,
        unit_of_measure_qty_nano=50_000_000_000,
        unit_of_measure="USD",
        source_release_id=H,
        source_manifest_sha256=H,
        source_file_path="synthetic/definition.dbn.zst",
        source_file_sha256=H,
        row_ordinal=ordinal,
        row_sha256=str(ordinal + 1) * 64,
    )


def _bar(*, instrument: int, event: str, digit: str, price: int) -> ProviderBar:
    return ProviderBar(
        dataset="GLBX.MDP3",
        market="ES",
        publisher_id=1,
        instrument_id=instrument,
        event_at_ns=_ns(event),
        open_nano=price,
        high_nano=price + 20,
        low_nano=price - 20,
        close_nano=price + 10,
        volume=10,
        source_release_id=H,
        source_manifest_sha256=H,
        source_file_path="synthetic/ohlcv_1m.dbn.zst",
        source_file_sha256=H,
        row_sha256=digit * 64,
    )


class _Publisher:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.count = 0

    def create_stage(self, purpose: str) -> Path:
        assert purpose == "causal_observation"
        self.count += 1
        stage = self.root / f"candidate-{self.count}"
        stage.mkdir(parents=True)
        return stage


def test_full_build_bounds_include_bounded_2025_while_active_contract_stays_blocked() -> None:
    source = json.loads((ROOT / "configs/source_contract.json").read_text(encoding="utf-8"))
    assert source["contract_id"] == ACTIVE_SOURCE_CONTRACT_ID
    assert source["active_canonical_source"]["release_id"] == ACTIVE_CANONICAL_RELEASE_ID
    assert len(source["universe"]["standard_roots"]) == STANDARD_ROOT_COUNT
    assert len(source["universe"]["deferred_micro_roots"]) == DEFERRED_MICRO_COUNT
    assert set(source["universe"]["standard_roots"]).isdisjoint(
        source["universe"]["deferred_micro_roots"]
    )
    assert (EXPECTED_ENTRY_COUNT, EXPECTED_DBN_COUNT, EXPECTED_SIDECAR_COUNT) == (
        8_506,
        4_253,
        4_253,
    )
    assert (EXPECTED_SOURCE_BYTES, EXPECTED_PAYLOAD_BYTES, EXPECTED_PRIMARY_1M_DBN_COUNT) == (
        17_123_147_852,
        17_119_024_382,
        617,
    )
    assert EXPECTED_WORK_UNIT_COUNT == 609
    assert source["selection_policy"]["admitted_standard_dbn_file_count"] == 3_966
    assert source["selection_policy"]["admitted_standard_dbn_file_count"] != EXPECTED_DBN_COUNT


def test_full_build_operation_is_nonpublic_and_receipt_is_one_use(tmp_path: Path) -> None:
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "source_contract.json").write_bytes(
        (ROOT / "configs/source_contract.json").read_bytes()
    )
    plan = _plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n", encoding="utf-8")
    plan_sha = sha256_file(plan_path)
    receipt = _receipt(tmp_path, plan, plan_sha)
    context = authorize_full_build_row_read(
        boundary=RepoBoundary(tmp_path),
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha,
    )
    assert context.operation == CAUSAL_OBSERVATION_FULL_BUILD_OPERATION
    with pytest.raises(UnauthorizedOperation):
        authorize_full_build_row_read(
            boundary=RepoBoundary(tmp_path),
            receipt=receipt,
            plan=plan,
            plan_sha256=plan_sha,
        )
    assert CAUSAL_OBSERVATION_FULL_BUILD_OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]
    assert all("causal_observation_full_build" not in str(target) for target in scripts.values())


@pytest.mark.parametrize(
    ("first_window", "second_window", "first_definition", "first_event", "second_definition", "second_event"),
    (
        (
            {"start": "2024-01-01T00:00:00Z", "end": "2024-02-01T00:00:00Z"},
            {"start": "2024-02-01T00:00:00Z", "end": "2024-03-01T00:00:00Z"},
            "2024-01-31T23:00:00Z",
            "2024-01-31T23:58:00Z",
            "2024-02-01T00:00:00Z",
            "2024-02-01T00:02:00Z",
        ),
        (
            {"start": "2024-12-01T00:00:00Z", "end": "2025-01-01T00:00:00Z"},
            {"start": "2025-01-01T00:00:00Z", "end": "2025-02-01T00:00:00Z"},
            "2024-12-31T23:00:00Z",
            "2024-12-31T23:58:00Z",
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:02:00Z",
        ),
    ),
)
def test_month_partitioning_and_cross_boundary_roll_gap_state_are_deterministic(
    tmp_path: Path,
    first_window: dict[str, str],
    second_window: dict[str, str],
    first_definition: str,
    first_event: str,
    second_definition: str,
    second_event: str,
) -> None:
    assert len(
        _month_windows("2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z")
    ) == 12
    context = issue_synthetic_observation_context(
        boundary=RepoBoundary(tmp_path), fixture_id="9" * 64
    )
    publisher = _Publisher(tmp_path)
    first = _build_market_candidate_with_state(
        publisher=publisher,
        context=context,
        market="ES",
        window=first_window,
        decoded=DecodedMarket(
            definitions=(
                _definition(instrument=2, received=first_definition, symbol="ESH4", ordinal=0),
            ),
            primary_1m=(
                _bar(instrument=2, event=first_event, digit="2", price=-100),
            ),
            reference_1s={},
            reference_1h={},
            reference_1d={},
            support_rows=(),
            decoded_record_count=2,
        ),
        allowed_roots=frozenset({"ES"}),
        economics_rulebook=RULEBOOK,
    )
    second = _build_market_candidate_with_state(
        publisher=publisher,
        context=context,
        market="ES",
        window=second_window,
        decoded=DecodedMarket(
            definitions=(
                _definition(instrument=3, received=second_definition, symbol="ESM4", ordinal=1),
            ),
            primary_1m=(
                _bar(instrument=3, event=second_event, digit="3", price=100),
            ),
            reference_1s={},
            reference_1h={},
            reference_1d={},
            support_rows=(),
            decoded_record_count=2,
        ),
        allowed_roots=frozenset({"ES"}),
        economics_rulebook=RULEBOOK,
        prior_observation=first.last_observation,
    )
    tables = read_bundle(second.prepared.stage / "candidate")
    rolls = tables["roll"]
    gaps = tables["missingness"]
    assert rolls[0]["roll_flag"] is True
    assert rolls[0]["price_discontinuity_flag"] is True
    assert rolls[0]["crossing_status"] == "ROLL_BOUNDARY_UNADJUSTED"
    assert sum(row["state"] == "UNKNOWN_FAIL_CLOSED" for row in gaps) == 1
    assert second.first_observation["bar_start_ns"] == _ns(second_event)


def test_slice_keeps_only_partition_rows_and_aggregated_references() -> None:
    start = _ns("2024-01-01T00:00:00Z")
    end = _ns("2024-02-01T00:00:00Z")
    decoded = DecodedMarket(
        definitions=(),
        primary_1m=(
            _bar(instrument=2, event="2024-01-31T23:59:00Z", digit="4", price=10),
            _bar(instrument=2, event="2024-02-01T00:00:00Z", digit="5", price=10),
        ),
        reference_1s={start: {"open_nano": 1}, end: {"open_nano": 2}},
        reference_1h={},
        reference_1d={},
        support_rows=((start, "status", H),),
        decoded_record_count=4,
    )
    sliced = _slice_decoded(
        decoded,
        start_ns=start,
        end_ns=end,
        definitions=(),
        carried_support=(),
    )
    assert len(sliced.primary_1m) == 1
    assert tuple(sliced.reference_1s) == (start,)
    assert sliced.support_rows == ((start, "status", H),)


def test_output_and_lock_fail_before_authorization_or_payload_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = _plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n", encoding="utf-8")
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "source_contract.json").write_text(
        json.dumps({"universe": {"standard_roots": ["ES"]}}) + "\n",
        encoding="utf-8",
    )
    calls: list[str] = []
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build._validate_plan", lambda *_: None)
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build._load_exact_source_entries", lambda *_: ())
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_full_build.validate_complete_development_boundary_metadata",
        lambda *_, **__: {},
    )
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build._load_economics_rulebook", lambda *_: RULEBOOK)
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build.issue_current_source_closure_context", lambda *_: object())
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build.select_exact_standard_source_entries", lambda *_, **__: ())
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build.authorize_full_build_row_read", lambda **_: calls.append("authorized"))
    monkeypatch.setattr("futures_rebuild.causal_observation_full_build.shutil.disk_usage", lambda *_: SimpleNamespace(free=10**15))
    output = tmp_path / str(plan["output_staging_path"])
    output.mkdir(parents=True)
    with pytest.raises(IntegrityError, match="already exists"):
        run_authorized_full_build(
            repository_root=tmp_path,
            receipt=object(),  # type: ignore[arg-type]
            plan_path=plan_path,
        )
    assert calls == []
    output.rmdir()
    lock = tmp_path / "state/locks/foundation-build.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("active", encoding="utf-8")
    with pytest.raises(UnauthorizedOperation, match="lock"):
        run_authorized_full_build(
            repository_root=tmp_path,
            receipt=object(),  # type: ignore[arg-type]
            plan_path=plan_path,
        )
    assert calls == []


def test_2025_boundary_metadata_is_exact_and_crossing_files_fail_closed() -> None:
    contract = json.loads((ROOT / "configs/source_contract.json").read_text())
    inventory = json.loads((ROOT / contract["complete_inventory"]["path"]).read_text())
    entries = [
        dict(entry)
        for entry in inventory["entries"]
        if entry.get("admitted_standard_foundation") is True
        and entry.get("market") == "ES"
        and entry.get("year") == 2025
    ]
    result = validate_complete_development_boundary_metadata(
        ROOT, entries, standard_roots=frozenset({"ES"})
    )
    assert result["boundary_dbn_count"] == 7
    assert result["boundary_sidecar_count"] == 7
    crossing = [dict(entry) for entry in entries]
    crossing[0]["interval_end_exclusive"] = "2026-01-01T00:00:00Z"
    with pytest.raises(UnauthorizedOperation, match="development boundary"):
        validate_complete_development_boundary_metadata(
            ROOT, crossing, standard_roots=frozenset({"ES"})
        )
    missing = entries[:-2]
    with pytest.raises(UnauthorizedOperation, match="omits exact"):
        validate_complete_development_boundary_metadata(
            ROOT, missing, standard_roots=frozenset({"ES"})
        )

    july = _month_windows(
        "2025-01-01T00:00:00Z", "2025-07-13T22:00:00Z"
    )
    assert len(july) == 7
    assert july[-1][3]["end"] == "2025-07-13T22:00:00Z"
    assert july[-1][2] == "2025-07-01_2025-07-13T220000Z"


def test_partition_reuse_requires_independent_exact_release_identity(tmp_path: Path) -> None:
    context = issue_synthetic_observation_context(
        boundary=RepoBoundary(tmp_path), fixture_id="8" * 64
    )
    built = _build_market_candidate_with_state(
        publisher=_Publisher(tmp_path),
        context=context,
        market="ES",
        window={"start": "2024-01-01T00:00:00Z", "end": "2024-02-01T00:00:00Z"},
        decoded=DecodedMarket(
            definitions=(
                _definition(instrument=2, received="2024-01-02T00:00:00Z", symbol="ESH4", ordinal=0),
            ),
            primary_1m=(
                _bar(instrument=2, event="2024-01-02T00:01:00Z", digit="6", price=-50),
            ),
            reference_1s={},
            reference_1h={},
            reference_1d={},
            support_rows=(),
            decoded_record_count=2,
        ),
        allowed_roots=frozenset({"ES"}),
        economics_rulebook=RULEBOOK,
    )
    certificate = validate_reusable_partition(
        stage=built.prepared.stage,
        manifest=built.prepared.manifest,
        expected_release_id=built.prepared.manifest.release_id,
        economics_rulebook=RULEBOOK,
    )
    assert certificate["release_id"] == built.prepared.manifest.release_id
    with pytest.raises(IntegrityError, match="identity differs"):
        validate_reusable_partition(
            stage=built.prepared.stage,
            manifest=built.prepared.manifest,
            expected_release_id="f" * 64,
            economics_rulebook=RULEBOOK,
        )
