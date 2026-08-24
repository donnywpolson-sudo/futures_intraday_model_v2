from __future__ import annotations

import json
import hashlib
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import pytest
import databento_dbn as dbn

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.causal_observation_canary import (
    _CanaryStageCreator,
    DecodedMarket,
    _authorize_then_decode,
    _causal_definition,
    _decode_selected_sources,
    _load_economics_rulebook,
    _query_contract,
    _validate_plan,
    _resolve_multiplier,
    build_market_candidate,
    MultiplierResolutionError,
)
from futures_rebuild.causal_observation_foundation import (
    CANARY_CANONICAL_RELEASE_ID,
    CANARY_SOURCE_CONTRACT_ID,
    CAUSAL_OBSERVATION_CONTRACT_ID,
    issue_synthetic_observation_context,
    required_canary_scope,
)
from futures_rebuild.causal_observation_parquet import read_bundle
from futures_rebuild.causal_observation_verifier import verify_observation_candidate
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.foundation.records import INT32_NULL, INT64_NULL, ProviderBar, ProviderDefinition
from futures_rebuild.foundation.economics import EconomicsRuleBook
from futures_rebuild.foundation import decoder as foundation_decoder
from futures_rebuild.research_gateway_policy import CAUSAL_OBSERVATION_CANARY_OPERATION


H = "a" * 64
ROOT = Path(__file__).resolve().parents[1]
RULEBOOK = EconomicsRuleBook.from_file(ROOT / "configs/contract_economics_rules.json")
START_NS = int(
    datetime(2024, 3, 4, 12, tzinfo=timezone.utc).timestamp() * 1_000_000_000
)
BOUNDED_START_NS = int(
    datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000
)
BOUNDED_END_NS = int(
    datetime(2025, 7, 13, 22, tzinfo=timezone.utc).timestamp() * 1_000_000_000
)


def _bounded_sidecar(
    tmp_path: Path,
    *,
    family: str,
    dbn_sha256: str = H,
    dbn_size: int = 123,
) -> tuple[dict[str, object], dict[str, object]]:
    dbn_path = (
        f"data/dbn/{family}/6A/2025/"
        "2025-01-01_2025-07-13T220000Z.dbn.zst"
    )
    core: dict[str, object] = {
        "authority": {
            "activation": False,
            "dbn_rows_decoded": 0,
            "holdout_or_forward_access": False,
            "provider_calls": 0,
            "publication": False,
        },
        "canonical_dbn": {
            "copy_semantics": "CREATE_ONLY_EXACT_BYTES_NO_OVERWRITE",
            "project_relative_path": dbn_path,
            "sha256": dbn_sha256,
            "size_bytes": dbn_size,
        },
        "coverage_interval": {
            "end_exclusive_utc": "2025-07-13T22:00:00Z",
            "start_inclusive_utc": "2025-01-01T00:00:00Z",
        },
        "dataset": "GLBX.MDP3",
        "family": family,
        "market": "6A",
        "schema_version": "bounded_2025_canonical_registration_sidecar/1.0.0",
        "source_provenance": {},
        "state": "CANDIDATE_INACTIVE_NOT_PUBLISHED_NOT_ACTIVATED",
    }
    payload = {**core, "manifest_id": sha256_json(core)}
    sidecar_path = f"{dbn_path}.manifest.json"
    seed = tmp_path / f"{family}-sidecar-seed.json"
    seed.write_bytes(canonical_bytes(payload))
    target = tmp_path / sidecar_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.hardlink_to(seed)
    entry: dict[str, object] = {
        "family": family,
        "identity_basis": "SIDECAR_BYTES_HASHED_METADATA_ONLY",
        "interval_end_exclusive": "2025-07-13T22:00:00Z",
        "interval_start_inclusive": "2025-01-01T00:00:00Z",
        "kind": "SIDECAR",
        "market": "6A",
        "paired_dbn_sha256": dbn_sha256,
        "paired_dbn_size_bytes": dbn_size,
        "path": sidecar_path,
        "sha256": sha256_file(target, reject_hardlinks=False),
        "sidecar_schema_version": "bounded_2025_canonical_registration_sidecar/1.0.0",
        "size_bytes": target.stat().st_size,
        "year": 2025,
    }
    return entry, payload


@pytest.mark.parametrize(
    "family",
    [
        "definition",
        "ohlcv_1d",
        "ohlcv_1h",
        "ohlcv_1m",
        "ohlcv_1s",
        "statistics",
        "status",
    ],
)
def test_bounded_2025_hardlinked_sidecar_builds_exact_query_contract(
    tmp_path: Path, family: str
) -> None:
    entry, _ = _bounded_sidecar(tmp_path, family=family)
    contract = _query_contract(tmp_path, entry)
    assert contract["schema"] == family.replace("_", "-")
    assert contract["start"] == "2025-01-01T00:00:00Z"
    assert contract["end"] == "2025-07-13T22:00:00Z"
    assert contract["stype_in"] == (
        "parent" if family == "definition" else "continuous"
    )


def test_bounded_2025_sidecar_pairing_drift_fails_closed(tmp_path: Path) -> None:
    entry, _ = _bounded_sidecar(tmp_path, family="definition")
    entry["paired_dbn_sha256"] = "f" * 64
    with pytest.raises(IntegrityError, match="sidecar contract differs"):
        _query_contract(tmp_path, entry)


def test_bounded_2025_registered_hardlink_reaches_real_decoder_path(
    tmp_path: Path,
) -> None:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=BOUNDED_START_NS,
        end=BOUNDED_END_NS,
        stype_in=dbn.SType.PARENT,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.DEFINITION,
        symbols=["6A.FUT"],
        ts_out=False,
    )
    record = dbn.InstrumentDefMsg(
        publisher_id=1,
        instrument_id=123,
        ts_event=BOUNDED_START_NS + 1,
        ts_recv=BOUNDED_START_NS,
        activation=BOUNDED_START_NS,
        expiration=BOUNDED_END_NS,
        min_price_increment=1,
        display_factor=1_000_000_000,
        raw_symbol="6AH5",
        asset="6A",
        security_type="FUT",
        instrument_class=dbn.InstrumentClass.FUTURE,
        security_update_action=dbn.SecurityUpdateAction.ADD,
        unit_of_measure_qty=1_000_000_000,
        currency="USD",
        group="6A",
        exchange="XCME",
        unit_of_measure="IPNT",
    )
    payload = metadata.encode() + bytes(record)
    dbn_path = (
        "data/dbn/definition/6A/2025/"
        "2025-01-01_2025-07-13T220000Z.dbn.zst"
    )
    seed = tmp_path / "definition-dbn-seed.dbn.zst"
    seed.write_bytes(payload)
    target = tmp_path / dbn_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.hardlink_to(seed)
    dbn_sha = sha256_file(target, reject_hardlinks=False)
    sidecar_entry, _ = _bounded_sidecar(
        tmp_path,
        family="definition",
        dbn_sha256=dbn_sha,
        dbn_size=len(payload),
    )
    dbn_entry: dict[str, object] = {
        "family": "definition",
        "identity_basis": "BOUNDED_2025_REGISTRATION_SIDECAR_DECLARATION",
        "interval_end_exclusive": "2025-07-13T22:00:00Z",
        "interval_start_inclusive": "2025-01-01T00:00:00Z",
        "kind": "DBN",
        "market": "6A",
        "path": dbn_path,
        "sha256": dbn_sha,
        "sidecar_schema_version": None,
        "size_bytes": len(payload),
        "year": 2025,
    }
    decoded = _decode_selected_sources(
        root=tmp_path,
        selected=(dbn_entry, sidecar_entry),
        windows={
            "6A": {
                "start": "2025-01-01T00:00:00Z",
                "end": "2025-07-13T22:00:00Z",
            }
        },
        source_contract={
            "active_canonical_source": {
                "release_id": "a" * 64,
                "release_manifest_sha256": "b" * 64,
            },
            "complete_inventory": {"content_inventory_sha256": "c" * 64},
        },
        maximum_decoded_records=10,
    )
    assert decoded["6A"].decoded_record_count == 1
    assert len(decoded["6A"].definitions) == 1


def _operation_plan() -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "causal_observation_canary_operation/1.0.0",
        "operation": CAUSAL_OBSERVATION_CANARY_OPERATION,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": CANARY_SOURCE_CONTRACT_ID,
            "canonical_release_id": CANARY_CANONICAL_RELEASE_ID,
            "exact_source_entries_sha256": "b" * 64,
        },
        "output_staging_path": "state/data_publication_staging/canary-exact",
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "roots": ["ES", "CL", "ZN", "6E", "GC", "ZC", "LE"],
        "windows": {
            market: {
                "start": "2024-03-04T00:00:00Z",
                "end": "2024-03-05T00:00:00Z",
            }
            for market in ("ES", "CL", "ZN", "6E", "GC", "ZC", "LE")
        },
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
            "maximum_payload_bytes": 176_929_782,
            "maximum_decoded_records": 176_929_782,
            "maximum_output_bytes": 1_515_438_256,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def _approved_receipt(tmp_path: Path) -> tuple[RepoBoundary, OperationReceipt, dict[str, object], str]:
    boundary = RepoBoundary(tmp_path)
    plan = _operation_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(plan, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    plan_sha = sha256_file(plan_path)
    required = required_canary_scope(plan=plan, plan_sha256=plan_sha)
    scope = {key: value for key, value in required.items() if not key.startswith("approval_")}
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=CAUSAL_OBSERVATION_CANARY_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CAUSAL_OBSERVATION_CANARY_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=_personal_approval_line(
            CAUSAL_OBSERVATION_CANARY_OPERATION,
            str(plan["plan_id"]),
            plan_sha,
        ),
    )
    return boundary, receipt, plan, plan_sha


def _definition(**changes: object) -> ProviderDefinition:
    values: dict[str, object] = {
        "dataset": "GLBX.MDP3",
        "market": "ES",
        "publisher_id": 1,
        "instrument_id": 2,
        "instrument_id_date_utc": "2024-03-04",
        "ts_event_ns": START_NS - 3_600_000_000_000,
        "ts_recv_ns": START_NS - 3_599_000_000_000,
        "activation_ns": START_NS - 86_400_000_000_000,
        "expiration_ns": START_NS + 86_400_000_000_000,
        "security_update_action": "ADD",
        "instrument_class": "FUTURE",
        "security_type": "FUT",
        "raw_symbol": "ESH4",
        "exchange": "XCME",
        "currency": "USD",
        "min_price_increment_nano": 250_000_000,
        "unit_of_measure_qty_nano": 50_000_000_000,
        "unit_of_measure": "USD",
        "source_release_id": H,
        "source_manifest_sha256": H,
        "source_file_path": "synthetic/definition.dbn.zst",
        "source_file_sha256": H,
        "row_ordinal": 0,
        "row_sha256": "1" * 64,
    }
    values.update(changes)
    return ProviderDefinition(**values)  # type: ignore[arg-type]


def _bar(offset_minutes: int, *, row_digit: str, **changes: object) -> ProviderBar:
    values: dict[str, object] = {
        "dataset": "GLBX.MDP3",
        "market": "ES",
        "publisher_id": 1,
        "instrument_id": 2,
        "event_at_ns": START_NS + offset_minutes * 60_000_000_000,
        "open_nano": -100_000_000,
        "high_nano": -50_000_000,
        "low_nano": -150_000_000,
        "close_nano": -75_000_000,
        "volume": 10,
        "source_release_id": H,
        "source_manifest_sha256": H,
        "source_file_path": "synthetic/ohlcv_1m.dbn.zst",
        "source_file_sha256": H,
        "row_sha256": row_digit * 64,
    }
    values.update(changes)
    return ProviderBar(**values)  # type: ignore[arg-type]


def test_authorization_is_consumed_before_decoder_callback(tmp_path: Path) -> None:
    boundary, receipt, plan, plan_sha = _approved_receipt(tmp_path)
    observed: list[bool] = []

    def decoder(**_: object) -> dict[str, DecodedMarket]:
        use = tmp_path / "state/authorization_uses" / f"{receipt.receipt_id}.json"
        observed.append(use.is_file())
        return {}

    context, decoded = _authorize_then_decode(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha,
        selected=(),
        source_contract={},
        decoder=decoder,
    )
    assert observed == [True]
    assert decoded == {}
    assert context.synthetic is False
    with pytest.raises(UnauthorizedOperation, match="already used"):
        _authorize_then_decode(
            boundary=boundary,
            receipt=receipt,
            plan=plan,
            plan_sha256=plan_sha,
            selected=(),
            source_contract={},
            decoder=decoder,
        )
    assert observed == [True]


def test_canary_stage_creator_uses_exact_path_and_has_no_publish_capability(
    tmp_path: Path,
) -> None:
    publisher = _CanaryStageCreator(
        boundary=RepoBoundary(tmp_path), relative="canary-exact/ES"
    )
    stage = publisher.create_stage("causal_observation")
    assert stage == tmp_path / "state/data_publication_staging/canary-exact/ES"
    assert not hasattr(publisher, "publish")
    with pytest.raises(UnauthorizedOperation, match="exact and one-use"):
        publisher.create_stage("causal_observation")


def test_causal_definition_rejects_future_definition_even_if_already_active() -> None:
    bar = _bar(0, row_digit="2")
    future = _definition(
        ts_event_ns=bar.event_at_ns + 1,
        ts_recv_ns=bar.event_at_ns + 1,
        activation_ns=bar.event_at_ns - 1,
        instrument_id_date_utc="2024-03-04",
    )
    from futures_rebuild.foundation.identity import DefinitionIndex

    with pytest.raises(ContractError, match="no same-day definition"):
        _causal_definition(DefinitionIndex((future,)), bar, bar.event_at_ns)


@pytest.mark.parametrize("raw", (0, INT32_NULL, INT64_NULL))
def test_multiplier_uses_pinned_rulebook_only_for_provider_null_states(raw: int) -> None:
    multiplier, state = _resolve_multiplier(
        rulebook=RULEBOOK,
        market="6A",
        definition=_definition(
            market="6A",
            raw_symbol="6AH4",
            unit_of_measure_qty_nano=raw,
        ),
    )
    assert multiplier == 100_000_000_000_000
    assert state == "RULEBOOK_VALUE_PROVIDER_UNIT_QTY_UNAVAILABLE"


def test_multiplier_rulebook_file_identity_fails_closed_before_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_canary.sha256_file",
        lambda *_: "0" * 64,
    )
    with pytest.raises(IntegrityError, match="economics rulebook differs"):
        _load_economics_rulebook(tmp_path)


def test_multiplier_rejects_negative_and_contradictory_provider_values() -> None:
    with pytest.raises(MultiplierResolutionError, match="negative multiplier") as caught:
        _resolve_multiplier(
            rulebook=RULEBOOK,
            market="ES",
            definition=_definition(unit_of_measure_qty_nano=-1),
        )
    assert caught.value.details["multiplier_state"] == "NEGATIVE_PROVIDER_VALUE"
    assert caught.value.details["definition_row_sha256"] == "1" * 64
    with pytest.raises(MultiplierResolutionError, match="contradicts"):
        _resolve_multiplier(
            rulebook=RULEBOOK,
            market="ES",
            definition=_definition(unit_of_measure_qty_nano=51_000_000_000),
        )


def test_multiplier_uses_contract_quantity_not_point_value_for_cent_quotes() -> None:
    multiplier, state = _resolve_multiplier(
        rulebook=RULEBOOK,
        market="ZC",
        definition=_definition(
            market="ZC",
            raw_symbol="ZCH4",
            unit_of_measure_qty_nano=INT64_NULL,
        ),
    )
    assert multiplier == 5_000_000_000_000
    assert state == "RULEBOOK_VALUE_PROVIDER_UNIT_QTY_UNAVAILABLE"


def test_decoder_admits_exact_canary_reference_bar_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    def chunks(*_: object, schema: str, **__: object):
        observed.append(schema)
        return iter(())

    monkeypatch.setattr(foundation_decoder, "_chunks", chunks)
    for schema in ("ohlcv-1s", "ohlcv-1h"):
        assert list(
            foundation_decoder.iter_bars(
                object(),  # type: ignore[arg-type]
                market="ES",
                expected_query_contract={},
                schema=schema,
            )
        ) == []
    assert observed == ["ohlcv-1s", "ohlcv-1h"]


def test_executed_plan_remains_historically_bound_and_cannot_be_reused() -> None:
    plan_path = ROOT / "configs/causal_observation_canary_plan_v2.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    execution_commit = "d3f60621201bebb95eaad7b5fa2de6da10b3bb31"
    for relative, expected in plan["implementation_bindings"].items():
        committed = subprocess.check_output(
            ["git", "show", f"{execution_commit}:{relative}"], cwd=ROOT
        )
        assert hashlib.sha256(committed).hexdigest() == expected
    assert plan["one_use_authorization"]["issued"] is False
    assert plan["one_use_authorization"]["consumed"] is False
    assert plan["execution_authorized"] is False
    with pytest.raises(IntegrityError, match="implementation binding differs"):
        _validate_plan(ROOT, plan)
    changed = json.loads(json.dumps(plan))
    changed["holdout_allowed"] = True
    with pytest.raises(UnauthorizedOperation, match="nonauthorizing"):
        _validate_plan(ROOT, changed)


def test_runner_is_not_a_public_command() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    targets = project["project"]["scripts"]
    assert all("causal_observation_canary" not in target for target in targets.values())


def test_selected_source_decoder_routes_exact_schema_without_real_payload_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_canary._binding", lambda **_: object()
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_canary._query_contract", lambda *_args, **_kwargs: {}
    )

    def bars(*_: object, schema: str, **__: object):
        observed.append(schema)
        return iter((_bar(0, row_digit="4"),))

    monkeypatch.setattr("futures_rebuild.causal_observation_canary.iter_bars", bars)
    selected = [
        {
            "kind": "DBN",
            "market": "ES",
            "family": "ohlcv_1m",
            "path": "data/dbn/ohlcv_1m/ES/2024/source.dbn.zst",
            "size_bytes": 10,
            "sha256": "4" * 64,
        },
        {
            "kind": "SIDECAR",
            "market": "ES",
            "family": "ohlcv_1m",
            "path": "data/dbn/ohlcv_1m/ES/2024/source.dbn.zst.manifest.json",
            "size_bytes": 10,
            "sha256": "5" * 64,
        },
    ]
    decoded = _decode_selected_sources(
        root=ROOT,
        selected=selected,
        windows={"ES": {"start": "2024-03-04T00:00:00Z", "end": "2024-03-05T00:00:00Z"}},
        source_contract={
            "active_canonical_source": {
                "release_id": CANARY_CANONICAL_RELEASE_ID,
                "release_manifest_sha256": "6" * 64,
            },
            "complete_inventory": {"content_inventory_sha256": "7" * 64},
        },
        maximum_decoded_records=1,
    )
    assert observed == ["ohlcv-1m"]
    assert decoded["ES"].primary_1m == (_bar(0, row_digit="4"),)
    assert decoded["ES"].decoded_record_count == 1


class _SyntheticPublisher:
    def __init__(self, root: Path) -> None:
        self.root = root

    def create_stage(self, purpose: str) -> Path:
        assert purpose == "causal_observation"
        stage = self.root / "candidate/ES"
        stage.mkdir(parents=True)
        return stage


def test_synthetic_market_candidate_is_observation_only_and_independently_verified(
    tmp_path: Path,
) -> None:
    context = issue_synthetic_observation_context(
        boundary=RepoBoundary(tmp_path), fixture_id="3" * 64
    )
    decoded = DecodedMarket(
        definitions=(_definition(),),
        primary_1m=(_bar(0, row_digit="2"), _bar(2, row_digit="3")),
        reference_1s={},
        reference_1h={},
        reference_1d={},
        support_rows=(),
        decoded_record_count=3,
    )
    prepared = build_market_candidate(
        publisher=_SyntheticPublisher(tmp_path),  # type: ignore[arg-type]
        context=context,
        market="ES",
        window={"start": "2024-03-04T00:00:00Z", "end": "2024-03-05T00:00:00Z"},
        decoded=decoded,
        economics_rulebook=RULEBOOK,
    )
    certificate = verify_observation_candidate(
        stage=prepared.stage,
        manifest=prepared.manifest,
        economics_rulebook=RULEBOOK,
    )
    assert certificate["outcome_count"] == 0
    assert certificate["feature_count"] == 0
    assert certificate["prediction_count"] == 0
    assert certificate["evaluation_count"] == 0
    tables = read_bundle(prepared.stage / "candidate")
    missingness = tables["missingness"]
    assert sum(row["state"] == "OBSERVED_VALID" for row in missingness) == 2
    assert sum(row["state"] == "UNKNOWN_FAIL_CLOSED" for row in missingness) == 1
    observations = tables["observations"]
    assert all(row["official_schedule_state"] == "UNKNOWN_FAIL_CLOSED" for row in observations)
    assert any(row["open_nano"] == -100_000_000 for row in observations)
