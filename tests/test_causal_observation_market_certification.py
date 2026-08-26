from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.causal_observation_foundation import CAUSAL_OBSERVATION_CONTRACT_ID
from futures_rebuild.causal_observation_market_certification import (
    CERTIFICATE_SCHEMA,
    PLAN_SCHEMA,
    ReplayEvidence,
    build_market_certification_plan,
    certify_complete_market_certificate_set,
    required_market_certification_scope,
    run_authorized_market_certification,
    _io_path,
    _partition_manifest,
    _reconstruct_tables,
    _run_replay_in_fresh_process,
    MARKET_CERTIFICATION_PLAN_OPERATION,
    bind_predecessor_market_certificate,
    validate_certified_market_sequence,
)
from futures_rebuild.causal_observation_canary import DecodedMarket, build_market_candidate
from futures_rebuild.causal_observation_foundation import issue_synthetic_observation_context
from futures_rebuild.causal_observation_parquet import read_bundle
from futures_rebuild.boundary import RepoBoundary
from tests.test_causal_observation_canary import (
    RULEBOOK,
    _SyntheticPublisher,
    _bar,
    _definition,
)
from futures_rebuild.causal_observation_market_checkpoint import (
    CHECKPOINT_SET_SCHEMA,
    MARKET_ORDER,
    checkpoint_set_identity,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
    PREPARATORY_REAL_HISTORY_OPERATIONS,
)


H = "a" * 64
ROOT = Path(__file__).resolve().parents[1]


def _checkpoint_set() -> dict[str, object]:
    return {
        "schema_version": CHECKPOINT_SET_SCHEMA,
        "market_order": list(MARKET_ORDER),
        "source_contract_id": "b" * 64,
        "canonical_release_id": "c" * 64,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "writer_configuration": {
            "format": "PARQUET",
            "compression": "ZSTD",
            "compression_level": 9,
            "partitioning": "market/year/month",
        },
        "implementation_bindings": {"synthetic.py": H},
    }


def _build_plan(market: str = "ES", attempt: str = "d" * 64) -> dict[str, object]:
    checkpoint_set = _checkpoint_set()
    return {
        "plan_id": "e" * 64,
        "execution_role": "COMPLETE_MARKET_CHECKPOINT",
        "target_market": market,
        "attempt_id": attempt,
        "checkpoint_set": checkpoint_set,
        "checkpoint_set_id": checkpoint_set_identity(checkpoint_set),
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "output_staging_path": (
            f"data/causally_gated_normalized/v10/"
            f"{checkpoint_set_identity(checkpoint_set)}/{market}/{attempt}"
        ),
        "source": {
            "source_contract_id": "b" * 64,
            "canonical_release_id": "c" * 64,
            "inventory_path": "inventory.json",
            "inventory_sha256": H,
            "exact_source_entries_sha256": "f" * 64,
            "exact_dbn_entries_sha256": "1" * 64,
            "exact_source_entry_count": 4,
            "exact_dbn_file_count": 2,
            "exact_sidecar_file_count": 2,
            "total_source_bytes": 240,
            "maximum_payload_bytes": 200,
            "work_unit_count": 1,
        },
    }


def _evidence(
    market: str = "ES",
    attempt: str = "d" * 64,
    output_inventory: tuple[dict[str, object], ...] | None = None,
) -> ReplayEvidence:
    inventory = output_inventory or (
        {"path": f"state/synthetic/{market}.parquet", "sha256": "2" * 64, "size": 100},
    )
    sample = (
        {
            "kind": "OBSERVATION_SOURCE_BINDING",
            "year": 2010,
            "interval": "2010-01-01_2010-02-01",
            "reasons": ["PARTITION_FIRST"],
            "row_id": "5" * 64,
        },
    )
    core = {
        "market": market,
        "attempt_id": attempt,
        "checkpoint_set_id": checkpoint_set_identity(_checkpoint_set()),
        "source_file_count": 2,
        "source_payload_bytes": 200,
        "decoded_record_count": 50,
        "partition_count": 1,
        "observation_count": 10,
        "negative_price_count": 0,
        "output_bytes": sum(int(item["size"]) for item in inventory),
        "output_inventory": list(inventory),
        "output_inventory_sha256": sha256_json(list(inventory)),
        "deterministic_source_sample": list(sample),
        "deterministic_source_sample_sha256": sha256_json(list(sample)),
        "ordered_row_ids_sha256": "3" * 64,
        "partition_evidence_sha256": "4" * 64,
    }
    return ReplayEvidence(
        evidence_id=sha256_json(core),
        **{
            **core,
            "output_inventory": inventory,
            "deterministic_source_sample": sample,
        },
    )


def _market_certificate(
    market: str,
    *,
    sha256: str,
    size: int,
    certifier_sha256: str = "b" * 64,
) -> dict[str, object]:
    evidence = _evidence(
        market,
        output_inventory=(
            {"path": f"state/synthetic/{market}.parquet", "sha256": sha256, "size": size},
        ),
    )
    core = {
        "schema_version": CERTIFICATE_SCHEMA,
        "status": "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE",
        "market": market,
        "attempt_id": "d" * 64,
        "checkpoint_set_id": checkpoint_set_identity(_checkpoint_set()),
        "checkpoint_result_id": "5" * 64,
        "checkpoint_sha256": "6" * 64,
        "source_contract_id": "b" * 64,
        "source_release_id": "c" * 64,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "economics_rulebook_sha256": "7" * 64,
        "build_plan_id": "8" * 64,
        "certification_plan_id": "9" * 64,
        "certifier_implementation_bindings_sha256": "a" * 64,
        "certifier_implementation_sha256": certifier_sha256,
        "receipt_id": "0" * 64,
        "replay_passes": 2,
        "replay_evidence_id": evidence.evidence_id,
        "replay_evidence": evidence.as_dict(),
        "provider_calls": 0,
        "holdout_rows": 0,
        "forward_rows": 0,
        "outcomes": 0,
        "features": 0,
        "fitting": 0,
        "predictions": 0,
        "evaluations": 0,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "certificate_id": sha256_json(core)}


def test_certification_operation_is_current_bounded_real_history_operation() -> None:
    assert CAUSAL_OBSERVATION_FULL_BUILD_OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    assert MARKET_CERTIFICATION_PLAN_OPERATION != CAUSAL_OBSERVATION_FULL_BUILD_OPERATION


def test_certifier_does_not_import_producer_transform_or_candidate_verifier() -> None:
    path = ROOT / "src/futures_rebuild/causal_observation_market_certification.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "causal_observation_full_build" not in modules
    assert "causal_observation_verifier" not in modules


def test_independent_replay_process_transports_failure_without_parent_fallback(
    tmp_path: Path,
) -> None:
    with pytest.raises(IntegrityError, match="process failed"):
        _run_replay_in_fresh_process(
            tmp_path,
            {"target_market": "ES", "build_plan_path": "missing.json"},
            {},
        )


def test_plan_binds_exact_market_checkpoint_and_two_complete_replays() -> None:
    build = _build_plan()
    plan = build_market_certification_plan(
        repository_root=ROOT,
        build_plan_path="plans/es.json",
        build_plan=build,
        build_plan_sha256=H,
    )
    assert plan["schema_version"] == PLAN_SCHEMA
    assert plan["replay_passes"] == 2
    assert plan["maximum_payload_bytes"] == 400
    assert plan["checkpoint_path"].endswith("/market_checkpoint.json")
    assert plan["certificate_path"].endswith(f"/ES/{'d' * 64}.json")
    scope = required_market_certification_scope(
        plan=plan, plan_sha256="2" * 64, checkpoint_sha256="3" * 64
    )
    assert scope["target_market"] == "ES"
    assert scope["maximum_payload_bytes"] == "400"
    assert scope["holdout"] == "false"
    assert scope["publication"] == "false"


def test_independent_reconstruction_matches_producer_without_calling_producer_verifier(
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
    expected, _ = _reconstruct_tables(
        market="ES",
        decoded=decoded,
        start_ns=1_709_510_400_000_000_000,
        end_ns=1_709_596_800_000_000_000,
        source_contract_id=context.source_contract_id,
        source_release_id=context.source_release_id,
        rulebook=RULEBOOK,
        prior=None,
    )
    assert read_bundle(prepared.stage / "candidate") == expected


def test_partial_day_checkpoint_selector_reconstructs_foundation_logical_identity(
    tmp_path: Path,
) -> None:
    context = issue_synthetic_observation_context(
        boundary=RepoBoundary(tmp_path), fixture_id="4" * 64
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
    start_ns = 1_709_510_400_000_000_000
    end_ns = 1_709_589_600_000_000_000  # 2024-03-04T22:00:00Z
    prepared = build_market_candidate(
        publisher=_SyntheticPublisher(tmp_path),  # type: ignore[arg-type]
        context=context,
        market="ES",
        window={"start": "2024-03-04T00:00:00Z", "end": "2024-03-04T22:00:00Z"},
        decoded=decoded,
        economics_rulebook=RULEBOOK,
    )
    tables = read_bundle(prepared.stage / "candidate")
    manifest, files = _partition_manifest(
        stage=prepared.stage / "candidate",
        market="ES",
        year=2024,
        start_ns=start_ns,
        end_ns=end_ns,
        plan={
            "causal_contract_id": context.causal_contract_id,
            "source": {
                "source_contract_id": context.source_contract_id,
                "canonical_release_id": context.source_release_id,
                "exact_source_entries_sha256": context.exact_source_entries_sha256,
            },
            "_build_plan": {"plan_id": context.plan_id},
            "build_plan_sha256": context.plan_sha256,
        },
        tables=tables,
    )
    assert manifest["release_id"] == prepared.manifest.release_id
    assert files == [entry.as_dict() for entry in prepared.manifest.files]
    assert all(
        "/2024-03-04_2024-03-04/" in str(item["logical_path"])
        for item in files
    )


def test_final_certificate_requires_all_41_compatible_robust_certificates(tmp_path: Path) -> None:
    certifier = tmp_path / "src/futures_rebuild/causal_observation_market_certification.py"
    certifier.parent.mkdir(parents=True)
    certifier.write_text("# synthetic certifier\n")
    certifier_sha256 = __import__("hashlib").sha256(certifier.read_bytes()).hexdigest()
    output = tmp_path / "state/synthetic"
    output.mkdir(parents=True)
    certificates = []
    for market in MARKET_ORDER:
        path = output / f"{market}.parquet"
        path.write_bytes(market.encode("ascii"))
        import hashlib

        certificates.append(
            _market_certificate(
                market,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                size=path.stat().st_size,
                certifier_sha256=certifier_sha256,
            )
        )
    final = certify_complete_market_certificate_set(
        repository_root=tmp_path,
        checkpoint_set=_checkpoint_set(),
        market_certificates=certificates,
    )
    assert final["status"] == "PASS_41_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
    assert final["market_count"] == 41
    assert final["total_observation_count"] == 410
    es_path = output / "ES.parquet"
    es_original = es_path.read_bytes()
    es_path.write_bytes(b"corrupt")
    with pytest.raises(IntegrityError, match="output file differs"):
        certify_complete_market_certificate_set(
            repository_root=tmp_path,
            checkpoint_set=_checkpoint_set(),
            market_certificates=certificates,
        )
    es_path.write_bytes(es_original)
    with pytest.raises(UnauthorizedOperation, match="exact 41-market"):
        certify_complete_market_certificate_set(
            repository_root=tmp_path,
            checkpoint_set=_checkpoint_set(),
            market_certificates=certificates[:-1],
        )
    damaged = list(certificates)
    damaged[0] = {**damaged[0], "publication_authorized": True}
    with pytest.raises(IntegrityError, match="invalid or incompatible"):
        certify_complete_market_certificate_set(
            repository_root=tmp_path,
            checkpoint_set=_checkpoint_set(),
            market_certificates=damaged,
        )


def test_next_market_is_bound_to_unchanged_predecessor_certificate_and_output(
    tmp_path: Path,
) -> None:
    implementation = tmp_path / "src/futures_rebuild/causal_observation_market_certification.py"
    implementation.parent.mkdir(parents=True)
    implementation.write_bytes((ROOT / implementation.relative_to(tmp_path)).read_bytes())
    output = tmp_path / "state/synthetic/ES.parquet"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"certified-es")
    import hashlib

    certificate = _market_certificate(
        "ES",
        sha256=hashlib.sha256(output.read_bytes()).hexdigest(),
        size=output.stat().st_size,
    )
    certificate_path = tmp_path / "state/certificates/es.json"
    certificate_path.parent.mkdir(parents=True)
    certificate_path.write_text(json.dumps(certificate, sort_keys=True) + "\n")
    gc = bind_predecessor_market_certificate(
        repository_root=tmp_path,
        build_plan=_build_plan("GC"),
        certificate_path="state/certificates/es.json",
    )
    validate_certified_market_sequence(tmp_path, gc)
    output.write_bytes(b"changed")
    with pytest.raises(IntegrityError, match="certified output changed"):
        validate_certified_market_sequence(tmp_path, gc)


class _Receipt:
    receipt_id = "a" * 64

    def __init__(self) -> None:
        self.consumed = False

    def consume(self, *_: object, **__: object) -> None:
        self.consumed = True


def _write_execution_fixture(root: Path) -> tuple[Path, dict[str, object]]:
    build = _build_plan()
    build_path = root / "plans/es.json"
    build_path.parent.mkdir(parents=True)
    build_path.write_text(json.dumps(build, sort_keys=True) + "\n")
    plan = build_market_certification_plan(
        repository_root=ROOT,
        build_plan_path="plans/es.json",
        build_plan=build,
        build_plan_sha256=H,
    )
    checkpoint_core = {
        "status": "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE",
        "target_market": "ES",
        "attempt_id": "d" * 64,
        "checkpoint_set_id": plan["checkpoint_set_id"],
        "publication_authorized": False,
        "activation_authorized": False,
    }
    checkpoint = {**checkpoint_core, "result_id": sha256_json(checkpoint_core)}
    checkpoint_path = root / str(plan["checkpoint_path"])
    checkpoint_path.parent.mkdir(parents=True)
    checkpoint_path.write_text(json.dumps(checkpoint, sort_keys=True) + "\n")
    plan_path = root / "plans/certify-es.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n")
    return plan_path, plan


def test_two_identical_passes_create_one_immutable_inactive_certificate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path, plan = _write_execution_fixture(tmp_path)
    receipt = _Receipt()
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_certification.validate_market_certification_plan",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_certification.sha256_file",
        lambda path: H if path.name == "es.json" else "6" * 64,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_certification._run_replay_in_fresh_process",
        lambda *_: _evidence(),
    )
    certificate = run_authorized_market_certification(
        repository_root=tmp_path,
        receipt=receipt,  # type: ignore[arg-type]
        plan_path=plan_path,
    )
    assert receipt.consumed is True
    assert certificate["status"] == "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
    assert certificate["replay_passes"] == 2
    assert _io_path(tmp_path / str(plan["certificate_path"])).is_file()
    assert not _io_path(tmp_path / str(plan["failure_path"])).exists()


def test_nondeterministic_replay_fails_closed_and_preserves_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path, plan = _write_execution_fixture(tmp_path)
    checkpoint_path = tmp_path / str(plan["checkpoint_path"])
    before = checkpoint_path.read_bytes()
    receipt = _Receipt()
    calls = 0

    def replay(*_: object) -> ReplayEvidence:
        nonlocal calls
        calls += 1
        value = _evidence()
        return value if calls == 1 else ReplayEvidence(
            **{**value.as_dict(), "evidence_id": "f" * 64}
        )

    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_certification.validate_market_certification_plan",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_certification.sha256_file",
        lambda path: H if path.name == "es.json" else "6" * 64,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_certification._run_replay_in_fresh_process",
        replay,
    )
    with pytest.raises(IntegrityError, match="not deterministic"):
        run_authorized_market_certification(
            repository_root=tmp_path,
            receipt=receipt,  # type: ignore[arg-type]
            plan_path=plan_path,
        )
    assert checkpoint_path.read_bytes() == before
    assert not _io_path(tmp_path / str(plan["certificate_path"])).exists()
    failure = json.loads(_io_path(tmp_path / str(plan["failure_path"])).read_text())
    assert failure["status"] == "FAILED_MARKET_CERTIFICATION_CHECKPOINT_PRESERVED"
    assert failure["checkpoint_or_partition_reuse_authorized"] is False
