from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import uuid
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import IntegrityError
from futures_rebuild.legacy_trial_census import publish_legacy_trial_census
from futures_rebuild.readiness import (
    CLOSED_RESEARCH_LINES,
    ENGINE_CONFIG_PATHS,
    ENGINE_TEST_PATHS,
    HISTORICAL_READY_RELEASE_KIND,
    REBUILD_COMPLETE_RELEASE_KIND,
    REQUIRED_HARD_PAUSES,
    ReadinessAssessment,
    ReadinessPublication,
    SyntheticTestAttestation,
    assess_readiness,
    engine_code_closure,
    load_historical_research_ready,
    load_rebuild_complete,
    publish_engine_registration,
    publish_project_isolation_evidence,
    publish_readiness_states,
    publish_synthetic_test_evidence,
)
from futures_rebuild.release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt
from tests.test_foundation_orchestrator import (
    _orchestrator as _foundation_orchestrator,
)
from tests.test_foundation_orchestrator import _setup as _foundation_setup
from tests.test_legacy_trial_census import _snapshot as _legacy_census_snapshot


REPO = Path(__file__).resolve().parents[1]


def _remove_readonly(function, path: str, _error) -> None:
    os.chmod(path, stat.S_IWRITE)
    function(path)


@pytest.fixture
def boundary() -> RepoBoundary:
    # The production-derived census snapshot has deep prescribed paths. Keep
    # the synthetic repository below legacy Windows MAX_PATH.
    root = Path(Path.cwd().anchor) / f"rdy-{uuid.uuid4().hex[:8]}"
    root.mkdir()
    active = root / "a"
    legacy = root / "l"
    stock = root / "s"
    active.mkdir()
    legacy.mkdir()
    stock.mkdir()
    (active / "configs").mkdir()
    (active / "bundles").mkdir()
    try:
        yield RepoBoundary(active.resolve(), (legacy.resolve(),), (stock.resolve(),))
    finally:
        shutil.rmtree(root, onerror=_remove_readonly)


def _copy(root: Path, relative: str) -> None:
    source = REPO / Path(relative)
    target = root / Path(relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def _install_readiness_closure(boundary) -> None:
    dependency = json.loads(
        (REPO / "configs" / "dependency_lock_receipt.json").read_text(
            encoding="utf-8"
        )
    )
    paths = {
        *(entry["path"] for entry in engine_code_closure(REPO)),
        *ENGINE_CONFIG_PATHS,
        *ENGINE_TEST_PATHS,
        *(entry["path"] for entry in dependency["files"]),
        "configs/controlled_rebuild_authorization.json",
        ".gitignore",
    }
    for relative in sorted(paths):
        _copy(boundary.active_root, relative)

    dependency_path = (
        boundary.active_root / "configs" / "dependency_lock_receipt.json"
    )
    dependency = json.loads(dependency_path.read_text(encoding="utf-8"))
    for entry in dependency["files"]:
        entry["sha256"] = sha256_file(boundary.active_root / Path(entry["path"]))
    dependency.pop("receipt_id")
    dependency["receipt_id"] = sha256_json(dependency)
    dependency_path.write_bytes(canonical_bytes(dependency) + b"\n")

    authorization_path = (
        boundary.active_root / "configs" / "controlled_rebuild_authorization.json"
    )
    authorization = json.loads(authorization_path.read_text(encoding="utf-8"))
    authorization.pop("authorization_id")
    authorization["active_root"] = str(boundary.active_root)
    authorization["legacy_root"] = str(boundary.legacy_roots[0])
    authorization["authorization_id"] = sha256_json(authorization)
    authorization_path.write_bytes(canonical_bytes(authorization) + b"\n")


def _commit_fixture_repo(root: Path) -> None:
    subprocess.run(("git", "init", "-b", "main", str(root)), check=True, capture_output=True)
    subprocess.run(
        ("git", "-C", str(root), "config", "user.email", "fixture@example.invalid"),
        check=True,
    )
    subprocess.run(
        ("git", "-C", str(root), "config", "user.name", "Readiness Fixture"),
        check=True,
    )
    status = subprocess.run(
        ("git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    paths = [line[3:] for line in status]
    assert paths
    subprocess.run(("git", "-C", str(root), "add", "--", *paths), check=True)
    subprocess.run(
        ("git", "-C", str(root), "commit", "-m", "test: readiness fixture"),
        check=True,
        capture_output=True,
    )


def _publisher(boundary, operation_factory, name: str = "readiness") -> AtomicPublisher:
    return AtomicPublisher(
        boundary.active_root
        / "data"
        / "vault"
        / ".staging"
        / "releases"
        / name,
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / f"{name}.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )


def _census(
    boundary,
    operation_factory,
    monkeypatch,
) -> VerifiedReleaseReceipt:
    snapshot, _ = _legacy_census_snapshot(
        boundary=boundary, monkeypatch=monkeypatch
    )
    return publish_legacy_trial_census(
        snapshot=snapshot,
        boundary=boundary,
        publisher=_publisher(boundary, operation_factory, "census-production-derived"),
    )


def _prerequisites(boundary, operation_factory, monkeypatch):
    _install_readiness_closure(boundary)
    snapshot, selection, spec = _foundation_setup(boundary, operation_factory)
    foundation = _foundation_orchestrator(boundary, operation_factory).run(
        source_snapshot_root=snapshot.root,
        source_selection_receipt=selection,
        feature_spec=spec,
    ).foundation_set_receipt
    census = _census(boundary, operation_factory, monkeypatch)
    _commit_fixture_repo(boundary.active_root)
    publisher = _publisher(boundary, operation_factory)
    synthetic = publish_synthetic_test_evidence(
        attestation=SyntheticTestAttestation(100, "b" * 64),
        boundary=boundary,
        publisher=publisher,
    )
    engine = publish_engine_registration(
        synthetic_test_evidence_receipt=synthetic,
        boundary=boundary,
        publisher=publisher,
    )
    isolation = publish_project_isolation_evidence(
        synthetic_test_evidence_receipt=synthetic,
        boundary=boundary,
        publisher=publisher,
    )
    return foundation, synthetic, engine, isolation, census, publisher


def test_readiness_publishes_exact_non_authorizing_states_idempotently(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, synthetic, engine, isolation, census, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )

    first = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    assert isinstance(first, ReadinessPublication)
    assert first.rebuild_complete_receipt.release_kind == REBUILD_COMPLETE_RELEASE_KIND
    assert (
        first.historical_research_ready_receipt.release_kind
        == HISTORICAL_READY_RELEASE_KIND
    )

    rebuild = load_rebuild_complete(
        first.rebuild_complete_receipt, boundary=boundary
    )
    historical = load_historical_research_ready(
        first.historical_research_ready_receipt, boundary=boundary
    )
    for payload in (rebuild, historical):
        assert payload["alpha_claim"] is False
        assert payload["candidate_claim"] is False
        assert payload["execution_authority_granted"] is False
        assert payload["live_trading_ready"] is False
        assert payload["real_history_execution_authorized"] is False
        assert payload["readiness_is_execution_authority"] is False
        safety = payload["safety_contract"]
        assert safety["hard_pauses"] == sorted(REQUIRED_HARD_PAUSES)
        assert safety["closed_research_lines"] == [
            dict(item) for item in CLOSED_RESEARCH_LINES
        ]
        assert all(value is False for value in safety["authority"].values())
        assert all(value is False for value in safety["claims"].values())
    assert rebuild["synthetic_test_evidence_receipt"] == synthetic.as_dict()
    census_payload = json.loads(
        (
            boundary.active_root
            / census.relative_root
            / "legacy_census.json"
        ).read_text(encoding="utf-8")
    )
    assert historical["legacy_census"] == census_payload
    assert historical["legacy_census"]["status"] == (
        "INVALID_TRIAL_CENSUS_UNRESOLVED"
    )
    assert historical["legacy_census"]["exact_count_state"] == "INDETERMINATE"
    assert historical["legacy_census"]["preregistered_penalty_count"] == 0
    assert historical["legacy_census"]["trusted_gate"] is False
    assert historical["historical_capability_closure_id"] == (
        historical["historical_capability_closure"]["capability_closure_id"]
    )
    assert historical["git_closure"] == rebuild["git_closure"]
    assert historical["mechanical_readiness_scope"] == (
        "NO_ALPHA_CLAIM_NO_REAL_HISTORY_AUTHORITY_NO_TRUST_GATE"
    )
    assert historical["real_history_trust_gate"] == {
        "execution_authorized": False,
        "status": "CLOSED_INVALID_TRIAL_CENSUS_UNRESOLVED",
        "trusted": False,
    }

    second = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    assert isinstance(second, ReadinessPublication)
    assert second.rebuild_complete_receipt == first.rebuild_complete_receipt
    assert (
        second.historical_research_ready_receipt
        == first.historical_research_ready_receipt
    )


def test_missing_census_returns_only_blocker_and_publishes_no_state(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, _, engine, isolation, _, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )
    release_root = boundary.active_root / "data" / "vault" / "releases"
    before = {path.name for path in release_root.iterdir()}

    result = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=None,
    )

    assert isinstance(result, ReadinessAssessment)
    assert result.as_dict() == {
        "blockers": [
            {
                "code": "MISSING_LEGACY_TRIAL_CENSUS",
                "state": "HISTORICAL_RESEARCH_READY",
            }
        ],
        "publication_allowed": False,
        "status": "BLOCKED",
    }
    assert {path.name for path in release_root.iterdir()} == before


def test_all_absent_inputs_return_blockers_without_writes(
    boundary, operation_factory
) -> None:
    _install_readiness_closure(boundary)
    _commit_fixture_repo(boundary.active_root)
    publisher = _publisher(boundary, operation_factory)

    result = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=None,
        engine_registration_receipt=None,
        isolation_evidence_receipt=None,
        legacy_census_release_receipt=None,
    )

    assert isinstance(result, ReadinessAssessment)
    assert not result.publication_allowed
    assert len(result.blockers) == 6
    assert not (boundary.active_root / "data" / "vault" / "releases").exists()


def test_tampered_legacy_census_returns_blocker_without_readiness_writes(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, _, engine, isolation, census, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )
    release_root = boundary.active_root / "data" / "vault" / "releases"
    before = {path.name for path in release_root.iterdir()}
    census_path = boundary.active_root / census.relative_root / "legacy_census.json"
    census_path.write_bytes(census_path.read_bytes() + b" ")

    result = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    assert isinstance(result, ReadinessAssessment)
    assert result.as_dict()["blockers"] == [
        {
            "code": "INVALID_OR_TAMPERED_LEGACY_TRIAL_CENSUS",
            "state": "HISTORICAL_RESEARCH_READY",
        }
    ]
    assert {path.name for path in release_root.iterdir()} == before


def test_receipt_kind_substitution_fails_closed(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, _, engine, isolation, census, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )

    with pytest.raises(IntegrityError, match="project isolation release contract"):
        assess_readiness(
            boundary=boundary,
            foundation_set_receipt=foundation,
            engine_registration_receipt=engine,
            isolation_evidence_receipt=engine,
            legacy_census_release_receipt=census,
        )

    different_test_run = publish_synthetic_test_evidence(
        attestation=SyntheticTestAttestation(100, "f" * 64),
        boundary=boundary,
        publisher=publisher,
    )
    substituted_isolation = publish_project_isolation_evidence(
        synthetic_test_evidence_receipt=different_test_run,
        boundary=boundary,
        publisher=publisher,
    )
    with pytest.raises(IntegrityError, match="different test runs"):
        assess_readiness(
            boundary=boundary,
            foundation_set_receipt=foundation,
            engine_registration_receipt=engine,
            isolation_evidence_receipt=substituted_isolation,
            legacy_census_release_receipt=census,
        )

    valid = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    assert isinstance(valid, ReadinessPublication)
    substituted_engine = publish_engine_registration(
        synthetic_test_evidence_receipt=different_test_run,
        boundary=boundary,
        publisher=publisher,
    )
    original_path = (
        boundary.active_root
        / valid.historical_research_ready_receipt.relative_root
        / "historical_research_ready.json"
    )
    substituted_payload = json.loads(original_path.read_text(encoding="utf-8"))
    substituted_payload["engine_registration_receipt"] = (
        substituted_engine.as_dict()
    )
    substituted_payload["isolation_evidence_receipt"] = (
        substituted_isolation.as_dict()
    )
    substituted_payload["synthetic_test_evidence_receipt"] = (
        different_test_run.as_dict()
    )
    substituted_payload.pop("readiness_receipt_id")
    substituted_payload["readiness_receipt_id"] = sha256_json(substituted_payload)
    stage = publisher.create_stage("substituted_historical")
    (stage / "historical_research_ready.json").write_bytes(
        canonical_bytes(substituted_payload) + b"\n"
    )
    substituted_manifest = ReleaseManifest.build(
        stage,
        release_kind=HISTORICAL_READY_RELEASE_KIND,
        schema_version="1.0.0",
        source_release_ids=(
            valid.rebuild_complete_receipt.release_id,
            substituted_engine.release_id,
            foundation.release_id,
            substituted_isolation.release_id,
            census.release_id,
            different_test_run.release_id,
        ),
        metadata={
            "readiness_receipt_id": substituted_payload["readiness_receipt_id"],
            "state": "HISTORICAL_RESEARCH_READY",
        },
    )
    substituted_receipt = VerifiedReleaseReceipt.from_release(
        publisher.publish(stage, substituted_manifest), boundary
    )
    with pytest.raises(IntegrityError, match="exact dependency closure"):
        load_historical_research_ready(substituted_receipt, boundary=boundary)

    engine_path = boundary.active_root / Path(engine_code_closure(REPO)[0]["path"])
    engine_path.write_bytes(engine_path.read_bytes() + b"\n")
    with pytest.raises(IntegrityError, match="stale, substituted, or unsafe"):
        assess_readiness(
            boundary=boundary,
            foundation_set_receipt=foundation,
            engine_registration_receipt=engine,
            isolation_evidence_receipt=substituted_isolation,
            legacy_census_release_receipt=census,
        )


def test_tampered_readiness_release_fails_reverification(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, _, engine, isolation, census, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )
    result = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    assert isinstance(result, ReadinessPublication)
    payload_path = (
        boundary.active_root
        / result.rebuild_complete_receipt.relative_root
        / "rebuild_complete.json"
    )
    payload_path.write_bytes(payload_path.read_bytes() + b" ")

    with pytest.raises(IntegrityError):
        load_rebuild_complete(result.rebuild_complete_receipt, boundary=boundary)
