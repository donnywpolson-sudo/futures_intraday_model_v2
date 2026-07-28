from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import futures_rebuild.readiness as readiness_module
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import IntegrityError
from futures_rebuild.exchange_calendar import (
    CME_TIMEZONE,
    active_pointer_payload,
    diff_exchange_calendars,
    publish_calendar_index,
)
from futures_rebuild.legacy_trial_census import publish_legacy_trial_census
from futures_rebuild.historical_capability import build_foundation_research_blueprint
from futures_rebuild.readiness import (
    CLOSED_RESEARCH_LINES,
    ENGINE_CONFIG_PATHS,
    HISTORICAL_READY_RELEASE_KIND,
    REBUILD_COMPLETE_RELEASE_KIND,
    REQUIRED_HARD_PAUSES,
    ReadinessAssessment,
    ReadinessPublication,
    _compute_isolation_proof,
    _scan_no_cross_import,
    assess_readiness,
    engine_code_closure,
    engine_test_closure,
    load_historical_research_ready,
    load_rebuild_complete,
    load_synthetic_test_evidence,
    publish_engine_registration,
    publish_project_isolation_evidence,
    publish_readiness_prerequisites,
    publish_readiness_states,
    publish_synthetic_test_evidence,
)
from futures_rebuild.data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from tests.test_foundation_orchestrator import (
    _orchestrator as _foundation_orchestrator,
)
from tests.test_foundation_orchestrator import _setup as _foundation_setup
from tests.test_legacy_trial_census import (
    _archive_receipt as _legacy_archive_receipt,
    _snapshot as _legacy_census_snapshot,
)
from tests.test_exchange_calendar import (
    _activation as _calendar_activation,
    _publish_calendar,
    _regular_session,
)


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
        *(entry["path"] for entry in engine_test_closure(REPO)),
        *(entry["path"] for entry in dependency["files"]),
        "configs/controlled_rebuild_authorization.json",
        ".gitattributes",
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
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / f"{name}.lock",
    )


def _publish_fixture_synthetic_evidence(
    *, boundary, publisher, monkeypatch, output: bytes
) -> VerifiedReleaseReceipt:
    monkeypatch.setattr(
        readiness_module,
        "_run_pinned_synthetic_suite",
        lambda _archive: output,
    )
    return publish_synthetic_test_evidence(
        boundary=boundary,
        publisher=publisher,
    )


def _census(
    boundary,
    operation_factory,
    monkeypatch,
) -> VerifiedReleaseReceipt:
    snapshot, _ = _legacy_census_snapshot(
        boundary=boundary, monkeypatch=monkeypatch
    )
    archive_receipt = _legacy_archive_receipt(
        boundary, operation_factory, snapshot
    )
    return publish_legacy_trial_census(
        snapshot=snapshot,
        source_archive_receipt=archive_receipt,
        boundary=boundary,
        publisher=_publisher(boundary, operation_factory, "census-production-derived"),
    )


def _prerequisites(boundary, operation_factory, monkeypatch):
    _install_readiness_closure(boundary)
    snapshot, selection, spec = _foundation_setup(boundary, operation_factory)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    today = now.astimezone(ZoneInfo(CME_TIMEZONE)).date()
    calendar_end = today + timedelta(days=89)
    sessions = []
    trade_date = date(2024, 1, 1)
    while trade_date <= calendar_end:
        sessions.append(_regular_session(trade_date.isoformat()))
        trade_date += timedelta(days=1)
    calendar_receipt, calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=sessions,
        retrieved_at_utc=now.isoformat().replace("+00:00", "Z"),
    )
    calendar_diff = diff_exchange_calendars(None, calendar)
    activation = _calendar_activation(
        calendar_release_id=calendar_receipt.release_id,
        predecessor_index_release_id=None,
        diff_report_id=str(calendar_diff["diff_report_id"]),
        approved_at_utc=(
            (now + timedelta(minutes=1))
            .isoformat()
            .replace("+00:00", "Z")
        ),
    )
    calendar_index_receipt = publish_calendar_index(
        candidate_calendar_receipt=calendar_receipt,
        activation_approval=activation,
        publisher=_publisher(boundary, operation_factory, "calendar-index"),
        expected_markets=("ES",),
        freshness_at=now,
    )
    active_pointer = active_pointer_payload(
        calendar_index_receipt,
        activation_approval_receipt_id=str(activation["approval_receipt_id"]),
        activated_at_utc=str(activation["approved_at"]),
    )
    (
        boundary.active_root / "configs" / "active_exchange_calendar.json"
    ).write_bytes(canonical_bytes(active_pointer) + b"\n")
    foundation = _foundation_orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection,
        feature_spec=spec,
        calendar_index_receipt=calendar_index_receipt,
    ).foundation_set_receipt
    census = _census(boundary, operation_factory, monkeypatch)
    _commit_fixture_repo(boundary.active_root)
    publisher = _publisher(boundary, operation_factory)
    monkeypatch.setattr(
        readiness_module,
        "_run_pinned_synthetic_suite",
        lambda _archive: (
            b"................................................................ 100 passed in 1.00s\n"
        ),
    )
    prerequisites = publish_readiness_prerequisites(
        boundary=boundary,
        publisher=publisher,
    )
    synthetic = prerequisites.synthetic_test_evidence_receipt
    engine = prerequisites.engine_registration_receipt
    isolation = prerequisites.isolation_evidence_receipt
    # These tests exercise the general readiness publication contract with a
    # compact synthetic schema-v6 fixture.  Schema-v7 empirical-observability
    # binding is covered separately by the fail-closed legacy-schema test.
    monkeypatch.setattr(
        readiness_module,
        "_calendar_readiness_codes",
        lambda _foundation, *, boundary: (),
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
    blueprint = build_foundation_research_blueprint(foundation, boundary=boundary)
    for payload in (rebuild, historical):
        assert payload["alpha_claim"] is False
        assert payload["candidate_claim"] is False
        assert payload["execution_authority_granted"] is False
        assert payload["live_trading_ready"] is False
        assert payload["real_history_execution_authorized"] is False
        assert payload["readiness_is_execution_authority"] is False
        assert payload["foundation_research_blueprint_id"] == blueprint.blueprint_id
        assert payload["query_manifest_id"] == blueprint.query_manifest_id
        safety = payload["safety_contract"]
        assert safety["hard_pauses"] == sorted(REQUIRED_HARD_PAUSES)
        assert safety["closed_research_lines"] == [
            dict(item) for item in CLOSED_RESEARCH_LINES
        ]
        assert all(value is False for value in safety["authority"].values())
        assert all(value is False for value in safety["claims"].values())
    assert rebuild["synthetic_test_evidence_receipt"] == synthetic.as_dict()
    census_payload = census.embedded_document("legacy_census.json", boundary)
    assert isinstance(census_payload, dict)
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


def test_synthetic_evidence_derives_and_rechecks_verbatim_pytest_output(
    boundary, operation_factory, monkeypatch
) -> None:
    _install_readiness_closure(boundary)
    _commit_fixture_repo(boundary.active_root)
    output = b"........................................................ 100 passed in 1.25s\n"
    receipt = _publish_fixture_synthetic_evidence(
        boundary=boundary,
        publisher=_publisher(boundary, operation_factory, "captured-tests"),
        monkeypatch=monkeypatch,
        output=output,
    )
    payload = load_synthetic_test_evidence(receipt, boundary=boundary)
    assert payload["test_attestation"]["passed_test_count"] == 100
    assert payload["test_attestation"]["test_output_size"] == len(output)
    execution = payload["test_execution_contract"]
    assert execution["fixed_environment"]["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert execution["pythonpath_role"] == "IMMUTABLE_COMMITTED_ARCHIVE_SRC_ONLY"
    assert "PYTEST_ADDOPTS" in execution["removed_environment"]
    closure_paths = {item["path"] for item in payload["test_closure"]}
    assert {
        "tests/conftest.py",
        "tests/test_legacy_trial_census.py",
        "tests/test_time_and_identity.py",
        "tests/test_trust_chain_hardening.py",
    }.issubset(closure_paths)
    manifest_path = boundary.active_root / receipt.manifest_path
    manifest_path.write_bytes(manifest_path.read_bytes() + b"tampered\n")
    with pytest.raises(IntegrityError, match="manifest"):
        load_synthetic_test_evidence(receipt, boundary=boundary)


def test_isolation_proof_rejects_hardlinked_mutable_descendant(
    boundary, operation_factory
) -> None:
    _install_readiness_closure(boundary)
    (boundary.active_root / "data").mkdir(exist_ok=True)
    (boundary.active_root / "state").mkdir(exist_ok=True)
    _commit_fixture_repo(boundary.active_root)
    source = boundary.active_root / "bundles" / "source.bin"
    alias = boundary.active_root / "bundles" / "alias.bin"
    source.write_bytes(b"not shared across repositories")
    try:
        os.link(source, alias)
    except OSError as exc:
        pytest.skip(f"hard links unavailable: {exc}")
    with pytest.raises(IntegrityError, match="hard-linked file"):
        _compute_isolation_proof(boundary)


def test_isolation_source_scan_rejects_literal_dynamic_stock_import(
    boundary,
) -> None:
    source = boundary.active_root / "src" / "futures_rebuild" / "dynamic.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        'import importlib\nimportlib.import_module("us_stocks_swing_model_v2.bad")\n',
        encoding="utf-8",
    )
    with pytest.raises(IntegrityError, match="stock-project import"):
        _scan_no_cross_import(boundary.active_root)


def test_missing_census_returns_only_blocker_and_publishes_no_state(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, _, engine, isolation, _, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )
    release_root = boundary.active_root / "manifests" / "data_releases"
    before = {
        path.relative_to(release_root).as_posix()
        for path in release_root.rglob("*.json")
    }

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
    assert {
        path.relative_to(release_root).as_posix()
        for path in release_root.rglob("*.json")
    } == before


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
    assert not (
        boundary.active_root / "manifests" / "data_releases" / "readiness"
    ).exists()


def test_legacy_foundation_is_classified_historical_observability_contract_not_bound(
    boundary, monkeypatch
) -> None:
    monkeypatch.setattr(
        readiness_module,
        "load_foundation_set",
        lambda _receipt, *, boundary: {
            "run_contract": {"repository_id": boundary.repository_id},
            "schema_version": "5.0.0",
        },
    )
    monkeypatch.setattr(
        readiness_module,
        "build_foundation_research_blueprint",
        lambda _receipt, *, boundary: object(),
    )
    monkeypatch.setattr(
        readiness_module,
        "committed_git_closure",
        lambda _root: {"git_closure_id": "0" * 64},
    )
    monkeypatch.setattr(readiness_module, "_safety_contract", lambda _boundary: {})
    monkeypatch.setattr(
        readiness_module,
        "load_exchange_calendar_policy",
        lambda _path: {},
    )
    legacy_receipt = SimpleNamespace(
        release_kind=readiness_module.FOUNDATION_SET_RELEASE_KIND
    )
    assessment = assess_readiness(
        boundary=boundary,
        foundation_set_receipt=legacy_receipt,
        engine_registration_receipt=None,
        isolation_evidence_receipt=None,
        legacy_census_release_receipt=None,
    )
    assert {
        (blocker.state, blocker.code) for blocker in assessment.blockers
    }.issuperset(
        {
            (
                "REBUILD_COMPLETE",
                "HISTORICAL_OBSERVABILITY_CONTRACT_NOT_BOUND",
            ),
            (
                "HISTORICAL_RESEARCH_READY",
                "HISTORICAL_OBSERVABILITY_CONTRACT_NOT_BOUND",
            ),
        }
    )


def test_tampered_legacy_census_returns_blocker_without_readiness_writes(
    boundary, operation_factory, monkeypatch
) -> None:
    foundation, _, engine, isolation, census, publisher = _prerequisites(
        boundary, operation_factory, monkeypatch
    )
    release_root = boundary.active_root / "manifests" / "data_releases"
    before = {
        path.relative_to(release_root).as_posix()
        for path in release_root.rglob("*.json")
    }
    census_path = boundary.active_root / census.manifest_path
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
    assert {
        path.relative_to(release_root).as_posix()
        for path in release_root.rglob("*.json")
    } == before


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

    different_test_run = _publish_fixture_synthetic_evidence(
        boundary=boundary,
        publisher=publisher,
        monkeypatch=monkeypatch,
        output=b"................................................................ 101 passed in 1.01s\n",
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
    substituted_payload = valid.historical_research_ready_receipt.embedded_document(
        "historical_research_ready.json", boundary
    )
    assert isinstance(substituted_payload, dict)
    substituted_payload = dict(substituted_payload)
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
    substituted_manifest = ReleaseManifest.build(
        stage,
        phase="readiness",
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
        embedded_documents={
            "historical_research_ready.json": substituted_payload
        },
    )
    substituted_receipt = VerifiedReleaseReceipt.from_manifest(
        publisher.publish(stage, substituted_manifest), boundary
    )
    with pytest.raises(IntegrityError, match="exact dependency closure"):
        load_historical_research_ready(substituted_receipt, boundary=boundary)

    engine_path = boundary.active_root / Path(engine_code_closure(REPO)[0]["path"])
    engine_path.write_bytes(engine_path.read_bytes() + b"\n")
    with pytest.raises(IntegrityError, match="one clean committed branch state"):
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
    payload_path = boundary.active_root / result.rebuild_complete_receipt.manifest_path
    payload_path.write_bytes(payload_path.read_bytes() + b" ")

    with pytest.raises(IntegrityError):
        load_rebuild_complete(result.rebuild_complete_receipt, boundary=boundary)
