import json
import hashlib
import os
from collections.abc import Callable
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.identity import ActualContractIdentity
from futures_rebuild.data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from tests.windows_temp_root import (
    WindowsTestRootUnavailable,
    create_windows_test_root,
)


UTC = timezone.utc


LEGACY_PATH_PARTS = (
    "closure_workflow",
    "legacy_",
    "successor",
    "durable_windows_task_transport",
    "task_scheduler_registration_authority",
    "active_data_full_transport",
    "tests/test_active_data_",
    "tests/test_calendar_",
    "tests/test_dbn_flat_",
    "tests/test_migration",
    "tests/test_phase1a_layout",
    "tests/test_successor",
    "tests/live/test_activate_",
    "tests/live/test_databento_",
    "tests/live/test_install_",
    "tests/live/test_live_cockpit",
)
CURRENT_TEST_FILES = {
    "test_controlled_rebuild_authorization.py",
    "test_data_layout.py",
    "test_dependency_lock.py",
    "test_operational_documents.py",
    "test_profiles_and_pipeline.py",
    "test_project_isolation.py",
    "test_repo_boundary.py",
    "test_runtime_environment.py",
    "test_schemas.py",
    "test_source_contract.py",
    "test_source_symbology.py",
    "test_time_and_identity.py",
    "test_windows_temp_root.py",
    "test_workflow_lanes.py",
}
LEGACY_RESEARCH_TEST_FILES = {
    "test_alpha_ladder_feature_gap_diagnostic.py",
    "test_alpha_ladder_reported_trade_exit_readiness.py",
    "test_cash_open_calendar_grid_publication.py",
    "test_cash_open_calendar_publication.py",
    "test_cash_open_source_compatibility_census_v2.py",
    "test_reported_bar_fixed_horizon_census.py",
    "test_reported_bar_trade_triggered_census.py",
    "test_tier1_authoritative_certified_lifecycle.py",
    "test_tier1_authoritative_lifecycle.py",
    "test_tier1_authoritative_stable_lifecycle.py",
    "test_tier1_authoritative_terminal_lifecycle.py",
    "test_tier1_bracket_v10_execution.py",
    "test_tier1_bracket_v10_registration.py",
    "test_tier1_bracket_v11_execution.py",
    "test_tier1_bracket_v11_registration.py",
    "test_tier1_bracket_v12_execution.py",
    "test_tier1_bracket_v12_registration.py",
    "test_tier1_bracket_v5.py",
    "test_tier1_bracket_v4.py",
    "test_tier1_bracket_v6.py",
    "test_tier1_bracket_v7.py",
    "test_tier1_bracket_v8.py",
    "test_tier1_bracket_v9.py",
    "test_tier1_final_decision_validity.py",
    "test_tier1_standard_only_execution.py",
    "test_tier1_standard_only_lifecycle.py",
    "test_tier1_standard_only_protocol.py",
    "test_trial_bundle_inference.py",
}
LEGACY_RESEARCH_TEST_NODES = {
    (
        "tests/test_overnight_inventory_reversal_preexecution_census_v2.py::"
        "test_parallel_successor_plan_is_hash_bound_and_preserves_consumed_attempt"
    ),
}
CURRENT_HIGH_RISK_TEST_FILES = {
    "test_alpha_ladder_full_regular_source_observable_successor.py",
}
LOCAL_EVIDENCE_TEST_FILES = {
    "test_tier1_economics_only.py",
}
LOCAL_EVIDENCE_TEST_NODES = {
    "tests/live/test_package_candidate.py::test_package_candidate_plan_binds_reviewed_bytes_and_is_create_only",
    "tests/test_alpha_ladder_full_regular_tier0.py::test_live_evidence_is_transition_stable_when_present",
    "tests/test_alpha_ladder_reported_trade_exit_tier0.py::test_live_evidence_is_transition_stable_when_present",
    "tests/test_cash_open_impulse_pre_registration_remediation.py::test_catalog_inventory_terminalizes_absent_and_quarantined_pairs",
    "tests/test_cash_open_impulse_pre_registration_remediation.py::test_prepared_41_market_plan_is_exact_and_fail_closed",
    "tests/test_cme_calendar_source_adequacy.py::test_prepared_successor_is_hash_bound_inactive_and_has_exact_coverage",
    "tests/test_cme_calendar_source_adequacy.py::test_jan1_2019_recovery_is_exact_inactive_and_complete",
    "tests/test_cme_calendar_source_adequacy.py::test_recovered_schedule_closes_every_bound_family",
    "tests/test_cme_calendar_source_adequacy.py::test_recovered_schedule_fails_closed_when_staged_raw_bytes_change",
    "tests/test_foundation_historical_observability.py::test_coverage_uses_manifest_observability_without_calendar_claims",
    "tests/test_historical_checkpoint_calendar.py::test_published_pointer_and_dependency_closure_verify_when_present",
    "tests/test_overnight_inventory_reversal_preexecution_census.py::test_consumed_serial_plan_is_preserved_and_not_reusable",
    "tests/test_phase8_economics_index.py::test_live_foundation_selection_is_explicit_and_complete",
    "tests/test_tier1_bracket_pipeline.py::test_live_preparation_refuses_to_register_the_accepted_trial_twice",
    "tests/test_tier1_bracket_post_audit.py::test_contract_is_post_audit_provider_neutral_and_closure_is_void",
    "tests/test_tier1_bracket_v12_source_census.py::test_v12_source_census_catalog_and_selection_are_frozen",
    "tests/test_tier1_frozen_diagnostic_recovery.py::test_exact_failed_feature_complete_target_set_is_frozen",
    "tests/test_tier1_frozen_diagnostic_recovery.py::test_diagnostic_catalog_binds_only_target_cells_and_records_absent_trades",
    "tests/test_tier1_frozen_diagnostic_recovery.py::test_plan_is_hash_bound_and_authorizes_no_row_read_by_itself",
    "tests/test_tier1_frozen_trial_pipeline.py::test_synthetic_verification_binds_the_complete_applicable_test_tree",
    "tests/test_tier1_phase8_preparation.py::test_preparation_pins_the_active_apex_risk_profile",
    "tests/test_tier1_phase8_readiness.py::test_readiness_audit_blocks_obsolete_five_minute_predictions",
    "tests/test_tier1_phase8_readiness.py::test_bracket_readiness_reports_registered_trial_and_next_boundary",
    "tests/test_tier1_preexecution_recovery_feasibility.py::test_real_gap_target_and_canonical_catalog_bindings_are_exact",
    "tests/test_tier1_preexecution_source_certification.py::test_real_operation_plan_is_hash_bound_and_forbids_research_actions",
    "tests/test_tier1_trade_triggered_trial_design.py::test_declaration_is_source_selected_and_nonregisterable",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--local-evidence-manifest",
        action="store",
        default=None,
        help=(
            "JSON manifest binding the explicit machine-local evidence root and "
            "SHA-256 of every file used by the local_evidence lane"
        ),
    )


def _load_local_evidence_manifest(config: pytest.Config) -> Path:
    manifest_arg = config.getoption("--local-evidence-manifest")
    if not manifest_arg:
        raise pytest.UsageError(
            "local_evidence is fail-closed: provide --local-evidence-manifest"
        )
    manifest_path = Path(manifest_arg).resolve()
    if not manifest_path.is_file():
        raise pytest.UsageError(f"local evidence manifest is missing: {manifest_path}")
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise pytest.UsageError(f"invalid local evidence manifest: {exc}") from exc
    if document.get("schema_version") != "local-evidence-manifest-1.0.0":
        raise pytest.UsageError("unsupported local evidence manifest schema")
    root_value = document.get("evidence_root")
    files = document.get("files")
    if not isinstance(root_value, str) or not Path(root_value).is_absolute():
        raise pytest.UsageError("local evidence root must be an absolute path")
    if not isinstance(files, dict) or not files:
        raise pytest.UsageError("local evidence manifest must bind at least one file")
    evidence_root = Path(root_value).resolve()
    if not evidence_root.is_dir():
        raise pytest.UsageError(f"local evidence root is missing: {evidence_root}")
    for relative, expected_sha in sorted(files.items()):
        if not isinstance(relative, str) or not isinstance(expected_sha, str):
            raise pytest.UsageError("local evidence file bindings must be strings")
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise pytest.UsageError(f"unsafe local evidence path: {relative}")
        candidate = (evidence_root / relative_path).resolve()
        try:
            candidate.relative_to(evidence_root)
        except ValueError as exc:
            raise pytest.UsageError(f"escaped local evidence root: {relative}") from exc
        if not candidate.is_file():
            raise pytest.UsageError(f"bound local evidence file is missing: {relative}")
        actual_sha = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if actual_sha != expected_sha:
            raise pytest.UsageError(f"local evidence hash mismatch: {relative}")
    setattr(config, "_local_evidence_root", evidence_root)
    return evidence_root


def _lane_for(item: pytest.Item) -> str:
    path = item.path.as_posix()
    if not path.endswith(".py") or "/tests/" not in path:
        raise pytest.UsageError(f"test item has no classified source path: {item.nodeid}")
    if item.nodeid in LOCAL_EVIDENCE_TEST_NODES or item.path.name in LOCAL_EVIDENCE_TEST_FILES:
        return "local_evidence"
    if item.nodeid in LEGACY_RESEARCH_TEST_NODES:
        return "legacy"
    if item.path.name in CURRENT_HIGH_RISK_TEST_FILES:
        return "high_risk"
    if item.path.name in LEGACY_RESEARCH_TEST_FILES or any(
        part in path for part in LEGACY_PATH_PARTS
    ):
        return "legacy"
    if item.path.name in CURRENT_TEST_FILES:
        return "current"
    return "high_risk"


def pytest_configure(config: pytest.Config) -> None:
    """Require a short Windows test root before collection begins.

    A process-specific root avoids collisions when separate Codex tasks test the
    repository concurrently. An explicit command-line ``--basetemp`` remains an
    intentional override. Never fall back below the repository: that path is
    long enough to turn one environment denial into broad MAX_PATH failures.
    """

    if hasattr(config, "addinivalue_line"):
        config.addinivalue_line(
            "markers",
            "local_evidence: exact machine-local evidence checks requiring an explicit hash manifest",
        )
    if os.name == "nt" and config.option.basetemp is None:
        try:
            candidate = create_windows_test_root("f")
        except WindowsTestRootUnavailable as exc:
            raise pytest.UsageError(str(exc)) from exc
        config.option.basetemp = str(candidate)
    if not getattr(config.option, "markexpr", ""):
        config.option.markexpr = "current"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Classify every test into one mutually exclusive operating lane."""

    root = Path(__file__).resolve().parents[1]
    published_v6 = root / (
        "state/trial_registry/tier1_bracket_successor_v6/"
        "c92c5a6ecfd96a00d0cf89aa02319878b479dad6c6e21b703e54bd55943a8608.json"
    )
    for item in items:
        lane = _lane_for(item)
        item.add_marker(getattr(pytest.mark, lane))
        if (
            published_v6.exists()
            and item.nodeid.endswith(
                "test_tier1_bracket_v6.py::"
                "test_v5_is_preserved_and_v6_prepares_without_publication"
            )
        ):
            item.add_marker(pytest.mark.xfail(
                reason=(
                    "registered V6 prepublication-only assertion is historical "
                    "after create-only publication"
                ),
                strict=True,
            ))
    markexpr = getattr(config.option, "markexpr", "")
    if "local_evidence" in markexpr:
        _load_local_evidence_manifest(config)


def pytest_runtest_setup(item: pytest.Item) -> None:
    if item.get_closest_marker("local_evidence") is not None and not hasattr(
        item.config, "_local_evidence_root"
    ):
        _load_local_evidence_manifest(item.config)


@pytest.fixture
def local_evidence_root(pytestconfig: pytest.Config) -> Path:
    root = getattr(pytestconfig, "_local_evidence_root", None)
    if root is None:
        root = _load_local_evidence_manifest(pytestconfig)
    return root


@pytest.fixture
def short_test_root_factory(
    tmp_path_factory: pytest.TempPathFactory,
) -> Callable[[str], Path]:
    """Create roots for fixtures whose prescribed trees approach MAX_PATH."""

    def create(prefix: str) -> Path:
        if os.name != "nt":
            return tmp_path_factory.mktemp(prefix.rstrip("-"))
        try:
            return create_windows_test_root(prefix)
        except WindowsTestRootUnavailable as exc:
            raise pytest.UsageError(str(exc)) from exc

    return create


@pytest.fixture
def boundary(tmp_path) -> RepoBoundary:
    active = tmp_path / "active"
    legacy = tmp_path / "legacy"
    stock = tmp_path / "stock"
    active.mkdir()
    legacy.mkdir()
    stock.mkdir()
    (active / "configs").mkdir()
    (active / "bundles").mkdir()
    return RepoBoundary(active.resolve(), (legacy.resolve(),), (stock.resolve(),))


@pytest.fixture
def operation_factory(boundary):
    def issue(
        operation: str,
        *,
        classification: OperationClassification = (
            OperationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
        scope: dict[str, str] | None = None,
    ) -> OperationReceipt:
        return OperationReceipt.issue_local(
            boundary,
            operation=operation,
            classification=classification,
            scope=scope,
        )

    return issue


@pytest.fixture
def release_factory(boundary, operation_factory):
    counter = 0

    def publish(
        *,
        release_kind: str,
        filename: str,
        content: bytes | str | dict | list,
        schema_version: str = "1.0.0",
        metadata: dict | None = None,
        phase: str = "evaluations",
        logical_path: str | None = None,
        source_release_ids: tuple[str, ...] = (),
        embedded_documents: dict | None = None,
    ) -> tuple[object, VerifiedReleaseReceipt]:
        nonlocal counter
        counter += 1
        publisher = AtomicPublisher(
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
            lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
        )
        stage = publisher.create_stage("synthetic")
        documents = dict(embedded_documents or {})
        staged_paths: dict[str, str] = {}
        logical_paths: dict[str, str] = {}
        if phase == "evaluations" and logical_path is None:
            routed = {
                "actual_contract_definitions": (
                    "reference",
                    f"data/reference/definitions/{Path(filename).name}",
                ),
                "actual_contract_economics": (
                    "reference",
                    f"data/reference/economics/{Path(filename).name}",
                ),
                "futures_phase2_causal_interval": (
                    "causally_gated_normalized",
                    f"data/causally_gated_normalized/ES/2026/1m/{Path(filename).name}",
                ),
                "feature_release": (
                    "features",
                    f"data/features/synthetic/ES/2026/1m/{Path(filename).name}",
                ),
            }.get(release_kind)
            if routed is not None:
                phase, logical_path = routed
        if release_kind == "versioned_session_policy" and not documents:
            phase = "controls"
            documents[filename] = content
        elif not documents:
            path = stage / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                path.write_bytes(content)
            elif isinstance(content, str):
                path.write_text(content, encoding="utf-8")
            else:
                path.write_text(
                    json.dumps(content, sort_keys=True, separators=(",", ":")),
                    encoding="utf-8",
                )
            logical = logical_path or (
                f"data/evaluations/SYNTHETIC/{counter:08d}/fold-0001/{Path(filename).name}"
            )
            logical_paths[filename] = logical
            staged_paths[logical] = filename
        manifest = ReleaseManifest.build(
            stage,
            phase=phase,
            release_kind=release_kind,
            schema_version=schema_version,
            logical_paths=logical_paths,
            source_release_ids=source_release_ids,
            embedded_documents=documents,
            metadata=metadata,
        )
        manifest_path = publisher.publish(
            stage, manifest, staged_paths=staged_paths or None
        )
        receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
        if manifest.files:
            payload_path = receipt.resolve_unique_filename(Path(filename).name, boundary)
            release_root: object = payload_path.parent
        else:
            release_root = manifest_path.parent
        return release_root, receipt

    return publish


@pytest.fixture
def contract() -> ActualContractIdentity:
    return ActualContractIdentity(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=12345,
        instrument_id_date_utc=date(2026, 7, 14),
        exchange_session_date=date(2026, 7, 14),
        raw_symbol="ESZ6",
        exchange="XCME",
        definition_release_id="d" * 64,
        definition_manifest_sha256="a" * 64,
        definition_row_id="b" * 64,
        currency="USD",
        multiplier=Decimal("50"),
        min_tick=Decimal("0.25"),
    )


@pytest.fixture
def decision() -> datetime:
    return datetime(2026, 7, 14, 15, 1, tzinfo=UTC)
