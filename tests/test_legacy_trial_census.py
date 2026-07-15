import hashlib
import json
import shutil
import uuid
from pathlib import Path

import pytest

import futures_rebuild.legacy_trial_census as census_module
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.foundation.snapshot import PublishedSourceSnapshot
from futures_rebuild.legacy_trial_census import (
    EXPERIMENT_LEDGER_PATH,
    FEATURE_REGISTRY_PATH,
    FEATURE_STATUSES_PATH,
    MUTATION_PACKAGE_PATH,
    ORAC_FAILURE_ANALYSIS_PATH,
    ORAC_FAILURE_AUTOPSY_PATH,
    PHASE6_STATISTICAL_SUMMARY_PATH,
    TARGET_REGISTRY_PATH,
    TARGET_STATUSES_PATH,
    TERMINAL_DISTRIBUTIONAL_ALPHA_PATH,
    TERMINAL_DISTRIBUTIONAL_AUDIT_PATH,
    TERMINAL_DISTRIBUTIONAL_PROGRAM_ID,
    TERMINAL_DISTRIBUTIONAL_WFA_PATH,
    load_legacy_trial_census,
    publish_legacy_trial_census,
)
from futures_rebuild.migration import (
    SNAPSHOT_RECEIPT_STATUS,
    SNAPSHOT_RECEIPT_VERSION,
)
from futures_rebuild.release import AtomicPublisher, VerifiedReleaseReceipt
from futures_rebuild.trial import LegacyCensusReceipt


@pytest.fixture
def boundary() -> RepoBoundary:
    # Snapshot paths are deliberately long; use a disposable short drive-root
    # fixture so the synthetic tree remains below legacy Windows MAX_PATH.
    root = Path(Path.cwd().anchor) / f"cns-{uuid.uuid4().hex[:8]}"
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
        shutil.rmtree(root)


def _jsonl(rows: list[dict[str, object]]) -> bytes:
    return b"".join(canonical_bytes(row) + b"\n" for row in rows)


def _fixture_family(source: str) -> str:
    return f"legacy_research_fixture_{hashlib.sha256(source.encode()).hexdigest()[:12]}"


def _fixture_destination(source: str) -> str:
    return (
        f"evidence/legacy_research/by_family/{_fixture_family(source)}"
        f"{Path(source).suffix}"
    )


def _evidence_bytes(
    *, duplicate_target_row: bool = False
) -> dict[str, bytes]:
    target_rows: list[dict[str, object]] = []
    for trial_index in range(22):
        if trial_index == 0:
            target_rows.extend(
                [
                    {
                        "hypothesis_id": "target-h0",
                        "status": "STARTED",
                        "trial_id": "target-source-00",
                    },
                    {
                        "hypothesis_id": "target-h0",
                        "source_trial_id": "target-source-00",
                        "status": "FAILED",
                        "trial_id": "target-backfill-00",
                    },
                ]
            )
            continue
        base: dict[str, object] = {
            "hypothesis_id": f"target-h{trial_index % 10}",
            "trial_id": f"target-trial-{trial_index:02d}",
        }
        if trial_index != 21:
            base["source_trial_id"] = f"target-source-{trial_index:02d}"
        target_rows.extend(
            [
                {**base, "status": "STARTED"},
                {**base, "status": "FAILED"},
            ]
        )
    if duplicate_target_row:
        target_rows.append(dict(target_rows[-1]))
    evidence: dict[str, bytes] = {
        TARGET_REGISTRY_PATH: canonical_bytes(
            {
                "hypotheses": [
                    {
                        **(
                            {"evidence_path": "reports/audit_00.json"}
                            if index == 0
                            else {
                                "evidence_path": "reports/missing_target_report.json"
                            }
                            if index == 1
                            else {}
                        ),
                        "target_hypothesis_id": f"target-h{index}",
                    }
                    for index in range(17)
                ],
                "schema_version": "1.0.0",
            }
        )
        + b"\n",
        TARGET_STATUSES_PATH: _jsonl(target_rows),
        FEATURE_REGISTRY_PATH: canonical_bytes(
            {
                "hypotheses": [
                    {"hypothesis_id": "feature-h0"},
                    {"hypothesis_id": "feature-h1"},
                ],
                "schema_version": "1.0.0",
            }
        )
        + b"\n",
        FEATURE_STATUSES_PATH: _jsonl(
            [
                {
                    "hypothesis_id": f"feature-h{index % 2}",
                    "source_trial_id": f"feature-source-{index}",
                    "status": "FAILED",
                    "trial_id": f"feature-event-{index}",
                }
                for index in range(3)
            ]
        ),
    }
    audit_names = (
        "anti_overfit_audit.json",
        "anti_overfit_audit_with_drilldown.json",
        "anti_overfit_audit_refreshed.json",
        "anti_overfit_audit_data_audit_guard_tier1_smoke.json",
    )
    experiment_lines: list[bytes] = []
    for ordinal, audit_name in enumerate(audit_names, start=1):
        if ordinal == 2:
            experiment_lines.append(
                (
                    "{\"audit_report_path\":\"reports/experiments/"
                    f"{audit_name}\",\"diagnostic\":NaN,"
                    f"\"experiment_id\":\"legacy-experiment-{ordinal}\"}}"
                ).encode("utf-8")
            )
        else:
            experiment_lines.append(
                canonical_bytes(
                    {
                        "audit_report_path": f"reports/experiments/{audit_name}",
                        "experiment_id": f"legacy-experiment-{ordinal}",
                    }
                )
            )
    evidence[EXPERIMENT_LEDGER_PATH] = b"\n".join(experiment_lines) + b"\n"
    candidates = [
        {
            "append_to_experiment_ledger_allowed": False,
            "append_to_trial_statuses_allowed": False,
            "canonical_mutation_executed": False,
            "disposition": "EXCLUDE_FROM_CANONICAL_TRIAL_SEARCH_LEDGER",
            "evidence_paths": [f"reports/experiments/{audit_name}"],
            "row_id": f"experiment_ledger_{ordinal:03d}",
            "row_origin": "experiment_ledger",
            "trial_id": f"experiment_ledger_row_{ordinal:03d}",
        }
        for ordinal, audit_name in enumerate(audit_names, start=1)
    ]
    candidates.append(
        {
            "append_to_experiment_ledger_allowed": False,
            "append_to_trial_statuses_allowed": False,
            "canonical_mutation_executed": False,
            "disposition": "EXCLUDE_FROM_CANONICAL_TRIAL_SEARCH_LEDGER",
            "evidence_paths": [PHASE6_STATISTICAL_SUMMARY_PATH],
            "row_id": "current_wfa_phase8_statistical_run_001",
            "row_origin": "current_wfa_phase8_statistical_run",
            "trial_id": "tier1_core_phase6_full_predictions_20260706_current_line",
        }
    )
    evidence[MUTATION_PACKAGE_PATH] = canonical_bytes(
        {
            "canonical_mutation_package": {
                "exclusion_disposition_candidates": candidates
            }
        }
    ) + b"\n"
    evidence[PHASE6_STATISTICAL_SUMMARY_PATH] = canonical_bytes(
        {
            "diagnostic_type": "phase9_statistical_validity",
            "failure_count": 5,
            "model_promotion_allowed": False,
            "research_only": True,
            "run": "tier1_core_phase6_full_predictions_20260706",
            "statistical_validity_ready": False,
            "status": "FAIL",
        }
    ) + b"\n"
    evidence[ORAC_FAILURE_ANALYSIS_PATH] = (
        "# opening_range_acceptance_continuation_30m_v1 failure analysis\n"
        "Phase 6 WFA expansion: prediction_count=72539 and fold_count=4.\n"
        "The costed net_return_dollars=-80468.5; all 4 folds were net negative.\n"
        "This report does not recommend tuning, rerunning, or promotion.\n"
    ).encode("utf-8")
    evidence[ORAC_FAILURE_AUTOPSY_PATH] = (
        "# opening_range_acceptance_continuation_30m_v1 autopsy\n"
        "WFA artifacts: 72539; net -80468.50.\n"
        "FIRST_TOUCH_FEASIBILITY_NO_GO with 0/36.\n"
        "Diagnostic-only: this autopsy does not approve rescue work or promotion.\n"
    ).encode("utf-8")
    wfa = {
        "evaluation_policy_sha256": "1" * 64,
        "models_config_sha256": "2" * 64,
        "phase6_policy_sha256": "3" * 64,
        "program_id": TERMINAL_DISTRIBUTIONAL_PROGRAM_ID,
        "projection_manifest_sha256": "4" * 64,
        "promotion_allowed": False,
        "research_only": True,
        "source_sha256": "5" * 64,
        "split_plan_sha256": "6" * 64,
        "status": "PASS",
    }
    wfa_bytes = canonical_bytes(wfa) + b"\n"
    evidence[TERMINAL_DISTRIBUTIONAL_WFA_PATH] = wfa_bytes
    evidence[TERMINAL_DISTRIBUTIONAL_AUDIT_PATH] = canonical_bytes(
        {
            "failure_count": 223,
            "prediction_artifact": {
                "path": "reports/missing_distributional_predictions.parquet",
                "sha256": "7" * 64,
            },
            "prediction_manifest": {
                "path": TERMINAL_DISTRIBUTIONAL_WFA_PATH.removeprefix(
                    "evidence/legacy_research/"
                ),
                "sha256": hashlib.sha256(wfa_bytes).hexdigest(),
            },
            "program_id": TERMINAL_DISTRIBUTIONAL_PROGRAM_ID,
            "status": "FAIL",
        }
    ) + b"\n"
    evidence[TERMINAL_DISTRIBUTIONAL_ALPHA_PATH] = canonical_bytes(
        {
            "audit_failure_count": 223,
            "audit_status": "FAIL",
            "costs_sha256": "8" * 64,
            "decision": "REJECT",
            "model_selection_allowed": False,
            "policy_id": "distributional-alpha-policy-v1",
            "policy_sha256": "9" * 64,
            "program_id": TERMINAL_DISTRIBUTIONAL_PROGRAM_ID,
            "promotion_allowed": False,
        }
    ) + b"\n"
    for index in range(12):
        path = f"reports/audit_{index:02d}.json"
        evidence[path] = canonical_bytes({"audit": index}) + b"\n"
    assert len(evidence) == 24
    return evidence


def _snapshot(
    *,
    boundary,
    monkeypatch,
    duplicate_target_row: bool = False,
    omit_manifest_binding: str | None = None,
) -> tuple[PublishedSourceSnapshot, str]:
    evidence = _evidence_bytes(duplicate_target_row=duplicate_target_row)
    entries: list[dict[str, object]] = []
    snapshot_contents: dict[str, bytes] = {}
    for source, content in sorted(evidence.items()):
        family = _fixture_family(source)
        destination = _fixture_destination(source)
        snapshot_contents[destination] = content
        entries.append(
            {
                "destination": destination,
                "disposition": "legacy_trial_census_evidence_only",
                "expected_bytes": len(content),
                "expected_files": 1,
                "expected_sha256": hashlib.sha256(content).hexdigest(),
                "family": family,
                "kind": "file",
                "source": source,
            }
        )
    if omit_manifest_binding is not None:
        entries = [
            entry for entry in entries if entry["source"] != omit_manifest_binding
        ]
        replacement_content = canonical_bytes({"replacement": True}) + b"\n"
        replacement_source = "reports/replacement.json"
        replacement_path = _fixture_destination(replacement_source)
        snapshot_contents[replacement_path] = replacement_content
        entries.append(
            {
                "destination": replacement_path,
                "disposition": "legacy_trial_census_evidence_only",
                "expected_bytes": len(replacement_content),
                "expected_files": 1,
                "expected_sha256": hashlib.sha256(replacement_content).hexdigest(),
                "family": _fixture_family(replacement_source),
                "kind": "file",
                "source": replacement_source,
            }
        )
    manifest = {
        "copy_authorized": True,
        "destination_root": str(
            boundary.active_root / "data" / "vault" / ".staging" / "copy"
        ),
        "entries": entries,
        "migration_id": "synthetic-legacy-census",
        "policy": {
            "follow_links": False,
            "operation": "copy_only",
            "overwrite": False,
            "require_source_stable_during_copy": True,
            "verify_destination_sha256": True,
        },
        "source_root": str(boundary.legacy_roots[0]),
    }
    manifest_hash = sha256_json(manifest)
    config_root = boundary.active_root / "configs"
    config_root.mkdir(parents=True, exist_ok=True)
    (config_root / "migration_manifest_authorized.json").write_bytes(
        canonical_bytes(manifest) + b"\n"
    )
    monkeypatch.setattr(
        census_module, "AUTHORIZED_MIGRATION_MANIFEST_SHA256", manifest_hash
    )

    file_records = [
        {
            "path": path,
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
        }
        for path, content in sorted(snapshot_contents.items())
    ]
    semantics = {
        "approval_id": "a" * 64,
        "files": file_records,
        "files_index_sha256": sha256_json(file_records),
        "inventory_sha256": "b" * 64,
        "manifest_sha256": manifest_hash,
        "migration_implementation_sha256": "c" * 64,
        "receipt_version": SNAPSHOT_RECEIPT_VERSION,
        "status": SNAPSHOT_RECEIPT_STATUS,
        "total_bytes": sum(item["size"] for item in file_records),
        "total_files": len(file_records),
        "user_authorization_id": "d" * 64,
    }
    snapshot_id = sha256_json(semantics)
    receipt = {**semantics, "source_snapshot_id": snapshot_id}
    root = (
        boundary.active_root
        / "data"
        / "vault"
        / "source_snapshots"
        / snapshot_id
    )
    root.mkdir(parents=True)
    for relative, content in snapshot_contents.items():
        path = root / Path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    (root / "SOURCE_SNAPSHOT_RECEIPT.json").write_bytes(
        canonical_bytes(receipt) + b"\n"
    )
    return PublishedSourceSnapshot.open(root, boundary=boundary), manifest_hash


def _publisher(boundary, operation_factory) -> AtomicPublisher:
    return AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "census",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "census.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )


def _source_contract(boundary, *, downloads_authorized: bool = False) -> Path:
    path = boundary.active_root / "configs" / "source_contract.json"
    path.write_bytes(
        canonical_bytes(
            {
                "active_repository": str(boundary.active_root),
                "discovery_policy": "manifest_only",
                "legacy_repository": str(boundary.legacy_roots[0]),
                "links_allowed": False,
                "provider": {
                    "downloads_authorized": downloads_authorized,
                    "paid_calls_authorized": False,
                },
                "recursive_fallbacks_allowed": False,
            }
        )
        + b"\n"
    )
    return path


def test_census_is_canonical_immutable_unresolved_and_snapshot_bound(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, _ = _snapshot(boundary=boundary, monkeypatch=monkeypatch)
    publisher = _publisher(boundary, operation_factory)

    receipt = publish_legacy_trial_census(
        snapshot=snapshot, boundary=boundary, publisher=publisher
    )
    loaded = load_legacy_trial_census(
        receipt, snapshot=snapshot, boundary=boundary
    )
    repeated = publish_legacy_trial_census(
        snapshot=snapshot, boundary=boundary, publisher=publisher
    )
    census_receipt = LegacyCensusReceipt.from_release(receipt, boundary)

    assert repeated == receipt
    assert loaded["status"] == "INVALID_TRIAL_CENSUS_UNRESOLVED"
    assert loaded["exact_count_state"] == "INDETERMINATE"
    assert loaded["preregistered_penalty_count"] == 0
    assert loaded["trusted_gate"] is False
    assert loaded["observed_attempt_floor"] == 39
    assert loaded["counting_rule"]["category_counts"] == {
        "EXPERIMENT_LEDGER_RUN": 4,
        "FEATURE_STATUS_TRIAL": 3,
        "TARGET_REGISTRY_ONLY_HYPOTHESIS": 7,
        "TARGET_STATUS_TRIAL": 22,
        "TERMINAL_DISTRIBUTIONAL_PROGRAM": 1,
        "TERMINAL_ORAC_PROGRAM": 1,
        "TERMINAL_PHASE6_PROGRAM": 1,
    }
    assert [item["path"] for item in loaded["unresolved_references"]] == [
        "reports/experiments/anti_overfit_audit.json",
        "reports/experiments/anti_overfit_audit_data_audit_guard_tier1_smoke.json",
        "reports/experiments/anti_overfit_audit_refreshed.json",
        "reports/experiments/anti_overfit_audit_with_drilldown.json",
        "reports/missing_distributional_predictions.parquet",
        "reports/missing_target_report.json",
    ]
    assert census_receipt.observed_attempt_floor == 39
    assert census_receipt.counting_attempt_count == 0
    assert census_receipt.exact_count_state == "INDETERMINATE"
    assert census_receipt.trusted_gate is False
    assert census_receipt.source_snapshot_id == snapshot.source_snapshot_id
    assert census_receipt.unresolved_reference_count == 6
    census_receipt.verify()


def test_census_fails_on_duplicate_provenance_rows(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, _ = _snapshot(
        boundary=boundary,
        monkeypatch=monkeypatch,
        duplicate_target_row=True,
    )
    with pytest.raises(IntegrityError, match="duplicate provenance rows"):
        publish_legacy_trial_census(
            snapshot=snapshot,
            boundary=boundary,
            publisher=_publisher(boundary, operation_factory),
        )


def test_census_fails_on_tamper_and_missing_core_evidence(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, _ = _snapshot(boundary=boundary, monkeypatch=monkeypatch)
    snapshot.file(_fixture_destination(TARGET_STATUSES_PATH)).path.write_bytes(b"{}\n")
    with pytest.raises(IntegrityError, match="snapshot"):
        publish_legacy_trial_census(
            snapshot=snapshot,
            boundary=boundary,
            publisher=_publisher(boundary, operation_factory),
        )


def test_census_fails_when_authorized_contract_substitutes_a_core_ledger(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, _ = _snapshot(
        boundary=boundary,
        monkeypatch=monkeypatch,
        omit_manifest_binding=FEATURE_STATUSES_PATH,
    )
    with pytest.raises(IntegrityError, match="missing, duplicated, or unexpected"):
        publish_legacy_trial_census(
            snapshot=snapshot,
            boundary=boundary,
            publisher=_publisher(boundary, operation_factory),
        )


def test_cli_defaults_to_read_only_canonical_assessment(
    boundary, monkeypatch, capsys
) -> None:
    snapshot, _ = _snapshot(boundary=boundary, monkeypatch=monkeypatch)
    source_contract = _source_contract(boundary)

    result = census_module.main(
        [
            "--repository-root",
            str(boundary.active_root),
            "--source-contract",
            str(source_contract),
            "--source-snapshot-root",
            str(snapshot.root),
        ]
    )
    output = capsys.readouterr().out.encode("utf-8")
    summary = json.loads(output.decode("utf-8"))

    assert result == 0
    assert output == canonical_bytes(summary) + b"\n"
    assert summary == {
        "census_sha256": summary["census_sha256"],
        "exact_count_state": "INDETERMINATE",
        "historical_execution_authorized": False,
        "mode": "READ_ONLY_ASSESSMENT",
        "observed_attempt_floor": 39,
        "paid_provider_call_count": 0,
        "preregistered_penalty_count": 0,
        "published": False,
        "real_history_trust_granted": False,
        "release_receipt": None,
        "source_evidence_sha256": summary["source_evidence_sha256"],
        "source_snapshot_id": snapshot.source_snapshot_id,
        "status": "INVALID_TRIAL_CENSUS_UNRESOLVED",
        "trusted_gate": False,
        "unresolved_reference_count": 6,
    }
    assert not (boundary.active_root / "data" / "vault" / "releases").exists()
    assert not (boundary.active_root / "state" / "locks").exists()


def test_cli_publish_is_explicit_controlled_non_alpha_and_stays_untrusted(
    boundary, monkeypatch, capsys
) -> None:
    snapshot, _ = _snapshot(boundary=boundary, monkeypatch=monkeypatch)
    source_contract = _source_contract(boundary)
    observed: dict[str, object] = {}
    original_issue = OperationReceipt.issue_local

    def capture_issue(
        receipt_boundary,
        *,
        operation,
        classification,
        scope=None,
    ):
        observed.update(
            {
                "classification": classification,
                "operation": operation,
                "scope": dict(scope or {}),
            }
        )
        receipt = original_issue(
            receipt_boundary,
            operation=operation,
            classification=classification,
            scope=scope,
        )
        observed["receipt"] = receipt
        return receipt

    monkeypatch.setattr(
        census_module.OperationReceipt,
        "issue_local",
        staticmethod(capture_issue),
    )
    result = census_module.main(
        [
            "--repository-root",
            str(boundary.active_root),
            "--source-contract",
            str(source_contract),
            "--source-snapshot-root",
            str(snapshot.root),
            "--publish",
        ]
    )
    output = capsys.readouterr().out.encode("utf-8")
    summary = json.loads(output.decode("utf-8"))
    release_receipt = VerifiedReleaseReceipt.from_dict(summary["release_receipt"])
    census_receipt = LegacyCensusReceipt.from_release(release_receipt, boundary)

    assert result == 0
    assert output == canonical_bytes(summary) + b"\n"
    assert observed["operation"] == "PUBLISH_RELEASE"
    assert observed["classification"] is (
        OperationClassification.CONTROLLED_REBUILD_NON_ALPHA
    )
    assert observed["scope"] == {
        "census_sha256": summary["census_sha256"],
        "exact_count_state": "INDETERMINATE",
        "historical_execution_authorized": "false",
        "preregistered_penalty_count": "0",
        "source_contract_sha256": hashlib.sha256(
            source_contract.read_bytes()
        ).hexdigest(),
        "source_snapshot_id": snapshot.source_snapshot_id,
        "status": "INVALID_TRIAL_CENSUS_UNRESOLVED",
        "trusted_gate": "false",
    }
    assert observed["receipt"].externally_authorized is False
    assert summary["mode"] == "PUBLISHED_UNRESOLVED_CENSUS"
    assert summary["published"] is True
    assert summary["historical_execution_authorized"] is False
    assert summary["real_history_trust_granted"] is False
    assert summary["paid_provider_call_count"] == 0
    assert summary["observed_attempt_floor"] == 39
    assert summary["trusted_gate"] is False
    assert census_receipt.exact_count_state == "INDETERMINATE"
    assert census_receipt.preregistered_penalty_count == 0
    assert census_receipt.trusted_gate is False


def test_cli_rejects_source_contract_that_authorizes_downloads(
    boundary, monkeypatch
) -> None:
    snapshot, _ = _snapshot(boundary=boundary, monkeypatch=monkeypatch)
    source_contract = _source_contract(boundary, downloads_authorized=True)
    with pytest.raises(ContractError, match="offline census safety"):
        census_module.main(
            [
                "--repository-root",
                str(boundary.active_root),
                "--source-contract",
                str(source_contract),
                "--source-snapshot-root",
                str(snapshot.root),
                "--publish",
            ]
        )
    assert not (boundary.active_root / "data" / "vault" / "releases").exists()
