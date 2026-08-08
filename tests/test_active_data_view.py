from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import futures_rebuild.active_data_view as active
from futures_rebuild.active_data_view import (
    CERTIFICATION_STATE,
    COHORT_PERMISSIONS,
    CatalogEntry,
    UpdateMode,
    build_access_policy_binding,
    build_catalog,
    build_content_validation_receipt,
    build_pending_approval,
    build_plan,
    build_sidecar,
    classify_update,
    cohort_for_year,
    content_receipt_reusable,
    disposition_for,
    publish_full_successor,
    publish_append_only,
    publish_initial,
    resolve,
    validate_access_policy_binding,
    validate_catalog,
    validate_content_validation_receipt,
    verify_approval,
    verify_view,
)
from futures_rebuild.canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation


H = {
    "active": "1" * 64,
    "foundation": "2" * 64,
    "foundation_manifest": "3" * 64,
    "implementation": "4" * 64,
    "environment": "5" * 64,
    "semantic": "6" * 64,
    "universe": "7" * 64,
    "schema": "8" * 64,
    "rows": "9" * 64,
}


def _plan(*, mode: UpdateMode, entries: list[dict[str, object]]) -> dict[str, object]:
    return build_plan(
        operation="PUBLISH_CAUSAL_ACTIVE_VIEW",
        mode=mode,
        foundation_release_id=H["foundation"],
        foundation_manifest_sha256=H["foundation_manifest"],
        semantic_bindings={"policy": H["semantic"]},
        entries=entries,
        limits={
            "maximum_bytes": 100_000,
            "maximum_duration_seconds": 60,
            "maximum_files": 100,
            "maximum_rows": 100,
            "maximum_workers": 1,
        },
        forbidden_actions=[
            "ARCHIVE",
            "DELETE",
            "HOLDOUT_OR_FORWARD_ACCESS",
            "OUTCOME_ACCESS",
            "PROVIDER_CALL",
        ],
        outputs=["data/active/catalog.json"],
        implementation_bindings={"active_data_view.py": H["implementation"]},
        environment_bindings={"lock": H["environment"]},
        recovery_boundary="LAST_KNOWN_GOOD_OR_AUTHORITATIVE_ABSENT",
    )


def _approval(plan: dict[str, object]) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T20:00:00Z",
        "operation": plan["operation"],
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_json(plan),
        "schema_version": active.APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "a" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _content(market: str, year: int) -> dict[str, object]:
    return build_content_validation_receipt(
        market=market,
        year=year,
        coverage_start=f"{year}-01-01",
        coverage_end=f"{year + 1}-01-01",
        source_bindings=[
            {
                "causal_release_id": "b" * 64,
                "dbn_release_id": "c" * 64,
                "raw_release_id": "d" * 64,
            }
        ],
        semantic_bindings={"policy": H["semantic"]},
        implementation_bindings={"validator": H["implementation"]},
        environment_bindings={"lock": H["environment"]},
        canonical_schema_fingerprint=H["schema"],
        canonical_row_hash=H["rows"],
        row_count=2,
        checks={
            "causality": "PASS",
            "lineage": "PASS",
            "ohlcv": "PASS",
            "ordering": "PASS",
        },
        limitations=["EMPIRICAL_OBSERVABILITY_NOT_OFFICIAL_HISTORICAL_CALENDAR"],
    )


def _materialized_entry(
    *,
    market: str,
    year: int,
    parquet_sha256: str,
    sidecar_sha256: str,
    content_id: str,
    access_id: str,
) -> CatalogEntry:
    cohort = cohort_for_year(year)
    return CatalogEntry(
        market=market,
        year=year,
        coverage_start=f"{year}-01-01",
        coverage_end=f"{year + 1}-01-01",
        coverage_kind="FULL_YEAR",
        cohort=cohort,
        disposition=CERTIFICATION_STATE,
        selection_eligible=cohort == "DISCOVERY_SELECTION",
        permitted_uses=COHORT_PERMISSIONS[cohort],
        source_bindings=(
            {
                "causal_release_id": "b" * 64,
                "dbn_release_id": "c" * 64,
                "raw_release_id": "d" * 64,
            },
        ),
        content_validation_receipt_id=content_id,
        access_policy_binding_id=access_id,
        parquet_sha256=parquet_sha256,
        sidecar_sha256=sidecar_sha256,
        row_count=2,
        schema_fingerprint=H["schema"],
    )


def _write_stage(
    root: Path,
    *,
    plan_id: str,
    markets: list[tuple[str, int]],
    protected: list[tuple[str, int, str]] | None = None,
    active_view_id: str = H["active"],
) -> tuple[Path, dict[str, object]]:
    stage = root / active.STAGING_ROOT / plan_id
    entries: list[CatalogEntry] = []
    for market, year in markets:
        payload = f"{market}-{year}-synthetic-causal".encode()
        parquet_relative = (
            Path("causally_gated_normalized")
            / market
            / str(year)
            / f"{year}.parquet"
        )
        parquet = stage / parquet_relative
        parquet.parent.mkdir(parents=True, exist_ok=True)
        parquet.write_bytes(payload)
        content = _content(market, year)
        content_id = validate_content_validation_receipt(content)
        access_binding = build_access_policy_binding(
            market=market,
            year=year,
            universe_contract_sha256=H["universe"],
            active_view_id=active_view_id,
            content_validation_receipt_id=content_id,
        )
        access_id = validate_access_policy_binding(access_binding)
        provisional = _materialized_entry(
            market=market,
            year=year,
            parquet_sha256=sha256_file(parquet),
            sidecar_sha256="0" * 64,
            content_id=content_id,
            access_id=access_id,
        )
        sidecar = build_sidecar(
            entry=provisional,
            content_receipt=content,
            access_binding=access_binding,
        )
        sidecar_path = parquet.with_suffix(".parquet.manifest.json")
        sidecar_path.write_bytes(canonical_bytes(sidecar) + b"\n")
        entries.append(
            _materialized_entry(
                market=market,
                year=year,
                parquet_sha256=sha256_file(parquet),
                sidecar_sha256=sha256_file(sidecar_path),
                content_id=content_id,
                access_id=access_id,
            )
        )
    for market, year, disposition in protected or []:
        cohort = cohort_for_year(year)
        entries.append(
            CatalogEntry(
                market=market,
                year=year,
                coverage_start=f"{year}-01-01",
                coverage_end=f"{year + 1}-01-01",
                coverage_kind="FULL_YEAR",
                cohort=cohort,
                disposition=disposition,
                selection_eligible=False,
                permitted_uses=(),
                source_bindings=({"foundation_binding": "e" * 64},),
                reason="SYNTHETIC_PROTECTED_OR_QUARANTINED",
            )
        )
    entries.sort(key=lambda item: (item.market, item.year))
    catalog = build_catalog(
        active_view_id=active_view_id,
        plan_id=plan_id,
        foundation_release_id=H["foundation"],
        foundation_manifest_sha256=H["foundation_manifest"],
        semantic_bindings={"policy": H["semantic"]},
        entries=entries,
    )
    (stage / "catalog.json").write_bytes(canonical_bytes(catalog) + b"\n")
    return stage, catalog


def test_cohorts_and_dispositions_are_explicit_and_fail_closed() -> None:
    expected = {
        2010: "DATA_QUALITY_ONLY",
        2011: "FORMATION_CONTEXT",
        2012: "LEGACY_FEED_STRESS",
        2016: "LEGACY_FEED_STRESS",
        2017: "FEED_TRANSITION_STRESS",
        2018: "DISCOVERY_SELECTION",
        2022: "DISCOVERY_SELECTION",
        2023: "NON_PRISTINE_RESEARCH",
        2024: "NON_PRISTINE_RESEARCH",
        2025: "LOCKED_HOLDOUT",
        2026: "FORWARD_ONLY",
    }
    assert {year: cohort_for_year(year) for year in expected} == expected
    assert disposition_for(year=2022, research_admissible=True) == CERTIFICATION_STATE
    assert disposition_for(year=2022, research_admissible=False) == (
        "QUARANTINED_NOT_MATERIALIZED"
    )
    assert disposition_for(year=2025, research_admissible=True) == (
        "LOCKED_HOLDOUT_NOT_MATERIALIZED"
    )
    assert disposition_for(year=2026, research_admissible=True) == (
        "FORWARD_ONLY_NOT_MATERIALIZED"
    )
    with pytest.raises(ContractError, match="explicit cohort"):
        cohort_for_year(2027)


def test_exact_approval_is_single_plan_and_hash_bound() -> None:
    plan = _plan(mode=UpdateMode.INITIAL, entries=[{"market": "ES", "year": 2022}])
    pending = build_pending_approval(plan)
    with pytest.raises(UnauthorizedOperation, match="exact hash-bound"):
        verify_approval(
            pending, plan, expected_operation="PUBLISH_CAUSAL_ACTIVE_VIEW"
        )
    approval = _approval(plan)
    assert verify_approval(
        approval, plan, expected_operation="PUBLISH_CAUSAL_ACTIVE_VIEW"
    ) == approval["approval_receipt_id"]
    changed = dict(plan)
    changed["limits"] = {"maximum_files": 101}
    with pytest.raises(UnauthorizedOperation, match="exact hash-bound"):
        verify_approval(
            approval, changed, expected_operation="PUBLISH_CAUSAL_ACTIVE_VIEW"
        )


def test_update_classification_is_append_only_only_for_unchanged_existing_entries(
    tmp_path: Path,
) -> None:
    initial_plan = _plan(mode=UpdateMode.INITIAL, entries=[{"market": "ES", "year": 2022}])
    _, current = _write_stage(
        tmp_path, plan_id=str(initial_plan["plan_id"]), markets=[("ES", 2022)]
    )
    new_entry = dict(current["entries"][0])
    new_entry["market"] = "NQ"
    proposed = [current["entries"][0], new_entry]
    proposed.sort(key=lambda item: (item["market"], item["year"]))
    assert (
        classify_update(
            current_catalog=current,
            proposed_entries=proposed,
            current_semantic_bindings=current["semantic_bindings"],
            proposed_semantic_bindings=current["semantic_bindings"],
        )
        is UpdateMode.APPEND_ONLY
    )
    corrected = json.loads(json.dumps(current["entries"]))
    corrected[0]["row_count"] = 3
    assert (
        classify_update(
            current_catalog=current,
            proposed_entries=corrected,
            current_semantic_bindings=current["semantic_bindings"],
            proposed_semantic_bindings=current["semantic_bindings"],
        )
        is UpdateMode.FULL_SUCCESSOR
    )
    assert (
        classify_update(
            current_catalog=current,
            proposed_entries=current["entries"],
            current_semantic_bindings=current["semantic_bindings"],
            proposed_semantic_bindings={"policy": "f" * 64},
        )
        is UpdateMode.FULL_SUCCESSOR
    )


def test_catalog_protects_quarantine_holdout_and_forward_without_false_receipts(
    tmp_path: Path,
) -> None:
    plan = _plan(mode=UpdateMode.INITIAL, entries=[{"market": "ES", "year": 2022}])
    stage, catalog = _write_stage(
        tmp_path,
        plan_id=str(plan["plan_id"]),
        markets=[("ES", 2022)],
        protected=[
            ("KE", 2021, "QUARANTINED_NOT_MATERIALIZED"),
            ("ES", 2025, "LOCKED_HOLDOUT_NOT_MATERIALIZED"),
            ("ES", 2026, "FORWARD_ONLY_NOT_MATERIALIZED"),
        ],
    )
    assert validate_catalog(catalog) == catalog["catalog_sha256"]
    verify_view(stage)
    protected = [
        item for item in catalog["entries"] if item["disposition"] != CERTIFICATION_STATE
    ]
    assert all(item["parquet_path"] is None for item in protected)
    assert all(item["content_validation_receipt_id"] is None for item in protected)


def test_view_rejects_extra_files_tampering_and_links(tmp_path: Path) -> None:
    plan = _plan(mode=UpdateMode.INITIAL, entries=[{"market": "ES", "year": 2022}])
    stage, catalog = _write_stage(
        tmp_path, plan_id=str(plan["plan_id"]), markets=[("ES", 2022)]
    )
    extra = stage / "unexpected.txt"
    extra.write_text("unexpected", encoding="utf-8")
    with pytest.raises(IntegrityError, match="missing or extra"):
        verify_view(stage)
    extra.unlink()
    parquet = stage / str(catalog["entries"][0]["parquet_path"]).removeprefix(
        "data/active/"
    )
    parquet.write_bytes(b"tampered")
    with pytest.raises(IntegrityError, match="Parquet hash differs"):
        verify_view(stage)
    parquet.write_bytes(b"ES-2022-synthetic-causal")
    link = stage / "forbidden-link"
    try:
        link.symlink_to(stage / "catalog.json")
    except OSError:
        pytest.skip("symbolic links are unavailable for this test account")
    with pytest.raises(IntegrityError, match="link"):
        verify_view(stage)


def test_initial_publication_is_transactional_and_resolver_is_capability_guarded(
    tmp_path: Path,
) -> None:
    plan = _plan(
        mode=UpdateMode.INITIAL,
        entries=[
            {"market": "ES", "year": 2022},
            {"market": "ES", "year": 2023},
            {"market": "ES", "year": 2025},
        ],
    )
    _write_stage(
        tmp_path,
        plan_id=str(plan["plan_id"]),
        markets=[("ES", 2022), ("ES", 2023)],
        protected=[("ES", 2025, "LOCKED_HOLDOUT_NOT_MATERIALIZED")],
    )
    receipt = publish_initial(
        repository_root=tmp_path,
        staging=tmp_path / active.STAGING_ROOT / str(plan["plan_id"]),
        plan=plan,
        approval=_approval(plan),
    )
    assert receipt["state"] == "PUBLISHED_VERIFIED"
    assert not (tmp_path / active.STAGING_ROOT / str(plan["plan_id"])).exists()
    assert resolve(
        repository_root=tmp_path,
        market="ES",
        year=2022,
        purpose="SELECTION",
    ).is_file()
    with pytest.raises(UnauthorizedOperation, match="not permitted"):
        resolve(
            repository_root=tmp_path,
            market="ES",
            year=2023,
            purpose="SELECTION",
        )
    with pytest.raises(UnauthorizedOperation, match="protected"):
        resolve(
            repository_root=tmp_path,
            market="ES",
            year=2025,
            purpose="SELECTION",
        )
    with pytest.raises(UnauthorizedOperation, match="pre-2025 status"):
        resolve(
            repository_root=tmp_path,
            market="ES",
            year=2022,
            purpose="FEATURE_GENERATION",
            require_status=True,
        )


def test_failed_full_successor_restores_last_known_good(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = _plan(mode=UpdateMode.INITIAL, entries=[{"market": "ES", "year": 2022}])
    _write_stage(
        tmp_path, plan_id=str(initial["plan_id"]), markets=[("ES", 2022)]
    )
    publish_initial(
        repository_root=tmp_path,
        staging=tmp_path / active.STAGING_ROOT / str(initial["plan_id"]),
        plan=initial,
        approval=_approval(initial),
    )
    current_catalog = verify_view(tmp_path / active.ACTIVE_ROOT)
    successor = _plan(
        mode=UpdateMode.FULL_SUCCESSOR,
        entries=[{"market": "ES", "year": 2022, "correction": True}],
    )
    _write_stage(
        tmp_path,
        plan_id=str(successor["plan_id"]),
        markets=[("ES", 2022)],
        active_view_id="f" * 64,
    )
    real_verify = active.verify_view
    calls = {"active_after_swap": 0}

    def fail_once(path: Path) -> dict[str, object]:
        if path.resolve(strict=False) == (tmp_path / active.ACTIVE_ROOT).resolve(
            strict=False
        ):
            calls["active_after_swap"] += 1
            if calls["active_after_swap"] == 2:
                raise IntegrityError("synthetic post-publication failure")
        return real_verify(path)

    monkeypatch.setattr(active, "verify_view", fail_once)
    with pytest.raises(IntegrityError, match="synthetic post-publication"):
        publish_full_successor(
            repository_root=tmp_path,
            staging=tmp_path / active.STAGING_ROOT / str(successor["plan_id"]),
            plan=successor,
            approval=_approval(successor),
        )
    restored = real_verify(tmp_path / active.ACTIVE_ROOT)
    assert restored["catalog_sha256"] == current_catalog["catalog_sha256"]


def test_pending_or_interrupted_publication_never_silently_recovers(
    tmp_path: Path,
) -> None:
    plan_id = "e" * 64
    journal = tmp_path / active.PUBLICATION_JOURNAL_ROOT / plan_id / "journal.json"
    core = {
        "active_view_id": H["active"],
        "approval_receipt_id": "a" * 64,
        "plan_id": plan_id,
        "rollback_path": None,
        "schema_version": active.JOURNAL_SCHEMA,
        "staging_path": f"state/active_data_view_staging/{plan_id}",
        "state": "INTENT",
        "update_mode": UpdateMode.INITIAL.value,
    }
    journal.parent.mkdir(parents=True)
    journal.write_bytes(canonical_bytes({**core, "journal_id": sha256_json(core)}) + b"\n")
    with pytest.raises(UnauthorizedOperation, match="exact recovery approval"):
        active.recover_publication(tmp_path, plan_id)


def test_interrupted_append_keeps_old_catalog_authoritative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = _plan(mode=UpdateMode.INITIAL, entries=[{"market": "ES", "year": 2022}])
    _write_stage(
        tmp_path, plan_id=str(initial["plan_id"]), markets=[("ES", 2022)]
    )
    publish_initial(
        repository_root=tmp_path,
        staging=tmp_path / active.STAGING_ROOT / str(initial["plan_id"]),
        plan=initial,
        approval=_approval(initial),
    )
    append = _plan(
        mode=UpdateMode.APPEND_ONLY,
        entries=[{"market": "ES", "year": 2022}, {"market": "NQ", "year": 2022}],
    )
    _write_stage(
        tmp_path,
        plan_id=str(append["plan_id"]),
        markets=[("ES", 2022), ("NQ", 2022)],
        active_view_id=H["active"],
    )
    real_write = active._write_new_or_exact

    def fail_catalog_commit(path: Path, payload: dict[str, object]) -> None:
        if path.name == "catalog.json.next":
            raise IntegrityError("synthetic interrupted append")
        real_write(path, payload)

    monkeypatch.setattr(active, "_write_new_or_exact", fail_catalog_commit)
    with pytest.raises(IntegrityError, match="synthetic interrupted append"):
        publish_append_only(
            repository_root=tmp_path,
            staging=tmp_path / active.STAGING_ROOT / str(append["plan_id"]),
            plan=append,
            approval=_approval(append),
        )
    # NQ files are inert because the old catalog is still the commit marker.
    assert resolve(
        repository_root=tmp_path,
        market="ES",
        year=2022,
        purpose="SELECTION",
    ).is_file()
    with pytest.raises(IntegrityError, match="absent or duplicated"):
        resolve(
            repository_root=tmp_path,
            market="NQ",
            year=2022,
            purpose="SELECTION",
        )


def test_insufficient_disk_fails_before_materialization_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"synthetic")
    destination = tmp_path / "stage" / "out.parquet"
    monkeypatch.setattr(
        active.shutil,
        "disk_usage",
        lambda _: shutil._ntuple_diskusage(100, 99, 1),
    )
    with pytest.raises(IntegrityError, match="insufficient disk"):
        active.materialize_parquet(
            sources=(source,),
            source_sha256s=(sha256_file(source),),
            destination=destination,
            expected_row_count=1,
            expected_schema_fingerprint="f" * 64,
        )
    assert not destination.exists()


def test_content_receipt_and_access_binding_reject_stale_or_tampered_evidence() -> None:
    reusable = _content("ES", 2022)
    assert content_receipt_reusable(
        reusable,
        source_bindings=reusable["source_bindings"],
        semantic_bindings=reusable["semantic_bindings"],
        implementation_bindings=reusable["implementation_bindings"],
        environment_bindings=reusable["environment_bindings"],
    )
    assert not content_receipt_reusable(
        reusable,
        source_bindings=reusable["source_bindings"],
        semantic_bindings={"policy": "f" * 64},
        implementation_bindings=reusable["implementation_bindings"],
        environment_bindings=reusable["environment_bindings"],
    )
    content = _content("ES", 2022)
    content["row_count"] = 3
    with pytest.raises(IntegrityError, match="content validation receipt"):
        validate_content_validation_receipt(content)
    valid = _content("ES", 2022)
    binding = build_access_policy_binding(
        market="ES",
        year=2022,
        universe_contract_sha256=H["universe"],
        active_view_id=H["active"],
        content_validation_receipt_id=valid["content_validation_receipt_id"],
    )
    binding["selection_eligible"] = False
    with pytest.raises(IntegrityError, match="access policy binding"):
        validate_access_policy_binding(binding)


def test_catalog_rejects_unknown_year_and_non_discovery_selection() -> None:
    with pytest.raises(ContractError, match="explicit cohort"):
        CatalogEntry(
            market="ES",
            year=2027,
            coverage_start="2027-01-01",
            coverage_end="2028-01-01",
            coverage_kind="FULL_YEAR",
            cohort="DISCOVERY_SELECTION",
            disposition="QUARANTINED_NOT_MATERIALIZED",
            selection_eligible=False,
            permitted_uses=(),
            source_bindings=(),
            reason="UNKNOWN_YEAR",
        )
    with pytest.raises(ContractError, match="selection eligibility"):
        CatalogEntry(
            market="ES",
            year=2023,
            coverage_start="2023-01-01",
            coverage_end="2024-01-01",
            coverage_kind="FULL_YEAR",
            cohort="NON_PRISTINE_RESEARCH",
            disposition=CERTIFICATION_STATE,
            selection_eligible=True,
            permitted_uses=COHORT_PERMISSIONS["NON_PRISTINE_RESEARCH"],
            source_bindings=(),
            content_validation_receipt_id="a" * 64,
            access_policy_binding_id="b" * 64,
            parquet_sha256="c" * 64,
            sidecar_sha256="d" * 64,
            row_count=1,
            schema_fingerprint="e" * 64,
        )
