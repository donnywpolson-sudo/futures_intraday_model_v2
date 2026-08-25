from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.causal_observation_bounded_2025_smoke import (
    BOUNDARY_SOURCE_FAMILIES,
    DEVELOPMENT_END_EXCLUSIVE,
    EXPECTED_DBN_COUNT,
    EXPECTED_ENTRY_COUNT,
    EXPECTED_SIDECAR_COUNT,
    MAXIMUM_OUTPUT_BYTES,
    MAXIMUM_PARTITION_COUNT,
    MAXIMUM_PEAK_ADDITIONAL_BYTES,
    MAXIMUM_RUNTIME_SECONDS,
    MINIMUM_FREE_AFTER_PEAK_BYTES,
    PLAN_SCHEMA,
    _load_exact_source_entries,
    _validate_plan,
)
from futures_rebuild.causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    _require_context,
    authorize_bounded_2025_smoke_row_read,
    required_bounded_2025_smoke_scope,
)
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
)


CONTRACT_ID = "a" * 64
RELEASE_ID = "b" * 64


def _entries() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for family in sorted(BOUNDARY_SOURCE_FAMILIES):
        dbn_path = (
            f"data/dbn/{family}/6A/2025/"
            "2025-01-01_2025-07-13T220000Z.dbn.zst"
        )
        for kind, suffix, size in (("DBN", "", 100), ("SIDECAR", ".manifest.json", 10)):
            result.append(
                {
                    "admitted_standard_foundation": True,
                    "family": family,
                    "interval_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
                    "interval_start_inclusive": "2025-01-01T00:00:00Z",
                    "kind": kind,
                    "lane": "STANDARD_41",
                    "market": "6A",
                    "path": dbn_path + suffix,
                    "sha256": ("c" if kind == "DBN" else "d") * 64,
                    "size_bytes": size,
                    "year": 2025,
                }
            )
    return result


def _write_contract(root: Path, *, release_id: str = RELEASE_ID) -> str:
    core: dict[str, object] = {
        "active_canonical_source": {"release_id": release_id},
        "selection_policy": {},
    }
    contract = {**core, "contract_id": sha256_json(core)}
    path = root / "configs/source_contract.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(contract))
    return str(contract["contract_id"])


def _plan(root: Path, contract_id: str) -> tuple[dict[str, object], Path]:
    entries = _entries()
    inventory_path = root / "reports/smoke/inventory.json"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_bytes(canonical_bytes({"entries": entries}))
    payload_bytes = sum(int(item["size_bytes"]) for item in entries if item["kind"] == "DBN")
    source_bytes = sum(int(item["size_bytes"]) for item in entries)
    binding_paths = (
        "configs/causal_observation_contract_v1.json",
        "configs/contract_economics_rules.json",
        "configs/source_contract.json",
        "src/futures_rebuild/causal_observation_bounded_2025_smoke.py",
        "src/futures_rebuild/causal_observation_canary.py",
        "src/futures_rebuild/causal_observation_foundation.py",
        "src/futures_rebuild/causal_observation_full_build.py",
        "src/futures_rebuild/causal_observation_verifier.py",
        "src/futures_rebuild/causal_source_closure.py",
        "src/futures_rebuild/foundation/decoder.py",
        "src/futures_rebuild/research_gateway_policy.py",
    )
    bindings: dict[str, str] = {}
    for relative in binding_paths:
        path = root / relative
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(relative, encoding="utf-8")
        bindings[relative] = sha256_file(path)
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "status": "PREPARED_NOT_AUTHORIZED_NO_ROW_READ",
        "operation": CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": contract_id,
            "canonical_release_id": RELEASE_ID,
            "inventory_path": inventory_path.relative_to(root).as_posix(),
            "inventory_sha256": sha256_file(inventory_path),
            "exact_source_entries_sha256": sha256_json(entries),
            "exact_source_entry_count": EXPECTED_ENTRY_COUNT,
            "exact_dbn_file_count": EXPECTED_DBN_COUNT,
            "exact_sidecar_file_count": EXPECTED_SIDECAR_COUNT,
            "total_source_bytes": source_bytes,
            "maximum_payload_bytes": payload_bytes,
        },
        "roots": ["6A"],
        "window": {
            "start": "2025-01-01T00:00:00Z",
            "end": DEVELOPMENT_END_EXCLUSIVE,
        },
        "output_staging_path": "state/data_publication_staging/smoke-test",
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "reuse_prior_receipt": False,
        "reuse_prior_partitions": False,
        "limits": {
            "maximum_payload_bytes": payload_bytes,
            "maximum_decoded_records": 1_000,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_partition_count": MAXIMUM_PARTITION_COUNT,
        },
        "execution": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
        },
        "storage": {
            "activation_authorized": False,
            "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
            "publication_authorized": False,
            "required_free_after_peak_bytes": MINIMUM_FREE_AFTER_PEAK_BYTES,
        },
        "economics": {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
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
        "implementation_bindings": bindings,
    }
    plan = {**core, "plan_id": sha256_json(core)}
    plan_path = root / "reports/smoke/plan.json"
    plan_path.write_bytes(canonical_bytes(plan))
    return plan, plan_path


def test_smoke_plan_selects_exact_6a_2025_seven_family_pairs(tmp_path: Path) -> None:
    contract_id = _write_contract(tmp_path)
    plan, _ = _plan(tmp_path, contract_id)
    _validate_plan(tmp_path, plan)
    selected = _load_exact_source_entries(tmp_path, plan)
    assert len(selected) == 14
    assert {(item["family"], item["kind"]) for item in selected} == {
        (family, kind)
        for family in BOUNDARY_SOURCE_FAMILIES
        for kind in ("DBN", "SIDECAR")
    }


def test_smoke_receipt_consumes_once_and_issues_active_context(tmp_path: Path) -> None:
    contract_id = _write_contract(tmp_path)
    plan, plan_path = _plan(tmp_path, contract_id)
    plan_sha = sha256_file(plan_path)
    required = required_bounded_2025_smoke_scope(
        plan=plan,
        plan_sha256=plan_sha,
        source_contract_id=contract_id,
        canonical_release_id=RELEASE_ID,
    )
    scope = {key: value for key, value in required.items() if not key.startswith("approval_")}
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=_personal_approval_line(
            CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
            str(plan["plan_id"]),
            plan_sha,
        ),
    )
    context = authorize_bounded_2025_smoke_row_read(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha,
    )
    _require_context(context)
    assert context.source_contract_id == contract_id
    assert context.source_release_id == RELEASE_ID
    with pytest.raises(UnauthorizedOperation, match="already used"):
        authorize_bounded_2025_smoke_row_read(
            boundary=boundary,
            receipt=receipt,
            plan=plan,
            plan_sha256=plan_sha,
        )


def test_smoke_active_source_drift_fails_before_receipt_consumption(tmp_path: Path) -> None:
    contract_id = _write_contract(tmp_path)
    plan, plan_path = _plan(tmp_path, contract_id)
    plan_sha = sha256_file(plan_path)
    required = required_bounded_2025_smoke_scope(
        plan=plan,
        plan_sha256=plan_sha,
        source_contract_id=contract_id,
        canonical_release_id=RELEASE_ID,
    )
    scope = {key: value for key, value in required.items() if not key.startswith("approval_")}
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=_personal_approval_line(
            CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
            str(plan["plan_id"]),
            plan_sha,
        ),
    )
    _write_contract(tmp_path, release_id="e" * 64)
    with pytest.raises(UnauthorizedOperation):
        authorize_bounded_2025_smoke_row_read(
            boundary=boundary,
            receipt=receipt,
            plan=plan,
            plan_sha256=plan_sha,
        )
    assert not (tmp_path / "state/authorization_uses" / f"{receipt.receipt_id}.json").exists()
