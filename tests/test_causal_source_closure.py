from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file
from futures_rebuild.causal_source_closure import (
    reject_unlisted_source_path,
    select_standard_dbn_paths,
    validate_source_contract_metadata,
    validate_full_build_selection_contract,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.foundation_operation_firewall import (
    CURRENT_SOURCE_CLOSURE_OPERATION,
    issue_current_source_closure_context,
)


REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "configs" / "source_contract.json"
VERSIONED = REPO / "configs" / "source_contract_v4.json"
RUNNER = REPO / "scripts" / "run_dual_resolution_tier01_foundation.py"
pytestmark = pytest.mark.current


def _json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _context():
    return issue_current_source_closure_context(REPO, contract_path=CONTRACT)


def test_installed_root_and_current_paths_are_repository_relative() -> None:
    assert REPO == Path(__file__).resolve().parents[1]
    assert CONTRACT == REPO / "configs/source_contract.json"
    assert RUNNER == REPO / "scripts/run_dual_resolution_tier01_foundation.py"


def test_current_contract_metadata_closure_and_context_are_exact() -> None:
    result = validate_source_contract_metadata(
        REPO,
        operation_context=_context(),
        contract_path=CONTRACT,
    )
    assert result == {
        "contract_id": _json(CONTRACT)["contract_id"],
        "content_inventory_sha256": "a915573b797bd5dc2b0d34beb1d0591dd6ae8b3bb9e448f41e130824d5e94ced",
        "deferred_micro_root_count": 17,
        "file_count": 11474,
        "payload_files_opened": 0,
        "release_id": "4ca353d7814941782bb4c6640afe89b04371492868f57174bb10d632b6e7c9be",
        "row_reads": 0,
        "standard_root_count": 41,
        "total_bytes": 24198934599,
        "valid": True,
    }
    assert _context().operation == CURRENT_SOURCE_CLOSURE_OPERATION
    assert CONTRACT.read_bytes() == VERSIONED.read_bytes()


def test_non_active_full_build_contract_successor_matches_admitted_inventory() -> None:
    active = _json(CONTRACT)
    inventory = _json(REPO / str(active["complete_inventory"]["path"]))
    with pytest.raises(IntegrityError, match="admitted DBN identity differs"):
        validate_full_build_selection_contract(active, inventory["entries"])

    candidate = _json(
        REPO
        / "reports/causal_full_build_source_rebind_preparation/"
        "cfbsrp_20260824T1856245925105Z_34fea2fa/"
        "SUCCESSOR_SOURCE_CONTRACT.json"
    )
    result = validate_full_build_selection_contract(candidate, inventory["entries"])
    assert result == {
        "admitted_standard_dbn_file_count": 4_253,
        "admitted_standard_dbn_inventory_sha256": (
            "e23b2709d7674ef9dfc6ef1178a6485b89cb86179ac0c3d2386d2e6cd51fb769"
        ),
        "development_end_exclusive": "2025-07-13T22:00:00Z",
    }
    assert candidate["active_canonical_source"]["release_id"] == (
        "4ca353d7814941782bb4c6640afe89b04371492868f57174bb10d632b6e7c9be"
    )
    assert candidate["status"].startswith("PREPARED_NON_ACTIVE")


def test_exact_standard_selection_rejects_micro_and_unlisted_paths() -> None:
    context = _context()
    selected = select_standard_dbn_paths(
        REPO,
        operation_context=context,
        market="ES",
        family="ohlcv_1m",
        contract_path=CONTRACT,
    )
    assert selected and selected == tuple(sorted(selected))
    assert all(path.startswith("data/dbn/ohlcv_1m/ES/") for path in selected)
    with pytest.raises(UnauthorizedOperation, match="standard source lane"):
        select_standard_dbn_paths(
            REPO,
            operation_context=context,
            market="MES",
            family="ohlcv_1m",
            contract_path=CONTRACT,
        )
    with pytest.raises(UnauthorizedOperation, match="exact admitted current source"):
        reject_unlisted_source_path(
            REPO,
            "data/causally_gated_normalized/ES/2024/2024.parquet",
            operation_context=context,
            contract_path=CONTRACT,
        )


def test_missing_or_forged_context_fails_closed() -> None:
    with pytest.raises(TypeError):
        validate_source_contract_metadata(REPO, contract_path=CONTRACT)  # type: ignore[call-arg]
    with pytest.raises(UnauthorizedOperation, match="context"):
        validate_source_contract_metadata(
            REPO,
            operation_context=object(),  # type: ignore[arg-type]
            contract_path=CONTRACT,
        )


def test_reconciliation_and_retirement_bindings_are_current() -> None:
    contract = _json(CONTRACT)
    reconciliation = _json(REPO / str(contract["unbound_reconciliation"]["path"]))
    retirement = _json(REPO / str(contract["retirement_contract"]["path"]))
    counts: dict[str, int] = {}
    for entry in reconciliation["entries"]:
        counts[str(entry["classification"])] = counts.get(str(entry["classification"]), 0) + 1
    assert counts == {
        "EXPECTED_ACTIVE_CANONICAL_ARTIFACT": 675,
        "REQUIRED_CANONICAL_SIDECAR": 675,
    }
    assert reconciliation["unregistered_extra_count"] == 0
    assert reconciliation["unresolved_integrity_issue_count"] == 0
    assert retirement["enforcement"]["historical_runner_sha256"] == sha256_file(RUNNER)


def test_current_selector_has_no_legacy_import_or_fallback() -> None:
    source = (REPO / "src/futures_rebuild/causal_source_closure.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    modules.update(node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))
    assert not any("dual_resolution" in name for name in modules)
    assert "runpy" not in modules
    assert "subprocess" not in modules
    assert "glob(" not in source
    assert "rglob(" not in source
    assert "newest" not in source
    assert "latest" not in source
