from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path

import pytest

from futures_rebuild.active_phase5_splits import build_tier1_phase5_split_plan
from futures_rebuild.active_phase3_outcomes import build_active_phase3_outcomes
from futures_rebuild.active_phase3_mechanics import run_active_phase3_mechanics_check
from futures_rebuild.active_phase4_features import build_active_phase4_features
from futures_rebuild.active_phase6_wfa import run_tier1_phase6_prediction_only_wfa
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.foundation.orchestrator import main as foundation_main
from futures_rebuild.live_cockpit import observation_status
from futures_rebuild.tier1_phase8_real_adapter import (
    _approved_real_read_for_codex_task,
)
from futures_rebuild.tier1_phase8_preparation import prepare_tier1_phase8


ROOT = Path(__file__).resolve().parents[1]


def _script(relative: str):
    path = ROOT / relative
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_phase6_direct_import_fails_before_historical_access() -> None:
    with pytest.raises(UnauthorizedOperation, match="retired"):
        run_tier1_phase6_prediction_only_wfa(
            boundary=RepoBoundary(active_root=ROOT)
        )


def test_foundation_and_split_shortcuts_fail_on_live_repository() -> None:
    runner = _script("scripts/run_tier1_core_foundation.py")
    with pytest.raises(UnauthorizedOperation, match="retired"):
        runner.run_pairs(
            boundary=RepoBoundary(active_root=ROOT), pairs=(("ES", 2018),)
        )
    with pytest.raises(UnauthorizedOperation, match="retired"):
        build_tier1_phase5_split_plan(boundary=RepoBoundary(active_root=ROOT))


def test_direct_phase3_phase4_and_phase8_imports_fail_before_inputs() -> None:
    boundary = RepoBoundary(active_root=ROOT)
    with pytest.raises(UnauthorizedOperation, match="retired"):
        build_active_phase3_outcomes(boundary=boundary, validation=object())
    with pytest.raises(UnauthorizedOperation, match="retired"):
        build_active_phase4_features(boundary=boundary, binding=object())
    with pytest.raises(UnauthorizedOperation, match="retired"):
        prepare_tier1_phase8(root=ROOT)
    with pytest.raises(UnauthorizedOperation, match="retired"):
        run_active_phase3_mechanics_check(boundary=boundary, validation=object())


def test_retired_bracket_registration_sources_and_modeling_fail_first() -> None:
    from futures_rebuild.tier1_bracket_finalizer import (
        build_bracket_chronological_split_plan,
        write_frozen_bracket_predictions,
    )
    from futures_rebuild.tier1_bracket_pipeline import register_tier1_bracket_pipeline
    from futures_rebuild.tier1_bracket_source_publisher import stage_indexed_bracket_market_year

    with pytest.raises(UnauthorizedOperation, match="retired"):
        register_tier1_bracket_pipeline(root=ROOT)
    with pytest.raises(UnauthorizedOperation, match="retired"):
        build_bracket_chronological_split_plan(stage=ROOT / "missing-stage")
    with pytest.raises(UnauthorizedOperation, match="retired"):
        write_frozen_bracket_predictions(
            stage=ROOT / "missing-stage", output=ROOT / "missing-output.parquet"
        )
    with pytest.raises(UnauthorizedOperation, match="retired"):
        stage_indexed_bracket_market_year(
            root=ROOT,
            phase8_index_release_id="missing",
            audit_receipt_id="missing",
            causal_release_id="missing",
            signal_contract_id="missing",
            stress_round_trip_cost_usd=__import__("decimal").Decimal("0"),
        )


def test_foundation_cli_rejects_before_opening_caller_paths() -> None:
    with pytest.raises(UnauthorizedOperation, match="retired"):
        foundation_main(
            [
                "--repository-root", str(ROOT),
                "--source-contract", "missing-source-contract.json",
                "--source-dbn-manifest", "missing-dbn.json",
                "--source-selection-manifest", "missing-selection.json",
                "--calendar-index-manifest", "missing-calendar.json",
                "--feature-spec", "missing-feature.json",
                "--execute",
            ]
        )


def test_phase7_import_has_no_row_read_or_publication_side_effect() -> None:
    module = _script("scripts/run_tier1_phase7_audit.py")
    with pytest.raises(UnauthorizedOperation, match="retired"):
        module.main(repository_root=ROOT)


def test_phase8_opaque_token_factory_is_retired() -> None:
    with pytest.raises(UnauthorizedOperation, match="retired"):
        _approved_real_read_for_codex_task()


def test_bracket_evaluation_is_blocked_before_reading_fake_paths() -> None:
    from futures_rebuild.tier1_bracket_evaluation import evaluate_and_publish_tier1_bracket

    with pytest.raises(UnauthorizedOperation, match="retired"):
        evaluate_and_publish_tier1_bracket(
            root=ROOT,
            prediction_index_release_id="missing",
            evaluation_config={},
        )


def test_bracket_successor_v2_is_blocked_before_registration_reads() -> None:
    from futures_rebuild.tier1_bracket_successor_v2_execution import (
        execute_registered_successor_v2,
    )

    with pytest.raises(UnauthorizedOperation, match="retired"):
        execute_registered_successor_v2(root=ROOT)


def test_cockpit_contract_remains_observation_only() -> None:
    source = inspect.getsource(observation_status)
    assert "no operator controls" in source
    assert "broker integration" in source
    assert '"mode=DISABLED"' in source


def test_cockpit_package_exposes_no_broker_or_position_mutation_call() -> None:
    forbidden_calls = (
        "submit_order(", "place_order(", "send_order(", "cancel_order(",
        "modify_order(", "close_position(", "open_position(",
    )
    forbidden_imports = ("import ib_insync", "import alpaca", "import ccxt")
    for path in sorted((ROOT / "src/futures_rebuild/live_cockpit").glob("*.py")):
        text = path.read_text(encoding="utf-8").lower()
        assert not any(token in text for token in forbidden_calls), path
        assert not any(token in text for token in forbidden_imports), path
