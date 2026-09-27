from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from futures_rebuild.bias_robustness_successor import validate_contract
from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.fixed_hour_globex_pipeline import (
    MARKETS,
    frozen_stage_2_5_contract,
    frozen_stage_5_contract,
    frozen_stage_5_contract_v2,
    frozen_stage_5_contract_v3,
    frozen_stage_5_contract_v4,
    global_trade_date_samples,
    run_synthetic_stage_6,
    run_synthetic_stage_6_v2,
    run_synthetic_stage_6_v3,
    run_synthetic_stage_6_v4,
)


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def test_all_globex_contracts_and_synthetic_stage_are_exact_and_inactive() -> None:
    stage_2_5 = frozen_stage_2_5_contract(ROOT)
    stage_2_5_path = (
        ROOT / "state/causal_search_access_contract_registry"
        / str(stage_2_5["contract_id"]) / "contract.json"
    )
    assert validate_contract(stage_2_5, root=ROOT) == stage_2_5
    assert json.loads(stage_2_5_path.read_text(encoding="utf-8")) == stage_2_5
    assert stage_2_5_path.read_bytes() == canonical_bytes(stage_2_5) + b"\n"
    assert stage_2_5["search"]["combined_information_family_burden"] == 181
    assert not any(stage_2_5["authority"].values())

    stage_5 = frozen_stage_5_contract(ROOT)
    stage_5_path = ROOT / "configs/fixed_hour_globex_mechanism_evaluation_v1.json"
    assert json.loads(stage_5_path.read_text(encoding="utf-8")) == stage_5
    assert stage_5_path.read_bytes() == canonical_bytes(stage_5) + b"\n"
    assert stage_5["chronology"]["split_unit"] == "COMPLETE_MARKET_TRADE_DATE"
    assert stage_5["model"]["parameters"] == "PER_MARKET_NOT_POOLED"
    assert not any(stage_5["authority"].values())

    stage_5_v2 = frozen_stage_5_contract_v2(ROOT)
    stage_5_v2_path = ROOT / "configs/fixed_hour_globex_mechanism_evaluation_v2.json"
    assert json.loads(stage_5_v2_path.read_text(encoding="utf-8")) == stage_5_v2
    assert stage_5_v2_path.read_bytes() == canonical_bytes(stage_5_v2) + b"\n"
    assert stage_5_v2["predecessor"]["contract_id"] == stage_5["contract_id"]
    assert stage_5_v2["chronology"] == stage_5["chronology"]
    stress = stage_5_v2["post_development"]["development_regime_stress_test"]
    assert stress["official_pass_fail_scope"] == "ENTIRE_PERIOD_ONLY"
    assert [item["label"] for item in stress["diagnostic_slices"]] == [
        "2023", "2024", "2025_TO_CUTOFF",
    ]
    assert stress["diagnostic_slice_role"] == "REPORT_ONLY_NOT_GATES_NOT_SELECTION"
    assert not any(stage_5_v2["authority"].values())

    stage_5_v3 = frozen_stage_5_contract_v3(ROOT)
    stage_5_v3_path = ROOT / "configs/fixed_hour_globex_mechanism_evaluation_v3.json"
    assert json.loads(stage_5_v3_path.read_text(encoding="utf-8")) == stage_5_v3
    assert stage_5_v3_path.read_bytes() == canonical_bytes(stage_5_v3) + b"\n"
    assert stage_5_v3["predecessor"]["contract_id"] == stage_5_v2["contract_id"]
    assert stage_5_v3["scope"]["opportunities"] == (
        "ALL_PREDECLARED_PLUS_22_MINUTE_ALIGNED_OPPORTUNITIES_NO_SUBSAMPLING"
    )
    assert not any(stage_5_v3["authority"].values())

    stage_5_v4 = frozen_stage_5_contract_v4(ROOT)
    stage_5_v4_path = ROOT / "configs/fixed_hour_globex_mechanism_evaluation_v4.json"
    assert json.loads(stage_5_v4_path.read_text(encoding="utf-8")) == stage_5_v4
    assert stage_5_v4_path.read_bytes() == canonical_bytes(stage_5_v4) + b"\n"
    assert stage_5_v4["predecessor"]["contract_id"] == stage_5_v3["contract_id"]
    assert stage_5_v4["chronology"]["minimum_initial_fit_trade_dates"] == 505
    assert stage_5_v4["chronology"]["minimum_initial_inner_fit_trade_dates"] == 252
    assert stage_5_v4["chronology"]["fold_manifest_required"] is True

    certificate = run_synthetic_stage_6(ROOT)
    certificate_path = (
        ROOT / "state/unpublished_evidence/fixed_hour_globex_synthetic"
        / str(certificate["certificate_id"]) / "certificate.json"
    )
    assert json.loads(certificate_path.read_text(encoding="utf-8")) == certificate
    assert certificate["state"] == "PASS_SYNTHETIC_ENGINEERING_ONLY"
    assert certificate["opportunities_checked"] == 12
    assert not any(certificate["authority"].values())

    certificate_v2 = run_synthetic_stage_6_v2(ROOT)
    certificate_v2_path = (
        ROOT / "state/unpublished_evidence/fixed_hour_globex_synthetic"
        / str(certificate_v2["certificate_id"]) / "certificate.json"
    )
    assert json.loads(certificate_v2_path.read_text(encoding="utf-8")) == certificate_v2
    assert certificate_v2_path.read_bytes() == canonical_bytes(certificate_v2) + b"\n"
    assert certificate_v2["stage_5_contract_id"] == stage_5_v2["contract_id"]
    assert certificate_v2["opportunities_checked"] == 12
    assert not any(certificate_v2["authority"].values())

    certificate_v3 = run_synthetic_stage_6_v3(ROOT)
    certificate_v3_path = (
        ROOT / "state/unpublished_evidence/fixed_hour_globex_synthetic"
        / str(certificate_v3["certificate_id"]) / "certificate.json"
    )
    assert json.loads(certificate_v3_path.read_text(encoding="utf-8")) == certificate_v3
    assert certificate_v3_path.read_bytes() == canonical_bytes(certificate_v3) + b"\n"
    assert certificate_v3["stage_5_contract_id"] == stage_5_v3["contract_id"]
    assert certificate_v3["opportunities_checked"] == 12
    assert not any(certificate_v3["authority"].values())

    certificate_v4 = run_synthetic_stage_6_v4(ROOT)
    certificate_v4_path = (
        ROOT / "state/unpublished_evidence/fixed_hour_globex_synthetic"
        / str(certificate_v4["certificate_id"]) / "certificate.json"
    )
    assert json.loads(certificate_v4_path.read_text(encoding="utf-8")) == certificate_v4
    assert certificate_v4_path.read_bytes() == canonical_bytes(certificate_v4) + b"\n"
    assert certificate_v4["stage_5_contract_id"] == stage_5_v4["contract_id"]


def test_global_split_adapter_keeps_every_market_trade_date_indivisible() -> None:
    rows = [
        {
            "market": market,
            "trade_date": trade_date,
            "opportunity_id": f"{trade_date}-{market}-{ordinal}",
        }
        for trade_date in ("2022-01-03", "2022-01-04", "2022-01-05")
        for market in reversed(MARKETS)
        for ordinal in range(2)
    ]
    samples, order, dates = global_trade_date_samples(rows)
    assert dates == ("2022-01-03", "2022-01-04", "2022-01-05")
    assert tuple(samples.decision_session[order_index] for order_index in range(len(order))) == (
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 1, 1, 1, 1,
        2, 2, 2, 2, 2, 2, 2, 2,
    )
    np.testing.assert_array_equal(samples.label_end, samples.decision_session + 1)
