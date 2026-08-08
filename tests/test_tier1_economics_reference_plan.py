from pathlib import Path
from futures_rebuild.tier1_economics_reference_plan import build_tier1_economics_reference_plan, MARKETS, REQUIRED_FIELDS

def test_plan_requires_only_prediction_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / 'manifests/data_releases/predictions/c65d3da960c025f09d28be8907e884cb10eb39b2ffe54aeb503581257d64c31a.json'
    manifest.parent.mkdir(parents=True); manifest.write_text('{}', encoding='utf-8')
    plan = build_tier1_economics_reference_plan(root=tmp_path)
    assert plan['markets'] == MARKETS
    assert plan['required_contract_fields'] == list(REQUIRED_FIELDS)
    assert plan['authority']['phase8_evaluation'] is False
