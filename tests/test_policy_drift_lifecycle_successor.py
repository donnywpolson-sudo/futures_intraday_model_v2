import importlib.util
from pathlib import Path


RUNNER = Path(__file__).resolve().parents[1] / "reports/bounded_2025_v10_campaign_lifecycle_remediation/clr_20260831T0256062502775Z_33287f1b/lifecycle_successor_runner.py"


def test_campaign_and_worker_accept_opposite_run_root_states(tmp_path: Path) -> None:
    spec = importlib.util.spec_from_file_location("lifecycle_successor_runner_test", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    run_root = tmp_path / "run"
    assert module.run_root_matches_phase(run_root, "packet")
    assert module.run_root_matches_phase(run_root, "campaign")
    assert not module.run_root_matches_phase(run_root, "worker")

    run_root.mkdir()
    (run_root / "started.json").write_text("{}", encoding="utf-8")
    assert not module.run_root_matches_phase(run_root, "campaign")
    assert module.run_root_matches_phase(run_root, "worker")
