import importlib.util
import json
from pathlib import Path


RUNNER = Path(__file__).resolve().parents[1] / "reports/bounded_2025_v10_campaign_lifecycle_remediation_v2/clr2_20260831T0307165158224Z_33287f1b/lifecycle_successor_v2_runner.py"


def module():
    spec = importlib.util.spec_from_file_location("lifecycle_successor_v2_test", RUNNER)
    assert spec is not None and spec.loader is not None
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_campaign_and_worker_lifecycle_states(tmp_path: Path) -> None:
    runner = module()
    run_root = tmp_path / "run"
    assert runner.run_root_matches_phase(run_root, "campaign")
    assert not runner.run_root_matches_phase(run_root, "worker")
    run_root.mkdir()
    (run_root / "started.json").write_text("{}", encoding="utf-8")
    assert not runner.run_root_matches_phase(run_root, "campaign")
    assert runner.run_root_matches_phase(run_root, "worker")


def test_full_synthetic_campaign_reaches_terminal_without_schema_reuse(tmp_path: Path, monkeypatch) -> None:
    runner = module()
    run_root = tmp_path / "run"
    certificate_path = tmp_path / "6n.json"
    certificate_path.write_text(json.dumps({"certificate_id": "c"}), encoding="utf-8")
    packet = {
        "packet_id": "p",
        "plan": {"plan_id": "q", "remaining_markets": list(runner.MARKET_ORDER[11:])},
        "predecessor": {"failure_id": "f", "six_n_certificate_path": str(certificate_path), "six_n_certificate_id": "c"},
    }

    class FakeModule:
        def write_once(self, path, value):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value), encoding="utf-8")

        def journal(self, event, **details):
            with (run_root / "journal.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({"event": event, **details}) + "\n")

        def valid_certificate(self, market, packet):
            return certificate_path, {"certificate_id": market}

    monkeypatch.setattr(runner, "exact_packet", lambda phase: (packet, run_root, FakeModule()))
    monkeypatch.setattr(runner, "validate_market_campaign_storage", lambda **kwargs: 0)
    runner.run_campaign()

    events = [json.loads(line) for line in (run_root / "journal.jsonl").read_text(encoding="utf-8").splitlines()]
    terminal = json.loads((run_root / "terminal.json").read_text(encoding="utf-8"))
    assert events[0] == {"event": "LIFECYCLE_V2_SUCCESSOR_STARTED", "predecessor_failure_id": "f", "resume_market": "6S"}
    assert terminal["status"] == "PASS_ALL_41_MARKETS_MAXIMUM_ROBUSTNESS_CERTIFIED_INACTIVE"
    assert terminal["completed_markets"] == list(runner.MARKET_ORDER)
