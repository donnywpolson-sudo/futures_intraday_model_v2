from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

import futures_rebuild.live_cockpit.engine as engine_module
from futures_rebuild.live_cockpit.automatic_history_canary import (
    _INPUT_PATHS,
    CACHE_PATH_TEMPLATE,
    STATE_PATH_TEMPLATE,
    TERMINAL_PATH_TEMPLATE,
    build_plan,
    prepare_confirmation,
    run_canary,
)


class _Metadata:
    cost = 0.01
    failure: Exception | None = None

    def get_dataset_range(self, **_kwargs):
        if self.failure is not None:
            raise self.failure
        return {
            "start": "2010-01-01T00:00:00Z",
            "end": "2099-01-01T00:00:00Z",
            "schema": {
                "ohlcv-1m": {
                    "start": "2010-01-01T00:00:00Z",
                    "end": "2099-01-01T00:00:00Z",
                }
            },
        }

    def get_cost(self, **_kwargs):
        return self.cost


class _Symbology:
    def __init__(self) -> None:
        self.market_by_instrument: dict[int, str] = {}

    def resolve(self, **kwargs):
        if kwargs["stype_out"] == "instrument_id":
            result = {}
            for index, query in enumerate(kwargs["symbols"]):
                instrument_id = 10_000 + index
                market = str(query).removesuffix(".v.0")
                self.market_by_instrument[instrument_id] = market
                result[str(query)] = [
                    {
                        "s": str(instrument_id),
                        "d0": "2020-01-01",
                        "d1": "2099-01-01",
                    }
                ]
            return {"result": result}
        return {
            "result": {
                str(instrument_id): [
                    {
                        "s": f"{market}U6",
                        "d0": "2020-01-01",
                        "d1": "2099-01-01",
                    }
                ]
                for instrument_id, market in self.market_by_instrument.items()
            }
        }


class _Timeseries:
    calls = 0

    def get_range(self, **kwargs):
        self.__class__.calls += 1
        instrument_id = int(kwargs["symbols"][0])
        timestamp = kwargs["start"] + timedelta(minutes=1)
        return [
            SimpleNamespace(
                instrument_id=instrument_id,
                ts_event=timestamp,
                open=100_000_000_000,
                high=101_000_000_000,
                low=99_000_000_000,
                close=100_500_000_000,
                volume=10,
            )
        ]


class _Historical:
    def __init__(self) -> None:
        self.metadata = _Metadata()
        self.symbology = _Symbology()
        self.timeseries = _Timeseries()


class _Db:
    @staticmethod
    def Historical(**_kwargs):
        return _Historical()


@pytest.fixture(autouse=True)
def _reset_fakes() -> None:
    _Metadata.cost = 0.01
    _Metadata.failure = None
    _Timeseries.calls = 0


def _materialize_repository(root: Path) -> Path:
    source_root = Path(__file__).resolve().parents[2]
    for relative in _INPUT_PATHS:
        source = source_root / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    subprocess.run(["git", "init", "-q"], cwd=root, check=True, shell=False)
    subprocess.run(
        ["git", "config", "user.email", "canary@test"],
        cwd=root,
        check=True,
        shell=False,
    )
    subprocess.run(
        ["git", "config", "user.name", "Canary Test"],
        cwd=root,
        check=True,
        shell=False,
    )
    subprocess.run(["git", "add", "--", "."], cwd=root, check=True, shell=False)
    subprocess.run(
        ["git", "commit", "-q", "-m", "fixture"],
        cwd=root,
        check=True,
        shell=False,
    )
    candidate = root / "artifacts/candidate/FuturesLiveCockpit.exe"
    candidate.parent.mkdir(parents=True)
    candidate.write_bytes(b"synthetic candidate")
    return candidate


def _prepared_plan(root: Path) -> tuple[Path, dict[str, object]]:
    candidate = _materialize_repository(root)
    return prepare_confirmation(
        root,
        candidate_executable=candidate,
        plan_root=root / "manifests/live_cockpit/automatic_history_canary",
    )


def test_automatic_canary_plan_is_one_market_one_chunk_and_isolated(
    tmp_path: Path,
) -> None:
    candidate = _materialize_repository(tmp_path)
    plan = build_plan(tmp_path, candidate_executable=candidate)

    assert plan["scope"] == {
        "dataset": "GLBX.MDP3",
        "market": "ES",
        "market_count": 1,
        "requested_hours": 24,
        "mode": "AUTO",
        "update_origin": "AUTO",
        "expected_terminal_state": "COMPLETE",
    }
    assert plan["limits"]["maximum_estimated_cost_usd"] == "0.05"
    assert plan["limits"]["maximum_automatic_attempts"] == 1
    assert plan["limits"]["maximum_timeseries_downloads"] == 1
    assert plan["limits"]["maximum_live_clients"] == 0
    assert plan["paths"] == {
        "state": STATE_PATH_TEMPLATE,
        "cache": CACHE_PATH_TEMPLATE,
        "terminal": TERMINAL_PATH_TEMPLATE,
    }


def test_automatic_canary_runs_normal_policy_worker_and_persists_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(engine_module, "HISTORY_MAPPING_WAIT_SECONDS", 0.01)
    plan_path, confirmation = _prepared_plan(tmp_path)
    assert confirmation["status"] == "CONFIRMATION_REQUIRED"

    terminal = run_canary(
        tmp_path,
        plan_path=plan_path,
        credential_resolver=lambda: SimpleNamespace(key="db-test"),
        db_module=_Db,
        poll_seconds=0.01,
    )

    assert terminal["status"] == "PASS"
    assert terminal["terminal_state"] == "COMPLETE"
    assert terminal["estimated_cost_usd"] == "0.01"
    assert terminal["update_origin"] == "AUTO"
    assert terminal["selected_market_first"] is True
    assert terminal["cache_validated"] is True
    assert terminal["request_counts"]["timeseries_download"] == 1
    assert terminal["request_counts"]["live_client"] == 0
    assert terminal["request_counts"]["order_or_execution"] == 0
    assert terminal["history_plan_confirmations"] == 1
    assert terminal["last_auto_outcome"] == "COMPLETE"
    assert terminal["restart_recent_attempt_blocked"] is True
    assert terminal["reasons"] == []


def test_automatic_canary_above_cap_stops_before_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(engine_module, "HISTORY_MAPPING_WAIT_SECONDS", 0.01)
    _Metadata.cost = 0.0501
    plan_path, _confirmation = _prepared_plan(tmp_path)

    terminal = run_canary(
        tmp_path,
        plan_path=plan_path,
        credential_resolver=lambda: SimpleNamespace(key="db-test"),
        db_module=_Db,
        poll_seconds=0.01,
    )

    assert terminal["status"] == "FAIL"
    assert terminal["terminal_state"] == "REVIEW_REQUIRED"
    assert terminal["diagnostic_category"] == "COST_LIMIT"
    assert terminal["request_counts"]["timeseries_download"] == 0


def test_automatic_canary_failure_evidence_excludes_credential_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(engine_module, "HISTORY_MAPPING_WAIT_SECONDS", 0.01)
    secret = "db-secret-must-not-appear"
    _Metadata.failure = TimeoutError(f"provider timed out {secret}")
    plan_path, _confirmation = _prepared_plan(tmp_path)

    terminal = run_canary(
        tmp_path,
        plan_path=plan_path,
        credential_resolver=lambda: SimpleNamespace(key=secret),
        db_module=_Db,
        poll_seconds=0.01,
    )

    assert terminal["status"] == "FAIL"
    assert terminal["diagnostic_category"] == "TIMEOUT"
    assert terminal["request_counts"]["timeseries_download"] == 0
    assert secret not in json.dumps(terminal)
