from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit.engine import (
    HISTORY_REQUEST_TIMEOUT_SECONDS,
    SYMBOL_REQUEST_TIMEOUT_SECONDS,
)
from futures_rebuild.live_cockpit.history_canary import (
    CACHE_PATH_TEMPLATE,
    EXPECTED_MARKET_COUNT,
    MAX_COST_CALLS,
    MAX_DATASET_RANGE_CALLS,
    MAX_DURATION_SECONDS,
    MAX_SYMBOLOGY_CALLS,
    OPERATION,
    TERMINAL_PATH_TEMPLATE,
    CanaryContractError,
    _parser,
    build_plan,
    prepare_confirmation,
    run_canary,
)


class _FakeMetadata:
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.failure = failure
        self.range_calls = 0
        self.cost_calls = 0

    def get_dataset_range(self, **_kwargs):
        self.range_calls += 1
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
        self.cost_calls += 1
        return 0.01


class _FakeSymbology:
    omit_last = False

    def __init__(self) -> None:
        self.calls = 0
        self.market_by_instrument: dict[int, str] = {}

    def resolve(self, **kwargs):
        self.calls += 1
        if kwargs["stype_out"] == "instrument_id":
            result = {}
            symbols = list(kwargs["symbols"])
            if self.omit_last:
                symbols = symbols[:-1]
            for index, query in enumerate(symbols):
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


class _FakeHistorical:
    def __init__(self, *, metadata_failure: Exception | None = None) -> None:
        self.metadata = _FakeMetadata(failure=metadata_failure)
        self.symbology = _FakeSymbology()
        self.timeseries = SimpleNamespace(
            get_range=lambda **_kwargs: pytest.fail("timeseries must be unreachable")
        )


class _FakeDb:
    metadata_failure: Exception | None = None
    instances: list[_FakeHistorical] = []

    @classmethod
    def Historical(cls, **_kwargs):
        instance = _FakeHistorical(metadata_failure=cls.metadata_failure)
        cls.instances.append(instance)
        return instance


@pytest.fixture(autouse=True)
def _reset_fake_db() -> None:
    _FakeDb.metadata_failure = None
    _FakeDb.instances = []
    _FakeSymbology.omit_last = False


def _materialize_runner(root: Path) -> None:
    source_root = Path(__file__).resolve().parents[2]
    for relative in (
        "src/futures_rebuild/live_cockpit/history_canary.py",
        "src/futures_rebuild/live_cockpit/engine.py",
        "src/futures_rebuild/live_cockpit/protocol.py",
        "src/futures_rebuild/live_cockpit/credentials.py",
        "src/futures_rebuild/live_cockpit/cache.py",
        "src/futures_rebuild/live_cockpit/history.py",
        "src/futures_rebuild/live_cockpit/market_groups.py",
        "configs/source_contract.json",
    ):
        source = source_root / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def _git_repo(root: Path) -> None:
    import subprocess

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


def _plan_and_confirmation(
    root: Path,
    *,
    predecessor_terminal: Path | None = None,
) -> tuple[Path, dict[str, object]]:
    plan_path, confirmation = prepare_confirmation(
        root,
        plan_root=root / "manifests/live_cockpit/history_canary/plans",
        predecessor_terminal=predecessor_terminal,
    )
    return plan_path, confirmation


def _timeout_predecessor(root: Path) -> Path:
    body = {
        "schema_version": "live_cockpit_history_canary_terminal/1.0.0",
        "plan_id": "predecessor-plan",
        "requested_start": 1_700_000_000,
        "historical_end": 1_700_604_800,
        "request_counts": {
            "dataset_range": 1,
            "symbology": 2,
            "cost_estimate": 0,
            "timeseries_download": 0,
            "live_client": 0,
            "production_cache_write": 0,
            "provider_failure_retry": 0,
        },
        "estimated_cost_usd": None,
        "terminal_state": "ERROR",
        "diagnostic_category": "TIMEOUT",
    }
    body["terminal_id"] = sha256_json(body)
    target = root / "reports/live_cockpit/history_canary/predecessor/terminal.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_bytes(body) + b"\n")
    return target


def test_canary_plan_is_exact_metadata_only_and_bounded(tmp_path: Path) -> None:
    _materialize_runner(tmp_path)
    _git_repo(tmp_path)
    plan = build_plan(tmp_path)
    assert plan["scope"] == {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "market_count": EXPECTED_MARKET_COUNT,
        "requested_hours": 168,
        "expected_terminal_state": "CONFIRMATION_REQUIRED",
    }
    assert plan["limits"] == {
        "maximum_dataset_range_calls": MAX_DATASET_RANGE_CALLS,
        "maximum_symbology_calls": MAX_SYMBOLOGY_CALLS,
        "maximum_cost_estimate_calls": MAX_COST_CALLS,
        "maximum_duration_seconds": MAX_DURATION_SECONDS,
        "maximum_timeseries_downloads": 0,
        "maximum_live_clients": 0,
        "maximum_provider_failure_retries": 0,
        "maximum_production_cache_writes": 0,
    }
    assert plan["paths"] == {
        "cache": CACHE_PATH_TEMPLATE,
        "terminal": TERMINAL_PATH_TEMPLATE,
    }


def test_timeout_successor_binds_predecessor_and_uses_bounded_timeout(
    tmp_path: Path,
) -> None:
    _materialize_runner(tmp_path)
    predecessor = _timeout_predecessor(tmp_path)
    _git_repo(tmp_path)
    original = build_plan(tmp_path)
    successor = build_plan(tmp_path, predecessor_terminal=predecessor)
    assert SYMBOL_REQUEST_TIMEOUT_SECONDS == HISTORY_REQUEST_TIMEOUT_SECONDS == 30
    assert successor["plan_id"] != original["plan_id"]
    assert successor["predecessor"] == {
        "path": "reports/live_cockpit/history_canary/predecessor/terminal.json",
        "sha256": sha256_file(predecessor),
        "terminal_id": json.loads(
            predecessor.read_text(encoding="utf-8")
        )["terminal_id"],
        "terminal_state": "ERROR",
        "diagnostic_category": "TIMEOUT",
    }
    plan_path, _confirmation = _plan_and_confirmation(
        tmp_path,
        predecessor_terminal=predecessor,
    )
    assert json.loads(plan_path.read_text(encoding="utf-8")) == successor


def test_timeout_successor_rejects_predecessor_drift_before_provider(
    tmp_path: Path,
) -> None:
    _materialize_runner(tmp_path)
    predecessor = _timeout_predecessor(tmp_path)
    _git_repo(tmp_path)
    plan_path, _confirmation = _plan_and_confirmation(
        tmp_path,
        predecessor_terminal=predecessor,
    )
    predecessor.write_text("{}", encoding="utf-8")
    with pytest.raises(CanaryContractError, match="predecessor"):
        run_canary(
            tmp_path,
            plan_path=plan_path,
            credential_resolver=lambda: pytest.fail("credential resolver must not run"),
            db_module=_FakeDb,
        )
    assert _FakeDb.instances == []


def test_canary_stops_at_confirmation_without_live_or_timeseries(
    tmp_path: Path,
) -> None:
    _materialize_runner(tmp_path)
    _git_repo(tmp_path)
    plan_path, confirmation = _plan_and_confirmation(tmp_path)
    assert confirmation["status"] == "CONFIRMATION_REQUIRED"
    assert "approval_to_paste" not in confirmation
    terminal = run_canary(
        tmp_path,
        plan_path=plan_path,
        credential_resolver=lambda: SimpleNamespace(key="db-test"),
        db_module=_FakeDb,
    )
    assert terminal["terminal_state"] == "CONFIRMATION_REQUIRED"
    assert terminal["diagnostic_category"] is None
    assert terminal["request_counts"] == {
        "dataset_range": 1,
        "symbology": 2,
        "cost_estimate": 8,
        "timeseries_download": 0,
        "live_client": 0,
        "production_cache_write": 0,
        "provider_failure_retry": 0,
    }
    assert terminal["requested_start"] < terminal["historical_end"]
    assert terminal["estimated_cost_usd"] == pytest.approx(0.08)
    terminal_path = tmp_path / TERMINAL_PATH_TEMPLATE.replace(
        "<PLAN_ID>", terminal["plan_id"]
    )
    assert json.loads(terminal_path.read_text(encoding="utf-8")) == terminal
    assert len(_FakeDb.instances) == 1


def test_canary_confirmation_summary_is_not_an_execution_token(
    tmp_path: Path,
) -> None:
    _materialize_runner(tmp_path)
    _git_repo(tmp_path)
    _plan_path, confirmation = _plan_and_confirmation(tmp_path)
    assert confirmation["operation"] == OPERATION
    assert confirmation["limits"]["maximum_timeseries_downloads"] == 0
    assert confirmation["preservation"]["production_cache"] == "NO_ACCESS_NO_MUTATION"
    assert "approval_to_paste" not in confirmation


def test_canary_cli_has_no_approval_line_flag() -> None:
    assert not hasattr(_parser().parse_args(["run", "--plan", "plan.json"]), "approval_line")


def test_canary_failure_receipt_is_sanitized_and_create_only(tmp_path: Path) -> None:
    _materialize_runner(tmp_path)
    _git_repo(tmp_path)
    plan_path, _confirmation = _plan_and_confirmation(tmp_path)
    secret = "db-canary-secret"
    _FakeDb.metadata_failure = TimeoutError(f"read timed out {secret}")
    terminal = run_canary(
        tmp_path,
        plan_path=plan_path,
        credential_resolver=lambda: SimpleNamespace(key="db-test"),
        db_module=_FakeDb,
    )
    assert terminal["terminal_state"] == "ERROR"
    assert terminal["diagnostic_category"] == "TIMEOUT"
    assert terminal["request_counts"]["dataset_range"] == 1
    assert terminal["request_counts"]["cost_estimate"] == 0
    assert terminal["request_counts"]["timeseries_download"] == 0
    assert secret not in json.dumps(terminal)
    with pytest.raises(CanaryContractError, match="create-only"):
        run_canary(
            tmp_path,
            plan_path=plan_path,
            credential_resolver=lambda: SimpleNamespace(key="db-test"),
            db_module=_FakeDb,
        )
    terminal_path = tmp_path / TERMINAL_PATH_TEMPLATE.replace(
        "<PLAN_ID>", terminal["plan_id"]
    )
    assert sha256_file(terminal_path) == sha256_file(terminal_path)


def test_canary_fails_closed_when_exact_market_universe_is_not_resolved(
    tmp_path: Path,
) -> None:
    _materialize_runner(tmp_path)
    _git_repo(tmp_path)
    plan_path, _confirmation = _plan_and_confirmation(tmp_path)
    _FakeSymbology.omit_last = True
    terminal = run_canary(
        tmp_path,
        plan_path=plan_path,
        credential_resolver=lambda: SimpleNamespace(key="db-test"),
        db_module=_FakeDb,
    )
    assert terminal["terminal_state"] == "ERROR"
    assert terminal["diagnostic_category"] == "UNAVAILABLE"
    assert terminal["request_counts"]["symbology"] == 2
    assert terminal["request_counts"]["timeseries_download"] == 0
