"""Provider-free production-shaped rehearsal for the causal full-build host."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .canonical import sha256_file, sha256_json
from .causal_full_build_durable_host import (
    DURABLE_HOST_ENVIRONMENT_KEY,
    expected_durable_host_plan,
    run_durable_full_build_worker,
)
from .causal_observation_full_build import (
    _write_create_only,
    validate_full_build_storage_floor,
)
from .causal_observation_market_checkpoint import (
    CHECKPOINT_SET_SCHEMA,
    checkpoint_set_identity,
)
from .causal_observation_parquet import _parquet_io_path, row_count, write_table
from .errors import IntegrityError, UnauthorizedOperation


REHEARSAL_SCHEMA = "causal_full_build_production_rehearsal/1.0.0"
REHEARSAL_CHECKPOINT_SCHEMA = "causal_full_build_production_rehearsal_checkpoint/1.0.0"
REHEARSAL_OUTPUT_CEILING_BYTES = 4 * 1024**2
REHEARSAL_PEAK_ADDITIONAL_BYTES = 8 * 1024**2
REHEARSAL_PATH_LENGTH = 265
_HASH = "a" * 64


class _NetworkDeniedSocket(socket.socket):
    def connect(self, *_args: object, **_kwargs: object) -> None:
        raise RuntimeError("network access is forbidden during the causal rehearsal")

    def connect_ex(self, *_args: object, **_kwargs: object) -> int:
        raise RuntimeError("network access is forbidden during the causal rehearsal")


@contextmanager
def _network_denied() -> Iterator[None]:
    original_socket = socket.socket
    original_create_connection = socket.create_connection

    def deny_connection(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("network access is forbidden during the causal rehearsal")

    socket.socket = _NetworkDeniedSocket
    socket.create_connection = deny_connection
    try:
        yield
    finally:
        socket.socket = original_socket
        socket.create_connection = original_create_connection


def _observation() -> dict[str, object]:
    return {
        "market": "ES",
        "source_contract_id": _HASH,
        "source_release_id": _HASH,
        "source_file_path": "synthetic/ohlcv_1m.dbn.zst",
        "source_file_sha256": _HASH,
        "source_row_sha256": _HASH,
        "source_cadence": "1m",
        "bar_start_ns": 1_000_000_000,
        "bar_end_ns": 61_000_000_000,
        "source_timestamp_ns": 1_000_000_000,
        "available_at_ns": 66_000_000_000,
        "decision_eligible_at_ns": 66_000_000_000,
        "publisher_id": 1,
        "instrument_id": 2,
        "raw_symbol": "ESH5",
        "actual_contract": "ESH5",
        "definition_source_file_path": "synthetic/definition.dbn.zst",
        "definition_source_file_sha256": _HASH,
        "definition_row_sha256": _HASH,
        "definition_event_at_ns": 1,
        "definition_received_at_ns": 2,
        "listing_activation_ns": 0,
        "expiration_ns": 2**64 - 1,
        "open_nano": -100,
        "high_nano": 0,
        "low_nano": -200,
        "close_nano": -50,
        "volume": 5,
        "currency": "USD",
        "min_price_increment_nano": 25,
        "multiplier_nano": 50,
        "project_session_id": "ES:1970-01-01",
        "project_trade_date": "1970-01-01",
        "project_grouping_start_ns": 0,
        "project_grouping_end_ns": 86_400_000_000_000,
        "project_timezone": "America/Chicago",
        "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
    }


def _long_observation_path(root: Path, attempt_id: str) -> Path:
    base = (
        root
        / "state/data_publication_staging/causal_observation_full_development_rehearsal"
        / "ES"
        / attempt_id
        / "2025/2025-07-01_2025-07-13T220000Z/candidate"
    )
    filename = "observations.parquet"
    unpadded = len(str(base / filename))
    target_length = max(REHEARSAL_PATH_LENGTH, unpadded + 8)
    padding = target_length - unpadded - 1
    path = base / ("p" * padding) / filename
    if len(str(path)) != target_length or len(str(path)) < REHEARSAL_PATH_LENGTH:
        raise IntegrityError("rehearsal long-path construction differs")
    return path


def run_production_rehearsal(*, rehearsal_root: Path, source_root: Path) -> dict[str, object]:
    if os.name != "nt":
        raise UnauthorizedOperation("causal full-build production rehearsal requires Windows")
    root = rehearsal_root.resolve(strict=True)
    source = source_root.resolve(strict=True)
    try:
        root.relative_to(source)
    except ValueError:
        pass
    else:
        raise UnauthorizedOperation("rehearsal root is inside the canonical repository")
    if any(root.iterdir()):
        raise IntegrityError("rehearsal root is not empty")

    source_launcher = source / "scripts/start_causal_full_build_v10_worker.ps1"
    launcher = root / "scripts/start_causal_full_build_v10_worker.ps1"
    launcher.parent.mkdir(parents=True)
    shutil.copyfile(source_launcher, launcher)

    attempt_id = sha256_json(
        {"schema_version": REHEARSAL_SCHEMA, "root": str(root).replace("\\", "/")}
    )
    plan_core: dict[str, object] = {
        "target_market": "ES",
        "attempt_id": attempt_id,
        "durable_host": expected_durable_host_plan("ES", attempt_id),
    }
    plan = {**plan_core, "plan_id": sha256_json(plan_core)}
    evidence = root / str(plan["durable_host"]["evidence_path"])
    checkpoint_path = (
        root
        / "state/data_publication_staging/causal_observation_full_development_rehearsal"
        / "ES"
        / attempt_id
        / "market_checkpoint.json"
    )

    def operation() -> dict[str, object]:
        observed_free = shutil.disk_usage(root).free
        required_floor = observed_free - REHEARSAL_PEAK_ADDITIONAL_BYTES
        projected_free = validate_full_build_storage_floor(
            free_bytes=observed_free,
            maximum_peak_additional_bytes=REHEARSAL_PEAK_ADDITIONAL_BYTES,
            minimum_free_after_peak_bytes=required_floor,
        )
        rejection_verified = False
        try:
            validate_full_build_storage_floor(
                free_bytes=observed_free,
                maximum_peak_additional_bytes=REHEARSAL_PEAK_ADDITIONAL_BYTES + 1,
                minimum_free_after_peak_bytes=required_floor,
            )
        except UnauthorizedOperation:
            rejection_verified = True
        if not rejection_verified:
            raise IntegrityError("rehearsal resource ceiling did not fail closed")

        parquet_path = _long_observation_path(root, attempt_id)
        write_table(parquet_path, name="observations", row_groups=((_observation(),),))
        parquet_io_path = Path(_parquet_io_path(parquet_path))
        output_bytes = parquet_io_path.stat().st_size
        if (
            output_bytes <= 0
            or output_bytes > REHEARSAL_OUTPUT_CEILING_BYTES
            or row_count(parquet_path, name="observations") != 1
        ):
            raise UnauthorizedOperation("rehearsal output ceiling or row count differs")

        checkpoint_set = {
            "schema_version": CHECKPOINT_SET_SCHEMA,
            "market_order": ["ES"],
            "production_rehearsal": True,
        }
        checkpoint_core = {
            "schema_version": REHEARSAL_CHECKPOINT_SCHEMA,
            "status": "PASS_SYNTHETIC_MARKET_CHECKPOINT_INACTIVE",
            "market": "ES",
            "attempt_id": attempt_id,
            "checkpoint_set_id": checkpoint_set_identity(checkpoint_set),
            "parquet_sha256": sha256_file(parquet_io_path),
            "parquet_path_length": len(str(parquet_path)),
            "output_bytes": output_bytes,
            "projected_free_after_peak_bytes": projected_free,
            "resource_ceiling_rejection_verified": rejection_verified,
            "provider_calls": 0,
            "source_rows_read": 0,
            "receipt_issued": False,
            "receipt_consumed": False,
            "publication_authorized": False,
            "activation_authorized": False,
        }
        checkpoint = {
            **checkpoint_core,
            "checkpoint_id": sha256_json(checkpoint_core),
        }
        _write_create_only(checkpoint_path, checkpoint)
        create_only_verified = False
        try:
            _write_create_only(checkpoint_path, checkpoint)
        except FileExistsError:
            create_only_verified = True
        if not create_only_verified:
            raise IntegrityError("rehearsal checkpoint was not create-only")
        return {
            "checkpoint_id": checkpoint["checkpoint_id"],
            "checkpoint_create_only_verified": create_only_verified,
            "parquet_path_length": len(str(parquet_path)),
            "output_bytes": output_bytes,
            "resource_ceiling_rejection_verified": rejection_verified,
        }

    previous_task = os.environ.get(DURABLE_HOST_ENVIRONMENT_KEY)
    os.environ[DURABLE_HOST_ENVIRONMENT_KEY] = str(
        plan["durable_host"]["task_name"]
    )
    try:
        with _network_denied():
            operation_result = run_durable_full_build_worker(
                repository_root=root, plan=plan, operation=operation
            )
    finally:
        if previous_task is None:
            os.environ.pop(DURABLE_HOST_ENVIRONMENT_KEY, None)
        else:
            os.environ[DURABLE_HOST_ENVIRONMENT_KEY] = previous_task

    terminal = json.loads((evidence / "exit.json").read_text(encoding="utf-8"))
    heartbeat = json.loads((evidence / "heartbeat.json").read_text(encoding="utf-8"))
    terminal_core = {key: value for key, value in terminal.items() if key != "exit_id"}
    heartbeat_core = {
        key: value for key, value in heartbeat.items() if key != "heartbeat_id"
    }
    if (
        terminal.get("status") != "PASS"
        or terminal.get("exit_id") != sha256_json(terminal_core)
        or heartbeat.get("status") != "TERMINAL"
        or heartbeat.get("heartbeat_id") != sha256_json(heartbeat_core)
        or not checkpoint_path.is_file()
        or (root / "state/authorization_uses").exists()
    ):
        raise IntegrityError("rehearsal terminal evidence differs")

    core = {
        "schema_version": REHEARSAL_SCHEMA,
        "status": "PASS_CAUSAL_FULL_BUILD_PRODUCTION_REHEARSAL",
        "real_launcher_path": "scripts/start_causal_full_build_v10_worker.ps1",
        "real_launcher_sha256": sha256_file(source_launcher),
        "durable_host_exit_id": terminal["exit_id"],
        "durable_host_heartbeat_terminal": True,
        **operation_result,
        "network_denied": True,
        "provider_calls": 0,
        "source_rows_read": 0,
        "receipt_issued": False,
        "receipt_consumed": False,
        "one_use_authority_consumed": False,
        "full_build_executed": False,
        "holdout_rows": 0,
        "forward_rows": 0,
        "publication_authorized": False,
        "activation_authorized": False,
        "scheduled_task_registered": False,
    }
    return {**core, "rehearsal_id": sha256_json(core)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rehearsal-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            run_production_rehearsal(
                rehearsal_root=args.rehearsal_root,
                source_root=args.source_root,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
