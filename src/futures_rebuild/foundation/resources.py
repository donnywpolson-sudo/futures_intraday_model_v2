"""Fail-closed capacity admission and runtime guards for foundation builds."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from ..canonical import canonical_bytes, sha256_json
from ..errors import ContractError, IntegrityError


RESOURCE_POLICY_SCHEMA_VERSION = "1.0.0"


@dataclass(frozen=True)
class FoundationResourcePolicy:
    minimum_free_reserve_bytes: int
    minimum_free_reserve_fraction: Decimal
    selected_compressed_input_multiplier: int
    maximum_next_batch_output_bytes: int

    @classmethod
    def from_file(cls, path: Path) -> "FoundationResourcePolicy":
        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("foundation resource policy is invalid JSON") from exc
        if (
            not isinstance(payload, dict)
            or raw != canonical_bytes(payload) + b"\n"
            or set(payload)
            != {
                "maximum_next_batch_output_bytes",
                "minimum_free_reserve_bytes",
                "minimum_free_reserve_fraction",
                "schema_version",
                "selected_compressed_input_multiplier",
            }
            or payload.get("schema_version") != RESOURCE_POLICY_SCHEMA_VERSION
        ):
            raise IntegrityError("foundation resource policy contract is invalid")
        try:
            fraction = Decimal(str(payload["minimum_free_reserve_fraction"]))
            result = cls(
                minimum_free_reserve_bytes=int(payload["minimum_free_reserve_bytes"]),
                minimum_free_reserve_fraction=fraction,
                selected_compressed_input_multiplier=int(
                    payload["selected_compressed_input_multiplier"]
                ),
                maximum_next_batch_output_bytes=int(
                    payload["maximum_next_batch_output_bytes"]
                ),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise IntegrityError("foundation resource policy values are invalid") from exc
        if (
            result.minimum_free_reserve_bytes <= 0
            or not Decimal("0") < fraction < Decimal("1")
            or result.selected_compressed_input_multiplier < 1
            or result.maximum_next_batch_output_bytes <= 0
        ):
            raise ContractError("foundation resource policy values are out of range")
        return result

    @property
    def policy_id(self) -> str:
        return sha256_json(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "maximum_next_batch_output_bytes": self.maximum_next_batch_output_bytes,
            "minimum_free_reserve_bytes": self.minimum_free_reserve_bytes,
            "minimum_free_reserve_fraction": str(
                self.minimum_free_reserve_fraction
            ),
            "schema_version": RESOURCE_POLICY_SCHEMA_VERSION,
            "selected_compressed_input_multiplier": (
                self.selected_compressed_input_multiplier
            ),
        }

    def reserve_bytes(self, total_volume_bytes: int) -> int:
        if type(total_volume_bytes) is not int or total_volume_bytes <= 0:
            raise ContractError("capacity volume size must be a positive integer")
        fractional = int(
            Decimal(total_volume_bytes) * self.minimum_free_reserve_fraction
        )
        return max(self.minimum_free_reserve_bytes, fractional)


def selected_compressed_bytes(selection: Mapping[str, object]) -> int:
    files = selection.get("files")
    if not isinstance(files, list) or not files:
        raise IntegrityError("capacity admission lacks a selected-file index")
    seen: set[str] = set()
    total = 0
    for item in files:
        if not isinstance(item, dict):
            raise IntegrityError("capacity selection file entry is invalid")
        path = item.get("path")
        size = item.get("size")
        if type(path) is not str or not path or path in seen:
            raise IntegrityError("capacity selection path is invalid or duplicated")
        if type(size) is not int or size < 0:
            raise IntegrityError("capacity selection size is invalid")
        seen.add(path)
        total += size
    if total <= 0:
        raise IntegrityError("capacity selected-byte census is empty")
    return total


def assert_capacity_admission(
    *,
    volume_path: Path,
    selection: Mapping[str, object],
    policy: FoundationResourcePolicy,
) -> dict[str, object]:
    usage = shutil.disk_usage(volume_path)
    selected_bytes = selected_compressed_bytes(selection)
    reserve = policy.reserve_bytes(usage.total)
    output_budget = selected_bytes * policy.selected_compressed_input_multiplier
    required = reserve + output_budget
    core = {
        "available_free_bytes": usage.free,
        "estimated_output_budget_bytes": output_budget,
        "minimum_reserve_bytes": reserve,
        "policy_id": policy.policy_id,
        "required_free_bytes": required,
        "selected_compressed_bytes": selected_bytes,
        "status": "PASS" if usage.free >= required else "FAIL",
        "total_volume_bytes": usage.total,
    }
    if usage.free < required:
        raise IntegrityError(
            "foundation capacity admission failed before checkpoint creation"
        )
    return {**core, "capacity_admission_id": sha256_json(core)}


def assert_runtime_capacity(
    *, volume_path: Path, policy: FoundationResourcePolicy
) -> int:
    usage = shutil.disk_usage(volume_path)
    required = (
        policy.reserve_bytes(usage.total)
        + policy.maximum_next_batch_output_bytes
    )
    if usage.free < required:
        raise IntegrityError("foundation runtime capacity reserve would be crossed")
    return usage.free
