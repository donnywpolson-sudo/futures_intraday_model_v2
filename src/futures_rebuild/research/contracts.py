"""Fail-closed, synthetic-only contracts for futures research mechanics."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from numbers import Real
import re
from typing import Iterable

import numpy as np


class ResearchContractError(ValueError):
    """A synthetic research-mechanics contract failed closed."""


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def explicit_real(value: object, *, name: str) -> float:
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        raise ResearchContractError(f"{name} must be an explicit real scalar")
    result = float(value)
    if not np.isfinite(result):
        raise ResearchContractError(f"{name} must be finite")
    return result


def explicit_int(value: object, *, name: str) -> int:
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, np.integer)):
        raise ResearchContractError(f"{name} must be an explicit integer")
    return int(value)


def finite_float64(
    value: np.ndarray,
    *,
    name: str,
    ndim: int | None = None,
) -> np.ndarray:
    if not isinstance(value, np.ndarray):
        raise ResearchContractError(f"{name} must be a numpy.ndarray")
    if value.dtype != np.dtype(np.float64):
        raise ResearchContractError(f"{name} must have dtype float64")
    if ndim is not None and value.ndim != ndim:
        raise ResearchContractError(f"{name} must have ndim={ndim}")
    if value.size == 0:
        raise ResearchContractError(f"{name} must be non-empty")
    if not bool(np.all(np.isfinite(value))):
        raise ResearchContractError(f"{name} contains NaN or infinity")
    return value


def int64_vector(value: np.ndarray, *, name: str) -> np.ndarray:
    if not isinstance(value, np.ndarray):
        raise ResearchContractError(f"{name} must be a numpy.ndarray")
    if value.dtype != np.dtype(np.int64) or value.ndim != 1:
        raise ResearchContractError(f"{name} must be a one-dimensional int64 array")
    if value.size == 0:
        raise ResearchContractError(f"{name} must be non-empty")
    return value


def array_sha256(value: np.ndarray) -> str:
    header = f"{value.dtype.str}|{value.shape}".encode("ascii")
    payload = np.ascontiguousarray(value).tobytes(order="C")
    return hashlib.sha256(header + b"\0" + payload).hexdigest()


@dataclass(frozen=True)
class SyntheticOnlyPermit:
    purpose: str
    source_kind: str
    generator_id: str
    seed: int
    dataset_sha256: str
    real_history_authorized: bool = False
    candidate_sealing_authorized: bool = False

    def validate(self) -> None:
        if self.purpose != "MECHANICS_ONLY" or self.source_kind != "SYNTHETIC":
            raise ResearchContractError("only SYNTHETIC MECHANICS_ONLY permits exist")
        if not self.generator_id or not self.generator_id.isascii():
            raise ResearchContractError("generator_id must be non-empty ASCII")
        seed = explicit_int(self.seed, name="seed")
        if not (0 <= seed < 2**64):
            raise ResearchContractError("seed must fit uint64")
        if _SHA256_RE.fullmatch(self.dataset_sha256) is None:
            raise ResearchContractError("dataset_sha256 must be lowercase SHA-256")
        if type(self.real_history_authorized) is not bool or type(
            self.candidate_sealing_authorized
        ) is not bool:
            raise ResearchContractError("authorization flags must be exact bool")
        if self.real_history_authorized or self.candidate_sealing_authorized:
            raise ResearchContractError("synthetic permits cannot authorize history or sealing")


def make_synthetic_permit(
    fixture: np.ndarray,
    *,
    generator_id: str,
    seed: int,
    source_kind: str = "SYNTHETIC",
) -> SyntheticOnlyPermit:
    finite_float64(fixture, name="fixture")
    permit = SyntheticOnlyPermit(
        purpose="MECHANICS_ONLY",
        source_kind=source_kind,
        generator_id=generator_id,
        seed=seed,
        dataset_sha256=array_sha256(fixture),
    )
    permit.validate()
    return permit


def require_synthetic_permit(
    permit: SyntheticOnlyPermit,
    fixture: np.ndarray | None = None,
) -> None:
    if not isinstance(permit, SyntheticOnlyPermit):
        raise ResearchContractError("a SyntheticOnlyPermit is required")
    permit.validate()
    if fixture is not None:
        finite_float64(fixture, name="fixture")
        if array_sha256(fixture) != permit.dataset_sha256:
            raise ResearchContractError("permit does not bind this exact fixture")


def assert_disjoint_partitions(
    fit_indices: np.ndarray,
    audit_indices: np.ndarray,
    *additional_partitions: np.ndarray,
) -> None:
    seen: set[int] = set()
    for number, partition in enumerate((fit_indices, audit_indices, *additional_partitions)):
        values = int64_vector(partition, name=f"partition_{number}")
        as_set = {int(item) for item in values.tolist()}
        if len(as_set) != len(values):
            raise ResearchContractError(f"partition_{number} contains duplicates")
        overlap = seen.intersection(as_set)
        if overlap:
            raise ResearchContractError(
                f"fit/audit partitions overlap at {sorted(overlap)[:5]}"
            )
        seen.update(as_set)


def require_unique_ascii_ids(values: Iterable[str], *, name: str) -> tuple[str, ...]:
    result = tuple(values)
    if not result:
        raise ResearchContractError(f"{name} must be non-empty")
    if any(not isinstance(value, str) or not value or not value.isascii() for value in result):
        raise ResearchContractError(f"{name} must contain non-empty ASCII strings")
    if len(set(result)) != len(result):
        raise ResearchContractError(f"{name} must be unique")
    return result


def require_sha256(value: str, *, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ResearchContractError(f"{name} must be lowercase SHA-256")
    return value
