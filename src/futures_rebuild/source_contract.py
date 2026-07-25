"""Steady-state repository-boundary helpers for the active source contract."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from .errors import ContractError


def legacy_roots_from_contract(payload: Mapping[str, object]) -> tuple[Path, ...]:
    """Read a migration-era boundary without making it a runtime dependency."""

    value = payload.get("legacy_repository")
    if value is None:
        if payload.get("external_repository_access") != "FORBIDDEN":
            raise ContractError(
                "retired legacy boundary requires external_repository_access=FORBIDDEN"
            )
        return ()
    if type(value) is not str or not value:
        raise ContractError("source contract legacy_repository is invalid")
    return (Path(value),)
