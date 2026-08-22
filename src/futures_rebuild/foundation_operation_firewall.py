"""Mandatory operation firewall for current causal-source preparation.

The historical dual-resolution foundation is retired.  Its unchanged runner
cannot acquire a current context, and every effect-capable construction path
in its implementation calls this firewall on every invocation.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .errors import IntegrityError, UnauthorizedOperation


CURRENT_SOURCE_CLOSURE_OPERATION = "PREPARE_CURRENT_CAUSAL_SOURCE_CLOSURE_METADATA_ONLY"
CURRENT_SOURCE_CLOSURE_ENTRY_POINT = "futures_rebuild.causal_source_closure"
RETIRED_DUAL_RESOLUTION_OPERATION = "RUN_DUAL_RESOLUTION_TIER01_FOUNDATION"
RETIRED_STATUS = "RETIRED_NO_READ"
_CONTEXT_SEAL = object()
_FALSE_AUTHORITY = {
    "deletion": False,
    "evaluation": False,
    "features": False,
    "fitting": False,
    "forward": False,
    "holdout": False,
    "labels": False,
    "mechanism_execution": False,
    "payload_or_row_read": False,
    "prediction": False,
    "provider": False,
    "registration": False,
    "wfa": False,
}


class RetiredFoundationOperation(UnauthorizedOperation):
    """The caller attempted to enter a retired foundation operation."""


@dataclass(frozen=True, slots=True)
class FoundationOperationContext:
    operation: str
    source_contract_id: str
    entry_point: str
    authority: Mapping[str, bool]
    _seal: object


def _load_contract(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise IntegrityError("current source contract is not an object")
    return value


def issue_current_source_closure_context(
    repository_root: Path,
    *,
    contract_path: Path | None = None,
) -> FoundationOperationContext:
    """Issue a non-row-reading context from one exact current source contract."""

    root = repository_root.resolve(strict=True)
    path = (contract_path or root / "configs/source_contract.json").resolve(strict=True)
    contract = _load_contract(path)
    contract_id = contract.get("contract_id")
    boundary = contract.get("operation_boundary")
    if (
        contract.get("schema_version") != "canonical_dbn_source_contract/4.0.0"
        or re.fullmatch(r"[0-9a-f]{64}", str(contract_id)) is None
        or not isinstance(boundary, dict)
        or boundary.get("current_operation") != CURRENT_SOURCE_CLOSURE_OPERATION
        or boundary.get("current_entry_point") != CURRENT_SOURCE_CLOSURE_ENTRY_POINT
        or boundary.get("retired_operation") != RETIRED_DUAL_RESOLUTION_OPERATION
        or boundary.get("missing_or_unknown_context") != RETIRED_STATUS
        or contract.get("authority") != {
            "activation": False,
            "deletion": False,
            "evaluation": False,
            "holdout": False,
            "model": False,
            "provider": False,
            "row_read": False,
            "trading": False,
        }
    ):
        raise UnauthorizedOperation("current source-closure context authority is invalid")
    return FoundationOperationContext(
        operation=CURRENT_SOURCE_CLOSURE_OPERATION,
        source_contract_id=str(contract_id),
        entry_point=CURRENT_SOURCE_CLOSURE_ENTRY_POINT,
        authority=dict(_FALSE_AUTHORITY),
        _seal=_CONTEXT_SEAL,
    )


def require_current_source_closure_context(
    context: FoundationOperationContext,
    *,
    source_contract_id: str,
) -> None:
    """Validate the explicit context on every metadata source operation."""

    if (
        type(context) is not FoundationOperationContext
        or context._seal is not _CONTEXT_SEAL
        or context.operation != CURRENT_SOURCE_CLOSURE_OPERATION
        or context.entry_point != CURRENT_SOURCE_CLOSURE_ENTRY_POINT
        or context.source_contract_id != source_contract_id
        or dict(context.authority) != _FALSE_AUTHORITY
    ):
        raise UnauthorizedOperation("current source-closure operation context is absent or invalid")


def reject_retired_dual_resolution_operation(operation_context: object = None) -> None:
    """Reject the retired operation on every sensitive legacy call.

    The rejecting default is intentional: the immutable historical runner has
    no operation-context parameter.  No context, including a valid current
    source-closure context, can authorize this retired operation.
    """

    del operation_context
    raise RetiredFoundationOperation(
        f"{RETIRED_STATUS}: {RETIRED_DUAL_RESOLUTION_OPERATION} is historical evidence only"
    )
