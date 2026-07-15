"""Repository-bound production and synthetic clock capabilities."""

from __future__ import annotations

from datetime import datetime, timezone

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .errors import ContractError, IntegrityError
from .time_contracts import require_utc


_PRODUCTION_CLOCK_FACTORY = object()
_CLOCK_BASE_FACTORY = object()
_PRODUCTION_CLASSIFICATIONS = {
    OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
    OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
}


class TrustedClock:
    """Base clock capability; direct construction and unknown subclasses are rejected."""

    __slots__ = ("_boundary", "_receipt", "_last")

    def __init__(
        self,
        boundary: RepoBoundary,
        receipt: OperationReceipt,
        *,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _CLOCK_BASE_FACTORY:
            raise ContractError("trusted clocks require a repository clock factory")
        receipt.verify(boundary)
        self._boundary = boundary
        self._receipt = receipt
        self._last: datetime | None = None

    @property
    def classification(self) -> OperationClassification:
        return self._receipt.classification

    @property
    def repository_id(self) -> str:
        return self._boundary.repository_id

    @property
    def operation_receipt_id(self) -> str:
        return self._receipt.receipt_id

    def _read(self) -> datetime:
        raise ContractError("abstract trusted clock cannot read time")

    def now(self) -> datetime:
        if type(self) not in {ProductionClock, SyntheticClock}:
            raise ContractError("unknown trusted-clock subclass is forbidden")
        value = require_utc(self._read(), "trusted_clock.now")
        if self._last is not None and value < self._last:
            raise IntegrityError("trusted clock moved backwards")
        self._last = value
        return value


class ProductionClock(TrustedClock):
    """Non-overridable wall clock issued only by `issue_production_clock`."""

    __slots__ = ()

    def __init__(
        self,
        boundary: RepoBoundary,
        receipt: OperationReceipt,
        *,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PRODUCTION_CLOCK_FACTORY:
            raise ContractError("production clocks require the production clock factory")
        if receipt.classification not in _PRODUCTION_CLASSIFICATIONS:
            raise ContractError("production clock requires a production-capable receipt")
        super().__init__(boundary, receipt, _factory_token=_CLOCK_BASE_FACTORY)

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError("ProductionClock is final")

    def _read(self) -> datetime:
        return datetime.now(timezone.utc)


class SyntheticClock(TrustedClock):
    """Mutable mechanics-only clock that can never satisfy a production capability."""

    __slots__ = ("_value",)

    def __init__(
        self, boundary: RepoBoundary, receipt: OperationReceipt, value: datetime
    ) -> None:
        receipt.verify(
            boundary,
            classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        )
        super().__init__(boundary, receipt, _factory_token=_CLOCK_BASE_FACTORY)
        self._value = require_utc(value, "synthetic_clock.value")

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError("SyntheticClock is final")

    def _read(self) -> datetime:
        return self._value

    def set(self, value: datetime) -> None:
        self._value = require_utc(value, "synthetic_clock.value")


def issue_production_clock(
    boundary: RepoBoundary, receipt: OperationReceipt
) -> ProductionClock:
    receipt.verify(boundary)
    return ProductionClock(
        boundary,
        receipt,
        _factory_token=_PRODUCTION_CLOCK_FACTORY,
    )


def require_trusted_clock(
    clock: object,
    *,
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
    allow_synthetic: bool,
) -> TrustedClock:
    allowed_types = {ProductionClock, SyntheticClock} if allow_synthetic else {ProductionClock}
    if type(clock) not in allowed_types:
        raise ContractError("an exact repository-issued clock capability is required")
    assert isinstance(clock, TrustedClock)
    if (
        clock.repository_id != boundary.repository_id
        or clock.operation_receipt_id != operation_receipt.receipt_id
        or clock.classification is not operation_receipt.classification
    ):
        raise ContractError("clock capability is bound to another repository or operation")
    operation_receipt.verify(boundary)
    return clock
