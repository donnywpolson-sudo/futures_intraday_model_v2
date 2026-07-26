"""Domain errors used to make every trust boundary fail closed."""


class RebuildError(RuntimeError):
    """Base error for the rebuild foundation."""


class ContractError(RebuildError):
    """A declared causal or schema contract was violated."""


class IntegrityError(RebuildError):
    """Bytes, lineage, or append-only history failed verification."""


class UnauthorizedOperation(RebuildError):
    """An operation crossed an explicit authorization boundary."""


class LeaseBusy(RebuildError):
    """Another writer owns the requested lease."""


class LeaseOwnershipError(RebuildError):
    """A process attempted to release a lease it does not own."""
