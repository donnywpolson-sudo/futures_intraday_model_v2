"""Execution-boundary exceptions with safe, non-secret messages."""


class ExecutionError(RuntimeError):
    """Base error for local execution-domain failures."""


class ExecutionBlocked(ExecutionError):
    """The requested operation did not pass a required gate."""


class TransportError(ExecutionError):
    """A sanitized transport failure."""


class UnknownBrokerState(ExecutionError):
    """Broker state could not be reconciled safely."""
