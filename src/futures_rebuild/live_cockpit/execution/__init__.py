"""Fail-closed execution domain for the Futures Live Cockpit.

Importing this package never creates a provider client or opens a connection.
"""

from .domain import ExecutionMode, OrderIntent
from .runtime import ExecutionRuntime

__all__ = ["ExecutionMode", "ExecutionRuntime", "OrderIntent"]
