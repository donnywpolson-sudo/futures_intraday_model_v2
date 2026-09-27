"""Cooperative exclusion for timeframe execution, integration and publication.

All launchers must participate. This does not detect or stop legacy workers.
"""
from pathlib import Path
from .locking import FileLease
from .causal_timeframe_storage import contained

LEASE = 'state/locks/causal_timeframe_execution.lock'


def execution_lease(root):
    return FileLease(contained(Path(root), LEASE))
