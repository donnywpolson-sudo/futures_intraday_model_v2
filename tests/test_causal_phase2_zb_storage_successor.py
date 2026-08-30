from pathlib import Path

import pytest

from futures_rebuild.causal_phase2_zb_storage_successor import (
    FAILED_ATTEMPT_ID,
    MAXIMUM_PEAK_ADDITIONAL_BYTES,
    REQUIRED_FREE_AFTER_PEAK_BYTES,
    _failed_preflight,
)


pytestmark = pytest.mark.current


def test_zb_storage_successor_preserves_the_pre_row_failure() -> None:
    root = Path(__file__).resolve().parents[1]
    terminal = _failed_preflight(root)
    assert terminal["status"] == "FAILED"
    assert terminal["error_message"] == "ZB free-space floor would be breached"
    assert terminal["automatic_restart_authorized"] is False
    assert MAXIMUM_PEAK_ADDITIONAL_BYTES == 20_000_000_000
    assert REQUIRED_FREE_AFTER_PEAK_BYTES == 107_374_182_400
