from pathlib import Path

import pytest

from futures_rebuild.causal_phase2_6j_successor import (
    MAXIMUM_PEAK_ADDITIONAL_BYTES,
    REQUIRED_FREE_AFTER_PEAK_BYTES,
    ZB_CERTIFICATE_ID,
    ZB_CERTIFICATE_PATH,
    ZB_CERTIFICATE_SHA256,
    _exact_zb_certificate,
)
from futures_rebuild.canonical import sha256_file


pytestmark = pytest.mark.current


def test_6j_successor_is_bound_to_the_exact_inactive_zb_certificate() -> None:
    root = Path(__file__).resolve().parents[1]
    certificate = _exact_zb_certificate(root, ZB_CERTIFICATE_PATH.as_posix())
    assert sha256_file(root / ZB_CERTIFICATE_PATH) == ZB_CERTIFICATE_SHA256
    assert certificate["certificate_id"] == ZB_CERTIFICATE_ID
    assert certificate["status"] == "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
    assert MAXIMUM_PEAK_ADDITIONAL_BYTES == 20_000_000_000
    assert REQUIRED_FREE_AFTER_PEAK_BYTES == 107_374_182_400
