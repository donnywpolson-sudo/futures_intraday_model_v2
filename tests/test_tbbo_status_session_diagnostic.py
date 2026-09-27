import pytest

from futures_rebuild import tbbo_status_session_diagnostic as diagnostic
from futures_rebuild import research_gateway_policy


pytestmark = pytest.mark.current


def test_status_snapshot_classification_is_aggregate_only() -> None:
    midnight_plus_one = 1
    assert diagnostic._window(0) == "EXACT_UTC_MIDNIGHT"
    assert diagnostic._window(midnight_plus_one) == "FIRST_UTC_SECOND"
    assert diagnostic._age(2 * 86_400_000_000_000, 0) == "AT_LEAST_ONE_DAY"
    assert diagnostic.OPERATION in research_gateway_policy.PREPARATORY_REAL_HISTORY_OPERATIONS
