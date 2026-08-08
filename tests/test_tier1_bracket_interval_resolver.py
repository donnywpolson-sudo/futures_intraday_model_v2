import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_interval_resolver import classify_source_disposition, resolve_bracket_interval_binding


HASH = "a" * 64


def _sidecar() -> dict[str, object]:
    return {"entry_binding": {"source_bindings": [{"causal_release_id": HASH}]}}


def test_sidecar_causal_interval_selects_exactly_one_index_entry() -> None:
    result = resolve_bracket_interval_binding(
        phase8_index_release_id="i" * 64, sidecar=_sidecar(),
        economics_by_interval=({"causal_release_id": HASH, "economics_release_id": "e" * 64, "interval_key": "ES/2018/x"},),
    )
    assert result.economics_release_id == "e" * 64


def test_missing_or_duplicate_index_entry_and_unknown_disposition_fail_closed() -> None:
    with pytest.raises(IntegrityError, match="no unique"):
        resolve_bracket_interval_binding(phase8_index_release_id="i" * 64, sidecar=_sidecar(), economics_by_interval=())
    with pytest.raises(IntegrityError, match="unknown"):
        classify_source_disposition("SURPRISE")
    assert classify_source_disposition("ELIGIBLE") is True
    assert classify_source_disposition("MISSING") is False
    assert classify_source_disposition("UNRESOLVED_FAIL_CLOSED") is False
