"""Publication order survives canonical JSON; no dependence on object-key order."""
import json
from pathlib import Path
import pytest

from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.errors import IntegrityError
from tests import test_daily_rollout_memory as previous

pytestmark = pytest.mark.current
REL = 'state/unpublished_evidence/daily_admission_v1/rollout_order_v1'
coord = previous.load('test_explicit_daily_order', REL + '/coordinator.py')


def test_order_survives_canonical_serialization():
    order = list(coord.PUBLICATION_ORDER)
    plan = dict(publication_order=order, changes={p:'candidate' for p in reversed(order)},
        predecessors={p:None for p in reversed(order)})
    decoded = json.loads(canonical_bytes(plan))
    assert list(decoded['changes']) != order
    changes, predecessors = coord.ordered_controls(decoded)
    assert list(changes) == list(predecessors) == order
    with pytest.raises(IntegrityError, match='order differs'):
        coord.ordered_controls(dict(decoded, publication_order=list(reversed(order))))
    with pytest.raises(IntegrityError, match='Control set'):
        coord.ordered_controls(dict(decoded, changes={}))


def test_real_coordinator_roundtripped_plan_and_backup_order(tmp_path, monkeypatch):
    original = coord.ordered_controls
    calls = []
    def roundtrip(plan):
        plan = json.loads(canonical_bytes(dict(plan, publication_order=list(coord.PUBLICATION_ORDER))))
        assert list(plan['changes']) != list(coord.PUBLICATION_ORDER)
        result = original(plan)
        calls.append(list(result[0]))
        return result
    monkeypatch.setattr(coord, 'ordered_controls', roundtrip)
    monkeypatch.setattr(previous, 'coord', coord)
    previous.test_full_coordinator_uses_small_worker_results(tmp_path, monkeypatch)
    assert calls == [list(coord.PUBLICATION_ORDER)]
    rollback = tmp_path/REL/'rollback'
    assert (rollback/'0.predecessor').exists()
    assert (rollback/'3.predecessor').exists()
    assert not (rollback/'4.predecessor').exists()
