"""Output and rejection invariants for the measured 1s optimizations."""
from dataclasses import asdict, replace
from hashlib import sha256

import pytest

from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.causal_1s import day_digest
from futures_rebuild.errors import ContractError
from tests.test_causal_1s import sample, BASE, DAY, SECOND

pytestmark = pytest.mark.current


def test_day_digest_matches_recursive_reference_for_every_source_family():
    day = sample(n=67)
    # Extra instance attributes were never part of the dataclass serialization.
    object.__setattr__(day.bars[0], 'unrelated', 'must not enter the digest')
    day = replace(day, references={
        '1m': day.references['1m'], '1h': (day.bars[1],), '1d': (day.bars[2],)},
        support=((BASE + SECOND, 'status', 'a' * 64),))
    expected = sha256(canonical_bytes({'day_ns': day.day_ns}) + b'\n')
    for family, rows in [('definition', day.definitions), ('1s', day.bars),
                         *((c, day.references[c]) for c in ('1m', '1h', '1d')),
                         ('support', day.support)]:
        expected.update(canonical_bytes({'family': family, 'count': len(rows)}) + b'\n')
        for row in rows:
            expected.update(canonical_bytes(row if family == 'support' else asdict(row)) + b'\n')
    assert day_digest(day) == expected.hexdigest()
    assert day_digest(replace(day, bars=day.bars[:-1])) != expected.hexdigest()


@pytest.mark.parametrize('change', [
    {'source_release_id': 'x' * 64}, {'source_manifest_sha256': 'a' * 63},
    {'source_file_sha256': 'A' * 64}, {'row_sha256': 'z' * 64},
    {'source_file_path': '../escape.dbn.zst'}, {'source_file_path': '/absolute.dbn.zst'},
    {'source_file_path': 'synthetic//file.dbn.zst'}, {'source_file_path': ''},
    {'volume': -1}, {'volume': True}, {'publisher_id': 0}, {'instrument_id': 0},
    {'event_at_ns': 0}, {'high_nano': -200_000_000},
])
def test_warm_source_validation_still_rejects_each_bad_row(change):
    bar = sample(n=1).bars[0]
    replace(bar)  # The same valid source fields have already been checked.
    with pytest.raises((ContractError, TypeError)):
        replace(bar, **change)


def test_shared_bar_validation_preserves_values_and_checks_changed_source():
    bar = sample(n=1).bars[0]
    for source in ('synthetic/ohlcv_1s.dbn.zst', 'synthetic/ohlcv_1m.dbn.zst'):
        changed = replace(bar, source_file_path=source, event_at_ns=BASE + DAY + SECOND,
                          row_sha256='f' * 64, volume=0,
                          open_nano=0, high_nano=0, low_nano=0, close_nano=0)
        assert changed.prices == (0, 0, 0, 0)
        assert changed.source_file_path == source
        assert changed.row_sha256 == 'f' * 64


def test_extended_stat_root_preserves_file_change_detection(tmp_path, monkeypatch):
    from futures_rebuild import causal_1s_native as native
    from futures_rebuild.boundary import RepoBoundary, OperationClassification
    from futures_rebuild.errors import IntegrityError
    from tests.test_causal_1s_native import native_fixture

    op, receipt, _ = native_fixture(tmp_path, monkeypatch)
    receipt.consume(RepoBoundary(tmp_path), operation=native.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=native.authorize(tmp_path, op, receipt, consumed=False))
    factory = native.HistoricalDbnSources(tmp_path, op, receipt, _worker_token=native._WORKER_TOKEN)
    factory._permission()
    path = tmp_path / 'src/futures_rebuild/research_gateway_policy.py'
    path.write_bytes(path.read_bytes() + b'\n')
    with pytest.raises(IntegrityError, match='changed during'):
        factory._permission()
