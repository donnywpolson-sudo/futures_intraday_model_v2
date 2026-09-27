from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import rolling_hour_predictive_source as s
from futures_rebuild import rolling_hour_preparation as prior
from futures_rebuild.certified_research_gateway import CertifiedResearchGateway
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation

pytestmark = pytest.mark.current


def columns():
    t = np.arange(1000, 1125, dtype=np.int64) * s.r.old.MINUTE
    return {'bar_start_ns': t, 'project_trade_date': np.full(125, '2018-01-02'),
        'actual_contract': np.full(125, 'A'), 'available_at_ns': t + s.r.old.MINUTE + s.r.old.DELAY,
        'decision_eligible_at_ns': t + s.r.old.MINUTE + s.r.old.DELAY,
        'definition_received_at_ns': t - 1}


def test_structural_windows_endpoints_missing_path_roll_and_late_input():
    c = columns()
    base = s.structural_support(c)
    assert base['feature_window_candidates'] == 65
    assert base['same_contract_endpoint_candidates'] == 3
    assert base['fixed_five_second_availability_rows'] == 125
    missing = {k: np.delete(v, 90) for k, v in c.items()}
    assert s.structural_support(missing)['same_contract_endpoint_candidates'] == 3
    c['actual_contract'][100:] = 'B'
    assert s.structural_support(c)['same_contract_endpoint_candidates'] == 0
    assert s.structural_support(c)['different_contract_endpoints'] == 3
    c = columns()
    c['definition_received_at_ns'][0] = c['bar_start_ns'][-1]
    assert s.structural_support(c)['same_contract_endpoint_candidates'] == 2
    c['bar_start_ns'][1] = c['bar_start_ns'][0]
    with pytest.raises(IntegrityError, match='duplicate'):
        s.structural_support(c)


def source_fixture(monkeypatch):
    session, rows, _ = prior.synthetic_session()
    contract, release = 'b' * 64, 'c' * 64
    for row in rows:
        row.update(source_contract_id=contract, source_release_id=release,
                   definition_received_at_ns=row['bar_start_ns'] - 1)
    class Calendar:
        receipt = SimpleNamespace(release_id='d' * 64)
        sessions = {(session.market, session.trade_date): session}
        def verify(self):
            pass
        def require_coverage(self, markets, start, end):
            assert markets == [session.market] and str(start) == '2018-01-01' and str(end) == '2022-12-31'
    monkeypatch.setattr(s.r.old, 'VerifiedExchangeCalendar', Calendar)
    day = session.trade_date.isoformat()
    binding = {'mechanism_id': s.r.mechanism_id(), 'market': session.market, 'trade_date': day,
        'source_contract_id': contract, 'source_release_id': release, 'calendar_release_id': 'd' * 64,
        'availability_basis': 'OBSERVED_RECEIPT_TIMES', 'calendar_basis': 'CERTIFIED_ORIGINAL_PUBLICATION',
        'calendar_known_at_ns': rows[0]['bar_start_ns'] - 1,
        'source_file_sha256s': [rows[0]['source_file_sha256']],
        'calendar_evidence_sha256': 'e' * 64, 'availability_evidence_sha256': 'f' * 64,
        'source_certificate_sha256': 'a' * 64}
    return rows, binding, dict(sessions=[session], rows_by_date={day: rows}, calendar=Calendar(),
        source_contract_id=contract, source_release_id=release,
        bindings={day: s.r.old.profit.bind(binding, 'binding_id')}, as_of_ns=rows[-1]['decision_eligible_at_ns'])


def test_source_bridge_retains_unobservable_forecasts_and_rejects_assumed_clock(monkeypatch):
    rows, binding, args = source_fixture(monkeypatch)
    d = s.adapt_source_sessions(**args)
    assert len(d.forecast_records) == 3
    assert s.fold_support(d)['status'] == 'BLOCKED'
    rows[0]['official_schedule_state'] = 'UNKNOWN_FAIL_CLOSED'
    assert len(s.adapt_source_sessions(**args).forecast_records) == 3
    assert rows[0]['official_schedule_state'] == 'UNKNOWN_FAIL_CLOSED'
    rows[-3]['actual_contract'] = 'OTHER'
    changed = s.adapt_source_sessions(**args)
    assert len(changed.forecast_records) == 3 and len(changed.opportunities) == 2
    day = binding['trade_date']
    for key, value in [('availability_basis', 'BAR_END_PLUS_FIVE_SECONDS_ASSUMED'),
                       ('calendar_known_at_ns', rows[-1]['bar_start_ns']),
                       ('calendar_basis', 'RETROSPECTIVE')]:
        bad = deepcopy(binding)
        bad[key] = value
        with pytest.raises(IntegrityError):
            s.adapt_source_sessions(**{**args, 'bindings': {day: s.r.old.profit.bind(bad, 'binding_id')}})


def test_gateway_rejects_duck_typed_receipt_before_data_use():
    # No constructor or active repository access needed to exercise the early boundary.
    gateway = object.__new__(CertifiedResearchGateway)
    with pytest.raises(UnauthorizedOperation, match='verified readiness receipt'):
        gateway.prepare_predictive_readiness(receipt=SimpleNamespace(), sessions=[], rows_by_date={},
            calendar=None, source_contract_id='b' * 64, source_release_id='c' * 64,
            bindings={}, as_of_ns=1)


def test_gateway_readiness_connection_and_changed_evidence(monkeypatch, tmp_path):
    import json
    from futures_rebuild import certified_research_gateway as gateway_module
    from futures_rebuild import rolling_hour_predictive_preparation as preparation
    from futures_rebuild.canonical import sha256_file, sha256_json
    rows, binding, args = source_fixture(monkeypatch)
    for prefix in ('calendar_evidence', 'availability_evidence', 'source_certificate'):
        path = tmp_path / (prefix + '.txt')
        path.write_text('generated fixture ' + prefix)
        binding[prefix + '_path'] = path.name
        binding[prefix + '_sha256'] = sha256_file(path)
    certificate = s.r.old.profit.bind({
        'schema_version': 'rolling_hour_predictive_source_certificate/1', 'evidence_class': 'ROW_CERTIFIED',
        'status': 'PASS', 'bundle_id': 'test', 'source_contract_id': args['source_contract_id'],
        'source_release_id': args['source_release_id'], 'calendar_release_id': 'd' * 64,
        'source_facts_sha256': s.source_facts_hash({binding['trade_date']: binding}),
        'checks': {key: 'PASS' for key in ('calendar_publication', 'calendar_applicability', 'bar_availability',
                                        'instrument_identity', 'price_quality')}}, 'certificate_id')
    (tmp_path / 'source_certificate.txt').write_text(json.dumps(certificate))
    binding['source_certificate_sha256'] = sha256_file(tmp_path / 'source_certificate.txt')
    args['bindings'] = {binding['trade_date']: s.r.old.profit.bind(binding, 'binding_id')}
    class Receipt:
        scope = {'purpose': 'PREDICTIVE_SOURCE_READINESS_NO_FIT', 'bundle_id': 'test',
            'source_contract_id': args['source_contract_id'], 'source_release_id': args['source_release_id'],
            'source_bindings_sha256': sha256_json(args['bindings']), 'calendar_release_id': 'd' * 64}
        consumed = False
        def assert_consumed(self, *a, **kw):
            self.consumed = True
    monkeypatch.setattr(gateway_module, 'OperationReceipt', Receipt)
    monkeypatch.setattr(preparation, 'validate_bundle', lambda root: {'bundle_id': 'test'})
    gateway = object.__new__(CertifiedResearchGateway)
    object.__setattr__(gateway, 'root', tmp_path)
    object.__setattr__(gateway, 'boundary', object())
    receipt = Receipt()
    dataset, support = gateway.prepare_predictive_readiness(receipt=receipt, **args)
    assert receipt.consumed and len(dataset.forecast_records) == 3
    assert support['status'] == 'BLOCKED'  # One generated date cannot satisfy 252/505.
    (tmp_path / 'availability_evidence.txt').write_text('changed')
    with pytest.raises(IntegrityError, match='evidence changed'):
        gateway.prepare_predictive_readiness(receipt=receipt, **args)
    (tmp_path / 'availability_evidence.txt').write_text('generated fixture availability_evidence')
    certificate.pop('certificate_id')
    certificate['status'] = 'BLOCKED'
    (tmp_path / 'source_certificate.txt').write_text(json.dumps(s.r.old.profit.bind(certificate, 'certificate_id')))
    binding['source_certificate_sha256'] = sha256_file(tmp_path / 'source_certificate.txt')
    args['bindings'] = {binding['trade_date']: s.r.old.profit.bind(binding, 'binding_id')}
    receipt.scope['source_bindings_sha256'] = sha256_json(args['bindings'])
    with pytest.raises(IntegrityError, match='scope or status'):
        gateway.prepare_predictive_readiness(receipt=receipt, **args)
