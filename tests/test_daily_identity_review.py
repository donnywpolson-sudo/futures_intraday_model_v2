"""Synthetic checks for a diagnostic-only completed-day identity alternative."""
from copy import deepcopy
from dataclasses import asdict, replace

import pytest

from futures_rebuild import daily_identity_review as review
from futures_rebuild import causal_1h as hourly
from futures_rebuild.errors import IntegrityError, ContractError
from tests.test_causal_1d import sample, BASE, RULEBOOK, H

pytestmark = pytest.mark.current


def evidence():
    day = sample()
    definition = replace(day.definitions[0], ts_recv_ns=BASE+21*review.HOUR)
    bars = day.references['1h'][22:]
    native = replace(day.bars[0], volume=sum(b.volume for b in bars))
    spec = hourly.Spec('ES', BASE, BASE+2*review.DAY, H, H, 'a'*64)
    tables, _ = hourly.produce_day(hourly.DayInput(BASE, (definition,), bars, {'1m':()}), spec, RULEBOOK, None)
    hours = [dict(h, origin='NATIVE_1H', quality_disposition='CORROBORATED_BY_SPARSE_MINUTE_AGGREGATE')
             for h in tables['observations']]
    return dict(source_row=asdict(native), matching_definitions=[asdict(definition)]), hours


def assess(item, hours):
    return review.assess(item, hours, reference_hashes={h['source_file_sha256'] for h in evidence()[1]}, rulebook=RULEBOOK)


def test_completed_day_alternative_does_not_rewrite_original_gate_or_inputs():
    from futures_rebuild.foundation.identity import DefinitionIndex
    from futures_rebuild.foundation.records import ProviderBar, ProviderDefinition, ns_to_datetime
    item, hours = evidence()
    before = deepcopy((item, hours))
    bar = ProviderBar(**item['source_row'])
    with pytest.raises(ContractError, match='no same-day definition received by bar start'):
        DefinitionIndex([ProviderDefinition(**d) for d in item['matching_definitions']]).resolve(
            bar, decision_at=ns_to_datetime(BASE+review.DAY+review.LAG, 'decision'))
    result = assess(item, hours)
    assert result['status']=='SUPPORTED_FOR_SUCCESSOR_REVIEW'
    assert result['recovered_rows']==0 and result['admission'] is False
    assert result['decision_at_ns']==BASE+review.DAY+review.LAG
    assert result['coverage']=='OBSERVED_NATIVE_HOURS_ONLY'
    assert before==(item, hours)


@pytest.mark.parametrize('kind', ['no_hours','duplicate','outside_day','excluded','derived','publisher',
    'instrument','contract','units','delivery','premature','definition_late','definition_hash',
    'definition_date','delete','economic_update','expired','price','volume'])
def test_unsupported_evidence_stays_quarantined(kind):
    item, hours = evidence()
    if kind=='no_hours': hours=[]
    elif kind=='duplicate': hours.append(deepcopy(hours[0]))
    elif kind=='outside_day': hours[0]['bar_start_ns']=BASE-review.HOUR
    elif kind=='excluded': hours[0]['quality_disposition']='QUARANTINE_UNRESOLVED'
    elif kind=='derived': hours[0]['origin']='DERIVED_1H_FROM_RETAINED_1M'
    elif kind=='publisher': hours[0]['publisher_id']+=1
    elif kind=='instrument': hours[0]['instrument_id']+=1
    elif kind=='contract': hours[-1]['actual_contract']='ESM4'
    elif kind=='units': hours[-1]['multiplier_nano']+=1
    elif kind=='delivery': hours[0]['source_file_sha256']='f'*64
    elif kind=='premature': hours[-1]['decision_eligible_at_ns']+=1
    elif kind=='definition_late': item['matching_definitions'][0]['ts_recv_ns']=BASE+22*review.HOUR+1
    elif kind=='definition_hash': hours[0]['definition_row_sha256']='f'*64
    elif kind=='definition_date':
        d=item['matching_definitions'][0]
        d['ts_recv_ns']=BASE-review.HOUR
        d['instrument_id_date_utc']='2024-03-03'
    elif kind=='delete': item['matching_definitions'][0]['security_update_action']='DELETE'
    elif kind=='economic_update':
        d=deepcopy(item['matching_definitions'][0]); d['min_price_increment_nano']+=1
        d['row_ordinal']+=1; d['ts_recv_ns']+=1
        item['matching_definitions'].append(d)
    elif kind=='expired': item['matching_definitions'][0]['expiration_ns']=BASE+23*review.HOUR
    elif kind=='price': item['source_row']['high_nano']+=1
    elif kind=='volume': item['source_row']['volume']+=1
    result=assess(item,hours)
    assert result['status']=='UNRESOLVED_QUARANTINED'
    assert result['reason'] and not result['admission'] and result['recovered_rows']==0


def test_cutoff_preserved_and_no_prior_date_id_carry():
    item, hours=evidence()
    item['source_row']['event_at_ns']=review.CUTOFF//review.DAY*review.DAY
    with pytest.raises(IntegrityError,match='cutoff'):
        assess(item,hours)


def test_cli_is_preparation_only_and_worker_is_not_reached(monkeypatch, tmp_path, capsys):
    from futures_rebuild.causal_timeframe_storage import signed
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(review,'prepare',lambda root:signed(dict(status='PREPARED',inputs={},input_bytes=0)))
    monkeypatch.setattr(review,'worker',lambda:pytest.fail('historical worker reached by CLI'))
    review.main()
    assert '"historical_execution": false' in capsys.readouterr().out


def test_worker_requires_exact_authorization_before_creating_output(tmp_path, monkeypatch):
    import io, json
    from futures_rebuild.canonical import canonical_bytes, sha256_file
    from futures_rebuild.causal_timeframe_storage import signed
    plan=signed(dict(operation=review.OPERATION))
    p=tmp_path/'plan.json'; p.write_bytes(canonical_bytes(plan))
    a=tmp_path/'auth.json'; a.write_bytes(canonical_bytes(signed(dict(status='NOT_APPROVED',
        operation=review.OPERATION,plan_id=plan['id'],plan_sha256=sha256_file(p)))))
    request=dict(plan_path='plan.json',authorization_path='auth.json')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(review.sys,'stdin',io.TextIOWrapper(io.BytesIO(json.dumps(request).encode())))
    with pytest.raises(IntegrityError, match='authorization'):
        review.worker()
    assert not (tmp_path/review.HERE).exists()


@pytest.mark.parametrize('tampered', [False, True])
def test_worker_accounts_for_generated_quarantine_and_rejects_changed_binding(tmp_path, monkeypatch, tampered):
    import io, json
    from futures_rebuild.canonical import canonical_bytes, sha256_file
    from futures_rebuild.causal_timeframe_storage import signed, read_record
    from futures_rebuild import hourly_quality_reader
    from pathlib import Path
    item, hours = evidence()
    def write(relative, value):
        p=tmp_path/relative; p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes(canonical_bytes(value))
        return sha256_file(p)
    seal=signed(dict(month='2024-03',coverage=[dict(day_ns=BASE,quarantine=[item])]))
    seal_hash=write('synthetic/seal.json',seal)
    manifest_hash=write('synthetic/release.json',signed(dict(seal=seal,candidate=dict(source_inventory=[
        dict(kind='DBN',family='ohlcv_1h',sha256=hours[0]['source_file_sha256'])]))))
    entry=dict(market='ES',month='2024-03',disposition='CERTIFIED',manifest='synthetic/release.json')
    plan=signed(dict(operation=review.OPERATION,rule=review.RULE,publication=False,limits=asdict(review.LIMITS),
        implementation={},bindings={},inputs={'synthetic/seal.json':seal_hash,'synthetic/release.json':manifest_hash},
        hourly_months=[entry],daily_months=[entry],quarantine_seals={'synthetic/seal.json':seal_hash},
        quarantine_months=[dict(market='ES',month='2024-03',seal_path='synthetic/seal.json')],expected_cases=1))
    directory=review.HERE+'/'+plan['id']
    plan_hash=write(directory+'/plan.json',plan)
    write(directory+'/authorization.json',signed(dict(status='OWNER_APPROVED_IN_THIS_TASK',
        operation=review.OPERATION,plan_id=plan['id'],plan_sha256=plan_hash)))
    if tampered: write('synthetic/seal.json',signed({'changed':True}))
    class Reader:
        def __init__(self, root): pass
        def _native_month(self, *args): return hours
        def _unchanged(self): pass
    monkeypatch.setattr(hourly_quality_reader,'HourlyReader',Reader)
    config=tmp_path/'configs/contract_economics_rules.json'
    config.parent.mkdir(parents=True,exist_ok=True)
    config.write_bytes((Path(__file__).resolve().parents[1]/'configs/contract_economics_rules.json').read_bytes())
    request=dict(plan_path=directory+'/plan.json',authorization_path=directory+'/authorization.json',
                 operation=dict(output=directory+'/execution'))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(review.sys,'stdin',io.TextIOWrapper(io.BytesIO(json.dumps(request).encode())))
    exit_code=review.worker()
    result=read_record(tmp_path/directory/'execution/result.json')
    assert exit_code==int(tampered), result
    assert result['recovered_rows']==0 and not result['admission']
    if tampered:
        assert result['status']=='FAILED_IDENTITY_ASSESSMENT'
        assert result['phase']=='bindings_before'
        assert not (tmp_path/directory/'execution/cases.json').exists()
    else:
        assert result['dispositions']=={'SUPPORTED_FOR_SUCCESSOR_REVIEW':1}
