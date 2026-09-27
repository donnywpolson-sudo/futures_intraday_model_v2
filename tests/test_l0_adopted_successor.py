"""Current adopted successor, using synthetic inputs only."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json

import pytest

from futures_rebuild import l0_alpha_candidate as c, l0_alpha_replay as replay
from futures_rebuild import l0_alpha_preparation as prep, l0_development_admission as admission
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def test_adopted_mean_timing_and_unresolved_occupancy():
    day='2018-01-02';records=[];outcomes=[]
    for i,at in enumerate(c.decision_times(day)):
        entry=(at//c.MINUTE+1)*c.MINUTE
        records.append(dict(market='ES',trade_date=day,decision_ns=at,entry_ns=entry,exit_ns=entry+60*c.MINUTE,
            status='ELIGIBLE',forecast_id=str(i),actual_contract='SYNTHETIC_ONLY',tick_nano=1,tick_value_nano_usd=10**9))
        outcomes.append(dict(entry_status='RESOLVED',exit_status='RESOLVED',entry_open_nano=100,exit_open_nano=100))
    result=replay.replay(records,outcomes,[20.]*330,[day],policy='date_balanced_training_mean',timing_only=True,legacy=True)
    fixed=replay.replay(records,outcomes,None,[day],policy='always_long',timing_only=True,legacy=True)
    assert result['events']==fixed['events'] and result['resolved_round_trips']>0
    assert result['scenario']=='adverse' and 'daily' not in result and 'net_usd' not in result
    for values,match in [(None,'requires'),([float('nan')]*330,'finite'),([20.]*329+[21.],'fixed')]:
        with pytest.raises(IntegrityError,match=match):
            replay.replay(records,outcomes,values,[day],policy='date_balanced_training_mean',timing_only=True,legacy=True)
    missing=deepcopy(outcomes);missing[0]['exit_status']='MISSING'
    blocked=replay.replay(records,missing,[20.]*330,[day],policy='date_balanced_training_mean',timing_only=True,legacy=True)
    assert blocked['attempted_entries']==1 and blocked['unresolved_position']['side']=='exit' and not blocked['complete']


def test_adopted_exact_final_count(monkeypatch):
    monkeypatch.setattr(c,'fold_schedule',lambda dataset: ())
    for count in (505,504):
        data=SimpleNamespace(decisions=[],outcomes=[],fit_positions=list(range(count)),
            terminal_data=SimpleNamespace(trade_dates=list(c.weekday_dates()[:count])))
        result=c.source_completeness(data)
        assert result['final_observed_training_dates']==(505 if count==505 else None)


def test_explicit_inactive_successor_and_stale_contract():
    root=Path(__file__).resolve().parents[1]
    bundle=prep.validate_bundle(root)
    parent=json.loads((root/'configs/l0_alpha_candidate_v6.json').read_text())
    contract=json.loads((root/admission.CONTRACT_PATH).read_text())
    admission.validate_contract(root,contract)
    assert bundle['design']==parent['design']
    assert bundle['configuration_ids']==parent['configuration_ids']
    assert bundle['entry_calendar']['calendar_id']=='a0fd2fe64d35c1af3f3dfb3fea32e224d518afa14f71be1afafe09c9fdf34879'
    assert contract['state']=='PREPARED_INACTIVE' and not any(contract['authority'].values())
    assert not (root/admission.ACTIVATION_PATH).exists()
    stale=json.loads((root/'configs/l0_development_admission_v1.json').read_text())
    with pytest.raises(IntegrityError,match='binding changed'):admission.validate_contract(root,stale)
