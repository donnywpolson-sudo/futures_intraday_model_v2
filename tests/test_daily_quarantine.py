"""Synthetic source accounting and published-window enforcement for daily quarantine."""
from dataclasses import replace

import pytest

from futures_rebuild import causal_1d as daily
from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.causal_timeframe_storage import read_record, signed, iter_bundle_groups
from futures_rebuild.daily_quality_reader import DailyReader, require_coverage
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from tests.test_causal_1d import BASE, DAY, SECOND, LIMITS, RULEBOOK, sample, empty, spec

pytestmark=pytest.mark.current


def late(day):
    d=sample(day)
    return replace(d,definitions=(replace(d.definitions[0],ts_recv_ns=day+1),))


def build(root,days,s):
    factory=lambda:iter(days)
    candidate,_=daily.build_candidate(root,s,factory,RULEBOOK,LIMITS)
    cert,_=daily.certify_candidate(root,s,factory,RULEBOOK,LIMITS)
    assert cert['passes'][0]==cert['passes'][1]
    assert cert['passes'][0]['source_accounting']==candidate['source_accounting']
    return candidate


@pytest.mark.parametrize('kind',['late','absent','publisher','instrument'])
def test_quarantine_continues_and_replays_exact_evidence(tmp_path,kind):
    d=late(BASE+DAY)
    if kind=='absent': d=replace(d,definitions=())
    elif kind in ('publisher','instrument'):
        d=replace(d,definitions=(replace(d.definitions[0],**{kind+'_id':999}),))
    c=build(tmp_path,[sample(),d,sample(BASE+2*DAY),empty(BASE+3*DAY)],spec(end=BASE+4*DAY))
    assert c['source_accounting']==dict(source_primary_rows=3,observations=2,quarantined=1,excluded_start=0,excluded_availability=0)
    seal=read_record(tmp_path/'2024-03.seal.json')
    q=seal['coverage'][1]
    assert q['state']=='DEFINITION_NOT_KNOWN_AT_BAR_START'
    assert q['quarantine'][0]['source_row']['row_sha256']==d.bars[0].row_sha256
    assert len(q['quarantine'][0]['matching_definitions'])==(1 if kind=='late' else 0)
    assert seal['coverage'][3]['state']=='SOURCE_UNAVAILABLE'
    with pytest.raises(UnauthorizedOperation): require_coverage(seal['coverage'],start_ns=BASE,end_ns=BASE+3*DAY)
    require_coverage(seal['coverage'],start_ns=BASE+2*DAY,end_ns=BASE+3*DAY)


def test_quarantined_month_and_following_roll(tmp_path):
    start=int(daily.datetime(2024,3,31,tzinfo=daily.timezone.utc).timestamp())*SECOND
    s=spec(start,start+4*DAY)
    c=build(tmp_path,[sample(start),late(start+DAY),sample(start+2*DAY,symbol='ESM4'),empty(start+3*DAY)],s)
    assert c['source_accounting']['quarantined']==1
    rows=list(iter_bundle_groups(tmp_path/'ES/2024/1d/2024-04/candidate'))
    assert rows[0]['roll'][0]['actual_contract_before']=='ESH4'
    assert rows[0]['roll'][0]['actual_contract_after']=='ESM4'
    assert rows[0]['roll'][0]['roll_flag']


def test_entirely_quarantined_month_is_not_empty_source(tmp_path):
    c=build(tmp_path,[late(BASE),late(BASE+DAY)],replace(spec(),availability_end_ns=BASE+3*DAY))
    assert c['observations']==0 and c['source_accounting']['quarantined']==2
    seal=read_record(tmp_path/'2024-03.seal.json')
    assert seal['files']=={} and seal['table_metadata']=={}
    assert all(d['source_primary_rows']==1 and d['quarantined']==1 for d in seal['coverage'])


@pytest.mark.parametrize('kind',['deleted','conflict','late_conflict','expired','economics'])
def test_other_definition_failures_remain_fatal(tmp_path,kind):
    d=sample()
    definition=d.definitions[0]
    if kind=='deleted': definition=replace(definition,security_update_action='DELETE')
    if kind=='expired': definition=replace(definition,expiration_ns=BASE)
    if kind=='economics': definition=replace(definition,unit_of_measure_qty_nano=123)
    if kind=='late_conflict': definition=replace(definition,ts_recv_ns=BASE+1)
    definitions=(definition,)
    if 'conflict' in kind:
        definitions+=(replace(definition,source_file_path='synthetic/conflict.dbn',raw_symbol='ESM4'),)
    d=replace(d,definitions=definitions)
    with pytest.raises((ContractError,IntegrityError)):
        daily.build_candidate(tmp_path,spec(),lambda:iter([d,empty(BASE+DAY)]),RULEBOOK,LIMITS)
    assert not (tmp_path/'candidate.json').exists()


def test_availability_and_start_exclusions_do_not_become_quarantine(tmp_path):
    c=build(tmp_path,[late(BASE),late(BASE+DAY)],spec(start=BASE+SECOND))
    assert c['source_accounting']==dict(source_primary_rows=2,observations=0,quarantined=0,excluded_start=1,excluded_availability=1)


def test_replay_rejects_changed_quarantine_evidence(tmp_path,monkeypatch):
    from futures_rebuild import causal_1d_replay as replay
    days=[late(BASE),empty(BASE+DAY)]
    daily.build_candidate(tmp_path,spec(),lambda:iter(days),RULEBOOK,LIMITS)
    original=replay.unavailable_definition
    def changed(day):
        result=original(day)
        result['reason']='SOURCE_UNAVAILABLE'
        return result
    monkeypatch.setattr(replay,'unavailable_definition',changed)
    with pytest.raises(IntegrityError,match='coverage'):
        daily.certify_candidate(tmp_path,spec(),lambda:iter(days),RULEBOOK,LIMITS)


def publish(root,days,s):
    from futures_rebuild import causal_timeframe_publication as pub
    from tests.test_causal_1d_publication import publication_receipt
    relative='state/unpublished_evidence/daily_quarantine/candidate.json'
    build(root/'state/unpublished_evidence/daily_quarantine',days,s)
    catalog=root/'data/active/catalog.json'
    catalog.parent.mkdir(parents=True)
    catalog.write_text('{}')
    proposal=pub.prepare_publication(root,candidates=[relative])
    pub.publish(root,proposal,publication_receipt(root,proposal),LIMITS)
    return DailyReader(root)


def test_public_reader_refuses_quarantine_gaps_and_premature_reads(tmp_path):
    reader=publish(tmp_path,[sample(),late(BASE+DAY),sample(BASE+2*DAY),empty(BASE+3*DAY)],spec(end=BASE+4*DAY))
    args=dict(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+4*DAY,purpose='SYNTHETIC_TEST')
    rows=reader.read_window(**args)
    assert len(rows)==1 and rows[0]['quality_evidence']['ohlc_valid']
    assert rows[0]['quality_disposition']=='CAUSAL_RECONSTRUCTION_AUDIT_ONLY'
    for changes in [dict(end_ns=BASE+3*DAY),dict(start_ns=BASE+3*DAY,end_ns=BASE+4*DAY),
                    dict(as_of_ns=BASE+DAY+4*SECOND),dict(purpose='LINEAGE_AUDIT'),
                    dict(purpose='TRAINING'),dict(source='minute_derived')]:
        with pytest.raises(UnauthorizedOperation): reader.read_window(**(args|changes))
    assert reader.read_window(**(args|dict(as_of_ns=BASE+DAY+5*SECOND)))


def test_public_reader_refuses_contract_change_and_tampering(tmp_path):
    reader=publish(tmp_path,[sample(),sample(BASE+DAY,symbol='ESM4'),empty(BASE+2*DAY)],spec(end=BASE+3*DAY))
    args=dict(market='ES',start_ns=BASE,end_ns=BASE+2*DAY,as_of_ns=BASE+3*DAY,purpose='SYNTHETIC_TEST')
    with pytest.raises(UnauthorizedOperation,match='contract'): reader.read_window(**args)
    path=tmp_path/reader.index['entries'][0]['manifest']
    release=read_record(path)
    release['seal']['coverage'][0]['quarantine']=[{}]
    path.write_bytes(canonical_bytes(signed({k:v for k,v in release.items() if k!='id'})))
    with pytest.raises(IntegrityError): reader.read_window(**(args|dict(end_ns=BASE+DAY)))


def test_reader_requires_coverage_and_development_boundary(tmp_path):
    reader=publish(tmp_path,[sample(),empty(BASE+DAY)],spec())
    with pytest.raises(ContractError):
        reader.read_window(market='ES',start_ns=BASE,end_ns=daily.CUTOFF+DAY,as_of_ns=daily.CUTOFF)
    with pytest.raises(UnauthorizedOperation): require_coverage([],start_ns=BASE,end_ns=BASE+DAY)


def test_prepared_raw_path_guard_covers_all_three_routes(tmp_path,monkeypatch):
    from pathlib import Path
    from futures_rebuild import causal_timeframe_publication as pub
    from futures_rebuild import active_data_view, causal_timeframe_view
    from futures_rebuild.canonical import sha256_file
    reader=publish(tmp_path,[sample(),empty(BASE+DAY)],spec())
    root=Path(__file__).resolve().parents[1]
    prep=root/'state/unpublished_evidence/daily_foundation_v1/remediation_preparation_v1'
    assert (prep/'publication.predecessor.sha256').read_text().strip()==sha256_file(root/'src/futures_rebuild/causal_timeframe_publication.py')
    namespace={'__name__':'futures_rebuild.prepared_daily_publication','__package__':'futures_rebuild'}
    exec(compile((prep/'publication.candidate.py').read_text(),str(prep/'publication.candidate.py'),'exec'),namespace)
    monkeypatch.setattr(pub,'resolve_partitions',namespace['resolve_partitions'])
    args=dict(market='ES',year=2024,timeframe='1d',purpose='SYNTHETIC_TEST')
    routes=(lambda:pub.resolve_partitions(tmp_path,reader.index,**args),
            lambda:causal_timeframe_view.resolve_partitions(repository_root=tmp_path,**args),
            lambda:active_data_view.resolve_partitions(repository_root=tmp_path,**args))
    for route in routes:
        with pytest.raises(UnauthorizedOperation,match='DailyReader.read_window'): route()


def test_reader_index_and_coverage_fields_fail_closed(tmp_path):
    reader=publish(tmp_path,[sample(),empty(BASE+DAY)],spec())
    entry=reader.index['entries'][0]
    release=read_record(tmp_path/entry['manifest'])
    coverage=release['seal']['coverage']
    del coverage[0]['quarantined']
    with pytest.raises(UnauthorizedOperation): require_coverage(coverage,start_ns=BASE,end_ns=BASE+DAY)
    reader.index_path.write_bytes(reader.index_path.read_bytes()+b' ')
    with pytest.raises(IntegrityError,match='index changed'):
        reader.read_window(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+2*DAY,purpose='SYNTHETIC_TEST')
