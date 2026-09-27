"""Synthetic admission decisions and delegation to the unchanged daily gate."""
from copy import deepcopy
import pytest

from futures_rebuild import daily_source_admission as admission
from futures_rebuild.daily_foundation_admission import classify
from futures_rebuild.causal_1d import DAY, CUTOFF
from futures_rebuild.hourly_quality_reader import HOUR
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from tests.test_hourly_source_admission import ceiling

pytestmark = pytest.mark.current
START = admission.timestamp('2024-01-01')


def daily(start=START):
    return dict(row_id='daily',market='ES',bar_start_ns=start,publisher_id=1,instrument_id=1,
        actual_contract='ESH4',currency='USD',min_price_increment_nano=1,multiplier_nano=1,
        open_nano=10,high_nano=12,low_nano=9,close_nano=11,volume=24)


def reference(i, **changes):
    return dict(daily(), row_id=f'hour{i}',bar_start_ns=START+i*HOUR,volume=1,
        source_file_sha256='source',quality_disposition='CORROBORATED_BY_FULL_MINUTES',**changes)


def test_complete_and_sparse_agreement_are_distinct():
    hours = [reference(i) for i in range(24)]
    full = classify(daily(),hours,original_comparison='MATCH',source_hashes={'source'})
    assert full['reason']=='FULL_HOURLY_AGREEMENT' and full['coverage']=='FULL_24_UTC_HOURS'
    sparse = classify(dict(daily(),volume=23),hours[1:],original_comparison='NOT_COMPARABLE',source_hashes={'source'})
    assert sparse['reason']=='OBSERVED_HOURLY_AGREEMENT'
    assert sparse['coverage']=='OBSERVED_HOURS_ONLY' and sparse['hourly_count']==23
    assert sparse['causes']==['INSUFFICIENT_HOURLY_COVERAGE']


@pytest.mark.parametrize('change,reason', [
    ({'publisher_id':2},'CONTRACT_OR_PUBLISHER_DIFFERENCE'),
    ({'instrument_id':2},'CONTRACT_OR_PUBLISHER_DIFFERENCE'),
    ({'actual_contract':'ESM4'},'CONTRACT_OR_PUBLISHER_DIFFERENCE'),
    ({'quality_disposition':'QUARANTINE_UNRESOLVED'},'EXCLUDED_HOURLY_REFERENCE'),
    ({'source_file_sha256':'different-delivery'},'UNRESOLVED_REFERENCE'),
])
def test_reference_exception_never_admits_by_matching_aggregate(change,reason):
    hours=[reference(i) for i in range(24)]
    hours[0].update(change)
    result=classify(daily(),hours,original_comparison='NOT_COMPARABLE',source_hashes={'source'})
    assert result['reason']==reason and result['evidence_requirement']


def test_missing_duplicate_and_conflicting_references():
    missing=classify(daily(),[],original_comparison='SOURCE_MISSING',source_hashes={'source'})
    assert missing['reason']=='REFERENCE_UNAVAILABLE'
    hours=[reference(i) for i in range(24)]
    with pytest.raises(IntegrityError):
        classify(dict(daily(),volume=25),hours,original_comparison='MATCH',source_hashes={'source'})
    conflict=classify(dict(daily(),volume=25),hours,original_comparison='DISAGREEMENT',source_hashes={'source'})
    assert conflict['reason']=='REFERENCE_DISAGREEMENT'
    duplicate=classify(daily(),[hours[0],hours[0]],original_comparison='NOT_COMPARABLE',source_hashes={'source'})
    assert duplicate['reason']=='UNRESOLVED_REFERENCE'
    contradiction=classify(daily(),hours,original_comparison='NOT_COMPARABLE',source_hashes={'source'})
    assert contradiction['reason']=='UNRESOLVED_REFERENCE'


def quality(start=START,reason='FULL_HOURLY_AGREEMENT'):
    return dict(row_id=str(start),market='ES',start_ns=start,reason=reason)


def policy_reader(monkeypatch, rows, ceilings):
    # Policy tests supply minimal synthetic catalogs. Catalog/sidecar binding is
    # checked separately by construction and the existing minute contracts.
    monkeypatch.setattr(admission.minute,'validate_catalog',lambda c:None)
    reader=object.__new__(admission.DailySourceReader)
    entries=admission.policy_entries(rows,{'entries':ceilings})
    reader.entries={(e['market'],e['start_ns']):e for e in entries}
    reader._unchanged=lambda:None
    return reader


def test_policy_ceiling_unknown_and_cross_cohort(monkeypatch):
    boundary=admission.timestamp('2023-01-01')
    reader=policy_reader(monkeypatch,[quality(boundary-DAY),quality(boundary)],[ceiling(2022),ceiling(2023)])
    uses=reader.permitted_uses(market='ES',start_ns=boundary-DAY,end_ns=boundary+DAY)
    assert set(uses)=={'FEATURE_GENERATION','LINEAGE_AUDIT'}
    reader=policy_reader(monkeypatch,[quality(reason='UNRESOLVED_REFERENCE')],[ceiling(2024)])
    assert reader.permitted_uses(market='ES',start_ns=START,end_ns=START+DAY)==('LINEAGE_AUDIT',)
    with pytest.raises(UnauthorizedOperation):
        reader.read_window(market='ES',start_ns=START,end_ns=START+DAY,as_of_ns=START+2*DAY,purpose='FEATURE_GENERATION')


def test_missing_protected_partial_and_duplicate_coverage(monkeypatch):
    for ceilings in ([],[ceiling(2024,disposition='LOCKED_HOLDOUT_NOT_MATERIALIZED')],
                     [ceiling(2024,coverage_start='2024-01-01T01:00:00Z')]):
        reader=policy_reader(monkeypatch,[quality()],ceilings)
        assert not reader.permitted_uses(market='ES',start_ns=START,end_ns=START+DAY)
    with pytest.raises(IntegrityError): policy_reader(monkeypatch,[quality(),quality()],[ceiling(2024)])
    with pytest.raises(IntegrityError): policy_reader(monkeypatch,[quality()],[ceiling(2024)]*2)
    reader=policy_reader(monkeypatch,[quality()],[ceiling(2024)])
    assert not reader.permitted_uses(market='ES',start_ns=START,end_ns=START+2*DAY)


def test_cutoff_source_and_delegation_preserve_rows(monkeypatch):
    reader=policy_reader(monkeypatch,[quality()],[ceiling(2024)])
    original=dict(daily(),row_id=str(START),quality_disposition='CAUSAL_RECONSTRUCTION_AUDIT_ONLY')
    calls=[]
    class Underlying:
        def read_window(self,**args):
            calls.append(args)
            if args['as_of_ns']<START+DAY+5*10**9:
                raise UnauthorizedOperation('not yet available')
            return [deepcopy(original)]
    reader.quality_reader=Underlying()
    args=dict(market='ES',start_ns=START,end_ns=START+DAY,as_of_ns=START+DAY+5*10**9,purpose='FEATURE_GENERATION')
    assert reader.read_window(**args)==[original]
    assert calls[-1]['purpose']=='LINEAGE_AUDIT'
    with pytest.raises(UnauthorizedOperation): reader.read_window(**(args|{'as_of_ns':args['as_of_ns']-1}))
    for changes in ({'end_ns':CUTOFF+DAY},{'source':'minute_derived'},{'start_ns':True},{'end_ns':START+DAY-1}):
        with pytest.raises(ContractError): reader.read_window(**(args|changes))
    original['row_id']='tampered'
    with pytest.raises(IntegrityError): reader.read_window(**args)
    last_end=CUTOFF//DAY*DAY
    latest=policy_reader(monkeypatch,[quality(last_end-DAY)],[ceiling(2025)])
    assert 'FEATURE_GENERATION' in latest.permitted_uses(market='ES',start_ns=last_end-DAY,end_ns=last_end)
    with pytest.raises(ContractError):
        latest.permitted_uses(market='ES',start_ns=last_end-DAY,end_ns=CUTOFF)


def test_missing_active_policy_fails_closed(tmp_path):
    with pytest.raises((OSError,IntegrityError)):
        admission.DailySourceReader(tmp_path)


@pytest.fixture
def prepared(tmp_path,monkeypatch):
    from futures_rebuild.canonical import canonical_bytes, sha256_file
    from futures_rebuild.causal_timeframe_storage import signed
    from futures_rebuild import daily_foundation_admission as foundation
    def write(relative,value):
        path=tmp_path/relative
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(canonical_bytes(value)+b'\n')
        return dict(path=relative,sha256=sha256_file(path))
    monkeypatch.setattr(admission.minute,'validate_catalog',lambda c:None)
    monkeypatch.setattr(foundation,'validate_permission_sidecars',lambda *a:None)
    monkeypatch.setattr(admission,'DailyReader',lambda root:object())
    bindings={}
    catalog={'entries':[ceiling(2024)]}
    for path,value in [('data/active/catalog.json',catalog),
        ('data/active/timeframes.json',signed({'entries':[]})),
        ('src/futures_rebuild/daily_source_admission.py',signed({})),
        ('src/futures_rebuild/daily_foundation_admission.py',signed({}))]:
        bindings[path]=write(path,value)['sha256']
    q=write('prepared/quality.json',signed(dict(status='PASS_DAILY_QUALITY_ACCOUNTING',rows=[quality()])))
    core=dict(schema=admission.SCHEMA,source='native',model_specific_eligibility='NOT_DECLARED',
        daily_implementation=admission.implementation_binding(),bindings=bindings,quality=q,
        entries=admission.policy_entries([quality()],catalog))
    manifest=write('prepared/manifest.json',signed(core))
    audit=write('prepared/audit.json',signed(dict(status='PASS_DAILY_FOUNDATION_WITH_LIMITATIONS',
                                              admission_sha256=manifest['sha256'])))
    supervision=write('prepared/supervision.json',signed(dict(status='PASS_SUPERVISED_DAILY_FOUNDATION',
                                                             result_sha256=audit['sha256'])))
    pointer=signed(dict(schema='active_daily_source_admission/1',manifest=manifest,audit=audit,supervision=supervision))
    return tmp_path,write,core,pointer


def test_default_pointer_requires_complete_audit_and_tracks_drift(prepared):
    root,write,core,pointer=prepared
    preview=admission.DailySourceReader(root,preview_manifest='prepared/manifest.json')
    assert not (root/admission.POINTER).exists()
    write(admission.POINTER,pointer)
    active=admission.DailySourceReader(root)
    args=dict(market='ES',start_ns=START,end_ns=START+DAY)
    assert active.permitted_uses(**args)==preview.permitted_uses(**args)
    write('data/active/timeframes.json',{'changed':True})
    with pytest.raises(IntegrityError): active.permitted_uses(**args)


@pytest.mark.parametrize('kind',['permission','source','implementation','unsigned','audit','supervision'])
def test_policy_tampering_is_refused(prepared,kind):
    from futures_rebuild.causal_timeframe_storage import signed
    root,write,core,pointer=prepared
    if kind in ('audit','supervision'):
        pointer[kind]=write('prepared/failed.json',signed({'status':'INCOMPLETE'}))
        write(admission.POINTER,pointer)
        with pytest.raises(IntegrityError): admission.DailySourceReader(root)
        return
    if kind=='permission': core['entries'][0]['permitted_uses'].append('SELECTION')
    if kind=='source': core['source']='minute_derived'
    if kind=='implementation': core['daily_implementation']={}
    write('prepared/manifest.json',core if kind=='unsigned' else signed(core))
    with pytest.raises(IntegrityError):
        admission.DailySourceReader(root,preview_manifest='prepared/manifest.json')
