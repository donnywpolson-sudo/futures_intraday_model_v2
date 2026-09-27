"""Real validation code against generated old/new releases; no historical reads."""
from pathlib import Path
from copy import deepcopy
from dataclasses import replace
import shutil

import pytest

from futures_rebuild import causal_1d as daily, causal_timeframe_publication as pub
from futures_rebuild import daily_identity_campaign as campaign, daily_identity_publication as recovery_pub
from futures_rebuild.canonical import sha256_file, canonical_bytes, sha256_json
from futures_rebuild.causal_timeframe_storage import contained, read_record, signed, Budget
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_daily_identity_review import evidence
from tests.test_causal_1d import BASE, DAY, sample, empty, spec, RULEBOOK, LIMITS
from tests.test_causal_1d_publication import publication_receipt

pytestmark=pytest.mark.current
ROOT=Path(__file__).resolve().parents[1]


def write(root,path,value):
    p=contained(root,path); p.parent.mkdir(parents=True,exist_ok=True)
    p.write_bytes(canonical_bytes(value)+b'\n')
    return dict(path=path,sha256=sha256_file(p))


def fixture(root, *, materialize=True):
    legacy=daily.implementation_binding()
    for name in set(legacy)|set(campaign.CODE)|{'causal_timeframe_publication.py'}:
        source=ROOT/'src/futures_rebuild'/name
        target=root/'src/futures_rebuild'/name
        target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(source,target)
    for name in ('daily_quality_reader.py','causal_timeframe_publication.py'):
        relative=campaign.HERE+'/candidate/'+name
        target=contained(root,relative); target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(contained(ROOT,relative),target)
    item,hours=evidence()
    from futures_rebuild.foundation.records import ProviderBar, ProviderDefinition
    quarantined=daily.DayInput(BASE,tuple(ProviderDefinition(**d) for d in item['matching_definitions']),
        (ProviderBar(**item['source_row']),),{'1h':sample().references['1h'][22:]})
    days=[quarantined,sample(BASE+DAY),empty(BASE+2*DAY)]
    s=spec(BASE,BASE+3*DAY)
    relative='state/unpublished_evidence/original/candidate.json'
    directory=contained(root,'state/unpublished_evidence/original')
    daily.build_candidate(directory,s,lambda:iter(days),RULEBOOK,LIMITS)
    daily.certify_candidate(directory,s,lambda:iter(days),RULEBOOK,LIMITS)
    catalog=root/'data/active/catalog.json'; catalog.parent.mkdir(parents=True); catalog.write_text('{}')
    proposal=pub.prepare_publication(root,candidates=[relative])
    old=pub.publish(root,proposal,publication_receipt(root,proposal),LIMITS)
    class Hours:
        def _native_month(self,*args): return hours
        def _unchanged(self): pass
    output='state/unpublished_evidence/recovery/reconstruction'
    budget=Budget(root,LIMITS)
    kwargs=dict(hourly_reader=Hours(),rulebook=RULEBOOK,output=output,plan_id='c'*64,budget=budget)
    generated=campaign.reconstruct_months(root,old['entries'],**kwargs)
    for _ in range(2):
        assert campaign.reconstruct_months(root,old['entries'],**kwargs,replay=True,expected=generated['months'])==generated
    census=dict(outputs=campaign.output_records(root,output,generated),accounting=generated)
    cert=signed(dict(schema='daily_identity_recovery_certificate/1',
        status='PASS_DAILY_IDENTITY_RECOVERY_CERTIFICATION',rule=campaign.RULE,research_execution=False,
        synthetic_only=True,passes=[census,census],implementation=campaign.implementation(root),environment=daily.ENVIRONMENT,
        legacy_implementation=legacy,legacy_spec_ids=[old['entries'][0]['spec_id']],plan_id='c'*64,
        predecessor_index_sha256=sha256_file(root/pub.INDEX)))
    reference=write(root,'state/unpublished_evidence/recovery/certificate.json',cert)
    content=read_record(contained(root,output+'/ES/2024-03/content.json'))
    release=signed({k:v for k,v in content.items() if k!='id'}|{'certificate':reference})
    before=old['entries'][0]
    destination='data/causally_gated_normalized/ES/2024/1d/2024-03/'+'c'*64
    src=contained(root,output+'/ES/2024-03/payload'); dest=contained(root,destination)
    dest.parent.mkdir(parents=True,exist_ok=True)
    if materialize:
        src.rename(dest)
        ref=write(root,destination+'/manifest.json',release)
    else:
        from hashlib import sha256
        ref=dict(path=destination+'/manifest.json',sha256=sha256(canonical_bytes(release)+b'\n').hexdigest())
    after=before|dict(spec_id='c'*64,directory=destination,manifest=ref['path'],manifest_sha256=ref['sha256'])
    overrides={p:campaign.HERE+'/candidate/'+Path(p).name for p in campaign.CHANGES[:2]}
    with campaign.projected_successor(root,overrides):
        assert daily.implementation_binding()['daily_quality_reader.py']==campaign.implementation(root)['src/futures_rebuild/daily_quality_reader.py']
        index=recovery_pub.replacement_index(root,old,[(before,after)],reference)
        if materialize: pub.validate_release(root,after)
        else: pub._validate_content(root,after,release,src,published=False)
    indexref=write(root,'state/unpublished_evidence/recovery/index.json',index)
    overrides[pub.INDEX]=indexref['path']
    return old,index,overrides,after,reference


def test_public_reader_uses_certified_completed_identity(tmp_path):
    old,index,overrides,after,reference=fixture(tmp_path)
    before=(tmp_path/pub.INDEX).read_bytes()
    with campaign.projected_successor(tmp_path,overrides):
        from futures_rebuild.daily_quality_reader import DailyReader
        reader=DailyReader(tmp_path)
        args=dict(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+DAY+5*10**9,purpose='SYNTHETIC_TEST')
        rows=reader.read_window(**args)
        assert len(rows)==1 and rows[0]['origin']=='NATIVE_1D'
        assert rows[0]['definition_received_at_ns']>BASE
        with pytest.raises(UnauthorizedOperation): reader.read_window(**(args|{'as_of_ns':args['as_of_ns']-1}))
        with pytest.raises(UnauthorizedOperation): reader.read_window(**(args|{'source':'minute_derived'}))
        # Retained native row in the same successor month keeps the original gate.
        assert len(reader.read_window(**(args|{'start_ns':BASE+DAY,'end_ns':BASE+2*DAY,'as_of_ns':BASE+2*DAY+5*10**9})))==1
        with pytest.raises(UnauthorizedOperation):
            pub.resolve_partitions(tmp_path,index,market='ES',year=2024,timeframe='1d',purpose='SYNTHETIC_TEST')
    assert (tmp_path/pub.INDEX).read_bytes()==before


@pytest.mark.parametrize('kind',['certificate','payload','predecessor','implementation','duplicate_replacement','incomplete_replacement'])
def test_tampering_and_ambiguous_replacements_fail_closed(tmp_path,kind):
    old,index,overrides,after,reference=fixture(tmp_path)
    with campaign.projected_successor(tmp_path,overrides):
        if kind=='certificate':
            contained(tmp_path,reference['path']).write_bytes(b'{}')
        elif kind=='payload':
            with contained(tmp_path,after['directory']+'/observations.parquet').open('ab') as f: f.write(b'changed')
        elif kind=='predecessor':
            with contained(tmp_path,old['entries'][0]['directory']+'/observations.parquet').open('ab') as f: f.write(b'changed')
        elif kind=='implementation':
            contained(tmp_path,'src/futures_rebuild/daily_identity_recovery.py').write_bytes(b'changed')
        if kind.endswith('replacement'):
            pairs=[] if kind=='incomplete_replacement' else [(old['entries'][0],after)]*2
            with pytest.raises(IntegrityError): recovery_pub.replacement_index(tmp_path,old,pairs,reference)
        else:
            with pytest.raises(IntegrityError): pub.validate_release(tmp_path,after)


def test_actual_inactive_staging_performs_one_complete_census(tmp_path,monkeypatch):
    old,index,overrides,after,reference=fixture(tmp_path,materialize=False)
    plan=dict(id='c'*64,output='state/unpublished_evidence/recovery',baseline={'observations':1},expected_recovered=1)
    proposal=campaign.prepare_replacement_proposal(tmp_path,plan)
    before=(tmp_path/pub.INDEX).read_bytes()
    census=campaign.stage_replacement(tmp_path,plan,proposal,publication_receipt(tmp_path,proposal),Budget(tmp_path,LIMITS))
    assert census['exhaustive_publication_passes']==1
    assert census['table_counts']=={'1d:'+k:2 for k in ('observations','missingness','roll','quality','cadence')}
    assert (tmp_path/pub.INDEX).read_bytes()==before
    assert contained(tmp_path,old['entries'][0]['manifest']).exists()
    assert contained(tmp_path,after['manifest']).exists()
    with pytest.raises(IntegrityError,match='destination'):
        campaign.prepare_replacement_proposal(tmp_path,plan)


def test_hourly_rebinding_preserves_non_daily_entries_and_permissions():
    hour=dict(timeframe='1h',market='GF',month='2010-07')
    old=dict(entries=[hour,dict(timeframe='1d',market='GF',month='2010-07')],legacy_1m={'sha256':'same'})
    new=old|{'daily_identity_recovery':{'path':'new','sha256':'new'}}
    manifest=signed(dict(implementation={'src/futures_rebuild/causal_timeframe_publication.py':'old'},
        evidence={'quality':{'path':'unchanged','sha256':'same'}},permitted_uses=['LINEAGE_AUDIT']))
    census=signed(dict(input_sha256={'data/active/timeframes.json':'old','data/active/catalog.json':'same'}))
    quality=signed(dict(census_sha256='old',quality_sha256='same'))
    records=campaign.hourly_successors(old,new,manifest,census,quality,prefix='next',publisher_hash='new')
    assert records['reader_manifest.json']['evidence']['quality']==manifest['evidence']['quality']
    assert records['reader_manifest.json']['permitted_uses']==['LINEAGE_AUDIT']
    assert records['quality_manifest.json']['quality_sha256']=='same'
    with pytest.raises(IntegrityError,match='hourly/minute'):
        campaign.hourly_successors(old,new|{'entries':[]},manifest,census,quality,prefix='next',publisher_hash='new')


def test_recovered_coverage_reuses_exact_utc_grid(tmp_path):
    from futures_rebuild.coverage_remediation import window_profile
    from futures_rebuild.active_data_view import COHORT_PERMISSIONS
    from futures_rebuild.causal_observation_parquet import read_table
    old,index,overrides,after,reference=fixture(tmp_path)
    output='state/unpublished_evidence/coverage_test'
    write(tmp_path,output+'/publication/index.json',index)
    before_rows=read_table(contained(tmp_path,old['entries'][0]['directory']+'/observations.parquet'),name='observations')
    rows=read_table(contained(tmp_path,after['directory']+'/observations.parquet'),name='observations')
    permissions=[dict(market='ES',start_ns=r['bar_start_ns'],end_ns=r['bar_end_ns'],permitted_uses=['LINEAGE_AUDIT']) for r in rows]
    write(tmp_path,output+'/policies/admission.json',signed(dict(entries=permissions)))
    purposes=sorted({p for uses in COHORT_PERMISSIONS.values() for p in uses})
    profile=window_profile(market='ES',step=DAY,left=BASE,right=BASE+3*DAY,rows=before_rows,
        permissions=permissions[1:],quarantines=[BASE],lengths=[1,5,20,60],purposes=purposes)
    baseline='state/unpublished_evidence/coverage_remediation_v1/f9c1d233fd48e4b54147a21672a49a870e1eee054167d6bf89b0918b6809089d/execution/coverage.json'
    ref=write(tmp_path,baseline,signed(dict(profiles=profile)))
    result=campaign.coverage_after(tmp_path,dict(output=output,bindings={baseline:ref['sha256']}),Budget(tmp_path,LIMITS))
    assert result['newly_readable_windows']==1
    with campaign.projected_successor(tmp_path,overrides):
        proposal=pub._prepare_publication(tmp_path,candidates=[],provenance_files=None,legacy_provenance=None,
            old=index,predecessor_sha256='a'*64)
        assert proposal['index']['daily_identity_recovery']==index['daily_identity_recovery']
