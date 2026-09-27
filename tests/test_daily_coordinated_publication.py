"""Exact staged publisher, ordinary reader validation, and control rollback."""
import importlib.util
from pathlib import Path

import pytest

from futures_rebuild import causal_timeframe_publication as pub
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import Budget, signed, read_record
from futures_rebuild.daily_quality_reader import DailyReader
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_daily_quarantine import build
from tests.test_causal_1d import sample, empty, spec, BASE, DAY, LIMITS
from tests.test_causal_1d_publication import publication_receipt

pytestmark=pytest.mark.current
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py'
module_spec=importlib.util.spec_from_file_location('daily_coordinated_test',PATH)
rollout=importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(rollout)


def test_staged_publisher_preserves_original_and_active_index(tmp_path):
    relative='state/unpublished_evidence/source/candidate.json'
    source=tmp_path/relative
    build(source.parent,[sample(),empty(BASE+DAY)],spec())
    # The historical publisher expects provenance.json; synthetic releases do
    # not consume its contents, but cloning still preserves this metadata file.
    (source.parent/'provenance.json').write_bytes(canonical_bytes(signed({})))
    catalog=tmp_path/'data/active/catalog.json'
    catalog.parent.mkdir(parents=True)
    catalog.write_text('{}')
    predecessor=signed(dict(schema=pub.SCHEMA,status='PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY',
        legacy_1m=dict(path='data/active/catalog.json',sha256=sha256_file(catalog)),expected=[],entries=[]))
    index=tmp_path/pub.INDEX
    index.write_bytes(canonical_bytes(predecessor)+b'\n')
    before=index.read_bytes()
    budget=Budget(tmp_path,LIMITS)
    clones=rollout.clone_candidates(tmp_path,[relative],'state/unpublished_evidence/clones',budget)
    proposal=pub.prepare_publication(tmp_path,candidates=clones)
    staged='state/unpublished_evidence/staged/index.json'
    result=rollout.stage_publication(tmp_path,proposal,publication_receipt(tmp_path,proposal),LIMITS,staged)
    assert index.read_bytes()==before and source.exists()
    seal=read_record(source.parent/'2024-03.seal.json')
    assert (source.parent/seal['directory']/'observations.parquet').exists()
    assert read_record(tmp_path/staged)==result
    guard=ROOT/'state/unpublished_evidence/daily_foundation_v1/remediation_preparation_v1/publication.candidate.py'
    with rollout.projected_reads(tmp_path,{pub.INDEX:tmp_path/staged},guard):
        reader=DailyReader(tmp_path)
        rows=reader.read_window(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+2*DAY,purpose='SYNTHETIC_TEST')
        assert len(rows)==1
        from futures_rebuild.daily_foundation_admission import check_reader_examples, check_raw_routes
        request=dict(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+2*DAY,purpose='SYNTHETIC_TEST')
        checks=check_reader_examples(reader,{'accepted':(request,True),
            'premature':(request|{'as_of_ns':BASE+DAY},False)})
        assert [c['result'] for c in checks]==['ACCEPTED','REFUSED']
        assert len(check_raw_routes(tmp_path,result))==3
        with pytest.raises(UnauthorizedOperation):
            pub.resolve_partitions(tmp_path,result,market='ES',year=2024,timeframe='1d',purpose='SYNTHETIC_TEST')
    assert index.read_bytes()==before
    assert DailyReader(tmp_path).index==predecessor


def test_hourly_successor_refuses_changed_existing_entries():
    old={'entries':[{'timeframe':'1h','market':'GF','month':'2010-07'}], 'legacy_1m':{'sha256':'minute'}}
    new=dict(old,entries=old['entries']+[{'timeframe':'1d','market':'GF'}])
    census=signed({'input_sha256':{'data/active/timeframes.json':'old','quality':'same'}})
    quality=signed({'census_sha256':rollout.digest(census),'quality_sha256':'same'})
    manifest=signed({'implementation':{'src/futures_rebuild/causal_timeframe_publication.py':'old'},
        'evidence':{'quality':{'path':'same','sha256':'same'}}})
    result=rollout.hourly_successors(old,new,manifest,census,quality,prefix='state/unpublished_evidence/next',guard_hash='new')
    assert result['census.json']['input_sha256']['quality']=='same'
    assert result['quality_manifest.json']['quality_sha256']=='same'
    with pytest.raises(IntegrityError):
        rollout.hourly_successors(old,dict(new,entries=[]),manifest,census,quality,prefix='x',guard_hash='new')


def test_failed_activation_restores_existing_and_absent_controls(tmp_path):
    (tmp_path/'configs').mkdir()
    existing=tmp_path/'configs/existing.json'
    existing.write_bytes(b'original')
    prepared=tmp_path/'state/unpublished_evidence/test'
    prepared.mkdir(parents=True)
    (prepared/'one').write_bytes(b'new-one')
    (prepared/'two').write_bytes(b'new-two')
    changes={'configs/existing.json':'state/unpublished_evidence/test/one',
             'configs/new.json':'state/unpublished_evidence/test/two'}
    expected={'configs/existing.json':sha256_file(existing),'configs/new.json':None}
    def fail(): raise IntegrityError('default reader rejected candidate')
    with pytest.raises(IntegrityError,match='default reader'):
        rollout.activate_controls(tmp_path,changes,expected=expected,
            rollback_directory='state/unpublished_evidence/rollback',validate=fail)
    assert existing.read_bytes()==b'original' and not (tmp_path/'configs/new.json').exists()
    assert (tmp_path/'state/unpublished_evidence/rollback/1.failed-new-pointer').read_bytes()==b'new-two'
