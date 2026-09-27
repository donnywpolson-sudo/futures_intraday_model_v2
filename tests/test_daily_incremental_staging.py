"""Inactive staging, one final census, and journal-owned recovery."""
import importlib.util
from unittest.mock import patch
import pytest

from tests.test_daily_coordinated_publication import (
    ROOT,build,sample,empty,spec,BASE,DAY,LIMITS,publication_receipt,rollout)
from futures_rebuild.canonical import canonical_bytes,sha256_file
from futures_rebuild.causal_timeframe_storage import Budget,signed,read_record
from futures_rebuild.errors import IntegrityError

pytestmark=pytest.mark.current


def load(name, path):
    spec=importlib.util.spec_from_file_location(name,ROOT/path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pub=load('futures_rebuild.incremental_publisher_test',
    'state/unpublished_evidence/daily_admission_v1/incremental_preparation_v1/publication.candidate.py')
stage=load('daily_incremental_staging_test',
    'state/unpublished_evidence/daily_admission_v1/incremental_staging.py')


def setup(root):
    catalog=root/'data/active/catalog.json'
    catalog.parent.mkdir(parents=True)
    catalog.write_text('{}')
    initial=signed(dict(schema=pub.SCHEMA,status='PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY',
        legacy_1m=dict(path='data/active/catalog.json',sha256=sha256_file(catalog)),expected=[],entries=[]))
    (root/pub.INDEX).write_bytes(canonical_bytes(initial)+b'\n')
    candidates=[]
    for number,day in enumerate((BASE,BASE+31*DAY)):
        relative=f'state/unpublished_evidence/candidates/{number}/candidate.json'
        build((root/relative).parent,[sample(day),empty(day+DAY)],spec(day,day+2*DAY))
        candidates.append(relative)
    return candidates


def test_incremental_staging_and_one_final_census(tmp_path):
    candidates=setup(tmp_path)
    original=(tmp_path/pub.INDEX).read_bytes()
    previous=pub.INDEX
    calls=[]
    original_validate=pub.validate_release
    def record(root,entry):
        calls.append(entry['month'])
        return original_validate(root,entry)
    with patch.object(pub,'validate_release',record):
        for number,candidate in enumerate(candidates):
            proposal=stage.prepare(tmp_path,candidate=candidate,previous=previous,pub=pub)
            output=f'state/unpublished_evidence/stages/{number}'
            envelope=stage.stage(tmp_path,proposal=proposal,receipt=publication_receipt(tmp_path,proposal),
                previous=previous,output=output,budget=Budget(tmp_path,LIMITS),pub=pub)
            assert len(calls)==number+1  # no reread of earlier staged payloads
            with pytest.raises(IntegrityError): pub.validate_index(tmp_path,envelope)
            previous=output+'/stage.json'
        proposal=stage.prepare_final(tmp_path,last=previous,pub=pub)
        calls.clear()
        final=stage.finalize(tmp_path,proposal=proposal,receipt=publication_receipt(tmp_path,proposal),
            output='state/unpublished_evidence/final',budget=Budget(tmp_path,LIMITS),pub=pub)
        assert sorted(calls)==['2024-03','2024-04']
        pub.validate_index(tmp_path,final)
    assert (tmp_path/pub.INDEX).read_bytes()==original


@pytest.mark.parametrize('interrupted',[False,True,'before_move'])
def test_recover_existing_ordinary_journal(tmp_path,interrupted):
    candidate=setup(tmp_path)[0]
    proposal=pub.prepare_publication(tmp_path,candidates=[candidate])
    receipt=publication_receipt(tmp_path,proposal)
    before=(tmp_path/pub.INDEX).read_bytes()
    original_replace=pub.os.replace
    def defer(source,destination):
        if str(destination).endswith('timeframes.json'):
            original_replace(source,tmp_path/'ordinary-staged-index.json')
        else: original_replace(source,destination)
    if interrupted:
        with patch.object(pub,'_stage_moves' if interrupted=='before_move' else '_validate_population',side_effect=IntegrityError('interrupted')):
            with pytest.raises(IntegrityError,match='interrupted'):
                pub.publish(tmp_path,proposal,receipt,LIMITS)
    else:
        with patch.object(pub.os,'replace',defer): pub.publish(tmp_path,proposal,receipt,LIMITS)
    journal='state/unpublished_evidence/causal_timeframe_publications/'+proposal['id']+'/plan.json'
    envelope=stage.stage(tmp_path,proposal=proposal,receipt=receipt,previous=pub.INDEX,
        output='state/unpublished_evidence/recovered',budget=Budget(tmp_path,LIMITS),pub=pub,
        recovery_plan=journal,recovery_predecessor=pub.INDEX,
        recovery_limits_id=dict(receipt.scope)['limits_id'])
    assert envelope['recovered'] and (tmp_path/pub.INDEX).read_bytes()==before
    stage.verify_chain(tmp_path,'state/unpublished_evidence/recovered/stage.json',pub)


def test_changed_payload_blocks_finalization(tmp_path):
    candidate=setup(tmp_path)[0]
    proposal=stage.prepare(tmp_path,candidate=candidate,previous=pub.INDEX,pub=pub)
    stage.stage(tmp_path,proposal=proposal,receipt=publication_receipt(tmp_path,proposal),previous=pub.INDEX,
        output='state/unpublished_evidence/one',budget=Budget(tmp_path,LIMITS),pub=pub)
    final=stage.prepare_final(tmp_path,last='state/unpublished_evidence/one/stage.json',pub=pub)
    payload=tmp_path/proposal['moves'][0]['entry']['directory']/'observations.parquet'
    with payload.open('ab') as stream: stream.write(b'tampered')
    with pytest.raises(IntegrityError,match='payload changed'):
        stage.finalize(tmp_path,proposal=final,receipt=publication_receipt(tmp_path,final),
            output='state/unpublished_evidence/final',budget=Budget(tmp_path,LIMITS),pub=pub)
    assert not (tmp_path/'state/unpublished_evidence/final/index.json').exists()


def test_changed_predecessor_and_unowned_destination_refused(tmp_path):
    candidate=setup(tmp_path)[0]
    proposal=stage.prepare(tmp_path,candidate=candidate,previous=pub.INDEX,pub=pub)
    target=tmp_path/proposal['moves'][0]['entry']['directory']
    target.mkdir(parents=True)
    with pytest.raises(IntegrityError,match='Both or neither'):
        stage.stage(tmp_path,proposal=proposal,receipt=publication_receipt(tmp_path,proposal),previous=pub.INDEX,
            output='state/unpublished_evidence/one',budget=Budget(tmp_path,LIMITS),pub=pub)


def test_receipt_mismatch_blocks_journal_recovery(tmp_path):
    candidate=setup(tmp_path)[0]
    proposal=pub.prepare_publication(tmp_path,candidates=[candidate])
    receipt=publication_receipt(tmp_path,proposal)
    with patch.object(pub,'_stage_moves',side_effect=IntegrityError('interrupted')):
        with pytest.raises(IntegrityError): pub.publish(tmp_path,proposal,receipt,LIMITS)
    wrong=publication_receipt(tmp_path,proposal)
    with pytest.raises(IntegrityError,match='receipt differs'):
        stage.stage(tmp_path,proposal=proposal,receipt=wrong,previous=pub.INDEX,
            output='state/unpublished_evidence/recovery',budget=Budget(tmp_path,LIMITS),pub=pub,
            recovery_plan='state/unpublished_evidence/causal_timeframe_publications/'+proposal['id']+'/plan.json',
            recovery_predecessor=pub.INDEX,recovery_limits_id=dict(receipt.scope)['limits_id'])


@pytest.mark.parametrize('failure',[0,1,2,3,4,'defaults'])
def test_five_control_rollback(tmp_path,failure):
    target=tmp_path/'configs'
    target.mkdir()
    evidence=tmp_path/'state/unpublished_evidence/rollout'
    evidence.mkdir(parents=True)
    changes={}
    expected={}
    for number in range(5):
        path=f'configs/{number}.json'
        if number<4: (tmp_path/path).write_bytes(b'old')
        candidate=f'state/unpublished_evidence/rollout/{number}'
        (tmp_path/candidate).write_bytes(b'new')
        changes[path]=candidate
        expected[path]=sha256_file(tmp_path/path) if number<4 else None
    import os
    original=os.replace
    triggered=[]
    def replace(source,destination):
        if failure!='defaults' and not triggered and str(destination).endswith(f'{failure}.json'):
            triggered.append(True)
            raise OSError('injected control failure')
        return original(source,destination)
    def defaults():
        if failure=='defaults': raise IntegrityError('default reader failure')
    with patch.object(rollout.os,'replace',replace):
        with pytest.raises((IntegrityError,OSError)):
            rollout.activate_controls(tmp_path,changes,expected=expected,
                rollback_directory='state/unpublished_evidence/rollback',validate=defaults)
    assert all((tmp_path/f'configs/{n}.json').read_bytes()==b'old' for n in range(4))
    assert not (tmp_path/'configs/4.json').exists()
