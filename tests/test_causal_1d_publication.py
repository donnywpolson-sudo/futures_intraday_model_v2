"""Generated daily releases through the shared publisher and active resolver."""
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild import causal_timeframe_publication as pub
from futures_rebuild.causal_timeframe_storage import contained, read_record, signed
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_causal_1d import daily_publication_fixture, LIMITS, RULEBOOK
from tests.test_causal_timeframe_publication import candidate as hourly_candidate
from tests.test_causal_1s_native import approved
from futures_rebuild.canonical import sha256_json
from dataclasses import asdict

pytestmark=pytest.mark.current


def publication_receipt(root,proposal):
    return approved(root,pub.OPERATION,
        {'publication_id':proposal['id'],'limits_id':sha256_json(asdict(LIMITS))})


def plan(root):
    relative=daily_publication_fixture(root)
    catalog=root/'data/active/catalog.json'
    catalog.parent.mkdir(parents=True,exist_ok=True)
    if not catalog.exists(): catalog.write_text('{}')
    return pub.prepare_publication(root,candidates=[relative])


def test_daily_shared_publication_and_recovery(tmp_path,monkeypatch):
    proposal=plan(tmp_path)
    move=proposal['moves'][0]
    receipt=publication_receipt(tmp_path,proposal)
    with monkeypatch.context() as patch:
        patch.setattr(pub,'validate_release',lambda *a,**k:(_ for _ in ()).throw(IntegrityError('interrupted after move')))
        with pytest.raises(IntegrityError,match='interrupted'):
            pub.publish(tmp_path,proposal,receipt,LIMITS)
    assert not (tmp_path/pub.INDEX).exists()
    result=pub.publish(tmp_path,proposal,receipt,LIMITS)
    paths=pub.resolve_partitions(tmp_path,result,market='ES',year=2024,timeframe='1d',purpose='SYNTHETIC_TEST')
    assert len(paths)==1
    assert '/ES/2024/1d/2024-03/' in paths[0].as_posix()
    release=pub.validate_release(tmp_path,result['entries'][0])
    assert release==move['release']
    assert release['seal']['table_metadata']==move['release']['seal']['table_metadata']
    assert not contained(tmp_path,move['source']).exists()
    assert not list((tmp_path/'data/active').rglob('*.parquet'))
    assert pub.publish(tmp_path,proposal,receipt,LIMITS)==result
    with pytest.raises(UnauthorizedOperation):
        pub.resolve_partitions(tmp_path,result,market='ES',year=2024,timeframe='1d',purpose='LINEAGE_AUDIT')


@pytest.mark.parametrize('field',['contract','implementation','environment','permitted_uses','comparison_counts','table_metadata'])
def test_rehashed_daily_metadata_cannot_bypass_publication(tmp_path,field):
    move=plan(tmp_path)['moves'][0]
    release=deepcopy(move['release'])
    candidate=release['candidate']
    if field in ('contract','implementation','environment'):
        candidate['provenance'][field]={'altered':True}
    elif field=='permitted_uses': candidate[field]=['RESEARCH']
    elif field=='comparison_counts': candidate[field]['MATCH']+=1
    else:
        seal=release['seal']
        seal['table_metadata']['observations']['rows']+=1
        release['seal']=signed({k:v for k,v in seal.items() if k!='id'})
        candidate['seals']=[release['seal']['id']]
    release['candidate']=signed({k:v for k,v in candidate.items() if k!='id'})
    cert={k:v for k,v in release['certificate'].items() if k!='id'}
    cert['candidate_id']=release['candidate']['id']
    release['certificate']=signed(cert)
    finish={k:v for k,v in release['certification_finish'].items() if k!='id'}
    finish['result_id']=release['certificate']['id']
    release['certification_finish']=signed(finish)
    release=signed({k:v for k,v in release.items() if k!='id'})
    with pytest.raises((IntegrityError,UnauthorizedOperation)):
        pub._validate_content(tmp_path,move['entry'],release,contained(tmp_path,move['source']),published=False)


def test_daily_hourly_legacy_seconds_and_minute_coexist(tmp_path):
    from tests.test_active_data_view import _write_stage, H
    from futures_rebuild import active_data_view as active, causal_timeframe_view as legacy
    from futures_rebuild.causal_1s import build_candidate, certify_candidate
    from tests.test_causal_1s import spec, sample
    from tests.test_causal_1s_native import approved
    from futures_rebuild.canonical import sha256_json
    from dataclasses import asdict
    stage,_=_write_stage(tmp_path,plan_id=H['active'],markets=[('ES',2024)])
    destination=tmp_path/'data/active'
    destination.parent.mkdir(parents=True,exist_ok=True)
    stage.rename(destination)
    kwargs=dict(repository_root=tmp_path,market='ES',year=2024,purpose='LINEAGE_AUDIT')
    before=active.resolve(**kwargs)
    catalog=(destination/'catalog.json').read_bytes()
    relative='state/unpublished_evidence/seconds/candidate.json'
    output=tmp_path/Path(relative).parent
    build_candidate(output,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    certify_candidate(output,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    proposal=legacy.prepare_publication(tmp_path,candidates=[relative],expected=[['ES','2024-03']])
    receipt=approved(tmp_path,'PUBLISH_CAUSAL_1S_RELEASE_ONCE',
        {'publication_id':proposal['id'],'limits_id':sha256_json(asdict(LIMITS))})
    old=legacy.publish(tmp_path,proposal,receipt,LIMITS)
    with pytest.raises(UnauthorizedOperation,match='requires v3 publication'):
        active.resolve_partitions(repository_root=tmp_path,market='ES',year=2024,
            timeframe='1d',purpose='SYNTHETIC_TEST')
    manifest=contained(tmp_path,old['entries'][0]['manifest'])
    original=manifest.read_bytes()
    hour=hourly_candidate(tmp_path)
    day=daily_publication_fixture(tmp_path)
    proposal=pub.prepare_publication(tmp_path,candidates=[hour,day])
    index=pub.publish(tmp_path,proposal,publication_receipt(tmp_path,proposal),LIMITS)
    assert {e['timeframe'] for e in index['entries']}=={'1s','1h','1d'}
    assert manifest.read_bytes()==original and (destination/'catalog.json').read_bytes()==catalog
    assert active.resolve(**kwargs)==before
    assert active.resolve_partitions(**kwargs,timeframe='1m')==(before,)
    for timeframe in ('1s','1h','1d'):
        assert len(active.resolve_partitions(repository_root=tmp_path,market='ES',year=2024,
            timeframe=timeframe,purpose='SYNTHETIC_TEST'))==1
