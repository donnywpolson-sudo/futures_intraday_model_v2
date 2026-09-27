"""Two real synthetic publisher steps with an unchanged active index."""
import importlib.util

import pytest

from tests.test_daily_coordinated_publication import (
    ROOT,rollout,build,sample,empty,spec,BASE,DAY,LIMITS,publication_receipt)
from futures_rebuild import causal_timeframe_publication as pub
from futures_rebuild.canonical import canonical_bytes,sha256_file
from futures_rebuild.causal_timeframe_storage import Budget,signed,read_record
from futures_rebuild.daily_quality_reader import DailyReader
from futures_rebuild.errors import IntegrityError

pytestmark=pytest.mark.current
module_spec=importlib.util.spec_from_file_location('daily_sequence_test',
    ROOT/'state/unpublished_evidence/daily_admission_v1/publication_sequence.py')
sequence=importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(sequence)


def test_two_published_candidates_preserve_active_and_prior_indexes(tmp_path):
    candidates=[]
    for number,day in enumerate((BASE,BASE+31*DAY)):
        relative=f'state/unpublished_evidence/source/{number}/candidate.json'
        source=tmp_path/relative
        build(source.parent,[sample(day),empty(day+DAY)],spec(day,day+2*DAY))
        (source.parent/'provenance.json').write_bytes(canonical_bytes(signed({})))
        candidates.append(relative)
    catalog=tmp_path/'data/active/catalog.json'
    catalog.parent.mkdir(parents=True)
    catalog.write_text('{}')
    original=signed(dict(schema=pub.SCHEMA,status='PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY',
        legacy_1m=dict(path='data/active/catalog.json',sha256=sha256_file(catalog)),expected=[],entries=[]))
    active=tmp_path/pub.INDEX
    active.write_bytes(canonical_bytes(original)+b'\n')
    before=active.read_bytes()
    clones=rollout.clone_candidates(tmp_path,candidates,'state/unpublished_evidence/clones',Budget(tmp_path,LIMITS))
    previous=pub.INDEX
    indexes={}
    for number,candidate in enumerate(clones):
        proposal=sequence.prepare_step(tmp_path,candidate=candidate,predecessor=previous,rollout=rollout)
        destination=f'state/unpublished_evidence/sequence/{number}/index.json'
        result=sequence.publish_step(tmp_path,proposal=proposal,receipt=publication_receipt(tmp_path,proposal),
            limits=LIMITS,predecessor=previous,staged_index=destination,rollout=rollout)
        assert len(result['entries'])==number+1 and active.read_bytes()==before
        indexes[destination]=sha256_file(tmp_path/destination)
        assert all(sha256_file(tmp_path/p)==digest for p,digest in indexes.items())
        previous=destination
    guard=ROOT/'state/unpublished_evidence/daily_foundation_v1/remediation_preparation_v1/publication.candidate.py'
    with rollout.projected_reads(tmp_path,{pub.INDEX:tmp_path/previous},guard):
        reader=DailyReader(tmp_path)
        for day in (BASE,BASE+31*DAY):
            assert len(reader.read_window(market='ES',start_ns=day,end_ns=day+DAY,
                as_of_ns=day+2*DAY,purpose='SYNTHETIC_TEST'))==1
    assert active.read_bytes()==before and all((tmp_path/p).exists() for p in candidates)
    with pytest.raises(IntegrityError,match='predecessor differs'):
        sequence.publish_step(tmp_path,proposal=proposal,receipt=None,limits=LIMITS,
            predecessor=pub.INDEX,staged_index='state/unpublished_evidence/never/index.json',rollout=rollout)
    with pytest.raises(IntegrityError,match='task evidence'):
        sequence.prepare_step(tmp_path,candidate=clones[0],predecessor='configs/other.json',rollout=rollout)
    assert read_record(active)==original
