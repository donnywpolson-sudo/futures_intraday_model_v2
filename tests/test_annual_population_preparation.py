"""Generated checks only; no historical payloads or active controls."""
import importlib.util
from pathlib import Path

import pytest

from futures_rebuild.causal_timeframe_storage import read_record
from futures_rebuild.errors import IntegrityError
from tests import test_annual_dependency_retirement as fixture

pytestmark=pytest.mark.current
BASE=Path(__file__).resolve().parents[1]/'state/unpublished_evidence/annual_dependency_retirement_v1'
spec=importlib.util.spec_from_file_location('population_preparation',BASE/'population_preparation.py')
population=importlib.util.module_from_spec(spec);spec.loader.exec_module(population)


def test_factored_runtime_preserves_default_readers(tmp_path,monkeypatch):
    monkeypatch.setattr(fixture,'HERE',BASE/'full_population_preparation_v1')
    fixture.test_cold_public_readers_and_integrity_without_predecessors(tmp_path)


@pytest.mark.parametrize('target',['pointer','unit','validation','supervision','implementation','index','annual_payload'])
def test_factored_runtime_retains_tamper_guards(tmp_path,monkeypatch,target):
    monkeypatch.setattr(fixture,'HERE',BASE/'full_population_preparation_v1')
    fixture.test_missing_or_changed_independence_evidence_refuses(tmp_path,target)


def test_factored_runtime_retains_daily_recovery(tmp_path,monkeypatch):
    monkeypatch.setattr(fixture,'HERE',BASE/'full_population_preparation_v1')
    fixture.test_recovered_daily_identity_survives_removal_of_both_generations(tmp_path)


@pytest.mark.parametrize('fault',[None,'census','supervision','population','source_root'])
def test_census_preview_is_not_publication(tmp_path,monkeypatch,fault):
    monkeypatch.setattr(fixture,'HERE',BASE/'full_population_preparation_v1')
    root,overrides,rows,_=fixture.fixture(tmp_path)
    original=read_record(root/fixture.prep.POINTER)
    manifest=original['manifest']
    census=fixture.save(root,'state/independent/census_preview.json',dict(
        status='FAILED' if fault=='census' else 'PASS_INDEPENDENCE_CENSUS_NOT_ADMITTED',
        manifest=manifest,unit_count=2 if fault=='population' else 3,
        checks=dict(population=True,permissions=True)))
    supervision=fixture.save(root,'state/independent/census_supervision.json',dict(
        status='PASS_SUPERVISED_PHASE',worker_result_sha256='0'*64 if fault=='supervision' else census['sha256']))
    preview=fixture.save(root,fixture.prep.POINTER,dict(schema='annual_independence_preview/1',activation=False,
        validation_root=str(root),source_repository=str(root if fault=='source_root' else BASE.parents[2]),
        manifest=manifest,census=census,supervision=supervision))
    (root/'data/causally_gated_normalized/ES').rename(root/'parked_legacy')
    with fixture.projected(root,overrides) as runtime:
        with pytest.raises(IntegrityError,match='publication authority'):
            runtime.authority(root)
        if fault:
            with pytest.raises(IntegrityError):population.verify_preview(runtime,root,preview)
            return
        # Only the task harness projects the verified census; normal authority
        # above rejects the preview. No passing final result is fabricated.
        with monkeypatch.context() as patch:
            patch.setattr(runtime,'authority',lambda r:population.verify_preview(runtime,r,preview))
            h=fixture.hourly.HourlySourceReader(root,preview_manifest='state/independent/hourly_admission.json')
            d=fixture.daily.DailySourceReader(root,preview_manifest='state/independent/daily_admission.json')
            for tf,reader in [('1h',h),('1d',d)]:
                got=reader.read_window(**fixture.args(rows[tf][0]))
                assert len(got)==1 and got[0]['row_id']==rows[tf][0]['row_id']
        with pytest.raises(IntegrityError):runtime.authority(root)


@pytest.mark.parametrize('path,start,end',[
    ('data/dbn/private.parquet',1,10),
    ('data/causally_gated_normalized/a.parquet',1,101),
    ('data/causally_gated_normalized/a.parquet',10,1)])
def test_physical_scope_rejects_before_access(path,start,end):
    with pytest.raises(IntegrityError):population.physical_input(path,dict(size=1,sha256='a'*64),start,end,100)


def test_retained_inventory_intervals_conflicts_and_protection():
    path='data/causally_gated_normalized/ES/2020/observations.parquet'
    source=dict(directory=str(Path(path).parent).replace('\\','/'),start_ns=1,end_ns=10)
    info=dict(size=1,sha256='a'*64)
    base=dict(units=[dict(sources=[source])],physical_payloads={path:info})
    extension=dict(additional_physical_inputs={})
    assert population.retained_payloads(base,extension,100)[path]==dict(info,start_ns=1,end_ns=10)
    extension['additional_physical_inputs'][path]=dict(info,start_ns=1,end_ns=10,sha256='b'*64)
    with pytest.raises(IntegrityError,match='conflict'):population.retained_payloads(base,extension,100)
    extension['additional_physical_inputs']={}
    source['end_ns']=101
    with pytest.raises(IntegrityError,match='Protected'):population.retained_payloads(base,extension,100)
    source['end_ns']=10;base['units']=[]
    with pytest.raises(IntegrityError,match='undeclared'):population.retained_payloads(base,extension,100)


@pytest.mark.parametrize('phase,status,passing',[
    ('independence_census','PASS_INDEPENDENCE_CENSUS_NOT_ADMITTED',True),
    ('independence_final','PASS_ANNUAL_INDEPENDENCE',True),
    ('independence_census','PASS_ANNUAL_INDEPENDENCE',False),
    ('activation','PASS_ANNUAL_INDEPENDENCE',False)])
def test_validation_supervisor_requires_exact_phase(tmp_path,phase,status,passing):
    import json,sys,time
    from futures_rebuild.causal_timeframe_storage import Limits
    from futures_rebuild.canonical import sha256_file
    spec=importlib.util.spec_from_file_location('validation_supervisor',BASE/'full_population_preparation_v1/validation_supervisor.py')
    supervisor=importlib.util.module_from_spec(spec);spec.loader.exec_module(supervisor)
    child=tmp_path/'child.py'
    child.write_text('''import json,sys,hashlib
from pathlib import Path
request=json.loads(sys.stdin.buffer.readline())
result={'phase':request['phase'],'status':request['test_status']}
result['id']=hashlib.sha256(json.dumps(result,sort_keys=True,separators=(',',':')).encode()).hexdigest()
Path(request['result_path']).write_text(json.dumps(result,sort_keys=True,separators=(',',':'))+'\\n')
''')
    request=dict(phase=phase,test_status=status,result_path='result.json',deadline_epoch=time.time()+30)
    limits=Limits(maximum_written_bytes=2**20,maximum_memory_bytes=256*2**20,maximum_seconds=30)
    if passing:
        result=supervisor.supervise([sys.executable,'-B',str(child)],tmp_path,request,limits,diagnostic=tmp_path/'supervision.json')
        assert result['worker_result_sha256']==sha256_file(tmp_path/'result.json')
    else:
        with pytest.raises(IntegrityError):
            supervisor.supervise([sys.executable,'-B',str(child)],tmp_path,request,limits,diagnostic=tmp_path/'supervision.json')
