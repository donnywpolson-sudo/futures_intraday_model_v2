"""A lease-blocked phase stays immutable and its bytes remain charged."""
import importlib.util
import time
from pathlib import Path
import pytest
from futures_rebuild.canonical import canonical_bytes,sha256_file
from futures_rebuild.causal_timeframe_storage import signed,read_record

pytestmark=pytest.mark.current


def test_resume_preserves_failure_and_carries_its_cost(tmp_path,monkeypatch):
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('daily_resume_test',root/
        'state/unpublished_evidence/daily_admission_v1/resume_incremental_execution.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    directory=tmp_path/module.REL
    previous=directory/'execution/preflight'
    previous.mkdir(parents=True)
    def save(path,record): path.write_bytes(canonical_bytes(signed(record))+b'\n')
    save(previous/'result.json',dict(status='PASS_PREFLIGHT',resources=dict(application_written_bytes=100)))
    save(previous/'supervision.json',dict(status='PASS_SUPERVISED_PHASE',
        result_sha256=sha256_file(previous/'result.json'),cumulative_created_files=2))
    failed=directory/'execution/stage_00'
    failed.mkdir()
    (failed/'result.json').write_bytes(b'original failed phase')
    expected=100+(previous/'result.json').stat().st_size+(failed/'result.json').stat().st_size
    monkeypatch.setattr(module,'ROOT',tmp_path)
    monkeypatch.setattr(module,'DIRECTORY',directory)
    monkeypatch.setattr(module,'authorize',lambda:({'id':'synthetic-plan'},{'deadline_epoch':time.time()+60}))
    def supervise(command,root,request,limits):
        assert request['prior_written']==expected and request['prior_files']==3
        assert request['operation']['output'].endswith('/stage_00_lease_resume')
        save(root/request['operation']['output']/'result.json',dict(status='PASS_STAGE',
            resources=dict(application_written_bytes=expected,peak_process_memory_bytes=1024,created_files=0)))
        return 1.0
    monkeypatch.setattr(module,'supervise',supervise)
    label=module.run_phase('stage','preflight',0)
    assert label=='stage_00_lease_resume'
    assert (failed/'result.json').read_bytes()==b'original failed phase'
    assert read_record(directory/'execution'/label/'supervision.json')['status']=='PASS_SUPERVISED_PHASE'
