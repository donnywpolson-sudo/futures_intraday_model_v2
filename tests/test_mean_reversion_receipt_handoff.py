"""Real receipt mechanics in a generated repository; no market rows are opened."""
import json

import pytest

from futures_rebuild import mean_reversion_readiness as r
from futures_rebuild.boundary import OperationReceipt, RepoBoundary
from futures_rebuild.errors import UnauthorizedOperation

pytestmark=pytest.mark.current


def receipt(root, plan):
    return OperationReceipt.issue_user_approved(RepoBoundary(root),operation=r.OPERATION,
        classification=r.CLASSIFICATION,scope=r.scope(plan),approval_command=r.OPERATION,
        approval_plan_id='a'*64,approval_plan_sha256='b'*64,
        approval_line=f"APPROVE {r.OPERATION} PLAN {'a'*64} SHA256 {'b'*64}")


def test_external_receipt_requires_canonical_authorization_directory(tmp_path):
    value=receipt(tmp_path,dict(claim='GENERATED_TEST_ONLY',plan_id='a'*64))
    wrong=tmp_path/'reports/receipt.json';wrong.parent.mkdir()
    wrong.write_text(json.dumps(value.as_dict()))
    with pytest.raises(UnauthorizedOperation,match='outside its active repository subtree'):
        OperationReceipt.load_external(wrong,RepoBoundary(tmp_path))
    correct=tmp_path/'state/authorizations/test.json';correct.parent.mkdir(parents=True)
    correct.write_text(json.dumps(value.as_dict()))
    assert OperationReceipt.load_external(correct,RepoBoundary(tmp_path)).receipt_id==value.receipt_id
    assert not (tmp_path/'state/authorization_uses').exists()


def test_worker_reaches_resolver_with_real_receipt_and_retains_failure(tmp_path,monkeypatch):
    plan=dict(claim='GENERATED_TEST_ONLY',plan_id='a'*64,output='state/generated',
        limits=dict(seconds=30,peak_working_set_bytes=3*2**30,free_reserve_bytes=0,
                    output_bytes=2**20,files=20,batch_rows=1),dates=['2020-01-02'],
        sources=[dict(year=2020)],folds=[])
    monkeypatch.setattr(r,'prepare',lambda root:plan)
    from futures_rebuild import active_data_view
    def refuse(**kwargs):
        assert kwargs==dict(repository_root=tmp_path,market='ES',year=2020,purpose='FEATURE_GENERATION')
        raise RuntimeError('GENERATED_STOP_BEFORE_SOURCE_ACCESS')
    monkeypatch.setattr(active_data_view,'resolve',refuse)
    value=receipt(tmp_path,plan)
    with pytest.raises(RuntimeError,match='GENERATED_STOP'):
        r.execute_once(tmp_path,plan,value)
    result=json.loads((tmp_path/'state/generated/FAILED.json').read_bytes())
    assert result['status']=='FAILED' and 'GENERATED_STOP' in result['error']
    assert (tmp_path/f'state/authorization_uses/{value.receipt_id}.json').exists()
    assert not (tmp_path/'state/locks/mean_reversion_readiness.lock').exists()
    with pytest.raises(UnauthorizedOperation,match='already exists'):
        r.execute_once(tmp_path,plan,value)


def test_actual_python_worker_joins_job_and_is_terminated(tmp_path):
    import ctypes
    from pathlib import Path
    import subprocess
    import sys
    from uuid import uuid4
    from futures_rebuild.durable_windows_task_transport import WindowsKillOnCloseJob, _ExtendedLimitInformation
    script=Path(__file__).resolve().parents[1]/'reports/mean_reversion_baseline_v1/run_approved_census_v2.py'
    with WindowsKillOnCloseJob() as job:
        name='Local\\mr_test_'+uuid4().hex
        named=job._kernel32.CreateJobObjectW(None,name)
        assert named
        job.close();job._handle=named
        limits=_ExtendedLimitInformation();limits.BasicLimitInformation.LimitFlags=0x2000
        assert job._kernel32.SetInformationJobObject(named,9,ctypes.byref(limits),ctypes.sizeof(limits))
        code=("import runpy,time; m=runpy.run_path("+repr(str(script))+"); "
              "m['join_worker_job']("+repr(name)+"); print('JOINED',flush=True); time.sleep(30)")
        child=subprocess.Popen([sys.executable,'-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                               creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            # This generated process opens no data and must be terminated by
            # the same job that owns the actual interpreter, not its redirector.
            import threading
            lines=[]
            thread=threading.Thread(target=lambda:lines.append(child.stdout.readline()),daemon=True)
            thread.start();thread.join(timeout=10)
            assert lines==[b'JOINED\r\n'] or lines==[b'JOINED\n']
            job.terminate()
            child.wait(timeout=10)
            assert child.returncode!=0
        finally:
            if child.poll() is None:
                job.terminate();child.kill();child.wait(timeout=10)
            child.stdout.close();child.stderr.close()


@pytest.mark.parametrize("changed", [None, "path", "payload", "sidecar"])
def test_extended_path_binding_before_decode(tmp_path, monkeypatch, changed):
    from futures_rebuild import active_data_view
    from futures_rebuild.canonical import io_path, sha256_file
    from futures_rebuild.errors import IntegrityError
    import pyarrow.parquet as pq
    payload=tmp_path/'observations.parquet';payload.write_bytes(b'generated-only')
    sidecar=tmp_path/'manifest.json';sidecar.write_bytes(b'{}')
    source=dict(year=2020,parquet_path=payload.name,parquet_sha256=sha256_file(payload),
                sidecar_path=sidecar.name,sidecar_sha256=sha256_file(sidecar))
    resolved=payload
    if changed=='path':
        resolved=tmp_path/'other.parquet';resolved.write_bytes(payload.read_bytes())
    elif changed=='payload':payload.write_bytes(b'changed')
    elif changed=='sidecar':sidecar.write_bytes(b'changed')
    plan=dict(claim='GENERATED_TEST_ONLY',plan_id='a'*64,output='state/generated',
        limits=dict(seconds=30,peak_working_set_bytes=3*2**30,free_reserve_bytes=0,
                    output_bytes=2**20,files=20,batch_rows=1),dates=['2020-01-02'],
        sources=[source],folds=[])
    monkeypatch.setattr(r,'prepare',lambda root:plan)
    monkeypatch.setattr(active_data_view,'resolve',lambda **kwargs:io_path(resolved))
    def decoder(path):raise RuntimeError('GENERATED_DECODER_REACHED')
    monkeypatch.setattr(pq,'ParquetFile',decoder)
    error=IntegrityError if changed else RuntimeError
    with pytest.raises(error,match='Resolved source differs' if changed else 'GENERATED_DECODER_REACHED'):
        r.execute_once(tmp_path,plan,receipt(tmp_path,plan))
