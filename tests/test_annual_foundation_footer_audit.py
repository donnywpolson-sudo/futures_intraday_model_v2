"""Generated footers only; no real source evaluation."""
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import pyarrow.parquet as pq
import pytest

from futures_rebuild import annual_foundation_footer_audit as audit
from futures_rebuild.canonical import canonical_bytes, sha256_file, io_path
from futures_rebuild.causal_timeframe_storage import signed, read_record
from futures_rebuild.errors import IntegrityError
from tests.test_annual_foundation_storage import source, budget

pytestmark=pytest.mark.current


def inventory(root):
    sources=[source(root,'2024-01',groups=2),source(root,'2024-02',cadence=False)]
    physical={}; units=[]
    for s in sources:
        files={n:s['directory']+'/'+n+'.parquet' for n in s['files']}
        physical.update({files[n]:info for n,info in s['files'].items()})
        left=datetime.fromtimestamp(s['start_ns']//10**9,timezone.utc).strftime('%Y-%m-%d')
        right=datetime.fromtimestamp(s['end_ns']//10**9,timezone.utc).strftime('%Y-%m-%d')
        units.append(dict(directory=s['directory'],month=s['month'],interval=left+'_'+right,files=files,metadata=None))
    return dict(physical_files=physical,metadata_bindings={},units=[dict(timeframe='1m',market='ES',year=2024,
                rows=3,sources=units)])


def test_footer_census_never_decodes_bars_and_detects_mixture(tmp_path,monkeypatch):
    data=inventory(tmp_path); tracker=budget(tmp_path)
    actual=pq.ParquetFile; calls=[]
    class FooterOnly:
        def __init__(self,path): self.file=actual(path); calls.append(path)
        def __enter__(self): return self
        def __exit__(self,*args): self.file.close()
        @property
        def schema_arrow(self): return self.file.schema_arrow
        @property
        def metadata(self): return self.file.metadata
        def read(self,*args,**kwargs): raise AssertionError('decoded bars')
        def read_row_group(self,*args,**kwargs): raise AssertionError('decoded bars')
    monkeypatch.setattr(pq,'ParquetFile',FooterOnly)
    result=audit.census(tmp_path,data,tracker)
    assert len(calls)==10 and result['decoded_observations']==0
    assert result['units'][0]['mixed_empty_cadence'] is True
    assert result['units'][0]['groups']==3 and result['all_within_existing_group_limits']


@pytest.mark.parametrize('kind',['protected','membership','rows','payload','metadata'])
def test_invalid_sources_fail_closed(tmp_path,kind,monkeypatch):
    data=inventory(tmp_path); tracker=budget(tmp_path)
    if kind=='protected':
        data['units'][0]['sources'][0]['interval']='2025-01-01_2026-01-01'
        monkeypatch.setattr(tracker,'hash',lambda _:pytest.fail('Protected file opened'))
    elif kind=='membership': data['physical_files']['data/causally_gated_normalized/unapproved.parquet']={}
    elif kind=='rows': data['units'][0]['rows']=4
    elif kind=='payload': next(iter(data['physical_files'].values()))['sha256']='0'*64
    else:
        p=tmp_path/'bound.json'; p.write_text('{}')
        data['metadata_bindings']={'bound.json':'0'*64}
    with pytest.raises(IntegrityError): audit.census(tmp_path,data,tracker)


def test_actual_supervised_footer_worker_and_no_automatic_retry(tmp_path):
    from dataclasses import asdict
    from futures_rebuild.causal_timeframe_storage import Limits
    data=inventory(tmp_path)
    prepared=tmp_path/'prepared'; prepared.mkdir()
    def save(name,value):
        p=prepared/name; p.write_bytes(canonical_bytes(signed(value))+b'\n'); return p
    inputs=save('inputs.json',data)
    scope=save('scope.json',dict(schema='annual_footer_audit_scope/1',output=audit.OUTPUT,
        implementation_sha256=sha256_file(Path(audit.__file__)),
        inventory=dict(path=inputs.relative_to(tmp_path).as_posix(),sha256=sha256_file(inputs)),
        implementation={},known_prior_measured_writes=0,
        limits=asdict(Limits(2*2**30,5*2**30,120,maximum_files=2000))))
    started=time.time()
    auth=save('authorization.json',dict(status='OWNER_APPROVED_IN_THIS_TASK',scope_id=read_record(scope)['id'],
        scope_sha256=sha256_file(scope),started_epoch=started,deadline_epoch=started+120))
    args=dict(scope_path=scope.relative_to(tmp_path).as_posix(),authorization_path=auth.relative_to(tmp_path).as_posix())
    result=audit.run(tmp_path,**args)
    assert result['status']=='PASS_READ_ONLY_FOOTER_AUDIT_NOT_MIGRATION_CERTIFICATION'
    worker=read_record(io_path(tmp_path/audit.OUTPUT/'result.json'))
    assert worker['files']==10 and worker['decoded_observations']==0
    assert worker['memory']['peak_bytes']<=5*2**30
    with pytest.raises(FileExistsError): audit.run(tmp_path,**args)
