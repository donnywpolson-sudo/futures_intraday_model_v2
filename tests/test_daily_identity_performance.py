"""Opt-in generated population benchmark; never loads historical payloads."""
import hashlib
import os
from pathlib import Path
import sys
import time

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import Limits, read_record, signed, Budget
from futures_rebuild.daily_identity_rollout import supervise, resource_gate

pytestmark=[pytest.mark.current,pytest.mark.skipif(not os.environ.get('DAILY_IDENTITY_BENCHMARK_OUTPUT'),
    reason='Explicit generated population benchmark only')]


def generated_record(path, count, size, references):
    """Stream a valid canonical-identity record, with exactly the requested bytes."""
    digest=hashlib.sha256(); written=0
    with path.open('xb') as stream:
        def emit(raw):
            nonlocal written
            digest.update(raw); stream.write(raw); written+=len(raw)
        emit(b'{"rows":[')
        for n in range(count):
            row=dict(row_id=f'{n:064x}',market='ES',start_ns=n*86400000000000,
                     reason='OBSERVED_HOURLY_AGREEMENT',permitted_uses=['LINEAGE_AUDIT'],
                     reference_row_ids=[f'{n*24+h:064x}' for h in range(references)])
            raw=canonical_bytes(row)
            emit((b',' if n else b'')+raw)
        emit(b'],"schema":"generated_daily_population/1","zz_padding":"')
        # Identity is written last in file order; its payload identity stays canonical.
        tail_bytes=len(b'","id":"'+b'0'*64+b'"}\n')
        remaining=size-written-tail_bytes
        assert remaining>=0
        while remaining:
            chunk=b'x'*min(65536,remaining); emit(chunk); remaining-=len(chunk)
        digest.update(b'"}')
        stream.write(b'","id":"'+digest.hexdigest().encode()+b'"}\n')
    assert path.stat().st_size==size


def test_generated_production_population_and_monthly_replay(tmp_path):
    started=time.monotonic(); population=167609
    sizes=[44726154*population//155026,289857415*population//155026]
    inputs=[]
    for number,(size,refs) in enumerate(zip(sizes,(0,18),strict=True)):
        path=tmp_path/f'population_{number}.json'
        generated_record(path,population,size,refs)
        inputs.append(dict(path=str(path),sha256=sha256_file(path),bytes=size))
    build_seconds=time.monotonic()-started
    command=[sys.executable,'-B','-c',
        'import sys,json,gc; from pathlib import Path; '
        'from futures_rebuild.canonical import sha256_file; '
        'from futures_rebuild.canonical import canonical_bytes; '
        'from futures_rebuild.causal_timeframe_storage import read_record,signed; '
        'from futures_rebuild.daily_identity_rollout import memory; '
        'request=json.loads(sys.stdin.buffer.readline());\n'
        'for entry in request["inputs"]:\n'
        ' p=Path(entry["path"]); assert sha256_file(p)==entry["sha256"]; '
        'record=read_record(p); assert len(record["rows"])==request["population"]; '
        'del record; gc.collect()\n'
        'Path("result.json").write_bytes(canonical_bytes(signed(dict(status="PASS_RECOVERY_PHASE",memory=memory())))+b"\\n")\n']
    limits=Limits(2*2**30,5*2**30,300,minimum_free_bytes=64*2**30,maximum_files=1000)
    measurement=supervise(command,tmp_path,dict(phase='generated_population',inputs=inputs,population=population,result_path='result.json'),
        limits,diagnostic=tmp_path/'supervision.json')
    resource_gate(measurement,coordinator_limit=960*2**20,worker_limit=5*2**30,
                  aggregate_limit=6*2**30,deadline=time.time()+1)
    from tests.test_daily_identity_publication import fixture
    from tests.test_daily_identity_review import evidence
    from tests.test_causal_1d import RULEBOOK, LIMITS
    from futures_rebuild import daily_identity_campaign as campaign
    old,index,overrides,after,reference=fixture(tmp_path,materialize=False)
    cert=read_record(tmp_path/reference['path'])
    expected=cert['passes'][0]['accounting']['months']
    class Hours:
        def _native_month(self,*args): return evidence()[1]
        def _unchanged(self): pass
    samples=[]
    for _ in range(10):
        began=time.monotonic()
        campaign.reconstruct_months(tmp_path,old['entries'],hourly_reader=Hours(),rulebook=RULEBOOK,
            output='state/unpublished_evidence/recovery/reconstruction',plan_id='c'*64,
            budget=Budget(tmp_path,LIMITS),replay=True,expected=expected)
        samples.append(time.monotonic()-began)
    import statistics
    from futures_rebuild.daily_identity_publication import certificate
    large={k:v for k,v in cert.items() if k!='id'}
    monthly=expected[0]
    month_census=[monthly|dict(market=f'GENERATED_{n:04d}') for n in range(6316)]
    outputs=[dict(market=m['market'],month=m['month'],content_id=f'{n:064x}') for n,m in enumerate(month_census)]
    large['passes']=[dict(accounting=dict(months=month_census),outputs=outputs)]*2
    certificate_path=tmp_path/'population_certificate.json'
    certificate_path.write_bytes(canonical_bytes(signed(large))+b'\n')
    cert_ref=dict(path='population_certificate.json',sha256=sha256_file(certificate_path))
    certificate_seconds=[]
    with campaign.projected_successor(tmp_path,campaign.code_overrides()):
        for _ in range(10):
            began=time.monotonic(); certificate(tmp_path,cert_ref)
            certificate_seconds.append(time.monotonic()-began)
    report=signed(dict(status='PASS_GENERATED_MECHANICS_MEASUREMENT',historical_validation=False,
        population=population,serialized_bytes=sizes,build_seconds=build_seconds,
        sequential_manifest_validation=measurement,monthly_replay_seconds=samples,
        monthly_replay_median_seconds=statistics.median(samples),
        certificate_months=6316,certificate_bytes=certificate_path.stat().st_size,
        certificate_validation_seconds=certificate_seconds,
        certificate_validation_median_seconds=statistics.median(certificate_seconds),
        replay_fixture='ONE_MONTH_THREE_DAYS_ONE_RECOVERY_ONE_RETAINED_ONE_EMPTY',
        generated_file_bytes=sum(p.stat().st_size for p in tmp_path.rglob('*') if p.is_file()),
        generated_file_count=sum(1 for p in tmp_path.rglob('*') if p.is_file()),
        total_seconds=time.monotonic()-started))
    destination=Path(os.environ['DAILY_IDENTITY_BENCHMARK_OUTPUT'])
    with destination.open('xb') as stream: stream.write(canonical_bytes(report)+b'\n')
