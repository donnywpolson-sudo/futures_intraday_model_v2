"""Generated metadata only; current audit and contract bytes remain untouched."""
from copy import deepcopy
import json
from pathlib import Path
import shutil

import pytest

from reports.dbn_layout_alignment_20260915.prepare_consumer_integration import render, contract, prepare, render_readiness
from futures_rebuild import annual_foundation_release as release
from futures_rebuild import annual_foundation_integration as integration
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.errors import IntegrityError
from tests.test_active_data_view import _write_stage
from tests.test_annual_foundation_integration import successors

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def audit():
    source = (ROOT/'src/futures_rebuild/v10_foundation_audit.py').read_text()
    namespace = {'__name__': 'futures_rebuild._annual_audit_candidate', '__package__': 'futures_rebuild'}
    exec(compile(render(source), 'annual_audit_candidate', 'exec'), namespace)
    return namespace


@pytest.mark.parametrize('tamper', [None, 'purpose', 'predecessor', 'index'])
def test_audit_uses_exact_original_catalog_without_waiving_permissions(tmp_path, tamper):
    stage, old = _write_stage(tmp_path, plan_id='a'*64, markets=[('ES', 2024)])
    shutil.copytree(stage, tmp_path/'data/active')
    prior = tmp_path/'predecessor.json'
    prior.write_bytes((tmp_path/'data/active/catalog.json').read_bytes())
    ref = dict(path='predecessor.json', sha256=sha256_file(prior))
    new = deepcopy(old)
    e = new['entries'][0]
    e['annual_release_key'] = 'b'*64
    e['parquet_path'], e['sidecar_path'] = release.minute_paths(e)
    if tamper == 'purpose':
        e['permitted_uses'] = ['LINEAGE_AUDIT']
    new['catalog_sha256'] = sha256_json({k:v for k,v in new.items() if k != 'catalog_sha256'})
    active = tmp_path/'data/active/catalog.json'
    active.write_bytes(canonical_bytes(new)+b'\n')
    index = signed(dict(entries=[], legacy_1m={'path':'data/active/catalog.json', 'sha256':sha256_file(active)},
                        annual_packaging={'predecessor_catalog':ref}))
    if tamper == 'predecessor':
        prior.write_text('{}')
    if tamper == 'index':
        index['legacy_1m']['sha256'] = '0'*64
    (tmp_path/'data/active/timeframes.json').write_bytes(canonical_bytes(index))
    _, overrides = successors(tmp_path)
    reads = []
    def read(path, expected=None):
        target = tmp_path/path
        if expected is not None and sha256_file(target) != expected:
            raise IntegrityError('metadata changed')
        reads.append(path)
        return json.loads(target.read_bytes())
    with integration.projected(tmp_path, overrides):
        fn = audit()['_annual_catalog_sha']
        if tamper:
            with pytest.raises(IntegrityError): fn(tmp_path, new, read)
        else:
            assert fn(tmp_path, new, read) == ref['sha256']
            assert reads == ['data/active/timeframes.json', 'predecessor.json']


def test_nested_permission_evidence_is_included_in_audit_read_inventory(tmp_path, monkeypatch):
    calls = []
    wrapper = {'comparison':dict(path='comparison.json',sha256='a'),
               'supervision':dict(path='supervision.json',sha256='b')}
    entry = dict(sidecar_path='manifest.json',sidecar_sha256='c',annual_release_key='d')
    def read(path, expected):
        calls.append((path,expected))
        return wrapper
    def validate(root, selected):
        assert root == tmp_path and selected is entry
        return {'validated_nested_permission':True}
    monkeypatch.setattr(release, 'minute_permission', validate)
    assert audit()['_annual_sidecar'](tmp_path,entry,read) == {'validated_nested_permission':True}
    assert calls == [('manifest.json','c'),('comparison.json','a'),('supervision.json','b')]


def test_contract_changes_only_layout_and_render_rejects_predecessor_drift():
    before = json.loads((ROOT/'configs/active_data_view_contract.json').read_bytes())
    after = contract(before)
    assert {k for k in before if before[k] != after[k]} == {'manifest_layout','parquet_layout'}
    assert before['resolver_rules'] == after['resolver_rules']
    with pytest.raises(ValueError, match='predecessor changed'):
        render('changed source')


def test_preparation_uses_one_windows_path_form_and_preserves_originals(tmp_path):
    from futures_rebuild.annual_foundation_storage import CandidateBudget
    from futures_rebuild.causal_timeframe_storage import Limits
    names = ('src/futures_rebuild/v10_foundation_audit.py','configs/active_data_view_contract.json')
    for name in names:
        target = tmp_path/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(ROOT/name,target)
    original = {name:sha256_file(tmp_path/name) for name in names}
    tracker = CandidateBudget(tmp_path,'generated_consumer',Limits(2**30,5*2**30,60,minimum_free_bytes=1))
    changes = prepare(tmp_path,tracker)
    assert {name:sha256_file(tmp_path/name) for name in names} == original
    for name, refs in changes.items():
        assert refs['predecessors']['sha256'] == original[name]
        assert refs['candidate']['sha256'] != original[name]
        assert all(v['path'].startswith('state/unpublished_evidence/') for v in refs.values())


@pytest.mark.parametrize('failure', list(range(18))+['default','memory','deadline'])
def test_audit_successor_is_in_exact_reverse_order_rollback(tmp_path, monkeypatch, failure):
    from tests import test_annual_foundation_rollback as rollback
    order = rollback.ORDER[:8] + tuple('src/futures_rebuild/'+n for n in (
        'v10_foundation_audit.py','l0_alpha_source_readiness.py','fixed_hour_ridge_discovery.py')) + rollback.ORDER[8:]
    assert len(order) == 18
    monkeypatch.setattr(rollback, 'ORDER', order)
    rollback.test_exact_annual_control_rollback(tmp_path, monkeypatch, failure)


@pytest.mark.parametrize('name',['l0_alpha_source_readiness.py','fixed_hour_ridge_discovery.py'])
def test_current_readiness_preparation_accepts_annual_paths_without_reading_prices(tmp_path, monkeypatch, name):
    from futures_rebuild.canonical import write_new_canonical_authority
    entries=[]
    for market in ('ES','NQ','CL','GC'):
        for year in range(2018,2023):
            entry=dict(market=market,year=year,annual_release_key='a'*64,
                row_count=100,coverage_start=f'{year}-01-01',coverage_end=f'{year+1}-01-01',
                disposition='RESEARCH_READY_CAUSAL_PRICE',selection_eligible=True,
                permitted_uses=['FEATURE_GENERATION'],parquet_sha256='b'*64,sidecar_sha256='c'*64,
                content_validation_receipt_id='d'*64,access_policy_binding_id='e'*64)
            entry['parquet_path'],entry['sidecar_path']=release.minute_paths(entry)
            path=tmp_path/entry['parquet_path'];path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(b'not a Parquet; metadata must not decode it')
            entries.append(entry)
    write_new_canonical_authority(tmp_path/'data/active/catalog.json',{'entries':entries})
    namespace={'__name__':'futures_rebuild._readiness_candidate','__package__':'futures_rebuild'}
    exec(compile(render_readiness(name,(ROOT/'src/futures_rebuild'/name).read_text()),name,'exec'),namespace)
    original=Path.read_bytes
    def read(path):
        assert path.suffix!='.parquet'
        return original(path)
    monkeypatch.setattr(Path,'read_bytes',read)
    def invoke():
        return (namespace['prepare'](tmp_path)['exact_sources'] if name.startswith('l0_')
                else namespace['_source_scope'](tmp_path,{'entries':entries}))
    assert len(invoke())==20
    entries[0]['sidecar_path']='unexpected.json'
    # L0 reads the frozen catalog, while Ridge takes it as an argument.
    if name.startswith('l0_'):
        (tmp_path/'data/active/catalog.json').write_bytes(canonical_bytes({'entries':entries})+b'\n')
    with pytest.raises(IntegrityError,match='sidecar path'):
        invoke()
