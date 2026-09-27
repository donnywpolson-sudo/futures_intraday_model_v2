"""Inactive ES/NQ access integration. CLI prepares; only approved task calls read.

The study's pure calculations are preserved. A real gateway and fresh one-use
receipt are required before resolving, hashing or opening an observation file.
"""
import argparse
from collections import Counter
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from importlib.metadata import version
import json
from pathlib import Path
import platform
import re
import runpy
import shutil
import time
from types import SimpleNamespace

from .canonical import assert_no_linklike_ancestors, assert_plain_file, canonical_bytes, io_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .l0_alpha_preparation import ResearchStore
from .rolling_hour_price_census import _working_set

BASE = Path('state/unpublished_evidence/l0_order_readiness_v1')
PREP = BASE / 'es_nq_transfer_preparation_v1'
PACKAGE = Path('state/unpublished_evidence/discovery_loop_v1/review_v8/transfer_access')
MATERIALIZATION = BASE / 'es_nq_transfer_adoption_v1/materialization_replacement_v1'
APPROVED = Path('configs/approved_es_nq_transfer.json')
POLICY = Path('src/futures_rebuild/research_gateway_policy.py')
READER = Path('src/futures_rebuild/es_nq_transfer_access.py')
OPERATIONS = {'materialize': 'MATERIALIZE_ES_NQ_TRANSFER_ONCE', 'execute': 'EXECUTE_ES_NQ_TRANSFER_ONCE'}
CLAIM = 'RETROSPECTIVE_ES_NQ_OBSERVED_TARGET_ONLY'
SCOPE_KEYS = {'study_id', 'operation_id', 'kind', 'implementation_id', 'source_plan_id', 'claim'}
LIMITS = {'attempts': 1, 'seconds': 3600, 'memory_bytes': 6*2**30, 'new_output_bytes': 256*2**20, 'additional_cost_usd': 0}
FAILURE = 512*1024
CONTROL = 32*2**20
DAY_CAP = 64*1024


def read(path):
    assert_no_linklike_ancestors(path)
    assert_plain_file(path)
    return json.loads(io_path(path).read_bytes())


def digest(value):
    if not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise UnauthorizedOperation('nonempty canonical identity required')
    return value


def identity(value, field):
    digest(value.get(field))
    if sha256_json({k:v for k,v in value.items() if k != field}) != value[field]:
        raise IntegrityError('identity differs: '+field)


def path_in(root, relative):
    if (not isinstance(relative, str) or not relative or ':' in relative or '\\' in relative
            or relative.startswith('/') or any(p in ('', '.', '..') for p in relative.split('/'))):
        raise UnauthorizedOperation('noncanonical repository path')
    path = root / relative
    assert_no_linklike_ancestors(path)
    if not path.resolve().is_relative_to(root.resolve()):
        raise UnauthorizedOperation('path escapes repository')
    return path


def scope(op):
    return {k:op[k] for k in SCOPE_KEYS}


def require_scope(root, operation, requested):
    """Called by the existing receipt verifier before any observation reader."""
    req = {k:v for k,v in requested.items() if k not in ('approval_command', 'approval_plan_id', 'approval_plan_sha256')}
    if set(req) != SCOPE_KEYS or req.get('kind') not in OPERATIONS or OPERATIONS[req['kind']] != operation:
        raise UnauthorizedOperation('ES/NQ operation scope differs')
    for key in SCOPE_KEYS - {'kind', 'claim'}:
        digest(req[key])
    adopted = read(root/APPROVED)
    selected = adopted.get('operation_ids', {}).get(req['kind'])
    digest(selected)  # null matching null is never an authorization.
    if (selected != req['operation_id'] or adopted.get('state') != 'ADOPTED_INACTIVE_RECEIPT_REQUIRED'
            or adopted.get('study_id') != req['study_id'] or req['claim'] != CLAIM
            or adopted.get('historical_reads_granted') is not False
            or adopted.get('historical_fitting_granted') is not False):
        raise UnauthorizedOperation('study/access scope is not adopted and exact')
    op_path = root/PACKAGE/(req['kind']+'_operation.json')
    op = read(op_path)
    identity(op, 'operation_id')
    impl = read(root/PACKAGE/'implementation.json')
    identity(impl, 'implementation_id')
    if (req != scope(op) or op['operation_name'] != operation or op['limits'] != LIMITS
            or sha256_file(op_path) != adopted['operation_sha256'][req['kind']]
            or sha256_file(root/PACKAGE/'implementation.json') != adopted['implementation_sha256']
            or impl['implementation_id'] != req['implementation_id'] or impl['study_id'] != req['study_id']
            or sha256_file(root/READER) != adopted['reader_sha256']
            or sha256_file(root/POLICY) != impl['bindings'].get(POLICY.as_posix())
            or sha256_file(root/PREP/'study.json') != adopted['study_sha256']
            or sha256_file(root/PREP/'source_plan.json') != adopted['source_plan_sha256']):
        raise UnauthorizedOperation('stale operation, implementation, policy or declaration')
    expected = (MATERIALIZATION if req['kind']=='materialize' else PACKAGE/'evaluation').as_posix()
    if op['destination'] != expected:
        raise UnauthorizedOperation('destination differs')
    return op, impl


def validate_sources(root, op):
    """Full path/field/manifest inspection once before readers, and at completion.

    Receipt checks bind these exact bytes each time; repeatedly walking every
    source directory at every write adds no extra scope check.
    """
    plan = read(root/PREP/'source_plan.json')
    identity(plan, 'source_plan_id')
    if plan['source_plan_id'] != op['source_plan_id']:
        raise UnauthorizedOperation('source plan differs')
    sources = plan['exact_sources']
    if len(sources) != 10 or {(x['market'],x['year']) for x in sources} != {(m,y) for m in ('ES','NQ') for y in range(2018,2023)}:
        raise UnauthorizedOperation('source market/year scope differs')
    for item in sources:
        expected = f"data/active/causally_gated_normalized/{item['market']}/{item['year']}/{item['year']}.parquet"
        if item['parquet_path'] != expected or item['sidecar_path'] != expected+'.manifest.json':
            raise UnauthorizedOperation('source path differs')
        path_in(root, expected); digest(item['parquet_sha256']); digest(item['sidecar_sha256'])
    if op['kind'] == 'materialize':
        if (op['read_set'] != {x['parquet_path']:x['parquet_sha256'] for x in sources}
                or op['fields'] != plan['fields'] or op['materialization_binding'] is not None):
            raise UnauthorizedOperation('source read set or fields differ')
    else:
        binding = op.get('materialization_binding')
        if not isinstance(binding, dict):
            raise UnauthorizedOperation('execution needs actual complete materialization')
        manifest_path = path_in(root, binding['path'])
        if manifest_path != root/MATERIALIZATION/'complete_manifest.json' or sha256_file(manifest_path) != binding['sha256']:
            raise UnauthorizedOperation('materialization binding differs')
        manifest = read(manifest_path); identity(manifest, 'manifest_id')
        recon_path = manifest_path.parent/'reconciliation.json'
        if (manifest['status'] != 'COMPLETE_RECONCILED' or manifest['study_id'] != op['study_id']
                or op['read_set'] != manifest['read_set'] or (manifest_path.parent/'fatal_stop.json').exists()
                or sha256_file(recon_path) != manifest['reconciliation_sha256']
                or read(recon_path)['status'] != 'PASS_RECONCILIATION_NO_FITS'):
            raise UnauthorizedOperation('materialization incomplete or read set differs')
    return plan


def preflight(root, op):
    """Metadata only, including immutable predecessor-policy lineage."""
    actual, impl = require_scope(root, op['operation_name'], scope(op))
    if op != actual:
        raise UnauthorizedOperation('caller specification differs from installed binding')
    plan = validate_sources(root, op)
    for relative, expected in impl['bindings'].items():
        if sha256_file(path_in(root, relative)) != digest(expected):
            raise IntegrityError('stale metadata/code: '+relative)
    runtime = {'python':platform.python_version(), **{p:version(p) for p in ('numpy','tzdata','pyarrow')}}
    if runtime != impl['runtime']:
        raise IntegrityError('bound runtime changed')
    study = read(root/PREP/'study.json'); identity(study, 'study_id')
    if study['study_id'] != op['study_id'] or study['source_plan_id'] != plan['source_plan_id']:
        raise IntegrityError('scientific declaration differs')
    from .causal_observation_parquet import SCHEMAS
    if plan['fields'] != SCHEMAS['observations'].names:
        raise IntegrityError('physical/logical field schema differs')
    return study, plan, read(root/PACKAGE/'source_bindings.json')


class Budget:
    """One attempt clock, exact writes and reserved bounded failure evidence."""
    def __init__(self, path, limits, *, started=None, clock=time.perf_counter, memory=_working_set):
        self.clock, self.memory = clock, memory
        self.started = clock() if started is None else started
        self.path, self.limits = path, limits
        self.failed = False; self.peak = 0; self.used = 0; self.control = 0
        self.check_receipt = lambda:None
        self.authorize_read = self.check
        self.files = {}; self.external = []
        assert_no_linklike_ancestors(path)
        if path.exists():
            raise UnauthorizedOperation('attempt destination exists; no automatic retry')
        if shutil.disk_usage(path.parent).free < limits['new_output_bytes'] + 256*2**20:
            raise IntegrityError('insufficient free disk reserve')
        self.store = ResearchStore(path, {}, maximum_bytes=limits['new_output_bytes'])

    def check(self):
        if self.failed:
            raise IntegrityError('operation permanently stopped')
        try:
            self.check_receipt(); self.peak = max(self.peak, self.memory())
            if (self.clock()-self.started >= self.limits['seconds']-5 or self.peak >= self.limits['memory_bytes']
                    or self.used+FAILURE > self.limits['new_output_bytes']):
                raise IntegrityError('attempt resource limit reached')
        except BaseException:
            self.failed = True
            raise

    def save(self, name, value):
        try:
            self.check(); size = len(canonical_bytes(value))+1
            daily = re.fullmatch(r'days/\d{4}-\d{2}-\d{2}/(inputs|targets)', name)
            if daily and size > DAY_CAP:
                raise IntegrityError('daily serialization exceeds declared allowance')
            if not daily and self.control+size > CONTROL:
                raise IntegrityError('control/result serialization exceeds allowance')
            if self.used+size+FAILURE > self.limits['new_output_bytes']:
                raise IntegrityError('next write would consume failure reserve')
            if self.store.path(name).exists():
                raise IntegrityError('artifact already written')
            self.store.save(name, value)
            path = self.store.path(name)
            if path.stat().st_size != size:
                raise IntegrityError('serialized size differs')
            self.files[name] = {'sha256':sha256_file(path), 'bytes':size}
            self.used += size
            if not daily: self.control += size
            self.check()
        except BaseException:
            self.failed = True
            raise

    def fatal(self, exc):
        self.failed = True
        value = {'status':'FATAL_STOP_NO_RETRY', 'error':(type(exc).__name__+': '+str(exc))[:2048],
                 'saved_files':len(self.files), 'partial_evidence':'Existing files retained; no resumption',
                 'elapsed_seconds':self.clock()-self.started, 'peak_memory_bytes':self.peak, 'bytes_before_failure':self.used}
        size = len(canonical_bytes(value))+1
        actual = sum(p.stat().st_size for p in self.path.rglob('*') if p.is_file()) + sum(p.stat().st_size for p in self.external)
        if size <= FAILURE and actual+size <= self.limits['new_output_bytes']:
            self.store.save('fatal_stop', value)
            return True
        return False


def source_rows(root, market, plan, provenance, counts, budget):
    """Only the ten bound normalized containers; existing resolver and decoder."""
    from .active_data_view import resolve
    from .causal_observation_parquet import _decode_physical, _parquet_io_path, SCHEMAS
    import pyarrow.parquet as pq
    previous = None
    for item in plan['exact_sources']:
        if item['market'] != market: continue
        budget.authorize_read()
        expected = path_in(root, item['parquet_path'])
        # Containment/receipt are checked before even the resolver's payload hash.
        path = resolve(repository_root=root, market=market, year=item['year'], purpose='FEATURE_GENERATION')
        if path != expected or sha256_file(path) != item['parquet_sha256']:
            raise IntegrityError('resolved source differs')
        binding = provenance[str(item['year'])][market]
        count = 0
        with pq.ParquetFile(_parquet_io_path(path)) as parquet:
            if parquet.metadata.num_rows != item['row_count'] or not parquet.schema_arrow.equals(SCHEMAS['observations'], check_metadata=True):
                raise IntegrityError('source count or physical schema differs')
            for batch in parquet.iter_batches(batch_size=4096, columns=plan['fields']):
                budget.authorize_read()
                for raw in batch.to_pylist():
                    row = _decode_physical('observations', raw); count += 1
                    key = (row['project_trade_date'], row['bar_start_ns'])
                    if (row['market'] != market or row['source_cadence'] != '1m'
                            or not f"{item['year']}-01-01" <= key[0] < f"{item['year']+1}-01-01"
                            or previous is not None and key <= previous or count > item['row_count']
                            or [row['source_contract_id'],row['source_release_id']] != binding):
                        raise IntegrityError('observation scope, ordering or release differs')
                    previous = key
                    yield row
        budget.check()
        if count != item['row_count'] or sha256_file(path) != item['parquet_sha256']:
            raise IntegrityError('source truncated or changed during read')
        counts[item['parquet_path']] = count


def paired_days(root, plan, provenance, counts, budget, mechanics):
    from .l0_alpha_source_readiness import daily_inputs
    with ExitStack() as stack:
        streams = {m:stack.enter_context(closing(source_rows(root,m,plan,provenance,counts,budget))) for m in ('ES','NQ')}
        daily = {m:stack.enter_context(closing(daily_inputs(streams[m]))) for m in streams}
        for day in mechanics.c.weekday_dates():
            budget.check(); pair = {}
            times = mechanics.clock(day)
            first, last = times[0]-mechanics.c.DELAY-6*mechanics.c.MINUTE, times[-1]-mechanics.c.DELAY+16*mechanics.c.MINUTE
            for m in streams:
                supplied, rows, _ = next(daily[m])
                if supplied != day: raise IntegrityError('paired date alignment differs')
                pair[m] = [{**r,'row_id':sha256_json(r)} for r in rows if first <= r['bar_start_ns'] <= last]
            yield day, pair
        for stream in daily.values():
            if next(stream, None) is not None: raise IntegrityError('extra source date')


def reconcile(root, budget, op, plan, materialization, mechanics):
    """Read this attempt's own separated artifacts, without loading source again."""
    counts = Counter(); masks = Counter(); daily = []; populations = [dict() for _ in range(8)]
    windows = mechanics.outer_plans(); dates = mechanics.c.weekday_dates()
    for day, entry in zip(dates, materialization['days'], strict=True):
        budget.check(); a,b = [read(budget.store.path(f'days/{day}/{n}')) for n in ('inputs','targets')]
        for n in ('inputs','targets'):
            path = budget.store.path(f'days/{day}/{n}')
            if sha256_file(path) != budget.files[f'days/{day}/{n}']['sha256']: raise IntegrityError('materialized file changed')
        mechanics.validate_input(a); mechanics.validate_target(a,b)
        if entry['date'] != day or entry['input_id'] != a['input_id'] or entry['target_id'] != b['target_id']:
            raise IntegrityError('materialization index differs')
        members = [{'forecast_id':r['forecast_id'],'known_at_ns':o['known_at_ns']} for r,o in zip(a['decisions'],b['outcomes'],strict=True) if o['status']=='MATURED']
        if entry['membership_id'] != sha256_json(members) or entry['observed_targets'] != len(members):
            raise IntegrityError('membership reconciliation differs')
        counts['scheduled'] += 22; counts['eligible'] += sum(r['status']=='ELIGIBLE' for r in a['decisions'])
        counts['observed_targets'] += len(members)
        masks.update(o['status'] for o in b['outcomes'])
        daily.append({'date':day,'eligible':sum(r['status']=='ELIGIBLE' for r in a['decisions']), 'observed':len(members)})
        for i,(w,allowed) in enumerate(windows):
            if day in allowed:
                n = sum(x['known_at_ns'] < mechanics.clock(dates[w.start])[0] for x in members)
                if n: populations[i][day] = n
    if dict(counts) != materialization['counts'] or len(daily)!=1305 or counts['scheduled']!=28710:
        raise IntegrityError('complete scheduled coverage/counts differ')
    result = {'status':'PASS_RECONCILIATION_NO_FITS', 'counts':dict(counts), 'target_status_counts':dict(masks),
              'daily':daily, 'training_support':[{'outer_fold':i,'rows':sum(v.values()),'observed_dates':len(v),
                    'minimum_met':len(v)>=505} for i,v in enumerate(populations)]}
    budget.save('reconciliation', result)
    return result


def execute_authorized(gateway, receipt, op):
    """No CLI approval parser. Caller must have the separately approved exact read."""
    from .boundary import OperationReceipt, OperationClassification
    from .certified_research_gateway import CertifiedResearchGateway
    classification = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
    started = time.perf_counter()
    if type(gateway) is not CertifiedResearchGateway or type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation('real gateway and fresh exact receipt required')
    root = gateway.root
    required = dict(receipt.scope)
    if {k:v for k,v in required.items() if not k.startswith('approval_')} != scope(op):
        raise UnauthorizedOperation('receipt differs from exact operation')
    receipt.verify(gateway.boundary, operation=op['operation_name'], classification=classification, required_scope=required)
    study, plan, provenance = preflight(root, op)
    mechanics = SimpleNamespace(**runpy.run_path(str(root/PREP/'transfer.py')))
    path = path_in(root, op['destination'])
    from .discovery_attempt import CampaignBudget
    budget = CampaignBudget(root, op, path, started=started)
    original_fit = mechanics.fit_ols
    def counted_fit(*args, **kwargs):
        budget.reserve_fit()
        return original_fit(*args, **kwargs)
    mechanics.run_screen.__globals__['fit_ols'] = counted_fit
    budget.check_receipt = lambda:receipt.verify(gateway.boundary, operation=op['operation_name'], classification=classification, required_scope=required)
    try:
        budget.check()
        allowance = 1305*2*DAY_CAP+CONTROL+FAILURE+4096
        if allowance > op['limits']['new_output_bytes']:
            raise IntegrityError('complete serialized output allowances exceed cap')
        budget.save('preflight', {'status':'PASS_METADATA_ONLY','total_output_allowance_bytes':allowance,
                    'basis':'Enforced 64KiB per input/target day; 32MiB other output; 512KiB failure; 4KiB receipt ledger.',
                    'not_a_guarantee_of_arbitrary_source_string_sizes':True})
        use = receipt.consume(gateway.boundary,operation=op['operation_name'],classification=classification,required_scope=required)
        budget.external.append(use); budget.used += use.stat().st_size
        if use.stat().st_size > 4096: raise IntegrityError('receipt ledger exceeds allowance')
        def lifecycle():
            if not receipt.not_before <= datetime.now(timezone.utc) < receipt.expires_at:
                raise UnauthorizedOperation('receipt expired during the operation')
        budget.check_receipt = lifecycle
        def authorize_read():
            try:
                budget.check()
                receipt.assert_consumed(gateway.boundary,operation=op['operation_name'],classification=classification,required_scope=required)
            except BaseException:
                budget.failed = True
                raise
        budget.authorize_read = authorize_read
        budget.save('operation_receipt', receipt.as_dict()); budget.save('operation', op)
        if op['kind'] == 'materialize':
            counts = {}
            with closing(paired_days(root,plan,provenance,counts,budget,mechanics)) as pairs:
                material = mechanics.materialize_days(pairs,{int(y):v for y,v in provenance.items()},study['study_id'],budget.save,budget.check)
            if counts != {x['parquet_path']:x['row_count'] for x in plan['exact_sources']}:
                raise IntegrityError('not all ten exact source counts reconciled')
            recon = reconcile(root,budget,op,plan,material,mechanics)
            read_set = {(path/f'days/{d}/{n}.json').relative_to(root).as_posix():budget.files[f'days/{d}/{n}']['sha256']
                        for d in study['dates'] for n in ('inputs','targets')}
            result = {'status':'COMPLETE_RECONCILED','study_id':study['study_id'],'operation_id':op['operation_id'],
                      'materialization_id':material['materialization_id'],'source_counts':counts,'read_set':read_set,
                      'reconciliation_sha256':budget.files['reconciliation']['sha256'], 'training_support':recon['training_support']}
        else:
            inputs,targets = {},{}
            for day in study['dates']:
                for name,values in (('inputs',inputs),('targets',targets)):
                    budget.authorize_read(); relative = (MATERIALIZATION/f'days/{day}/{name}.json').as_posix()
                    p = path_in(root, relative)
                    if relative not in op['read_set'] or sha256_file(p) != op['read_set'][relative]:
                        raise IntegrityError('execution artifact hash or scope differs')
                    values[day] = read(p)
            result = mechanics.run_screen(inputs,targets,study['study_id'],budget.save,budget.check)
        budget.authorize_read(); preflight(root,op)
        actual = sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) + sum(p.stat().st_size for p in budget.external)
        if actual != budget.used: raise IntegrityError('attempt output accounting differs')
        budget.save('resources', {'elapsed_seconds':time.perf_counter()-started,'peak_memory_bytes':budget.peak,
                    'bytes_before_terminal_record':actual,'cost_usd':0,'active':False})
        if op['kind']=='materialize':
            result['manifest_id'] = sha256_json(result)
            budget.save('complete_manifest',result)
        if op['kind'] == 'execute':
            budget.publish_transfer(result)
        budget.finish('COMPLETE')
        return result
    except BaseException as exc:
        try: budget.fatal(exc)
        except Exception: pass  # Preserve the original global stop if storage itself fails.
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('validate','materialize','execute'))
    args = parser.parse_args(); root = Path(__file__).resolve().parents[2]
    op = read(root/PACKAGE/('execute_operation.json' if args.action=='execute' else 'materialize_operation.json'))
    if args.action != 'execute': preflight(root,op)
    print(json.dumps({'status':'PREPARE_ONLY_NO_DATA_READ','operation_id':op['operation_id'],
                      'read_approval_required':True,'fit_approval_required':True,'activation':False}))
