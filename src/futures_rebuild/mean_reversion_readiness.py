"""Metadata preparation and receipt-gated availability census. No model execution."""
from collections import Counter
from datetime import datetime, timezone
from itertools import groupby
from pathlib import Path
import shutil
import time

from . import mean_reversion_baseline as m
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, read_canonical_authority, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .l0_alpha_candidate import weekday_dates, frozen_ranges
from .locking import FileLease
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION as OPERATION
from .rolling_hour_price_census import _working_set

BASE = Path('reports/mean_reversion_baseline_v1')
CATALOG = 'data/causally_gated_normalized/_control/catalog.json'
CLASSIFICATION = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
AS_OF = int(datetime(2023,1,1,tzinfo=timezone.utc).timestamp())*m.NS
CODE = ('mean_reversion_baseline.py','mean_reversion_readiness.py','active_data_view.py',
        'annual_foundation_release.py','causal_observation_parquet.py','boundary.py',
        'research_gateway_policy.py','discovery.py','l0_alpha_candidate.py','research/splits.py')


def bind(value):
    return dict(value, plan_id=sha256_json(value))


def prepare(root):
    """Read control metadata and stat named payloads only; never decode or hash data."""
    root=Path(root).resolve()
    catalog=read_canonical_authority(root/CATALOG)
    sources=[]
    for year in range(2018,2023):
        matches=[r for r in catalog['entries'] if r['market']=='ES' and r['year']==year]
        if len(matches)!=1: raise IntegrityError('One admitted ES annual source required')
        row=matches[0]
        if (row['disposition']!='RESEARCH_READY_CAUSAL_PRICE'
                or not {'FEATURE_GENERATION','DISCOVERY_RESEARCH'} <= set(row['permitted_uses'])
                or not row.get('annual_release_key')
                or row['coverage_start']!=f'{year}-01-01' or row['coverage_end']!=f'{year+1}-01-01'):
            raise UnauthorizedOperation('Declared ES source is not admitted')
        source={k:row[k] for k in ('market','year','parquet_path','parquet_sha256','sidecar_path',
                                  'sidecar_sha256','row_count')}
        if not source['parquet_path'].startswith(f'data/causally_gated_normalized/ohlcv_1m/ES/{year}/'):
            raise IntegrityError('Unexpected annual source location')
        source['bytes']=(root/source['parquet_path']).stat().st_size
        sources.append(source)
    paths=[CATALOG, (BASE/'DECLARATION.md').as_posix(),
           'CURRENT_WORKFLOW.md',
           'configs/active_annual_foundation_independence.json',
           'configs/active_data_view_contract.json',
           *('src/futures_rebuild/'+p for p in CODE),
           *(p.as_posix() for p in sorted(Path(root/'state/discovery_history').glob('[0-9]*.json')))]
    # root may be absolute; bindings remain repository-relative.
    paths=[Path(p).relative_to(root).as_posix() if Path(p).is_absolute() else p for p in paths]
    return bind(dict(operation=OPERATION,claim='ES_MEAN_REVERSION_INPUT_LABEL_AVAILABILITY_ONLY',
        bindings={p:sha256_file(root/p) for p in paths},sources=sources,
        raw_rows=sum(s['row_count'] for s in sources), input_bytes=sum(s['bytes'] for s in sources),
        dates=list(weekday_dates()), folds=frozen_ranges(weekday_dates()),
        output='state/unpublished_evidence/mean_reversion_baseline_v1/coverage_v3',
        limits=dict(seconds=1800,peak_working_set_bytes=3*2**30,output_bytes=32*2**20,
                    free_reserve_bytes=64*2**30,files=1400,batch_rows=4096),
        fitting=False, label_statistics=False,protected_access=False))


def scope(plan):
    return dict(purpose=plan['claim'],mean_reversion_readiness_plan_id=plan['plan_id'])


def summarize(records, folds):
    """Counts only; preserve fixed denominators and overlapping refusal reasons."""
    summaries=[]
    for h in m.HORIZONS:
        selected=[r for r in records if r['horizon']==h]
        for year in range(2018,2023):
            subset=[r for r in selected if r['day'].startswith(str(year))]
            reasons=Counter();hours=Counter();volatility=Counter()
            feature_hours=Counter();feature_volatility=Counter()
            for r in subset:
                reasons.update(r['reasons']);hours.update(r['events_by_hour']);volatility.update(r['events_by_volatility'])
                feature_hours.update(r['feature_events_by_hour']);feature_volatility.update(r['feature_events_by_volatility'])
            totals={k:sum(r[k] for r in subset) for k in ('candidates','observed_signal_bars',
                'observations_outside_signal_clock','feature_valid','label_valid','eligible_events',
                'long_events','short_events','feature_long_events','feature_short_events','unique_refused')}
            summaries.append(dict(horizon=h,year=year,**totals,reasons=dict(reasons),
                retained_fraction=totals['label_valid']/totals['candidates'] if totals['candidates'] else None,
                event_fraction=totals['eligible_events']/totals['candidates'] if totals['candidates'] else None,
                higher_timeframe_losses=0,deduplication_losses=0,
                events_by_hour=dict(hours),events_by_volatility=dict(volatility),
                feature_events_by_hour=dict(feature_hours),feature_events_by_volatility=dict(feature_volatility)))
    fold_counts=[]
    for n,fold in enumerate(folds):
        days=set(fold['outer']['audit_dates'])
        for h in m.HORIZONS:
            subset=[r for r in records if r['horizon']==h and r['day'] in days]
            r=dict(fold=n,horizon=h,label_dates=sum(x['label_valid']>0 for x in subset))
            for side in ('long','short'):
                r[side+'_events']=sum(x[side+'_events'] for x in subset)
                r[side+'_dates']=sum(x[side+'_events']>0 for x in subset)
            r['support_floor_pass']=r['label_dates']>=40 and all(
                r[s+'_events']>=100 and r[s+'_dates']>=20 for s in ('long','short'))
            fold_counts.append(r)
    gaps=[]
    for h in m.HORIZONS:
        for field in ('label_valid','eligible_events'):
            best=[];run=[]
            for r in (x for x in records if x['horizon']==h):
                if not r[field]:
                    run.append(r['day'])
                    if len(run)>len(best):best=list(run)
                else:run=[]
            gaps.append(dict(horizon=h,measure=field,weekdays=len(best),first=best[0] if best else None,
                             last=best[-1] if best else None,
                             meaning='NO_USABLE_ROWS_NOT_PROOF_OF_MISSING_TRADES'))
    # Match the existing split helper: decisions before start minus one date,
    # and labels known strictly before the held-out date. Same-day horizons
    # never cross the boundary once these two conditions hold.
    all_dates=weekday_dates()
    date_positions={day:i for i,day in enumerate(all_dates)}
    training=[]
    for n,fold in enumerate(folds):
        windows=[('outer',fold['outer']),*[(f'inner_{i}',w) for i,w in enumerate(fold.get('inner',[]))]]
        for role,window in windows:
            first=window['first_date']; cutoff=window['start_index']-1
            for h in m.HORIZONS:
                counts=Counter();dates_with_labels=0
                for record in records:
                    if record['horizon']!=h or date_positions[record['day']]>=cutoff:
                        continue
                    matured=Counter()
                    for known,values in record['maturity_by_date'].items():
                        if known < first:
                            matured.update(values)
                    counts.update(matured)
                    dates_with_labels+=matured['label_valid']>0
                training.append(dict(fold=n,role=role,horizon=h,heldout_first_date=first,
                    training_label_dates=dates_with_labels,label_valid=counts['label_valid'],
                    long_events=counts['long_events'],short_events=counts['short_events'],
                    embargo_dates=1,maturity_rule='KNOWN_DATE_STRICTLY_BEFORE_HELDOUT'))
    return dict(year_profiles=summaries,fold_profiles=fold_counts,training_profiles=training,largest_event_gaps=gaps,
                all_support_floors_pass=all(r['support_floor_pass'] for r in fold_counts))


def execute_once(root, plan, receipt):
    """Approved task only; real receipt before resolver, hashes or observation reads."""
    root=Path(root).resolve()
    if type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation('Real readiness receipt required')
    if plan != prepare(root): raise IntegrityError('Prepared bindings changed')
    boundary=RepoBoundary(root)
    requested={k:v for k,v in receipt.scope if k not in ('approval_command','approval_plan_id','approval_plan_sha256')}
    if requested!=scope(plan): raise UnauthorizedOperation('Receipt scope differs')
    required=dict(receipt.scope)
    receipt.verify(boundary,operation=OPERATION,classification=CLASSIFICATION,required_scope=required)
    output=root/plan['output']
    if output.exists(): raise UnauthorizedOperation('Attempt already exists; no automatic retry')
    if shutil.disk_usage(root).free<plan['limits']['free_reserve_bytes']+plan['limits']['output_bytes']:
        raise IntegrityError('Free-space reserve unavailable')
    start=time.monotonic(); written=files=0
    limits=plan['limits']
    def check():
        if time.monotonic()-start>limits['seconds']: raise IntegrityError('Readiness deadline exceeded')
        if _working_set()>limits['peak_working_set_bytes']: raise IntegrityError('Readiness memory exceeded')
        if shutil.disk_usage(root).free<limits['free_reserve_bytes']: raise IntegrityError('Free-space reserve breached')
    def save(name,value,terminal=False):
        nonlocal written,files
        payload=canonical_bytes(value)+b'\n'
        if files+1>limits['files'] or written+len(payload)>limits['output_bytes']-(0 if terminal else 65536):
            raise IntegrityError('Readiness output budget exceeded')
        with (output/name).open('xb') as f: f.write(payload)
        files+=1;written+=len(payload)
    with FileLease(root/'state/locks/mean_reversion_readiness.lock'):
        output.mkdir(parents=True,exist_ok=False)
        try:
            receipt.consume(boundary,operation=OPERATION,classification=CLASSIFICATION,required_scope=required)
            save('started.json',dict(plan_id=plan['plan_id'],receipt_id=receipt.receipt_id))
            from .active_data_view import resolve
            from .causal_observation_parquet import _decode_physical
            import pyarrow.parquet as pq
            def rows():
                for source in plan['sources']:
                    check()
                    receipt.assert_consumed(boundary,operation=OPERATION,classification=CLASSIFICATION,required_scope=required)
                    path=resolve(repository_root=root,market='ES',year=source['year'],purpose='FEATURE_GENERATION')
                    if (io_path(path).resolve()!=io_path(root/source['parquet_path']).resolve()
                            or sha256_file(path)!=source['parquet_sha256']
                            or sha256_file(root/source['sidecar_path'])!=source['sidecar_sha256']):
                        raise IntegrityError('Resolved source differs')
                    count=0
                    with pq.ParquetFile(path) as pf:
                        for batch in pf.iter_batches(batch_size=limits['batch_rows']):
                            check()
                            for physical in batch.to_pylist():
                                row=_decode_physical('observations',physical);count+=1
                                stamp=datetime.fromtimestamp(row['bar_start_ns']/m.NS,timezone.utc)
                                if stamp.year!=source['year'] or row['market']!='ES':
                                    raise IntegrityError('Source escaped declared year/market')
                                yield row
                    if count!=source['row_count'] or sha256_file(path)!=source['parquet_sha256']:
                        raise IntegrityError('Source count/hash changed')
            groups=iter(groupby(rows(),key=lambda r:datetime.fromtimestamp(r['bar_start_ns']/m.NS,m.NY).date().isoformat()))
            current=next(groups,None); completed=[]
            for day in plan['dates']:
                check()
                while current is not None and current[0]<day:
                    for _ in current[1]: pass
                    current=next(groups,None)
                selected=[]
                if current is not None and current[0]==day:
                    selected=list(current[1]);current=next(groups,None)
                records=m.coverage_day(day,selected,as_of_ns=AS_OF)
                save(day+'.json',records);completed.extend(records)
            while current is not None:
                for _ in current[1]: pass
                current=next(groups,None)
            if prepare(root)!=plan: raise IntegrityError('Bindings changed during census')
            check()
            save('coverage_summary.json',summarize(completed,plan['folds']))
            save('COMPLETE.json',dict(status='PASS_AVAILABILITY_CENSUS',plan_id=plan['plan_id'],
                day_horizon_records=len(completed),seconds=time.monotonic()-start,
                peak_working_set_bytes=_working_set(), prior_output_bytes=written,
                historical_coverage_only=True,training_ready=False),terminal=True)
        except Exception as exc:
            save('FAILED.json',dict(status='FAILED',error=str(exc),seconds=time.monotonic()-start,
                 peak_working_set_bytes=_working_set(),prior_output_bytes=written),terminal=True)
            raise
    return output
