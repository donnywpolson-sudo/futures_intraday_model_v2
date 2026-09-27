"""Inactive daily candidate lifecycle. Synthetic execution only until separate adoption.

No CLI, publication, activation, network calls, or historical authority issuance.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, fields, replace
from datetime import datetime, timezone
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
import json
import os
import sys

from .canonical import canonical_bytes, sha256_json, sha256_file, io_path
from .causal_observation_foundation import V10_CHECKPOINT_MARKETS, ECONOMICS_RULEBOOK_SHA256
from .causal_timeframe_storage import (Limits, Budget, MonthlyWriter, contained, signed,
                                      read_record, file_inventory, iter_bundle_groups, table_metadata)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .foundation.records import ProviderBar, ProviderDefinition

SECOND=1_000_000_000
DAY=86_400*SECOND
CUTOFF=1_752_444_000*SECOND  # 2025-07-13T22:00:00Z
CONTRACT = {'schema':'causal_timeframe_contract/1', 'timeframes':{'1d':DAY,'1h':3600*SECOND},
    'availability_lag_ns':5*SECOND, 'availability_basis':'ASSUMED_NOT_MEASURED_DELIVERY',
    'definition_identity':'RECEIVED_BY_BAR_START_DAILY_INSTRUMENT_ID',
    'storage':'MONTHLY_FIVE_TABLES_ALIGNED_UTC_DAY_GROUPS',
    'active':'MANIFEST_REFERENCES_NO_PAYLOAD_COPY', 'execution':'EXACT_RECEIPT_REQUIRED_FOR_HISTORY',
    'daily_semantics':'PROVIDER_UTC_DAY_NOT_EXCHANGE_SESSION',
    'reconciliation':'NATIVE_1H_ALL_24_SAME_INSTRUMENT_ONLY',
    'permitted_uses':['LINEAGE_AUDIT'], 'markets':list(V10_CHECKPOINT_MARKETS),
    'availability_scope':'EXPLICIT_RELEASE_CUTOFF_SEPARATE_FROM_STORAGE_RANGE',
    'definition_unavailable':'QUARANTINE_IN_MONTHLY_COVERAGE_NO_OBSERVATION',
    'development_end_exclusive_ns':CUTOFF}
CONTRACT_ID=sha256_json(CONTRACT)
ENVIRONMENT={'python':sys.version,'packages':{name:version(name) for name in
    ('pyarrow','databento','databento-dbn','numpy','tzdata')}}


def implementation_binding():
    here=Path(__file__).parent
    names=('causal_1d.py','causal_1d_transform.py','causal_1d_replay.py','causal_1d_sources.py','daily_quality_reader.py',
           'causal_timeframe_storage.py','causal_timeframe_execution.py','causal_1d_native.py',
           'active_data_view.py','boundary.py','causal_source_closure.py',
           'foundation_operation_firewall.py','durable_windows_task_transport.py','causal_observation_canary.py',
           'causal_observation_market_certification.py','causal_observation_parquet.py',
           'foundation/decoder.py','foundation/records.py','foundation/identity.py',
           'foundation/economics.py','foundation/snapshot.py','source_symbology.py',
           'causal_observation_foundation.py','time_contracts.py','locking.py','canonical.py')
    return {name:sha256_file(here/name) for name in names}


@dataclass(frozen=True)
class Spec:
    market: str
    start_ns: int
    end_ns: int
    source_contract_id: str
    source_release_id: str
    source_manifest_id: str
    timeframe: str='1d'
    synthetic_only: bool=True
    availability_end_ns: int | None=None

    def __post_init__(self):
        if self.market not in CONTRACT['markets'] or self.timeframe != '1d' or type(self.synthetic_only) is not bool:
            raise UnauthorizedOperation('explicit standard-market 1d specification required')
        if (type(self.start_ns) is not int or type(self.end_ns) is not int
                or not 0 < self.start_ns < self.end_ns <= CUTOFF
                or self.start_ns % SECOND or self.end_ns % SECOND):
            raise ContractError('invalid development window')
        if self.availability_end_ns is None:
            object.__setattr__(self,'availability_end_ns',self.end_ns)
        if (type(self.availability_end_ns) is not int
                or not self.end_ns <= self.availability_end_ns <= CUTOFF
                or self.availability_end_ns % SECOND):
            raise ContractError('invalid exclusive daily availability cutoff')
        for value in (self.source_contract_id,self.source_release_id,self.source_manifest_id):
            if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
                raise ContractError('exact source identities required')

    @property
    def identity(self):
        return {'spec':asdict(self),'contract_id':CONTRACT_ID,
                           'economics':ECONOMICS_RULEBOOK_SHA256,'implementation':implementation_binding(),
                           'environment':ENVIRONMENT}

    @property
    def id(self):
        return sha256_json(self.identity)


@dataclass(frozen=True)
class DayInput:
    day_ns: int
    definitions: tuple
    bars: tuple
    references: dict
    support: tuple=()


def stamp(ns):
    return datetime.fromtimestamp(ns//SECOND,tz=timezone.utc)


def validate_day(day, spec, limits):
    if type(day) is not DayInput or type(day.day_ns) is not int or day.day_ns % DAY:
        raise IntegrityError('exact ordered UTC day required')
    if not spec.start_ns//DAY*DAY <= day.day_ns < spec.end_ns:
        raise IntegrityError('day outside declared coverage')
    if set(day.references) != {'1h'}:
        raise IntegrityError('reference families differ')
    total=len(day.bars)+len(day.definitions)+len(day.support)+sum(len(v) for v in day.references.values())
    if total > limits.maximum_day_source_rows or len(day.bars)>1:
        raise IntegrityError('daily source-state bound exceeded')
    last=-1
    for b in day.bars:
        if (type(b) is not ProviderBar or not day.day_ns <= b.event_at_ns < day.day_ns+DAY
                or b.event_at_ns % DAY or b.event_at_ns <= last):
            raise IntegrityError('primary daily order, alignment, or duplicate error')
        last=b.event_at_ns
    for cadence, rows in day.references.items():
        duration={'1m':60*SECOND,'1h':3600*SECOND,'1d':DAY}[cadence]
        if len({b.event_at_ns for b in rows}) != len(rows):
            raise IntegrityError('duplicate reference interval')
        for b in rows:
            if (type(b) is not ProviderBar or b.event_at_ns % duration
                    or not day.day_ns <= b.event_at_ns < day.day_ns+DAY):
                raise IntegrityError('reference boundary differs')
    for b in (*day.bars,*(b for rows in day.references.values() for b in rows),*day.definitions):
        if (b.market != spec.market or b.source_release_id != spec.source_release_id
                or b.source_manifest_sha256 != spec.source_contract_id
                or not b.source_file_path.startswith('synthetic/' if spec.synthetic_only else 'data/dbn/')):
            raise UnauthorizedOperation('source lineage or execution mode differs')
    for d in day.definitions:
        if type(d) is not ProviderDefinition or d.instrument_id_date_utc != stamp(day.day_ns).date().isoformat():
            raise IntegrityError('definition instrument index date differs')
    if day.support:
        raise UnauthorizedOperation('daily support families are not declared')


def decoded(day, spec):
    bars=tuple(b for b in day.bars if spec.start_ns <= b.event_at_ns < spec.end_ns
               and b.event_at_ns+DAY+5*SECOND < spec.availability_end_ns)
    return SimpleNamespace(definitions=day.definitions,primary_1d=bars,
        reference_1h=tuple(day.references['1h']))


def add_edges(tables, market, start, end):
    rows=tables['observations']
    for a,b in ((start,rows[0]['bar_start_ns']),(rows[-1]['bar_end_ns'],end)):
        if a>=b: continue
        core={'observation_row_id':None,'market':market,'interval_start_ns':a,'interval_end_ns':b,
              'state':'UNKNOWN_FAIL_CLOSED','authority':'DECLARED_WINDOW_NO_SCHEDULE_AUTHORITY',
              'evidence_sha256':sha256_json({'start_ns':a,'end_ns':b,'basis':'NO_OBSERVATIONS'})}
        tables['missingness'].append({'evidence_id':sha256_json(core),**core})


def produce_day(day,spec,rulebook,prior):
    from .causal_1d_transform import build_day
    d=decoded(day,spec)
    if not d.primary_1d: return None,prior
    start,end=max(spec.start_ns,day.day_ns),min(spec.end_ns,day.day_ns+DAY)
    tables,last=build_day(context=SimpleNamespace(source_contract_id=spec.source_contract_id,
        source_release_id=spec.source_release_id), market=spec.market,
        window={'start_ns':start,'end_ns':end},decoded=d,allowed_roots=frozenset(V10_CHECKPOINT_MARKETS),
        economics_rulebook=rulebook,prior_observation=prior)
    add_edges(tables,spec.market,start,end)
    return tables,last


def reconstruct(day,spec,rulebook,prior):
    from .causal_1d_replay import reconstruct_day
    # Selection is recomputed here, not accepted from producer output.
    eligible=tuple(b for b in day.bars if spec.start_ns<=b.event_at_ns<spec.end_ns
                   and b.event_at_ns+DAY+5*SECOND < spec.availability_end_ns)
    if not eligible: return None,prior
    d=SimpleNamespace(definitions=day.definitions,primary_1d=eligible,
        reference_1h=tuple(day.references['1h']))
    start=max(day.day_ns,spec.start_ns)
    end=min(day.day_ns+DAY,spec.end_ns)
    tables,last=reconstruct_day(market=spec.market,decoded=d,start_ns=start,end_ns=end,
        source_contract_id=spec.source_contract_id,source_release_id=spec.source_release_id,
        rulebook=rulebook,prior=prior)
    # Independently create boundary evidence (no producer helper).
    bounds=[(start,tables['observations'][0]['bar_start_ns']),
            (tables['observations'][-1]['bar_end_ns'],end)]
    for left,right in bounds:
        if right>left:
            core=dict(observation_row_id=None,market=spec.market,interval_start_ns=left,interval_end_ns=right,
                state='UNKNOWN_FAIL_CLOSED',authority='DECLARED_WINDOW_NO_SCHEDULE_AUTHORITY',
                evidence_sha256=sha256_json(dict(start_ns=left,end_ns=right,basis='NO_OBSERVATIONS')))
            tables['missingness'].append(dict(evidence_id=sha256_json(core),**core))
    return tables,last


def day_digest(day):
    digest=sha256(canonical_bytes({'day_ns':day.day_ns})+b'\n')
    for family,rows in [('definition',day.definitions),('1d',day.bars),
                        *((c,day.references[c]) for c in ('1h',)),
                        ('support',day.support)]:
        digest.update(canonical_bytes({'family':family,'count':len(rows)})+b'\n')
        # These validated records contain only scalar fields. Avoid asdict's
        # recursive deepcopy while preserving its exact field set and JSON.
        names=tuple(f.name for f in fields(ProviderDefinition if family=='definition' else ProviderBar))
        for row in rows:
            digest.update(canonical_bytes(row if family=='support' else
                {name:getattr(row,name) for name in names})+b'\n')
    return digest.hexdigest()


def _days(factory,spec,limits,budget):
    expected=spec.start_ns//DAY*DAY
    for day in factory():
        budget.check()
        validate_day(day,spec,limits)
        if day.day_ns != expected: raise IntegrityError('day missing, duplicated, or out of order')
        yield day
        del day
        expected+=DAY
    if expected < spec.end_ns: raise IntegrityError('source factory ended before declared scope')


def _verify_seal(root,seal,spec_id,budget):
    if seal['spec_id'] != spec_id or seal['timeframe']!='1d':
        raise IntegrityError('checkpoint semantic identity differs')
    if seal['files']:
        directory=contained(root,seal['directory'])
        if file_inventory(directory,budget) != seal['files']:
            raise IntegrityError('sealed monthly file changed')
        if table_metadata(directory)!=seal['table_metadata']:
            raise IntegrityError('sealed table metadata differs')
    elif seal['table_metadata']:
        raise IntegrityError('empty seal has table metadata')


def _build_candidate(root:Path,spec:Spec,source_factory,rulebook,limits:Limits,*,resume=False,budget):
    """Build only synthetic candidates. Resume verifies every sealed input day."""
    root=Path(root)
    root.mkdir(parents=True,exist_ok=True)
    identity=spec.id
    start=signed({'schema':'causal_1d_attempt/1','spec_id':identity,'contract_id':CONTRACT_ID,'spec':asdict(spec)})
    marker=root/'attempt.json'
    if marker.exists():
        if not resume or read_record(marker)!=start: raise IntegrityError('attempt already exists or differs')
    else: budget.save(marker,start)
    prior=None
    current=None
    writer=None
    months=[]
    seals=[]
    coverage=[]
    input_days=[]
    incoming=None
    reused=None

    def finish():
        nonlocal writer
        if current is None: return
        if reused:
            if input_days != reused['input_days'] or coverage != reused['coverage'] or prior != reused['last_observation']:
                raise IntegrityError('reused month input/state differs')
            seals.append(reused)
            return
        files={}
        directory=f'{spec.market}/{current[:4]}/1d/{current}/candidate'
        if writer:
            writer.close()
            writer=None
            files=file_inventory(contained(root,directory),budget)
        seal=signed({'schema':'causal_1d_month_seal/1','spec_id':identity,'timeframe':'1d',
            'month':current,'directory':directory,'files':files,'input_days':list(input_days),
            'table_metadata':table_metadata(contained(root,directory)) if files else {},
            'coverage':list(coverage),'incoming_state_id':sha256_json(incoming),'last_observation':prior})
        budget.save(root/f'{current}.seal.json',seal)
        seals.append(seal)

    try:
        for day in _days(source_factory,spec,limits,budget):
            month=stamp(day.day_ns).strftime('%Y-%m')
            if month!=current:
                finish()
                current=month
                months.append(month)
                coverage=[]
                input_days=[]
                incoming=prior
                sealpath=root/f'{month}.seal.json'
                reused=read_record(sealpath) if resume and sealpath.exists() else None
                if reused:
                    _verify_seal(root,reused,identity,budget)
                    if reused['incoming_state_id']!=sha256_json(incoming): raise IntegrityError('checkpoint prior state differs')
                elif resume:
                    relative=f'{spec.market}/{current[:4]}/1d/{current}/candidate'
                    unfinished=contained(root,relative)
                    if unfinished.exists():
                        from .causal_observation_parquet import FILENAMES
                        if any(not p.is_file() or p.name not in FILENAMES.values() for p in unfinished.iterdir()):
                            raise IntegrityError('unowned files in interrupted month')
                        archive=contained(root,f'interrupted/{current}/{budget.attempt_number}')
                        archive.parent.mkdir(parents=True,exist_ok=True)
                        os.replace(unfinished,archive)
            quarantine=[]
            try:
                tables,prior=produce_day(day,spec,rulebook,prior)
            except (ContractError,IntegrityError) as exc:
                if type(exc) is ContractError and str(exc)=='bar instrument has no same-day definition received by bar start':
                    from .causal_1d_transform import unavailable_definition
                    quarantine=[unavailable_definition(day)]
                    tables=None
                else:
                    # Preserve the exact failed day for source investigation; never
                    # weaken the identity gate or skip a source defect silently.
                    budget.save(root/f'{day.day_ns}.{budget.attempt_number}.source_failure.json',signed({
                        'market':spec.market,'day_ns':day.day_ns,'spec_id':identity,
                        'error_type':type(exc).__name__,'message':str(exc),
                        'source_rows':[{'row_sha256':b.row_sha256,'source_path':b.source_file_path,
                            'source_sha256':b.source_file_sha256,'publisher_id':b.publisher_id,
                            'instrument_id':b.instrument_id,'event_at_ns':b.event_at_ns} for b in day.bars],
                        'matching_day_definitions':[{'row_sha256':d.row_sha256,
                            'source_path':d.source_file_path,'source_sha256':d.source_file_sha256,
                            'publisher_id':d.publisher_id,'instrument_id':d.instrument_id,
                            'instrument_id_date_utc':d.instrument_id_date_utc,'raw_symbol':d.raw_symbol,
                            'ts_recv_ns':d.ts_recv_ns,'ts_event_ns':d.ts_event_ns,
                            'activation_ns':d.activation_ns,'expiration_ns':d.expiration_ns,
                            'security_update_action':d.security_update_action} for d in day.definitions
                            if any((d.publisher_id,d.instrument_id)==(b.publisher_id,b.instrument_id) for b in day.bars)]}))
                    raise
            input_days.append({'day_ns':day.day_ns,'sha256':day_digest(day)})
            count=0 if tables is None else len(tables['observations'])
            coverage.append({'day_ns':day.day_ns,'start_ns':max(spec.start_ns,day.day_ns),
                'end_ns':min(spec.end_ns,day.day_ns+DAY),'observations':count,
                'source_primary_rows':len(day.bars),
                'excluded_start':sum(b.event_at_ns<spec.start_ns for b in day.bars),
                'quarantined':len(quarantine),'quarantine':quarantine,
                'state':('DEFINITION_NOT_KNOWN_AT_BAR_START' if quarantine else
                         'OBSERVED_WITH_EXPLICIT_GAPS' if count else
                         'EXCLUDED_AVAILABILITY_OR_START' if day.bars else 'SOURCE_UNAVAILABLE'),
                'comparison_counts':{k:sum(r['result']==k for r in (tables or {}).get('cadence',()))
                                     for k in ('MATCH','DISAGREEMENT','NOT_COMPARABLE','SOURCE_MISSING')},
                'excluded_availability':sum(spec.start_ns<=b.event_at_ns<spec.end_ns and b.event_at_ns+DAY+5*SECOND>=spec.availability_end_ns for b in day.bars)})
            covered=coverage[-1]
            if covered['source_primary_rows'] != sum(covered[k] for k in ('observations','quarantined','excluded_start','excluded_availability')):
                raise IntegrityError('daily source accounting differs')
            if tables is not None and not reused:
                if writer is None:
                    relative=f'{spec.market}/{current[:4]}/1d/{current}/candidate'
                    writer=MonthlyWriter(contained(root,relative),budget)
                writer.write(tables)
            # Release this day's logical rows before decoding/reconstructing the next.
            del tables,day
        finish()
        result=signed({'schema':'causal_1d_candidate/1','status':('SYNTHETIC_CANDIDATE_NOT_ACTIVE' if spec.synthetic_only else 'HISTORICAL_CANDIDATE_NOT_ACTIVE'),
            'spec_id':identity,'spec':asdict(spec),'contract_id':CONTRACT_ID,
            'identity':spec.identity,
            'provenance':{'contract':CONTRACT,'implementation':implementation_binding(),'environment':ENVIRONMENT},
            'source_inventory':list(getattr(source_factory,'entries',())),
            'operation':getattr(source_factory,'op',None),
            'permitted_uses':['SYNTHETIC_TEST'] if spec.synthetic_only else ['LINEAGE_AUDIT'],
            'seals':[s['id'] for s in seals], 'months':months,
            'observations':sum(d['observations'] for s in seals for d in s['coverage']),
            'source_accounting':{k:sum(d[k] for s in seals for d in s['coverage']) for k in
                ('source_primary_rows','observations','quarantined','excluded_start','excluded_availability')},
            'comparison_counts':{k:sum(d['comparison_counts'][k] for s in seals for d in s['coverage'])
                for k in ('MATCH','DISAGREEMENT','NOT_COMPARABLE','SOURCE_MISSING')}})
        output=root/'candidate.json'
        if output.exists():
            if read_record(output)!=result: raise IntegrityError('existing candidate differs')
        else: budget.save(output,result)
        return result,budget.record()
    except BaseException:
        # Incomplete files remain task-owned evidence, never silently deleted/reused.
        if writer:
            try: writer.close()
            except BaseException: pass
        raise


def _certify_candidate(root:Path,spec:Spec,source_factory,rulebook,limits:Limits,*,budget):
    """Two full independent reconstructions; comparison reads produce no payloads."""
    root=Path(root)
    candidate=read_record(root/'candidate.json')
    if candidate['spec_id']!=spec.id: raise IntegrityError('candidate implementation/source drift')
    passes=[]
    for _ in range(2):
        prior=None
        current=None
        iterator=None
        digest=sha256()
        rows=0
        source_accounting=dict.fromkeys(('source_primary_rows','observations','quarantined','excluded_start','excluded_availability'),0)
        comparison_counts=dict.fromkeys(('MATCH','DISAGREEMENT','NOT_COMPARABLE','SOURCE_MISSING'),0)
        months=[]
        seal=None
        day_index=0
        def close_month():
            if seal is not None:
                if day_index!=len(seal['coverage']) or prior!=seal['last_observation']:
                    raise IntegrityError('month coverage or state differs')
                if iterator is not None and next(iterator,None) is not None:
                    raise IntegrityError('extra persisted daily group')
        try:
            for day in _days(source_factory,spec,limits,budget):
                month=stamp(day.day_ns).strftime('%Y-%m')
                if month!=current:
                    close_month()
                    current=month
                    months.append(month)
                    seal=read_record(root/f'{month}.seal.json')
                    _verify_seal(root,seal,spec.id,budget)
                    if seal['id']!=candidate['seals'][len(months)-1] or seal['incoming_state_id']!=sha256_json(prior):
                        raise IntegrityError('seal set or prior differs')
                    iterator=iter_bundle_groups(contained(root,seal['directory']),maximum_group_bytes=limits.maximum_group_bytes) if seal['files'] else None
                    day_index=0
                if seal['input_days'][day_index] != {'day_ns':day.day_ns,'sha256':day_digest(day)}:
                    raise IntegrityError('independent source-day identity differs')
                quarantine=[]
                try:
                    expected,prior=reconstruct(day,spec,rulebook,prior)
                except ContractError as exc:
                    if type(exc) is not ContractError or exc.args != ('bar instrument has no same-day definition received by bar start',):
                        raise
                    from .causal_1d_replay import unavailable_definition
                    quarantine=[unavailable_definition(day)]
                    expected=None
                count=0 if expected is None else len(expected['observations'])
                cover=seal['coverage'][day_index]
                expected_coverage={'day_ns':day.day_ns,'start_ns':max(spec.start_ns,day.day_ns),
                    'end_ns':min(spec.end_ns,day.day_ns+DAY),'observations':count,
                    'source_primary_rows':len(day.bars),
                    'excluded_start':sum(b.event_at_ns<spec.start_ns for b in day.bars),
                    'quarantined':len(quarantine),'quarantine':quarantine,
                'state':('DEFINITION_NOT_KNOWN_AT_BAR_START' if quarantine else
                         'OBSERVED_WITH_EXPLICIT_GAPS' if count else
                         'EXCLUDED_AVAILABILITY_OR_START' if day.bars else 'SOURCE_UNAVAILABLE'),
                'comparison_counts':{k:sum(r['result']==k for r in (expected or {}).get('cadence',()))
                                     for k in ('MATCH','DISAGREEMENT','NOT_COMPARABLE','SOURCE_MISSING')},
                    'excluded_availability':sum(spec.start_ns<=b.event_at_ns<spec.end_ns
                        and b.event_at_ns+DAY+5*SECOND>=spec.availability_end_ns for b in day.bars)}
                if expected_coverage['source_primary_rows'] != sum(expected_coverage[k] for k in ('observations','quarantined','excluded_start','excluded_availability')):
                    raise IntegrityError('independent source accounting differs')
                if cover!=expected_coverage:
                    raise IntegrityError('independent day coverage differs')
                for key in source_accounting:
                    source_accounting[key]+=expected_coverage[key]
                for key,comparison_count in expected_coverage['comparison_counts'].items():
                    comparison_counts[key]+=comparison_count
                day_index+=1
                if expected is not None:
                    actual=None if iterator is None else next(iterator,None)
                    if actual != expected: raise IntegrityError('independent logical-table reconstruction differs')
                    for name in sorted(expected):
                        for row in expected[name]: digest.update(canonical_bytes({'table':name,'row':row})+b'\n')
                    rows+=count
                    # The paused Parquet iterator still owns this dict until next().
                    # Clear it now so it cannot overlap the next reconstruction.
                    actual.clear()
                    del actual
                del expected,day
            close_month()
        finally:
            if iterator is not None: iterator.close()
        if months!=candidate['months'] or rows!=candidate['observations']:
            raise IntegrityError('complete candidate counts differ')
        if comparison_counts!=candidate['comparison_counts']:
            raise IntegrityError('candidate comparison counts differ')
        if source_accounting!=candidate['source_accounting']:
            raise IntegrityError('independent source accounting totals differ')
        passes.append({'source_accounting':source_accounting,'rows':rows,'logical_sha256':digest.hexdigest(),'months':months,
                       'comparison_counts':comparison_counts})
    if candidate['provenance']!={'contract':CONTRACT,'implementation':implementation_binding(),'environment':ENVIRONMENT}:
        raise IntegrityError('daily provenance differs')
    if candidate['identity']!=spec.identity:
        raise IntegrityError('daily frozen identity differs')
    if (candidate['source_inventory']!=list(getattr(source_factory,'entries',()))
            or candidate['operation']!=getattr(source_factory,'op',None)
            or candidate['permitted_uses']!=(['SYNTHETIC_TEST'] if spec.synthetic_only else ['LINEAGE_AUDIT'])):
        raise IntegrityError('daily source inventory, operation or access binding differs')
    if passes[0]!=passes[1]: raise IntegrityError('independent passes disagree')
    certificate=signed({'schema':'causal_1d_certificate/1','status':('SYNTHETIC_ONLY_NOT_HISTORICAL_CERTIFICATION' if spec.synthetic_only else 'HISTORICAL_RECONSTRUCTION_CERTIFIED_NOT_ACTIVE'),
        'candidate_id':candidate['id'],'spec_id':spec.id,'contract_id':CONTRACT_ID,
        'passes':passes,'authority':{'historical_read':False,'publication':False,'activation':False,'research':False}})
    path=root/'certificate.json'
    if path.exists():
        if read_record(path)!=certificate: raise IntegrityError('existing certificate differs')
    else: budget.save(path,certificate)
    return certificate,budget.record()


def _operation(kind,root,spec,factory,rulebook,limits,*,resume=False):
    """One writer and durable accounting; interrupted runs consume their write cap."""
    root=Path(root)
    if not spec.synthetic_only:
        from .causal_1d_native import HistoricalDbnSources
        if type(factory) is not HistoricalDbnSources:
            raise UnauthorizedOperation('receipt-bound historical source required')
        factory.authorize_output(root,spec,limits,resume,kind)
    from .foundation.economics import EconomicsRuleBook
    rulebook_path=Path(__file__).resolve().parents[2]/'configs/contract_economics_rules.json'
    if (sha256_file(rulebook_path)!=ECONOMICS_RULEBOOK_SHA256
            or rulebook!=EconomicsRuleBook.from_file(rulebook_path)):
        raise IntegrityError('economics rulebook differs from the bound contract')
    from .canonical import assert_no_linklike_ancestors
    assert_no_linklike_ancestors(root)
    root.mkdir(parents=True,exist_ok=True)
    lease=FileLease(io_path(root/'operation.lock'))
    # Check before the lock write; charge its exact serialized size below.
    Budget(root,limits).check(65536)
    with lease:
        lock_bytes=io_path(root/'operation.lock').stat().st_size
        runs=contained(root,'runs')
        runs.mkdir(exist_ok=True)
        starts=sorted(runs.glob('*.start.json'))
        previous=0
        for path in starts:
            old=read_record(path)
            finish=path.with_name(path.name.replace('.start.json','.finish.json'))
            if finish.exists():
                end=read_record(finish)
                previous+=end['written_before_finish']+finish.stat().st_size
            else:
                previous+=old['maximum_written_bytes']
        if previous>=limits.maximum_written_bytes:
            raise IntegrityError('prior attempts exhausted cumulative write budget')
        budget=Budget(root,replace(limits,maximum_written_bytes=limits.maximum_written_bytes-previous))
        budget.written=lock_bytes
        budget.files=1
        budget.terminal_reserve=65536
        number=len(starts)+1
        budget.attempt_number=number
        budget.save(runs/f'{number:06d}.start.json',signed({'kind':kind,'spec_id':spec.id,
            'maximum_written_bytes':budget.limits.maximum_written_bytes,'limits':asdict(limits)}))
        old_check=getattr(factory,'check',None)
        def combined_check():
            if old_check is not None: old_check()
            budget.check()
        if old_check is not None: factory.check=combined_check
        status='FAILED'
        result=None
        try:
            if kind=='BUILD':
                result,_=_build_candidate(root,spec,factory,rulebook,limits,resume=resume,budget=budget)
            else:
                result,_=_certify_candidate(root,spec,factory,rulebook,limits,budget=budget)
            budget.check()
            status='COMPLETE'
        finally:
            if old_check is not None: factory.check=old_check
            budget.finalizing=True
            budget.terminal_reserve=0
            payload=signed({'kind':kind,'status':status,'spec_id':spec.id,
                'result_id':None if result is None else result['id'],
                'written_before_finish':budget.written,'previous_attempt_written_bytes':previous,
                'resources':budget.record(),'source_decoded_rows_last_pass':getattr(factory,'decoded_rows',None),
                'source_file_bytes_last_pass':getattr(factory,'source_bytes_per_pass',None)})
            budget.save(runs/f'{number:06d}.finish.json',payload)
        return result,{**budget.record(),'cumulative_application_written_bytes':previous+budget.written}


def build_candidate(root:Path,spec:Spec,source_factory,rulebook,limits:Limits,*,resume=False):
    return _operation('BUILD',root,spec,source_factory,rulebook,limits,resume=resume)


def certify_candidate(root:Path,spec:Spec,source_factory,rulebook,limits:Limits):
    return _operation('CERTIFY',root,spec,source_factory,rulebook,limits)
