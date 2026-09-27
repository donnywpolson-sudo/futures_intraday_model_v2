"""Exact synthetic DBN/sidecar reader using the existing offline decoder.

No real-data adapter is adopted by this software-only change.
"""
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path

from .canonical import sha256_json
from .causal_1h import DayInput, DAY, SECOND, CUTOFF
from .causal_timeframe_storage import contained
from .causal_observation_canary import _query_contract
from .foundation.decoder import iter_bars, iter_definitions, iter_statuses, iter_statistics
from .foundation.snapshot import DbnReleaseFile
from .errors import IntegrityError, UnauthorizedOperation, ContractError

FAMILIES=('definition','ohlcv_1h','ohlcv_1m','status','statistics')


def timestamp(value):
    if type(value) is not str or '.' in value:
        raise ContractError('physical source boundaries must be exact whole seconds')
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())*SECOND


class SyntheticDbnSources:
    def __init__(self,root:Path,spec,entries,limits,*,batch_rows=4096,check=lambda:None):
        if type(batch_rows) is not int or not 1<=batch_rows<=limits.maximum_day_source_rows:
            raise ContractError('decoder batch exceeds the declared bounded source state')
        self.root,self.spec,self.entries,self.limits=root,spec,tuple(entries),limits
        self.batch_rows,self.check=batch_rows,check
        self.decoded_rows=self.source_bytes_per_pass=0

    def _validated(self):
        self._admit()
        if sha256_json(self.entries)!=self.spec.source_manifest_id:
            raise UnauthorizedOperation('exact synthetic inventory required')
        pairs={}
        for e in self.entries:
            if (not e['path'].startswith('synthetic/' if self.spec.synthetic_only else 'data/dbn/') or e['family'] not in FAMILIES
                    or e['market']!=self.spec.market or e['kind'] not in ('DBN','SIDECAR')):
                raise UnauthorizedOperation('undeclared/synthetic source family or path')
            contained(self.root,e['path'])
            key=e['path'].removesuffix('.manifest.json')
            pair=pairs.setdefault(key,{})
            if e['kind'] in pair: raise IntegrityError('duplicate inventory entry')
            pair[e['kind']]=e
        result={f:[] for f in FAMILIES}
        for key,pair in sorted(pairs.items()):
            if set(pair)!={'DBN','SIDECAR'}: raise IntegrityError('DBN/sidecar pairing incomplete')
            dbn,side=pair['DBN'],pair['SIDECAR']
            if side['family']!=dbn['family'] or side['path']!=dbn['path']+'.manifest.json':
                raise IntegrityError('sidecar family/path differs')
            query=_query_contract(self.root,{**side,'paired_dbn_sha256':dbn['sha256'],
                                             'paired_dbn_size_bytes':dbn['size_bytes']})
            start,end=timestamp(query['start']),timestamp(query['end'])
            if (end>CUTOFF or start>=end or query['schema']!=dbn['family'].replace('_','-')
                    or query['market']!=self.spec.market):
                raise UnauthorizedOperation('protected source or schema substitution refused before DBN open')
            binding=DbnReleaseFile(dbn['path'],contained(self.root,dbn['path']),dbn['path'],dbn['size_bytes'],
                dbn['sha256'],self.spec.source_release_id,self.spec.source_contract_id,self.spec.source_manifest_id)
            result[dbn['family']].append((start,end,binding,query))
        if any(not result[f] for f in ('definition','ohlcv_1h','ohlcv_1m')):
            raise IntegrityError('definition, primary 1s, and native 1m reference required')
        for units in result.values():
            units.sort(key=lambda u:u[0])
            if any(a[1]>b[0] for a,b in zip(units,units[1:])):
                raise IntegrityError('overlapping source intervals')
        return result

    def _admit(self):
        if not self.spec.synthetic_only:
            raise UnauthorizedOperation('synthetic adapter cannot read historical inputs')

    def _records(self,family,units):
        previous=None
        for _,_,binding,query in units:
            self.check()
            self.source_bytes_per_pass+=binding.size
            kwargs=dict(market=self.spec.market,expected_query_contract=query,batch_rows=self.batch_rows)
            if family.startswith('ohlcv_'):
                iterator=iter_bars(binding,schema=family.replace('_','-'),**kwargs)
            else:
                iterator={'definition':iter_definitions,'status':iter_statuses,'statistics':iter_statistics}[family](binding,**kwargs)
            try:
                for row in iterator:
                    self.decoded_rows+=1
                    if self.decoded_rows % self.batch_rows==0: self.check()
                    ts=row.event_at_ns if family.startswith('ohlcv_') else row.ts_recv_ns
                    if previous is not None and ts<previous: raise IntegrityError('source ordering regressed')
                    previous=ts
                    yield ts,row
            finally:
                iterator.close()

    def __call__(self):
        units=self._validated()  # validate every sidecar before opening any DBN
        self.decoded_rows=self.source_bytes_per_pass=0
        streams={f:iter(self._records(f,units[f])) for f in FAMILIES}
        heads={f:None for f in FAMILIES}
        try:
            for f in FAMILIES: heads[f]=next(streams[f],None)
            day=self.spec.start_ns//DAY*DAY
            while day<self.spec.end_ns:
                rows={f:[] for f in FAMILIES}
                count=0
                for f in FAMILIES:
                    while heads[f] is not None and heads[f][0]<day+DAY:
                        ts,row=heads[f]
                        if ts>=day:
                            count+=1
                            if count>self.limits.maximum_day_source_rows: raise IntegrityError('source day state ceiling')
                            rows[f].append(row)
                        heads[f]=next(streams[f],None)
                support=tuple((r.ts_event_ns,f,r.row_sha256) for f in ('status','statistics') for r in rows[f]
                              if day<=r.ts_event_ns<day+DAY)
                yield DayInput(day,tuple(rows['definition']),tuple(rows['ohlcv_1h']),
                    {c:tuple(rows['ohlcv_'+c]) for c in ('1m',)},support)
                day+=DAY
            # Exhaust each selected immutable file once; no annual reopen per month.
            for stream in streams.values():
                for _ in stream: pass
        finally:
            for stream in streams.values(): stream.close()
