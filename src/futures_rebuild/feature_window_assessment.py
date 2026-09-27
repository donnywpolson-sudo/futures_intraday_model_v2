"""Structural endpoint accounting on approved observation metadata, not samples.

No file reads, source admission or publication. Callers must supply certified
closures and quality/permission-labeled rows from a separately approved audit.
"""
from collections import deque
from datetime import datetime, timezone

import numpy as np

from .errors import IntegrityError
from .feature_window_reader import HOUR, DAY, LAG, IDENTITY
from .feature_calendar_feasibility import interval_partition


def range_maxima(values, starts):
    """Maxima on [starts[e], e), where starts are monotone; linear work."""
    result=np.zeros(len(starts),dtype=np.int64)
    queue=deque()
    previous=0
    for end,start in enumerate(starts):
        start=int(start)
        if not previous<=start<=end:
            raise IntegrityError('Nonmonotone endpoint search bounds')
        previous=start
        if end:
            i=end-1
            while queue and values[queue[-1]]<=values[i]:queue.pop()
            queue.append(i)
        while queue and queue[0]<start:queue.popleft()
        if queue:result[end]=values[queue[0]]
    return result


def closure_availability(closures, *, left, right, step):
    """Earliest complete-bar closure proof, including overlapping alternatives."""
    n=(right-left)//step
    unknown=np.iinfo(np.int64).max
    result=np.full(n,unknown,dtype=np.int64)
    # Intervals are certified CLOSED evidence only. Sort knowledge times once;
    # reusing the union parser avoids equating partial evidence with whole bars.
    known=sorted({x['available_at_ns'] for x in closures if x.get('available_at_ns') is not None})
    for asof in known:
        if asof<0:raise IntegrityError('Invalid closure availability')
        usable=[x for x in closures if x['available_at_ns'] is not None and x['available_at_ns']<=asof]
        # interval_partition requires query end <= as_of; filtering above retains
        # decision-relative knowledge while this pass partitions spatial coverage.
        parts=interval_partition(usable,start_ns=left,end_ns=right,as_of_ns=max(right,asof))
        unions=[]
        for p in parts:
            if p['state']!='CLOSED':continue
            if unions and unions[-1][1]==p['start_ns']:unions[-1][1]=p['end_ns']
            else:unions.append([p['start_ns'],p['end_ns']])
        for a,b in unions:
            lo=max(0,((a+step-1)//step*step-left)//step)
            hi=min(n,(b//step*step-left)//step)
            if lo<hi:result[lo:hi]=np.minimum(result[lo:hi],asof)
    return result


def compare_endpoints(*, market, timeframe, left, right, rows, permissions,
                      unavailable, closures, lengths, purposes):
    """Same exclusive endpoints [left,right); history never precedes left.

    Permissions span the minute ceiling, including closure gaps. Each observed
    row additionally supplies its actual source permitted_uses and quality flag.
    """
    step=HOUR if timeframe=='1h' else DAY if timeframe=='1d' else 0
    if not step or left%step or right%step or left>=right:
        raise IntegrityError('Invalid assessment grid')
    n=(right-left)//step
    ends=np.arange(n,dtype=np.int64)
    present=np.zeros(n,dtype=bool); excluded=np.zeros(n,dtype=bool)
    eligible=np.zeros(n,dtype=np.int64); identities=np.zeros(n,dtype=np.int32)
    groups={}; row_purposes={}
    for row in rows:
        start=row['bar_start_ns'];i=(start-left)//step
        if (not left<=start<right or start%step or present[i] or row['market']!=market
                or row['bar_end_ns']!=start+step or 'permitted_uses' not in row
                or 'quality_accepted' not in row):
            raise IntegrityError('Invalid or duplicate assessment observation')
        if row['available_at_ns']<row['bar_end_ns']+LAG or row['decision_eligible_at_ns']<row['available_at_ns']:
            raise IntegrityError('Invalid row availability')
        present[i]=True;excluded[i]=not row['quality_accepted'];eligible[i]=row['decision_eligible_at_ns']
        key=tuple(row[k] for k in IDENTITY);identities[i]=groups.setdefault(key,len(groups)+1)
        row_purposes[i]=set(row['permitted_uses'])
    positions=np.flatnonzero(present)
    transitions=np.zeros(n,dtype=bool)
    if len(positions)>1:transitions[positions[1:]]=identities[positions[1:]]!=identities[positions[:-1]]
    closed_known=closure_availability(closures,left=left,right=right,step=step)
    if np.any(present & (closed_known<np.iinfo(np.int64).max)):
        raise IntegrityError('Closure absence certificate conflicts with observed bar')
    closure_needed=np.where(present,0,closed_known)
    delivery_bad=np.zeros(n,dtype=bool)
    for a,b in unavailable:
        if a>=b:raise IntegrityError('Invalid unavailable interval')
        if b<=left or a>=right:continue
        lo=max(0,(a-left)//step);hi=min(n,(b-left+step-1)//step)
        delivery_bad[lo:hi]=True
    masks={p:np.zeros(n,dtype=bool) for p in purposes};covered=np.zeros(n,dtype=bool)
    for entry in permissions:
        a=max(left,entry['start_ns']);b=min(right,entry['end_ns'])
        lo=max(0,((a+step-1)//step*step-left)//step);hi=min(n,(b//step*step-left)//step)
        if lo>=hi:continue
        if covered[lo:hi].any():raise IntegrityError('Ambiguous permission ceiling')
        covered[lo:hi]=True
        for p in set(entry['permitted_uses']) & set(purposes):masks[p][lo:hi]=True
    for i,allowed in row_purposes.items():
        for p in purposes:masks[p][i] &= p in allowed
    datetimes=[datetime.fromtimestamp((left+int(e)*step)//10**9,timezone.utc) for e in ends]
    years=np.array([d.year for d in datetimes]);weekdays=np.array([d.weekday() for d in datetimes]);hours=np.array([d.hour for d in datetimes])
    asofs=left+ends*step+LAG
    def sums(values,starts,exclude_first=False):
        prefix=np.concatenate(([0],np.cumsum(values,dtype=np.int64)))
        lo=np.minimum(ends,starts+1) if exclude_first else starts
        return prefix[ends]-prefix[lo]
    records=[]
    for length in lengths:
        if type(length) is not int or length<=0:raise IntegrityError('Invalid horizon')
        count=np.searchsorted(positions,ends,side='left')
        last_ends=positions[np.maximum(0,count-1)]+1 if len(positions) else np.zeros(n,dtype=np.int64)
        ages=(ends-last_ends)*step
        native_starts=positions[np.maximum(0,count-length)] if len(positions) else np.zeros(n,dtype=np.int64)
        native_starts=np.where(count>=length,native_starts,0)
        strict_starts=np.maximum(0,ends-length)
        outcomes={}
        for method,starts,insufficient in [('strict_utc',strict_starts,ends<length),
                                           ('calendar_native_bars',native_starts,count<length)]:
            common=dict(insufficient_history=insufficient,
                missing_coverage=(sums(~present,starts)>0 if method=='strict_utc' else range_maxima(closure_needed,starts)>asofs),
                excluded_observation=sums(excluded,starts)>0,
                unavailable_delivery=sums(delivery_bad,starts)>0,
                contract_or_unit=sums(transitions,starts,True)>0,
                timing=range_maxima(eligible,starts)>asofs)
            for purpose in purposes:
                reasons=common|{'permission_or_cohort':sums(~masks[purpose],starts)>0}
                bad=np.logical_or.reduce(list(reasons.values()));good=~bad
                outcomes[method,purpose]=good
                spans=(ends-starts)*step
                for year in np.unique(years):
                    subset=years==year;selected=subset & good
                    records.append(dict(market=market,timeframe=timeframe,method=method,lookback_bars=length,
                        purpose=purpose,year=int(year),endpoints=int(subset.sum()),
                        readable_windows=int(selected.sum()),unique_refusals=int((subset & bad).sum()),
                        overlapping_reasons={k:int((subset & v).sum()) for k,v in reasons.items()},
                        elapsed_span_ns=None if not selected.any() else dict(min=int(spans[selected].min()),max=int(spans[selected].max()),mean=float(spans[selected].mean())),
                        final_observation_age_ns=None if not selected.any() else dict(min=int(ages[selected].min()),max=int(ages[selected].max()),mean=float(ages[selected].mean())),
                        readable_weekdays={str(d):int((selected & (weekdays==d)).sum()) for d in range(7)},
                        readable_utc_hours={str(h):int((selected & (hours==h)).sum()) for h in range(24)}))
        for purpose in purposes:
            gained=outcomes['calendar_native_bars',purpose] & ~outcomes['strict_utc',purpose]
            for record in records:
                if record['lookback_bars']==length and record['purpose']==purpose and record['method']=='calendar_native_bars':
                    record['newly_readable_from_certified_closures']=int((gained & (years==record['year'])).sum())
    return records
