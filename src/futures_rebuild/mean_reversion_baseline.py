"""ES minute-only preregistration mechanics. No file reader, fitting or execution.

Callers must supply admitted observations. Features and future labels have
separate functions; coverage returns availability counts, never effect estimates.
"""
from collections import Counter
from datetime import date, datetime, time
from statistics import fmean, pstdev
from zoneinfo import ZoneInfo

from .errors import IntegrityError

NS = 10**9
MINUTE = 60*NS
LAG = 5*NS
HORIZONS = (15, 30, 60)
NY = ZoneInfo('America/New_York')
IDENTITY = ('market', 'actual_contract', 'publisher_id', 'instrument_id',
            'currency', 'min_price_increment_nano', 'multiplier_nano')
FEATURES = ('displacement_z', 'momentum_5_ticks', 'volatility_60_ticks',
            'minutes_since_1000')


class Unavailable(IntegrityError):
    def __init__(self, *reasons):
        self.reasons = tuple(sorted(set(reasons)))
        super().__init__(', '.join(self.reasons))


def decision_times(day):
    day = date.fromisoformat(day)
    if not date(2018, 1, 1) <= day < date(2023, 1, 1):
        raise IntegrityError('Only declared 2018-2022 development dates')
    if day.weekday() >= 5:
        return ()
    left = int(datetime.combine(day, time(10), NY).timestamp())*NS
    return tuple(left+i*MINUTE+LAG for i in range(300))


def indexed_rows(rows):
    result = {}
    previous = -1
    for row in rows:
        t = row['bar_start_ns']
        if type(t) is not int or t <= previous or t % MINUTE:
            raise IntegrityError('Duplicate, unordered or unaligned minute source')
        result[t] = row
        previous = t
    return result


def _validate(row):
    fields = ('bar_start_ns', 'bar_end_ns', 'available_at_ns', 'decision_eligible_at_ns',
              'definition_received_at_ns', 'listing_activation_ns', 'expiration_ns',
              'open_nano', 'high_nano', 'low_nano', 'close_nano', 'volume',
              'publisher_id', 'instrument_id', 'min_price_increment_nano', 'multiplier_nano')
    if any(type(row.get(k)) is not int for k in fields):
        raise IntegrityError('Incomplete minute timing, identity or economics')
    if (row['market'] != 'ES' or row.get('source_cadence') != '1m'
            or not isinstance(row.get('actual_contract'), str) or not row['actual_contract']
            or row.get('currency') != 'USD'
            or row['min_price_increment_nano'] != NS//4 or row['multiplier_nano'] != 50*NS
            or row['bar_end_ns'] != row['bar_start_ns']+MINUTE
            or row['available_at_ns'] < row['bar_end_ns']
            or row['decision_eligible_at_ns'] < row['available_at_ns']
            or row['volume'] < 0
            or row['low_nano'] > min(row['open_nano'], row['close_nano'])
            or row['high_nano'] < max(row['open_nano'], row['close_nano'])
            or any(row[k] % (NS//4) for k in ('open_nano','high_nano','low_nano','close_nano'))):
        raise IntegrityError('Invalid ES minute observation')


def _eligible_at(row):
    return max(row['decision_eligible_at_ns'], row['definition_received_at_ns'], row['bar_end_ns']+LAG)


def _window(index, starts, as_of_ns, *, expected_identity=None, label=False):
    rows, reasons = [], []
    for t in starts:
        row = index.get(t)
        if row is None:
            reasons.append('missing_future_horizon' if label else 'missing_feature_inputs')
            continue
        _validate(row)
        rows.append(row)
        if _eligible_at(row) > as_of_ns:
            reasons.append('label_not_mature' if label else 'causal_availability')
        if not row['listing_activation_ns'] <= t < row['bar_end_ns'] <= row['expiration_ns']:
            reasons.append('contract_lifecycle')
    identities = {tuple(r[k] for k in IDENTITY) for r in rows}
    if len(identities) > 1 or (expected_identity is not None and identities - {expected_identity}):
        reasons.append('contract_or_unit_transition')
    if reasons:
        raise Unavailable(*reasons)
    return rows


def feature_at(index, decision_ns):
    day = datetime.fromtimestamp(decision_ns/NS, NY).date().isoformat()
    if decision_ns not in decision_times(day):
        raise Unavailable('session_restriction')
    end = decision_ns-LAG
    starts = range(end-61*MINUTE, end, MINUTE)
    try:
        rows = _window(index, starts, decision_ns)
    except Unavailable as exc:
        reasons = list(exc.reasons)
        if not index or (min(index) > starts.start):
            reasons.append('feature_warmup')
        raise Unavailable(*reasons) from exc
    closes = [r['close_nano'] for r in rows]
    # Subtraction before floating conversion avoids precision loss from absolute prices.
    previous = [p-closes[-1] for p in closes[:-1]]
    scale = pstdev(previous)
    if scale == 0:
        raise Unavailable('zero_displacement_scale')
    offset = fmean(previous)
    z = -offset/scale
    tick = rows[-1]['min_price_increment_nano']
    vol = pstdev([(b-a)/tick for a,b in zip(closes,closes[1:])])
    direction = -1 if z >= 2 else 1 if z <= -2 else 0
    return dict(decision_ns=decision_ns, history_start_ns=starts.start,
                entry_ns=end+MINUTE, direction=direction,
                reference_mean_nano=closes[-1]+offset,
                signal_close_nano=closes[-1], identity=tuple(rows[-1][k] for k in IDENTITY),
                features=dict(zip(FEATURES, (z, (closes[-1]-closes[-6])/tick, vol,
                    (decision_ns-decision_times(day)[0])//MINUTE))))


def label_for(index, feature, horizon, *, as_of_ns):
    if horizon not in HORIZONS or type(horizon) is not int:
        raise IntegrityError('Horizon must be 15, 30 or 60 minutes')
    entry_ns = feature['entry_ns']
    exit_ns = entry_ns+horizon*MINUTE
    rows = _window(index, range(entry_ns, exit_ns+MINUTE, MINUTE), as_of_ns,
                   expected_identity=feature['identity'], label=True)
    entry, exit_price = rows[0]['open_nano'], rows[-1]['open_nano']
    direction, mean = feature['direction'], feature['reference_mean_nano']
    tick = rows[0]['min_price_increment_nano']
    result = dict(horizon_minutes=horizon, entry_ns=entry_ns, exit_ns=exit_ns,
                  label_known_at_ns=max(_eligible_at(r) for r in rows),
                  raw_return_ticks=(exit_price-entry)/tick,
                  frozen_mean_nano=mean,
                  distance_reduction_ticks=(abs(entry-mean)-abs(exit_price-mean))/tick,
                  direction=direction, price_reference='NEXT_MINUTE_OPEN_NOT_FILL')
    if not direction:
        return result | dict(path_status='NOT_DISPLACED')
    target_distance = direction*(mean-entry)
    # Labels never remove a signal retrospectively: already-crossed entries are explicit.
    target_time = adverse_time = None
    mae = mfe = 0
    mae_time = mfe_time = [entry_ns,entry_ns]
    for row in rows[:-1]:  # Exit minute's high/low are outside the fixed horizon.
        worst = direction*((row['low_nano'] if direction == 1 else row['high_nano'])-entry)
        best = direction*((row['high_nano'] if direction == 1 else row['low_nano'])-entry)
        bounds = [row['bar_start_ns'], row['bar_end_ns']]
        if -worst > mae:
            mae, mae_time = -worst, bounds
        if best > mfe:
            mfe, mfe_time = best, bounds
        if target_distance > 0 and target_time is None and best >= target_distance:
            target_time = bounds
        if target_distance > 0 and adverse_time is None and worst <= -target_distance:
            adverse_time = bounds
    # Include the terminal open in excursions and boundary hits, never its H/L/C.
    terminal = direction*(exit_price-entry)
    if -terminal > mae: mae,mae_time = -terminal,[exit_ns,exit_ns]
    if terminal > mfe: mfe,mfe_time = terminal,[exit_ns,exit_ns]
    if target_distance > 0 and target_time is None and terminal >= target_distance: target_time=[exit_ns,exit_ns]
    if target_distance > 0 and adverse_time is None and terminal <= -target_distance: adverse_time=[exit_ns,exit_ns]
    first = ('NEITHER' if target_time is None and adverse_time is None else
             'ADVERSE_FIRST' if target_time is None else 'TARGET_FIRST' if adverse_time is None else
             'AMBIGUOUS_SAME_MINUTE' if target_time[0] == adverse_time[0] else
             'TARGET_FIRST' if target_time[0] < adverse_time[0] else 'ADVERSE_FIRST')
    if target_distance <= 0:
        first, target_time = 'MEAN_REACHED_BEFORE_ENTRY', [entry_ns,entry_ns]
    return result | dict(path_status=first, directed_return_ticks=terminal/tick,
        mae_ticks=mae/tick, mfe_ticks=mfe/tick, mae_time_bounds_ns=mae_time,
        mfe_time_bounds_ns=mfe_time, target_time_bounds_ns=target_time,
        adverse_time_bounds_ns=adverse_time,
        adverse_boundary_nano=entry-direction*target_distance if target_distance > 0 else None)


def coverage_day(day, rows, *, as_of_ns):
    """Availability census only; no return or excursion aggregates leave this call."""
    index = indexed_rows(rows)
    output = {h:dict(day=day, horizon=h, candidates=len(decision_times(day)),
                    observed_signal_bars=sum(t-LAG-MINUTE in index for t in decision_times(day)),
                    observations_outside_signal_clock=sum(t+MINUTE+LAG not in decision_times(day) for t in index),
                    feature_valid=0, label_valid=0, eligible_events=0, long_events=0, short_events=0,
                    feature_long_events=0, feature_short_events=0,
                    unique_refused=0, reasons=Counter(), events_by_hour=Counter(), events_by_volatility=Counter(),
                    feature_events_by_hour=Counter(), feature_events_by_volatility=Counter(), maturity_by_date={})
              for h in HORIZONS}
    for decision in decision_times(day):
        try:
            feature = feature_at(index, decision)
        except Unavailable as exc:
            for record in output.values():
                record['unique_refused'] += 1
                record['reasons'].update(exc.reasons)
            continue
        for h,record in output.items():
            record['feature_valid'] += 1
            hour=str(datetime.fromtimestamp(decision/NS,NY).hour)
            vol=feature['features']['volatility_60_ticks']
            bucket='lt1' if vol<1 else '1to2' if vol<2 else '2to4' if vol<4 else 'ge4'
            if feature['direction']:
                record['feature_long_events' if feature['direction']==1 else 'feature_short_events'] += 1
                record['feature_events_by_hour'][hour] += 1
                record['feature_events_by_volatility'][bucket] += 1
            try:
                label=label_for(index, feature, h, as_of_ns=as_of_ns)
            except Unavailable as exc:
                record['unique_refused'] += 1
                record['reasons'].update(exc.reasons)
                continue
            record['label_valid'] += 1
            known_day=datetime.fromtimestamp(label['label_known_at_ns']/NS,NY).date().isoformat()
            maturity=record['maturity_by_date'].setdefault(known_day,Counter())
            maturity['label_valid'] += 1
            if feature['direction']:
                record['eligible_events'] += 1
                record['long_events' if feature['direction']==1 else 'short_events'] += 1
                maturity['long_events' if feature['direction']==1 else 'short_events'] += 1
                record['events_by_hour'][hour] += 1
                record['events_by_volatility'][bucket] += 1
    return list(output.values())
