"""Diagnostic only: Ardia, Guidotti and Kroencke EDGE, signed estimate.

Algorithm source: https://github.com/eguidotti/bidask/blob/main/python/bidask/edge.py
The estimator below is adapted from that implementation. No calibration data.

MIT License
Copyright (c) 2024 Emanuele Guidotti
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import warnings

import numpy as np

from .rolling_hour_assumed_cost import MARKETS


def signed_edge(ohlc):
    a = np.asarray(ohlc, dtype=float)
    if a.ndim != 2 or a.shape[1] != 4 or len(a) < 3:
        return float('nan')
    with warnings.catch_warnings(), np.errstate(all='ignore'):
        warnings.simplefilter('ignore', RuntimeWarning)
        o, h, l, c = np.log(a).T
        m = (h + l) / 2
        h1, l1, c1, m1 = h[:-1], l[:-1], c[:-1], m[:-1]
        o, h, l, c, m = [x[1:] for x in (o, h, l, c, m)]
        r1, r2, r3, r4, r5 = m-o, o-m1, m-c1, c1-m1, o-c1
        tau = np.where(np.isnan(h) | np.isnan(l) | np.isnan(c1), np.nan, (h != l) | (l != c1))
        po1 = tau * np.where(np.isnan(o) | np.isnan(h), np.nan, o != h)
        po2 = tau * np.where(np.isnan(o) | np.isnan(l), np.nan, o != l)
        pc1 = tau * np.where(np.isnan(c1) | np.isnan(h1), np.nan, c1 != h1)
        pc2 = tau * np.where(np.isnan(c1) | np.isnan(l1), np.nan, c1 != l1)
        pt, po, pc = np.nanmean(tau), np.nanmean(po1)+np.nanmean(po2), np.nanmean(pc1)+np.nanmean(pc2)
        if np.nansum(tau) < 2 or po == 0 or pc == 0:
            return float('nan')
        d1, d3, d5 = [r - np.nanmean(r) / pt * tau for r in (r1, r3, r5)]
        x1 = -4/po*d1*r2 - 4/pc*d3*r4
        x2 = -4/po*d1*r5 - 4/pc*d5*r4
        e1, e2 = np.nanmean(x1), np.nanmean(x2)
        v1, v2 = np.nanmean(x1**2)-e1**2, np.nanmean(x2**2)-e2**2
        s2 = (v2*e1 + v1*e2)/(v1+v2) if v1+v2 > 0 else (e1+e2)/2
        return float(np.sign(s2) * np.sqrt(abs(s2)))


def diagnostic(market, contract, decision_ns, sessions):
    """Twenty declared prior sessions, no pooling across contracts or filling gaps.

    Each session carries trade_date, complete_at_ns, expected_bar_starts, rows.
    Missing bars become NaN only in the diagnostic, never fitting observations.
    """
    result = {'market': market, 'contract': contract, 'decision_ns': decision_ns,
        'state': 'UNAVAILABLE', 'signed_relative_spread': None, 'selected_policy_effect': 'NONE'}
    def unavailable(reason):
        return {**result, 'reason': reason}
    if market not in MARKETS:
        return unavailable('UNKNOWN_MARKET')
    if len(sessions) != 20:
        return unavailable('REQUIRES_20_PRIOR_COMPLETE_SESSIONS')
    days = [s['trade_date'] for s in sessions]
    if days != sorted(set(days)) or any(s['complete_at_ns'] >= decision_ns for s in sessions):
        return unavailable('UNORDERED_OR_FUTURE_SESSION')
    values, missing = [], 0
    for session in sessions:
        grid, rows = session['expected_bar_starts'], session['rows']
        if grid != sorted(set(grid)) or not grid or any(t >= session['complete_at_ns'] for t in grid):
            return unavailable('INVALID_SESSION_GRID')
        indexed = {r['bar_start_ns']: r for r in rows}
        if len(indexed) != len(rows) or not set(indexed) <= set(grid):
            return unavailable('DUPLICATE_OR_OUTSIDE_GRID')
        # NaN separator prevents overnight/closed-interval returns becoming minute returns.
        values.append([np.nan]*4)
        previous = None
        for t in grid:
            if previous is not None and t - previous != 60*10**9:
                values.append([np.nan]*4)
            previous = t
            r = indexed.get(t)
            if r is None:
                missing += 1
                values.append([np.nan]*4)
                continue
            if r['actual_contract'] != contract or r.get('market') != market:
                return unavailable('CONTRACT_ROLL_OR_MARKET_MISMATCH')
            if r['decision_eligible_at_ns'] > session['complete_at_ns']:
                return unavailable('LATE_AVAILABILITY')
            bar = [r[f'{k}_nano'] for k in ('open', 'high', 'low', 'close')]
            if not np.isfinite(bar).all() or min(bar) <= 0:
                return unavailable('NONPOSITIVE_OR_NONFINITE_PRICE')
            if not bar[2] <= min(bar[0], bar[3]) <= max(bar[0], bar[3]) <= bar[1]:
                return unavailable('INVALID_OHLC')
            values.append(bar)
    estimate = signed_edge(values)
    result.update(missing_minutes=missing, prior_dates=days)
    if not np.isfinite(estimate):
        return {**result, 'reason': 'INSUFFICIENT_PRICE_VARIATION_OR_SUPPORT'}
    return {**result, 'state': 'UNSTABLE_NEGATIVE' if estimate < 0 else 'AVAILABLE_DIAGNOSTIC_ONLY',
        'signed_relative_spread': estimate, 'reason': None}
