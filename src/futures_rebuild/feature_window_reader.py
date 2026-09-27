"""Native feature histories above admitted readers; no raw partition access.

An immutable closure certificate is mandatory even for previews. Calendar
candidates and successful diagnostics are not certificates or runtime authority.
"""
from datetime import datetime, timezone
from pathlib import Path
import json

from .canonical import sha256_file
from .causal_timeframe_storage import contained, read_record
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .feature_calendar_feasibility import interval_partition
from .hourly_quality_reader import IDENTITY, CUTOFF, HOUR

DAY = 24*HOUR
LAG = 5*10**9
POINTER = 'configs/active_feature_window_admission.json'
REQUIRED_BINDINGS = frozenset({
    'src/futures_rebuild/feature_window_reader.py',
    'src/futures_rebuild/feature_calendar_feasibility.py',
    'configs/active_hourly_source_admission.json',
    'configs/active_daily_source_admission.json',
    'configs/active_hourly_quality_reader.json',
    'configs/active_daily_recovery_reader_binding.json',
    'data/causally_gated_normalized/_control/catalog.json', 'data/causally_gated_normalized/_control/timeframes.json',
})


class FeatureWindowRefused(UnauthorizedOperation):
    def __init__(self, reason, start_ns, end_ns, detail=''):
        self.reason, self.start_ns, self.end_ns = reason, start_ns, end_ns
        super().__init__(f'{reason} at [{start_ns}, {end_ns}): {detail}')


def plan_segments(*, closures, start_ns, end_ns, as_of_ns, step, lookback_bars):
    """Select dense segments backward from an exact endpoint, without bar reads."""
    if (step not in (HOUR, DAY) or type(lookback_bars) is not int or lookback_bars <= 0
            or any(type(v) is not int for v in (start_ns, end_ns, as_of_ns))
            or not 0 < start_ns < end_ns <= as_of_ns or start_ns % step or end_ns % step):
        raise ContractError('Aligned, positive feature bounds and lookback required')
    parts = interval_partition(closures, start_ns=start_ns, end_ns=end_ns, as_of_ns=as_of_ns)
    # Union before rounding to complete UTC bars; adjacent partial proofs may
    # together establish a whole interval, while a partial closure cannot.
    unions = []
    for part in parts:
        if part['state'] != 'CLOSED':
            continue
        if unions and unions[-1]['end_ns'] == part['start_ns']:
            unions[-1]['end_ns'] = part['end_ns']
            unions[-1]['evidence_ids'] = sorted(set(unions[-1]['evidence_ids']+part['evidence_ids']))
        else:
            unions.append(dict(part))
    closed = [dict(x, start_ns=((x['start_ns']+step-1)//step)*step,
                   end_ns=(x['end_ns']//step)*step) for x in unions]
    closed = [x for x in closed if x['start_ns'] < x['end_ns']]
    cursor, remaining = end_ns, lookback_bars
    reads, skips = [], []
    i = len(closed)-1
    while remaining:
        if cursor <= start_ns:
            raise FeatureWindowRefused('INSUFFICIENT_HISTORY', start_ns, end_ns)
        while i >= 0 and closed[i]['start_ns'] >= cursor:
            i -= 1
        if i >= 0 and closed[i]['start_ns'] <= cursor-step and cursor <= closed[i]['end_ns']:
            skips.append(dict(closed[i], end_ns=cursor))
            cursor = closed[i]['start_ns']
            i -= 1
            continue
        boundary = max(start_ns, closed[i]['end_ns'] if i >= 0 else start_ns)
        take = min(remaining, (cursor-boundary)//step)
        if take <= 0:
            raise IntegrityError('Feature segment planning made no progress')
        reads.append(dict(start_ns=cursor-take*step, end_ns=cursor))
        cursor -= take*step
        remaining -= take
    return dict(reads=list(reversed(reads)), skipped_closures=list(reversed(skips)), start_ns=cursor)


def require_span(entries, *, start_ns, end_ns, start_key, end_key, accepts, reason):
    """Exact non-overlapping coverage; missing and ambiguous spans fail closed."""
    relevant = sorted((x for x in entries if x[start_key] < end_ns and x[end_key] > start_ns),
                      key=lambda x: x[start_key])
    cursor = start_ns
    for item in relevant:
        left, right = max(start_ns, item[start_key]), min(end_ns, item[end_key])
        if left != cursor or not accepts(item):
            raise FeatureWindowRefused(reason, min(cursor, left), max(cursor+1, right))
        cursor = right
    if cursor != end_ns:
        raise FeatureWindowRefused(reason, cursor, end_ns)


class FeatureWindowReader:
    def __init__(self, repository_root, *, preview_manifest=None):
        self.root = Path(repository_root).resolve()
        self.bindings, self._readers = {}, {}
        pointer = None
        if preview_manifest is None:
            pointer = self._record(POINTER)
            if pointer.get('schema') != 'active_feature_window_admission/1':
                raise IntegrityError('Unsupported feature pointer')
            preview_manifest = pointer['manifest']['path']
        manifest = self._record(preview_manifest, pointer['manifest']['sha256'] if pointer else None)
        if (manifest.get('schema') != 'feature_window_policy/1' or manifest.get('source') != 'native'
                or manifest.get('contract_policy') != 'RESET_AT_IDENTITY_CHANGE'
                or not REQUIRED_BINDINGS <= manifest.get('bindings', {}).keys()):
            raise IntegrityError('Feature policy lacks required authority or bindings')
        for path, digest in manifest['bindings'].items():
            self._bind(path, digest)
        cert_ref = manifest['closure_certificate']
        certificate = self._record(cert_ref['path'], cert_ref['sha256'])
        if (certificate.get('status') != 'PASS_CLOSED_INTERVAL_CERTIFICATION'
                or not REQUIRED_BINDINGS-{'src/futures_rebuild/feature_window_reader.py'}
                    <= certificate.get('bindings', {}).keys()):
            raise IntegrityError('Calendar candidates are not certified closure authority')
        for path, digest in certificate['bindings'].items():
            self._bind(path, digest)
        evidence = certificate.get('evidence', {})
        for reference in evidence.values():
            self._bind(reference['path'], reference['sha256'])
        self.closures = {}
        for entry in certificate['closures']:
            if (type(entry.get('market')) is not str or not entry['market']
                    or any(type(entry.get(k)) is not int for k in ('start_ns','end_ns','available_at_ns'))
                    or not 0 < entry['start_ns'] < entry['end_ns'] <= CUTOFF
                    or entry['available_at_ns'] < 0
                    or not entry.get('timeframes') or not set(entry['timeframes']) <= {'1h','1d'}
                    or not entry.get('evidence_ids')
                    or any(x not in evidence for x in entry['evidence_ids'])
                    or any(entry.get(k) is not True for k in (
                        'product_venue_qualified', 'schedule_qualified', 'availability_qualified',
                        'no_conflicts', 'observations_absent_verified'))):
                raise IntegrityError('Incomplete closure certification')
            for timeframe in entry['timeframes']:
                self.closures.setdefault((entry['market'],timeframe), []).extend(
                    dict(start_ns=entry['start_ns'], end_ns=entry['end_ns'], state='CLOSED',
                         available_at_ns=entry['available_at_ns'], evidence_id=ev) for ev in entry['evidence_ids'])
        self.policy = manifest
        self.index = self._record('data/causally_gated_normalized/_control/timeframes.json')
        self.catalog = json.loads(self._bind('data/causally_gated_normalized/_control/catalog.json').read_bytes())
        if pointer:
            validation = self._record(pointer['validation']['path'], pointer['validation']['sha256'])
            supervision = self._record(pointer['supervision']['path'], pointer['supervision']['sha256'])
            if (validation.get('status') != 'PASS_RECOVERY_PHASE'
                    or validation.get('validation_status') != 'PASS_FEATURE_WINDOW_VALIDATION'
                    or validation.get('policy_sha256') != pointer['manifest']['sha256']
                    or validation.get('supported') != manifest['supported']
                    or validation.get('demonstrated_new_windows', 0) <= 0
                    or supervision.get('status') != 'PASS_SUPERVISED_PHASE'
                    or supervision.get('worker_result_sha256') != pointer['validation']['sha256']):
                raise IntegrityError('Feature pointer lacks passing real-window validation')
        self._unchanged()

    def _bind(self, relative, expected=None):
        path = contained(self.root, relative)
        digest = sha256_file(path)
        if ((expected is not None and digest != expected)
                or (relative in self.bindings and self.bindings[relative] != digest)):
            raise IntegrityError('Feature binding changed: '+relative)
        self.bindings[relative] = digest
        return path

    def _record(self, relative, expected=None):
        return read_record(self._bind(relative, expected))

    def _unchanged(self):
        for path, digest in self.bindings.items():
            if sha256_file(contained(self.root, path)) != digest:
                raise IntegrityError('Feature binding changed during use: '+path)

    def _reader(self, timeframe):
        if timeframe not in self._readers:
            if timeframe == '1h':
                from .hourly_source_admission import HourlySourceReader
                self._readers[timeframe] = HourlySourceReader(self.root)
            else:
                from .daily_source_admission import DailySourceReader
                self._readers[timeframe] = DailySourceReader(self.root)
        return self._readers[timeframe]

    def read_feature_window(self, *, market, timeframe, start_ns, end_ns, as_of_ns,
                            lookback_bars, purpose, source='native'):
        self._unchanged()
        if (type(market) is not str or not market or timeframe not in ('1h', '1d')
                or source != 'native' or type(purpose) is not str):
            raise ContractError('Choose native hourly/daily market and purpose explicitly')
        step = HOUR if timeframe == '1h' else DAY
        if (any(type(v) is not int for v in (start_ns, end_ns, as_of_ns, lookback_bars))
                or not self.policy['start_ns'] <= start_ns < end_ns <= min(CUTOFF,self.policy['end_ns'])):
            raise ContractError('Feature request outside policy coverage')
        if not any(x['market']==market and x['timeframe']==timeframe
                   and lookback_bars in x['lookbacks'] and purpose in x['purposes'] for x in self.policy['supported']):
            raise FeatureWindowRefused('UNSUPPORTED_MARKET_HORIZON_PURPOSE',start_ns,end_ns)
        plan = plan_segments(closures=self.closures.get((market,timeframe), []), start_ns=start_ns,
                             end_ns=end_ns, as_of_ns=as_of_ns, step=step, lookback_bars=lookback_bars)
        left = plan['start_ns']
        monthly = [x for x in self.index['entries'] if x['market']==market and x['timeframe']==timeframe]
        require_span(monthly, start_ns=left,end_ns=end_ns,start_key='coverage_start_ns',end_key='coverage_end_ns',
                     accepts=lambda x:x['disposition']=='CERTIFIED',reason='UNAVAILABLE_DELIVERY')
        from .hourly_source_admission import timestamp
        ceilings = [dict(x, start_ns=timestamp(x['coverage_start']),end_ns=timestamp(x['coverage_end']))
                    for x in self.catalog['entries'] if x['market']==market]
        require_span(ceilings,start_ns=left,end_ns=end_ns,start_key='start_ns',end_key='end_ns',
            accepts=lambda x:x['disposition']=='RESEARCH_READY_CAUSAL_PRICE' and purpose in x['permitted_uses'],
            reason='PERMISSION_OR_COHORT_BOUNDARY')
        reader = self._reader(timeframe)
        rows = []
        for segment in plan['reads']:
            try:
                got = reader.read_window(market=market,**segment,as_of_ns=as_of_ns,source=source,purpose=purpose)
            except UnauthorizedOperation as exc:
                raise FeatureWindowRefused('ADMITTED_SEGMENT_REFUSED',segment['start_ns'],segment['end_ns'],str(exc)) from exc
            if [r['bar_start_ns'] for r in got] != list(range(segment['start_ns'],segment['end_ns'],step)):
                raise IntegrityError('Admitted reader returned incomplete feature segment')
            rows.extend(got)
        if len(rows) != lookback_bars:
            raise IntegrityError('Feature bar count differs')
        for row in rows:
            if any(row[k] != rows[0][k] for k in IDENTITY):
                raise FeatureWindowRefused('INSUFFICIENT_HISTORY_AFTER_IDENTITY_RESET',row['bar_start_ns'],row['bar_end_ns'])
            if (row['market'] != market or row['bar_end_ns'] != row['bar_start_ns']+step
                    or row['origin'] != ('NATIVE_1H' if timeframe=='1h' else 'NATIVE_1D')
                    or row['available_at_ns'] < row['bar_end_ns']+LAG
                    or row['decision_eligible_at_ns'] < row['available_at_ns']):
                raise IntegrityError('Feature row identity, origin or availability differs')
            if row['decision_eligible_at_ns'] > as_of_ns:
                raise FeatureWindowRefused('PREMATURE_AVAILABILITY',row['bar_start_ns'],row['bar_end_ns'])
        self._unchanged()
        return dict(rows=rows, history_endpoint_ns=end_ns, actual_start_ns=rows[0]['bar_start_ns'],
            elapsed_span_ns=end_ns-rows[0]['bar_start_ns'],
            final_observation_age_ns=end_ns-rows[-1]['bar_end_ns'],
            observation_age_at_decision_ns=as_of_ns-rows[-1]['bar_end_ns'],
            segment_identity={k:rows[0][k] for k in IDENTITY}, skipped_closures=plan['skipped_closures'])
