"""Daily source permissions above the unchanged, certified window reader.

Permission queries describe policy only. They neither read bars nor authorize a
research operation. A successful read still requires the underlying quality gate.
"""
from pathlib import Path
import json

from . import active_data_view as minute
from .canonical import sha256_file
from .causal_timeframe_storage import contained, read_record
from .daily_quality_reader import DailyReader
from .causal_1d import DAY, CUTOFF, implementation_binding
from .hourly_source_admission import timestamp, verify_bindings
from .errors import ContractError, IntegrityError, UnauthorizedOperation

POINTER = 'configs/active_daily_source_admission.json'
SCHEMA = 'daily_source_admission/1'
SUPPORTED = frozenset({'FULL_HOURLY_AGREEMENT', 'OBSERVED_HOURLY_AGREEMENT'})
REASONS = frozenset({'FULL_HOURLY_AGREEMENT', 'OBSERVED_HOURLY_AGREEMENT',
    'INSUFFICIENT_HOURLY_COVERAGE', 'CONTRACT_OR_PUBLISHER_DIFFERENCE',
    'EXCLUDED_HOURLY_REFERENCE', 'REFERENCE_DISAGREEMENT', 'REFERENCE_UNAVAILABLE',
    'UNRESOLVED_REFERENCE'})


def policy_entries(quality, catalog):
    """Intersect evidence-supported UTC days with verified minute cohort ceilings."""
    minute.validate_catalog(catalog)
    ceilings = {(e['market'], e['year']): e for e in catalog['entries']}
    if len(ceilings) != len(catalog['entries']):
        raise IntegrityError('Ambiguous minute permission coverage')
    entries = []
    seen = set()
    for row in quality:
        market, start = row['market'], row['start_ns']
        key = market, start
        if (type(start) is not int or start % DAY or not 0 < start < start+DAY <= CUTOFF
                or key in seen or row['reason'] not in REASONS):
            raise IntegrityError('Invalid or ambiguous daily quality coverage')
        seen.add(key)
        from datetime import datetime, timezone
        year = datetime.fromtimestamp(start//10**9, timezone.utc).year
        ceiling = ceilings.get((market, year))
        uses, cohort = [], None
        if ceiling and ceiling['disposition'] == minute.CERTIFICATION_STATE:
            cohort = minute.cohort_for_coverage(year=year,
                coverage_start=ceiling['coverage_start'], coverage_end=ceiling['coverage_end'])
            allowed = list(minute.COHORT_PERMISSIONS[cohort])
            if ceiling['cohort'] != cohort or ceiling['permitted_uses'] != allowed:
                raise IntegrityError('Minute cohort policy differs')
            if timestamp(ceiling['coverage_start']) <= start and start+DAY <= timestamp(ceiling['coverage_end']):
                uses = allowed if row['reason'] in SUPPORTED else [u for u in allowed if u == 'LINEAGE_AUDIT']
        entries.append(dict(market=market, start_ns=start, end_ns=start+DAY,
            row_id=row['row_id'], cohort=cohort, reason=row['reason'], permitted_uses=uses))
    return sorted(entries, key=lambda e: (e['market'], e['start_ns']))


class DailySourceReader:
    """Dense native daily windows; no derived substitution or path-returning API."""

    def __init__(self, repository_root, *, preview_manifest=None):
        self.root = Path(repository_root).resolve()
        self.bindings = {}
        if preview_manifest is None:
            pointer = self._record(POINTER)
            if pointer.get('schema') != 'active_daily_source_admission/1':
                raise IntegrityError('Unsupported daily admission pointer')
            preview_manifest = pointer['manifest']['path']
            self.bindings[preview_manifest] = pointer['manifest']['sha256']
            audit = self._record(pointer['audit']['path'], pointer['audit']['sha256'])
            supervision = self._record(pointer['supervision']['path'], pointer['supervision']['sha256'])
            if (audit.get('status') != 'PASS_DAILY_FOUNDATION_WITH_LIMITATIONS'
                    or audit.get('admission_sha256') != pointer['manifest']['sha256']
                    or supervision.get('status') != 'PASS_SUPERVISED_DAILY_FOUNDATION'
                    or supervision.get('result_sha256') != pointer['audit']['sha256']):
                raise IntegrityError('Daily admission lacks passing integrated validation')
        manifest = self._record(preview_manifest, self.bindings.get(preview_manifest))
        required = {'data/causally_gated_normalized/_control/timeframes.json', 'data/causally_gated_normalized/_control/catalog.json',
                    'src/futures_rebuild/daily_source_admission.py',
                    'src/futures_rebuild/daily_foundation_admission.py'}
        if (manifest.get('schema') != SCHEMA or manifest.get('source') != 'native'
                or manifest.get('model_specific_eligibility') != 'NOT_DECLARED'
                or not required <= manifest['bindings'].keys()
                or manifest['daily_implementation'] != implementation_binding()):
            raise IntegrityError('Daily policy or implementation binding differs')
        for path, digest in manifest['bindings'].items():
            if path in self.bindings and self.bindings[path] != digest:
                raise IntegrityError('Conflicting daily policy binding')
            self.bindings[path] = digest
        self._unchanged()
        quality = self._record(manifest['quality']['path'], manifest['quality']['sha256'])
        # The minute catalog has its own catalog_sha256 contract, not the
        # timeframe record's id field. Its bytes are already bound above.
        catalog = json.loads(contained(self.root,'data/causally_gated_normalized/_control/catalog.json').read_bytes())
        from .daily_foundation_admission import validate_permission_sidecars
        validate_permission_sidecars(self.root, catalog, self.bindings)
        if (quality.get('status') != 'PASS_DAILY_QUALITY_ACCOUNTING'
                or manifest['entries'] != policy_entries(quality['rows'], catalog)):
            raise IntegrityError('Daily policy entries differ from bound quality evidence')
        self.manifest = manifest
        self.entries = {(e['market'], e['start_ns']): e for e in manifest['entries']}
        self.quality_reader = DailyReader(self.root)
        self._unchanged()

    def _record(self, relative, expected=None):
        path = contained(self.root, relative)
        digest = sha256_file(path)
        if expected is not None and digest != expected:
            raise IntegrityError('Daily policy evidence changed: '+relative)
        self.bindings[relative] = digest
        return read_record(path)

    def _unchanged(self):
        from .annual_foundation_independence import verify_reader
        verify_reader(self.root, self.bindings)
        verify_bindings(self.root, self.bindings)

    def permitted_uses(self, *, market, start_ns, end_ns, source='native'):
        self._unchanged()
        if (type(market) is not str or not market or source != 'native'
                or any(type(v) is not int for v in (start_ns, end_ns))
                or not 0 < start_ns < end_ns <= CUTOFF or start_ns % DAY or end_ns % DAY):
            raise ContractError('Daily policy requires aligned native development windows')
        allowed = None
        for start in range(start_ns, end_ns, DAY):
            entry = self.entries.get((market, start))
            if entry is None:
                return ()
            uses = set(entry['permitted_uses'])
            allowed = uses if allowed is None else allowed & uses
        return tuple(sorted(allowed or ()))

    def read_window(self, *, market, start_ns, end_ns, as_of_ns, source='native', purpose='LINEAGE_AUDIT'):
        uses = self.permitted_uses(market=market, start_ns=start_ns, end_ns=end_ns, source=source)
        if type(purpose) is not str or purpose not in uses:
            raise UnauthorizedOperation('Daily purpose is not permitted throughout this window')
        rows = self.quality_reader.read_window(market=market, start_ns=start_ns, end_ns=end_ns,
            as_of_ns=as_of_ns, source=source, purpose='LINEAGE_AUDIT')
        for row in rows:
            if row['row_id'] != self.entries[(market, row['bar_start_ns'])]['row_id']:
                raise IntegrityError('Daily admitted observation identity differs')
        self._unchanged()
        return rows
