"""Source-use admission above the immutable hourly quality reader.

The CLI prepares metadata only. Publication and historical validation belong to
an approved task; source permissions do not authorize a research operation.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import os

from . import active_data_view as minute
from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_timeframe_storage import contained, read_record, signed
from .causal_timeframe_publication import months
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .hourly_quality_reader import HourlyReader, HOUR, CUTOFF, POINTER as QUALITY_POINTER

POINTER = 'configs/active_hourly_source_admission.json'
HERE = 'state/unpublished_evidence/hourly_source_admission_v1'
FOUNDATION = ('state/unpublished_evidence/hourly_foundation_audit_v1/'
              '00728479c1435f814e10cfa023fbb5f1edaf907ff009e56a4422a374ef1b5305')
IMPLEMENTATION = ('src/futures_rebuild/hourly_source_admission.py',
                  'src/futures_rebuild/active_data_view.py')


def timestamp(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return int(stamp.timestamp()) * 10**9


def policy_entries(index, catalog):
    """Minute permissions are a ceiling, never hourly content certification."""
    by_year = {(e['market'], e['year']): e for e in catalog['entries']}
    if len(by_year) != len(catalog['entries']):
        raise IntegrityError('Ambiguous minute policy coverage')
    entries = []
    for entry in index['entries']:
        if entry['timeframe'] != '1h':
            continue
        start, end = entry['coverage_start_ns'], entry['coverage_end_ns']
        if not 0 < start < end <= CUTOFF:
            raise IntegrityError('Hourly admission exceeds development coverage')
        if months(start, end) != [entry['month']]:
            raise IntegrityError('Hourly admission month differs from coverage')
        year = int(entry['month'][:4])
        ceiling = by_year.get((entry['market'], year))
        uses, cohort, admitted_start, admitted_end = [], None, None, None
        if ceiling and ceiling['disposition'] == minute.CERTIFICATION_STATE:
            cohort = minute.cohort_for_coverage(year=year,
                coverage_start=ceiling['coverage_start'], coverage_end=ceiling['coverage_end'])
            if ceiling['cohort'] != cohort or ceiling['permitted_uses'] != list(minute.COHORT_PERMISSIONS[cohort]):
                raise IntegrityError('Minute cohort permission differs')
            left = max(start, timestamp(ceiling['coverage_start']))
            right = min(end, timestamp(ceiling['coverage_end']))
            if entry['disposition'] == 'CERTIFIED' and left < right:
                admitted_start, admitted_end = left, right
                uses = list(minute.COHORT_PERMISSIONS[cohort])
        entries.append(dict(market=entry['market'], month=entry['month'],
            coverage_start_ns=start, coverage_end_ns=end, disposition=entry['disposition'],
            cohort=cohort, admitted_start_ns=admitted_start, admitted_end_ns=admitted_end,
            permitted_uses=uses))
    entries.sort(key=lambda e: (e['market'], e['month']))
    if len({(e['market'], e['month']) for e in entries}) != len(entries):
        raise IntegrityError('Ambiguous hourly admission coverage')
    return entries


def verify_bindings(root, bindings):
    for relative, digest in bindings.items():
        if sha256_file(contained(Path(root), relative)) != digest:
            raise IntegrityError('Hourly admission binding changed: ' + relative)


def prepare_manifest(root):
    """Metadata only: do not instantiate the quality reader or open bar values."""
    root = Path(root).resolve()
    bindings = {}
    def record(relative, expected=None):
        path = contained(root, relative)
        digest = sha256_file(path)
        if expected is not None and digest != expected:
            raise IntegrityError('Hourly admission evidence changed: ' + relative)
        bindings[relative] = digest
        return json.loads(path.read_bytes())
    pointer = read_record(contained(root, QUALITY_POINTER))
    record(QUALITY_POINTER)
    quality = record(pointer['manifest']['path'], pointer['manifest']['sha256'])
    for relative, digest in quality['implementation'].items():
        bindings[relative] = digest
    index = record('data/causally_gated_normalized/_control/timeframes.json')
    catalog = record('data/causally_gated_normalized/_control/catalog.json')
    minute.validate_catalog(catalog)
    # Bind the actual minute permission sidecars, not just their catalog labels.
    for entry in catalog['entries']:
        if entry['disposition'] != minute.CERTIFICATION_STATE:
            continue
        sidecar = record(entry['sidecar_path'], entry['sidecar_sha256'])
        if 'annual_release_key' in entry:
            from .annual_foundation_release import minute_permission
            sidecar = minute_permission(root, entry)
        expected_entry = {k: v for k, v in entry.items() if k != 'sidecar_sha256'}
        if (sidecar.get('schema_version') != minute.SIDECAR_SCHEMA
                or sidecar.get('sidecar_id') != sha256_json({k: v for k, v in sidecar.items() if k != 'sidecar_id'})
                or sidecar.get('entry_binding') != expected_entry
                or minute.validate_access_policy_binding(sidecar['access_policy_binding']) != entry['access_policy_binding_id']
                or minute.validate_content_validation_receipt(sidecar['content_validation_receipt']) != entry['content_validation_receipt_id']):
            raise IntegrityError('Minute permission sidecar differs')
    supervision = record(FOUNDATION + '/supervision.json')
    result = record(FOUNDATION + '/execution/result.json', supervision['worker_result_sha256'])
    if (supervision['status'] != 'PASS_SUPERVISED_FOUNDATION_AUDIT'
            or result['status'] != 'PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS'):
        raise IntegrityError('Retained hourly foundation audit did not pass')
    for relative in IMPLEMENTATION:
        bindings[relative] = sha256_file(contained(root, relative))
    verify_bindings(root, bindings)
    return signed(dict(schema='hourly_source_admission/1', bindings=bindings,
        quality_pointer=QUALITY_POINTER, entries=policy_entries(index, catalog),
        derived_permitted_uses=['LINEAGE_AUDIT'], model_specific_eligibility='NOT_DECLARED',
        expected_counts=dict(partitions=6862, native_rows=3023365, native_accepted=3023362,
                             native_quarantined=3, unavailable_months=21, derived_rows=2179)))


class HourlySourceReader:
    """One source and contract per dense, eligible window; no path-returning API."""
    def __init__(self, repository_root, *, preview_manifest=None):
        self.root = Path(repository_root).resolve()
        self.bindings = {}
        if preview_manifest is None:
            pointer = self._record(POINTER)
            if pointer.get('schema') != 'active_hourly_source_admission/1':
                raise IntegrityError('Unsupported hourly source admission pointer')
            preview_manifest = pointer['manifest']['path']
            self.bindings[preview_manifest] = pointer['manifest']['sha256']
            supervision = self._record(pointer['supervision']['path'], pointer['supervision']['sha256'])
            result = self._record(pointer['result']['path'], pointer['result']['sha256'])
            if (supervision.get('status') != 'PASS_SUPERVISED_FOUNDATION_AUDIT'
                    or supervision.get('worker_result_sha256') != pointer['result']['sha256']
                    or result.get('status') != 'PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS'
                    or result.get('source_admission_manifest_sha256') != pointer['manifest']['sha256']):
                raise IntegrityError('Hourly source admission lacks passing integrated validation')
        manifest = self._record(preview_manifest, self.bindings.get(preview_manifest))
        if (manifest.get('schema') != 'hourly_source_admission/1'
                or manifest.get('quality_pointer') != QUALITY_POINTER
                or manifest.get('derived_permitted_uses') != ['LINEAGE_AUDIT']):
            raise IntegrityError('Unsupported hourly source admission policy')
        required = {*IMPLEMENTATION, QUALITY_POINTER, 'data/causally_gated_normalized/_control/catalog.json', 'data/causally_gated_normalized/_control/timeframes.json',
                    FOUNDATION + '/supervision.json', FOUNDATION + '/execution/result.json'}
        if not required <= manifest['bindings'].keys():
            raise IntegrityError('Hourly source admission lacks required bindings')
        if any(relative in self.bindings and self.bindings[relative] != digest
               for relative, digest in manifest['bindings'].items()):
            raise IntegrityError('Conflicting hourly source admission binding')
        self.bindings.update(manifest['bindings'])
        self._unchanged()
        catalog = json.loads(contained(self.root, 'data/causally_gated_normalized/_control/catalog.json').read_bytes())
        minute.validate_catalog(catalog)
        index = read_record(contained(self.root, 'data/causally_gated_normalized/_control/timeframes.json'))
        if manifest['entries'] != policy_entries(index, catalog):
            raise IntegrityError('Hourly source admission entries differ from bound policy')
        self.manifest = manifest
        self._entries = {(e['market'], e['month']): e for e in manifest['entries']}
        self.quality_reader = HourlyReader(self.root)
        self._unchanged()

    def _record(self, relative, expected=None):
        path = contained(self.root, relative)
        digest = sha256_file(path)
        if expected is not None and digest != expected:
            raise IntegrityError('Hourly source admission record changed: ' + relative)
        self.bindings[relative] = digest
        return read_record(path)

    def _unchanged(self):
        from .annual_foundation_independence import verify_reader
        verify_reader(self.root, self.bindings)
        verify_bindings(self.root, self.bindings)

    def permitted_uses(self, *, market, start_ns, end_ns, source='native'):
        """Policy-only query. Returned permissions do not establish bar quality."""
        self._unchanged()
        if (type(market) is not str or not market or source not in ('native', 'minute_derived')
                or any(type(v) is not int for v in (start_ns, end_ns))
                or not 0 < start_ns < end_ns <= CUTOFF or start_ns % HOUR or end_ns % HOUR):
            raise ContractError('Invalid hourly source admission window')
        if source == 'minute_derived':
            return ('LINEAGE_AUDIT',) if market == 'ZQ' else ()
        permitted = None
        for month in months(start_ns, end_ns):
            entry = self._entries.get((market, month))
            if entry is None or entry['admitted_start_ns'] is None:
                return ()
            year, number = map(int, month.split('-'))
            next_month = f'{year + (number == 12):04d}-{number % 12 + 1:02d}-01'
            if (max(start_ns, timestamp(month + '-01')) < entry['admitted_start_ns']
                    or min(end_ns, timestamp(next_month)) > entry['admitted_end_ns']):
                return ()
            uses = set(entry['permitted_uses'])
            permitted = uses if permitted is None else permitted & uses
        return tuple(sorted(permitted or ()))

    def read_window(self, *, market, start_ns, end_ns, as_of_ns, source='native', purpose='LINEAGE_AUDIT'):
        self._unchanged()
        uses = self.permitted_uses(market=market, start_ns=start_ns, end_ns=end_ns, source=source)
        if type(purpose) is not str or purpose not in uses:
            raise UnauthorizedOperation('Hourly purpose is not admitted throughout this window')
        rows = self.quality_reader.read_window(market=market, start_ns=start_ns, end_ns=end_ns,
            as_of_ns=as_of_ns, source=source, purpose='LINEAGE_AUDIT')
        self._unchanged()
        return rows


def main():
    from . import hourly_foundation_audit as audit
    root = Path.cwd()
    manifest = prepare_manifest(root)
    relative = HERE + '/' + manifest['id'] + '/manifest.json'
    directory = contained(root, relative).parent
    directory.mkdir(parents=True, exist_ok=False)
    (directory / 'manifest.json').write_bytes(canonical_bytes(manifest) + b'\n')
    plan = audit.prepare(root, admission_manifest=relative)
    plan_relative = audit.HERE + '/' + plan['id'] + '/plan.json'
    plan_directory = contained(root, plan_relative).parent
    plan_directory.mkdir(parents=True, exist_ok=False)
    (plan_directory / 'plan.json').write_bytes(canonical_bytes(plan) + b'\n')
    destination = contained(root, POINTER)
    proposal = signed(dict(status='PREPARED_REQUIRES_OWNER_APPROVAL', manifest=relative,
        audit_plan=plan_relative, destination=POINTER,
        expected_destination_sha256=sha256_file(destination) if destination.exists() else None,
        conditional_publication='Only after passing full integrated audit and supervision',
        source_mutation=False, payload_copy_bytes=0, provider_calls=0,
        rollback='Restore exactly the prior admission pointer; leave the quality pointer and data intact',
        limits=plan['limits'], free_bytes_at_preparation=plan['free_bytes_at_preparation']))
    (directory / 'publication_proposal.json').write_bytes(canonical_bytes(proposal) + b'\n')
    print(json.dumps(dict(manifest=relative, audit_plan=plan_relative, blockers=plan['blockers'],
                         rows=plan['rows'], normalized_bytes=plan['normalized_bytes'],
                         source_hash_only_bytes=plan['source_hash_only_bytes'])))


def publish_prepared(root, proposal_path, authorization_path):
    """Approved-task action only. The CLI deliberately does not expose publication."""
    from . import hourly_foundation_audit as audit
    from .causal_timeframe_execution import execution_lease
    from .causal_timeframe_storage import Budget
    root = Path(root).resolve()
    proposal = read_record(contained(root, proposal_path))
    plan = read_record(contained(root, proposal['audit_plan']))
    authorization = json.loads(contained(root, authorization_path).read_bytes())
    audit.authorize(root, plan, authorization)
    if (authorization.get('publication_proposal_id') != proposal['id']
            or proposal.get('destination') != POINTER
            or proposal.get('manifest') != plan.get('source_admission_manifest')):
        raise UnauthorizedOperation('Exact conditional hourly publication approval required')
    with execution_lease(root):
        relative = audit.HERE + '/' + plan['id']
        result_path, supervision_path = relative + '/execution/result.json', relative + '/supervision.json'
        result = read_record(contained(root, result_path))
        supervision = read_record(contained(root, supervision_path))
        result_hash = sha256_file(contained(root, result_path))
        if (result.get('status') != 'PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS'
                or supervision.get('status') != 'PASS_SUPERVISED_FOUNDATION_AUDIT'
                or result.get('plan_id') != plan['id'] or supervision.get('plan_id') != plan['id']
                or supervision.get('worker_result_sha256') != result_hash
                or result.get('source_admission_manifest_sha256') != plan['source_admission_manifest_sha256']):
            raise IntegrityError('Conditional hourly publication audit did not pass')
        directory = contained(root, proposal_path).parent / 'activation'
        directory.mkdir(exist_ok=False)
        budget = Budget(directory, audit.LIMITS)
        audit.verify_bindings(root, plan, budget)
        manifest = read_record(contained(root, proposal['manifest']))
        verify_bindings(root, manifest['bindings'])
        destination = contained(root, POINTER)
        before = sha256_file(destination) if destination.exists() else None
        if before != proposal['expected_destination_sha256']:
            raise IntegrityError('Hourly admission predecessor changed')
        previous = destination.read_bytes() if before is not None else None
        if previous is not None:
            (directory / 'previous_pointer.json').write_bytes(previous)
        pointer = signed(dict(schema='active_hourly_source_admission/1',
            manifest=dict(path=proposal['manifest'], sha256=plan['source_admission_manifest_sha256']),
            result=dict(path=result_path, sha256=result_hash),
            supervision=dict(path=supervision_path, sha256=sha256_file(contained(root, supervision_path)))))
        pending = directory / 'pending_pointer.json'
        pending.write_bytes(canonical_bytes(pointer) + b'\n')
        proposed_hash = sha256_file(pending)
        (directory / 'proposed_pointer.json').write_bytes(pending.read_bytes())
        # Recheck immediately before the only active mutation, under the publisher lease.
        if (sha256_file(destination) if destination.exists() else None) != before:
            raise IntegrityError('Hourly admission predecessor changed before commit')
        os.replace(pending, destination)
        status = dict(proposal_id=proposal['id'], plan_id=plan['id'], pointer_sha256=proposed_hash)
        try:
            reader = HourlySourceReader(root)
            examples = {case['case']: (case['request'], case['result'] == 'ACCEPTED')
                        for case in result['actual_window_reads']}
            if not examples or len(examples) != len(result['actual_window_reads']):
                raise IntegrityError('Integrated reader examples absent or ambiguous')
            checks = audit.check_reader_examples(reader, examples, budget)
            routes = audit.check_raw_routes(root, read_record(contained(root, 'data/causally_gated_normalized/_control/timeframes.json')))
            reader._unchanged()
            budget.check()
            status.update(status='ACTIVE_VERIFIED', window_checks=len(checks), raw_routes_refused=routes)
        except BaseException as exc:
            if sha256_file(destination) != proposed_hash:
                raise IntegrityError('Activation failed and pointer drift prevents safe rollback') from exc
            if previous is None:
                destination.unlink()  # Exact task-created pointer only; quality guard remains active.
            else:
                pending.write_bytes(previous)
                os.replace(pending, destination)
            status.update(status='ROLLED_BACK', error_type=type(exc).__name__, error=str(exc))
            (directory / 'result.json').write_bytes(canonical_bytes(signed(status)) + b'\n')
            raise
        (directory / 'result.json').write_bytes(canonical_bytes(signed(status)) + b'\n')
        return status


if __name__ == '__main__':
    main()
