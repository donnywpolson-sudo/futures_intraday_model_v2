"""Published daily audit windows. Reconstruction grants no research admission."""
from pathlib import Path
from .annual_foundation_independence import verify_reader

from .canonical import sha256_file
from .causal_1d import DAY, SECOND, CUTOFF, CONTRACT_ID, implementation_binding
from .causal_observation_parquet import read_bundle
from .causal_timeframe_storage import contained, read_record
from .causal_timeframe_publication import validate_index, validate_release, months
from .errors import ContractError, IntegrityError, UnauthorizedOperation


def require_coverage(coverage, *, start_ns, end_ns):
    days=[c for c in coverage if c['start_ns']<end_ns and c['end_ns']>start_ns]
    if [c['day_ns'] for c in days]!=list(range(start_ns,end_ns,DAY)):
        raise UnauthorizedOperation('Daily coverage evidence is missing or duplicated')
    for c in days:
        if (c.get('state')!='OBSERVED_WITH_EXPLICIT_GAPS' or c.get('quarantined')!=0
                or c.get('quarantine')!=[] or c.get('observations')!=1
                or c.get('source_primary_rows')!=1 or c.get('excluded_start')!=0
                or c.get('excluded_availability')!=0
                or c['start_ns']!=c['day_ns'] or c['end_ns']!=c['day_ns']+DAY):
            raise UnauthorizedOperation('Daily window includes unavailable or quarantined coverage')


class DailyReader:
    """Read only certified published native rows, labeled for lineage auditing."""

    def __init__(self, repository_root):
        self.root=Path(repository_root).resolve()
        self.independence_bindings={}
        self.index_path=contained(self.root,'data/causally_gated_normalized/_control/timeframes.json')
        self.index_hash=sha256_file(self.index_path)
        self.index=read_record(self.index_path)
        validate_index(self.root,self.index)
        verify_reader(self.root,self.independence_bindings)

    def read_window(self, *, market, start_ns, end_ns, as_of_ns, source='native', purpose='LINEAGE_AUDIT'):
        if source!='native':
            raise UnauthorizedOperation('No derived daily source is admitted')
        if purpose not in ('LINEAGE_AUDIT','SYNTHETIC_TEST'):
            raise UnauthorizedOperation('Daily reconstruction grants no research permission')
        if (any(type(v) is not int for v in (start_ns,end_ns,as_of_ns))
                or not 0<start_ns<end_ns<=CUTOFF or start_ns%DAY or end_ns%DAY or as_of_ns<=0):
            raise ContractError('Daily window must be aligned and within development coverage')
        if sha256_file(self.index_path)!=self.index_hash:
            raise IntegrityError('Daily index changed during reader use')
        validate_index(self.root,self.index)
        verify_reader(self.root,self.independence_bindings)
        result=[]
        checked=[]
        recovered_evidence={}
        from .annual_foundation_release import read_months
        selected_months = months(start_ns, end_ns)
        annual = [e for e in self.index['entries'] if e['timeframe']=='1d'
                  and e['market']==market and e['month'] in selected_months and 'annual_release' in e]
        annual_bundles = read_months(self.root, annual)
        for month in months(start_ns,end_ns):
            entries=[e for e in self.index['entries'] if (e['timeframe'],e['market'],e['month'])==('1d',market,month)]
            if len(entries)!=1 or entries[0]['disposition']!='CERTIFIED':
                raise UnauthorizedOperation('Native daily month is unavailable')
            entry=entries[0]
            if 'annual_release' in entry:
                from .annual_foundation_independence import daily_metadata
                metadata = daily_metadata(self.root, entry)
                allowed, coverage = metadata['purpose'], metadata['coverage']
                for evidence in metadata['recovery_evidence']:
                    if evidence['row_id'] in recovered_evidence:
                        raise IntegrityError('Duplicate daily recovery evidence')
                    recovered_evidence[evidence['row_id']] = evidence
            else:
                release=validate_release(self.root,entry)
                recovered = release.get('schema')=='daily_identity_recovery_month/1'
                if recovered:
                    from .daily_identity_publication import certificate
                    cert = certificate(self.root, release['certificate'])
                    allowed = 'SYNTHETIC_TEST' if cert['synthetic_only'] else 'LINEAGE_AUDIT'
                    coverage = release['coverage']
                    for evidence in release['evidence']:
                        if evidence['row_id'] in recovered_evidence:
                            raise IntegrityError('Duplicate daily recovery evidence')
                        recovered_evidence[evidence['row_id']] = evidence
                else:
                    from .daily_identity_publication import legacy_identity
                    candidate=release['candidate']
                    allowed='SYNTHETIC_TEST' if candidate['spec']['synthetic_only'] else 'LINEAGE_AUDIT'
                    if (not legacy_identity(self.root,self.index,candidate)
                            or candidate['source_accounting']!=release['certificate']['passes'][0]['source_accounting']):
                        raise IntegrityError('Daily implementation or accounting binding differs')
                    coverage=release['seal']['coverage']
            if purpose!=allowed or (allowed=='LINEAGE_AUDIT' and 'publication' not in self.index):
                raise UnauthorizedOperation('Daily source purpose or publication authority differs')
            left=max(start_ns,coverage[0]['day_ns'])
            right=min(end_ns,coverage[-1]['day_ns']+DAY)
            require_coverage(coverage,start_ns=left,end_ns=right)
            bundle=(annual_bundles[month] if 'annual_release' in entry
                    else read_bundle(contained(self.root,entry['directory'])))
            observations=bundle['observations']
            quality=bundle['quality']
            labels={q['row_id']:q for q in quality}
            if len(labels)!=len(quality) or len(observations)!=len(labels) or set(labels)!={r['row_id'] for r in observations}:
                raise IntegrityError('Daily observation-quality correspondence differs')
            for row in observations:
                if start_ns<=row['bar_start_ns']<end_ns:
                    result.append(dict(row,origin='NATIVE_1D',quality_disposition='CAUSAL_RECONSTRUCTION_AUDIT_ONLY',
                                       quality_evidence=labels[row['row_id']]))
            checked.append(entry)
        if ([r['bar_start_ns'] for r in result]!=list(range(start_ns,end_ns,DAY))
                or any(r['market']!=market or r['source_cadence']!='1d' or r['bar_end_ns']!=r['bar_start_ns']+DAY for r in result)):
            raise UnauthorizedOperation('Daily observations have gaps or invalid boundaries')
        identity=('market','publisher_id','instrument_id','actual_contract','currency','min_price_increment_nano','multiplier_nano')
        for row in result:
            if any(row[k]!=result[0][k] for k in identity):
                raise UnauthorizedOperation('Daily window crosses a contract or unit change')
            if row['row_id'] in recovered_evidence:
                from .daily_identity_recovery import verify_recovered_timing
                verify_recovered_timing(row,row['quality_evidence'],recovered_evidence[row['row_id']])
            elif row['definition_received_at_ns']>row['bar_start_ns']:
                raise IntegrityError('Late daily identity lacks certified recovery evidence')
            if (row['available_at_ns']<row['bar_end_ns']+5*SECOND
                    or row['decision_eligible_at_ns']<row['available_at_ns']):
                raise IntegrityError('Daily causal timing differs')
            if row['decision_eligible_at_ns']>as_of_ns:
                raise UnauthorizedOperation('Daily observation is not yet available')
        for entry in checked:
            if 'annual_release' in entry:
                daily_metadata(self.root, entry)
            else:
                validate_release(self.root,entry)
        validate_index(self.root,self.index)
        verify_reader(self.root,self.independence_bindings)
        if sha256_file(self.index_path)!=self.index_hash:
            raise IntegrityError('Daily index changed during read')
        return result
