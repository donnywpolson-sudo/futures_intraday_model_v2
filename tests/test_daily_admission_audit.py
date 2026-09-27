"""Repeat-audit preparation binds sources but never reads their payloads."""
import pytest
from futures_rebuild import daily_admission_audit as audit
from futures_rebuild.canonical import canonical_bytes,sha256_file
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.errors import IntegrityError

pytestmark=pytest.mark.current


def test_prepare_binds_without_opening_sources(tmp_path,monkeypatch):
    def save(relative,value):
        path=tmp_path/relative
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(canonical_bytes(signed(value))+b'\n')
        return dict(path=relative,sha256=sha256_file(path))
    monkeypatch.setattr(audit.daily,'implementation_binding',lambda:{'frozen':'same'})
    monkeypatch.setattr(audit,'DailySourceReader',lambda *a,**k:pytest.fail('Preparation opened reader'))
    source=save('state/unpublished_evidence/source_plan.json',dict(source_bindings={'absent/raw.dbn':'not-read'}))
    manifest=save('state/unpublished_evidence/admission.json',dict(bindings={},
        quality={'path':'unopened-quality','sha256':'not-read'},daily_implementation={'frozen':'same'}))
    result=save('state/unpublished_evidence/result.json',dict(retained_source_plan=source))
    supervision=save('state/unpublished_evidence/supervision.json',dict(status='retained'))
    save(audit.POINTER,dict(manifest=manifest,audit=result,supervision=supervision))
    implementation=tmp_path/'src/futures_rebuild/daily_admission_audit.py'
    implementation.parent.mkdir(parents=True)
    implementation.write_text('# synthetic implementation binding')
    plan=audit.prepare(tmp_path)
    assert plan['historical_execution'] is False and plan['publication'] is False
    assert plan['source_bindings']=={'absent/raw.dbn':'not-read'}
    with (tmp_path/manifest['path']).open('ab') as stream: stream.write(b' ')
    with pytest.raises(IntegrityError): audit.prepare(tmp_path)


def test_recovery_prepare_retains_exact_sources_without_loading_quality(tmp_path,monkeypatch):
    def save(relative,value):
        path=tmp_path/relative
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(canonical_bytes(signed(value))+b'\n')
        return dict(path=relative,sha256=sha256_file(path))
    monkeypatch.setattr(audit.daily,'implementation_binding',lambda:{'frozen':'same'})
    monkeypatch.setattr(audit,'DailySourceReader',lambda *a,**k:pytest.fail('Preparation opened reader'))
    source=save('state/unpublished_evidence/source_plan.json',dict(source_bindings={'absent/annual.dbn':'not-read'}))
    monkeypatch.setattr(audit,'RETAINED_SOURCE_PLAN',source)
    manifest=save('state/unpublished_evidence/admission.json',dict(bindings={},
        quality={'path':'unopened-quality','sha256':'not-read'},daily_implementation={'frozen':'same'}))
    result=save('state/unpublished_evidence/result.json',dict(
        integrity='CERTIFIED_NATIVE_RECOVERY_AND_BOUND_REPLAY',recovered_rows=2,quality='unopened-quality'))
    supervision=save('state/unpublished_evidence/supervision.json',dict(status='retained'))
    save(audit.POINTER,dict(manifest=manifest,audit=result,supervision=supervision))
    implementation=tmp_path/'src/futures_rebuild/daily_admission_audit.py'
    implementation.parent.mkdir(parents=True)
    implementation.write_text('# synthetic implementation binding')
    plan=audit.prepare(tmp_path)
    assert plan['source_bindings']=={'absent/annual.dbn':'not-read'}
    assert plan['audit']==result and not plan['historical_execution'] and not plan['publication']
    with (tmp_path/source['path']).open('ab') as stream: stream.write(b' ')
    with pytest.raises(IntegrityError): audit.prepare(tmp_path)


@pytest.mark.parametrize('recovered',[0,1,2])
def test_current_accounting_preserves_exclusions(monkeypatch,recovered):
    baseline=dict(source_primary_rows=7,observations=4,quarantined=2,excluded_availability=1,excluded_start=0)
    monkeypatch.setattr(audit.daily,'BASELINE',baseline)
    expected=baseline|dict(observations=4+recovered,quarantined=2-recovered)
    quality=dict(baseline=baseline,rows=[{}]*(4+recovered),recovered_rows=recovered,
        current_source_accounting=expected,corrected_rows=0)
    assert audit.current_accounting(quality)==expected
    quality['current_source_accounting']=expected|{'excluded_availability':0}
    with pytest.raises(IntegrityError): audit.current_accounting(quality)
    quality['current_source_accounting']=expected
    quality['rows'].pop()
    with pytest.raises(IntegrityError): audit.current_accounting(quality)


def test_recovery_examples_use_bound_successor_expectations(tmp_path):
    path=tmp_path/'audit.json'
    checks=[dict(case='quarantine_ES',request={'market':'ES'},result='ACCEPTED'),
            dict(case='recovered_premature',request={'market':'ES'},result='REFUSED')]
    path.write_bytes(canonical_bytes(signed(dict(recovered_rows=2,daily_reader_checks=checks))))
    plan={'audit':dict(path='audit.json',sha256=sha256_file(path))}
    assert audit.reader_examples(tmp_path,plan,{},dict(recovered_rows=2))=={
        'quarantine_ES':({'market':'ES'},True),'recovered_premature':({'market':'ES'},False)}
    with pytest.raises(IntegrityError): audit.reader_examples(tmp_path,plan,{},dict(recovered_rows=1))
    path.write_bytes(canonical_bytes(signed(dict(recovered_rows=2,daily_reader_checks=checks+checks))))
    with pytest.raises(IntegrityError): audit.reader_examples(tmp_path,plan,{},dict(recovered_rows=2))
    plan['audit']['sha256']=sha256_file(path)
    with pytest.raises(IntegrityError): audit.reader_examples(tmp_path,plan,{},dict(recovered_rows=2))
