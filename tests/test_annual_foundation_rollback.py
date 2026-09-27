"""Generated controls only: proposed annual order uses the existing rollback."""
from pathlib import Path
import pytest

from futures_rebuild.annual_foundation_integration import FILES
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError
from tests.test_daily_rollout_memory import helper

pytestmark = pytest.mark.current
ORDER = tuple('src/futures_rebuild/'+n for n in FILES) + (
    'configs/active_data_view_contract.json', 'data/active/catalog.json',
    'data/active/timeframes.json', 'configs/active_daily_recovery_reader_binding.json',
    'configs/active_hourly_quality_reader.json', 'configs/active_hourly_source_admission.json',
    'configs/active_daily_source_admission.json')


@pytest.mark.parametrize('failure', list(range(len(ORDER)))+['default','memory','deadline'])
def test_exact_annual_control_rollback(tmp_path, monkeypatch, failure):
    changes, expected = {}, {}
    for i, name in enumerate(ORDER):
        target = tmp_path/name
        target.parent.mkdir(parents=True,exist_ok=True)
        if i != len(ORDER)-1:
            target.write_bytes(('prior:'+name).encode())
        candidate = tmp_path/f'candidate_{i}'
        candidate.write_bytes(('new:'+name).encode())
        changes[name] = candidate.name
        expected[name] = sha256_file(target) if target.exists() else None
    actual_replace = helper.os.replace
    def replace(source, target):
        if type(failure) is int and Path(source).name == f'{failure}.pending':
            raise OSError('generated replacement failure')
        return actual_replace(source,target)
    monkeypatch.setattr(helper.os,'replace',replace)
    def validate():
        assert all((tmp_path/name).read_bytes() == ('new:'+name).encode() for name in ORDER)
        if isinstance(failure,str): raise IntegrityError('generated '+failure+' failure')
    with pytest.raises((OSError,IntegrityError)):
        helper.activate_controls(tmp_path,changes,expected=expected,rollback_directory='rollback',validate=validate)
    for name,digest in expected.items():
        if digest is None: assert not (tmp_path/name).exists()
        else: assert sha256_file(tmp_path/name) == digest
