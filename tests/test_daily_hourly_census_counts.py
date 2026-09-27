"""Hourly missingness includes explicit gaps and has its own certified count."""
import importlib.util
from pathlib import Path
import pytest
from futures_rebuild.errors import IntegrityError

pytestmark=pytest.mark.current


def test_counts_match_each_certified_table_not_observation_count():
    root=Path(__file__).resolve().parents[1]
    spec=importlib.util.spec_from_file_location('daily_census_counts_test',root/
        'state/unpublished_evidence/daily_admission_v1/incremental_validation_census_v2.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    counts=dict(observations=3,cadence=3,quality=3,roll=3,missingness=5)
    retained=dict(totals={'table_rows_'+k:v for k,v in counts.items()})
    module.check_hourly_counts(counts,retained)
    with pytest.raises(IntegrityError): module.check_hourly_counts(dict(counts,missingness=3),retained)
    with pytest.raises(IntegrityError): module.check_hourly_counts(dict(counts,observations=4),retained)
    with pytest.raises(IntegrityError): module.check_hourly_counts({k:v for k,v in counts.items() if k!='roll'},retained)
