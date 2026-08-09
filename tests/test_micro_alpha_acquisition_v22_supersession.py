from __future__ import annotations

import inspect
import json

import pytest

from futures_rebuild.canonical import sha256_json
from scripts import prepare_apex_micro_phase1a_acquisition_v22_supersession as subject


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_v22_supersession_reconstructs_and_preserves_unexecuted_outputs() -> None:
    stored = json.loads((subject.ROOT / subject.OUTPUT).read_text(encoding="utf-8"))
    assert subject.build_report(root=subject.ROOT) == stored
    core = dict(stored)
    report_id = core.pop("report_id")
    assert report_id == sha256_json(core)
    assert stored["state"] == "SUPERSEDED_PREPARATION_SELF_REFERENTIAL_CENSUS"
    assert stored["plan"]["provider_execution_performed"] is False
    assert stored["plan"]["authorization_consumed"] is False
    assert stored["disposition"]["execute_v22_plan"] is False
    assert stored["disposition"]["overwrite_or_delete_v22_artifacts"] is False
    assert stored["effects"]["provider_calls"] == 0
    assert stored["effects"]["downloads"] == 0
    assert stored["effects"]["dbn_rows_decoded"] == 0


def test_v22_supersession_has_no_provider_download_or_mutation_surface() -> None:
    source = inspect.getsource(subject)
    assert "api.env" not in source
    assert "get_range" not in source
    assert "get_cost" not in source
    assert "requests." not in source
    assert "unlink(" not in source
    assert "rmtree" not in source
