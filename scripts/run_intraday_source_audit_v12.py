"""Run the resumable, registration-bound intraday source audit."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.intraday_alpha_execution_v12 import audit_registered_source_rows
from futures_rebuild.intraday_alpha_pipeline_v12 import build_failure_record, build_program, build_source_registration


root = Path(__file__).resolve().parents[1]
registration = build_source_registration(root, build_program(root, build_failure_record(root)))
print(json.dumps({"registration_id": registration["registration_id"], "scan": "RESUMING", "dbn_count": registration["dbn_count"]}, sort_keys=True), flush=True)
result = audit_registered_source_rows(root, registration)
print(json.dumps({"registration_id": registration["registration_id"], "audit": result}, sort_keys=True), flush=True)
