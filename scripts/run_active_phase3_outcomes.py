from __future__ import annotations
import json
from pathlib import Path
from futures_rebuild.active_phase3_outcomes import build_active_phase3_outcomes
from futures_rebuild.active_phase3_validation import prepare_active_phase3_mechanics_validation
from futures_rebuild.boundary import RepoBoundary

if __name__ == "__main__":
    boundary = RepoBoundary(active_root=Path.cwd())
    validation = prepare_active_phase3_mechanics_validation(boundary=boundary)
    print(json.dumps(build_active_phase3_outcomes(boundary=boundary, validation=validation), sort_keys=True))
