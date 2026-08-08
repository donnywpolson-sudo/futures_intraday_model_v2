"""Build the paired active-view releases on their common causal decision grid."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.active_phase3_outcomes import build_active_phase3_outcomes
from futures_rebuild.active_phase3_validation import prepare_active_phase3_mechanics_validation
from futures_rebuild.active_phase4_features import (
    build_active_phase4_features,
    prepare_active_phase4_feature_binding,
)
from futures_rebuild.boundary import RepoBoundary


def main() -> int:
    boundary = RepoBoundary(active_root=Path.cwd())
    phase3 = build_active_phase3_outcomes(
        boundary=boundary,
        validation=prepare_active_phase3_mechanics_validation(boundary=boundary),
    )
    phase4 = build_active_phase4_features(
        boundary=boundary,
        binding=prepare_active_phase4_feature_binding(boundary=boundary),
    )
    print(json.dumps({"phase3": phase3, "phase4": phase4}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
