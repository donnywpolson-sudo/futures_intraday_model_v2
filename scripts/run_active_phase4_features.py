"""Run the one approved bounded active-view Phase 4 feature build."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.active_phase4_features import (
    build_active_phase4_features,
    prepare_active_phase4_feature_binding,
)
from futures_rebuild.boundary import RepoBoundary


def main() -> int:
    boundary = RepoBoundary(active_root=Path.cwd())
    binding = prepare_active_phase4_feature_binding(boundary=boundary)
    result = build_active_phase4_features(boundary=boundary, binding=binding)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
