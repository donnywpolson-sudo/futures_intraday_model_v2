"""Print the offline Phase 6 binding and unregistered trial template."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.active_phase6_wfa import prepare_tier1_phase6_binding, run_tier1_phase6_prediction_only_wfa
from futures_rebuild.boundary import RepoBoundary


def main() -> int:
    if "--run" in __import__("sys").argv:
        print(json.dumps(run_tier1_phase6_prediction_only_wfa(boundary=RepoBoundary(active_root=Path.cwd())), sort_keys=True))
        return 0
    binding = prepare_tier1_phase6_binding(boundary=RepoBoundary(active_root=Path.cwd()))
    print(json.dumps(binding.trial_declaration_template(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
