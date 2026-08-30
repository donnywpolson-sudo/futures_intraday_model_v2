from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_sr3_remediation import prepare_package


if __name__ == "__main__":
    print(json.dumps(prepare_package(Path(__file__).resolve().parents[1]), sort_keys=True))
