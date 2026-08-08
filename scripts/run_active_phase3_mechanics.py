"""Run the one approved bounded ES 2019 Phase 3 mechanics check."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.active_phase3_mechanics import run_active_phase3_mechanics_check
from futures_rebuild.active_phase3_validation import prepare_active_phase3_mechanics_validation
from futures_rebuild.boundary import RepoBoundary


def main() -> None:
    boundary = RepoBoundary(active_root=Path.cwd())
    validation = prepare_active_phase3_mechanics_validation(boundary=boundary)
    report = run_active_phase3_mechanics_check(boundary=boundary, validation=validation)
    print(
        json.dumps(
            {
                "report_id": report.report_id,
                "report_path": report.report_relative_path,
                "row_read_cap": report.row_read_cap,
                "rows_read": report.rows_read,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
