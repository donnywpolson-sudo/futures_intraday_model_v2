#!/usr/bin/env python3
"""PyInstaller-friendly entrypoint for Futures Live Cockpit."""

from __future__ import annotations

import multiprocessing

from futures_rebuild.live_cockpit.app import main


def run_entrypoint() -> int:
    multiprocessing.freeze_support()
    return main()


if __name__ == "__main__":
    raise SystemExit(run_entrypoint())
