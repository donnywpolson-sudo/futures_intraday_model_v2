"""The single reviewed live-model attachment point.

Replace this function with an explicit adapter import when a real model is ready,
then add that adapter and its exact dependencies to the PyInstaller spec.  There is
intentionally no path, pickle, joblib, entry-point, or configuration based loader.
"""

from __future__ import annotations

from .model_runtime import TrustedModelAdapter


def build_live_model_adapter() -> TrustedModelAdapter | None:
    return None
