"""Repository-owned, constrained loader for a fixed sealed forecast artifact."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .boundary import RepoBoundary
from .bundle import BundleMetadata, verify_bundle
from .canonical import sha256_file, sha256_json
from .errors import ContractError, IntegrityError


ARTIFACT_FORMAT = "FUTURES_TRUSTED_LINEAR_FORECAST_V1"
_CONSTRUCTION_TOKEN = object()
TRUSTED_RUNTIME_MODULES = (
    "boundary.py",
    "bundle.py",
    "canonical.py",
    "clock.py",
    "economics.py",
    "identity.py",
    "inference.py",
    "predictor.py",
    "release.py",
    "schemas.py",
    "session_policy.py",
    "time_contracts.py",
)
INFERENCE_RUNTIME_CONFIG = Path("configs/inference_runtime.json")


@dataclass(frozen=True)
class TrustedRawForecast:
    expected_return: float
    probability_up: float
    probability_down: float
    probability_neutral: float
    uncertainty: float

    def as_dict(self) -> dict[str, float]:
        return {
            "expected_return": self.expected_return,
            "probability_down": self.probability_down,
            "probability_neutral": self.probability_neutral,
            "probability_up": self.probability_up,
            "uncertainty": self.uncertainty,
        }


def trusted_loader_code_hash() -> str:
    return sha256_file(Path(__file__))


def trusted_runtime_code_hash() -> str:
    root = Path(__file__).parent
    return sha256_json(
        [
            {"path": name, "sha256": sha256_file(root / name)}
            for name in TRUSTED_RUNTIME_MODULES
        ]
    )


def trusted_runtime_config_hash(boundary: RepoBoundary) -> str:
    path = boundary.assert_active_path(
        boundary.active_root / INFERENCE_RUNTIME_CONFIG,
        purpose="trusted inference runtime config",
        subtree="configs",
    )
    return sha256_file(path)


def trusted_dependency_lock_hash(boundary: RepoBoundary) -> str:
    path = boundary.assert_active_path(
        boundary.active_root / "configs" / "dependency_lock_receipt.json",
        purpose="trusted dependency lock receipt",
        subtree="configs",
    )
    return sha256_file(path)


class TrustedPredictor:
    """Predict-only value object with no path, network, fit, or order capability."""

    __slots__ = (
        "artifact_sha256",
        "bundle_id",
        "environment_hash",
        "feature_names",
        "_expected_return_scale",
        "_intercept",
        "_sealed",
        "_uncertainty",
        "_weights",
    )

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("trusted predictor is immutable after sealed loading")
        object.__setattr__(self, name, value)

    def __init__(
        self,
        token: object,
        *,
        artifact_sha256: str,
        bundle_id: str,
        environment_hash: str,
        feature_names: tuple[str, ...],
        weights: tuple[float, ...],
        intercept: float,
        expected_return_scale: float,
        uncertainty: float,
    ) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise ContractError("trusted predictors can only be constructed by the sealed loader")
        self.artifact_sha256 = artifact_sha256
        self.bundle_id = bundle_id
        self.environment_hash = environment_hash
        self.feature_names = feature_names
        self._weights = weights
        self._intercept = intercept
        self._expected_return_scale = expected_return_scale
        self._uncertainty = uncertainty
        self._sealed = True

    def predict_one(
        self, features: Mapping[str, float | int | bool | None]
    ) -> TrustedRawForecast:
        if tuple(features) != self.feature_names:
            raise ContractError("predictor received a nonsealed feature order")
        values: list[float] = []
        for name in self.feature_names:
            raw = features[name]
            if raw is None or isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ContractError("trusted predictor requires finite numeric features")
            value = float(raw)
            if not math.isfinite(value):
                raise ContractError("trusted predictor received a nonfinite feature")
            values.append(value)
        score = self._intercept + sum(
            weight * value for weight, value in zip(self._weights, values, strict=True)
        )
        probability_up = 1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, score))))
        return TrustedRawForecast(
            expected_return=score * self._expected_return_scale,
            probability_up=probability_up,
            probability_down=1.0 - probability_up,
            probability_neutral=0.0,
            uncertainty=self._uncertainty,
        )


class TrustedPredictorLoader:
    @staticmethod
    def load(bundle_path: Path, *, boundary: RepoBoundary) -> TrustedPredictor:
        manifest = verify_bundle(bundle_path, boundary=boundary)
        metadata = BundleMetadata.from_dict(manifest["metadata"])  # type: ignore[arg-type]
        if metadata.loader_code_hash != trusted_loader_code_hash():
            raise IntegrityError("sealed bundle loader code hash is not the running loader")
        if metadata.code_hash != trusted_runtime_code_hash():
            raise IntegrityError("sealed bundle runtime code hash is not the running code tree")
        if metadata.config_hash != trusted_runtime_config_hash(boundary):
            raise IntegrityError("sealed bundle config hash is not the active runtime config")
        environment_path = boundary.active_root / "configs" / "environment.lock.json"
        if (
            sha256_file(environment_path) != metadata.environment_hash
            or trusted_dependency_lock_hash(boundary) != metadata.dependency_lock_hash
        ):
            raise IntegrityError("sealed environment/dependency hashes are not current")
        artifact_path = bundle_path / "model.artifact"
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("trusted model artifact is not canonical JSON") from exc
        expected = {
            "artifact_format",
            "expected_return_scale",
            "feature_names",
            "intercept",
            "parity_input",
            "parity_output",
            "uncertainty",
            "weights",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise IntegrityError("trusted model artifact schema is invalid")
        try:
            feature_names = tuple(str(item) for item in payload["feature_names"])
            weights = tuple(float(item) for item in payload["weights"])
            parity_input = tuple(float(item) for item in payload["parity_input"])
            intercept = float(payload["intercept"])
            scale = float(payload["expected_return_scale"])
            uncertainty = float(payload["uncertainty"])
        except (TypeError, ValueError) as exc:
            raise IntegrityError("trusted model artifact values are invalid") from exc
        if (
            feature_names != metadata.feature_names
            or len(weights) != len(feature_names)
            or len(parity_input) != len(feature_names)
            or any(not math.isfinite(value) for value in (*weights, *parity_input, intercept, scale, uncertainty))
            or scale <= 0
            or uncertainty < 0
        ):
            raise IntegrityError("trusted model artifact shape or values are invalid")
        predictor = TrustedPredictor(
            _CONSTRUCTION_TOKEN,
            artifact_sha256=str(manifest["artifact_sha256"]),
            bundle_id=str(manifest["bundle_id"]),
            environment_hash=metadata.environment_hash,
            feature_names=feature_names,
            weights=weights,
            intercept=intercept,
            expected_return_scale=scale,
            uncertainty=uncertainty,
        )
        observed = predictor.predict_one(
            MappingProxyType(dict(zip(feature_names, parity_input, strict=True)))
        ).as_dict()
        expected_parity = payload["parity_output"]
        if not isinstance(expected_parity, dict) or set(expected_parity) != set(observed):
            raise IntegrityError("trusted model reload parity schema is invalid")
        try:
            if any(
                not math.isclose(observed[key], float(expected_parity[key]), rel_tol=1e-14, abs_tol=1e-14)
                for key in observed
            ):
                raise IntegrityError("trusted model reload parity failed")
        except (TypeError, ValueError) as exc:
            raise IntegrityError("trusted model reload parity values are invalid") from exc
        return predictor
