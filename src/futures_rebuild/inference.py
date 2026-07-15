"""Trusted-clock, verified-release, predict-only inference with abstention."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .bundle import BundleClassification, BundleMetadata, verify_bundle
from .canonical import sha256_json
from .clock import ProductionClock, SyntheticClock, TrustedClock, require_trusted_clock
from .economics import VerifiedContractEconomics, VerifiedEconomicsRegistry
from .errors import ContractError, IntegrityError
from .identity import ContractDefinition, DefinitionObservation, ActualContractIdentity
from .predictor import TrustedPredictor, TrustedPredictorLoader, TrustedRawForecast
from .release import VerifiedReleaseReceipt
from .schemas import FeatureRow, PredictionRow, prediction_id_for
from .time_contracts import require_utc


RawForecast = TrustedRawForecast
_VERIFIED_IDENTITY_FACTORY = object()


@dataclass(frozen=True)
class InferencePolicy:
    max_input_age_seconds: int
    minimum_absolute_expected_return: float
    maximum_uncertainty: float
    minimum_direction_probability: float

    def __post_init__(self) -> None:
        if self.max_input_age_seconds <= 0:
            raise ContractError("maximum input age must be positive")
        values = (
            self.minimum_absolute_expected_return,
            self.maximum_uncertainty,
            self.minimum_direction_probability,
        )
        if any(not math.isfinite(value) for value in values):
            raise ContractError("inference thresholds must be finite")
        if self.minimum_absolute_expected_return < 0 or self.maximum_uncertainty < 0:
            raise ContractError("return and uncertainty thresholds cannot be negative")
        if not 0 <= self.minimum_direction_probability <= 1:
            raise ContractError("direction probability threshold must be between zero and one")

    def as_dict(self) -> dict[str, object]:
        return {
            "max_input_age_seconds": self.max_input_age_seconds,
            "maximum_uncertainty": self.maximum_uncertainty,
            "minimum_absolute_expected_return": self.minimum_absolute_expected_return,
            "minimum_direction_probability": self.minimum_direction_probability,
        }

    @property
    def policy_hash(self) -> str:
        return sha256_json(self.as_dict())


@dataclass(frozen=True)
class VerifiedIdentityRegistry:
    release_receipt: VerifiedReleaseReceipt
    definitions: Mapping[str, DefinitionObservation]
    registry_hash: str
    boundary: RepoBoundary
    _factory_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _VERIFIED_IDENTITY_FACTORY:
            raise ContractError(
                "verified identity registry can only be created from a verified release"
            )

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "VerifiedIdentityRegistry":
        manifest = receipt.verify(boundary)
        if manifest.release_kind != "actual_contract_definitions":
            raise IntegrityError("definition receipt has the wrong release kind")
        if {entry.path for entry in manifest.files} != {"identities.json"}:
            raise IntegrityError("definition release must contain exactly identities.json")
        path = boundary.active_root / receipt.relative_root / "identities.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("definition registry JSON is invalid") from exc
        schema_version = payload.get("schema_version") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or set(payload) != {"records", "schema_version"}
            or schema_version not in {"1.0.0", "1.1.0"}
            or not isinstance(payload.get("records"), list)
            or not payload["records"]
        ):
            raise IntegrityError("definition registry schema/version is invalid")
        expected = {
            "available_at",
            "currency",
            "dataset",
            "effective_at",
            "exchange",
            "instrument_id",
            "min_tick",
            "multiplier",
            "publisher_id",
            "raw_symbol",
            "source_received_at",
        }
        bridge_fields = {
            "market",
            "provider_definition_manifest_sha256",
            "provider_definition_release_id",
            "provider_definition_row_sha256",
            "provider_definition_ts_event_ns",
            "provider_definition_ts_recv_ns",
            "provider_min_price_increment_nano",
            "provider_source_file_path",
            "provider_source_file_sha256",
            "provider_unit_of_measure",
            "provider_unit_of_measure_qty_nano",
        }
        if schema_version == "1.1.0":
            expected |= bridge_fields
        definitions: dict[str, DefinitionObservation] = {}
        for raw in payload["records"]:
            if not isinstance(raw, dict) or set(raw) != expected:
                raise IntegrityError("definition row schema is invalid")
            if (
                any(
                    type(raw[name]) is not str
                    for name in (
                        "available_at",
                        "currency",
                        "dataset",
                        "effective_at",
                        "exchange",
                        "min_tick",
                        "multiplier",
                        "raw_symbol",
                        "source_received_at",
                    )
                )
                or any(
                    type(raw[name]) is not int
                    for name in ("instrument_id", "publisher_id")
                )
                or (
                    schema_version == "1.1.0"
                    and (
                        any(
                            type(raw[name]) is not str
                            for name in bridge_fields.difference(
                                {
                                    "provider_definition_ts_event_ns",
                                    "provider_definition_ts_recv_ns",
                                    "provider_min_price_increment_nano",
                                    "provider_unit_of_measure_qty_nano",
                                }
                            )
                        )
                        or any(
                            type(raw[name]) is not int
                            for name in (
                                "provider_definition_ts_event_ns",
                                "provider_definition_ts_recv_ns",
                                "provider_min_price_increment_nano",
                                "provider_unit_of_measure_qty_nano",
                            )
                        )
                    )
                )
            ):
                raise IntegrityError("definition row field types are not exact")
            row_id = sha256_json(raw)
            try:
                definition = ContractDefinition(
                    dataset=raw["dataset"],
                    publisher_id=raw["publisher_id"],
                    instrument_id=raw["instrument_id"],
                    raw_symbol=raw["raw_symbol"],
                    exchange=raw["exchange"],
                    definition_release_id=receipt.release_id,
                    definition_manifest_sha256=receipt.manifest_sha256,
                    definition_row_id=row_id,
                    currency=raw["currency"],
                    multiplier=Decimal(raw["multiplier"]),
                    min_tick=Decimal(raw["min_tick"]),
                )
                observation = DefinitionObservation(
                    definition=definition,
                    effective_at=datetime.fromisoformat(raw["effective_at"]),
                    source_received_at=datetime.fromisoformat(raw["source_received_at"]),
                    available_at=datetime.fromisoformat(raw["available_at"]),
                    source_release_id=receipt.release_id,
                )
            except (
                KeyError,
                TypeError,
                ValueError,
                InvalidOperation,
                ContractError,
            ) as exc:
                raise IntegrityError("definition row is not verified") from exc
            if row_id in definitions:
                raise IntegrityError("definition registry contains duplicate row identities")
            definitions[row_id] = observation
        core = {
            "definitions": {
                key: {
                    "definition": definitions[key].definition.as_dict(),
                    "effective_at": definitions[key].effective_at.isoformat(),
                    "source_received_at": definitions[key].source_received_at.isoformat(),
                    "available_at": definitions[key].available_at.isoformat(),
                }
                for key in sorted(definitions)
            },
            "release_receipt": receipt.as_dict(),
        }
        return cls(
            receipt,
            MappingProxyType(definitions),
            sha256_json(core),
            boundary,
            _VERIFIED_IDENTITY_FACTORY,
        )

    @property
    def release_ids(self) -> tuple[str, ...]:
        return (self.release_receipt.release_id,)

    def verify(self) -> None:
        rebuilt = type(self).from_release(self.release_receipt, self.boundary)
        if (
            rebuilt.registry_hash != self.registry_hash
            or dict(rebuilt.definitions) != dict(self.definitions)
        ):
            raise IntegrityError("definition registry changed after verification")

    def contains(
        self,
        actual: ActualContractIdentity,
        bar_event_at: datetime,
        decision_at: datetime,
    ) -> bool:
        self.verify()
        event = require_utc(bar_event_at, "bar_event_at")
        decision = require_utc(decision_at, "decision_at")
        if event > decision:
            return False
        observation = self.definitions.get(actual.definition_row_id)
        if observation is None:
            return False
        definition = observation.definition
        return (
            definition.dataset == actual.dataset
            and definition.publisher_id == actual.publisher_id
            and definition.instrument_id == actual.instrument_id
            and definition.raw_symbol == actual.raw_symbol
            and definition.exchange == actual.exchange
            and definition.definition_release_id == actual.definition_release_id
            and definition.definition_manifest_sha256
            == actual.definition_manifest_sha256
            and definition.currency == actual.currency
            and definition.multiplier == actual.multiplier
            and definition.min_tick == actual.min_tick
            and observation.effective_at <= event
            and observation.source_received_at <= decision
            and observation.available_at <= decision
        )


class InferenceAdapter:
    def __init__(
        self,
        *,
        bundle_path: Path,
        policy: InferencePolicy,
        identity_registry: VerifiedIdentityRegistry,
        economics_registry: VerifiedEconomicsRegistry,
        predictor: TrustedPredictor,
        clock: TrustedClock,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
    ) -> None:
        operation_receipt.verify(boundary, operation="INFER")
        self.boundary = boundary
        self.operation_receipt = operation_receipt
        self.clock = require_trusted_clock(
            clock,
            boundary=boundary,
            operation_receipt=operation_receipt,
            allow_synthetic=(
                operation_receipt.classification
                is OperationClassification.SYNTHETIC_MECHANICS_ONLY
            ),
        )
        manifest = verify_bundle(bundle_path, boundary=boundary)
        metadata = BundleMetadata.from_dict(manifest["metadata"])  # type: ignore[arg-type]
        expected_classification = (
            OperationClassification.SYNTHETIC_MECHANICS_ONLY
            if metadata.bundle_classification
            is BundleClassification.SYNTHETIC_MECHANICS_ONLY
            else OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION
        )
        operation_receipt.verify(
            boundary,
            operation="INFER",
            classification=expected_classification,
        )
        expected_clock_type = (
            SyntheticClock
            if metadata.bundle_classification
            is BundleClassification.SYNTHETIC_MECHANICS_ONLY
            else ProductionClock
        )
        if type(self.clock) is not expected_clock_type:
            raise ContractError("inference clock type differs from bundle classification")
        if (
            metadata.bundle_classification is BundleClassification.CANDIDATE
            and metadata.candidate_provenance is None
        ):
            raise ContractError("candidate inference lacks passed-trial readiness provenance")
        if metadata.decision_policy_hash != policy.policy_hash:
            raise ContractError("inference policy differs from the sealed bundle")
        identity_registry.verify()
        economics_registry.verify()
        if tuple(item.receipt_id for item in metadata.definition_release_receipts) != (
            identity_registry.release_receipt.receipt_id,
        ):
            raise ContractError("identity registry receipt differs from the sealed bundle")
        if tuple(item.receipt_id for item in metadata.economics_release_receipts) != (
            economics_registry.release_receipt.receipt_id,
        ):
            raise ContractError("economics registry receipt differs from the sealed bundle")
        if not isinstance(predictor, TrustedPredictor):
            raise ContractError("arbitrary Python predictors are forbidden")
        if (
            predictor.artifact_sha256 != manifest["artifact_sha256"]
            or predictor.bundle_id != manifest["bundle_id"]
            or predictor.environment_hash != metadata.environment_hash
        ):
            raise ContractError("predictor is not loaded from this exact sealed bundle")
        bundle_id = str(manifest["bundle_id"])
        candidate_provenance_id = (
            None
            if metadata.candidate_provenance is None
            else metadata.candidate_provenance.provenance_id
        )
        authorization_scope = (
            {}
            if metadata.bundle_classification
            is BundleClassification.SYNTHETIC_MECHANICS_ONLY
            else {
                "bundle_id": bundle_id,
                "candidate_provenance_id": candidate_provenance_id or "",
                "decision_policy_hash": metadata.decision_policy_hash,
            }
        )
        if metadata.bundle_classification is BundleClassification.CANDIDATE:
            operation_receipt.consume(
                boundary,
                operation="INFER",
                classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
                required_scope=authorization_scope,
            )
            operation_receipt.assert_consumed(
                boundary,
                operation="INFER",
                classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
                required_scope=authorization_scope,
            )
        else:
            operation_receipt.verify(
                boundary,
                operation="INFER",
                classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
                required_scope=authorization_scope,
            )
        self.bundle_id = bundle_id
        self.bundle_path = boundary.assert_active_path(
            bundle_path, purpose="inference bundle", subtree="bundles"
        )
        self.feature_names = metadata.feature_names
        self.bundle_classification = metadata.bundle_classification
        self.candidate_provenance_id = candidate_provenance_id
        self.production_eligible = (
            metadata.bundle_classification is BundleClassification.CANDIDATE
        )
        self.policy = policy
        self.identity_registry = identity_registry
        self.economics_registry = economics_registry
        self.predictor = predictor
        self.authorization_scope = MappingProxyType(dict(authorization_scope))
        self.allowed_source_receipts = {
            item.release_id: item
            for item in metadata.inference_source_release_receipts
        }

    def infer(self, row: FeatureRow) -> PredictionRow:
        """Infer at the trusted clock time; no caller time override exists."""

        self.operation_receipt.verify(
            self.boundary,
            operation="INFER",
            classification=(
                OperationClassification.SYNTHETIC_MECHANICS_ONLY
                if self.bundle_classification
                is BundleClassification.SYNTHETIC_MECHANICS_ONLY
                else OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION
            ),
            required_scope=self.authorization_scope,
        )
        if self.bundle_classification is BundleClassification.CANDIDATE:
            self.operation_receipt.assert_consumed(
                self.boundary,
                operation="INFER",
                classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
                required_scope=self.authorization_scope,
            )
        now = self.clock.now()
        manifest = verify_bundle(self.bundle_path, boundary=self.boundary)
        metadata = BundleMetadata.from_dict(manifest["metadata"])  # type: ignore[arg-type]
        reasons: list[str] = []
        economics: VerifiedContractEconomics | None = None
        current_predictor: TrustedPredictor | None = None
        try:
            current_predictor = TrustedPredictorLoader.load(
                self.bundle_path, boundary=self.boundary
            )
            if (
                current_predictor.artifact_sha256 != self.predictor.artifact_sha256
                or current_predictor.bundle_id != self.predictor.bundle_id
                or current_predictor.environment_hash != self.predictor.environment_hash
            ):
                reasons.append("PREDICTOR_IDENTITY_MISMATCH")
        except (ContractError, IntegrityError):
            reasons.append("PREDICTOR_INTEGRITY_FAILURE")
        try:
            for receipt in row.verified_release_receipts:
                receipt.verify(self.boundary)
        except (ContractError, IntegrityError):
            reasons.append("RELEASE_TAMPER_OR_MISMATCH")
        if metadata.decision_policy_hash != self.policy.policy_hash:
            reasons.append("POLICY_HASH_MISMATCH")
        sealed_source = self.allowed_source_receipts.get(row.source_release_id)
        if sealed_source is None or sealed_source != row.source_release_receipt:
            reasons.append("SOURCE_RELEASE_MISMATCH")
        if tuple(row.values) != self.feature_names:
            reasons.append("FEATURE_SCHEMA_MISMATCH")
        if not self.identity_registry.contains(
            row.actual, row.bar_event_at, row.decision_at
        ):
            reasons.append("UNKNOWN_ACTUAL_CONTRACT")
        try:
            economics = self.economics_registry.resolve(row.actual, row.decision_at)
        except (ContractError, IntegrityError):
            reasons.append("MISSING_OR_AMBIGUOUS_ECONOMICS")
        if not row.inputs_complete:
            reasons.append("INCOMPLETE_INPUTS")
        if now < row.decision_at:
            reasons.append("DECISION_NOT_REACHED")
        elif now >= row.planned_entry_at:
            reasons.append("ENTRY_WINDOW_CLOSED")
        elif now - row.available_at_max > timedelta(
            seconds=self.policy.max_input_age_seconds
        ):
            reasons.append("STALE_INPUT")
        economics_id = economics.record_id if economics is not None else "0" * 64
        prediction_id = prediction_id_for(
            bundle_id=self.bundle_id,
            actual=row.actual,
            decision_at=row.decision_at,
            recorded_at=now,
            source_release_id=row.source_release_id,
            source_release_receipt_id=row.source_release_receipt.receipt_id,
            economics_record_id=economics_id,
            feature_row_id=row.row_id,
            planned_entry_at=row.planned_entry_at,
            label_unlock_at=row.label_unlock_at,
            bundle_classification=self.bundle_classification.value,
            candidate_provenance_id=self.candidate_provenance_id,
            production_eligible=self.production_eligible,
        )
        if reasons:
            return self._abstention(row, prediction_id, economics_id, now, reasons)
        try:
            if current_predictor is None:
                raise IntegrityError("trusted predictor failed reload verification")
            raw = current_predictor.predict_one(row.values)
            values = (
                raw.expected_return,
                raw.probability_up,
                raw.probability_down,
                raw.probability_neutral,
                raw.uncertainty,
            )
            if any(not math.isfinite(value) for value in values):
                raise ValueError("non-finite forecast")
            threshold_reasons: list[str] = []
            if abs(raw.expected_return) < self.policy.minimum_absolute_expected_return:
                threshold_reasons.append("BELOW_RETURN_THRESHOLD")
            if raw.uncertainty > self.policy.maximum_uncertainty:
                threshold_reasons.append("UNCERTAINTY_TOO_HIGH")
            if max(raw.probability_up, raw.probability_down) < self.policy.minimum_direction_probability:
                threshold_reasons.append("DIRECTION_PROBABILITY_TOO_LOW")
            if threshold_reasons:
                return self._abstention(
                    row, prediction_id, economics_id, now, threshold_reasons
                )
            return PredictionRow(
                prediction_id=prediction_id,
                bundle_id=self.bundle_id,
                actual=row.actual,
                decision_at=row.decision_at,
                recorded_at=now,
                source_release_id=row.source_release_id,
                source_release_receipt_id=row.source_release_receipt.receipt_id,
                economics_record_id=economics_id,
                feature_row_id=row.row_id,
                planned_entry_at=row.planned_entry_at,
                label_unlock_at=row.label_unlock_at,
                abstained=False,
                abstention_reasons=(),
                expected_return=raw.expected_return,
                probability_up=raw.probability_up,
                probability_down=raw.probability_down,
                probability_neutral=raw.probability_neutral,
                uncertainty=raw.uncertainty,
                bundle_classification=self.bundle_classification.value,
                candidate_provenance_id=self.candidate_provenance_id,
                production_eligible=self.production_eligible,
            )
        except Exception:
            return self._abstention(
                row, prediction_id, economics_id, now, ["PREDICTOR_FAILURE"]
            )

    def _abstention(
        self,
        row: FeatureRow,
        prediction_id: str,
        economics_record_id: str,
        recorded_at: datetime,
        reasons: list[str],
    ) -> PredictionRow:
        return PredictionRow(
            prediction_id=prediction_id,
            bundle_id=self.bundle_id,
            actual=row.actual,
            decision_at=row.decision_at,
            recorded_at=recorded_at,
            source_release_id=row.source_release_id,
            source_release_receipt_id=row.source_release_receipt.receipt_id,
            economics_record_id=economics_record_id,
            feature_row_id=row.row_id,
            planned_entry_at=row.planned_entry_at,
            label_unlock_at=row.label_unlock_at,
            abstained=True,
            abstention_reasons=tuple(sorted(set(reasons))),
            expected_return=None,
            probability_up=None,
            probability_down=None,
            probability_neutral=None,
            uncertainty=None,
            bundle_classification=self.bundle_classification.value,
            candidate_provenance_id=self.candidate_provenance_id,
            production_eligible=self.production_eligible,
        )
