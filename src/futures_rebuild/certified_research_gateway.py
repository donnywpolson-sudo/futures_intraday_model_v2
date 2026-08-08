"""The only current registration and historical trial-execution gateway."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from .canonical import canonical_bytes
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .alpha_research_ladder import validate_stage_registration
from .errors import IntegrityError, UnauthorizedOperation
from .preexecution_fold_certification import (
    create_registration_after_gate,
    load_registration_ready_certificate,
    load_execution_ready_registration,
)
from .research_gateway_policy import (
    CERTIFIED_GATEWAY_SCHEMA,
    CERTIFIED_EXECUTION_SCOPE_KEYS,
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
    RETIRED_PRE_REGISTRATION_PROTOCOL_IDS,
)


_APPROVAL_SCOPE_KEYS = frozenset(
    {"approval_command", "approval_plan_id", "approval_plan_sha256"}
)


@dataclass(frozen=True)
class CertifiedResearchGateway:
    """Bind current research writes and claims to one row-certified trial."""

    root: Path
    boundary: RepoBoundary

    def __post_init__(self) -> None:
        root = self.root.resolve(strict=False)
        self.boundary.assert_active_root(root)
        object.__setattr__(self, "root", root)

    def register_trial(
        self, *, registration_path: Path, registration: Mapping[str, object],
        readiness_evidence_path: Path,
    ) -> dict[str, str]:
        """Create one immutable registration only after a row-certified PASS."""

        if registration.get("protocol_id") in RETIRED_PRE_REGISTRATION_PROTOCOL_IDS:
            raise UnauthorizedOperation(
                "rejected pre-registration protocol cannot be registered"
            )

        registration_path = registration_path.resolve(strict=False)
        readiness_evidence_path = readiness_evidence_path.resolve(strict=False)
        certificate, _relative, _evidence_sha256 = load_registration_ready_certificate(
            root=self.root, certificate_evidence_path=readiness_evidence_path,
        )
        validate_stage_registration(
            registration, certificate=certificate, root=self.root,
        )
        create_registration_after_gate(
            root=self.root,
            path=registration_path,
            payload=registration,
            certificate_evidence_path=readiness_evidence_path,
        )
        raw = registration_path.read_bytes()
        binding = registration.get("fold_readiness_binding")
        if not isinstance(binding, Mapping):
            raise IntegrityError("registered trial lost its readiness binding")
        return {
            "trial_id": str(registration["trial_id"]),
            "registration_path": registration_path.relative_to(self.root).as_posix(),
            "registration_sha256": sha256(raw).hexdigest(),
            "readiness_certificate_id": str(binding["certificate_id"]),
        }

    def execution_scope(
        self, *, registration_path: Path, expected_registration_sha256: str,
        additional_scope: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Derive the receipt scope from immutable registration evidence."""

        registration_path = registration_path.resolve(strict=False)
        registration, certificate = load_execution_ready_registration(
            root=self.root,
            registration_path=registration_path,
            expected_registration_sha256=expected_registration_sha256,
        )
        if registration.get("protocol_id") in RETIRED_PRE_REGISTRATION_PROTOCOL_IDS:
            raise UnauthorizedOperation(
                "rejected pre-registration protocol cannot be executed"
            )
        binding = registration["fold_readiness_binding"]
        if not isinstance(binding, Mapping):
            raise IntegrityError("registered trial lacks readiness evidence")
        ladder_scope = validate_stage_registration(
            registration, certificate=certificate, root=self.root,
        )
        scope = {
            "gateway_schema": CERTIFIED_GATEWAY_SCHEMA,
            "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
            "trial_id": str(registration["trial_id"]),
            "trial_family": str(registration["trial_family"]),
            "protocol_id": str(registration["protocol_id"]),
            "registration_path": registration_path.relative_to(self.root).as_posix(),
            "registration_sha256": expected_registration_sha256,
            "readiness_certificate_id": str(certificate["certificate_id"]),
            "readiness_evidence_sha256": str(binding["evidence_sha256"]),
            **ladder_scope,
        }
        extras = dict(additional_scope or {})
        if set(extras) & (CERTIFIED_EXECUTION_SCOPE_KEYS | _APPROVAL_SCOPE_KEYS):
            raise UnauthorizedOperation(
                "additional execution scope collides with certified bindings"
            )
        if any(type(key) is not str or not key or type(value) is not str or not value
               for key, value in extras.items()):
            raise UnauthorizedOperation("additional execution scope is invalid")
        return {**scope, **extras}

    def claim_historical_execution(
        self, *, registration_path: Path, expected_registration_sha256: str,
        receipt: OperationReceipt,
        additional_scope: Mapping[str, str] | None = None,
    ) -> Path:
        """Revalidate the row certificate, then atomically consume one claim."""

        registration_path = registration_path.resolve(strict=False)
        expected = self.execution_scope(
            registration_path=registration_path,
            expected_registration_sha256=expected_registration_sha256,
            additional_scope=additional_scope,
        )
        observed = dict(receipt.scope)
        observed_research = {
            key: value for key, value in observed.items()
            if key not in _APPROVAL_SCOPE_KEYS
        }
        if observed_research != expected:
            raise UnauthorizedOperation(
                "historical execution receipt is not bound to the certified trial"
            )
        holdout_marker = self.root / "state" / "alpha_ladder_holdout_claims" / "2025.json"
        if expected["alpha_ladder_stage"] == "holdout" and holdout_marker.exists():
            raise UnauthorizedOperation("the project-level 2025 holdout was already claimed")
        use_path = receipt.consume(
            self.boundary,
            operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
            classification=(
                OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
            ),
            required_scope=observed,
        )
        if expected["alpha_ladder_stage"] == "holdout":
            holdout_marker.parent.mkdir(parents=True, exist_ok=True)
            marker = {
                "schema_version": "alpha_ladder_holdout_claim/1.0.0",
                "year": 2025,
                "trial_id": expected["trial_id"],
                "contract_id": expected["alpha_ladder_contract_id"],
                "mechanism_sha256": expected["mechanism_sha256"],
                "authorization_receipt_id": receipt.receipt_id,
                "state": "CLAIMED_ONCE_NO_RETRY",
            }
            try:
                with holdout_marker.open("xb") as stream:
                    stream.write(canonical_bytes(marker) + b"\n")
            except FileExistsError as exc:
                raise UnauthorizedOperation(
                    "the project-level 2025 holdout was concurrently claimed"
                ) from exc
        return use_path
