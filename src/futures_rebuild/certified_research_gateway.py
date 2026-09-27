"""The only current registration and historical trial-execution gateway."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path

from .canonical import canonical_bytes
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .alpha_research_ladder import load_active_ladder, validate_stage_registration
from .causal_search_access_contract import (
    REQUIRED_ALPHA_LADDER_SCHEMA,
    registration_contract_scope,
)
from .bias_robustness_successor import (
    require_stage3_active_authority,
    validate_row_request,
)
from .errors import IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .preexecution_fold_certification import (
    create_registration_after_gate,
    load_registration_ready_certificate,
    load_execution_ready_registration,
)
from .research_gateway_policy import (
    CERTIFIED_GATEWAY_SCHEMA,
    CERTIFIED_EXECUTION_SCOPE_KEYS,
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
    BIAS_GATEWAY_SCHEMA,
    RETIRED_PRE_REGISTRATION_PROTOCOL_IDS,
    scientific_governance_binding,
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

        require_stage3_active_authority(self.root)
        from . import l0_development_admission as l0
        is_l0 = registration.get("schema_version") == l0.REGISTRATION_SCHEMA
        if not is_l0:
            access_request = registration.get("stage3_access_request")
            if not isinstance(access_request, Mapping):
                raise IntegrityError("trial registration lacks its Stage 3+ access request")
            validate_row_request(self.root, access_request)
        if registration.get("protocol_id") in RETIRED_PRE_REGISTRATION_PROTOCOL_IDS:
            raise UnauthorizedOperation(
                "rejected pre-registration protocol cannot be registered"
            )
        registration_path = registration_path.resolve(strict=False)
        readiness_evidence_path = readiness_evidence_path.resolve(strict=False)
        certificate, _relative, _evidence_sha256 = load_registration_ready_certificate(
            root=self.root, certificate_evidence_path=readiness_evidence_path,
        )
        if is_l0:
            l0.validate_registration(self.root, registration, certificate)
        else:
            active_contract, _profile = load_active_ladder(self.root)
            registration_contract_scope(self.root, registration, required=True)
            validate_stage_registration(registration, certificate=certificate, root=self.root)
        with FileLease(self.root / "state/locks/certified_research_registration.lock"):
            if registration.get(
                "scientific_governance_binding"
            ) != scientific_governance_binding(self.root):
                raise IntegrityError(
                    "trial registration lacks the current scientific governance binding"
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

    def prepare_predictive_readiness(
        self, *, receipt: OperationReceipt, sessions, rows_by_date, calendar,
        source_contract_id: str, source_release_id: str, bindings, as_of_ns: int,
    ):
        """Connect V9 source products to fold readiness, never registration or fitting.

        The source reader must already have consumed its bounded readiness receipt.
        Hash-bound evidence files are checked before any predictive transformation.
        Existing registration/execution/holdout gates are unchanged.
        """
        from .canonical import sha256_file, sha256_json, assert_plain_file, assert_no_linklike_ancestors
        from .rolling_hour_price_census import OPERATION
        from .rolling_hour_predictive_preparation import validate_bundle
        from .rolling_hour_predictive_source import adapt_source_sessions, fold_support, source_facts_hash
        from . import rolling_hour_research as numerical

        if type(receipt) is not OperationReceipt:
            raise UnauthorizedOperation("verified readiness receipt required")
        bundle = validate_bundle(self.root)
        expected = {
            "purpose": "PREDICTIVE_SOURCE_READINESS_NO_FIT",
            "bundle_id": bundle["bundle_id"],
            "source_contract_id": source_contract_id,
            "source_release_id": source_release_id,
            "source_bindings_sha256": sha256_json(bindings),
            "calendar_release_id": calendar.receipt.release_id,
        }
        if any(receipt.scope.get(key) != value for key, value in expected.items()):
            raise UnauthorizedOperation("predictive readiness receipt scope differs")
        receipt.assert_consumed(self.boundary, operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=dict(receipt.scope))
        verified = set()
        for binding in bindings.values():
            for prefix in ("calendar_evidence", "availability_evidence", "source_certificate"):
                relative, digest = binding.get(prefix + "_path"), binding.get(prefix + "_sha256")
                if not isinstance(relative, str) or Path(relative).is_absolute():
                    raise IntegrityError("source evidence needs a repository-relative path")
                requested = self.root / relative
                assert_no_linklike_ancestors(requested)
                path = requested.resolve(strict=True)
                try:
                    path.relative_to(self.root)
                except ValueError as exc:
                    raise IntegrityError("source evidence leaves repository") from exc
                if (relative, digest) not in verified:
                    assert_plain_file(path)
                    if sha256_file(path) != digest:
                        raise IntegrityError("predictive source evidence changed")
                    if prefix == "source_certificate":
                        try:
                            certificate = json.loads(path.read_bytes())
                            numerical.profit.identity(certificate, "certificate_id")
                        except (ValueError, TypeError, KeyError) as exc:
                            raise IntegrityError("invalid predictive source certificate") from exc
                        expected_certificate = {
                            "schema_version": "rolling_hour_predictive_source_certificate/1",
                            "evidence_class": "ROW_CERTIFIED", "status": "PASS",
                            "bundle_id": bundle["bundle_id"],
                            "source_contract_id": source_contract_id,
                            "source_release_id": source_release_id,
                            "calendar_release_id": calendar.receipt.release_id,
                            "source_facts_sha256": source_facts_hash(bindings),
                        }
                        if any(certificate.get(k) != v for k, v in expected_certificate.items()):
                            raise IntegrityError("predictive source certificate scope or status differs")
                        if certificate.get("checks") != {key: "PASS" for key in (
                                "calendar_publication", "calendar_applicability", "bar_availability",
                                "instrument_identity", "price_quality")}:
                            raise IntegrityError("predictive source facts are not certified")
                    verified.add((relative, digest))
        dataset = adapt_source_sessions(sessions=sessions, rows_by_date=rows_by_date,
            calendar=calendar, source_contract_id=source_contract_id,
            source_release_id=source_release_id, bindings=bindings, as_of_ns=as_of_ns)
        return dataset, fold_support(dataset)

    def execution_scope(
        self, *, registration_path: Path, expected_registration_sha256: str,
        additional_scope: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Derive the receipt scope from immutable registration evidence."""

        require_stage3_active_authority(self.root)
        registration_path = registration_path.resolve(strict=False)
        try:
            raw = registration_path.read_bytes()
            preflight_registration = json.loads(raw)
        except (OSError, ValueError) as exc:
            raise IntegrityError("trial registration preflight is unreadable") from exc
        if sha256(raw).hexdigest() != expected_registration_sha256 or not isinstance(preflight_registration, dict) or raw != canonical_bytes(preflight_registration) + b"\n":
            raise IntegrityError("trial registration preflight changed")
        from . import l0_development_admission as l0
        is_l0 = preflight_registration.get("schema_version") == l0.REGISTRATION_SCHEMA
        if not is_l0:
            access_request = preflight_registration.get("stage3_access_request")
            if not isinstance(access_request, Mapping):
                raise IntegrityError("trial registration lacks its Stage 3+ access request")
            validate_row_request(self.root, access_request)
        registration, certificate = load_execution_ready_registration(
            root=self.root,
            registration_path=registration_path,
            expected_registration_sha256=expected_registration_sha256,
        )
        if registration.get("protocol_id") in RETIRED_PRE_REGISTRATION_PROTOCOL_IDS:
            raise UnauthorizedOperation(
                "rejected pre-registration protocol cannot be executed"
            )
        if is_l0:
            return l0.execution_scope(self.root, registration, certificate, registration_path,
                                      expected_registration_sha256, dict(additional_scope or {}))
        binding = registration["fold_readiness_binding"]
        if not isinstance(binding, Mapping):
            raise IntegrityError("registered trial lacks readiness evidence")
        governance = registration.get("scientific_governance_binding")
        if not isinstance(governance, Mapping):
            raise IntegrityError("registered trial lacks scientific governance")
        policy = governance.get("scientific_policy")
        ledger = governance.get("global_trial_ledger")
        legacy = governance.get("legacy_attempts")
        if not all(isinstance(item, Mapping) for item in (policy, ledger, legacy)):
            raise IntegrityError("registered scientific governance is invalid")
        current = scientific_governance_binding(self.root)
        if (
            policy != current["scientific_policy"]
            or legacy != current["legacy_attempts"]
        ):
            raise IntegrityError("registered scientific policy or legacy count is stale")
        active_contract, _profile = load_active_ladder(self.root)
        causal_scope = registration_contract_scope(
            self.root,
            registration,
            required=True,
        )
        ladder_scope = validate_stage_registration(
            registration, certificate=certificate, root=self.root,
        )
        scope = {
            "gateway_schema": (
                BIAS_GATEWAY_SCHEMA if causal_scope else CERTIFIED_GATEWAY_SCHEMA
            ),
            "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
            "trial_id": str(registration["trial_id"]),
            "trial_family": str(registration["trial_family"]),
            "protocol_id": str(registration["protocol_id"]),
            "registration_path": registration_path.relative_to(self.root).as_posix(),
            "registration_sha256": expected_registration_sha256,
            "readiness_certificate_id": str(certificate["certificate_id"]),
            "readiness_evidence_sha256": str(binding["evidence_sha256"]),
            "scientific_policy_sha256": str(policy["sha256"]),
            "global_trial_ledger_head": str(ledger["head"]),
            "conservative_legacy_attempt_count": str(legacy["conservative_count"]),
            **causal_scope,
            **ladder_scope,
        }
        extras = dict(additional_scope or {})
        if set(extras) & (
            CERTIFIED_EXECUTION_SCOPE_KEYS | set(causal_scope) | _APPROVAL_SCOPE_KEYS
        ):
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
        if expected.get("alpha_ladder_stage") == "holdout" and holdout_marker.exists():
            raise UnauthorizedOperation("the project-level 2025 holdout was already claimed")
        use_path = receipt.consume(
            self.boundary,
            operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
            classification=(
                OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
            ),
            required_scope=observed,
        )
        if expected.get("alpha_ladder_stage") == "holdout":
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
