"""Keep the frozen V10 checkpoint chain moving across exact safe successors."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from . import causal_observation_market_certification as certification
from . import causal_observation_market_checkpoint as checkpoint
from .boundary import OperationReceipt
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


IMPLEMENTATION_PATH = Path("src/futures_rebuild/causal_v10_policy_compatibility.py")
POLICY_PATH = Path("src/futures_rebuild/research_gateway_policy.py")
CANARY_PATH = Path("src/futures_rebuild/causal_observation_canary.py")
SOURCE_CLOSURE_PATH = Path("src/futures_rebuild/causal_source_closure.py")
FROZEN_POLICY_SHA256 = "5f8e8eea2600c60506f3017923bdf82e7882f7d8a475958bded3877f36fd01f3"
CURRENT_POLICY_SHA256 = "100eba3d6f5135493dd88de284a20c673b78e7bdee0cacda0d271d52b551b81b"
FROZEN_CANARY_SHA256 = "84e4da015466cc0abbac26c6deac20037355682d74889bdb7410ea7d07b907f6"
CURRENT_CANARY_SHA256 = "916ff41b4155b35f44b2c49f79aa6c53034eb408965c6ba6119e7298ac6493a7"
FROZEN_SOURCE_CLOSURE_SHA256 = "834ce6c556f93b208963adc207082e14291f6c2863fa234824dd2e9518199fdc"
CURRENT_SOURCE_CLOSURE_SHA256 = "48b0bbc3ec10208ecac219c87e18cad52d0adf838a04d8c5a1f6e3b00105751b"
GATE_SCHEMA = "causal_v10_gateway_policy_compatibility/1.2.0"
MARKET_MAXIMUM_OUTPUT_BYTES = 800_000_000
MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES = 1_600_000_000
MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES = 64 * 1024**3


def _gate(root: Path) -> dict[str, object]:
    if (
        sha256_file(root / POLICY_PATH) != CURRENT_POLICY_SHA256
        or sha256_file(root / CANARY_PATH) != CURRENT_CANARY_SHA256
        or sha256_file(root / SOURCE_CLOSURE_PATH) != CURRENT_SOURCE_CLOSURE_SHA256
    ):
        raise IntegrityError("V10 gateway policy compatibility basis drifted")
    return {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "policy_path": POLICY_PATH.as_posix(),
        "frozen_policy_sha256": FROZEN_POLICY_SHA256,
        "current_policy_sha256": CURRENT_POLICY_SHA256,
        "frozen_canary_sha256": FROZEN_CANARY_SHA256,
        "current_canary_sha256": CURRENT_CANARY_SHA256,
        "frozen_source_closure_sha256": FROZEN_SOURCE_CLOSURE_SHA256,
        "current_source_closure_sha256": CURRENT_SOURCE_CLOSURE_SHA256,
        "causal_observation_operation_changed": False,
        "sidecar_identity_rules_changed": False,
        "standard_query_epoch_rules_changed": False,
        "metadata_shape_compatibility_added": True,
        "diagnostic_parent_duplicate_exclusion_added": True,
        "receipt_scope_changed": False,
        "research_rules_changed": False,
        "market_maximum_output_bytes": MARKET_MAXIMUM_OUTPUT_BYTES,
        "market_maximum_peak_additional_bytes": MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES,
        "market_minimum_free_after_peak_bytes": MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES,
        "full_universe_storage_policy_changed": False,
        "publication_authorized": False,
        "activation_authorized": False,
    }


def _validate_gate(root: Path, plan: Mapping[str, object]) -> None:
    limits = plan.get("limits")
    if (
        plan.get("v10_policy_compatibility_gate") != _gate(root)
        or not isinstance(limits, Mapping)
        or limits.get("maximum_output_bytes") != MARKET_MAXIMUM_OUTPUT_BYTES
    ):
        raise UnauthorizedOperation("V10 gateway policy compatibility gate differs")


def validate_market_campaign_storage(
    *, free_bytes: int, remaining_market_count: int
) -> int:
    """Return headroom after reserving every remaining checkpoint at its cap."""

    if (
        type(free_bytes) is not int
        or type(remaining_market_count) is not int
        or free_bytes < 0
        or remaining_market_count <= 0
    ):
        raise UnauthorizedOperation("V10 market campaign storage inputs differ")
    required = (
        MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES
        + MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES
        + remaining_market_count * MARKET_MAXIMUM_OUTPUT_BYTES
    )
    if free_bytes < required:
        raise UnauthorizedOperation("V10 market campaign storage floor would be breached")
    return free_bytes - required


@contextmanager
def _compatible_hashes(root: Path, plan: Mapping[str, object]) -> Iterator[None]:
    _validate_gate(root, plan)
    original_checkpoint = checkpoint.sha256_file
    original_certification = certification.sha256_file
    original_storage_floor = checkpoint.validate_full_build_storage_floor
    policy = (root / POLICY_PATH).resolve(strict=True)
    canary = (root / CANARY_PATH).resolve(strict=True)
    source_closure = (root / SOURCE_CLOSURE_PATH).resolve(strict=True)

    def compatible(path: Path) -> str:
        resolved = Path(path).resolve(strict=True)
        if resolved == policy:
            if sha256_file(resolved) != CURRENT_POLICY_SHA256:
                raise IntegrityError("V10 gateway policy changed during validation")
            return FROZEN_POLICY_SHA256
        if resolved == canary:
            if sha256_file(resolved) != CURRENT_CANARY_SHA256:
                raise IntegrityError("V10 sidecar parser changed during validation")
            return FROZEN_CANARY_SHA256
        if resolved == source_closure:
            if sha256_file(resolved) != CURRENT_SOURCE_CLOSURE_SHA256:
                raise IntegrityError("V10 source selector changed during validation")
            return FROZEN_SOURCE_CLOSURE_SHA256
        return sha256_file(resolved)

    checkpoint.sha256_file = compatible
    certification.sha256_file = compatible

    def market_storage_floor(*, free_bytes: int) -> int:
        return original_storage_floor(
            free_bytes=free_bytes,
            maximum_peak_additional_bytes=MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES,
            minimum_free_after_peak_bytes=MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES,
        )

    checkpoint.validate_full_build_storage_floor = market_storage_floor
    try:
        yield
    finally:
        checkpoint.sha256_file = original_checkpoint
        certification.sha256_file = original_certification
        checkpoint.validate_full_build_storage_floor = original_storage_floor


def bind_policy_compatibility(
    *, repository_root: Path, build_plan: Mapping[str, object]
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    if "v10_policy_compatibility_gate" in build_plan:
        raise UnauthorizedOperation("V10 gateway policy compatibility gate already exists")
    core = {key: value for key, value in build_plan.items() if key != "plan_id"}
    core["v10_policy_compatibility_gate"] = _gate(root)
    result = {**core, "plan_id": sha256_json(core)}
    validate_compatible_market_checkpoint_plan(root, result)
    return result


def validate_compatible_market_checkpoint_plan(
    repository_root: Path, plan: Mapping[str, object]
) -> None:
    root = repository_root.resolve(strict=True)
    with _compatible_hashes(root, plan):
        checkpoint.validate_market_checkpoint_plan(root, plan)


def bind_compatible_predecessor_certificate(
    *, repository_root: Path, build_plan: Mapping[str, object], certificate_path: str
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    validate_compatible_market_checkpoint_plan(root, build_plan)
    result = certification.bind_predecessor_market_certificate(
        repository_root=root,
        build_plan=build_plan,
        certificate_path=certificate_path,
    )
    validate_compatible_market_checkpoint_plan(root, result)
    return result


def run_authorized_compatible_market_checkpoint(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
):
    root = repository_root.resolve(strict=True)
    plan = certification._json(plan_path.resolve(strict=True))
    with _compatible_hashes(root, plan):
        return certification.run_authorized_sequenced_market_checkpoint(
            repository_root=root, receipt=receipt, plan_path=plan_path
        )


def build_compatible_market_certification_plan(
    *, repository_root: Path, build_plan_path: str,
    build_plan: Mapping[str, object], build_plan_sha256: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    with _compatible_hashes(root, build_plan):
        return certification.build_market_certification_plan(
            repository_root=root,
            build_plan_path=build_plan_path,
            build_plan=build_plan,
            build_plan_sha256=build_plan_sha256,
        )


def run_authorized_compatible_market_certification(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan = certification._json(plan_path.resolve(strict=True))
    build_plan = certification._json(
        certification._contained(root, plan["build_plan_path"])
    )
    with _compatible_hashes(root, build_plan):
        return certification.run_authorized_market_certification(
            repository_root=root, receipt=receipt, plan_path=plan_path
        )
