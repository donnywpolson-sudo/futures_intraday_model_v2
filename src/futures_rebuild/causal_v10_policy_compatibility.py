"""Keep the frozen V10 checkpoint chain moving across exact safe successors."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from . import causal_observation_market_certification as certification
from . import causal_observation_market_checkpoint as checkpoint
from . import canonical as canonical_module
from . import data_layout
from . import causal_observation_foundation as foundation
from . import research_gateway_policy as policy
from .boundary import OperationReceipt
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


IMPLEMENTATION_PATH = Path("src/futures_rebuild/causal_v10_policy_compatibility.py")
POLICY_PATH = Path("src/futures_rebuild/research_gateway_policy.py")
CANARY_PATH = Path("src/futures_rebuild/causal_observation_canary.py")
SOURCE_CLOSURE_PATH = Path("src/futures_rebuild/causal_source_closure.py")
CANONICAL_PATH = Path("src/futures_rebuild/canonical.py")
DATA_LAYOUT_PATH = Path("src/futures_rebuild/data_layout.py")
ACTIVE_SOURCE_CONTRACT_PATH = Path("configs/source_contract.json")
FROZEN_SOURCE_CONTRACT_PATH = Path(
    "state/dbn_congruence_release_archive/"
    "4ca353d7814941782bb4c6640afe89b04371492868f57174bb10d632b6e7c9be/"
    "l0cutpub_17df59981242bdf6e70c0d270699d538/prior_source_contract.json"
)
FROZEN_POLICY_SHA256 = "5f8e8eea2600c60506f3017923bdf82e7882f7d8a475958bded3877f36fd01f3"
FROZEN_CANARY_SHA256 = "84e4da015466cc0abbac26c6deac20037355682d74889bdb7410ea7d07b907f6"
CURRENT_CANARY_SHA256 = "b8ebb95c9367c996e0946027227bfc35f9816b939c5e7a719e75a1c9cca30642"
PREVIOUS_CANARY_SHA256 = "b86a20861fde1e24cb1c09a65e3d5a982e72a3f65bb6f0bc80f637b093169097"
FROZEN_SOURCE_CLOSURE_SHA256 = "834ce6c556f93b208963adc207082e14291f6c2863fa234824dd2e9518199fdc"
FROZEN_CANONICAL_SHA256 = "969c366b59b638f97d6c02f6bf5fd532694dc6db1a82ee0d201312d3e72ba516"
FROZEN_DATA_LAYOUT_SHA256 = "a605eb3cacebae887fe43fbdd463c384b19729aa2c0354ba8a16e154131ec035"
CURRENT_SOURCE_CLOSURE_SHA256 = "48b0bbc3ec10208ecac219c87e18cad52d0adf838a04d8c5a1f6e3b00105751b"
CURRENT_DATA_LAYOUT_SHA256 = "5df7a16871d18721f8b59fb2ae4ced128c0b13f26e8715117fb805ebea248c08"
FROZEN_SOURCE_CONTRACT_SHA256 = "17601c9d3f15cb19245f441024f553a52c10c55e051b60a84994a74087b29cbd"
PREVIOUS_IMPLEMENTATION_SHA256S = frozenset(
    {
        "6b8899bc93d7a151e366cf9d63baa02b0c068109bdae452b255bb0cb05075e45",
        "02072965d3e40c07d740eb33c7c27b866410f996a10f3d8bb5807c073e064770",
        "192b35b1f734392f75d8cff5c1b3069ad01b123195f3420bf5f56ad77c11d8ef",
        "5a6ad4dbaa4d5d46c15798dfad88da2d4789add8ebca7ce920c1f8a65393ffbc",
        "615b875d5fe335368a7d4900274301b4a5ba39e6e8421f0d1426964ecd811685",
    }
)
EARLY_CERTIFIER_BINDINGS_SHA256 = "54c9f1ec48caa6948fb6711cce169028737f76012a2f8735c78cde715b6da71d"
CURRENT_CERTIFIER_BINDINGS_SHA256 = "aad0b905e4711fff0dfb8906643cfb1cc2199ded73906441a39a746c59271683"
EARLY_CERTIFIER_MARKETS = frozenset(
    {"ES", "GC", "6E", "CL", "NQ", "6A", "6B", "6C", "6J", "6M", "6N", "6S", "BTC", "ETH", "GF", "HE", "HG", "HO", "KE", "LE", "NG", "PA", "PL", "RB", "SR3", "ZB", "ZN"}
)
CURRENT_CERTIFIER_MARKETS = frozenset(
    {"RTY", "SI", "SR1", "TN", "UB", "YM", "ZC", "ZF", "ZL", "ZM", "ZQ", "ZS", "ZT", "ZW"}
)
EARLY_CERTIFICATION_PLAN_PATH = Path(
    "reports/bounded_2025_v10_es_market_certification_preparation/"
    "esv10mrcp_20260826T1724552865219Z_61365601/"
    "ES_MAXIMUM_ROBUSTNESS_CERTIFICATION_PLAN_V10.json"
)
CURRENT_CERTIFICATION_PLAN_PATH = Path(
    "state/causal_observation_v10_campaigns/"
    "1809dbe19ead0d0008eb5e20698ddd6f14bf93e49dd61f3fff626f96cf816cfc/"
    "RTY/certification_plan.json"
)
COMPATIBLE_SET_CERTIFICATE_SCHEMA = "causal_observation_41_market_compatible_certificate/1.0.0"
GATE_SCHEMA = "causal_v10_gateway_policy_compatibility/1.5.0"
EXPECTED_CAUSAL_BUILD_OPERATION = "BUILD_DEVELOPMENT_CAUSAL_OBSERVATION_FOUNDATION_ONCE"
EXPECTED_CANONICAL_SEMANTICS = {
    "canonical_bytes_hex": "7b2261223a312c2262223a5b747275652c6e756c6c2c2278225d7d",
    "sha256_json": "eca8cfb31ab74533e1eb2f4c74d2d55dfe3c79ac704787e54be8647ea7777eb1",
    "sha256_bytes": "7f8282e63b59669554ff3aa69bdacfe0d7f037ab4648ceb503c438ea6c50305d",
}
MARKET_MAXIMUM_OUTPUT_BYTES = 800_000_000
MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES = 1_600_000_000
MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES = 64 * 1024**3


def _policy_semantics() -> dict[str, object]:
    if (
        policy.CAUSAL_OBSERVATION_FULL_BUILD_OPERATION
        != EXPECTED_CAUSAL_BUILD_OPERATION
        or EXPECTED_CAUSAL_BUILD_OPERATION
        not in policy.PREPARATORY_REAL_HISTORY_OPERATIONS
    ):
        raise IntegrityError("V10 causal-build gateway semantics drifted")
    policy.require_current_real_history_operation(EXPECTED_CAUSAL_BUILD_OPERATION, {})
    return {
        "causal_build_operation": EXPECTED_CAUSAL_BUILD_OPERATION,
        "preparatory_operation_allowed": True,
        "trial_gateway_changes_isolated": True,
    }


CURRENT_POLICY_SEMANTICS_ID = sha256_json(_policy_semantics())


def _canonical_semantics() -> dict[str, str]:
    vector = {"b": [True, None, "x"], "a": 1}
    actual = {
        "canonical_bytes_hex": canonical_module.canonical_bytes(vector).hex(),
        "sha256_json": canonical_module.sha256_json(vector),
        "sha256_bytes": canonical_module.sha256_bytes(b"phase2-canonical-vector"),
    }
    if actual != EXPECTED_CANONICAL_SEMANTICS:
        raise IntegrityError("V10 canonical primitives drifted")
    return actual


CURRENT_CANONICAL_SEMANTICS_ID = sha256_json(EXPECTED_CANONICAL_SEMANTICS)


def _data_layout_semantics() -> dict[str, str]:
    logical_path = "data/causally_gated_normalized/ES/2025/1m/part.parquet"
    actual = {
        "layout_version": data_layout.LAYOUT_VERSION,
        "manifest_version": data_layout.MANIFEST_VERSION,
        "causal_template": data_layout._LOGICAL_TEMPLATES["causally_gated_normalized"],
        "causal_pattern": data_layout._LOGICAL_PATTERNS["causally_gated_normalized"].pattern,
        "causal_path": data_layout._logical_data_path(
            logical_path, "causally_gated_normalized"
        ).as_posix(),
    }
    expected = {
        "layout_version": "2.0.0",
        "manifest_version": "2.0.0",
        "causal_template": "data/causally_gated_normalized/{market}/{year}/{interval}/{filename}",
        "causal_pattern": r"^data/causally_gated_normalized/[0-9A-Z]{1,16}/\d{4}/[^/]+/[^/]+$",
        "causal_path": logical_path,
    }
    if actual != expected:
        raise IntegrityError("V10 causal data-layout semantics drifted")
    return actual


CURRENT_DATA_LAYOUT_SEMANTICS_ID = sha256_json(_data_layout_semantics())


def _gate(root: Path) -> dict[str, object]:
    if (
        sha256_file(root / CANARY_PATH) != CURRENT_CANARY_SHA256
        or sha256_file(root / SOURCE_CLOSURE_PATH) != CURRENT_SOURCE_CLOSURE_SHA256
    ):
        raise IntegrityError("V10 gateway policy compatibility basis drifted")
    canonical_semantics = _canonical_semantics()
    semantics = _policy_semantics()
    if sha256_json(semantics) != CURRENT_POLICY_SEMANTICS_ID:
        raise IntegrityError("V10 causal-build gateway semantics drifted")
    return {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "policy_path": POLICY_PATH.as_posix(),
        "frozen_policy_sha256": FROZEN_POLICY_SHA256,
        "current_policy_semantics": semantics,
        "current_policy_semantics_id": CURRENT_POLICY_SEMANTICS_ID,
        "canonical_path": CANONICAL_PATH.as_posix(),
        "frozen_canonical_sha256": FROZEN_CANONICAL_SHA256,
        "current_canonical_semantics": canonical_semantics,
        "current_canonical_semantics_id": CURRENT_CANONICAL_SEMANTICS_ID,
        "frozen_canary_sha256": FROZEN_CANARY_SHA256,
        "current_canary_sha256": CURRENT_CANARY_SHA256,
        "frozen_source_closure_sha256": FROZEN_SOURCE_CLOSURE_SHA256,
        "current_source_closure_sha256": CURRENT_SOURCE_CLOSURE_SHA256,
        "causal_observation_operation_changed": False,
        "sidecar_identity_rules_changed": False,
        "standard_query_epoch_rules_changed": False,
        "metadata_shape_compatibility_added": True,
        "backfill_midnight_boundary_normalization_added": True,
        "diagnostic_parent_duplicate_exclusion_added": True,
        "receipt_scope_changed": False,
        "research_rules_changed": False,
        "scientific_governance_binding_added": True,
        "historical_trial_gateway_strengthened": True,
        "market_maximum_output_bytes": MARKET_MAXIMUM_OUTPUT_BYTES,
        "market_maximum_peak_additional_bytes": MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES,
        "market_minimum_free_after_peak_bytes": MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES,
        "full_universe_storage_policy_changed": False,
        "publication_authorized": False,
        "activation_authorized": False,
    }


def _validate_gate(root: Path, plan: Mapping[str, object]) -> None:
    limits = plan.get("limits")
    current = _gate(root)
    compatible = [
        {
            **current,
            "implementation_sha256": digest,
            "current_canary_sha256": PREVIOUS_CANARY_SHA256,
        }
        for digest in PREVIOUS_IMPLEMENTATION_SHA256S
    ]
    if (
        plan.get("v10_policy_compatibility_gate") not in (current, *compatible)
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
    original_checkpoint_json = checkpoint._json
    original_active_source_identity = foundation._active_source_identity
    original_certification = certification.sha256_file
    original_storage_floor = checkpoint.validate_full_build_storage_floor
    policy_path = (root / POLICY_PATH).resolve(strict=True)
    canary = (root / CANARY_PATH).resolve(strict=True)
    source_closure = (root / SOURCE_CLOSURE_PATH).resolve(strict=True)
    canonical_path = (root / CANONICAL_PATH).resolve(strict=True)
    data_layout_path = (root / DATA_LAYOUT_PATH).resolve(strict=True)
    active_source_contract_path = (root / ACTIVE_SOURCE_CONTRACT_PATH).resolve(
        strict=True
    )
    frozen_source_contract_path = (root / FROZEN_SOURCE_CONTRACT_PATH).resolve(
        strict=True
    )
    frozen_source_contract = original_checkpoint_json(frozen_source_contract_path)
    checkpoint_set = plan.get("checkpoint_set")
    source = plan.get("source")
    if (
        sha256_file(frozen_source_contract_path) != FROZEN_SOURCE_CONTRACT_SHA256
        or set(frozen_source_contract.get("universe", {}).get("standard_roots", []))
        != set(checkpoint.MARKET_ORDER)
        or (
            (checkpoint_set is not None or source is not None)
            and (
                not isinstance(checkpoint_set, Mapping)
                or not isinstance(source, Mapping)
                or frozen_source_contract.get("contract_id")
                != checkpoint_set.get("source_contract_id")
                != source.get("source_contract_id")
                or frozen_source_contract.get("active_canonical_source", {}).get(
                    "release_id"
                )
                != checkpoint_set.get("canonical_release_id")
                != source.get("canonical_release_id")
            )
        )
    ):
        raise IntegrityError("V10 frozen source contract differs")

    def compatible_json(path: Path) -> dict[str, object]:
        resolved = Path(path).resolve(strict=True)
        if resolved == active_source_contract_path:
            return frozen_source_contract
        return original_checkpoint_json(resolved)

    def compatible_active_source_identity(boundary) -> tuple[str, str]:
        if boundary.active_root != root:
            raise UnauthorizedOperation("V10 frozen source boundary differs")
        return (
            str(frozen_source_contract["contract_id"]),
            str(frozen_source_contract["active_canonical_source"]["release_id"]),
        )

    def compatible(path: Path) -> str:
        resolved = Path(path).resolve(strict=True)
        if resolved == active_source_contract_path:
            if sha256_file(frozen_source_contract_path) != FROZEN_SOURCE_CONTRACT_SHA256:
                raise IntegrityError("V10 frozen source contract changed during validation")
            return FROZEN_SOURCE_CONTRACT_SHA256
        if resolved == policy_path:
            if sha256_json(_policy_semantics()) != CURRENT_POLICY_SEMANTICS_ID:
                raise IntegrityError("V10 causal-build gateway semantics changed during validation")
            return FROZEN_POLICY_SHA256
        if resolved == canary:
            if sha256_file(resolved) != CURRENT_CANARY_SHA256:
                raise IntegrityError("V10 sidecar parser changed during validation")
            return FROZEN_CANARY_SHA256
        if resolved == source_closure:
            if sha256_file(resolved) != CURRENT_SOURCE_CLOSURE_SHA256:
                raise IntegrityError("V10 source selector changed during validation")
            return FROZEN_SOURCE_CLOSURE_SHA256
        if resolved == canonical_path:
            if sha256_json(_canonical_semantics()) != CURRENT_CANONICAL_SEMANTICS_ID:
                raise IntegrityError("V10 canonical primitives changed during validation")
            return FROZEN_CANONICAL_SHA256
        if resolved == data_layout_path:
            if (
                sha256_json(_data_layout_semantics()) != CURRENT_DATA_LAYOUT_SEMANTICS_ID
            ):
                raise IntegrityError("V10 causal data-layout semantics changed during validation")
            return FROZEN_DATA_LAYOUT_SHA256
        return sha256_file(resolved)

    def compatible_certification(path: Path) -> str:
        resolved = Path(path).resolve(strict=True)
        if resolved == data_layout_path:
            if (
                sha256_json(_data_layout_semantics()) != CURRENT_DATA_LAYOUT_SEMANTICS_ID
            ):
                raise IntegrityError("V10 causal data-layout semantics changed during validation")
            return CURRENT_DATA_LAYOUT_SHA256
        return compatible(resolved)

    checkpoint.sha256_file = compatible
    checkpoint._json = compatible_json
    foundation._active_source_identity = compatible_active_source_identity
    certification.sha256_file = compatible_certification

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
        checkpoint._json = original_checkpoint_json
        foundation._active_source_identity = original_active_source_identity
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


def certify_compatible_complete_market_certificate_set(
    *,
    repository_root: Path,
    checkpoint_set: Mapping[str, object],
    market_certificates: list[Mapping[str, object]],
) -> dict[str, object]:
    """Certify the exact 41-market set across the approved layout-only transition."""

    root = repository_root.resolve(strict=True)
    plans = [
        certification._json(root / EARLY_CERTIFICATION_PLAN_PATH),
        certification._json(root / CURRENT_CERTIFICATION_PLAN_PATH),
    ]
    bindings = [plan.get("certifier_implementation_bindings") for plan in plans]
    if not all(isinstance(item, Mapping) for item in bindings):
        raise IntegrityError("certifier compatibility bindings are unavailable")
    early, current = bindings
    early_id, current_id = map(sha256_json, bindings)
    differing = {
        key
        for key in set(early) | set(current)
        if early.get(key) != current.get(key)
    }
    if (
        (early_id, current_id)
        != (EARLY_CERTIFIER_BINDINGS_SHA256, CURRENT_CERTIFIER_BINDINGS_SHA256)
        or differing != {DATA_LAYOUT_PATH.as_posix()}
        or early.get(DATA_LAYOUT_PATH.as_posix()) != FROZEN_DATA_LAYOUT_SHA256
        or current.get(DATA_LAYOUT_PATH.as_posix()) != CURRENT_DATA_LAYOUT_SHA256
        or sha256_json(_data_layout_semantics()) != CURRENT_DATA_LAYOUT_SEMANTICS_ID
    ):
        raise IntegrityError("certifier binding transition is not layout-only compatible")

    by_market = {str(item.get("market")): item for item in market_certificates}
    if set(by_market) != set(checkpoint.MARKET_ORDER) or len(by_market) != len(market_certificates):
        raise IntegrityError("compatible certificate set lacks exact market coverage")
    compatibility_id = sha256_json(
        {
            "early_bindings_sha256": early_id,
            "current_bindings_sha256": current_id,
            "only_changed_path": DATA_LAYOUT_PATH.as_posix(),
            "data_layout_semantics_id": CURRENT_DATA_LAYOUT_SEMANTICS_ID,
        }
    )
    projected = []
    original_ids = []
    for market in checkpoint.MARKET_ORDER:
        item = by_market[market]
        core = {key: value for key, value in item.items() if key != "certificate_id"}
        expected_binding = (
            EARLY_CERTIFIER_BINDINGS_SHA256
            if market in EARLY_CERTIFIER_MARKETS
            else CURRENT_CERTIFIER_BINDINGS_SHA256
        )
        if (
            market not in EARLY_CERTIFIER_MARKETS | CURRENT_CERTIFIER_MARKETS
            or item.get("certificate_id") != sha256_json(core)
            or item.get("certifier_implementation_bindings_sha256") != expected_binding
        ):
            raise IntegrityError("market certificate is outside the exact compatible transition")
        original_ids.append(str(item["certificate_id"]))
        projected_core = {
            **core,
            "certifier_implementation_bindings_sha256": compatibility_id,
        }
        projected.append(
            {**projected_core, "certificate_id": sha256_json(projected_core)}
        )

    validated = certification.certify_complete_market_certificate_set(
        repository_root=root,
        checkpoint_set=checkpoint_set,
        market_certificates=projected,
    )
    core = {key: value for key, value in validated.items() if key != "certificate_id"}
    core.update(
        {
            "schema_version": COMPATIBLE_SET_CERTIFICATE_SCHEMA,
            "market_certificate_ids": original_ids,
            "market_certificate_ids_sha256": sha256_json(original_ids),
            "validation_projection_certificate_id": validated["certificate_id"],
            "certifier_binding_compatibility": {
                "compatibility_id": compatibility_id,
                "early_bindings_sha256": early_id,
                "current_bindings_sha256": current_id,
                "only_changed_path": DATA_LAYOUT_PATH.as_posix(),
                "frozen_data_layout_sha256": FROZEN_DATA_LAYOUT_SHA256,
                "current_data_layout_sha256": CURRENT_DATA_LAYOUT_SHA256,
                "data_layout_semantics_id": CURRENT_DATA_LAYOUT_SEMANTICS_ID,
            },
            "compatibility_implementation_path": IMPLEMENTATION_PATH.as_posix(),
            "compatibility_implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        }
    )
    return {**core, "certificate_id": sha256_json(core)}
