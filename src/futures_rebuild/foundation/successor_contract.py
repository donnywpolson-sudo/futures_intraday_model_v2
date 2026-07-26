"""Content-addressed compatibility contract for a bounded foundation successor.

The contract proves that the accepted 33-market component may remain bound to
its historical policy receipt while the eight newly admitted markets are
rebuilt under the 41-market successor.  It never upgrades a historical receipt
in place and never treats mutable documentation as trust evidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping, Sequence

from ..boundary import RepoBoundary
from ..canonical import assert_plain_file, canonical_bytes, sha256_file, sha256_json
from ..data_layout import (
    DataReleaseReceipt as VerifiedReleaseReceipt,
    verify_data_release_manifest,
)
from ..errors import IntegrityError
from ..producer_bridge import SESSION_RELEASE_KIND
from .market_state import MARKET_STATE_RELEASE_KIND
from .support import POLICY_FILENAMES, POLICY_RELEASE_KIND, POLICY_SCHEMA_VERSION


SUCCESSOR_CONTRACT_SCHEMA_VERSION = "foundation_policy_successor/1.0.0"
SUCCESSOR_PROVENANCE_SCHEMA_VERSION = "foundation_successor_provenance/1.0.0"
REBUILT_MARKETS = frozenset({"6N", "6S", "BTC", "ETH", "GF", "PA", "PL", "ZQ"})
_AUTHORITATIVE_SOURCE_ID = "DATABENTO_DEFINITION_GLBX_MDP3"
_DBN_LOCATOR = re.compile(
    r"^manifests/data_releases/dbn/([0-9a-f]{64})\.json"
    r"#data/dbn/definition/\{market\}/\{year\}/\{filename\}$"
)
_UNCHANGED_POLICY_DOCUMENTS = frozenset(POLICY_FILENAMES) - {
    "contract_economics_rules.json"
}
_UNCHANGED_ECONOMICS_FIELDS = {
    "authority_policy",
    "currency",
    "dataset",
    "forbidden_authorities",
    "point_value_definition",
    "valid_from",
}


def _policy_manifest(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
):
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != POLICY_RELEASE_KIND
        or manifest.schema_version != POLICY_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents) != POLICY_FILENAMES
        or set(manifest.metadata) != {"policy_payload_release_id", "policy_set_id"}
    ):
        raise IntegrityError("foundation successor policy receipt is invalid")
    return manifest


def _rule_map(payload: object, *, name: str) -> dict[str, dict[str, object]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("rules"), list):
        raise IntegrityError(f"{name} economics rulebook is invalid")
    result: dict[str, dict[str, object]] = {}
    for raw in payload["rules"]:
        if not isinstance(raw, dict) or type(raw.get("market")) is not str:
            raise IntegrityError(f"{name} economics rule is invalid")
        market = str(raw["market"])
        if market in result:
            raise IntegrityError(f"{name} economics rulebook contains a duplicate")
        result[market] = dict(raw)
    if not result:
        raise IntegrityError(f"{name} economics rulebook is empty")
    return result


def _authoritative_release_id(payload: Mapping[str, object], *, name: str) -> str:
    sources = payload.get("verification_sources")
    if not isinstance(sources, dict):
        raise IntegrityError(f"{name} economics source registry is invalid")
    authoritative = [
        key
        for key, value in sources.items()
        if isinstance(value, dict) and value.get("authoritative") is True
    ]
    source = sources.get(_AUTHORITATIVE_SOURCE_ID)
    if authoritative != [_AUTHORITATIVE_SOURCE_ID] or not isinstance(source, dict):
        raise IntegrityError(f"{name} economics authority is ambiguous")
    match = _DBN_LOCATOR.fullmatch(str(source.get("locator")))
    if match is None:
        raise IntegrityError(f"{name} economics DBN locator is invalid")
    return match.group(1)


def _selected_source_superset_evidence(
    *,
    boundary: RepoBoundary,
    selection_receipt: VerifiedReleaseReceipt,
    predecessor_release_id: str,
    successor_release_id: str,
    reused_markets: frozenset[str],
) -> dict[str, object]:
    selection_manifest = selection_receipt.verify(boundary)
    selection = selection_receipt.embedded_document("source_selection.json", boundary)
    if (
        not isinstance(selection, dict)
        or not isinstance(selection.get("files"), list)
        or selection.get("source_dbn_release_id") != successor_release_id
        or selection_manifest.metadata.get("source_dbn_release_id")
        != successor_release_id
    ):
        raise IntegrityError("successor selection is not bound to the DBN successor")

    predecessor_path = (
        boundary.active_root
        / "manifests"
        / "data_releases"
        / "dbn"
        / f"{predecessor_release_id}.json"
    )
    successor_path = (
        boundary.active_root
        / "manifests"
        / "data_releases"
        / "dbn"
        / f"{successor_release_id}.json"
    )
    predecessor = verify_data_release_manifest(
        predecessor_path, boundary, verify_files=False
    )
    successor = verify_data_release_manifest(successor_path, boundary, verify_files=False)
    if (
        predecessor.release_id != predecessor_release_id
        or successor.release_id != successor_release_id
        or successor.source_release_ids != (predecessor_release_id,)
        or successor.metadata.get("parent_release_id") != predecessor_release_id
        or predecessor.phase != successor.phase
        or predecessor.release_kind != successor.release_kind
        or predecessor.schema_version != successor.schema_version
    ):
        raise IntegrityError("DBN successor does not bind its exact parent release")

    predecessor_files = {
        entry.logical_path: (entry.sha256, entry.size) for entry in predecessor.files
    }
    successor_files = {
        entry.logical_path: (entry.sha256, entry.size) for entry in successor.files
    }
    selected_paths: set[str] = set()
    for raw in selection["files"]:
        if not isinstance(raw, dict) or type(raw.get("market")) is not str:
            raise IntegrityError("successor selection file entry is invalid")
        if raw["market"] not in reused_markets:
            continue
        for field in ("path", "sidecar_path"):
            value = raw.get(field)
            if type(value) is not str or value in selected_paths:
                raise IntegrityError("reused source selection path is invalid or duplicate")
            selected_paths.add(value)
    if not selected_paths:
        raise IntegrityError("successor contract has no reused selected DBN paths")
    differences = [
        path
        for path in sorted(selected_paths)
        if predecessor_files.get(path) != successor_files.get(path)
    ]
    if differences:
        raise IntegrityError(
            "reused selected DBN bytes differ between parent and successor"
        )
    bindings = [
        {
            "logical_path": path,
            "sha256": successor_files[path][0],
            "size": successor_files[path][1],
        }
        for path in sorted(selected_paths)
    ]
    return {
        "predecessor_manifest_sha256": sha256_file(predecessor_path),
        "predecessor_release_id": predecessor_release_id,
        "reused_selected_binding_count": len(bindings),
        "reused_selected_bindings_id": sha256_json(bindings),
        "successor_manifest_sha256": sha256_file(successor_path),
        "successor_release_id": successor_release_id,
    }


def build_policy_successor_contract(
    *,
    boundary: RepoBoundary,
    predecessor_policy_receipt: VerifiedReleaseReceipt,
    successor_policy_receipt: VerifiedReleaseReceipt,
    selection_receipt: VerifiedReleaseReceipt,
    reused_markets: Sequence[str],
    rebuilt_markets: Sequence[str],
) -> dict[str, object]:
    """Build the only accepted 33-market reuse/eight-market rebuild proof."""

    reused = frozenset(reused_markets)
    rebuilt = frozenset(rebuilt_markets)
    if (
        rebuilt != REBUILT_MARKETS
        or not reused
        or reused & rebuilt
        or len(reused) != 33
    ):
        raise IntegrityError("foundation successor market partition is invalid")
    predecessor_manifest = _policy_manifest(
        predecessor_policy_receipt, boundary=boundary
    )
    successor_manifest = _policy_manifest(successor_policy_receipt, boundary=boundary)
    predecessor_docs = dict(predecessor_manifest.embedded_documents)
    successor_docs = dict(successor_manifest.embedded_documents)
    if any(
        predecessor_docs[name] != successor_docs[name]
        for name in _UNCHANGED_POLICY_DOCUMENTS
    ):
        raise IntegrityError(
            "non-economics foundation policy changed across the bounded successor"
        )

    predecessor_economics = predecessor_docs["contract_economics_rules.json"]
    successor_economics = successor_docs["contract_economics_rules.json"]
    if not isinstance(predecessor_economics, dict) or not isinstance(
        successor_economics, dict
    ):
        raise IntegrityError("foundation successor economics document is invalid")
    predecessor_rules = _rule_map(predecessor_economics, name="predecessor")
    successor_rules = _rule_map(successor_economics, name="successor")
    if (
        frozenset(predecessor_rules) != reused
        or frozenset(successor_rules) != reused | rebuilt
        or any(
            predecessor_rules[market] != successor_rules[market]
            for market in reused
        )
        or any(
            predecessor_economics.get(field) != successor_economics.get(field)
            for field in _UNCHANGED_ECONOMICS_FIELDS
        )
    ):
        raise IntegrityError(
            "foundation successor mutates or omits an accepted economics rule"
        )

    predecessor_sources = predecessor_economics.get("verification_sources")
    successor_sources = successor_economics.get("verification_sources")
    if not isinstance(predecessor_sources, dict) or not isinstance(
        successor_sources, dict
    ):
        raise IntegrityError("foundation successor source registry is invalid")
    for source_id, source in predecessor_sources.items():
        if source_id == _AUTHORITATIVE_SOURCE_ID:
            continue
        if successor_sources.get(source_id) != source:
            raise IntegrityError(
                "foundation successor mutates an existing human-review source"
            )
    added_sources = set(successor_sources) - set(predecessor_sources)
    if any(
        not isinstance(successor_sources[source_id], dict)
        or successor_sources[source_id].get("authoritative") is not False
        or successor_sources[source_id].get("binding")
        != "MUTABLE_PUBLIC_REFERENCE_NOT_TRUST_EVIDENCE"
        for source_id in added_sources
    ):
        raise IntegrityError("foundation successor adds an authoritative mutable source")
    predecessor_authority = predecessor_sources.get(_AUTHORITATIVE_SOURCE_ID)
    successor_authority = successor_sources.get(_AUTHORITATIVE_SOURCE_ID)
    if (
        not isinstance(predecessor_authority, dict)
        or not isinstance(successor_authority, dict)
        or {
            key: value
            for key, value in predecessor_authority.items()
            if key != "locator"
        }
        != {
            key: value for key, value in successor_authority.items() if key != "locator"
        }
    ):
        raise IntegrityError("foundation successor changes DBN authority semantics")

    predecessor_release_id = _authoritative_release_id(
        predecessor_economics, name="predecessor"
    )
    successor_release_id = _authoritative_release_id(
        successor_economics, name="successor"
    )
    source_evidence = _selected_source_superset_evidence(
        boundary=boundary,
        selection_receipt=selection_receipt,
        predecessor_release_id=predecessor_release_id,
        successor_release_id=successor_release_id,
        reused_markets=reused,
    )
    unchanged_documents = {
        name: sha256_json(predecessor_docs[name])
        for name in sorted(_UNCHANGED_POLICY_DOCUMENTS)
    }
    unchanged_rules = [
        {
            "market": market,
            "rule_sha256": sha256_json(predecessor_rules[market]),
        }
        for market in sorted(reused)
    ]
    added_rules = [
        {
            "market": market,
            "rule_sha256": sha256_json(successor_rules[market]),
        }
        for market in sorted(rebuilt)
    ]
    core: dict[str, object] = {
        "added_human_review_source_ids": sorted(added_sources),
        "added_rules": added_rules,
        "predecessor_economics_rulebook_hash": sha256_json(
            predecessor_economics
        ),
        "predecessor_policy_receipt": predecessor_policy_receipt.as_dict(),
        "predecessor_policy_set_id": predecessor_manifest.metadata["policy_set_id"],
        "provider_call_count": 0,
        "rebuilt_markets": sorted(rebuilt),
        "reused_markets": sorted(reused),
        "schema_version": SUCCESSOR_CONTRACT_SCHEMA_VERSION,
        "source_superset_evidence": source_evidence,
        "successor_economics_rulebook_hash": sha256_json(successor_economics),
        "successor_policy_receipt": successor_policy_receipt.as_dict(),
        "successor_policy_set_id": successor_manifest.metadata["policy_set_id"],
        "unchanged_policy_documents": unchanged_documents,
        "unchanged_rules": unchanged_rules,
    }
    return {**core, "successor_contract_id": sha256_json(core)}


def verify_policy_successor_contract(
    contract: Mapping[str, object],
    *,
    boundary: RepoBoundary,
    predecessor_policy_receipt: VerifiedReleaseReceipt,
    successor_policy_receipt: VerifiedReleaseReceipt,
    selection_receipt: VerifiedReleaseReceipt,
) -> None:
    if not isinstance(contract, dict):
        raise IntegrityError("foundation successor contract is invalid")
    observed_id = contract.get("successor_contract_id")
    core = {
        key: value for key, value in contract.items() if key != "successor_contract_id"
    }
    if observed_id != sha256_json(core):
        raise IntegrityError("foundation successor contract content address is invalid")
    rebuilt = contract.get("rebuilt_markets")
    reused = contract.get("reused_markets")
    if not isinstance(rebuilt, list) or not isinstance(reused, list):
        raise IntegrityError("foundation successor market partition is invalid")
    expected = build_policy_successor_contract(
        boundary=boundary,
        predecessor_policy_receipt=predecessor_policy_receipt,
        successor_policy_receipt=successor_policy_receipt,
        selection_receipt=selection_receipt,
        reused_markets=[str(item) for item in reused],
        rebuilt_markets=[str(item) for item in rebuilt],
    )
    if dict(contract) != expected:
        raise IntegrityError("foundation successor contract differs from exact evidence")


def _session_policy_binding(
    receipt: VerifiedReleaseReceipt,
    *,
    policy_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
) -> None:
    manifest = receipt.verify(boundary)
    policy_manifest = _policy_manifest(policy_receipt, boundary=boundary)
    if (
        manifest.release_kind != SESSION_RELEASE_KIND
        or manifest.source_release_ids != (policy_receipt.release_id,)
        or manifest.metadata.get("foundation_policy_receipt_id")
        != policy_receipt.receipt_id
        or manifest.metadata.get("foundation_policy_set_id")
        != policy_manifest.metadata.get("policy_set_id")
        or set(manifest.embedded_documents) != {"session_policy.json"}
        or manifest.embedded_documents["session_policy.json"]
        != policy_manifest.embedded_documents["session_policy.json"]
    ):
        raise IntegrityError("foundation successor session-policy binding is invalid")


def _canonical_checkpoint(
    relative_path: str, *, boundary: RepoBoundary
) -> dict[str, object]:
    path = boundary.assert_active_path(
        boundary.active_root / relative_path,
        purpose="foundation successor predecessor checkpoint",
        subtree="state/foundation_runs_v2",
    )
    try:
        assert_plain_file(path)
        raw = path.read_bytes()
        import json

        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("foundation predecessor checkpoint is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("foundation predecessor checkpoint is not canonical")
    expected_keys = {
        "checkpoint_id",
        "checkpoint_version",
        "completed",
        "layout_version",
        "run_contract",
        "run_id",
        "status",
    }
    core = {key: value for key, value in payload.items() if key != "checkpoint_id"}
    if set(payload) != expected_keys or payload.get("checkpoint_id") != sha256_json(core):
        raise IntegrityError("foundation predecessor checkpoint identity is invalid")
    return payload


def build_foundation_successor_provenance(
    *,
    boundary: RepoBoundary,
    predecessor_checkpoint_path: str,
    predecessor_policy_receipt: VerifiedReleaseReceipt,
    predecessor_session_receipt: VerifiedReleaseReceipt,
    successor_policy_receipt: VerifiedReleaseReceipt,
    successor_session_receipt: VerifiedReleaseReceipt,
    market_state_receipt: VerifiedReleaseReceipt,
    selection_receipt: VerifiedReleaseReceipt,
    policy_successor_contract: Mapping[str, object],
) -> dict[str, object]:
    """Bind the exact failed predecessor and the disjoint component policies."""

    verify_policy_successor_contract(
        policy_successor_contract,
        boundary=boundary,
        predecessor_policy_receipt=predecessor_policy_receipt,
        successor_policy_receipt=successor_policy_receipt,
        selection_receipt=selection_receipt,
    )
    _session_policy_binding(
        predecessor_session_receipt,
        policy_receipt=predecessor_policy_receipt,
        boundary=boundary,
    )
    _session_policy_binding(
        successor_session_receipt,
        policy_receipt=successor_policy_receipt,
        boundary=boundary,
    )
    market_state_manifest = market_state_receipt.verify(boundary)
    predecessor_manifest = _policy_manifest(
        predecessor_policy_receipt, boundary=boundary
    )
    if (
        market_state_manifest.release_kind != MARKET_STATE_RELEASE_KIND
        or market_state_manifest.metadata.get("foundation_policy_set_id")
        != predecessor_manifest.metadata.get("policy_set_id")
    ):
        raise IntegrityError("foundation successor market-state reuse is invalid")

    checkpoint = _canonical_checkpoint(
        predecessor_checkpoint_path, boundary=boundary
    )
    completed = checkpoint.get("completed")
    run_contract = checkpoint.get("run_contract")
    if (
        checkpoint.get("status") != "RUNNING"
        or not isinstance(completed, dict)
        or not isinstance(run_contract, dict)
        or checkpoint.get("run_id") != sha256_json(run_contract)
        or completed.get("foundation_policy")
        != predecessor_policy_receipt.as_dict()
        or completed.get("session_policy") != predecessor_session_receipt.as_dict()
        or completed.get("market_state") != market_state_receipt.as_dict()
        or "foundation_set" in completed
        or run_contract.get("source_selection_receipt")
        != selection_receipt.as_dict()
    ):
        raise IntegrityError("foundation predecessor checkpoint closure is invalid")
    interval_states = completed.get("intervals")
    if not isinstance(interval_states, dict) or not interval_states:
        raise IntegrityError("foundation predecessor interval checkpoint is invalid")
    required_phases = {
        "raw",
        "definitions",
        "causal",
        "status_eligibility",
        "economics",
        "feature_input",
        "outcome_source_input",
    }
    if any(
        not isinstance(state, dict) or set(state) != required_phases
        for state in interval_states.values()
    ):
        raise IntegrityError("foundation predecessor interval closure is incomplete")

    reused = policy_successor_contract.get("reused_markets")
    rebuilt = policy_successor_contract.get("rebuilt_markets")
    if not isinstance(reused, list) or not isinstance(rebuilt, list):
        raise IntegrityError("foundation successor market partition is invalid")
    reused_set = frozenset(str(item) for item in reused)
    rebuilt_set = frozenset(str(item) for item in rebuilt)
    reused_keys = sorted(
        key for key in interval_states if str(key).split("/", 1)[0] in reused_set
    )
    rebuilt_keys = sorted(
        key for key in interval_states if str(key).split("/", 1)[0] in rebuilt_set
    )
    if (
        len(reused_keys) != 565
        or len(rebuilt_keys) != 118
        or set(reused_keys) | set(rebuilt_keys) != set(interval_states)
    ):
        raise IntegrityError("foundation successor interval partition is not exact")

    component_cores = [
        {
            "foundation_policy_receipt": predecessor_policy_receipt.as_dict(),
            "markets": sorted(reused_set),
            "role": "REUSED_IMMUTABLE_33_MARKET_COMPONENT",
            "session_policy_receipt": predecessor_session_receipt.as_dict(),
        },
        {
            "foundation_policy_receipt": successor_policy_receipt.as_dict(),
            "markets": sorted(rebuilt_set),
            "role": "REBUILT_ECONOMICS_SUCCESSOR_8_MARKET_COMPONENT",
            "session_policy_receipt": successor_session_receipt.as_dict(),
        },
    ]
    components = [
        {**component, "component_id": sha256_json(component)}
        for component in component_cores
    ]
    core: dict[str, object] = {
        "components": components,
        "components_id": sha256_json(components),
        "market_state_reuse": {
            "economics_inputs_consumed": False,
            "market_state_release_receipt": market_state_receipt.as_dict(),
            "predecessor_policy_set_id": predecessor_manifest.metadata[
                "policy_set_id"
            ],
            "successor_policy_set_id": _policy_manifest(
                successor_policy_receipt, boundary=boundary
            ).metadata["policy_set_id"],
        },
        "policy_successor_contract": dict(policy_successor_contract),
        "predecessor_checkpoint_id": checkpoint["checkpoint_id"],
        "predecessor_checkpoint_path": predecessor_checkpoint_path,
        "predecessor_run_id": checkpoint["run_id"],
        "provider_call_count": 0,
        "rebuilt_interval_count": len(rebuilt_keys),
        "rebuilt_interval_keys_id": sha256_json(rebuilt_keys),
        "reused_interval_count": len(reused_keys),
        "reused_interval_keys_id": sha256_json(reused_keys),
        "schema_version": SUCCESSOR_PROVENANCE_SCHEMA_VERSION,
    }
    return {**core, "successor_provenance_id": sha256_json(core)}


def verify_foundation_successor_provenance(
    provenance: Mapping[str, object],
    *,
    boundary: RepoBoundary,
    selection_receipt: VerifiedReleaseReceipt,
    interval_markets_by_key: Mapping[str, str],
) -> dict[str, tuple[VerifiedReleaseReceipt, VerifiedReleaseReceipt]]:
    """Verify provenance and return the exact policy/session pair per market."""

    if not isinstance(provenance, dict):
        raise IntegrityError("foundation successor provenance is invalid")
    observed_id = provenance.get("successor_provenance_id")
    core = {
        key: value
        for key, value in provenance.items()
        if key != "successor_provenance_id"
    }
    if (
        observed_id != sha256_json(core)
        or provenance.get("schema_version")
        != SUCCESSOR_PROVENANCE_SCHEMA_VERSION
        or provenance.get("provider_call_count") != 0
    ):
        raise IntegrityError("foundation successor provenance identity is invalid")
    components = provenance.get("components")
    if (
        not isinstance(components, list)
        or len(components) != 2
        or provenance.get("components_id") != sha256_json(components)
    ):
        raise IntegrityError("foundation successor component contract is invalid")
    component_pairs: dict[str, tuple[VerifiedReleaseReceipt, VerifiedReleaseReceipt]] = {}
    parsed_components: list[
        tuple[VerifiedReleaseReceipt, VerifiedReleaseReceipt, frozenset[str]]
    ] = []
    for component in components:
        if not isinstance(component, dict):
            raise IntegrityError("foundation successor component is invalid")
        component_core = {
            key: value for key, value in component.items() if key != "component_id"
        }
        markets = component.get("markets")
        if (
            component.get("component_id") != sha256_json(component_core)
            or not isinstance(markets, list)
            or markets != sorted(set(str(item) for item in markets))
        ):
            raise IntegrityError("foundation successor component identity is invalid")
        try:
            policy = VerifiedReleaseReceipt.from_dict(
                component["foundation_policy_receipt"]
            )
            session = VerifiedReleaseReceipt.from_dict(
                component["session_policy_receipt"]
            )
        except (KeyError, IntegrityError) as exc:
            raise IntegrityError(
                "foundation successor component receipt is invalid"
            ) from exc
        _policy_manifest(policy, boundary=boundary)
        _session_policy_binding(
            session, policy_receipt=policy, boundary=boundary
        )
        market_set = frozenset(str(item) for item in markets)
        if any(market in component_pairs for market in market_set):
            raise IntegrityError("foundation successor components overlap")
        for market in market_set:
            component_pairs[market] = (policy, session)
        parsed_components.append((policy, session, market_set))

    predecessor_policy, predecessor_session, reused = parsed_components[0]
    successor_policy, successor_session, rebuilt = parsed_components[1]
    policy_contract = provenance.get("policy_successor_contract")
    if not isinstance(policy_contract, dict):
        raise IntegrityError("foundation successor policy contract is invalid")
    verify_policy_successor_contract(
        policy_contract,
        boundary=boundary,
        predecessor_policy_receipt=predecessor_policy,
        successor_policy_receipt=successor_policy,
        selection_receipt=selection_receipt,
    )
    market_state_reuse = provenance.get("market_state_reuse")
    try:
        if not isinstance(market_state_reuse, dict):
            raise IntegrityError("foundation successor market-state proof is invalid")
        market_state_receipt = VerifiedReleaseReceipt.from_dict(
            market_state_reuse["market_state_release_receipt"]
        )
    except (KeyError, IntegrityError) as exc:
        raise IntegrityError(
            "foundation successor market-state receipt is invalid"
        ) from exc
    expected = build_foundation_successor_provenance(
        boundary=boundary,
        predecessor_checkpoint_path=str(
            provenance.get("predecessor_checkpoint_path")
        ),
        predecessor_policy_receipt=predecessor_policy,
        predecessor_session_receipt=predecessor_session,
        successor_policy_receipt=successor_policy,
        successor_session_receipt=successor_session,
        market_state_receipt=market_state_receipt,
        selection_receipt=selection_receipt,
        policy_successor_contract=policy_contract,
    )
    if dict(provenance) != expected:
        raise IntegrityError("foundation successor provenance differs from exact evidence")
    observed_markets = frozenset(interval_markets_by_key.values())
    if observed_markets != reused | rebuilt or observed_markets != frozenset(
        component_pairs
    ):
        raise IntegrityError("foundation successor interval markets are incomplete")
    reused_keys = sorted(
        key for key, market in interval_markets_by_key.items() if market in reused
    )
    rebuilt_keys = sorted(
        key for key, market in interval_markets_by_key.items() if market in rebuilt
    )
    if (
        provenance.get("reused_interval_count") != len(reused_keys)
        or provenance.get("rebuilt_interval_count") != len(rebuilt_keys)
        or provenance.get("reused_interval_keys_id") != sha256_json(reused_keys)
        or provenance.get("rebuilt_interval_keys_id") != sha256_json(rebuilt_keys)
    ):
        raise IntegrityError("foundation successor interval binding is invalid")
    return component_pairs


@dataclass(frozen=True)
class HistoricalFoundationPolicyBinding:
    """Minimal immutable policy view used only to verify a historical release."""

    receipt: VerifiedReleaseReceipt
    boundary: RepoBoundary
    policy_set_id: str
    foundation: object

    def verify(self) -> None:
        manifest = _policy_manifest(self.receipt, boundary=self.boundary)
        if manifest.metadata.get("policy_set_id") != self.policy_set_id:
            raise IntegrityError("historical foundation policy binding changed")


def historical_policy_binding(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> HistoricalFoundationPolicyBinding:
    manifest = _policy_manifest(receipt, boundary=boundary)
    foundation = manifest.embedded_documents.get("foundation_policy.json")
    epochs = manifest.embedded_documents.get("provider_data_epochs.json")
    if not isinstance(foundation, dict) or not isinstance(epochs, dict):
        raise IntegrityError("historical foundation policy documents are invalid")
    epoch_hash = foundation.get("provider_data_epochs_sha256")
    if (
        type(epoch_hash) is not str
        or re.fullmatch(r"[0-9a-f]{64}", epoch_hash) is None
    ):
        raise IntegrityError("historical provider epoch hash is invalid")
    epoch_path = (
        boundary.active_root
        / "configs"
        / "provider_data_epochs.json"
    )
    # The successor contract already proves this document is unchanged.  Use
    # the active immutable-equivalent file solely to reproduce its tracked hash.
    if (
        manifest.embedded_documents.get("provider_data_epochs.json")
        != _read_json_file(epoch_path)
        or sha256_file(epoch_path) != epoch_hash
    ):
        raise IntegrityError("historical provider epoch binding is invalid")
    view = SimpleNamespace(
        policy_hash=sha256_json(foundation),
        provider_data_epochs_sha256=epoch_hash,
    )
    return HistoricalFoundationPolicyBinding(
        receipt=receipt,
        boundary=boundary,
        policy_set_id=str(manifest.metadata["policy_set_id"]),
        foundation=view,
    )


def _read_json_file(path: Path) -> object:
    import json

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("foundation successor JSON input is invalid") from exc
