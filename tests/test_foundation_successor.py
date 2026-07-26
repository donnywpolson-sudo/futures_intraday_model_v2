from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

import futures_rebuild.foundation.successor_contract as successor_contract
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.successor_contract import (
    REBUILT_MARKETS,
    _canonical_checkpoint,
    build_policy_successor_contract,
)
from futures_rebuild.foundation.support import POLICY_FILENAMES


REUSED_MARKETS = frozenset(
    {
        "6A",
        "6B",
        "6C",
        "6E",
        "6J",
        "6M",
        "CL",
        "ES",
        "GC",
        "HE",
        "HG",
        "HO",
        "KE",
        "LE",
        "NG",
        "NQ",
        "RB",
        "RTY",
        "SI",
        "SR1",
        "SR3",
        "TN",
        "UB",
        "YM",
        "ZB",
        "ZC",
        "ZF",
        "ZL",
        "ZM",
        "ZN",
        "ZS",
        "ZT",
        "ZW",
    }
)


@dataclass(frozen=True)
class FakeReceipt:
    name: str

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name}


def _rule(market: str) -> dict[str, object]:
    return {
        "expected_unit_qty": "1",
        "market": market,
        "point_value": "1",
        "quote_convention": "SYNTHETIC_TEST",
        "source_ids": ["CME", "DATABENTO_DEFINITION_GLBX_MDP3"],
    }


def _economics(
    markets: frozenset[str], *, release_id: str, version: str
) -> dict[str, object]:
    return {
        "authority_policy": {
            "eligible_contract_requires_provider_unit_qty_match": True,
            "mutable_public_urls_authorize_economics": False,
            "provider_sentinel_allowed": False,
            "rulebook_hash_bound_into_every_economics_record": True,
        },
        "currency": "USD",
        "dataset": "GLBX.MDP3",
        "forbidden_authorities": ["legacy"],
        "point_value_definition": "SYNTHETIC_TEST",
        "rules": [_rule(market) for market in sorted(markets)],
        "rules_version": version,
        "valid_from": "2010-01-01",
        "verification_sources": {
            "CME": {
                "authoritative": False,
                "binding": "MUTABLE_PUBLIC_REFERENCE_NOT_TRUST_EVIDENCE",
                "locator": "https://example.invalid",
                "role": "HUMAN_REVIEW_ONLY",
            },
            "DATABENTO_DEFINITION_GLBX_MDP3": {
                "authoritative": True,
                "binding": (
                    "EXACT_LAYOUT_V2_DBN_RELEASE_LOGICAL_DEFINITION_PATH_"
                    "AND_PROVIDER_EVENT_TIME"
                ),
                "locator": (
                    f"manifests/data_releases/dbn/{release_id}.json"
                    "#data/dbn/definition/{market}/{year}/{filename}"
                ),
                "role": (
                    "ACTUAL_CONTRACT_QUANTITY_TICK_CURRENCY_AND_EFFECTIVE_IDENTITY"
                ),
            },
        },
    }


def _documents(economics: dict[str, object]) -> dict[str, object]:
    return {
        name: (
            economics
            if name == "contract_economics_rules.json"
            else {"document": name, "unchanged": True}
        )
        for name in POLICY_FILENAMES
    }


def _setup_policy_evidence(monkeypatch):
    predecessor = FakeReceipt("predecessor")
    successor = FakeReceipt("successor")
    predecessor_documents = _documents(
        _economics(REUSED_MARKETS, release_id="a" * 64, version="1.2.0")
    )
    successor_economics = _economics(
        REUSED_MARKETS | REBUILT_MARKETS,
        release_id="b" * 64,
        version="1.3.0",
    )
    successor_economics["verification_sources"]["ADDED"] = {
        "authoritative": False,
        "binding": "MUTABLE_PUBLIC_REFERENCE_NOT_TRUST_EVIDENCE",
        "locator": "https://example.invalid/added",
        "role": "HUMAN_REVIEW_ONLY",
    }
    successor_documents = _documents(successor_economics)
    manifests = {
        predecessor: SimpleNamespace(
            embedded_documents=predecessor_documents,
            metadata={"policy_set_id": "c" * 64},
        ),
        successor: SimpleNamespace(
            embedded_documents=successor_documents,
            metadata={"policy_set_id": "d" * 64},
        ),
    }
    monkeypatch.setattr(
        successor_contract,
        "_policy_manifest",
        lambda receipt, *, boundary: manifests[receipt],
    )
    monkeypatch.setattr(
        successor_contract,
        "_selected_source_superset_evidence",
        lambda **_kwargs: {
            "predecessor_manifest_sha256": "e" * 64,
            "predecessor_release_id": "a" * 64,
            "reused_selected_binding_count": 66,
            "reused_selected_bindings_id": "f" * 64,
            "successor_manifest_sha256": "1" * 64,
            "successor_release_id": "b" * 64,
        },
    )
    return predecessor, successor, manifests


def test_policy_successor_accepts_only_exact_33_plus_8_delta(
    boundary, monkeypatch
) -> None:
    predecessor, successor, _ = _setup_policy_evidence(monkeypatch)
    contract = build_policy_successor_contract(
        boundary=boundary,
        predecessor_policy_receipt=predecessor,
        successor_policy_receipt=successor,
        selection_receipt=FakeReceipt("selection"),
        reused_markets=sorted(REUSED_MARKETS),
        rebuilt_markets=sorted(REBUILT_MARKETS),
    )
    assert contract["reused_markets"] == sorted(REUSED_MARKETS)
    assert contract["rebuilt_markets"] == sorted(REBUILT_MARKETS)
    assert contract["successor_contract_id"] == sha256_json(
        {
            key: value
            for key, value in contract.items()
            if key != "successor_contract_id"
        }
    )


def test_policy_successor_rejects_original_rule_mutation_and_partition_drift(
    boundary, monkeypatch
) -> None:
    predecessor, successor, manifests = _setup_policy_evidence(monkeypatch)
    successor_rules = manifests[successor].embedded_documents[
        "contract_economics_rules.json"
    ]["rules"]
    next(rule for rule in successor_rules if rule["market"] == "ES")[
        "point_value"
    ] = "2"
    with pytest.raises(IntegrityError, match="mutates or omits"):
        build_policy_successor_contract(
            boundary=boundary,
            predecessor_policy_receipt=predecessor,
            successor_policy_receipt=successor,
            selection_receipt=FakeReceipt("selection"),
            reused_markets=sorted(REUSED_MARKETS),
            rebuilt_markets=sorted(REBUILT_MARKETS),
        )

    predecessor, successor, _ = _setup_policy_evidence(monkeypatch)
    with pytest.raises(IntegrityError, match="market partition"):
        build_policy_successor_contract(
            boundary=boundary,
            predecessor_policy_receipt=predecessor,
            successor_policy_receipt=successor,
            selection_receipt=FakeReceipt("selection"),
            reused_markets=sorted(REUSED_MARKETS | {"6N"}),
            rebuilt_markets=sorted(REBUILT_MARKETS),
        )


def test_predecessor_checkpoint_hash_fails_closed(boundary) -> None:
    relative = (
        "state/foundation_runs_v2/"
        + "a" * 64
        + "/checkpoint.json"
    )
    path = boundary.active_root / relative
    path.parent.mkdir(parents=True)
    core = {
        "checkpoint_version": "4.0.0",
        "completed": {},
        "layout_version": "2.0.0",
        "run_contract": {},
        "run_id": "b" * 64,
        "status": "RUNNING",
    }
    path.write_bytes(
        canonical_bytes({**core, "checkpoint_id": "0" * 64}) + b"\n"
    )
    with pytest.raises(IntegrityError, match="checkpoint identity"):
        _canonical_checkpoint(relative, boundary=boundary)
