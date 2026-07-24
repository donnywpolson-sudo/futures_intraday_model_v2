"""Verified immutable policy set required by causal foundation materialization."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..boundary import RepoBoundary
from ..canonical import sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from ..time_contracts import require_utc
from .economics import EconomicsRuleBook
from .policy import FoundationPolicy, KnownAnomalyPolicy


POLICY_RELEASE_KIND = "futures_foundation_policy_set"
POLICY_SCHEMA_VERSION = "2.0.0"
POLICY_FILENAMES = {
    "contract_economics_rules.json",
    "foundation_policy.json",
    "known_anomalies.json",
    "provider_data_epochs.json",
    "session_policy.json",
}


class VerifiedFoundationPolicies:
    def __init__(
        self,
        *,
        receipt: VerifiedReleaseReceipt,
        boundary: RepoBoundary,
        foundation: FoundationPolicy,
        anomalies: KnownAnomalyPolicy,
        economics: EconomicsRuleBook,
        session_rules: Mapping[str, tuple[str, time, int]],
        policy_set_id: str,
    ) -> None:
        self.receipt = receipt
        self.boundary = boundary
        self.foundation = foundation
        self.anomalies = anomalies
        self.economics = economics
        self.session_rules = MappingProxyType(dict(session_rules))
        self.policy_set_id = policy_set_id

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
    ) -> "VerifiedFoundationPolicies":
        manifest = receipt.verify(boundary)
        if (
            manifest.release_kind != POLICY_RELEASE_KIND
            or manifest.schema_version != POLICY_SCHEMA_VERSION
            or manifest.files
            or set(manifest.embedded_documents) != POLICY_FILENAMES
            or set(manifest.metadata) != {
                "policy_payload_release_id",
                "policy_set_id",
            }
        ):
            raise IntegrityError("foundation policy release file/kind contract is invalid")
        payload_release_id = manifest.metadata["policy_payload_release_id"]
        if (
            not isinstance(payload_release_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", payload_release_id) is None
        ):
            raise IntegrityError("foundation policy payload release ID is invalid")
        config_root = boundary.active_root / "configs"
        active_documents: dict[str, object] = {}
        for name in POLICY_FILENAMES:
            try:
                active_documents[name] = json.loads(
                    (config_root / name).read_text(encoding="utf-8")
                )
            except (OSError, ValueError) as exc:
                raise IntegrityError("active foundation policy JSON is invalid") from exc
        if dict(manifest.embedded_documents) != active_documents:
            raise IntegrityError("active foundation policies differ from their release")
        foundation = FoundationPolicy.from_file(config_root / "foundation_policy.json")
        anomalies = KnownAnomalyPolicy.from_file(
            config_root / "known_anomalies.json",
            expected_sha256=foundation.known_anomalies_sha256,
        )
        economics = EconomicsRuleBook.from_file(
            config_root / "contract_economics_rules.json"
        )
        rules = _load_session_rules(config_root / "session_policy.json")
        core = {
            "anomalies_sha256": anomalies.policy_hash,
            "economics_rulebook_hash": economics.rulebook_hash,
            "foundation_policy_hash": foundation.policy_hash,
            "provider_data_epochs_sha256": foundation.provider_data_epochs_sha256,
            "release_id": payload_release_id,
            "session_policy_sha256": sha256_file(
                config_root / "session_policy.json"
            ),
        }
        policy_set_id = sha256_json(core)
        if manifest.metadata["policy_set_id"] != policy_set_id:
            raise IntegrityError("foundation policy-set ID is invalid")
        return cls(
            receipt=receipt,
            boundary=boundary,
            foundation=foundation,
            anomalies=anomalies,
            economics=economics,
            session_rules=rules,
            policy_set_id=policy_set_id,
        )

    def verify(self) -> None:
        manifest = self.receipt.verify(self.boundary)
        if manifest.metadata.get("policy_set_id") != self.policy_set_id:
            raise IntegrityError("foundation policy release changed after loading")

    def exchange_session_date(self, exchange: str, event_at: datetime) -> date:
        event = require_utc(event_at, "bar_event_at")
        try:
            timezone_name, roll, offset = self.session_rules[exchange]
        except KeyError as exc:
            raise ContractError(f"foundation session policy has no rule for {exchange}") from exc
        local = event.astimezone(ZoneInfo(timezone_name))
        result = local.date()
        if local.timetz().replace(tzinfo=None) >= roll:
            result += timedelta(days=offset)
        return result


def _load_session_rules(path: Path) -> Mapping[str, tuple[str, time, int]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("foundation session policy JSON is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"policy_version", "rules"}
        or payload.get("policy_version") != "1.0.0"
        or not isinstance(payload.get("rules"), list)
        or not payload["rules"]
    ):
        raise IntegrityError("foundation session policy schema is invalid")
    rules: dict[str, tuple[str, time, int]] = {}
    normalized: list[dict[str, object]] = []
    for raw in payload["rules"]:
        if not isinstance(raw, dict) or set(raw) != {
            "exchange",
            "post_roll_day_offset",
            "session_roll_local",
            "timezone",
        }:
            raise IntegrityError("foundation session rule schema is invalid")
        exchange = raw["exchange"]
        timezone_name = raw["timezone"]
        offset = raw["post_roll_day_offset"]
        roll_text = raw["session_roll_local"]
        if (
            not isinstance(exchange, str)
            or not exchange
            or not isinstance(timezone_name, str)
            or not timezone_name
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or not 0 <= offset <= 2
            or not isinstance(roll_text, str)
            or exchange in rules
        ):
            raise IntegrityError("foundation session rule fields are invalid")
        try:
            ZoneInfo(timezone_name)
            roll = time.fromisoformat(roll_text)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise IntegrityError("foundation session rule timezone/time is invalid") from exc
        if roll.isoformat() != roll_text:
            raise IntegrityError("foundation session roll time is not canonical")
        rules[exchange] = (timezone_name, roll, offset)
        normalized.append(dict(raw))
    if normalized != sorted(normalized, key=lambda item: str(item["exchange"])):
        raise IntegrityError("foundation session rules are not canonically ordered")
    return MappingProxyType(rules)


def publish_foundation_policies(
    *,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
    config_root: Path,
) -> VerifiedReleaseReceipt:
    boundary.assert_active_path(
        config_root / "foundation_policy.json",
        purpose="foundation config input",
        subtree="configs",
    )
    if config_root.resolve(strict=False) != (boundary.active_root / "configs").resolve(
        strict=False
    ):
        raise ContractError("foundation policies must come from the exact active configs root")
    source_paths = {name: config_root / name for name in POLICY_FILENAMES}
    before = {name: sha256_file(path) for name, path in source_paths.items()}
    foundation = FoundationPolicy.from_file(source_paths["foundation_policy.json"])
    KnownAnomalyPolicy.from_file(
        source_paths["known_anomalies.json"],
        expected_sha256=foundation.known_anomalies_sha256,
    )
    EconomicsRuleBook.from_file(source_paths["contract_economics_rules.json"])
    _load_session_rules(source_paths["session_policy.json"])
    documents: dict[str, object] = {}
    for name, source in source_paths.items():
        try:
            documents[name] = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("foundation policy source JSON is invalid") from exc
    after = {name: sha256_file(path) for name, path in source_paths.items()}
    if before != after:
        raise IntegrityError("foundation policy source changed during publication")
    stage = publisher.create_stage("foundation_policy")
    provisional = ReleaseManifest.build(
        stage,
        phase="controls",
        release_kind=POLICY_RELEASE_KIND,
        schema_version=POLICY_SCHEMA_VERSION,
        logical_paths={},
        embedded_documents=documents,
        metadata={},
    )
    core = {
        "anomalies_sha256": before["known_anomalies.json"],
        "economics_rulebook_hash": EconomicsRuleBook.from_file(
            source_paths["contract_economics_rules.json"]
        ).rulebook_hash,
        "foundation_policy_hash": FoundationPolicy.from_file(
            source_paths["foundation_policy.json"]
        ).policy_hash,
        "provider_data_epochs_sha256": before["provider_data_epochs.json"],
        "release_id": provisional.release_id,
        "session_policy_sha256": before["session_policy.json"],
    }
    # policy_set_id includes the final release ID, and the final release includes
    # policy_set_id. Avoid a circular content address by binding the file-only
    # provisional release ID whose metadata is empty and immutable.
    policy_set_id = sha256_json(core)
    manifest = ReleaseManifest.build(
        stage,
        phase="controls",
        release_kind=POLICY_RELEASE_KIND,
        schema_version=POLICY_SCHEMA_VERSION,
        logical_paths={},
        embedded_documents=documents,
        metadata={"policy_set_id": policy_set_id, "policy_payload_release_id": provisional.release_id},
    )
    manifest_path = publisher.publish(stage, manifest)
    return VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
