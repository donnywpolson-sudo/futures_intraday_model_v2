"""Pure causal-bar assembly with explicit denominator-preserving dispositions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Sequence

from ..errors import ContractError
from ..identity import ActualContractIdentity
from ..time_contracts import require_utc
from .economics import EconomicsRuleBook, ResolvedEconomics
from .identity import DefinitionSource, SessionDateResolver, actual_identity_as_of
from .policy import FoundationPolicy, KnownAnomalyPolicy
from .records import ProviderBar, ProviderDefinition, datetime_to_ns, ns_to_datetime


class CoverageDisposition(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    ANOMALY_QUARANTINED = "ANOMALY_QUARANTINED"


@dataclass(frozen=True)
class CausalBarResult:
    actual: ActualContractIdentity
    definition: ProviderDefinition
    economics: ResolvedEconomics
    event_at_ns: int
    available_at_ns: int
    decision_at_ns: int
    event_at: datetime
    available_at: datetime
    decision_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    availability_basis: str
    availability_policy_hash: str
    provider_timestamp_epoch_id: str
    source_release_id: str
    source_manifest_sha256: str
    source_file_path: str
    source_file_sha256: str
    source_row_sha256: str
    definition_row_sha256: str
    disposition: CoverageDisposition
    prediction_in_coverage_denominator: bool


def build_causal_bar(
    bar: ProviderBar,
    definitions: Sequence[ProviderDefinition] | DefinitionSource,
    *,
    decision_at: datetime,
    policy: FoundationPolicy,
    anomaly_policy: KnownAnomalyPolicy,
    session_policy: SessionDateResolver,
    economics_rules: EconomicsRuleBook,
) -> CausalBarResult:
    decision = require_utc(decision_at, "decision_at")
    decision_ns = datetime_to_ns(decision, "decision_at")
    available_ns = policy.bar_available_at_ns(bar.event_at_ns)
    if decision_ns < available_ns:
        raise ContractError("bar is not modeled available at the decision time")
    policy.assert_definition_lifecycle_trusted(bar.event_at_ns)
    actual, definition, economics = actual_identity_as_of(
        bar,
        definitions,
        decision_at=decision,
        session_policy=session_policy,
        economics_rules=economics_rules,
    )
    opening, high, low, closing = bar.prices
    disposition = (
        CoverageDisposition.ANOMALY_QUARANTINED
        if anomaly_policy.is_quarantined(bar.market, bar.event_at.year)
        else CoverageDisposition.ELIGIBLE
    )
    return CausalBarResult(
        actual=actual,
        definition=definition,
        economics=economics,
        event_at_ns=bar.event_at_ns,
        available_at_ns=available_ns,
        decision_at_ns=decision_ns,
        event_at=bar.event_at,
        available_at=ns_to_datetime(available_ns, "bar.available_at_ns"),
        decision_at=decision,
        open=opening,
        high=high,
        low=low,
        close=closing,
        volume=bar.volume,
        availability_basis=policy.availability_basis,
        availability_policy_hash=policy.policy_hash,
        provider_timestamp_epoch_id=policy.provider_timestamp_epoch_id(
            bar.event_at_ns
        ),
        source_release_id=bar.source_release_id,
        source_manifest_sha256=bar.source_manifest_sha256,
        source_file_path=bar.source_file_path,
        source_file_sha256=bar.source_file_sha256,
        source_row_sha256=bar.row_sha256,
        definition_row_sha256=definition.row_sha256,
        disposition=disposition,
        prediction_in_coverage_denominator=True,
    )
