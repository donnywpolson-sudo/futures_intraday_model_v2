"""Deterministic contract-economics and continuous-roll integrity audit.

The public entry point deliberately consumes already-decoded DBN records.  It
does not open a provider connection, run a model, or interpret a roll as a
price return.  High-risk orchestration owns reading a real release and
publishing the resulting immutable report.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher,
)
from .errors import ContractError, IntegrityError
from .foundation.economics import EconomicsRuleBook
from .foundation.decoder import iter_bars, iter_definitions
from .foundation.records import ProviderBar, ProviderDefinition
from .foundation.snapshot import PublishedDbnRelease
from .high_risk import confirmation_required
from .source_symbology import build_query_contract


AUDIT_SCHEMA_VERSION = "1.0.0"
AUDIT_RELEASE_KIND = "contract_economics_signature_audit"
MAPPING_RESOLUTIONS = frozenset({"ohlcv-1d", "ohlcv-1h", "ohlcv-1m"})
CHECKPOINT_SCHEMA_VERSION = "phase8_contract_economics_audit_checkpoint/1.0.0"
AUDIT_POLICY_VERSION = "2.0.0"


def prepare_contract_economics_signature_audit(
    *, markets: Sequence[str], years: Sequence[int], dbn_release_id: str
) -> dict[str, object]:
    """Describe, but never execute, a real-history economics audit."""

    if not markets or not years or len(dbn_release_id) != 64:
        raise ContractError("audit preparation requires pinned markets, years, and release")
    return confirmation_required(
        "Audit point-in-time Databento contract economics and continuous rolls",
        scope={
            "markets": ", ".join(sorted(set(markets))),
            "years": f"{min(years)} through {max(years)}",
            "dbn_release_id": dbn_release_id,
            "provider_calls": "0",
            "evaluation": "0",
        },
        outputs=("one immutable contract_economics_signature_audit release",),
        preservation=(
            "Read only the pinned DBN release; preserve accepted releases and do not "
            "publish an economics registry, model output, or trading action."
        ),
    )


@dataclass(frozen=True)
class EconomicsSignature:
    market: str
    currency: str
    tick_size: Decimal
    contract_unit_quantity: Decimal
    point_value: Decimal
    tick_value: Decimal
    quote_convention: str

    @property
    def signature_id(self) -> str:
        return sha256_json(self.as_dict())

    def as_dict(self) -> dict[str, str]:
        return {
            "contract_unit_quantity": str(self.contract_unit_quantity),
            "currency": self.currency,
            "market": self.market,
            "point_value": str(self.point_value),
            "quote_convention": self.quote_convention,
            "tick_size": str(self.tick_size),
            "tick_value": str(self.tick_value),
        }


@dataclass(frozen=True)
class SignatureException:
    """A bounded human-reviewed explanation for one non-baseline signature."""

    market: str
    signature_id: str
    reason: str


@dataclass(frozen=True)
class RollBoundary:
    market: str
    before_instrument_id: int
    after_instrument_id: int
    before_at_ns: int
    after_at_ns: int
    elapsed_ns: int

    def as_dict(self) -> dict[str, int | str]:
        return {
            "after_at_ns": self.after_at_ns,
            "after_instrument_id": self.after_instrument_id,
            "before_at_ns": self.before_at_ns,
            "before_instrument_id": self.before_instrument_id,
            "elapsed_ns": self.elapsed_ns,
            "market": self.market,
        }


@dataclass(frozen=True)
class UnresolvedContract:
    market: str
    instrument_id: int
    first_event_at_ns: int
    reason: str

    def as_dict(self) -> dict[str, int | str]:
        return {
            "first_event_at_ns": self.first_event_at_ns,
            "instrument_id": self.instrument_id,
            "market": self.market,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ContractEconomicsAudit:
    signatures: Mapping[str, EconomicsSignature]
    contracts_by_signature: Mapping[str, tuple[int, ...]]
    roll_boundaries: tuple[RollBoundary, ...]
    unapproved_signature_ids: tuple[str, ...]
    bar_count: int
    unresolved_contracts: tuple[UnresolvedContract, ...] = ()
    mapping_resolution_by_market: Mapping[str, str] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.unapproved_signature_ids and not self.unresolved_contracts

    def as_dict(self) -> dict[str, object]:
        return {
            "bar_count": self.bar_count,
            "contracts_by_signature": {
                key: list(value) for key, value in sorted(self.contracts_by_signature.items())
            },
            "roll_boundaries": [item.as_dict() for item in self.roll_boundaries],
            "mapping_resolution_by_market": dict(sorted(self.mapping_resolution_by_market.items())),
            "schema_version": AUDIT_SCHEMA_VERSION,
            "signatures": {
                key: value.as_dict() for key, value in sorted(self.signatures.items())
            },
            "status": (
                "PASSED" if self.passed else
                "BLOCKED_UNRESOLVED_CONTRACT" if self.unresolved_contracts else
                "BLOCKED_UNAPPROVED_SIGNATURE"
            ),
            "unapproved_signature_ids": list(self.unapproved_signature_ids),
            "unresolved_contracts": [item.as_dict() for item in self.unresolved_contracts],
        }


@dataclass(frozen=True)
class VerifiedContractEconomicsAudit:
    """A readable, non-authorizing immutable audit result."""

    receipt: VerifiedReleaseReceipt
    payload: Mapping[str, object]

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "VerifiedContractEconomicsAudit":
        manifest = receipt.verify(boundary)
        if (
            receipt.phase != "reference"
            or manifest.release_kind != AUDIT_RELEASE_KIND
            or manifest.schema_version != AUDIT_SCHEMA_VERSION
            or len(manifest.source_release_ids) != 1
            or {entry.logical_path for entry in manifest.files}
            != {"data/reference/economics/contract_economics_signature_audit.json"}
        ):
            raise IntegrityError("contract economics audit receipt has the wrong contract")
        path = receipt.resolve_file(
            "data/reference/economics/contract_economics_signature_audit.json", boundary
        )
        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("contract economics audit payload is invalid") from exc
        if raw != canonical_bytes(payload) + b"\n" or not isinstance(payload, dict):
            raise IntegrityError("contract economics audit payload is not canonical")
        _validate_payload(payload)
        return cls(receipt, payload)


def require_phase8_passing_contract_economics_audit(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    rulebook: EconomicsRuleBook,
) -> VerifiedContractEconomicsAudit:
    """Require the all-market Databento audit before Phase 8 economics use."""

    verified = VerifiedContractEconomicsAudit.from_release(receipt, boundary)
    payload = verified.payload
    mappings = payload["mapping_resolution_by_market"]
    if (
        payload["status"] != "PASSED"
        or not isinstance(mappings, dict)
        or frozenset(mappings) != frozenset(rulebook.rules)
        or any(value not in MAPPING_RESOLUTIONS for value in mappings.values())
    ):
        raise IntegrityError("Phase 8 requires a passing all-market Databento economics audit")
    return verified


def load_signature_exceptions(path: Path) -> tuple[SignatureException, ...]:
    """Read the deliberately small, reviewable exception registry."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("signature exception registry is invalid") from exc
    if not isinstance(payload, dict) or set(payload) != {"exceptions", "schema_version"}:
        raise IntegrityError("signature exception registry schema is invalid")
    if payload["schema_version"] != AUDIT_SCHEMA_VERSION or not isinstance(payload["exceptions"], list):
        raise IntegrityError("signature exception registry version is invalid")
    parsed: list[SignatureException] = []
    seen: set[tuple[str, str]] = set()
    for raw in payload["exceptions"]:
        if not isinstance(raw, dict) or set(raw) != {"market", "reason", "signature_id"}:
            raise IntegrityError("signature exception entry is invalid")
        item = SignatureException(**raw)
        if (
            not item.market
            or len(item.signature_id) != 64
            or not item.reason.strip()
            or (item.market, item.signature_id) in seen
        ):
            raise IntegrityError("signature exception entry is ambiguous")
        seen.add((item.market, item.signature_id))
        parsed.append(item)
    return tuple(sorted(parsed, key=lambda item: (item.market, item.signature_id)))


def _publish_contract_economics_signature_audit(
    audit: ContractEconomicsAudit,
    *,
    source_release_id: str,
    boundary: RepoBoundary,
    publisher: PhasePublisher,
) -> VerifiedReleaseReceipt:
    """Codex orchestration hook; it never reads DBN or runs an evaluation."""

    if publisher.boundary != boundary or len(source_release_id) != 64:
        raise ContractError("audit publisher or source release is invalid")
    if not audit.passed:
        raise IntegrityError("blocked economics audit cannot publish an acceptance receipt")
    payload = audit.as_dict()
    _validate_payload(payload)
    stage = publisher.create_stage("contract_economics_signature_audit")
    filename = "contract_economics_signature_audit.json"
    (stage / filename).write_bytes(canonical_bytes(payload) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=AUDIT_RELEASE_KIND,
        schema_version=AUDIT_SCHEMA_VERSION,
        logical_paths={filename: "data/reference/economics/contract_economics_signature_audit.json"},
        source_release_ids=(source_release_id,),
        metadata={
            "authoritative_economics": False,
            "publication_status": "AUDIT_ONLY",
            "signature_count": len(audit.signatures),
            "status": audit.as_dict()["status"],
        },
    )
    receipt = VerifiedReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    VerifiedContractEconomicsAudit.from_release(receipt, boundary)
    return receipt


def _run_pinned_dbn_contract_economics_audit(
    *,
    manifest_path: Path,
    rulebook_path: Path,
    exceptions_path: Path,
    boundary: RepoBoundary,
) -> VerifiedReleaseReceipt:
    """Codex-only execution hook for an approved, pinned historical audit.

    There is intentionally no command-line route to this function.  It streams
    verified daily continuous mapping bars, opens no network connection, and publishes only the
    non-authorizing audit report.
    """

    snapshot = PublishedDbnRelease.open(manifest_path, boundary=boundary, verify_files=False)
    rulebook = EconomicsRuleBook.from_file(rulebook_path)
    exceptions = load_signature_exceptions(exceptions_path)
    checkpoint_context = {
        "audit_policy_version": AUDIT_POLICY_VERSION,
        "dbn_release_id": snapshot.source_release_id,
        "exceptions_id": sha256_json([item.__dict__ for item in exceptions]),
        "rulebook_hash": rulebook.rulebook_hash,
    }
    # Inputs define the checkpoint namespace.  A policy or rulebook update
    # must never overwrite or reuse checkpoints made under different rules.
    checkpoint_root = (
        boundary.active_root
        / "state"
        / "phase8_contract_economics_audit"
        / sha256_json(checkpoint_context)
    )
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    definitions_by_market: dict[str, list[tuple[object, dict[str, object]]]] = {}
    bar_candidates: dict[str, dict[str, list[tuple[object, dict[str, object]]]]] = {}
    for relative, binding in sorted(snapshot.files.items()):
        if not relative.endswith(".dbn.zst"):
            continue
        parts = relative.split("/")
        if len(parts) != 5 or parts[0] != "dbn":
            raise IntegrityError("DBN audit input path is invalid")
        _, directory, market, _, filename = parts
        schema = {
            "definition": "definition",
            "ohlcv_1d": "ohlcv-1d",
            "ohlcv_1h": "ohlcv-1h",
            "ohlcv_1m": "ohlcv-1m",
        }.get(directory)
        if schema is None:
            continue
        sidecar = snapshot.file(f"{relative}.manifest.json")
        binding.verify()
        sidecar.verify()
        contract = _query_contract_from_sidecar(sidecar.path, schema=schema, market=market)
        if contract is None:
            # Legacy parent-symbol bar files are diagnostic duplicates.  The
            # continuous mapping remains the only audit input.
            continue
        if schema == "definition":
            definitions_by_market.setdefault(market, []).append((binding, contract))
        else:
            bar_candidates.setdefault(market, {}).setdefault(schema, []).append((binding, contract))

    priority = ("ohlcv-1d", "ohlcv-1h", "ohlcv-1m")
    bars_by_market: dict[str, list[tuple[object, dict[str, object]]]] = {}
    mapping_resolution: dict[str, str] = {}
    for market, candidates in bar_candidates.items():
        selected = next((schema for schema in priority if candidates.get(schema)), None)
        if selected is not None:
            bars_by_market[market] = candidates[selected]
            mapping_resolution[market] = selected

    signatures: dict[str, EconomicsSignature] = {}
    contracts: dict[str, set[int]] = {}
    boundaries: list[RollBoundary] = []
    unapproved: set[str] = set()
    unresolved: list[UnresolvedContract] = []
    bar_count = 0
    if set(definitions_by_market) != set(bars_by_market):
        raise IntegrityError("DBN audit definition/bar market coverage differs")
    for market in sorted(bars_by_market):
        checkpoint_path = checkpoint_root / f"{market}.json"
        market_audit = _load_market_checkpoint(
            checkpoint_path, context=checkpoint_context, market=market
        )
        if market_audit is None:
            definitions: list[ProviderDefinition] = []
            for binding, contract in definitions_by_market[market]:
                definitions.extend(
                    iter_definitions(binding, market=market, expected_query_contract=contract)
                )

            def bars() -> Iterable[ProviderBar]:
                for binding, contract in bars_by_market[market]:
                    yield from iter_bars(
                        binding,
                        market=market,
                        schema=mapping_resolution[market],
                        expected_query_contract=contract,
                    )

            market_audit = audit_contract_economics(
                definitions, bars(), rulebook=rulebook, exceptions=exceptions
            )
            del definitions
            market_audit = ContractEconomicsAudit(
                signatures=market_audit.signatures,
                contracts_by_signature=market_audit.contracts_by_signature,
                roll_boundaries=market_audit.roll_boundaries,
                unapproved_signature_ids=market_audit.unapproved_signature_ids,
                bar_count=market_audit.bar_count,
                unresolved_contracts=market_audit.unresolved_contracts,
                mapping_resolution_by_market={market: mapping_resolution[market]},
            )
            _write_market_checkpoint(
                checkpoint_path, context=checkpoint_context, audit=market_audit
            )
        signatures.update(market_audit.signatures)
        for signature_id, instrument_ids in market_audit.contracts_by_signature.items():
            contracts.setdefault(signature_id, set()).update(instrument_ids)
        boundaries.extend(market_audit.roll_boundaries)
        unapproved.update(market_audit.unapproved_signature_ids)
        unresolved.extend(market_audit.unresolved_contracts)
        bar_count += market_audit.bar_count
    audit = ContractEconomicsAudit(
        signatures=dict(sorted(signatures.items())),
        contracts_by_signature={key: tuple(sorted(value)) for key, value in sorted(contracts.items())},
        roll_boundaries=tuple(sorted(boundaries, key=lambda item: (item.market, item.after_at_ns, item.after_instrument_id))),
        unapproved_signature_ids=tuple(sorted(unapproved)),
        bar_count=bar_count,
        unresolved_contracts=tuple(
            sorted(unresolved, key=lambda item: (item.market, item.instrument_id, item.reason))
        ),
        mapping_resolution_by_market=mapping_resolution,
    )
    if not audit.passed:
        raise IntegrityError(
            "all-market economics audit is blocked; no acceptance receipt was published"
        )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=OperationReceipt.issue_local(
            boundary,
            operation="PUBLISH_RELEASE",
            classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
            scope={"release_kind": AUDIT_RELEASE_KIND, "source_release_id": snapshot.source_release_id},
        ),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    return _publish_contract_economics_signature_audit(
        audit,
        source_release_id=snapshot.source_release_id,
        boundary=boundary,
        publisher=publisher,
    )


def audit_contract_economics(
    definitions: Iterable[ProviderDefinition],
    bars: Iterable[ProviderBar],
    *,
    rulebook: EconomicsRuleBook,
    exceptions: Sequence[SignatureException] = (),
) -> ContractEconomicsAudit:
    """Resolve each actual bar to a valid definition and audit its economics.

    The first signature observed for a market is its baseline.  Any later
    signature requires a matching, explicit exception; a blocked result still
    returns its complete deterministic findings for review.
    """

    by_instrument: dict[tuple[str, int], list[ProviderDefinition]] = {}
    for definition in definitions:
        by_instrument.setdefault((definition.market, definition.instrument_id), []).append(definition)
    for candidates in by_instrument.values():
        candidates.sort(key=lambda item: (item.ts_recv_ns, item.ts_event_ns, item.row_sha256))

    approved = {(item.market, item.signature_id) for item in exceptions}
    signatures: dict[str, EconomicsSignature] = {}
    contracts: dict[str, set[int]] = {}
    baseline_by_market: dict[str, str] = {}
    unapproved: set[str] = set()
    boundaries: list[RollBoundary] = []
    unresolved: dict[tuple[str, int, str], UnresolvedContract] = {}
    previous_by_market: dict[str, ProviderBar] = {}
    bar_count = 0

    for bar in bars:
        bar_count += 1
        try:
            definition = _definition_at(
                by_instrument.get((bar.market, bar.instrument_id), ()), bar, rulebook
            )
            signature = _signature_for(definition, rulebook)
        except IntegrityError as exc:
            key = (bar.market, bar.instrument_id, str(exc))
            unresolved.setdefault(
                key,
                UnresolvedContract(
                    bar.market, bar.instrument_id, bar.event_at_ns, str(exc)
                ),
            )
        else:
            signature_id = signature.signature_id
            signatures.setdefault(signature_id, signature)
            contracts.setdefault(signature_id, set()).add(bar.instrument_id)
            baseline = baseline_by_market.setdefault(bar.market, signature_id)
            if signature_id != baseline and (bar.market, signature_id) not in approved:
                unapproved.add(signature_id)
        previous = previous_by_market.get(bar.market)
        if previous is not None and bar.event_at_ns < previous.event_at_ns:
            raise IntegrityError("bars are not chronologically ordered within a market")
        if previous is not None and previous.instrument_id != bar.instrument_id:
            boundaries.append(
                RollBoundary(
                    market=bar.market,
                    before_instrument_id=previous.instrument_id,
                    after_instrument_id=bar.instrument_id,
                    before_at_ns=previous.event_at_ns,
                    after_at_ns=bar.event_at_ns,
                    elapsed_ns=bar.event_at_ns - previous.event_at_ns,
                )
            )
        previous_by_market[bar.market] = bar

    if not bar_count:
        raise IntegrityError("economics audit has no bars")
    return ContractEconomicsAudit(
        signatures=dict(sorted(signatures.items())),
        contracts_by_signature={key: tuple(sorted(value)) for key, value in sorted(contracts.items())},
        roll_boundaries=tuple(boundaries),
        unapproved_signature_ids=tuple(sorted(unapproved)),
        bar_count=bar_count,
        unresolved_contracts=tuple(
            sorted(
                unresolved.values(),
                key=lambda item: (item.market, item.instrument_id, item.reason),
            )
        ),
    )


def _definition_at(
    candidates: Sequence[ProviderDefinition], bar: ProviderBar, rulebook: EconomicsRuleBook
) -> ProviderDefinition:
    eligible = [
        item
        for item in candidates
        if item.publisher_id == bar.publisher_id
        and item.ts_recv_ns <= bar.event_at_ns
        and (item.activation_ns in {0, 2**64 - 1} or item.activation_ns <= bar.event_at_ns)
        and (item.expiration_ns in {0, 2**64 - 1} or bar.event_at_ns < item.expiration_ns)
    ]
    if not eligible:
        raise IntegrityError("bar has no point-in-time eligible definition")
    chosen = eligible[-1]
    tied = [item for item in eligible if item.ts_recv_ns == chosen.ts_recv_ns]
    if len(tied) > 1:
        try:
            signatures = {_signature_for(item, rulebook).signature_id for item in tied}
        except IntegrityError as exc:
            raise IntegrityError("bar definition resolution is ambiguous") from exc
        if len(signatures) != 1:
            raise IntegrityError("bar definition resolution is ambiguous")
        chosen = min(tied, key=lambda item: item.row_sha256)
    return chosen


def _signature_for(definition: ProviderDefinition, rulebook: EconomicsRuleBook) -> EconomicsSignature:
    try:
        resolved = rulebook.resolve(definition.market, definition)
    except ContractError as exc:
        raise IntegrityError(
            f"definition contradicts the pinned economics rule: {exc}"
        ) from exc
    # Unit quantity and quote convention come from the protected rulebook.
    # Databento verifies the supplied value when present in a definition;
    # _resolve_economics rejects any disagreement.
    unit_qty = rulebook.rules[definition.market].expected_unit_qty
    if resolved.tick_size * resolved.point_value != resolved.tick_value:
        raise IntegrityError("tick size multiplied by point value is inconsistent")
    return EconomicsSignature(
        market=definition.market,
        currency=resolved.currency,
        tick_size=resolved.tick_size,
        contract_unit_quantity=unit_qty,
        point_value=resolved.point_value,
        tick_value=resolved.tick_value,
        quote_convention=resolved.quote_convention,
    )


def _validate_payload(payload: object) -> None:
    required = {
        "bar_count", "contracts_by_signature", "roll_boundaries", "schema_version",
        "mapping_resolution_by_market", "signatures", "status", "unapproved_signature_ids", "unresolved_contracts",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise IntegrityError("contract economics audit schema is invalid")
    if (
        payload["schema_version"] != AUDIT_SCHEMA_VERSION
        or payload["status"] not in {
            "PASSED", "BLOCKED_UNAPPROVED_SIGNATURE", "BLOCKED_UNRESOLVED_CONTRACT"
        }
        or not isinstance(payload["bar_count"], int)
        or payload["bar_count"] <= 0
        or not isinstance(payload["signatures"], dict)
        or not isinstance(payload["contracts_by_signature"], dict)
        or not isinstance(payload["roll_boundaries"], list)
        or not isinstance(payload["unapproved_signature_ids"], list)
        or not isinstance(payload["unresolved_contracts"], list)
        or not isinstance(payload["mapping_resolution_by_market"], dict)
    ):
        raise IntegrityError("contract economics audit fields are invalid")
    if (
        tuple(payload["mapping_resolution_by_market"])
        != tuple(sorted(payload["mapping_resolution_by_market"]))
        or any(value not in MAPPING_RESOLUTIONS for value in payload["mapping_resolution_by_market"].values())
    ):
        raise IntegrityError("contract economics mapping resolutions are invalid")
    signature_ids = tuple(sorted(payload["signatures"]))
    if not signature_ids or tuple(payload["signatures"]) != signature_ids:
        raise IntegrityError("contract economics signatures are not canonical")
    if tuple(payload["contracts_by_signature"]) != signature_ids:
        raise IntegrityError("contract economics signature groups differ")
    blocked = tuple(payload["unapproved_signature_ids"])
    if blocked != tuple(sorted(set(blocked))) or any(item not in payload["signatures"] for item in blocked):
        raise IntegrityError("contract economics unapproved signatures are invalid")
    if (payload["status"] == "PASSED") != (not blocked and not payload["unresolved_contracts"]):
        raise IntegrityError("contract economics status disagrees with findings")


def _query_contract_from_sidecar(
    path: Path, *, schema: str, market: str
) -> dict[str, object] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("DBN audit sidecar is invalid") from exc
    required = {"end", "market", "schema", "start", "stype_in", "symbols_requested"}
    if not isinstance(raw, dict) or not required.issubset(raw):
        raise IntegrityError("DBN audit sidecar lacks its query contract")
    if raw["schema"] != schema or raw["market"] != market:
        raise IntegrityError("DBN audit sidecar differs from its path")
    if schema in {"ohlcv-1d", "ohlcv-1h", "ohlcv-1m"} and raw["stype_in"] == "parent":
        return None
    return build_query_contract(
        schema=schema,
        market=market,
        start=str(raw["start"]),
        end=str(raw["end"]),
        stype_in=raw["stype_in"],
        symbols=raw["symbols_requested"],
    )


def _write_market_checkpoint(
    path: Path, *, context: Mapping[str, str], audit: ContractEconomicsAudit
) -> None:
    payload = {
        "audit": audit.as_dict(),
        "context": dict(sorted(context.items())),
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
    }
    encoded = canonical_bytes(payload) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
    except FileExistsError:
        existing = path.read_bytes()
        if existing != encoded:
            raise IntegrityError("Phase 8 market checkpoint conflicts with current audit inputs")


def _load_market_checkpoint(
    path: Path, *, context: Mapping[str, str], market: str
) -> ContractEconomicsAudit | None:
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 8 market checkpoint is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION
        or payload.get("context") != dict(sorted(context.items()))
        or not isinstance(payload.get("audit"), dict)
        or raw != canonical_bytes(payload) + b"\n"
    ):
        raise IntegrityError("Phase 8 market checkpoint differs from current audit inputs")
    return _audit_from_payload(payload["audit"], required_market=market)


def _audit_from_payload(payload: Mapping[str, object], *, required_market: str) -> ContractEconomicsAudit:
    _validate_payload(payload)
    mappings = payload["mapping_resolution_by_market"]
    if mappings != {required_market: mappings.get(required_market)}:
        raise IntegrityError("Phase 8 market checkpoint has the wrong mapping scope")
    signatures = {
        key: EconomicsSignature(
            market=value["market"], currency=value["currency"],
            tick_size=Decimal(value["tick_size"]),
            contract_unit_quantity=Decimal(value["contract_unit_quantity"]),
            point_value=Decimal(value["point_value"]), tick_value=Decimal(value["tick_value"]),
            quote_convention=value["quote_convention"],
        )
        for key, value in payload["signatures"].items()
    }
    return ContractEconomicsAudit(
        signatures=signatures,
        contracts_by_signature={key: tuple(value) for key, value in payload["contracts_by_signature"].items()},
        roll_boundaries=tuple(RollBoundary(**item) for item in payload["roll_boundaries"]),
        unapproved_signature_ids=tuple(payload["unapproved_signature_ids"]),
        bar_count=payload["bar_count"],
        unresolved_contracts=tuple(UnresolvedContract(**item) for item in payload["unresolved_contracts"]),
        mapping_resolution_by_market=mappings,
    )
