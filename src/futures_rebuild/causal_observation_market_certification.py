"""Independent, receipt-gated certification of complete causal markets.

This verifier deliberately does not call the causal producer or its candidate
verifier.  It performs a second low-level DBN decode, reconstructs the five
logical evidence tables, and compares them with every inactive Parquet
partition.  Two identical replay passes are required before a market can be
included in the final 41-market certificate.
"""

from __future__ import annotations

import json
import multiprocessing
import queue
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import io_path as canonical_io_path, sha256_file, sha256_json
from .causal_observation_canary import (
    SOURCE_FAMILIES,
    _binding,
    _is_registered_bounded_2025_pair,
    _query_contract,
)
from .causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_ID,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    EVIDENCE_SCHEMA_VERSION,
    RELEASE_KIND,
    SCHEMA_VERSION,
)
from .causal_observation_market_checkpoint import (
    CHECKPOINT_SET_SCHEMA,
    MARKET_ORDER,
    _load_market_entries,
    checkpoint_set_identity,
    run_authorized_market_checkpoint,
    validate_market_checkpoint_plan,
)
from .causal_observation_parquet import FILENAMES, FORMAT_VERSION, read_bundle
from .data_layout import LAYOUT_VERSION, MANIFEST_VERSION
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.decoder import (
    iter_bars,
    iter_definitions,
    iter_statistics,
    iter_statuses,
)
from .foundation.economics import EconomicsRuleBook
from .foundation.identity import DefinitionIndex
from .foundation.records import NANO, ProviderBar, datetime_to_ns, ns_to_datetime
from .research_gateway_policy import CAUSAL_OBSERVATION_FULL_BUILD_OPERATION


PLAN_SCHEMA = "causal_observation_market_certification_plan/1.0.0"
CERTIFICATE_SCHEMA = "causal_observation_market_certificate/1.0.0"
SET_CERTIFICATE_SCHEMA = "causal_observation_41_market_certificate/1.0.0"
CERTIFICATE_ROOT = Path("state/causal_observation_market_certificates")
MINUTE_NS = 60_000_000_000
HOUR_NS = 60 * MINUTE_NS
DAY_NS = 24 * HOUR_NS
AVAILABILITY_LAG_NS = 5_000_000_000
PROJECT_ZONE = ZoneInfo("America/Chicago")
REPLAY_PASSES = 2
DEVELOPMENT_END_EXCLUSIVE = "2025-07-13T22:00:00Z"
MARKET_CERTIFICATION_PLAN_OPERATION = (
    "CERTIFY_COMPLETE_MARKET_CAUSAL_OBSERVATION_ONCE"
)
CERTIFIER_BINDING_PATHS = (
    "configs/causal_observation_contract_v1.json",
    "configs/contract_economics_rules.json",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/causal_observation_canary.py",
    "src/futures_rebuild/causal_observation_foundation.py",
    "src/futures_rebuild/causal_observation_market_checkpoint.py",
    "src/futures_rebuild/causal_observation_market_certification.py",
    "src/futures_rebuild/causal_observation_parquet.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/foundation/decoder.py",
    "src/futures_rebuild/foundation/economics.py",
    "src/futures_rebuild/foundation/identity.py",
    "src/futures_rebuild/foundation/records.py",
    "src/futures_rebuild/research_gateway_policy.py",
)


@dataclass(frozen=True, slots=True)
class IndependentDecodedMarket:
    definitions: tuple[object, ...]
    primary_1m: tuple[ProviderBar, ...]
    reference_1s: Mapping[int, Mapping[str, int]]
    reference_1h: Mapping[int, ProviderBar]
    reference_1d: Mapping[int, ProviderBar]
    support_rows: tuple[tuple[int, str, str], ...]
    decoded_record_count: int


@dataclass(frozen=True)
class ReplayEvidence:
    evidence_id: str
    market: str
    attempt_id: str
    checkpoint_set_id: str
    source_file_count: int
    source_payload_bytes: int
    decoded_record_count: int
    partition_count: int
    observation_count: int
    negative_price_count: int
    output_bytes: int
    output_inventory: tuple[dict[str, object], ...]
    output_inventory_sha256: str
    deterministic_source_sample: tuple[dict[str, object], ...]
    deterministic_source_sample_sha256: str
    ordered_row_ids_sha256: str
    partition_evidence_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "evidence_id": self.evidence_id,
            "market": self.market,
            "attempt_id": self.attempt_id,
            "checkpoint_set_id": self.checkpoint_set_id,
            "source_file_count": self.source_file_count,
            "source_payload_bytes": self.source_payload_bytes,
            "decoded_record_count": self.decoded_record_count,
            "partition_count": self.partition_count,
            "observation_count": self.observation_count,
            "negative_price_count": self.negative_price_count,
            "output_bytes": self.output_bytes,
            "output_inventory": list(self.output_inventory),
            "output_inventory_sha256": self.output_inventory_sha256,
            "deterministic_source_sample": list(self.deterministic_source_sample),
            "deterministic_source_sample_sha256": self.deterministic_source_sample_sha256,
            "ordered_row_ids_sha256": self.ordered_row_ids_sha256,
            "partition_evidence_sha256": self.partition_evidence_sha256,
        }


def _plain_relative(value: object, name: str) -> Path:
    if type(value) is not str or not value:
        raise ContractError(f"{name} is absent")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ContractError(f"{name} is not a canonical contained path")
    return path


def _io_path(path: Path) -> Path:
    return canonical_io_path(path)


def _contained(root: Path, relative: object) -> Path:
    path = _plain_relative(relative, "certification path")
    candidate = (root / path).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("certification path escapes the repository") from exc
    return candidate


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(_io_path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"certification JSON is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"certification JSON is not an object: {path}")
    return value


def _independent_work_units(
    selected: Sequence[Mapping[str, object]], *, expected_count: int
) -> tuple[tuple[str, int, tuple[dict[str, object], ...]], ...]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
    families: dict[tuple[str, int], set[str]] = defaultdict(set)
    for item in selected:
        key = (str(item["market"]), int(item["year"]))
        grouped[key].append(dict(item))
        if item.get("kind") == "DBN":
            families[key].add(str(item.get("family")))
    units = []
    for key in sorted(grouped, key=lambda value: (value[0], value[1])):
        if "ohlcv_1m" not in families[key]:
            continue
        if "definition" not in families[key]:
            raise IntegrityError("independent work unit lacks definitions")
        units.append(
            (
                key[0],
                key[1],
                tuple(sorted(grouped[key], key=lambda item: str(item["path"]))),
            )
        )
    if len(units) != expected_count:
        raise IntegrityError("independent market work-unit count differs")
    return tuple(units)


def _independent_work_unit_window(
    entries: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    markets = {str(item["market"]) for item in entries}
    if len(markets) != 1:
        raise IntegrityError("independent work unit spans multiple markets")

    def exact_utc(value: object) -> datetime:
        text = str(value)
        rendered = text if "T" in text else f"{text}T00:00:00Z"
        try:
            parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IntegrityError("independent source interval is invalid") from exc
        if parsed.tzinfo != timezone.utc:
            raise IntegrityError("independent source interval is not UTC")
        return parsed

    start = min(exact_utc(item["interval_start_inclusive"]) for item in entries)
    end = max(exact_utc(item["interval_end_exclusive"]) for item in entries)
    if not start < end or end > datetime.fromisoformat(
        DEVELOPMENT_END_EXCLUSIVE.replace("Z", "+00:00")
    ):
        raise UnauthorizedOperation("independent work unit crosses development boundary")
    return {
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
    }


def _independent_month_windows(
    start_inclusive: str, end_exclusive: str
) -> tuple[tuple[int, int, str], ...]:
    start = datetime.fromisoformat(start_inclusive.replace("Z", "+00:00"))
    end_bound = datetime.fromisoformat(end_exclusive.replace("Z", "+00:00"))
    result: list[tuple[int, int, str]] = []
    while start < end_bound:
        next_month = (
            datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
            if start.month == 12
            else datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc)
        )
        end = min(next_month, end_bound)
        rendered_end = (
            end.date().isoformat()
            if end.time() == datetime.min.time()
            else end.strftime("%Y-%m-%dT%H%M%SZ")
        )
        result.append(
            (
                int(start.timestamp() * 1_000_000_000),
                int(end.timestamp() * 1_000_000_000),
                f"{start.date().isoformat()}_{rendered_end}",
            )
        )
        start = end
    return tuple(result)


def _write_create_only(path: Path, payload: Mapping[str, object]) -> None:
    io_path = _io_path(path)
    io_path.parent.mkdir(parents=True, exist_ok=True)
    with io_path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(dict(payload), stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")


def _certificate_path(checkpoint_set_id: str, market: str, attempt_id: str) -> str:
    return (CERTIFICATE_ROOT / checkpoint_set_id / market / f"{attempt_id}.json").as_posix()


def build_market_certification_plan(
    *,
    repository_root: Path,
    build_plan_path: str,
    build_plan: Mapping[str, object],
    build_plan_sha256: str,
) -> dict[str, object]:
    """Build deterministic preparation metadata without opening a DBN payload."""

    market = str(build_plan["target_market"])
    attempt_id = str(build_plan["attempt_id"])
    checkpoint_set_id = str(build_plan["checkpoint_set_id"])
    output = str(build_plan["output_staging_path"])
    checkpoint_path = f"{output}/market_checkpoint.json"
    certificate_path = _certificate_path(checkpoint_set_id, market, attempt_id)
    bindings = {
        relative: sha256_file(repository_root / relative)
        for relative in CERTIFIER_BINDING_PATHS
    }
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "operation": MARKET_CERTIFICATION_PLAN_OPERATION,
        "target_market": market,
        "attempt_id": attempt_id,
        "checkpoint_set": dict(build_plan["checkpoint_set"]),
        "checkpoint_set_id": checkpoint_set_id,
        "build_plan_path": build_plan_path,
        "build_plan_sha256": build_plan_sha256,
        "checkpoint_path": checkpoint_path,
        "output_staging_path": output,
        "certificate_path": certificate_path,
        "failure_path": certificate_path.removesuffix(".json") + ".failure.json",
        "certifier_implementation_bindings": bindings,
        "certifier_implementation_bindings_sha256": sha256_json(bindings),
        "source": dict(build_plan["source"]),
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "economics_rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "replay_passes": REPLAY_PASSES,
        "maximum_payload_bytes": int(build_plan["source"]["maximum_payload_bytes"])
        * REPLAY_PASSES,
        "provider_calls": 0,
        "holdout_allowed": False,
        "forward_allowed": False,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_market_certification_plan(root: Path, plan: Mapping[str, object]) -> None:
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    market = plan.get("target_market")
    attempt_id = plan.get("attempt_id")
    checkpoint_set_id = plan.get("checkpoint_set_id")
    source = plan.get("source")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("operation") != MARKET_CERTIFICATION_PLAN_OPERATION
        or market not in MARKET_ORDER
        or type(attempt_id) is not str
        or type(checkpoint_set_id) is not str
        or not isinstance(source, Mapping)
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or plan.get("economics_rulebook_sha256") != ECONOMICS_RULEBOOK_SHA256
        or plan.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or plan.get("replay_passes") != REPLAY_PASSES
        or plan.get("maximum_payload_bytes")
        != int(source.get("maximum_payload_bytes", -1)) * REPLAY_PASSES
        or plan.get("provider_calls") != 0
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("publication_authorized") is not False
        or plan.get("activation_authorized") is not False
        or plan.get("plan_id") != sha256_json(core)
        or plan.get("certificate_path")
        != _certificate_path(checkpoint_set_id, market, attempt_id)
        or plan.get("failure_path")
        != str(plan["certificate_path"]).removesuffix(".json") + ".failure.json"
    ):
        raise UnauthorizedOperation("market certification plan is not exact")
    build_plan_path = _contained(
        root, _plain_relative(plan.get("build_plan_path"), "build plan").as_posix()
    )
    if sha256_file(build_plan_path) != plan.get("build_plan_sha256"):
        raise IntegrityError("market certification build plan drifted")
    build_plan = _json(build_plan_path)
    validate_market_checkpoint_plan(root, build_plan)
    validate_certified_market_sequence(root, build_plan)
    expected = build_market_certification_plan(
        repository_root=root,
        build_plan_path=build_plan_path.relative_to(root).as_posix(),
        build_plan=build_plan,
        build_plan_sha256=str(plan["build_plan_sha256"]),
    )
    if dict(plan) != expected:
        raise IntegrityError("market certification plan differs from its build authority")
    checkpoint = _contained(
        root, _plain_relative(plan.get("checkpoint_path"), "checkpoint").as_posix()
    )
    if not checkpoint.is_file():
        raise UnauthorizedOperation("market checkpoint is not terminal")


def required_market_certification_scope(
    *, plan: Mapping[str, object], plan_sha256: str, checkpoint_sha256: str
) -> dict[str, str]:
    source = plan["source"]
    return {
        "operation_kind": "COMPLETE_MARKET_CAUSAL_OBSERVATION_CERTIFICATION_ONLY",
        "target_market": str(plan["target_market"]),
        "attempt_id": str(plan["attempt_id"]),
        "checkpoint_set_id": str(plan["checkpoint_set_id"]),
        "causal_contract_id": str(plan["causal_contract_id"]),
        "source_contract_id": str(source["source_contract_id"]),
        "canonical_release_id": str(source["canonical_release_id"]),
        "exact_source_entries_sha256": str(source["exact_source_entries_sha256"]),
        "exact_dbn_entries_sha256": str(source["exact_dbn_entries_sha256"]),
        "exact_dbn_file_count": str(source["exact_dbn_file_count"]),
        "maximum_payload_bytes": str(plan["maximum_payload_bytes"]),
        "replay_passes": str(REPLAY_PASSES),
        "checkpoint_sha256": checkpoint_sha256,
        "output_staging_path": str(plan["output_staging_path"]),
        "certificate_path": str(plan["certificate_path"]),
        "certifier_implementation_bindings_sha256": str(
            plan["certifier_implementation_bindings_sha256"]
        ),
        "provider_calls": "0",
        "holdout": "false",
        "forward": "false",
        "outcomes": "false",
        "features": "false",
        "fitting": "false",
        "prediction": "false",
        "evaluation": "false",
        "publication": "false",
        "activation": "false",
        "approval_command": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def bind_predecessor_market_certificate(
    *,
    repository_root: Path,
    build_plan: Mapping[str, object],
    certificate_path: str,
) -> dict[str, object]:
    """Bind a non-first market plan to the immediately preceding certificate."""

    market = str(build_plan.get("target_market"))
    if market not in MARKET_ORDER or MARKET_ORDER.index(market) == 0:
        raise ContractError("only a successor market has a predecessor certificate")
    path = _contained(
        repository_root,
        _plain_relative(certificate_path, "predecessor certificate").as_posix(),
    )
    certificate = _json(path)
    expected_market = MARKET_ORDER[MARKET_ORDER.index(market) - 1]
    if certificate.get("market") != expected_market:
        raise IntegrityError("predecessor market certificate order differs")
    core = {key: value for key, value in build_plan.items() if key != "plan_id"}
    core["certified_sequence_gate"] = {
        "schema_version": "causal_observation_certified_market_sequence/1.0.0",
        "implementation_path": "src/futures_rebuild/causal_observation_market_certification.py",
        "implementation_sha256": sha256_file(
            repository_root / "src/futures_rebuild/causal_observation_market_certification.py"
        ),
        "predecessor_market": expected_market,
        "predecessor_certificate_path": certificate_path,
        "predecessor_certificate_sha256": sha256_file(path),
        "predecessor_certificate_id": certificate.get("certificate_id"),
    }
    result = {**core, "plan_id": sha256_json(core)}
    validate_certified_market_sequence(repository_root, result)
    return result


def validate_certified_market_sequence(root: Path, build_plan: Mapping[str, object]) -> None:
    """Fail closed unless the preceding market remains robustly certified."""

    market = build_plan.get("target_market")
    if market not in MARKET_ORDER:
        raise ContractError("market sequence target is invalid")
    index = MARKET_ORDER.index(str(market))
    gate = build_plan.get("certified_sequence_gate")
    if index == 0:
        if gate is not None:
            raise IntegrityError("first market cannot claim a predecessor certificate")
        return
    if not isinstance(gate, Mapping):
        raise UnauthorizedOperation("next market lacks its predecessor certificate gate")
    expected_market = MARKET_ORDER[index - 1]
    path = _contained(
        root,
        _plain_relative(
            gate.get("predecessor_certificate_path"), "predecessor certificate"
        ).as_posix(),
    )
    certificate = _json(path)
    core = {key: value for key, value in certificate.items() if key != "certificate_id"}
    evidence = certificate.get("replay_evidence")
    inventory = evidence.get("output_inventory") if isinstance(evidence, Mapping) else None
    if (
        gate.get("schema_version")
        != "causal_observation_certified_market_sequence/1.0.0"
        or gate.get("implementation_path")
        != "src/futures_rebuild/causal_observation_market_certification.py"
        or gate.get("implementation_sha256")
        != sha256_file(root / str(gate["implementation_path"]))
        or gate.get("predecessor_market") != expected_market
        or gate.get("predecessor_certificate_sha256") != sha256_file(path)
        or gate.get("predecessor_certificate_id") != certificate.get("certificate_id")
        or certificate.get("certificate_id") != sha256_json(core)
        or certificate.get("status")
        != "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
        or certificate.get("market") != expected_market
        or certificate.get("checkpoint_set_id") != build_plan.get("checkpoint_set_id")
        or certificate.get("source_contract_id")
        != build_plan["source"]["source_contract_id"]
        or certificate.get("source_release_id")
        != build_plan["source"]["canonical_release_id"]
        or certificate.get("causal_contract_id") != build_plan.get("causal_contract_id")
        or certificate.get("publication_authorized") is not False
        or certificate.get("activation_authorized") is not False
        or not isinstance(inventory, list)
        or not inventory
    ):
        raise IntegrityError("predecessor market certificate is invalid or incompatible")
    for item in inventory:
        if not isinstance(item, Mapping):
            raise IntegrityError("predecessor output inventory entry is invalid")
        output = _contained(
            root, _plain_relative(item.get("path"), "predecessor output").as_posix()
        )
        if (
            not _io_path(output).is_file()
            or _io_path(output).stat().st_size != item.get("size")
            or sha256_file(_io_path(output)) != item.get("sha256")
        ):
            raise IntegrityError("predecessor certified output changed")


def run_authorized_sequenced_market_checkpoint(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
):
    """Run a later market only after readback of the preceding certificate."""

    root = repository_root.resolve(strict=True)
    build_plan = _json(plan_path.resolve(strict=True))
    validate_certified_market_sequence(root, build_plan)
    return run_authorized_market_checkpoint(
        repository_root=root, receipt=receipt, plan_path=plan_path
    )


def _aggregate(rows: Sequence[ProviderBar]) -> dict[str, int]:
    ordered = sorted(rows, key=lambda row: (row.event_at_ns, row.row_sha256))
    return {
        "open_nano": ordered[0].open_nano,
        "high_nano": max(row.high_nano for row in ordered),
        "low_nano": min(row.low_nano for row in ordered),
        "close_nano": ordered[-1].close_nano,
        "volume": sum(row.volume for row in ordered),
        "count": len(ordered),
    }


def _stream(target: dict[int, dict[str, int]], key: int, row: ProviderBar) -> None:
    current = target.get(key)
    if current is None:
        target[key] = _aggregate((row,))
        return
    current["high_nano"] = max(current["high_nano"], row.high_nano)
    current["low_nano"] = min(current["low_nano"], row.low_nano)
    current["close_nano"] = row.close_nano
    current["volume"] += row.volume
    current["count"] += 1


def _decode_independently(
    *, root: Path, entries: Sequence[Mapping[str, object]], window: Mapping[str, str]
) -> IndependentDecodedMarket:
    """Second DBN decode using only low-level immutable decoder primitives."""

    source_contract = _json(root / "configs/source_contract.json")
    release = source_contract["active_canonical_source"]
    inventory = source_contract["complete_inventory"]
    market = str(entries[0]["market"])
    start = datetime.fromisoformat(window["start"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(window["end"].replace("Z", "+00:00"))
    start_ns = datetime_to_ns(start, "independent replay start")
    end_ns = datetime_to_ns(end, "independent replay end")
    pairs: dict[str, dict[str, Mapping[str, object]]] = defaultdict(dict)
    for entry in entries:
        pairs[str(entry["path"]).removesuffix(".manifest.json")][str(entry["kind"])] = entry
    definitions = []
    primary: list[ProviderBar] = []
    ref_1s: dict[int, dict[str, int]] = {}
    ref_1h: dict[int, ProviderBar] = {}
    ref_1d: dict[int, ProviderBar] = {}
    support: list[tuple[int, str, str]] = []
    count = 0
    for base in sorted(pairs):
        pair = pairs[base]
        if set(pair) != {"DBN", "SIDECAR"}:
            raise IntegrityError("independent replay DBN pairing differs")
        dbn_entry = pair["DBN"]
        sidecar_entry = dict(pair["SIDECAR"])
        sidecar_entry["paired_dbn_sha256"] = dbn_entry["sha256"]
        sidecar_entry["paired_dbn_size_bytes"] = dbn_entry["size_bytes"]
        family = str(dbn_entry["family"])
        if family not in SOURCE_FAMILIES or dbn_entry["market"] != market:
            raise UnauthorizedOperation("independent replay source is outside the market")
        bounded = _is_registered_bounded_2025_pair(dbn_entry, sidecar_entry)
        binding = _binding(
            root=root,
            entry=dbn_entry,
            release_id=str(release["release_id"]),
            release_manifest_sha256=str(release["release_manifest_sha256"]),
            files_index_sha256=str(inventory["content_inventory_sha256"]),
            allow_registered_hardlinks=bounded,
        )
        query = _query_contract(root, sidecar_entry)
        if family == "definition":
            for row in iter_definitions(binding, market=market, expected_query_contract=query):
                count += 1
                if row.ts_recv_ns < end_ns and row.expiration_ns > start_ns:
                    definitions.append(row)
        elif family.startswith("ohlcv_"):
            for row in iter_bars(
                binding,
                market=market,
                expected_query_contract=query,
                schema=family.replace("_", "-"),
            ):
                count += 1
                if not start_ns <= row.event_at_ns < end_ns:
                    continue
                if family == "ohlcv_1m":
                    primary.append(row)
                elif family == "ohlcv_1s":
                    _stream(ref_1s, row.event_at_ns // MINUTE_NS * MINUTE_NS, row)
                elif family == "ohlcv_1h":
                    ref_1h[row.event_at_ns] = row
                else:
                    ref_1d[row.event_at_ns // DAY_NS * DAY_NS] = row
        elif family == "status":
            for row in iter_statuses(binding, market=market, expected_query_contract=query):
                count += 1
                if start_ns <= row.ts_event_ns < end_ns:
                    support.append((row.ts_event_ns, family, row.row_sha256))
        else:
            for row in iter_statistics(binding, market=market, expected_query_contract=query):
                count += 1
                if start_ns <= row.ts_event_ns < end_ns:
                    support.append((row.ts_event_ns, family, row.row_sha256))
    return IndependentDecodedMarket(
        definitions=tuple(definitions),
        primary_1m=tuple(primary),
        reference_1s=ref_1s,
        reference_1h=ref_1h,
        reference_1d=ref_1d,
        support_rows=tuple(sorted(support)),
        decoded_record_count=count,
    )


def _project_grouping(timestamp_ns: int) -> tuple[str, str, int, int]:
    utc = datetime.fromtimestamp(timestamp_ns // 1_000_000_000, tz=timezone.utc)
    local = utc.astimezone(PROJECT_ZONE)
    trade_date = local.date() + timedelta(days=1) if local.time() >= time(17) else local.date()
    start = datetime.combine(trade_date - timedelta(days=1), time(17), PROJECT_ZONE)
    end = datetime.combine(trade_date, time(17), PROJECT_ZONE)
    return (
        f"PROJECT-{trade_date.isoformat()}",
        trade_date.isoformat(),
        int(start.timestamp() * 1_000_000_000),
        int(end.timestamp() * 1_000_000_000),
    )


def _comparison(
    row_id: str,
    cadence: str,
    observed: Mapping[str, int],
    reference: Mapping[str, int] | None,
    complete: bool,
) -> dict[str, object]:
    if reference is None:
        result, exception = "SOURCE_MISSING", "REFERENCE_SOURCE_MISSING_NO_OVERWRITE"
    elif not complete:
        result, exception = "NOT_COMPARABLE", "INCOMPLETE_INTERVAL_NO_OVERWRITE"
    else:
        equal = all(observed[name] == reference[name] for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume"))
        result, exception = ("MATCH", "NONE") if equal else ("DISAGREEMENT", "PRESERVE_BOTH_NO_OVERWRITE")
    core = {
        "row_id": row_id,
        "source_cadence": "1m",
        "comparison_cadence": cadence,
        "interval_boundary_compatible": True,
        "result": result,
        "exception_state": exception,
    }
    return {"comparison_id": sha256_json(core), **core}


def _reconstruct_tables(
    *,
    market: str,
    decoded: IndependentDecodedMarket,
    start_ns: int,
    end_ns: int,
    source_contract_id: str,
    source_release_id: str,
    rulebook: EconomicsRuleBook,
    prior: Mapping[str, object] | None,
) -> tuple[dict[str, list[dict[str, object]]], Mapping[str, object]]:
    definitions = DefinitionIndex(decoded.definitions)
    bars = sorted(
        (row for row in decoded.primary_1m if start_ns <= row.event_at_ns < end_ns),
        key=lambda row: (row.event_at_ns, row.row_sha256),
    )
    if not bars or len({(row.event_at_ns, row.instrument_id) for row in bars}) != len(bars):
        raise IntegrityError("independent replay primary rows are empty or duplicate")
    observations: list[dict[str, object]] = []
    missingness: list[dict[str, object]] = []
    rolls: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    cadence: list[dict[str, object]] = []
    previous = dict(prior) if prior is not None else None
    initial_prior = previous
    by_hour: dict[int, list[ProviderBar]] = defaultdict(list)
    by_day: dict[int, list[ProviderBar]] = defaultdict(list)
    for bar in bars:
        bar_end = bar.event_at_ns + MINUTE_NS
        available = bar_end + AVAILABILITY_LAG_NS
        definition = definitions.resolve(
            bar,
            decision_at=ns_to_datetime(available, "independent replay availability"),
        )
        resolved = rulebook.resolve(market, definition)
        multiplier = rulebook.rules[market].expected_unit_qty * NANO
        if multiplier != multiplier.to_integral_value() or multiplier <= 0:
            raise IntegrityError("independent replay multiplier is invalid")
        session, trade_date, grouping_start, grouping_end = _project_grouping(bar.event_at_ns)
        core: dict[str, object] = {
            "market": market,
            "source_contract_id": source_contract_id,
            "source_release_id": source_release_id,
            "source_file_path": bar.source_file_path,
            "source_file_sha256": bar.source_file_sha256,
            "source_row_sha256": bar.row_sha256,
            "source_cadence": "1m",
            "bar_start_ns": bar.event_at_ns,
            "bar_end_ns": bar_end,
            "source_timestamp_ns": bar.event_at_ns,
            "available_at_ns": available,
            "decision_eligible_at_ns": available,
            "publisher_id": bar.publisher_id,
            "instrument_id": bar.instrument_id,
            "raw_symbol": definition.raw_symbol,
            "actual_contract": definition.raw_symbol,
            "definition_source_file_path": definition.source_file_path,
            "definition_source_file_sha256": definition.source_file_sha256,
            "definition_row_sha256": definition.row_sha256,
            "definition_event_at_ns": definition.ts_event_ns,
            "definition_received_at_ns": definition.ts_recv_ns,
            "listing_activation_ns": definition.activation_ns,
            "expiration_ns": definition.expiration_ns,
            "open_nano": bar.open_nano,
            "high_nano": bar.high_nano,
            "low_nano": bar.low_nano,
            "close_nano": bar.close_nano,
            "volume": bar.volume,
            "currency": definition.currency,
            "min_price_increment_nano": definition.min_price_increment_nano,
            "multiplier_nano": int(multiplier),
            "project_session_id": session,
            "project_trade_date": trade_date,
            "project_grouping_start_ns": grouping_start,
            "project_grouping_end_ns": grouping_end,
            "project_timezone": "America/Chicago",
            "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
        }
        observation = {"row_id": sha256_json(core), **core}
        observations.append(observation)
        evidence = {
            "market": market,
            "source_row_sha256": bar.row_sha256,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
        }
        missing_core = {
            "observation_row_id": observation["row_id"],
            "market": market,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "state": "OBSERVED_VALID",
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
            "evidence_sha256": sha256_json(evidence),
        }
        missingness.append({"evidence_id": sha256_json(missing_core), **missing_core})
        prior_contract = str(previous["actual_contract"]) if previous else definition.raw_symbol
        roll = previous is not None and prior_contract != definition.raw_symbol
        rolls.append({
            "row_id": observation["row_id"],
            "actual_contract_before": prior_contract,
            "actual_contract_after": definition.raw_symbol,
            "effective_time_ns": bar.event_at_ns if roll else None,
            "causal_selection_evidence_sha256": sha256_json({
                "definition_row_sha256": definition.row_sha256,
                "definition_received_at_ns": definition.ts_recv_ns,
                "prior_contract": prior_contract,
            }),
            "roll_flag": roll,
            "price_discontinuity_flag": bool(roll and int(previous["close_nano"]) != bar.open_nano) if previous else False,
            "crossing_status": "ROLL_BOUNDARY_UNADJUSTED" if roll else "NO_CROSSING",
        })
        flags = [
            "OHLC_VOLUME_TIMESTAMP_VALID",
            f"MULTIPLIER_{resolved.provider_unit_qty_state}",
            f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}",
        ]
        if min(bar.open_nano, bar.high_nano, bar.low_nano, bar.close_nano) < 0:
            flags.append("PROVIDER_VALID_NEGATIVE_PRICE")
        quality.append({
            "row_id": observation["row_id"],
            "row_identity_sha256": observation["row_id"],
            "ohlc_valid": True,
            "volume_valid": True,
            "timestamp_order_valid": True,
            "duplicate_state": "UNIQUE",
            "source_contract_id": source_contract_id,
            "source_release_id": source_release_id,
            "source_file_sha256": bar.source_file_sha256,
            "quality_flags": flags,
        })
        by_hour[bar.event_at_ns // HOUR_NS * HOUR_NS].append(bar)
        by_day[bar.event_at_ns // DAY_NS * DAY_NS].append(bar)
        previous = observation

    pairs = []
    if initial_prior is not None:
        pairs.append((initial_prior, observations[0]))
    pairs.extend(zip(observations, observations[1:]))
    for left, right in pairs:
        gap_start, gap_end = int(left["bar_end_ns"]), int(right["bar_start_ns"])
        if gap_end <= gap_start:
            continue
        support = [
            {"event_at_ns": ts, "family": family, "row_sha256": row_sha}
            for ts, family, row_sha in decoded.support_rows
            if gap_start <= ts < gap_end
        ]
        gap_core = {
            "observation_row_id": None,
            "market": market,
            "interval_start_ns": gap_start,
            "interval_end_ns": gap_end,
            "state": "UNKNOWN_FAIL_CLOSED",
            "authority": "OBSERVED_ABSENCE_WITH_STATUS_REVIEW_NO_SCHEDULE_AUTHORITY",
            "evidence_sha256": sha256_json({"gap_start_ns": gap_start, "gap_end_ns": gap_end, "support_rows": support}),
        }
        missingness.append({"evidence_id": sha256_json(gap_core), **gap_core})

    by_start = {int(row["bar_start_ns"]): row for row in observations}
    if decoded.reference_1s:
        for start, row in by_start.items():
            reference = decoded.reference_1s.get(start)
            observed = {name: int(row[name]) for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume")}
            cadence.append(_comparison(str(row["row_id"]), "1s", observed, reference, reference is not None and int(reference["count"]) == 60))
    for start, rows in sorted(by_hour.items()):
        first = by_start[min(row.event_at_ns for row in rows)]
        reference = decoded.reference_1h.get(start)
        reference_value = None if reference is None else _aggregate((reference,))
        cadence.append(_comparison(str(first["row_id"]), "1h", _aggregate(rows), reference_value, len(rows) == 60))
    for start, rows in sorted(by_day.items()):
        first = by_start[min(row.event_at_ns for row in rows)]
        reference = decoded.reference_1d.get(start)
        reference_value = None if reference is None else _aggregate((reference,))
        cadence.append(_comparison(str(first["row_id"]), "1d", _aggregate(rows), reference_value, len(rows) == 1440))
    return {
        "observations": observations,
        "missingness": missingness,
        "roll": rolls,
        "quality": quality,
        "cadence": cadence,
    }, observations[-1]


def _partition_manifest(
    *, stage: Path, market: str, year: int, interval: str, plan: Mapping[str, object], tables: Mapping[str, Sequence[Mapping[str, object]]]
) -> tuple[dict[str, object], list[dict[str, object]]]:
    expected_files = set(FILENAMES.values())
    io_stage = _io_path(stage)
    observed = {path.name for path in io_stage.iterdir() if path.is_file()}
    if observed != expected_files or any(path.is_dir() for path in io_stage.iterdir()):
        raise IntegrityError("certified partition file set is not exact")
    files = sorted(
        ({
            "logical_path": f"data/causally_gated_normalized/{market}/{year}/{interval}/{filename}",
            "sha256": sha256_file(_io_path(stage / filename)),
            "size": _io_path(stage / filename).stat().st_size,
        } for filename in expected_files),
        key=lambda item: str(item["logical_path"]),
    )
    metadata = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "storage_format": FORMAT_VERSION,
        "compression": "zstd-9",
        "deterministic_identity_columns_reconstructed": True,
        "causal_contract_id": plan["causal_contract_id"],
        "source_contract_id": plan["source"]["source_contract_id"],
        "source_release_id": plan["source"]["canonical_release_id"],
    }
    build_plan = plan["_build_plan"]
    metadata.update({
        "plan_id": build_plan["plan_id"],
        "plan_sha256": plan["build_plan_sha256"],
        "exact_source_entries_sha256": plan["source"]["exact_source_entries_sha256"],
        "economics_rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
        "economics_rulebook_id": ECONOMICS_RULEBOOK_ID,
        "observation_count": len(tables["observations"]),
        "missingness_count": len(tables["missingness"]),
        "roll_count": len(tables["roll"]),
        "quality_count": len(tables["quality"]),
        "cadence_comparison_count": len(tables["cadence"]),
        "outcome_count": 0,
        "feature_count": 0,
        "prediction_count": 0,
        "evaluation_count": 0,
        "publication_authorized": False,
        "activation_authorized": False,
    })
    core = {
        "embedded_documents": {},
        "files": files,
        "layout_version": LAYOUT_VERSION,
        "manifest_version": MANIFEST_VERSION,
        "metadata": metadata,
        "phase": "causally_gated_normalized",
        "release_kind": RELEASE_KIND,
        "schema_version": SCHEMA_VERSION,
        "source_release_ids": [plan["source"]["canonical_release_id"]],
    }
    return {**core, "release_id": sha256_json(core)}, files


def _is_dst_transition_date(timestamp_ns: int) -> bool:
    observed = ns_to_datetime(timestamp_ns, "source sample timestamp").date()
    if observed.month not in {3, 11}:
        return False
    first = observed.replace(day=1)
    first_sunday = 1 + ((6 - first.weekday()) % 7)
    transition = first_sunday + (7 if observed.month == 3 else 0)
    return observed.day == transition


def _deterministic_partition_sample(
    *, year: int, interval: str, tables: Mapping[str, Sequence[Mapping[str, object]]]
) -> list[dict[str, object]]:
    observations = list(tables["observations"])
    selected: dict[str, set[str]] = {}

    def select(row: Mapping[str, object], reason: str) -> None:
        selected.setdefault(str(row["row_id"]), set()).add(reason)

    select(observations[0], "PARTITION_FIRST")
    select(observations[-1], "PARTITION_LAST")
    by_id = {str(row["row_id"]): row for row in observations}
    for row in tables["roll"]:
        if row.get("roll_flag") is True:
            select(by_id[str(row["row_id"])], "ROLL")
    for row in observations:
        if min(int(row[name]) for name in ("open_nano", "high_nano", "low_nano", "close_nano")) < 0:
            select(row, "NEGATIVE_PRICE")
        if _is_dst_transition_date(int(row["bar_start_ns"])):
            select(row, "DST_TRANSITION_DATE")
    result = [
        {
            "kind": "OBSERVATION_SOURCE_BINDING",
            "year": year,
            "interval": interval,
            "reasons": sorted(reasons),
            "row_id": row_id,
            "bar_start_ns": by_id[row_id]["bar_start_ns"],
            "source_file_path": by_id[row_id]["source_file_path"],
            "source_file_sha256": by_id[row_id]["source_file_sha256"],
            "source_row_sha256": by_id[row_id]["source_row_sha256"],
            "definition_source_file_path": by_id[row_id]["definition_source_file_path"],
            "definition_source_file_sha256": by_id[row_id]["definition_source_file_sha256"],
            "definition_row_sha256": by_id[row_id]["definition_row_sha256"],
        }
        for row_id, reasons in sorted(selected.items())
    ]
    gaps = [row for row in tables["missingness"] if row.get("observation_row_id") is None]
    if gaps:
        gap = gaps[0]
        result.append({
            "kind": "SPARSE_GAP_EVIDENCE",
            "year": year,
            "interval": interval,
            "evidence_id": gap["evidence_id"],
            "interval_start_ns": gap["interval_start_ns"],
            "interval_end_ns": gap["interval_end_ns"],
            "state": gap["state"],
            "authority": gap["authority"],
            "evidence_sha256": gap["evidence_sha256"],
        })
    return result


def _replay_once(root: Path, plan: Mapping[str, object], checkpoint: Mapping[str, object]) -> ReplayEvidence:
    build_plan = _json(_contained(root, plan["build_plan_path"]))
    internal_plan = {**dict(plan), "_build_plan": build_plan}
    entries = _load_market_entries(root, build_plan)
    market = str(plan["target_market"])
    partitions = checkpoint.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise IntegrityError("market checkpoint partitions are absent")
    by_selector = {(int(item["year"]), str(item["interval"])): item for item in partitions}
    if len(by_selector) != len(partitions):
        raise IntegrityError("market checkpoint partition selector is duplicate")
    rulebook = EconomicsRuleBook.from_file(root / ECONOMICS_RULEBOOK_PATH)
    prior: Mapping[str, object] | None = None
    carried_support: tuple[tuple[int, str, str], ...] = ()
    partition_evidence: list[dict[str, object]] = []
    output_inventory: list[dict[str, object]] = []
    source_sample: list[dict[str, object]] = []
    all_row_ids: list[str] = []
    negative_prices = 0
    decoded_count = 0
    output_bytes = 0
    source_payload = 0
    used: set[tuple[int, str]] = set()
    for _, year, unit_entries in _independent_work_units(
        entries, expected_count=int(build_plan["source"]["work_unit_count"])
    ):
        window = _independent_work_unit_window(unit_entries)
        decoded = _decode_independently(root=root, entries=unit_entries, window=window)
        decoded_count += decoded.decoded_record_count
        source_payload += sum(int(item["size_bytes"]) for item in unit_entries if item["kind"] == "DBN")
        for start_ns, end_ns, interval in _independent_month_windows(
            window["start"], window["end"]
        ):
            bars = tuple(row for row in decoded.primary_1m if start_ns <= row.event_at_ns < end_ns)
            if not bars:
                continue
            sliced = IndependentDecodedMarket(
                definitions=decoded.definitions,
                primary_1m=bars,
                reference_1s={key: value for key, value in decoded.reference_1s.items() if start_ns <= key < end_ns},
                reference_1h={key: value for key, value in decoded.reference_1h.items() if start_ns <= key < end_ns},
                reference_1d={key: value for key, value in decoded.reference_1d.items() if start_ns <= key < end_ns},
                support_rows=tuple(sorted(carried_support + tuple(row for row in decoded.support_rows if start_ns <= row[0] < end_ns))),
                decoded_record_count=decoded.decoded_record_count,
            )
            expected, last = _reconstruct_tables(
                market=market,
                decoded=sliced,
                start_ns=start_ns,
                end_ns=end_ns,
                source_contract_id=str(plan["source"]["source_contract_id"]),
                source_release_id=str(plan["source"]["canonical_release_id"]),
                rulebook=rulebook,
                prior=prior,
            )
            item = by_selector.get((year, interval))
            if item is None or item.get("market") != market:
                raise IntegrityError("market checkpoint partition coverage differs")
            candidate = _contained(root, item["stage"]) / "candidate"
            actual = read_bundle(candidate)
            for name in FILENAMES:
                if actual[name] != expected[name]:
                    raise IntegrityError(f"independent replay {name} differs")
            manifest, files = _partition_manifest(
                stage=candidate,
                market=market,
                year=year,
                interval=interval,
                plan=internal_plan,
                tables=actual,
            )
            counts = {name: len(actual[name]) for name in FILENAMES}
            verifier_core = {
                "schema_version": "causal_observation_candidate_certificate/1.0.0",
                "status": "PASS_SYNTHETIC_OR_AUTHORIZED_CANDIDATE_ONLY_NOT_PUBLISHED",
                "release_id": manifest["release_id"],
                "manifest_identity": sha256_json(manifest),
                "file_inventory_sha256": sha256_json(files),
                "ordered_row_ids_sha256": sha256_json([row["row_id"] for row in actual["observations"]]),
                "counts": counts,
                "source_contract_id": plan["source"]["source_contract_id"],
                "source_release_id": plan["source"]["canonical_release_id"],
                "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
                "producer_success_flag_accepted": False,
                "publication_authorized": False,
                "activation_authorized": False,
                "outcome_count": 0,
                "feature_count": 0,
                "prediction_count": 0,
                "evaluation_count": 0,
            }
            bytes_here = sum(int(file["size"]) for file in files)
            if (
                item.get("release_id") != manifest["release_id"]
                or item.get("inventory_sha256") != sha256_json(files)
                or item.get("certificate_id") != sha256_json(verifier_core)
                or item.get("output_bytes") != bytes_here
            ):
                raise IntegrityError("independent partition identity differs")
            row_ids = [str(row["row_id"]) for row in actual["observations"]]
            if all_row_ids and int(actual["observations"][0]["bar_start_ns"]) <= int(prior["bar_start_ns"]):
                raise IntegrityError("market partition time continuity is not strict")
            all_row_ids.extend(row_ids)
            negative_prices += sum(min(int(row[name]) for name in ("open_nano", "high_nano", "low_nano", "close_nano")) < 0 for row in actual["observations"])
            output_bytes += bytes_here
            partition_evidence.append({
                "year": year,
                "interval": interval,
                "release_id": manifest["release_id"],
                "file_inventory_sha256": sha256_json(files),
                "logical_tables_sha256": sha256_json({name: sha256_json(actual[name]) for name in FILENAMES}),
            })
            output_inventory.extend(
                {
                    "path": (candidate / str(file["logical_path"]).rsplit("/", 1)[-1])
                    .relative_to(root)
                    .as_posix(),
                    "sha256": file["sha256"],
                    "size": file["size"],
                }
                for file in files
            )
            source_sample.extend(
                _deterministic_partition_sample(
                    year=year, interval=interval, tables=actual
                )
            )
            prior = last
            last_end = int(last["bar_end_ns"])
            carried_support = tuple(row for row in decoded.support_rows if last_end <= row[0] < end_ns)
            used.add((year, interval))
    if used != set(by_selector):
        raise IntegrityError("market checkpoint contains missing or extra partitions")
    if len(all_row_ids) != len(set(all_row_ids)):
        raise IntegrityError("market row identities are duplicate across partitions")
    output_inventory.sort(key=lambda item: str(item["path"]))
    source_sample.sort(key=sha256_json)
    if (
        source_payload != int(plan["source"]["maximum_payload_bytes"])
        or decoded_count > int(build_plan["limits"]["maximum_decoded_records"])
        or len(partition_evidence) != int(checkpoint["partition_count"])
        or output_bytes != int(checkpoint["output_bytes"])
        or sha256_json(partitions) != checkpoint["partition_inventory_sha256"]
    ):
        raise IntegrityError("market replay totals differ from the terminal checkpoint")
    core = {
        "market": market,
        "attempt_id": plan["attempt_id"],
        "checkpoint_set_id": plan["checkpoint_set_id"],
        "source_file_count": int(plan["source"]["exact_dbn_file_count"]),
        "source_payload_bytes": source_payload,
        "decoded_record_count": decoded_count,
        "partition_count": len(partition_evidence),
        "observation_count": len(all_row_ids),
        "negative_price_count": negative_prices,
        "output_bytes": output_bytes,
        "output_inventory": output_inventory,
        "output_inventory_sha256": sha256_json(output_inventory),
        "deterministic_source_sample": source_sample,
        "deterministic_source_sample_sha256": sha256_json(source_sample),
        "ordered_row_ids_sha256": sha256_json(all_row_ids),
        "partition_evidence_sha256": sha256_json(partition_evidence),
    }
    return ReplayEvidence(
        evidence_id=sha256_json(core),
        **{
            **core,
            "output_inventory": tuple(output_inventory),
            "deterministic_source_sample": tuple(source_sample),
        },
    )


def _replay_process_worker(
    root: str,
    plan: dict[str, object],
    checkpoint: dict[str, object],
    result_queue: object,
) -> None:
    """Process entrypoint; the parent consumes authority before spawning it."""

    try:
        evidence = _replay_once(Path(root), plan, checkpoint)
        result_queue.put(("PASS", evidence.as_dict()))  # type: ignore[attr-defined]
    except BaseException as exc:  # process boundary must preserve terminal evidence
        result_queue.put(  # type: ignore[attr-defined]
            ("FAIL", {"error_type": type(exc).__name__, "error_message": str(exc)})
        )


def _run_replay_in_fresh_process(
    root: Path, plan: Mapping[str, object], checkpoint: Mapping[str, object]
) -> ReplayEvidence:
    """Run one replay in a fresh spawned interpreter and return canonical evidence."""

    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue(maxsize=1)
    process = context.Process(
        target=_replay_process_worker,
        args=(str(root), dict(plan), dict(checkpoint), result_queue),
        name=f"V10-{plan['target_market']}-independent-replay",
    )
    process.start()
    try:
        process.join()
    except BaseException:
        if process.is_alive():
            process.terminate()
            process.join()
        raise
    try:
        status, payload = result_queue.get(timeout=5)
    except queue.Empty as exc:
        raise IntegrityError(
            f"independent replay process exited without evidence: {process.exitcode}"
        ) from exc
    finally:
        result_queue.close()
        result_queue.join_thread()
    if process.exitcode != 0 or status != "PASS" or not isinstance(payload, Mapping):
        error_type = payload.get("error_type") if isinstance(payload, Mapping) else None
        error_message = payload.get("error_message") if isinstance(payload, Mapping) else None
        raise IntegrityError(
            f"independent replay process failed: {error_type}: {error_message}"
        )
    values = dict(payload)
    values["output_inventory"] = tuple(values["output_inventory"])
    values["deterministic_source_sample"] = tuple(
        values["deterministic_source_sample"]
    )
    return ReplayEvidence(**values)  # type: ignore[arg-type]


def run_authorized_market_certification(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan_path = plan_path.resolve(strict=True)
    plan_path.relative_to(root)
    plan = _json(plan_path)
    validate_market_certification_plan(root, plan)
    checkpoint_path = _contained(root, plan["checkpoint_path"])
    checkpoint = _json(checkpoint_path)
    checkpoint_core = {key: value for key, value in checkpoint.items() if key != "result_id"}
    if (
        checkpoint.get("result_id") != sha256_json(checkpoint_core)
        or checkpoint.get("status") != "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE"
        or checkpoint.get("target_market") != plan["target_market"]
        or checkpoint.get("attempt_id") != plan["attempt_id"]
        or checkpoint.get("checkpoint_set_id") != plan["checkpoint_set_id"]
        or checkpoint.get("publication_authorized") is not False
        or checkpoint.get("activation_authorized") is not False
    ):
        raise IntegrityError("market checkpoint is not certifiable")
    certificate_path = _contained(root, plan["certificate_path"])
    failure_path = _contained(root, plan["failure_path"])
    if _io_path(certificate_path).exists() or _io_path(failure_path).exists():
        raise IntegrityError("market certification destination already exists")
    receipt.consume(
        boundary,
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_market_certification_scope(
            plan=plan,
            plan_sha256=sha256_file(plan_path),
            checkpoint_sha256=sha256_file(checkpoint_path),
        ),
    )
    try:
        passes = tuple(
            _run_replay_in_fresh_process(root, plan, checkpoint)
            for _ in range(REPLAY_PASSES)
        )
        if passes[0] != passes[1]:
            raise IntegrityError("independent replay is not deterministic")
        evidence = passes[0]
        core = {
            "schema_version": CERTIFICATE_SCHEMA,
            "status": "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE",
            "market": plan["target_market"],
            "attempt_id": plan["attempt_id"],
            "checkpoint_set_id": plan["checkpoint_set_id"],
            "checkpoint_result_id": checkpoint["result_id"],
            "checkpoint_sha256": sha256_file(checkpoint_path),
            "source_contract_id": plan["source"]["source_contract_id"],
            "source_release_id": plan["source"]["canonical_release_id"],
            "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
            "economics_rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "build_plan_id": _json(_contained(root, plan["build_plan_path"]))["plan_id"],
            "certification_plan_id": plan["plan_id"],
            "certifier_implementation_bindings_sha256": plan[
                "certifier_implementation_bindings_sha256"
            ],
            "certifier_implementation_sha256": plan[
                "certifier_implementation_bindings"
            ]["src/futures_rebuild/causal_observation_market_certification.py"],
            "receipt_id": receipt.receipt_id,
            "replay_passes": REPLAY_PASSES,
            "replay_evidence_id": evidence.evidence_id,
            "replay_evidence": evidence.as_dict(),
            "provider_calls": 0,
            "holdout_rows": 0,
            "forward_rows": 0,
            "outcomes": 0,
            "features": 0,
            "fitting": 0,
            "predictions": 0,
            "evaluations": 0,
            "publication_authorized": False,
            "activation_authorized": False,
        }
        certificate = {**core, "certificate_id": sha256_json(core)}
        _write_create_only(certificate_path, certificate)
        return certificate
    except (Exception, KeyboardInterrupt) as exc:
        failure = {
            "schema_version": "causal_observation_market_certification_failure/1.0.0",
            "status": "FAILED_MARKET_CERTIFICATION_CHECKPOINT_PRESERVED",
            "market": plan["target_market"],
            "attempt_id": plan["attempt_id"],
            "checkpoint_set_id": plan["checkpoint_set_id"],
            "receipt_id": receipt.receipt_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "checkpoint_or_partition_reuse_authorized": False,
            "required_remediation": "GENERAL_FIX_NEW_MARKET_ATTEMPT_AND_NEW_CERTIFICATION_RECEIPT",
            "publication_authorized": False,
            "activation_authorized": False,
        }
        _write_create_only(failure_path, failure)
        raise


def certify_complete_market_certificate_set(
    *,
    repository_root: Path,
    checkpoint_set: Mapping[str, object],
    market_certificates: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Create the inactive release-wide certificate from 41 robust market certificates."""

    if checkpoint_set.get("schema_version") != CHECKPOINT_SET_SCHEMA:
        raise ContractError("checkpoint set schema differs")
    root = repository_root.resolve(strict=True)
    checkpoint_set_id = checkpoint_set_identity(checkpoint_set)
    current_certifier_sha256 = sha256_file(
        root / "src/futures_rebuild/causal_observation_market_certification.py"
    )
    by_market: dict[str, Mapping[str, object]] = {}
    all_paths: set[str] = set()
    for certificate in market_certificates:
        core = {key: value for key, value in certificate.items() if key != "certificate_id"}
        market = certificate.get("market")
        evidence = certificate.get("replay_evidence")
        inventory = evidence.get("output_inventory") if isinstance(evidence, Mapping) else None
        source_sample = (
            evidence.get("deterministic_source_sample")
            if isinstance(evidence, Mapping)
            else None
        )
        evidence_core = (
            {key: value for key, value in evidence.items() if key != "evidence_id"}
            if isinstance(evidence, Mapping)
            else {}
        )
        if (
            market not in MARKET_ORDER
            or market in by_market
            or certificate.get("schema_version") != CERTIFICATE_SCHEMA
            or certificate.get("status") != "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
            or certificate.get("certificate_id") != sha256_json(core)
            or certificate.get("checkpoint_set_id") != checkpoint_set_id
            or certificate.get("source_contract_id") != checkpoint_set.get("source_contract_id")
            or certificate.get("source_release_id") != checkpoint_set.get("canonical_release_id")
            or certificate.get("causal_contract_id") != checkpoint_set.get("causal_contract_id")
            or certificate.get("certifier_implementation_sha256")
            != current_certifier_sha256
            or certificate.get("replay_passes") != REPLAY_PASSES
            or not isinstance(evidence, Mapping)
            or not isinstance(inventory, list)
            or not inventory
            or not isinstance(source_sample, list)
            or not source_sample
            or evidence.get("output_inventory_sha256") != sha256_json(inventory)
            or evidence.get("deterministic_source_sample_sha256")
            != sha256_json(source_sample)
            or evidence.get("evidence_id") != sha256_json(evidence_core)
            or certificate.get("replay_evidence_id") != evidence.get("evidence_id")
            or evidence.get("market") != market
            or evidence.get("attempt_id") != certificate.get("attempt_id")
            or evidence.get("checkpoint_set_id") != checkpoint_set_id
            or evidence.get("output_bytes")
            != sum(int(item.get("size", -1)) for item in inventory if isinstance(item, Mapping))
            or any(certificate.get(name) != 0 for name in ("provider_calls", "holdout_rows", "forward_rows", "outcomes", "features", "fitting", "predictions", "evaluations"))
            or certificate.get("publication_authorized") is not False
            or certificate.get("activation_authorized") is not False
        ):
            raise IntegrityError("market robustness certificate is invalid or incompatible")
        for item in inventory:
            if not isinstance(item, Mapping):
                raise IntegrityError("market output inventory entry is invalid")
            relative = _plain_relative(item.get("path"), "certified market output")
            rendered = relative.as_posix()
            path = _contained(root, rendered)
            if (
                rendered in all_paths
                or not _io_path(path).is_file()
                or _io_path(path).stat().st_size != item.get("size")
                or sha256_file(_io_path(path)) != item.get("sha256")
            ):
                raise IntegrityError("certified market output file differs")
            all_paths.add(rendered)
        by_market[str(market)] = certificate
    if set(by_market) != set(MARKET_ORDER):
        raise UnauthorizedOperation("robustness certificate set lacks exact 41-market coverage")
    if len(
        {
            certificate.get("certifier_implementation_bindings_sha256")
            for certificate in by_market.values()
        }
    ) != 1:
        raise IntegrityError("market certificates use incompatible certifier bindings")
    ordered_ids = [str(by_market[market]["certificate_id"]) for market in MARKET_ORDER]
    core = {
        "schema_version": SET_CERTIFICATE_SCHEMA,
        "status": "PASS_41_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE",
        "checkpoint_set_id": checkpoint_set_id,
        "market_count": len(MARKET_ORDER),
        "market_order": list(MARKET_ORDER),
        "market_certificate_ids": ordered_ids,
        "market_certificate_ids_sha256": sha256_json(ordered_ids),
        "total_partition_count": sum(int(by_market[m]["replay_evidence"]["partition_count"]) for m in MARKET_ORDER),
        "total_observation_count": sum(int(by_market[m]["replay_evidence"]["observation_count"]) for m in MARKET_ORDER),
        "total_output_bytes": sum(int(by_market[m]["replay_evidence"]["output_bytes"]) for m in MARKET_ORDER),
        "complete_output_file_count": len(all_paths),
        "complete_output_paths_sha256": sha256_json(sorted(all_paths)),
        "deterministic_source_sample_count": sum(
            len(by_market[m]["replay_evidence"]["deterministic_source_sample"])
            for m in MARKET_ORDER
        ),
        "deterministic_source_sample_sha256": sha256_json(
            [
                by_market[m]["replay_evidence"]["deterministic_source_sample_sha256"]
                for m in MARKET_ORDER
            ]
        ),
        "set_certifier_implementation_sha256": current_certifier_sha256,
        "source_contract_id": checkpoint_set["source_contract_id"],
        "source_release_id": checkpoint_set["canonical_release_id"],
        "causal_contract_id": checkpoint_set["causal_contract_id"],
        "development_end_exclusive": checkpoint_set["development_end_exclusive"],
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "certificate_id": sha256_json(core)}
