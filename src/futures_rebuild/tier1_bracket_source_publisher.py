"""Verified causal-source adapter for local Tier 1 bracket staging.

This is deliberately not a release publisher.  It resolves one causal receipt
through the accepted Phase 8 index, revalidates its economics context, and
writes only resumable local feature/outcome chunks.  Promotion to immutable
research artifacts remains a separate boundary.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Iterator, Mapping

from .boundary import RepoBoundary
from .canonical import sha256_file, sha256_json
from .data_layout import DataReleaseReceipt
from .errors import IntegrityError
from .current_research_surface import reject_retired_project_execution
from .foundation.materialize import load_causal_interval
from .foundation.support import VerifiedFoundationPolicies
from .phase8_economics_index import (
    Phase8EconomicsContext,
    load_phase8_actual_contract_economics_index,
    resolve_indexed_phase8_economics,
)
from .producer_bridge import (
    load_actual_contract_definitions,
    load_versioned_session_policy,
)
from .tier1_bracket_checkpoint import checkpoint_path
from .tier1_bracket_interval_resolver import BracketIntervalBinding, checkpoint_context
from .tier1_bracket_materializer import (
    DEFAULT_CHUNK_ROWS,
    indexed_bracket_economics_from_registry,
    write_streamed_bracket_chunks,
)


def _receipt(root: Path, phase: str, release_id: str, boundary: RepoBoundary) -> DataReleaseReceipt:
    if not isinstance(release_id, str) or len(release_id) != 64:
        raise IntegrityError("bracket source receipt ID is invalid")
    return DataReleaseReceipt.from_manifest(
        root / "manifests" / "data_releases" / phase / f"{release_id}.json", boundary
    )


def _causal_batches(path: Path, *, rows_per_batch: int) -> Iterator[list[dict[str, object]]]:
    """Read causal Parquet in bounded batches without inventing source fields."""

    if type(rows_per_batch) is not int or rows_per_batch <= 0:
        raise IntegrityError("bracket source batch size is invalid")
    try:
        import pyarrow.parquet as pq
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=rows_per_batch):
            yield batch.to_pylist()
    except IntegrityError:
        raise
    except Exception as exc:  # pragma: no cover - dependency and file boundary
        raise IntegrityError("bracket causal source cannot be streamed") from exc


def stage_indexed_bracket_market_year(
    *,
    root: Path,
    phase8_index_release_id: str,
    audit_receipt_id: str,
    causal_release_id: str,
    signal_contract_id: str,
    stress_round_trip_cost_usd: Decimal,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    rows_per_batch: int = 50_000,
) -> Mapping[str, object]:
    """Stage one fully verified causal interval as resumable bracket chunks.

    The index, audit, policy, session, definitions, and causal receipts must
    agree before the first Parquet batch is opened.  A completed checkpoint is
    exact-reuse only; a changed input creates a different checkpoint location.
    """

    reject_retired_project_execution(
        root=root, surface="Tier 1 bracket historical source staging"
    )

    boundary = RepoBoundary(root)
    index = _receipt(root, "reference", phase8_index_release_id, boundary)
    audit = _receipt(root, "reference", audit_receipt_id, boundary)
    causal = _receipt(root, "causally_gated_normalized", causal_release_id, boundary)
    causal_path, causal_report = load_causal_interval(causal, boundary=boundary)
    market, year = causal_report.get("market"), causal_report.get("year")
    if market not in {"ES", "CL", "ZN", "6E"} or type(year) is not int or not isinstance(stress_round_trip_cost_usd, Decimal) or stress_round_trip_cost_usd < 0:
        raise IntegrityError("bracket source interval or cost scope is invalid")

    # Loading the index with the entry's policy release first prevents a stale
    # active config from silently changing the authority used for this source.
    index_manifest = index.verify(boundary)
    if index_manifest.schema_version != "1.1.0":
        raise IntegrityError("bracket source requires the current Phase 8 index schema")
    import json
    try:
        raw_index = json.loads(index.resolve_file(
            "data/reference/economics/phase8_actual_contract_economics_index.json", boundary
        ).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("Phase 8 index payload is unreadable") from exc
    entries = raw_index.get("economics_by_interval") if isinstance(raw_index, dict) else None
    matches = [item for item in entries if isinstance(item, dict) and item.get("causal_release_id") == causal_release_id] if isinstance(entries, list) else []
    if len(matches) != 1:
        raise IntegrityError("causal source has no unique current Phase 8 index entry")
    entry = matches[0]
    economics = _receipt(root, "reference", str(entry.get("economics_release_id")), boundary)
    economics_manifest = economics.verify(boundary)
    dependencies = set(economics_manifest.source_release_ids)
    dependencies.discard(causal_release_id)
    def _dependency(phase: str, kind: str) -> DataReleaseReceipt:
        matches = []
        for release_id in dependencies:
            path = root / "manifests" / "data_releases" / phase / f"{release_id}.json"
            if path.is_file():
                receipt = DataReleaseReceipt.from_manifest(path, boundary)
                if receipt.release_kind == kind:
                    matches.append(receipt)
        if len(matches) != 1:
            raise IntegrityError(f"selected economics registry lacks one {kind} dependency")
        return matches[0]

    policy_receipt = _dependency("controls", "futures_foundation_policy_set")
    session_receipt = _dependency("controls", "versioned_session_policy")
    definitions_receipt = _dependency("reference", "actual_contract_definitions")
    if (
        policy_receipt.receipt_id != entry.get("foundation_policy_receipt_id")
        or session_receipt.receipt_id != entry.get("session_policy_receipt_id")
        or definitions_receipt.receipt_id != entry.get("definition_receipt_id")
        or economics.receipt_id != entry.get("economics_receipt_id")
    ):
        raise IntegrityError("Phase 8 index receipt provenance differs from the economics registry")
    policies = VerifiedFoundationPolicies.from_release(policy_receipt, boundary=boundary)
    # Re-open under the policy rulebook after the dependency chain has been
    # authenticated; this validates the index's canonical payload too.
    payload = load_phase8_actual_contract_economics_index(index, boundary=boundary, rulebook=policies.economics)
    raw_id = str(causal_report.get("source_raw_release_id"))
    definitions = load_actual_contract_definitions(
        definitions_receipt,
        raw_receipt=_receipt(root, "raw", raw_id, boundary), policies=policies, boundary=boundary,
    )
    session = load_versioned_session_policy(
        session_receipt,
        policies=policies, boundary=boundary,
    )
    registry = resolve_indexed_phase8_economics(
        index, audit_receipt=audit,
        context=Phase8EconomicsContext(causal_receipt=causal, definitions=definitions, policies=policies, session_policy=session),
        boundary=boundary,
    )
    selected = next(item for item in payload["economics_by_interval"] if item["causal_release_id"] == causal_release_id)
    context = checkpoint_context(
        binding=BracketIntervalBinding(
            phase8_index_release_id=phase8_index_release_id,
            causal_release_id=causal_release_id,
            economics_release_id=str(selected["economics_release_id"]),
            interval_key=str(selected["interval_key"]),
        ),
        source_parquet_sha256=sha256_file(causal_path), signal_contract_id=signal_contract_id,
    )
    # A crash can leave chunk files behind before their atomic checkpoint
    # append.  Include the two writer modules in the context so a corrected
    # implementation never mistakes those unrecorded bytes for resumable work.
    context = {
        **context,
        "writer_implementation_sha256": sha256_json({
            "materializer": sha256_file(Path(__file__).with_name("tier1_bracket_materializer.py")),
            "source_publisher": sha256_file(Path(__file__)),
        }),
    }
    stage = root / "state" / "tier1_bracket_staging" / signal_contract_id / sha256_json(context) / f"{market}-{year}"
    checkpoint = checkpoint_path(root=root, context=context, market=market, year=year)
    checkpoint_payload = write_streamed_bracket_chunks(
        batches=_causal_batches(causal_path, rows_per_batch=rows_per_batch),
        stress_round_trip_cost_usd=stress_round_trip_cost_usd,
        indexed_economics=indexed_bracket_economics_from_registry(registry),
        stage=stage, checkpoint=checkpoint, root=root, context=context, chunk_rows=chunk_rows,
    )
    return {"market": market, "year": year, "checkpoint": checkpoint, "stage": stage, "context": context, "checkpoint_payload": checkpoint_payload}
