"""Canonical checkpoint selection for the Tier 1 bracket successor.

The finalizer intentionally starts by selecting and hash-verifying exactly one
completed checkpoint per approved market-year.  Promotion/model fitting is not
allowed to discover chunks from the filesystem directly.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_bracket_checkpoint import load_checkpoint
from .boundary import RepoBoundary, OperationReceipt, OperationClassification
from .data_layout import DataReleaseManifest, PhasePublisher, DataReleaseReceipt

MARKETS = ("ES", "CL", "ZN", "6E")
YEARS = tuple(range(2018, 2023))


@dataclass(frozen=True)
class CanonicalBracketCheckpoint:
    market: str
    year: int
    path: Path
    context: Mapping[str, str]
    input_rows: int
    output_rows: int
    chunks: tuple[Mapping[str, object], ...]


def load_canonical_bracket_checkpoints(*, root: Path) -> tuple[CanonicalBracketCheckpoint, ...]:
    """Return exactly one complete, hash-verified checkpoint per Tier 1 year."""
    expected = {f"{market}-{year}" for market in MARKETS for year in YEARS}
    candidates: dict[str, list[CanonicalBracketCheckpoint]] = {name: [] for name in expected}
    for path in (root / "state" / "tier1_bracket_checkpoints").glob("*/*.json"):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            context = raw["context"]
            payload = load_checkpoint(path=path, context=context, root=root)
        except (OSError, ValueError, KeyError, IntegrityError) as exc:
            raise IntegrityError("bracket checkpoint selection is unreadable") from exc
        causal = context.get("causal_release_id") if isinstance(context, dict) else None
        if not isinstance(causal, str) or not payload["complete"] or payload["input_rows"] != payload["output_rows"]:
            continue
        name = path.name.removesuffix(".json")
        if name not in candidates:
            continue
        candidates[name].append(CanonicalBracketCheckpoint(
            market=name.split("-")[0], year=int(name.split("-")[1]), path=path,
            context=dict(context), input_rows=payload["input_rows"], output_rows=payload["output_rows"], chunks=tuple(payload["chunks"]),
        ))
    result=[]
    for name in sorted(expected):
        rows=candidates[name]
        if not rows:
            raise IntegrityError(f"bracket checkpoint is missing for {name}")
        # Prefer the implementation-bound successor; historical complete
        # checkpoints remain readable but cannot silently win selection.
        preferred=[item for item in rows if "writer_implementation_sha256" in item.context]
        selected=(preferred or rows)
        if len(selected) != 1:
            raise IntegrityError(f"bracket checkpoint selection is ambiguous for {name}")
        result.append(selected[0])
    if len(result) != 20 or sum(item.output_rows for item in result) <= 0:
        raise IntegrityError("bracket checkpoint selection has incomplete coverage")
    return tuple(result)


def checkpoint_selection_payload(*, root: Path) -> dict[str, object]:
    selected=load_canonical_bracket_checkpoints(root=root)
    core={"schema_version":"tier1_bracket_checkpoint_selection/1.0.0","entries":[{
        "market":item.market,"year":item.year,"checkpoint":item.path.relative_to(root).as_posix(),
        "checkpoint_sha256":sha256_file(item.path),"context":dict(item.context),"input_rows":item.input_rows,"output_rows":item.output_rows,
        "chunk_count":len(item.chunks)} for item in selected]}
    return {**core,"selection_id":sha256_json(core)}


def stage_canonical_bracket_artifacts(*, root: Path, stage: Path) -> dict[str, object]:
    """Copy only selected checkpoint chunk pairs into one verified local stage.

    Chunk paths and hashes stay explicit: later split/model code cannot include
    an orphaned or duplicate checkpoint merely because it is on disk.
    """
    import shutil
    selected=load_canonical_bracket_checkpoints(root=root)
    if stage.exists():
        raise IntegrityError("bracket canonical stage already exists")
    entries=[]
    try:
        for item in selected:
            for chunk in item.chunks:
                for kind in ("feature", "outcome"):
                    source=root / str(chunk[f"{kind}_payload"])
                    digest=str(chunk[f"{kind}_payload_sha256"])
                    if not source.is_file() or sha256_file(source)!=digest:
                        raise IntegrityError("selected bracket chunk changed before finalization")
                    target=stage / kind / item.market / str(item.year) / source.name
                    target.parent.mkdir(parents=True,exist_ok=True)
                    shutil.copyfile(source,target)
                    if sha256_file(target)!=digest:
                        raise IntegrityError("bracket canonical stage copy differs from checkpoint chunk")
                entries.append({"market":item.market,"year":item.year,"sequence":chunk["sequence"],
                    "feature":str((Path("feature")/item.market/str(item.year)/Path(str(chunk["feature_payload"])).name).as_posix()),
                    "feature_sha256":chunk["feature_payload_sha256"],"outcome":str((Path("outcome")/item.market/str(item.year)/Path(str(chunk["outcome_payload"])).name).as_posix()),
                    "outcome_sha256":chunk["outcome_payload_sha256"]})
    except Exception:
        raise
    selection=checkpoint_selection_payload(root=root)
    manifest={"schema_version":"tier1_bracket_canonical_stage/1.0.0","checkpoint_selection":selection,"chunk_pairs":entries}
    path=stage/"canonical_bracket_stage.json"; path.write_text(json.dumps(manifest,sort_keys=True,separators=(",",":"))+"\n",encoding="utf-8")
    return {"stage":stage,"selection_id":selection["selection_id"],"chunk_pair_count":len(entries),"stage_manifest":path}


def build_bracket_chronological_split_plan(*, stage: Path) -> dict[str, object]:
    """Freeze chronological session folds from mature bracket rows only."""
    import pyarrow.parquet as pq
    manifest=json.loads((stage/"canonical_bracket_stage.json").read_text(encoding="utf-8"))
    sessions=set()
    for item in manifest["chunk_pairs"]:
        table=pq.read_table(stage/item["feature"],columns=["status","exchange_session_date"])
        for row in table.to_pylist():
            if row["status"]=="FEATURE_READY" and isinstance(row["exchange_session_date"],str): sessions.add(row["exchange_session_date"])
    ordered=tuple(sorted(sessions))
    if len(ordered)<700: raise IntegrityError("bracket history cannot support chronological splits")
    # Same locked session geometry as the Tier 1 plan, but derived solely from
    # the bracket data and never from old five-minute releases.
    first=504+1+4*42+1; folds=[]
    for number in range(8):
        start=first+number*63; stop=start+63
        if stop>len(ordered): raise IntegrityError("bracket history is too short for eight folds")
        folds.append({"outer_fold":number,"fit_session_dates":[ordered[0],ordered[start-2]],"test_session_dates":[ordered[start],ordered[stop-1]],"embargo_sessions":1})
    core={"schema_version":"tier1_bracket_chronological_splits/1.0.0","stage_manifest_sha256":sha256_file(stage/"canonical_bracket_stage.json"),"session_dates":list(ordered),"outer_folds":folds,"training_only_threshold":0.60,"holdout_excluded":"2025"}
    return {**core,"split_plan_id":sha256_json(core)}


def write_frozen_bracket_predictions(*, stage: Path, output: Path) -> dict[str, object]:
    """Fit two fixed Ridge models per chronological fold and freeze test rows."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    plan=build_bracket_chronological_split_plan(stage=stage); manifest=json.loads((stage/"canonical_bracket_stage.json").read_text())
    if output.exists(): raise IntegrityError("bracket frozen-prediction target already exists")
    stats=[(np.zeros((5,5)),np.zeros(5),np.zeros(5)) for _ in plan["outer_folds"]]
    for item in manifest["chunk_pairs"]:
        f=pq.read_table(stage/item["feature"]); o=pq.read_table(stage/item["outcome"])
        if f.num_rows!=o.num_rows or f.column("upstream_source_row_sha256").to_pylist()!=o.column("upstream_source_row_sha256").to_pylist(): raise IntegrityError("bracket feature/outcome chunk pairing differs")
        rows=zip(f.to_pylist(),o.to_pylist())
        for fr,orow in rows:
            if fr["status"]!="FEATURE_READY" or orow["status"]!="MATURED": continue
            x=np.array([1.,fr["bar_body_fraction"],fr["bar_return"],fr["intrabar_range_fraction"],fr["volume"]]); d=fr["exchange_session_date"]
            if not np.isfinite(x).all(): raise IntegrityError("bracket feature is non-finite")
            for n,fold in enumerate(plan["outer_folds"]):
                if fold["fit_session_dates"][0]<=d<=fold["fit_session_dates"][1]:
                    stats[n]=(stats[n][0]+np.outer(x,x),stats[n][1]+x*float(orow["long_realized_net_r"]),stats[n][2]+x*float(orow["short_realized_net_r"]))
    models=[]
    for a,b,c in stats:
        penalty=np.eye(5); penalty[0,0]=0
        try: models.append((np.linalg.solve(a+penalty,b),np.linalg.solve(a+penalty,c)))
        except np.linalg.LinAlgError as exc: raise IntegrityError("bracket Ridge system is singular") from exc
    training_scores=[[] for _ in plan["outer_folds"]]
    for item in manifest["chunk_pairs"]:
        f=pq.read_table(stage/item["feature"])
        for fr in f.to_pylist():
            if fr["status"]!="FEATURE_READY": continue
            x=np.array([1.,fr["bar_body_fraction"],fr["bar_return"],fr["intrabar_range_fraction"],fr["volume"]]); d=fr["exchange_session_date"]
            for n,fold in enumerate(plan["outer_folds"]):
                if fold["fit_session_dates"][0]<=d<=fold["fit_session_dates"][1]:
                    long=float(x@models[n][0]); short=float(x@models[n][1]); training_scores[n].append(abs((long-short)/(abs(long)+abs(short)+1e-12)))
    thresholds=[float(np.quantile(values,0.60,method="nearest")) if values else (_ for _ in ()).throw(IntegrityError("bracket fold has no training scores")) for values in training_scores]
    output.parent.mkdir(parents=True); writer=None; count=0
    try:
        for item in manifest["chunk_pairs"]:
            f=pq.read_table(stage/item["feature"]); rows=[]
            for fr in f.to_pylist():
                if fr["status"]!="FEATURE_READY": continue
                x=np.array([1.,fr["bar_body_fraction"],fr["bar_return"],fr["intrabar_range_fraction"],fr["volume"]]); d=fr["exchange_session_date"]
                for n,fold in enumerate(plan["outer_folds"]):
                    if fold["test_session_dates"][0]<=d<=fold["test_session_dates"][1]:
                        long=float(x@models[n][0]); short=float(x@models[n][1]); score=(long-short)/(abs(long)+abs(short)+1e-12)
                        threshold=thresholds[n]; direction="long" if score>=threshold else "short" if score<=-threshold else "neutral"
                        rows.append({"market":item["market"],"year":item["year"],"outer_fold":n,"exchange_session_date":d,"actual_identity_hash":fr["actual_identity_hash"],"decision_at_ns":fr["decision_at_ns"],"upstream_source_row_sha256":fr["upstream_source_row_sha256"],"long_prediction_net_r":long,"short_prediction_net_r":short,"bounded_signal":score,"neutral_threshold":threshold,"selected_direction":direction}); count+=1
            if rows:
                table=pa.Table.from_pylist(rows)
                if writer is None: writer=pq.ParquetWriter(output,table.schema,compression="zstd")
                writer.write_table(table)
    finally:
        if writer is not None: writer.close()
    if count==0: raise IntegrityError("bracket frozen predictions contain no test rows")
    return {"split_plan":plan,"prediction_rows":count,"payload":output,"payload_sha256":sha256_file(output)}


def _require_sha256(value: object, *, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise IntegrityError(f"{name} must be a SHA-256 identifier")
    return value


def prediction_market_year_coverage(*, staged_payload: Path) -> dict[str, object]:
    """Describe which approved source years have genuine out-of-sample rows.

    A source year with no test rows is training provenance, not an empty
    prediction release.  Keeping that distinction explicit prevents a caller
    from accidentally publishing in-sample scores as frozen predictions.
    """
    import pyarrow.parquet as pq

    if not staged_payload.is_file():
        raise IntegrityError("bracket frozen-prediction payload is missing")
    expected = {(market, year) for market in MARKETS for year in YEARS}
    observed: list[dict[str, object]] = []
    total = 0
    for market, year in sorted(expected):
        table = pq.read_table(staged_payload, filters=[("market", "=", market), ("year", "=", year)])
        if table.num_rows <= 0:
            continue
        dates = table.column("exchange_session_date").to_pylist()
        if any(not isinstance(value, str) or not value.startswith(str(year)) for value in dates):
            raise IntegrityError("bracket prediction partition has an out-of-scope session date")
        directions = table.column("selected_direction").to_pylist()
        if any(value not in {"long", "short", "neutral"} for value in directions):
            raise IntegrityError("bracket prediction direction is invalid")
        thresholds = table.column("neutral_threshold").to_pylist()
        if any(not isinstance(value, (float, int)) or value < 0 for value in thresholds):
            raise IntegrityError("bracket prediction threshold is invalid")
        observed.append({"market": market, "year": year, "prediction_rows": table.num_rows})
        total += table.num_rows
    if total != pq.ParquetFile(staged_payload).metadata.num_rows:
        raise IntegrityError("bracket frozen predictions contain an unexpected market/year partition")
    return {
        "schema_version": "tier1_bracket_prediction_coverage/1.0.0",
        "out_of_sample_market_years": observed,
        "training_only_market_years": [
            {"market": market, "year": year}
            for market, year in sorted(expected - {(str(item["market"]), int(item["year"])) for item in observed})
        ],
        "total_prediction_rows": total,
    }


def _partitioned_prediction_tables(
    *, staged_payload: Path, expected_market_years: frozenset[tuple[str, int]],
) -> dict[tuple[str, int], object]:
    """Load only an explicitly approved set of out-of-sample partitions."""
    import pyarrow.parquet as pq

    coverage = prediction_market_year_coverage(staged_payload=staged_payload)
    observed = {
        (str(item["market"]), int(item["year"]))
        for item in coverage["out_of_sample_market_years"]  # type: ignore[index]
    }
    if expected_market_years != observed:
        missing = sorted(expected_market_years - observed)
        extra = sorted(observed - expected_market_years)
        raise IntegrityError(
            "approved out-of-sample scope differs from staged predictions: "
            f"missing={missing}; extra={extra}"
        )
    return {
        key: pq.read_table(staged_payload, filters=[("market", "=", key[0]), ("year", "=", key[1])])
        for key in sorted(expected_market_years)
    }


def publish_partitioned_frozen_bracket_predictions(
    *, root: Path, staged_payload: Path, trial_id: str, phase8_index_release_id: str,
    audit_receipt_id: str, expected_market_years: frozenset[tuple[str, int]],
) -> tuple[DataReleaseReceipt, ...]:
    """Publish only the explicitly approved, out-of-sample prediction releases."""
    import pyarrow.parquet as pq

    _require_sha256(trial_id, name="trial ID")
    _require_sha256(phase8_index_release_id, name="Phase 8 index release ID")
    _require_sha256(audit_receipt_id, name="audit receipt ID")
    selected = {(item.market, item.year): item for item in load_canonical_bracket_checkpoints(root=root)}
    if not expected_market_years or not expected_market_years <= set(selected):
        raise IntegrityError("approved bracket prediction scope is invalid")
    tables = _partitioned_prediction_tables(staged_payload=staged_payload, expected_market_years=expected_market_years)
    boundary = RepoBoundary(root)
    operation = OperationReceipt.issue_local(
        boundary, operation="PUBLISH_RELEASE", classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"operation": "tier1_bracket_frozen_predictions", "scope": "ES_CL_ZN_6E_2018_2022_only"},
    )
    publisher = PhasePublisher(
        boundary=boundary, operation_receipt=operation,
        lock_path=root / "state/locks/tier1-bracket-prediction-publication.lock",
    )
    receipts: list[DataReleaseReceipt] = []
    for (market, year), table in sorted(tables.items()):
        checkpoint = selected[(market, year)]
        causal_id = _require_sha256(checkpoint.context.get("causal_release_id"), name="causal release ID")
        economics_id = _require_sha256(checkpoint.context.get("economics_release_id"), name="economics release ID")
        if checkpoint.context.get("phase8_index_release_id") != phase8_index_release_id:
            raise IntegrityError("checkpoint is not bound to the requested Phase 8 index")
        date_values = table.column("exchange_session_date").to_pylist()
        first_date = min(date_values)
        stage = publisher.create_stage("tier1_bracket_prediction")
        payload = stage / "prediction.parquet"
        pq.write_table(table, payload, compression="zstd")
        staged_size = payload.stat().st_size
        logical = f"data/predictions/tier1_bracket/{market}/{year}/{first_date}/frozen_predictions.parquet"
        metadata = {
            "audit_receipt_id": audit_receipt_id,
            "causal_release_id": causal_id,
            "checkpoint_sha256": sha256_file(checkpoint.path),
            "economics_release_id": economics_id,
            "market": market,
            "phase8_index_release_id": phase8_index_release_id,
            "prediction_rows": table.num_rows,
            "source_staged_payload_sha256": sha256_file(staged_payload),
            "trial_id": trial_id,
            "year": year,
        }
        manifest = DataReleaseManifest.build(
            stage, phase="predictions", release_kind="tier1_bracket_frozen_predictions",
            schema_version="1.0.0", logical_paths={"prediction.parquet": logical},
            source_release_ids=tuple(sorted({causal_id, economics_id, phase8_index_release_id, audit_receipt_id})),
            metadata=metadata,
        )
        receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
        verified = receipt.verify(boundary)
        if verified.metadata != metadata or receipt.resolve_file(logical, boundary).stat().st_size != staged_size:
            raise IntegrityError("published bracket prediction receipt differs from staged partition")
        receipts.append(receipt)
    if len(receipts) != len(expected_market_years):
        raise IntegrityError("bracket prediction publication did not create the approved partitions")
    return tuple(receipts)


def publish_tier1_bracket_prediction_index(
    *, root: Path, receipts: tuple[DataReleaseReceipt, ...], trial_id: str,
    phase8_index_release_id: str, audit_receipt_id: str, split_plan: Mapping[str, object],
) -> DataReleaseReceipt:
    """Publish the one aggregate receipt that makes the 20 releases discoverable."""
    _require_sha256(trial_id, name="trial ID")
    _require_sha256(phase8_index_release_id, name="Phase 8 index release ID")
    _require_sha256(audit_receipt_id, name="audit receipt ID")
    if not receipts:
        raise IntegrityError("bracket prediction index requires at least one release")
    boundary = RepoBoundary(root)
    entries: list[dict[str, object]] = []
    observed: set[tuple[str, int]] = set()
    for receipt in receipts:
        manifest = receipt.verify(boundary)
        if receipt.phase != "predictions" or receipt.release_kind != "tier1_bracket_frozen_predictions":
            raise IntegrityError("bracket prediction index contains the wrong release kind")
        meta = manifest.metadata
        market, year = meta.get("market"), meta.get("year")
        if not isinstance(market, str) or type(year) is not int or (market, year) in observed:
            raise IntegrityError("bracket prediction index coverage is ambiguous")
        if meta.get("trial_id") != trial_id or meta.get("phase8_index_release_id") != phase8_index_release_id or meta.get("audit_receipt_id") != audit_receipt_id:
            raise IntegrityError("bracket prediction provenance differs from the requested index")
        observed.add((market, year))
        entries.append({"market": market, "year": year, "prediction_release_id": receipt.release_id, "prediction_receipt_id": receipt.receipt_id, "prediction_rows": meta["prediction_rows"]})
    expected = {(market, year) for market in MARKETS for year in (2020, 2021, 2022)}
    if observed != expected:
        raise IntegrityError("bracket prediction index does not cover the locked out-of-sample scope")
    payload = {
        "audit_receipt_id": audit_receipt_id,
        "phase8_index_release_id": phase8_index_release_id,
        "prediction_releases": sorted(entries, key=lambda item: (str(item["market"]), int(item["year"]))),
        "schema_version": "tier1_bracket_frozen_prediction_index/1.0.0",
        "split_plan_id": split_plan.get("split_plan_id"),
        "trial_id": trial_id,
    }
    if not isinstance(payload["split_plan_id"], str):
        raise IntegrityError("bracket split plan is invalid")
    operation = OperationReceipt.issue_local(
        boundary, operation="PUBLISH_RELEASE", classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"operation": "tier1_bracket_frozen_prediction_index", "scope": "ES_CL_ZN_6E_2018_2022_only"},
    )
    publisher = PhasePublisher(boundary=boundary, operation_receipt=operation, lock_path=root / "state/locks/tier1-bracket-prediction-index-publication.lock")
    stage = publisher.create_stage("tier1_bracket_prediction_index")
    filename = "tier1_bracket_frozen_prediction_index.json"
    (stage / filename).write_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
    manifest = DataReleaseManifest.build(
        stage, phase="reference", release_kind="tier1_bracket_frozen_prediction_index", schema_version="1.0.0",
        logical_paths={filename: "data/reference/economics/tier1_bracket_frozen_prediction_index.json"},
        source_release_ids=tuple(sorted({phase8_index_release_id, audit_receipt_id, *[receipt.release_id for receipt in receipts]})),
        metadata={"market_year_count": len(receipts), "prediction_release_count": len(receipts), "trial_id": trial_id},
    )
    receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    receipt.verify(boundary)
    return receipt
