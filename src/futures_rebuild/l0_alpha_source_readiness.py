"""Bounded source readiness using the existing certified active-view resolver.

Metadata preparation is read-only. Row access requires a fresh exact receipt
under the existing Alpha pilot/Tier-1 readiness operation; no fitting lives here.
"""
from datetime import date
from itertools import chain, groupby
import shutil
import time

from . import l0_alpha_candidate as c
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import read_canonical_authority, sha256_file, sha256_json, write_new_canonical_authority as write
from .errors import IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION as OPERATION

CLASSIFICATION = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION


def prepare(root):
    catalog_path = root / "data/causally_gated_normalized/_control/catalog.json"
    catalog = read_canonical_authority(catalog_path)
    sources = []
    for market in c.MARKETS:
        for year in range(2018, 2023):
            found = [r for r in catalog["entries"] if r["market"] == market and r["year"] == year]
            if len(found) != 1:
                raise IntegrityError("exact four-market development source metadata required")
            row = found[0]
            expected = f"data/active/causally_gated_normalized/{market}/{year}/{year}.parquet"
            if 'annual_release_key' in row:
                from .annual_foundation_release import minute_paths
                expected, expected_sidecar = minute_paths(row)
            else:
                expected_sidecar = expected + '.manifest.json'
            if row['sidecar_path'] != expected_sidecar:
                raise IntegrityError('source sidecar path differs from its storage contract')
            if (row["parquet_path"] != expected or row["sidecar_path"] != expected_sidecar
                    or row["coverage_start"] != f"{year}-01-01" or row["coverage_end"] != f"{year + 1}-01-01"
                    or row["disposition"] != "RESEARCH_READY_CAUSAL_PRICE"
                    or "FEATURE_GENERATION" not in row["permitted_uses"] or row["selection_eligible"] is not True):
                raise IntegrityError("source metadata does not permit the declared development purpose")
            sources.append({k: row[k] for k in ("market", "year", "parquet_path", "parquet_sha256", "sidecar_path",
                "sidecar_sha256", "row_count", "content_validation_receipt_id", "access_policy_binding_id")}
                | {"physical_bytes": (root / expected).stat().st_size})
    return c.bind({"purpose": "L0_ALPHA_FIXED_CLOCK_SOURCE_READINESS_NO_FIT_OR_ECONOMIC_SCORE",
        "operation": OPERATION, "catalog_sha256": sha256_file(catalog_path), "exact_sources": sources,
        "markets": list(c.MARKETS), "years": [2018, 2019, 2020, 2021, 2022],
        "row_limit": sum(r["row_count"] for r in sources), "source_bytes": sum(r["physical_bytes"] for r in sources),
        "maximum_output_bytes": 2**30, "maximum_peak_working_set_bytes": 6 * 2**30, "hard_stop_minutes": 20,
        "output": "state/unpublished_evidence/l0_alpha_candidate_v1/approved_source_readiness",
        "provider_cost_usd": 0, "features_and_label_availability": "MECHANICS_ONLY_FOR_FOLD_SUPPORT;NO_PREDICTIONS_OR_PNL",
        "source_admission": "EXISTING_ACTIVE_VIEW_CONTENT_CERTIFICATE_AND_ACCESS_POLICY_VALIDATION",
        "source_row_id": "SHA256_OF_DECODED_LOGICAL_ROW;ACCEPTED_BYTES_UNCHANGED",
        "retention": "IMMUTABLE_RESULTS;PRESERVE_SOURCE_AND_PREDECESSOR_EVIDENCE",
        "excluded": ["L1_VALUES", "2010_2017", "2023_PLUS", "HOLDOUT", "FORWARD", "PROVIDER", "TRADING"]}, "readiness_plan_id")


def scope(bundle):
    plan = bundle["historical_readiness_proposal"]
    return {"purpose": plan["purpose"], "l0_bundle_id": bundle["bundle_id"],
            "l0_readiness_plan_id": plan["readiness_plan_id"], "catalog_sha256": plan["catalog_sha256"]}


def _verified_rows(root, sources, receipt, boundary, required_scope, check_limit, counts):
    from .l0_alpha_preparation import validate_bundle
    if type(receipt) is not OperationReceipt or type(boundary) is not RepoBoundary:
        raise UnauthorizedOperation("verified source reader requires the real receipt and repository boundary")
    boundary.assert_active_root(root)
    bundle = validate_bundle(root)
    actual = {k: v for k, v in required_scope.items() if k not in ("approval_command", "approval_plan_id", "approval_plan_sha256")}
    if actual != scope(bundle) or not sources or sources != [s for s in bundle["historical_readiness_proposal"]["exact_sources"]
            if s["market"] == sources[0]["market"]]:
        raise UnauthorizedOperation("source reader scope is not the exact approved market and bundle")
    import pyarrow.parquet as pq
    from .active_data_view import resolve
    from .causal_observation_parquet import _decode_physical
    for source in sources:
        check_limit()
        receipt.assert_consumed(boundary, operation=OPERATION, classification=CLASSIFICATION, required_scope=required_scope)
        path = resolve(repository_root=root, market=source["market"], year=source["year"], purpose="FEATURE_GENERATION")
        if (path != root / source["parquet_path"] or path.stat().st_size != source["physical_bytes"]
                or sha256_file(path) != source["parquet_sha256"]
                or sha256_file(root / source["sidecar_path"]) != source["sidecar_sha256"]):
            raise IntegrityError("trusted resolved source differs from the exact approved source")
        observed = 0
        with pq.ParquetFile(path) as parquet:
            if parquet.metadata.num_rows != source["row_count"]:
                raise IntegrityError("source count differs from its frozen catalog")
            for batch in parquet.iter_batches(batch_size=8192):
                check_limit()
                for raw in batch.to_pylist():
                    row = _decode_physical("observations", raw)
                    observed += 1
                    if observed > source["row_count"] or row["market"] != source["market"]:
                        raise IntegrityError("source row scope differs")
                    day = row["project_trade_date"]
                    if not "2018-01-01" <= day < "2023-01-01":
                        raise IntegrityError("source row crosses the selection boundary")
                    if row["source_cadence"] != "1m":
                        raise IntegrityError("source is not the declared minute family")
                    yield {**row, "row_id": sha256_json(row)}
        if observed != source["row_count"] or sha256_file(path) != source["parquet_sha256"]:
            raise IntegrityError("source changed or was truncated during the read")
        counts[source["parquet_path"]] = observed


def daily_inputs(rows):
    """Retain every weekday, with only the fixed history/endpoint time scope."""
    groups = iter(groupby(rows, key=lambda r: r["project_trade_date"]))
    current = next(groups, None)
    for day in c.weekday_dates():
        while current is not None and current[0] < day:
            if date.fromisoformat(current[0]).weekday() < 5:
                raise IntegrityError("source day ordering regressed")
            list(current[1])
            current = next(groups, None)
        selected = []
        if current is not None and current[0] == day:
            first = c.decision_times(day)[0] - c.DELAY - 61 * c.MINUTE
            last = c.decision_times(day)[-1] - c.DELAY + 61 * c.MINUTE
            selected = [r for r in current[1] if first <= r["bar_start_ns"] <= last]
            current = next(groups, None)
        yield day, selected, []
    while current is not None:
        if date.fromisoformat(current[0]).weekday() < 5:
            raise IntegrityError("extra or reordered source weekday")
        list(current[1])
        current = next(groups, None)


def execute_once(gateway, receipt):
    from .certified_research_gateway import CertifiedResearchGateway
    from .l0_alpha_preparation import AS_OF_NS, validate_bundle, ResearchStore
    from .rolling_hour_price_census import _working_set
    if type(gateway) is not CertifiedResearchGateway or type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation("existing certified gateway and a fresh readiness receipt required")
    bundle = validate_bundle(gateway.root)
    plan = bundle["historical_readiness_proposal"]
    expected = scope(bundle)
    actual = {k: v for k, v in dict(receipt.scope).items() if k not in ("approval_command", "approval_plan_id", "approval_plan_sha256")}
    if actual != expected:
        raise UnauthorizedOperation("readiness receipt is not bound to this successor and source scope")
    required = dict(receipt.scope)
    receipt.verify(gateway.boundary, operation=OPERATION, classification=CLASSIFICATION, required_scope=required)
    if prepare(gateway.root) != plan:
        raise IntegrityError("source metadata changed before the approved read")
    output = gateway.root / plan["output"] / bundle["bundle_id"]
    with FileLease(output / "writer.lock"):
        if (output / "started.json").exists():
            raise UnauthorizedOperation("source-readiness attempt is consumed; no automatic retry")
        if shutil.disk_usage(output).free < plan["maximum_output_bytes"] + 256 * 2**20:
            raise IntegrityError("source-readiness output reserve unavailable")
        receipt.consume(gateway.boundary, operation=OPERATION, classification=CLASSIFICATION, required_scope=required)
        store = ResearchStore(output, expected, maximum_bytes=plan["maximum_output_bytes"])
        store.save("started", {"receipt_id": receipt.receipt_id, "approved_scope": plan})
        started, counts, reports = time.monotonic(), {}, {}
        def check_limit():
            if time.monotonic() - started > plan["hard_stop_minutes"] * 60 or _working_set() > plan["maximum_peak_working_set_bytes"]:
                raise IntegrityError("source-readiness time or memory bound exceeded")
        try:
            for market in c.MARKETS:
                rows = iter(_verified_rows(gateway.root, [s for s in plan["exact_sources"] if s["market"] == market],
                    receipt, gateway.boundary, required, check_limit, counts))
                first = next(rows, None)
                if first is None:
                    raise IntegrityError("certified market source contains no rows")
                dataset = c.assemble_dataset(market, daily_inputs(chain([first], rows)), as_of_ns=AS_OF_NS,
                    source_contract_id=first["source_contract_id"], source_release_id=first["source_release_id"])
                reports[market] = {"evidence_id": dataset._evidence_id, **c.source_completeness(dataset),
                    "trusted_source_release": "VERIFIED_BY_EXISTING_ACTIVE_VIEW_RESOLVER",
                    "causal_status_restrictions": "NOT_AVAILABLE_IN_2018_2022_ACTIVE_PRICE_VIEW;NOT_INFERRED",
                    "availability": "DECLARED_FIVE_SECOND_ASSUMPTION;NOT_HISTORICAL_RECEIPT_CERTIFICATION",
                    "mapping": "CONDITIONAL_ON_SUPPLIED_HISTORICAL_CONTRACT_SEQUENCE",
                    "strict_certificate_changed": False, "registration_ready": False}
                store.save(market, reports[market])
                del dataset
                check_limit()
                print(f"{market}: complete fixed-clock source-readiness accounting", flush=True)
            if sum(counts.values()) != plan["row_limit"]:
                raise IntegrityError("complete source-readiness row count differs")
            if sha256_file(gateway.root / "data/causally_gated_normalized/_control/catalog.json") != plan["catalog_sha256"]:
                raise IntegrityError("source catalog changed during the read")
            report = {"bundle_id": bundle["bundle_id"], "markets": reports, "source_rows": counts,
                "state": "COMPLETE_CONDITIONAL_SOURCE_READINESS_NOT_REGISTRATION_OR_ALPHA",
                "models_fitted": False, "predictions_generated": False, "economic_scores_computed": False,
                "peak_working_set_bytes": _working_set(), "elapsed_seconds": time.monotonic() - started,
                "confirmation": c.design()["confirmation"]}
            store.save("report", report)
            return report
        except Exception as exc:
            store.save("failure", {"state": "CONSUMED_INCOMPLETE_NO_RETRY", "error": str(exc),
                                   "completed_markets": list(reports), "source_rows": counts})
            raise
