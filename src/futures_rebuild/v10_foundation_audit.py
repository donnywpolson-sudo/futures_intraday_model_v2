"""Task-scoped V10 foundation audit. CLI prepares metadata only.

Historical execution is called by Codex only after the owner approves the exact
prepared read scope. The task authorization record documents that approval; it
does not issue a research capability or permit a new source replay.
"""
from __future__ import annotations

from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from importlib.metadata import version
import json
import shutil
import sys
import xml.etree.ElementTree as ET

from .active_data_certification import canonical_parquet_sequence_fingerprint
from .active_data_view import validate_catalog, PUBLICATION_LOCK
from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .causal_incremental_release import _coverage
from .causal_market_binding_compatibility import prove_bindings
from .causal_observation_foundation import (
    ECONOMICS_RULEBOOK_PATH, ECONOMICS_RULEBOOK_SHA256, V10_CHECKPOINT_MARKETS,
)
from .causal_observation_parquet import (
    FILENAMES, _parquet, _decode_physical, _decode_group,
)
from .causal_timeframe_execution import execution_lease
from .causal_timeframe_storage import Budget, Limits, contained
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.economics import EconomicsRuleBook
from .locking import FileLease

HERE = "state/unpublished_evidence/v10_foundation_audit_v1"
CATALOG = "data/causally_gated_normalized/_control/catalog.json"
COMPATIBILITY = "reports/gate_b_binding_compatibility_v1/compatibility_manifest.json"
CUTOVER = ("reports/causal_compact_successor_v1/"
           "e564263f41f2aee7cccdfe5e8b101076e77174348c2f2dac428532d51c6f0e02/approved_cutover/result.json")
MINUTE = 60_000_000_000
CUTOFF = 1_752_444_000_000_000_000
LIMITS = Limits(2 * 2**30, 6 * 2**30, 8 * 3600, maximum_files=1500)
IMPLEMENTATION = (
    "src/futures_rebuild/v10_foundation_audit.py",
    "src/futures_rebuild/annual_foundation_release.py",
    "src/futures_rebuild/annual_foundation_storage.py",
    "src/futures_rebuild/annual_foundation_integration.py",
    "src/futures_rebuild/active_data_view.py",
    "src/futures_rebuild/active_data_certification.py",
    "src/futures_rebuild/causal_observation_parquet.py",
    "src/futures_rebuild/causal_market_binding_compatibility.py",
    "src/futures_rebuild/causal_timeframe_storage.py",
    "src/futures_rebuild/causal_timeframe_execution.py",
    "src/futures_rebuild/causal_1h_native.py",
    "src/futures_rebuild/durable_windows_task_transport.py",
    "src/futures_rebuild/causal_timeframe_view.py",
    "src/futures_rebuild/causal_timeframe_publication.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/locking.py",
    "src/futures_rebuild/foundation/economics.py",
    "src/futures_rebuild/l0_alpha_source_readiness.py",
    "src/futures_rebuild/l0_alpha_candidate.py",
    "src/futures_rebuild/l0_alpha_research.py",
    "src/futures_rebuild/rolling_hour_predictive_source.py",
    "tests/test_v10_foundation_audit.py",
    "tests/test_causal_observation_parquet.py",
    "tests/test_active_data_view.py",
    "tests/test_active_data_certification.py",
    "tests/test_l0_alpha_candidate.py",
    "tests/test_l0_alpha_research.py",
    "tests/test_rolling_hour_predictive.py",
    "requirements.lock",
)


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


def identity(value, key):
    require(value.get(key) == sha256_json({k: v for k, v in value.items() if k != key}),
            f"invalid {key}")


def signed(value, key="id"):
    return {**value, key: sha256_json(value)}


def environment():
    return {"python": sys.version, "packages": {n: version(n) for n in ("pyarrow", "numpy", "tzdata")}}


def task_output_bytes(root):
    directory = contained(root, HERE)
    return sum(p.stat().st_size for p in directory.rglob("*") if p.is_file()) if directory.exists() else 0


def timestamp(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1_000_000_000



def _annual_catalog_sha(root, catalog, read):
    from .annual_foundation_release import predecessor_index
    from .causal_timeframe_storage import signed as storage_signed
    current = sha256_file(contained(root, CATALOG))
    if not any('annual_release_key' in e for e in catalog['entries']):
        return current
    index = read('data/causally_gated_normalized/_control/timeframes.json')
    identity(index, 'id')
    require(index['legacy_1m'] == {'path': CATALOG, 'sha256': current},
            'annual audit index/catalog binding differs')
    ref = index['annual_packaging']['predecessor_catalog']
    read(ref['path'], ref['sha256'])
    # Reuse the entry-by-entry proof that packaging changes no permission/content identity.
    predecessor_index(root, storage_signed({k:v for k,v in index.items() if k != 'id'}))
    return ref['sha256']


def _annual_sidecar(root, entry, read):
    side = read(entry['sidecar_path'], entry['sidecar_sha256'])
    if 'annual_release_key' not in entry:
        return side
    from .annual_foundation_release import minute_permission
    for name in ('comparison', 'supervision'):
        ref = side[name]
        read(ref['path'], ref['sha256'])
    return minute_permission(root, entry)


def prepare(root):
    """Read exact metadata, never DBN or Parquet payloads (including footers)."""
    root = Path(root).resolve()
    metadata = {}

    def read(relative, expected=None):
        path = contained(root, relative)
        raw = path.read_bytes()
        digest = sha256(raw).hexdigest()
        require(expected is None or digest == expected, f"metadata changed: {relative}")
        metadata[relative] = {"sha256": digest, "size": len(raw)}
        return json.loads(raw)

    catalog = read(CATALOG)
    validate_catalog(catalog)
    for path, digest in catalog["semantic_bindings"].items():
        read(path, digest)
    compat = read(COMPATIBILITY)
    identity(compat, "manifest_id")
    cutover = read(CUTOVER)
    require(cutover["status"] == "PUBLISHED_VERIFIED_ROLLBACK_PRESERVED"
            and cutover["active_catalog_file_sha256"] == _annual_catalog_sha(root, catalog, read),
            "catalog is not the recorded V10 compact publication")
    aliases = {}
    for path, binding in compat["preserved_artifacts"].items():
        actual = path
        if path == CATALOG:
            require(binding["sha256"] == cutover["rollback_catalog_file_sha256"],
                    "historical catalog is not the recorded predecessor")
            actual = cutover["rollback_path"] + "/catalog.json"
            aliases[path] = actual
        read(actual, binding["sha256"])
    # Replay provenance is historical; do not demand equality with later admission code.
    for digest, snapshot in compat["snapshots"].items():
        require(sha256(snapshot["source"].encode()).hexdigest() == digest,
                "historical implementation snapshot changed")
    proof = prove_bindings([v["bindings"] for v in compat["binding_groups"].values()], compat["snapshots"])
    require(proof == compat["proof"], "historical replay compatibility proof differs")
    complete_paths = [p for p in catalog["semantic_bindings"] if "/complete_set/" in p]
    require(len(complete_paths) == 1, "one selected complete-set certificate required")
    complete = read(complete_paths[0], catalog["foundation_manifest_sha256"])
    identity(complete, "certificate_id")
    require(complete["compatibility_manifest_id"] == compat["manifest_id"], "complete-set provenance differs")
    annual = [e for e in catalog["entries"] if e["disposition"] == "RESEARCH_READY_CAUSAL_PRICE"]
    require(len(annual) == 609 and {e["market"] for e in annual} == set(V10_CHECKPOINT_MARKETS),
            "41-market/609-file scope changed; prepare a reviewed scope successor")
    protected = [e for e in catalog["entries"] if e not in annual]
    require(len(protected) == 8 and all(e["parquet_path"] is None for e in protected),
            "protected catalog disposition changed")
    selected = {e["market"]: [] for e in annual}
    for e in annual:
        side = _annual_sidecar(root, e, read)
        identity(side, "sidecar_id")
        require(side["entry_binding"] == {k: v for k, v in e.items() if k != "sidecar_sha256"},
                "annual manifest does not match catalog")
        from .active_data_view import validate_content_validation_receipt, validate_access_policy_binding
        require(validate_content_validation_receipt(side["content_validation_receipt"]) == e["content_validation_receipt_id"]
                and validate_access_policy_binding(side["access_policy_binding"]) == e["access_policy_binding_id"],
                "annual certification/access identity differs")
        require(timestamp(e["coverage_start"]) < timestamp(e["coverage_end"]) <= CUTOFF,
                "annual interval exceeds development boundary")
        selected[e["market"]].extend(e["source_bindings"])
    months, sources, payloads, ids = [], {}, {}, []
    for record in compat["markets"]:
        market = record["market"]
        cert = read(record["certificate_path"])
        identity(cert, "certificate_id")
        require(cert["certificate_id"] == record["certificate_id"] and cert["replay_passes"] == 2
                and cert["status"] == "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE",
                f"market certification differs: {market}")
        ids.append(cert["certificate_id"])
        evidence = cert["replay_evidence"]
        identity(evidence, "evidence_id")
        require(evidence["evidence_id"] == cert["replay_evidence_id"] == record["replay_evidence_id"]
                and sha256_json(evidence["output_inventory"]) == evidence["output_inventory_sha256"],
                "replay output identity differs")
        output = {f["path"]: f for f in evidence["output_inventory"]}
        require(len(output) == len(evidence["output_inventory"]), "duplicate certified output")
        observations = [f for f in output.values() if f["path"].endswith("/observations.parquet")]
        require(sorted(observations, key=lambda f: f["path"]) == sorted(selected[market], key=lambda f: f["path"]),
                f"annual source bindings omit or duplicate certified observations: {market}")
        plan = read(record["certification_plan_path"])
        build = read(plan["build_plan_path"], plan["build_plan_sha256"])
        cp = read(plan["checkpoint_path"], cert["checkpoint_sha256"])
        identity(plan, "plan_id"); identity(build, "plan_id"); identity(cp, "result_id")
        require(cp["result_id"] == cert["checkpoint_result_id"] and build["plan_id"] == cert["build_plan_id"]
                and plan["plan_id"] == cert["certification_plan_id"]
                and sha256_json(cp["partitions"]) == cp["partition_inventory_sha256"], "checkpoint chain differs")
        inventory = read(build["source"]["inventory_path"], build["source"]["inventory_sha256"])
        entries = sorted((e for e in inventory["entries"] if e["market"] == market), key=lambda e: e["path"])
        require(sha256_json(entries) == build["source"]["exact_source_entries_sha256"], "exact source selection differs")
        for e in entries:
            require(e["admitted_standard_foundation"] and timestamp(e["interval_end_exclusive"]) <= CUTOFF,
                    "source crosses protected boundary")
            binding = {"path": e["path"], "sha256": e["sha256"], "size": e["size_bytes"],
                       "market": market, "family": e["family"], "kind": e["kind"],
                       "start_ns": timestamp(e["interval_start_inclusive"]),
                       "end_ns": timestamp(e["interval_end_exclusive"])}
            require(e["path"] not in sources or sources[e["path"]] == binding, "ambiguous source identity")
            sources[e["path"]] = binding
        seals = {}
        for year in sorted({p["year"] for p in cp["partitions"]}):
            path = str(Path(plan["checkpoint_path"]).parent.as_posix()) + f"/work_unit_seals/{year}.json"
            seal = read(path)
            identity(seal, "seal_id")
            require(seal["status"] == "PASS_SEALED_INACTIVE_WORK_UNIT"
                    and sha256_json(seal["files"]) == seal["files_sha256"]
                    and sha256_json(seal["partitions"]) == seal["partitions_sha256"]
                    and seal["partitions"] == [p for p in cp["partitions"] if p["year"] == year],
                    "year seal differs from checkpoint")
            for f in seal["files"]:
                require(output.get(f["path"]) == f, "year seal differs from replay inventory")
                seals[f["path"]] = f
        require(seals == output, "year seals do not cover exact replay inventory")
        for part in cp["partitions"]:
            directory = part["stage"] + "/candidate"
            files = {n: output[directory + "/" + filename] for n, filename in FILENAMES.items()}
            start, end = _coverage([files["observations"]])
            require(timestamp(start) < timestamp(end) <= CUTOFF, "monthly interval exceeds cutoff")
            months.append({"market": market, "year": part["year"], "interval": part["interval"],
                           "start_ns": timestamp(start), "end_ns": timestamp(end), "directory": directory,
                           "files": files, "source_contract_id": cert["source_contract_id"],
                           "source_release_id": cert["source_release_id"], "certificate_id": cert["certificate_id"]})
        payloads.update(output)
    require(ids == complete["market_certificate_ids"] and sha256_json(ids) == complete["market_certificate_ids_sha256"],
            "complete-set selected certificates differ")
    require(len(months) == complete["total_partition_count"] == 6878
            and sum(e["row_count"] for e in annual) == complete["total_observation_count"] == 139892740,
            "complete scope reconciliation differs")
    for e in annual:
        payloads[e["parquet_path"]] = {"path": e["parquet_path"], "sha256": e["parquet_sha256"],
                                       "size": contained(root, e["parquet_path"]).stat().st_size}
    for f in [*payloads.values(), *sources.values()]:
        require(contained(root, f["path"]).stat().st_size == f["size"], f"input missing/size differs: {f['path']}")
    read(ECONOMICS_RULEBOOK_PATH, ECONOMICS_RULEBOOK_SHA256)
    bindings = {p: sha256_file(contained(root, p)) for p in IMPLEMENTATION}
    tests = {}
    for name in ("audit_checks.xml", "reader_checks.xml", "resolver_checks.xml"):
        relative = HERE + "/" + name
        raw = contained(root, relative).read_bytes()
        suites = ET.fromstring(raw).findall("testsuite")
        counts = {k: sum(int(s.attrib[k]) for s in suites) for k in ("tests", "failures", "errors", "skipped")}
        require(counts["tests"] > 0 and counts["failures"] == counts["errors"] == 0, "generated checks failed or absent")
        metadata[relative] = {"sha256": sha256(raw).hexdigest(), "size": len(raw)}
        tests[name] = counts
    return signed({"status": "PREPARED_REQUIRES_OWNER_APPROVAL", "repository_root": str(root),
        "catalog_sha256": metadata[CATALOG]["sha256"], "metadata": metadata, "historical_aliases": aliases,
        "implementation": bindings, "environment": environment(), "limits": asdict(LIMITS), "generated_checks": tests,
        "annual": annual, "months": sorted(months, key=lambda m: (m["market"], m["start_ns"])),
        "sources": list(sources.values()), "payloads": list(payloads.values()), "protected": protected,
        "expected_rows": sum(e["row_count"] for e in annual),
        "normalized_bytes": sum(f["size"] for f in payloads.values()),
        "source_hash_only_bytes": sum(f["size"] for f in sources.values()),
        "scope": "FULL_NORMALIZED_SCAN_AND_SOURCE_HASH_REVALIDATION_NO_DBN_DECODE",
        "model_specific_training_readiness": "NOT_DECLARED"}, "plan_id")


def groups(directory, check=lambda: None):
    """Bounded V10 decoder; supports the existing valid empty cadence table."""
    with ExitStack() as stack:
        files = {n: stack.enter_context(_parquet(directory / f, n)) for n, f in FILENAMES.items()}
        count = files["observations"].metadata.num_row_groups
        require(all(files[n].metadata.num_row_groups == count for n in ("missingness", "quality", "roll"))
                and files["cadence"].metadata.num_row_groups in (0, count), "supporting row groups misaligned")
        for index in range(count):
            check()
            logical = {}
            for name, file in files.items():
                rows = []
                if file.metadata.num_row_groups:
                    meta = file.metadata.row_group(index)
                    require(meta.total_byte_size <= LIMITS.maximum_group_bytes and meta.num_rows <= 250_000,
                            "oversized row group refused before decode")
                    rows = [_decode_physical(name, r) for r in file.read_row_group(index).to_pylist()]
                logical[name] = _decode_group(name, rows, logical.get("observations", ()))
            check()
            yield logical


def inspect_group(tables, part, sources, multiplier, previous, emit):
    """Validate recorded mechanics without declaring schedule or training eligibility."""
    rows = tables["observations"]
    require(rows, "empty observation group")
    by_id = {r["row_id"]: r for r in rows}
    require(len(by_id) == len(rows), "duplicate observation identity")
    counts = Counter({"observations": len(rows)})
    for name in ("quality", "roll"):
        require([r["row_id"] for r in tables[name]] == list(by_id), f"{name} coverage/order differs")
    observed = [m for m in tables["missingness"] if m["observation_row_id"] is not None]
    require([m["observation_row_id"] for m in observed] == list(by_id), "observed missingness coverage differs")
    for row, quality, roll, missing in zip(rows, tables["quality"], tables["roll"], observed):
        start, end = row["bar_start_ns"], row["bar_end_ns"]
        require(row["market"] == part["market"] and row["source_cadence"] == "1m"
                and row["source_contract_id"] == part["source_contract_id"]
                and row["source_release_id"] == part["source_release_id"], "observation scope differs")
        require(start % MINUTE == 0 and end == start + MINUTE
                and part["start_ns"] <= start < end <= part["end_ns"]
                and row["source_timestamp_ns"] == start
                and end <= row["available_at_ns"] <= row["decision_eligible_at_ns"] < CUTOFF,
                "bar timing or eligibility violates development boundary")
        require(row["definition_received_at_ns"] <= start
                and row["listing_activation_ns"] <= start < row["expiration_ns"], "definition timing/lifecycle differs")
        for prefix, family in (("source", "ohlcv_1m"), ("definition_source", "definition")):
            item = sources.get(row[prefix + "_file_path"])
            require(item is not None and item["sha256"] == row[prefix + "_file_sha256"]
                    and item["market"] == row["market"] and item["family"] == family,
                    "observation source lineage is not selected")
            require(item["start_ns"] <= start < item["end_ns"], "source interval does not contain observation")
        require(row["actual_contract"] == row["raw_symbol"] and row["actual_contract"]
                and row["currency"] == "USD" and row["min_price_increment_nano"] > 0
                and row["multiplier_nano"] == multiplier and row["volume"] >= 0,
                "contract/economics fields invalid")
        require(row["low_nano"] <= min(row["open_nano"], row["close_nano"])
                <= max(row["open_nano"], row["close_nano"]) <= row["high_nano"], "invalid OHLC ordering")
        require(all(quality[k] is True for k in ("ohlc_valid", "volume_valid", "timestamp_order_valid"))
                and quality["duplicate_state"] == "UNIQUE", "quality evidence contradicts accepted observation")
        require(f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}" in quality["quality_flags"],
                "quality economics provenance differs")
        negative = row["low_nano"] < 0
        require(not negative or "PROVIDER_VALID_NEGATIVE_PRICE" in quality["quality_flags"], "negative-price evidence missing")
        require(missing["market"] == row["market"] and missing["interval_start_ns"] == start
                and missing["interval_end_ns"] == end and missing["state"] == "OBSERVED_VALID",
                "observed missingness interval differs")
        before = previous["actual_contract"] if previous else row["actual_contract"]
        changed = before != row["actual_contract"]
        require(roll["actual_contract_before"] == before and roll["actual_contract_after"] == row["actual_contract"]
                and roll["roll_flag"] == changed and roll["effective_time_ns"] == (start if changed else None)
                and roll["crossing_status"] == ("ROLL_BOUNDARY_UNADJUSTED" if changed else "NO_CROSSING")
                and roll["price_discontinuity_flag"] == bool(changed and previous["close_nano"] != row["open_nano"]),
                "contract transition evidence differs")
        if previous:
            require(start >= previous["bar_end_ns"], "duplicate, overlapping or unordered observations")
        left = max(previous["bar_end_ns"], part["start_ns"]) if previous else part["start_ns"]
        if left < start:
            emit({"kind": "UNCOVERED_INTERVAL", "start_ns": left, "end_ns": start, "state": "UNKNOWN",
                  "basis": "OBSERVATION_ABSENCE_NO_OFFICIAL_SCHEDULE"})
            counts["uncovered_intervals"] += 1
            counts["uncovered_ns"] += start - left
        if changed:
            emit({"kind": "CONTRACT_CHANGE", "start_ns": start, "end_ns": end, "state": "UNADJUSTED",
                  "row_id": row["row_id"]})
        counts["contract_changes"] += changed
        counts["negative_price_rows"] += negative
        counts["zero_price_rows"] += any(row[n] == 0 for n in ("open_nano", "high_nano", "low_nano", "close_nano"))
        counts["assumed_five_second_rows"] += row["available_at_ns"] == end + 5_000_000_000
        counts["unknown_schedule_rows"] += row["official_schedule_state"] == "UNKNOWN_FAIL_CLOSED"
        counts["observed_ns"] += end - start
        previous = row
    for missing in tables["missingness"]:
        if missing["observation_row_id"] is not None:
            continue
        require(missing["market"] == part["market"] and missing["interval_start_ns"] < missing["interval_end_ns"] <= CUTOFF,
                "missingness interval invalid")
        emit({"kind": "RECORDED_MISSINGNESS", "start_ns": missing["interval_start_ns"],
              "end_ns": missing["interval_end_ns"], "state": missing["state"],
              "authority": missing["authority"], "evidence_id": missing["evidence_id"]})
        counts["recorded_gap_intervals"] += 1
    for cadence in tables["cadence"]:
        row = by_id[cadence["row_id"]]
        require(cadence["source_cadence"] == "1m" and cadence["comparison_cadence"] in ("1s", "1h", "1d")
                and cadence["result"] in ("MATCH", "DISAGREEMENT", "NOT_COMPARABLE", "SOURCE_MISSING"),
                "cadence evidence invalid")
        counts["cadence_" + cadence["comparison_cadence"] + "_" + cadence["result"]] += 1
        if cadence["result"] != "MATCH":
            emit({"kind": "CADENCE_COMPARISON", "start_ns": row["bar_start_ns"], "end_ns": row["bar_end_ns"],
                  "state": cadence["result"], "comparison_cadence": cadence["comparison_cadence"],
                  "exception_state": cadence["exception_state"], "comparison_id": cadence["comparison_id"]})
    for name, table in tables.items():
        counts["table_rows_" + name] += len(table)
    return counts, previous


def compare_annual(annual, monthly, expected_rows):
    before = canonical_parquet_sequence_fingerprint(monthly)
    after = canonical_parquet_sequence_fingerprint([annual])
    require(before == after and after["row_count"] == expected_rows, "annual logical rows differ from monthly sources")
    return after


def verify_bindings(root, plan, budget):
    for relative, binding in plan["metadata"].items():
        require(budget.hash(contained(root, relative)) == binding["sha256"], f"metadata drift: {relative}")
    for relative, digest in plan["implementation"].items():
        require(budget.hash(contained(root, relative)) == digest, f"implementation drift: {relative}")
    require(environment() == plan["environment"], "audit runtime drift")


def execute(root, plan, authorization_path):
    """No execution CLI. Caller must obtain and record owner approval in this task."""
    root = Path(root).resolve()
    identity(plan, "plan_id")
    auth_path = contained(root, authorization_path)
    auth = json.loads(auth_path.read_bytes())
    if (auth.get("plan_id") != plan["plan_id"] or auth.get("scope") != plan["scope"]
            or auth.get("status") != "OWNER_APPROVED_IN_THIS_TASK" or not auth.get("owner_message")):
        raise UnauthorizedOperation("exact owner-approved task record required before historical access")
    require(str(root) == plan["repository_root"] and plan["limits"] == asdict(LIMITS), "execution scope/limits differ")
    directory = contained(root, HERE + "/" + plan["plan_id"] + "/execution")
    with execution_lease(root), FileLease(root / PUBLICATION_LOCK):
        directory.mkdir(parents=True, exist_ok=False)
        budget = Budget(directory, LIMITS)
        preparation_bytes = task_output_bytes(root)
        budget.written = preparation_bytes
        budget.terminal_reserve = 1 * 2**20
        result = {"status": "INCOMPLETE", "plan_id": plan["plan_id"],
                  "model_specific_training_readiness": "NOT_DECLARED", "months_completed": 0,
                  "annual_completed": 0, "sources_hashed": 0, "payloads_hashed": 0, "totals": {}}
        result["preparation_bytes_charged_to_output_limit"] = preparation_bytes
        try:
            verify_bindings(root, plan, budget)
            budget.save(directory / "task_authorization.json", auth)
            for item in plan["sources"] + plan["payloads"]:
                path = contained(root, item["path"])
                require(path.stat().st_size == item["size"] and budget.hash(path) == item["sha256"],
                        f"input hash differs: {item['path']}")
                result["sources_hashed" if "kind" in item else "payloads_hashed"] += 1
            sources = {s["path"]: s for s in plan["sources"]}
            rulebook = EconomicsRuleBook.from_file(root / ECONOMICS_RULEBOOK_PATH)
            totals = Counter()
            market_counts = {}
            previous = None
            previous_part = None
            import gzip
            with (budget.open(directory / "exceptions.jsonl.gz") as stream,
                  gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as ledger,
                  budget.open(directory / "months.jsonl") as monthly):
                for part in plan["months"]:
                    budget.check()
                    result["current_partition"] = part["directory"]
                    if previous_part and previous_part["market"] != part["market"]:
                        previous = None
                    if previous_part and previous_part["market"] == part["market"]:
                        require(previous_part["end_ns"] == part["start_ns"], "declared monthly coverage gap/overlap")
                    counts = Counter()
                    def emit(event):
                        ledger.write(canonical_bytes({"market": part["market"], "partition": part["directory"], **event}) + b"\n")
                    names = {p.name for p in contained(root, part["directory"]).iterdir()}
                    require(names == set(FILENAMES.values()), "unexpected monthly file membership")
                    multiplier = int(rulebook.rules[part["market"]].expected_unit_qty * 10**9)
                    for group_index, tables in enumerate(groups(contained(root, part["directory"]), budget.check)):
                        result["current_group"] = group_index
                        c, previous = inspect_group(tables, part, sources, multiplier, previous, emit)
                        counts.update(c)
                    finish_partition(previous, part, counts, emit)
                    require(counts["observed_ns"] + counts["uncovered_ns"] == part["end_ns"] - part["start_ns"],
                            "monthly interval accounting differs")
                    counts["partitions"] = 1
                    totals.update(counts)
                    market_counts.setdefault(part["market"], Counter()).update(counts)
                    monthly.write(canonical_bytes({
                        "market": part["market"], "interval": part["interval"], "certificate_id": part["certificate_id"],
                        "status": "PASS_RECORDED_MECHANICS", "counts": dict(counts)}) + b"\n")
                    monthly.flush()
                    result["months_completed"] += 1
                    result["totals"] = dict(totals)
                    previous_part = part
            for e in plan["annual"]:
                budget.check()
                result["current_annual"] = e["parquet_path"]
                fingerprint = compare_annual(contained(root, e["parquet_path"]),
                    [contained(root, f["path"]) for f in sorted(e["source_bindings"], key=lambda f: f["path"])], e["row_count"])
                budget.check()
                budget.save(directory / f"annual_{e['market']}_{e['year']}.json", {"market": e["market"], "year": e["year"],
                    "status": "PASS_EXACT_LOGICAL_EQUALITY", "fingerprint": fingerprint, "permitted_uses": e["permitted_uses"]})
                result["annual_completed"] += 1
            require(totals["observations"] == plan["expected_rows"] and result["months_completed"] == len(plan["months"])
                    and result["annual_completed"] == len(plan["annual"]), "incomplete scan accounting")
            for item in plan["payloads"] + plan["sources"]:
                require(budget.hash(contained(root, item["path"])) == item["sha256"], "input changed during audit")
            verify_bindings(root, plan, budget)
            budget.check()
            result.update(status="PASS_FOUNDATION_INTEGRITY_WITH_COVERAGE_LIMITATIONS", totals=dict(totals),
                markets={m: dict(c) for m, c in market_counts.items()},
                source_reconstruction="EXISTING_REPLAY_EVIDENCE_REVALIDATED_NO_NEW_DBN_DECODE",
                annual_equivalence="PASS", coverage="UNKNOWN_INTERVALS_RETAINED_NO_TRAINING_ADMISSION")
        except Exception as exc:
            result.update(error_type=type(exc).__name__, error=str(exc))
        finally:
            budget.finalizing = True
            budget.terminal_reserve = 0
            result["resources"] = budget.record()
            budget.save(directory / "result.json", signed(result))
        # A late terminal-write/resource failure leaves no successful supervisor receipt.
        if result["status"].startswith("PASS_"):
            budget.finalizing = False
            budget.check()
        return result


def worker():
    """Private task worker; stdin is released only after native job containment."""
    request = json.loads(sys.stdin.buffer.readline(2**20))
    root = Path.cwd()
    # native_supervise verifies process membership and installs the OS memory
    # limit before it releases this request through stdin.
    plan = json.loads(contained(root, request["plan_path"]).read_bytes())
    result = execute(root, plan, request["authorization_path"])
    return 0 if result["status"].startswith("PASS_") else 1


def supervise(root, plan_path, authorization_path):
    """Reuse native OS memory/time containment; no hourly inputs are accessed."""
    from .causal_1h_native import supervise as native_supervise
    root = Path(root).resolve()
    plan = json.loads(contained(root, plan_path).read_bytes())
    identity(plan, "plan_id")
    require(plan_path == HERE + "/" + plan["plan_id"] + "/plan.json", "noncanonical prepared plan")
    output = HERE + "/" + plan["plan_id"] + "/execution"
    record = {"plan_id": plan["plan_id"], "status": "INCOMPLETE", "model_specific_training_readiness": "NOT_DECLARED"}
    try:
        seconds = native_supervise([sys.executable, "-B", "-c",
            "from futures_rebuild.v10_foundation_audit import worker; raise SystemExit(worker())"], root,
            {"plan_path": plan_path, "authorization_path": authorization_path,
             "operation": {"id": plan["plan_id"], "output": output}}, LIMITS)
        directory = contained(root, output)
        result = json.loads((directory / "result.json").read_bytes())
        identity(result, "id")
        require(result["status"].startswith("PASS_") and result["plan_id"] == plan["plan_id"], "worker is incomplete")
        sizes = [p.stat().st_size for p in directory.iterdir() if p.is_file()]
        # Include this final receipt with a conservative explicit 64 KiB reservation.
        require(task_output_bytes(root) + 65536 <= LIMITS.maximum_written_bytes and len(sizes) + 1 <= LIMITS.maximum_files
                and seconds <= LIMITS.maximum_seconds and shutil.disk_usage(root).free >= LIMITS.minimum_free_bytes,
                "complete-run resource bound failed")
        record.update(status="PASS_SUPERVISED_FOUNDATION_AUDIT", worker_result_sha256=sha256_file(directory / "result.json"),
            seconds=seconds, output_bytes_before_supervision_receipt=sum(sizes), final_receipt_reserved_bytes=65536,
            worker_peak_process_memory_bytes=result["resources"]["peak_process_memory_bytes"],
            os_process_memory_limit_bytes=LIMITS.maximum_memory_bytes, workers_stopped=True)
    except Exception as exc:
        record.update(error_type=type(exc).__name__, error=str(exc))
    destination = contained(root, HERE + "/" + plan["plan_id"] + "/supervision.json")
    with destination.open("xb") as f:
        f.write(canonical_bytes(signed(record)) + b"\n")
    report = ("# V10 foundation audit\n\n"
              f"Status: **{record['status']}**. Model-specific training readiness: **NOT_DECLARED**.\n\n"
              "The worker result, market totals, monthly summaries, annual fingerprints and interval ledger "
              "are in `execution/`. The supervisor receipt is required to recognize a complete run.\n\n"
              "Source reconstruction uses the revalidated existing replay evidence; no DBN was decoded. "
              "Unknown coverage is not proof of exchange closure or missing vendor trades. "
              "Calendar support, measured delivery timing, feature/target scope and training admission "
              "remain model-specific requirements.\n\n"
              "Reader findings and generated checks are recorded in the task's PREPARATION.md. "
              "A completed data scan does not reopen any research experiment.\n")
    if record["status"] == "INCOMPLETE":
        report += "\n" + record.get("error", "No complete supervisor receipt.") + "\n"
    with destination.with_name("REPORT.md").open("x", encoding="utf-8") as f:
        f.write(report)
    return record


def finish_partition(previous, part, totals, emit):
    require(previous is not None, "empty monthly audit")
    left, right = previous["bar_end_ns"], part["end_ns"]
    require(left <= right, "market end exceeds declared coverage")
    if left < right:
        emit({"kind": "UNCOVERED_INTERVAL", "start_ns": left, "end_ns": right, "state": "UNKNOWN",
              "basis": "TRAILING_OBSERVATION_ABSENCE_NO_OFFICIAL_SCHEDULE"})
        totals["uncovered_intervals"] += 1
        totals["uncovered_ns"] += right - left


def main():
    root = Path.cwd()
    plan = prepare(root)
    directory = contained(root, HERE + "/" + plan["plan_id"])
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "plan.json").write_bytes(canonical_bytes(plan) + b"\n")
    print(json.dumps({"status": plan["status"], "plan": str(directory / "plan.json"),
        "annual_files": len(plan["annual"]), "monthly_partitions": len(plan["months"]), "rows": plan["expected_rows"],
        "normalized_bytes": plan["normalized_bytes"], "source_hash_only_bytes": plan["source_hash_only_bytes"],
        "free_bytes": shutil.disk_usage(root).free, "limits": plan["limits"]}))


if __name__ == "__main__":
    main()
