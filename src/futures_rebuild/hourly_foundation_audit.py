"""Task-owned hourly foundation audit; CLI prepares metadata, never reads rows.

Historical scanning requires the completed campaign and separate task approval.
No source decode, publication, permission change, or training is provided.
"""
from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import gzip
import json
import shutil
import sys
from uuid import uuid4

from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_timeframe_storage import (
    Budget, Limits, contained, signed, read_record, iter_bundle_groups, memory_peak,
)
from . import causal_timeframe_publication as publication
from .causal_timeframe_execution import execution_lease
from .hourly_quality_reader import HourlyReader, NATIVE_ALLOWED, validate_window, IDENTITY
from .causal_observation_parquet import FILENAMES
from .causal_observation_foundation import V10_CHECKPOINT_MARKETS, ECONOMICS_RULEBOOK_PATH, ECONOMICS_RULEBOOK_SHA256
from .foundation.economics import EconomicsRuleBook
from .errors import IntegrityError, UnauthorizedOperation

HERE = "state/unpublished_evidence/hourly_foundation_audit_v1"
CAMPAIGN = "state/unpublished_evidence/causal_1h_campaign_v1/status.json"
HOUR = 3_600_000_000_000
DAY = 24 * HOUR
LAG = 5_000_000_000
CUTOFF = publication.CUTOFF
LIMITS = Limits(2 * 2**30, 6 * 2**30, 8 * 3600, maximum_files=100)
# Separate lifetime peaks are a conservative upper bound on simultaneous memory.
COORDINATOR_BYTES = 2**30
WORKER_LIMITS = replace(LIMITS, maximum_memory_bytes=LIMITS.maximum_memory_bytes - COORDINATOR_BYTES)
STATES = {"MATCH": "NONE", "NOT_COMPARABLE": "INCOMPLETE_INTERVAL_NO_OVERWRITE",
          "SOURCE_MISSING": "REFERENCE_SOURCE_MISSING_NO_OVERWRITE", "DISAGREEMENT": "PRESERVE_BOTH_NO_OVERWRITE"}
MODEL_REQUIREMENTS = (
    "Selected model declaration and actual reader", "Markets and permitted dates",
    "Decision times and required session/calendar evidence", "Features, lookbacks and required source families",
    "Targets, horizons and maturity", "Gap, roll, units and zero/negative-price rules",
    "Comparison/exclusion policy and minimum usable coverage",
    "Chronological splits, training-only transforms, purge and embargo", "Separate research admission",
)


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


def source_time(value):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return int(stamp.timestamp()) * 10**9


def validate_empty(entry, release, seal):
    """An unavailable index entry needs its selected candidate's retained empty seal."""
    candidate = release["candidate"]
    publication.identity(seal)
    require(entry["disposition"] == "UNAVAILABLE" and entry["reason"] == "CERTIFIED_SOURCE_UNAVAILABLE"
            and entry["spec_id"] == candidate["spec_id"] and entry["market"] == candidate["spec"]["market"]
            and seal["id"] == entry["seal_id"] == candidate["seals"][candidate["months"].index(entry["month"])]
            and seal["spec_id"] == candidate["spec_id"] and seal["month"] == entry["month"]
            and seal["timeframe"] == "1h" and not seal["files"] and not seal["table_metadata"]
            and seal["coverage"][0]["start_ns"] == entry["coverage_start_ns"]
            and seal["coverage"][-1]["end_ns"] == entry["coverage_end_ns"]
            and all(d["observations"] == 0 and d["state"] == "SOURCE_UNAVAILABLE"
                    and not any(d["comparison_counts"].values()) for d in seal["coverage"]),
            "certified empty-month evidence differs")


def prepare(root, *, admission_manifest=None):
    """Snapshot declared metadata and file sizes only; no Parquet or DBN opens."""
    from .causal_1h import implementation_binding, ENVIRONMENT
    root = Path(root).resolve()
    bindings, sources, parts, blockers, selected_scopes = {}, {}, [], [], {}

    def bind(relative, expected=None, parse=True):
        path = contained(root, relative)
        raw = path.read_bytes()
        digest = sha256(raw).hexdigest()
        require(expected is None or digest == expected, f"metadata changed: {relative}")
        bindings[relative] = {"sha256": digest, "size": len(raw)}
        return json.loads(raw) if parse else None

    index = bind(publication.INDEX)
    publication.validate_index(root, index)
    bind("data/causally_gated_normalized/_control/catalog.json", index["legacy_1m"]["sha256"])
    if "publication" in index:
        p = index["publication"]
        bind(p["plan_path"], p["plan_sha256"])
        bind(f"state/authorization_uses/{p['receipt']['receipt_id']}.json")
    campaign = bind(CAMPAIGN)
    expected = campaign["markets"]
    require(set(expected) == set(V10_CHECKPOINT_MARKETS), "campaign must declare all 41 markets")
    provenance_cache = {}
    for entry in index["entries"]:
        if entry["timeframe"] != "1h":
            continue
        if entry["disposition"] not in ("CERTIFIED", "UNAVAILABLE"):
            blockers.append(f"{entry['market']} {entry['month']}: {entry['disposition']}")
            continue
        if entry["disposition"] == "UNAVAILABLE":
            siblings = [e for e in index["entries"] if e["disposition"] == "CERTIFIED" and e["spec_id"] == entry["spec_id"]]
            require(siblings, "empty month lacks a published candidate evidence anchor")
            entry = {**entry, "evidence_entry": siblings[0], "manifest": siblings[0]["manifest"],
                     "manifest_sha256": siblings[0]["manifest_sha256"]}
        release = bind(entry["manifest"], entry["manifest_sha256"])
        for obj in (release, release["candidate"], release["certificate"], release["seal"]):
            publication.identity(obj)
        candidate, seal = release["candidate"], release["seal"]
        spec = candidate["spec"]
        require(spec["market"] not in selected_scopes or selected_scopes[spec["market"]] == spec,
                "conflicting selected market scopes")
        selected_scopes[spec["market"]] = spec
        require(not spec["synthetic_only"] and spec["timeframe"] == "1h"
                and release["permitted_uses"] == ["LINEAGE_AUDIT"], "historical hourly audit scope differs")
        provenance = release.get("provenance") or entry.get("provenance")
        require(provenance is not None, "retained operation provenance missing")
        if set(provenance) == {"path", "sha256"}:
            if provenance["path"] not in provenance_cache:
                provenance_cache[provenance["path"]] = bind(provenance["path"], provenance["sha256"])
            provenance = provenance_cache[provenance["path"]]
        if entry["disposition"] == "UNAVAILABLE":
            seal_path = provenance["operation"]["output"] + "/" + entry["month"] + ".seal.json"
            seal = bind(seal_path)
            validate_empty(entry, release, seal)
            entry = {**entry, "seal_path": seal_path}
        if spec["market"] not in {p["market"] for p in parts}:
            publication.verify_provenance(root, candidate, release["certificate"], provenance)
            bind(provenance["aggregate_memory"]["path"], provenance["aggregate_memory"]["sha256"])
            bind(f"state/authorization_uses/{provenance['receipt']['receipt_id']}.json")
            for source in provenance["operation"]["entries"]:
                require(source["kind"] in ("DBN", "SIDECAR") and source["market"] == spec["market"],
                        "source metadata scope differs")
                item = {**source, "start_ns": source_time(source["interval_start_inclusive"]),
                        "end_ns": source_time(source["interval_end_exclusive"])}
                require(0 < item["start_ns"] < item["end_ns"] <= CUTOFF, "protected source refused")
                path = contained(root, source["path"])
                require(path.stat().st_size == source["size_bytes"], "source size changed")
                require(source["path"] not in sources or sources[source["path"]] == item, "source identity conflict")
                sources[source["path"]] = item
                if source["kind"] == "SIDECAR":
                    bind(source["path"], source["sha256"])
        require(set(seal["files"]) == (set(FILENAMES) if entry["disposition"] == "CERTIFIED" else set()),
                "five retained hourly tables required for materialized months")
        for name, info in seal["files"].items():
            require(info["filename"] == FILENAMES[name], "noncanonical table name")
            require(contained(root, entry["directory"] + "/" + info["filename"]).stat().st_size == info["size"],
                    "published payload size changed")
        parts.append({**entry, "rows": sum(d["observations"] for d in seal["coverage"]),
                      "payload_bytes": sum(f["size"] for f in seal["files"].values())})
    for market, state in sorted(expected.items()):
        selected = sorted((p for p in parts if p["market"] == market), key=lambda p: p["month"])
        scope = state.get("coverage") or selected_scopes.get(market)
        if scope is None:
            blockers.append(f"{market}: campaign has not prepared or published a declared interval ({state['state']})")
            continue
        require(scope["market"] == market and scope["end_ns"] == CUTOFF, "campaign boundary differs")
        if selected:
            require(selected_scopes[market]["start_ns"] == scope["start_ns"], "campaign/publication start differs")
        require(len({p["spec_id"] for p in selected}) <= 1, "multiple candidates selected for one campaign market")
        if ([p["month"] for p in selected] != publication.months(scope["start_ns"], scope["end_ns"])
                or not selected or selected[0]["coverage_start_ns"] != scope["start_ns"]
                or selected[-1]["coverage_end_ns"] != scope["end_ns"]
                or any(a["coverage_end_ns"] != b["coverage_start_ns"] for a, b in zip(selected, selected[1:]))):
            blockers.append(f"{market}: complete declared publication absent ({state['state']})")
    for name, digest in implementation_binding().items():
        bind("src/futures_rebuild/" + name, digest, parse=False)
    for relative in ("src/futures_rebuild/hourly_foundation_audit.py", "tests/test_hourly_foundation_audit.py",
                     "requirements.lock", ECONOMICS_RULEBOOK_PATH):
        bind(relative, ECONOMICS_RULEBOOK_SHA256 if relative == ECONOMICS_RULEBOOK_PATH else None, parse=False)
    # Admission preparation is metadata-only; constructing either reader opens values.
    from .hourly_quality_reader import POINTER
    pointer = bind(POINTER)
    quality = bind(pointer['manifest']['path'], pointer['manifest']['sha256'])
    for relative, digest in quality['implementation'].items():
        bind(relative, digest, parse=False)
    admission = None
    from .hourly_source_admission import POINTER as ADMISSION_POINTER
    if admission_manifest is None and contained(root, ADMISSION_POINTER).exists():
        admission_pointer = bind(ADMISSION_POINTER)
        admission_manifest = admission_pointer['manifest']['path']
        bind(admission_manifest, admission_pointer['manifest']['sha256'])
    if admission_manifest is not None:
        admission = bind(admission_manifest)
        for relative, digest in admission['bindings'].items():
            bind(relative, digest, parse=False)
        for relative in ('tests/test_hourly_source_admission.py', 'tests/test_hourly_quality_reader.py'):
            bind(relative, parse=False)
    free = shutil.disk_usage(root).free
    if free < LIMITS.minimum_free_bytes + LIMITS.maximum_written_bytes:
        blockers.append("insufficient free-space reserve plus maximum output")
    # All metadata must still describe one snapshot; a running publisher may invalidate preparation.
    for relative, item in bindings.items():
        if sha256_file(contained(root, relative)) != item["sha256"]:
            if relative == CAMPAIGN and blockers:
                blockers.append("campaign progress changed during incomplete preparation; refresh after completion")
            else:
                raise IntegrityError(f"preparation drift: {relative}")
    return signed({"schema": "hourly_foundation_audit_plan/1", "repository_root": str(root),
        "attempt_id": str(uuid4()),
        "status": "BLOCKED_RELEASE_INCOMPLETE" if blockers else "PREPARED_REQUIRES_OWNER_APPROVAL",
        "blockers": blockers, "parts": sorted(parts, key=lambda p: (p["market"], p["month"])),
        "expected_markets": sorted(expected), "metadata": bindings, "sources": list(sources.values()),
        "environment": ENVIRONMENT, "limits": asdict(LIMITS), "free_bytes_at_preparation": free,
        "rows": sum(p["rows"] for p in parts), "normalized_bytes": sum(p["payload_bytes"] for p in parts),
        "source_hash_only_bytes": sum(s["size_bytes"] for s in sources.values() if s["kind"] == "DBN"),
        "source_admission_manifest": admission_manifest,
        "source_admission_manifest_sha256": bindings[admission_manifest]['sha256'] if admission else None,
        "scan_estimate": "Prior foundation audit 61.4 minutes plus quality scan 6.8 minutes; integrated run estimated 70-100 minutes, not yet measured",
        "model_specific_eligibility": "NOT_DECLARED", "model_requirements": list(MODEL_REQUIREMENTS)})


def classification(cadence):
    """Stored generic flags never establish the specific noncomparability cause."""
    state = cadence["result"]
    require(state in STATES and cadence["exception_state"] == STATES[state], "invalid comparison evidence")
    return {"MATCH": "MATCH", "DISAGREEMENT": "RECORDED_DISAGREEMENT",
            "SOURCE_MISSING": "MISSING_MINUTE_REFERENCE", "NOT_COMPARABLE": "UNKNOWN_CAUSE"}[state]


def inspect_group(tables, entry, candidate, sources, multiplier, previous, emit):
    rows = tables["observations"]
    require(rows and len({r["row_id"] for r in rows}) == len(rows), "empty or duplicate observation group")
    ids = [r["row_id"] for r in rows]
    for name in ("quality", "roll", "cadence"):
        require([r["row_id"] for r in tables[name]] == ids, f"{name} references/order differ")
    observed = [r for r in tables["missingness"] if r["observation_row_id"] is not None]
    require([r["observation_row_id"] for r in observed] == ids, "missingness references differ")
    counts = Counter({"observations": len(rows)})
    spec = candidate["spec"]
    for row, quality, roll, missing, cadence in zip(rows, tables["quality"], tables["roll"], observed, tables["cadence"]):
        start, end = row["bar_start_ns"], row["bar_end_ns"]
        require(row["market"] == entry["market"] and row["source_cadence"] == "1h"
                and row["source_contract_id"] == spec["source_contract_id"]
                and row["source_release_id"] == spec["source_release_id"], "observation scope differs")
        require(start % HOUR == 0 and end == start + HOUR
                and entry["coverage_start_ns"] <= start < end <= entry["coverage_end_ns"]
                and row["source_timestamp_ns"] == start
                and row["available_at_ns"] == row["decision_eligible_at_ns"] == end + LAG
                and end + LAG < spec["end_ns"] <= CUTOFF, "hourly timing or protected boundary differs")
        # The source contract orders definitions by receive time; event clocks are independent.
        require(row["definition_received_at_ns"] <= start
                and row["listing_activation_ns"] <= start < row["expiration_ns"], "definition timing differs")
        for prefix, family in (("source", "ohlcv_1h"), ("definition_source", "definition")):
            source = sources.get(row[prefix + "_file_path"])
            require(source is not None and source["family"] == family and source["kind"] == "DBN"
                    and source["sha256"] == row[prefix + "_file_sha256"] and source["market"] == row["market"]
                    and source["start_ns"] <= start < source["end_ns"], "selected source lineage differs")
        require(row["actual_contract"] == row["raw_symbol"] and row["actual_contract"]
                and row["currency"] == "USD" and row["min_price_increment_nano"] > 0
                and row["multiplier_nano"] == multiplier and row["volume"] >= 0, "contract or units invalid")
        require(row["low_nano"] <= min(row["open_nano"], row["close_nano"])
                <= max(row["open_nano"], row["close_nano"]) <= row["high_nano"], "invalid OHLC")
        require(all(quality[k] is True for k in ("ohlc_valid", "volume_valid", "timestamp_order_valid"))
                and quality["duplicate_state"] == "UNIQUE"
                and f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}" in quality["quality_flags"], "quality differs")
        require(row["low_nano"] >= 0 or "PROVIDER_VALID_NEGATIVE_PRICE" in quality["quality_flags"], "negative price evidence absent")
        require(missing["market"] == row["market"] and missing["interval_start_ns"] == start
                and missing["interval_end_ns"] == end and missing["state"] == "OBSERVED_VALID", "missingness interval differs")
        before = previous["actual_contract"] if previous else row["actual_contract"]
        changed = before != row["actual_contract"]
        require(roll["actual_contract_before"] == before and roll["actual_contract_after"] == row["actual_contract"]
                and roll["roll_flag"] == changed and roll["effective_time_ns"] == (start if changed else None)
                and roll["crossing_status"] == ("ROLL_BOUNDARY_UNADJUSTED" if changed else "NO_CROSSING")
                and roll["price_discontinuity_flag"] == bool(changed and previous["close_nano"] != row["open_nano"]), "roll evidence differs")
        left = max(entry["coverage_start_ns"], previous["bar_end_ns"]) if previous else entry["coverage_start_ns"]
        require(start >= left, "overlap or unordered rows")
        if left < start:
            emit({"kind": "UNCOVERED_INTERVAL", "start_ns": left, "end_ns": start, "state": "UNKNOWN"})
            counts["uncovered_ns"] += start - left
            counts["uncovered_intervals"] += 1
        require(cadence["source_cadence"] == "1h" and cadence["comparison_cadence"] == "1m"
                and cadence["interval_boundary_compatible"] is True, "comparison scope differs")
        cause = classification(cadence)
        counts["comparison_" + cadence["result"]] += 1
        counts["cause_" + cause] += 1
        if cause != "MATCH":
            refs = [{"path": s["path"], "sha256": s["sha256"]} for s in sources.values()
                    if s["market"] == row["market"] and s["kind"] == "DBN" and s["family"] == "ohlcv_1m"
                    and s["start_ns"] <= start < s["end_ns"]]
            emit({"kind": "MINUTE_COMPARISON", "start_ns": start, "end_ns": end, "row_id": row["row_id"],
                  "comparison_id": cadence["comparison_id"], "original_result": cadence["result"], "cause": cause,
                  "minute_references": refs, "needed_evidence": "Original minute timestamps, instrument/publisher IDs and OHLCV; separate read approval",
                  "training_eligibility": "NOT_DECLARED"})
        if changed:
            emit({"kind": "CONTRACT_CHANGE", "start_ns": start, "end_ns": end, "row_id": row["row_id"]})
        counts["contract_changes"] += changed
        counts["observed_ns"] += HOUR
        counts["negative_price_rows"] += row["low_nano"] < 0
        counts["zero_price_rows"] += any(row[k] == 0 for k in ("open_nano", "high_nano", "low_nano", "close_nano"))
        counts["unknown_schedule_rows"] += row["official_schedule_state"] == "UNKNOWN_FAIL_CLOSED"
        counts["definition_event_after_bar_start"] += row["definition_event_at_ns"] > start
        previous = row
    for gap in tables["missingness"]:
        if gap["observation_row_id"] is None:
            require(gap["market"] == entry["market"] and gap["interval_start_ns"] < gap["interval_end_ns"] <= CUTOFF,
                    "recorded gap boundary differs")
            emit({"kind": "RECORDED_MISSINGNESS", **gap})
            counts["recorded_gap_intervals"] += 1
    for name, table in tables.items():
        counts["table_rows_" + name] += len(table)
    return counts, previous


def reader_month(reader, admission, entry, rows, examples):
    """Complete row-policy census, distinct from selected public API checks."""
    counts = Counter()
    for row in rows:
        args = dict(market=entry['market'], start_ns=row['bar_start_ns'], end_ns=row['bar_end_ns'],
                    as_of_ns=row['decision_eligible_at_ns'], source='native')
        accepted = row['quality_disposition'] in NATIVE_ALLOWED
        try:
            validate_window([row], **args)
        except UnauthorizedOperation:
            require(not accepted, 'corroborated row failed causal window validation')
            counts['native_quarantined'] += 1
            examples['quarantine_' + row['row_id']] = (args, False)
        else:
            require(accepted, 'unresolved hourly row admitted')
            counts['native_accepted'] += 1
            uses = (admission.permitted_uses(market=args['market'], start_ns=args['start_ns'],
                    end_ns=args['end_ns']) if admission else ('LINEAGE_AUDIT',))
            if not uses:
                counts['quality_accepted_without_source_permission'] += 1
            for purpose in uses:
                counts['permitted_' + purpose] += 1
                # One real example per cohort/purpose, plus one per market.
                cohort = entry['month'][:4] if not admission else admission._entries[(entry['market'], entry['month'])]['cohort']
                examples.setdefault(str(cohort) + '_' + purpose, (dict(args, purpose=purpose), True))
            if admission:
                cohort = admission._entries[(entry['market'], entry['month'])]['cohort']
                examples.setdefault(str(cohort) + '_forbidden_selection',
                    (dict(args, purpose='SELECTION'), 'SELECTION' in uses))
                examples.setdefault('training_not_a_source_use', (dict(args, purpose='TRAINING'), False))
            if 'LINEAGE_AUDIT' in uses:
                examples.setdefault('market_' + entry['market'], (args, True))
    for left, right in zip(rows, rows[1:]):
        if any(row['quality_disposition'] not in NATIVE_ALLOWED for row in (left, right)):
            continue
        args = dict(market=entry['market'], start_ns=left['bar_start_ns'], end_ns=right['bar_end_ns'],
                    as_of_ns=max(left['decision_eligible_at_ns'], right['decision_eligible_at_ns']))
        if right['bar_start_ns'] != left['bar_end_ns']:
            examples.setdefault('gap', (args, False))
        elif any(left[k] != right[k] for k in IDENTITY):
            examples.setdefault('contract_or_units', (args, False))
    return counts


def check_reader_examples(reader, examples, budget=None):
    results = []
    for label, (args, expected) in sorted(examples.items()):
        if budget:
            budget.check()
        try:
            rows = reader.read_window(**args)
        except UnauthorizedOperation as exc:
            require(not expected, 'public hourly reader unexpectedly refused: ' + label)
            results.append(dict(case=label, request=args, result='REFUSED', reason=str(exc)))
        else:
            require(expected and rows and [r['bar_start_ns'] for r in rows] == list(range(args['start_ns'], args['end_ns'], HOUR)),
                    'public hourly reader selection differs: ' + label)
            results.append(dict(case=label, request=args, result='ACCEPTED', rows=len(rows)))
    return results


def check_raw_routes(root, index):
    from . import active_data_view, causal_timeframe_view
    kwargs = dict(market='ES', year=2024, timeframe='1h', purpose='LINEAGE_AUDIT')
    for name, call in (
        ('publication', lambda: publication.resolve_partitions(root, index, **kwargs)),
        ('timeframe_view', lambda: causal_timeframe_view.resolve_partitions(repository_root=root, **kwargs)),
        ('active_data_view', lambda: active_data_view.resolve_partitions(repository_root=root, **kwargs))):
        try:
            call()
        except UnauthorizedOperation as exc:
            require('HourlyReader.read_window' in str(exc), 'unexpected raw path refusal: ' + name)
        else:
            raise IntegrityError('raw hourly path admitted: ' + name)
    return ['publication', 'timeframe_view', 'active_data_view']


def scan(root, plan, budget):
    """Called only by the approved worker; generated fixtures exercise the same path."""
    index = read_record(contained(root, publication.INDEX))
    rulebook = EconomicsRuleBook.from_file(contained(root, ECONOMICS_RULEBOOK_PATH))
    source_map = {s["path"]: s for s in plan["sources"]}
    totals, markets, certificates = Counter(), {}, {}
    previous = None
    previous_entry = None
    admission = None
    if plan.get('source_admission_manifest'):
        from .hourly_source_admission import HourlySourceReader
        admission = HourlySourceReader(root, preview_manifest=plan['source_admission_manifest'])
    reader = admission.quality_reader if admission else HourlyReader(root)
    examples, unavailable_months = {}, []
    with budget.open(budget.directory / "exceptions.jsonl.gz") as output, gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as ledger, budget.open(budget.directory / "months.jsonl") as monthly:
        for entry in plan["parts"]:
            budget.check()
            if entry["disposition"] == "UNAVAILABLE":
                release = publication.validate_release(root, entry["evidence_entry"])
                seal = read_record(contained(root, entry["seal_path"]))
                validate_empty(entry, release, seal)
                release = {**release, "seal": seal}
            else:
                release = publication.validate_release(root, entry)
            candidate, seal = release["candidate"], release["seal"]
            if previous_entry is None or previous_entry["market"] != entry["market"]:
                previous = None
            else:
                require(previous_entry["coverage_end_ns"] == entry["coverage_start_ns"], "partition boundary gap/overlap")
            cert_id = candidate["spec_id"]
            state = certificates.setdefault(cert_id, {"digest": sha256(), "rows": 0, "months": [], "comparisons": Counter(),
                                                     "certificate": release["certificate"], "candidate": candidate})
            require(state["certificate"] == release["certificate"] and state["candidate"] == candidate, "mixed certificate binding")
            require(seal["incoming_state_id"] == sha256_json(previous), "incoming contract state differs")
            counts = Counter()
            quality_rows = reader._native_month(entry['market'], entry['month']) if entry['disposition'] == 'CERTIFIED' else []
            require(len(quality_rows) == entry['rows'], 'quality reader monthly row count differs')
            quality_by_id = {r['row_id']: r for r in quality_rows}
            require(len(quality_by_id) == len(quality_rows), 'quality reader duplicate row identity')
            def emit(event):
                ledger.write(canonical_bytes({"market": entry["market"], "month": entry["month"],
                    "manifest": entry["manifest"], **event}) + b"\n")
            iterator = (iter_bundle_groups(contained(root, entry["directory"]), maximum_group_bytes=LIMITS.maximum_group_bytes)
                        if entry["disposition"] == "CERTIFIED" else iter(()))
            try:
                boundary = entry["coverage_start_ns"]
                for day in seal["coverage"]:
                    budget.check()
                    require(day["start_ns"] == boundary and day["start_ns"] < day["end_ns"] <= entry["coverage_end_ns"]
                            and day["day_ns"] == day["start_ns"] // DAY * DAY
                            and day["end_ns"] <= day["day_ns"] + DAY, "daily coverage discontinuity")
                    boundary = day["end_ns"]
                    require(day["source_primary_rows"] == day["observations"] + day["excluded_start"] + day["excluded_availability"],
                            "source-to-normalized accounting differs")
                    for key in ("source_primary_rows", "excluded_start", "excluded_availability"):
                        require(type(day[key]) is int and day[key] >= 0, "invalid source accounting")
                        counts[key] += day[key]
                    if day["excluded_start"] or day["excluded_availability"] or not day["observations"]:
                        emit({"kind": "DECLARED_DAILY_COVERAGE", **day, "interpretation": "NO_SCHEDULE_OR_TRAINING_AUTHORITY"})
                    if not day["observations"]:
                        require(not any(day["comparison_counts"].values()), "empty day has comparisons")
                        continue
                    tables = next(iterator, None)
                    require(tables is not None and all(day["start_ns"] <= r["bar_start_ns"] < day["end_ns"] for r in tables["observations"]), "daily groups differ")
                    for row in tables['observations']:
                        actual = quality_by_id.pop(row['row_id'], None)
                        require(actual is not None and all(actual.get(k) == v for k, v in row.items()),
                                'quality reader observation differs from normalized tables')
                    c, previous = inspect_group(tables, entry, candidate, source_map,
                        int(rulebook.rules[entry["market"]].expected_unit_qty * 10**9), previous, emit)
                    require(c["observations"] == day["observations"] and
                            {k: c["comparison_" + k] for k in STATES} == day["comparison_counts"], "daily counts differ")
                    counts.update(c)
                    for name in sorted(tables):
                        for row in tables[name]:
                            state["digest"].update(canonical_bytes({"table": name, "row": row}) + b"\n")
                require(boundary == entry["coverage_end_ns"] and next(iterator, None) is None, "incomplete monthly decode")
            finally:
                if entry["disposition"] == "CERTIFIED":
                    iterator.close()
            require(previous == seal["last_observation"], "terminal contract state differs")
            require(not quality_by_id, 'quality reader has unexplained additional rows')
            left = max(entry["coverage_start_ns"], previous["bar_end_ns"]) if previous else entry["coverage_start_ns"]
            if left < entry["coverage_end_ns"]:
                emit({"kind": "UNCOVERED_INTERVAL", "start_ns": left, "end_ns": entry["coverage_end_ns"], "state": "UNKNOWN"})
                counts["uncovered_ns"] += entry["coverage_end_ns"] - left
                counts["uncovered_intervals"] += 1
            require(counts["observed_ns"] + counts["uncovered_ns"] == entry["coverage_end_ns"] - entry["coverage_start_ns"], "monthly interval accounting differs")
            require(counts["observations"] == entry["rows"], "prepared row count differs")
            for name in FILENAMES:
                require(counts["table_rows_" + name] == seal["table_metadata"].get(name, {}).get("rows", 0), "supporting table count differs")
            state["rows"] += counts["observations"]
            state["months"].append(entry["month"])
            state["comparisons"].update({k: counts["comparison_" + k] for k in STATES})
            counts["partitions"] = 1
            counts["certified_empty_months"] = int(entry["disposition"] == "UNAVAILABLE")
            counts.update(reader_month(reader, admission, entry, quality_rows, examples))
            if entry['disposition'] == 'UNAVAILABLE':
                unavailable_months.append([entry['market'], entry['month']])
                examples.setdefault('unavailable_' + entry['market'], (dict(market=entry['market'],
                    start_ns=entry['coverage_start_ns'], end_ns=entry['coverage_start_ns'] + HOUR,
                    as_of_ns=entry['coverage_start_ns'] + HOUR + LAG), False))
            totals.update(counts)
            markets.setdefault(entry["market"], Counter()).update(counts)
            monthly.write(canonical_bytes({"market": entry["market"], "month": entry["month"], "counts": dict(counts)}) + b"\n")
            budget.check()
            previous_entry = entry
            if totals['partitions'] % 250 == 0:
                print(totals['partitions'], 'months;', totals['observations'], 'observations checked', flush=True)
    fingerprints = {}
    for spec_id, state in certificates.items():
        actual = {"rows": state["rows"], "logical_sha256": state["digest"].hexdigest(),
                  "months": state["months"], "comparison_counts": dict(state["comparisons"])}
        require(state["certificate"]["passes"] == [actual, actual], "published logical rows differ from both retained source replays")
        fingerprints[spec_id] = actual
    require(totals["observations"] == plan["rows"] and totals["partitions"] == len(plan["parts"]), "complete scan accounting differs")
    require(read_record(contained(root, publication.INDEX)) == index, "index changed during scan")
    for row in reader.derived:
        decorated = dict(row, quality_disposition=row['disposition'], decision_eligible_at_ns=row['available_at_ns'])
        args = dict(market='ZQ', start_ns=row['bar_start_ns'], end_ns=row['bar_end_ns'],
                    as_of_ns=row['available_at_ns'], source='minute_derived')
        validate_window([decorated], **args)
        totals['derived_rows'] += 1
        examples.setdefault('explicit_derived', (args, True))
        examples.setdefault('derived_native_no_fallback', (dict(args, source='native'), False))
        examples.setdefault('derived_no_research', (dict(args, purpose='FEATURE_GENERATION'), False))
        examples.setdefault('premature_derived', (dict(args, as_of_ns=args['as_of_ns']-1), False))
    if admission:
        measured = dict(partitions=totals['partitions'], native_rows=totals['observations'],
                        native_accepted=totals['native_accepted'], native_quarantined=totals['native_quarantined'],
                        unavailable_months=len(unavailable_months), derived_rows=totals['derived_rows'])
        require(measured == admission.manifest['expected_counts'], 'complete hourly admission census differs')
    actual_reads = check_reader_examples(admission or reader, examples, budget)
    raw_routes = check_raw_routes(root, index)
    reader._unchanged()
    if admission:
        admission._unchanged()
    return {"totals": dict(totals), "markets": {m: dict(c) for m, c in markets.items()},
            "certified_logical_fingerprints": fingerprints, "unavailable_months": unavailable_months,
            "actual_window_reads": actual_reads,
            "comparison_flags_scope": "ORIGINAL_NORMALIZATION_FLAGS; CURRENT_DISPOSITIONS_ARE_IN_BOUND_QUALITY_POLICY",
            "raw_hourly_routes_refused": raw_routes,
            "source_admission_manifest_sha256": plan.get('source_admission_manifest_sha256'),
            "data_integrity": "PASS_RETAINED_SOURCE_REPLAY_AND_PUBLICATION_EQUALITY",
            "coverage": "MEASURED_WITH_EXPLICIT_UNKNOWN_INTERVALS", "reader_correctness": "PASS_FULL_ROW_POLICY_AND_SELECTED_PUBLIC_WINDOWS",
            "model_specific_eligibility": "NOT_DECLARED",
            "research_permission": "SOURCE_COHORT_POLICY_VALIDATED_NOT_EXECUTION_AUTHORITY" if admission else "LINEAGE_AUDIT_ONLY"}


def verify_bindings(root, plan, budget):
    from .causal_1h import ENVIRONMENT
    require(plan["environment"] == ENVIRONMENT, "audit environment changed")
    for relative, item in plan["metadata"].items():
        require(budget.hash(contained(root, relative)) == item["sha256"], f"prepared input changed: {relative}")


def hash_inputs(root, plan, budget):
    for source in plan["sources"]:
        path = contained(root, source["path"])
        require(path.stat().st_size == source["size_bytes"] and budget.hash(path) == source["sha256"], "selected source changed")
    for entry in plan["parts"]:
        if entry["disposition"] == "UNAVAILABLE":
            continue  # Its retained seal is hash-bound metadata, with no payload to open.
        release = read_record(contained(root, entry["manifest"]))
        for info in release["seal"]["files"].values():
            path = contained(root, entry["directory"] + "/" + info["filename"])
            require(path.stat().st_size == info["size"] and budget.hash(path) == info["sha256"], "selected payload changed")


def authorize(root, plan, authorization):
    publication.identity(plan)
    require(plan["repository_root"] == str(Path(root).resolve()) and plan["limits"] == asdict(LIMITS), "audit scope/limits differ")
    if (plan["status"] != "PREPARED_REQUIRES_OWNER_APPROVAL" or plan["blockers"]
            or set(plan["expected_markets"]) != set(V10_CHECKPOINT_MARKETS)
            or set(p["market"] for p in plan["parts"]) != set(V10_CHECKPOINT_MARKETS)):
        raise UnauthorizedOperation("complete 41-market publication required before historical scan")
    if (authorization.get("plan_id") != plan["id"] or authorization.get("status") != "OWNER_APPROVED_IN_THIS_TASK"
            or not authorization.get("owner_message")):
        raise UnauthorizedOperation("separate exact-scope owner approval required before historical scan")


def worker():
    request = json.loads(sys.stdin.buffer.readline(2**20))
    root = Path.cwd()
    plan = read_record(contained(root, request["plan_path"]))
    authorization = json.loads(contained(root, request["authorization_path"]).read_bytes())
    authorize(root, plan, authorization)
    directory = contained(root, HERE + "/" + plan["id"] + "/execution")
    directory.mkdir(exist_ok=False)
    budget = Budget(directory, WORKER_LIMITS)
    budget.written = sum(p.stat().st_size for p in directory.parent.iterdir() if p.is_file())
    budget.terminal_reserve = 2**20
    result = {"status": "INCOMPLETE", "plan_id": plan["id"], "model_specific_eligibility": "NOT_DECLARED"}
    try:
        verify_bindings(root, plan, budget)
        hash_inputs(root, plan, budget)
        budget.save(directory / "authorization.json", authorization)
        result.update(scan(root, plan, budget))
        hash_inputs(root, plan, budget)
        verify_bindings(root, plan, budget)
        budget.check()
        result["status"] = "PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS"
    except Exception as exc:
        result.update(status="INCOMPLETE", error_type=type(exc).__name__, error=str(exc))
    finally:
        budget.finalizing = True
        budget.terminal_reserve = 0
        report = ("# Hourly foundation audit\n\n"
            f"Worker status: **{result['status']}**. A passing supervision.json is also required for completion.\n\n"
            f"Data integrity: {result.get('data_integrity', 'INCOMPLETE')}. "
            f"Coverage: {result.get('coverage', 'INCOMPLETE')}. "
            f"Reader correctness: {result.get('reader_correctness', 'INCOMPLETE')}.\n\n"
            f"Source permission: {result.get('research_permission', 'NOT_VALIDATED')}. "
            "Model-specific eligibility: **NOT_DECLARED**; this does not block data-foundation completion.\n\n"
            "result.json contains complete accounting, market totals and logical fingerprints; months.jsonl "
            "contains monthly counts; exceptions.jsonl.gz contains interval evidence and original comparison flags. "
            "Those original flags are preserved provenance, not a claim that the later quality investigation remains unresolved. "
            "The source chain uses retained replay certificates and hash-only source verification, with no raw decode. "
            "Unknown intervals are not classified as exchange closures, and noncomparability is not corruption.\n\n"
            + (f"Failure: {result['error_type']}: {result['error']}\n\n" if "error" in result else "")
            + "## Requirements before a model-specific audit\n\n" + "\n".join("- " + r for r in MODEL_REQUIREMENTS) + "\n")
        with budget.open(directory / "REPORT.md") as stream:
            stream.write(report.encode("utf-8"))
        result["resources"] = budget.record()
        budget.save(directory / "result.json", signed(result))
    return 0 if result["status"].startswith("PASS_") else 1


def supervise(root, plan_path, authorization_path):
    """Separate approved task action; the CLI intentionally exposes preparation only."""
    from .causal_1h_native import supervise as native_supervise
    root = Path(root).resolve()
    plan = read_record(contained(root, plan_path))
    authorization = json.loads(contained(root, authorization_path).read_bytes())
    authorize(root, plan, authorization)
    require(plan_path == HERE + "/" + plan["id"] + "/plan.json", "noncanonical plan path")
    directory = contained(root, HERE + "/" + plan["id"])
    output = HERE + "/" + plan["id"] + "/execution"
    record = {"status": "INCOMPLETE", "plan_id": plan["id"], "model_specific_eligibility": "NOT_DECLARED"}
    with execution_lease(root):
        require(not (directory / "execution").exists() and not (directory / "supervision.json").exists(), "audit attempt already used")
        try:
            require(memory_peak() <= COORDINATOR_BYTES, "coordinator exceeds reserved memory")
            seconds = native_supervise([sys.executable, "-B", "-c",
                "from futures_rebuild.hourly_foundation_audit import worker; raise SystemExit(worker())"], root,
                {"plan_path": plan_path, "authorization_path": authorization_path,
                 "operation": {"id": plan["id"], "output": output}}, WORKER_LIMITS)
            result = read_record(contained(root, output) / "result.json")
            peak = memory_peak() + result["resources"]["peak_process_memory_bytes"]
            total_bytes = sum(p.stat().st_size for p in directory.iterdir() if p.is_file()) + result["resources"]["retained_logical_bytes"] + (contained(root, output) / "result.json").stat().st_size
            require(result["status"].startswith("PASS_") and result["plan_id"] == plan["id"]
                    and peak <= LIMITS.maximum_memory_bytes and seconds <= LIMITS.maximum_seconds
                    and total_bytes + 65536 <= LIMITS.maximum_written_bytes
                    and shutil.disk_usage(root).free >= LIMITS.minimum_free_bytes + 65536, "complete-run acceptance failed")
            record.update(status="PASS_SUPERVISED_FOUNDATION_AUDIT", conservative_aggregate_peak_bytes=peak,
                          seconds=seconds, output_bytes_before_receipt=total_bytes, workers_stopped=True,
                          worker_result_sha256=sha256_file(contained(root, output) / "result.json"))
        except Exception as exc:
            record.update(error_type=type(exc).__name__, error=str(exc))
        with (directory / "supervision.json").open("xb") as stream:
            stream.write(canonical_bytes(signed(record)) + b"\n")
    return record


def main():
    root = Path.cwd()
    plan = prepare(root)
    directory = contained(root, HERE + "/" + plan["id"])
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "plan.json").write_bytes(canonical_bytes(plan) + b"\n")
    report = ("# Hourly foundation audit preparation\n\n"
        f"Status: **{plan['status']}**. Historical scan has not run.\n\n"
        f"Selected so far: {len(plan['parts'])} partitions, {plan['rows']:,} recorded observations. "
        "These are metadata counts, not fresh payload verification.\n\n"
        f"Normalized input: {plan['normalized_bytes']:,} bytes. Source hash-only input: "
        f"{plan['source_hash_only_bytes']:,} bytes per pass. No source decode is planned. "
        f"{plan['scan_estimate']}. The hard runtime limit remains eight hours.\n\n"
        "Limits: one worker; 6 GiB conservative aggregate process-memory ceiling; 2 GiB output; "
        "eight hours; 64 GiB free-space reserve. Preparation sizes cover only the currently published selection.\n\n"
        "Data integrity: NOT_SCANNED. Coverage: NOT_SCANNED. Reader runtime audit: NOT_SCANNED. "
        "Model-specific eligibility: NOT_DECLARED and outside this phase. "
        f"Source admission candidate: {plan.get('source_admission_manifest') or 'none; quality-reader audit only'}. "
        "Preparation changes no active permissions.\n\n"
        "## Blockers\n\n" + ("\n".join("- " + b for b in plan["blockers"]) or "- Exact-scope owner approval required.") +
        "\n\n## Future model audit, outside this phase\n\n" + "\n".join("- " + b for b in MODEL_REQUIREMENTS) + "\n")
    (directory / "REPORT.md").write_text(report, encoding="utf-8")
    print(json.dumps({k: plan[k] for k in ("id", "status", "blockers", "rows", "normalized_bytes", "source_hash_only_bytes", "free_bytes_at_preparation")}))


if __name__ == "__main__":
    main()
