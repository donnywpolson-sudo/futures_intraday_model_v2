"""In-memory reference joins only; no source access, certification or fitting.

The authorized caller supplies logical observations and exact reference scopes.
A passing report is structural evidence, never a historical execution grant.
Frozen v7's adapter and learning procedure are deliberately unchanged.
"""
from collections import defaultdict

from . import rolling_hour_research as r
from .errors import ContractError, IntegrityError
from .foundation.identity import DefinitionIndex
from .foundation.market_state import AsOfStatisticsLedger, AsOfStatusLedger
from .foundation.records import ProviderBar, datetime_to_ns, ns_to_datetime
from .rolling_hour_source import adapt_source_sessions


def audit_source_joins(*, definitions, statuses, statistics, statistics_roles,
                       reference_sources, **source):
    """Audit one market without filtering dates/opportunities or returning arrays.

``source`` is the existing adapter's keyword contract. ``reference_sources``
maps definition/status/statistics to their certified source_release_id,
source_manifest_sha256 and source_file_sha256s. These contents are not certificates:
the calling access boundary remains responsible for verifying custody/publication.
Statistics queries include only keys already received, so future keys cannot
change an earlier diagnostic. Values are omitted; none are model features.
"""
    sessions = tuple(source["sessions"])
    source = {**source, "sessions": sessions}
    dates = [s.trade_date.isoformat() for s in sessions]
    if not dates or dates != sorted(set(dates)) or len({s.market for s in sessions}) != 1:
        raise IntegrityError("one market and complete ordered unique dates required")
    if any(set(source[name]) != set(dates) for name in ("rows_by_date", "quotes_by_date", "bindings")):
        raise IntegrityError("exact declared dates required for audit inputs")
    market = sessions[0].market
    if market not in r.MARKETS or any(not "2018-01-01" <= d < "2023-01-01" for d in dates):
        raise IntegrityError("reference audit is bounded to declared 2018-2022 markets")
    families = dict(definition=tuple(definitions), status=tuple(statuses), statistics=tuple(statistics))
    if set(reference_sources) != set(families):
        raise IntegrityError("exact reference source families required")
    # Ledger keys omit source release and market: constrain these before joining.
    for family, records in families.items():
        scope = reference_sources[family]
        r.profit.digest(scope["source_release_id"])
        r.profit.digest(scope["source_manifest_sha256"])
        if not scope["source_file_sha256s"]:
            raise IntegrityError("exact reference source file identities required")
        for digest in scope["source_file_sha256s"]:
            r.profit.digest(digest)
        if any(record.market != market or record.source_release_id != scope["source_release_id"]
               or record.source_manifest_sha256 != scope["source_manifest_sha256"]
               or record.source_file_sha256 not in scope["source_file_sha256s"] for record in records):
            raise IntegrityError(f"{family} record outside declared market/source scope")
    index = DefinitionIndex(families["definition"])
    errors, ledgers = [], {}
    for name, build in (("status", lambda: AsOfStatusLedger(families["status"])),
                        ("statistics", lambda: AsOfStatisticsLedger(families["statistics"], roles=statistics_roles))):
        try:
            ledgers[name] = build()
        except ContractError as exc:
            errors.append(f"{name}: {exc}")
            ledgers[name] = None
    statistic_rows = {row.record_id: row for row in families["statistics"]}
    statistic_keys = defaultdict(dict)
    for row in families["statistics"]:
        key = (row.dataset, row.publisher_id, row.instrument_id, row.instrument_id_date_utc)
        slot = (row.stat_type, row.ts_ref_ns)
        statistic_keys[key][slot] = min(row.ts_recv_ns, statistic_keys[key].get(slot, row.ts_recv_ns))
    audits = []
    for session in sessions:
        day = session.trade_date.isoformat()
        rows, binding = source["rows_by_date"][day], source["bindings"][day]
        grid = r.opportunity_grid(session, calendar=source["calendar"],
            source_contract_id=source["source_contract_id"], source_release_id=source["source_release_id"])
        expected = [t for interval in session.intervals if interval.state == "OPEN"
            for t in range(datetime_to_ns(interval.starts_at_utc, "open"),
                           datetime_to_ns(interval.ends_at_utc, "close"), r.MINUTE)]
        reasons, joins = [], []
        if [row.get("bar_start_ns") for row in rows] != expected:
            reasons.append("complete chronological open-interval minute coverage required")
        by_start = defaultdict(list)
        for row in rows:
            by_start[row.get("bar_start_ns")].append(row)
            joined = {"row_id": row.get("row_id"), "bar_start_ns": row.get("bar_start_ns"),
                      "definition_row_id": None, "known_at_ns": None, "error": None}
            try:
                bar = ProviderBar(dataset="GLBX.MDP3", market=market,
                    publisher_id=row["publisher_id"], instrument_id=row["instrument_id"],
                    event_at_ns=row["bar_start_ns"],
                    **{k: row[k] for k in ("open_nano", "high_nano", "low_nano", "close_nano", "volume",
                                          "source_release_id", "source_file_path", "source_file_sha256")},
                    source_manifest_sha256=row["source_contract_id"], row_sha256=row["source_row_sha256"])
                definition = index.resolve(bar, decision_at=bar.event_at)
                joined.update(definition_row_id=definition.row_sha256, known_at_ns=definition.ts_recv_ns)
                if (definition.raw_symbol != row["actual_contract"]
                        or definition.min_price_increment_nano != row["min_price_increment_nano"]
                        or definition.row_sha256 != row["definition_row_sha256"]
                        or definition.source_file_sha256 != row["definition_source_file_sha256"]):
                    raise IntegrityError("resolved definition differs from observation identity/economics/provenance")
            except (ContractError, IntegrityError, KeyError, TypeError, ValueError) as exc:
                joined["error"] = str(exc)
            joins.append(joined)
        if any(join["error"] is not None for join in joins):
            reasons.append("unresolved or incompatible minute definitions; see minute_joins")
        opportunities = []
        for scheduled in grid:
            opportunity = {"schedule": scheduled, "checks": {}, "reasons": []}
            for role in ("decision", "entry"):
                at = scheduled[f"{role}_ns"]
                matches = by_start.get(at // r.MINUTE * r.MINUTE, ())
                check = {"status": None, "statistics": []}
                opportunity["checks"][role] = check
                if len(matches) != 1:
                    opportunity["reasons"].append(f"{role}: missing or ambiguous minute identity")
                    continue
                row = matches[0]
                if any(type(row.get(k)) is not int for k in ("publisher_id", "instrument_id")):
                    opportunity["reasons"].append(f"{role}: missing instrument key")
                    continue
                key = dict(dataset="GLBX.MDP3", publisher_id=row["publisher_id"], instrument_id=row["instrument_id"],
                    instrument_id_date_utc=ns_to_datetime(at, "decision").date().isoformat())
                check["identity"] = key
                if ledgers["status"] is not None:
                    state = ledgers["status"].as_of(**key, decision_at_ns=at)
                    check["status"] = state.as_dict()
                    if not state.foundation_eligible:
                        opportunity["reasons"].append(f"{role}: {state.status_disposition}")
                if ledgers["statistics"] is not None:
                    for (stat_type, ts_ref_ns), first_received in sorted(statistic_keys[tuple(key.values())].items()):
                        if first_received > at:
                            continue
                        state = ledgers["statistics"].as_of(**key, stat_type=stat_type, ts_ref_ns=ts_ref_ns, decision_at_ns=at)
                        matched = statistic_rows[state.record_id]
                        check["statistics"].append(dict(stat_type=stat_type, ts_ref_ns=ts_ref_ns,
                            state=state.state, role=state.role, feature_eligible=False, record_id=state.record_id,
                            known_at_ns=matched.ts_recv_ns, ts_event_ns=matched.ts_event_ns))
                check["statistics_availability"] = ("LEDGER_CONFLICT" if ledgers["statistics"] is None
                    else "RECEIVED_KEYS_AUDITED" if check["statistics"] else "NO_RECEIVED_KEYS")
            opportunities.append(opportunity)
        if any(opportunity["reasons"] for opportunity in opportunities):
            reasons.append("unresolved or incompatible opportunity references; see opportunities")
        audits.append(dict(market=market, trade_date=day, binding_id=binding.get("binding_id"),
            calendar_release_id=source["calendar"].receipt.release_id, actual_contract=binding.get("actual_contract"),
            expected_minutes=len(expected), observed_minutes=len(rows), minute_joins=joins,
            opportunities=opportunities, reasons=reasons))
    # Do not construct a dataset from failed references. Reuse the unchanged
    # adapter for every other structural rule, including publication and quotes.
    adapter_check = "NOT_ATTEMPTED_REFERENCE_JOIN_BLOCKED"
    if not errors and not any(audit["reasons"] for audit in audits):
        try:
            adapt_source_sessions(**source)
            adapter_check = "PASSED_STRUCTURAL_CHECKS_ONLY"
        except (ContractError, IntegrityError, KeyError, TypeError, ValueError) as exc:
            adapter_check = "BLOCKED"
            errors.append(f"source adapter: {exc}")
    return dict(schema_version="rolling_hour_reference_audit/1.0.0", claim="STRUCTURAL_AUDIT_ONLY",
        historical_execution_authorized=False, source_contract_id=source["source_contract_id"],
        source_release_id=source["source_release_id"], reference_sources=reference_sources,
        reference_record_counts={family: len(records) for family, records in families.items()},
        statistics_roles=statistics_roles.as_dict(), dates=audits, errors=errors, adapter_check=adapter_check,
        complete=adapter_check == "PASSED_STRUCTURAL_CHECKS_ONLY")
