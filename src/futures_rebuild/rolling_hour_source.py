"""Strict in-memory bridge from certified source products; no reader or access grant.

The authorized source/readiness caller must verify immutable files and evidence
before handing over rows. These structural checks do not certify source custody.
Historical registration/execution still belongs to CertifiedResearchGateway.
"""
from . import rolling_hour_research as r
from . import profit_aligned_research as profit
from .errors import IntegrityError
from .fixed_hour_ridge_audit_successor import validate_fixed_hour_rows


def adapt_source_sessions(*, sessions, rows_by_date, quotes_by_date, calendar,
                          source_contract_id, source_release_id, policy, bindings, as_of_ns):
    """Reject incomplete joins, retrospective selection and synthetic substitutes.

Bindings are per-date source-certificate contents, not caller approval flags.
The bridge never opens a source file or turns bindings into access authority.
"""
    if (source_contract_id == r.SYNTHETIC_SOURCE_CONTRACT_ID
            or source_release_id == r.SYNTHETIC_SOURCE_RELEASE_ID
            or policy["source_release_id"] in {"a" * 64, r.SYNTHETIC_SOURCE_RELEASE_ID}):
        raise IntegrityError("synthetic source identities forbidden in real-source adapter")
    if not isinstance(calendar, r.VerifiedExchangeCalendar):
        raise IntegrityError("verified exchange calendar required")
    calendar.verify()
    sessions = tuple(sessions)
    dates = profit.ordered_dates([s.trade_date.isoformat() for s in sessions])
    if not dates or any(not "2018-01-01" <= d < "2023-01-01" for d in dates):
        raise IntegrityError("adapter is bounded to 2018-2022 selection dates")
    if any(set(mapping) != set(dates) for mapping in (rows_by_date, quotes_by_date, bindings)):
        raise IntegrityError("exact complete declared dates required for prices quotes and bindings")
    markets = {s.market for s in sessions}
    if len(markets) != 1 or not markets <= set(r.MARKETS):
        raise IntegrityError("one exact declared market required")
    # The calendar, not the observed row dates, supplies every session in scope.
    expected = sorted(s.trade_date.isoformat() for (m, _), s in calendar.sessions.items()
        if m in markets and dates[0] <= s.trade_date.isoformat() <= dates[-1])
    if list(dates) != expected:
        raise IntegrityError("declared dates omit verified calendar sessions")
    profit.validate_policy(policy)
    batches = []
    for session in sessions:
        day = session.trade_date.isoformat()
        rows, quotes, binding = rows_by_date[day], quotes_by_date[day], bindings[day]
        profit.identity(binding, "binding_id")
        for key in ("calendar_evidence_id", "contract_selection_evidence_id", "source_certificate_id"):
            profit.digest(binding.get(key))
        if (binding.get("calendar_release_id") != calendar.receipt.release_id
                or binding.get("source_contract_id") != source_contract_id
                or binding.get("source_release_id") != source_release_id
                or binding.get("market") != session.market or binding.get("trade_date") != day):
            raise IntegrityError("source certificate scope or calendar applicability differs")
        starts = [int(i.starts_at_utc.timestamp()) * 10**9 for i in session.intervals if i.state == "OPEN"]
        known = (binding.get("calendar_known_at_ns"), binding.get("contract_selection_known_at_ns"))
        if any(type(t) is not int or t < 0 or (starts and t > min(starts)) for t in known):
            raise IntegrityError("calendar or contract selection is not known before session open")
        if binding.get("point_in_time_evidence") != "CERTIFIED_ORIGINAL_PUBLICATION":
            raise IntegrityError("verified retrospective calendar is not point-in-time evidence")
        expected_minutes = [t for i in session.intervals if i.state == "OPEN"
            for t in range(int(i.starts_at_utc.timestamp()) * 10**9,
                int(i.ends_at_utc.timestamp()) * 10**9, r.MINUTE)]
        if [row.get("bar_start_ns") for row in rows] != expected_minutes:
            raise IntegrityError("complete chronological open-interval minute coverage required")
        contract = binding.get("actual_contract")
        if not isinstance(contract, str) or not contract:
            raise IntegrityError("unambiguous point-in-time actual contract required")
        validate_fixed_hour_rows(rows, expected_market=session.market, expected_trade_date=day,
            expected_contract=contract, expected_source_contract_id=source_contract_id,
            expected_source_release_id=source_release_id)
        allowed = binding.get("source_file_sha256s")
        if not isinstance(allowed, list) or not allowed:
            raise IntegrityError("exact certified source file identities required")
        for digest in allowed:
            profit.digest(digest)
        if any(row["source_file_sha256"] not in allowed for row in rows):
            raise IntegrityError("price row lies outside certified source files")
        grid = r.opportunity_grid(session, calendar=calendar,
            source_contract_id=source_contract_id, source_release_id=source_release_id)
        if set(quotes) != {s["opportunity_id"] for s in grid}:
            raise IntegrityError("complete quote join required; no implicit historical abstention")
        for scheduled in grid:
            fields = quotes[scheduled["opportunity_id"]]
            if set(fields) != {"decision_quote", "entry_quote", "exit_quote"}:
                raise IntegrityError("exact quote roles required")
            for key, at in (("decision_quote", scheduled["decision_ns"]),
                            ("entry_quote", scheduled["entry_ns"]), ("exit_quote", scheduled["exit_ns"])):
                quote = fields[key]
                if quote is None or quote.get("source_release_id") != policy["source_release_id"]:
                    raise IntegrityError("missing or incompatible historical quote source")
                if not profit.usable_quote(quote, at_ns=at, market=session.market, contract=contract,
                        tick_nano=rows[0]["min_price_increment_nano"], policy=policy):
                    raise IntegrityError("historical quote clock contract quality or cadence incompatible")
        batches.append(r.assemble_session(session, rows, as_of_ns=as_of_ns, quotes=quotes,
            calendar=calendar, source_contract_id=source_contract_id, source_release_id=source_release_id))
    return r.RollingDataset(tuple(batches), policy, sessions, calendar, source_contract_id, source_release_id)
