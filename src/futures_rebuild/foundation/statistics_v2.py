"""Opt-in statistics interpretation; the V1 ledger and its bytes stay unchanged.

This is preparation, not feature admission. Records and definitions must come
from the existing verified offline decoders. One instance holds one bounded
market/year, not a minute-by-statistic materialization.
"""
from collections import defaultdict
from dataclasses import dataclass
from itertools import groupby, islice

import databento_dbn as dbn

from ..errors import ContractError
from .decoder import _chunks, _enum_name, _index_date_iso, _row_id
from .identity import DefinitionIndex
from .market_state import _normalize_equal_receive_rows
from .records import INT64_NULL, ProviderBar, StatisticsRecordV1, ns_to_datetime

VERSION = "statistics_interpretation/2"
TYPES = ("OPEN_INTEREST", "SETTLEMENT_PRICE")


@dataclass(frozen=True)
class DecodedStatisticV2:
    record: StatisticsRecordV1
    quantity_bits: int

    def __post_init__(self):
        if type(self.record) is not StatisticsRecordV1 or self.quantity_bits not in (32, 64):
            raise ContractError("Statistics require a V1 raw record and decoded signed width")
        if not 0 <= self.record.quantity <= (1 << (self.quantity_bits - 1)) - 1:
            raise ContractError("Quantity does not fit its decoded signed field")


def iter_statistics_v2(binding, *, market, expected_query_contract, batch_rows=100_000):
    """Reuse source verification and decoding, retaining the actual field width."""
    ordinal, dates = 0, {}
    for chunk in _chunks(binding, schema="statistics", market=market,
                         expected_query_contract=expected_query_contract, batch_rows=batch_rows):
        dtype = chunk.dtype.fields["quantity"][0]
        if dtype.kind != "i" or dtype.itemsize not in (4, 8):
            raise ContractError("Unsupported decoded statistics quantity encoding")
        for row in chunk:
            received = int(row["ts_recv"])
            yield DecodedStatisticV2(StatisticsRecordV1(
                dataset="GLBX.MDP3", market=market, publisher_id=int(row["publisher_id"]),
                instrument_id=int(row["instrument_id"]),
                instrument_id_date_utc=_index_date_iso(received, dates),
                ts_event_ns=int(row["ts_event"]), ts_recv_ns=received, ts_ref_ns=int(row["ts_ref"]),
                ts_in_delta=int(row["ts_in_delta"]),
                stat_type=_enum_name(dbn.StatType, row["stat_type"], "stat_type"),
                update_action=_enum_name(dbn.StatUpdateAction, row["update_action"], "update_action"),
                price_nano=int(row["price"]), quantity=int(row["quantity"]),
                sequence=int(row["sequence"]), channel_id=int(row["channel_id"]), flags=int(row["stat_flags"]),
                source_release_id=binding.source_release_id,
                source_manifest_sha256=binding.source_manifest_sha256,
                source_file_path=binding.relative_path, source_file_sha256=binding.sha256,
                row_ordinal=ordinal, row_sha256=_row_id(binding, ordinal, row.tobytes()),
            ), dtype.itemsize * 8)
            ordinal += 1


def interpret_statistic(value):
    record = value.record
    price = quantity = None
    if record.update_action == "DELETE":
        reason = "DELETED"
    elif record.update_action != "NEW":
        reason = "UNKNOWN_UPDATE_ACTION"
    elif record.stat_type == "OPEN_INTEREST":
        quantity = record.quantity
        reason = "KNOWN" if quantity != (1 << (value.quantity_bits - 1)) - 1 else "UNDEFINED_QUANTITY"
        if reason != "KNOWN":
            quantity = None
    elif record.stat_type == "SETTLEMENT_PRICE":
        price = record.price_nano
        reason = "KNOWN" if price != INT64_NULL else "UNDEFINED_PRICE"
        if reason != "KNOWN":
            price = None
    else:
        reason = "UNSUPPORTED_STATISTIC"
    return dict(version=VERSION, reason=reason, price_nano=price, quantity=quantity,
                record=record, quantity_bits=value.quantity_bits)


def observation_identity(row):
    from ..hourly_quality_reader import IDENTITY
    return tuple(row[k] for k in (*IDENTITY, "listing_activation_ns", "expiration_ns"))


class AsOfStatisticsV2:
    def __init__(self, records, *, definitions, economics):
        index = DefinitionIndex(definitions)
        self.events = defaultdict(list)
        self.unresolved = defaultdict(list)
        grouped = defaultdict(list)
        self.raw = tuple(islice((v for v in records if v.record.stat_type in TYPES), 1_000_001))
        if len(self.raw) > 1_000_000:
            raise ContractError("Optional-statistics batch exceeds one million records")
        positions = [(v.record.source_file_path, v.record.row_ordinal) for v in self.raw]
        if len(set(positions)) != len(positions):
            raise ContractError("Statistics batch repeats a source position")
        self.widths = {}
        for value in self.raw:
            r = value.record
            if r.stat_type not in TYPES:
                continue
            try:
                # The provider's receipt date scopes reusable numeric IDs.
                bar = ProviderBar(dataset=r.dataset, market=r.market, publisher_id=r.publisher_id,
                    instrument_id=r.instrument_id, event_at_ns=r.ts_recv_ns,
                    open_nano=0, high_nano=0, low_nano=0, close_nano=0, volume=0,
                    source_release_id=r.source_release_id, source_manifest_sha256=r.source_manifest_sha256,
                    source_file_path=r.source_file_path, source_file_sha256=r.source_file_sha256,
                    row_sha256=r.row_sha256)
                # datetime has microsecond resolution; the index still gates its
                # definition baseline against bar.event_at_ns, at exact receipt ns.
                definition = index.resolve(bar, decision_at=ns_to_datetime(r.ts_recv_ns+999, "statistics receipt"))
                units = economics.resolve(r.market, definition)
                identity = (r.market, r.publisher_id, r.instrument_id, definition.raw_symbol,
                            definition.currency, definition.min_price_increment_nano,
                            int(units.point_value * 1_000_000_000), definition.activation_ns, definition.expiration_ns)
            except ContractError:
                self.unresolved[(r.market, r.publisher_id, r.instrument_id, r.stat_type)].append(r)
                continue
            self.widths[r.record_id] = value.quantity_bits
            grouped[(identity, r.stat_type, r.ts_ref_ns, r.flags if r.stat_type == "SETTLEMENT_PRICE" else 0)].append(r)
        for (identity, kind, reference, flags), values in grouped.items():
            for received, equal in groupby(sorted(values, key=lambda r: r.ts_recv_ns), lambda r: r.ts_recv_ns):
                equal = tuple(equal)
                try:
                    chosen = _normalize_equal_receive_rows(equal, kind="statistics")[0]
                    if len({self.widths[r.record_id] for r in equal}) != 1:
                        raise ContractError("Conflicting decoded widths")
                    projection = interpret_statistic(DecodedStatisticV2(chosen, self.widths[chosen.record_id]))
                except ContractError:
                    projection = dict(version=VERSION, reason="CONFLICTING_RECORDS", price_nano=None,
                                      quantity=None, record=None, conflicting_records=equal)
                self.events[(identity, kind)].append((received, reference, flags, projection))
        for events in self.events.values():
            events.sort(key=lambda event: event[:3])

    def batch_asof(self, requests):
        """Backward receipt-time join; declarations qualify only this inactive candidate.

        Requests contain a verified observation, decision_at_ns, segment_start_ns,
        and optionally max_age_ns (by reference time) and settlement_flags (exact
        stat_flags, not a guessed daily settlement). Results retain raw provenance.
        """
        requests = tuple(requests)
        results = [{kind: None for kind in TYPES} for _ in requests]
        groups = defaultdict(list)
        for i, request in enumerate(requests):
            decision, start = request["decision_at_ns"], request["segment_start_ns"]
            age, flags = request.get("max_age_ns"), request.get("settlement_flags")
            if (type(decision) is not int or type(start) is not int or not 0 <= start <= decision
                    or age is not None and (type(age) is not int or age < 0)
                    or flags is not None and (type(flags) is not int or not 0 <= flags <= 15)):
                raise ContractError("Invalid statistics decision, segment, age, or settlement declaration")
            groups[observation_identity(request["observation"])].append((decision, i, request))
        for identity, queries in groups.items():
            for kind in TYPES:
                events = iter(self.events.get((identity, kind), ()))
                pending = next(events, None)
                current = {}
                for decision, i, request in sorted(queries, key=lambda q: q[:2]):
                    while pending is not None and pending[0] <= decision:
                        current[pending[1:3]] = pending
                        pending = next(events, None)
                    flags = request.get("settlement_flags")
                    candidates = [e for e in current.values() if request["segment_start_ns"] <= e[0]
                                  and 0 < e[1] <= decision and
                                  (kind != "SETTLEMENT_PRICE" or flags is None or e[2] == flags
                                   or e[3]["reason"] in ("DELETED", "UNKNOWN_UPDATE_ACTION", "CONFLICTING_RECORDS"))]
                    result = dict(version=VERSION, reason="NO_ASOF_VALUE", price_nano=None, quantity=None,
                                  receipt_time_ns=None, reference_time_ns=None, receipt_age_ns=None,
                                  age_ns=None, candidate_feature_eligible=False, feature_eligible=False,
                                  research_authority=False, record=None)
                    if candidates:
                        latest_ref = max(e[1] for e in candidates)
                        selected = [e for e in candidates if e[1] == latest_ref]
                        event = max(selected, key=lambda e: e[0])
                        # A deletion or ambiguous update at this reference blocks
                        # every settlement variant; never resurrect an old price
                        # merely because the delete used another flags value.
                        blockers = [e for e in current.values() if e[1] == latest_ref
                                    and e[0] >= event[0] and e[3]["reason"] in
                                    ("DELETED", "UNKNOWN_UPDATE_ACTION", "CONFLICTING_RECORDS")]
                        if blockers:
                            event = max(blockers, key=lambda e:e[0])
                        result.update(event[3], receipt_time_ns=event[0], reference_time_ns=event[1],
                                      receipt_age_ns=decision-event[0], age_ns=decision-event[1], flags=event[2])
                        if kind == "SETTLEMENT_PRICE" and flags is None and len(selected) > 1:
                            result.update(reason="SETTLEMENT_TYPE_UNDECLARED", price_nano=None)
                        age = request.get("max_age_ns")
                        if result["reason"] == "KNOWN":
                            if age is None or kind == "SETTLEMENT_PRICE" and flags is None:
                                result["reason"] = "DIAGNOSTIC_ONLY_MISSING_DECLARATION"
                            elif result["age_ns"] > age:
                                result["reason"] = "STALE"
                            else:
                                result["candidate_feature_eligible"] = True
                    unknown = self.unresolved.get((*identity[:3], kind), ())
                    if any(request["segment_start_ns"] <= r.ts_recv_ns <= decision
                           and r.ts_recv_ns >= (result["receipt_time_ns"] or 0) for r in unknown):
                        result.update(reason="IDENTITY_UNRESOLVED", price_nano=None, quantity=None,
                                      candidate_feature_eligible=False)
                    results[i][kind] = result
        return results
