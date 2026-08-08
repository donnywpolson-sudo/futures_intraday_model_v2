"""Pre-data decision-validity remediation for the cash-open mechanism.

The functions here are metadata and synthetic-mechanics only.  They preserve
the completed forensic evidence, correct dependent failure semantics, derive
folds only from explicitly checkpoint-eligible sessions, and route future
source access through the authoritative active catalog resolver.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .active_data_view import resolve as resolve_active_view
from .active_data_view import validate_catalog
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


YEARS = tuple(range(2018, 2023))
INITIAL_TRAINING_SESSIONS = 504
OUTER_TEST_SESSIONS = 63
OUTER_FOLDS = 8
EMBARGO_SESSIONS = 1
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")
FORENSIC_EVIDENCE_PATH = Path(
    "state/unpublished_evidence/cash_open_impulse_dependency_forensics_v2/"
    "fc59dd719820964ffc0d270307f62588acd4f1ca51ef982a4436fbc969d5c04a/"
    "dependency_forensics.json"
)
FORENSIC_EVIDENCE_SHA256 = (
    "c8e4b05849003f02f68e867c981414cd5910b4cb105c333d4a55eef8a6553ccb"
)
DEPENDENT_ENTRY_REASON = "ENTRY_NOT_AFTER_DECISION"
CORRECTED_DEPENDENT_REASON = "DECISION_UNAVAILABLE_DUE_TO_FEATURE_GAP"
FEATURE_BLOCKING_REASONS = frozenset(
    {
        "MISSING_MINUTE",
        "NON_EXECUTABLE_DISPOSITION",
        "DUPLICATE_EXECUTABLE_MINUTE",
        "LATE_AVAILABILITY",
        "MISSING_IDENTITY",
        "IDENTITY_CHANGE",
    }
)


def _read_canonical(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is unreadable") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return value


def verify_preserved_forensic_evidence(root: Path) -> str:
    path = root / FORENSIC_EVIDENCE_PATH
    if sha256_file(path) != FORENSIC_EVIDENCE_SHA256:
        raise IntegrityError("completed forensic evidence changed")
    evidence = _read_canonical(path, description="completed forensic evidence")
    report_id = evidence.get("report_id")
    if not isinstance(report_id, str):
        raise IntegrityError("completed forensic evidence lacks report identity")
    core = {key: value for key, value in evidence.items() if key != "report_id"}
    if sha256_json(core) != report_id:
        raise IntegrityError("completed forensic evidence self-hash changed")
    return report_id


def correct_dependent_timing_failures(
    checkpoint: Mapping[str, object],
) -> dict[str, object]:
    """Relabel a missing decision; retain genuine entry-order violations."""

    failures = checkpoint.get("failures")
    if not isinstance(failures, list) or any(not isinstance(item, Mapping) for item in failures):
        raise IntegrityError("forensic checkpoint failures are malformed")
    feature_failed = any(
        item.get("role") == "FEATURE"
        and item.get("reason") in FEATURE_BLOCKING_REASONS
        for item in failures
    )
    corrected: list[dict[str, object]] = []
    replaced = False
    for raw in failures:
        item = dict(raw)
        if feature_failed and item.get("reason") == DEPENDENT_ENTRY_REASON:
            if not replaced:
                corrected.append(
                    {"role": "DECISION", "reason": CORRECTED_DEPENDENT_REASON}
                )
                replaced = True
            continue
        corrected.append(item)
    result = dict(checkpoint)
    result["failures"] = corrected
    result["dependent_timing_label_corrected"] = replaced
    return result


def correct_forensic_evidence_summary(evidence: Mapping[str, object]) -> dict[str, object]:
    """Derive correction counts without mutating the sealed evidence."""

    markets = evidence.get("market_results")
    if not isinstance(markets, list):
        raise IntegrityError("forensic market evidence is absent")
    checkpoint_count = corrected_count = retained_entry_count = 0
    for market in markets:
        if not isinstance(market, Mapping):
            raise IntegrityError("forensic market evidence is malformed")
        failures = market.get("active_failures")
        if not isinstance(failures, list):
            raise IntegrityError("forensic active failures are absent")
        for checkpoint in failures:
            if not isinstance(checkpoint, Mapping):
                raise IntegrityError("forensic checkpoint is malformed")
            checkpoint_count += 1
            corrected = correct_dependent_timing_failures(checkpoint)
            corrected_count += int(bool(corrected["dependent_timing_label_corrected"]))
            retained_entry_count += sum(
                item.get("reason") == DEPENDENT_ENTRY_REASON
                for item in corrected["failures"]
            )
    return {
        "failed_checkpoint_count": checkpoint_count,
        "dependent_entry_labels_reclassified": corrected_count,
        "genuine_entry_order_failures_retained": retained_entry_count,
        "corrected_reason": CORRECTED_DEPENDENT_REASON,
        "sealed_evidence_mutated": False,
    }


def _date_strings(values: Sequence[str], *, description: str) -> tuple[str, ...]:
    normalized = tuple(values)
    if not normalized or normalized != tuple(sorted(set(normalized))):
        raise IntegrityError(f"{description} is not unique and chronological")
    for value in normalized:
        try:
            parsed = date.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise IntegrityError(f"{description} contains an invalid date") from exc
        if parsed.year not in YEARS:
            raise IntegrityError(f"{description} leaves 2018-2022")
    return normalized


def build_checkpoint_eligible_outer_folds(
    *, eligible_sessions_by_market: Mapping[str, Sequence[str]],
    required_markets: Sequence[str],
    initial_training_sessions: int = INITIAL_TRAINING_SESSIONS,
    outer_test_sessions: int = OUTER_TEST_SESSIONS,
    outer_folds: int = OUTER_FOLDS,
    embargo_sessions: int = EMBARGO_SESSIONS,
) -> dict[str, object]:
    """Build every fold from the common eligible calendar, never date ranges."""

    markets = tuple(required_markets)
    if not markets or len(markets) != len(set(markets)):
        raise IntegrityError("required fold markets are absent or duplicated")
    if set(markets) != set(eligible_sessions_by_market):
        raise IntegrityError("fold calendar coverage differs from required markets")
    normalized = {
        market: _date_strings(
            eligible_sessions_by_market[market],
            description=f"eligible sessions for {market}",
        )
        for market in markets
    }
    common = tuple(sorted(set.intersection(*(set(values) for values in normalized.values()))))
    minimum = (
        initial_training_sessions
        + (outer_folds - 1) * outer_test_sessions
        + embargo_sessions
        + outer_test_sessions
    )
    if len(common) < minimum:
        raise IntegrityError("checkpoint-eligible calendar cannot support locked folds")
    folds: list[dict[str, object]] = []
    for fold_index in range(outer_folds):
        fit_count = initial_training_sessions + fold_index * outer_test_sessions
        fit = common[:fit_count]
        embargo = common[fit_count : fit_count + embargo_sessions]
        test = common[
            fit_count + embargo_sessions:
            fit_count + embargo_sessions + outer_test_sessions
        ]
        if (
            len(fit) != fit_count
            or len(embargo) != embargo_sessions
            or len(test) != outer_test_sessions
            or not fit[-1] < embargo[0] < test[0]
        ):
            raise IntegrityError("checkpoint-eligible fold construction failed closed")
        folds.append(
            {
                "fold_id": f"fold-{fold_index}",
                "fit_session_count": len(fit),
                "fit_start": fit[0],
                "fit_end": fit[-1],
                "embargo_sessions": list(embargo),
                "test_session_count": len(test),
                "test_start": test[0],
                "test_end": test[-1],
                "test_session_sha256": sha256_json(list(test)),
            }
        )
    return {
        "required_markets": list(markets),
        "common_checkpoint_eligible_session_count": len(common),
        "common_checkpoint_eligible_session_sha256": sha256_json(list(common)),
        "initial_training_sessions": initial_training_sessions,
        "outer_test_sessions": outer_test_sessions,
        "outer_folds": outer_folds,
        "embargo_sessions": embargo_sessions,
        "folds": folds,
    }


@dataclass(frozen=True)
class CatalogSourceDisposition:
    market: str
    year: int
    disposition: str
    parquet_path: str | None
    parquet_sha256: str | None
    reason: str | None


def census_active_catalog_metadata(
    *, root: Path, markets: Sequence[str], years: Sequence[int] = YEARS,
) -> tuple[CatalogSourceDisposition, ...]:
    """Inventory source authority without opening any historical Parquet."""

    raw = _read_canonical(root / ACTIVE_CATALOG_PATH, description="active catalog")
    validate_catalog(raw)
    entries = raw.get("entries")
    assert isinstance(entries, list)
    by_key = {
        (str(item["market"]), int(item["year"])): item
        for item in entries if isinstance(item, Mapping)
    }
    results: list[CatalogSourceDisposition] = []
    for market in markets:
        for year in years:
            item = by_key.get((market, year))
            if item is None:
                results.append(CatalogSourceDisposition(
                    market, year, "ABSENT_FROM_ACTIVE_CATALOG", None, None,
                    "MARKET_YEAR_NOT_CATALOGED",
                ))
                continue
            if item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE":
                results.append(CatalogSourceDisposition(
                    market, year, str(item.get("disposition")), None, None,
                    str(item.get("reason")),
                ))
                continue
            if (
                item.get("selection_eligible") is not True
                or not isinstance(item.get("parquet_path"), str)
                or not isinstance(item.get("parquet_sha256"), str)
            ):
                raise IntegrityError("active discovery source is not selection-authorized")
            results.append(CatalogSourceDisposition(
                market, year, "RESOLVABLE_FROM_ACTIVE_CATALOG",
                str(item["parquet_path"]), str(item["parquet_sha256"]), None,
            ))
    return tuple(results)


def resolve_active_source_for_authorized_census(
    *, root: Path, market: str, year: int,
) -> Path:
    """Only future row-authorized census code may call the canonical resolver."""

    if year not in YEARS:
        raise UnauthorizedOperation("source census leaves discovery years")
    return resolve_active_view(
        repository_root=root,
        market=market,
        year=year,
        purpose="SELECTION",
        require_status=False,
    )


def validate_41_market_plan(root: Path, plan_path: Path) -> dict[str, object]:
    plan = _read_canonical(root / plan_path, description="41-market census plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan.get("plan_id") != sha256_json(core):
        raise IntegrityError("41-market census plan self-hash changed")
    if (
        plan.get("execution_allowed") is not False
        or plan.get("historical_row_read_allowed") is not False
        or plan.get("calendar_coverage_gate") != "FAIL_CLOSED_37_MARKETS_UNVERIFIED"
    ):
        raise UnauthorizedOperation("41-market census preparation is not fail-closed")
    bindings = plan.get("bindings")
    if not isinstance(bindings, Mapping) or any(
        sha256_file(root / str(path)) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("41-market census plan binding changed")
    return plan
