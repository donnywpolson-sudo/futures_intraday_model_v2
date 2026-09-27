# Inactive reference/calendar integration design v1

Status: **IN_MEMORY_AUDITS_IMPLEMENTED_AND_SYNTHETIC_TESTED — NOT HISTORICALLY CERTIFIED OR ACTIVATED**.
Scope: ES, NQ, CL and GC, 2018-01-01 through 2023-01-01 exclusive.
This design uses owned reference sources to validate the frozen v7 input
contract. It grants no observation read, publication, provider purchase, model
execution, holdout/forward access or change to an active policy.

The first implementation is a source-join and readiness check. It does not
introduce a new calendar inferred from future observations, a status-based
forecast filter, new features, or a new contract selector. A failed join remains
a readiness failure; it is not repaired by removing dates or opportunities.

## Evidence and unchanged research identity

The exact sixty DBNs and sixty sidecars are listed in
[the owned-input assessment plan](../configs/rolling_hour_owned_inputs_assessment_v1.json),
plan ID `e1355af96afde2647bc750598cbfa8345a7aaca505d8e896b68b694dc618f0c2`.
The [complete report](../state/unpublished_evidence/rolling_hour_research_v1/owned_inputs_assessment_v1/report.json)
and [independent inspection](../state/unpublished_evidence/rolling_hour_research_v1/owned_inputs_assessment_v1/independent_inspection.json)
establish 3,083,406 definition, 3,360,593 statistics and 38,181 status records,
with all sixty files nonempty. Six HALT records and historical trading/close
states exist. Message counts do not establish continuous calendar coverage.

Retain [v7](../configs/rolling_hour_research_v7.json), bundle
`b698ffc863bf9650f99717e4c69176718dde285f429e12d4e477470b1ec7ae73`:
fixed-hour continuous labels, minute forecast cadence, 5/10/16-feature packs,
Ridge/XGBoost candidates, normalized equal-date training, penalties, training-only
transforms, chronological folds, purging/maturity, and 252/505 observed-date
support. Fitting loss, profit-based inner selection, descriptive outer
date-balanced MSE and statistical/economic qualification remain separate.
The final candidate is not selected using outer-test performance.

No new learning-procedure charge is created merely by this design or by
validating the existing source contract. If implementation changes eligibility,
calendar-dependent features, contract selection, candidate behavior or economic
replay, stop the v7-compatible path. Identify and account for that successor
before historical results, preserving all earlier identities and charges.

## Reuse the existing owners

| Concern | Existing owner and bounded use |
|---|---|
| Exact sources | `causal_observation_canary._binding`, `_query_contract`, and the pinned `foundation.decoder`; use only the plan's exact pairs and query contracts under a fresh applicable receipt. |
| Physical price format | `causal_observation_parquet` decodes binary digests and reconstructs logical observations. Do not validate physical bytes as logical strings. |
| Instrument identity | `foundation.identity.DefinitionIndex`; replay exact ADD/MODIFY/DELETE records using the bar's daily publisher/instrument identity. |
| Status | Pure `foundation.market_state.AsOfStatusLedger` for as-received decision audits. Do not run retired foundation publication/orchestration paths. |
| Statistics | Pure `AsOfStatisticsLedger` and `StatisticsRolePolicy`; retain NEW/DELETE semantics and diagnostic-only roles. |
| Scheduled calendar | Existing `CalendarSession` and `VerifiedExchangeCalendar`, with real source receipts and independently verified publication evidence. |
| Research handoff | Extend checks around `rolling_hour_source.adapt_source_sessions`; keep its exact dates, source/calendar identities, complete-minute and quote-join requirements. |

Use one small in-memory join function beside the current adapter, with synthetic
tests beside its existing tests. The source-access caller supplies certified
logical records; the join function must not become a reader or certificate
issuer. Persist its audit using existing canonical report patterns. Do not add
a second calendar framework, general event bus, source discovery fallback,
new model runner or gateway bypass.

## Time and identity rules

1. **Status/statistics visibility uses `ts_recv <= decision_time`.** Reuse the
   existing ledgers' capture-receive ordering and equality convention. Event
   time and statistics `ts_ref` are audit/reference fields, not permission to
   backfill availability. Retain receive-before-event cases without shifting
   timestamps, taking absolute delays or tuning a skew tolerance. This models
   availability at the recorded capture point, not broker/execution latency.
2. **Definitions remain available by bar start.** Preserve `DefinitionIndex`'s
   same-UTC-day key, update order, lifecycle and outright-contract checks. A
   definition first received after the bar start cannot validate that bar,
   even if the later forecast or final assessment has seen it.
3. **Use full keys.** Dataset, publisher, instrument, UTC identity date and
   applicable contract/market must agree. Definition parents contain many
   FUTURE_SPREAD records; do not treat those as eligible outrights. Statistics
   and status use `*.v.0`, while definitions use `*.FUT`.
4. **Do not invent ordering or state.** Reuse the ledger's source ordinal and
   equal-receive conflict rules. Unknown actions, conflicting records and
   unresolved identities remain explicit. No state carries across a UTC
   identity reset, another contract or a source gap without certified evidence.
   Silence between event-driven messages is not itself a proven source gap;
   do not invent a status time-to-live to force additional exclusions.
   Snapshots become available at their recorded receive time, never retroactively
   at an older event time. Their timestamp construction/provenance must be
   verified in a future source certificate.
5. **Do not equate identity with selection.** Matching a `*.v.0` record to an
   outright proves which instrument appears, not why it was selected at that
   time. Bind the exact historical continuous-roll rule and its knowability,
   and verify the price/reference mapping across every required window. Do not
   reconstruct a cross-contract volume ranking from selected-contract statistics
   alone, choose whichever contract has fewer gaps, or switch to `.c.0`.
6. **Keep time zones explicit.** Store event/capture timestamps in UTC; display
   new research schedules in America/Los_Angeles. Preserve exchange-local and
   immutable project trade-date definitions using their declared DST-aware zone.
   UTC message dates are not exchange sessions or observed fitting dates.

The assessment's same-day definition-presence count is only a preliminary join
check. It did not replay all revisions, resolve every price-window contract,
verify snapshots, or establish original publication times. These are acceptance
requirements, not already-passed certifications.

## Scheduled time and observed state must remain distinct

V7's context features calculate session quartiles from both scheduled interval
start and end. Its forecast grid also uses the scheduled end. An unexpected
close or halt received later must not shorten the earlier denominator or remove
earlier forecast opportunities. This would introduce future information.

Keep the complete planned calendar supplied independently of observed prices
and status. For v7, the relevant schedule and contract-selection evidence must
be known before the session opens, as required by the current adapter. Each
publication must identify its applicable products/dates, version, provenance
and known-at time. A modern download of a historically applicable schedule
does not alone prove that historical traders knew it.

Compute the status audit separately at the scheduled decision and entry instants.
Record the ledger result and source record IDs without changing the scheduled
grid. Preserve unknown/not-trading states and expected-minute discrepancies in
the report. A state conflict cannot be used to certify an input as compatible;
the scoped readiness result remains blocked until the conflict is explained
under the declared rules. Never make the certificate pass by deleting that
date, dropping a losing period or filling a nonexistent bar.

A later halt may be used only at or after receipt. Fixed-hour labels and replay
still require the declared complete path and exit inputs. An unavailable exit
must not become an invented fill, an early exit or a new holding period. Report
the failure according to the frozen model/diagnostic rules. Reference audits do
not erase otherwise valid evidence, including empty audit dates and failed runs.

There are two current incompatibilities to preserve visibly:

- `exchange_calendar_policy.json` labels historical DBN evidence empirical and
  makes no official-calendar claim, whereas the v7 adapter requires certified
  original-publication calendar evidence. Empirical status reconstruction alone
  cannot satisfy that field. Do not label it `CERTIFIED_ORIGINAL_PUBLICATION`.
- `status_research_scope_policy.json` starts in 2025 based on schema launch.
  The owned assessment disproves an absence claim for 2018-2022, but does not
  activate an earlier research interval. An inactive successor must replace
  that rationale with exact interval/identity/clock coverage certification,
  not simply change 2025 to 2018 or treat every nonempty file as eligible.

The active calendar pointer references a manifest absent at its current path:
`2b1ec84769b655dd0e73789522126aa1650ae2209210dd5c4c49d0ae67bc423c`, expected file
SHA-256 `51ad8a036eadcb9f11831ede6dcb65575c2e796deb4bc03ad27f87c3fb7d25a7`.
Its exact bytes were subsequently located in
`C:\Users\donny\Desktop\futures_intraday_model_v2-preserved-20260825T233500Z`.
The manifest serves 2026 onward and cannot satisfy v7's 2018-2022 requirement.
The preserved historical checkpoint calendar and thirty original reference files
were also hash-verified, but do not certify full prepublished Globex intervals.
See the [owned-record reconciliation](../state/unpublished_evidence/rolling_hour_research_v1/owned_records_reconciliation_v1/task_report.md)
for recovered locations, timing evidence and remaining gaps. Nothing was restored
or activated; different bytes must never be regenerated under an accepted ID.

## Handoff and failure contract

Produce one per-market/date join audit with the existing source, calendar,
contract and opportunity identities; matched definition/status/statistics row
IDs; their known-at times; expected versus observed minute counts; and explicit
unresolved/conflict reasons. Use the current adapter's binding fields only when
the certificate contents actually support them. A caller flag or a newly
computed report hash is not a substitute for a verified certificate.

Preserve every declared date and scheduled opportunity in the audit. The current
dataset rejects incomplete features/outcomes; this design does not weaken that
rule. No fitting array is built from a failed join. If usable historical data
requires status-based abstention or a different missing-label policy, that is a
material research successor, not an adapter cleanup. It needs its own explicit
predeclared eligibility, scoring denominators, accounting and full synthetic run.

Statistics retain their existing diagnostic roles. They do not enter the feature
matrix, replace the fixed-hour label, or become quote/cost observations.
HIGHEST_BID/LOWEST_OFFER statistics do not certify the historical bid/ask/size
stream required by the profit selector. No model fitting depends on repairing
profitability inference, but its failed calibration block remains intact.

## Implementation and acceptance order

1. Implement the in-memory audits using the existing ledgers and adapter shape.
   Add no real-data reader. Keep source/certificate and calendar evidence
   requirements independent so missing historical publications do not prevent
   synthetic implementation.
2. Verify synthetic temporal cases: a later halt cannot affect an earlier
   decision or quartile; event-old/receive-late records are not backward-filled;
   receive-before-event behavior matches the existing ledger; equal-time
   conflicts fail; UTC resets and contract rolls cannot borrow another key;
   definition DELETE/MODIFY applies in order; FUTURE_SPREAD cannot satisfy an
   outright identity; deleted/unknown statistics remain diagnostics.
3. Verify synthetic handoff cases: a retrospective-only calendar is rejected;
   a prepublished early close is reproducible; missing publications/quotes and
   missing expected minutes remain blocked; empty dates stay in audit calendars;
   a future halt never changes earlier opportunity IDs or feature values.
4. Validate v7 bindings and confirm successful compatible inputs reproduce
   existing predictions/scores exactly. If this cannot hold, stop and identify
   the material successor. Only that successor would require refreezing affected
   procedure identities and rerunning complete four-market synthetic acceptance.
5. Before a new source-readiness execution, freeze its exact files, any required
   boundary lookback, outputs and limits and obtain the applicable bounded read
   authority. The sixty-file assessment receipt is consumed. Do not quietly
   reopen it, extend to 2017/2023, or borrow holdout/forward observations.
6. Historical model registration/execution remains through the certified route
   after complete source readiness and justified historical-use accounting.
   Preserve failed selected fits, unavailable diagnostics, earlier trial charges
   and result-view history. No promotion or usefulness claim follows from joins.

Steps 1-4 are now implemented and synthetically checked in
[`audit_source_joins`](../src/futures_rebuild/rolling_hour_reference_audit.py),
beside the unchanged frozen adapter. The caller supplies its existing adapter
arguments, typed definition/status/statistics records, diagnostic roles and exact
per-family source scopes. No reader, policy activation or certificate issuer was
added. Reference failures retain all scheduled opportunities and prevent dataset
construction. The existing adapter runs only after reference checks pass; its
first structural rejection is recorded. A skipped adapter check is explicitly
unavailable, not a calendar/quote pass. Missing statistics keys are explicitly
reported and cannot become features or quote coverage.

The [34 final audit tests](../tests/test_rolling_hour_reference_audit.py) passed,
including all four markets' exact feature/prediction/score preservation and the
temporal and handoff cases above. The unchanged rolling-hour file's 29 tests also
passed. Five saved synthetic reports were independently inspected: 625 minute
joins, 15 opportunities, 30 status checks and 30 statistics checks, including the
retrospective-calendar rejection. See the
[task report](../state/unpublished_evidence/rolling_hour_research_v1/reference_join_audit_v1/task_report.md)
for attempts, checks and limitations. V7 binding validation passed; no learning
procedure changed or new configuration charge arose. The full nested acceptance
bundle remains prior evidence for unchanged v7, not a new historical run.

Exact historical calendar-publication evidence, continuous-roll knowability,
compatible quote/cost inputs and research-history accounting remain execution
dependencies. No protected observations were reopened and no replacement
definition/statistics/status acquisition is proposed. CURRENT_WORKFLOW.md,
PROJECT_OUTLINE.md and README.md: NO_CHANGE because no active stage changed.
