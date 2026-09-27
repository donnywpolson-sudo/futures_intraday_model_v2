# Current workflow

This is the day-to-day guide for this repository. When another operational
document differs, this guide controls normal-work procedure; research evidence
requirements still apply to real research.

## How to read this project

One alpha-discovery pipeline supports separately declared experiments. Its
reusable workflow is **propose -> declare -> check feasibility/readiness ->
validate mechanics -> approve and run -> assess evidence -> close or qualify**.
A failure remains in the history; another experiment needs a justified question,
not a renamed retry. No historical operation is currently enabled by the shared
discovery allowlist (`configs/approved_discovery_batch.json`: `operations=[]`).

| Layer | What it contains | What changes |
| --- | --- | --- |
| Research safeguards | Point-in-time inputs, chronological evaluation, training-only selection, prior-search accounting, missingness, budgets and protected-data access | Stable requirements; evidence must be refreshed when affected bindings change |
| Shared infrastructure | Certified observations, feature calculations, fitting/evaluation tools, the guarded execution path, experiment history and reporting | Reused; extended only for a demonstrated requirement |
| Experiment choices | Hypothesis, claim, markets, dates, decision schedule, target, features, model/rule, benchmarks and success criteria | Declared and frozen per experiment; dates remain within the permitted research boundary |

The five/seven-feature L0 design is one experiment definition, not the mandatory
feature set for new hypotheses. Predictive, economic and diagnostic studies
have different claim requirements. Synthetic correctness is not evidence of alpha.

## Experiment board

This is the single owner-facing summary of current settings and operational
status. The detailed sections below retain supporting and historical context;
exact bound declarations and active configurations remain authoritative.

| Experiment | Software availability | Historical execution | Scientific outcome | Next blocker or disposition |
| --- | --- | --- | --- | --- |
| ES intraday mean reversion | Minute campaign integration completed; v6 bounded historical route completed once and temporary controls were restored; no adopted inference procedure | Frozen displacement ablation complete (48/48 descriptive fits) and frozen A-D scale-in/partial-exit campaign complete (16,756 common opportunities; 67,024 policy paths); both receipts consumed and inactive | Explicit displacement input did not improve pooled equal-date MSE; separately, every pooled A-D policy/horizon equal-date modeled net mean was negative under the frozen hypothetical execution/cost assumptions; both exact candidates are owner-closed/not advanced; calibration and formal inferential requirements remain unmet | [Fitted-model disposition](reports/mean_reversion_baseline_v1/displacement_ablation_disposition_v1/decision.json) and [campaign disposition](reports/mean_reversion_baseline_v1/campaign_disposition_v1/decision.json); no tuning, retry, promotion, or further execution authorized |
| ES closing momentum | Fixed-rule mechanics and generated-data checks retained; no adopted historical runner | Not run | No historical finding; original inference failed synthetic calibration | Closed by owner on 2026-09-14 without historical evaluation; no further preparation or execution authorized |
| ES opening-range breakout | Rule mechanics prepared | Not run | No historical finding | Withdrawn by owner on 2026-09-22; no further preparation or execution authorized |
| ES opening reversal | Not implemented | This proposal not run | Prior family closure preserved; coverage-inconclusive is not proof of economic failure | Closed to incremental rescue |
| Four-market retrospective predictive screen | Adopted screen implementation | Complete: 96 fits, 32 market-windows | All four markets unsupported; no economic claim | Closed result; no sensitivity or time-bucket rescue |
| ES-to-NQ predictive screen | Adopted transfer implementation | Complete: 16 fits, eight windows | No supported predictive lead; owner closed study | Closed result; no retry or further fitting authorized |
| Original L0 trading-policy experiment | Preparation and shared numerical/reporting software exist | No historical fitting/evaluation or registration | No L0 historical result | Calendar, execution references, fees and source admission unresolved |

The frozen ES mean-reversion displacement fitted-model candidate remains closed
and not advanced on descriptive evidence. The separate frozen A-D path-dependent
campaign was historically evaluated once under hypothetical execution/cost
assumptions and is owner-closed/not advanced because every pooled policy/horizon
equal-date modeled net mean was negative. This exact campaign is no longer
untested; broader variants, prospective behavior, population profitability and
account safety remain unestablished. Confirmatory calibration, formal inferential
requirements and economic thresholds remain unmet or deferred as declared. No
profitable model or further execution is authorized. Opening-range breakout was
withdrawn by the owner without historical evaluation.

### Closed ES closing-momentum preparation: retained settings

Source: the local [prediction scope](state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/prediction_scope.json)
and [readiness findings](state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/REPORT.md).
The owner [closed this preparation on 2026-09-14](state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/closure.json).
These settings remain historical context, not an active work plan or permission
to execute. Closure is an owner disposition, not a finding of no predictive effect.

| Choice | Prepared setting |
| --- | --- |
| Hypothesis and claim | Prior-full-cash-close displacement predicts ES closing-window continuation; PREDICTIVE, with costs diagnostic only |
| Markets and dates | ES; exposed 2018-2022 development history; eight fixed 63-weekday audit windows, 504 dates from 2021-01-26 through 2022-12-30 |
| Decision schedule | 12:30:05 Pacific on a supported FULL cash session; exact completed-bar and recorded eligibility required |
| Features / signal | Sign of the 12:29 bar close minus the same-contract prior eligible FULL cash session's 12:59 bar close; unknown intervening sessions block selection |
| Model / rule | Fixed sign rule, no model fitting, feature search or automatic use of the L0 feature packs |
| Target | Signed change between same-contract 12:31 and 13:00 opening-price references, in ticks; these are not demonstrated fills |
| Benchmarks | Zero signed movement and matched always-long; matched always-short is diagnostic |
| Success criteria | **Unresolved for a valid historical support claim.** The original mean-tick bootstrap failed calibration. Its frozen proposal used six claims, primary block 20, 12,000 draws and six positive windows out of eight; support floors alone cannot establish success. Direction-only inference is unadopted and changes the question |
| Costs | Assumed 3/6/12-tick allowances plus a $10 round-trip fee, diagnostic only; no economic pass/fail gate |
| Disposition | Closed without historical evaluation. Endpoint/inference, calendar availability and selected-contract limitations remain unresolved; further work requires a new owner request |

The completed four-market screen used seven hour-context features, primary Ridge
1.0 (0.1/10 sensitivities), a one-hour tick-change target and zero/training-mean
benchmarks. The completed transfer screen used five-minute ES/NQ changes, OLS,
a following-15-minute NQ target and NQ-only/zero/training-mean benchmarks. Original
L0 instead compares five- and seven-feature packs across Ridge 0.1/1/10 and
XGBoost depths 2/3, selecting trading policies under adverse assumed costs.
These are distinct frozen experiments, not interchangeable current settings.

### Infrastructure and audit status

The published native foundations have **41 markets and 1,817 annual releases**:
609 minute, 604 hourly and 604 daily releases. Their observation counts remain
**139,892,740 minute, 3,023,365 hourly and 167,609 daily**. The minute catalog
also retains eight protected, unmaterialized SR3 entries. The
[verified annual migration](state/unpublished_evidence/annual_foundation_migration_v1/migration_v1_validation_retry_v1/COMPLETION.md) records full five-table equivalence,
population accounting, unchanged permission populations and passing default readers.
Old catalog-bound research approvals require fresh preparation for this view.
The installed discovery infrastructure retains history and budgets, but its empty
operation allowlist enables no research campaign. Reporting does not establish
alpha or trading authority.

**Minute model-input safeguards: ACTIVE for newly declared studies.** Use
`futures_rebuild.model_input_admission.read_windows(root, market=..., year=...,
requests=..., purpose=...)` under the
[active input policy](configs/active_model_input_admission.json). Each request is
a `WindowRequest` from that module, with half-open UTC nanosecond bounds and an
explicit decision time. Pass the complete feature lookback or target path,
including its exit reference. The entry point checks the active policy and the
existing catalog's permission for the requested purpose before returning data.
Only `ELIGIBLE` feature windows and `COMPLETE` target paths may be used;
retain every refused/`INDETERMINATE` result in coverage/outcome reporting.

Unknown gaps, the disputed 6N/6S intervals, identity/economics changes and
premature inputs fail closed. A full lookback must restart after a gap or roll.
Prices remain unadjusted, the five-second availability delay remains an explicit
assumption, and entry references must follow the signal. No missing prices or
calendar closures were manufactured. Open interest and settlement use the corrected
V2 interpretation only as optional diagnostics; no statistics feature permission
or status-dependent research was activated. Calls remain bounded to one annual
release; unavailable history stays unavailable.

The [activation checks](state/unpublished_evidence/free_model_input_remediation_v1/activation_v1/REPORT.md)
cover this shared entry point. Closed experiments, their frozen readers, the
underlying raw/audit resolvers and legacy replay remain unchanged. New minute-input
studies must use this entry point; activation does not start or authorize a study.

**Hourly data-foundation phase: COMPLETE.** The [active source-admission policy](configs/active_hourly_source_admission.json)
and [completion report](state/unpublished_evidence/hourly_source_admission_v1/ddc4fdf707a4ed253735d609a2f3239f89f94da21084f691084a8b18b2b2c533/COMPLETION.md)
record full verification of 41 markets, 6,862 monthly entries and 3,023,365 native
hourly observations. There are 3,023,362 quality-accepted native rows, three GF
exclusions and 21 unavailable months. The 2,179 separately derived ZQ candidates
remain audit-only; native reads never substitute them automatically.

The [completed exception investigation](state/unpublished_evidence/hourly_exception_recovery_v1/REPORT.md)
classifies five empty months as pre-listing absence and sixteen as unresolved
source coverage; all 21 remain unavailable. The additional GF parent-source
check found no exact-hour corroboration, so all three exclusions remain.
Broader derived-ZQ admission was assessed and remains unsupported. The offline
validation passed 123 labeled reader checks and all three raw-path guards;
active pointers and permissions were unchanged.

Use `futures_rebuild.hourly_source_admission.HourlySourceReader(root).read_window`
with `market`, aligned half-open `start_ns`/`end_ns`, `as_of_ns`, and `purpose`;
times are UTC nanoseconds. Native is the default; `source="minute_derived"` is
explicit. The reader enforces the minute cohort permission ceiling across the
whole window, plus the existing quality, availability, gap and contract/unit
gates. Research execution remains separately governed; this source admission
changes no research-operation allowlist.

To prepare a repeat audit against the active chain, run
`.venv\Scripts\python.exe -m futures_rebuild.hourly_foundation_audit` from the
repository root. This prepares a fresh bound attempt only; an approved task runs
the existing supervisor. The completed audit passed all five-table/source checks,
the full row-policy census, 79 selected public windows and all three blocked raw
hourly path routes; those window checks passed again after activation. The
five-second availability assumption and unknown official-calendar limitations
remain explicit. Model-specific eligibility is `NOT_DECLARED` and is not an
unfinished requirement of this foundation phase.

**Daily foundation complete with explicit coverage limitations.** The
[active daily admission policy](configs/active_daily_source_admission.json) and
[completion report](state/unpublished_evidence/daily_identity_recovery_v1/authorization_v2/COMPLETION.md)
record coordinated publication of 41 markets and 6,862 monthly entries:
167,639 source rows reconcile to 167,609 normalized observations, zero remaining
definition quarantines and 30 availability exclusions. All five daily tables
contain 167,609 rows; 21 unavailable months remain unreadable.

The 12,583 previously quarantined rows were recovered under the approved
completed-native-day identity rule, corroborated by accepted native hourly bars.
This establishes identity for the completed bar; it does not claim that later
definitions were known at midnight. Native OHLCV and definition receive times
remain unchanged. Two independent reconstruction passes and the complete
publication census passed; prior releases and rollback evidence remain retained.

Quality accounting distinguishes 12,438 full hourly agreements, 155,168 agreements
with the observed hourly references and three excluded-hourly-reference cases.
Agreement does not establish complete market coverage or universal permission.
The three excluded-reference cases remain audit-only. Permitted uses are enforced
by market, date and cohort, bounded by
the minute catalog's permissions throughout each requested window.

Use `futures_rebuild.daily_source_admission.DailySourceReader(root).read_window`
with keyword-only `market`, `start_ns`, `end_ns`, `as_of_ns`,
`source="native"` and `purpose="LINEAGE_AUDIT"` (the latter two are defaults).
Windows are complete, half-open UTC days in nanoseconds. The reader returns
quality-labeled rows and enforces availability, missing days and contract/unit
boundaries. Derived daily sources are unsupported; raw daily path routes are
blocked. `permitted_uses` reports policy only and does not establish readability.

To prepare a repeat daily audit, run
`.venv\Scripts\python.exe -m futures_rebuild.daily_admission_audit` from the
repository root. This creates a bound preparation record only; an approved task
separately invokes its supervisor. The completed publication retained the full
census and passed 165 default daily cases, 79 hourly cases, 41 minute metadata
access checks and all daily/hourly raw-path guards. Hourly successor bindings
preserve its existing permissions, GF exclusions and explicit audit-only derived
ZQ behavior. Provider UTC-day semantics, the assumed five-second lag and unknown
official calendars remain explicit. This source admission changes no research
operation allowlist; model-specific eligibility remains `NOT_DECLARED`.

**Hourly enforcement: fixed and activated.** The
[enforcement completion report](state/unpublished_evidence/hourly_enforcement_repair_v1/certified_successor_v2/COMPLETION.md)
records passing preactivation and default-reader checks. All public hourly raw-path
routes now refuse access regardless of quality-pointer presence. Hourly
`permitted_uses` checks binding freshness before every return, including empty
and derived-source results; this query still describes policy, not readability.
The [daily recovery binding pointer](configs/active_daily_recovery_reader_binding.json)
retains the original certificate and replay evidence for these exact reader fixes.
Native payloads, source permissions and both foundation-complete statuses are unchanged.

**Feature windows: feasibility unresolved.** The prepared 41-market 2018-2022
calendar inventory is not a certified closure policy or demonstrated window gain.
Dense UTC windows and contract/unit resets remain enforced; no feature adapter
has been admitted.

**Coverage recovery: no new observations in this repair.** The three GF exclusions,
21 unavailable months per timeframe, audit-only derived ZQ and three audit-only
daily reference cases remain. The earlier 12,583 daily identity recoveries remain
admitted under their existing permissions. KE/ZQ recovery requires new supporting evidence.

**Research limitations remain:** assumed five-second availability, UTC-day rather
than exchange-session semantics, roll resets, unknown calendars and unverified
execution assumptions. This repair establishes neither executable fills nor alpha;
model-specific eligibility remains `NOT_DECLARED`.

See the [focused safeguard audit](docs/DISCOVERY_WORKFLOW_AUDIT.md) for the
reviewed path, checks, findings and exclusions. It is supporting evidence, not
another workflow authority or blanket certification of every experiment.

## Normal local work

Ask Codex to change, fix, build, implement, or complete an outcome. That request
authorizes inspection, editing, testing, non-research local artifacts, exact-path
staging, and a scoped local commit when needed to deliver the outcome. Preserve
unrelated work and never use broad staging. If an intended task path overlaps a
pre-existing user change, Codex asks once before staging or committing it. You
do not need to copy commands, hashes, plan IDs, or approval lines for ordinary
local work. A local commit never includes a push.

## Project time convention

Use Pacific Time through the DST-aware `America/Los_Angeles` zone for every
current or new research decision, schedule, chart, and user-facing time. Keep
canonical event timestamps in UTC. Exchange-local calendar rules and immutable
historical evidence may retain their original timezone because changing them
would change source meaning or rewrite past experiments; they do not define the
clock for new work.

## High-risk work

Before provider/network use, a real-data read or evaluation, publication or
active-data mutation, installation/activation, deletion/cutover,
holdout/forward access, trading/order access, or push, Codex will ask once in
plain language. The request states the scope, impact or cost, outputs, and
preservation/rollback boundary. Replying `Approve` is sufficient.
Include an approximate expected duration and its basis, separately from the hard
runtime limit; mark uncertain estimates rather than presenting them as measured.

## Certified final-evaluation boundary

The active Alpha ladder seals the research-selection-pristine **Final Sealed 252-Session Holdout** under successor manifest `ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549`, covering trade dates 2025-07-14 through 2026-07-13. Its ordered-session identity exactly reproduces the declared historical sequence. The unrecoverable identity `0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1` remains historical evidence and was not reused. Authority is resolved and durable, but the holdout remains unread: neither holdout nor forward value access is authorized.

## Current research status

The [bounded discovery infrastructure is installed](state/unpublished_evidence/discovery_loop_v1/activation_v1/REPORT.md).
The gateway now requires the shared study/history and budget binding before
covered research operations. The installed operation allowlist is empty:
activation grants no research execution, confirmation, forward access or trading.
The persistent history retains ES/NQ's completed unsupported evaluation and its
earlier failed/replacement attempts. Qualification remains statistically blocked;
each next batch needs a new scientific rationale and owner-reviewed scope.

The owner closed the ES closing-momentum preparation on 2026-09-14 without
historical evaluation. No further preparation or execution is authorized for
this closed study; no replacement preparation is selected. The original plans,
synthetic findings, source evidence and all three hypotheses/six comparison
slots remain preserved. The following paragraphs record its preparation history.

The owner previously selected the three ES session-rule drafts for local preparation.
[Closing momentum and opening-range breakout now have synthetic mechanics](state/unpublished_evidence/l0_order_readiness_v1/es_session_rules_preparation_v1/REPORT.md):
17 generated-data checks passed; no historical read, adoption or installation ran.
Opening reversal remains blocked by the prior overnight-reversal closure's
no-incremental-rescue requirement, despite that prior outcome being inconclusive
for coverage. All three hypotheses/six comparison slots remain recorded.
The owner chose **prediction first, costs diagnostic** for closing momentum;
breakout was later withdrawn and reversal stays closed. The installed PREDICTIVE
branch needs no economic profit/drawdown hurdles; no policy repair was necessary.
[The prerequisite checks are saved here](state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/REPORT.md).
The proposed mean-tick bootstrap failed synthetic false-positive calibration.
A direction-only alternative passed limited synthetic null checks but has weak
power for modest effects and remains an unadopted endpoint proposal. Neither
method has authority to declare this historical study supported.
The approved NYSE/ICE lookup is complete. Its 46 documented closure dates and
10 early-close dates are now [integrated as inactive source-linked evidence](state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/cash_calendar_integration/REPORT.md).
All 56 decision-facing exception records remain UNKNOWN because historical
availability is unresolved; unlisted dates gain no ordinary-session assumption.
NYSE cash closures grant no CME trading or termination classification.
Historical ordinary-session continuity, availability, selected-contract evidence
and valid predictive inference still prevent historical testing. No calendar
activation, method adoption, observation read or research execution occurred.

One repository, one futures research pipeline: find credible predictive and
ultimately economic value using existing historical data and free tools. Task
#1 coordinates that work; task #2 supplied the completed shared reporting,
comparison, lineage-accounting and cockpit improvements. They are not separate
research authorities or competing continuations.

| Current component | Adopted state |
| --- | --- |
| Preparation software | `configs/l0_alpha_candidate_v9.json`, bundle `7155d0b92f2754d6d82e23ab93029b7e4df6165f4c04e06a34e4f8c10b21993a` |
| Development-admission contract | `configs/l0_development_admission_v4.json`, contract `42cf1dcba6f9bf92a23c4870f2b86bf395f5ee2b955fbef4faeaf4a09f38d5ff` |
| Reporting/comparison schemas | `l0_alpha_report/2`, `l0_alpha_report_catalog/2`, `l0_alpha_comparison_view/2`; installed lifecycle integration |
| Research execution | Original L0 inactive, not historically admitted or registered; no L0 historical fitting/evaluation |

The [corrected reporting adoption receipt](state/unpublished_evidence/l0_alpha_candidate_v1/reporting_adoption_v2/task_report.md)
records the exact six-file installation for request
`015723e9bb178ac9c1ddeedbcc8298f14aa965cdb0fc5a4c30ff2997cf3de6a0`.
Reporting integration is complete; do not repeat its preparation/adoption.
The [screen access integration](state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_adoption_v1/REPORT.md) updated the current pipeline's
implementation bindings, preserving earlier artifacts and all reporting behavior.
Owner-facing name: **current pipeline**. The retrospective predictive screen is
a completed study; the experiment board above records current dispositions.
Numbered components in technical references
are revision identifiers, not separate pipelines or a master experiment version.
Browser visual correctness remains unverified.
V7/V2 and rejected/pre-adoption packets remain historical evidence. Their old
hashes and descriptions must not be refreshed to describe today's installation.

The original frozen [L0 alpha-candidate experiment](docs/L0_ALPHA_CANDIDATE.md)
is supported by that installed software without changing its scientific design.
It uses ES/NQ/CL/GC development
history from 2018-2022, an independent weekday clock from 06:30:05 to 11:59:05
Pacific, ten candidates per market, and inner selection by mean daily net
dollars under frozen adverse cost assumptions. Forecasts precede outcome
attachment; unresolved scheduled executions preserve the position and prevent
complete economic conclusions. The final model uses the last inner winner;
no-trade policies and inactive fitted models are retained.

Use `python -m futures_rebuild.l0_alpha_preparation validate` with `PYTHONPATH=src`
to check the preparation. `synthetic` runs all four markets sequentially;
`rehearsal` measures one dense market; `readiness` only describes the bounded
historical source-readiness operation. No CLI accepts approval text or executes
history. Existing trusted source admission and `CertifiedResearchGateway`
registration/execution remain required. The approved scan covered all 20 files
and 7,010,381 rows; all four markets meet the frozen training-date minima.
The current preparation and admission bindings retain the original L0 scientific requirements, including the inactive calendar
successor, exact final-support counts and nonfinancial training-mean benchmark.
Saved training-support counts are not final eligible counts or admission.
Original configurations remain preserved.
The approved metadata projection and remaining dependencies are recorded in the
[calendar/admission successor report](state/unpublished_evidence/l0_order_readiness_v1/calendar_remediation_v4/REPORT.md).
Registration still requires
passing scope-matched evidence and separate activation of the exact contract
and certificate. No L0 activation or historical registration has been created.
Missing entry/exit references remain in every market. Applicable fee evidence
and successor-specific admission remain unresolved. See the source-readiness
results in the linked experiment guide.
L1 values stay unopened pending a frozen, separately authorized execution
challenge. Confirmation is not authorized or ready; the failed statistical
qualification and all prior search accounting remain preserved.

### Adopted inactive predictive study within this pipeline

Task #1 completed calendar-reader/metadata recovery, provenance integration and
the [necessity audit](state/unpublished_evidence/l0_order_readiness_v1/necessity_audit_v1/REPORT.md).
The [corrected screen and scientific/access adoption](state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_adoption_v1/REPORT.md)
is complete: 16 final synthetic checks passed, including 96 generated fits,
operation-wide fatal stops and the installed authorization-to-synthetic-reader
path. Study `861c10553585503db65267cad6bc2516e1a817b0f47179776b4cc61a6feb9cd1`
is adopted through `configs/approved_retrospective_screen.json`, with 12 related
alternatives recorded and prior failures/exposure preserved. The original review
packet remains historical evidence. The first materialization attempt stopped at
its time limit and remains preserved. The separately authorized
[replacement materialization and reconciliation passed](state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_materialization/replacement_v1/REPORT.md):
20 unchanged partitions, 7,010,381 source rows, all 1,305 scheduled dates and
430,650 decisions per market. The validated complete manifest preserves missing
observations and exact training membership; all 32 outer populations meet the
training-date minimum. The subsequently authorized [fixed historical evaluation](state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_execution/task_v1/REPORT.md)
ran 60/96 fits and 20/32 market-windows before the output-headroom check stopped it.
ES and NQ completed their tests with `NO_SUPPORTED_EDGE_IN_SCREEN`; CL completed
four windows and GC was not run. That partial campaign and its consumed receipt
remain preserved. The subsequently approved [storage remedy and unchanged completion](state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_completion_v1/replacement/REPORT.md)
completed **96/96 fits and 32/32 windows**. All four markets returned
`NO_SUPPORTED_EDGE_IN_SCREEN`; CL/GC's small positive primary averages failed the
predefined uncertainty, consistency and sensitivity criteria. All 60 earlier
forecast/model batches and 20 scores reproduced exactly as canonical values.
This completes the same retrospective study, not independent confirmation.
No economic replay, original L0 admission, registration or activation occurred.

The adopted study retains ES/NQ/CL/GC, 2018–2022, seven hour-context features, the
one-hour tick-change target and eight outer windows. Ridge 1.0 is primary;
0.1/10.0 are fixed sensitivities; zero-change and training-mean predictions are
benchmarks. It changes the question from trading-policy selection to retrospective
observed-target prediction conditional on stored vintage and supplied contracts.
It cannot claim tradability, P&L, Sharpe or alpha. Distinct study/candidate parents,
prior failures, related searches and information use remain visible within the
one pipeline. Existing reporting/attempt/comparison conventions are shared where
applicable; predictive diagnostics are not fabricated economic reports, and
screen-to-cockpit publication is not claimed as completed.

The separately approved [fixed time-of-day diagnostic](state/unpublished_evidence/l0_order_readiness_v1/screen_clock_diagnostic_v1/replacement/REPORT.md)
is complete: all four markets/eight windows, eleven predefined half-hour buckets,
and 7,964 arithmetic comparisons with zero discrepancies. The corrected replacement
used saved primary forecasts and targets only; no fitting, new forecasts or economic
replay occurred. Small forecast dispersion and mixed or negative bucket averages
do not overturn the completed unsupported study. The initial reader-format failure
and consumed receipt remain preserved. Its corrected access extension changes
operational policy bindings only, not historical scientific identities.

**Completed transfer-study history:** the owner requested local implementation of the
[ES-to-NQ information-transfer screen](state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1/REPORT.md).
Its task-local declaration and synthetic implementation use completed five-minute
ES/NQ changes to predict NQ's following 15-minute change, with one primary OLS
model and matched NQ-only/zero/training-mean comparisons. Its
[access integration is adopted](state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/REPORT.md)
through `configs/approved_es_nq_transfer.json`; six installed-path synthetic tests
and eleven unchanged scientific regression checks passed. The
[first failed materialization](state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/MATERIALIZATION_REPORT.md)
and consumed receipt remain preserved. The separately approved
[source-binding repair and replacement materialization](state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/REPORT.md)
are complete: seven focused synthetic checks passed, the corrected producer uses
the checkpoint's explicit source IDs, and all 120 provenance joins verified.
All ten partitions and 3,511,073 rows reconciled across 1,305 dates and 28,710
scheduled decisions: 28,055 eligible, 27,970 observed targets, 655 ineligible and
85 unavailable targets retained. All eight training populations meet the minimum.
The authentic run took 349.614 seconds, peaked at 474,128,384 bytes, and cost $0.
The separately approved [fixed evaluation completed](state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/evaluation_task_v1/REPORT.md):
16/16 OLS fits and 8/8 windows, with `NO_SUPPORTED_LEAD`. The primary model's
average error was slightly higher than all three benchmarks; every primary lower
bound was negative and the ES coefficient was positive in only 2/8 windows.
All 500 scorable audit dates and missingness records remain represented. Runtime
was 135.895 seconds, peak memory 380,473,344 bytes, campaign output 14,587,394 bytes,
and cost $0. The exact execution receipt is consumed; no retry or further fitting
is authorized. The owner [closed this exact study as unsupported](state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/evaluation_task_v1/closure.json).
Its results and prior exposure remain preserved; another experiment needs
a separately justified scientific question. No economic replay or activation
occurred. Shared-policy lineage preserves the original software/evidence bindings;
no stale check or original L0 gate was bypassed. The completed
four-market screen remains unsupported; this completed study preserves the earlier
prepared cross-market family and all prior-search/time-bucket exposure.
The earlier [ES closing-window proposal](state/unpublished_evidence/l0_order_readiness_v1/es_close_continuation_proposal_v1/REPORT.md)
was declined by the owner and remains unadopted. Any next study must state a new
scientific question and retain prior-search and time-bucket observation exposure.
Do not select a favorable diagnostic bucket, promote CL/GC's unsupported averages,
or substitute a sensitivity winner as a rescue of the completed screen.
The operational remedy's complete output bound was 1,671,157,429 bytes; all 32
membership files reproduced the exact 1,415,865,949-byte total. Eight focused
synthetic checks passed. A startup error in the first new driver launch was fixed
and preserved; the owner then explicitly approved one replacement. That successful
run took 1,292.454 seconds, peaked at 1.892 GiB memory and saved 1,632,988,743 bytes
including the receipt-consumption record, for $0. No measured limit overshoot
occurred. The exact implementation-only successor and consumed receipt are linked
in the completion report; no scientific/reporting re-adoption or source rescan
occurred. No further fitting or automatic retry is authorized.
Original trading-policy calendar,
termination, execution and fee requirements remain unresolved for that experiment.
Calendar-only changes do not automatically alter current training populations
or training means; they invalidate affected trading benchmark replays and may
change diagnostic strata.

The earlier documentation reconciliation verified the V8/V3 reporting receipt.
The subsequent [adoption receipt](state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_adoption_v1/adoption_receipt.json)
records exact component lineage, unchanged original science, preserved V8/V3 artifacts,
passing final synthetic checks and installed screen authority. No original L0
active pointer or data-operation receipt was created. All stages remain in one
pipeline coordinated in #1; reporting schema 2 is unchanged.

### Original L0 readiness and preserved history

The V5 calendar guard requires a supported open interval through the full holding
period, strictly before a known close. Unknown calendar coverage causes entry
abstention without deleting forecasts. The order-readiness diagnostic uses the
same replay state machine and all 19 distinct frozen audit windows, with separate
evidence-only and midnight-availability-assumption views. It calculates no P&L.
Neither benchmark reference coverage nor a calendar assumption grants research
admission. Ordinary-session calendar coverage remains unestablished in the bound
holiday sources. The completed diagnostic read all 20 files and reconciled all 9,800 gaps within
20 minutes. GC retained an unresolved GCZ1 exit at 10:34 Pacific on 2021-11-26 in
two overlapping assumed-availability windows. Calendar coverage and existing
fee/admission requirements still block original L0 fitting. See the [order-readiness report](state/unpublished_evidence/l0_order_readiness_v1/4a6d6ef53fe16dcc930689b36cb29b1bc29db3f5ef23d9e3851aa50c84346f93/task_report.md).

The preserved predictive V9 predecessor is
configured in `configs/rolling_hour_research_v9.json`. It keeps the existing
minute bars, one-hour endpoint target and feature packs, and selects by
date-balanced inner-validation MSE. Each forecast refers to the contract
observed at its decision; unavailable future endpoints remain disclosed instead
of removing the forecast. See [predictive implementation status](docs/ROLLING_HOUR_PREDICTIVE_STATUS.md).
Its original adapter still admits generated inputs only. A separate source bridge
now connects verified source products to `CertifiedResearchGateway.prepare_predictive_readiness`.
The [source-readiness report](state/unpublished_evidence/rolling_hour_predictive_v1/source_readiness_v1/task_report.md)
records the complete timestamp census and outstanding evidence. Historical
calendar/timing certification, registration and execution remain blocked;
no historical model or new historical execution authority is implied.
Use `.\.venv\Scripts\python.exe -m futures_rebuild.rolling_hour_predictive_preparation validate`
with `PYTHONPATH=src` to check the inactive binding. The `synthetic` action saves
generated-data acceptance evidence and cannot read historical inputs.

The preserved predecessor is the inactive [V8 assumed-cost successor](docs/ALL_MARKET_COST_REMEDIATION_PLAN.md),
configured in `configs/rolling_hour_research_v8.json`. It retains ES/NQ/CL/GC
2018-2022 research scope, with compatibility checks for all 41 markets. Selection
uses one frozen adverse assumed-cost schedule; base/adverse/severe evaluations
replay complete policies. EDGE is diagnostic only. The [descriptive historical-use
decision](docs/ROLLING_HOUR_DESCRIPTIVE_ADMISSIBILITY.md) permits consideration of
reused development history with all 65 unresolved legacy references and the
105 penalty retained. The census remains incomplete; calendar-publication,
pre-open contract-selection, certification and execution gates remain unresolved.
No historical execution is authorized. Use `python -m futures_rebuild.rolling_hour_assumed_preparation validate`
for the inactive binding and `synthetic` for generated-data acceptance. Current
measured results are in the linked implementation report.

The [ordered remediation plan](docs/ROLLING_HOUR_ORDERED_REMEDIATION.md)
records the V8/V9 predecessor work. Source compatibility precedes certification and fitting.
The shared calendar parser now handles all 192 assessed workbook/family
combinations, including correct overnight opening dates. This does not certify
original publication timing or repair V8's single-contract/full-minute source
mismatches. The user selected predictive MSE, observed-contract forecasts and
endpoint-only labels for V9. V8 and its historical-execution block remain unchanged.

The preserved [minute-entry, fixed-hour V7 predecessor](docs/ROLLING_HOUR_RESEARCH.md)
uses the quote-cost route described below.
It retains causal OHLCV-1m and sidecar eligibility checks, refreshes forecasts every
eligible minute, enters only when flat, and holds one contract per market for a
fixed 60 minutes. Three existing feature packs and five model profiles define
60 new counted configurations across ES/NQ/CL/GC. Selection, six baselines, and
outer evaluation share the same position and quote-cost replay. It is inactive
and synthetic-only. The v4 remediation requires an independently declared session
calendar, retains empty dates, and adds richer full-WFA synthetic coverage and
descriptive diagnostics. V5 adds matched zero-change and training-mean
forecast comparisons using date-balanced MSE; profit-based selection is unchanged.
V6 adds distinct observed-date support, sum-one date-balanced training weights,
normalized regularization and a descriptive selection-period final refit. Its
source adapter has synthetic checks; historical custody, compatible 2018-2022
quotes and the historical-use route remain blocked.
The three packs contain 5/10/16 features. Its disposition
is descriptive-only; source readiness and downstream handoffs remain incomplete.
The v4 expanded four-market policy replay passed; the ES interaction-pack diagnostic
is incomplete because its outer refits failed leaf support in folds 6 and 7.
Profitability qualification is
`BLOCKED_RAW_PNL_INFERENCE_NOT_VALIDATED`: no replacement raw-dollar test has
been implemented or validated. Closed experiments and existing authority remain
unchanged. Use `python -m futures_rebuild.pipeline rolling-hour-research` for its
read-only phase contracts and blocks. The detailed fixed-hour descriptions below
are preserved predecessor contracts, not authorization for this minute cadence.

The preserved [profit-aligned successor](docs/PROFIT_ALIGNED_RESEARCH.md) implements
daily summed net-dollar selection, causal bid/ask accounting, and a frozen
252-future-session test of profit versus zero. Ridge and XGBoost can qualify
numerically; benchmark improvement is descriptive. It is inactive and has no
real-data runner or access authority. Its complete synthetic statistical report
controls whether calibration passed; engineering tests cannot clear that gate.
The final v6 report is `BLOCKED_STATISTICAL_VALIDATION_FAILED`: the complete
28,000-replication calibration failed the false-positive limits. No research
or activation block is cleared; measured results are in the linked successor guide.
The v2 comparison and historical-census requirements below remain preserved
predecessor contracts, not the objective of this new successor.

A separately requested [strategy-discovery successor](docs/STRATEGY_DISCOVERY_SUCCESSOR.md)
retains the comparison's session-balanced stress-net-tick score and historical
DSR requirements. It adds one five-feature context pack (200 configurations)
and diagnostic cost margins; Ridge added value is diagnostic rather than a
qualification veto. It is inactive, does not supersede the profit-aligned
experiment, and preserves all unresolved census/readiness and access blocks.

The bounded ES flow-continuation rescue executed once under registered program
`a243ec7bc23d0f0876c802bb8c0e9250d1bbe9d0cdc4159ac189e3422d6b5732`.
Its frozen family contained eight variants: 15-minute absolute-return z-score
thresholds 1.5/2.0 crossed with same-minute log-volume z-score thresholds
0.5/1.0 and 15/60-minute holding horizons. The historical screen exposed only
2018-2020 outcomes; 2021-2022 WFA was conditional on a screen pass and did not
run.

Official result
`5923a09a9719cd18d8925d2ba6b3f2a88ce959adcbdf288f926956b4631d2d2a`
is `STOP_NO_ECONOMIC_EDGE`: all eight variants failed, no candidate qualified,
and `next_step_authorized=false`. The family is closed. The earlier proposed
four-variant 30-minute screen was not the registered execution and must not be
run as a retry, tuning step, or result-informed rescue. The remaining
32-market build, WFA, portfolio inference, 2023+ data, holdout, forward,
provider, and trading paths receive no authority from this result.

`PROJECT_OUTLINE.md` is the practical current pipeline reference. The prior
Tier 0 mechanism, bounded ES flow-continuation family, micro-exhaustion screen,
and fixed-hour multifamily pilot are closed after ES failures; no successor
evaluation is active. Active ladder contract
`720cd2382c9fa513673e3227a7e2ec576663127b6bdca13106f5c9be688fce5a`
is schema v9 and binds the existing internal Stage-2.5-v2.1 policy identity. The new
all-Globex mechanism separately has an inactive prepared internal Stage-2.5-v2.4
successor; it is not an active pointer or access grant. Immutable
pre-result selector evidence resolves its macro disposition as
`NO_ELIGIBLE_MACRO_DIVERSIFIER`; it may not be rerun or silently substituted.
These active identities grant no row read, mechanism execution, evaluation,
staging, or publication authority. The
causal-observation contract is frozen and its
observation-only safety implementation is built, validated, committed, and
remotely recoverable. The complete 41-market causal observation release is
certified and its uniform annual view is active. The
seven-market development-only canary ran once under consumed authority,
passed independent verification, and remains unpublished and inactive. The
historical full-build attempts and V9 evidence remain immutable and grant no
fresh authority. V10 is the current immutable monthly certification source: it
processes and maximum-robustness-certifies one complete market at a time in a
frozen 41-market order and stores source evidence below
`data/causally_gated_normalized/v10/`, and cannot advance until the preceding
market certificate remains valid. Each completed year is sealed with exact
source, state, partition, and file identities; a fresh attempt may reuse only
an unchanged contiguous sealed-year prefix. Two independent certification
replays run in separate processes. The controller and provider-free production
rehearsal are synthetic-test proven. A dedicated ES-2025 canary successor is
implemented and passed under consumed authority: it bound seven exact
registered DBNs plus sidecars, performed one producer decode and one
fresh-process independent replay, wrote only below the V10 `_canary` lane, and
did not seed the complete ES checkpoint. All 41 market checkpoints and their
two-pass certificates are complete and preserved. A successor complete-set
certificate established compatibility without rebuilding certified markets.
The active minute view contains 609 annual releases and 139,892,740 rows, from
each market's available start through the exclusive
`2025-07-13T22:00:00Z` cutoff. The
partial 2025 files are interval-bound non-selection research; July 14 onward
remains inaccessible. Complete-set data certification and publication grant no
new experiment access, Alpha qualification, model or evaluation authority, or
holdout access.

The native 1m/1h/1d releases now share the annual physical layout
`data/causally_gated_normalized/ohlcv_<timeframe>/<market>/<year>/<release_key>/`.
Each release contains observations.parquet, quality.parquet, cadence.parquet,
missingness.parquet, roll.parquet and manifest.json. Logical monthly coverage
and permission entries remain explicit; annual packaging does not fill unavailable
months or change the public admitted-reader contracts. Hourly/daily raw-path
access remains blocked. Explicit audit-only derived ZQ stays in its existing lane.

All 1,817 annual units passed independent five-table source equivalence; all
609 minute units also matched the previously active compact observations.
Full publication and permission accounting passed, followed by fresh default
hourly/daily/minute checks and a final binding recheck. See the
[completion report](state/unpublished_evidence/annual_foundation_migration_v1/migration_v1_validation_retry_v1/COMPLETION.md) for representative-check limits and measured resources.
Annual dependency independence is now active through
`configs/active_annual_foundation_independence.json`. The native annual readers
use the validated independent unit certificates instead of opening predecessor
payload folders. The [live activation report](state/unpublished_evidence/annual_dependency_retirement_v1/activation_v1/COMPLETION.md)
records fresh daily/hourly/minute default checks, raw-path guards and full binding
rechecks before and after cutover. Existing permissions, exclusions and recovered
daily identities remain unchanged; model-specific eligibility remains `NOT_DECLARED`.
The predecessor market folders and V10 checkpoints are archived under
`data/archived_normalized_predecessors/annual_independence_v1/`. See the
[archive receipt](state/unpublished_evidence/annual_dependency_retirement_v1/archive_v2/COMPLETION.md).
Restore their exact original locations before predecessor-dependent audits,
producers or rollback to older readers. Compact files, historical certificates
and rollback controls remain retained; deletion is not authorized.
Future writers need compatible annual
packaging before publishing; this activation does not convert their output
automatically or refresh research approvals.

`data/causally_gated_normalized/_control/catalog.json` is the operational
authority for materialized minute intervals. Hourly/daily publication uses
`data/causally_gated_normalized/_control/timeframes.json` and the active
quality/admission pointers. **Native annual control relocation: COMPLETE.**
The [completion report](state/unpublished_evidence/control_root_relocation_v1/execution_v2/COMPLETION.md)
records passing isolated and fresh live 1m/1h/1d checks with `data/active` access
denied, unchanged population/permission accounting and final binding checks.
Use the admitted readers and existing preparation-only foundation audit commands;
their current implementations select the relocated controls, never a discovered
folder or fallback catalog. `_control` contains control records, not market data.
Compact publication entry points refuse; new publication must use the compatible
coordinated annual publisher. `data/active` remains retained for historical and
separate micro lineage; this cutover does not authorize its archive or deletion
or refresh frozen research approvals. Permissions, exclusions, recovered daily
identities and causal timing are unchanged. The older accepted whole-calendar-year policy remains a
hash-bound historical policy and a fail-closed default when no exact interval
is supplied; its `2025 = LOCKED_HOLDOUT` label does not override the catalog's
six exact development-prefix entries. Those entries are non-selection research
only. No row at or after the development cutoff is admitted.

Real research still needs its durable trial declaration and immutable evidence.
Before registration, every row-dependent sample, fold, feature, execution,
baseline, cost, risk, metric, and promotion requirement needs a source-bound
readiness certificate produced under separate row-access authority. Synthetic
tests and aggregate coverage prove mechanics, not real-source readiness. The
certificate must name every exact preregistered fold and independently account
for each baseline, including a true zero-trade baseline and scenario-specific
risk abstentions. Its semantics and actual bound files must be revalidated
before a single-use historical execution claim is consumed.

The failed ES screen authorizes no platform expansion. The bounded Ridge/XGBoost
all-Globex comparison is prepared inactive in
`configs/fixed_hour_model_comparison_v2.json`. Its hash-bound Phase 3, 4, 5,
6 and ladder contracts share one mechanism identity. This is a new counted
mechanism, not a redirection of the preserved three-head predecessor or an
active-pointer change. Its route is Phases 1–9, then 12–15; Phases 10–11 are not
applicable. No macro selector is rerun or substituted.
The previous Ridge-only `configs/fixed_hour_terminal_successor_v2.json` and its
unaccepted v1 preparation draft remain preserved; neither certifies this comparison.

The inactive v2 engineering successor preserves the v1 experiment and its
180 configurations. Evaluation replays complete deterministic selection and
fitting before accepting a run; hashes alone do not establish training provenance.
Expected-cost evidence separates prior observations from later certification,
which must precede a bound evaluation freeze supplied by the trusted caller.
V1 and Ridge-only certificates remain historical evidence and do not certify
the changed implementation. The new bundle separates synthetic mechanics from
blocked census recovery, uncertified real-source readiness and uncalibrated
expected costs. It grants no access.

Future feature discovery is organized around continuation, stretched-price reversal,
and session-context conditioning in [the OHLCV research plan](docs/OHLCV_HYPOTHESIS_RESEARCH_PLAN.md).
The first comparison changes model capacity, not formulas: retain the 29-feature
library and nine packs. An explicit stretch feature and separate hypothesis-specific
claims remain deferred. No historical research authority follows from preparation.

Phase 3 declares an unproven predictive hypothesis: recent direction and
price-volume behavior, conditioned on longer trend/volatility and verified
interval context, may predict next-hour terminal change enough to clear stress
costs. Directional baselines, chronological evaluation and identical candidate
samples distinguish unconditional exposure, sample noise and coverage changes.
Freeze the full procedure before synthetic testing or any outer WFA.

Phase 4 retains terminal change and both realized excursions, with exact
open-print provenance, maturity and tick-safe consistency checks. Only terminal
change in ticks is a fitting target; excursions are diagnostic records that
cannot affect fitting or trades. Entry and fixed 60-minute exit use one-minute
`open_nano` print proxies, not bid/ask prices or guaranteed boundary fills.
Exit-bar later highs/lows are excluded. Positive, zero and negative prices are
supported. Missing minutes, breaks, unresolved rolls and future-known data fail
closed.

Phase 5 uses 61 consecutive causal bars in the same verified open interval and
actual contract. The original +22-minute, 60-minute-spaced grid is unchanged,
but +22 is insufficient-history and +82 is the earliest eligible entry.
Separate original-grid, insufficient-history, valid-feature and invalid-outcome
counts remain visible; the 98% requirement uses the frozen successor-eligible
schedule. Every candidate uses the same sample.

The nine fixed packs comprise baseline-only, five leave-one-baseline-out packs,
the existing 18-formula library, baseline plus two 60-minute features and three
interval-quartile indicators, and that context pack plus six interactions.
Quartiles are verified interval context, not claimed liquidity regimes.
Crossing the packs with Ridge penalties 0.1, 1 and 10 plus two XGBoost depths
(2 and 3) gives 45 candidates per market and 180 total. No Random Forest,
algorithm tournament, open-ended formula discovery or separate
session model. Constants and exact duplicate columns are removed using training
data only. Model/pack/feature/family stability reports are descriptive, not an
adaptive pruning search or Jaccard promotion gate. This comparison runner does
not emit correlation reports; the inherited descriptive-only correlation rule
is a restriction on use, not a promise of that output.

Phase 6 freezes one terminal-model selection procedure per market, stress-cost long/short/
abstain thresholds (equality abstains), and median inner-fold session-balanced
stress-net ticks as the selection score. Abstentions count as zero eligible
opportunities; this is not a risk-adjusted objective. Candidates within 0.01 tick
prefer Ridge, fewer retained columns, larger Ridge penalty or shallower tree,
then stable candidate identity. Ridge is standardized; XGBoost is unscaled.
XGBoost 3.0.5 uses CPU hist, one thread, seed 62817, 100 rounds, learning rate
0.05, max_bin 64, L2 10, no subsampling or early stopping. Child weight is
max(50, ceil(5% of training rows)); every fitted leaf needs 20 distinct dates.
Failure invalidates the candidate, not observations. A failed selected outer
refit stops affected execution; it cannot select a replacement after peeking.
The 2018–2022 selection period retains 505/252 initial fit minima, eight 63-date
outer folds, four 63-date inner folds, global whole-date purge and one-session
embargo. Selection actually occurs within outer training through inner WFA.
Selected transforms and coefficients are refit on full outer training and
applied unchanged to that fold's outer rows. No full-sample feature selection
is allowed. Independently inner-select a Ridge-only benchmark on identical folds.
Freeze both native artifacts and predictions before outer scoring; no pickle.

Existing base/stress/extreme schedules and the $10 round-trip fee are preserved
and explicitly provisional. Stress remains the decision, selection and promotion
hurdle. All scenarios use identical frozen trades. Expected-net reporting
requires independently certified, pre-evaluation, coverage-bound evidence with
the same open-print price reference; otherwise it is
`EXPECTED_COST_UNCALIBRATED`. The L1 midpoint-relative half-spread helper is not
an open-print slippage or impact estimator and is not reused for this purpose.
Daily P&L variance, cumulative fixed-exit drawdown and adverse excursions report
ticks/dollars and counts, not capital returns or intrabar equity drawdown.

Phase 7's successor certificate exercises generated bars through features,
outcomes, nested selection, refit, predictions, independent qualification and
downstream eligibility in all four markets. Phase 8 keeps ES as engineering
pilot. Phase 9 independently fits NQ, CL and GC; an ES or NQ economic failure
does not veto another market. Complete results or explicit terminal failures
for all four markets are required. Synchronized date-based inference retains
the complete declared market/baseline family. Only independently qualified
markets advance to Phase 12, then stress survivors to deterministic refit.
Activity, coverage, quality, stress-net, baseline and statistical gates remain.
The synchronized Romano–Wolf family has 28 hypotheses: six baselines plus the
Ridge-only differential for each market. Combined-minus-Ridge session-balanced
stress-net improvement must exceed 0.01 tick/opportunity, have a positive
one-sided 95% HAC lower bound and adjusted p <= 0.05. No added value means this
route closes; do not retrospectively promote Ridge as a fallback.
Interpret the outcome as an added-value experiment, not a search for any
profitable model. Positive stress-net performance alone is insufficient; a
positive strategy can fail the Ridge differential. Failure establishes neither
that Ridge is unprofitable nor that all OHLCV approaches lack an edge. A different
promotion objective requires a separately declared experiment, never a change
made after inspecting this experiment's outer results.
The macro-required legacy qualifier is incompatible and must not run this
successor.

The registration-only ledger census preserves predecessor attempts and adds
180 configurations once; the prepared conservative burden is 469 (289 + 180).
Historical Sharpe census evidence was not recovered or imputed: complete DSR
evidence remains required before any historical qualification. The new end-to-end
fixture explicitly verifies that missing census evidence prevents promotion.

Census readiness is a prerequisite to historical research, not a problem to
defer until qualification. Current status: `BLOCKED_CENSUS_RECOVERY_UNRESOLVED`.
The accounting remediation is implemented in
`configs/fixed_hour_search_accounting_v1.json`; see
[search accounting](docs/FIXED_HOUR_SEARCH_ACCOUNTING.md). It separates charged
search from observed performance and imports two audited dispositions without
inventing Sharpe ratios. The proposed count-based replacement is arithmetic-tested
only: `STATISTICAL_REPLACEMENT_NOT_VALIDATED` and
`SEARCH_UPPER_BOUND_NOT_ESTABLISHED` still block qualification. The current DSR
qualifier remains unchanged; the v2 comparison changes engineering verification
only. The preserved accounting v1 assessment is historical evidence, not a
current-implementation certificate; 469 is not a proven
upper bound, and no replacement threshold has been activated.
The [bounded prospective assessment](docs/PROSPECTIVE_EVIDENCE_ASSESSMENT.md)
is complete: available evidence does not establish a historical upper bound.
A frozen fresh-outcome bounded-score test passed scoped synthetic calibration,
but is not an uncapped-profitability or DSR replacement. It remains assessment
code only, with no campaign activation, historical execution, or holdout access.
Before requesting experiment activation or result-bearing data access, resolve
whether the predecessor performance evidence can support the frozen DSR rule.
A count of 469 configurations is not 469 observed Sharpe ratios. Inventory
declarations first; any inspection of historical result artifacts needs separate
bounded read-only approval. Retain failed, abandoned, and unevaluated trials
explicitly; do not fabricate performance for unevaluated trials, replace missing
values with zero, or rerun old trials to reconstruct apparently original results.
Recovery must establish identities, source provenance, comparable return periods
and units, and how the frozen census accounts for every charged configuration.
If this cannot be established, this experiment remains blocked. Any alternative
statistical treatment requires a separately approved pre-result successor, not
a waived DSR gate. Complete result-bound census validation is still required at
qualification; readiness does not claim future candidate results already exist.

Predecessor contracts remain unchanged:
`configs/fixed_hour_directional_outcome_foundation_v3.json`,
`configs/fixed_hour_feature_expansion_foundation_v3.json`, and
`configs/fixed_hour_globex_mechanism_evaluation_v4.json`.
Their three-head/ranking/Jaccard semantics and synthetic certificates certify
only the predecessor. The prior inactive Stage-2.5-v2.4 identity
`6bbfcbefd92930d9c2bc1858335e90be2d00ba0bb2458f222f413f660c3b4b0d`
does not grant the new mechanism access. Active pointers, accepted artifacts
and the common final-holdout boundary are preserved.

The successor has no historical execution route or trading authority.
Activation and source-bound readiness must be separately authorized before any
real-history registration or evaluation. Measured expected costs, real-source
readiness, historical performance, holdout results and profitability remain
unproven.

The current authoritative historical design remains Rank 1, bound by
`configs/active_alpha_historical_research_design.json`: selection, feature
selection, fitting, WFA, and mechanism selection use only 2018-2022. After each
market's qualification result is sealed, the same frozen mechanism receives one
non-pristine development-regime stress test over 2023 through the exclusive
`2025-07-13T22:00:00Z` cutoff. It is not a holdout or confirmation. No tuning,
design change, or retry may follow its result. Its single execution makes the
official decision over the whole period and reports 2023, 2024, and
2025-to-cutoff diagnostic slices; slices are not gates or selection inputs. A
passing market must then be refit using only coefficient or
fit-parameter changes over the complete 2018-to-cutoff development interval,
with no further performance evaluation before Final-252. Every row-dependent
step still requires its own readiness evidence and authorization; the active
design pointer grants identity only.
The preserved schema-v6 and schema-v7 authorities are historical and inactive.
The active schema-v9/internal Stage-2.5-v2.1 policy orders one scope-wide
development-regime stress test after all applicable replication, followed by a
fit-parameter-only deterministic refit with no evaluation. Its active pointer
provides policy identity only: no target, feature, WFA, fit, evaluation,
challenge, refit, Final-252, or forward row access follows without separate
authorization.
Future trial publication must use the certified registration writer: the
registration identity binds one canonical readiness-evidence path, file hash,
certificate identity, trial family, and protocol. Future historical execution
must use the certified claim wrapper, which reloads that exact immutable
registration and certificate before it invokes the one-use authorization
claim. A caller-supplied or cross-trial certificate is not sufficient.
The only current code surface for those two actions is
`CertifiedResearchGateway`. It derives the exact trial, family, protocol,
registration hash, certificate identity, and evidence hash used by the
single-use receipt. The shared receipt boundary rejects every unknown or
retired real-history trial operation before a claim is written. Preparatory
source and readiness censuses remain separate operations and cannot fit,
predict, evaluate, register, or promote a trial.
The gateway also requires one active, hash-bound Alpha research ladder. With no
active ladder pointer, all new real-history registration and execution fails
closed. The ladder successor governs future counted mechanisms only; it does
not reinterpret the previous mechanism, which remains closed and nonretryable.

Phase 7 is synthetic engineering. It can prove mechanics, point-in-time ordering,
accounting, and failure behavior, but makes no Alpha claim and reads no real
history. The complete material mechanism identity must be frozen before Phase 8 / Tier 0.

Here, `causal` in observation, feature, outcome, and execution terminology means
only point-in-time information legality and ordering. The default research claim
ceiling is robust predictive association, not economic causal identification. A
hypothesis may seek `CAUSALLY_SUPPORTED_MECHANISM` status only if that stronger
claim is declared before outcome access and freezes mechanism-specific competing
explanations, negative controls, boundary predictions, and an identification
argument. Missing any item retains the default predictive-association ceiling.

Before outcome access, every mechanism must also freeze a small mechanism card
with two or three plausible competing explanations, an expected boundary, and
one discriminating test for each. Promotion requires those frozen tests and
complete evidence reporting by predeclared market family and training-only
trend/volatility regime cluster. Market and fold counts remain visible but are
never reported as counts of independent evidence.

Every hypothesis must also freeze its expected market, family, asset-class, and
session scope before outcome access. Scientific corroboration follows only that
predicted scope. The fixed 16- and 41-market ladders apply only to hypotheses
that preregister broad generality. ES remains the cheap engineering pilot for
all hypotheses, but ES Alpha failure rejects a hypothesis only when that frozen
hypothesis predicts an ES effect; otherwise it is not a scientific gate.
The historical Phase 8 / Tier 0 was one row-certified ES pilot with 504 training and 63 evaluation
sessions. Program
`4fa73a9a8547e49e67c2b1b1eafc31cbb1278ae5118080144b9ec58bcc64e340`
froze one ES-only micro-exhaustion candidate. Registered trial
`d52d112e9708d00b95ce6051c9266e51bb2a6800ff10a35cd40eb0493d497d5a`
ran its 2018-2020 development screen once and returned terminal official result
`ad7f318c5f26a4c5f316999c943546c9c1a95dde0f3dfae91e46056b50f24140`:
`STOP_NO_ECONOMIC_EDGE_CLOSE_CANDIDATE`. It is counted as individual
information-family mechanism 169, cannot be retried, and cannot reinterpret
or rescue the failed continuation family. The conditional 2021-2022 WFA was
not read or run. Failure record
`9fdba237dd8a3ebf73c0b053fa4f50c5e03e242bdc759cbffae30c38d381c5d1`
also marks the result non-promotable because the pre-outcome program omitted
the required competing-explanations mechanism card.

The later fixed-hour multifamily mechanism
`b828013f32f1ecb03b83bfd68eed87a90284e8113db0c006e42fa94cde5cdd18`
froze 18 causal OHLCV features in five families and put the complete library
into each of three signed-target Ridge candidates. Only penalties 0.1, 1.0,
and 10.0 varied; normalization was learned inside each training fold, and no
feature-subset or full-sample correlation search was allowed. Registered trial
`349204fd20506cd21a4e14c2a59e425e90a9a8c452898c14c4c18e2391e723b7`
ran the approved ES 2018-2020 pilot and sealed result
`e422e5f3a547980dc4b88b06a70c02798a30225ab65264cd450394cc422eb474`.
No penalty passed both the per-inner-fold activity gate and positive median
stress-net-return gate, so outer outcomes were not scored. The mechanism is
closed and nonretryable; the result grants no 2021+, other-market, holdout,
forward, provider, publication, or trading authority.

The preserved historical Phase 9 / Tier 1 evaluated one frozen incremental pack:
NQ, CL, and GC; the terminal no-eligible-macro disposition is retained. ES and
NQ together provide at most one equity-family result; NQ is a
correlated extension whose result remains individually visible. CL supplies
energy evidence and GC supplies metals evidence. ES, CL, and GC must pass,
every required
result must exist, the mechanism identity must match, and no between-market
tuning may occur. NQ failure makes NQ individually Alpha-ineligible but does
not erase otherwise successful distinct-family evidence. No pooled score
can conceal a required market or family failure. Those historical family gates
are not the terminal successor's independent-market qualification rule above.

Phases 10–11 are not applicable to the current four-market contract. A future
broad-generality hypothesis must restart at Phase 3 before result access. For
that preregistered branch, Phase 10 / Tier 2 preserves the exact balanced
16-market pack and predecessor promotion thresholds, and Phase 11 / Tier 3
preserves the exact 41-standard-market universe: 38
traditional markets plus BTC, ETH, and PA satellites. Traditional markets must
pass individually; satellites cannot rescue failure. The 17 deferred micros
remain disjoint, cannot rescue failure, and cannot create another holdout.
Every tier uses the same frozen mechanism, and material changes restart a new
counted mechanism at Phase 7.

Final historical evaluation is a future gate only. It is currently
`SEALED_AUTHORITY_RESOLVED_ACCESS_FORBIDDEN`; the active manifest defines and
seals the interval but grants no holdout or forward access. This research
pipeline ends at Phase 15 and never authorizes trading. Any later deployment
uses a separate execution-qualification pipeline covering bid/ask or order-book
fill simulation, latency, slippage, market impact, capacity, rolls, paper
trading, operational controls, and separate live-order approval. OHLCV proxies
certify none of those items. The 41 markets are a scientific universe, not an
automatic deployment list.
The active ladder pointer, not `configs/alpha_tiered.yaml` by itself, is the
current operational truth. `futures-pipeline` is synthetic-only. Current
real-history actions consist only of a separately authorized immutable
readiness census followed, after a passing certificate, by registration and
execution through `CertifiedResearchGateway`.
Any row-certified preparatory census also consumes its own authorization even
when it times out or produces no report. A successor must use a new immutable
plan and approval; it may not silently reuse the consumed claim.
The cockpit is observation-only and credentials remain outside Git. It has no
order assistant, broker adapter, account binding, or order transmission path.
MFF Evaluation and Rapid EOD Sim Funded remain manual-only outside the app;
MFF Live API capability remains unconfirmed. See
`docs/MFF_TRADOVATE_EXECUTION.md`.

When present, the machine-local accepted micro-contract source catalog is the
immutable legacy publication at `data/active/catalogs/apex_micro.json`. Its
pointer and catalog are exact local evidence, not tracked checkout inputs and
not dependencies of the canonical `current` test lane. Exact-byte checks run
only in the fail-closed `local_evidence` lane with an explicit hash manifest.
New code and prospective artifacts use the `micro_futures_*` namespace. The prepare-only generic cutover
plan is `configs/micro_futures_catalog_migration_plan_v1.json`; creating its
proposed catalog or pointer is a separate active-data mutation and is not
authorized by ordinary local work. See `docs/NAMING_AND_LINEAGE.md`.

## Prop-firm risk profiles

Future research preparation uses the provider-neutral profiles in
`configs/prop_firm_profiles.json`. The active immutable profile is
`mff_rapid_eod_50k_2026_08_10` at explicit stage `sim_funded`. Its modeled
ledger starts at $0, uses an EOD-only $2,000 trailing floor with a permanent
+$100 lock, and permits micro-only strategy intents under one portfolio-wide
30-micro-equivalent cap. Evaluation and inactive Live rules are separate
stages and cannot leak into funded research.

Strategy risk, execution mapping, platform costs, and payout policy live in
separate `prop_firm_*` configurations and their selected IDs/hashes are part of
every current run/cache identity. The platform and official MFF fees are
currently `UNSET`; unknown current news/session/price-limit data also fails
closed. Production/live readiness therefore remains false. The hash-bound
`configs/prop_firm_risk_profile.json`, previous provider profile, and former
Phase 8 chain are historical evidence. Current Phase 8 model-evaluation
preparation uses
`configs/prop_firm_phase8_evaluation.json` and
`src/futures_rebuild/prop_firm_phase8.py`. See `docs/PROP_FIRM_RISK.md` and
`docs/NAMING_AND_LINEAGE.md` before changing providers.

Read-only preparation commands:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-risk-policy
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-phase8
```

These commands print deterministic, non-authorizing preparation records. They
do not access provider services or rows and do not grant model or prop-firm
evaluation, publication, active-data, payout, deployment, or trading authority.

## Historic workflow material

Old hash-bound plans, approvals, receipts, closure runs, and successor modules
are evidence only. Do not use them as instructions for new work. See
`docs/LEGACY_WORKFLOWS.md` when a historic artifact must be interpreted.
Versioned V4-V12, Standard-only, Final, and Authoritative registration and
historical-execution helpers are retired. Importing an old helper does not make
it a current execution route; their historical operation names fail closed.

## Working state

Preserve unrelated work; never use broad staging, and report only the decision
or blocker that needs attention.

## Current data boundary

Canonical source closure is complete and active. The canonical DBN source is
frozen and validated. The development-only causal-observation contract is
frozen, and its observation-only safety implementation is built, validated,
committed, and remotely recoverable. The complete 41-market causal observation
release is certified and its uniform annual view is active. Raw DBN cannot feed
features, models, WFA, or backtests directly; it may feed only that exact approved
causal builder.

Old derived releases and retired foundation runners remain no-read. Historical
canary and full-build authorizations are consumed; their inactive outputs and
V9 artifacts remain evidence only. The V10 market-by-market controller,
checkpoint writer, sealed-year recovery, independent market certifier, and
long-path-safe provider-free rehearsal are implemented. The ES-2025 canary
remains separate historical evidence, not a seed for the complete release.
All 41 market certificates and the successor complete-set certificate are
preserved. The active minute view has 609 annual releases and 139,892,740 rows;
its catalog retains eight additional protected, unmaterialized SR3 entries.
The exclusive development cutoff is `2025-07-13T22:00:00Z`; July 14 onward
remains protected. Each later market row read,
certification, scheduler action, publication, and activation still requires the
applicable approval. The Final Sealed 252-Session Holdout and forward values
remain inaccessible.

The verified custody successor preserves originals under `data/source_snapshots`
and keeps `data/dbn/tbbo` usable through 192 registered hardlink pairs. The
separately approved deletion removed only the 192 displaced physical copies,
reclaiming about 8.87 GB; all other source and rollback evidence remains protected.
See the [verified deletion result](reports/custody_successor_v1/DELETION_RESULT.md).

When a milestone changes the active stage, status, or next goal, check
`PROJECT_OUTLINE.md`, `CURRENT_WORKFLOW.md`, and `README.md`; update only
affected files or record `NO_CHANGE` in the task report. Change `AGENTS.md`
only when durable policy changes.
