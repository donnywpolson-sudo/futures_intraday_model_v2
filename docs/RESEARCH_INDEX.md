# Hypotheses, tests and results

Start here to find a scientific question and its associated files. This is a
human navigation index, not a new registry, execution interface or complete
historical-search census. [CURRENT_WORKFLOW.md](../CURRENT_WORKFLOW.md) owns
current status and next actions; existing declarations and receipts retain
their exact identities. Local evidence links may be absent in a clean export.

The [bounded discovery successor](DISCOVERY_LOOP.md) supplies the installed history,
budget and common-result integrations. Its operation allowlist is empty, and its
[qualification review](DISCOVERY_QUALIFICATION_REVIEW.md) keeps promotion blocked.

## How the pieces relate

**Hypothesis -> declared study -> candidates and benchmarks -> execution
attempts -> folds/tests -> result and diagnostics.**

- A hypothesis is the scientific question, such as whether ES adds information
  about NQ beyond NQ's own recent move.
- A study fixes the data, target, clock, models, comparisons and decision rule.
  Changing that question or rule creates a distinct scientific declaration.
- A candidate is a model or policy within the declared study. A sensitivity
  or benchmark is not an automatically selectable replacement winner.
- An attempt is one execution of a declared operation. A storage failure and
  approved replacement do not become two independent studies.
- Folds and fitted models are constituents of the study. Ninety-six fits do
  not imply ninety-six independent hypotheses.
- A diagnostic retains its parent and result-exposure history. Reusing a
  favorable diagnostic observation to design another study is exploratory.

Use the readable names below for navigation; keep machine IDs and revision
suffixes for exact provenance. Software, access-policy and reporting revisions
do not automatically change the scientific hypothesis.

## Current research family

Status descriptions below are the September 9, 2026 Pacific audit snapshot;
follow the workflow for subsequent adoption or execution changes.

| Study or related work | Scientific question / role | Evidence at audit |
| --- | --- | --- |
| [ES-to-NQ information transfer](#es-to-nq-information-transfer) | Does ES's completed five-minute change improve NQ's following 15-minute forecast beyond NQ alone? | Closed unsupported by owner after 16 fits/eight windows; `NO_SUPPORTED_LEAD`. No economic claim. |
| [Four-market retrospective prediction](#four-market-retrospective-prediction) | Do seven recent-hour features improve one-hour tick-change prediction in ES/NQ/CL/GC? | Completed, 96 fits / 32 windows; all four markets unsupported under the declared rule. |
| [Time-of-day diagnostic](#time-of-day-diagnostic) | How do the completed screen's fixed primary errors vary across predefined clock buckets? | Completed; descriptive reuse of the same forecasts, not independent confirmation. |
| [Original L0 trading-policy experiment](#original-l0-trading-policy-experiment) | Can the frozen selection and trading procedure produce credible net-dollar value under its execution/cost assumptions? | Inactive; historical admission and economic execution unresolved. |
| [ES closing-window proposal](#es-closing-window-proposal) | Proposed closing-window continuation question following the completed screen. | Declined and unadopted, as recorded in the workflow. |

### ES-to-NQ information transfer

| File role | Existing location |
| --- | --- |
| Question, prior exposure, splits and decision rule | [study.json](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1/study.json) |
| Source scope | [source_plan.json](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1/source_plan.json) |
| Implementation and checks | [transfer.py](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1/transfer.py), [test_transfer.py](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1/test_transfer.py) |
| Preparation result and limitations | [REPORT.md](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1/REPORT.md) |
| Access adoption and historical preparation | [adoption receipt](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/REPORT.md); [corrected replacement report and fitting proposal](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/REPORT.md) |
| Completed historical result | [evaluation report](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/evaluation_task_v1/REPORT.md), [assessment](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/evaluation_task_v1/assessment.json) — no supported predictive lead |
| Owner disposition | [Closed unsupported](../state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_adoption_v1/source_binding_repair_v1/evaluation_task_v1/closure.json); results and prior exposure preserved; no continuation authorized |

The completed comparison tested primary OLS against matched NQ-only OLS, zero
change and training mean across eight windows. Exact scores remain in the linked
evaluation report; the owner has closed this study as unsupported.

### Four-market retrospective prediction

| File role | Existing location |
| --- | --- |
| Scientific declaration | [study.json](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_preparation_v1/study.json) |
| Implementation and checks | [screen.py](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_preparation_v1/screen.py), [test_screen.py](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_preparation_v1/test_screen.py) |
| Installed study reference / adoption | [approved_retrospective_screen.json](../configs/approved_retrospective_screen.json), [adoption report](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_adoption_v1/REPORT.md) |
| Materialization and reconciliation | [replacement materialization report](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_materialization/replacement_v1/REPORT.md) |
| Earlier partial execution | [60-fit attempt report](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_execution/task_v1/REPORT.md) |
| Complete result, uncertainty, sensitivity and attempt lineage | [96-fit completion report](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_completion_v1/replacement/REPORT.md) |

The complete report is the result entry point. It links its exact operation,
saved outputs, reproduction checks and resource records. Retain the partial
attempt as history; do not count the completion as independent confirmation.

### Time-of-day diagnostic

Parent: the four-market retrospective screen above. Read the
[completed diagnostic report](../state/unpublished_evidence/l0_order_readiness_v1/screen_clock_diagnostic_v1/replacement/REPORT.md),
[implementation](../state/unpublished_evidence/l0_order_readiness_v1/screen_clock_diagnostic_v1/replacement/diagnostic.py)
and [checks](../state/unpublished_evidence/l0_order_readiness_v1/screen_clock_diagnostic_v1/replacement/test_diagnostic.py).
This is a linked diagnostic, not another supported hypothesis or economic test.

### Original L0 trading-policy experiment

Start with the [experiment guide](L0_ALPHA_CANDIDATE.md),
[current preparation configuration](../configs/l0_alpha_candidate_v9.json),
[candidate design](../src/futures_rebuild/l0_alpha_candidate.py),
[research implementation](../src/futures_rebuild/l0_alpha_research.py),
[replay](../src/futures_rebuild/l0_alpha_replay.py) and
[research checks](../tests/test_l0_alpha_research.py).
The [report contract](L0_REPORTING.md) describes the existing per-run
`candidate_reports/` catalogs and metric prerequisites. Shared reporting
software is installed; this does not imply a completed historical L0 run.

### ES closing-window proposal

Read the [proposal](../state/unpublished_evidence/l0_order_readiness_v1/es_close_continuation_proposal_v1/REPORT.md)
and [scientific proposal file](../state/unpublished_evidence/l0_order_readiness_v1/es_close_continuation_proposal_v1/study_proposal.json).
Its original report describes a proposal; the subsequent owner disposition
is in [CURRENT_WORKFLOW.md](../CURRENT_WORKFLOW.md).

## Earlier families and shared infrastructure

These are navigation links, not alternate active continuations.

| Family / role | Entry point |
| --- | --- |
| Closed ES flow continuation, micro-exhaustion and multifamily pilot | [Pipeline status and official result identities](../PROJECT_OUTLINE.md#2-current-status) |
| Rolling-hour predecessors and source-readiness dependencies | [Ordered remediation](ROLLING_HOUR_ORDERED_REMEDIATION.md), [rolling-hour design](ROLLING_HOUR_RESEARCH.md), [assumed-cost design](ALL_MARKET_COST_REMEDIATION_PLAN.md) |
| Profit-aligned research and failed statistical calibration | [Profit-aligned research](PROFIT_ALIGNED_RESEARCH.md) |
| Prepared strategy-discovery successor | [Strategy-discovery design](STRATEGY_DISCOVERY_SUCCESSOR.md) |
| Shared economic report generation / lifecycle | [Report contract](L0_REPORTING.md), [report calculations](../src/futures_rebuild/l0_alpha_reporting.py), [ReportingStore](../src/futures_rebuild/l0_alpha_report_store.py) |
| Cockpit report display | [candidate-reports.js](../src/futures_rebuild/live_cockpit/assets/candidate-reports.js) |
| Physical path roles and immutable legacy material | [Generated folder map](../PIPELINE_FOLDER_MAP.md), [legacy registry](LEGACY_WORKFLOWS.md), [naming and lineage](NAMING_AND_LINEAGE.md) |

## Organization and reporting audit

This audit inspected current workflow/design documents, report producer/store
and display code, the current screen implementations, and saved human reports.
It did not reopen market rows, forecasts or targets, rerun research, verify
every historical artifact, or visually test the cockpit. The directory
inventory found 181 immediate folders under `state/unpublished_evidence` and
26 under `l0_order_readiness_v1` excluding `__pycache__`; these are folder
counts, not hypothesis or independent-trial counts.

| Finding | Evidence and consequence | Disposition |
| --- | --- | --- |
| Scientific questions are buried in operational folders | L0 readiness contains two predictive studies, a diagnostic, a declined proposal and operational remediation. The folder name does not tell the owner which result answers which question. | This index groups existing paths by scientific meaning and file role. No evidence relocation. |
| A maintained guide contradicted the completed result | The L0 guide said no authentic screen read or fit had run, while the complete report records 96 fits. | Replaced the stale status paragraph with a workflow reference and complete-result link. |
| The catalog is not a repository-wide hypothesis inventory | `ReportingStore` emits catalogs inside one L0 run. `experiment_history` explicitly says supplied reports are not a complete census. The predictive screen uses distinct result artifacts. | Index provides navigation; automatic cross-study dashboard integration remains absent. Do not report the local catalog count as all hypotheses tried. |
| Important context is difficult to see in the cockpit | Its summary uses candidate name, stage, execution, dates and dollar metrics. Hypothesis, run/attempt identity, metric reasons and uncertainty mostly sit in expandable JSON. Predictive screen publication is not established. | Remaining presentation gap: a readable study/result summary should precede technical details. No installed UI changed in this audit. |
| Executable study code lives beside immutable evidence | The current screen's `screen.py` and transfer study's `transfer.py` live in preparation directories that other records reference. | Keep existing bound paths. For future unbound work, use ordinary source/tests locations and let this index link declarations and attempt outputs. No new runner or storage schema is needed for navigation. |

### Which statistics belong on a result?

The [metric guide](L0_REPORTING.md#which-statistics-belong-to-which-test)
distinguishes predictive scores, reference trading economics and account
returns. The completed screen's human report contains the central declared
error, benchmark, uncertainty, consistency, sensitivity and coverage statistics.
That does not establish that every statistic from every historical test is
centrally displayed. Current shared economic reports intentionally cannot
calculate account Sharpe or CAGR; they also do not expose a standard win-rate,
average-win/loss or profit-factor summary.

For a future readable study summary, use existing identities to show the
question and parent; stage/data scope; completed versus expected work;
primary effect and benchmark; uncertainty and declared decision; sensitivity
and missingness; then links to every attempt and detailed artifacts. Display
an unavailable metric with its reason, never as zero. These are presentation
recommendations, not retroactive changes to frozen selection rules.
