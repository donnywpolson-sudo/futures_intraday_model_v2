# Ordered remediation of the rolling-hour research pipeline

2026-09-07. Scope: ES/NQ/CL/GC, 2018–2022, owned inputs and free public
references. Preserve V8 and all prior results. This plan supersedes the order
in the free-feasibility report; it does not waive missing source evidence.

## Dependency order and acceptance

1. **Resolve source compatibility and the research objective.** Reconcile the
   entire declared price-source inventory before attempting to certify it.
   Determine whether a complete actual-contract alternative exists for the
   fixed pre-open selection rule. Do not repair a UTC-to-exchange-session roll
   mismatch by renaming contracts, filling prices or retrospectively selecting
   eligible trades. If no declared adequate alternative exists, choose the
   scientific successor before integration: either retain V8's profit selector
   and its source requirements, or explicitly select the one-hour predictive
   model by date-balanced MSE, with economics evaluated separately where valid.
   The existing zero-change/training-mean benchmarks and small feature family
   remain the baseline; any necessary change is declared before results.
   Acceptance: an explicit objective and source-compatible specification, not
   a document that merely says an input is certified.
2. **Repair calendar extraction and establish timing.** Fix the shared parser
   against the observed compact-workbook layouts and event dates. Verify every
   owned workbook/family pair, keeping exceptions and source hashes. Distinguish
   a schedule's contents from its original publication time and applicable
   dates. In particular, filenames and sheet names cannot supply a missing
   January 2019 schedule when the contents describe January 2020. Historical
   status-policy changes require received-time semantics and applicability,
   not file-existence flags. Acceptance: correct extraction plus complete
   evidence for whichever calendar knowledge the selected design consumes.
3. **Implement and test the selected design's causal adapter.** Decisions use
   only information available then. Keep clock dates with zero eligible rows;
   record absent features, contract transitions and unobservable future labels
   separately. Never use future availability to erase a decision or a losing
   trade. Features, labels and position valuation cannot cross contracts by
   stitching the continuous series. Define censored-label reporting before
   fitting; observed-only scores cannot establish full-population skill.
   Acceptance: causal negative tests and complete source-support measurements.
4. **Certify the source and bind the existing execution route.** Reuse the
   calendar verifier, fold certificate, training code and
   `CertifiedResearchGateway`. Bind the approved descriptive historical-use
   decision without duplicating its trial charges or pretending the legacy
   census is complete. Enforce the declared 252/505 distinct-date minima after
   all source/label restrictions. Acceptance: measured full-scope readiness
   and immutable registration for the chosen design.
5. **Run once, record results and freeze the historical models.** Execute the
   declared nested procedure, retain failures and scores, perform the
   predetermined final fits and reproduce predictions after reload. Preserve
   final confirmation and forward data unread. A poor score is a valid outcome;
   a failed final fit is not a completed model. Do not tune the declaration
   after seeing the historical results.

Steps 1 and 2 contain independent work and may proceed together, but step 3
cannot silently choose a new learning objective, and steps 4–5 cannot precede
source readiness. Synthetic tests prove implementation, not historical alpha.

## Implementation status

The source-inventory reconciliation and calendar-parser repair are complete:
80 declared OHLCV sidecars checked, all 192 owned workbook/family combinations
parsed, and nine focused parser regressions passed. The user subsequently chose
a predictive successor selected by date-balanced MSE, with observed-contract
forecasts and labels requiring valid endpoints rather than a complete future
minute path. V9 implements the generated-data mechanics of that decision;
V8 is preserved. Current acceptance and the still-incomplete historical route
are recorded in [predictive status](ROLLING_HOUR_PREDICTIVE_STATUS.md).

The [task report](../state/unpublished_evidence/rolling_hour_assumed_cost_v1/ordered_remediation_v1/task_report.md)
records completed checks, exact remaining requirements and the
status of original steps 5–6. They become fully complete only after historical
validation and execution respectively, not when this plan or code is written.
