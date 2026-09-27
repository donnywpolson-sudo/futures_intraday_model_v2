# Plan 1: ES rest-of-day momentum into the cash close

**Status: CLOSED BY OWNER on 2026-09-14, without historical evaluation. No historical predictive or economic result was established.**

The [closure record](../state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/closure.json) ends this preparation. Further preparation or execution requires a new owner request. The plan below is retained historical context, including its unresolved inference and source requirements; its implementation sequence is no longer an active work plan. Original evidence and all three hypotheses/six reserved comparison slots remain preserved. Opening-range breakout was later withdrawn by the owner; this closure selected no replacement.

This revision incorporates the owner's prediction-first decision and removes the failed inference method from the passing criteria. The signal, times, dates, comparisons and prior-search history are unchanged. Earlier hash-bound preparation remains historical evidence; its [original plan bytes are preserved](../state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/ES_CLOSE_MOMENTUM_PLAN.before_revision.c00bc812e5deed03c74c7df9176d3e67a7745baeb40285d20bc23f0a19a94802.md). This document edit does not adopt a method or refresh execution bindings.

## Research question and frozen rule

Does ES's displacement from the previous eligible cash close to 12:30 Pacific predict positive signed price changes from the delayed 12:31 entry reference to the 13:00 exit reference?

| Component | Fixed specification |
| --- | --- |
| Market | ES; one standard contract; at most one event per eligible session |
| Observation | 12:30:05 Pacific (15:30:05 New York) |
| Current price | Close of the exact 12:29-12:30 Pacific minute |
| Prior price | Same actual contract's close of the exact 12:59-13:00 minute on the previous eligible full-length cash session |
| Signal | x = (current close - prior close) / tick size |
| Direction | sign(x); exactly zero means no trade |
| Entry reference | Opening print of the 12:31 Pacific minute (15:31 New York) |
| Exit reference | Opening print of the 13:00 Pacific minute (16:00 New York) |
| Holding period | 29 minutes between reference-minute starts |
| Search budget | One sign rule; no fitted slope, threshold, earlier-window control, stops or profit targets |

Determine the previous eligible full-length cash session from the frozen calendar, not from surviving price observations. This intentionally skips calendar-ineligible holidays and shortened sessions; it is not the previous weekday and not necessarily the immediately preceding shortened cash close. Freeze this interpretation explicitly. If that session's required same-contract close is missing, abstain with unavailable-input status; do not search farther back. The first in-scope session without an in-scope prior reference is a declared warm-up date, not an authorization to open 2017 values.

At the decision, bind the selected actual contract and require both signal prices to belong to it. If the supplied continuous series contains only yesterday's different contract, that is insufficient; do not subtract the two contracts or fetch an undeclared replacement. A contract selected today may use yesterday's price for that same contract only where the admitted release actually contains it.

The exit is a futures reference near the equity cash close, not an equity auction fill or a statement that futures trading ends at 13:00. Its OHLCV bar is not complete until 13:01; nominal outcome availability is at least 13:01:05, subject to later recorded eligibility.

## Prior-search and feasibility gate

The repository already contains an owner-declined, unadopted closing-window proposal at state/unpublished_evidence/l0_order_readiness_v1/es_close_continuation_proposal_v1/REPORT.md. Preserve it. That proposal used a 12:29:05 decision, prior-weekday 13:00 opening reference, 12:30 entry, log-return regression and an earlier-window contrast. This plan follows the newly supplied fixed-sign proposal instead. Record the relationship and every changed choice; do not claim the same economic idea is newly discovered or already approved for execution.

Also compare with the closed short-horizon high-volume continuation and fixed-hour multifamily mechanisms. The candidate distinction is cumulative prior-cash-close displacement, a single scheduled late-day event and a 29-minute delayed-entry target. Literature motivation does not by itself establish admissibility.

Verify full-session calendar coverage and source coverage through the 13:00 reference minute. The original screen's last 11:59:05 decision and one-hour targets do not implement this later decision. If calendar availability cannot be established, finish preparation with that blocker; do not silently replace this full-session hypothesis with the earlier fixed-weekday proposal.

The pasted text attributes the motivation to Baltussen, Da, Lammers and Martens, *Hedging Demand and Market Intraday Momentum* (2021). Hedging/rebalancing is a possible explanation; the experiment tests an association and cannot identify its cause. The pasted source is [the supplied paper link](https://academicweb.nd.edu/~zda/intramom.pdf).

## Plan-specific synthetic checks

- Positive, negative and exactly zero prior-close displacement produce long, short and abstain.
- Weekend, holiday and early-close sequences select the preceding eligible full session from calendar metadata.
- Missing prior-session price does not search another date; a roll cannot create a spurious displacement.
- The 12:30 completed close is unavailable before the decision; entry is 12:31, never 12:30.
- The 13:00 opening reference belongs to the declared contract; absent exit remains unresolved.
- An always-rising synthetic window with uninformative signs cannot pass the incremental always-long contrast.

## Task and authority

Prepare this experiment in the existing futures research pipeline through local implementation and synthetic verification. Read the canonical root's AGENTS.md and CURRENT_WORKFLOW.md first, confirm repository identity, and preserve unrelated work. This plan is a draft specification, not a research declaration, installed policy, receipt, or authorization to read market values.

Proceed with ordinary local preparation when this plan is assigned for implementation. Before any historical read/evaluation or access-policy installation, present the exact prepared operation and obtain the applicable approval unless the current task already authorizes that bounded action. Keep provider/network access, protected dates, publication, activation and trading outside this task. Do not create another pipeline, revive a retired runner, or alter the ongoing ES-to-NQ study.

## Scope and chronology

- ES only; one standard contract in scenario calculations. Use owned, permitted, verified immutable OHLCV-1m releases and necessary instrument/calendar metadata.
- Development interval: 2018-2022 only. Reuse the exact eight 63-weekday audit-date lists in the current screen lineage, January 26, 2021 through December 30, 2022. Copy date membership, not another experiment's clock, fitted-model requirements or authority.
- These dates have already informed research. Call this a chronological development evaluation, never pristine out-of-sample confirmation.
- Keep the independent weekday ledger, including holidays, early closes and missing dates. Trade eligibility requires a verified full-length US cash session, established from calendar information available by the decision. Unknown calendar eligibility is unavailable, not an ordinary zero-return day.
- Pacific display/decision zone: America/Los_Angeles; canonical timestamps: UTC. Interpret the cash-session rules in America/New_York with timezone conversion, not a hard-coded UTC offset.
- One frozen rule; no machine learning, candidate selection, inner-fold tuning, parameter grid, additional markets or automatic expansion. Prior history supplies causal inputs only. Never use an audit outcome to change the rule.
- Where a fitted transform or benchmark is introduced by an existing mandatory contract, preserve training-only estimation, strict outcome maturity, purge and embargo. Do not import a 505-training-date model-fit requirement into a rule that fits no model.

## Source timing and complete accounting

A close at time T means the close of the exact completed minute [T-1 minute,T). A minute opening reference at T means the first recorded trade in [T,T+1 minute), not proof of a fill exactly at T. Require the signal bar's recorded eligibility, applicable metadata availability, and bar end plus five seconds to precede or equal the decision. The five-second rule is a replay assumption; it does not establish original delivery or remove stored revisions.

Choose the actual ES contract causally using the admitted mapping. Match instrument, publisher and units throughout each signal price difference and entry/exit pair. No back-adjusted splice, nearby substitute, future-volume-based contract choice or favorable roll exclusion. If causal mapping/receipt cannot be established, report that limitation and do not relabel a strict source certificate as passing.

Persist the scheduled population and signal/abstention records before attaching future prices. Distinguish calendar-ineligible, missing causal input, valid no-event, scheduled trade, missing target, unresolved entry and unresolved exit. Missing bars alone do not prove closure or no trading. Future prices cannot alter decision IDs or directions.

A scheduled trade with a missing entry reference is unresolved, not retrospectively cancelled. A missing exit preserves the open/unresolved position. Stop complete economic conclusions; do not drop the trade, carry forward a price, use another bar or contract, or resume as though flat. Observed-target diagnostics may still be reported with their exact missingness mask and an explicit conditional interpretation; they cannot satisfy the complete-study support gate below.

## Predictive evaluation and pending inference correction

The numerical choices here are proposed design choices, not literature estimates or a claim that statistical calibration has passed. Freeze them before historical access; document any necessary pre-outcome revision as a successor.

For a valid nonzero signal, let r be the entry-to-exit price difference in ticks and s the frozen direction. Report g = s*r. On valid no-event dates, the policy's daily result is zero. Calendar-ineligible dates have a documented zero policy result; unavailable decisions and unresolved executions remain missing, never zero.

Use no trade, always long and always short as comparisons. Long/short comparisons enter and exit on exactly the rule's event dates and references; they do not use the rule's sign. They are fixed diagnostics, not alternative candidates. Two primary contrasts are mean daily gross signed ticks versus no trade, and paired mean daily gross improvement over always long. Report always short as a declared diagnostic. This tests directional value on the declared event population; it does not independently establish that the event timing beats every alternative time.

Treat all three pasted hypotheses as one prospective family: two primary contrasts each, six reserved claims. Preserve those six slots even if only this plan runs or another is inadmissible; do not recycle unused slots. Preserve the full prior-search ledger, prepared alternatives, failed studies and time-bucket exposure. This small family adjustment cannot correct unknown lifetime adaptive search or establish alpha.

The originally proposed within-window stationary bootstrap **failed synthetic false-positive calibration**. Its 20-weekday blocks within 63-weekday windows understated uncertainty in the checked independent-noise case; rare-loss scenarios exposed additional finite-sample limitations. Its 12,000 draws, 100th order statistic and 5/63-day sensitivities remain recorded in the [calibration and diagnosis evidence](../state/unpublished_evidence/l0_order_readiness_v1/es_close_momentum_readiness_v1/REPORT.md). They are not an approved pass/fail method. More draws or selecting a favorable sensitivity does not repair the failure.

Before historical execution, specify and synthetically validate one corrected uncertainty method for the **same mean signed-tick contrasts**, then freeze its exact procedure and identity. Preserve paired daily observations, common missing-date masks, chronological accounting, pooled sums/counts across windows, the 5% family-error target across six reserved claims, and the six-of-eight positive-window requirement. Record dependence/tail assumptions, numerical and empty/nonfinite-case handling, calibration acceptance limits and Monte Carlo uncertainty. Assess power at explicitly declared predictive effect sizes; no owner profit or drawdown hurdle is required. Any fixed sensitivity checks must be declared with the corrected method before historical access and cannot rescue primary failure. Do not choose methods using this study's historical outcomes or silently change the endpoint to directional hit rate. The prepared direction-only alternative changes the question and remains unadopted.

Support floors: at least 40 fully resolved eligible-session decisions per window, at least 10 resolved nonzero events per window, and at least 100 resolved nonzero events overall. All scheduled dates require a reconciled disposition. These are adequacy floors, not proof of power. If a defensible inference method cannot be established under stated assumptions, support remains unestablished. Removing the failed method from this plan does not itself resolve that prerequisite or authorize a descriptive historical run.

Report separate outcomes:

1. **Supported in this development test:** available only after a corrected inference method is validated and frozen. Complete eligible-session accounting, no unavailable required decisions or unresolved scheduled executions, support and inference checks pass, both primary means and corrected adjusted lower bounds are positive, and both contrasts are positive in at least six of eight windows. Any robustness claim must also satisfy the corrected method's predeclared sensitivity checks. The failed original bounds cannot meet this gate.
2. **Not supported:** complete, adequately supported, valid evaluation fails the frozen statistical or consistency criteria. This rejects a supported lead under this specification, not every related relationship.
3. **Insufficient evidence:** inadequate events, unresolved coverage, invalid inference or an incomplete run. Do not turn this into PASS or proof of no effect.

## Assumed costs and later execution challenge

Keep gross predictability and trading economics separate. Reuse the reference plan's ES round-trip price allowances of 3, 6 and 12 ticks, each plus the declared $10 round-trip fee assumption. Validate tick value from applicable instrument metadata; total cost in ticks is allowance + 10/tick_value_usd. Count fees once. These are hypothetical scenarios, not measured historical costs or guaranteed conservative bounds.

The sign/event rule does not depend on cost, so reprice its one fixed trade ledger across scenarios; no separate cost-driven signal search is needed. Report all three scenarios regardless of sign. Costs, net dollars and drawdown are **diagnostics only**: there is no economic pass/fail gate, minimum net-profit requirement or owner drawdown hurdle for this predictive study. A favorable cost scenario cannot rescue a failed predictive result, and an unfavorable scenario does not change its predictive classification. Economic viability remains a separate, unestablished claim.

Report event counts, opportunity coverage, gross/net daily and window results, both directional comparisons, drawdown, worst day, profit concentration, and break-even all-in cost per trade (gross ticks / resolved trades). Missing executions prevent a complete break-even or net result. Do not claim capital returns, portfolio Sharpe, capacity or prop-firm suitability.

Any promising result only motivates a separately frozen, authorized L1 execution challenge on explicitly permitted dates. Define exact same-contract bid/ask timestamps, side-correct prices, freshness, missing quotes, fees and additional slippage before quote access. Avoid counting assumed spread again on top of quote-side costs. Quote references do not guarantee fills. Protected dates remain protected even for an execution-only diagnostic; independent confirmation and deployment require their own valid protocols.

## Implementation sequence and acceptance

1. **Admissibility and freeze:** reuse the completed prior-search review and recorded three-hypothesis family; investigate only a new discrepancy. Preserve the original declaration and prediction-scope record. Resolve and validate the inference correction described above before freezing a successor declaration for historical access. Bind exact date lists, metadata/releases, the unchanged rule, missingness, prediction-only acceptance and prior-search accounting. Do not relabel the old declaration or preparation as current after this plan edit. If a prohibited retry cannot be distinguished, finish with an inadmissibility finding.
2. **Small local implementation:** trace the existing calendar/source admission, decision ledger, replay accounting, report store and bootstrap paths. Reuse compatible parts. The original L0 replay has cost-threshold model policy semantics: do not force this rule through it unchanged or weaken its guards. Add only the necessary signal/schedule adapter and affected contract handling. Keep configuration, signals, outcomes and reports separate; no generic strategy framework or new dependencies.
3. **Synthetic verification:** reuse matching completed checks for unchanged signal mechanics, timezone/DST boundaries, exact endpoint availability, same-contract enforcement, missing-calendar/input/entry/exit behavior, diagnostic fee arithmetic, deterministic reload and future-outcome invariance. Exercise the corrected inference against independent arithmetic, dependent and tail-sensitive nulls, and declared predictive effects; preserve the original failures. Check that economic diagnostics cannot affect predictive acceptance and that sensitivities cannot rescue primary failure. Repeat other checks only if changed dependencies require it. Synthetic success proves mechanics and the stated calibration scope only.
4. **Concrete execution preparation:** inventory release metadata without reading observations. Identify exact permitted ES partitions and required prior-session context; saved minute-decision forecasts from the old screen cannot supply new signal history. Keep warm-up within 2018-2022. Rehearse a bounded generated workload and propose one attempt with caps of 60 minutes, 6 GiB peak memory, 256 MiB aggregate new output and $0 provider spend. These are ceilings, not measured needs. If inadequate, revise before requesting approval. Use existing attempt-wide deadline, output reservation and one-use receipt enforcement; no automatic real-data retry.
5. **Approval boundary:** present the final declaration, synthetic results, exact source read set, destination, resource limits and any required narrow policy installation. Finish all safe preparation first. No historical materialization/evaluation occurs under this planning request. After a future approved materialization, reconcile the complete manifest before any separately scoped evaluation; authority does not transfer from task #1.
6. **Final report:** give the rule, prior-search/admissibility disposition, files changed, checks actually run, all measured resource limits, all eight results when authorized, predictive/economic statuses, assumptions and the single next boundary. Preserve failures and inactive artifacts. Do not update current research status to claim adoption or execution merely because this plan exists.

## Reference context

This plan follows task **#1** (01a07dd6-d797-79b2-90dc-e2592e6417de): frozen scope, chronological accounting, explicit assumptions, synthetic preparation, and separate historical authority. Inspect CURRENT_WORKFLOW.md and the current task-local preparation before implementation; version bindings can change. The completed four-market screen remains unsupported.

The hypothesis and literature rationale came from the user's pasted text. This planning task did not independently retrieve or validate the papers. The additional accounting, inference and support thresholds above are proposed implementation choices.
