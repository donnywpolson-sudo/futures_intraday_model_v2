# Plan 3: ES reversal after a large opening move

**Status: standalone draft; planning only. Priority: third in the user's pasted text. Admissibility needs particular attention.**

## Research question and frozen rule

Does a large same-contract move from the previous eligible cash close through the first five cash-session minutes reverse during the hour after a delayed entry?

| Component | Fixed specification |
| --- | --- |
| Market | ES; one standard contract |
| Observation | 06:35:05 Pacific (09:35:05 New York) |
| Current price | Close of the exact 06:34-06:35 Pacific minute |
| Prior price | Same actual contract's close of the exact 12:59-13:00 minute on the previous eligible full-length cash session |
| Opening move | m = (current close - prior close) / tick size |
| History | Corresponding opening moves from the preceding 60 calendar-eligible full sessions; current session excluded |
| Scale | Sample standard deviation of those 60 moves, denominator 59 |
| Event | abs(m) strictly exceeds the historical standard deviation |
| Direction | -sign(m) on an event; otherwise no trade |
| Entry/exit references | 06:36 and 07:36 Pacific (09:36 and 10:36 New York) |
| Search budget | One 60-session lookback, one standard-deviation cutoff, symmetric long/short rule |

Compute standard deviation around the historical sample mean, but compare the absolute raw current move to that standard deviation: abs(m) > sd. Do not silently replace this with abs(m - historical_mean) > sd; that is a different hypothesis. Equality abstains. A nonpositive or nonfinite scale is unavailable, not an automatic all-trade rule.

Determine the previous eligible session and preceding 60-session history from the frozen calendar, not the last 60 surviving observed moves. Require all 60 valid historical moves; a missing one makes the current signal unavailable, without expanding the lookback or imputing. A historical move itself requires its preceding eligible close, so 60 observations may need 61 prior session dates. Keep that context inside the permitted interval; identify initial warm-up dates from the calendar before reading outcomes.

Each historical move must compare prices within its own causally selected actual contract. The 60 daily moves may span successive ES contracts with verified compatible tick units; never compute a daily move by subtracting different contracts. This is a daily-move history across rolls, not a 60-day price difference or an implicit promise of 60 days in today's contract.

Update the trailing input statistic causally each eligible session, including later evaluation sessions, using prior opening observations only. Do not use their subsequent reversal outcomes or full-window estimates. This update is part of the frozen rule, not model reselection. The scheduled exit's minute bar makes its target nominally available no earlier than 07:37:05, subject to actual eligibility.

## Prior-search and feasibility gate

Inspect the overnight inventory reversal terminal closure identified in the earlier closing proposal, and the closed micro-exhaustion/flow/fixed-hour records. The saved closing-proposal report characterizes overnight reversal as inconclusive for coverage, not a completed economic rejection. Verify that characterization against the authoritative declaration and terminal record before admission.

This proposal may be substantially the same scientific family: prior-close displacement followed by cash-open reversal. Compare the exact reference, event threshold, decision time, holding period, market and closure's retry scope. If the proposal is a prohibited retry, stop with that finding. If the existing record permits a coverage-remediation successor, preserve that lineage and disposition; do not register it as unrelated novelty or quietly rerun it.

The pasted text attributes the motivation to Grant, Wolf and Yu, *Intraday Price Reversals in the US Stock Index Futures Market: A 15-Year Study* (2005), with stronger historical positive-opening effects and a material cost warning. The symmetric rule here is the user's proposed adaptation; do not switch to short-only after inspecting results. Source supplied by the user: [paper record](https://researchwith.montclair.edu/en/publications/intraday-price-reversals-in-the-us-stock-index-futures-market-a-1).

Report positive- and negative-opening event counts and returns as descriptive predeclared strata. Neither side may replace the symmetric primary rule. Small or one-sided support must remain visible and can restrict interpretation even if the aggregate screen passes.

## Plan-specific synthetic checks

- Sixty known moves reproduce sample standard deviation with denominator 59.
- Changing the current move changes the event but never its past-only scale.
- A nonzero historical mean distinguishes the frozen raw-move cutoff from a centered z-score rule.
- Equality abstains; zero variance and a missing history member are unavailable.
- Holiday/early-close selection follows the calendar; roll pairs never mix contracts.
- A large positive move produces a short and a large negative move a long.
- Warm-up needs enough prior-close context; no 2017 value is read implicitly.
- Future reversal outcomes do not affect the next day's scale, event IDs or signs.

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

## Frozen evaluation and decision rules

The numerical choices here are proposed design choices, not literature estimates or a claim that statistical calibration has passed. Freeze them before historical access; document any necessary pre-outcome revision as a successor.

For a valid nonzero signal, let r be the entry-to-exit price difference in ticks and s the frozen direction. Report g = s*r. On valid no-event dates, the policy's daily result is zero. Calendar-ineligible dates have a documented zero policy result; unavailable decisions and unresolved executions remain missing, never zero.

Use no trade, always long and always short as comparisons. Long/short comparisons enter and exit on exactly the rule's event dates and references; they do not use the rule's sign. They are fixed diagnostics, not alternative candidates. Two primary contrasts are mean daily gross signed ticks versus no trade, and paired mean daily gross improvement over always long. Report always short as a declared diagnostic. This tests directional value on the declared event population; it does not independently establish that the event timing beats every alternative time.

Treat all three pasted hypotheses as one prospective family: two primary contrasts each, six reserved claims. Preserve those six slots even if only this plan runs or another is inadmissible; do not recycle unused slots. Preserve the full prior-search ledger, prepared alternatives, failed studies and time-bucket exposure. This small family adjustment cannot correct unknown lifetime adaptive search or establish alpha.

Proposed inference reuses the existing stationary-bootstrap index kernel: 12,000 draws, mean block length 20 weekdays, seed 20260910 plus zero-based window index. Resample each full 63-weekday grid, preserving calendar dispositions and missing masks; use identical indices for all contrasts and plans. Pool sampled sums/counts across windows, not unweighted window means. Use the 100th ordered bootstrap mean, one-based with no interpolation, for each one-sided lower bound (nominal 5%/6 tail). Empty/nonfinite draws invalidate inference; never drop or retry them. Block lengths 5 and 63 are fixed fragility checks using the same draw count and seed convention; they cannot rescue primary failure.

Support floors: at least 40 fully resolved eligible-session decisions per window, at least 10 resolved nonzero events per window, and at least 100 resolved nonzero events overall. All scheduled dates require a reconciled disposition. These are adequacy floors, not proof of power. Before historical access, exercise the inference under synthetic dependent nulls and known effects, declare calibration/power acceptance limits and the minimum economically relevant effect, and report Monte Carlo uncertainty. If a valid calibrated inference contract cannot be established, keep inference descriptive and support unestablished.

Report separate outcomes:

1. **Supported in this development test:** complete eligible-session accounting, no unavailable required decisions or unresolved scheduled executions, support and inference checks pass, both primary means and adjusted lower bounds are positive, and both contrasts are positive in at least six of eight windows. Call it robust only if both fixed block-length checks also retain positive bounds.
2. **Not supported:** complete, adequately supported, valid evaluation fails the frozen statistical or consistency criteria. This rejects a supported lead under this specification, not every related relationship.
3. **Insufficient evidence:** inadequate events, unresolved coverage, invalid inference or an incomplete run. Do not turn this into PASS or proof of no effect.

## Assumed costs and later execution challenge

Keep gross predictability and trading economics separate. Reuse the reference plan's ES round-trip price allowances of 3, 6 and 12 ticks, each plus the declared $10 round-trip fee assumption. Validate tick value from applicable instrument metadata; total cost in ticks is allowance + 10/tick_value_usd. Count fees once. These are hypothetical scenarios, not measured historical costs or guaranteed conservative bounds.

The sign/event rule does not depend on cost, so reprice its one fixed trade ledger across scenarios; no separate cost-driven signal search is needed. A potentially economic development candidate additionally requires positive mean daily net dollars under the 6-tick-plus-fee scenario and positive net totals in at least six of eight windows. Report the 3/12-tick scenarios regardless of sign; neither rescues failure. This economic screen is descriptive, not a new alpha significance claim.

Report event counts, opportunity coverage, gross/net daily and window results, both directional comparisons, drawdown, worst day, profit concentration, and break-even all-in cost per trade (gross ticks / resolved trades). Missing executions prevent a complete break-even or net result. Do not claim capital returns, portfolio Sharpe, capacity or prop-firm suitability.

Any promising result only motivates a separately frozen, authorized L1 execution challenge on explicitly permitted dates. Define exact same-contract bid/ask timestamps, side-correct prices, freshness, missing quotes, fees and additional slippage before quote access. Avoid counting assumed spread again on top of quote-side costs. Quote references do not guarantee fills. Protected dates remain protected even for an execution-only diagnostic; independent confirmation and deployment require their own valid protocols.

## Implementation sequence and acceptance

1. **Admissibility and freeze:** compare the actual signal, decision population and target with prior closure records; resolve the plan-specific overlap in this plan. Record all three proposed hypotheses before viewing any new result. Save one concise durable declaration using current conventions, with exact date lists, metadata/release bindings, rule, missingness, inference and search accounting. If a prohibited retry cannot be distinguished, finish with an inadmissibility finding.
2. **Small local implementation:** trace the existing calendar/source admission, decision ledger, replay accounting, report store and bootstrap paths. Reuse compatible parts. The original L0 replay has cost-threshold model policy semantics: do not force this rule through it unchanged or weaken its guards. Add only the necessary signal/schedule adapter and affected contract handling. Keep configuration, signals, outcomes and reports separate; no generic strategy framework or new dependencies.
3. **Synthetic verification:** verify hand-calculated signal and P&L examples, timezone/DST boundaries, exact endpoint availability, same-contract enforcement, missing-calendar/input/entry/exit behavior, fee units, deterministic reload, future-outcome invariance, and a complete generated eight-window campaign. Include the plan-specific cases in this plan. Verify negative and no-signal controls, uncertainty arithmetic and no sensitivity rescue. Synthetic success proves mechanics only.
4. **Concrete execution preparation:** inventory release metadata without reading observations. Identify exact permitted ES partitions and required prior-session context; saved minute-decision forecasts from the old screen cannot supply new signal history. Keep warm-up within 2018-2022. Rehearse a bounded generated workload and propose one attempt with caps of 60 minutes, 6 GiB peak memory, 256 MiB aggregate new output and $0 provider spend. These are ceilings, not measured needs. If inadequate, revise before requesting approval. Use existing attempt-wide deadline, output reservation and one-use receipt enforcement; no automatic real-data retry.
5. **Approval boundary:** present the final declaration, synthetic results, exact source read set, destination, resource limits and any required narrow policy installation. Finish all safe preparation first. No historical materialization/evaluation occurs under this planning request. After a future approved materialization, reconcile the complete manifest before any separately scoped evaluation; authority does not transfer from task #1.
6. **Final report:** give the rule, prior-search/admissibility disposition, files changed, checks actually run, all measured resource limits, all eight results when authorized, predictive/economic statuses, assumptions and the single next boundary. Preserve failures and inactive artifacts. Do not update current research status to claim adoption or execution merely because this plan exists.

## Reference context

This plan follows task **#1** (01a07dd6-d797-79b2-90dc-e2592e6417de): frozen scope, chronological accounting, explicit assumptions, synthetic preparation, and separate historical authority. Inspect CURRENT_WORKFLOW.md and the current task-local preparation before implementation; version bindings can change. The completed four-market screen remains unsupported.

The hypothesis and literature rationale came from the user's pasted text. This planning task did not independently retrieve or validate the papers. The additional accounting, inference and support thresholds above are proposed implementation choices.

