# Codex remediation plan: all 41 markets without pre-2025 Level 1 data

Prepared and implemented 2026-09-07. **Inactive V8 successor; no historical
execution approval.** The implementation brief below remains the scope contract.
Current configuration: `configs/rolling_hour_research_v8.json`, bundle
`46fb3c87f517ec13121d3d8015d7bdf8fc3bcb720f240622c790b5fa65a3b02c`.
V7 and its bound implementation remain unchanged. See the current
[implementation report](../state/unpublished_evidence/rolling_hour_assumed_cost_v1/task_report.md)
for measured acceptance and remaining evidence gaps.

Subsequent [descriptive historical-use decision](ROLLING_HOUR_DESCRIPTIVE_ADMISSIBILITY.md):
reuse is admissible at the design level with the 65 missing references and 105
penalty disclosed. This does not authorize execution or resolve calendar/roll
evidence, source certification or the applicable gateway requirements.

## Frozen numerical choices and implementation order

All amounts below are **round trip**, per one standard contract. They are
uncalibrated research assumptions, not estimated historical fills or upper bounds.

| Component | Base | Adverse (selection) | Severe |
|---|---:|---:|---:|
| Assumed spread, all 41 markets | 1 tick | 2 ticks | 4 ticks |
| Additional execution allowance, ES/NQ | 2 ticks | 4 ticks | 8 ticks |
| Additional execution allowance, CL/GC and other 37 compatibility defaults | 4 ticks | 8 ticks | 16 ticks |
| Assumed fee, all markets | $10 | $10 | $10 |

Thus total allowances are 3/6/12 ticks for ES/NQ and 5/10/20 ticks otherwise,
plus the fee. Dollar tick values must come from applicable certified contract
units; synthetic compatibility uses explicitly synthetic units. No historical
fee evidence is asserted. The fee interval validator supports the greater of
$10 and a verified applicable fee, but adding such evidence changes the frozen
policy and requires rebinding before fitting. There is no session, volatility
or separately fitted market/year variation in V8.

Implementation follows these six dependencies:

1. Read the hash-bound owned inventory metadata and create all 697 market/year
   dispositions (2010-2026). Catalog intervals do not prove row coverage,
   listing dates, contract units or point-in-time applicability.
2. Bind the numerical assumptions above before any successor results.
3. Select on mean daily adverse assumed-net dollars; replay complete policies
   under all three schedules. Report fixed-trade repricing and additional
   break-even cost headroom separately from those full replays.
4. Run signed EDGE as an isolated diagnostic using the previous 20 completed
   same-contract sessions, updated once before each session. Minute grids retain
   gaps; session/break separators prevent overnight returns being treated as
   minute returns. No future data, roll pooling, price shifts or zero imputation.
5. Run all-41 cost/EDGE interface checks and the complete unchanged-seed
   ES/NQ/CL/GC nested synthetic procedure. Other 37 nested runs remain not run.
6. Freeze and account for the successor: 60 new configuration identities,
   preserved prior charges, no duplicate preparation or compatibility charges.
   Historical execution remains blocked by the separate evidence and access route.

Fitting retains average-date squared loss with weight `1/(D*n_date)` and total
weight one. Ridge uses training-only weighted standardization and penalties
0.1/1/10 on standardized coefficients, with an unpenalized intercept. XGBoost
uses half weighted squared loss, lambda 0.01, alpha/gamma zero, minimum child
weight 0.05 and the inherited 20-distinct-date leaf support. All eligible
overlapping labels remain; neither independence nor uniqueness weighting is
assumed. Inner/outer/final fitting share the same kernel and 252/505/505 observed
date minima. The selector targets assumed economics; equal-date forecast MSE
against zero and eligible outer-training row-mean forecasts remains descriptive.

Reproduce with `.venv/Scripts/python.exe -m futures_rebuild.rolling_hour_assumed_preparation validate` and the `synthetic`
action. Outputs are immutable under the bundle directory: a differing rerun
cannot overwrite saved evidence. The CLI has no historical action.

## Feasibility and intended outcome

It is possible to implement one consistent, reproducible research procedure
for all 41 markets and all applicable owned years without historical L1 data.
Its economics must be described as simulations under explicit cost assumptions.
It cannot reconstruct actual historical spreads, queue positions, fills or
execution shortfall. A high assumed cost is not a proven upper bound, and
neither favorable nor unfavorable assumed-cost P&L establishes actual profitability.

The intended deliverable is a frozen descriptive research successor, with
transparent cost sensitivity and per-market predictive evaluation. Missing L1
must cease to be an input requirement for this successor. Other missing inputs,
insufficient histories and existing research-use restrictions remain explicit.
Every market/year must receive a metadata disposition; success does not mean
every one must produce a passing or even trainable model. **Implementation
compatibility covers all 41 markets; the current research focus remains the
rolling-hour v7 scope: ES, NQ, CL and GC, 2018-2022.** This is the meaning of
the user's current Tier 1 focus in this plan, not a redefinition of other
historical Tier 1 registries. Expanding research execution to the other 37
markets or additional years is a later explicit scope decision.

## Scientific sequence to preserve

Incorporate the six accepted recommendations in this order. The implementation
phases below supply their dependencies and validation; they do not replace them.

1. Separate documented fees/contract units from assumed execution allowances.
2. Freeze base/adverse/severe schedules, with justified market/session differences
   and only causal, predetermined time variation.
3. Keep profit-based inner selection under one predetermined adverse schedule.
4. Replay the entire policy under each schedule without outer-result selection.
5. Report cost sensitivity and break-even cost headroom as descriptive quantities.
6. Implement EDGE as a separately reported diagnostic and validate predetermined
   synthetic cases; do not calibrate with sealed recent-year quotes.

## Implementation brief for Codex

Work in `C:\Users\donny\Desktop\futures_intraday_model_v2`.

Implement an inactive descriptive research successor supporting all 41 standard
markets and their applicable owned history, using predeclared execution-cost
assumptions without requiring pre-2025 BBO, TBBO, trades or order-book purchases.
Read `AGENTS.md`, `CURRENT_WORKFLOW.md`, the cost-alternatives review, and the
owned-record reconciliation before editing. Inspect current code, exact source
catalogs and task-relevant Git status. Preserve unrelated work, immutable
evidence, accepted source bytes, all predecessor identities and failed results.

Build the shared all-market capability, then complete current Tier 1 research
mechanics first. Do not automatically run 41 historical experiments or treat
all-market compatibility as authority to broaden the current four-market trial.
Continue all authorized independent local implementation and synthetic work.
Do not stop because historical calendar evidence, statistical qualification or
old L1 data are unavailable. Do not bypass a missing dependency to execute
history. Follow the dependency order and acceptance conditions below. Reuse
existing owners; do not create a general backtesting or cost-estimation framework.

### 1. Reconcile exact markets, years, units and access

Use `configs/source_contract.json` and its hash-bound canonical inventory as
the starting universe. The verified standard-root list at plan preparation is:

```text
ES NQ RTY YM CL NG RB HO GC SI HG PL SR3 SR1 ZQ TN ZT ZF ZN ZB UB
6A 6B 6C 6E 6J 6M 6N 6S ZC ZS ZL ZM ZW KE LE HE GF BTC ETH PA
```

Require exact set equality and 41 unique entries. Deferred micro roots are not
silently included. A changed source universe requires an explicit reconciliation.

The current canonical inventory's combined interval is
`[2010-06-06, 2026-09-01)`. Metadata identifies 699 OHLCV-1m, 701 statistics,
673 status and 691 definition DBN entries across these roots. These counts
describe catalog entries, including possible split periods, not certified
nonempty full-year histories. Reverify rather than hard-code the counts.

Create one metadata-only market/year matrix for 2010-2026, with separate
interval rows where a year is split. Cover all 697 market/year combinations
explicitly; do not imply that each market existed or had data in every year.
Future years enter only through verified source successors, not extrapolation.
For every row record:

- Exact source releases, periods and available schema families; distinguish
  raw custody, normalized release and certified eligibility.
- Contract/listing applicability, tick size, tick value, multiplier, quotation
  convention/currency and effective dates. Do not infer dollars from the root
  name or copy ES conventions into rates, FX, commodities or crypto futures.
- Calendar applicability/publication status, continuous-contract selection
  evidence and real-source readiness.
- Research-use classification and observed training support when authorized
  observations become available. Before that, mark support unmeasured.
- Explicit status: ready for a named purpose, reference-only, insufficient
  history, missing source, unverified applicability, or protected. Mark
  pre-listing periods only when records establish the listing boundary.

"All years supported by the implementation" is not permission to train on all
years. Preserve the 2018-2022 selection period, the separately governed
2023-to-development-cutoff route, and sealed holdout/forward restrictions.
Do not use 2010-2017 as additional fitting history merely because files exist.
Any expanded historical fitting/evaluation scope requires an explicit design
decision, declaration and applicable access approval. Retain partial-year
boundaries; no whole-year read across a protected cutoff.

### 2. Specify the smallest complete cost-assumption contract

Use the existing one-contract, marketable-entry interpretation. Specify dollar
costs per side and per round trip without ambiguous unit conversions. Bind
known contract units and fee schedules to effective dates. Unverified fee
amounts may be explicit scenario assumptions; fundamental price/unit identity
must be verified before historical dollar evaluation.

Prepare a complete numerical cost table for all 41 markets before historical
results. It must give the base/adverse/severe allowances, fee components,
effective-date rule, time variation if any, source or assumption rationale,
and exact treatment of unavailable inputs. Do not invent market/year evidence.
This plan does not supply numerical estimates that the literature failed to
establish. Codex must make the proposed numbers and their evidential status
reviewable as part of the successor freeze, before requesting historical access.

Start with the fewest defensible choices: market tick values, explicit fees
and a small ordered set of adverse tick allowances. Different dollar costs
across markets need not require 41 unrelated fitted models. A common rule may
produce market-specific costs through verified units and declared assumptions.
Add a small number of session categories only if justified and calendar-valid.
Do not create independently tuned market/year coefficients or a large cost grid.

Time variation must use a fixed, declared rule and decision-available inputs.
If lagged range/volatility is used to scale an allowance, label it as a stress
rule, not an empirically identified spread relationship. Do not backfill the
whole year's realized volatility into earlier costs. Distinguish choosing
present-day retrospective research assumptions from claiming historical
publication/knowledge of those assumptions.

Represent assumptions directly. **Never create synthetic bid/ask records,
provider identities, receive times or historical quote releases** to satisfy
the existing real-quote adapter. Preserve that adapter's certification rules.
Keep measured-quote evaluation separate and optional; lack of quotes in any
year, including 2025+, must not break the assumed-cost procedure.

No mandatory estimator fallback chain. Implement EDGE as one diagnostic,
using a specified implementation and fixed causal estimation window. It must
not become a forecast feature, candidate-selection input or mandatory cost
calibrator in the initial successor. Diagnostic failure preserves the selected
policy result. Moving it into selection later is a separately accounted change.

The same EDGE implementation must accept every admitted market through common
OHLC inputs, actual-contract identity and time-grid conventions. There is no
four-market whitelist or year cutoff inside the estimator. Nevertheless, its
applicability is conditional, not universal: require finite positive OHLC,
valid bar relationships, sufficient prescribed window support, and correct
same-contract boundaries. Sparse trading, unstable estimates and dependence
can undermine reliability even when a function returns a number. OHLCV volume
counts contracts, not necessarily individual trades; do not invent a trade
count to claim the estimator's sampling conditions have been verified.

For every attempted diagnostic interval, report an estimate with its support
and flags, or a precise unavailable/unsupported/unstable disposition. Keep the
original signed/raw estimate where the chosen implementation permits it and
predeclare any display transformation. Never replace failure with zero spread,
force a tick floor while calling it a measured estimate, shift nonpositive
prices, or remove those dates from the primary research calendar. The primary
assumed-cost procedure must work without any usable EDGE result.

Log-price estimators must report nonpositive prices as unsupported, not shift
prices or discard difficult dates. The primary tick/dollar allowance must work
with zero/negative transaction prices when contract rules permit them. Unknown
units or impossible source records still fail historical validation.

### 3. Preserve the research question and learning mathematics

Retain causal OHLCV-1m, minute forecasts, fixed one-hour continuous open-print
labels, strict feature/label separation, existing 5/10/16-feature packs, Ridge
and shallow XGBoost. Do not expand indicators, introduce new model families,
switch to triple barriers or OHLCV-1s, or use feature importance to expand scope.

Preserve v7's normalized date-balanced fitting, training-only preprocessing,
Ridge penalty grid, XGBoost weight/regularization conventions and leaf-date
support. Keep at least 252 distinct eligible inner-training dates and 505
outer-training dates, including final-refit support rules. Retain calendar and
row-count checks independently. Short-lived markets may be unsupported for a
declared fold; do not relax requirements or silently borrow another market's
observations. No pooled cross-market model is introduced by this expansion.

Keep all four roles explicit:

| Role | Successor requirement |
|---|---|
| Fitting | Existing normalized squared-error learning conventions. |
| Inner selection | Mean daily simulated stress-net dollars under exactly one frozen adverse cost schedule. |
| Outer predictive evaluation | Per-market date-balanced MSE, zero-change primary benchmark and eligible training-mean secondary benchmark; descriptive. |
| Economics/inference | Assumed-cost sensitivity is conditional; predictive significance and profitability qualification remain separate. |

Do not claim the profit-based selector chooses the most accurate forecast.
Do not turn descriptive metrics into promotion gates. Changing selection to
accuracy, worst-case optimization or a blend of objectives needs an explicit
new design decision; none is implied here.

### 4. Implement all-market replay and successor identity

Trace the existing fitting, inner selection, outer refit, final refit, replay
and report callers before changing shared functions. Relevant owners include
`rolling_hour_research.py`, `rolling_hour_source.py`,
`rolling_hour_preparation.py`, `rolling_hour_reference_audit.py`,
`profit_aligned_research.py` and `fixed_hour_terminal_successor.py`.

Current rolling-hour code inherits a four-market tuple and uses positional
market cost lookups. Remove those assumptions only in the new successor's
necessary path. Do not globally change predecessor market lists, mutate
historical mechanisms, or make old configurations silently mean 41 markets.
Use the existing source universe and a complete explicitly keyed cost policy.
Make the smallest coherent change that preserves the old quote-based behavior.

An assumed-cost entry has a causal open-print reference and an explicit
adverse adjustment, never a claim of a historical executable quote. Declare
how the allowance relates to transaction-price labels to avoid double-counting
spread costs. Preserve decision delay, next-boundary entry, fixed-hour exit,
flat-only entry, pending reservations and chronological occupancy. Do not infer
passive fills from touched OHLC prices or introduce guaranteed-fill claims.

Costs affect entry thresholds and therefore future occupancy. For each
predeclared sensitivity schedule, replay the same frozen fitted predictor with
that schedule's decisions; do not merely subtract costs from a prior trade list.
Also retain a distinctly labelled fixed-trade-list cost curve to isolate the
arithmetic charge effect. Neither diagnostic may choose the final model.

Benchmark policies must use the same assumed-cost engine and complete calendar.
Retain empty dates, forced-direction baseline semantics, abstention reasons and
label-maturity/purging checks. Missing cost assumptions are errors, not zero cost.
Distinguish legitimate eligibility exclusions from unverified source coverage.

Create a correctly identified successor; inspect existing identities before
choosing a version number. Preserve v7 bundle
`b698ffc863bf9650f99717e4c69176718dde285f429e12d4e477470b1ec7ae73`.
At the inspected 15 candidate definitions per market, the current four-market
scope has 60 market-candidate configurations. A later 41-market research scope
would contain 615. These are scope arithmetic, not executed trials, incremental
charges automatically owed today, or a historical-search upper bound. Do not
declare or charge the other 37 research markets merely for compatibility tests.
Recover existing declarations,
charge the new material procedure once under the established accounting rule,
and preserve every predecessor charge. Record cost variants if actually used
for selection; diagnostics do not become unrecorded tuning opportunities.

### 5. Validate locally and freeze before historical execution

Run affected predecessor regressions and current binding validation. Exercise
the cost and EDGE diagnostic interfaces synthetically for all 41 exact market
identities, including contract-unit differences and date-boundary classes.
Then run the complete successor synthetic nested procedure for the current
four-market research scope, with the required history and legitimate empty
dates. Use predeclared seeds and record every failure.

Report these separately as all-market interface/contract compatibility and
complete four-market nested acceptance. Do not describe them as complete
41-market nested acceptance. Before later expanding research, run the complete
nested synthetic procedure for every market in that expanded declaration.
Shared fixtures may reduce duplication; representative markets or sampled
folds never establish a complete-scope pass.

Required checks:

- Exact universe and complete market/year dispositions; correct contract units,
  decimal/fractional tick conversion, historical unit changes, signed prices,
  explicit fee assumptions and split-year/protected boundaries.
- The 1,010-date calendar / 476 first-inner rows from two dates must fail.
  Observed-date support must also cover outer and final fitting paths.
- Training-only transforms, normalized-loss replication behavior, purging,
  mature labels and equivalent inner/outer/final fitting conventions.
- Missing required assumptions fail; no fake historical quotes can enter the
  certified source adapter; optional quote/EDGE diagnostics may be unavailable.
- Higher charges reduce P&L for a fixed trade list. For changed entry decisions,
  verify thresholds and occupancy through full replay rather than asserting
  a false monotonicity property for the resulting different trade lists.
- Every candidate result, invalidity, selected fold model, forecast, benchmark,
  empty date and selected-model failure survives in saved outputs. No fallback
  winner after a selected-model failure; no dropped adverse scenario.
- Save and independently reload each completed market's synthetic final model
  with transforms, ordered features and dependencies; reproduce predictions.
  Clearly distinguish synthetic artifacts from historical models.

For the required EDGE diagnostic capability, test known spreads, serial
dependence, predictable returns, volatility jumps, sparse/no trades, missing
bars and rolls. Do not make that diagnostic's success a prerequisite for the
primary assumption-based model. Do not install a library or general estimation
framework merely to add the optional diagnostic.

Inspect saved outputs independently of the producer's status flag. Report all
41 compatibility dispositions and complete candidate/fold counts for the
four-market nested run, with the other 37 nested runs explicitly not run.
Run scoped diff and
whitespace checks. Measure runtime and output sizes with fixed synthetic work
and estimate full-run peak storage; do not extrapolate a sample into a pass claim.

Freeze the complete successor before real results: source/period eligibility,
calendar/contract rules, labels, candidates, weights/transforms/penalties,
support/folds, complete numerical cost assumptions, inner selection, predictive
metrics, economic scenarios, deterministic final candidate/refit rule, failure
and stopping rules, permitted claims and accounting. The final candidate may
not be the best outer-test performer.

### 6. Execute only the separately authorized historical route

Cost assumptions remove the new successor's L1 dependency. They do not resolve
calendar publication/applicability, continuous-contract knowability, immutable
source certification, missing census records, or historical-use authority.
Complete independent coding and synthetic validation while reconciling those
dependencies from owned manifests and already authorized evidence.

Do not open raw DBN/Parquet observations, existing protected result artifacts,
holdout or forward data under an implementation request. Inventory metadata
first. Use the existing certified source-access, registration and one-use
execution boundaries. Do not create a special bypass for assumed-cost research.
If the descriptive successor is incompatible with the existing historical-use
policy, state the precise research-design decision needed; do not waive it.

Before each materially distinct real-data operation, prepare exact markets,
periods, sources, purpose, reads, outputs, measured runtime/storage estimate,
cost limit and preservation boundaries. Request one plain-English approval at
that boundary. Earlier approvals for four-market reference or metadata checks
do not authorize 41-market data access. No provider purchases, downloads,
activation, publication, broker integration, trading, deletion or push are part
of this plan. Holdout/forward access remains separately protected.

Once the exact historical scope is ready, registered and approved, execute once,
save all declared candidate and selected-policy evidence, perform the frozen
final refit and independently verify model loading/prediction. Persist explicit
failure/unsupported dispositions where a market cannot complete. Do not rerun
with changed parameters or replace a failed selected candidate after results.

## Completion report required from the implementing Codex task

Report the six steps above, distinguishing implementation, synthetic acceptance,
source readiness, historical evaluation and final-model production. Include
the 41-market/year matrix, configuration identity, candidate accounting, cost
table and rationale, tests and saved-output inspection, report/model paths,
scientific conclusion and the single next unresolved decision or approval.

Do not claim all-market readiness from a sample, synthetic success from an old
bundle, statistically supported predictive usefulness from descriptive MSE,
or profitability from assumed costs. Preserve the failed profitability-test
evidence and its block. A documented negative or cost-sensitive result is valid.
If history is blocked, deliver the complete authorized local implementation
and exact boundary without marking the overall research mission complete.

## Evidence behind this plan

- [Cost alternatives literature review](ROLLING_HOUR_COST_ALTERNATIVES_REVIEW.md)
  compares EDGE, older spread estimators, books, futures evidence and scenarios.
- [Owned-record reconciliation](../state/unpublished_evidence/rolling_hour_research_v1/owned_records_reconciliation_v1/task_report.md)
  records calendar custody/publication limitations and unresolved accounting.
- [Metadata inquiry](../state/unpublished_evidence/rolling_hour_research_v1/quote_metadata_v1/task_report.md)
  is acquisition-price evidence only; it is not trading cost or owned quote data.
- [Source contract](../configs/source_contract.json) bound the inventory hash
  `cf702920ace8ff5686cd785a6c067a1cb26041a0677ce84abfe786138db8a6d9`
  when this plan was written. Reverify current bindings before implementation.

The literature supports investigating spread proxies and explicitly stating
execution assumptions. The six-step design, scenario roles and acceptance
criteria here are project recommendations; no source proves them universally
unbiased or conservative for every market and year.
