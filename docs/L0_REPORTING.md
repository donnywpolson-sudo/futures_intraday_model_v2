# L0 candidate report contract

This is the maintained schema and usage specification for
`l0_alpha_reporting.py`. It extends L0's saved unversioned replay/evaluation
contract as `l0_alpha_report/2`; original artifacts remain unchanged. Reports
describe reference economics and information use. They never qualify, select,
seal, promote or activate a candidate. Current authority remains in
`CURRENT_WORKFLOW.md`, `AGENTS.md` and `L0_ALPHA_CANDIDATE.md`.

## Producing reports

`candidate_report(context, replay, ...)` accepts an **already computed** L0 replay
mapping, or no replay for a failed/incomplete attempt. It neither reads source
rows nor fits, forecasts, replays trades or evaluates a model. Unknown legacy
metadata becomes JSON `null`; legacy daily records without explicit complete
accounting remain unavailable. Supply declarations before observing results;
adding metadata later cannot establish historical independence.

`ReportingStore` in `l0_alpha_report_store.py` subclasses the existing
`ResearchStore` and observes its save/checkpoint callbacks. `l0_context` projects
known configuration metadata; `operation_contexts` maps the current declared
outer/inner windows to the exact existing operation keys. The caller supplies
experiment/run/parent identity, hypothesis/change, observation cutoff, immutable
data identity, provenance and available freeze/exposure history. Missing values
remain unknown and block descriptive compatibility where relevant.

`code_provenance(root, explicit_paths)` hashes the named working code/config
bytes, including uncommitted and untracked files, and the scoped tracked diff
against Git HEAD. It records hashes, not diff contents. This is a scoped identity,
not a claim that unrelated repository files are clean or that a frozen binding
was renewed.

Started attempts emit incomplete reports. Saved replays remain provisional
until the original checkpoint completes. Checkpoint failures and invalid leaf
support emit failed reports; `record_abandoned` records explicit abandonment.
Raw failures and all previous derived artifacts remain immutable. Model callbacks
record candidate features/settings, model/training IDs and inner selection-input
report IDs, without copying fitted coefficients or forecasts. Unknown callbacks,
source ledgers and final-refit payloads are not inspected by reporting.

Saving the existing completed `run` creates outer-fold aggregates from the
derived fold reports. It retains the declared calendar, selected policies by
fold and source report IDs; missing/failed folds cannot become complete economics.
Aggregates are not additional trials. Available fold cost/exposure/turnover,
exceptional-trade and fixed-trade sensitivity summaries are combined only when
every constituent supplies them; missing trade diagnostics remain unavailable.
The source trade ledgers are not duplicated into aggregate reports.

Reports live below the existing run's `candidate_reports/` directory:

- `<report_id>.json` and `.md`: canonical result and human-readable diagnostics.
- `catalog-<catalog_id>.json` and `.md`: explicit report snapshot, comparison
  requests and human comparison index. Older catalogs are retained.

`write_catalog` only creates artifacts or accepts identical bytes. No mutable
leaderboard/latest pointer is installed. Resume requires the explicit previous
derived catalog in the same run directory and matching lifecycle context; it
does not backfill from original research observations. Existing completed
checkpoints retain the original integrity verification and no-repeat behavior.
Catalog snapshots grow with the number of attempts and remain subject to the
existing run output budget. No new retention/deletion policy is introduced.

The [corrected reporting adoption](../state/unpublished_evidence/l0_alpha_candidate_v1/reporting_adoption_v2/task_report.md)
installed the shared `ReportingStore` lifecycle integration, V8 preparation and
V3 admission-contract bindings. Its six installed payloads and saved 38-check
result were verified; no repeat adoption is needed. V7/V2 remain historical
bindings. The later [screen access integration](../state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_adoption_v1/REPORT.md)
updates the current pipeline's shared-policy bindings without changing reporting behavior.
Software adoption did not admit, register, run or activate L0, and
browser visual correctness remains unverified.

This is the shared reporting component for the single pipeline coordinated in
#1; #2 completed its reporting contribution. The adopted retrospective screen
is a different study question within that pipeline, not another reporting system.
Reuse these attempt/parent/exposure and compatibility conventions where their
meaning applies. The current economic schema does not turn predictive losses
into P&L, equity, Sharpe or alpha; unavailable economic fields stay unavailable.
Screen-to-cockpit publication has not been verified or installed by the task-local
screen preparation. Current state and next decisions are maintained only
in [CURRENT_WORKFLOW.md](../CURRENT_WORKFLOW.md). The [research index](RESEARCH_INDEX.md)
links the completed predictive result and the other studies. Reporting adds no
alternative research entrypoint or research authority.

## Schema and interpretation

All dates use `YYYY-MM-DD` in the declared session timezone; availability and
freeze/exposure timestamps require explicit offsets (UTC `Z` is preferred).
Date lists are ordered, unique, explicit evaluation calendars. Fold/regime maps
identify every date. The cutoff convention and observation availability cutoff
are separate fields. The latter is not inferred from artifact creation time.
Explicit metadata-only records can describe sealed/forward work or unknown legacy
attempts without daily observations. Ordinary catalog loaders check every entry's
payload kind and reject protected observation scopes before opening any report
payload. Observed L0
reports are limited to inner/outer development dates in 2018–2022. There is no
filesystem glob, source resolver, holdout replacement or protected-data loader.

| Field group | Content and interpretation |
| --- | --- |
| Identity/provenance | Candidate, experiment, run, operation, attempt, parent, hypothesis/change, start/end times, immutable source/config/code references; unsupported fields null. |
| Policy | Model/features, selection/ties, refitting/training/purge/embargo/final rule, entry/exit, sizing, opportunity set, execution and costs. Full policy changes cannot be presented as isolated model effects. |
| Scope | Dates/instruments, calendar/session/timezone/cutoffs, stage, fold/regime, P&L basis, risk/sizing and execution/cost assumptions, synthetic/research data kind. |
| Execution | `COMPLETE`, `FAILED`, `INCOMPLETE`, `ABANDONED`; reason and accounting status separate. A completed execution can have unavailable/incomplete economics. `valid_no_trade` requires complete accounting with explicit zero trades, gross, costs and net on every date. |
| Metric eligibility | Complete reference economics is separate from execution, stage, comparison eligibility and capital-return eligibility. Partial rows never become full-run totals. |
| Coverage | Expected/available dates, missing/unresolved and excluded dates with reasons, unresolved execution, supplied source/opportunity coverage and existing replay decision-count/hash metadata. |
| Lineage | Declared family/search space/settings, parent-child change, selection metric/rule, human changes, result-informed revisions, inner selection inputs, freeze/exposure evidence and historical-search limitations. |
| Economics/risk | Gross, costs, net, mean daily dollars, fold stability, counts, concentration, reference-dollar drawdown and underwater episodes. |
| Readiness | Alpha, research-gate, winner-selection, confirmation and deployment flags remain false. No report is a promotion receipt. |

Inner results are development evidence for selection/redesign. Outer chronological
results concern the previously frozen selection/refitting procedure. Final sealed
evaluation is governed confirmation under its previously frozen rule. Forward
observations concern the version frozen before they arrived. Result-informed
redesign creates a new lineage entry: reusing those dates is exploratory.
Unknown exposure history means unknown independence; a freeze timestamp alone
does not prove independence. Recorded inner selection within a declared procedure
is distinguished from redesign informed by outer results.

The observed attempt count deduplicates supplied attempt IDs, excluding benchmark
views and fold aggregates. It is neither historical configuration charges nor an
independent-trial count. Unknown historical search remains explicit. A supplied
effective-trial estimate/method stays in lineage as an estimate; the global ledger
does not fabricate one. Keeping every result does not remove adaptive selection
bias. Reports never sort candidates into an automatic winner.

JSON preserves finite numerical precision; USD is the currency, integer counts
retain their units, and unavailable metrics have `value: null`, availability,
reason and unit. Human displays round to at most six decimal places and never
feed rounded numbers back into calculations. Canonical hashes and explicit
comparison/inference declarations make identical inputs deterministic. Unknown
values never become zeros or infinities.

## Paired comparisons and uncertainty

`compare_candidates(A, B, role=..., inference=...)` is used by both machine/human
reports and the cockpit. Both sides must match all scope fields, including the
**entire declared calendar**, session, instruments/opportunities, costs/execution,
sizing basis, stage, fold/regime and P&L convention. Unknown policy or scope
blocks a complete comparison. Changed policy fields and undeclared/confounding
changes are shown; benchmark comparisons are full-policy comparisons.

For each explicitly matched date, `difference_usd = net_B - net_A`. A valid zero
is retained. An absent record or zero new trades does not imply zero P&L: costs,
holdings, valuation and unresolved execution matter. There is no inner join or
discarding of adverse dates. Coverage shows expected, matched, missing and
excluded observations with reasons. Partial diagnostics are separately labeled
and cannot establish full-run or winner evidence. Complete comparisons report
A/B means, paired mean/cumulative dollar difference, date count and each fold.

The four existing declared simple benchmarks are `no_trade`,
`date_balanced_training_mean`, `always_long`, `always_short`. A missing governed
benchmark result stays unavailable. Comparisons do not choose the easiest
benchmark. Profitable on this sample, above the incumbent, above a declared
benchmark and independently supported improvement are distinct claims. The last
remains unavailable: L0 has no validated paired confirmation rule, and the failed
predecessor statistical qualification remains preserved.

Optional uncertainty reuses `research.bootstrap.stationary_bootstrap_index_rows`.
Its declaration must identify the estimand `MEAN_DAILY_NET_DIFFERENCE_USD`, eligible
stages, declaration time before both recorded result exposures, minimum dates and
minimum segment dates, mean block length, declared sensitivity lengths, replicate
count, seed, confidence level, percentile interval method, stationary-block method,
gap rule and within-segment stationarity assumption. No default confidence level,
economic threshold or promotion rule is supplied. The tiny demonstration's
settings are synthetic test choices and are not a research decision configuration.

Sampling uses identical indices for A/B, within contiguous declared fold/regime
segments only. Circular blocks may wrap inside a segment. Weekends/closures are
absent only if absent from the declared evaluation calendar; missing expected
dates disallow inference. Segment order and sample weights stay fixed. Boundaries
are never crossed or repaired. Block lengths must exceed one and not exceed the
shortest segment; caller-declared date minima must be met. Zero-variance paired
differences, missing settings/exposure or unsupported data return unavailable.
Work is bounded to five million sampled date positions across declared sensitivities.
The percentile interval describes conditional uncertainty under these assumptions;
it does not correct adaptive selection, prove calibration or validate alpha.
Minimum support rules themselves require scientific justification before real use.

If explicitly declared, drawdown scenarios use the same joint samples and the
observed evaluation-calendar length as their horizon. Other horizons are
unavailable. Each declared block length shows dollar-depth quantiles for both
strategies. These conditional reference-P&L scenarios are not guaranteed loss
bounds and cannot model crises absent from the supplied sample.

`joint_loss_diagnostics` supports aligned cross-strategy dollar vectors, shared
loss dates and nondegenerate correlations, retaining explicit portfolio experiment
metadata. It creates no weights, optimizer or portfolio P&L. Portfolio combinations
and weights require their own governed experiments, not standalone ranking.

## Economic calculations and prerequisites

The basis is `CLOSED_TRADE_DAILY_REFERENCE_PNL;NOT_INTRABAR_OR_ACCOUNT_EQUITY`.
Gross minus cost must reconcile to net within USD 0.0000001. A full finite daily
series is required for full-run metrics. Mean dollars per closed round trip is
undefined at zero trades; variance needs two dates. Profit concentration reports
exceptional winning/losing days and shares of positive-day profits or positive
total net profit. Nonpositive denominators remain unavailable; valid net-profit
shares can exceed one.

Dollar drawdown uses cumulative daily net reference P&L with an initial zero
baseline. Depth is positive USD below the running peak. Duration and time
underwater count declared evaluation dates below the peak; recovery to the prior
peak ends an episode. Completed recoveries and the current unrecovered episode
remain separate. This series can omit intratrade/open-position losses and is not
account-equity risk.

With a reconciled complete trade ledger, one-contract exposure is contract-minutes
by side with evaluation-date denominator; turnover is one-way standard-contract
units. Itemized assumed fees and price allowances sum to already deducted costs.
Existing declared base/adverse/severe allowances reprice a fixed trade list from
gross P&L once. These are fixed-selection sensitivities; independently saved
scenario-policy replays can change entries and are labeled by scenario. No
additional fee/slippage deduction is applied to net P&L.

Decision-to-fill cost drift accepts explicit order ID, decision time/price, side,
requested quantity, USD multiplier, fill time/price/quantity/fees, observed
FILLED/PARTIAL/MISSED status and optional assumed cost for filled quantity.
Signed decision-to-fill price shortfall plus fees measures observed execution
cost; a positive value is adverse. It permits negative reference prices. Fees,
unfilled quantity and assumption-versus-observation drift are separated. No fills
means unavailable; unfilled opportunity cost needs a declared counterfactual and
horizon. Price movement is attribution, not proof of market impact, and must not
be deducted again from fill-based P&L.

CAGR, percentage drawdown, account Sharpe/Sortino and Ulcer Index remain unavailable:
L0 does not supply the formula-specific capital, financing, sizing, cash flow,
valuation and return definitions. Dollar P&L is not a return series and receives
no automatic square-root-of-time annualization. DSR additionally needs a valid
return series and defensible search dispersion/dependence accounting; neither
an invented effective trial count nor DSR can certify alpha. VaR/CVaR remain
unavailable without units, horizon, confidence level, estimation method and valid
inputs. Source-of-return, benchmark exposure and stress/liquidation assumptions
are nullable identity fields, not inferred economic explanations or alpha targets.

## Which statistics belong to which test

Use the statistic that answers the declared question. Adding a diagnostic after
results are exposed does not change the original pass/fail rule or establish
independent confirmation.

| Test type | Useful result summary | Current reporting coverage |
| --- | --- | --- |
| Predictive screen | Primary MSE and benchmark MSE in the same units; paired improvement; uncertainty bounds and multiplicity rule; positive folds; sensitivities; expected/scorable dates and missing targets | The completed four-market screen's separate human report contains these central statistics. They are not a unified predictive cockpit view. |
| Trading-policy replay | Gross/cost/net dollars; mean daily net and per-trade expectancy; trade count; reference-dollar drawdown and underwater duration; exposure/turnover; fold stability; benchmark differences and cost stress | Shared L0 reporting supports these when complete inputs exist. Some diagnostics and optional uncertainty require additional declared inputs. Original L0 has no completed historical economic run. |
| Account/portfolio evaluation | Cash-flow-adjusted returns/equity; CAGR; Sharpe and return uncertainty; percentage drawdown and recovery; costs, leverage/margin and financing; tail losses and stress | Not implemented by the current L0 reference-P&L schema. Its capital metrics explicitly return unavailable with reasons. |
| Synthetic mechanics test | Checks passed/failed, invariants exercised and generated-input scope | Verifies implementation; no historical return, predictive-edge or profitability claim. |

Sharpe summarizes average excess return relative to its variability. It needs
an explicit return/benchmark convention, sampling frequency and appropriate
dependence treatment. Naive square-root annualization can be wrong for
serially correlated returns; see [Lo, The Statistics of Sharpe Ratios](https://rpc.cfainstitute.org/research/financial-analysts-journal/2002/the-statistics-of-sharpe-ratios).
It does not by itself establish an edge after searching many alternatives.

CAGR describes compounded annual growth. It needs a defined investable capital
path and cash-flow treatment; an arbitrary denominator applied to futures
contract P&L would change its meaning. Annualized return helps compare growth
over different holding periods, but does not show the losses endured along
the way; see [FINRA, Evaluating Performance](https://www.finra.org/investors/investing/investing-basics/evaluating-performance).
For account evaluation, show growth alongside drawdown and risk, not alone.

Win rate, average win/loss and profit factor can explain a trading strategy's
payoff shape, but the current shared schema does not expose them as standard
summary fields. Add them only from a complete, reconciled trade ledger;
define break-even trades and zero-loss denominators explicitly. They are
secondary to net expectancy, costs and reliable out-of-sample evidence.
They do not belong on a prediction-only test as fabricated trading results.

## Cockpit and synthetic demonstration

The existing cockpit's **Research** button renders the same `report_view`
calculation as JSON/Markdown. Supply an explicit catalog with
`--candidate-reports <catalog.json>` to the existing cockpit entrypoint. The report
API reads the supplied snapshot and calls no engine method. The flag adds no
provider authority and does not change the cockpit's normal live/demo mode.
The standalone synthetic preview below uses the same HTML/CSS/report JavaScript
without the chart/live scripts or engine.

From the repository root, with the existing environment:

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m futures_rebuild.l0_alpha_report_demo --output state/unpublished_evidence/l0_alpha_candidate_v1/reporting_implementation_v2/synthetic_demo_final
```

This writes handwritten accounting fixtures, not model performance or research
evidence. The printed paths identify JSON, Markdown and an HTML cockpit preview.
Use a fresh task output directory after software changes; existing preview bytes
are never replaced. There is no registry-compatible source identity, source
ledger, fitted model, research receipt or activation command in this demonstration.
