# Minute-entry, fixed-hour futures research

This is the current requested research direction, implemented as an **inactive
synthetic-only successor**. It neither activates the Alpha ladder nor retries
the closed ES mechanisms. Existing profit-aligned and strategy-discovery
experiments, source releases, and immutable evidence are preserved.
The current declaration is `configs/rolling_hour_research_v7.json`; v1/v2/v3/v4/v5/v6 and
their evidence remain historical preparations. V6 preserves the original sixty charge identities and declares sixty new identities
for its changed learning procedure. Rebuilding unchanged preparation adds no charge.

The research policy objective is to refresh an hour-ahead forecast every eligible minute
and determine whether a frozen entry policy produces positive daily net dollars.
It is an unproven predictive hypothesis, not a replication of a published effect
or an economic explanation of order flow.

## 1. Trading behavior

Use 61 completed, consecutive one-minute bars in one verified open interval and
one actual contract. Retain the existing five-second decision delay. The first
decision is at interval open plus 61 minutes and five seconds; its modeled entry
is the next minute boundary, plus 62 minutes from interval open. Subsequent
eligible decisions occur every minute. The entry latency is a frozen research
assumption, not measured deployment latency.

Enter only when flat. Hold at most one standard contract per market, with no
stacking, reversal, pyramiding, or adaptive exit. A position closes exactly
60 minutes after its entry. Forecasts made while invested remain in the replay
but cannot move its exit. A new entry requires a decision made after the prior
exit; this deliberately does not preauthorize reopening at the exit boundary.

Entry, path, and the complete exit bar must fit inside the same open interval.
There are no price stops, targets, or trailing exits. Fixed size, fixed duration,
and session containment are the research risk constraints; account loss limits
and executable emergency exits require a separate deployment design. Missing
entry quotes cause abstention; a missing exit for an entered position invalidates
the complete result. No fill is invented.

## 2. Labels and causal data

Phase 3 declares the question, scope, competing explanations, and access boundary.
Phase 4 retains continuous 60-minute terminal open-print change in ticks. Its
label differs from realized quote-based strategy P&L. Excursions remain diagnostic
and exclude the exit bar's later high and low. Labels stay unavailable until
their complete source path is mature; overlapping minute labels are not
independent observations.

Keep OHLCV-1m as the primary source. OHLCV-1s remains preserved and unconsumed;
testing its incremental information requires a separately declared matched
experiment. No resampling, replacement of accepted causal files, or causal
release rebuild is performed. Sidecar-derived identity, availability, cadence,
contract economics, and schedule fields validate eligibility. Retrospective
quality information is never introduced as a predictive feature.

## 3. Feature discovery and freezing

Phase 5 computes 29 existing formulas. Its three nested packs, `baseline`,
`context`, and `context_interactions`, contain 5, 10 and 16 features respectively;
their union is 16, not all 29. Crossing three Ridge penalties with two
existing shallow XGBoost profiles gives 15 candidates per market, 60 across
ES/NQ/CL/GC. This is a new accounting charge; predecessor charges remain intact.
It is not a complete historical performance census or upper bound.

The matched questions are baseline versus the added long-horizon/context bundle,
then context versus the six existing context interactions. Select each pack's
model profile within inner folds and report its later outer performance. These
contrasts are descriptive and cannot choose a replacement strategy after seeing
outer results; they do not attribute an effect to an individual feature.

Phase 6 freezes formulas, candidates, selection, transforms, position policy,
cost rules, and the inference requirement before outer evaluation. Candidate
selection uses mean daily summed stress net dollars from the same flat-only
position replay that evaluates outer predictions and all six baselines.
Zero-trade dates count as zero; missing dates are never converted into zero.
The fitting bridge now requires a separately declared complete session calendar.
Missing dates, intervals, or decisions fail. Declared empty sessions retain their
place in P&L denominators, audits and embargo; they supply no invented fit rows.
The predecessor's observation-array container remains unchanged and is not the
calendar authority for this successor. A fold still needs observations to score;
an entirely empty audit fold fails explicitly rather than being dropped.

Retain eight outer folds, four inner folds, 63-date audits, whole-date partitions,
and one-date embargo. Additionally require every training label to be known
strictly before the earliest audit decision. Constant/duplicate removal,
standardization, feature-pack selection, and fitting use training data only.
Outer model/prediction records must reproduce complete deterministic fitting and
selection. V6 uses one shared normalized fitting path with distinct successor identities;
their old certificates grant no new authority.

Reports now distinguish row-weighted and date-weighted forecast error, RMSE,
predictions within one tick of the cost threshold, turnover, fixed-duration
exposure and adjacent overlapping labels. These are descriptive outputs with
no selection effect. Empty dates have no forecast-error observation. Positive
strategy P&L alone does not establish incremental forecast value or portfolio alpha.

V5 adds the predictive question: does the profit-selected model forecast the
modeled hour's change better than zero change (primary benchmark) and the
training-fold mean (secondary)? Each outer fold freezes the row mean of its
eligible training targets before audit use. Both benchmarks and the selected
model are scored on identical outer forecast rows, including forecasts while
held and forecasts that cause no trade. Profit-based selection is unchanged.

The primary accuracy score is squared error averaged within each date, then
equally across dates with forecasts, separately per market. Reports retain daily
MSE, date-balanced MSE in ticks squared, benchmark-minus-model MSE improvement,
and skill `1 - model MSE / benchmark MSE`. Positive skill means lower measured
error; a zero benchmark MSE produces null skill with an explicit reason. Empty
audit dates are listed as coverage, never assigned zero error. Existing
row-weighted RMSE remains a separate diagnostic.

These comparisons are descriptive and cannot select or qualify a model. A
predictive qualification claim still requires a predeclared, validated inference
procedure accounting for overlapping forecasts, serial dependence and multiple
claims. Adding these metrics does not repair the blocked profitability test.

## 4. Statistical repair remains blocked

The saved profit-aligned v6 calibration is preserved. Recomputing its complete
28,000-replication counts reproduces ten failed gated null scenarios. Empirical
resampling can omit rare losses and misrepresent dependence. Changing bar cadence,
labels, feature packs, or acceptance thresholds does not establish a repair.

The successor therefore has **no implemented replacement profitability test**.
The remediation adopts the plan's explicit **descriptive-only disposition**:
no justified moment bound or dependence model has been established, and the
existing accounting owner still has no historical search upper bound. Historical
execution remains blocked; neither issue is represented as repaired.
Its qualification gate always returns `BLOCKED_RAW_PNL_INFERENCE_NOT_VALIDATED`.
The prospective clipped-score assessment cannot substitute for the required
unclipped daily-dollar mean. No caller-supplied success flag or predecessor
certificate can open Phases 12-15.

A replacement needs justified tail/dependence assumptions and a declaration made
before fresh validation. All fourteen predecessor scenarios, including failures,
remain regression challenges with 2,000 replications each; simultaneous null
upper limits remain 2.5%, and the strong-power lower limit remains 80%. Passing
these known challenges alone would still not prove distribution-free validity.
Historical census recovery, source-bound readiness, expected-cost calibration,
fresh prediction custody, and holdout/forward access remain separate unresolved
requirements. The historical sealed holdout is never relabeled as fresh evidence.

For example, [Wang and Ramdas's heavy-tail confidence sequences](https://arxiv.org/abs/2202.01250)
require a known moment bound under their stated process assumptions. This project
has not established such a bound for raw daily futures P&L. A historical sample
variance or a stop-loss setting does not establish a population variance bound;
neither is inserted as a convenient constant. This is a scientific prerequisite,
not a software permission that an approval flag can supply.

The final-stage rules are now explicit: choose the last outer fold's inner
winner, never an outer-score winner; use the same minute policy for one-shot
2023-to-cutoff stress; refit that same candidate without evaluation; distinguish
the sealed historical holdout from genuinely post-freeze outcomes. A descriptive selection-period final refit is implemented separately from these
blocked Phase 12-15 handoffs. It fits the last inner winner on all declared
eligible 2018-2022 observations, without evaluating that refit or choosing on
outer performance. It does not complete the later 2018-to-cutoff Phase 13 refit.

The declaration contains a metadata-only preparation identifying the 2018–2022
ES/NQ/CL/GC price files from the active catalog. It does not open those files or
certify their bytes. Compatible 2018-2022 quote inputs, calendar known-at evidence and fee/latency
evidence remain unresolved. A bounded price-only readiness census is now
prepared in the declaration; it cannot certify the profit selector on its own. Resolve those bindings
before requesting source access; this preparation grants none.

## 5. Local interfaces and verification

Read-only current successor description:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline rolling-hour-research
```

Create the inactive declaration once, then exercise all four markets with
generated bars and quotes, including complete nested selection and replay:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.rolling_hour_preparation
.\.venv\Scripts\python.exe -m futures_rebuild.rolling_hour_preparation --synthetic
.\.venv\Scripts\python.exe -m pytest -q tests/test_rolling_hour_research.py
```

The v6 full replay uses 1,026 declared generated dates and 360-minute sessions every 63 dates, 120-minute
empty sessions every other 127th date, and the original short schedule otherwise.
It varies volatility, price regimes, spreads and quote availability. Quote events
use a separate event clock. Acceptance measures all four quartiles, variable
candidate features, multi-entry days and empty audit dates. Winner diversity is
reported without tuning the fixture to manufacture it. The original small fixture
remains available for fast tests. Neither fixture is statistical calibration.

The v3 replay exposed an unsupported XGBoost leaf in a diagnostic pack's outer
refit. V4 preserves that failure, the fixture and the 20-date leaf rule. A failed
diagnostic refit now makes its entire pack contrast unavailable, without dropping
dates, substituting a model, or vetoing the selected trading policy. A selected
model failure still closes that market; the report records all four markets'
outcomes. Incomplete diagnostics are explicit in the report's state.

The synthetic report is engineering evidence only. There is no provider or
real-data reader, live order adapter, activation command, or production runner.
Complete calendar accounting is enforced for the synthetic fitting bridge; a
real-source adapter is structurally tested with generated inputs; its actual
source custody and readiness are not certified. It accepts verified calendar
products and exact per-date source bindings, but provides no file reader, access
grant or historical gateway bypass. Pure in-memory records and
hashes do not confer source-access authority. The September 6 audit artifacts
preserve the prior behavior and are not current acceptance tests.


## V6 support, fitting and claim boundaries

The fitting loss is date-balanced squared prediction error. For D observed
training dates and n_d eligible rows on date d, w_i = 1/(D n_d), so weights sum
to one and every observed date contributes 1/D. Empty calendar dates remain in
splits, embargo and economic denominators, but contribute no fabricated targets.
Overlapping labels are all retained as forecasts at the declared minute cadence;
weights do not make them independent. No uniqueness weighting or sequential
bootstrap is introduced.

Ridge removes constants/exact duplicates on training rows, standardizes using
weighted training means and population variances, and minimizes
sum(w_i * error_i^2) + lambda * sum(beta_j^2), with an unpenalized intercept.
The fixed lambda grid is 0.1, 1, 10. Repeating all rows, or repeating a date's
complete observations, does not weaken this penalty. Selectively repeating just
one of several different observations changes the empirical target and is not
an invariance claim. Predictions never learn transforms from their input.

XGBoost uses the same sum-one weights and weighted training-mean base score.
Its squared-error objective has gradient w_i*(prediction-target) and Hessian w_i;
lambda=0.01 is the L2 leaf penalty in that normalized convention, alpha=gamma=0,
and min_child_weight=0.05 requires five percent of total training weight.
The independent 20-distinct-date-per-leaf rule remains. The two depths, 100
rounds, fixed seed and other existing settings remain declared. These choices
were fixed before the new complete synthetic run; they are design choices,
not parameters fitted to observed market or synthetic outer performance.
The [XGBoost 3.0 parameter documentation](https://xgboost.readthedocs.io/en/release_3.0.0/parameter.html)
defines the Hessian threshold and L2 regularization semantics. Floating-point
histogram construction means the mathematical replication invariance is checked
numerically, not claimed as universal bitwise identity.

All inner fits require at least 252 distinct dates with eligible observations;
outer and descriptive final refits require 505. The complete calendar and row
counts are separate checks. The 1,010-date, 476-first-inner-row/two-date regression
fails explicitly. The synthetic fixture uses 1,026 dates to meet support while
retaining the same empty-date and regime rules; its seed is unchanged.

Inner selection still maximizes mean daily simulated stress-net dollars and
therefore does not select the most accurate forecaster. The outer accuracy
score remains equal-date MSE; zero change and the eligible outer-training row
mean remain the matched benchmarks. Predictive evaluation and economic replay
are descriptive. No significance test or promotion gate has been added.

Predictive usefulness is not established. A future inferential successor needs
predeclared daily loss differences, dependence/tail assumptions, treatment of
missing forecast dates and estimated/nested models, multiple-comparison control,
and complete fixed-seed null and power validation. A plug-in HAC or bootstrap
p-value is not treated as validated automatically; see the primary research on
[forecast comparison under strong dependence](https://arxiv.org/abs/2409.12662).
The failed raw-dollar profitability calibration and its block are preserved.
Neither inferential problem prevents saving an honestly labeled descriptive
model, but existing historical access and census restrictions remain binding.

`rolling_hour_source.adapt_source_sessions` checks exact market/date/contract,
complete one-minute open intervals, source identities, feature availability,
label maturity and all quote roles using existing row and quote validators.
Original-publication calendar and contract-selection bindings must predate the
session. A retrospective verified calendar alone cannot supply that evidence.
The adapter is an in-memory validation boundary, not a source certification
issuer; the authorized caller must establish immutable custody before it supplies
these records. Generated adapter tests do not establish historical readiness.
The local catalogs inspected cover 2025-2026 quotes, not the 2018-2022 selection
period. Options are to locate a compatible already-owned historical release or
separately approve a priced acquisition after coverage/availability review.
Changing to an accuracy selector or proxy costs would require a separately
approved experiment; neither substitution is made here.

The declaration records the recovered trial subset, prior ledger snapshot,
original charges and sixty new procedure identities. Known closed results remain
closed. Missing result-view and decision history is explicit; charges are not
performance observations or a justified upper bound. The historical route needs
that evidence or an explicit governed design decision permitting descriptive
reuse of previously inspected development data. Approval cannot make it untouched.
The prepared price-only operation has exact files and bounds and requests no
model evaluation, 2023+ observations, provider purchase or source mutation.

V7 isolates unavailable forecast diagnostics from valid selected-policy results.
It retains the V6 learning procedure and sixty identities without another charge.
The V6 synthetic attempt was interrupted for this reporting correction and is
not acceptance evidence; its attempt status and frozen configuration remain.

The [inactive reference/calendar integration design](ROLLING_HOUR_REFERENCE_CALENDAR_DESIGN.md)
uses the completed owned-input assessment to specify as-received joins and the
remaining calendar-publication boundary. It changes neither v7 nor active policies.
