# Futures historical research harness

Version: `1.1.0-contract`
Project: `futures_intraday_model_v2`
Status: core contracts implemented and synthetically validated; historical readiness is false

This document specifies the governance harness that must exist and pass synthetic
and contract-level validation before this repository may emit
`HISTORICAL_RESEARCH_READY`. It grants no execution authority by itself. The
current repository configuration still has `copy_authorized: false`; no source
copy has been performed. Synthetic results can never count as alpha evidence.
Paid Databento downloads, real-history hypothesis/WFA
execution, candidate sealing, destructive cutover, external push, trading, and
all writes to the legacy repository remain hard-paused pending new user
authorization.

The harness is local to this repository. A common interpreter and byte-identical
provenance-preserving source copies are permitted, but no mutable data path,
environment, trial ledger, fold decision, model artifact, readiness receipt, or
evaluation result is shared with a stock project. Repository root, Git directory,
environment-lock hash, release ID, ledger path, bundle path, and readiness receipt
must be distinct and verified by no-cross-import/no-cross-write tests. "Global
trial ledger" below means all outcome-informed attempts in this futures project;
outcome-informed ideas transferred from another project must be recorded as
external exposure rather than treated as pristine.

## 1. Research basis

Hyperparameter selection and performance estimation must be separated because
reusing a selection criterion as the reported test result creates selection
bias. Nested evaluation is supported by Varma and Simon's almost-unbiased nested
CV result and Cawley and Talbot's analysis of selection-criterion overfitting.

- [Varma and Simon, 2006](https://doi.org/10.1186/1471-2105-7-91)
- [Cawley and Talbot, 2010](https://www.jmlr.org/papers/v11/cawley10a.html)

Financial samples are temporally dependent. Chronological rolling origins and
information gaps therefore replace random row folds. The dependent-data basis
includes h-block/hv-block cross-validation and rolling-origin forecast testing.

- [Burman, Chow, and Nolan, 1994](https://doi.org/10.1093/biomet/81.2.351)
- [Racine, 2000](https://doi.org/10.1016/S0304-4076(00)00030-0)
- [Tashman, 2000](https://doi.org/10.1016/S0169-2070(00)00065-0)

Inference uses HAC covariance and session-block resampling rather than treating
bars or trades as independent observations.

- [Newey and West, 1987](https://doi.org/10.2307/1913610)
- [Politis and Romano, 1994](https://doi.org/10.1080/01621459.1994.10476870)
- [Romano and Wolf, 2005](https://doi.org/10.1111/j.1468-0262.2005.00615.x)

Selection-bias diagnostics follow the Deflated Sharpe Ratio and the Probability
of Backtest Overfitting/CSCV literature.

- [Bailey and Lopez de Prado, Deflated Sharpe Ratio](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)
- [Bailey, Borwein, Lopez de Prado, and Zhu, PBO](https://doi.org/10.21314/JCF.2016.322)

Databento documents that OHLCV `ts_event` is the interval start, OHLCV does not
carry `ts_recv`, empty trade intervals may produce no record, definitions carry
instrument properties, and `instrument_id` is only guaranteed unique within a
day. Those facts are binding in this harness. Archive retrieval time is retained
separately and is never relabeled as provider receipt time.

The authoritative historical source is the approved, hashed local DBN vault.
Legacy `data/raw` and `data/causally_gated_normalized` copies are comparison-only
and cannot be promoted into the authoritative chain. A successor immutable
release may be added, but no release is edited in place and no paid download is
implied or authorized by readiness.

- [Databento schemas](https://databento.com/docs/schemas-and-data-formats)
- [Databento symbology](https://databento.com/docs/standards-and-conventions/symbology)

## 2. Trial ledger and preregistration

The repository owns one append-only, SHA-256 hash-chained trial-event ledger.
The implemented ledger writes one immutable JSON file per event and records the
sequence, prior-event hash, trusted UTC event time, charter ID, counted trial
number, legacy-census receipt ID, multiplicity family and rule hash,
authorization receipt ID, event type, and event hash. The content-addressed
charter separately binds its release receipts, economics receipts, feature,
target, decision, fold, cost, holdout and multiplicity policy hashes, benchmark,
minimum effect, classification, and `outcome_unlock_at`.

Appends are lock-protected, atomic, and fail closed on a sequence or hash mismatch.
For a real-history classification, declaration and exactly one external
pre-outcome anchor must be written under the trusted clock strictly before the
charter's `outcome_unlock_at`. The external receipt must carry an RSA
PKCS#1-v1.5/SHA-256 signature under the pinned production public key and exact
charter/census/multiplicity scope. A content hash created by repository code is
not authorization. The repository contains no matching private key or signer;
future execution therefore requires a one-time signer outside this repository,
or an explicit source-reviewed public-key rotation before any outcomes are read.

Rewriting the ledger and recomputing its chain is invalid. A machine-readable
legacy trial census, including uncertain and manual plot/report exposure, is a
readiness prerequisite. Exact historical trial count is not claimed. The census
must either remain `INVALID_TRIAL_CENSUS_UNRESOLVED`, which blocks real-history
access, or pre-register `CONSERVATIVE_PENALTY_PREREGISTERED` with an observed
attempt floor, a strictly larger penalty count, and hashes of the census,
rationale, and source evidence. The conservative penalty is the mechanical
multiplicity count; it is not represented as reconstructed truth.

Every outcome-informed variation counts, including abandoned smokes and changes
to features, thresholds, universe, horizon, roll rule, cost, sizing, refit
cadence, or selection metric. Synthetic fixtures and outcome-free data checks
are ledgered but do not count as alpha trials. A successor data release cannot
reset multiplicity when it contains the same historical outcomes. Only genuinely
new prospective dates may begin an independent confirmation information set.

Multiplicity-family assignment and its rule hash must predate outcome access.
Overlapping outcomes, shared strategy ancestry, or transferred outcome-informed
ideas cannot be split into new families merely to reduce the trial count.

The implemented global events are `DECLARED` and, for real history,
`PRE_OUTCOME_ANCHORED`. The later evaluation-state sequence remains a required
M4 extension: `BUILT`, `OUTER_EVALUATED`, then `OUTER_SCREEN_PASS` or `CLOSED`,
followed only by `HOLDOUT_SPEC_FROZEN`, one optional `HOLDOUT_ACCESSED`, and
`HOLDOUT_PASS` or `CLOSED`. Those later states do not exist merely because this
document names them. Declaration and all contract hashes must predate outcome
access. Any semantic change after an outer result creates a new counted trial.

## 3. Sample, timing, and actual-contract identity

Every sample carries:

- `sample_id`, `hypothesis_id`, and `sleeve_id`;
- Databento dataset, `publisher_id`, UTC trading date, `instrument_id`, raw
  contract symbol, and definition-release ID;
- exchange session ID and source-release ID;
- feature-window start, `feature_available_at`, `decision_at`, intended entry,
  label start, label end, and intended exit;
- multiplier, minimum tick, tick value, currency, and contract expiration as
  known at the decision time.

The authoritative instrument key is
`(dataset, publisher_id, utc_trading_date, instrument_id)`. Continuous symbols
are selectors only. The pinned `.v.0` selector means rank zero by the previous
trading day's volume and uses original unadjusted prices. Provider mapping
interval ends are reconciliation-only. Labels, fills, costs, predictions, and
P&L retain the actual contract key.

A definition may identify a bar only when its own effective time is no later
than that bar's event time and its source-received/available times are no later
than the decision. Any actual-contract identity change, including a mapping
change near UTC midnight within one CME session, creates a hard contract segment.
Features and outcomes may not bridge that segment silently.

The following ordering is mandatory; label start is the intended entry and label
end is the intended exit:

```text
feature_window_start <= feature_available_at <= decision_at
< intended_entry_at = label_start_at < label_end_at = intended_exit_at
```

For a one-minute bar, `event_at` is the start of the minute. The complete bar is
unavailable until `event_at + 1 minute + publication_latency`. A signal using
that bar cannot fill at its close; it may enter only at the next causally
tradable event. A missing OHLCV record means `NO_REPORTED_TRADE_BAR`, not zero
return, and is never silently forward-filled.

Missing entry, exit, or required path observations retain explicit outcome
statuses, remain in coverage denominators, and cannot be deleted after outcome
access. Their predeclared handling may yield `INCONCLUSIVE_OUTCOME_COVERAGE`, but
cannot depend on whether the omitted return would help or hurt the model.

An unexpected future contract change inside a label interval is retained as
`ROLL_UNRESOLVED`, with no return value, and remains in the exact one-outcome-per-
prediction coverage report. It is never removed from the denominator.

Roll and eligibility decisions may use only definition, status, statistics,
volume, and open-interest observations available by `decision_at`. Retrospective
continuous-symbol interval endpoints and future realized roll facts are
forbidden.

## 4. Information-interval purge and embargo

Label intervals are half-open: `[label_start_at, label_end_at)`. For every
inner or outer split, remove any training sample whose label interval intersects
any validation/test label interval. When a split permits training observations
after a validation block, also remove training decisions through the validation
block's maximum label end plus the embargo.

The embargo is expressed in exchange sessions, never row counts. It is the
greater of one complete exchange session or the longest chartered holding
interval rounded up to sessions. Split artifacts must contain pre-purge and
post-purge IDs, removal reasons, interval bounds, and a zero-overlap assertion.

## 5. Nested chronological walk-forward analysis

The default outer schedule is expanding-window rolling origin:

| Setting | Required value |
|---|---:|
| Initial training window | 504 exchange sessions |
| Outer test block | 63 exchange sessions |
| Step | 63 exchange sessions |
| Minimum outer folds | 8 |
| Minimum outer OOS | 504 distinct sessions |
| Sealed final holdout | 252 sessions |

Each outer training window contains four chronological inner folds with
42-session validation blocks. Every hyperparameter, feature subset, calibration
rule, threshold, sizing rule, refit cadence, cost choice, and benchmark-based
selection rule is chosen inside the inner folds. The chosen configuration is
frozen before the outer block.

Outer observations arrive sequentially. Scheduled parameter refits may use
earlier outer observations only after their labels have matured. Hyperparameters
cannot change. If the required schedule is infeasible, the result is
`INCONCLUSIVE_DATA_LENGTH`; folds may not be shortened after outcomes are seen.

Outer OOS is a screen, not the final holdout. After an outer screen passes, the
complete model, feature set, thresholds, costs, inference code, and holdout test
must be frozen before the one-time holdout unlock. Outer and holdout results are
reported separately and are never pooled for selection. A holdout failure closes
the trial; no retuning, rescue, retry, or reuse of that holdout is permitted. A
successor is a new counted trial and cannot claim a fresh holdout drawn from dates
already exposed. Neither screen nor holdout passage authorizes candidate sealing.

## 6. Fold-local fit audit

Imputation, winsorization, scaling, volatility normalization, feature selection,
dimensionality reduction, calibration, class weighting, resampling, risk
scaling, and sizing parameters implement `fit(train_ids)` and
`transform(sample_ids)`. Every fit records the exact sample IDs and input hash.
Any fit/test-ID intersection is fatal.

Deterministic lag calculations may be materialized before splitting only when
their lineage proves all inputs were available by `decision_at` and they have no
fitted population statistic. Roll selection and liquidity eligibility remain
causal data contracts, not globally fitted conveniences.

## 7. Session-level uncertainty

The primary inferential series is one net portfolio return per complete exchange
session, retaining all markets and trades together. Trade-level results are
diagnostic only. HAC lag is
`max(overlap_lag, floor(4 * (T / 100)^(2/9)))`. Confidence intervals are
two-sided 95%; the net-edge test is one-sided against mean net edge less than or
equal to zero.

Stationary bootstrap inference uses 10,000 resamples and a deterministic seed
derived from the trial ID. Blocks resample entire sessions. The charter freezes
the training-selected mean block length and reports 5-, 10-, and 20-session
sensitivity. Fewer than 30 required clusters yields
`INCONCLUSIVE_CLUSTER_COUNT`.

## 8. Multiple testing, DSR, and PBO

DSR uses the concatenated outer-OOS net session returns and the conservative raw
count of all outcome-informed alternatives in the information family. Passing
requires DSR probability at least 0.95. Joint candidate/baseline comparisons use
Romano-Wolf stepdown with the same session-block resampling and adjusted one-sided
`p <= 0.05`.

The predeclared family includes every tested sleeve, direction, horizon, primary
metric, candidate/baseline contrast, and negative control. A baseline is either
selected inside the inner folds or all baselines remain in the joint max-statistic
family. No outcome-informed family split or metric substitution is permitted.

PBO is computed only when at least ten comparable configurations and an even
number `S >= 8` of equal contiguous chronological blocks exist. The charter pins
`S`, the stress-cost Sharpe selection statistic, and the configuration set before
access. CSCV enumerates every `S choose S/2` in-sample block combination, uses
its complement out of sample, selects only by the pinned in-sample statistic,
assigns deterministic midranks for ties, and records the selected configuration's
OOS rank and logit. PBO is the fraction of logits below zero. It uses a common-
session, stress-cost return matrix;
missing sessions cannot be silently zero-filled. `PBO <= 0.20` passes the
diagnostic, `(0.20, 0.50]` is `INCONCLUSIVE_OVERFIT_RISK`, and `PBO > 0.50` is
`FAIL_BACKTEST_OVERFIT`. A genuinely single predeclared configuration is
`NOT_APPLICABLE` and receives no positive credit. PBO remains a diagnostic and
cannot substitute for the binding chronological WFA. Tried variants with a
missing return matrix are `INVALID_MISSING_TRIAL_EVIDENCE`.

## 9. Costs, baselines, controls, and sleeves

Actual-contract gross P&L is

```text
side * contracts * (exit_price - entry_price) * contract_multiplier
```

Net P&L subtracts broker commission, exchange/regulatory fees, spread,
slippage, and declared impact in tick-value units. Every fill is tick-valid.
The cost grid is:

- `zero_cost`: diagnostic only;
- `base`: deployment fees plus at least one tick round trip;
- `stress`: deployment fees plus at least two ticks round trip and no smaller
  than twice base spread/slippage;
- `extreme`: deployment fees plus at least four ticks round trip and no smaller
  than four times base spread/slippage.

Promotion evidence must pass `stress`. Observed-spread claims are forbidden
without causal top-of-book input.

Mandatory baselines are flat/zero, fold-local unconditional return by
market/session bucket, previous-bar-sign momentum, previous-bar-sign reversal,
and a risk-matched always-long intraday exposure where meaningful. Negative
controls are a whole-session circular label shift, deterministic random feature,
training-only permuted market identity, and a future-data canary that must be
rejected before fitting. No negative control may pass the complete historical
screen gate.

A sleeve is
`hypothesis x market_root x session_window x holding_horizon x direction`.
Each sleeve gates independently. Combining sleeves or choosing weights is a new
trial; aggregate P&L cannot hide a failed or underpowered sleeve.

## 10. Power and binding outcomes

The charter declares a minimum economically meaningful effect (`MEES`) and a
strictly larger design alternative before outer evaluation. Power is estimated
from training-only centered session residuals at that design alternative using
5,000 stationary-block samples at the planned OOS length and the exact final
test. Required power is at least 0.80.

Outcomes are mutually exclusive and applied in this order:

- `INVALID`: lineage, leakage, timing, role, ledger, cost, or evidence failure;
- `INCONCLUSIVE_DATA_OR_POWER`: power below 0.80, insufficient clusters, folds,
  active observations, or outcome coverage;
- `FAIL_NO_EDGE`: the 95% confidence upper bound is at most zero;
- `FAIL_NOT_ECONOMIC`: after the prior rule, the 95% confidence upper bound is at
  most MEES;
- `INCONCLUSIVE_EFFECT`: the interval still intersects MEES;
- `FAIL_MULTIPLICITY_OR_CONTROL`: the effect bound passes but any adjusted test,
  DSR, applicable PBO policy, stress-cost, baseline, sleeve, or control gate fails;
- `PASS_HISTORICAL_SCREEN`: the multiplicity-adjusted one-sided 95% lower bound
  exceeds MEES, adjusted p-value is at most 0.05, DSR is at least 0.95, applicable
  PBO policy is satisfied, stress costs pass, and every required sleeve, baseline,
  and control passes.

A historical pass does not seal a candidate. It only makes the result eligible
for a separately authorized sealing decision; until then it remains an unsealed
research result. It does not establish alpha or authorize trading.

## 11. Builder/evaluator isolation

The splitter owns labels, intervals, and folds. The builder receives training
features/labels plus sequential unlabeled test features and writes only a frozen
evaluation artifact and predictions. The evaluator reads frozen predictions and test
labels, cannot import training modules, and must make zero `.fit()` calls.

Prediction manifests must predate label-unlock receipts. Evaluation reports bind
the evaluation artifact, prediction, data release, charter, ledger head, code, and
environment hashes. The evaluator writes only to its unique run directory. The
final-holdout receipt is one-time, append-only, and anchored to the frozen
holdout specification and pre-unlock ledger head. Repeated access is fatal.

This is process isolation, not proof of independent human judgment. Genuine
independence requires a separate account or external evaluator; prospective
confirmation remains required.

## 12. Readiness boundary

`HISTORICAL_RESEARCH_READY` remains false until the rebuilt DBN-to-validated-
actual-contract-to-causal release chain passes its complete handoff, the trial
census is imported conservatively, the configuration contract is validated, and
all required synthetic/adversarial harness tests pass. This document alone marks
nothing ready.

Reproducibility also requires the exact Python and package lock, dependency-lock
receipt, and platform wheel closure. The current Windows CPython 3.11 runtime
pins the NumPy wheel filename, byte size, and SHA-256; changing the interpreter,
dependency files, or wheel lock changes the runtime closure and invalidates
artifact trust.
