# Bounded prospective evidence assessment

Status: completed, synthetic-only. No historical upper bound was established.
The proposed prospective test passed its predeclared scoped simulation checks;
it is not a validated replacement for the pipeline's profitability qualifier.
No raw data, holdout, or forward observations were opened. No active policy changed.

## 1. Historical upper bound: not established

The saved legacy penalty decision is 39 observed-attempt charges plus 65 unresolved
references plus one safety margin. The census implementation explicitly records
`manual_plot_or_report_exposure_complete=false` and an indeterminate count. Its
unresolved items are paths absent from prescribed evidence; a path is not a proven
maximum number of tests. The prescribed scope contains 24 evidence files, not an
exhaustive record of every research interaction.

Consequently, the available evidence cannot justify an upper bound of 105, 469,
or an invented larger round number. This is not proof that recovery is impossible
everywhere; it is the bounded audit's conclusion about the available evidence.
The 469 accounting charges remain intact. Searching more result files alone
cannot certify that unrecorded manual research never occurred.

Evidence: `configs/legacy_trial_penalty_decision.json`,
`src/futures_rebuild/legacy_trial_census.py` (unresolved references and counting
rule), and `configs/fixed_hour_search_accounting_v1.json`.

## 2. Prospective alternative assessed

Freeze one combined model and one independently chosen Ridge benchmark per
market, transformations, trading/cost rules and all 28 claims before a future
authorized campaign. Require immutable predictions before their outcomes. Use
only outcomes that occur after the campaign freeze: do not relabel old 2018–2022,
2023–2025, final-holdout, or already observed forward data as fresh confirmation.
Old experimentation remains disclosed; fresh evidence does not validate old claims.

Each complete common trade date contributes a session-balanced net-tick score for
each combined-versus-baseline comparison. Subtract 0.01 tick for the Ridge contrast,
zero for other contrasts, divide by 100 ticks, then clip to [-1,1]. This fixed
normalization is an assessed design choice, not a data-calibrated optimum.
Keep abstentions in opportunity denominators. Missing or invalid dates halt the
campaign as incomplete; never silently omit or zero-fill them.

For each claim, maintain the equally weighted mixture of four wealth processes:
`E_t = mean_lambda product_s<=t (1 + lambda * score_s)`, for fixed
`lambda = 0.05, 0.1, 0.2, 0.4`. Use log arithmetic. A fixed mixture is one test,
not a post-result selection of the winning lambda.

The null is **conditional expected bounded score <=0 at every date**, given all
information before that date. Since each nonnegative multiplier has conditional
expectation <=1 under this null, each product and their fixed mixture are
nonnegative supermartingales starting at one. Ville's inequality supplies an
anytime crossing bound. This does not require independent scores, but unconditional
zero mean alone is insufficient. See [Howard et al.](https://arxiv.org/abs/1810.08240)
and [Waudby-Smith and Ramdas](https://doi.org/10.1093/jrsssb/qkad009).

Spend at most 0.05 across future campaigns: campaign k receives `0.05/2^k`,
starting at k=1. Split each campaign's budget equally across all 28 fixed claims;
the first campaign crosses at E>=1120. No resetting wealth or reclaiming failed
markets' budgets. Monitoring within a campaign is allowed; changing the model or
procedure requires a new, separately authorized future campaign and its next
alpha allocation. Existing legacy alpha is not retroactively restored.

This is a conditional fresh-evidence design, not a claim that multiplying a
historical p-value by an incomplete count repairs prior search.

## 3. Frozen simulation and measured results

Declaration: `configs/prospective_evidence_assessment_v1.json`.
Full result: `state/unpublished_evidence/prospective_evidence_assessment_v1/report.json`.
The declaration and implementation hashes were frozen before the Monte Carlo run.
No scenarios, seed, bets, sample limits or acceptance thresholds were tuned afterward.

Eight scenarios used 2,000 campaigns each (16,000 total), up to 1,008 dates, with
all 28 claims inspected every date. Null scenarios cover independent signs,
perfect cross-claim common shocks, past-predictable changing volatility, negative
drift, and true-null claims alongside genuinely positive claims. These satisfy
the stated conditional null on the null columns; they do not cover every possible
market process. Simulation runtime used NumPy 2.4.4 and SciPy 1.17.1.

Across scenarios, the largest observed probability of any false null-claim
crossing by 1,008 sessions was **1.20%**, with simultaneous confidence upper bound
**2.264%**. All null upper bounds met the predeclared 5% assessment ceiling.
Intervals are exact binomial Clopper–Pearson bounds with Bonferroni allocation
across all 72 reported intervals, giving 95% simultaneous coverage. Zero observed
errors do not imply a zero true error rate. The theoretical first-campaign budget
is 2.5%; simulation calibration is finite-scenario evidence, not a universal proof.

Detection below means all seven first-market claims crossed by the horizon:

| Synthetic bounded-score mean | 252 sessions | 504 sessions | 1,008 sessions |
| --- | ---: | ---: | ---: |
| 0.05, shared signal shock | 0.40% | 0.65% | 1.55% |
| 0.10, shared signal shock | 1.30% | 6.75% | 22.30% |
| 0.20, shared signal shock | 23.95% | 77.05% | 99.15% |
| 0.20, independent signal claims | 0.00% | 13.20% | 94.30% |

The strong-edge 1,008-session lower confidence bound was 98.21% (shared) and
92.33% (independent), exceeding the predeclared 80% strong-edge target. Weak and
moderate edge power is poor; those scenarios had no minimum-power acceptance
claim. These normalized synthetic effects are not estimates of real trading alpha.

Runtime: **91.78 seconds**, below the frozen 300-second limit. Conservative NumPy
working-array estimate: **13.67 MiB**, below 64 MiB; whole-process peak RSS was
not measured. Updates require only 28 x 4 log-wealth values, not 262,639 bootstrap
resamples. Thus computation is feasible; collecting persuasive fresh evidence
for small effects may not be practically fast enough.

## 4. What this does not establish

- Clipping changes the claim. A positive bounded score can coexist with negative
  uncapped mean P&L: 99 gains of 20 ticks and one loss of 10,000 ticks provide a
  concrete counterexample covered by a rejection/interpretation test.
- A series can have unconditional mean zero while violating the conditional
  null. A single random sign held for the entire campaign rejects on half its
  paths; this counterexample is tested, not counted as a valid-null simulation.
- The run simulates score processes, not Ridge/XGBoost training, price bars,
  execution, market source certification, or prospective prediction custody.
- Crossing rejects the all-dates conditional nonpositive-score null. It does not
  prove a constant positive mean, persistence of the edge, or future profitability.

## Decision

Do not pursue a count-only historical correction on the unsupported 469 upper
bound. A genuinely prospective bounded-score campaign is statistically coherent
under its explicit null and computationally feasible, but it is **not a drop-in
DSR or uncapped-profitability replacement** and has low power for small effects.

Before any real campaign, a separate decision must accept its narrower objective
or choose a justified raw-P&L inference model; then bind model/prediction custody,
date completeness, a durable no-reset campaign-alpha ledger, readiness and
qualification policy successors. The current historical qualifier remains
unchanged and blocked. Holdout and forward access still require separate approval.
