# Qualification review: evidence requirements before a successor

Status: **BLOCKED_STATISTICAL_VALIDATION_FAILED**. No replacement has been
selected, implemented, validated or adopted. This review is the Milestone 2
owner-review deliverable; it does not assert that a defensible qualification
method has already been found. Scientific acceptance is not software acceptance.

## Diagnosis

The preserved [complete validation report](PROFIT_ALIGNED_RESEARCH.md) covers
28,000 replications across 14 declared scenarios. Its reported familywise
false-qualification rates include 3.70% for independent normal returns, 4.30%
for AR(0.5), 29.70% for rare losses and 2.45% for a partial null. Their simultaneous
upper bounds are 5.21%, 5.90%, 32.98% and 3.73%, respectively, against a 2.50%
maximum. Strong-effect detection's 96.76% lower bound exceeds 80%, but that
cannot compensate for failed false-positive control. These are preserved
measurements, not a rerun or a newly certified result.

The rare-loss generator in `profit_aligned_validation.generated_pnl` has shocks
`0.2 - 20 * Bernoulli(0.01) + 0.2 * Normal(0,1)`, scaled to dollars. Its mean is
zero. With 252 sessions, the probability of observing no rare loss in one market
is `0.99**252 = 7.9445%`. Across the generator's four independent markets, the
probability that at least one misses all rare losses is
`1 - (1 - 0.99**252)**4 = 28.1878%`.

That is an explanatory probability, **not** a recalculation or exact derivation
of the observed 29.70% qualification rate. Noise and the actual decision kernel
also affect qualification. Resampling a return sample which contains no rare
loss cannot reconstruct an unobserved loss event. More bootstrap draws improve
numerical resolution, not the information in 252 observed sessions.

Separate the three issues:

| Issue | Finding / consequence |
| --- | --- |
| Implementation defect | Static tracing did not establish a specific coding defect that explains and repairs all failed scenarios. Preserve the failure; do not label this a software fix. |
| Statistical approximation | Null-centering, HAC lag 5, stationary blocks of mean length 10, studentization and Bonferroni did not meet the declared finite-sample calibration limits, even under some ordinary nulls. Multiplicity correction needs valid individual tests. |
| Insufficient tail information | With no independently justified tail/moment constraints, an observed profitable sample can be consistent with an unobserved sufficiently damaging loss. A stop rule or assumed fill is not proof of a hard loss bound in futures. More resampling cannot establish that bound. |

## Bounded successor proposal for owner review

The next scientific deliverable should resolve **one** candidate method or return
an insufficient-evidence finding. Do not tune methods repeatedly against the
final validation suite. No economic research run is needed to review the
requirements below.

1. **Population and quantity.** Freeze a strategy, sizing and execution/cost
   policy, eligible sessions and the evaluation horizon. Test mean daily net
   strategy dollars over the complete scheduled calendar, including genuine
   flat dates. Separately test paired daily net improvement over every declared
   implementable benchmark under comparable sizing and cost treatment. Positive
   P&L alone cannot establish benchmark superiority or alpha.
2. **Economic decision.** The campaign owner supplies minimum worthwhile daily
   net dollars, minimum worthwhile benchmark differences and risk limits from
   intended implementation. Reject the joint claim if any required component
   lacks support. Drawdown/stress/capacity checks are distinct implementation
   requirements; a mean test cannot certify acceptable tail risk.
3. **Admissible assumptions.** State the actual dependence condition and
   tail/moment information under which a proposed test is valid. Explain their
   external justification and failure behavior. Do not infer an upper loss
   bound from the observed largest loss, assume independent trades, clip losses,
   or treat a stop order as guaranteed execution. If defensible assumptions
   cannot be supplied, stop with insufficient evidence about the mean claim.
4. **Individual test and sample requirement.** A successor specification must
   identify the exact statistic, reference distribution or bound, nuisance
   estimation, horizon, missing-data rule and uncertainty calculation. Derive
   sample requirements for the owner-selected meaningful effects under the
   supported dependence/tail scenarios. The old 252-session horizon and
   500-trade count are not general sufficiency proofs. No formula is proposed
   here as validated; choosing one is the outstanding scientific decision.
5. **Repeated campaigns.** Reuse the existing persistent campaign chain and
   `0.05 / 2**k` spending convention where its assumptions apply, starting at the
   retained ordinal, never at a fresh folder's ordinal. Preserve failed charges
   and used confirmation periods. Preallocate candidate/benchmark slots; unused
   slots are not recycled after results. Check numerical resolution before
   access. No data-dependent early success or horizon extension. Valid
   per-campaign tests remain a prerequisite; accounting alone removes no bias.
6. **Selection scope.** Freeze the procedure before unused confirmation evidence.
   Validation must run any automated selection included in that procedure.
   Simulating four already frozen return streams cannot certify an omitted
   human-guided development search. Human redesign inherits exposure; further
   confirmation requires genuinely unused evidence under a new approved scope.

## Validation design requirements

Retain the old failures as regression evidence, including ordinary/correlated
nulls, dependence, heavy tails, volatility clustering, rare losses, negative
drift and mixed null/non-null candidates. Do not remove a failed scenario to
obtain a pass. Any proposed restriction in the claim's applicability requires
an explicit scientific justification and review.

Freeze the exact method and a new final-validation declaration before its final
run. Reserve independent seeds and additional scenario variants which were not
used in method development; retain their custody and exposure history. Testing
old scenarios alone does not establish independent validation. Passing a finite
simulation suite supports the specified scenarios, not a distribution-free
claim over arbitrary future returns.

Preserve the simultaneous 95% binomial-interval convention, the supported-null
familywise upper limit of 2.50%, the strong-effect power lower limit of 80%, and
the existing 14-scenario, 2,000-replication-per-scenario regression requirement.
The independent final suite needs its own frozen enumeration and replication
count, with simultaneous uncertainty covering every declared rate. Added tests
must not silently weaken familywise coverage. Any change to these limits needs
a separately reviewed scientific justification, not a response to failure.

The full production procedure must report false positives, power, interval
bounds, Monte Carlo resolution, completion counts and all failures for every
declared scenario. No partial result can establish a pass. Before execution,
measure a small synthetic timing/memory/output pilot and declare maximum
replications, runtime, memory, output and cost for the complete run. Missing
resource limits block preparation; do not launch an unbounded calibration run.

**Owner decision now:** accept or amend these claim/assumption requirements
before selecting an exact method. Method specification and independent
validation design must then be complete and reviewed before inactive
implementation/final validation. If the information or assumptions cannot
support a meaningful test, close this qualification effort with that finding
and keep promotion blocked. No promise of an eventual profitable strategy is
part of acceptance.
