# Search charges and performance evidence

Status: implemented accounting successor, inactive. Statistical replacement is
specified but not validated for qualification. No historical run, policy cutover,
or holdout access is authorized.

## What is fixed

`configs/fixed_hour_search_accounting_v1.json` separates the conservative search
ledger from actual performance evidence without changing the model-comparison
v1 bundle, its DSR gate, or active policies.

The retained 469 charges comprise 39 legacy observed-attempt charges, 65
unresolved-reference charges, one safety-margin charge, 76 other predecessor
charges, 108 prepared Ridge-successor configurations and 180 current comparison
configurations. These are accounting categories, not 469 measured Sharpe ratios.
They also do not establish a proven upper bound on undocumented historical search.

Performance records have separate statuses: observed complete, observed partial,
aborted without performance, not evaluated, or unresolved. Only observed-complete
records may carry a finite Sharpe and a comparison-scope identity. Partial or
absent results cannot be zero-filled. Conflicting identities are rejected;
deduplicating evidence never refunds charges. A complete supplied subset is not
automatically a complete historical population.

The prepared inventory imports two audited dispositions: a Ridge failure after
one outer fold and a mechanical abort before fitting. Neither supplies a complete
comparable Sharpe. Other result artifacts are not silently declared absent.

## Statistical-policy successor

The proposed alternative uses Bonferroni adjustment over a defensible search
upper bound and all 28 market/baseline claims, while retaining the existing
economic, chronology, coverage, activity, baseline and Ridge-added-value rules.
It is not described as DSR or as equivalent to DSR. Missing claim evidence gets
no rejection credit; missing performance does not acquire a synthetic Sharpe.

The implementation tests adjustment arithmetic only. Bonferroni control requires
valid individual p-values and a family covering the search. It does not repair
invalid time-series inference, undocumented adaptive search, or a lower bound
mistaken for an upper bound. See the [multiple-testing derivation](https://katsevich-teaching.github.io/stat-9610-notes/multiple-testing-chapter.html).
DSR depends on information about trial Sharpe dispersion, not merely an accounting
count; see the [original DSR paper](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf).

Even assuming an upper bound of 469, there would be 13,132 claim slots and a raw
p-value threshold of approximately 0.00000380749. The existing 999-resample
plus-one calculation has minimum p=0.001 and cannot resolve that threshold.
At least 262,639 resamples would merely reach it; that is not adequate-precision
or calibration evidence. No bootstrap budget is increased by this implementation.

## What still blocks qualification

1. Establish a defensible search upper bound before any new result access.
   Retaining the 469 floor is mandatory but is not proof of such a bound.
2. Validate p-values for the entire nested selection procedure under declared
   dependent-date null scenarios, with predeclared false-positive, precision,
   power and sample-adequacy limits. Synthetic arithmetic tests are insufficient.
3. Bind an inactive successor of global readiness, Phase 3, Phase 6 and the
   qualifier to the validated rule. Do not replace only the local DSR branch.
4. Obtain separate policy activation and historical-access authority.

Until those requirements are met, the existing qualifier stays unchanged and
blocked. The new module cannot qualify markets, waive DSR, activate a policy,
or claim that either synthetic success or absent historical evidence proves alpha.
