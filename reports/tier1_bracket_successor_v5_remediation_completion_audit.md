# Tier 1 V5 remediation completion audit

Status: implementation complete; create-only V4 retirement and V5 registration publication remain separately approval-gated.

Prepared identities:

- V4 retirement record: `22b2ee6a3c499888e0b16d745eb90313919f0ff5c9f35e3037f5ebf09763e654`
- V5 trial: `8f6fed0171979ffe76256117c29937bc1f469f674d722525414b16ca5bfd4e03`
- V5 historical checkpoint-calendar index: `038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3`

## Requirement-by-requirement disposition

1. Source eligibility: remediated. The Parquet reader requires `disposition`; only the explicit tradable allowlist is executable. Missing, unknown, or quarantined values become non-executable records.
2. Independent opportunity census: remediated. The census is generated from the immutable CME-authored 2018-2022 checkpoint calendar. Every market/date/checkpoint exists even when an entire price session is absent, producing `MISSING_SOURCE_SESSION` abstentions.
3. Continuous risk: remediated. Favorable extremes update the equity peak; adverse extremes, including the ordinary exit bar, update drawdown and daily-loss state before the ordinary exit; a breach liquidates at the next causal open or makes the path incomplete. Promotion uses the resulting intraday marked-equity drawdown.
4. Registered inference alignment: remediated. Bootstrap sensitivity runs 5, 10, and 20-session blocks with 10,000 resamples. Training power uses 5,000 resamples, a $30 portfolio alternative, and a $1.25 sleeve alternative.
5. Power versus economics: remediated. Negative or uneconomic effects fail before insufficient-power classifications. Nested training crossfit constructs the same realized candidate-minus-baseline session returns and realized policy-sleeve contributions used in evaluation.
6. Coverage gate: remediated. Terminal-ledger coverage must equal 100%; overall causal-feature coverage must be at least 95%; each market-year at least 90%; prediction coverage of feature-eligible opportunities at least 99%. Missing or malformed coverage is invalid.
7. Risk mandate: remediated. Planned initial and total open risk are capped at $250; continuous drawdown is capped at $1,500; daily loss remains $1,000.
8. Evidence chain: remediated. Model, predictions, opportunity ledger, fills, continuous equity marks, segmented metrics, inference, decision, and runtime receipt are create-only, individually hashed, and bound by one manifest. Registration binds the dependency-lock receipt, calendar dependency closure, V5/V4 engines, release verifier, locking, errors, and all local statistical dependencies.
9. Equal-timestamp overlap: remediated. Equality blocks admission unless the prior exit is proven at the bar open.
10. DSR labeling: remediated. Conventional DSR is not claimed without an observed, hash-bound trial-Sharpe census; the old normal-quantile proxy is explicitly non-DSR and cannot promote.
11. Risk-matched always-long baseline: remediated. It builds its own long signals and schedule, uses the same per-entry $250 planned-loss cap and locked costs, and runs through an independent account/risk path.
12. Memory bound: remediated. Parquet is consumed with bounded `iter_batches`; sessions are buffered one at a time with a 2,000-row hard limit. Whole-file `to_pylist` is absent.
13. Durable authorization: remediated. Historical execution requires a single-use, user-approved operation receipt bound to trial ID, source-binding ID, output root, publication false, provider access false, and holdout access false. A local boolean-like receipt is rejected.

## Verification

- Focused V5/calendar tests: 33 passed.
- Combined V4 preservation, post-audit, V5, and calendar tests: 67 passed.
- Current repository test lane: 113 passed.
- Python compilation: passed.
- `git diff --check`: passed.
- V4 registered files verify against their frozen registration hashes.
- Corrected calendar census: 7,304 market-dates and 21,912 unique checkpoints, exactly 2018-01-01 through 2022-12-31.
- No V5 registry/event exists for the prepared trial ID.
- No price rows, model fitting, predictions, evaluation, 2025 data, staging, commit, push, or trading occurred during this audit.

## Remaining boundary

The implementation is ready to freeze. Publication requires one separate approval and creates only the V4 retirement registry/event and V5 trial registry/event. Historical execution remains a later, separate real-data approval.
