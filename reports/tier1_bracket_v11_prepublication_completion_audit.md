# Tier 1 bracket V11 prepublication completion audit

Status: `MECHANICS_COMPLETE_AWAITING_REGISTRATION`

This audit is a control assessment, not historical performance evidence. No V11
historical source row has been opened, no model has been fitted, and no
prediction or evaluation outcome exists.

## Original V4 findings

| Finding | V11 disposition | Authoritative control evidence |
|---|---|---|
| Non-tradable rows became executable | Remediated | `tier1_bracket_v10.iter_source_records_from_parquet_v10` retains source disposition and sets executability only for declared tradable dispositions; non-tradable and sessionless recovery tests prove fail-closed behavior. |
| Source-derived census hid absent sessions | Remediated | `tier1_bracket_v11_execution.execute_authorized_v11` loads the immutable registered calendar before constructing the census; missing sessions become checkpoint records rather than disappearing. |
| Drawdown was not continuous | Remediated | `tier1_bracket_v5.apply_continuous_risk_v5` marks favorable and adverse bar extremes, includes the ordinary exit bar, liquidates on the next executable bar, and carries the peak across trades. Adversarial tests cover intratrade drawdown, same-bar breach precedence, and incomplete liquidation paths. |
| Registered inference differed from code | Remediated | The locked implementation uses block sensitivities 5, 10, and 20; portfolio power alternative $30/session; sleeve power alternative $1.25/session; 5,000 training-power resamples; and 10,000 evaluation bootstrap resamples. |
| Power and economic effect were combined | Remediated | `classify_power_and_effect_v5` checks negative or uneconomic effect before inadequate power, and crossfit training series use realized candidate-minus-baseline session returns matching evaluation construction. |
| Coverage had no sufficiency gate | Remediated | Candidate, feature, market-year, model-availability, selected-path, and per-baseline gates run before performance inference. A failed gate returns `INCONCLUSIVE_DATA_OR_COVERAGE` or its registered fail-closed equivalent. |
| Risk limits drifted | Remediated | The inherited contract and simulator enforce a $250 maximum planned loss, three entries per session, daily-loss state, and $1,500 continuous drawdown protection. |
| Evidence chain was incomplete | Remediated in design | V11 evidence is create-only and content-addressed, with model, predictions, candidate opportunity ledger, all-strategy fills, all-strategy continuous marks and terminal ledgers, independent market-year views, inference, decision, source audit, authorization claim, dependency lock, and runtime receipt. Actual evidence remains nonexistent until separately authorized execution and publication. |

## Additional V4 faults

| Finding | V11 disposition | Authoritative control evidence |
|---|---|---|
| Equal uncertain exit and next entry bypassed overlap | Remediated | `entries_overlap_v5` treats equality as overlap unless exit-at-open is explicitly proven; the account simulator never supplies that exception for ordinary fills. |
| Synthetic proxy was labeled DSR | Remediated | Conventional DSR is explicitly not claimed without an observed, hash-bound trial-Sharpe census; the legacy proxy is excluded from promotion. |
| Always-long baseline was not demonstrably risk matched | Remediated | It uses the same opportunity-specific ATR bracket construction, stress costs, $250 planned-loss cap, and its own entries, scheduling, costs, equity, daily loss, and drawdown state. |
| Whole Parquet files were materialized | Remediated | The V10 source adapter uses `ParquetFile.iter_batches`; source materialization is bounded by market-session and checkpoint dependency windows. |
| Authorization used caller booleans | Remediated | V11 requires an exact externally authorized single-use operation receipt and atomically creates a durable local claim before source hashes or rows are opened. Publication requires another receipt and approval. |

## V10 defect found before execution

V10 incorrectly constructed every active baseline from the candidate prediction
identities. V11 now constructs a separate declared opportunity universe for
every strategy. Model-independent baselines retain every scoped calendar
checkpoint, including missing-feature checkpoints as explicit abstentions.
Candidate model unavailability cannot censor them. Each baseline receives its
own ranking, selected-path coverage, schedule, costs, and risk state.

## Remaining proof boundary

Synthetic verification proves mechanics only. The following remain deliberately
unproven until separate authorization: registered-source integrity at runtime,
real coverage rates, model availability, predictions, costs and fills on the
historical rows, statistical results, and the final promotion or rejection
classification. The 2025 holdout remains forbidden and untouched.
