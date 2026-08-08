# Cash-open impulse Tier 1 pre-registration design

## Status

Rejected before registration by the authorized row-readiness gate. It is not
a strategy loss: no model was fitted, no prediction or return was computed,
and no economic evaluation occurred.

The proposed mechanism is materially different from the closed overnight
inventory reversal. It uses two short same-session windows and requires no
overnight contract continuity. It may proceed to registration only if the
separately authorized source census produces a row-certified PASS for every
locked dependency.

## Frozen mechanism

- Markets: ES, CL, ZN, and 6E; years: 2018-2022; 2025 remains excluded.
- Checkpoints: 09:00 and 10:30 America/Chicago.
- Feature dependency: exactly 30 consecutive reported one-minute bars ending
  one minute before the checkpoint.
- Decision: checkpoint plus five seconds, after the final feature bar's locked
  availability time.
- Entry: open of the checkpoint-plus-one-minute bar.
- Path: 31 consecutive reported bars through the open 30 minutes after entry.
- Identity: one actual standard contract throughout each feature and execution
  dependency; a roll or identity change fails that opportunity closed.
- Direction: sign of the 30-minute impulse; zero means abstain.
- Model: one market-specific, outer-fold-local ridge regression with fixed
  lambda 10.0 and training-only standardization. No feature, threshold, or
  hyperparameter search is permitted.
- Entry hurdle: predicted net expectancy must exceed +0.10R after stress costs,
  equivalent to more than $25 net on the locked $250 risk budget.
- Ranking: independently at each checkpoint across the four simultaneously
  observable markets. At most one winner per checkpoint and two entries per
  session. No future checkpoint score is available to an earlier decision.
- Risk: one standard contract, maximum $250 planned initial loss, $500 daily
  stop, and $1,500 continuous drawdown limit. Risk feasibility is terminalized
  separately for base, stress, and extreme costs.

The complete frozen contract is
`configs/cash_open_impulse_pre_registration_protocol.json`, whose current
protocol identity is
`3b8e09d65015afd33fc033aa72c8bb0be22425cafac8b8b145eeccb639258067`.

## Required independent baselines

1. True flat/no-trade, exactly $0 with no entries or costs.
2. Always long at the first checkpoint.
3. Always short at the first checkpoint.
4. First-checkpoint opening-impulse continuation.
5. First-checkpoint opening-impulse reversal.

Each active baseline owns its causal schedule, direction, path, costs, risk
dispositions, and later account simulation. Candidate admission is never
reused as a baseline schedule.

## Pre-registration proof gate

The prepared census must rehash the 20 immutable local sources and authoritative
calendar, read only 2018-2022 rows, and report all 32 outer-fold/market cells
plus market-year counts and exact exclusions. Registration fails closed unless:

- every fold has at least 504 complete training sessions and 63 complete
  evaluation sessions per market;
- every expected feature window and every possible candidate path is exact,
  causal, consecutive, and identity-stable;
- every active baseline path is independently supported;
- every cost-scenario risk result is either feasible or an explicit risk
  abstention, never unresolved;
- all calendar opportunities have a terminal source disposition;
- every source hash still matches; and
- the resulting generic fold certificate is an authorized-row PASS.

Synthetic fixtures prove these mechanics only. They do not prove the real
sources sufficient and cannot authorize registration.

## Current boundary

Prepared census plan:
`configs/cash_open_impulse_fold_readiness_census_plan.json`.

The census is $0 and creates only one unpublished readiness report plus the
single-use authorization record. It does not fit a model, generate predictions,
compute returns, evaluate performance, register a trial, publish evidence,
touch 2025, access a provider, mutate active data, or trade.

The first authorized attempt consumed authorization-use record
`9f2eb538...f3e4` but Windows denied multiprocessing pipe creation before any
worker started. No source row was decoded and no report was created. The attempt
is preserved and cannot be retried. An additive V2 plan retains identical
research semantics and changes only the required host permission for the four
worker pipes; it required a new authorization.

The separately approved V2 host successor completed and sealed unpublished
report `5c7fe871...6812`. Its certificate decision is `FAIL`; registration is
false. The report file SHA-256 is `57b6d368...8570`.

Exact certification results:

- 11 of 32 fold-market cells passed; 21 failed.
- ES passed seven cells, CL two, 6E two, and ZN zero.
- The full census found 145 incomplete sessions: 79 incomplete or
  identity-changing execution paths and 66 incomplete or identity-changing
  feature windows.
- Every source-complete candidate session was risk-feasible under base, stress,
  and extreme costs. Baseline risk-terminal failures were downstream effects
  of missing selected paths, not excessive dollar risk.
- Fold 2 had only 62 calendar-eligible evaluation sessions for every market,
  below the immutable minimum of 63. This alone makes the protocol impossible
  to certify without changing its calendar universe, fold requirement, or
  checkpoints.
- ZN was the least compatible market. Its worst evaluation cell had only 55 of
  63 complete sessions.

Disposition: `PRE_REGISTRATION_SOURCE_COMPATIBILITY_REJECTION`. Do not register,
fit, execute, or incrementally tune this prepared mechanism. The evidence says
only that the exact protocol cannot be completely executed on the bound source
family; it says nothing about profitability.
