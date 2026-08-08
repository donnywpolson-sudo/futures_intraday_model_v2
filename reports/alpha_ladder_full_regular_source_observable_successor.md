# Alpha full-regular/source-observable successor preparation

## Status

Prepared locally and unpublished. No trial was registered or executed.

## Preserved closure

- Mechanism: `50dfc52cb5b4145dcbd6a761b3c626dae28c0aa974f6db35a1b60099297034e5`
- Classification: `PRE_REGISTRATION_SOURCE_INCOMPATIBLE`
- Closure: `e9bc7a727e74d77df257225dcf83cb9c7a3ad3880a3a2e060311a2f84e25547e`
- Closure SHA-256: `cf7878784352935c4d91ac77564ab2dc4017fae9b000535a91b8446b0e408428`
- Economic result: not produced
- Strategy-failure conclusion: false

The sealed evidence remains unchanged. It reconciles nine failed fold-market
results to 12 market-session feature gaps: two corrected calendar closures, six
explicit source-unobservable sessions, and four holiday-modified ZN sessions.

## New counted mechanism

- Mechanism: `cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3`
- Mechanism SHA-256: `b63305f7d12e393e5fa7289913c23b47087eee4f3f52ca99e70621b70e3111a1`
- State: `PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED`
- Registration/execution/publication authority: false
- Synthetic Tier 0 certificate:
  `cc5535ede6b07ef78a82fc6c071f6c90106e55ab9275e408349ddc74a253a36b`
- Synthetic Tier 0 decision:
  `3329490bc9921e39e2c5485df496d52e11def3422c3fba4b2dcdacc5e91e55d1`
- Tier 0 decision: `PASS` (engineering evidence only)

The only semantic change is session eligibility before fold construction:

1. The 10:00 checkpoint must be open in the hash-bound authoritative calendar.
2. The session must use the full regular-weekday disposition; every
   holiday-modified open is an explicit abstention.
3. The market-session must not have an explicit source-unobservable record.
4. Every exclusion remains hash-bound and explicitly accounted; unknown open
   calendar states fail closed.
5. The candidate and active baselines share the eligibility predicate but keep
   their independent schedules and simulations.

Pre-data calendar accounting across ES, CL, ZN, and 6E:

- 7,304 calendar provenance rows
- 5,161 calendar-open checkpoints
- 4,992 full-regular, provisionally source-observable fold sessions
- 163 holiday-modified abstentions
- 6 source-unobservable abstentions
- 2,143 closed calendar rows

This is not row-level readiness evidence. A new 100% row-certified census is
still mandatory before pilot registration.

## Unchanged controls

Features, transformations, model family and parameters, checkpoint, entry and
exit rules, stress costs, one-contract sizing, $250 planned-loss cap, $500 daily
loss limit, $1,500 drawdown limit, independent baselines, folds other than the
eligibility basis, metrics, statistics, and promotion gates match the preserved
predecessor.

## Verification

- Full default current lane: 103 passed.
- Full high-risk lane used for certification: 852 passed.
- Seven superseded pre-execution/publication snapshots are explicitly routed to
  the legacy lane. Current calendar lifecycle, protocol, source eligibility,
  gateway, evaluator, and statistical controls remain in the live lanes.
- Certificate SHA-256:
  `92a70595068ba44d30cad6b2b4dba80bda50d77c3fd0b9671c687ea9e0cc0f93`
- Decision SHA-256:
  `d1ae5695b3a562cb04be3b8506b332a7bb13e732b13febf48a1da566e073db91`
- Historical price rows, returns, fitting, predictions, evaluation, providers,
  credentials, 2025, active data, and trading paths were not accessed.
- Nothing was staged or committed.

## Next boundary

Prepare one immutable ES/CL/ZN/6E 2018-2022 row-readiness census plan bound to
this mechanism, certificate, active catalog, and authoritative calendar. Its
historical-row execution remains a separate approval boundary and must pass
100% checkpoint, baseline, feature, and filled-entry exit-path coverage before
pilot registration.
