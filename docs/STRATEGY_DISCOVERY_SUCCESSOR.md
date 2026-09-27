# Inactive strategy-discovery successor

`configs/fixed_hour_strategy_discovery_v1.json` prepares a separate experiment.
It does not replace the preserved comparison v2 or profit-aligned v6 experiment,
activate a pointer, register research, or grant real-data access. The latter's
failed statistical calibration remains unchanged. This successor retains the
comparison's session-balanced stress-net-tick objective and historical DSR gate;
it is not a replacement statistical procedure or a claim of profitability.

## Selection and qualification

The existing nine packs plus `context_discovery` cross three Ridge penalties
and two shallow XGBoost profiles: 50 candidates per market, 200 across ES, NQ,
CL and GC. Selection stays inside the four inner chronological folds of each
of eight outer folds. All candidates share the same eligible observations.
The score, deterministic simplicity ties, training-only transforms, purge,
embargo, 60-minute target/exit, stress costs, and complete fitting replay remain.

A Ridge winner may qualify. The six non-Ridge baseline requirements and all
other economic, activity, coverage, quality, segment, DSR and downstream gates
remain. The independent Ridge comparison is diagnostic: equal streams report
no added value with p=1. All 28 declared comparisons remain accounted for;
failed/untestable comparisons retain p=1. Neither an economic failure nor the
Ridge diagnostic may trigger a replacement selected from outer results.

## Single additional pack

The new pack is the existing context pack plus five fixed features. On the
same 61 validated closes, calculate 60 one-minute changes in ticks and
`s = max(population_std(changes), 1)`:

- 5-, 20-, and 60-minute tick changes divided by `s * sqrt(horizon)`;
- last close minus the 61-close mean, in ticks, divided by `s`;
- positive squared changes minus negative squared changes, divided by total
  squared changes, with zero for a flat window.

Subtract integer prices before floating-point conversion. The same causal
history, gap, contract, interval, tick and nonfinite checks apply. Negative and
zero prices remain valid. No lookback, threshold, extra interaction or subset
search is introduced. These are unproven hypotheses. The asymmetry is a
tick-change proxy inspired by semivariance, not a published-strategy replication;
bundle selection cannot attribute improvement to an individual new feature.

## Cost diagnostics and accounting

For each market's selected procedure and independently selected Ridge, report
executed trades, `(gross dollars - fees) / trades`, and `stress-net dollars /
trades`. These are pooled per-trade break-even adverse execution and remaining
stress margins, not session-balanced selection scores. Negative margins remain;
zero trades produce explicit unavailable values. Frozen trades and the existing
cost schedules determine the arithmetic. Reports do not change selection or
promotion, calibrate costs, or claim achievable fills or capacity.

Accounting retains the existing terminal, comparison and profit-aligned family
charges and adds this 200-configuration declaration once. Repeated preparation
versions are not new experiments. Registration-only inventory is distinct from
observed performance; the arithmetic floor is not a historical search upper
bound and supplies no missing Sharpe ratios.

Historical research remains `BLOCKED_CENSUS_RECOVERY_UNRESOLVED`, source-bound
readiness is `NOT_CERTIFIED`, and expected costs remain
`EXPECTED_COST_UNCALIBRATED` without existing accepted cost evidence. No gate is
waived, and the prospective bounded-score assessment is not substituted.

## Synthetic preparation

Run the focused tests with the existing virtual environment:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_fixed_hour_strategy_discovery.py
.\.venv\Scripts\python.exe -m futures_rebuild.fixed_hour_strategy_discovery_preparation --prepare
```

The prepare-only command constructs generated bars in all four markets, runs
the complete frozen search and evaluation replay, checks missing-census and
downstream failure behavior, and writes create-only inactive contracts with
synthetic evidence. It has no real-data execution option. Engineering success
proves mechanics, not statistical calibration, alpha, or profitability.
