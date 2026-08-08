# Alpha ES pilot execution package

## Status

Prepared, source-safe, and not authorized for historical execution.

- Trial: `a6ae7b8394906c3661b9f1456f30cf513d5a1df43a072c9e8a601bc8989c82bc`
- Registration SHA-256: `b2b78123080cf4eb9a09778f0815f12a0d7b1839e4e39d4c147566f6af2e8e44`
- Authoritative execution plan: `aeff50fab7f7f4733a9b3931821fe7020ecae827a52bc8ff923a930d49623ff9`
- Plan SHA-256: `1ec9b67d94336ca38ab39f9eafaee886f3168b114347fbbfa751de374f698411`
- Attempts: one; retries: zero; external cost: `$0`; runtime ceiling: 900 seconds.

The first local preparation, plan `ab6d2557...bd817e`, is preserved but is not
executable. Focused review found that its executor signature retained a synthetic
row-loader injection hook. The V2 plan removes that hook and binds the predecessor
as `INVALID_PREPARATION_SYNTHETIC_ROW_LOADER_INJECTION_SURFACE`.

## Concrete control value

The package prevents a certified claim from being consumed by substituted code,
sources, sessions, or synthetic input. This improves the pilot decision because
the sealed PASS or FAIL can only come from the registered mechanism and exact
row-certified fold. The existing gateway alone was insufficient because it binds
the trial and readiness certificate, but not the complete economic runner,
source subset, output root, runtime, or deterministic replay.

## Exact execution behavior

- `CertifiedResearchGateway.claim_historical_execution()` runs before any bound
  Parquet file is hashed or opened.
- Only ES 2018, 2019, and 2020 from the active catalog are bound.
- The exact 504 training sessions, 2020-01-13 embargo, and 63 evaluation sessions
  are immutable inputs.
- Nine causal features use training-only population standardization.
- Two Ridge targets use penalty `1.0`, no search, and an unpenalized intercept.
- Ordered argmax retains LONG on exact ties; the `+0.25R` hurdle is inclusive.
- Candidate and all six mandatory baselines own separate schedules, paths, costs,
  risk state, equity, and drawdown.
- Base, stress, and extreme daily series retain all 63 sessions, including zeros.
- The pilot gate is exactly eight trades, positive stress net P&L, strict victory
  over zero and every mandatory baseline, drawdown at or below `$1,500`, and
  complete coverage and metrics.
- Results are unpublished, create-only, and terminalized last. No raw source rows
  are copied into evidence.

## Verification

- Focused executor suite: 14 passed.
- Complete current lane: 107 passed.
- Complete high-risk source-safe lane: 851 passed; 880 separately classified
  tests were deselected and were not counted as passing.
- No historical rows, returns, model fits, predictions, or economic outcomes were
  opened during preparation or testing.
- No provider, network, credential, 2025, broker, order, or trading access occurred.
- The package exposes no direct historical CLI.

## Next controlled boundaries

Exact repository-consolidation scope:

1. `CODEX_HANDOFF.md`
2. `configs/alpha_ladder_es_pilot_execution_plan.json`
3. `configs/alpha_ladder_es_pilot_execution_plan_v2.json`
4. `reports/alpha_ladder_es_pilot_execution_package.md`
5. `scripts/prepare_alpha_ladder_es_pilot_execution_plan.py`
6. `src/futures_rebuild/alpha_ladder_es_pilot_execution.py`
7. `tests/test_alpha_ladder_es_pilot_execution.py`
8. `tests/test_operational_documents.py`

1. Stage only this package's exact paths after separate approval.
2. Commit them after separate approval.
3. Push the commit after separate approval.
4. Only from the clean pushed HEAD, issue one literal authorization bound to the
   final plan and execute the ES pilot once.
