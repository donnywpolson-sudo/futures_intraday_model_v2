# Alpha ladder frozen-mechanism implementation

Historical disposition: mechanism `186d8a10...5de3f` was rejected before
registration for source incompatibility. This report preserves its preparation
and synthetic certification; it is not the current counted mechanism.

Status: ladder active; frozen mechanism and Tier 0 synthetic PASS are sealed
as unpublished evidence.

## Completed

- Added one ladder-wide mechanism builder and validator. It retains the causal
  10:00 reported-bar ridge design while rejecting reuse of the old six-market
  universe.
- Locked the canonical 41-market ranking, stage-restricted universes,
  provisional cost schedule, standard-contract risk rules, six independent
  baselines, metrics, bootstrap settings, and pilot/Tier 1/Tier 2/Tier 3 gates.
- Locked the pilot at eight minimum trades and no formal-confirmation claim.
  Tier 1, Tier 2, and Tier 3 use the predeclared 3/4, 11/16, and 26/38 breadth
  requirements and their subgroup rules.
- Strengthened registration so it binds the exact immutable mechanism file and
  mechanism identity. A changed mechanism, ladder, source certificate, session
  manifest, or predecessor decision fails closed.
- Strengthened the pilot boundary so the ES pilot cannot register unless both
  its exact 504/63 row certificate and the complete four-market Tier 1 row
  certificate pass against session manifests carrying the same 63 exclusions.
- Added synthetic Tier 0 certificate semantics that explicitly deny historical
  evidence, alpha evidence, profitability claims, or authority.
- Added a prepare-only script which, after ladder activation, runs the focused
  synthetic targets before create-only writing the unpublished mechanism and
  Tier 0 certificate.

## Checks

- Focused ladder, gateway, readiness, mechanism, accounting, and bootstrap
  checks passed.
- Complete current lane passed 103 tests, and the final complete high-risk lane
  passed 771 tests with 767 intentionally deselected.
- No historical rows, returns, model fit, prediction, economic evaluation,
  provider, network, credential, 2025, active research data, or trading path
  was accessed.

## Publication order

1. Publish the already prepared contract, profile, and invalid-preparation
   records under contract `d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18`.
2. Write `configs/active_alpha_research_ladder.json` last and validate the
   registered context; restore the exact no-pointer predecessor on failure.
3. Run the prepare-only frozen-mechanism script and its Tier 0 synthetic suite.
4. Prepare, but do not execute, one separately authorized combined readiness
   census for the ES pilot and four-market Tier 1.

Publication and activation authorize none of the historical-row work in step
4. That remains a separate approval.

## Completed activation identities

- Active ladder pointer SHA-256:
  `e4274219b2117af6926845866726a8dc2192754941fc5abdba3c9a7156c5edc9`
- Frozen mechanism ID:
  `186d8a103a581ae8c27fc531e0a556070991c9d2f87bbe5d62c1478867b5de3f`
- Frozen mechanism SHA-256:
  `1b0fa1d2beb1b463ec5c37f1341cca348a7ce1fee6d9dbae6074603b5ec37798`
- Tier 0 certificate ID:
  `e05d30374c4ca3ea0df96beb568828448dada87c7f9dd2abd4f44cecceb29a05`
- Tier 0 decision ID:
  `6c9aca048217b364c47dfeac9a467c08735b9f2663a5c8fbae1919d6b8cb2175`
- Tier 0 preparation suite: 102 passed.

## Exact local implementation paths

- `src/futures_rebuild/alpha_ladder_frozen_mechanism.py`
- `src/futures_rebuild/alpha_research_ladder.py`
- `scripts/prepare_alpha_ladder_frozen_mechanism.py`
- `tests/test_alpha_ladder_frozen_mechanism.py`
- `tests/test_alpha_research_ladder.py`
- `tests/test_preexecution_fold_certification.py`
- `CURRENT_WORKFLOW.md`
- `CODEX_HANDOFF.md`
- this report

Nothing is staged or committed by this implementation.
