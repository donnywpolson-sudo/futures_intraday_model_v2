# Overnight inventory reversal preexecution audit

Status: **COMPLETED — PUBLISHED ADDITIVELY**

This audit did not reopen the mechanism, compute a return, fit a model, or
authorize a retry. Trial
`24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c`
remains terminal `INCONCLUSIVE_DATA_OR_COVERAGE`; its one strategy attempt is
consumed.

## Exact reconstructed failure

The registered rule required at least 252 complete market-local training
sessions before fitting the fold-local median/MAD transformation. A complete
session required one actual identity across the overnight feature and exact
08:31–09:31 execution path.

The historical runtime consumed authorization, opened the 20 bound sources,
materialized the four markets, entered base scenario fold 0 in market order,
and stopped on ES. The row-certified census now proves the omitted context:

- fold: `fold-0`;
- market: `ES`;
- expected training sessions: 673;
- complete training sessions: 1;
- locked minimum: 252;
- exclusions: 664 incomplete-or-identity-changing paths, 5 missing exact 08:29
  feature bars, and 3 missing overnight entry or exit endpoints.

All 32 outer fold-market cells failed readiness. Every cell failed the 252
complete-training minimum and the 63 feature-complete-evaluation minimum. Only
fold 7 CL had any execution-complete evaluation session, and it had one. Fold
2 ES and CL also had only 62 expected evaluation sessions versus the locked 63.
No return, prediction, fit, or economic decision was produced.

The authoritative evidence is report
`abc910ff3...5b440`, certificate `0b8357d2...eefbc`, with file SHA-256
`51c38031...0bb24`. Additive closure clarification
`d4f97ae6...ea643` binds it to the preserved original closure.

## Classification

- Source/protocol compatibility failure: **proven**. The 20 bound sources could
  not satisfy the registered same-actual-identity completeness rule at the
  locked sample minimums.
- Historical strategy implementation defect: **not proven**. The runtime
  enforced the registered identity and completeness rules.
- Pipeline implementation defect: **proven**. The mandatory readiness check ran
  only after the one-use claim and source opening, and its exception discarded
  fold, market, counts, and exclusions.
- Certification omission: **proven**. Registration checked that eight folds
  existed but did not require mechanism-specific row-certified executability.
- Strategy failure: **not proven**. No economic evaluation occurred.

The combined exclusion code
`INCOMPLETE_OR_IDENTITY_CHANGING_EXECUTION_PATH` does not separately count
missing path minutes versus identity changes. It is nevertheless sufficient to
prove that the registered complete-session requirement failed. No further row
read or strategy retry is authorized.

## Evidence timing

- Before registration: the identity rule, 252-session minimum, eight fold
  definitions, 20 immutable source bindings, calendar, and required execution
  horizon were known. Aggregate and prior-strategy source records did not prove
  this mechanism's row-level executability.
- Before historical authorization: the same prerequisites remained available,
  and the pipeline could have required a separately authorized readiness census
  before allowing registration or the strategy claim. It did not.
- Only after authorized rows were opened: exact fold/market/market-year usable
  counts and exclusions became knowable. The strategy runtime discovered the
  first failure but discarded its context; the authorized audit census later
  preserved the complete 32-cell evidence.

## Remediated control

Concrete risk prevented: a trial can no longer consume its only historical
attempt on a mandatory sample or path requirement that was never certified.

Decision improved: future registration and historical authorization are
allowed only when every exact outer and applicable nested fold-market passes
its declared feature, execution, baseline, cost, risk, metric, and promotion
requirements.

The reusable gate in
`src/futures_rebuild/preexecution_fold_certification.py` requires exact fold
topology, actual source hashes, authorized row evidence, training/evaluation
minimums, purge and embargo, training-only transformations, causal feature and
execution paths, identity/roll terminalization, independent baseline universes,
a true no-trade baseline, scenario-specific risk dispositions, complete
metrics, and a computable promotion path.

The governed registration writer binds one canonical evidence path, file hash,
certificate ID, trial family, and protocol into the registration identity. The
execution wrapper reloads that exact registration and evidence before invoking
the single-use claim. Changed, failing, synthetic, or cross-trial evidence fails
before authorization consumption.

Synthetic tests prove mechanics only. Every future mechanism still needs its
own separately authorized, source-bound row certificate before registration.

## Census lifecycle

The first serial census consumed receipt `3b81b12f...e5312`, exceeded its
runtime, and produced no report. Its plan and receipt remain preserved and are
not retryable.

The separately approved parallel successor retained the same 20 sources,
parser, observation builder, fold evidence, and certificate semantics. Four
market workers completed within the approved runtime and consumed receipt
`6fc820d1...23d0`. Its readiness evidence and closure clarification were later
published additively without changing the original closure or active pointer. It did
not touch 2025, providers, credentials, economics, active data, or trading.
