# Tier 1 V10 prepublication control audit

Status: prepared-only mechanics audit. This document is not a trial
registration, historical evaluation, evidence publication, promotion decision,
or authorization to read source rows.

## Original V4 findings

| # | Required correction | Current control evidence | Status |
|---|---|---|---|
| 1 | Preserve source disposition and never make non-tradable rows executable | `source_record_from_mapping`; V5 and V10 non-tradable-row tests | Proven in code and synthetic tests |
| 2 | Use an independent census that retains wholly missing sessions | Immutable CME checkpoint calendar; missing-session abstention tests | Proven in code and synthetic tests |
| 3 | Use marked intraday equity for continuous drawdown, including favorable peaks and adverse exit-bar movement | V5 continuous-risk and incomplete-liquidation adversarial tests | Proven in code and synthetic tests |
| 4 | Match registered 5/10/20 bootstrap sensitivities, $30 portfolio alternative, $1.25 sleeve alternative, and 5,000 power resamples | V5 inference-alignment tests | Proven in code and synthetic tests |
| 5 | Classify negative economics as failure independently of statistical power, using evaluation-consistent returns | V5 effect-classification and crossfit tests | Proven in code and synthetic tests |
| 6 | Enforce terminal, causal-feature, market-year, prediction, and model-availability coverage gates | V5 coverage tests and V9 empty-fold/model-availability tests | Proven in code and synthetic tests |
| 7 | Restore the $1,500 continuous drawdown mandate and $250 planned/open-risk caps | Hash-bound inherited V9 contract verified by the V10 loader | Proven in prepared contract chain |
| 8 | Bind predictions, ledgers, fills, marks, inference, runtime, and dependency lock into create-only evidence | V5 and V9 evidence-manifest/create-only tests | Proven in code and synthetic tests; no V10 evidence published |
| 9 | Block equal-timestamp overlap unless the prior exit is proven at the bar open | V5 equal-timestamp adversarial test | Proven in code and synthetic tests |
| 10 | Do not label the synthetic proxy as conventional DSR | V5 DSR labeling test and promotion fail-closed rule | Proven in code and synthetic tests |
| 11 | Give always-long its own risk-capped signals, schedule, costs, and account path | V5 baseline and independent-path tests | Proven in code and synthetic tests |
| 12 | Stream Parquet in bounded batches rather than whole-file `to_pylist()` | V5/V10 streaming readers and source-inspection test | Proven in code and synthetic tests |
| 13 | Require a durable single-use external authorization receipt | V5/V9 boolean-rejection, create-only-consumption, and 2025 rejection tests | Proven in code and synthetic tests |

## Post-V4 continuity defect and V10 correction

V6 through V9 interpreted every adjacent timestamp discontinuity sharing a
session label as proof that the whole session was ambiguous. The registered
calendar declares decision checkpoints, not a full expected-minute tape, so
that inference was unsupported and could censor unrelated checkpoints.

The prepared V10 adapter instead:

- retains each source row exactly once and preserves executability;
- treats adjacency discontinuities as diagnostics, not missing-bar proof;
- requires an exact causal 61-bar feature window at each checkpoint;
- requires entry after the decision and a minute-contiguous causal prefix only
  through each direction/scenario-specific exit; a later gap cannot erase an
  already proven fill;
- converts a missing required dependency into a checkpoint-specific abstention;
- scopes duplicate timestamps to the checkpoints whose exact dependency
  windows contain them, rather than erasing an unrelated full session;
- retains wholly absent sessions through the independent calendar census; and
- leaves the strategy, model, costs, risk, inference, and promotion rules frozen.

Synthetic adversarial tests prove that a discontinuity outside a checkpoint's
dependencies does not erase that checkpoint, while a missing minute inside the
feature window fails closed.

## Remaining completion evidence

The authorized read-only dependency-window census completed in memory. It
established that the whole-session adjacency rule was overbroad, while also
exposing a separate V9 nested-crossfit defect: prediction eligibility depended
on future outcome availability. The prepared V10 decision-validity extension
now makes prediction eligibility depend only on causal features, treats risk
rejections as zero-return policy abstentions, freezes ranking before outcome
lookup, and forbids substituting an observable runner-up when the selected
intent has a missing path.

V10 remains prepared only. The V9 disposition and full V10 registration must
be prepared, reviewed, and published create-only under separate approval. No
historical model or return result may be opened under V10 before registration.
