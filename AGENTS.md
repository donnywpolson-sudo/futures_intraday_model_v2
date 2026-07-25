# Futures research operating rules

## Project boundary

- This repository is the complete operational home for the futures research
  pipeline and observation-only live cockpit.
- Discover or consume data only through source-family paths declared in
  `configs/source_contract.json` and verified immutable release manifests.
- External or retired repositories are provenance evidence only. Never import
  their code, configuration, runtime state, credentials, or generated research
  outputs, and never require them at runtime.
- Preserve accepted releases. Corrections create immutable successors; they
  never overwrite accepted bytes.

## Source-of-truth roles

- `AGENTS.md`: durable safety, authorization, research-integrity, and completion
  rules.
- `PROJECT_OUTLINE.md`: authoritative objective, profile ladder, Phase 1A-11
  workflow, commands, gates, outputs, and stop conditions.
- `CODEX_HANDOFF.md`: concise current execution and continuation state.
- `README.md`: setup, orientation, credentials, pipeline, and cockpit use.
- `MASTER_AUDIT.md`: canonical non-authorizing evidence audit.
- `META_MASTER_AUDIT.md`: independent audit-quality and false-pass review.
- `configs/research_universe_contract.json`: canonical point-in-time universe
  admission and cohort authority.
- `configs/alpha_tiered.yaml`: validated operational profile view; never an
  admission or holdout authority.
- `configs/source_contract.json`: allowed immutable source families and roots.
- `manifests/**`: durable content-addressed release, approval, and provenance
  evidence.

## Safety and authorization

- Provider calls, downloads, real-history evaluation, WFA/OOS, prediction
  materialization, candidate sealing, holdout/forward access, live smoke,
  paper/shadow/live trading, order placement, remote push, and destructive
  cutover each require separate explicit approval for the exact scope.
- When a contract requires a durable receipt, do not act until its content hash
  and scope match the planned action. The universe `approval_receipt_id` must
  equal the exact receipt content hash.
- Copy migrations are no-overwrite and copy-only. Never use hard links,
  junctions, symbolic links, mutable references, or destructive source changes.
- Never use `git add .` or `git add -A`. Stage only explicitly reviewed paths
  after authorization. Never stage, commit, package, archive, print, or report a
  credential.
- Preserve the previous cockpit installation and shortcut metadata until the
  new installed version passes its approved verification and rollback test. Do
  not create auto-start behavior.

## Research integrity

- Existing historical data and observed results are discovery evidence.
  Synthetic tests prove mechanics only and never alpha.
- A real-data attempt is any evaluation or model operation using non-synthetic
  historical, live, provider, or imported data. Before it begins,
  `state/trial_registry` must contain a durable declaration binding its
  semantics, evidence, configuration, stop rules, and multiplicity policy.
- Feature builders cannot read outcome, label, prediction-score, or evaluation
  paths. Inference cannot fit, read outcomes, refresh a bundle, place an order,
  or suppress an abstention.
- Profiles may narrow the approved universe. They cannot admit a market, change
  selection eligibility, unlock holdout/forward cohorts, or use satellite
  results to rescue traditional-universe failure.
- Missing, stale, mismatched, incomplete, ambiguous, or future-known inputs fail
  closed before evaluation or publication. Observation/inference may instead
  return an explicit reasoned abstention without a score or trade.

## Cockpit contract

- The Futures Live Cockpit is observation-only. Keep order, broker, execution,
  and trading-control code outside its dependency graph and public API.
- Provider errors, unavailable history, stale state, or missing predictions
  produce bounded visible errors or abstention; never reconnect indefinitely or
  fall through to an order path.
- Resolve `DATABENTO_API_KEY` from the ignored v2-local `api.env` (or an explicit
  environment variable) without logging it. The credential must not be copied
  into an installation or package.

## Validation and completion

- Use UTC, canonical JSON, content hashes, exact schemas, deterministic
  ordering, clean-room reproduction, and fail-closed tests.
- Run targeted synthetic tests before broader affected suites. Code changes
  require targeted contract tests; config changes require schema/hash/drift
  tests; release changes require full manifest and provenance verification.
- Final closure requires `python -m pytest`, dependency-lock verification,
  secret and external-path scans, standalone operation, `git diff --check`, and
  a clean committed Git state.
- A state is ready only when its root Master Audit result is `SUPPORTABLE`.
  Final closure also requires a `SUPPORTABLE` Meta Audit with no unresolved
  Critical/High or P0/P1 gap.
- Keep `CODEX_HANDOFF.md` short and reconcile it with current files, command
  evidence, manifests, and Git state.

## User-facing output

- Lead with what the result means. Use concise plain English.
- Include only the paths, identifiers, counts, warnings, uncertainty, and next
  authority decision needed to understand or continue the work.
