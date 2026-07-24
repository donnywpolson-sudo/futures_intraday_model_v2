# Futures v2 operating rules

## Scope

- This repository may use only the immutable evidence paths declared in `configs/source_contract.json`; it must not import code, configuration, runtime state, or unreviewed outputs from `futures_intraday_model` or any stock project.
- The legacy repository is read-only evidence. Never edit, stage, commit, move, delete, or clean it.
- “Discover data” means enumerating, searching, opening, reading, or deriving from a data or artifact path. Code may discover data only within a source-family path declared in `configs/source_contract.json` and a verified immutable release whose `release_manifest.json` passes `src/futures_rebuild/release.py` verification.

## Safety and authorization

- Provider calls, paid downloads, real-history alpha tests, WFA, candidate sealing, order placement, remote pushes, and destructive cutover require explicit, separate user approval that identifies the action and its dataset, release or provider scope. Where a contract requires an approval record, do not proceed until its durable approval receipt is present and hash-bound; for research-universe admission, `configs/research_universe_contract.json` must be approved and its `approval_receipt_id` must match the receipt content hash.
- Migration is copy-only. Do not use hard links, junctions, symbolic links, or mutable references to a legacy path.
- Never use `git add .` or `git add -A`. Stage only reviewed paths after explicit authorization.
- Preserve immutable releases. Corrections create successor releases; they never overwrite accepted bytes.

## Research integrity

- Treat all existing historical data and observed results as discovery evidence.
- Synthetic smoke tests prove mechanics only and never alpha.
- Feature builders cannot read outcome, label, prediction-score, or evaluation paths. Enforce this through the separate feature/outcome/prediction schemas in `src/futures_rebuild/schemas.py` and their targeted tests.
- Inference cannot fit, read outcomes, place orders, or silently refresh a bundle. Enforce this through the inference eligibility and abstention checks and their targeted tests.
- A “real-data attempt” is any evaluation or modeling operation that consumes non-synthetic historical, live, provider, or legacy data. Before evaluation, it must have a durable `TrialRegistry` declaration under `state/trial_registry` that binds the required trial semantics and evidence/configuration hashes. A semantic change creates a new trial.
- “Unknown,” “stale,” “mismatched,” or “incomplete” means any input that fails its declared schema, hash, provenance, timestamp, or contract check. It must be rejected before evaluation or artifact publication; an inference decision may instead return an explicit abstention with a reason, and must not emit a score, trade, or refreshed bundle.

## Validation

- Use deterministic UTC timestamps, canonical JSON, content hashes, exact schemas, and clean-room reproducibility tests.
- Run targeted synthetic tests before broader validation.
- Definition of done: instruction-only changes require path/reference review and `git diff --check`; code changes require targeted tests for the changed contract before broader affected-suite validation; config or contract changes require exact-schema, hash, and fail-closed coverage; release or evidence changes require verified manifest and provenance checks.
- Keep `CODEX_HANDOFF.md` concise and reconcile it against current files and command evidence.

## Plain-English User-Facing Output

- Write every user-facing progress update, explanation, audit summary, and final response concisely and in plain English by default. The user should not need to ask, "Tell me this entire output concisely and in plain English."
- Lead with what the result means for the user. Translate technical findings and tool output into ordinary language instead of repeating raw logs or jargon.
- Include only the technical details, file paths, numbers, warnings, and evidence needed to understand the result or make the next decision.
- Do not remove important uncertainty, safety warnings, failed checks, limitations, or blockers for the sake of brevity. State them briefly and clearly.
- If a technical term is necessary, explain it in a short plain-English phrase the first time it appears.
