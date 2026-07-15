# Futures v2 operating rules

## Scope

- This repository is independent of `futures_intraday_model` and every stock project.
- The legacy repository is read-only evidence. Never edit, stage, commit, move, delete, or clean it.
- No code may discover data outside paths declared in `configs/source_contract.json` and a reviewed release manifest.

## Safety and authorization

- Provider calls, paid downloads, real-history alpha tests, WFA, candidate sealing, order placement, remote pushes, and destructive cutover require separate user approval.
- Migration is copy-only. Do not use hard links, junctions, symbolic links, or mutable references to a legacy path.
- Never use `git add .` or `git add -A`. Stage only reviewed paths after explicit authorization.
- Preserve immutable releases. Corrections create successor releases; they never overwrite accepted bytes.

## Research integrity

- Treat all existing historical data and observed results as discovery evidence.
- Synthetic smoke tests prove mechanics only and never alpha.
- Feature builders cannot read outcome, label, prediction-score, or evaluation paths.
- Inference cannot fit, read outcomes, place orders, or silently refresh a bundle.
- Every real-data attempt must be registered before evaluation. A semantic change creates a new trial.
- Unknown, stale, mismatched, or incomplete inputs must fail closed or abstain.

## Validation

- Use deterministic UTC timestamps, canonical JSON, content hashes, exact schemas, and clean-room reproducibility tests.
- Run targeted synthetic tests before broader validation.
- Keep `CODEX_HANDOFF.md` concise and reconcile it against current files and command evidence.
