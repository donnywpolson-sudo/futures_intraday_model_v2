# futures_intraday_model_v2

This repository is the independent, controlled rebuild of the futures research project. The legacy repository at `C:\Users\donny\Desktop\futures_intraday_model` is immutable evidence and must remain unchanged.

Current status: `REBUILD_IN_PROGRESS`.

The pre-copy implementation and its synthetic/adversarial tests are complete. The exact 9,122-file, 33,313,291,079-byte inventory was independently reproduced, and its hash-bound copy approval is checked in. The local source copy, real DBN foundation build, and mechanical readiness publications have not run. There is no alpha, candidate, live-readiness, or trading claim.

## Controlled-rebuild boundary

The controlling user authorization permits hash-verified copying of approved data already stored locally, including the existing Databento DBN archive. It does not authorize a Databento download or any other provider call.

The authorized copy manifest is exact and copy-only: no overwrite, move, link, junction, or symlink is allowed; source stability and destination SHA-256 verification are mandatory. The detailed inventory was reproduced byte-for-byte and the exact approval is checked in. No copy has run yet.

Hard pauses remain in force for:

- paid or free provider downloads and calls;
- real-history hypothesis testing, WFA, or OOS evaluation;
- candidate sealing, live or shadow trading, and order placement;
- destructive cutover, legacy-repository writes, and external pushes.

## Source roles

- The eight local Databento DBN families are the only authoritative historical source group after exact hash-copy, snapshot publication, and re-verification.
- Legacy Phase 1B raw parquet and Phase 2 causal parquet are comparison evidence only. They can be used for reconciliation but can never become authoritative v2 inputs.
- V2 must regenerate Phase 1B and Phase 2 from the verified DBN snapshot. Only those v2 releases may feed features, outcomes, or later research.
- Legacy coordination, audit, economics, and research files are evidence only and cannot reactivate a closed research line.
- `FuturesLiveCockpit.exe` is a generated charting artifact copied, if approved, only to an evidence path. It is non-active, untrusted, non-executable, and has never been executed by this rebuild.
- Long legacy research paths are copied to short `evidence/legacy_research/by_family/` aliases for Windows path safety. Each binding still preserves the exact logical legacy-relative source path, byte size, and SHA-256, so the alias does not weaken provenance.

## Authoritative v2 flow

```text
verified local Databento DBN snapshot
  -> v2 Phase 1B actual-contract raw releases
  -> v2 Phase 2 causal/as-of-available releases
  -> independent feature releases + separate outcome-source releases
  -> separately authorized historical WFA/OOS program
  -> separately authorized sealed candidate
  -> fit-free, no-order prospective inference
```

The current authorization stops before the historical WFA/OOS step. A future mechanical `HISTORICAL_RESEARCH_READY` receipt would only prove exact non-alpha prerequisites; it would not authorize reading historical outcomes or claim alpha.

Continuous symbols are selection references only. Databento `.v.0` uses previous-trading-day volume rank 0 on original unadjusted prices. Trusted rows retain the bar's actual instrument identity, raw contract symbol, definition lineage, and verified actual-contract economics. Future mapping boundaries and retrospective mapping ends are never features.

## Status/statistics and coverage gates

Status and statistics are causal, as-received source families. At a decision time, only records whose event and receive timestamps are already available may be used. Missing, unknown, halted, suspended, deleted, or ambiguous status fails closed as `STATUS_UNRESOLVED`; the row remains in the coverage denominator and is not eligible for a feature or trade.

A production foundation preserves an all-history archive census, including
unresolved rows. Trust-eligible historical research is separately restricted by
`configs/status_research_scope_policy.json` to intervals beginning
`2025-01-01`, the first complete UTC year after Databento introduced CME status
in July 2024. This boundary is source-capability based, not chosen from model
outcomes. Every selected interval at or after the boundary must pass; earlier
history remains immutable and explicitly abstains.

The research scope must satisfy all of these gates:

- at least 1,000,000 bar rows;
- at least 100,000 status-eligible rows;
- at least 100,000 status-gated feature-ready rows;
- at least 95% status-resolved decision coverage;
- at least 95% status-gated feature-ready coverage;
- at least 99% status market-year coverage;
- exactly 100% statistics market-year coverage.

Synthetic fixture policies may be smaller only to prove mechanics. They do not satisfy the production gate.

## Legacy research census and closed lines

The production-derived legacy census is intentionally unresolved:

- `status`: `INVALID_TRIAL_CENSUS_UNRESOLVED`
- `exact_count_state`: `INDETERMINATE`
- `observed_attempt_floor`: 39
- `preregistered_penalty_count`: 0
- `trusted_gate`: `false`

The observed floor is evidence-bound, not an exact historical count. A zero penalty is not a multiplicity reset. The unresolved census may support only a mechanical readiness receipt while keeping the trust and real-history gates closed.

The legacy current-alpha line, ORAC line, and distributional 30-minute line are closed. They cannot be rescued, promoted, or treated as v2 alpha. Any future research must be a new, separately predeclared and separately authorized program.

## What is complete and what remains

Complete in code and synthetic/adversarial tests:

- exact resumable hash-copy and immutable snapshot mechanics;
- offline DBN catalog and overlap validation;
- v2 Phase 1B/Phase 2, status/statistics, feature/outcome separation, and coverage gates;
- non-alpha historical capability contracts;
- production-derived census and mechanical readiness receipts/CLI;
- clean Git, capability, dependency, census, and source-release closure checks.

Still pending on real local inputs:

1. Execute the approved local hash-copy.
2. Publish and reverify the immutable DBN source snapshot.
3. Build the v2 foundation and pass every production coverage gate.
4. Publish `REBUILD_COMPLETE` and mechanical `HISTORICAL_RESEARCH_READY` receipts only if their exact closures pass.

These pending steps prevent any premature milestone or readiness claim. They do not authorize downloads, real-history execution, a candidate, a push, or trading.

## Synthetic validation

```powershell
python -m pytest -q
```

Synthetic tests prove mechanics only. They do not inspect the real archive or establish research performance.
