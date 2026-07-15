# Codex handoff

## Current state

- Repository: `C:\Users\donny\Desktop\futures_intraday_model_v2`
- Legacy repository: `C:\Users\donny\Desktop\futures_intraday_model` (read-only evidence; never modify).
- Current state: `REBUILD_IN_PROGRESS`.
- Implementation and synthetic/adversarial tests for the pre-copy controlled rebuild are complete.
- Local copy, real DBN foundation construction, and readiness publication are pending.
- Claims: no alpha, candidate, live readiness, execution authority, or trading readiness.

Do not convert code completion into `REBUILD_COMPLETE` or `HISTORICAL_RESEARCH_READY`. Those are immutable publication states that require the real copied source snapshot, a production-gated foundation, exact capability/census/Git/dependency closure, and successful re-verification.

## Authorization boundary

The controlling authorization permits exact hash-copy of approved data already present locally, including the local Databento DBN archive. The exact 9,122-file, 33,313,291,079-byte inventory was independently reproduced and its hash-bound approval is checked in. Neither the copy nor a real-archive decode has run.

Allowed before the next gate:

- review repository code, contracts, tests, and local evidence;
- run synthetic/adversarial tests;
- perform only the approved local hash-copy and non-alpha validation.

Blocked:

- Databento or other provider downloads/calls;
- real-history hypothesis execution, WFA, or OOS evaluation;
- candidate sealing, order placement, live/shadow trading, or alpha claims;
- legacy writes, destructive cutover, or external pushes.

## Evidence and authority roles

- The eight hash-pinned Databento DBN families become authoritative only after copied snapshot publication and complete receipt/tree/hash re-verification.
- Legacy Phase 1B raw and Phase 2 causal files are `comparison_only_never_authoritative_regenerate`; v2 must regenerate both from the verified DBNs.
- The authoritative path is DBN -> v2 Phase 1B -> v2 Phase 2 -> independent features/outcome sources -> separately authorized WFA/OOS.
- `FuturesLiveCockpit.exe` is evidence-only, non-active, untrusted, and non-executable. Do not run it; this rebuild has never executed it.
- Short `by_family` evidence aliases exist for Windows path safety. The census binding preserves the exact logical legacy source path, size, and SHA-256.
- The failed legacy alpha, ORAC, and distributional lines are closed and cannot be rescued or promoted.

## Production gates

Missing or unknown status is `STATUS_UNRESOLVED`: fail closed, retain denominator membership, and deny feature/trade eligibility.

The foundation must have:

- at least 1,000,000 bars;
- at least 100,000 status-eligible rows;
- at least 100,000 status-gated feature-ready rows;
- at least 95% status-resolved decisions;
- at least 95% status-gated feature-ready rows;
- at least 99% status market-year coverage;
- 100% statistics market-year coverage.

The legacy census remains `INVALID_TRIAL_CENSUS_UNRESOLVED` / `INDETERMINATE`, with observed floor 39, penalty 0, and `trusted_gate: false`. It may permit only mechanical `HISTORICAL_RESEARCH_READY`; trust and real-history authorization remain closed.

## Next dependency-ordered work

1. Execute the restart-safe approved local hash-copy; publish and reverify the source snapshot.
2. Catalog the copied DBNs and build v2 Phase 1B, Phase 2, status/statistics, features, and separate outcome sources.
3. Require every production coverage gate to pass.
4. Publish `REBUILD_COMPLETE` and mechanical `HISTORICAL_RESEARCH_READY` only if all exact closures pass.
7. Stop. Real-history WFA/OOS still requires a separate external authorization and pre-outcome controls.

No download, real-history run, candidate action, push, or trading action belongs to the current gate.
