# Codex handoff

## Current state

- Repository: `C:\Users\donny\Desktop\futures_intraday_model_v2`
- Branch: `codex/standalone-v2-completion`
- Current pre-closure HEAD:
  `63a326dc07639e95dc8e2991eece1bef4e91b6b7`.
- The approved 41-market DBN successor is
  `086282eaef7b36a61626f88d93d06c93b87c1cb3407c936d065d0d1b9d98599e`:
  4,491 DBNs plus 4,491 sidecars across 41 markets.
- The complete schema-v5 foundation release is
  `78806ef01714c72f6da537c1b6e6f8b2e903b14728822b0daa31b4c6c75a8909`;
  manifest SHA-256 is
  `bfa86450b77ab4b19c6e17e641aab6684895d721188497a664eb4b37ab4c8ce9`.
  Its bounded successor run rebuilt 118 eight-market intervals, reused 565
  contract-compatible intervals, assembled all 683 intervals, and made zero
  provider calls.
- Economics-policy successor:
  `747bb91353c7ef7271e3760aed6ecdfd177a4e333a557f42bd4ffd15d69e5556`.
  Session-policy successor:
  `0986b5220fc84397763c0195e3ca86acfa7f5004b6e9ab414ec7bc2f2a7a6632`.
- The research-scope gate is 82/82 for 2025 onward. The archive-wide status
  census remains explicit and no longer incorrectly blocks that scoped gate.
- Local `api.env` is ignored, v2-local, and has a nonempty
  `DATABENTO_API_KEY`. Never print, stage, commit, package, archive, install, or
  report the key.

## Cockpit evidence and active gate

- Approved smoke attempt 1 is immutable failed evidence under
  `manifests/live_cockpit_smoke_attempts/65a2e0b45a68f595d49609822fe27ad054cfebe156a7eb7da32c84e2b9e624ac/`.
  Result ID:
  `e3647f34a14da41df8f9a3434d54fd1ff39581709604b450a6c6451cd4fdf74f`;
  result SHA-256:
  `1dc195a4b6aba23d879eaa0d139018efe7bbf3d6a2687cafdc7d0795394a3752`.
  It failed because source Python, not the approved package, executed. It made
  exactly two bounded live sessions, made no history/cache request, shut both
  sessions down, and did not authorize or perform cutover.
- Package identity is now checked before provider construction. Pending
  approval, source runtime, wrong executable hash, result-path drift, and
  create-only collisions all fail before Databento starts.
- The offline self-check state probe now returns a bounded fail-closed result
  when its state directory is unwritable; it no longer spins in Windows
  temporary-file creation.
- Corrected prepared installation:
  `C:\Users\donny\AppData\Local\Programs\FuturesLiveCockpit\20260726-022002-ab15788e`.
- Corrected executable SHA-256:
  `f028897a7807ae75bcec986e203b37ffc18c697248309e6da542b530c160e9f2`.
- Active successor plan ID:
  `967ab4ed95ab198adbdad8657e36c164620a690a90edd40c6aa72de83d0ff8ff`;
  plan-file SHA-256:
  `ab15788ee839298c32e6561bc5eb993a9902a3ba539899d8fdc0d6b8c704407e`.
- The user supplied the exact successor approval token on 2026-07-25. Its plan
  ID, plan-file SHA-256, and executable SHA-256 match the active successor
  artifacts. Preserve the token unbound and unconsumed until the Sunday Globex
  reopen so the required ES-focus leg can produce meaningful live evidence.
  This session exposes no Codex scheduling/automation tool, so a later task
  wake or user resume is required; do not substitute a raw Windows scheduled
  task for the gated interactive verification.
  Exact-token UTF-8 SHA-256 / future `user_authorization_id`:
  `fd3a6b3cd3c61c08c49fd001b665a232e25065a3e9c6f6b5c7025cb1e8e43542`.
  `configs/live_cockpit_smoke_approval.json` remains pending meanwhile. The
  installed package contains the exact active plan, contains no credential,
  points its locator to the ignored v2-local `api.env`, and passes offline
  self-check.
- Both Desktop and Start Menu shortcuts still target preserved version
  `20260714-115629`; no auto-start shortcut exists.
- Cockpit tests: 88 passed and 1 skipped only because Node is unavailable.
  Targeted self-check/approval/cutover tests and operational-document tests
  pass. `git diff --check` passes.
- A pre-closure full suite completed in 459.6 seconds with 470 passed,
  1 Node-only skip, 0 failed, and 0 errors. The Master Audit receipt verifier
  was then corrected to accept the project’s actual self-hashed approval
  receipt while independently verifying the receipt file hash; 14 targeted
  Master/Meta tests pass after that change. Therefore the full suite must be
  rerun once more after live closure before final Meta Audit publication.
- The completed offline implementation was committed locally, without a push,
  as milestone `544ebbb6eff8fd37e3f13c220b27d9225ebbb596`
  (`Complete 41-market foundation and prepare cockpit cutover`). Immediately
  before that commit, the exact staged content passed the fresh full suite:
  471 passed, 1 Node-only skip, 0 failed, and 0 errors in 395.9 seconds.
  Runtime foundation checkpoints, batch stdout logs, installed/package output,
  DBNs, and the ignored credential were excluded. A final suite is still
  required after the approved smoke/cutover state is recorded.
- Synthetic Phase 1A-11 evidence is complete at
  `reports/pipeline/synthetic_phase1a_11.json`, run ID
  `c1f4680580e74d16fe5e99a99debe2193fb3d8679dfed64338e272c9dc4d0c62`;
  it grants no provider or real-history authority.
- Dependency locking passes. The ignored local key is absent from Git,
  repository/package text, and the prepared installation. Operational v2 trees
  contain zero reparse points. The retirement inventory now records all 4,491
  DBNs and 4,491 sidecars as migrated immutable v2 evidence.

## Continue in this order

1. The exact user approval has been received:
   `APPROVE BOUNDED OBSERVATION-ONLY DATABENTO SMOKE SUCCESSOR 967ab4ed95ab198adbdad8657e36c164620a690a90edd40c6aa72de83d0ff8ff SHA256 ab15788ee839298c32e6561bc5eb993a9902a3ba539899d8fdc0d6b8c704407e EXECUTABLE f028897a7807ae75bcec986e203b37ffc18c697248309e6da542b530c160e9f2`
2. After the Sunday Globex reopen, first reverify the active plan and
   executable hashes, absence of the attempt-2 result, old shortcut targets,
   zero cockpit processes, and ES market availability. Then bind that exact
   token into the active approval receipt and run only the installed 41-market
   overview / ES-focus smoke, for at most 120 seconds and two sessions,
   publishing create-only result
   `reports/live_cockpit/bounded_live_smoke_result_attempt_2.json`.
3. Only a package-bound `PASS` may activate the prepared installation. Verify
   both shortcuts, observation-only behavior, clean shutdown, and rollback.
   Otherwise preserve the current shortcuts and installations.
4. Run the full pinned suite, dependency/secret/external-path scans,
   `git diff --check`, all active Master Audits, Meta Audit, and retirement
   audit. Remediate all Critical/High or P0/P1 gaps.
5. Stage only reviewed explicit paths, create the authorized local closure
   commit and clean-HEAD receipts, and do not push.

## Stop lines

- No further provider call without the exact successor approval above.
- No real-history alpha/WFA/OOS, prediction materialization, candidate sealing,
  holdout/forward access, trading, or orders.
- No shortcut cutover before the successor smoke passes.
- No credential exposure, legacy write/delete, remote push, immutable release
  overwrite, link-based migration, or destructive cleanup.
