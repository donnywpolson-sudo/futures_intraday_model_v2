# Codex handoff

## Current state

- Repository: `C:\Users\donny\Desktop\futures_intraday_model_v2`
- Branch: `codex/standalone-v2-completion`
- Preserved baseline commit: `157daf59e8377781c71b2126f9cbd5e4d6997aa6`
- Accepted parent DBN release:
  `9e5a9f2a405e50b0cda6702b67506b0951b057500781d37c45171da3967e9b51`
  with 4,020 DBNs and 4,020 sidecars.
- Frozen eight-market inventory:
  `f304447d9cba074b86f25c5a50661c0b2135a5470ef438ef3eaefa11f4be8a81`
  with 471 verified pairs and three exact unfinished 6N files excluded.
- Frozen migration plan:
  `bce35d7d7d4c11e7a0cc3c9ad4e6774abb68a2846946b3bc053450223b4f9f62`
  (`configs/eight_market_successor_migration_plan.json`).
- The migration receipt remains pending. No candidate byte has been copied and
  no source/universe contract has been activated.
- The v2-native Phase 1A-11 synthetic interface, 41-market profile view, root
  Master/Meta Audit, and observation-only cockpit source are present.
- Local `api.env` is ignored, contains a nonempty Databento key, and has
  restricted Windows permissions. Never expose or stage it.
- Cockpit unit/fake-provider/all-market tests, initial packaged self-check,
  secret scan, and installer `-WhatIf` passed. No provider smoke, installation,
  or shortcut change has occurred.

## Next actions

1. Finish audit/doc, live-smoke gate, dependency-lock, and standalone tests.
2. Obtain exact hash-bound approval for the frozen successor migration before
   copying any candidate data.
3. After approval, execute and reverify the successor, approve the canonical
   universe, and regenerate all affected foundation releases from that manifest.
4. Rebuild/package and pass all offline cockpit checks, then obtain separate
   approval for the bounded provider-backed smoke.
5. Only after that smoke passes, install the version and cut over both shortcuts
   with rollback verification.
6. Run full validation, Master/Meta Audits, standalone/secret/dependency scans,
   create explicit-path local commits, bind final receipts to clean HEAD, and
   publish the legacy-retirement report. Do not push or delete external data.

## Active stop lines

- No provider call, real-history alpha/WFA/OOS, prediction materialization,
  candidate sealing, holdout/forward access, trading, order placement, push, or
  destructive operation.
- No successor copy without the exact migration approval receipt.
- No shortcut cutover without the separately approved bounded live smoke.
