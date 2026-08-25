# Futures Live Cockpit remediation closure

## Outcome

All ten confirmed product findings were remediated and the exact legacy cockpit lane passes 190 tests with no failures or skips. The repository package contains the sole remaining `FuturesLiveCockpit.exe`, at `C:\Users\donny\Desktop\futures_intraday_model_v2\FuturesLiveCockpit\FuturesLiveCockpit.exe`.

The overall audit closure is **BLOCKED**, not because a product defect remains, but because a bare-path audit launch opened the configured provider connection and changed 41 `market_binding` rows in the live SQLite cache. The approved rollback removed exactly those rows while preserving the logical bars and coverage datasets. The original file's byte hash cannot be recreated; the exact post-incident bytes remain in a backup.

## Evidence

- Source remediation: `00a3e2e9f910292fb77f0a63a5f8d49f4cbe7668`.
- Test portability corrections: `eeb9a497ea69d8ffb8a57ef1ae9c696e577b39b8` and `583dcb9caaccaf3ecf0786a97054b881a6e6d34d`.
- Canonical executable SHA-256: `76b1daddb6027c7a2d2d971e2603300ce489bc42aa1e68948a759d6de7776ed1`.
- Canonical package: 1,534 files and 165,632,667 bytes.
- Self-check: PASS, 41 markets, writable cache, all bundled assets present, observation-only, no provider opened by self-check.
- Legacy cockpit tests: 190 passed, 69 deselected, 0 failed, 0 skipped.
- Broader current lane: 48 passed, 259 deselected, 2 failed. Both failures reproduce at the pre-remediation basis and demand execution/trading surfaces that contradict the controlling observation-only cockpit contract.
- Demo start median improved from 410.694 ms to 121.820 ms; first-chart median improved from 407.082 ms to 117.061 ms.
- Five live-cache-copy initializations had a 75.035 ms median and did not contact the provider.
- Eight packaged GUI launches were visible and responsive in 788-1,061 ms, with zero orphan processes.
- Full profile scan found one cockpit executable, zero cockpit shortcuts, zero scheduled tasks, zero services, and zero registry launch/uninstall entries.

## Cache incident and rollback

Pre-launch cache SHA-256 was `f5ce584f0bd51ab5bdcc35892ca98464742c5e9dab6ab6d5b5b2a46ea52af26c`. The provider-connected launch produced SHA-256 `75e2463c8b5398a6fb76f5ce70c7a32833a472e65f11dda5f832f1599d92186a` and inserted 41 bindings. Approved logical rollback removed exactly those 41 bindings and atomically replaced the cache. Current SHA-256 is `ecb7b8f2acec6cebd6e9617bd5d9c9f6d3bcd7f538b2b5d75f6e84b1b4c13480`; `quick_check` is `ok`, bindings are zero, and the 201,655 bars plus 43 coverage records retain their pre-rollback logical digests. Backup `bars.sqlite3.incident-backup-75e2463c` retains the exact post-incident bytes.

## Residual decisions

- Product findings remaining: zero.
- Audit-purity blocker: exact pre-launch cache bytes are unrecoverable.
- PyInstaller output is source-reproducible but not bit-reproducible.
- WebView2 uses its expected multiprocess runtime; OS page-cache state was uncontrolled.
- The two broader-lane execution UI failures are pre-existing contradictory test debt and were not implemented because the cockpit is contractually observation-only.
