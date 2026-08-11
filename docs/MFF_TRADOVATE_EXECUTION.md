# Gated MFF / Tradovate execution capability

The cockpit contains an offline-testable execution domain, but actual MFF or
Tradovate access is disabled. `Live cockpit` can mean live Databento market
data. It does not mean that the MFF account is Live Funded or that order
submission is authorized.

## Current verdict

- Selected implementation candidate: Tradovate.
- Active provider/profile/stage: MyFundedFutures,
  `mff_rapid_eod_50k_2026_08_10`, `sim_funded`.
- Active execution mode: `OBSERVATION_ONLY`.
- Direct API entitlement for an MFF-issued user: `UNCONFIRMED`.
- Evaluation, Sim Funded, and Live endpoint bindings: `UNCONFIRMED`.
- Exact account binding: `UNSET`.
- Active official execution-cost profile: `UNSET`.
- Execution authorization and production readiness: `false`.

The generic Tradovate documentation describes public REST and WebSocket
capability. Its API-access article says API-key generation generally requires a
qualifying Live Tradovate account, the CME Information License Agreement, and
the API Access add-on. That does not establish that an MFF-issued Evaluation,
Sim Funded, or Live Funded user can generate or use a key. MFF documents that
Tradovate is a supported trading platform and permits some tailored automated
strategies; neither statement grants custom REST/WebSocket entitlement.

## Official-source matrix

Reviewed on 2026-08-11. `Technical` means the source describes generic API or
platform capability. `MFF permission` means the source states a firm policy.
Neither column establishes account-specific entitlement unless explicitly
noted.

| Source | Provider date | Material support | Technical | MFF permission | MFF-issued entitlement |
| --- | --- | --- | --- | --- | --- |
| [MFF supported platforms](https://help.myfundedfutures.com/en/articles/8528335-overview-of-supported-platforms-at-mffu) | 2026-03-04 | Tradovate is a supported platform | No | Platform use only | Unconfirmed |
| [MFF Tradovate login](https://help.myfundedfutures.com/en/articles/8445591-tradovate-login-instructions) | 2025-11-10 | MFF credentials, Simulation selection, exact-account warning | Platform UI | Platform login | Unconfirmed |
| [MFF fair play](https://help.myfundedfutures.com/en/articles/8444599-fair-play-and-prohibited-trading-practices) | 2025-11-24 | Tailored automation, HFT ban, order conduct, hedging ban | No | Yes | Unconfirmed |
| [MFF cross-instrument policy](https://help.myfundedfutures.com/en/articles/10244682-cross-instrument-policy) | 2026-03-03 | Aggregate exposure cannot bypass plan limits | No | Yes | Unconfirmed |
| [MFF news policy](https://help.myfundedfutures.com/en/articles/8230009-news-trading-policy) | 2026-02-23 | No orders or positions around restricted releases | No | Yes | Unconfirmed |
| [MFF permitted times](https://help.myfundedfutures.com/en/articles/9558251-permitted-times-to-trade) | 2026-03-02 | 18:00-16:10 New York wording and holiday ambiguity | No | Yes | Unconfirmed |
| [MFF 2% price-limit rule](https://help.myfundedfutures.com/en/articles/9698984-2-price-limit-rule) | 2026-03-02 | Current contract/session price-limit data required | No | Yes | Unconfirmed |
| [MFF futures instrument list](https://help.myfundedfutures.com/en/articles/9735811-futures-instrument-list) | relative update only | Instruments, current restrictions, and published round-trip costs | No | Yes | Unconfirmed |
| [Tradovate API](https://api.tradovate.com/) | No visible update date | REST, WebSocket, auth, accounts, orders, positions, fills, `user/syncrequest` | Yes | No | Unconfirmed |
| [Tradovate API access](https://tradovate.zendesk.com/hc/en-us/articles/4403105829523-How-Do-I-Get-Access-to-the-Tradovate-API) | 2024-03-20 | Generic API-access prerequisites and key permissions | Yes | No | Does not cover MFF-issued users |
| [Tradovate API-key test](https://tradovate.zendesk.com/hc/en-us/articles/4403105746579-How-Can-I-Test-My-API-Key) | 2021-12-06 | Pre-authorized docs can test allowed endpoints | Yes | No | Unconfirmed; no test authorized |
| [Tradovate key permissions](https://tradovate.zendesk.com/hc/en-us/articles/4408873526547-How-Do-I-Change-My-API-Key-Permissions) | 2021-10-20 | Separate read/modify domains for account, orders, positions, and risk | Yes | No | Unconfirmed |
| [Tradovate OAuth registration](https://tradovate.zendesk.com/hc/en-us/articles/4403100442515-How-Do-I-Register-an-OAuth-App) | 2021-12-06 | OAuth registration follows attestation/agreement | Yes | No | Unconfirmed |

Material ambiguities remain fail-closed: the authentication flow supported for
an MFF-issued user, the endpoint environment for each MFF account stage, the
manual custom-application `isAutomated` value, current account-specific fees,
and whether MFF permits direct custom REST/WebSocket use at each stage.

## Architecture and modes

Databento remains the chart and signal-data source. The webview contains no
credentials or tokens and cannot decide compliance or authoritative quantity.
Native Python owns typed intents, MFF runtime gates, a provider-neutral adapter,
Tradovate REST/WebSocket transports, reconciliation, and the local journal.

- `OBSERVATION_ONLY`: startup default; no Tradovate client or account API.
- `TRADOVATE_READ_ONLY`: prepared but inactive; requires entitlement and a
  separately approved read-only smoke.
- `LOCAL_EXECUTION_SIMULATOR`: deterministic, network-free lifecycle testing;
  always labeled synthetic and never represented as MFF execution.
- `MFF_TRADOVATE_SIM_FUNDED`: blocked pending entitlement, endpoint, account,
  costs, feeds, readiness, and approval.
- `MFF_TRADOVATE_LIVE`: blocked separately; no activation shortcut exists.

Every restart, reconnect, binding/stage/endpoint/profile/cost change, stale
input, authentication failure, unknown order state, or reconciliation conflict
disarms execution. Arm state exists only in memory, is time-limited, and is
never restored from UI preferences.

## Credentials and account binding

Tradovate credentials are referenced through Windows Credential Manager under
`futures_intraday_model_v2/tradovate`. Tokens remain in backend memory. They
are never returned to JavaScript, placed in `api.env`, logged, packaged, or
stored in Git. Revocation deletes that Windows credential and requires API-key
revocation through the official provider UI when entitlement exists.

The ignored local binding path is
`state/live_cockpit/execution_binding.json`. It must name one exact account,
stage, environment, user, profile, connection, mapping, cost profile, evidence
reference, and binding hash. No code selects the first returned account.
Changing any binding input disarms execution.

To return to observation-only mode, remove or quarantine the ignored binding,
delete the credential reference, and restart. Startup still remains
`OBSERVATION_ONLY` even when both artifacts exist.

## Offline checks and packaging

```powershell
.\.venv\Scripts\futures-live-cockpit.exe --self-check
.\.venv\Scripts\futures-live-cockpit.exe --demo
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit.execution.preparation --operation tradovate-read-only-smoke
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit.execution.preparation --operation tradovate-sim-order-smoke
.\scripts\build_live_cockpit.ps1 -CandidatePath build\FuturesLiveCockpit-candidate
.\scripts\install_live_cockpit_candidate.ps1 -ExpectedCandidateSha256 <validated-64-character-sha256>
```

The candidate build never overwrites the installed tree. After its self-check,
tests, secret scan, and visual demo inspection pass, installation is a separate
approved cutover to `FuturesLiveCockpit\FuturesLiveCockpit.exe`. The installer
uses a same-filesystem backup and restores it if replacement fails. The backup
is returned as `rollback_path` and retained after the installed offline
self-check. Remove that exact backup only after the required post-install native
launch, process-identity, visual, shutdown, and package checks pass; restore it
if any of those checks fail. The candidate is consumed by the atomic move. No
build, install, or self-check authorizes Tradovate authentication or an order
path.

Before moving the candidate into place, the installer fingerprints every file
in the candidate and installed trees and verifies that the moved rollback tree
is byte-for-byte identical to the original installed tree. It also verifies
the published tree against the validated candidate tree before reporting
success.

The preparation commands only print a bounded, hash-bound scope. They do not
authenticate or connect. A future read-only smoke requires separate approval
for exact executable/config hashes, endpoint, stage, operations, duration, and
output. A future Sim Funded smoke additionally requires the exact binding, one
specified micro, maximum quantity one, a broker-native protective stop, and
explicit cleanup. A Live order smoke is represented as blocked and has no
executable operation.

Reconciliation treats Tradovate as authoritative for accounts, positions,
orders, fills, and rejections. An unknown or lost order response is never
retried blindly; the cockpit disarms and performs fresh account reconciliation.
It never reports flat until broker-authoritative state confirms flat.
