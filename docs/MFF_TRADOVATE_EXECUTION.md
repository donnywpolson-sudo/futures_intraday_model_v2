# MFF manual execution assistant and dormant Tradovate capability

FuturesLiveCockpit does not transmit orders for MFF simulated accounts.
`Live cockpit` can mean live market data. It never means that an MFF account is
an actual MFF Live account or that provider order submission is authorized.

## Current verdict

- Selected implementation candidate: Tradovate.
- Active provider/profile/stage: MyFundedFutures,
  `mff_rapid_eod_50k_2026_08_10`, `sim_funded`.
- Startup mode: `OBSERVATION_ONLY`; the selected simulated-stage workflow is
  the local `MFF_MANUAL_ASSISTANT`.
- MFF Evaluation execution capability: `MANUAL_ONLY`.
- MFF Rapid EOD Sim Funded execution capability: `MANUAL_ONLY`.
- MFF Live capability: `UNCONFIRMED`, pending an actual Live account and
  Tradovate permission verification.
- Direct API read/order access for the two simulated stages: `false`.
- Provider API readiness: `false`.
- Automatic execution authorization: `false`.
- Exact account binding: `UNSET`.
- Active official execution-cost profile: `UNSET`.
- Execution authorization and production readiness: `false`.

The user supplied a first-party MFF support response on 2026-08-12. In the
specific context of MFF simulated accounts, the response described Tradovate
API usage as live-account-only. The bounded durable summary is
`configs/mff_execution_capability_evidence.json`. It supports only the manual
classification of MFF Evaluation and Rapid EOD Sim Funded. It does not verify
official costs, authorize a connection, establish a universal Tradovate
policy, or establish future MFF Live access. The full transcript is excluded
from Git and remains user-controlled outside the repository.

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

Material ambiguities remain fail-closed: future actual MFF Live entitlement,
the relevant endpoint for that future account, current account-specific fees,
and the manual custom-application `isAutomated` treatment for any future API
integration.

## Capability and readiness model

The provider-neutral capability vocabulary is `MANUAL_ONLY`, `READ_ONLY_API`,
and `ORDER_API`. Unknown providers, stages, or values fail closed. Capability
selection is explicit and never inferred from credentials, account text,
endpoint behavior, simulator state, environment variables, or UI choices.

Manual preview, manual readiness, provider API readiness, and automatic
execution authorization are independent. A provisional ticket can be displayed
with the conservative `mff_micro_provisional_stress_v1` policy while manual
readiness remains false. Manual readiness requires current market/model
records, an OOS-promoted strategy policy, operator snapshot, session, news, and
price-limit records; verified mapping; valid risk inputs; reconciled
positions/orders; allowed provisional costs; and every MFF/internal gate. No
override exists for missing compliance feeds. The packaged synthetic demo may
bind an explicitly synthetic promoted-policy record, but that is not
production evidence.

## Operator-reported workflow

The local account snapshot is explicitly `OPERATOR_REPORTED`, profile/stage/
alias bound, hashed, and short-lived. The alias is local and is not an API
account binding; `account_binding` remains `UNSET`. A restart, state-changing
manual event, expiry, contradiction, or corrupt state requires reconciliation.
Submitted orders with unknown fill status are treated as worst-case exposure.
Filled positions without operator-confirmed protection and every
`STATE_UNCERTAIN` block new tickets.

Snapshot collections have exact non-provider schemas. Open positions report
signal root, micro symbol and contract, side, integer quantity, positive stop
ticks, and protection status. Working entries additionally report requested
quantity, fill status, order type, and entry price. Protective orders report
their micro contract, side, integer quantity, stop price, and working status.
Provider account IDs, credentials, nested secret fields, and private machine
paths are rejected before journaling.

The manual ticket supports only exact micro contract-month mappings ES to MES,
CL to MCL, and 6E to M6E. ZN and mini/standard contracts fail closed. The
backend authoritative MFF risk runtime determines quantity using reported open
and working exposure, the 30-micro cap, concurrent/session risk, balance/floor
cushion, reserve, concentration, liquidity, hedge, duplicate-order, session,
news, and price-limit gates. Provisional costs are never described as official.

The state machine is:

`DRAFT`, `BLOCKED`, `VALIDATED`, `READY_FOR_MANUAL_ENTRY`,
`OPERATOR_REPORTED_SUBMITTED`, `OPERATOR_REPORTED_PARTIALLY_FILLED`,
`OPERATOR_REPORTED_FILLED`, `OPERATOR_CONFIRMED_PROTECTED`,
`OPERATOR_REPORTED_REJECTED`, `OPERATOR_REPORTED_CANCELLED`,
`OPERATOR_REPORTED_CLOSED`, `OPERATOR_RECONCILED`, `ABANDONED`, and
`STATE_UNCERTAIN`. Invalid transitions fail. `BROKER_CONFIRMED` is reserved for
a future supported API and is never produced in MFF manual-only mode.

Copy actions produce bounded local text with the exact contract, side,
quantity, order type, entry, stop, target, risk, provisional cost status,
ticket ID, timestamp, and the statement `NO ORDER HAS BEEN TRANSMITTED BY
FUTURESLIVECOCKPIT`. They do not launch or automate Tradovate.

Operator-reported actuals are compared with the plan for contract, side,
quantity and partial fills, fill price, slippage in ticks and dollars, timing,
stop/target distance, fees, actual risk, reward/risk, aggregate exposure, exit,
and realized result. Material mismatches remain visible, update the
conservative local state, and can block further tickets. The cockpit never
claims to correct an order in Tradovate.

## Operator runbook

1. Reconcile account state from MFF and Tradovate.
2. Review the model setup and data freshness.
3. Prepare the manual ticket.
4. Verify every risk and compliance gate.
5. Copy the manual instructions.
6. Enter and verify the order manually in Tradovate.
7. Mark the ticket submitted.
8. Record actual or partial fills, rejection, or cancellation.
9. Confirm the protective stop is working.
10. Record the exit.
11. Reconcile manual state.

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
- `MFF_MANUAL_ASSISTANT`: provider-free manual preparation and operator
  reporting for Evaluation and Sim Funded; no credential or provider client.
- `MFF_TRADOVATE_SIM_FUNDED`: retained as dormant historic/future API mode but
  unreachable for the current simulated stage.
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

The ignored provider API binding path is
`state/live_cockpit/execution_binding.json`. It must name one exact account,
stage, environment, user, profile, connection, mapping, cost profile, evidence
reference, and binding hash. No code selects the first returned account.
Changing any binding input disarms future API execution. The manual assistant
does not read or require this file.

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

The mandatory current-source lane must also pass from a detached committed-only
checkout while using the operational virtual environment as the dependency
source:

```powershell
$env:PYTHONPATH = 'C:\clean-source\src'
$env:PYTHONDONTWRITEBYTECODE = '1'
C:\path\to\operational\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider --basetemp=C:\short-clean-temp -m current
```

The 2026-08 clean-source remediation corrected three failures in
`tests/test_runtime_environment.py`: the exact-lock check, the aggregated
package-mismatch check, and the global-interpreter rejection check. All three
failed before their assertions because the validator required a
checkout-local `.venv`, even though a clean committed checkout intentionally
contains no virtual environment. A clean checkout now accepts only the active
locked virtual-environment interpreter; a checkout-local `.venv`, when
present, remains the required interpreter. Global interpreters still fail.

Packaged offline validation uses
`scripts/validate_live_cockpit_offline.ps1`. It launches only an exact absolute
candidate path through `System.Diagnostics.ProcessStartInfo`, passes
`--self-check` or `--demo` separately, sets `UseShellExecute=false`, removes
all inherited Databento, Tradovate, and MFF environment variables without
reading their values, and records the root plus every observed descendant
process. Computer Use is not a launch path.

Socket evidence reports polling samples, raw repeated observations, unique
socket identities, first-seen and last-seen times, owners, listeners, outbound
connections, and loopback/private/link-local/multicast/broadcast/global
classification separately. Repeated samples are not new connections.
Unrelated known host processes are excluded; unknown socket ownership and
root-path mismatches fail closed. WebView2 descendants are part of the
candidate process tree and are never ignored. Process identity includes PID,
parent, executable, command line, and start time; socket identities include
that start time so PID reuse cannot merge different processes. An incomplete
descendant identity or an early demo exit fails validation, and the requested
observation duration must complete.

pywebview serves local assets over loopback HTTP. In demo mode only, the
cockpit starts a dynamically allocated loopback proxy that rejects nonlocal
WebView2 requests. WebView2 officially supports
[additional browser arguments](https://learn.microsoft.com/en-us/dotnet/api/microsoft.web.webview2.core.corewebview2environmentoptions.additionalbrowserarguments),
and Chromium documents both
[manual proxy configuration and its implicit localhost bypass](https://chromium.googlesource.com/chromium/src/+/HEAD/net/docs/proxy.md).
That bypass keeps required local IPC direct. The configuration uses no
external IP/domain allowlist and does not change normal or future separately
authorized provider modes. Both demo and self-check must show zero globally
routable process-tree sockets; only the documented local loopback IPC is
eligible. An unknown owner or any nonloopback outbound socket fails
validation.

Candidate preparation and installation remain separate future operations.
Offline remediation validation creates only temporary packages, removes them
after validation, and never emits or retains an installation approval line.

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

The packaged cockpit retains the locked PyArrow dependency. Databento 0.78.0
declares PyArrow as a runtime dependency, and the supported cockpit history
path converts DBN stores to pandas frames. Removing PyArrow from the executable
would therefore break the declared Databento runtime closure.

The mandatory package validator distinguishes a private-key object from an
isolated parser-format literal without granting any native-binary exemption.
Complete or ambiguous private-key blocks always fail, including ASCII or
UTF-16 blocks embedded in a DLL, executable, source/configuration file, or
other binary. An isolated marker in text-like content also fails. An isolated
marker in a native dependency is non-secret only when package metadata proves
the exact distribution and version, its wheel `RECORD` path/hash/size, the
repository dependency-lock receipt, a valid expected PE image, the expected
package-relative ownership path, and byte-identical source-versus-packaged
hashes. Missing provenance, a rename, substitution, version change, one-byte
change, appended content, or an unknown owner fails closed.

There is intentionally no filename-only or path-only exception for
`arrow.dll`. A PyArrow upgrade invalidates the prior source hash and `RECORD`
binding and must pass fresh package validation. Candidate receipts use
`live_cockpit_package_candidate_receipt/1.2.0` and record the scanner version,
overall result, classification counts, and safe findings containing only the
relative path, classification, label, byte offset, bounded-context hash,
verification reason, and dependency provenance. They never contain a key
payload. Current safe native findings are classified
`VERIFIED_DEPENDENCY_PARSER_LITERAL`; rejected classifications remain
`ACTUAL_PRIVATE_KEY_MATERIAL`, `SUSPICIOUS_COMPLETE_PRIVATE_KEY_BLOCK`,
`TEXT_PRIVATE_KEY_MARKER`, or `UNVERIFIED_BINARY_PRIVATE_KEY_MARKER`.

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
