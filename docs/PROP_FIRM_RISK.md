# Prop-firm account and strategy risk

The active immutable profile is `mff_rapid_eod_50k_2026_08_10` in
[`configs/prop_firm_profiles.json`](../configs/prop_firm_profiles.json). It
selects MyFundedFutures Rapid EOD 50K with the explicit account stage
`sim_funded`. The previous provider profile remains loadable for historical
reproduction; no historical report, manifest, path, identifier, or hash is
rewritten to look like current-provider evidence.

Selecting a profile is a local research configuration choice. It grants no
provider access, challenge/model evaluation, activation, publication,
active-data mutation, deployment, payout submission, or trading authority.
An unexpected provider stage must block new orders until the configured stage
and external state are reconciled.

## Separate immutable bindings

Current runs bind the selected object, not merely a mutable filename:

- `prop_firm_profiles.json`: provider, plan, sourced firm rules, stages, and
  compliance ambiguities.
- `prop_firm_strategy_risk_policies.json`: internal research candidates and
  safety controls. These are not firm rules and are not claimed optimal.
- `prop_firm_execution_instruments.json`: standard-signal to micro-execution
  mapping, exchange economics, risk group, calendar, and roll relationship.
- `prop_firm_execution_costs.json`: platform-specific fees. The current MFF
  platform and official fees are `UNSET`.
- `prop_firm_payout_policies.json`: eligibility, gross withdrawal, 90/10 split,
  and replay-safe simulated payout state.

The run/cache identity contains provider, plan, profile ID/hash, rules-as-of,
account stage, and the ID/hash of every strategy, instrument, cost, and payout
binding. A result from another provider, stage, or policy therefore cannot pass
the current cache-identity check.
Versioned discriminators select a generic structural validator or the exact
MFF Rapid EOD 50K validator. Unknown discriminators fail closed. A structurally
valid profile for another provider or account size does not inherit MFF values.

## Account stages

`evaluation`, `sim_funded`, and `live` are different prop-firm account stages.
They must not be confused with Phase 8 model evaluation.

MFF Evaluation and Rapid EOD Sim Funded are `MANUAL_ONLY`. They have no direct
API read or order access and use operator-reported reconciliation. MFF Live
remains `UNCONFIRMED` until an actual Live account exists and Tradovate verifies
permissions. These stage capabilities do not weaken risk checks or remove the
dormant provider-neutral adapter architecture.

- Evaluation is inactive and separately records the $50,000 nominal balance,
  $3,000 target, $2,000 maximum EOD loss, 30% consistency, four trading days,
  3-mini/30-micro limit, and T1-news allowance. Its inactivity-source conflict
  remains unresolved and is excluded from funded-strategy optimization.
- Sim Funded is the active research stage. The displayed plan size is $50,000,
  but the actual modeled ledger starts at $0. The initial floor is -$2,000,
  the floor permanently locks at +$100, there is no encoded firm daily-loss or
  funded-consistency rule, and the portfolio cap is 30 micro-equivalent units.
- Live is an inactive successor. It has its own zero-based ledger, $2,000 EOD
  loss, $0 floor lock, 4-mini/40-micro cap, daily 90/10 payouts, no buffer, the
  currently sourced $10,000 single-session transition trigger, and up to a
  $5,000 reserve allocation. It is never selected automatically.

## EOD drawdown state

For Sim Funded, one verified completed provider session updates the persisted
state using:

```text
candidate_floor = completed_session_eod_balance - 2000
next_floor = min(100, max(previous_floor, candidate_floor))
```

The completed-session event is derived from a hash-bound calendar-provider
record containing profile ID/hash, stage, calendar version/hash, New York open
and close, freshness interval, and ordinary or explicitly verified-shortened
status. Caller-supplied session IDs or status strings are not accepted.
Intraday unrealized profit never ratchets the floor. Current equity is checked
against the fixed active floor, and the conservative unresolved-touch rule
treats `equity <= floor` as breached. Session IDs and completed balances make
the update replay-safe. A withdrawal never lowers or resets the floor. Unknown
holiday, shortened-session, or completion status blocks an EOD update; calendar
midnight is not a session boundary.
The funded state has one deterministic serialization and state hash covering
runtime identity, realized balance, floor/lock, completed session IDs and
hashes, calendar bindings, breach state, and payout chronology. Restart replay
deserializes a new object and cannot apply the same event twice.

## Micro-only execution and sizing

Current verified research mappings are ES to MES, CL to MCL, and 6E to M6E.
Each mapping binds multiplier, tick, session calendar, roll relationship,
underlying-risk group, and official CME source. ZN is disabled because the
configuration has no verified directly interchangeable micro execution
contract; a yield future or different Treasury tenor is not a substitute.

Firm counting and exchange exposure are separate. The generic mixed validator
uses `10 * minis + micros <= 30` across every open position and working entry.
The active strategy rejects every mini/standard intent even when it would fit
that mixed firm cap.

Micro quantity is the floor of allowed risk divided by stop-defined risk per
contract (stop ticks times tick value, plus explicit slippage and round-turn
fees), then capped by firm, instrument, portfolio open-risk, session-loss,
floor-cushion/reserve, platform, concentration, and liquidity constraints.
Quantity zero is rejected and quantity is never rounded upward.
The public sizing boundary resolves mapping metadata and costs from the selected
hash-bound profile, reconstructs open-position and worst-case working-entry stop
risk, and then applies every cap. Callers cannot supply tick values, fees, or
slippage. Production sizing rejects the unresolved active MFF cost profile.
A named nonzero micro-specific stress profile is available only as visibly
provisional research economics; its ID/hash enter run identity and readiness
remains false.

For manual preparation, that conservative provisional profile can make a
ticket preview available, but it never establishes official costs or manual
entry readiness by itself. Operator-reported open positions, working entries,
unknown fills, protection, realized balance, EOD floor, and session P&L feed
the same authoritative sizing runtime. Unknown submitted orders count at their
worst-case requested exposure; unprotected fills and uncertain state block all
new tickets.

## Internal strategy policy

The old $250 trade risk, $500 daily stop, $1,500 total-drawdown stop, $500
reserve, one-standard-contract assumption, no-pyramiding rule, and three-entry
cap survive only as a named historical strategy seed. They are not attributed
to either the current or former firm.

The active bounded design uses sequential coarse-to-fine selection over micro
risk, concurrent risk, internal session stop, reserve, entry count, and
pyramiding. Logical constraints are validated before any row-dependent study.
Production readiness remains false until a registered, locked out-of-sample
study through the repository's research gateway promotes one policy. The
focused local smoke command is:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_prop_firm_account_runtime.py tests/test_prop_firm_eod_risk.py tests/test_prop_firm_phase8.py
```

There is no current authorized full risk-policy study command: the existing
real-history gateway requires a new durable trial declaration, exact row
certificate, frozen micro costs, and separate approval before it may expose an
execution operation. Inventing a bypass command would violate the current
workflow. This is a readiness blocker, not evidence that the candidate values
are optimal.

## Payout and compliance state

Payout eligibility is independent of signals and of withdrawal choice. The
simulation tracks first funded trade, completed funded days, realized balance,
active floor, buffer status, post-payout balance, profit since payout, gross
withdrawal, firm share, net trader cash, and cumulative cash. First eligibility
requires the sourced $2,100 buffer and timing; later eligibility requires $500
new profit; requests below $500 fail. Requests are daily and replay-safe. The
exact maximum withdrawable amount remains unresolved, so automated maximum
requests and provider submission are disabled. Approval chronology is compared
in absolute UTC time, while the daily-frequency boundary is derived from the
provider session day in `America/New_York` (18:00 session open), not caller
calendar dates.

The local compliance helpers:

- cancel entries, flatten applicable positions, and block orders through a
  data-driven restricted-news window, with an additional internal safety lead;
- require current timezone-aware event data and record the broader "any data
  release" wording as unresolved;
- use `America/New_York`, the sourced 6:00 PM open and 4:10 PM close, plus a
  configurable five-minute internal flatten buffer;
- fail closed on stale or unknown news, holiday/session, and contract-specific
  price-limit data in live enforcement;
- escalate funded inactivity warnings on days five, six, and seven without
  manufacturing a trade;
- reject simultaneous opposite exposure in the same underlying across sizes,
  expiries, strategy sleeves, positions, and working orders;
- apply configurable order-rate and duplicate-order controls without inventing
  a provider HFT number.

These helpers do not submit orders and do not claim complete live compliance.
Every simulated compliance decision can be bound to the runtime/cache identity,
input snapshot, and previous record through a deterministic hash-chained
append-only record. A durable live sink and actual provider/broker
reconciliation remain outside this local simulation surface.

## Sources and ambiguities

Every MFF source record stores title, official URL, access date, provider
update date when available, supported fields, and `rules_as_of: 2026-08-10`.
The profile explicitly retains the T1 double-negative, evaluation inactivity
conflict, provider "EST" wording, standard-Rapid scaling mismatch, floor-touch
assumption, and unresolved maximum withdrawal. Newer plan-specific official
rules control when they conflict with a generic article; conflicts are never
silently erased.

Before any separately authorized provider or live operation, re-review the
official pages and bind current calendars, platform, fees, and external state.

## Prepare-only commands

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-risk-policy
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-phase8
```

They print deterministic, non-authorizing records. Phase 8 is model evaluation;
it does not activate the prop-firm evaluation stage and reads no historical
rows.

## Migration log

- 2026-08-10: added immutable profile `mff_rapid_eod_50k_2026_08_10`, selected
  `sim_funded`, introduced separate hash-bound strategy/mapping/cost/payout
  bindings, implemented zero-based EOD, micro cap/sizing, payout, and compliance
  mechanics, and kept production readiness false.
- Historical provider profile and hash-bound evidence remain unchanged and
  loadable for lineage. Prior results are not re-labeled as MFF evidence.

See [`NAMING_AND_LINEAGE.md`](NAMING_AND_LINEAGE.md) for generic namespaces and
the immutable legacy-path rules.
