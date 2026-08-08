# Prop-firm risk profile

The current Phase 8 research default is **Apex Trader Funding — EOD Performance
Account, $50K**. Its complete, editable settings live in
[`configs/prop_firm_risk_profile.json`](../configs/prop_firm_risk_profile.json).

This is a risk envelope, not broker connectivity or permission to trade. Phase
8 must apply both sets of limits:

- **Apex firm limits** model the chosen account's official drawdown, daily-loss,
  scaling, and contract constraints.
- **Project limits** are stricter internal stops. They leave room for execution
  uncertainty and avoid using the whole firm allowance.

The initial profile uses Apex's published $50K EOD Performance Account rules:
$2,000 EOD drawdown, initial 2-contract / $1,000 daily-loss tier, and daily
scaling up to 4 contracts. The source pages and review date are recorded in the
profile. Apex may change rules, so review them before each real-data evaluation
or account change.

To switch firms, add a fully sourced profile, review it, and change only
`active_profile_id`. Do not edit a profile after it has been used in an
evaluation; create a successor profile instead. The execution-cost model remains
separate: firm risk rules do not replace exchange, platform, clearing, or
slippage assumptions. The first Tier 1 configuration is in
[`configs/tier1_phase8_evaluation.json`](../configs/tier1_phase8_evaluation.json).
It locks ES, CL, ZN, and 6E base/stress/extreme costs, one-bar delay, one
standard-contract-equivalent cap, $250 maximum planned initial risk (including
stress costs), a $500 daily stop, a $1,500 internal drawdown stop, and a three
entry-per-session cap. The $2,000 firm drawdown remains an external emergency
boundary with a $500 project reserve; it is not a usable strategy-risk budget.
The bracket policy is a 1.5x Wilder ATR(20) protective stop, net 2R target,
60-minute maximum hold, and last-verified-bar session-end safety exit. These
are local research controls, not a claim that stop orders or bar-based fills
replicate live execution.

The bracket successor is a separate, local-only trial contract in
[`configs/tier1_bracket_trial.json`](../configs/tier1_bracket_trial.json). It
uses only the 2018-2022 discovery period and keeps 2025 untouched. A passing
discovery result is not confirmation, live execution validation, or Apex
readiness.

`LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED` in that configuration describes the
immutable **template**, not the state of a particular trial. The authoritative
state is the create-only registry record. The current registered bracket trial
is `e296f6e8...f78b`; its source rows and pipeline outputs remain unopened.
The selected connection is **Tradovate**. Apex's current published all-in fees
are recorded exactly for ES, CL, and 6E. The current public Tradovate schedule
does not publish a ZN rate. The project
therefore uses a deliberately conservative $2.50-per-side ZN placeholder for
research only. Reports must say `PROVISIONAL_EXECUTION_COSTS`; they cannot claim
exact Apex live costs or deployment readiness until the account-specific rate is
recorded.
