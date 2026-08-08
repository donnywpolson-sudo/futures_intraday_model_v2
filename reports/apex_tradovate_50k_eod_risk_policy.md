# Apex–Tradovate 50K EOD risk-policy preparation

Status: **PREPARED — AWAITING OWNER-SELECTED R AND EMERGENCY RESERVE**

This additive policy replaces the proposed automatic `2R` daily and `6R`
drawdown relationships. It does not change the completed full-contract census,
create a counted mechanism, register a trial, or authorize historical execution.

## Account-native controls

- Apex 50K EOD Performance Account through Tradovate.
- New Level 1 state: $2,000 EOD drawdown and $1,000 daily-loss limit.
- One full contract and one strategy trade per session, even when Apex permits
  more contracts.
- `R` remains the owner-selected maximum planned loss for one trade.
- A strictly positive emergency reserve must remain above the Apex liquidation
  threshold.
- A new trade's admission cap is the minimum of `R`, remaining Apex drawdown
  after the reserve, and remaining Apex DLL capacity.
- The Apex EOD threshold begins at $48,000, trails the highest EOD balance by
  $2,000, never decreases, and locks at $50,100.

## Commission policy

Use the official Apex–Tradovate PA round-turn schedule, not retail Tradovate
pricing. Planned loss equals stop risk plus round-turn commission plus locked
stress slippage.

Of the project's 41 full contracts:

- 26 have explicit published Apex round-turn amounts;
- RB and HO are supported but have no amount in the published table, so each
  receives the conservative $4.54 fallback—the highest published round turn
  among the supported full contracts in this universe;
- 13 are not listed as supported full contracts and fail closed: 6M, BTC, ETH,
  KE, SR1, SR3, TN, UB, ZB, ZF, ZN, ZQ, and ZT.

A fallback fee never makes an unsupported instrument tradable. Evaluation,
activation, optional data, and similar fixed fees belong in separate owner-level
economics rather than per-trade expectancy.

Official sources verified 2026-08-08:

- https://apextraderfunding.com/help-center/tradovate/tradovate-commission-instruments/
- https://apextraderfunding.com/help-center/eod-trailing-drawdown-accounts/eod-performance-accounts-pa/

## Remaining owner decisions

Before a successor mechanism can be created, the owner must select:

1. fixed planned-loss unit `R` in dollars; and
2. fixed emergency reserve in dollars from the $2,000 Apex drawdown.

Those values must reflect account tolerance and may not be selected solely to
pass the completed historical feasibility census.
