# Provider-neutral prop-firm EOD risk preparation

The current non-authorizing preparation selects immutable profile
`mff_rapid_eod_50k_2026_08_10` at explicit stage `sim_funded`.

Current state:

- the funded ledger starts at $0 despite the $50,000 display plan size;
- the $2,000 EOD floor starts at -$2,000 and locks permanently at +$100;
- the strategy is micro-only and the firm cap is 30 micro-equivalent units
  across positions and working entries;
- ES/MES, CL/MCL, and 6E/M6E mappings are enabled; ZN has no verified direct
  micro mapping and is disabled;
- internal risk candidates are separate from firm rules and have not passed a
  locked out-of-sample study;
- platform/connection and official MFF fees are `UNSET`;
- payout submission, provider access, row evaluation, publication, deployment,
  active-data mutation, and trading are not authorized.

Production/live readiness is false. Current blockers include platform fees,
micro cost/liquidity validation, a promoted strategy policy, current news and
session calendars, current contract-specific price-limit data, and a confirmed
maximum-withdrawable payout rule.

The historic provider-specific policy family remains at its original paths
because immutable research evidence binds those bytes. It is lineage, not the
current configuration or runtime.
