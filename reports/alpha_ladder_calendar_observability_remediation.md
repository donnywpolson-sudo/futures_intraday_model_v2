# Alpha calendar and source-observability remediation

Current disposition: the calendar/source-observability successor remains
authoritative. References below to mechanism `50dfc52c...034e5` are historical;
that mechanism was later closed as source-incompatible and superseded by the
unregistered counted mechanism `cfefe8ce...563dc3`.

## Status

Published and active as the authoritative calendar/source-observability view.
The trading mechanism remains unregistered.

The remediation uses the sealed price-free provenance report as its only
row-derived input. It does not reread historical prices.

## Corrected calendar facts

Exactly two rows change relative to active calendar
`cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b`:

- ES, 2018-12-05: all 09:00, 09:30, 10:00, and 10:30 Chicago checkpoints
  are closed because the authoritative CME equity session ended at 08:30.
- ZN, 2018-12-05: all four checkpoints are closed because interest-rate
  products did not reopen until trade date 2018-12-06.

No other one of the 74,866 market-session rows changes.

## Explicit source-unobservable facts

The following six calendar-open feature windows are retained in required
checkpoint accounting as `EXPLICIT_SOURCE_UNOBSERVABLE_ABSTENTION`:

- CL, 2020-02-28
- ZN, 2020-02-28
- 6E, 2020-06-30
- CL, 2020-06-30
- ES, 2020-06-30
- ZN, 2020-06-30

For each, the sealed audit found no reported bars in the required 09:30-10:00
Chicago window in any bound local provider, raw, causal, or active source. It
also found no normalization loss and no complete independent trade stream that
could prove a genuine no-trade session. These sessions therefore remain
calendar-open; they are not relabelled as exchange closures or verified
no-trade days, and they may not be silently removed.

## Policy value

- Risk prevented: missing source data cannot masquerade as a closure, a
  no-trade day, or complete research evidence.
- Decision improved: later readiness work can distinguish exchange eligibility
  from source eligibility before it certifies folds.
- Why the simpler calendar flag is insufficient: a market can be open while a
  required immutable source window is absent, so one Boolean cannot represent
  both facts accurately.

## Published artifact

- Calendar ID:
  `ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9`
- SHA-256:
  `efdf4f765e44ac2f312dce62b7145bb1ed70d01fd8c76fb5bfb3f32652f1a632`
- Path:
  `state/unpublished_evidence/alpha_ladder_calendar_observability_successor/ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9/historical_calendar_successor.json`
- Provenance report ID:
  `ca6e3173dbd986c959b2f59f80349f68d9aafba53caaf2b8f2d40feb27907ec3`

## Activation lifecycle

- Active pointer ID:
  `bfc3036f739f7fac592e9f7ebf6ff9ee225c8f257d6f1e324875d74a0cec35e4`
- Corrected calendar registration ID:
  `eb57241b1214e2dc85e8a36695059da3e3e0e65222eb4f2e8e7808b0f6a3ff6b`
- Activation event ID:
  `1ac22903b81ca4dd21b24957a09ee95bc93f8d66d52445791cd13de27e11f823`
- Failed V1 activation record ID:
  `1f42769886b72636921e5e83a2eb87088bea4be079706699ef0847e7e4a0ab80`

The first pointer-last activation failed its postcheck because the prepared
artifact's predecessor binding referenced the mutable active-pointer path. The
rollback restored the predecessor pointer byte-for-byte. The failed
registration and event remain immutable. The corrected lifecycle verifies that
historic binding against the immutable predecessor-pointer snapshot; it then
activated successfully and passed the same validation again in registered
context.

## Registration consequence

Mechanism `50dfc52cb5b4145dcbd6a761b3c626dae28c0aa974f6db35a1b60099297034e5`
remains unregistered. Explicitly recording the six source gaps does not satisfy
its locked 100% readiness requirement. Registration remains fail-closed until
a new row-level readiness census passes, or authoritative source rows are
recovered under a separate approval. A future change that excludes
source-unobservable sessions from fold construction would be a new counted
source-eligibility semantic, not a silent repair of this mechanism.

## Preservation and authority

- The active calendar pointer now selects the exact published successor.
- The predecessor pointer is preserved byte-for-byte in the successor registry.
- The counted mechanism bytes are unchanged.
- The sealed provenance report and every prior readiness artifact are
  unchanged.
- No historical row was reread, and no return, model, prediction, or economic
  result was computed.
- No provider, network, credential, 2025, active-data, registration, staging,
  commit, push, or trading access occurred.
