# Cash-open pre-registration forensic remediation

Historical disposition: this report records the pre-execution remediation that
preceded the later source-compatibility rejection. It is evidence, not current
operating instruction.

Status: PREPARED — NO HISTORICAL EXECUTION AUTHORIZED

The completed forensic evidence remains immutable and unchanged. Its exact
file SHA-256 is
`c8e4b05849003f02f68e867c981414cd5910b4cb105c333d4a55eef8a6553ccb`.

## Corrections

- The 35 `ENTRY_NOT_AFTER_DECISION` labels are dependent consequences of
  missing feature inputs. They are reclassified as
  `DECISION_UNAVAILABLE_DUE_TO_FEATURE_GAP`; no genuine entry-order violation
  remains in the completed evidence.
- Outer folds are derived from the intersection of explicitly
  checkpoint-eligible sessions for every required market. Each evaluation
  fold still contains exactly 63 sessions, with one embargo session.
- Future source access must use the authoritative active-catalog resolver.
  Direct folder construction, globbing, newest-release selection, and fallback
  release selection are forbidden.

## Forty-one-market preparation boundary

The active catalog contains 41 markets, but the 2018-2022 discovery matrix is
not a complete 205 materialized pairs:

- 198 pairs are research-ready and selection-eligible.
- KE 2019, KE 2021, SR1 2020, and SR3 2020 are quarantined.
- ETH 2018, ETH 2019, and ETH 2020 are absent from the active catalog.

The registered authoritative checkpoint calendar covers only ES, CL, ZN, and
6E. The remaining 37 markets cannot be assigned checkpoint-eligible folds
without a separately certified calendar expansion. The prepared 41-market
census therefore fails closed and cannot read historical rows or execute until
that missing calendar authority is supplied through an immutable successor.

This is a data- and calendar-compatibility screen only. It does not select
markets using returns, fit models, generate predictions, or evaluate a
strategy.
