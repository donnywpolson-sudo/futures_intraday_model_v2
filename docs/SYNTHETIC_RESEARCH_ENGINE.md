# Synthetic research engine boundary

Status: mechanically implemented and synthetic-test only. This is not alpha
evidence, historical readiness, a candidate, or authorization to run real
history.

The implemented path has three isolated roles. The splitter owns all labels and
hands the builder matured training/inner labels plus outer features with the
outer labels physically absent. The builder fits mean/std scaling inside each
fit fold, selects a positive-ridge linear forecast using only inner validation
scores, refits on the eligible outer-training rows, and freezes predictions and
an artifact compatible with `FUTURES_TRUSTED_LINEAR_FORECAST_V1`. Every fit and
audit records exact sample IDs and content hashes. The evaluator imports no
builder implementation and makes zero fit calls; it receives only frozen
predictions and its outer-label packet.

The causal outcome producer uses verified Phase 2 one-minute bars and the exact
definition, session-policy, and economics dependency closure. Its pinned method
is `ACTUAL_CONTRACT_EVENT_OPEN_TO_EVENT_OPEN_1M_V1`: entry and exit must land on
exact one-minute event opens, every grid minute must contain exactly one
eligible resolved row, prices must be tick-valid, economics and session identity
must remain unchanged, and the actual contract segment may never change. Missing,
ambiguous, quarantined, off-grid, or cross-session paths become
`MISSING_SOURCE`; an observed contract change becomes `ROLL_UNRESOLVED`. Both
remain in the exact prediction census with no return. A causal outcome release
records the label-method ID and is re-derived on load before it is trusted.

Synthetic tests cover known-signal recovery, deterministic no-edge noise,
outer-label mutation, a future-feature canary rejected before linear algebra,
manifest/artifact/parity tampering, exact fit/audit bindings, missing bars, and
mid-horizon rolls. Evaluations hard-code `alpha_evidence=false` and
`candidate_eligible=false`.

Still absent by design are the real feature/outcome dataset assembler, real
historical hypothesis/WFA execution, portfolio/session inference, costs, HAC or
block-bootstrap uncertainty, DSR/Romano-Wolf/PBO gates, final-holdout access, and
candidate sealing. Those omissions keep `HISTORICAL_RESEARCH_READY` false and
preserve the user's pause before any real-history research execution.
