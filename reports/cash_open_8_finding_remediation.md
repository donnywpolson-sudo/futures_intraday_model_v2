# Cash-open eight-finding remediation

Historical disposition: the required calendar was later activated, the census
was executed, and this cash-open mechanism was conclusively rejected before
registration for source incompatibility. The status below records the earlier
pre-activation checkpoint.

Status: `PRE_DATA_IMPLEMENTED_CALENDAR_ACTIVATION_REQUIRED`

## Outcome

All decision-validity controls are implemented without opening historical
price rows. The rejected four-market protocol remains immutable and is denied
by the current registration gateway. The source-only 41-market census cannot
be planned or executed until the prepared four-checkpoint calendar successor
is separately published and activated.

## Controls

- Feature-dependent timing failures use
  `DECISION_UNAVAILABLE_DUE_TO_FEATURE_GAP`; a genuine entry-order failure is
  evaluated only after a causal decision exists.
- Folds are built from each market's mechanism-eligible calendar sessions
  before source completeness is inspected: 504 initial training sessions,
  one embargo session, 63 evaluation sessions, eight expanding folds, and a
  31-minute purge.
- The active catalog's `SELECTION` resolver is the only current source route.
  The four old cash-open forensic/readiness operations are removed from the
  central real-history allowlist; their files remain historical evidence.
- Every feature, candidate, execution, and independently scheduled active
  baseline path requires 100% coverage. Future-incomplete paths remain explicit
  failures in the denominator.
- Selection uses source compatibility only. It tries `09:00+10:30`, then the
  preregistered non-overlapping fallback pairs, then single checkpoints, and
  includes every passing market only when at least two pass.

## Policy value

The control prevents an archive search, missing future path, or calendar gap
from changing the opportunity universe after source rows are seen. It improves
the registration decision by making source compatibility a reproducible input
rather than an inferred workflow convention. Documentation alone is
insufficient because the retired scripts could still consume real-history
authorization and the prior calendar preparation invalidated itself when its
mutable pointer changed.

## Preserved invalid preparation

The first four-checkpoint preparation `fa9dc5cd...46105` and specification
`26a00b54...45ae` are preserved and classified
`INVALID_PRE_DATA_MUTABLE_POINTER_BINDING`. No historical row or economic
result was involved. The corrected calendar `cd64f912...0843b` binds immutable
predecessor-pointer evidence and has 74,866 rows, four checkpoints, and zero
unresolved reference states.

## Current identities

- Corrected calendar ID: `cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b`
- Corrected calendar SHA-256: `e76ec4310da674e1bbacf5356662d97d8a2c8b115c728fa9386b53f8d289be52`
- Corrected source-only specification ID: `56d6f1631de779ea13626c81b96a263d7828397ee2944aee6151cd84c6cebe41`
- Four-market rejection record ID: `136f0856bf00c7a2924cab2b2c4d7b9ad4b134a9adf03eae69f8f7eaafd6628e`

## Next boundary

Publication and activation of the corrected calendar require separate
approval. The active pointer must be replaced last and restored byte-for-byte
on any registered-context or complete-suite failure. Only after activation may
the repository create the final hash-bound census plan. Historical source-row
access remains a later, separate approval.
