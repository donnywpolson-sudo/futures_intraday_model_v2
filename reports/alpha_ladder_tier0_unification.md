# Alpha ladder Tier 0 unification

## Status

Activated locally on 2026-08-08 after the owner requested that synthetic
engineering and the ES pilot be combined into one Tier 0.

## Decision

Tier 0 is one visible ladder level with two mandatory gates in order:

1. `synthetic_engineering`: synthetic ES mechanics validation only.
2. `es_pilot`: one row-certified ES go/no-go screen with 504 training sessions
   and 63 evaluation sessions.

Both gates must pass for Tier 0 to pass. The ES pilot remains qualification,
not multi-market alpha confirmation. Immutable operational artifacts retain
`tier_0` and `pilot` as the two gate identifiers so historical evidence remains
interpretable.

## Control justification

- Risk prevented: synthetic success being mistaken for authority or evidence
  for a real-history ES evaluation.
- Decision improved: whether the frozen mechanism has completed Tier 0 and may
  advance to Tier 1.
- Why documentation alone is insufficient: the prior active machine-readable
  contract represented the pilot as a separate level, so a label-only change
  would leave code and operational truth inconsistent.

## Activation record

- Preserved predecessor contract:
  `d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18`
- Predecessor pointer file SHA-256:
  `e4274219b2117af6926845866726a8dc2192754941fc5abdba3c9a7156c5edc9`
- Active successor contract:
  `53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815`
- Active successor profile:
  `18fbb7a3a405ee2bcaef5dd7d6e757cfb3a69ec8485afd34e5fcf1f627aaeca6`
- Successor pointer file SHA-256:
  `bd119473e0b2b60ffcfcf41923ff2e07dc2e8d3608fac9caaf6818336fb5e624`

The predecessor registry, failed pilot evidence, closure, completed risk census,
and authorization-use records were not modified. They remain bound to their
original identities. No old mechanism can inherit the successor contract; a
new counted mechanism starts at the Tier 0 synthetic engineering gate.

## Authority boundary

This local activation read no historical rows, contacted no provider, selected
no model or risk parameter, performed no evaluation, and authorized no research,
holdout, forward, publication outside the repository, trading, staging, commit,
or push.

## Verification

- Active contract/profile/pointer hash validation: pass.
- Current test lane: `107 passed`.
- Alpha high-risk test selection: `185 passed`.
- Focused affected contract selection: `98 passed`.
- `git diff --check`: pass.

The unfiltered high-risk lane exceeded its five-minute command timeout without
producing a result. It is not counted as a pass; the complete Alpha selection
above is the direct safety surface for this change.
