# Alpha research ladder publication manifest

Status: published and active; post-activation validation passed.

## Outcome

The successor implements one frozen, sequential ladder:

`synthetic Tier 0 -> ES pilot -> Tier 1 -> Tier 2 -> Tier 3 -> one 2025 holdout -> forward monitoring`

The ES pilot uses 504 training sessions and the immediately following 63
evaluation sessions after row-certified purge and embargo. Those 63 evaluation
sessions must be excluded from every later market. Tier membership is exactly
4, 16, and 41 markets. Tier 3 reports the 38 traditional and three satellite
markets separately, and satellites cannot rescue traditional failure.

## Prepared immutable artifacts

- `state/unpublished_evidence/alpha_research_ladder_preparation/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/universe_contract.json`
  - contract ID: `d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18`
  - file SHA-256: `0e258e6d7b375763d9e2d4795ecd7e9f1ee8e2d83ae0993f1f8305558900c453`
- `state/unpublished_evidence/alpha_research_ladder_preparation/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/alpha_tiered.yaml`
  - profile ID: `a2088ceb344f1aa44bf3a663ca2e2036e0cbea575e5521d04976ef0443a53210`
  - file SHA-256: `f7a914d275aca3ecfa41486fd4cf9dbeab5d5e4bf4a41bf577ac8af13f73cf39`
- `state/unpublished_evidence/alpha_research_ladder_preparation/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/invalid_preparations.json`
  - record ID: `5fab42d141b0508c53753365048395bf49cce2c9a7bd3b15822dc6afbb95c8bd`
  - file SHA-256: `c142cded10886c7acd80ac5bb0ff5cce83d7214ed21b004c3b909f6dbd9259b9`
- `state/unpublished_evidence/alpha_research_ladder_preparation/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/es_2018_alpha_profile_preparation.yaml`
  - exact preserved copy of the superseded uncommitted ES-2018 preparation
  - file SHA-256: `f66109b982e4ecaaf5eef3c9426bdc34f6fd5b1da0959fd90e559fe18a07ffe2`
- The earlier inactive preparation at
  `state/unpublished_evidence/alpha_research_ladder_preparation/db38f7254775c4bd8ade1aae130607746810867d13ec289bebdcbd1fc4e96576/`
  is preserved as invalid because its profile bound an unpublished path.

## Exact implementation scope

- `src/futures_rebuild/alpha_research_ladder.py`
- `src/futures_rebuild/certified_research_gateway.py`
- `src/futures_rebuild/preexecution_fold_certification.py`
- `src/futures_rebuild/research_gateway_policy.py`
- `scripts/prepare_alpha_research_ladder.py`
- `tests/test_alpha_research_ladder.py`
- `tests/test_certified_research_gateway.py`
- `tests/test_preexecution_fold_certification.py`
- `tests/test_repo_boundary.py`
- `CURRENT_WORKFLOW.md`
- `CODEX_HANDOFF.md`
- this manifest

## Activation boundary

Publication must create these exact immutable copies:

- `state/alpha_ladder_registry/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/universe_contract.json`
- `state/alpha_ladder_registry/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/alpha_tiered.yaml`
- `state/alpha_ladder_registry/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/invalid_preparations.json`
- `state/alpha_ladder_registry/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/es_2018_alpha_profile_preparation.yaml`

It must verify exact bytes and create
`configs/active_alpha_research_ladder.json` last. Post-activation validation is
mandatory, with restoration of the prior no-pointer state on any failure. There
is currently no active ladder pointer.

Publication and activation do not authorize historical-row reads, a readiness
census, trial registration or execution, model fitting, predictions, economic
evaluation, 2025 access, provider or credential access, staging, commit, push,
or trading.

## Activation result

- Active pointer SHA-256:
  `e4274219b2117af6926845866726a8dc2192754941fc5abdba3c9a7156c5edc9`
- All four registry files are exact-byte copies of the prepared artifacts.
- The pointer was written last, the registered-context loader passed, and 81
  post-activation focused tests passed. Rollback was not required.
