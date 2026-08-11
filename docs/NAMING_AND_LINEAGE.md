# Generic naming and immutable legacy lineage

## Operational namespaces

Future prop-firm account and risk work uses `prop_firm_*`. Future micro-contract
catalog, pointer, lock, failure, evidence, plan, and operation names use
`micro_futures_*`.

Provider identity belongs inside a sourced profile. The active selection is in
`configs/prop_firm_profiles.json`; adding another firm changes profile data and
`active_profile_id`, not a module, schema, filename, or directory.
Account stage, internal strategy risk, execution mapping, platform costs, and
payout behavior are separate generic bindings. Their IDs and hashes are part
of the current runtime/cache identity, so switching a provider, stage, or
policy cannot reuse a stale result.
Provider-neutral Phase 8 preparation uses
`configs/prop_firm_phase8_evaluation.json` and
`src/futures_rebuild/prop_firm_phase8.py`; the Phase 8 file contains only
generic model-evaluation scope and resolves provider rules, explicit account
stage, mapping, costs, URLs, and result status from selected `prop_firm_*`
bindings. Profile selection grants no provider, prop-firm evaluation, model
evaluation, publication, payout, deployment, or trading authority.

The prepared micro-futures cutover is in
`configs/micro_futures_catalog_migration_plan_v1.json`. It proposes:

- `data/active/catalogs/micro_futures.json`;
- `configs/active_micro_futures_research_ladder.json`;
- `state/locks/micro_futures_publication.lock`;
- `state/micro_futures_publication_failed/`;
- `state/unpublished_evidence/micro_futures_catalog_migration_v1/`;
- lane `micro_futures_integer_11`.

That plan is prepare-only. It grants no provider access, row read, publication,
active-data change, registration, evaluation, holdout/forward access, or
trading authority.

## Immutable Apex lineage

The Apex-named micro publication completed successfully before this migration:
144 manifests and approximately 7.08 GB were published to content-addressed
release paths, machine-local `data/active/catalogs/apex_micro.json` became the active micro
source catalog, and `configs/active_micro_alpha_research_ladder.json` became its
pointer. The standard active catalog was not changed, and no 2025 or 2026
payload was opened.

Those ignored pointer/catalog bytes are optional exact local evidence. They are
not inputs to a clean tracked checkout or the canonical `current` test lane.
Their hash-bound verification is isolated in `local_evidence`; absence of the
explicit local-evidence manifest fails that lane closed.

Those accepted bytes and paths are preserved. The following families are
historical lineage, not current configuration or future command surfaces:

- `configs/apex_micro_*` and `scripts/prepare_apex_micro_*`;
- `state/unpublished_evidence/apex_micro_*` and associated authorization use;
- completed `state/data_publication_staging/apex_integer_micro_11/` and
  `state/data_publication_staging/apex_micro_phase2_diagnostic_v1/` trees;
- retained `state/provider_acquisition_staging/apex_micro_*` acquisition
  attempts, including incomplete terminal and partial-download evidence;
- `src/futures_rebuild/micro_alpha_publication.py` and
  `scripts/prepare_apex_micro_publication_v1.py`;
- `data/active/catalogs/apex_micro.json` and
  `configs/active_micro_alpha_research_ladder.json` until an approved cutover;
- the `apex_tradovate_50k_eod_risk_policy` module, config, report, and test;
- `configs/prop_firm_risk_profile.json`, `configs/tier1_phase8_evaluation.json`,
  and the `tier1_phase8_*` implementation and test family bound to the former
  frozen Phase 8 declarations;
- Apex-named schema, operation, lane, and profile IDs embedded in accepted
  manifests, plans, receipts, reports, catalogs, and pointers.

The complete retained micro operation-ID and path-family classification is in
`configs/micro_futures_legacy_lineage.json`. Its bindings include the
hash-bound central policy bytes that historically recognized those operation
IDs. Recognition in those preserved bytes is not authority for a new plan or
future output; new micro work must use the `MICRO_FUTURES` operation namespace.

Ignored `__pycache__` files derived from those legacy modules have no workflow
authority. They may retain the source module name but are not prospective
artifacts or supported command surfaces.

These names remain because changing them would break hashes, bindings, audit
relationships, or truthful provider attribution. They do not authorize new
Apex-named outputs. The former publication writer is retired after its accepted
single-use publication; future work starts from the generic cutover plan.

## Cutover boundary

Creating the generic active catalog or changing the active pointer is an
active-data mutation. It requires a separate plain-language approval after live
source hashes and plan bytes are revalidated. The legacy catalog and pointer
must remain readable after cutover for provenance; cutover is a create-only
successor, not a rename or deletion.
