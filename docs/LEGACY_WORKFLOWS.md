# Legacy workflow registry

This registry classifies preserved historic workflow material. It is evidence,
not an operating guide. Current work starts at `CURRENT_WORKFLOW.md`.

## Retired code and tests

The closure engine; `active_data_closure_successor_v1.py`;
`active_data_closure_suite_successor_v1_4.py`; `active_data_full_successor.py`;
`active_data_full_successor_v11.py`; `active_data_full_successor_v11_2.py`;
`active_data_full_successor_v11_3.py`; `active_data_publication_successor_v1_1.py`;
`active_data_staging_successor_v1.py`; active-data transports; durable task
transports; and task-scheduler authority helpers, with their matching
`tests/test_*successor*.py`, `tests/test_active_data_*.py`,
`tests/test_durable_windows_task_transport*.py`, and
`tests/test_*task_scheduler_registration_authority*.py` tests are legacy.
Calendar capture/recovery modules and their token-era approval readers are also
legacy high-risk support. They remain readable for provenance, never appear in
current command discovery, and must not create a new approval token.

The versioned Tier 1 V4-V12 succession code, Standard-only, Final, and
Authoritative lifecycle, registration, execution modules, and their transition
snapshot tests are also retired. Their immutable registrations, receipts,
closures, and decisions remain authoritative evidence, but their operation
names cannot consume a new real-history claim. The former generic
`TrialRegistry` may still exercise synthetic mechanics; real-history
registration through it is disabled. Current trial registration and execution
must use `CertifiedResearchGateway` with a passing source-bound fold-readiness
certificate.
The consumed parallel overnight-readiness census plan's pre-execution loader is
likewise historical after its bound implementation changed. Its immutable plan,
authorization use, completed readiness report, and closure remain evidence;
only the pre-execution snapshot assertion is in the legacy test lane.
The cash-open V1/V2 dependency-forensics and four-market fold-readiness
operations are also retired. Their exact modules, plans, receipts, and reports
remain readable evidence, but the central real-history policy refuses their
operation names before authorization consumption or row access. Current
cash-open source work must use the active-catalog-only 41-market compatibility
census operation.

The following preserved scripts are historical command surfaces, not current
operating instructions:

- `scripts/acquire_cme_jan1_2019_calendar_recovery.py`
- `scripts/publish_alpha_ladder_calendar_observability_successor.py`
- `scripts/publish_alpha_ladder_calendar_observability_successor_v2.py`
- `scripts/publish_alpha_research_ladder.py`
- `scripts/run_cash_open_impulse_dependency_forensics.py`
- `scripts/run_cash_open_impulse_dependency_forensics_v2.py`
- `scripts/run_cash_open_impulse_fold_readiness_census.py`
- `scripts/run_cash_open_impulse_fold_readiness_census_v2.py`

They are retained at their original paths because immutable plans and evidence
bind those bytes. They are absent from packaged command entry points and must
not be used to mint a new authorization, access a provider, publish or activate
state, or read historical rows. Current work must use the workflow and gateway
named in `CURRENT_WORKFLOW.md`.

## Preserved evidence families

- `configs/*_approval_v*.json` for retired active-data and scheduler attempts
- `data/active/**`, `manifests/active_data_view/**`, and
  `reports/active_data_view/**`
- `manifests/workflow/closure/**`, `reports/workflow/closure/**`, and
  `state/closure_readiness_backups/**`
- inherited Phase 3 input records, Phase 4 trial reports, and closure-launch
  scripts
- retired cockpit history, installation, package-candidate, and workflow
  manifests and reports
- associated closure, transport, scheduler, and recovery scripts

## Apex-named micro and prop-firm lineage

The completed Apex-named micro source build and publication family is retained
at its original paths because plans, manifests, receipts, catalogs, pointers,
and evidence bind those names and bytes. This includes `configs/apex_micro_*`,
`scripts/prepare_apex_micro_*`, `state/unpublished_evidence/apex_micro_*`, the
accepted `data/active/catalogs/apex_micro.json` catalog, its
`configs/active_micro_alpha_research_ladder.json` pointer, and embedded Apex
schema, operation, and lane identifiers.

Completed publication staging under
`state/data_publication_staging/apex_integer_micro_11/`, the retained
`state/data_publication_staging/apex_micro_phase2_diagnostic_v1/` diagnostic,
and provider attempts under `state/provider_acquisition_staging/apex_micro_*`
are historical lineage as well. Ignored Apex-named bytecode caches inherit
legacy module names and carry no operating authority.

`src/futures_rebuild/micro_alpha_publication.py` and
`scripts/prepare_apex_micro_publication_v1.py` are retired after their accepted
single-use publication. They are not future publication command surfaces. The
`apex_tradovate_50k_eod_risk_policy` module, config, report, and test are also
historical because micro preparation evidence binds their exact bytes.
The original `configs/prop_firm_risk_profile.json` and
`configs/tier1_phase8_evaluation.json` are hash-bound Phase 8 lineage. The
corresponding `tier1_phase8_*` modules and tests are retired compatibility and
reconstruction surfaces; current Phase 8 preparation uses the
`prop_firm_phase8_*` successor.

Future account/risk work uses `prop_firm_*`; future micro catalog work uses
`micro_futures_*`. See `docs/NAMING_AND_LINEAGE.md`. Moving the active catalog
or pointer to the generic namespace is a separately approved active-data
cutover, not a local rename.

`configs/micro_futures_legacy_lineage.json` inventories the retained Apex
operation identifiers present in the hash-bound historical policy. They are
lineage identifiers, not names for a new plan, receipt, lock, failure root, or
evidence writer.

These families are intentionally ignored by normal Git status. Ignored files
remain in place and can be inspected. Force-adding one is allowed only for an
explicit archival task; it never makes that artifact current workflow authority.

## Compatibility

Historic manifests, schemas, plans, receipts, snapshots, and reports are kept
byte-for-byte. Schema and artifact readers remain available for interpretation.
Current high-risk work uses the prepare-only interface and a plain-language
Codex confirmation, while trial declarations, release validation, protected
data boundaries, and the no-trading cockpit rule still fail closed.

## Retired Phase 3-8 execution chain

The former direct foundation, Phase 5 split, Phase 6 WFA, Phase 7 audit, Phase
8 opaque-token adapter, bracket evaluator, and bracket-successor executors are
historical evidence surfaces. On the live Alpha repository they fail before
opening or hashing caller-supplied protected payloads. Their public scripts and
functions must not be used for current registration, fitting, prediction,
evaluation, or publication. Current real research uses an immutable readiness
census and `CertifiedResearchGateway` only.
