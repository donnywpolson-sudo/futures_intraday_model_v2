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

These families are intentionally ignored by normal Git status. Ignored files
remain in place and can be inspected. Force-adding one is allowed only for an
explicit archival task; it never makes that artifact current workflow authority.

## Compatibility

Historic manifests, schemas, plans, receipts, snapshots, and reports are kept
byte-for-byte. Schema and artifact readers remain available for interpretation.
Current high-risk work uses the prepare-only interface and a plain-language
Codex confirmation, while trial declarations, release validation, protected
data boundaries, and the no-trading cockpit rule still fail closed.
