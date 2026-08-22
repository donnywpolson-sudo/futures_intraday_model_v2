# Current pipeline folder map

> **Generated topology view.** This file is deterministically rendered from
> `configs/repository_surface.json` and `pyproject.toml`. Do not maintain
> topology classifications independently in this file.

## Purpose and authority

This map is concise current navigation. It is generated and is not an authority system.

- `CURRENT_WORKFLOW.md` controls normal work.
- `AGENTS.md` contains durable safety and research-integrity policy.
- `configs/repository_surface.json` is the canonical machine-readable path-role registry.
- `SOURCE_OF_TRUTH.md` is the broader generated repository navigation view.
- `PIPELINE_FOLDER_MAP.md` is this generated topology view only.
- `ACTIVE_SOURCE_FILES.txt` is the generated virtual view of tracked current operational and supporting files.
- `pyproject.toml` defines the public package and command surface.
- `docs/LEGACY_WORKFLOWS.md` controls interpretation of retired workflow material.
- The complete former map is preserved at `docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md`.

This view does not replace the workflow, safety policy, canonical registry, or broader source-of-truth view, and it does not establish research, provider, production, execution, or trading readiness.

## Current authority and pointer surfaces

Rows below resolve from canonical registry roles. Directory or file presence never supplies authority.

| Role | Path | Classification | Local only | Registry boundary |
| --- | --- | --- | --- | --- |
| Canonical path-role registry | `configs/repository_surface.json` | `CURRENT_SUPPORTING` | no | Canonical fail-closed classification of repository-relative source, authority, evidence, generated, runtime, secret, packaging, prepared, and unresolved surfaces. |
| Normal work | `CURRENT_WORKFLOW.md` | `CURRENT_OPERATIONAL` | no | The sole day-to-day workflow authority; the current worktree version controls machine-local work. |
| Durable safety policy | `AGENTS.md` | `CURRENT_OPERATIONAL` | no | Durable safety and research policy; it defers normal procedure to CURRENT_WORKFLOW.md. |
| Generated source-of-truth view | `SOURCE_OF_TRUTH.md` | `CURRENT_SUPPORTING` | no | Deterministic human navigation view derived from configs/repository_surface.json and pyproject.toml. It is not workflow, safety-policy, pointer, catalog, trial, execution, or cleanup authority and grants no provider, data-read, publication, installation, trading, Git, or active-state authority. |
| Generated pipeline-folder-map view | `PIPELINE_FOLDER_MAP.md` | `CURRENT_SUPPORTING` | no | Deterministically rendered from configs/repository_surface.json and pyproject.toml as a current topology and navigation view. It is not the normal-work authority, safety-policy authority, canonical machine-readable registry, or historical evidence ledger. The complete former map is preserved under docs/history/. This view grants no provider, research, data-read, publication, installation, trading, cleanup, Git, or active-state authority. |
| Generated active-source-files view | `ACTIVE_SOURCE_FILES.txt` | `CURRENT_SUPPORTING` | no | Deterministically rendered from the tracked path inventory and configs/repository_surface.json. It lists only files classified CURRENT_OPERATIONAL or CURRENT_SUPPORTING and is a virtual active-source view that does not physically move or hide files. It is not normal-work authority, not safety-policy authority, and not the canonical registry. It grants no provider, research, data-read, publication, installation, trading, deletion, Git, or active-state authority. Historical paths remain preserved in place, and absent paths in sanitized no-Git exports do not establish deletion or retirement. |
| Public package and commands | `pyproject.toml` | `CURRENT_OPERATIONAL` | no | Defines the Python package, dependency surface, pytest baseline, and public console commands. |
| Standard Alpha pointer | `configs/active_alpha_research_ladder.json` | `CURRENT_OPERATIONAL` | no | Machine-readable standard Alpha contract and profile pointer; distinct from the micro source pointer. |
| Standard active data catalog | `data/active/catalog.json` | `CURRENT_OPERATIONAL` | yes | Machine-local standard-data selection metadata; absence is permitted in provider-free source exports. |
| Micro source-selection pointer | `configs/active_micro_alpha_research_ladder.json` | `CURRENT_OPERATIONAL` | yes | Machine-local source-only micro pointer. It states that the mechanism is not frozen and permits no evaluation, holdout, forward, or registration action. |
| Micro source catalog | `data/active/catalogs/apex_micro.json` | `CURRENT_OPERATIONAL` | yes | Accepted Apex-named micro catalog retained at its lineage-bound path; absence is permitted in provider-free source exports. |
| Current real-history boundary | `src/futures_rebuild/certified_research_gateway.py` | `CURRENT_OPERATIONAL` | no | The sole current real-history registration and trial-execution gateway; use remains separately controlled. |
| Synthetic-only public pipeline | `src/futures_rebuild/pipeline.py` | `CURRENT_OPERATIONAL` | no | Current futures-pipeline target; its built-in phase mechanics are synthetic-only. |
| Retired workflow registry | `docs/LEGACY_WORKFLOWS.md` | `CURRENT_SUPPORTING` | no | Current classifier for retired material; its retired paths are evidence, never new command surfaces. |

The standard Alpha pointer/catalog and micro source pointer/catalog remain separate. Local-only controls may be absent from clean provider-free exports. Micro source selection does not establish a frozen mechanism, registered trial, historical-row authority, research passage, holdout authority, production readiness, execution readiness, or trading authority.

The versioned data-closure controls are `configs/micro_contract_universe_v1.json`, `configs/core_databento_standard_l0_dependency_policy_v1.json`, `configs/data_surface_registry_v1.json`, `configs/data_capability_baseline_v1.json`, and `configs/data_phase_closed_v1.json`. They register policy, evidence identities, and fail-closed selection rules; they do not replace either active catalog or pointer and grant no market activation or research authority.

Data Phase Closed v1 records a certified standard foundation, a completed capability assessment, and structurally verified opaque custody for 17 micro markets. The legacy four remain the only active micro catalog members; the additional 13 remain inactive and uncertified for research. Alpha research remains disabled, and the next boundary is feature/label/split/transform successor design.

The governing sequence is Phase 0 Immutable provider custody; Phase 1 Certified historical data foundation; Phase 2 Historical data capability and alpha investigability; Phase 3 Hypothesis, feature, label, split, and transform contracts; Phase 4 ES discovery sandbox; Phase 5 Full-size Tier-1 falsification; Phase 6 Micro transfer and execution validation; Phase 7 Expanded robustness; Phase 8 Economic and execution validation; Phase 9 Sealed holdout; and Phase 10 Paper/live readiness.

`CertifiedResearchGateway` is the sole current real-history registration and trial-execution boundary; use remains separately controlled. `futures-pipeline` is synthetic-only. No other public command provides a real-history execution surface.

## Public commands

This is the exact deterministic `[project.scripts]` mapping from `pyproject.toml`.

| Command | Python target |
| --- | --- |
| `futures-dbn-catalog` | `futures_rebuild.dbn_catalog:main` |
| `futures-high-risk-prepare` | `futures_rebuild.high_risk:main` |
| `futures-master-audit` | `futures_rebuild.audit.__main__:main` |
| `futures-meta-audit` | `futures_rebuild.meta_audit:main` |
| `futures-pipeline` | `futures_rebuild.pipeline:main` |
| `futures-readiness` | `futures_rebuild.readiness:main` |
| `futures-retirement-audit` | `futures_rebuild.retirement:main` |

Private helpers, documentation-only commands, historical scripts, ignored installation candidates, and untracked execution-looking modules are not public commands.

## Major repository topology

Each family appears once. Its exact root entry supplies the role, tracking/local state, deletion policy, and notes; represented classifications summarize all registry entries in that family.

| Family | Current role | Represented classifications | Tracking and locality | Deletion policy | Registry notes |
| --- | --- | --- | --- | --- | --- |
| `src/` | `PACKAGE_SOURCE_ROOT` | `CURRENT_OPERATIONAL`, `CURRENT_SUPPORTING`, `HISTORICAL_HASH_BOUND`, `REGENERABLE_CACHE` | `MIXED`; local-only: no | `PRESERVE` | Package source root; descendant currentness is refined by exact and family entries. |
| `configs/` | `MACHINE_READABLE_CONTROL_ROOT` | `CURRENT_OPERATIONAL`, `CURRENT_SUPPORTING`, `HISTORICAL_HASH_BOUND`, `HISTORICAL_UNBOUND`, `PREPARED_NOT_EXECUTED`, `LOCAL_SECRET`, `UNRESOLVED_MANUAL_REVIEW` | `MIXED`; local-only: no | `PRESERVE` | Mixed control root whose more-specific entries determine current, prepared, historical, and secret roles. |
| `data/` | `PROTECTED_DATA_ROOT` | `CURRENT_OPERATIONAL`, `CURRENT_SUPPORTING`, `HISTORICAL_HASH_BOUND`, `UNRESOLVED_MANUAL_REVIEW` | `MIXED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Protected mixed source, release, evidence, and active-selection root; payload contents are outside this registry validator. |
| `manifests/` | `MANIFEST_AND_LINEAGE_ROOT` | `HISTORICAL_HASH_BOUND` | `MIXED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Tracked and ignored manifests bind releases, attempts, paths, and bytes. |
| `reports/` | `REPORT_AND_DIAGNOSTIC_EVIDENCE_ROOT` | `HISTORICAL_HASH_BOUND` | `MIXED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Reports mix tracked evidence, ignored diagnostics, and generated output; no family-wide cleanup applies. |
| `state/` | `EVIDENCE_AND_RUNTIME_STATE_ROOT` | `CURRENT_SUPPORTING`, `HISTORICAL_HASH_BOUND`, `PREPARED_NOT_EXECUTED`, `LOCAL_RUNTIME_STATE` | `MIXED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Mixed registries, receipts, immutable evidence, staging, runtime state, and placeholders. |
| `scripts/` | `SCRIPT_ROOT` | `CURRENT_OPERATIONAL`, `CURRENT_SUPPORTING`, `HISTORICAL_HASH_BOUND`, `PREPARED_NOT_EXECUTED`, `REGENERABLE_CACHE`, `UNRESOLVED_MANUAL_REVIEW` | `MIXED`; local-only: no | `PRESERVE` | Mixed current helpers, prepare-only interfaces, and hash-bound historical command surfaces. |
| `tests/` | `TEST_ROOT` | `CURRENT_SUPPORTING`, `REGENERABLE_CACHE` | `MIXED`; local-only: no | `PRESERVE` | Provider-free current, high-risk, legacy, and local-evidence lanes coexist under explicit selection. |
| `docs/` | `DOCUMENTATION_ROOT` | `CURRENT_SUPPORTING`, `HISTORICAL_HASH_BOUND` | `MIXED`; local-only: no | `PRESERVE` | Current supporting documentation plus explicitly classified historical guidance. |
| `FuturesLiveCockpit/` | `MIXED_COCKPIT_PACKAGING_ROOT` | `CURRENT_OPERATIONAL`, `GENERATED_OUTPUT`, `MIXED_PACKAGING_SOURCE_OUTPUT` | `MIXED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Tracked packaging inputs coexist with ignored executable and onedir runtime output; never apply a directory-wide cleanup rule. |
| `build/` | `CONCURRENT_BUILD_OUTPUT` | `UNRESOLVED_MANUAL_REVIEW` | `UNTRACKED_GENERATED`; local-only: yes | `MANUAL_REVIEW_REQUIRED` | Local build output may contain a current candidate, rollback material, diagnostics, or interrupted work; exact review is required. |
| `dist/` | `ABSENT_OR_GENERATED_DISTRIBUTION_ROOT` | `GENERATED_OUTPUT` | `ABSENT_EXPECTED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Distribution output may be absent or generated; any future contents require exact revalidation. |
| `tmp/` | `AMBIGUOUS_LOCAL_REFERENCE_ROOT` | `UNRESOLVED_MANUAL_REVIEW` | `IGNORED_LOCAL`; local-only: yes | `MANUAL_REVIEW_REQUIRED` | Contains reference downloads and possible only local copies, not a proven cache. |
| `artifacts/` | `AMBIGUOUS_ARTIFACT_ROOT` | `GENERATED_OUTPUT` | `UNTRACKED_GENERATED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Build intermediates and diagnostics may be reproducible in principle, but current dependency and rollback closure is absent. |
| `bundles/` | `BUNDLE_OUTPUT_ROOT` | `GENERATED_OUTPUT` | `MIXED`; local-only: yes | `NO_AUTOMATIC_DELETE` | Tracked placeholder with possible ignored bundles; no automatic cleanup treatment. |

## Classification summary

The counts below use only the canonical classification vocabulary and count registry entries, not files present in this checkout.

| Classification | Entry count |
| --- | --- |
| `CURRENT_OPERATIONAL` | 38 |
| `CURRENT_SUPPORTING` | 39 |
| `HISTORICAL_HASH_BOUND` | 59 |
| `HISTORICAL_UNBOUND` | 3 |
| `PREPARED_NOT_EXECUTED` | 6 |
| `GENERATED_OUTPUT` | 10 |
| `REGENERABLE_CACHE` | 17 |
| `LOCAL_RUNTIME_STATE` | 7 |
| `LOCAL_SECRET` | 8 |
| `MIXED_PACKAGING_SOURCE_OUTPUT` | 2 |
| `UNRESOLVED_MANUAL_REVIEW` | 14 |

## Historical and retired boundaries

The former complete map, including its version-by-version status chronology, is preserved at `docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md`; its provenance manifest is `docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.json`. That historical record is not a runtime renderer input.

Historical exact paths may remain at their original locations. `docs/LEGACY_WORKFLOWS.md` controls interpretation of retired workflow material. Historical path presence does not make a surface current; tracked does not imply current; ignored does not imply disposable; and a current replacement must be explicitly declared. No version-by-version history belongs in this generated root map.

## Generated, local-only, mixed, and unresolved material

Local-only files may be absent from clean provider-free exports. Generated-looking paths are not automatically disposable. `FuturesLiveCockpit/` is a mixed packaging source/output surface. Directory presence does not grant authority, and untracked execution-looking code is not current merely because it exists.

The registry currently contains 14 `UNRESOLVED_MANUAL_REVIEW` entries. They require explicit review; use `configs/repository_surface.json` for their exact paths and policies.

## Safety and non-authority boundary

Neither this generated map nor the registry authorizes deletion, movement or renaming, provider access, credential access, market-data reads, historical-row access, holdout or forward access, research execution, prediction publication, candidate sealing, active-data mutation, publication, installation, activation, live smoke, trading, order placement, staging, commit, or push.

Cache deletion still requires a fresh exact machine-local census and separate approval.

## Exact installed data paths after rlac_20260814T0642492268888Z_0b571482

- Original Databento DBN custody: `data/dbn` (immutable provider source).
- Provider receipts and support files: `data/dbn` and `manifests/data_releases/dbn` (permanent provider evidence).
- Provider job, condition, request, and lifecycle evidence: `reports/overnight_data_phase_orchestrator/odpo_20260813T1345290300316Z_0b571482`.
- Standard certified causal 1m: `data/active/causally_gated_normalized` (exact catalog-selected payload vault).
- Tier 0/Tier 1 certified causal 1s: `data/vault/releases/dual_resolution_tier01/drt01_20260813T2333053884139Z_0b571482/causal_1s`.
- Tier 0/Tier 1 certified causal 1m: `data/vault/releases/dual_resolution_tier01/drt01_20260813T2333053884139Z_0b571482/causal_1m`.
- Tier 0/Tier 1 causal reference metadata: `data/vault/releases/dual_resolution_tier01/drt01_20260813T2333053884139Z_0b571482/causal_reference_metadata`.
- Full-size DBN-native reference authority: `data/vault/releases/direct_causal_authority/rlac_20260814T0642492268888Z_0b571482/causal_reference_metadata`.
- Active catalogs and pointers: `data/active` and `configs/active_micro_alpha_research_ladder.json` (lightweight authority only).
- Sealed 2025/2026 causal custody: `data/causally_gated_normalized` (year-partitioned custody outside current catalog selection; no row access).
- Sealed source and trades custody: `data/dbn` (immutable DBNs and manifests, including `trades`; no row access).
- Former materialized raw payload: all 3,792 Wave E targets are permanently absent; preserved empty parent directories are evidence only and provide no rollback or data authority.
- Cutover reports and certification: `reports/raw_layer_authority_cutover/rlac_20260814T0642492268888Z_0b571482` (certification evidence outside runtime payload roots).

No conceptual placeholder in this table grants path authority. Exact catalogs, manifests, policies, and receipt hashes control selection.
