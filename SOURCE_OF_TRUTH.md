# Repository source of truth

> **Generated navigation view.** This file is deterministically rendered from
> `configs/repository_surface.json` and `pyproject.toml`. Do not maintain
> repository roles independently in this file.

## Purpose and authority

- `CURRENT_WORKFLOW.md` controls normal day-to-day work.
- `AGENTS.md` contains durable safety and research-integrity policy.
- `configs/repository_surface.json` is the canonical machine-readable path-role registry.
- `SOURCE_OF_TRUTH.md` is this generated navigation view only; it is not a workflow or safety authority.
- `pyproject.toml` defines the public package and command surface.
- `README.md` provides setup and operator orientation.
- `PROJECT_OUTLINE.md` is the detailed research runbook.
- `PIPELINE_FOLDER_MAP.md` is a topology and reference guide, not authority.
- `docs/LEGACY_WORKFLOWS.md` classifies retired workflow material.
- `MASTER_AUDIT.md` and `META_MASTER_AUDIT.md` are audit specifications, not current-state dashboards.
- `CODEX_HANDOFF.md` is continuation context only and grants no authority.
- `PUBLIC_SNAPSHOT.md` is not an operational workflow authority.

## Start here

1. Read `CURRENT_WORKFLOW.md` for normal work.
2. Read `AGENTS.md` for durable safety policy.
3. Use `SOURCE_OF_TRUTH.md` to navigate repository roles.
4. Use `README.md` for setup.
5. Use `PROJECT_OUTLINE.md` for the detailed research process.

## Active machine-readable pointers

| Role | Registry path |
| --- | --- |
| Standard Alpha pointer | `configs/active_alpha_research_ladder.json` |
| Standard active data catalog | `data/active/catalog.json` |
| Micro source-selection pointer | `configs/active_micro_alpha_research_ladder.json` |
| Micro source catalog | `data/active/catalogs/apex_micro.json` |

Local-only pointers or catalogs may be absent from a clean provider-free source export.
The micro pointer and catalog establish source selection only. They do not by themselves establish a frozen mechanism, trial registration, historical-row authority, research passage, holdout authority, production readiness, live-execution readiness, provider authorization, or trading authority. Rendering and validation do not open referenced market-data payloads.

## Public commands

The exact public command mapping comes from `[project.scripts]` in `pyproject.toml`.

| Command | Python target |
| --- | --- |
| `futures-dbn-catalog` | `futures_rebuild.dbn_catalog:main` |
| `futures-high-risk-prepare` | `futures_rebuild.high_risk:main` |
| `futures-master-audit` | `futures_rebuild.audit.__main__:main` |
| `futures-meta-audit` | `futures_rebuild.meta_audit:main` |
| `futures-pipeline` | `futures_rebuild.pipeline:main` |
| `futures-readiness` | `futures_rebuild.readiness:main` |
| `futures-retirement-audit` | `futures_rebuild.retirement:main` |

Private helpers, documentation-only commands, ignored installation candidates, historical commands, and untracked execution-looking modules are not public commands.

## Major folder roles

These concise roles come from each exact registry classification and note. More-specific entries override the family role.

| Family | Classification | Registry-derived role |
| --- | --- | --- |
| `src/` | `CURRENT_OPERATIONAL` | Package source root; descendant currentness is refined by exact and family entries. |
| `configs/` | `CURRENT_SUPPORTING` | Mixed control root whose more-specific entries determine current, prepared, historical, and secret roles. |
| `data/` | `CURRENT_SUPPORTING` | Protected mixed source, release, evidence, and active-selection root; payload contents are outside this registry validator. |
| `manifests/` | `HISTORICAL_HASH_BOUND` | Tracked and ignored manifests bind releases, attempts, paths, and bytes. |
| `reports/` | `HISTORICAL_HASH_BOUND` | Reports mix tracked evidence, ignored diagnostics, and generated output; no family-wide cleanup applies. |
| `state/` | `HISTORICAL_HASH_BOUND` | Mixed registries, receipts, immutable evidence, staging, runtime state, and placeholders. |
| `scripts/` | `CURRENT_SUPPORTING` | Mixed current helpers, prepare-only interfaces, and hash-bound historical command surfaces. |
| `tests/` | `CURRENT_SUPPORTING` | Provider-free current, high-risk, legacy, and local-evidence lanes coexist under explicit selection. |
| `docs/` | `CURRENT_SUPPORTING` | Current supporting documentation plus explicitly classified historical guidance. |
| `FuturesLiveCockpit/` | `MIXED_PACKAGING_SOURCE_OUTPUT` | Tracked packaging inputs coexist with ignored executable and onedir runtime output; never apply a directory-wide cleanup rule. |
| `build/` | `UNRESOLVED_MANUAL_REVIEW` | Local build output may contain a current candidate, rollback material, diagnostics, or interrupted work; exact review is required. |
| `dist/` | `GENERATED_OUTPUT` | Distribution output may be absent or generated; any future contents require exact revalidation. |
| `tmp/` | `UNRESOLVED_MANUAL_REVIEW` | Contains reference downloads and possible only local copies, not a proven cache. |
| `artifacts/` | `GENERATED_OUTPUT` | Build intermediates and diagnostics may be reproducible in principle, but current dependency and rollback closure is absent. |
| `bundles/` | `GENERATED_OUTPUT` | Tracked placeholder with possible ignored bundles; no automatic cleanup treatment. |

`FuturesLiveCockpit/` is a mixed packaging source/output surface and is not automatically disposable. Build, distribution, temporary, artifact, log, package, backup, and generated-report material still requires exact classification and review.

## Historical and retired material

Tracked does not imply current, and ignored does not imply disposable. Exact historical paths may remain at their original locations because plans, manifests, tests, receipts, reports, or other evidence bind their names or bytes. Historical material is not current workflow or command authority. A replacement must be explicitly declared with `current_replacement`; physical relocation requires separate reference and hash closure.

## Generated, local-only, and unresolved material

- `GENERATED_OUTPUT`: produced material whose existence does not provide deletion authority.
- `REGENERABLE_CACHE`: a narrowly identified cache with understood regeneration; cleanup still needs a fresh census and separate approval.
- `LOCAL_RUNTIME_STATE`: machine-local operating state that must be preserved unless separately governed.
- `LOCAL_SECRET`: credential or secret material whose contents must never be inspected or reported by this view.
- `MIXED_PACKAGING_SOURCE_OUTPUT`: tracked packaging inputs and generated output coexist under one family.
- `UNRESOLVED_MANUAL_REVIEW`: evidence is insufficient for an automatic decision. The registry currently contains 14 such entries.
- `PREPARED_NOT_EXECUTED`: a plan or preparation exists, but execution, activation, or publication is not established.

Use `configs/repository_surface.json` for exact classifications. A present, ignored, or untracked file that looks like execution, publication, activation, installation, or cleanup code does not become current merely because it exists.

## Cleanup and deletion rules

The registry grants no deletion authority, and `SOURCE_OF_TRUTH.md` grants no deletion authority. Only exact regenerable cache paths may become cleanup candidates, after a fresh machine-local census and separate exact approval. Modified, staged, and non-ignored untracked work is preserved by default.

Active catalogs, data, manifests, reports, state, receipts, authorization uses, credentials, and unpublished evidence are protected. Build output, distributions, `.venv`, temporary material, artifacts, packages, backups, logs, and reports are not automatically disposable. Git ignore status does not establish deletion safety, and broad cleanup commands must not be used.

Prohibited broad cleanup commands:

```text
git clean -fdx
git clean -fdX
```

## Supersession rules

Currentness is not determined by the highest version number, newest modification timestamp, newest-looking filename, tracked status, ignored status, directory presence, or words such as final, authoritative, successor, current, active, live, old, retired, or legacy.

Resolve currentness through `CURRENT_WORKFLOW.md`, `AGENTS.md`, `configs/repository_surface.json`, exact active pointers, explicit `current_replacement` relationships, `pyproject.toml` public command definitions, and current fail-closed policy boundaries.

## What this document does not authorize

Neither `SOURCE_OF_TRUTH.md` nor the registry authorizes deletion, movement or renaming, provider access, credential access, market-data reads, real-history research, holdout or forward access, prediction publication, candidate sealing, active-data mutation, publication, installation, activation, live smoke, trading, order placement, staging, commit, or push.
