# Pipeline Flowchart Remediation Plan

## Purpose

Produce a truthful, readable map of the Phase 1A-11 pipeline and the folders
each phase reads, writes, publishes, or resolves. The map must distinguish the
workflow described in `PROJECT_OUTLINE.md` from the implementation reachable in
the current checkout.

This is a bounded documentation task. It does not authorize provider or network
access, protected-data reads or hashes, real evaluation, publication, trial
registration, candidate sealing, holdout access, installation, staging,
commit, push, deletion, or trading.

Canonical recall path:

`docs/PIPELINE_FLOWCHART_REMEDIATION_PLAN.md`

The plan is currently local and may remain untracked until staging and a local
commit are separately authorized.

## Deliverable

Create or update one file:

`PIPELINE_FOLDER_MAP.md`

It contains:

1. A compact Mermaid overview of the intended and current pipelines.
2. A plain Markdown table listing every phase/folder relationship.
3. A short evidence and limitations section.

No JSON inventory, JSON Schema, Python validator, preservation manifest,
Phase 7-9 evidence audit, or Phase 10/11 architecture decision is part of this
task. Those require separate plans only if the completed map reveals a concrete
need.

## Why the retained controls are sufficient

Only existing repository controls are retained:

- Read code and documentation without opening protected research artifacts.
  This prevents accidental result or authority disclosure while still allowing
  folder and adapter topology to be mapped.
- Check repository identity and the complete tracked/untracked status before
  and after editing. This prevents the documentation change from absorbing or
  overwriting unrelated work.
- Require source-code evidence for implemented edges. This prevents abandoned
  folders and historical files from being presented as live implementation.

A reviewed Markdown diff is sufficient for this documentation artifact. No new
policy framework or validation program is justified.

## Preflight

Before editing:

1. Verify repository root, branch, HEAD, governing `AGENTS.md`, and
   `git status --porcelain=v1 --untracked-files=all`.
2. Confirm whether `PIPELINE_FOLDER_MAP.md` already exists or contains user
   changes. Preserve unrelated work and stop if safe merging is unclear.
3. Record the current HEAD and UTC verification time in the map.
4. Use only current code, tests, workflow documents, and non-content path
   presence as evidence.
5. Classify every evidence file as `HEAD`, `WORKTREE_MODIFIED`, or
   `WORKTREE_UNTRACKED`. Record a SHA-256 only for modified or untracked
   non-protected evidence files.

Do not open or freshly hash data-release manifests, receipts, trial events,
authorization-use records, predictions, evaluations, reports containing real
results, or historical payloads. Filename or directory presence may support
only a path-topology statement. If protected contents are necessary, record
`BLOCKED_PROTECTED_READ` and leave that claim unresolved for a separately
approved task.

## Evidence rules

For every implemented edge:

- Identify a reachable producer or resolver in current code.
- Identify a reachable consumer when one is claimed.
- Cite the repository-relative code path and symbol or line.
- State the authority boundary for any real execution.

Folder presence alone never proves implementation, acceptance, payload
integrity, execution, or research validity. Handoff prose, branch names,
docstrings, and filenames are leads, not proof.

Current reachability requires a path from a public CLI entrypoint, a command
identified by `CURRENT_WORKFLOW.md`, or a named current preparation or approved
orchestration seam with a direct consumer. A standalone module is not current
merely because it exists under `src/`; classify it `UNREACHABLE_MODULE`,
`BESPOKE_EVIDENCE_SCRIPT`, `RETIRED`, or `UNKNOWN` as appropriate.

Use these implementation labels:

- `SYNTHETIC_ONLY`
- `IMPLEMENTED`
- `PREPARE_ONLY`
- `BESPOKE_EVIDENCE_SCRIPT`
- `UNREACHABLE_MODULE`
- `MISSING`
- `RETIRED`
- `UNKNOWN`

Use a separate authority label:

- `NO_PROTECTED_ACCESS`
- `FULLY_GUARDED`
- `UNSAFE_PREAPPROVAL_PROTECTED_ACCESS`
- `UNKNOWN_AUTHORITY_BOUNDARY`

`FULLY_GUARDED` is valid only when every protected manifest open, payload hash,
resolver, and row read is behind the required authorization. If a reachable
path opens protected metadata or hashes a protected payload before that
boundary, use `UNSAFE_PREAPPROVAL_PROTECTED_ACCESS`, even if later row reads are
guarded.

## Map format

### Mermaid overview

Use two clearly labeled subgraphs:

1. `Intended workflow` from `PROJECT_OUTLINE.md`.
2. `Current implementation` from reachable current code.

Keep the overview readable:

- Show phases and only their primary folder families.
- Use edge labels `READS`, `WRITES`, `PUBLISHES`, `RESOLVES`, or `CONCEPTUAL`.
- Show configs, manifests, state, trials, and authorization once in a small
  cross-cutting-controls group; do not connect every control to every phase.
- Put complete folder detail in the table, not in a web of arrows.
- If one diagram becomes crowded, use one small diagram per lane.

If an existing local Mermaid checker is already available, run it. Do not
install software or use the network merely to render the chart. Otherwise
manually inspect the code fence and state that rendering was not independently
tested.

### Phase/folder table

Include one row per phase/folder/operation tuple with these columns:

| Phase | Intended purpose | Implementation | Authority | Claim status | Operation | Path kind | Folder or path pattern | Owning code or command | Evidence | Limitation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

Cover all twelve ordered phase identifiers:

`1A`, `1B`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`, `10`, `11`.

Use separate rows when a phase reads and writes different folders. Distinguish
logical release families from physical active-view folders with one of these
path kinds: `LOGICAL_RELEASE`, `PHYSICAL_ACTIVE_VIEW`, `MANIFEST`, `REPORT`,
`STATE`, `STAGING`, `CONFIG`, or `RUNTIME`.

Label every material claim exactly `Verified`, `Inferred`, `Assumed`, or
`Not established` as required by `PROJECT_OUTLINE.md`.

## Required live checks

Revalidate rather than assume:

- Whether `futures-pipeline phase1a` through `phase11` remain synthetic-only.
- Which physical folders feed the current Phase 3 and Phase 4 adapters.
- Which exact columns and folders the current Phase 5 split planner reads.
- Whether `scripts/run_tier1_phase7_audit.py` remains a hard-coded bespoke
  evidence script rather than a general adapter.
- Which Phase 8 preparation, execution, report, and publication surfaces are
  reachable in the current checkout.
- Whether Phase 8 pinning opens manifests or hashes payloads before its opaque
  approval seam; label that path unsafe if confirmed statically.
- Whether Phase 9-11 surfaces are implemented, prepare-only, retired,
  conflicted, or missing.
- Whether any claimed current consumer discovers accepted artifacts by direct
  path or glob instead of a current manifest/receipt contract.

Do not inspect protected artifact contents to settle these questions. Mark an
unresolvable content-dependent question `BLOCKED_PROTECTED_READ`.

## Bounded search order

Inspect only what is needed for the map, in this order:

1. `PROJECT_OUTLINE.md`, `CURRENT_WORKFLOW.md`, and
   `docs/LEGACY_WORKFLOWS.md`.
2. Public pipeline entrypoints and commands.
3. Directly imported phase producers, resolvers, and consumers.
4. Direct tests that establish path or authority behavior.
5. One explicitly referenced legacy predecessor only when required to classify
   a current surface.

Do not reconstruct every historical research branch. Stop following a chain
when the next module has no current consumer or governing role.

## Completion states

`COMPLETE` requires:

- Both intended and current lanes are present.
- All twelve phase identifiers appear in the table.
- Every implemented edge cites reachable current code.
- Every folder relationship has an explicit operation.
- Implementation and authority are reported independently.
- Every evidence file identifies its `HEAD`, `WORKTREE_MODIFIED`, or
  `WORKTREE_UNTRACKED` basis; modified or untracked non-protected evidence also
  records its SHA-256.
- Every non-conceptual Mermaid edge maps to at least one table row, every
  primary table relationship appears in the overview, and phase statuses agree
  between both representations.
- Synthetic, historic, conceptual, unsafe, missing, and blocked paths are not
  presented as verified production implementation.
- The map records branch, HEAD, UTC verification time, limitations, and the
  protected-evidence boundary.
- `git diff --check` passes for tracked changes. Each untracked plan or map
  separately passes `git diff --no-index --check -- NUL <path>`; ordinary Git
  diff does not inspect untracked files. The final full status is reported.

Use `PARTIAL_WITH_BLOCKERS` only when a genuine protected-read boundary,
ambiguous ownership, overlapping user edit, or missing current contract blocks
a mandatory claim. List the unresolved phase, claim, evidence inspected, and
smallest next action. Never call a partial map complete.

Use `FAILED` for repository drift during the edit, accidental protected access,
overwritten user work, malformed Markdown, or a failed required check.
Repository drift means an unexpected change to repository root, branch, HEAD,
or an inspected evidence input. Planned edits to this plan and the map are not
drift. Recheck root, branch, HEAD, and all modified or untracked evidence inputs
before declaring completion.

## Final handoff

Report:

- Completion state.
- The exact map path.
- Checks performed and whether Mermaid rendering was tested.
- Any blocked or unsafe paths.
- Full tracked/untracked status for the plan and map.

If the plan or map remains untracked, state that it can be lost during cleanup,
branch replacement, or transfer. Staging and a local commit require separate
authorization for the exact paths. Never infer push authority.
