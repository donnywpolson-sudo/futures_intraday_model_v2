# Data layout v2

Layout v2 makes phase-specific paths the canonical home of every data payload.
Consumers identify a release through its tracked central manifest; they must not
scan a data directory or infer a release from filenames.

## Canonical roots

The exact logical and physical path templates are declared in
`configs/data_layout_contract.json`. Payloads live only under these roots:

- `data/dbn`
- `data/raw`
- `data/causally_gated_normalized`
- `data/reference`
- `data/market_state`
- `data/status_eligibility`
- `data/features`
- `data/outcome_sources`
- `data/outcomes`
- `data/predictions`
- `data/evaluations`

DBN payloads use their logical paths directly, so their files live at
`data/dbn/{family}/{market}/{year}/{filename}`. Other physical payload paths
retain the full 64-character release ID in the directory immediately before
the filename. Release manifests live at
`manifests/data_releases/{phase}/{release-id}.json`. Data publication stages
live at `state/data_publication_staging/{operation-id}`.

The canonical DBN release was copied into the flat layout and then completed a
separately approved destructive cutover. The 3,777 retained release-ID
directories and their 8,040 verified duplicates were removed. The direct flat
files are now the only DBN payloads under `data/dbn`.

Operational state remains under `state`, and model artifacts remain under
`bundles`. Neither is a data payload root.

## Commit and recovery rule

The requested hierarchy distributes one release across multiple market/year
directories, so the whole release cannot be promoted with one directory rename.
Publication instead uses these transaction rules:

1. Hash every staged file and build the release ID from logical paths and bytes.
2. Durably write an exact publication intent.
3. Promote each release-ID directory entry without replacing existing bytes.
4. Write the central manifest last. The manifest is the only commit marker.
5. Ignore promoted files until that manifest exists. A retry completes an exact
   intent after a crash and rejects any conflicting byte.

This preserves atomic visibility even though the physical files have multiple
parents. The closure verifier rejects orphaned, overlapping, linked, missing,
or unexpected data files.

For the flat DBN phase, an existing filename is immutable. A later publication
must use a new non-colliding filename; publication never replaces accepted
bytes. The controlled flat-layout copy is separately plan-, approval-, code-,
contract-, manifest-, inventory-, and hash-bound under
`configs/dbn_flat_layout_migration_*.json`. Its completion receipt lives under
`manifests/data_layout_transitions`. The later deletion is independently bound
by `configs/dbn_flat_layout_cutover_*.json`; its completion receipt lives under
`manifests/data_layout_cutovers`. The original copy receipt remains unchanged
as historical evidence.

## Phase 1A migration gate

The original vault has an external copy-only archive. Its full file inventory
and hashes are embedded in a tracked migration manifest. Phase 1A then uses two
additional tracked artifacts:

- `configs/data_layout_migration_plan.json`
- `configs/data_layout_migration_approval.json`

The plan binds the archive receipt, accepted snapshot receipt, exact DBN
inventory, layout contract, implementation files, destination template, byte
and file limits, and staging scope. The generated approval remains
`PENDING_APPROVAL`. Repository code does not convert it to `APPROVED`.

After an explicit user approval is encoded with its UTC approval time, user
authorization ID, and exact content hash, the copy command can run. It copies
only verified DBNs and their sidecars. It does not copy legacy raw/causal files,
call a provider, evaluate history, overwrite bytes, or remove `data/vault`.

`data/vault` remains audit evidence until layout-v2 regeneration, foundation
closure, archive re-verification, and a separate destructive-cutover approval
are complete.
