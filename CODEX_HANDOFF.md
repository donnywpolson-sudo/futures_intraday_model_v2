# Codex handoff

## Current state

- Repository: `C:\Users\donny\Desktop\futures_intraday_model_v2`.
- Status: `REBUILD_IN_PROGRESS`; Phase 1A layout-v2 DBN publication is complete.
- Canonical DBN release: `9e5a9f2a405e50b0cda6702b67506b0951b057500781d37c45171da3967e9b51`.
- Manifest: `manifests/data_releases/dbn/9e5a9f2a405e50b0cda6702b67506b0951b057500781d37c45171da3967e9b51.json`.
- Receipt ID: `5dd1d20b7dc9cba02ed8796532a0f54e24b256745f425189c8d4683df2246c96`.
- Independently reverified census: 4,020 DBNs, 4,020 sidecars, 8,040 total files, 25,007,876,004 bytes.
- Controlled flat-layout migration is complete. DBN consumers now resolve files at `data/dbn/{family}/{market}/{year}/{filename}`.
- Flat-layout receipt: `manifests/data_layout_transitions/a41d87b4732537326388ec9838e6b7f3303d6f01753a73b5e4add117f758fefa.json`; receipt ID `80c8ea483afe8b1ec1da426447e2037639dad4d85f198e0986d837cc39c3fb65`.
- The DBN flat-only cutover is complete. Exactly 8,040 verified retained files in 3,777 release-ID directories were removed; the 8,040 direct files remain the sole DBN payloads under `data/dbn`.
- Cutover receipt: `manifests/data_layout_cutovers/65a2bb8e077ca0f79cd3715adca9e1013c368d3de3bf22ea81fc5e4f30873666.json`; receipt ID `ded2ea4e00c9ee325bb19f09020ca31ac6e78006d158224a8383094523bc7840`.
- Post-cutover production closure passed under physical-layout revision `2.2.0`: 8,040 managed data files and two valid data-release manifests, with no retained release-ID directory.
- The original `data/vault` remains intact as audit evidence. Do not delete it without the later separate destructive-cutover approval.

The full vault also has an independently hash-verified external copy at
`C:\Users\donny\Desktop\futures_intraday_model_v2_data_archive\vault_6dc18d3104e37cb1bd65e5387b7a7a92f851d0e3ea571baa3157349872f5a872`.
Its tracked archive receipt is
`manifests/data_releases/migration/9fa55b84cc66ee7d39b6906a39d11a3b77767bb5cfa02678885fd8e3fad051d8.json`.

## Layout-v2 authority

- `configs/data_layout_contract.json` is the active layout contract.
- DBN is the one declared flat physical phase. All other data phases retain release-ID directories unless a later separately controlled migration changes their contract.
- `configs/dbn_flat_layout_migration_plan.json` and `configs/dbn_flat_layout_migration_approval.json` bind the completed copy-only transition.
- `configs/dbn_flat_layout_cutover_plan.json` and `configs/dbn_flat_layout_cutover_approval.json` bind the completed retained-copy deletion; `data/vault` and the external archive were outside its scope and remain intact.
- Payload manifests live at `manifests/data_releases/{phase}/{release-id}.json`.
- Publication stages live at `state/data_publication_staging/{operation-id}`.
- Consumers must resolve data through a verified layout-v2 manifest; do not recursively discover files or use the old snapshot root as a source interface.
- `src/futures_rebuild/data_layout.py` implements manifest v2, deterministic release IDs, commit-last publication, durable recovery intents, receipts, and tree closure.
- New layout-v1 `data/vault/releases` publication fails closed while the layout-v2 contract is active.
- Existing layout-v1 checkpoints and receipts are audit evidence only and cannot authorize or resume layout-v2 work.

## Next dependency-ordered work: Review trading pipeline Step 2

1. Reconcile the current dirty worktree. Preserve all existing changes; do not reset, stash, overwrite, stage, commit, or push.
2. Replace DBN catalog and foundation CLI source-snapshot inputs with the verified DBN manifest/release ID above.
3. Route Phase 1B raw publication to `data/raw/{market}/{year}/{interval}/{release-id}`; raw was not part of the DBN flat-layout authorization.
4. Route Phase 2 publication to `data/causally_gated_normalized/{market}/{year}/{interval}/{release-id}`.
5. Route reference, market-state, eligibility, feature, outcome-source, outcome, prediction, and evaluation payloads through their exact layout-v2 paths.
6. Embed policies, selections, interval receipts, foundation sets, readiness evidence, and other control documents in central manifests rather than generic data directories.
7. Move prediction payloads to sealed `data/predictions` releases; retain only locks, anchors, heads, and append intents under `state`.
8. Reject all layout-v1 foundation checkpoint resumes and use layout-v2 staging/checkpoint schemas.
9. Regenerate Phase 1B, Phase 2, and all downstream foundation artifacts from the verified DBN manifest. Do not migrate legacy raw/causal parquet or incomplete old staging outputs.
10. Run targeted tests, the affected suite, full validation, coverage/reproducibility/isolation/readiness gates, and layout closure.
11. Publish the immutable foundation manifest and reach `CUTOVER_READY`. Stop for separate destructive-cutover approval before removing `data/vault`.

## Safety boundary

- No provider calls or paid downloads.
- No real-history alpha/WFA/OOS evaluation.
- No candidate sealing, order placement, live/shadow trading, remote push, or alpha claim.
- Legacy repositories remain read-only evidence.
- Corrections create successor releases; never overwrite accepted bytes.
- The current Python environment has `databento==0.79.0` and `databento-dbn==0.59.0`, while the repository pins `0.78.0` and `0.58.0`. Decoder-dependent tests correctly fail closed until the pinned environment is restored; do not loosen the version checks.
