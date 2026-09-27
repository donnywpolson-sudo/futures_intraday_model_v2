"""Evidence-only recovery of the terminal annual DBN campaign; never runs markets.

Compatibility is limited to the exact reviewed historical source bytes. It does
not make historical admission checks equivalent or authorize another execution.
"""
from __future__ import annotations

import ast
import difflib
import json
import subprocess
from pathlib import Path

from .canonical import io_path, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError

CAMPAIGN = "a2cdf9109f969e5c026c7aec56c0a594cd176331e3b9e7694f8cd8d4a4c795e5"
CAMPAIGN_ROOT = "state/annual_dbn_gate_b_market_campaign_v1"
SCHEMA = "causal_market_binding_compatibility/1.0.0"
ORIGINAL_CERTIFICATE_IDS_SHA256 = "05b0453dcf15dfd4da5c15f47762873c958516745d68d69fb4dba3749caf3bd5"
MODULE = "src/futures_rebuild/causal_market_binding_compatibility.py"
CERTIFIER = "src/futures_rebuild/causal_observation_market_certification.py"
CHECKPOINT = "src/futures_rebuild/causal_observation_market_checkpoint.py"
POLICY = "src/futures_rebuild/research_gateway_policy.py"
OLD_CERTIFIER = "58ca6ab9b6c06deb3a46ca5271fe1c31a4099e9afe98c5c9b13841b64ad6509a"
FILE_CERTIFIER = "7d584a94780a668274c841fc9853e0edcfe25bc717b1ccc02c3a836966d63e93"
OLD_CHECKPOINT = "87b7a9e551eed92817f63397f30545c0fafad021d5d755ae1f69a24a65b6193c"
NEW_CHECKPOINT = "9b81240f62e15519cd9f02f355187ce19ccbc02f82508967fb726f3dda8083c5"
POLICY_DIGESTS = frozenset({
    "c75c993a2cca692379a9bae47d39e6276d6250fc96325c98adfcf4e5fbab9cfa",
    "5b3cc710834e25ff6ac83b4d41985d35dfc310b1a619d8f3c02924b202270686",
    "be63b7aa935d5fed6452e499a9fbe6a22be4979fbc02411428e813585e34f856",
    "8aa3d908b8dee938b86003c5f8e7433d2fca659deb635f8507c84388915a9c4c",
    "21bed9c09b20318a59524891efca46981fb9b2e84134041dd08c0de5fd97b6e1",
    "8230dbda67c02b2ee2a1ccfd3e5f0749f828cdc7140cdd0a72aef55b035caa22",
    "e1491ac74d28c1828e7a7bcc4bdd06e27408d0afbe3ef537dcc4278c7617dfca",
})


def _require(condition, message):
    if not condition:
        raise IntegrityError(message)


def _identity(value, field):
    _require(value.get(field) == sha256_json({k: v for k, v in value.items() if k != field}),
             f"invalid compatibility evidence identity: {field}")


def _path(root, relative):
    _require(isinstance(relative, str) and relative and "\\" not in relative
             and ":" not in relative and not relative.startswith("/")
             and all(p not in {"", ".", ".."} for p in relative.split("/")),
             "compatibility evidence path is not plain relative")
    path = root / relative
    _require(path.resolve().is_relative_to(root.resolve()), "compatibility path escapes repository")
    return io_path(path)


def _nodes(source, excluded=()):
    return [ast.dump(n, include_attributes=False) for n in ast.parse(source).body
            if getattr(n, "name", None) not in excluded]


def recover_snapshots(root: Path, saved=None):
    """Recover exact bytes; hash-matching reconstructed text is not rewritten evidence."""
    from .causal_observation_market_certification import CERTIFIER_BINDING_PATHS

    snapshots = dict(saved or {})
    def add(path, raw, origin):
        digest = sha256_bytes(raw)
        snapshots.setdefault(digest, {"path": path, "source": raw.decode("utf-8"), "origin": origin})

    for path in CERTIFIER_BINDING_PATHS:
        add(path, _path(root, path).read_bytes(), path)
    for path, revision in ((CERTIFIER, "613656016f23e0866a114a36c3a3a4a7e02a7961"),
                           (CHECKPOINT, "fde7d03b514c1696d7e8b23130ed2e551bbf431b")):
        raw = subprocess.check_output(["git", "show", f"{revision}:{path}"], cwd=root)
        add(path, raw, f"git:{revision}:{path}")
    base = "state/unpublished_evidence/l0_order_readiness_v1/"
    for relative in (
        "retrospective_screen_adoption_v1/originals/research_gateway_policy.py",
        "screen_value_audit_v1/original_research_gateway_policy.py",
        "screen_clock_diagnostic_v1/original_research_gateway_policy.py",
        "screen_clock_diagnostic_v1/replacement/proposed_research_gateway_policy.py",
    ):
        add(POLICY, _path(root, base + relative).read_bytes(), base + relative)
    original = base + "retrospective_screen_adoption_v1/originals/research_gateway_policy.py"
    raw = _path(root, original).read_bytes()
    branch = (b"    from . import l0_development_admission as l0\n"
              b"    if gateway_schema == l0.GATEWAY_SCHEMA:\n"
              b"        l0.validate_execution_scope(research_scope)\n        return\n")
    _require(raw.count(branch) == 1, "policy recovery basis changed")
    raw = raw.replace(branch, b"")
    add(POLICY, raw, original + "; remove the L0 admission branch")
    for operation in ("CENSUS_ROLLING_HOUR_V6_PRICE_READINESS_ONCE",
                      "DIAGNOSE_ROLLING_HOUR_PRICE_LINEAGE_FORMAT_ONCE",
                      "ASSESS_OWNED_ROLLING_HOUR_REFERENCE_INPUTS_ONCE"):
        line = f'        "{operation}",\n'.encode()
        _require(raw.count(line) == 1, "policy recovery operation changed")
        raw = raw.replace(line, b"")
    add(POLICY, raw, original + "; remove L0 admission branch and three rolling-hour operations")
    for digest, snapshot in snapshots.items():
        _require(sha256_bytes(snapshot["source"].encode()) == digest, "snapshot bytes differ from binding")
    return snapshots


def prove_bindings(bindings, snapshots):
    """Return scoped differences, or reject unreviewed/semantic source changes."""
    baseline = bindings[0]
    paths = set(baseline)
    _require(all(set(b) == paths for b in bindings), "binding path sets differ")
    varying = {p for p in paths if len({b[p] for b in bindings}) > 1}
    _require(varying <= {CERTIFIER, CHECKPOINT, POLICY}, "replay/data-contract bindings changed")
    for b in bindings:
        for path, digest in b.items():
            _require(digest in snapshots and snapshots[digest]["path"] == path,
                     f"historical snapshot unavailable: {path} {digest}")
        _require(b[CERTIFIER] in {OLD_CERTIFIER, FILE_CERTIFIER}, "unreviewed replay implementation")
        _require(b[CHECKPOINT] in {OLD_CHECKPOINT, NEW_CHECKPOINT}, "unreviewed checkpoint implementation")
        _require(b[POLICY] in POLICY_DIGESTS, "unreviewed gateway implementation")

    # Exact reviewed hashes cover the transport bodies; compare everything else.
    before = snapshots[OLD_CERTIFIER]["source"]
    after = snapshots[FILE_CERTIFIER]["source"]
    after_without_imports = after.replace("import os\n", "").replace("import tempfile\n", "")
    transport = {"_run_replay_in_fresh_process", "_replay_process_file_worker",
                 "_receive_replay_process_file_result"}
    _require(_nodes(before, transport) == _nodes(after_without_imports, transport),
             "certifier changed outside reviewed transport")
    # The success/failure check and ReplayEvidence reconstruction must also match.
    def tail(source):
        function = next(n for n in ast.parse(source).body if getattr(n, "name", None) == "_run_replay_in_fresh_process")
        return [ast.dump(n) for n in function.body[-5:]]
    _require(tail(before) == tail(after), "transport changed evidence acceptance")

    old = snapshots[OLD_CHECKPOINT]["source"]
    new = snapshots[NEW_CHECKPOINT]["source"]
    exception = ('        if relative in {\n'
                 '            "src/futures_rebuild/annual_dbn_causal_campaign.py",\n'
                 '            "src/futures_rebuild/causal_observation_market_checkpoint.py",\n'
                 '        }:\n            continue\n')
    _require(new.count(exception) == 1 and new.replace(exception, "") == old,
             "checkpoint changed outside the reviewed admission guard")
    # The certifier imports only this policy constant. No policy function is part
    # of _replay_once; admission changes are explicitly NOT granted equivalence.
    policy_constant = None
    for b in bindings:
        tree = ast.parse(snapshots[b[POLICY]]["source"])
        assignment = next(n for n in tree.body if isinstance(n, ast.Assign)
                          and any(isinstance(t, ast.Name) and t.id == "CAUSAL_OBSERVATION_FULL_BUILD_OPERATION" for t in n.targets))
        actual = ast.literal_eval(assignment.value)
        _require(actual == "BUILD_DEVELOPMENT_CAUSAL_OBSERVATION_FOUNDATION_ONCE", "causal operation changed")
        policy_constant = actual
    proof = {
        "unchanged_paths": sorted(paths - varying),
        "changed_paths": sorted(varying),
        "certifier": "TRANSPORT_ONLY_ALL_OTHER_AST_AND_EVIDENCE_ACCEPTANCE_IDENTICAL",
        "checkpoint": "ADMISSION_GUARD_CHANGED_REPLAY_SOURCE_SELECTION_AND_PRODUCER_UNCHANGED",
        "policy": "REPLAY_IMPORT_CONSTANT_IDENTICAL_OTHER_OPERATION_ADMISSION_NOT_CERTIFIED",
        "causal_operation": policy_constant,
        "scope": "EXISTING_COMPLETED_REPLAY_RESULTS_ONLY",
        "fresh_execution_authorized": False,
        "diffs": {},
    }
    for path in sorted(varying):
        digests = sorted({b[path] for b in bindings})
        reference = digests[0]
        for digest in digests[1:]:
            proof["diffs"][f"{path}:{reference}:{digest}"] = "".join(difflib.unified_diff(
                snapshots[reference]["source"].splitlines(True), snapshots[digest]["source"].splitlines(True),
                fromfile=reference, tofile=digest))
    return proof


def audit_campaign(root: Path, *, saved_snapshots=None):
    """Read receipts, plans and inventories only; no DBN or Parquet payload access."""
    from .causal_observation_market_certification import CERTIFIER_BINDING_PATHS
    from .causal_observation_market_checkpoint import MARKET_ORDER, checkpoint_set_identity

    root = root.resolve()
    references = {}
    def read(relative):
        path = _path(root, relative)
        raw = path.read_bytes()
        references[relative] = {"sha256": sha256_bytes(raw), "size": len(raw)}
        return json.loads(raw)

    prefix = f"{CAMPAIGN_ROOT}/{CAMPAIGN}"
    campaign = read(f"{prefix}/campaign_plan.json")
    _require(campaign["campaign_id"] == campaign["plan_id"] == CAMPAIGN
             == sha256_json({k: v for k, v in campaign.items() if k not in {"campaign_id", "plan_id"}}),
             "campaign identity differs")
    terminal = read(f"{prefix}/terminal.json")
    _identity(terminal, "terminal_id")
    _require(terminal["terminal_id"] == "0ea2d84fbf115fad152f6cd84cbdb2f521f36235f5cab36a428a85c0ef2abfc2"
             and terminal["completed_markets"] == list(MARKET_ORDER), "terminal campaign changed")
    active = campaign["active_catalog"]
    read(active["path"])
    _require(references[active["path"]]["sha256"] == active["sha256"], "active catalog changed")
    checkpoint_set = campaign["checkpoint_set"]
    set_id = checkpoint_set_identity(checkpoint_set)
    _require(set_id == campaign["checkpoint_set_id"] == terminal["checkpoint_set_id"], "checkpoint set differs")
    anchors = {item["market"]: item["certificate_id"] for item in campaign["certified_prefix"]}
    for path in sorted((root / prefix / "events").glob("*.json")):
        event = json.loads(path.read_bytes())
        if event.get("certificate_id") and event.get("market"):
            event = read(path.relative_to(root).as_posix())
            _require(event["market"] not in anchors or anchors[event["market"]] == event["certificate_id"],
                     "conflicting campaign certificate events")
            anchors[event["market"]] = event["certificate_id"]
    _require(set(anchors) == set(MARKET_ORDER), "campaign certificate anchors incomplete")
    _require(sha256_json([anchors[m] for m in MARKET_ORDER]) == ORIGINAL_CERTIFICATE_IDS_SHA256,
             "original campaign certificate identities changed")
    plan_index = {}
    for path in (root / CAMPAIGN_ROOT).glob("*/markets/*/*/certification_plan.json"):
        value = json.loads(path.read_bytes())
        plan_index[value["plan_id"]] = path.relative_to(root).as_posix()
    records, certificates, groups = [], [], {}
    for market in MARKET_ORDER:
        directory = root / "state/causal_observation_market_certificates" / set_id / market
        candidates = []
        for path in directory.glob("*.json"):
            if not path.name.endswith(".failure.json"):
                value = json.loads(path.read_bytes())
                if value.get("certificate_id") == anchors[market]:
                    candidates.append(path.relative_to(root).as_posix())
        _require(len(candidates) == 1, f"missing/ambiguous anchored certificate: {market}")
        certificate = read(candidates[0])
        _identity(certificate, "certificate_id")
        evidence = certificate["replay_evidence"]
        _identity(evidence, "evidence_id")
        plan_path = plan_index[certificate["certification_plan_id"]]
        plan = read(plan_path)
        _identity(plan, "plan_id")
        bindings = plan["certifier_implementation_bindings"]
        binding_id = sha256_json(bindings)
        _require(set(bindings) == set(CERTIFIER_BINDING_PATHS)
                 and binding_id == plan["certifier_implementation_bindings_sha256"]
                 == certificate["certifier_implementation_bindings_sha256"]
                 and bindings[CERTIFIER] == certificate["certifier_implementation_sha256"], "certificate binding differs")
        build = read(plan["build_plan_path"])
        _identity(build, "plan_id")
        checkpoint = read(plan["checkpoint_path"])
        _identity(checkpoint, "result_id")
        inventory = read(build["source"]["inventory_path"])
        entries = sorted((e for e in inventory["entries"] if e.get("market") == market), key=lambda e: e["path"])
        dbns = [e for e in entries if e["kind"] == "DBN"]
        _require(references[plan["build_plan_path"]]["sha256"] == plan["build_plan_sha256"]
                 and references[plan["checkpoint_path"]]["sha256"] == certificate["checkpoint_sha256"]
                 and references[build["source"]["inventory_path"]]["sha256"] == build["source"]["inventory_sha256"]
                 and build["plan_id"] == certificate["build_plan_id"]
                 and checkpoint["result_id"] == certificate["checkpoint_result_id"]
                 and checkpoint["status"] == "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE"
                 and checkpoint["partition_inventory_sha256"] == sha256_json(checkpoint["partitions"])
                 and checkpoint["partition_count"] == len(checkpoint["partitions"]) == evidence["partition_count"]
                 and checkpoint["output_bytes"] == evidence["output_bytes"]
                 and plan["source"] == build["source"]
                 and sha256_json(entries) == build["source"]["exact_source_entries_sha256"]
                 and sha256_json(dbns) == build["source"]["exact_dbn_entries_sha256"],
                 f"checkpoint/source chain differs: {market}")
        _require(plan["checkpoint_set"] == build["checkpoint_set"] == checkpoint_set
                 and plan["target_market"] == build["target_market"] == checkpoint["target_market"] == certificate["market"] == market
                 and plan["attempt_id"] == build["attempt_id"] == checkpoint["attempt_id"] == certificate["attempt_id"],
                 f"market identity chain differs: {market}")
        groups.setdefault(binding_id, {"bindings": bindings, "markets": []})["markets"].append(market)
        records.append({"market": market, "certificate_path": candidates[0], "certificate_id": certificate["certificate_id"],
                        "certification_plan_path": plan_path, "binding_id": binding_id,
                        "replay_evidence_id": evidence["evidence_id"], "partition_count": evidence["partition_count"],
                        "observation_count": evidence["observation_count"], "output_bytes": evidence["output_bytes"],
                        "output_inventory_sha256": evidence["output_inventory_sha256"]})
        certificates.append(certificate)
    snapshots = recover_snapshots(root, saved_snapshots)
    used = {digest for group in groups.values() for digest in group["bindings"].values()}
    snapshots = {h: snapshots[h] for h in sorted(used) if h in snapshots}
    proof = prove_bindings([g["bindings"] for g in groups.values()], snapshots)
    # Permit only the explicit aggregate-validator extension in today's certifier.
    _require(_nodes(_path(root, CERTIFIER).read_text(encoding="utf-8"), {"certify_complete_market_certificate_set"})
             == _nodes(snapshots[FILE_CERTIFIER]["source"], {"certify_complete_market_certificate_set"}),
             "current certifier changed outside aggregate validation")
    current = {p: sha256_file(_path(root, p)) for p in CERTIFIER_BINDING_PATHS}
    for path, digest in current.items():
        if path != CERTIFIER:
            _require(digest in {g["bindings"][path] for g in groups.values()}, f"current bound implementation drifted: {path}")
    core = {"schema_version": SCHEMA, "status": "PASS_EXISTING_REPLAY_BINDING_COMPATIBILITY",
            "campaign_id": CAMPAIGN, "terminal_id": terminal["terminal_id"], "checkpoint_set": checkpoint_set,
            "checkpoint_set_id": set_id, "markets": records, "binding_groups": groups,
            "snapshots": snapshots, "proof": proof, "preserved_artifacts": references,
            "current_implementation_bindings": {**current, MODULE: sha256_file(root / MODULE)},
            "recertification_required_markets": [], "market_rebuilds": 0,
            "dbn_payloads_opened": 0, "parquet_payloads_opened": 0,
            "publication_authorized": False, "activation_authorized": False,
            "fresh_execution_authorized": False}
    return {**core, "manifest_id": sha256_json(core)}, certificates


def validate_compatibility_manifest(root, manifest, checkpoint_set, certificates):
    """Recompute the audit, not merely a caller-provided compatibility assertion."""
    _identity(manifest, "manifest_id")
    actual, original = audit_campaign(root, saved_snapshots=manifest.get("snapshots"))
    _require(manifest == actual and checkpoint_set == actual["checkpoint_set"], "compatibility manifest is stale or differs")
    _require(len(certificates) == len(original)
             and {c["market"]: c["certificate_id"] for c in certificates}
             == {c["market"]: c["certificate_id"] for c in original}, "certificate set differs from audited campaign")
    return manifest["manifest_id"]


def verify_output_metadata(root, certificates):
    """Check every schema/footer and exact checkpoint coverage without decoding rows."""
    import pyarrow.parquet as pq
    from .causal_observation_parquet import FILENAMES, SCHEMAS

    records = []
    for certificate in certificates:
        evidence = certificate["replay_evidence"]
        by_partition = {}
        observation_count = 0
        for item in evidence["output_inventory"]:
            path = _path(root, item["path"])
            name = path.stem
            _require(name in FILENAMES and path.name == FILENAMES[name], "unknown output table")
            with pq.ParquetFile(path) as parquet:
                _require(parquet.schema_arrow.equals(SCHEMAS[name], check_metadata=True), "output schema differs")
                rows = parquet.metadata.num_rows
            parent = item["path"].rsplit("/", 1)[0]
            partition = by_partition.setdefault(parent, {})
            _require(name not in partition, "duplicate output table")
            partition[name] = rows
            if name == "observations":
                observation_count += rows
        _require(len(by_partition) == evidence["partition_count"]
                 and observation_count == evidence["observation_count"], "output row/partition totals differ")
        for counts in by_partition.values():
            _require(set(counts) == set(FILENAMES)
                     and all(counts[n] == counts["observations"] for n in ("roll", "quality")),
                     "output table coverage or aligned row counts differ")
        checkpoint_path = (f"data/causally_gated_normalized/v10/{certificate['checkpoint_set_id']}/"
                           f"{certificate['market']}/{certificate['attempt_id']}/market_checkpoint.json")
        checkpoint = json.loads(_path(root, checkpoint_path).read_bytes())
        _require(set(by_partition) == {p["stage"] + "/candidate" for p in checkpoint["partitions"]},
                 "output partition coverage differs from checkpoint")
        records.append({"market": certificate["market"], "partition_count": len(by_partition),
                        "observation_count": observation_count, "footer_counts_sha256": sha256_json(by_partition)})
    return {"status": "PASS_ALL_OUTPUT_SCHEMAS_ROWS_AND_CHECKPOINT_COVERAGE",
            "markets": records, "market_count": len(records), "row_values_decoded": 0,
            "observation_count": sum(r["observation_count"] for r in records),
            "partition_count": sum(r["partition_count"] for r in records)}
