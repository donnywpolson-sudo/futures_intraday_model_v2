from copy import deepcopy
import json
from pathlib import Path
from queue import Queue

import pytest

from futures_rebuild import causal_market_binding_compatibility as compatibility
from futures_rebuild import causal_observation_market_certification as certification
from futures_rebuild.canonical import sha256_bytes, sha256_json
from futures_rebuild.errors import IntegrityError
from tests.test_causal_observation_market_certification import _checkpoint_set, _market_certificate, _evidence


def _sign(value, field):
    value = {k: v for k, v in value.items() if k != field}
    return {**value, field: sha256_json(value)}


def _synthetic_set(root):
    certifier = root / compatibility.CERTIFIER
    certifier.parent.mkdir(parents=True)
    certifier.write_bytes(b"# synthetic certifier\n")
    output = root / "state/synthetic"
    output.mkdir(parents=True)
    certificates = []
    for index, market in enumerate(certification.MARKET_ORDER):
        raw = market.encode()
        (output / f"{market}.parquet").write_bytes(raw)
        certificate = _market_certificate(market, sha256=sha256_bytes(raw), size=len(raw),
                                          certifier_sha256=sha256_bytes(certifier.read_bytes()))
        certificate["certifier_implementation_bindings_sha256"] = str(index % 2) * 64
        certificates.append(_sign(certificate, "certificate_id"))
    manifest = _sign({"schema_version": compatibility.SCHEMA, "checkpoint_set": _checkpoint_set(),
                      "snapshots": {}, "terminal_id": "a" * 64}, "manifest_id")
    return certificates, manifest


def test_explicit_compatible_set_preserves_ids_and_rejects_tampering(tmp_path, monkeypatch):
    certificates, manifest = _synthetic_set(tmp_path)
    # Only the expensive audit is replaced with synthetic evidence. The manifest
    # equality/identity, market-set guard and original aggregate validator run.
    monkeypatch.setattr(compatibility, "audit_campaign", lambda *a, **kw: (deepcopy(manifest), deepcopy(certificates)))
    kwargs = dict(repository_root=tmp_path, checkpoint_set=_checkpoint_set(), market_certificates=certificates)
    with pytest.raises(IntegrityError, match="incompatible certifier bindings"):
        certification.certify_complete_market_certificate_set(**kwargs)
    result = certification.certify_complete_market_certificate_set(**kwargs, compatibility_manifest=manifest)
    assert result["market_certificate_ids"] == [c["certificate_id"] for c in certificates]
    assert result["compatibility_manifest_id"] == manifest["manifest_id"]
    assert result["market_rebuilds"] == result["additional_replay_passes"] == 0
    assert result["activation_authorized"] is result["publication_authorized"] is False
    altered = _sign({**manifest, "terminal_id": "b" * 64}, "manifest_id")
    with pytest.raises(IntegrityError, match="stale or differs"):
        certification.certify_complete_market_certificate_set(**kwargs, compatibility_manifest=altered)
    with pytest.raises(IntegrityError, match="audited campaign"):
        certification.certify_complete_market_certificate_set(**{**kwargs, "market_certificates": certificates[:-1]}, compatibility_manifest=manifest)
    # Altered content with the original ID must still fail the original checks.
    tampered = deepcopy(certificates)
    tampered[0]["publication_authorized"] = True
    with pytest.raises(IntegrityError, match="invalid or incompatible"):
        certification.certify_complete_market_certificate_set(**{**kwargs, "market_certificates": tampered}, compatibility_manifest=manifest)
    path = tmp_path / "state/synthetic/ES.parquet"
    path.write_bytes(b"xx")  # Same byte count, wrong hash.
    with pytest.raises(IntegrityError, match="output file differs"):
        certification.certify_complete_market_certificate_set(**kwargs, compatibility_manifest=manifest)


def test_unlisted_and_missing_snapshots_fail_closed():
    binding = {compatibility.CERTIFIER: "a" * 64, compatibility.CHECKPOINT: "b" * 64,
               compatibility.POLICY: "c" * 64, "decoder.py": "d" * 64}
    with pytest.raises(IntegrityError, match="snapshot unavailable"):
        compatibility.prove_bindings([binding], {})
    with pytest.raises(IntegrityError, match="data-contract bindings changed"):
        compatibility.prove_bindings([binding, {**binding, "decoder.py": "e" * 64}], {})
    snapshots = {h: {"path": p, "source": ""} for p, h in binding.items()}
    with pytest.raises(IntegrityError, match="unreviewed replay implementation"):
        compatibility.prove_bindings([binding], snapshots)


def test_footer_validation_allows_multiple_cadence_comparisons(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import pyarrow.parquet as pq
    from futures_rebuild.causal_observation_parquet import SCHEMAS, FILENAMES

    stage = "data/causally_gated_normalized/v10/set/ES/attempt/sealed/2010/month"
    checkpoint = tmp_path / "data/causally_gated_normalized/v10/set/ES/attempt/market_checkpoint.json"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_text(json.dumps({"partitions": [{"stage": stage}]}))
    counts = {"observations": 10, "roll": 10, "quality": 10, "missingness": 12, "cadence": 15}

    class Footer:
        def __init__(self, path):
            self.schema_arrow = SCHEMAS[path.stem]
            self.metadata = SimpleNamespace(num_rows=counts[path.stem])
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

    monkeypatch.setattr(pq, "ParquetFile", Footer)
    certificate = {"market": "ES", "checkpoint_set_id": "set", "attempt_id": "attempt",
                   "replay_evidence": {"partition_count": 1, "observation_count": 10,
                                       "output_inventory": [{"path": stage + "/candidate/" + n} for n in FILENAMES.values()]}}
    assert compatibility.verify_output_metadata(tmp_path, [certificate])["observation_count"] == 10
    counts["quality"] = 9
    with pytest.raises(IntegrityError, match="aligned row counts"):
        compatibility.verify_output_metadata(tmp_path, [certificate])


@pytest.mark.parametrize("failure", [False, True])
def test_queue_and_file_transport_preserve_same_canonical_evidence(tmp_path, monkeypatch, failure):
    def replay(*args):
        if failure:
            raise IntegrityError("synthetic replay failure")
        return _evidence()
    monkeypatch.setattr(certification, "_replay_once", replay)
    queue = Queue()
    certification._replay_process_worker(str(tmp_path), {}, {}, queue)
    old_status, old_payload = queue.get_nowait()
    result = tmp_path / "result.json"
    certification._replay_process_file_worker(str(tmp_path), {}, {}, str(result))
    envelope = json.loads(result.read_bytes())
    assert envelope["status"] == old_status
    assert sha256_json(envelope["payload"]) == sha256_json(old_payload)
    assert old_status == ("FAIL" if failure else "PASS")
    assert not Path(str(result) + ".tmp").exists()
