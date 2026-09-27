import json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild import rolling_hour_lineage_diagnostic as d
from futures_rebuild.boundary import OperationReceipt, OperationClassification, RepoBoundary
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import UnauthorizedOperation


@pytest.mark.current
def test_format_counts_keep_types_lengths_and_no_values():
    values = [None, "a"*64, b"a"*64, "A"*64, "versioned-id", "", 5, "a"*32]
    counts = d.format_counts(values)
    assert counts == {"rows":8,"nulls":1,"types":{"NoneType":1,"str":5,"bytes":1,"int":1},
        "lengths":{"64":3,"12":1,"0":1,"32":1},
        "formats":{"null":1,"lowercase_hex":3,"uppercase_or_mixed_hex":1,"non_hex_or_empty":2,"non_text_type":1}}
    assert "versioned-id" not in json.dumps(counts)


@pytest.mark.current
def test_synthetic_integration_and_authorization(tmp_path, monkeypatch):
    root = tmp_path
    (root/"data/active").mkdir(parents=True)
    (root/"data/active/catalog.json").write_text("{}")
    policy = root/"src/futures_rebuild/research_gateway_policy.py"
    policy.parent.mkdir(parents=True)
    policy.write_text("synthetic")
    (root/d.PLAN).parent.mkdir(parents=True)
    (root/d.PLAN).write_text("{}")
    columns = ["source_release_id", "source_contract_id", "source_file_sha256", "source_row_sha256",
               "definition_source_file_sha256", "definition_row_sha256"]
    path = root/"data/active/synthetic.parquet"
    pq.write_table(pa.table({k:["a"*64, "prefix", None] for k in columns}),path)
    manifest = path.with_suffix(".json")
    manifest.write_text("{}")
    source = {"market":"ES","year":2018,"row_count":3,"physical_bytes":path.stat().st_size,
        "parquet_path":path.relative_to(root).as_posix(),"parquet_sha256":sha256_file(path),
        "sidecar_path":manifest.relative_to(root).as_posix(),"sidecar_sha256":sha256_file(manifest)}
    plan = {"purpose":"SYNTHETIC","exact_sources":[source],"columns":columns,"row_limit":3,
        "hard_stop_minutes":1,"working_memory_bytes_max":2**31,"estimated_output_bytes_max":2**26,
        "output_directory":"evidence"}
    monkeypatch.setattr(d,"load_plan",lambda root:({"readiness_preparation":{
        "price_catalog_sha256":sha256_file(root/"data/active/catalog.json")}},plan))
    calls = []

    def resolve(**args):
        calls.append(args)
        assert args['purpose'] == 'LINEAGE_AUDIT'
        return path

    monkeypatch.setattr(d.census,"resolve",resolve)
    boundary = RepoBoundary(root)
    local = OperationReceipt.issue_local(boundary,operation=d.OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,scope={})
    with pytest.raises(UnauthorizedOperation):
        d.execute_once(root,local)
    assert not calls
    digest = sha256_file(root/d.PLAN)
    receipt = OperationReceipt.issue_user_approved(boundary,operation=d.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=d.scope(root,plan),approval_command=d.OPERATION,approval_plan_id=d.PLAN_ID,
        approval_plan_sha256=digest,approval_line=f"APPROVE {d.OPERATION} PLAN {d.PLAN_ID} SHA256 {digest}")
    result = d.execute_once(root,receipt)
    assert result['rows'] == 3 and result['files'] == 1 and len(calls) == 1
    saved = json.loads((root/'evidence/ES_2018.json').read_bytes())
    assert all(x['rows']==3 and x['nulls']==1 and x['lengths']=={'64':1,'6':1} for x in saved['fields'].values())
    assert 'prefix' not in json.dumps(saved)
    with pytest.raises((UnauthorizedOperation,FileExistsError)):
        d.execute_once(root,receipt)
    assert len(calls)==1
