"""Generated inputs only. No saved research predictions or market data are read."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import socket
import re
import shutil
import subprocess

import numpy as np
import pytest

from futures_rebuild import historical_model_inspection as h
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.historical_simulation import save_demo
from futures_rebuild.simulation_records import encoded, validate_bundle
from futures_rebuild.live_cockpit.replay import ReplayCockpitEngine
from futures_rebuild.live_cockpit.protocol import validate_event


def test_inspector_follows_first_fast_batch_and_preserves_manual_selection():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for the cockpit JavaScript check")
    source = (Path(__file__).parents[1] / "src/futures_rebuild/live_cockpit/assets/app.js").read_text(encoding="utf-8")
    expression = re.search(r"const follow = (.*?);", source, re.S).group(1)
    subprocess.run([node, "-e", """
const assert = require('node:assert/strict');
const follow = new Function('replayInspection', 'oldIndex', 'payload', 'meta', 'return ' + process.argv[1]);
const previous = n => ({records: Array(n), cursor: n, metadata: {spec_id: 'same'}});
const meta = {spec_id: 'same'};
assert.equal(follow(previous(0), 0, {cursor: 10}, meta), true);
assert.equal(follow(previous(10), 9, {cursor: 20}, meta), true);
assert.equal(follow(previous(10), 2, {cursor: 20}, meta), false);
assert.equal(follow(previous(20), 2, {cursor: 0}, meta), true);
assert.equal(follow(previous(20), 2, {cursor: 20}, {spec_id: 'new'}), true);
""", expression], check=True, capture_output=True, text=True)


def test_compact_inspector_keeps_exact_values_and_handles_context():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for the cockpit JavaScript check")
    source = (Path(__file__).parents[1] / "src/futures_rebuild/live_cockpit/assets/app.js").read_text(encoding="utf-8")
    function = "function inspectReplayBar() {" + source.split("function inspectReplayBar() {", 1)[1].split('\n  document.getElementById("replay-inspect-bar")', 1)[0]
    subprocess.run([node, "-e", """
const assert = require('node:assert/strict');
const element = () => ({value: '0', textContent: '', children: [],
  replaceChildren() { this.children = []; }, append(child) { this.children.push(child); }});
const nodes = new Map();
const document = {getElementById(id) { if (!nodes.has(id)) nodes.set(id, element()); return nodes.get(id); }, createElement: element};
const state = {};
let replayInspection = {records: [], metadata: {strategy: 'test', feature_names: ['first', 'second'], forecast_units: 'ticks', horizon_minutes: 60}};
eval(process.argv[1]);
inspectReplayBar();
assert.match(document.getElementById('replay-prediction').textContent, /Press Play/);
assert.equal(document.getElementById('replay-feature-table').hidden, true);
const row = {bar: {opened_at: '2021-01-26T14:29:00+00:00'}, inspection: {status: 'CONTEXT', reasons: ['lookback']}};
replayInspection.records.push(row);
inspectReplayBar();
assert.match(document.getElementById('replay-prediction').textContent, /Lookback context/);
row.inspection = {status: 'ELIGIBLE', prediction: 0.2689386199876725, features: [2, 3.123456], reasons: ['eligible']};
inspectReplayBar();
assert.match(document.getElementById('replay-prediction').textContent, /0.27 ticks/);
assert.match(document.getElementById('replay-prediction-exact').textContent, /0.2689386199876725/);
assert.equal(document.getElementById('replay-feature-table').hidden, false);
assert.deepEqual(document.getElementById('replay-features').children.map(tr => tr.children.map(td => td.textContent)), [['first', 2], ['second', 3.123456]]);
replayInspection.metadata.kind = 'historical_diagnostics';
row.inspection.prediction = 0;
inspectReplayBar();
assert.equal(document.getElementById('replay-outcome').textContent, 'Outcome pending.');
row.outcomes = [{decision_index: 0, actual_ticks: '2', error_ticks: '2', available_at: 'later'}];
inspectReplayBar();
assert.match(document.getElementById('replay-outcome').textContent, /Actual: 2.00 ticks/);
assert.equal(document.getElementById('replay-outcome-jump').disabled, true);
replayInspection.records = [];
inspectReplayBar();
assert.equal(document.getElementById('replay-features').children.length, 0);
assert.equal(document.getElementById('replay-feature-table').hidden, true);
""", function], check=True, capture_output=True, text=True)


@pytest.fixture
def inputs():
    times = h.candidate.decision_times(h.SESSION)
    first, stop = times[0] - h.DELAY - 61*h.MINUTE, times[-1] - h.DELAY
    model = h.candidate.bind({"mechanism_id": h.candidate.mechanism_id(),
        "feature_names": list(h.candidate.FEATURES), "dependency_versions": h.candidate.old.comparison.VERSIONS,
        "candidate": {"family": "ridge", "candidate_id": "generated-test-model"},
        "columns": list(range(7)), "means": [0.0]*7, "scales": [1.0]*7,
        "weights": [0.25]*7, "training_mean_ticks": 0.0, "latest_label_ns": first-h.MINUTE}, "model_id")
    rows = []
    for i, t in enumerate(range(first, stop, h.MINUTE)):
        p = 5000*10**9 + i*250_000_000
        rows.append(dict(market="ES", actual_contract="ES-GENERATED-TEST", publisher_id=1, instrument_id=1,
            listing_activation_ns=first-100*h.MINUTE, expiration_ns=stop+100*h.MINUTE,
            min_price_increment_nano=250_000_000, multiplier_nano=50*10**9, currency="USD",
            bar_start_ns=t, bar_end_ns=t+h.MINUTE, available_at_ns=t+h.MINUTE+h.DELAY,
            decision_eligible_at_ns=t+h.MINUTE+h.DELAY, definition_received_at_ns=first-h.MINUTE,
            open_nano=p, high_nano=p+10**9, low_nano=p-10**9, close_nano=p, volume=100+i))
    windows = [dict(request=h.WindowRequest(at-h.DELAY-61*h.MINUTE,at-h.DELAY,at),
                    status="ELIGIBLE", reasons=(), observations=rows[i:i+61]) for i,at in enumerate(times)]
    spec = dict(operation=h.OPERATION, session=h.SESSION, market="ES", start_ns=first, end_ns=stop,
        model_id=model["model_id"], training_cutoff=h.stamp(model["latest_label_ns"]), spec_id="a"*64,
        source_entry={"parquet_path":"generated/release/observations.parquet"}, disposition="GENERATED TEST ONLY")
    return model, spec, windows


def test_inference_matches_frozen_function_and_has_no_trades(inputs):
    model, spec, windows = inputs
    bundle, forecasts = h.prediction_bundle(*inputs)
    assert len(bundle["records"]) == 390 and len(forecasts) == 330
    assert bundle["records"][59]["inspection"]["prediction"] is None
    first = bundle["records"][60]["inspection"]
    features = h.candidate.feature_values(windows[0]["observations"])
    assert first["features"] == [repr(float(v)) for v in features]
    assert float(first["prediction"]) == h.research.predict(model, [features])[0]
    assert all(set(r) == {"bar", "available_at", "inspection"} for r in bundle["records"])
    assert "P&L unavailable" in bundle["summary"]["message"]
    assert encoded(bundle) == encoded(h.prediction_bundle(*inputs)[0])
    validate_bundle(bundle)


def test_future_changes_cannot_change_earlier_predictions(inputs):
    before = h.prediction_bundle(*inputs)[0]
    changed = deepcopy(inputs)
    last = changed[2][-1]["observations"][-1]
    last["close_nano"] += 250_000_000
    after = h.prediction_bundle(*changed)[0]
    assert before["records"][:-1] == after["records"][:-1]
    assert before["records"][-1]["inspection"] != after["records"][-1]["inspection"]


@pytest.mark.parametrize("bad", ["future", "gap", "contract", "refused", "cutoff", "features", "scale"])
def test_invalid_inputs_stop_without_fallback(inputs, bad):
    model, spec, windows = inputs
    if bad == "future": windows[0]["observations"][-1]["decision_eligible_at_ns"] += 1
    if bad == "gap": windows[0]["observations"].pop()
    if bad == "contract": windows[0]["observations"][0]["actual_contract"] = "OTHER"
    if bad == "refused": windows[0].update(status="REFUSED", reasons=("GAP",))
    if bad == "cutoff": model["latest_label_ns"] = windows[0]["request"].decision_at_ns
    if bad == "features": model["feature_names"].reverse()
    if bad == "scale": model["scales"][0] = 0
    with pytest.raises((IntegrityError, ValueError)):
        h.prediction_bundle(model, spec, windows)


@pytest.mark.parametrize("bad", ["version", "future", "account", "nan", "summary", "features"])
def test_bundle_rejects_malformed_or_invented_economics(inputs, bad):
    b = h.prediction_bundle(*inputs)[0]
    if bad == "version": b["schema"] = "offline-simulation/999"
    if bad == "future": b["records"][60]["inspection"]["decision_at"] = b["records"][61]["available_at"]
    if bad == "account": b["records"][0]["account"] = {}
    if bad == "nan": b["records"][60]["inspection"]["prediction"] = "NaN"
    if bad == "summary": b["summary"]["predictions"] -= 1
    if bad == "features": b["records"][60]["inspection"]["features"].pop()
    with pytest.raises(ValueError): validate_bundle(b)


def write_bundle(tmp_path, bundle):
    folder = tmp_path/"historical"; folder.mkdir()
    (folder/"result.json").write_bytes(encoded(bundle))
    return folder


def test_replay_prefix_switch_restart_speed_and_isolation(inputs, tmp_path, monkeypatch):
    bundle = h.prediction_bundle(*inputs)[0]
    folder = write_bundle(tmp_path, bundle)
    demo = tmp_path/"demo"; save_demo(demo)
    def blocked(*a, **kw): raise AssertionError("Viewer invoked data or calculation")
    for name in ("read_windows", "saved_model", "prediction_bundle", "execute_authorized"):
        monkeypatch.setattr(h, name, blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    engine = ReplayCockpitEngine(folder)
    assert engine.snapshot()["payload"]["records"] == []
    engine.command("step")
    assert len(engine.snapshot()["payload"]["records"]) == 1
    validate_event(engine.snapshot())
    for speed in (1,10,100):
        engine.command("restart"); engine.command("speed",speed); engine.command("play")
        engine.advance(61/speed)
        payload = engine.snapshot()["payload"]
        assert payload["records"] == bundle["records"][:61] and payload["summary"] is None
        validate_event(engine.snapshot())
        engine.advance(1000)
        assert engine.snapshot()["payload"]["summary"] == bundle["summary"]
        validate_event(engine.snapshot())
    engine.open_run(demo)
    assert engine.cursor == 0 and engine.snapshot()["payload"]["metadata"]["synthetic"]
    bad = tmp_path/"bad"; bad.mkdir(); (bad/"result.json").write_text('{"status":"INCOMPLETE"}')
    with pytest.raises(ValueError): engine.open_run(bad)
    assert engine.snapshot()["payload"]["metadata"]["synthetic"]
    engine.open_run(folder)
    assert engine.snapshot()["payload"]["summary"] is None
    engine.stop()


def test_missing_authority_does_not_read_data(monkeypatch, tmp_path):
    monkeypatch.setattr(h, "read_windows", lambda *a, **kw: pytest.fail("Unauthorized data read"))
    with pytest.raises(UnauthorizedOperation): h.execute_authorized(tmp_path, None)


def test_native_picker_cancel_and_wrong_mode(inputs, tmp_path, monkeypatch):
    from futures_rebuild.live_cockpit.app import CockpitApi
    folder = write_bundle(tmp_path, h.prediction_bundle(*inputs)[0])
    engine = ReplayCockpitEngine(folder)
    controller = SimpleNamespace(engine=engine, window=SimpleNamespace(create_file_dialog=lambda *a, **k: None))
    api = CockpitApi(controller)
    assert api.open_replay_run()["cancelled"]
    controller.window.create_file_dialog = lambda *a, **k: [str(folder/"result.json")]
    engine.command("step")
    assert api.open_replay_run()["ok"] and engine.cursor == 0
    controller.engine = object()
    with pytest.raises(ValueError): api.open_replay_run()


@pytest.mark.parametrize("failure", [None, "mismatch", "write"])
def test_authorized_path_parity_single_use_and_atomic_output(inputs, tmp_path, monkeypatch, failure):
    from futures_rebuild.boundary import OperationReceipt, OperationClassification, RepoBoundary
    model, spec, windows = inputs
    expected, forecasts = h.prediction_bundle(*inputs)
    saved = h.candidate.bind({"forecast_ids": list(forecasts), "predictions": list(forecasts.values())}, "forecast_batch_id")
    if failure == "mismatch":
        saved["predictions"][0] += 1
        saved = h.candidate.bind({k:v for k,v in saved.items() if k != "forecast_batch_id"}, "forecast_batch_id")
    batch = tmp_path/"generated-batch.json"; batch.write_bytes(encoded(saved))
    spec.update(output="run", model_batch_sha256=h.sha256_file(batch))
    monkeypatch.setattr(h, "MODEL_BATCH", batch.name)
    monkeypatch.setattr(h, "require_scope", lambda root, scope: spec)
    monkeypatch.setattr(h, "validate_spec", lambda root, s: None)
    monkeypatch.setattr(h, "saved_model", lambda root: model)
    calls = []
    def read(*a, **kwargs):
        calls.append(kwargs)
        assert kwargs["purpose"] == "FEATURE_GENERATION" and kwargs["year"] == 2021
        assert len(kwargs["requests"]) == 330
        return windows
    monkeypatch.setattr(h, "read_windows", read)
    boundary = RepoBoundary(tmp_path)
    command, identity, digest = h.OPERATION, "a"*64, "b"*64
    receipt = OperationReceipt.issue_user_approved(boundary, operation=command,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={"spec_path":"generated-spec.json", "spec_id":identity,"spec_sha256":digest},
        approval_command=command, approval_plan_id=identity, approval_plan_sha256=digest,
        approval_line=f"APPROVE {command} PLAN {identity} SHA256 {digest}")
    if failure == "write":
        original = Path.replace
        def fail_output(self, target):
            if self.name == "result.partial": raise OSError("interrupted output")
            return original(self,target)
        monkeypatch.setattr(Path, "replace", fail_output)
    if failure:
        with pytest.raises((IntegrityError,OSError)): h.execute_authorized(tmp_path,receipt)
        assert not (tmp_path/"run/result.json").exists()
    else:
        report = h.execute_authorized(tmp_path,receipt)
        assert report["predictions_verified"] == 330
        assert (tmp_path/"run/result.json").read_bytes() == encoded(expected)
    with pytest.raises(UnauthorizedOperation): h.execute_authorized(tmp_path,receipt)
    assert len(calls) == 1


def test_scope_binds_exact_spec_and_personal_approval(tmp_path, monkeypatch):
    import json
    spec = {"spec_id":"a"*64,"output":"FuturesLiveCockpit/ReplayRuns/generated"}
    p=tmp_path/"spec.json"; p.write_bytes(encoded(spec))
    scope=dict(spec_path="spec.json",spec_id=spec["spec_id"],spec_sha256=h.sha256_file(p),
               approval_command=h.OPERATION,approval_plan_id=spec["spec_id"],approval_plan_sha256=h.sha256_file(p))
    monkeypatch.setattr(h,"prepare_spec",lambda root, output:spec)
    assert h.require_scope(tmp_path,scope)==spec
    for key in ("spec_id","spec_sha256","approval_plan_id","approval_plan_sha256","approval_command"):
        changed={**scope,key:"c"*64}
        with pytest.raises(IntegrityError): h.require_scope(tmp_path,changed)


def test_historical_cli_is_prepare_only(tmp_path, monkeypatch):
    from futures_rebuild.historical_simulation import main
    spec={"session":h.SESSION,"model_id":"a"*64,"output":str(tmp_path/"result"),"physical_read_scope":"generated test"}
    calls=[]
    monkeypatch.setattr(h,"prepare_file",lambda *a: calls.append(a) or spec)
    monkeypatch.setattr(h,"execute_authorized",lambda *a:pytest.fail("CLI executed historical inference"))
    assert main(["--prepare-historical",str(tmp_path/"spec.json"),"--output",str(tmp_path/"result")])==0
    assert len(calls)==1 and not (tmp_path/"result").exists()


def diagnostic_inputs(inputs, step=1):
    model, spec, windows = deepcopy(inputs)
    indexed = {r["bar_start_ns"]: r for w in windows for r in w["observations"]}
    template = indexed[max(indexed)]
    for start in range(spec["end_ns"], spec["end_ns"]+62*h.MINUTE, h.MINUTE):
        indexed[start] = {**template, "bar_start_ns":start, "bar_end_ns":start+h.MINUTE,
                          "available_at_ns":start+h.MINUTE+h.DELAY,
                          "decision_eligible_at_ns":start+h.MINUTE+h.DELAY}
    for i, row in enumerate(indexed.values()):
        price = 5000*10**9 + step*i*250_000_000
        row.update(open_nano=price, close_nano=price, high_nano=price+10**9, low_nano=price-10**9)
    # Rebuild overlapping windows to share the exact normalized rows.
    for w in windows:
        w["observations"] = [indexed[t] for t in range(w["request"].start_ns,w["request"].end_ns,h.MINUTE)]
    identity = h.observation_identity(indexed[min(indexed)])
    targets=[]
    for at in h.candidate.decision_times(spec["session"]):
        start, end = at-h.DELAY+h.MINUTE, at-h.DELAY+62*h.MINUTE
        request=h.WindowRequest(start,end,at,"target",end+h.DELAY,identity,at)
        targets.append(dict(request=request,status="COMPLETE",reasons=(),
                            observations=[indexed[t] for t in range(start,end,h.MINUTE)]))
    return model,spec,windows,targets


@pytest.mark.parametrize("step", [-1,0,1])
def test_original_outcomes_and_exact_signed_errors(inputs, step):
    from decimal import Decimal
    bundle, forecasts=h.diagnostic_bundle(*diagnostic_inputs(inputs,step))
    rows=bundle["records"]
    assert len(rows)==452 and len(forecasts)==330
    assert all(not r["outcomes"] for r in rows[:122])
    event=rows[122]["outcomes"][0]
    assert event["decision_index"]==60
    assert Decimal(event["actual_ticks"])==60*step
    assert Decimal(event["error_ticks"])==Decimal(60*step)-Decimal(rows[60]["inspection"]["prediction"])
    assert rows[-1]["outcomes"][0]["decision_index"]==389
    assert all(r["inspection"]["prediction"] is None for r in rows[390:])
    validate_bundle(bundle)


def test_outcome_only_changes_never_enter_forecasts(inputs):
    data=diagnostic_inputs(inputs)
    before=h.diagnostic_bundle(*data)[0]
    # Only the final exit reference changes, beyond every feature window.
    data[-1][-1]["observations"][-1]["open_nano"]+=250_000_000
    after=h.diagnostic_bundle(*data)[0]
    assert [r["inspection"] for r in before["records"]]==[r["inspection"] for r in after["records"]]
    assert before["records"][-1]["outcomes"]!=after["records"][-1]["outcomes"]


@pytest.mark.parametrize("bad", ["gap","contract","delay","refused"])
def test_library_excludes_incompatible_targets(inputs,bad):
    data=diagnostic_inputs(inputs)
    target=data[-1][-1]
    if bad=="gap": target["observations"].pop()
    if bad=="contract": target["observations"][-1]["actual_contract"]="OTHER"
    if bad=="delay": target["observations"][-1]["available_at_ns"]+=1
    if bad=="refused": target.update(status="INDETERMINATE",reasons=("CLOSED_OR_MISSING",))
    with pytest.raises(h.SessionExcluded): h.diagnostic_bundle(*data)


def library_fixture(inputs,tmp_path):
    from futures_rebuild.simulation_records import LIBRARY_SCHEMA,OOS_CONTEXT
    base=h.diagnostic_bundle(*diagnostic_inputs(inputs))[0]
    library=dict(schema=LIBRARY_SCHEMA,status="COMPLETE",model_id=base["metadata"]["model_id"],
                 spec_id=base["metadata"]["spec_id"],evaluation_context=OOS_CONTEXT,sessions=[])
    for day in ["2021-01-26","2021-01-27"]:
        bundle=deepcopy(base)
        if day.endswith("27"):
            # Generated consecutive session, same frozen model and price pattern.
            for row in bundle["records"]:
                for k in ("opened_at","closed_at"): row["bar"][k]=row["bar"][k].replace("2021-01-26","2021-01-27")
                row["available_at"]=row["available_at"].replace("2021-01-26","2021-01-27")
                for k in ("decision_at","feature_window_start"):
                    if row["inspection"][k]: row["inspection"][k]=row["inspection"][k].replace("2021-01-26","2021-01-27")
                for event in row["outcomes"]:
                    for k in ("entry_at","exit_at","available_at"): event[k]=event[k].replace("2021-01-26","2021-01-27")
            bundle["metadata"].update(session=day,input_sha256=h.hashlib.sha256(encoded([r["bar"] for r in bundle["records"]])).hexdigest())
        validate_bundle(bundle)
        target=tmp_path/day; target.mkdir()
        raw=encoded(bundle); (target/"result.json").write_bytes(raw)
        library["sessions"].append(dict(session=day,status="INCLUDED",contract=base["metadata"]["contract"],
            path=f"{day}/result.json",sha256=h.hashlib.sha256(raw).hexdigest(),reasons=[]))
    library["sessions"].append(dict(session="2021-01-28",status="EXCLUDED",contract=None,path=None,sha256=None,reasons=["Generated gap"]))
    (tmp_path/"library.json").write_bytes(encoded(library))
    return library


def test_library_replay_visibility_navigation_and_random(inputs,tmp_path,monkeypatch):
    library_fixture(inputs,tmp_path)
    engine=ReplayCockpitEngine(tmp_path/"library.json")
    assert engine.snapshot()["payload"]["metadata"]["session"]=="2021-01-26"
    assert engine.snapshot()["payload"]["records"]==[]
    engine.command("next_prediction")
    payload=engine.snapshot()["payload"]
    assert payload["cursor"]==61 and not payload["playing"]
    assert all(not r["outcomes"] for r in payload["records"])
    validate_event(engine.snapshot())
    with pytest.raises(ValueError): engine.command("outcome",index=61)
    engine.command("outcome",index=60)
    payload=engine.snapshot()["payload"]
    assert payload["cursor"]==123 and sum(len(r["outcomes"]) for r in payload["records"])==1
    validate_event(engine.snapshot())
    reached=deepcopy(payload["records"])
    for speed in (1,10,100):
        engine.command("restart"); engine.command("speed",speed); engine.command("play"); engine.advance(123/speed)
        assert engine.snapshot()["payload"]["records"]==reached
    selections=[]
    def choose(entries):
        selections.append([e["session"] for e in entries]); return entries[0]
    monkeypatch.setattr("futures_rebuild.live_cockpit.replay.random.choice",choose)
    engine.select_session()
    assert selections==[["2021-01-27"]] and engine.cursor==0 and not engine.playing
    validate_event(engine.snapshot())
    engine.select_session("2021-01-26")
    with pytest.raises(ValueError): engine.select_session("2021-01-28")
    assert engine.snapshot()["payload"]["metadata"]["session"]=="2021-01-26"
    engine.stop()


@pytest.mark.parametrize("bad", ["premature","wrong_value","missing","duplicate","version"])
def test_bad_diagnostic_bundles_fail_closed(inputs,bad):
    bundle=h.diagnostic_bundle(*diagnostic_inputs(inputs))[0]
    event=bundle["records"][122]["outcomes"][0]
    if bad=="premature": bundle["records"][121]["outcomes"]=bundle["records"][122]["outcomes"]; bundle["records"][122]["outcomes"]=[]
    if bad=="wrong_value": event["actual_ticks"]="999"
    if bad=="missing": bundle["records"][122]["outcomes"]=[]
    if bad=="duplicate": bundle["records"][123]["outcomes"]=[event]
    if bad=="version": bundle["schema"]="offline-simulation/999"
    with pytest.raises(ValueError): validate_bundle(bundle)


@pytest.mark.parametrize("bad",["escape","incomplete","hash","malformed","missing"])
def test_invalid_library_preserves_open_run(inputs,tmp_path,bad):
    from futures_rebuild.simulation_records import load_library
    library=library_fixture(inputs,tmp_path)
    engine=ReplayCockpitEngine(tmp_path); engine.command("next_prediction")
    before=engine.snapshot()["payload"]
    if bad=="escape": library["sessions"][0]["path"]="../result.json"
    if bad=="incomplete": library["status"]="INCOMPLETE"
    if bad=="hash": library["sessions"][0]["sha256"]="f"*64
    if bad=="missing": (tmp_path/"2021-01-27/result.json").unlink()
    (tmp_path/"library.json").write_bytes(b"{}" if bad=="malformed" else encoded(library))
    with pytest.raises((ValueError,OSError)): engine.open_run(tmp_path)
    assert engine.snapshot()["payload"]==before
    engine.stop()


def test_no_library_authority_never_reads(monkeypatch,tmp_path):
    monkeypatch.setattr(h,"read_windows",lambda *a,**kw:pytest.fail("Unapproved history access"))
    with pytest.raises(UnauthorizedOperation): h.execute_library_authorized(tmp_path,None)


def test_session_scope_and_dst(inputs):
    from datetime import datetime
    spec={**inputs[1],"sessions":["2021-03-12","2021-03-15"]}
    a=h.session_spec(spec,"2021-03-12"); b=h.session_spec(spec,"2021-03-15")
    assert datetime.fromisoformat(h.stamp(a["first_decision_ns"])).hour==14
    assert datetime.fromisoformat(h.stamp(b["first_decision_ns"])).hour==13
    with pytest.raises(IntegrityError): h.session_spec(spec,"2022-01-01")


@pytest.mark.parametrize("failure",[None,"parity","interrupt"])
def test_library_execution_generated_receipt_exclusions_and_atomic_index(inputs,tmp_path,monkeypatch,failure):
    from futures_rebuild.boundary import OperationReceipt,OperationClassification,RepoBoundary
    model,spec,features,targets=diagnostic_inputs(inputs)
    _,forecasts=h.prediction_bundle(model,spec,features)
    batch=h.candidate.bind({"forecast_ids":list(forecasts),"predictions":list(forecasts.values())},"forecast_batch_id")
    if failure=="parity":
        batch["predictions"][0]+=1
        batch=h.candidate.bind({k:v for k,v in batch.items() if k!="forecast_batch_id"},"forecast_batch_id")
    batchpath=tmp_path/"batch.json"; batchpath.write_bytes(encoded(batch))
    study=tmp_path/"study.json"; study.write_text('{}')
    spec.update(operation=h.LIBRARY_OPERATION,output="library",sessions=["2021-01-26","2021-01-27"],
                model_batch_sha256=h.sha256_file(batchpath),study_sha256=h.sha256_file(study),
                input_admission_id="generated",bindings={})
    monkeypatch.setattr(h,"MODEL_BATCH",batchpath.name); monkeypatch.setattr(h,"STUDY",study.name)
    monkeypatch.setattr(h,"require_scope",lambda *a:spec)
    monkeypatch.setattr(h,"saved_model",lambda *a:model)
    monkeypatch.setattr(h,"active_policy",lambda *a:{"id":"generated"})
    calls=[]
    def read(root,**kwargs):
        calls.append(kwargs["purpose"])
        assert len(kwargs["requests"])==330
        if len(calls)==3:
            return [{"status":"REFUSED","reasons":["GAP"],"request":kwargs["requests"][0],"observations":[]}]
        return features if kwargs["purpose"]=="FEATURE_GENERATION" else targets
    monkeypatch.setattr(h,"read_windows",read)
    command=h.LIBRARY_OPERATION
    receipt=OperationReceipt.issue_user_approved(RepoBoundary(tmp_path),operation=command,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={"spec_path":"test.json","spec_id":"a"*64,"spec_sha256":"b"*64},
        approval_command=command,approval_plan_id="a"*64,approval_plan_sha256="b"*64,
        approval_line=f"APPROVE {command} PLAN {'a'*64} SHA256 {'b'*64}")
    if failure=="interrupt":
        original=Path.replace
        def interrupt(self,target):
            if self.name=="library.partial": raise OSError("generated interruption")
            return original(self,target)
        monkeypatch.setattr(Path,"replace",interrupt)
    if failure:
        with pytest.raises((IntegrityError,OSError)): h.execute_library_authorized(tmp_path,receipt)
        assert not (tmp_path/"library/library.json").exists()
        if failure=="parity": assert calls==["FEATURE_GENERATION"]
    else:
        result=h.execute_library_authorized(tmp_path,receipt)
        assert (result["included"],result["excluded"])==(1,1)
        assert calls==["FEATURE_GENERATION","DISCOVERY_RESEARCH","FEATURE_GENERATION"]
        engine=ReplayCockpitEngine(tmp_path/"library"); engine.command("next_prediction"); validate_event(engine.snapshot()); engine.stop()
    previous_calls=list(calls)
    with pytest.raises(UnauthorizedOperation): h.execute_library_authorized(tmp_path,receipt)
    assert calls==previous_calls


def test_library_cli_prepares_without_execution(tmp_path,monkeypatch):
    from futures_rebuild.historical_simulation import main
    called=[]
    spec={"session":h.SESSION,"sessions":[h.SESSION],"model_id":"a"*64,"output":"test","physical_read_scope":"generated"}
    monkeypatch.setattr(h,"prepare_library_file",lambda *args:called.append(args) or spec)
    monkeypatch.setattr(h,"execute_library_authorized",lambda *a:pytest.fail("CLI executed history"))
    assert main(["--prepare-library",str(tmp_path/"spec.json"),"--output",str(tmp_path/"library")])==0
    assert len(called)==1 and not (tmp_path/"library").exists()


def test_library_viewer_has_no_inference_or_provider_dependency(inputs,tmp_path,monkeypatch):
    library_fixture(inputs,tmp_path)
    monkeypatch.setattr(h,"read_windows",lambda *a,**k:pytest.fail("Viewer read research data"))
    monkeypatch.setattr(h.research,"predict",lambda *a,**k:pytest.fail("Viewer ran inference"))
    monkeypatch.setattr(socket,"socket",lambda *a,**k:pytest.fail("Viewer opened a network connection"))
    engine=ReplayCockpitEngine(tmp_path)
    engine.command("next_prediction"); engine.command("outcome",index=60); engine.select_session(); engine.stop()


@pytest.mark.parametrize("bad",[None,"membership","cutoff","fit_role","permission"])
def test_library_preparation_binds_evaluation_membership_without_history(inputs,tmp_path,monkeypatch,bad):
    import json
    dates=h.candidate.frozen_ranges(h.candidate.weekday_dates())[0]["outer"]["audit_dates"]
    study={"outer_windows":[{"outer_fold":0,"audit_dates":list(dates)}]}
    if bad=="membership": study["outer_windows"][0]["audit_dates"][0]="2020-01-01"
    (tmp_path/"study.json").write_text(json.dumps(study))
    model=deepcopy(inputs[0]); model["fit_kind"]="inner" if bad=="fit_role" else "outer"
    if bad=="cutoff": model["latest_label_ns"]=h.candidate.decision_times(h.SESSION)[0]
    base={**inputs[1],"source_entry":{"permitted_uses":[] if bad=="permission" else ["DISCOVERY_RESEARCH"]}}
    monkeypatch.setattr(h,"prepare_spec",lambda *a:deepcopy(base))
    monkeypatch.setattr(h,"saved_model",lambda *a:model)
    monkeypatch.setattr(h,"STUDY","study.json")
    monkeypatch.setattr(h,"read_windows",lambda *a,**k:pytest.fail("Preparation read historical observations"))
    if bad:
        with pytest.raises(IntegrityError): h.prepare_library_spec(tmp_path,tmp_path/"new")
    else:
        spec=h.prepare_library_spec(tmp_path,tmp_path/"new")
        assert spec["sessions"]==dates and spec["limits"]["sessions"]==63
        assert spec["limits"]["decisions"]==20790 and spec["limits"]["fits"]==0
