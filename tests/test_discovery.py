"""Generated metadata/accounting checks; no authentic observations or receipts."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import sys
import shutil
from unittest.mock import patch

from futures_rebuild import discovery as d
from futures_rebuild.canonical import sha256_file, write_new_canonical_authority
from futures_rebuild.discovery_attempt import CampaignBudget
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.es_nq_transfer_access import scope as operation_scope
from futures_rebuild.l0_alpha_report_demo import fixture_context, fixture_replay
from futures_rebuild.l0_alpha_reporting import candidate_report, load_catalog as load_legacy, write_catalog

ROOT = Path(__file__).resolve().parents[1]
SCOPE = {"start": "2018-01-01", "end": "2022-12-31", "markets": ["ES", "NQ"], "data_kind": "SYNTHETIC"}
LIMITS = {"attempts": 1, "fits": 2, "seconds": 60, "memory_bytes": 64*2**20,
          "new_output_bytes": 4*2**20, "additional_cost_usd": 0}


def rebind(value, key):
    return d.bind({k: v for k, v in value.items() if k != key}, key)


def synthetic_study(root, scientific_id="study", *, kind="PREDICTIVE", parents=(), exposure=()):
    path = root / "declaration.json"
    write_new_canonical_authority(path, {"generated": True})
    return d.bind({"schema_version": d.SCHEMA, "scientific_id": scientific_id, "question": "Generated hypothesis",
        "kind": kind, "parents": list(parents), "parentage": "DECLARED" if parents else "NO_PARENT_KNOWN",
        "intended_change": "Generated mechanics", "informed_by": [], "exposure": list(exposure),
        "history_complete": False, "declaration": {"path": "declaration.json", "sha256": sha256_file(path)}}, "study_summary_id")


def batch(history, study, limits=None):
    return d.prepare_batch(history, studies=[study["scientific_id"]],
        scope={"stage": "DEVELOPMENT", "start": "2018-01-01", "end": "2022-12-31", "markets": ["ES", "NQ"]},
        candidates=["primary"], benchmarks=["nq_only", "zero", "mean"], primary_rule="frozen generated rule",
        limits=limits or LIMITS, stop_conditions=["one attempt", "any fatal failure"], rationale="test mechanics")


def result(study, **changes):
    arguments = dict(execution="COMPLETE", completed={"fits": 2}, expected={"fits": 2}, primary_pass=False,
                     evidence_complete=True, power_sufficient=True, data_scope=SCOPE,
                     metrics=[{"name": "MSE improvement", **d.measured(-2., "ticks squared")}])
    arguments.update(changes)
    return d.result_summary(study, **arguments)


def adopted(root, history, study, limits=None):
    limits = limits or LIMITS
    b = batch(history, study, limits)
    op = {"operation_name": "EXECUTE_ES_NQ_TRANSFER_ONCE", "operation_id": "a"*64,
          "study_id": study["scientific_id"], "kind": "execute", "implementation_id": "b"*64,
          "source_plan_id": "c"*64, "claim": "GENERATED_ONLY", "limits": {k:v for k,v in limits.items() if k != "fits"}}
    value = d.bind({"state": "ADOPTED", "module_sha256": sha256_file(Path(d.__file__)), "batch": b,
                   "operations": [{"operation": op["operation_name"], "scope": operation_scope(op), "reservation": limits}]}, "adoption_id")
    write_new_canonical_authority(root/d.APPROVED, value)
    return b, op


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="discovery_synthetic_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.history = d.History(self.root/d.HISTORY)
        self.study = synthetic_study(self.root)
        self.history.add_study(self.study)

    def test_identity_ancestry_and_global_exposure_cannot_reset(self):
        renamed = deepcopy(self.study); renamed["question"] = "Renamed to look fresh"
        with self.assertRaises(IntegrityError): self.history.add_study(rebind(renamed,"study_summary_id"))
        exposure = [{"market":"NQ","start":"2018-01-01","end":"2022-12-31","reason":"observed diagnostic"}]
        self.history.expose("study", exposure, "diagnostic-result")
        for parents in ((), ("study",)):
            child = synthetic_study(self.root,"child",parents=parents)
            with self.assertRaises(IntegrityError): self.history.add_study(child)
        child = synthetic_study(self.root,"child",parents=("study",),exposure=exposure)
        child["informed_by"] = ["diagnostic-result"]
        child = rebind(child,"study_summary_id")
        self.history.add_study(child)
        b = batch(self.history, child)
        self.history.expose("child",exposure,"another diagnostic")
        with self.assertRaises(IntegrityError): d.check_batch(self.history,b)

    def test_missing_limits_and_protected_scopes(self):
        for key in LIMITS:
            limits = dict(LIMITS); del limits[key]
            with self.assertRaises(IntegrityError): batch(self.history,self.study,limits)
        b = batch(self.history,self.study)
        for field,value in (("stage","CONFIRMATION"),("start","2025-07-14"),("end","2026-07-13")):
            changed = deepcopy(b); changed["scope"][field]=value
            with self.assertRaises((IntegrityError,UnauthorizedOperation)):
                d.check_batch(self.history,rebind(changed,"batch_id"))

    def test_attempts_are_reserved_before_work_and_never_refunded(self):
        b = batch(self.history,self.study)
        d.reserve_attempt(self.history,b,"operation-1",LIMITS)
        for op in ("operation-1","replacement"):
            with self.assertRaises((IntegrityError,UnauthorizedOperation)): d.reserve_attempt(self.history,b,op,LIMITS)
        d.finish_attempt(self.history,"operation-1",result(self.study))
        with self.assertRaises(IntegrityError): d.finish_attempt(self.history,"operation-1",result(self.study,primary_pass=True))
        self.assertEqual(len([r for r in self.history.records() if r["kind"]=="STUDY"]),1)

    def test_history_deletion_is_detected_by_reviewed_prefix(self):
        b=batch(self.history,self.study)
        isolated=d.History(self.root/"empty-history")
        with self.assertRaises(IntegrityError): d.check_batch(isolated,b)

    def test_complete_no_edge_and_candidate_both_stop(self):
        for passed,decision in ((False,"UNSUPPORTED"),(True,"PREDICTIVE_FINDING")):
            r=result(self.study,primary_pass=passed)
            self.assertEqual(r["decision"],decision)
            self.assertFalse(r["automatic_continuation"])
            self.assertFalse(r["confirmation_authorized"])
            self.assertFalse(r["alpha_established"])
        for status,decision in (("FAILED","MECHANICAL_FAILURE"),("INCOMPLETE","INCONCLUSIVE"),("ABANDONED","INCONCLUSIVE")):
            r=result(self.study,execution=status,primary_pass=True)
            self.assertEqual(r["decision"],decision)
            self.assertTrue(all(m["value"] is None for m in r["metrics"]))
        self.assertEqual(result(self.study,power_sufficient=False)["decision"],"INCONCLUSIVE")
        self.assertEqual(result(self.study,completed={"fits":1})["decision"],"INCONCLUSIVE")
        with self.assertRaises(IntegrityError): result(self.study,completed={"fits":0},expected={"fits":0})

    def test_economic_prerequisites_and_trade_diagnostics(self):
        economic=synthetic_study(self.root,"economic",kind="ECONOMIC")
        self.history.add_study(economic)
        with self.assertRaises(IntegrityError): batch(self.history,economic)
        replay=fixture_replay([2,-1,0,0,0,0,0,0])
        # Add one explicit break-even round trip, retaining flat-date accounting.
        replay["daily"][fixture_context()["scope"]["dates"][2]]["trades"]=1
        report=candidate_report(fixture_context(),replay)
        trades=[{"gross_nano_usd":g*10**9,"cost_nano_usd":c*10**9,"net_nano_usd":n*10**9}
                for g,c,n in ((3,1,2),(0,1,-1),(0,0,0))]
        stats=d.trade_statistics(report,trades)
        self.assertEqual(stats["Profit factor"]["value"],2)
        self.assertEqual(stats["Win rate"]["value"],1/3)
        self.assertEqual(stats["Average loss"]["value"],-1)
        self.assertEqual(stats["Break-even trades"]["value"],1)
        self.assertTrue(all(v["value"] is None for v in d.trade_statistics(report,trades[:1]).values()))
        zero=candidate_report(fixture_context(),fixture_replay([0]*8))
        self.assertEqual(d.trade_statistics(zero,[])["Profit factor"]["reason"],"ZERO_LOSS_DENOMINATOR")
        context=fixture_context(); context["experiment_id"]="economic"
        report=candidate_report(context,replay)
        summary=d.economic_summary(economic,report,benchmarks=[],trades=trades,primary_pass=True,power_sufficient=True)
        self.assertEqual(summary["decision"],"INCONCLUSIVE")  # Missing benchmark cannot qualify.
        self.assertIn(["Profit factor","2 net winning dollars / absolute net losing dollars"],d.display_rows(summary))
        self.assertIn("Drawdown scope",d.markdown(summary))
        with self.assertRaises(IntegrityError): d.economic_summary(self.study,report,benchmarks=[])

    def test_new_and_legacy_catalogs_remain_separate(self):
        summary=result(self.study)
        catalog=d.bind({"schema_version":d.CATALOG_SCHEMA,"payload_kind":"METADATA_ONLY","summaries":[summary]},"catalog_id")
        path=self.root/"summary.json"; write_new_canonical_authority(path,catalog)
        rows=d.load_catalog(path)["study_summaries"][0]["rows"]
        for label,value in rows:
            self.assertIn(label.replace("|","&#124;"),d.markdown(summary))
        self.assertEqual(rows,d.display_rows(summary))
        for _,value in rows:
            escaped=value.replace("&","&amp;").replace("<","&lt;").replace("|","&#124;").replace("\n","<br>")
            self.assertIn(escaped,d.markdown(summary))
        legacy=write_catalog(self.root/"legacy",[candidate_report(fixture_context(),fixture_replay([0]*8))])
        self.assertEqual(load_legacy(legacy)["schema_version"],"l0_alpha_comparison_view/2")
        with self.assertRaises((IntegrityError,UnauthorizedOperation)): d.load_catalog(legacy)
        changed=deepcopy(summary); changed["data_scope"]["end"]="2026-07-13"
        with self.assertRaises(UnauthorizedOperation): d.display_rows(rebind(changed,"result_id"))

    def test_exact_adopted_scope_and_declaration_before_input(self):
        with self.assertRaises(UnauthorizedOperation): d.require_adopted_scope(self.root,"EXECUTE_ES_NQ_TRANSFER_ONCE",{})
        with self.assertRaises(UnauthorizedOperation): d.require_adopted_scope(self.root,"AUDIT_RETROSPECTIVE_SCREEN_ONCE",{})
        b,op=adopted(self.root,self.history,self.study)
        d.require_adopted_scope(self.root,op["operation_name"],operation_scope(op))
        changed=operation_scope(op); changed["study_id"]="different"
        with self.assertRaises(UnauthorizedOperation): d.require_adopted_scope(self.root,op["operation_name"],changed)
        (self.root/"declaration.json").write_text('{}',encoding='utf-8')
        with self.assertRaises(IntegrityError): d.require_adopted_scope(self.root,op["operation_name"],operation_scope(op))

    def test_all_six_policy_dispatches_reject_before_adapter_access(self):
        from futures_rebuild import research_gateway_policy as policy
        b, op = adopted(self.root, self.history, self.study)
        approved = json.loads((self.root/d.APPROVED).read_bytes())
        approved["operations"] = []
        (self.root/d.APPROVED).write_text(json.dumps(rebind(approved, "adoption_id")))
        self.assertEqual(len(d.GUARDED_OPERATIONS), 6)
        # Only the repository location changes; the installed policy and shared
        # guard run unchanged. Adapter metadata access must remain unreachable.
        with patch.object(policy, "__file__", str(self.root/"src/futures_rebuild/research_gateway_policy.py")), \
                patch.object(policy, "assert_plain_file") as adapter_read:
            for operation in sorted(d.GUARDED_OPERATIONS):
                with self.subTest(operation=operation), self.assertRaisesRegex(UnauthorizedOperation, "operation differs"):
                    policy.require_current_real_history_operation(operation, operation_scope(op))
            adapter_read.assert_not_called()

    def test_stale_binding_and_missing_declaration_reject_exact_scope(self):
        _, op = adopted(self.root, self.history, self.study)
        path = self.root/d.APPROVED
        approved = json.loads(path.read_bytes())
        changed = deepcopy(approved); changed["module_sha256"] = "0" * 64
        path.write_text(json.dumps(rebind(changed, "adoption_id")))
        with self.assertRaisesRegex(UnauthorizedOperation, "absent or stale"):
            d.require_adopted_scope(self.root, op["operation_name"], operation_scope(op))
        path.write_text(json.dumps(approved))
        (self.root/"declaration.json").unlink()
        with self.assertRaises((IntegrityError, OSError)):
            d.require_adopted_scope(self.root, op["operation_name"], operation_scope(op))

    def test_existing_budget_counts_fits_and_stops_permanently(self):
        b,op=adopted(self.root,self.history,self.study)
        budget=CampaignBudget(self.root,op,self.root/"attempt",clock=lambda:0,memory=lambda:1)
        budget.reserve_fit(); budget.save("first",{"generated":True}); budget.reserve_fit()
        with self.assertRaises(IntegrityError): budget.reserve_fit()
        with self.assertRaises(IntegrityError): budget.save("after_failure",{})
        self.assertFalse((self.root/"attempt/after_failure.json").exists())
        self.assertTrue(budget.fatal(IntegrityError("generated limit")))
        self.assertTrue((self.root/"attempt/fatal_stop.json").exists())
        failed=json.loads((self.root/"attempt/failed_study_summary.json").read_bytes())
        self.assertEqual(failed["decision"],"MECHANICAL_FAILURE")
        self.assertFalse(failed["confirmation_authorized"])
        self.assertEqual(self.history.records()[-1]["payload"]["summary"]["execution"],"FAILED")
        with self.assertRaises(UnauthorizedOperation): CampaignBudget(self.root,op,self.root/"renamed-attempt",clock=lambda:0,memory=lambda:1)

    def test_time_memory_and_next_write_reserves(self):
        for which in ("time","memory","output"):
            root=self.root/which; root.mkdir()
            h=d.History(root/d.HISTORY); s=synthetic_study(root); h.add_study(s)
            b,op=adopted(root,h,s)
            state={"time":0,"memory":1}
            budget=CampaignBudget(root,op,root/"attempt",clock=lambda:state["time"],memory=lambda:state["memory"])
            if which=="time": state["time"]=60
            if which=="memory": state["memory"]=LIMITS["memory_bytes"]
            with self.assertRaises(IntegrityError): budget.save("must_not_write",{"x":"x"*LIMITS["new_output_bytes"]} if which=="output" else {})
            self.assertFalse((root/"attempt/must_not_write.json").exists())

    @unittest.skipIf((ROOT/d.APPROVED).is_file(), "Predecessor-only preparation; use the installed activation verifier")
    def test_prepared_integration_keeps_live_bytes_and_calls_both_guards(self):
        spec=importlib.util.spec_from_file_location("prepare_discovery",ROOT/"scripts/prepare_discovery.py")
        builder=importlib.util.module_from_spec(spec); spec.loader.exec_module(builder)
        # Preparation itself needs existing declared metadata, not source payloads.
        fingerprint=d.sha256_json([sha256_file(Path(d.__file__)),sha256_file(ROOT/"scripts/prepare_discovery.py"),sha256_file(ROOT/"src/futures_rebuild/discovery_attempt.py")])[:16]
        output=ROOT/"state/unpublished_evidence/discovery_loop_v1"/f"test_preparation_{fingerprint}"
        before={p:sha256_file(ROOT/p) for p in builder.SOURCES}
        prepared = builder.prepare(ROOT,output)
        self.assertEqual(prepared["historical_evaluation"]["decision"],"UNSUPPORTED")
        self.assertEqual(d.History(output/"history").records()[-1]["payload"]["execution"],"COMPLETE")
        generated_op={"destination":"generated/evaluation","operation_id":"generated","study_id":"generated"}
        self.assertEqual(builder.evaluation_metadata(self.root,generated_op)["execution"],"UNKNOWN")
        metadata=self.root/"generated/evaluation_task_v1"
        write_new_canonical_authority(metadata/"task_result.json",{"operation_id":"wrong"})
        write_new_canonical_authority(metadata/"final_checks.json",{"operation_id":"wrong"})
        with self.assertRaises(IntegrityError): builder.evaluation_metadata(self.root,generated_op)
        self.assertEqual(before,{p:sha256_file(ROOT/p) for p in builder.SOURCES})
        path=output/"candidate/src/futures_rebuild/research_gateway_policy.py"
        spec=importlib.util.spec_from_file_location("futures_rebuild._prepared_discovery_policy",path)
        module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        with patch.object(d,"require_adopted_scope",side_effect=UnauthorizedOperation("generated blocked")) as guard:
            with self.assertRaises(UnauthorizedOperation): module.require_current_real_history_operation("EXECUTE_ES_NQ_TRANSFER_ONCE",{})
            guard.assert_called_once()
        with patch.object(d,"require_adopted_scope") as guard:
            with self.assertRaises(UnauthorizedOperation): module.require_current_real_history_operation("UNKNOWN_OPERATION",{})
            guard.assert_called_once()

    def test_real_transfer_fits_generated_no_edge_and_lead_close_without_authority(self):
        folder=ROOT/"state/unpublished_evidence/l0_order_readiness_v1/es_nq_transfer_preparation_v1"
        with patch.object(sys,"path",[str(folder),*sys.path]):
            spec=importlib.util.spec_from_file_location("_discovery_transfer_fixture",folder/"test_transfer.py")
            fixture=importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)
        inputs,targets=fixture.generated_population()
        for mode in ("no_edge","lead"):
            root=self.root/mode; root.mkdir()
            h=d.History(root/d.HISTORY); s=synthetic_study(root,fixture.SID); h.add_study(s)
            limits={**LIMITS,"fits":16,"seconds":180,"memory_bytes":6*2**30,"new_output_bytes":64*2**20}
            b,op=adopted(root,h,s,limits)
            budget=CampaignBudget(root,op,root/"attempt")
            outcomes=deepcopy(targets)
            if mode=="no_edge":
                for day,item in outcomes.items():
                    for row in item['outcomes']:
                        if row['status']=='MATURED':
                            row.update(target_ticks=0.,entry_open_nano=0,exit_open_nano=0)
                    outcomes[day]=rebind(item,'target_id')
            original=fixture.s.fit_ols
            def counted(*args,**kwargs):
                budget.reserve_fit()
                return original(*args,**kwargs)
            with patch.object(fixture.s,'fit_ols',side_effect=counted):
                output=fixture.s.run_screen(inputs,outcomes,fixture.SID,budget.save,budget.check)
            budget.publish_transfer(output,data_kind="SYNTHETIC")
            budget.finish("COMPLETE")
            summary=json.loads((root/"attempt/study_summary.json").read_bytes())
            self.assertEqual(budget.fits,16)
            self.assertEqual(summary['completed'],{'fits':16,'windows':8})
            self.assertEqual(summary['decision'],'UNSUPPORTED' if mode=='no_edge' else 'PREDICTIVE_FINDING')
            self.assertFalse(summary['confirmation_authorized'])
            self.assertFalse(summary['automatic_continuation'])
            self.assertEqual(summary['data_scope']['data_kind'],'SYNTHETIC')
            actual=sum(p.stat().st_size for p in (root/'attempt').rglob('*') if p.is_file())+sum(p.stat().st_size for p in budget.external)
            self.assertEqual(actual,budget.used)

    @unittest.skipIf((ROOT/d.APPROVED).is_file(), "Predecessor-only preparation; use the installed activation verifier")
    def test_prepared_receipt_to_reader_with_generated_partitions(self):
        from futures_rebuild import es_nq_transfer_access as live, boundary
        folder=ROOT/live.PACKAGE
        spec=importlib.util.spec_from_file_location("_discovery_access_fixture",folder/"test_access.py")
        fixture=importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)
        spec=importlib.util.spec_from_file_location("prepare_discovery",ROOT/"scripts/prepare_discovery.py")
        builder=importlib.util.module_from_spec(spec); spec.loader.exec_module(builder)
        fingerprint=d.sha256_json([sha256_file(Path(d.__file__)),sha256_file(ROOT/"scripts/prepare_discovery.py"),sha256_file(ROOT/"src/futures_rebuild/discovery_attempt.py")])[:16]
        prepared=ROOT/"state/unpublished_evidence/discovery_loop_v1"/f"test_preparation_{fingerprint}"
        builder.prepare(ROOT,prepared)
        def module_at(name,path):
            spec=importlib.util.spec_from_file_location(name,path)
            module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            return module
        with fixture.fixture() as (root,op,access,gateway,issue,resolver):
            for name in (*builder.SOURCES[:2],"src/futures_rebuild/discovery.py","src/futures_rebuild/discovery_attempt.py"):
                source=prepared/"candidate"/name if name in builder.SOURCES else ROOT/name
                (root/name).parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(source,root/name)
            reader=module_at("futures_rebuild._discovery_reader",root/live.READER)
            policy=module_at("futures_rebuild._discovery_policy",root/live.POLICY)
            new=root/reader.PACKAGE; new.mkdir(parents=True)
            shutil.copyfile(root/live.PACKAGE/"source_bindings.json",new/"source_bindings.json")
            impl=live.read(root/live.PACKAGE/"implementation.json")
            for name in (*builder.SOURCES[:2],"src/futures_rebuild/discovery.py","src/futures_rebuild/discovery_attempt.py"):
                impl["bindings"][name]=sha256_file(root/name)
            impl["bindings"][(reader.PACKAGE/"source_bindings.json").as_posix()]=sha256_file(new/"source_bindings.json")
            impl=rebind(impl,"implementation_id"); fixture.write(new/"implementation.json",impl)
            op.update(implementation_id=impl["implementation_id"],destination=reader.MATERIALIZATION.as_posix())
            op=rebind(op,"operation_id"); fixture.write(new/"materialize_operation.json",op)
            access.update(reader_sha256=sha256_file(root/live.READER),implementation_sha256=sha256_file(new/"implementation.json"))
            access["operation_ids"]["materialize"]=op["operation_id"]
            access["operation_sha256"]["materialize"]=sha256_file(new/"materialize_operation.json")
            fixture.write(root/live.APPROVED,access)
            history=d.History(root/d.HISTORY)
            study=synthetic_study(root,op["study_id"])
            study["declaration"]={"path":(live.PREP/"study.json").as_posix(),"sha256":sha256_file(root/live.PREP/"study.json")}
            study=rebind(study,"study_summary_id"); history.add_study(study)
            limits={**op["limits"],"fits":0}; reviewed=batch(history,study,limits)
            fixture.write(root/d.APPROVED,d.bind({"state":"ADOPTED","module_sha256":sha256_file(Path(d.__file__)),
                "batch":reviewed,"operations":[{"operation":op["operation_name"],"scope":operation_scope(op),"reservation":limits}]},"adoption_id"))
            with patch.object(live,"require_scope",reader.require_scope),patch.object(boundary,"require_current_real_history_operation",policy.require_current_real_history_operation):
                receipt=issue(op)
                output=reader.execute_authorized(gateway,receipt,op)
                self.assertEqual(output["status"],"COMPLETE_RECONCILED")
                self.assertEqual(len(output["read_set"]),2610)
                self.assertEqual(resolver.call_count,10)
                records=history.records()
                self.assertEqual([r["kind"] for r in records],["STUDY","ATTEMPT","RESULT"])
                self.assertEqual(records[-1]["payload"]["summary"]["fits"],0)
                with self.assertRaises(UnauthorizedOperation): reader.execute_authorized(gateway,receipt,op)
                self.assertEqual(resolver.call_count,10)


if __name__ == "__main__":
    unittest.main(verbosity=2)
