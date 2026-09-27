"""Create a generated result preview using the prepared cockpit renderer."""
import argparse
import json
from pathlib import Path

from futures_rebuild import discovery as d
from futures_rebuild.canonical import sha256_file, write_new_canonical_authority
from prepare_discovery import prepare, write_fresh


def generate(root, output):
    prepare(root, output)
    study = d.History(output / "history").records()[1]["payload"]
    summaries = []
    scope = {"start": "2018-01-01", "end": "2022-12-31", "markets": ["ES", "NQ"], "data_kind": "SYNTHETIC"}
    for name, execution, count, passed in (("unsupported", "COMPLETE", 16, False),
            ("predictive lead", "COMPLETE", 16, True), ("partial", "INCOMPLETE", 6, None),
            ("mechanical failure", "FAILED", 0, None)):
        summary = d.result_summary(study, execution=execution, completed={"fits": count, "windows": count//2},
            expected={"fits": 16, "windows": 8}, primary_pass=passed, evidence_complete=execution=="COMPLETE",
            power_sufficient=execution=="COMPLETE", data_scope=scope,
            metrics=[{"name": "Generated benchmark MSE improvement", **d.measured(2. if passed else -2., "ticks squared")}],
            uncertainty={"scope": "Handwritten demonstration only; not an inference calculation"},
            reason="SYNTHETIC DISPLAY EXAMPLE: " + name + "; not ES/NQ historical performance.",
            attempts=["generated/"+name])
        summaries.append(summary)
    catalog=d.bind({"schema_version":d.CATALOG_SCHEMA,"payload_kind":"METADATA_ONLY","summaries":summaries},"catalog_id")
    write_new_canonical_authority(output/"synthetic_catalog.json",catalog)
    write_fresh(output/"synthetic_results.md", "# Generated lifecycle examples\n\n"+"\n".join(d.markdown(r) for r in summaries))
    assets=root/"src/futures_rebuild/live_cockpit/assets"
    html=(assets/"index.html").read_text(encoding="utf-8")
    html=html.replace('<link rel="stylesheet" href="styles.css?v=chart-first-2" />',"<style>"+(assets/"styles.css").read_text(encoding="utf-8")+"</style>")
    renderer=(output/"candidate/src/futures_rebuild/live_cockpit/assets/candidate-reports.js").read_text(encoding="utf-8")
    view=d.load_catalog(output/"synthetic_catalog.json")
    html=html.replace('<script src="candidate-reports.js"></script>',"<script>window.l0CandidateReportDemo="+json.dumps(view,allow_nan=False).replace("<","\\u003c")+";</script><script>"+renderer+"</script>")
    for script in ("lightweight-charts.standalone.production.js","time-format.js","app.js?v=chart-first-2"):
        html=html.replace(f'<script src="{script}"></script>',"")
    html=html.replace("<title>Futures Live Cockpit</title>","<title>Discovery lifecycle — synthetic preview</title>")
    write_fresh(output/"preview/index.html",html)
    return {"preview":str(output/"preview/index.html"),"catalog":str(output/"synthetic_catalog.json"),
            "renderer_sha256":sha256_file(output/"candidate/src/futures_rebuild/live_cockpit/assets/candidate-reports.js")}


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(generate(Path(__file__).resolve().parents[1],args.output.resolve()),indent=2))
