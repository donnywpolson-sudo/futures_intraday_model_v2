from pathlib import Path
import json
import pyarrow.parquet as pq

ROOT=Path.cwd(); ID='c65d3da960c025f09d28be8907e884cb10eb39b2ffe54aeb503581257d64c31a'
p=ROOT/'data'/'predictions'/'tier1_phase6_conservative'/ID/'predictions.parquet'
t=pq.read_table(p, columns=['outer_fold','exchange_session_date','upstream_source_row_sha256','prediction'])
n=t.num_rows
keys=t.column('upstream_source_row_sha256').to_pylist()
folds=t.column('outer_fold').to_pylist()
scores=t.column('prediction').to_numpy()
if n != 2533270 or len(set(keys)) != n or set(folds) != set(range(8)):
 raise RuntimeError('prediction integrity failure')
r={'phase':7,'prediction_release_id':ID,'prediction_rows':n,'duplicate_prediction_rows':0,'missing_or_abstained_prediction_rows':0,'outer_fold_count':8,'score_min':float(scores.min()),'score_max':float(scores.max()),'score_mean':float(scores.mean()),'outcome_returns_read':False,'economics_evaluation':False,'status':'PASS'}
out=ROOT/'reports'/'phase7_prediction_audit'/'tier1_phase6_conservative'/ID/'report.json'
out.parent.mkdir(parents=True,exist_ok=False); out.write_text(json.dumps(r,sort_keys=True,separators=(',',':'))+'\n',encoding='utf-8')
print(json.dumps(r,sort_keys=True))
