"""Bounded, generated ES stream; no historical input or activation command."""
import argparse
from dataclasses import replace
from pathlib import Path
import json
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'src')]
from tests.test_causal_1s import sample,spec,BASE,DAY,SECOND,RULEBOOK
from futures_rebuild.causal_1s import build_candidate,certify_candidate
from futures_rebuild.causal_timeframe_storage import Limits,contained


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--days',type=int,choices=(1,2,4),required=True)
    parser.add_argument('--rows-per-day',type=int,default=86340)
    args=parser.parse_args()
    if not 1<=args.rows_per_day<=86340: parser.error('rows per day must be 1..86340')
    if args.output.exists(): parser.error('output must be absent; generated artifacts are create-only')
    args.output.mkdir(parents=True)
    limits=Limits(512*2**20,3*2**30,1800)
    invocation=0
    def source():
        nonlocal invocation
        invocation+=1
        for i in range(args.days):
            day=sample(BASE+i*DAY,n=args.rows_per_day)
            refs=tuple(replace(day.bars[j],volume=600,source_file_path='synthetic/ohlcv_1m.dbn.zst')
                       for j in range(0,len(day.bars)-59,60))
            print(f'generated pass={invocation} day={i+1}/{args.days} rows={len(day.bars)}',flush=True)
            yield replace(day,references={'1m':refs,'1h':(),'1d':()})
            del day,refs
    scope=spec(end=BASE+args.days*DAY)
    candidate,build=build_candidate(args.output,scope,source,RULEBOOK,limits)
    certificate,verify=certify_candidate(args.output,scope,source,RULEBOOK,limits)
    payloads=list(args.output.rglob('*.parquet'))
    summary={'kind':'GENERATED_SYNTHETIC_REHEARSAL_NOT_HISTORICAL_ESTIMATE',
        'days':args.days,'source_rows_per_day':args.rows_per_day,
        'observations':candidate['observations'],'full_reconstruction_passes':len(certificate['passes']),
        'payload_files':len(payloads),'payload_bytes':sum(p.stat().st_size for p in payloads),
        'build':build,'verification':verify,'source_days_in_memory':1}
    contained(args.output,'rehearsal.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2),flush=True)


if __name__=='__main__': main()
