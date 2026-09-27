"""Run the v1.2 synthetic engine; never reads market or provider data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[variable] = "1"

from futures_rebuild.intraday_alpha_pipeline_v12 import (
    benchmark_synthetic_configuration,
    build_failure_record,
    build_program,
    run_synthetic_certification,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--program", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--run-ordinal", type=int)
    parser.add_argument("--null-campaigns", type=int)
    parser.add_argument("--known-edge-campaigns", type=int)
    parser.add_argument("--chunk-size", type=int)
    parser.add_argument("--resamples", type=int)
    parser.add_argument("--exact", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.benchmark:
        if args.exact or any(value is not None for value in (args.manifest, args.run_root, args.run_ordinal, args.null_campaigns, args.known_edge_campaigns, args.chunk_size, args.resamples)):
            parser.error("benchmark mode accepts only optional --program and --output")
        if args.program is None:
            program = build_program(ROOT, build_failure_record(ROOT))
        else:
            program_path = (ROOT / args.program).resolve(strict=True)
            program_path.relative_to(ROOT.resolve(strict=True))
            program = json.loads(program_path.read_bytes())
        result = benchmark_synthetic_configuration(program)
        encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        if args.output is not None:
            output = (ROOT / args.output).resolve(strict=False)
            output.relative_to(ROOT.resolve(strict=True))
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("xb") as stream:
                stream.write(encoded)
        print(encoded.decode().rstrip())
        return 0
    manifest = None
    run_root = None
    if args.exact:
        if args.manifest is None or args.run_root is None or args.run_ordinal is None or args.program is not None:
            parser.error("exact mode requires --manifest, --run-root, and --run-ordinal and rejects --program")
        manifest_path = (ROOT / args.manifest).resolve(strict=True)
        manifest_path.relative_to(ROOT.resolve(strict=True))
        manifest = json.loads(manifest_path.read_bytes())
        program_path = (ROOT / manifest["program"]["path"]).resolve(strict=True)
        run_root = (ROOT / args.run_root).resolve(strict=False)
        run_root.relative_to((ROOT / "state/intraday_research_synthetic_run_registry").resolve(strict=False))
    else:
        if args.program is None or None in (args.null_campaigns, args.known_edge_campaigns, args.chunk_size, args.resamples):
            parser.error("smoke mode requires --program and all explicit scientific settings")
        program_path = (ROOT / args.program).resolve(strict=True)
    program_path.relative_to(ROOT.resolve(strict=True))
    specification = json.loads(program_path.read_bytes())
    if args.exact:
        for relative, expected in specification["bindings"]["implementation"].items():
            if hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() != expected:
                raise SystemExit(f"implementation binding mismatch: {relative}")
    result = run_synthetic_certification(
        specification,
        manifest=manifest,
        run_root=run_root,
        run_ordinal=args.run_ordinal,
        null_campaigns=args.null_campaigns,
        known_edge_campaigns=args.known_edge_campaigns,
        chunk_size=args.chunk_size,
        resamples=args.resamples,
        exact=args.exact,
    )
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    if args.output is not None:
        output = (ROOT / args.output).resolve(strict=False)
        output.relative_to(ROOT.resolve(strict=True))
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as stream:
            stream.write(encoded)
    print(encoded.decode().rstrip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
