from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable
from futures_rebuild.active_phase3_input import load_active_phase3_input
from futures_rebuild.active_phase3_outcomes import build_active_phase3_outcomes
from futures_rebuild.active_phase3_validation import ActivePhase3MechanicsValidation
from futures_rebuild.active_phase4_features import ActivePhase4FeatureBinding, build_active_phase4_features
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.current_research_surface import reject_retired_project_execution

MARKETS = ("ES", "CL", "ZN", "6E")
YEARS = tuple(range(2018, 2023))
CORE_PAIRS = tuple((market, year) for market in MARKETS for year in YEARS)


def _release_state(
    *, boundary: RepoBoundary, market: str, year: int, kind: str
) -> str:
    """Return absent, complete, or fail closed on an incomplete release tree."""
    if kind == "outcomes":
        base = boundary.active_root / "data" / "outcomes" / "active_es_60s_300s_v2"
        payload_name = "outcomes.parquet"
        manifest_root = boundary.active_root / "manifests" / "data_releases" / "outcomes"
        report_root = boundary.active_root / "reports" / "phase3_outcomes"
    elif kind == "features":
        base = boundary.active_root / "data" / "features" / "active_es_mechanical_v3"
        payload_name = "features.parquet"
        manifest_root = boundary.active_root / "manifests" / "data_releases" / "features"
        report_root = boundary.active_root / "reports" / "phase4_features"
    else:
        raise ValueError(f"unsupported release kind: {kind}")
    release_root = base / market / str(year) / str(year)
    if not release_root.exists():
        return "absent"
    releases = tuple(path for path in release_root.iterdir() if path.is_dir())
    if len(releases) != 1:
        raise RuntimeError(f"{kind} release tree is partial or ambiguous for {market}-{year}")
    release_id = releases[0].name
    payload = releases[0] / payload_name
    manifest = manifest_root / f"{release_id}.json"
    reports = tuple(report_root.glob(f"**/{release_id}/report.json"))
    if payload.is_file() and manifest.is_file() and len(reports) == 1:
        return "complete"
    raise RuntimeError(f"{kind} release tree is partial for {market}-{year}")


def _parse_pairs(values: Iterable[str] | None) -> tuple[tuple[str, int], ...]:
    if values is None:
        return CORE_PAIRS
    pairs: list[tuple[str, int]] = []
    for value in values:
        try:
            market, year_text = value.rsplit("-", 1)
            pair = (market, int(year_text))
        except (AttributeError, ValueError) as exc:
            raise ValueError(f"invalid market-year pair: {value!r}") from exc
        if pair not in CORE_PAIRS or pair in pairs:
            raise ValueError(f"unsupported or duplicate Tier 1 pair: {value!r}")
        pairs.append(pair)
    if not pairs:
        raise ValueError("at least one Tier 1 pair is required")
    return tuple(pairs)

def run_pairs(*, boundary: RepoBoundary, pairs: Iterable[tuple[str, int]]) -> list[dict[str, object]]:
    reject_retired_project_execution(
        root=boundary.active_root,
        surface="legacy Tier 1 Phase 3/4 foundation runner",
    )
    spec_path = boundary.active_root / "configs/mechanical_feature_spec.json"
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec_hash = sha256_file(spec_path)
    results = []
    for market, year in pairs:
        active = load_active_phase3_input(boundary=boundary, market=market, year=year)
        direct_binding = sha256_json(active.core())
        validation = ActivePhase3MechanicsValidation(
            active, "manifests/phase3_inputs/direct_active_binding.json", direct_binding
        )
        phase3_state = _release_state(
            boundary=boundary, market=market, year=year, kind="outcomes"
        )
        phase4_state = _release_state(
            boundary=boundary, market=market, year=year, kind="features"
        )
        phase3 = None if phase3_state == "complete" else build_active_phase3_outcomes(
            boundary=boundary, validation=validation
        )
        phase4 = None if phase4_state == "complete" else build_active_phase4_features(
            boundary=boundary,
            binding=ActivePhase4FeatureBinding(
                active,
                "manifests/phase3_inputs/direct_active_binding.json",
                direct_binding,
                "configs/mechanical_feature_spec.json",
                spec_hash,
                spec,
            ),
        )
        results.append(
            {
                "market": market,
                "year": year,
                "phase3": None if phase3 is None else phase3["release_id"],
                "phase4": None if phase4 is None else phase4["release_id"],
            }
        )
        print(json.dumps(results[-1], sort_keys=True), flush=True)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resume bounded Tier 1 foundation builds.")
    parser.add_argument("--pairs", nargs="+", metavar="MARKET-YEAR")
    args = parser.parse_args(argv)
    boundary = RepoBoundary(active_root=Path.cwd())
    run_pairs(boundary=boundary, pairs=_parse_pairs(args.pairs))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
