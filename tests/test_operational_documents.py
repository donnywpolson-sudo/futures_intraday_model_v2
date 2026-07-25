from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_root_operational_documents_are_steady_state_and_v2_owned() -> None:
    documents = [
        "AGENTS.md",
        "PROJECT_OUTLINE.md",
        "README.md",
        "MASTER_AUDIT.md",
        "META_MASTER_AUDIT.md",
    ]
    combined = "\n".join(_text(path) for path in documents)
    assert "REBUILD_COMPLETE" not in combined
    assert "REBUILD_IN_PROGRESS" not in combined
    assert r"C:\Users\donny\Desktop\futures_intraday_model" not in combined
    assert "FOUNDATION_READY" in combined
    assert "OBSERVATION_COCKPIT_READY" in combined


def test_root_master_is_canonical_and_versioned_copy_is_redirect_only() -> None:
    master = _text("MASTER_AUDIT.md")
    redirect = _text("docs/MASTER_AUDIT_V3.md")
    assert "canonical audit specification" in master
    assert "This file is non-authoritative" in redirect
    assert "../MASTER_AUDIT.md" in redirect
    assert len(redirect) < 1000


def test_meta_audit_is_blind_first_and_has_strict_high_severity_closure() -> None:
    meta = _text("META_MASTER_AUDIT.md")
    blind = meta.index("Before reading `MASTER_AUDIT.md` closely")
    reconcile = meta.index("Only then read the Master Audit")
    assert blind < reconcile
    for required in (
        "false-pass",
        "Critical",
        "High",
        "P0",
        "P1",
        "standalone operation",
        "secret exposure",
        "stale hashes",
    ):
        assert required in meta
    assert (
        "no unresolved Critical/High or P0/P1 item remains"
        in " ".join(meta.split())
    )


def test_master_audit_explicitly_covers_cockpit_false_pass_paths() -> None:
    master = _text("MASTER_AUDIT.md")
    for required in (
        "all 41 approved markets",
        "observation-only architecture",
        "provider error handling",
        "state/cache bounds",
        "packaged self-check",
        "shortcut targets",
        "rollback",
        "credential filenames",
    ):
        assert required.lower() in master.lower()
