from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
ACTIVATOR = REPO / "scripts" / "activate_live_cockpit.ps1"


def _activator_text() -> str:
    return ACTIVATOR.read_text(encoding="utf-8")


def test_activation_accepts_any_nonempty_preserved_shortcut_subset() -> None:
    text = _activator_text()
    assert "@($rollback.shortcuts).Count -eq 0" in text
    assert "@($rollback.shortcuts).Count -ne 2" not in text
    assert "replace preserved shortcuts" in text
    assert "replace both preserved shortcuts" not in text
    assert "Prepared shortcut rollback metadata is invalid." in text


def test_activation_updates_only_captured_shortcuts_and_rolls_back() -> None:
    text = _activator_text()
    assert "foreach ($record in @($rollback.shortcuts))" in text
    assert "Required prior shortcut is missing" in text
    assert "$shell.CreateShortcut([string]$record.Path)" in text
    assert "$shortcut.TargetPath = $preparedExe" in text
    assert "$shortcut.WorkingDirectory = $preparedPath" in text
    assert "Shortcut verification failed" in text
    assert "Restore-ShortcutRecord -Shell $shell -Record $record" in text
    assert "Cutover failed and shortcut rollback verification failed" in text
    assert "RollbackVerified = $true" in text


def test_activation_remains_guarded_and_observation_only() -> None:
    text = _activator_text()
    assert "[Parameter(Mandatory = $true)][string]$LiveSmokePlan" in text
    assert "$planPath = (Resolve-Path -LiteralPath $LiveSmokePlan).Path" in text
    assert "configs\\live_cockpit_smoke_plan.json" not in text
    assert "-m futures_rebuild.live_cockpit.cutover_guard" in text
    assert "Cutover guard rejected the prepared installation" in text
    assert "Refusing to cut over while an auto-start shortcut exists" in text
    assert "Unexpected auto-start shortcut was created" in text
    assert "CredentialCopied = $false" in text
    assert "AutoStartCreated = $false" in text
    assert "DATABENTO_API_KEY" not in text
    assert "api.env" not in text
    assert "Start-Process" not in text
    assert "Invoke-WebRequest" not in text
    assert "git push" not in text
    assert text.count("futures_rebuild.") == 1
