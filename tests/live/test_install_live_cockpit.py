from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
INSTALLER = REPO / "scripts" / "install_live_cockpit.ps1"


def _installer_text() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_upgrade_accepts_any_nonempty_owned_shortcut_subset() -> None:
    text = _installer_text()
    assert "$Upgrade -and $existingShortcuts.Count -eq 0" in text
    assert "requires at least one existing cockpit shortcut" in text
    assert "$existingShortcuts.Count -ne $shortcutPaths.Count" not in text
    assert "requires both existing cockpit shortcuts" not in text
    assert "Use -Upgrade when cockpit shortcuts already exist." in text


def test_existing_shortcuts_remain_owned_unchanged_and_non_startup() -> None:
    text = _installer_text()
    assert "foreach ($shortcutPath in $existingShortcuts)" in text
    assert "Assert-ChildPath -Parent $installRoot -Child $record.TargetPath" in text
    assert "[IO.Path]::GetFileName($record.TargetPath)" in text
    assert "Existing shortcut is not cockpit-owned" in text
    assert "foreach ($record in $shortcutRecords)" in text
    assert "Shortcut changed during preparation" in text
    assert "Refusing to proceed while an auto-start shortcut exists" in text
    assert "Unexpected auto-start shortcut was created" in text


def test_installation_is_hash_bound_and_uses_isolated_offline_state() -> None:
    text = INSTALLER.read_text(encoding="utf-8")
    assert "[string]$ExpectedExecutableSha256" in text
    assert "$sourceHash -ne $expectedHash" in text
    assert "$stagedHash -ne $expectedHash" in text
    assert "$preparedHash -ne $expectedHash" in text
    assert "'_offline_validation'" in text
    assert "$env:LOCALAPPDATA = $isolatedSelfCheckLocalAppData" in text
    assert "ExecutableSha256 = $preparedHash" in text
    assert "ShortcutsChanged = $false" in text
    assert "AutoStartCreated = $false" in text


def test_installation_remains_side_by_side_and_credential_copy_free() -> None:
    text = _installer_text()
    assert "Version is already installed" in text
    assert "Move-Item -LiteralPath $stagingPath -Destination $installPath" in text
    assert "credential-source.json" in text
    assert "CredentialCopied = $false" in text
    assert "Packaged self-check exceeded 60 seconds" in text
    assert "Packaged self-check failed with exit code" in text
