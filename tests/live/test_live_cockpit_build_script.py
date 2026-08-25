from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts" / "build_live_cockpit.ps1"


def test_build_script_uses_disposable_pyinstaller_work_tree() -> None:
    script = BUILD_SCRIPT.read_text(encoding="utf-8")

    assert "$workPath = Join-Path $stagingRoot 'pyinstaller-work'" in script
    assert "--workpath $workPath `" in script
    assert (
        "$buildRootEntries = @(Get-ChildItem -LiteralPath $buildRoot -Force)"
        in script
    )
    assert "if ($buildRootEntries.Count -eq 0)" in script
    assert "Remove-Item -LiteralPath $buildRoot -Force" in script
    assert "Remove-Item -LiteralPath $buildRoot -Recurse" not in script


def test_default_build_publishes_only_the_canonical_repository_package() -> None:
    script = BUILD_SCRIPT.read_text(encoding="utf-8")

    assert "$publishPath = Join-Path $repoRoot 'FuturesLiveCockpit'" in script
    assert "$resultPath = $publishPath" in script
    assert "'CANONICAL_PACKAGE_PUBLISHED'" in script
    assert "local_appdata_installation_changed = $false" in script
    assert "shortcut_changes = 0" in script
    assert "WScript.Shell" not in script
    assert "CreateShortcut" not in script
