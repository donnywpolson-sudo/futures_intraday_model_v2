import importlib.metadata
import json
import platform
import re
import sys
from pathlib import Path

from futures_rebuild.canonical import sha256_file, sha256_json


def test_dependency_lock_receipt_matches_exact_files_and_running_environment() -> None:
    root = Path(__file__).parents[1]
    path = root / "configs" / "dependency_lock_receipt.json"
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert set(receipt) == {"files", "receipt_id", "receipt_version", "runtime"}
    assert receipt["receipt_version"] == "1.1.0"
    assert isinstance(receipt["files"], list) and receipt["files"]
    assert receipt["files"] == sorted(receipt["files"], key=lambda item: item["path"])
    assert all(set(item) == {"path", "sha256"} for item in receipt["files"])
    assert len({item["path"] for item in receipt["files"]}) == len(receipt["files"])
    for item in receipt["files"]:
        relative = Path(item["path"])
        assert not relative.is_absolute() and ".." not in relative.parts
        assert re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
        assert sha256_file(root / relative) == item["sha256"]
    core = {key: receipt[key] for key in receipt if key != "receipt_id"}
    assert sha256_json(core) == receipt["receipt_id"]

    runtime = receipt["runtime"]
    assert set(runtime) == {"implementation", "packages", "platform", "python"}
    assert runtime["implementation"] == platform.python_implementation()
    assert runtime["platform"] == sys.platform
    assert runtime["python"] == platform.python_version()
    assert len(runtime["packages"]) == 45
    assert runtime["packages"] == {
        package: importlib.metadata.version(package)
        for package in runtime["packages"]
    }

    unhashed = [
        line.strip()
        for line in (root / "requirements.lock").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    hashed_text = (root / "requirements.sha256.lock").read_text(encoding="utf-8")
    hashed = [
        line.strip()
        for line in hashed_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert len(unhashed) == len(hashed) == 45
    assert len(set(unhashed)) == 45
    assert [line.split(" --hash=", 1)[0] for line in hashed] == unhashed
    assert all(
        re.fullmatch(r"[^\s=]+==[^\s=]+ --hash=sha256:[0-9a-f]{64}", line)
        for line in hashed
    )
    target_artifacts = [
        line.split(": ", 1)[1]
        for line in hashed_text.splitlines()
        if line.startswith(("# target-wheel: ", "# target-sdist: "))
    ]
    assert len(target_artifacts) == 45 and len(set(target_artifacts)) == 45
    assert sum(name.endswith(".whl") for name in target_artifacts) == 44
    assert target_artifacts.count("proxy_tools-0.1.0.tar.gz") == 1

    environment = json.loads(
        (root / "configs" / "environment.lock.json").read_text(encoding="utf-8")
    )
    closure = environment["complete_binary_closure"]
    assert closure == {
        "install_policy": "--require-hashes",
        "package_count": 45,
        "requirements_path": "requirements.sha256.lock",
        "requirements_sha256": sha256_file(root / "requirements.sha256.lock"),
    }
    offline = json.loads(
        (root / "configs" / "offline_vault_environment.lock.json").read_text(
            encoding="utf-8"
        )
    )
    assert offline["package_count"] == 45
    assert offline["requirements_sha256"] == sha256_file(
        root / "requirements.sha256.lock"
    )
    assert offline["packages"] == unhashed

    wheel_lock = json.loads(
        (root / "configs" / "runtime_wheel_lock.json").read_text(encoding="utf-8")
    )
    assert set(wheel_lock) == {"artifacts", "lock_version", "platform", "python"}
    assert wheel_lock["lock_version"] == "1.0.0"
    assert wheel_lock["platform"] == "win32" and wheel_lock["python"] == "3.11"
    assert wheel_lock["artifacts"] == [
        {
            "filename": "numpy-2.4.4-cp311-cp311-win_amd64.whl",
            "package": "numpy",
            "sha256": "6bbe4eb67390b0a0265a2c25458f6b90a409d5d069f1041e6aff1e27e3d9a79e",
            "size": 12_614_257,
            "source": (
                "https://files.pythonhosted.org/packages/bd/63/05d193dbb4b5eec1eca73822d80da98b511f8328ad4ae3ca4caf0f4db91d/"
                "numpy-2.4.4-cp311-cp311-win_amd64.whl"
            ),
            "version": "2.4.4",
            "wheel_tag": "cp311-cp311-win_amd64",
        }
    ]
    runtime_requirements = (root / "requirements-runtime.lock").read_text(
        encoding="utf-8"
    )
    assert "numpy==2.4.4" in runtime_requirements
    assert wheel_lock["artifacts"][0]["sha256"] in runtime_requirements
