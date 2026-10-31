"""从 pyproject 到三类发布产物及最低 Python 干净安装的真实验收。"""

from configparser import ConfigParser
from email.parser import Parser
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
from zipfile import ZipFile

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
import pytest

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def _run(command, *, cwd, environment=None):
    completed = subprocess.run(command, cwd=cwd, env=environment, capture_output=True,
                               text=True, encoding="utf-8", errors="replace", timeout=300)
    assert completed.returncode == 0, f"命令失败：{command}\n{completed.stdout}\n{completed.stderr}"
    return completed.stdout


def _requirement(value):
    parsed = Requirement(value)
    return (canonicalize_name(parsed.name), str(parsed.specifier), tuple(sorted(parsed.extras)),
            parsed.url, str(parsed.marker) if parsed.marker else None)


def _expected(project):
    dependencies = [_requirement(value) for value in project["dependencies"]]
    for extra, values in project["optional-dependencies"].items():
        for value in values:
            requirement = Requirement(value)
            marker = f'({requirement.marker}) and extra == "{extra}"' if requirement.marker else f'extra == "{extra}"'
            requirement.marker = None
            dependencies.append(_requirement(f"{requirement}; {marker}"))
    return {
        "name": canonicalize_name(project["name"]), "version": project["version"],
        "summary": project["description"], "requires_python": project["requires-python"],
        "dependencies": set(dependencies), "extras": set(project["optional-dependencies"]),
    }


def _metadata(text):
    value = Parser().parsestr(text)
    return {
        "name": canonicalize_name(value["Name"]), "version": value["Version"],
        "summary": value["Summary"], "requires_python": value["Requires-Python"],
        "dependencies": {_requirement(item) for item in value.get_all("Requires-Dist", [])},
        "extras": set(value.get_all("Provides-Extra", [])),
    }


@pytest.fixture(scope="module")
def release_artifacts(tmp_path_factory):
    root = tmp_path_factory.mktemp("release-ssot")
    builder = os.environ.get("RP_RELEASE_BUILD_PYTHON", sys.executable)
    payload = json.loads(_run([
        builder, str(ROOT / "tools/build_release_artifacts.py"),
        "--project", str(ROOT), "--output", str(root / "artifacts"),
    ], cwd=root))
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return root, payload, pyproject


@pytest.fixture(scope="module")
def minimum_installs(release_artifacts):
    root, payload, pyproject = release_artifacts
    python = os.environ.get("RP_MINIMUM_PYTHON", sys.executable)
    minimum = re.match(r">=([0-9]+\.[0-9]+)", pyproject["project"]["requires-python"])
    assert minimum is not None, "最低 Python 声明需提供可核实的下界"
    version = json.loads(_run([python, "-I", "-c", "import json,sys; print(json.dumps(list(sys.version_info[:2])))"], cwd=root))
    assert version == [int(value) for value in minimum.group(1).split(".")], "RP_MINIMUM_PYTHON 必须指向最低受支持 Python"
    wheelhouse_value = os.environ.get("RP_RELEASE_WHEELHOUSE")
    assert wheelhouse_value, "最低版本验收必须提供 RP_RELEASE_WHEELHOUSE 离线依赖；不能跳过真实安装"
    wheelhouse = Path(wheelhouse_value).resolve(strict=True)
    extracted = root / "source"
    with ZipFile(payload["source_archive"]) as archive:
        archive.extractall(extracted)
    source = extracted / "research_pipeline-source"
    environment = {key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME"}}
    environment["PIP_NO_INDEX"] = "1"
    environment["PIP_FIND_LINKS"] = str(wheelhouse)
    environment["PIP_NO_CACHE_DIR"] = "1"
    installs = []
    for label, artifact, extras in (
        ("wheel", payload["wheel"], "test"),
        ("sdist", payload["sdist"], "dev"),
        ("source", str(source), "test,dev"),
    ):
        directory = root / f"venv-{label}"
        _run([python, "-I", "-m", "venv", str(directory)], cwd=root, environment=environment)
        interpreter = directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        _run([str(interpreter), "-I", "-m", "pip", "install", "--no-index", "--find-links", str(wheelhouse),
              "--no-compile", *pyproject["build-system"]["requires"]], cwd=root, environment=environment)
        _run([str(interpreter), "-I", "-m", "pip", "install", "--no-build-isolation", "--no-index",
              "--find-links", str(wheelhouse), "--no-compile", f"{artifact}[{extras}]"], cwd=root, environment=environment)
        probe = _run([str(interpreter), "-I", "-c",
            "import importlib.metadata as m,json,research_pipeline,sys; d=m.distribution('quantwitness'); "
            "print(json.dumps({'metadata':d.read_text('METADATA'),'scripts':{e.name:e.value for e in d.entry_points if e.group=='console_scripts'},"
            "'package_path':research_pipeline.__file__,'prefix':sys.prefix,'base_prefix':sys.base_prefix}))"], cwd=root, environment=environment)
        info = json.loads(probe)
        assert Path(info["package_path"]).is_relative_to(directory)
        assert info["prefix"] != info["base_prefix"]
        installs.append((label, interpreter, info, environment, directory))
    return root, installs


def test_pyproject_wheel_sdist_and_installed_metadata_match(release_artifacts, minimum_installs):
    _, payload, pyproject = release_artifacts
    expected = _expected(pyproject["project"])
    with ZipFile(payload["wheel"]) as archive:
        metadata = next(name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
        assert _metadata(archive.read(metadata).decode("utf-8")) == expected
        entry_points = next(name for name in archive.namelist() if name.endswith(".dist-info/entry_points.txt"))
        parser = ConfigParser()
        parser.read_string(archive.read(entry_points).decode("utf-8"))
        assert dict(parser["console_scripts"]) == pyproject["project"]["scripts"]
    with tarfile.open(payload["sdist"], "r:gz") as archive:
        member = next(member for member in archive.getmembers() if member.name.endswith("/PKG-INFO")
                      and ".egg-info/" not in member.name)
        assert _metadata(archive.extractfile(member).read().decode("utf-8")) == expected
    for _, _, info, _, _ in minimum_installs[1]:
        assert _metadata(info["metadata"]) == expected
        assert info["scripts"] == pyproject["project"]["scripts"]


def test_minimum_supported_python_installs_test_extras_and_runs_smoke(minimum_installs):
    root, installs = minimum_installs
    smoke = root / "smoke"
    smoke.mkdir()
    for name in (
        "test_runtime_checkpoint.py",
        "test_simulation_semantics.py",
    ):
        shutil.copyfile(ROOT / "tests" / name, smoke / name)
    for _, python, _, environment, directory in installs:
        _run([str(python), "-I", "-m", "pip", "check"], cwd=root, environment=environment)
        _run([str(python), "-I", "-c", "import pytest,build,tomli; import research_pipeline"], cwd=root, environment=environment)
        cli = directory / ("Scripts/quantwitness.exe" if os.name == "nt" else "bin/quantwitness")
        assert "package" in _run([str(cli), "--help"], cwd=root, environment=environment)
        _run([str(python), "-I", "-m", "pytest", str(smoke), "-q"], cwd=root, environment=environment)


def test_release_build_has_no_setup_or_handwritten_metadata_source():
    assert not (ROOT / "setup.py").exists()
    source = (ROOT / "tools/build_release_artifacts.py").read_text(encoding="utf-8")
    for field in ("Metadata-Version:", "Requires-Python:", "Requires-Dist:", "[console_scripts]"):
        assert field not in source
    assert '"setup.py"' not in source


@pytest.mark.parametrize("field,replacement", [
    ("Name", "wrong-project"), ("Version", "99.0.0"), ("Summary", "wrong summary"),
    ("Requires-Python", ">=3.99"), ("Requires-Dist", "wrong-dependency"),
    ("Provides-Extra", "wrong-extra"), ("entry_points", "wrong.module:main"),
])
def test_backend_metadata_drift_is_rejected_before_release(release_artifacts, tmp_path, field, replacement):
    _, payload, _ = release_artifacts
    sys.path.insert(0, str(ROOT / "tools"))
    try:
        from release_metadata import verify_distribution_metadata
    finally:
        sys.path.pop(0)
    forged = tmp_path / "changed.whl"
    with ZipFile(payload["wheel"]) as original, ZipFile(forged, "w") as target:
        for item in original.infolist():
            content = original.read(item.filename)
            if item.filename.endswith(".dist-info/METADATA") and field != "entry_points":
                text = content.decode("utf-8")
                text, count = re.subn(rf"(?m)^{re.escape(field)}:.*$", f"{field}: {replacement}", text, count=1)
                assert count == 1
                content = text.encode("utf-8")
            elif item.filename.endswith(".dist-info/entry_points.txt") and field == "entry_points":
                content = content.replace(b"research_pipeline.cli:main", replacement.encode())
            target.writestr(item, content)
    with pytest.raises(ValueError, match="发布元数据与 pyproject 不一致"):
        verify_distribution_metadata(project=ROOT, wheel=forged, sdist=Path(payload["sdist"]))
