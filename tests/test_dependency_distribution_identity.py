"""依赖分发身份 v2：只忽略安装器产物，保留可再分发内容。"""

from __future__ import annotations

import base64
from copy import deepcopy
import csv
import hashlib
from importlib import metadata
import io
import json
import os
from pathlib import Path
import platform
import sys
import sysconfig

import pytest

from research_pipeline.platform import build_manifest


DIST_INFO = "demo_pkg-1.0.dist-info"
ENTRY_POINTS = (
    "[console_scripts]\ndemo-cli = demo_pkg.cli:main\n"
    "[gui_scripts]\ndemo-gui = demo_pkg.gui:main\n"
    "[demo.plugins]\ndemo-plugin = demo_pkg.plugin:load\n"
)


def _record(path: str, content: bytes) -> tuple[str, str, str]:
    """用真实 RECORD 的摘要格式表达文件内容，不建立实体包文件。"""
    digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=")
    return path, "sha256=" + digest.decode("ascii"), str(len(content))


class FakeDistribution(metadata.Distribution):
    """复用标准库的 metadata、files 和 entry_points 解析，只替换存储。"""

    def __init__(self, root: Path, *, unix_layout: bool = False) -> None:
        self.site_packages = root / (
            "lib/python3.10/site-packages" if unix_layout else "Lib/site-packages"
        )
        self.scripts = root / ("bin" if unix_layout else "Scripts")
        self._path = self.site_packages / DIST_INFO
        self.texts = {
            "METADATA": "Metadata-Version: 2.1\nName: demo-pkg\nVersion: 1.0\n\n示例分发包\n",
            "entry_points.txt": ENTRY_POINTS,
        }
        self.records = [
            _record("demo_pkg/__init__.py", b"VALUE = 1\n"),
            _record("demo_pkg/_native.pyd", b"native-extension"),
            _record("demo_pkg/data/table.csv", b"name,value\nx,1\n"),
            _record(f"{DIST_INFO}/METADATA", self.texts["METADATA"].encode("utf-8")),
            _record(f"{DIST_INFO}/entry_points.txt", ENTRY_POINTS.encode("utf-8")),
            (f"{DIST_INFO}/RECORD", "", ""),
        ]

    def read_text(self, filename: str) -> str | None:
        if filename == "RECORD":
            stream = io.StringIO(newline="")
            csv.writer(stream, lineterminator="\n").writerows(self.records)
            return stream.getvalue()
        return self.texts.get(filename)

    def locate_file(self, path: str | metadata.PackagePath) -> Path:
        return self.site_packages / path

    def script_path(self, name: str) -> str:
        return Path(os.path.relpath(self.scripts / name, self.site_packages)).as_posix()


@pytest.fixture
def identity(monkeypatch: pytest.MonkeyPatch):
    """让生产入口读取当前假安装，并按该安装的 scripts 目录判断包装器。"""
    original_get_path = sysconfig.get_path
    original_get_paths = sysconfig.get_paths

    def calculate(distribution: FakeDistribution) -> tuple[str, str, str]:
        def find_distribution(name: str) -> FakeDistribution:
            assert name == "demo-pkg"
            return distribution

        def get_path(name: str, *args, **kwargs):
            if name == "scripts":
                return str(distribution.scripts)
            return original_get_path(name, *args, **kwargs)

        def get_paths(*args, **kwargs):
            return {**original_get_paths(*args, **kwargs), "scripts": str(distribution.scripts)}

        monkeypatch.setattr(build_manifest.metadata, "distribution", find_distribution)
        monkeypatch.setattr(sysconfig, "get_path", get_path)
        monkeypatch.setattr(sysconfig, "get_paths", get_paths)
        return build_manifest.installed_distribution_digest("demo-pkg")

    return calculate


def test_identity_preserves_public_return_shape(tmp_path: Path, identity) -> None:
    name, version, digest = identity(FakeDistribution(tmp_path))
    assert (name, version) == ("demo-pkg", "1.0")
    assert len(digest) == 64
    assert set(digest) <= set("0123456789abcdef")


@pytest.mark.parametrize("marker", ["INSTALLER", "REQUESTED", "direct_url.json"])
def test_target_installation_markers_do_not_change_identity(tmp_path: Path, identity, marker: str) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    distribution.records.append(_record(f"{DIST_INFO}/{marker}", b"first-install"))
    assert identity(distribution) == baseline
    distribution.records[-1] = _record(f"{DIST_INFO}/{marker}", b"another-install-location")
    assert identity(distribution) == baseline


def test_record_order_and_self_record_do_not_change_identity(tmp_path: Path, identity) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    distribution.records[-1] = _record(f"{DIST_INFO}/RECORD", b"installer-record")
    distribution.records.reverse()
    assert identity(distribution) == baseline


@pytest.mark.parametrize(
    "path", ["demo_pkg/__pycache__/cli.cpython-310.pyc", "demo_pkg/cli.pyc", "demo_pkg/cli.pyo"],
)
def test_generated_bytecode_records_do_not_change_identity(tmp_path: Path, identity, path: str) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    distribution.records.append((path, "", ""))
    assert identity(distribution) == baseline
    # NumPy 的真实安装清单会重复列出无摘要、无大小的生成字节码。
    distribution.records.append((path, "", ""))
    assert identity(distribution) == baseline


@pytest.mark.parametrize("suffix", [".pyc", ".pyo"])
def test_distributed_bytecode_hash_is_preserved(tmp_path: Path, identity, suffix: str) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    path = f"demo_pkg/compiled{suffix}"
    distribution.records.append(_record(path, b"distributed-bytecode"))
    with_bytecode = identity(distribution)
    assert with_bytecode[2] != baseline[2]
    distribution.records[-1] = _record(path, b"changed-bytecode")
    assert identity(distribution)[2] != with_bytecode[2]


@pytest.mark.parametrize("entry", ["demo-cli", "demo-gui"])
@pytest.mark.parametrize("suffix", ["", ".exe", "-script.py", "-script.pyw"])
def test_declared_wrappers_ignore_install_paths_and_bytes(
    tmp_path: Path, identity, entry: str, suffix: str,
) -> None:
    first = FakeDistribution(tmp_path / "environment-a")
    second = FakeDistribution(tmp_path / "environment-b", unix_layout=True)
    baseline = identity(first)
    first.records.append(_record(first.script_path(entry + suffix), b"first-python-path"))
    second.records.append(_record(second.script_path(entry + suffix), b"different-python-path"))
    assert first.records[-1][0] != second.records[-1][0]
    assert identity(first) == identity(second) == baseline


@pytest.mark.parametrize("index", [0, 1, 2], ids=["source", "binary", "data"])
@pytest.mark.parametrize("field", [1, 2], ids=["hash", "size"])
def test_distribution_content_record_changes_identity(
    tmp_path: Path, identity, index: int, field: int,
) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    row = list(distribution.records[index])
    row[field] = _record(row[0], b"replacement")[1] if field == 1 else str(int(row[2]) + 1)
    distribution.records[index] = tuple(row)
    assert identity(distribution)[2] != baseline[2]


def test_metadata_text_is_bound_even_with_unchanged_record(tmp_path: Path, identity) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    distribution.texts["METADATA"] += "新的分发描述\n"
    assert identity(distribution)[2] != baseline[2]


def test_version_changes_identity(tmp_path: Path, identity) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    distribution.texts["METADATA"] = distribution.texts["METADATA"].replace("Version: 1.0", "Version: 2.0")
    name, version, digest = identity(distribution)
    assert (name, version) == ("demo-pkg", "2.0")
    assert digest != baseline[2]


@pytest.mark.parametrize("update_record", [False, True], ids=["declaration-text", "declaration-record"])
def test_entry_point_declaration_is_preserved(
    tmp_path: Path, identity, update_record: bool,
) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    declaration = ENTRY_POINTS.replace("demo_pkg.cli:main", "demo_pkg.cli:other")
    distribution.texts["entry_points.txt"] = declaration
    if update_record:
        distribution.records[4] = _record(f"{DIST_INFO}/entry_points.txt", declaration.encode("utf-8"))
    assert identity(distribution)[2] != baseline[2]


@pytest.mark.parametrize(
    "location,name",
    [
        ("package", "demo-cli.exe"),
        ("scripts", "undeclared.exe"),
        ("scripts", "demo-plugin.exe"),
        ("scripts", "demo-cli.dll"),
        ("scripts-child", "demo-cli.exe"),
    ],
)
def test_non_wrapper_files_remain_in_identity(
    tmp_path: Path, identity, location: str, name: str,
) -> None:
    distribution = FakeDistribution(tmp_path)
    if location == "package":
        path = f"demo_pkg/{name}"
    elif location == "scripts-child":
        path = distribution.script_path(f"nested/{name}")
    else:
        path = distribution.script_path(name)
    baseline = identity(distribution)
    distribution.records.append(_record(path, b"distributed-file"))
    with_file = identity(distribution)
    assert with_file[2] != baseline[2]
    distribution.records[-1] = _record(path, b"changed-file")
    assert identity(distribution)[2] != with_file[2]


@pytest.mark.parametrize("name", ["INSTALLER", "REQUESTED", "direct_url.json", "RECORD"])
def test_same_named_package_data_is_not_an_installation_marker(
    tmp_path: Path, identity, name: str,
) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    distribution.records.append(_record(f"demo_pkg/data/{name}", b"package-data"))
    assert identity(distribution)[2] != baseline[2]


def test_nested_dist_info_is_package_content(tmp_path: Path, identity) -> None:
    distribution = FakeDistribution(tmp_path)
    baseline = identity(distribution)
    nested = f"demo_pkg/data/{DIST_INFO}"
    distribution.records.append(_record(f"{nested}/METADATA", b"nested-metadata"))
    with_metadata = identity(distribution)
    assert with_metadata[2] != baseline[2]
    distribution.records.append(_record(f"{nested}/INSTALLER", b"nested-package-data"))
    assert identity(distribution)[2] != with_metadata[2]


def test_record_csv_retains_quoted_data_paths(tmp_path: Path, identity) -> None:
    distribution = FakeDistribution(tmp_path)
    distribution.records.append(_record("demo_pkg/data/table,part.csv", b"x,1\n"))
    baseline = identity(distribution)
    distribution.records.reverse()
    assert identity(distribution) == baseline
    distribution.records[0] = _record("demo_pkg/data/table,part.csv", b"x,2\n")
    assert identity(distribution)[2] != baseline[2]


def test_dependency_lock_v2_accepts_current_identity_and_rejects_v1(
    tmp_path: Path, identity,
) -> None:
    assert build_manifest.DEPENDENCY_LOCK_VERSION == "research-dependency-distribution-lock-v2"
    name, version, digest = identity(FakeDistribution(tmp_path))
    payload = {
        "contract_version": "research-dependency-distribution-lock-v2",
        "platform": platform.system().lower(),
        "python_cache_tag": sys.implementation.cache_tag,
        "distributions": [{"name": name, "version": version, "distribution_digest": digest}],
    }
    current = tmp_path / "current-lock.json"
    current.write_text(json.dumps(payload), encoding="utf-8")
    assert dict(build_manifest.verify_dependency_distribution_lock(current)) == {f"{name}=={version}": digest}

    historical_payload = deepcopy(payload)
    historical_payload["contract_version"] = "research-dependency-distribution-lock-v1"
    historical = tmp_path / "historical-lock.json"
    historical.write_text(json.dumps(historical_payload), encoding="utf-8")
    historical_bytes = historical.read_bytes()
    with pytest.raises(ValueError, match="版本不受支持"):
        build_manifest.verify_dependency_distribution_lock(historical)
    assert historical.read_bytes() == historical_bytes
