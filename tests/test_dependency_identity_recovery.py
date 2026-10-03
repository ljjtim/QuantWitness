"""依赖身份升级后的恢复与跨运行复用合同；所有运行均为无数据库样例。"""
from __future__ import annotations

import base64
import csv
import hashlib
import importlib
from importlib.metadata import PathDistribution
from pathlib import Path

import pytest

from research_pipeline.platform import build_manifest
from research_pipeline.runtime import (
    AuditEnvironmentManifest,
    CheckpointPolicy,
    DagSpec,
    NodeSpec,
    PartitionSpec,
    ResourceBudget,
    RetryPolicy,
    RuntimeExecutionService,
    RuntimeIntegrityError,
    RuntimeNodeOutputs,
    RuntimeNodeValue,
)
from research_pipeline.runtime.diagnostics import write_finalize_status
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest
from research_pipeline.runtime.operator_registry import NODE_IDENTITY_PROJECTION_CURRENT
from research_pipeline.runtime.required_run_reuse import prepare_required_run_reuse


_CLOCK = "2026-09-28T00:00:00+08:00"


def _distribution(root: Path, *, requested: bool) -> PathDistribution:
    """两个安装保留相同分发内容，只改变安装标记与路径包装器。"""
    site = root / "Lib" / "site-packages"
    info = site / "numpy-2.2.6.dist-info"
    info.mkdir(parents=True)
    files = {
        "numpy/__init__.py": b"__version__ = '2.2.6'\n",
        "numpy-2.2.6.dist-info/METADATA": (
            b"Metadata-Version: 2.1\nName: numpy\nVersion: 2.2.6\n"
        ),
        "numpy-2.2.6.dist-info/entry_points.txt": (
            b"[console_scripts]\nf2py = numpy.f2py:main\n"
        ),
        "numpy-2.2.6.dist-info/INSTALLER": b"pip\n",
        "../../Scripts/f2py.exe": str(root).encode("utf-8"),
    }
    if requested:
        files["numpy-2.2.6.dist-info/REQUESTED"] = b""
    rows = []
    for relative, content in files.items():
        path = site / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest())
        rows.append((relative, "sha256=" + digest.decode().rstrip("="), str(len(content))))
    rows.append(("numpy-2.2.6.dist-info/RECORD", "", ""))
    with (info / "RECORD").open("w", encoding="utf-8", newline="") as stream:
        csv.writer(stream).writerows(rows)
    return PathDistribution(info)


def _v1_digest(distribution: PathDistribution) -> str:
    """封存 v1 的 METADATA 加完整 RECORD 算法，保持旧身份语义。"""
    text = distribution.read_text("METADATA") + "\n--RECORD--\n" + distribution.read_text("RECORD")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _v2_digest(monkeypatch, distribution: PathDistribution, root: Path) -> str:
    with monkeypatch.context() as patch:
        patch.setattr(build_manifest.metadata, "distribution", lambda name: distribution)
        original = build_manifest.sysconfig.get_path
        patch.setattr(
            build_manifest.sysconfig,
            "get_path",
            lambda name: str(root / "Scripts") if name == "scripts" else original(name),
        )
        name, version, digest = build_manifest.installed_distribution_digest("numpy")
    assert (name, version) == ("numpy", "2.2.6")
    return digest


@pytest.fixture(autouse=True)
def _forbid_database_connections(monkeypatch):
    import duckdb

    def forbidden(*args, **kwargs):
        pytest.fail("依赖身份恢复测试不得连接数据库")

    monkeypatch.setattr(duckdb, "connect", forbidden)


@pytest.fixture
def dependency_identities(tmp_path, monkeypatch):
    first_root = tmp_path / "first-venv"
    second_root = tmp_path / "second-venv"
    first = _distribution(first_root, requested=True)
    second = _distribution(second_root, requested=False)
    old = _v1_digest(first)
    new = _v2_digest(monkeypatch, first, first_root)
    relocated = _v2_digest(monkeypatch, second, second_root)
    assert build_manifest.DEPENDENCY_LOCK_VERSION == "research-dependency-distribution-lock-v2"
    assert old != new
    assert new == relocated
    return old, new, relocated


@pytest.fixture(params=["byte_exact", "numerical"])
def runtime_case(request, monkeypatch):
    operator_id = {
        "byte_exact": "data.catalog.admission",
        "numerical": "research.model.fit",
    }[request.param]
    definition = build_mainline_operator_manifest().require_operator(
        operator_id, "2.0.0" if request.param == "numerical" else "1.0.0"
    )
    assert definition.cache_compatibility_mode == request.param
    node = NodeSpec(
        "shared",
        definition.implementation_ref.implementation_id,
        (),
        (("out", "test.dependency-identity.v1"),),
        ResourceBudget(1024 * 1024 * 1024, 1, 1024 * 1024, 30),
        RetryPolicy(1, ()),
        CheckpointPolicy.REQUIRED,
        PartitionSpec(False),
        cacheable=True,
        pure=True,
        configuration_hash="c" * 64,
    )
    calls = []

    def action(context):
        calls.append(context.node_context.node.node_id)
        return RuntimeNodeOutputs.single(RuntimeNodeValue.inline(
            name="out", artifact_type="test.dependency-identity.v1", content=b"stable"
        ))

    # 只替换算子计算，身份、事件、checkpoint 和复用门均执行正式实现。
    adapter = definition.runtime_adapter_ref
    monkeypatch.setattr(importlib.import_module(adapter.module_name), adapter.symbol_name, action)
    return DagSpec("dependency-identity", (node,), ()), calls


def _service(digest: str) -> RuntimeExecutionService:
    return RuntimeExecutionService(
        audit_environment=AuditEnvironmentManifest.capture(
            build_artifact_digest="a" * 64,
            dependency_distribution_digests={"numpy": digest},
        ),
        numerical_backend_names=("numpy",),
    )


def _execute(service, dag, root: Path, **kwargs):
    return service.execute(
        dag=dag,
        environment=None,
        run_root=root,
        project_id="dependency-identity",
        root_seed=7,
        fixed_clock=_CLOCK,
        **kwargs,
    )


def _completed_source(service, dag, root: Path):
    result = _execute(service, dag, root)
    assert result["status"] == "succeeded"
    # 无 DB 样例只声明完成发布；跨运行门另行复验实际 checkpoint 内容。
    published = root / "published-result"
    published.mkdir()
    write_finalize_status(root, status="pending")
    write_finalize_status(
        root, status="succeeded", result_id="fixture-result",
        result_directory=str(published), result_published=True,
    )
    return result


def _require_reuse(service, dag, source: Path, target: Path):
    return prepare_required_run_reuse(
        service=service,
        dag=dag,
        source_run_roots=(source,),
        target_run_root=target,
        root_seed=7,
        fixed_clock=_CLOCK,
        required_node_ids=("shared",),
        node_identity_projection=NODE_IDENTITY_PROJECTION_CURRENT,
    )


def test_v1_to_v2_resume_rejects_at_run_identity(
    tmp_path, dependency_identities, runtime_case,
):
    old, new, _ = dependency_identities
    dag, calls = runtime_case
    root = tmp_path / "run"
    _completed_source(_service(old), dag, root)
    sealed = {name: (root / name).read_bytes() for name in (
        "events.jsonl", "operator-dag-run.json",
    )}
    with pytest.raises(RuntimeIntegrityError, match="operator DAG run identity 漂移"):
        _execute(_service(new), dag, root, resume=True)
    assert calls == ["shared"]
    assert {name: (root / name).read_bytes() for name in sealed} == sealed


def test_v1_to_v2_cross_run_rejects_required_reuse_and_recomputes_optional_reuse(
    tmp_path, dependency_identities, runtime_case,
):
    old, new, _ = dependency_identities
    dag, calls = runtime_case
    source = tmp_path / "source"
    _completed_source(_service(old), dag, source)
    target = tmp_path / "required-target"
    with pytest.raises(RuntimeIntegrityError, match="启动前预检"):
        _require_reuse(_service(new), dag, source, target)
    assert calls == ["shared"]
    assert not (target / "events.jsonl").exists()
    assert not (target / "operator-dag-run.json").exists()
    assert not any((target / "checkpoints").iterdir())
    result = _execute(
        _service(new), dag, tmp_path / "optional-target", reuse_run_roots=(source,),
    )
    assert result["status"] == "succeeded"
    assert result["reused_nodes"] == []
    assert calls == ["shared", "shared"]


def test_equal_v2_identity_across_install_paths_resumes_and_reuses(
    tmp_path, dependency_identities, runtime_case,
):
    _, first, second = dependency_identities
    dag, calls = runtime_case
    source = tmp_path / "source"
    _completed_source(_service(first), dag, source)
    resumed = _execute(_service(second), dag, source, resume=True)
    assert resumed["status"] == "succeeded"
    assert resumed["reused_nodes"] == ["shared"]
    target = tmp_path / "target"
    prepared = _require_reuse(_service(second), dag, source, target)
    assert prepared["prepared_nodes"] == ["shared"]
    result = _execute(_service(second), dag, target, reuse_run_roots=(source,))
    assert result["status"] == "succeeded"
    assert result["reused_nodes"] == ["shared"]
    assert calls == ["shared"]
