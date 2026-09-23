from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

import research_pipeline.extensions.project_bundle as project_bundle

from research_pipeline.extensions import (
    ExtensionError,
    OperatorSpec,
    ParameterSpec,
    ParameterType,
    PortSpec,
    ProjectArtifactCommit,
    ProjectArtifactInput,
    ProjectOperatorContext,
    ProjectOperatorPermissionProfile,
    build_admitted_operator_registry,
    compile_project_operator_bundle,
    project_source_hash,
    verify_project_operator_bundle,
)
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry


REGISTERED_OPERATOR_SPECS = build_mainline_operator_registry().operator_specs
DEFAULT_PROJECT_ARTIFACT_TYPES = ("research.statistics.v1",)


def _source(root: Path, text: str = "def run(context, inputs, output):\n    return {'status': 'ok'}\n") -> Path:
    source = root / "source"
    source.mkdir()
    (source / "operator.py").write_text(text, encoding="utf-8")
    return source


def _spec(
    source: Path, *, output_type: str = "research.statistics.v1",
    input_type: str = "data.columnar-bundle.v1",
) -> OperatorSpec:
    return OperatorSpec.build(
        operator_id="research.event.analyst_revision",
        operator_version="1.0.0",
        input_ports=(PortSpec("data", input_type),),
        output_ports=(PortSpec("statistics", output_type),),
        parameters=(),
        strategy_roles=(),
        resource_profile={"memory_bytes": 64 * 1024 * 1024, "cpu_slots": 1, "temp_bytes": 64 * 1024 * 1024, "wall_seconds": 60},
        determinism_mode="deterministic",
        seed_policy="none",
        code_hash=project_source_hash(source),
        pit_capabilities=("pit.as_of.v1",),
    )


def test_bundle_compile_verify_and_identity_are_deterministic(tmp_path: Path) -> None:
    source = _source(tmp_path)
    first = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles-a",
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
        project_id="analyst-revision",
        operator_spec=_spec(source),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    second = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles-b",
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
        project_id="analyst-revision",
        operator_spec=_spec(source),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    left = verify_project_operator_bundle(first)
    right = verify_project_operator_bundle(second)
    assert left.bundle_hash == right.bundle_hash == first.name == second.name
    assert left.operator_spec.code_hash == project_source_hash(source)
    assert left.permissions == ProjectOperatorPermissionProfile()
    assert left.requires_python == ">=3.10"
    assert "permission_hash" not in left.to_dict()
    assert "adapter_hash" not in left.to_dict()
    assert "environment_hash" not in left.to_dict()


def test_project_bundle_allows_deduplicated_source_and_control_files(tmp_path: Path) -> None:
    source = _source(tmp_path)
    os.link(source / "operator.py", tmp_path / "source-copy.py")
    bundle = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles",
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
        project_id="analyst-revision",
        operator_spec=_spec(source),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    os.link(bundle / "manifest.json", tmp_path / "manifest-copy.json")
    os.link(bundle / "sources/operator.py", tmp_path / "bundle-source-copy.py")
    assert verify_project_operator_bundle(bundle).operator_spec.code_hash == project_source_hash(source)
    (tmp_path / "bundle-source-copy.py").write_text("def changed(): pass\n", encoding="utf-8")
    with pytest.raises(ExtensionError, match="源码漂移"):
        verify_project_operator_bundle(bundle)


def test_source_bundle_verification_is_not_bound_to_current_cpython_minor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = _source(tmp_path)
    bundle = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles",
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
        project_id="analyst-revision",
        operator_spec=_spec(source),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    monkeypatch.setattr(
        project_bundle,
        "sys",
        SimpleNamespace(
            version_info=(3, 12, 0),
            stdlib_module_names=sys.stdlib_module_names,
        ),
    )

    assert verify_project_operator_bundle(bundle).requires_python == ">=3.10"


def test_source_bundle_rejects_binary_extension(tmp_path: Path) -> None:
    source = _source(tmp_path)
    (source / "native.pyd").write_bytes(b"not-a-real-extension")
    with pytest.raises(ExtensionError, match="只允许 Python 源码"):
        project_source_hash(source)


def test_bundle_requires_project_declaration_for_factor_analysis_artifact(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path)
    spec = _spec(source, output_type="research.factor-analysis.v1")
    with pytest.raises(ExtensionError, match="Artifact/schema"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "undeclared",
            project_id="analyst-revision",
            operator_spec=spec,
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        )
    bundle = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles",
        project_id="analyst-revision",
        operator_spec=spec,
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=("research.factor-analysis.v1",),
    )
    manifest = verify_project_operator_bundle(bundle)
    assert manifest.operator_spec.output_ports[0].artifact_type == (
        "research.factor-analysis.v1"
    )


@pytest.mark.parametrize(
    "artifact_type",
    ("research.feature-set.v1", "research.label.v1"),
)
def test_bundle_rejects_formal_causal_output_without_core_runtime_context(
    tmp_path: Path,
    artifact_type: str,
) -> None:
    source = _source(tmp_path)

    with pytest.raises(ExtensionError, match="无法从项目输入生成核心逐行时间事实"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "bundles",
            project_id="analyst-revision",
            operator_spec=_spec(source, output_type=artifact_type),
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        )


def test_bundle_rejects_project_specific_derived_label_contract(tmp_path: Path) -> None:
    source = _source(tmp_path)
    spec = OperatorSpec.build(
        operator_id="research.event.analyst_revision",
        operator_version="1.0.0",
        input_ports=(PortSpec("data", "data.columnar-bundle.v1"),),
        output_ports=(PortSpec("labels", "research.label.v1"),),
        parameters=(ParameterSpec("derived_causal_plan", ParameterType.JSON),),
        strategy_roles=(),
        resource_profile={
            "memory_bytes": 64 * 1024 * 1024,
            "cpu_slots": 1,
            "temp_bytes": 64 * 1024 * 1024,
            "wall_seconds": 60,
        },
        determinism_mode="deterministic",
        seed_policy="none",
        code_hash=project_source_hash(source),
        pit_capabilities=("pit.as_of.v1",),
    )
    with pytest.raises(ExtensionError, match="必填 JSON causal_plan"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "bundles",
            project_id="fixture-project",
            operator_spec=spec,
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        )


def test_bundle_rejects_source_and_manifest_tampering(tmp_path: Path) -> None:
    source = _source(tmp_path)
    bundle = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles",
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
        project_id="analyst-revision",
        operator_spec=_spec(source),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    (bundle / "sources" / "operator.py").write_text("def run(*args):\n    return 2\n", encoding="utf-8")
    with pytest.raises(ExtensionError, match="源码漂移"):
        verify_project_operator_bundle(bundle)

    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    manifest["abi_version"] = "unknown-abi"
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ExtensionError, match="ABI 版本"):
        verify_project_operator_bundle(bundle)


def test_bundle_rejects_unknown_dependency_extra_file_and_unknown_schema(tmp_path: Path) -> None:
    source = _source(tmp_path, "import unknown_package\n\ndef run(context, inputs, output):\n    return {}\n")
    with pytest.raises(ExtensionError, match="未登记依赖"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "bundles",
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
            project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
            project_id="analyst-revision",
            operator_spec=_spec(source),
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
        )
    clean = tmp_path / "clean"
    clean.mkdir()
    source = _source(clean)
    with pytest.raises(ExtensionError, match="未登记 Artifact"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "unknown-schema",
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
            project_id="analyst-revision",
            operator_spec=_spec(source, output_type="project.custom.v1"),
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
        )

    bundle = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "project-schema",
        registered_operator_specs=REGISTERED_OPERATOR_SPECS,
        project_artifact_types=("project.custom.v1",),
        project_id="analyst-revision",
        operator_spec=_spec(source, output_type="project.custom.v1"),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    assert verify_project_operator_bundle(bundle).operator_spec.output_ports[0].artifact_type == (
        "project.custom.v1"
    )


def test_project_artifact_input_requires_a_current_project_producer(tmp_path: Path) -> None:
    source = _source(tmp_path)
    builtin = build_mainline_operator_registry()
    bundle = compile_project_operator_bundle(
        source_root=source,
        output_root=tmp_path / "bundles",
        registered_operator_specs=builtin.operator_specs,
        project_artifact_types=("project.custom.v1", *DEFAULT_PROJECT_ARTIFACT_TYPES),
        project_id="analyst-revision",
        operator_spec=_spec(source, input_type="project.custom.v1"),
        entry_module="operator",
        entry_function="run",
        dependency_lock={},
    )
    with pytest.raises(ExtensionError, match="未登记 Artifact"):
        build_admitted_operator_registry(
            (bundle,), builtin_registry=builtin,
            expected_project_id="analyst-revision",
        )


def test_bundle_rejects_permissions_entry_and_code_hash_drift(tmp_path: Path) -> None:
    source = _source(tmp_path)
    with pytest.raises(ExtensionError, match="output root"):
        ProjectOperatorPermissionProfile(artifact_write_scope="attempt_only")
    with pytest.raises(ExtensionError, match="入口函数不存在"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "missing-entry",
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
            project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
            project_id="analyst-revision",
            operator_spec=_spec(source),
            entry_module="operator",
            entry_function="missing",
            dependency_lock={},
        )
    spec = _spec(source)
    (source / "operator.py").write_text("def run(context, inputs, output):\n    return {'changed': True}\n", encoding="utf-8")
    with pytest.raises(ExtensionError, match="code_hash"):
        compile_project_operator_bundle(
            source_root=source,
            output_root=tmp_path / "drift",
            registered_operator_specs=REGISTERED_OPERATOR_SPECS,
            project_artifact_types=DEFAULT_PROJECT_ARTIFACT_TYPES,
            project_id="analyst-revision",
            operator_spec=spec,
            entry_module="operator",
            entry_function="run",
            dependency_lock={},
        )


def test_project_operator_abi_rejects_paths_executable_fields_and_naive_clock() -> None:
    with pytest.raises(ExtensionError, match="安全相对路径"):
        ProjectArtifactInput("data", "data.columnar-bundle.v1", "../escape", "a" * 64, "b" * 64)
    with pytest.raises(ExtensionError, match="安全相对路径"):
        ProjectArtifactCommit("statistics", "research.statistics.v1", "C:\\escape", "a" * 64, "b" * 64, 1)
    with pytest.raises(ExtensionError, match="带时区"):
        ProjectOperatorContext("project", "run", "node", "attempt", "2026-08-03T09:00:00", 1, {})
    with pytest.raises(ExtensionError, match="不安全字段"):
        ProjectOperatorContext("project", "run", "node", "attempt", "2026-08-03T09:00:00+08:00", 1, {"module": "evil"})


def test_project_operator_abi_round_trip_nested_parameters() -> None:
    artifact_input = ProjectArtifactInput("data", "data.columnar-bundle.v1", "inputs/data", "a" * 64, "b" * 64)
    commit = ProjectArtifactCommit("statistics", "research.statistics.v1", "outputs/result.json", "c" * 64, "d" * 64, 12)
    context = ProjectOperatorContext(
        "project", "run", "node", "attempt", "2026-08-03T09:00:00+08:00", 7,
        {"window": 5, "nested": {"values": [1, 2]}},
        {
            "memory_bytes": 1024,
            "cpu_slots": 1,
            "temp_bytes": 0,
            "wall_seconds": 60,
        },
    )
    assert ProjectArtifactInput.from_dict(artifact_input.to_dict()) == artifact_input
    assert ProjectArtifactCommit.from_dict(commit.to_dict()) == commit
    rebuilt = ProjectOperatorContext.from_dict(context.to_dict())
    assert rebuilt.to_dict() == context.to_dict()
    assert rebuilt.effective_resource_budget["memory_bytes"] == 1024
