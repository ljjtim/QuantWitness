"""LEAN 安装锁与正式计划加载的内容复验。"""

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from research_pipeline.platform.lean_runtime import freeze_lean_runtime, validate_lean_runtime


REVISION = "a" * 40
LAUNCHER = f"src/Lean-{REVISION}/Launcher/bin/Release"
BASE = "python-managed/cpython-3.11-fixture"
SHARED = "sdk/shared/Microsoft.NETCore.App/10.0.14"
HOST = "sdk/host/fxr/10.0.14"


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("LEAN 安装锁测试不得打开数据库")
    monkeypatch.setattr("duckdb.connect", forbidden)


@pytest.fixture
def installation(tmp_path):
    root = tmp_path / "installation"
    files = [
        "sdk/dotnet.exe", f"{SHARED}/coreclr.dll", f"{SHARED}/hostpolicy.dll",
        f"{SHARED}/System.Private.CoreLib.dll", f"{HOST}/hostfxr.dll",
        f"{LAUNCHER}/QuantConnect.Lean.Launcher.dll",
        f"{LAUNCHER}/QuantConnect.Lean.Launcher.deps.json",
        f"{LAUNCHER}/QuantConnect.Lean.Launcher.runtimeconfig.json",
        *(f"{LAUNCHER}/{name}.dll" for name in (
            "QuantWitnessLeanHost", "ChinaRules", "CorporateActions", "ParquetInput",
            "ParquetSharp", "ParquetSharpNative", "Apache.Arrow", "Python.Runtime")),
        "rp-integration/ResearchLeanAlgorithm.py", "rp-integration/config-template.json",
        "rp-integration/metadata/market-hours/market-hours-database.json",
        "rp-integration/metadata/symbol-properties/symbol-properties-database.csv",
        f"{BASE}/python.exe", f"{BASE}/python311.dll", f"{BASE}/vcruntime140.dll",
        f"{BASE}/Lib/os.py", f"{BASE}/Lib/encodings/__init__.py", f"{BASE}/DLLs/_socket.pyd",
        "python-env/Scripts/python.exe", "python-env/Lib/site-packages/sample/__init__.py",
        "python-env/Lib/site-packages/sample/data.csv",
        "python-env/Lib/site-packages/sample-1.0.dist-info/METADATA",
    ]
    for name in files:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"依赖内容: {name}\n", encoding="utf-8")
    (root / "python-env/pyvenv.cfg").write_text(
        f"home = {root / BASE}\ninclude-system-site-packages = false\n", encoding="utf-8")
    (root / "upstream-revision.txt").write_text(REVISION, encoding="utf-8")
    return root


def test_freeze_returns_json_parameters_with_complete_runtime_files(installation):
    from research_pipeline.platform.operator_contracts import ParameterSpec, ParameterType, validate_parameters

    lock = freeze_lean_runtime(installation)
    assert lock["files"] == {
        name: hashlib.sha256((installation / name).read_bytes()).hexdigest()
        for name in lock["files"]
    }
    assert json.loads(json.dumps(lock)) == lock
    assert validate_lean_runtime(lock) == lock
    frozen = validate_parameters({"lean_runtime": lock}, (ParameterSpec("lean_runtime", ParameterType.JSON),), "parameters")
    assert validate_lean_runtime(frozen["lean_runtime"]) == lock
    assert lock["files"][f"{LAUNCHER}/QuantWitnessLeanHost.dll"]
    assert f"{SHARED}/coreclr.dll" in lock["files"]
    assert f"{HOST}/hostfxr.dll" in lock["files"]
    assert f"{BASE}/DLLs/_socket.pyd" in lock["files"]
    assert "python-env/Lib/site-packages/sample/data.csv" in lock["files"]
    assert "identity_hash" not in lock


@pytest.mark.parametrize("relative", [
    f"{LAUNCHER}/QuantConnect.Lean.Launcher.dll", f"{LAUNCHER}/QuantWitnessLeanHost.dll",
    f"{LAUNCHER}/ParquetSharpNative.dll", "sdk/dotnet.exe", f"{SHARED}/coreclr.dll",
    f"{HOST}/hostfxr.dll", f"{BASE}/python311.dll", f"{BASE}/Lib/os.py",
    "python-env/Lib/site-packages/sample/__init__.py", "rp-integration/ResearchLeanAlgorithm.py",
    "rp-integration/config-template.json", "rp-integration/metadata/market-hours/market-hours-database.json",
    "rp-integration/metadata/symbol-properties/symbol-properties-database.csv",
])
def test_same_location_same_size_and_mtime_still_rejects_changed_bytes(installation, relative):
    lock = freeze_lean_runtime(installation)
    target = installation / relative
    before = target.stat()
    target.write_bytes(b"X" * before.st_size)
    os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(ValueError, match="内容变化"):
        validate_lean_runtime(lock)


@pytest.mark.parametrize("relative", [
    f"{LAUNCHER}/ChinaRules.dll", f"{SHARED}/coreclr.dll",
    "python-env/Lib/site-packages/sample/data.csv",
    "rp-integration/metadata/market-hours/market-hours-database.json",
])
def test_missing_dependency_rejects_lock(installation, relative):
    lock = freeze_lean_runtime(installation)
    (installation / relative).unlink()
    with pytest.raises(ValueError, match="缺失|闭包变化"):
        validate_lean_runtime(lock)


def test_new_runtime_file_invalidates_old_closure(installation):
    lock = freeze_lean_runtime(installation)
    (installation / LAUNCHER / "ExtraPlugin.dll").write_text("新增插件", encoding="utf-8")
    with pytest.raises(ValueError, match="闭包变化.*ExtraPlugin"):
        validate_lean_runtime(lock)


def test_unused_launcher_config_sdk_caches_and_logs_do_not_change_lock(installation):
    lock = freeze_lean_runtime(installation)
    for relative in (
        f"{LAUNCHER}/config.json", f"{LAUNCHER}/config.example.json",
        f"{LAUNCHER}/results/result.json", f"{LAUNCHER}/logs/session.json",
        "sdk/sdk/10.0.100/BuildTool.dll", "cache/nuget/package.dll",
        "python-env/Lib/site-packages/sample/__pycache__/module.pyc",
    ):
        target = installation / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("运行产物", encoding="utf-8")
    assert freeze_lean_runtime(installation) == lock
    assert validate_lean_runtime(lock) == lock


def test_custom_layout_and_structural_validation(installation, monkeypatch):
    source = installation / "rp-integration/ResearchLeanAlgorithm.py"
    target = installation / "alternative/ResearchLeanAlgorithm.py"
    target.parent.mkdir()
    source.rename(target)
    lock = freeze_lean_runtime(installation, layout={
        "algorithm_relative": "alternative/ResearchLeanAlgorithm.py",
        "python_base_relative": BASE, "python_library_relative": f"{BASE}/python311.dll",
    })
    target.unlink()
    with monkeypatch.context() as isolated:
        def no_file_access(*args, **kwargs):
            pytest.fail("结构检查不得访问依赖文件")
        isolated.setattr("research_pipeline.platform.lean_runtime._contained", no_file_access)
        assert validate_lean_runtime(lock, verify_files=False) == lock
    with pytest.raises(ValueError, match="必需文件缺失"):
        validate_lean_runtime(lock)


def test_existing_lean_preparation_defers_content_check_to_execution(installation, tmp_path, monkeypatch):
    import importlib.util
    import research_pipeline.packages as packages
    from research_pipeline.packages.store import PACKAGE_FILES
    from research_pipeline.runtime import lean_strategy_execution as execution

    path = Path(__file__).resolve().parents[1] / "integrations/lean/prepare.py"
    spec = importlib.util.spec_from_file_location("lean_existing_prepare", path)
    prepare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prepare)
    lock = freeze_lean_runtime(installation)
    parameters = dict(execution_backend="lean", lean_runtime=lock, initial_cash_units=100000000,
                      strategy_declaration={"bundle_id": "bundle"})
    node = dict(operator_id="project.strategy", operator_version="1.0.0", parameters=parameters,
                inputs=[{"input_port": port} for port in ("bars", "snapshot", "signals")])
    source = tmp_path / "package"
    for name in PACKAGE_FILES:
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps({"graph": {"nodes": [node]}} if name == "spec/research.yaml" else {}),
                          encoding="utf-8")
    package = object()
    monkeypatch.setattr(packages, "load_research_package", lambda *args, **kwargs: package)
    manifest = SimpleNamespace(bundle_hash="bundle", operator_spec=SimpleNamespace(resource_profile={"process_slots": 4},
        input_ports=(SimpleNamespace(port="bars", artifact_type="data.minute-bars.v1"),)))
    monkeypatch.setattr(prepare, "bundle_catalog", lambda root: {("project.strategy", "1.0.0"): (source, manifest)})
    (installation / "rp-integration/ResearchLeanAlgorithm.py").write_text("更新后的算法", encoding="utf-8")
    summary, loaded = prepare.prepare_package(source, tmp_path / "conversion", installation)
    assert loaded is package
    assert summary["research_executed"] is False
    registry = SimpleNamespace(project_token_by_implementation=lambda identity: SimpleNamespace(
        manifest=SimpleNamespace(operator_spec=SimpleNamespace(operator_id="project.strategy", operator_version="1.0.0"))))
    service = SimpleNamespace(project_parameters_by_node={"simulation": parameters}, project_registry=registry)
    context = SimpleNamespace(node=SimpleNamespace(node_id="simulation", implementation_id="implementation"))
    monkeypatch.setattr(execution, "project_runtime_identity", lambda *args: None)
    with pytest.raises(ValueError, match="内容变化"):
        execution.execute_lean_strategy_node(service, node_context=context, run_id="run", attempt_id="attempt", environment={})


@pytest.mark.parametrize("mutation", ["outside", "absolute", "missing_lock_entry", "forbidden", "other_entry"])
def test_lock_rejects_incomplete_or_outside_installation_metadata(installation, mutation):
    lock = freeze_lean_runtime(installation)
    if mutation == "outside":
        lock["algorithm_relative"] = "../ResearchLeanAlgorithm.py"
    elif mutation == "absolute":
        lock["files"][str(installation / "outside.dll")] = "0" * 64
    elif mutation == "missing_lock_entry":
        lock["files"].pop(f"{LAUNCHER}/QuantWitnessLeanHost.dll")
    elif mutation == "forbidden":
        lock["module"] = "arbitrary"
    else:
        lock["dotnet_relative"] = "sdk/arbitrary.exe"
    with pytest.raises(ValueError):
        validate_lean_runtime(lock, verify_files=False)


def _publish_plan(root, lock, *, backend="lean"):
    from observation_package_support import copy_observation_package
    from test_data_plane_provider_contract import _data_plane_execution_budget, _execution_estimate
    from test_generic_run_admission import _admit_query
    from research_pipeline.application.research_plan_store import publish_operator_graph_research_plan
    from research_pipeline.extensions import (OperatorSpec, ParameterSpec, ParameterType, PortSpec,
        build_admitted_operator_registry, compile_project_operator_bundle, project_source_hash)
    from research_pipeline.packages import compile_research_package, load_research_package
    from research_pipeline.platform import typed_canonical_hash
    from research_pipeline.runtime.operator_registry import build_mainline_operator_registry, compile_operator_graph_dag

    package_root = copy_observation_package(root / "package")
    builtin = build_mainline_operator_registry()
    source = root / "source"
    source.mkdir()
    (source / "operator.py").write_text("def run(context, inputs, output_root):\n    return ()\n", encoding="utf-8")
    spec = OperatorSpec.build(operator_id="project.lean.identity", operator_version="1.0.0",
        input_ports=(PortSpec("data", "data.columnar-bundle.v1"),),
        output_ports=(PortSpec("identity", "project.lean-identity.v1"),),
        parameters=(ParameterSpec("execution_backend", ParameterType.STRING), ParameterSpec("lean_runtime", ParameterType.JSON),
                    ParameterSpec("initial_cash_units", ParameterType.INTEGER)),
        strategy_roles=(), resource_profile={"memory_bytes": 64 * 1024**2, "cpu_slots": 1,
            "temp_bytes": 64 * 1024**2, "process_slots": 3, "wall_seconds": 60},
        determinism_mode="deterministic", seed_policy="none", code_hash=project_source_hash(source),
        pit_capabilities=("pit.as_of.v1",))
    bundle = compile_project_operator_bundle(source_root=source, output_root=root / "bundles",
        project_id="lean-identity-test", operator_spec=spec, entry_module="operator", entry_function="run",
        dependency_lock={}, registered_operator_specs=builtin.operator_specs,
        project_artifact_types=("project.lean-identity.v1",))
    registry = build_admitted_operator_registry((bundle,), builtin_registry=builtin)
    spec_file = package_root / "spec/research.yaml"
    payload = yaml.safe_load(spec_file.read_text(encoding="utf-8"))
    node = {"node_id": "lean_identity", "inputs": [{"input_port": "data", "source_node_id": "data_plane", "source_output_port": "data"}], "strategies": []}
    payload["graph"]["nodes"].append(node)
    node.update(operator_id=spec.operator_id, operator_version=spec.operator_version,
                parameters={"execution_backend": backend, "lean_runtime": lock, "initial_cash_units": 100000000})
    spec_file.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    package = load_research_package(package_root)
    plan = compile_research_package(package, admission=registry)
    admitted = {key: _admit_query(query) for key, query in zip(plan.request_ids, plan.queries, strict=True)}
    budget = _data_plane_execution_budget(plan, registry)
    estimates = {key: _execution_estimate(value, budget) for key, value in admitted.items()}
    plan = compile_research_package(package, admission=registry, platform_admission={
        "catalog_hash": "2" * 64, "pit_contract_hash": "6" * 64, "drift_proof_hash": "7" * 64,
        "execution_estimates_hash": typed_canonical_hash({key: value.to_dict() for key, value in sorted(estimates.items())}),
    })
    target = publish_operator_graph_research_plan(root / "plan", package_id=package.package_id, plan=plan,
        research_identity=package.research_identity_payload(plan_hash=plan.plan_hash),
        metric_contract={"contract_hash": package.metric_contract.contract_hash, **package.metric_contract.payload()},
        claim_contract={"contract_hash": package.claim_contract.contract_hash, **package.claim_contract.payload()},
        admitted_plans=admitted, execution_estimates=estimates, dag=compile_operator_graph_dag(plan, registry), registry=registry)
    return target.directory, plan


def test_formal_plan_load_revalidates_lean_files_and_existing_configuration_identity(installation, tmp_path, monkeypatch):
    from research_pipeline.application import research_plan_store
    from research_pipeline.runtime.operator_registry import _node_configuration_hash

    lock = freeze_lean_runtime(installation)
    target, plan = _publish_plan(tmp_path, lock)
    calls = []
    def validate(payload, verify_files=True):
        calls.append(verify_files)
        return validate_lean_runtime(payload, verify_files=verify_files)
    monkeypatch.setattr(research_plan_store, "validate_lean_runtime", validate)
    research_plan_store.load_operator_graph_research_plan(target=target)
    assert calls == [True]
    node = next(item for item in plan.recipe.nodes if item.node_id == "lean_identity")
    changed = deepcopy(lock)
    changed["files"][f"{LAUNCHER}/QuantWitnessLeanHost.dll"] = "f" * 64
    other = replace(node, parameters={**dict(node.parameters), "lean_runtime": changed})
    assert _node_configuration_hash(plan, node) != _node_configuration_hash(plan, other)


def test_non_lean_plan_never_calls_runtime_validator(installation, tmp_path, monkeypatch):
    from research_pipeline.application import research_plan_store

    target, _ = _publish_plan(tmp_path, {"unresolved": True}, backend="rp")
    def unexpected(*args, **kwargs):
        pytest.fail("RP 原路径不得触发 LEAN 安装校验")
    monkeypatch.setattr(research_plan_store, "validate_lean_runtime", unexpected)
    research_plan_store.load_operator_graph_research_plan(target=target)


@pytest.mark.parametrize("resume", [False, True])
def test_formal_run_rejects_engine_drift_before_checkpoint_stage(installation, tmp_path, monkeypatch, resume):
    from research_pipeline.application import research_run

    target, _ = _publish_plan(tmp_path, freeze_lean_runtime(installation))
    (installation / LAUNCHER / "QuantWitnessLeanHost.dll").write_text("不同安装内容", encoding="utf-8")
    monkeypatch.setattr(research_run, "_require_data_run_arguments", lambda args: None)
    def checkpoint_stage(*args, **kwargs):
        pytest.fail("安装漂移必须在 Runtime/checkpoint 阶段之前拒绝")
    monkeypatch.setattr(research_run, "_execute_unified_operator_runtime", checkpoint_stage)
    args = SimpleNamespace(execution_engine="unified", plan=str(target), runtime_resume=resume)
    with pytest.raises(ValueError, match="内容变化"):
        research_run._execute_owned_operator_graph(args, SimpleNamespace())
