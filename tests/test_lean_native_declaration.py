from pathlib import Path
from types import SimpleNamespace

import pytest

from research_pipeline.domain.lean_algorithm import (
    LeanAlgorithm, LeanAlgorithmLimits, LeanDataBinding, LeanStrategyDeclaration,
    parse_lean_strategy_declaration,
)
from research_pipeline.platform.errors import StrategyProtocolError
from research_pipeline.extensions import (
    OperatorSpec, PortSpec, ExtensionError, compile_project_operator_bundle,
    project_source_hash, verify_project_operator_bundle,
)
from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
from research_pipeline.platform.operator_contracts import OperatorNodeRecipe
from research_pipeline.runtime.operator_registry import _node_configuration_hash


def _spec(source):
    return OperatorSpec.build(operator_id="project.native.strategy", operator_version="1.0.0",
        input_ports=(PortSpec("bars", "data.minute-bars.v1"),),
        output_ports=(PortSpec("simulation", LEAN_ARTIFACT_TYPE),), parameters=(), strategy_roles=(),
        resource_profile={"memory_bytes": 64 * 1024**2, "cpu_slots": 1, "process_slots": 4,
                          "temp_bytes": 64 * 1024**2, "wall_seconds": 60},
        determinism_mode="deterministic", seed_policy="none",
        code_hash=project_source_hash(source, native_algorithm=True), pit_capabilities=("pit.as_of.v1",))


def _compile(source, output, algorithm):
    return compile_project_operator_bundle(source_root=source, output_root=output,
        project_id="native", operator_spec=_spec(source), lean_algorithm=algorithm, dependency_lock={},
        registered_operator_specs=build_mainline_operator_registry().operator_specs,
        project_artifact_types=(LEAN_ARTIFACT_TYPE,))


def test_native_python_bundle_does_not_import_or_execute_algorithm(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "main.py").write_text("from AlgorithmImports import *\nraise RuntimeError('准入不能执行')\nclass Example(QCAlgorithm):\n    pass\n", encoding="utf-8")
    algorithm = LeanAlgorithm(language="Python", entry_file="main.py", type_name="Example")
    bundle = _compile(source, tmp_path / "bundles", algorithm)
    manifest = verify_project_operator_bundle(bundle)
    assert manifest.lean_algorithm == algorithm
    assert manifest.entry_module is None and manifest.entry_function is None
    assert manifest.to_dict()["entry"] == algorithm.to_dict()
    declaration = LeanStrategyDeclaration(strategy_id="native", bundle_id=manifest.bundle_hash,
        algorithm=algorithm, limits=LeanAlgorithmLimits(initialize_seconds=30, time_step_seconds=60),
        input_bindings=(LeanDataBinding(binding_id="minute", input_port="bars", request_id="raw",
                         kind="bars", resolution="1m", columns=("close",)),))
    assert parse_lean_strategy_declaration(declaration.to_dict()) == declaration


def test_native_python_declared_class_must_exist(tmp_path):
    (tmp_path / "main.py").write_text("class Different: pass\n", encoding="utf-8")
    with pytest.raises(ExtensionError, match="指定算法类"):
        _compile(tmp_path, tmp_path.parent / (tmp_path.name + "-bundles"),
                 LeanAlgorithm(language="Python", entry_file="main.py", type_name="Missing"))


def test_strategy_cache_identity_includes_imports_and_executable_logging(tmp_path, monkeypatch):
    from dataclasses import replace

    from research_pipeline.extensions.project_admission import build_admitted_operator_registry
    from research_pipeline.platform import lean_runtime
    from research_pipeline.runtime.execution_service import RuntimeExecutionService
    from research_pipeline.runtime.graph import DagSpec
    from test_cross_run_reuse_variants import _completed_source, _runtime_fixture

    monkeypatch.setattr(lean_runtime, "lean_runtime_semantic_identity", lambda lock: lock["semantic"])
    source = tmp_path / "source"
    source.mkdir()
    algorithm = LeanAlgorithm(language="Python", entry_file="main.py", type_name="Example")
    (source / "main.py").write_text(
        "from helper import signal\nclass Example:\n    def initialize(self):\n"
        "        self.Debug(signal())\n", encoding="utf-8",
    )
    helper = source / "helper.py"
    plan = SimpleNamespace()

    def compile_version(helper_code):
        helper.write_text(helper_code, encoding="utf-8")
        return verify_project_operator_bundle(_compile(source, tmp_path / "bundles", algorithm))

    def configuration(manifest, installation):
        node = OperatorNodeRecipe(
            "strategy", manifest.operator_spec.operator_id, manifest.operator_spec.operator_version, (),
            {"execution_backend": "lean", "strategy_declaration": {"bundle_id": manifest.bundle_hash},
             "lean_runtime": {"installation_root": installation, "semantic": {"plugins": "1"}}}, (),
        )
        return _node_configuration_hash(plan, node, project_manifest=manifest)

    before = compile_version("def signal():\n    return 1\n")
    editorial = compile_version("# 信号说明\n\ndef signal():\n    return 1  # 买入\n")
    changed = compile_version("def signal():\n    return 2\n")
    logging = compile_version("def signal():\n    print('signal')\n    return 1\n")
    assert before.bundle_hash != editorial.bundle_hash
    assert before.cache_bundle_hash() == editorial.cache_bundle_hash()
    assert configuration(before, "first") == configuration(editorial, "second")
    assert configuration(before, "first") != configuration(changed, "first")
    assert configuration(before, "first") != configuration(logging, "first")

    base_service, base_node, context = _runtime_fixture()

    def runtime(manifest, location):
        registry = build_admitted_operator_registry(
            (tmp_path / "bundles" / manifest.bundle_hash,),
            builtin_registry=build_mainline_operator_registry(),
        )
        spec = manifest.operator_spec
        token = registry.project_token(spec.operator_id, spec.operator_version)
        service = RuntimeExecutionService(
            audit_environment=base_service.audit_environment,
            numerical_backend_names=("numpy",),
            project_registry=registry,
        )
        node = replace(
            base_node, implementation_id=token.implementation_id,
            configuration_hash=configuration(manifest, location),
            output_types=(("simulation", LEAN_ARTIFACT_TYPE),),
        )
        return service, node

    source_service, source_node = runtime(before, "first")
    source_run = tmp_path / "completed"
    _, expectation, _ = _completed_source(source_service, source_node, context, source_run)
    target_service, target_node = runtime(editorial, "second")
    _, target_expectation = target_service._identity(target_node, (), context)
    assert target_expectation == expectation
    result = target_service.execute(
        dag=DagSpec("semantic-reuse", (target_node,), ()), environment=object(),
        run_root=tmp_path / "target", project_id="native", root_seed=context.root_seed,
        fixed_clock=context.fixed_clock.isoformat(), reuse_run_roots=(source_run,),
    )
    assert result["reused_nodes"] == [target_node.node_id]
    changed_service, changed_node = runtime(changed, "first")
    _, changed_expectation = changed_service._identity(changed_node, (), context)
    assert changed_expectation.node_execution_id != expectation.node_execution_id

    monkeypatch.setenv("QUANTWITNESS_STRICT_AUDIT", "1")
    assert configuration(before, "first") != configuration(editorial, "second")


def test_portfolio_target_binding_references_artifact_without_query():
    binding = LeanDataBinding(binding_id="portfolio_targets", input_port="targets", request_id=None,
                              kind="targets", resolution="event", columns=("weight_ppb", "batch_id"))
    assert binding.request_id is None
    with pytest.raises(StrategyProtocolError, match="模型目标"):
        LeanDataBinding(binding_id="portfolio_targets", input_port="targets", request_id="invented_query",
                        kind="targets", resolution="event", columns=("weight_ppb", "batch_id"))


def test_native_csharp_seals_source_and_assembly(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "Example.cs").write_text("namespace Research; public class Example {}", encoding="utf-8")
    (source / "Research.dll").write_bytes(b"MZ-declaration-test")
    algorithm = LeanAlgorithm(language="CSharp", entry_file="Research.dll", type_name="Research.Example")
    bundle = _compile(source, tmp_path / "bundles", algorithm)
    assert {value["path"] for value in verify_project_operator_bundle(bundle).source_files} == {"Example.cs", "Research.dll"}
    (bundle / "sources/Research.dll").write_bytes(b"changed")
    with pytest.raises(ExtensionError, match="源码漂移"):
        verify_project_operator_bundle(bundle)


@pytest.mark.parametrize("entry", ["../algo.py", "C:/algo.py", "/algo.py", "algo.dll"])
def test_native_python_rejects_invalid_entry(entry):
    with pytest.raises(StrategyProtocolError):
        LeanAlgorithm(language="Python", entry_file=entry, type_name="Example")
