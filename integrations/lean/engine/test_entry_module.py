"""原生入口封存实际算法类，准入不导入算法或覆盖标准库。"""
import operator
from pathlib import Path
import sys
from research_pipeline.domain.lean_algorithm import LeanAlgorithm
from research_pipeline.extensions import OperatorSpec,PortSpec,compile_project_operator_bundle,project_source_hash,verify_project_operator_bundle
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry


def test_operator_entry_keeps_real_native_class_without_importing_source(tmp_path):
    original=sys.modules['operator']
    source=tmp_path/'source'
    source.mkdir()
    (source/'operator.py').write_text('from AlgorithmImports import *\nraise RuntimeError("准入禁止执行算法")\nclass NativeFixture(QCAlgorithm):\n    pass\n',encoding='utf-8')
    spec=OperatorSpec.build(operator_id='project.native.entry_fixture',operator_version='1.0.0',input_ports=(),
        output_ports=(PortSpec('simulation','research.lean-simulation.v4'),),parameters=(),strategy_roles=(),
        resource_profile=dict(memory_bytes=1024**3,cpu_slots=1,process_slots=3,temp_bytes=1024**3,wall_seconds=60),
        determinism_mode='deterministic',seed_policy='none',code_hash=project_source_hash(source,native_algorithm=True),pit_capabilities=('pit.as_of.v1',))
    algorithm=LeanAlgorithm(language='Python',entry_file='operator.py',type_name='NativeFixture')
    bundle=compile_project_operator_bundle(source_root=source,output_root=tmp_path/'bundles',project_id='native-entry-fixture',
        operator_spec=spec,lean_algorithm=algorithm,dependency_lock={},registered_operator_specs=build_mainline_operator_registry().operator_specs,
        project_artifact_types=('research.lean-simulation.v4',))
    assert verify_project_operator_bundle(bundle).lean_algorithm==algorithm
    assert sys.modules['operator'] is original is operator
