"""主线算子定义的统一构造、源码身份和 Runtime adapter 映射。"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from research_pipeline.extensions import (
    OperatorDefinition,
    OperatorImplementationRef,
    OperatorSpec,
    ParameterSpec,
    ParameterType,
    PortSpec,
    StrategyRole,
)
from research_pipeline.extensions.operators import OperatorRuntimeAdapterRef
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.source_identity import canonical_python_source_bytes
from .errors import RuntimeRegistryError


def _source_module_path(module_name: str, label: str) -> Path:
    prefix = "research_pipeline."
    if not module_name.startswith(prefix):
        raise RuntimeRegistryError(f"正式源码模块不属于主线: {label}")
    package_root = Path(__file__).resolve().parents[1]
    relative = module_name.removeprefix(prefix).split(".")
    source_path = package_root.joinpath(*relative).with_suffix(".py")
    if source_path.is_file():
        return source_path
    package_path = package_root.joinpath(*relative, "__init__.py")
    if package_path.is_file():
        return package_path
    raise RuntimeRegistryError(f"正式源码模块不存在: {label}")


_VERSION = "1.0.0"
_MIB = 1024 * 1024
_GIB = 1024 * _MIB
# 明确登记共同变化的算子族；不从导入或调用图推断实现身份。
_ADAPTER_MODULES_BY_SYMBOL = {
    "execute_data_catalog_admission_v1": "research_pipeline.runtime.adapters.validity",
    "execute_data_columnar_materialize_v1": "research_pipeline.application.grid_data",
    "execute_data_minute_adjustment_snapshot_v1": "research_pipeline.runtime.adapters.minute_adjustment",
    "execute_data_minute_scan_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_finance_simulation_intraday_v3": "research_pipeline.runtime.adapters.minute_simulation",
    "execute_research_bars_minute_adjust_v1": "research_pipeline.runtime.adapters.minute_adjustment",
    "execute_research_bars_minute_resample_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_features_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_labels_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_model_fit_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_fold_metrics_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_locked_holdout_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_predict_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_preprocess_fit_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_selection_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_split_manifest_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_observation_minute_bars_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_signals_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_statistics_minute_v1": "research_pipeline.runtime.adapters.minute_statistics",
    "execute_research_targets_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_validity_adjusted_minute_observation_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_validity_data_observation_v1": "research_pipeline.runtime.adapters.validity",
    "execute_research_validity_minute_observation_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_validity_minute_v1": "research_pipeline.runtime.adapters.minute_statistics",
}


# 下列清单是审阅后的源码边界。共享模块自身的语义依赖明确展开，
# 不把整个 registry、整个包或相邻执行族当成当前节点的实现。
_COMMON_ADAPTER_DEPENDENCIES = (
    "research_pipeline.runtime.adapters.common",
    "research_pipeline.runtime.operator_runtime",
    "research_pipeline.platform.canonical",
    "research_pipeline.data_plane.research_data_bundle",
    "research_pipeline.domain.corporate_actions",
    "research_pipeline.research.semantics",
)
_MINUTE_IO_DEPENDENCIES = (
    "research_pipeline.runtime.adapters.minute_io",
    "research_pipeline.data_plane.minute_scan",
    "research_pipeline.data_plane.minute_resampling",
    "research_pipeline.data_plane.partitioned_artifacts",
    "research_pipeline.domain.session_calendar",
    "research_pipeline.research.minute_operators",
)
_ADAPTER_FAMILY_DEPENDENCIES = {
    "research_pipeline.runtime.adapters.model": (
        "research_pipeline.runtime.walk_forward_model_execution",
        "research_pipeline.research.validation.holdout",
    ),
    "research_pipeline.runtime.adapters.validity": (
        "research_pipeline.runtime.operator_graph_evidence",
        "research_pipeline.runtime.validity_facts_common",
    ),
    "research_pipeline.runtime.adapters.minute_data": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.data_plane.minute_quality",
        "research_pipeline.runtime.operator_graph_evidence",
        "research_pipeline.runtime.validity_facts_common",
        "research_pipeline.platform.metric_contracts",
    ),
    "research_pipeline.runtime.adapters.minute_adjustment": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.catalog.minute",
        "research_pipeline.data_plane.minute_adjustment",
        "research_pipeline.data_plane.minute_quality",
        "research_pipeline.data_plane.dataset_artifacts",
        "research_pipeline.simulation.corporate_actions",
    ),
    "research_pipeline.runtime.adapters.minute_research": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.catalog.minute",
        "research_pipeline.data_plane.minute_adjustment",
    ),
    "research_pipeline.runtime.adapters.minute_simulation": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.data_plane.minute_adjustment",
        "research_pipeline.domain.minute_rule_snapshots",
        "research_pipeline.domain.trading",
        "research_pipeline.platform.minute_operator_contracts",
        "research_pipeline.simulation.bar_tca",
        "research_pipeline.simulation.events",
        "research_pipeline.simulation.minute_execution",
        "research_pipeline.simulation.result_contract",
        "research_pipeline.runtime.bar_tca_adapter",
    ),
    "research_pipeline.runtime.adapters.minute_statistics": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.platform.metric_contracts",
        "research_pipeline.research.statistics.minute_profile",
        "research_pipeline.runtime.operator_graph_evidence",
        "research_pipeline.runtime.validity_facts_common",
    ),
    "research_pipeline.application.grid_data": (
        "research_pipeline.application.grid_data_contract",
        "research_pipeline.application.grid_measurement",
        "research_pipeline.data_plane.service",
        "research_pipeline.data_plane.admitted_plan_codec",
        "research_pipeline.data_plane.dataset_artifacts",
        "research_pipeline.data_plane.execution_budget",
        "research_pipeline.data_plane.execution_estimate",
        "research_pipeline.data_plane.errors",
        "research_pipeline.platform.metric_contracts",
    ),
}


def _parameter(name: str, value_type: ParameterType) -> ParameterSpec:
    return ParameterSpec(name, value_type)


def _choice_parameter(
    name: str, value_type: ParameterType, allowed_values: tuple[object, ...]
) -> ParameterSpec:
    return ParameterSpec(name, value_type, allowed_values=allowed_values)


def _bar_tca_parameters(
    *,
    claim_ceilings: tuple[str, ...],
) -> tuple[ParameterSpec, ...]:
    """Bar TCA 不允许任何会改变结果的隐式参数。"""
    parameters = (
        _choice_parameter(
            "tca_impact_model",
            ParameterType.STRING,
            ("fixed_bps_v1", "sqrt_participation_v1"),
        ),
        _parameter("tca_spread_slippage_bps", ParameterType.INTEGER),
        _parameter("tca_fixed_impact_bps", ParameterType.INTEGER),
        _parameter("tca_sqrt_impact_coefficient_bps", ParameterType.INTEGER),
        _parameter("tca_participation_cap_ppm", ParameterType.INTEGER),
        _choice_parameter(
            "tca_delay_benchmark",
            ParameterType.STRING,
            ("decision_price_v1",),
        ),
        _choice_parameter(
            "tca_rounding_rule",
            ParameterType.STRING,
            ("price_half_up_cost_ceil_v1",),
        ),
        _parameter("tca_policy_available_at", ParameterType.STRING),
        _choice_parameter("tca_claim_ceiling", ParameterType.STRING, claim_ceilings),
    )
    return parameters


def _walk_forward_model_parameters() -> tuple[ParameterSpec, ...]:
    """模型阶段共享的冻结候选、预处理、目标和确定性合同。"""
    return (
        _parameter("candidate_jsons", ParameterType.STRING_LIST),
        _choice_parameter(
            "target_kind", ParameterType.STRING, ("regression", "classification")
        ),
        _choice_parameter(
            "objective",
            ParameterType.STRING,
            ("neg_mean_squared_error", "neg_mean_absolute_error", "accuracy"),
        ),
        _choice_parameter("direction", ParameterType.STRING, ("maximize", "minimize")),
        _parameter("search_id", ParameterType.STRING),
        _parameter("search_frozen_at", ParameterType.STRING),
        _choice_parameter(
            "preprocessing",
            ParameterType.STRING,
            ("median_standardize_v1", "median_only_v1"),
        ),
        _parameter("feature_selection_k", ParameterType.INTEGER),
        _parameter("research_identity_hash", ParameterType.STRING),
        _parameter("simple_model_gate_passed", ParameterType.BOOLEAN),
        _parameter("thread_count", ParameterType.INTEGER),
    )


def _implementation_code_hash(
    *,
    implementation_id: str,
    code_fingerprint: str,
    module_name: str,
    symbol_name: str,
    dependency_modules: tuple[str, ...] = (),
) -> str:
    modules = (module_name, *dependency_modules)
    source_hashes = {}
    for index, dependency in enumerate(modules):
        source_path = _resolve_source_module(dependency, implementation_id)
        source = canonical_python_source_bytes(source_path.read_bytes())
        source_hashes[dependency] = hashlib.sha256(source).hexdigest()
        if index == 0:
            _require_static_symbol(
                source,
                module_name=dependency,
                symbol_name=symbol_name,
                implementation_id=implementation_id,
            )
    return typed_canonical_hash(
        {
            "implementation_id": implementation_id,
            "fingerprint": code_fingerprint,
            "module": module_name,
            "symbol": symbol_name,
            "source_hashes": dict(sorted(source_hashes.items())),
        }
    )


def _resolve_source_module(module_name: str, implementation_id: str) -> Path:
    return _source_module_path(module_name, implementation_id)


def _require_static_symbol(
    source: bytes,
    *,
    module_name: str,
    symbol_name: str,
    implementation_id: str,
) -> None:
    try:
        tree = ast.parse(source, filename=module_name)
    except SyntaxError as exc:
        raise RuntimeRegistryError(
            f"正式 implementation 源码无法解析: {implementation_id}"
        ) from exc
    definitions = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    definitions.update(
        target.id
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        for target in (node.targets if isinstance(node, ast.Assign) else (node.target,))
        if isinstance(target, ast.Name)
    )
    if symbol_name not in definitions:
        raise RuntimeRegistryError(
            f"正式 implementation 没有真实代码落点: {implementation_id}"
        )


def _runtime_adapter_ref(
    implementation_id: str,
    implementation_modules: tuple[str, ...],
) -> OperatorRuntimeAdapterRef:
    adapter_id = f"{implementation_id}.runtime-adapter.v1"
    symbol_name = "execute_" + implementation_id.replace(".", "_").replace("-", "_")
    module_name = _ADAPTER_MODULES_BY_SYMBOL[symbol_name]
    dependency_modules = tuple(
        sorted(
            set(
                (
                    *_COMMON_ADAPTER_DEPENDENCIES,
                    *_ADAPTER_FAMILY_DEPENDENCIES[module_name],
                    *implementation_modules,
                )
            )
            - {module_name}
        )
    )
    code_hash = _implementation_code_hash(
        implementation_id=adapter_id,
        code_fingerprint="operator-runtime-adapter-v1",
        module_name=module_name,
        symbol_name=symbol_name,
        dependency_modules=dependency_modules,
    )
    return OperatorRuntimeAdapterRef(
        adapter_id=adapter_id,
        module_name=module_name,
        symbol_name=symbol_name,
        code_hash=code_hash,
        dependency_modules=dependency_modules,
    )


def _definition(
    operator_id: str,
    implementation_id: str,
    *,
    inputs: tuple[tuple[str, str], ...],
    outputs: tuple[tuple[str, str], ...],
    parameters: tuple[ParameterSpec, ...],
    resource_profile: dict[str, int],
    code_fingerprint: str,
    capability: str,
    module_name: str,
    symbol_name: str,
    dependency_modules: tuple[str, ...] = (),
    roles: tuple[StrategyRole, ...] = (),
    partition_keys: tuple[str, ...] = (),
    seeded: bool = False,
    cache_compatibility_mode: str = "byte_exact",
    implementation_scope: str,
    operator_version: str = _VERSION,
) -> OperatorDefinition:
    code_hash = _implementation_code_hash(
        implementation_id=implementation_id,
        code_fingerprint=code_fingerprint,
        module_name=module_name,
        symbol_name=symbol_name,
        dependency_modules=dependency_modules,
    )
    specification = OperatorSpec.build(
        operator_id=operator_id,
        operator_version=operator_version,
        input_ports=tuple(PortSpec(*item) for item in inputs),
        output_ports=tuple(PortSpec(*item) for item in outputs),
        parameters=parameters,
        strategy_roles=roles,
        resource_profile=resource_profile,
        determinism_mode="seeded" if seeded else "deterministic",
        seed_policy="fixed_root" if seeded else "none",
        code_hash=code_hash,
        pit_capabilities=("pit.as_of.v1", "pit.source_revision.v1"),
    )
    implementation = OperatorImplementationRef(
        implementation_id,
        code_fingerprint,
        capability,
        module_name,
        symbol_name,
        code_hash,
        dependency_modules,
        implementation_scope,
    )
    return OperatorDefinition.build(
        operator_spec=specification,
        implementation_ref=implementation,
        runtime_adapter_ref=_runtime_adapter_ref(
            implementation_id, (module_name, *dependency_modules)
        ),
        cache_profile_ref="cache.semantic-pure.v1",
        cache_compatibility_mode=cache_compatibility_mode,
        resource_hint_ref=f"resource.{operator_id}@{operator_version}",
        partition_keys=partition_keys,
    )
