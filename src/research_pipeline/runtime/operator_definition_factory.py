"""主线算子定义的统一构造、源码身份和 Runtime adapter 映射。"""

from __future__ import annotations

from functools import lru_cache
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
from research_pipeline.platform.source_identity import (
    canonical_python_source_bytes,
    semantic_python_source_bytes,
)
from .errors import RuntimeRegistryError


def _source_module_path(module_name: str, label: str) -> Path:
    prefix = "research_pipeline."
    if not module_name.startswith(prefix):
        raise RuntimeRegistryError(f"正式源码模块不属于主线: {label}")
    package_root = Path(__file__).parents[1]
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
    "execute_data_catalog_admission_v1": "research_pipeline.runtime.adapters.catalog",
    "execute_data_columnar_materialize_v1": "research_pipeline.application.grid_data",
    "execute_data_minute_adjustment_snapshot_v1": "research_pipeline.runtime.adapters.minute_adjustment",
    "execute_data_minute_adjustment_snapshot_import_v1": "research_pipeline.runtime.adjustment_snapshot_import",
    "execute_data_minute_scan_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_bars_minute_adjust_v1": "research_pipeline.runtime.adapters.minute_adjustment",
    "execute_research_bars_minute_resample_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_features_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_labels_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_model_fit_v2": "research_pipeline.runtime.adapters.model",
    "execute_research_model_import_v1": "research_pipeline.runtime.adapters.model",
    "execute_research_model_fold_metrics_v2": "research_pipeline.runtime.adapters.model",
    "execute_research_model_locked_holdout_v2": "research_pipeline.runtime.adapters.model",
    "execute_research_model_predict_v2": "research_pipeline.runtime.adapters.model",
    "execute_research_model_selection_v2": "research_pipeline.runtime.adapters.model",
    "execute_research_model_split_manifest_v2": "research_pipeline.runtime.adapters.model",
    "execute_research_observation_minute_bars_v1": "research_pipeline.runtime.adapters.minute_data",
    "execute_research_signals_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
    "execute_research_statistics_minute_v1": "research_pipeline.runtime.adapters.minute_statistics",
    "execute_research_targets_intraday_v1": "research_pipeline.runtime.adapters.minute_research",
}


# 下列清单是审阅后的源码边界。共享模块自身的语义依赖明确展开，
# 不把整个 registry、整个包或相邻执行族当成当前节点的实现。
_COMMON_ADAPTER_DEPENDENCIES = (
    "research_pipeline.runtime.adapters.common",
    "research_pipeline.runtime.logical_table_verification",
    "research_pipeline.data_plane.verification_lifecycle",
    "research_pipeline.runtime.operator_runtime",
    "research_pipeline.platform.canonical",
    "research_pipeline.data_plane.research_data_bundle",
    "research_pipeline.domain.corporate_actions",
    "research_pipeline.research.semantics",
)
_MINUTE_IO_DEPENDENCIES = (
    "research_pipeline.domain.minute_scope_input",
    "research_pipeline.domain.minute_scope_columnar",
    "research_pipeline.domain.minute_scope_columnar_coverage",
    "research_pipeline.runtime.adapters.minute_io",
    "research_pipeline.data_plane.minute_scan",
    "research_pipeline.data_plane.archived_inputs",
    "research_pipeline.data_plane.archive_read",
    "research_pipeline.data_plane.dataset_files",
    "research_pipeline.data_plane.minute_resampling",
    "research_pipeline.platform.arrow_rows",
    "research_pipeline.data_plane.partitioned_artifacts",
    "research_pipeline.domain.session_calendar",
    "research_pipeline.domain.minute_request_scope",
    "research_pipeline.domain.cash_price_limits",
    "research_pipeline.domain.minute_rule_snapshots",
    "research_pipeline.research.minute_operators",
)
_ADAPTER_FAMILY_DEPENDENCIES = {
    "research_pipeline.runtime.adjustment_snapshot_import": (
        "research_pipeline.application.published_adjustment_snapshot",
        "research_pipeline.runtime.external_artifact",
    ),


    "research_pipeline.runtime.adapters.model": (
        "research_pipeline.runtime.model_fit_parallel",
        "research_pipeline.research.validation.search_manifest",
        "research_pipeline.runtime.walk_forward_model_execution",
        "research_pipeline.runtime.model_sequence",
        "research_pipeline.research.modeling.sequence",
        "research_pipeline.research.modeling.inputs",
        "research_pipeline.research.dataframe_budget",
        "research_pipeline.research.validation.holdout",
    ),
    "research_pipeline.runtime.adapters.catalog": (
    ),
    "research_pipeline.runtime.adapters.minute_data": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.data_plane.minute_quality",
        "research_pipeline.platform.metric_contracts",
    ),
    "research_pipeline.runtime.adapters.minute_adjustment": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.catalog.minute",
        "research_pipeline.data_plane.minute_adjustment",
        "research_pipeline.data_plane.minute_quality",
        "research_pipeline.data_plane.dataset_artifacts",
        "research_pipeline.data_plane.dataset_files",
        "research_pipeline.data_plane.archive_read",
        "research_pipeline.data_plane.direct_sources",
        "research_pipeline.data_plane.providers.epoch_date",
        "research_pipeline.data_plane.providers.sql",
        "research_pipeline.domain.corporate_action_source",
        "research_pipeline.domain.financial_events",
    ),
    "research_pipeline.runtime.adapters.minute_research": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.catalog.minute",
        "research_pipeline.data_plane.minute_adjustment",
    ),

    "research_pipeline.runtime.adapters.minute_statistics": (
        *_MINUTE_IO_DEPENDENCIES,
        "research_pipeline.platform.metric_contracts",
        "research_pipeline.research.statistics.minute_profile",
    ),
    "research_pipeline.application.grid_data": (
        "research_pipeline.application.grid_data_contract",
        "research_pipeline.data_plane.service",
        "research_pipeline.data_plane.archived_inputs",
        "research_pipeline.data_plane.admitted_plan_codec",
        "research_pipeline.data_plane.dataset_artifacts",
        "research_pipeline.data_plane.dataset_files",
        "research_pipeline.data_plane.archive_read",
        "research_pipeline.data_plane.direct_sources",
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


def _walk_forward_model_parameters() -> tuple[ParameterSpec, ...]:
    """Qlib 候选、目标及确定性合同。"""
    return (
        _parameter("candidate_jsons", ParameterType.STRING_LIST),
        _choice_parameter("target_kind", ParameterType.STRING, ("regression",)),
        _choice_parameter("objective", ParameterType.STRING, ("neg_mean_squared_error", "neg_mean_absolute_error")),
        _choice_parameter("direction", ParameterType.STRING, ("maximize", "minimize")),
        _parameter("search_id", ParameterType.STRING),
        _parameter("search_frozen_at", ParameterType.STRING),
        _parameter("research_identity_hash", ParameterType.STRING),
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
    for dependency in modules:
        source_path = _resolve_source_module(dependency, implementation_id)
        stat = source_path.stat()
        source_hashes[dependency] = _module_source_hash(
            source_path, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns
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


@lru_cache(maxsize=512)
def _module_source_hash(path: Path, size: int, mtime_ns: int, ctime_ns: int) -> str:
    """同一个源码版本供依赖该模块的各算子共享。"""
    return hashlib.sha256(canonical_python_source_bytes(path.read_bytes())).hexdigest()


@lru_cache(maxsize=512)
def _semantic_module_source_hash(path: Path, size: int, mtime_ns: int, ctime_ns: int) -> str:
    """同一个语义源码版本（忽略注释与空行）供语义缓存键复用。"""
    return hashlib.sha256(semantic_python_source_bytes(path.read_bytes())).hexdigest()


def semantic_implementation_code_hash(implementation: OperatorImplementationRef) -> str:
    """计算算子实现与其依赖模块的语义源码哈希（不含 adapter 与 build 工件）。"""
    modules = (implementation.module_name, *implementation.dependency_modules)
    source_hashes = {}
    for dependency in modules:
        source_path = _resolve_source_module(dependency, implementation.implementation_id)
        stat = source_path.stat()
        source_hashes[dependency] = _semantic_module_source_hash(
            source_path, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns
        )
    return typed_canonical_hash(
        {
            "implementation_id": implementation.implementation_id,
            "fingerprint": implementation.code_fingerprint,
            "module": implementation.module_name,
            "symbol": implementation.symbol_name,
            "source_hashes": dict(sorted(source_hashes.items())),
        }
    )


def _runtime_adapter_ref(
    implementation_id: str,
    implementation_modules: tuple[str, ...],
    *,
    code_hash: str | None = None,
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
    code_hash = code_hash or _implementation_code_hash(
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
    optional_inputs: tuple[str, ...] = (),
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
        input_ports=tuple(PortSpec(*item, required=item[0] not in optional_inputs) for item in inputs),
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
            implementation_id, (module_name, *dependency_modules),
        ),
        cache_profile_ref="cache.semantic-pure.v1",
        cache_compatibility_mode=cache_compatibility_mode,
        resource_hint_ref=f"resource.{operator_id}@{operator_version}",
        partition_keys=partition_keys,
    )
