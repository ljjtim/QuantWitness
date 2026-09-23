"""研究框架三层边界和通用算子晋级合同。"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from research_pipeline.platform.operator_contracts import (
    OperatorSpec,
    ParameterType,
)

from .errors import ExtensionError


FRAMEWORK_LAYER = "framework"
PROJECT_EXTENSION_LAYER = "project_extension"
DECLARATION_LAYER = "declaration"
PROMOTION_CONTRACT_VERSION = "research-operator-promotion-v2"

_PROJECT_PATH_MARKERS = (
    "project_extensions",
    "research_packages",
    "retirement_migration",
    "reference_profile",
    "reference_scopes",
)
_ID = re.compile(r"^[a-z][a-z0-9_.:-]*$")
_VERSION = re.compile(r"^[0-9]+(?:\.[0-9]+){2}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ExtensionError(f"{field} 必须是稳定小写 ID")
    return value


def _require_evidence_reference(value: object, field: str) -> str:
    if isinstance(value, str) and _ID.fullmatch(value):
        return value
    if (
        not isinstance(value, str)
        or value.count("::") != 1
        or "\\" in value
        or value.startswith("/")
    ):
        raise ExtensionError(f"{field} 必须是稳定 ID 或仓库内 pytest 节点")
    relative_path, test_name = value.split("::", 1)
    parts = relative_path.split("/")
    if (
        not relative_path.endswith(".py")
        or not test_name.startswith("test_")
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ExtensionError(f"{field} 必须是稳定 ID 或仓库内 pytest 节点")
    return value


def _roots(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value).split(".", 1)[0] for value in values}))


def validate_framework_implementation_boundary(
    *,
    implementation_scope: str,
    implementation_id: str,
    module_name: str,
    dependency_modules: Iterable[str] = (),
) -> None:
    """检查公共实现不能反向依赖项目、参考样本或退休材料。"""
    if implementation_scope not in {"core", "project"}:
        raise ExtensionError("implementation_scope 必须是 core 或 project")
    values = (implementation_id, module_name, *tuple(dependency_modules))
    lowered = tuple(value.lower() for value in values)
    if implementation_scope == "core":
        if any(
            any(marker in value for marker in _PROJECT_PATH_MARKERS)
            for value in lowered
        ):
            raise ExtensionError("公共实现不得依赖项目、参考样本或退休目录")
        if not module_name.startswith("research_pipeline."):
            raise ExtensionError("公共实现必须位于 research_pipeline 包内")


def validate_project_operator_artifacts(
    spec: OperatorSpec,
    registered_operator_specs: Iterable[OperatorSpec],
    *,
    project_artifact_types: Iterable[str] = (),
) -> None:
    """校验项目算子的 Artifact 类型和正式因果输出声明。"""

    registered_types = {
        port.artifact_type
        for specification in registered_operator_specs
        for port in (*specification.input_ports, *specification.output_ports)
    }
    artifact_types = {
        port.artifact_type for port in (*spec.input_ports, *spec.output_ports)
    }
    unknown = artifact_types - registered_types - set(project_artifact_types)
    if unknown:
        raise ExtensionError(f"项目算子引用未登记 Artifact/schema: {sorted(unknown)}")
    formal_causal_outputs = tuple(
        item
        for item in spec.output_ports
        if item.artifact_type in {"research.feature-set.v1", "research.label.v1"}
    )
    if not formal_causal_outputs:
        return
    causal_parameters = tuple(
        item for item in spec.parameters if item.name == "causal_plan"
    )
    valid = (
        len(formal_causal_outputs) == 1
        and len(spec.output_ports) == 1
        and len(causal_parameters) == 1
        and causal_parameters[0].required
        and causal_parameters[0].value_type is ParameterType.JSON
    )
    if not valid:
        raise ExtensionError(
            "无法从项目输入生成核心逐行时间事实：正式因果算子必须"
            "单输出并声明必填 JSON causal_plan"
        )


def validate_project_dependency_lock(dependencies: Iterable[str]) -> None:
    """项目 Worker 不得通过依赖锁反向导入框架内部实现。"""
    forbidden = {
        "research_pipeline",
        "research_dashboard",
        "quantdb",
        "jq_remote",
        "factor_calc",
        "jq_collector_v2",
    }
    blocked = sorted(forbidden.intersection(_roots(dependencies)))
    if blocked:
        raise ExtensionError(f"项目扩展不得反向依赖框架或其他运行系统: {blocked}")


@dataclass(frozen=True)
class OperatorPromotionReview:
    """通用算子评审记录；不是新的运行时或签名收据。"""

    operator_id: str
    operator_version: str
    implementation_id: str
    module_name: str
    definition_hash: str
    status: str
    reuse_projects: tuple[str, ...]
    oracle_ids: tuple[str, ...]
    attack_ids: tuple[str, ...]
    heterogeneous_reuse_confirmed: bool
    generic_semantics_confirmed: bool
    contract_version: str = PROMOTION_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _require_id(self.operator_id, "operator_id")
        if not isinstance(self.operator_version, str) or not _VERSION.fullmatch(
            self.operator_version
        ):
            raise ExtensionError("operator_version 必须是三段数字版本")
        _require_id(self.implementation_id, "implementation_id")
        if not isinstance(self.module_name, str) or not self.module_name:
            raise ExtensionError("module_name 必须是非空模块路径")
        if not isinstance(self.definition_hash, str) or not _HASH.fullmatch(
            self.definition_hash
        ):
            raise ExtensionError("definition_hash 必须是 64 位小写摘要")
        if self.status not in {"candidate", "approved", "rejected"}:
            raise ExtensionError("算子评审状态无效")
        for field in ("reuse_projects", "oracle_ids", "attack_ids"):
            values = getattr(self, field)
            if tuple(sorted(values)) != values or len(set(values)) != len(values):
                raise ExtensionError(f"{field} 必须唯一并规范排序")
            for value in values:
                if field == "reuse_projects":
                    _require_id(value, field)
                else:
                    _require_evidence_reference(value, field)
        if type(self.heterogeneous_reuse_confirmed) is not bool:
            raise ExtensionError("heterogeneous_reuse_confirmed 必须是 bool")
        if type(self.generic_semantics_confirmed) is not bool:
            raise ExtensionError("generic_semantics_confirmed 必须是 bool")
        if self.contract_version != PROMOTION_CONTRACT_VERSION:
            raise ExtensionError("算子晋级合同版本不受支持")
        if self.status == "approved":
            if len(self.reuse_projects) < 2:
                raise ExtensionError("通用算子至少需要两个异构复用项目")
            if not self.oracle_ids or not self.attack_ids:
                raise ExtensionError("通用算子缺少独立 oracle 或攻击测试")
            if not self.heterogeneous_reuse_confirmed:
                raise ExtensionError("通用算子缺少异构复用人工结论")
            if not self.generic_semantics_confirmed:
                raise ExtensionError("通用算子缺少通用语义人工结论")
            validate_framework_implementation_boundary(
                implementation_scope="core",
                implementation_id=self.implementation_id,
                module_name=self.module_name,
            )

    @property
    def is_public(self) -> bool:
        return self.status == "approved"

    def to_dict(self) -> dict[str, object]:
        return {
            "operator_id": self.operator_id,
            "operator_version": self.operator_version,
            "implementation_id": self.implementation_id,
            "module_name": self.module_name,
            "definition_hash": self.definition_hash,
            "status": self.status,
            "reuse_projects": list(self.reuse_projects),
            "oracle_ids": list(self.oracle_ids),
            "attack_ids": list(self.attack_ids),
            "heterogeneous_reuse_confirmed": self.heterogeneous_reuse_confirmed,
            "generic_semantics_confirmed": self.generic_semantics_confirmed,
            "contract_version": self.contract_version,
        }


__all__ = [
    "DECLARATION_LAYER",
    "FRAMEWORK_LAYER",
    "PROJECT_EXTENSION_LAYER",
    "PROMOTION_CONTRACT_VERSION",
    "OperatorPromotionReview",
    "validate_framework_implementation_boundary",
    "validate_project_dependency_lock",
]
