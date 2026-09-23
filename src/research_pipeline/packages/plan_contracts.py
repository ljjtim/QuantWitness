"""ResearchPackage 编译阶段共享的纯计划合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Mapping

from research_pipeline.data_plane import QueryIR
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.metric_contracts import MetricReachabilityProof
from research_pipeline.platform.operator_contracts import OperatorGraphRecipe
from research_pipeline.research.semantics import ResearchSemantics
from research_pipeline.results.contracts import ResultSpec

from .models import ResearchPackageError


OPERATOR_GRAPH_PLAN_VERSION = "research-operator-graph-package-v2"


@dataclass(frozen=True)
class QueryCompileResult:
    """声明中的 Query IR 纯编译结果，不携带 Catalog 准入资格。"""

    request_ids: tuple[str, ...]
    queries: tuple[QueryIR, ...]

    def __post_init__(self) -> None:
        if (
            not self.request_ids
            or len(self.request_ids) != len(self.queries)
            or len(self.request_ids) != len(set(self.request_ids))
        ):
            raise ResearchPackageError("QueryCompileResult requests 无效")


@dataclass(frozen=True)
class RecipeCompileResult:
    """可信本地注册表对 Recipe 的纯编译结果，不等于平台准入证明。"""

    recipe: OperatorGraphRecipe
    topological_order: tuple[str, ...]
    registry_hash: str
    admission_hash: str
    metric_proofs: tuple[MetricReachabilityProof, ...]

    def __post_init__(self) -> None:
        if (
            not self.topological_order
            or not self.registry_hash
            or not self.admission_hash
            or not self.metric_proofs
        ):
            raise ResearchPackageError("RecipeCompileResult 身份或输出证明无效")


@dataclass(frozen=True)
class OperatorGraphPlan:
    research_id: str
    package_hash: str
    request_ids: tuple[str, ...]
    queries: tuple[QueryIR, ...]
    recipe: OperatorGraphRecipe
    topological_order: tuple[str, ...]
    registry_hash: str
    admission_hash: str
    metric_proofs: tuple[MetricReachabilityProof, ...]
    result_spec: ResultSpec
    root_seed: int
    fixed_clock: str
    plan_hash: str
    project_id: str | None = None
    project_bundle_hashes: tuple[str, ...] = ()
    project_implementation_hashes: Mapping[str, str] | None = None
    verifier_identity: Mapping[str, object] | None = None
    research_semantics: ResearchSemantics | None = None
    contract_version: str = OPERATOR_GRAPH_PLAN_VERSION

    def __post_init__(self) -> None:
        if not self.research_id:
            raise ResearchPackageError("OperatorGraphPlan research_id 不能为空")
        if self.contract_version != OPERATOR_GRAPH_PLAN_VERSION:
            raise ResearchPackageError("OperatorGraphPlan contract version 不受支持")
        if (
            not self.request_ids
            or len(self.request_ids) != len(self.queries)
            or len(set(self.request_ids)) != len(self.request_ids)
        ):
            raise ResearchPackageError("OperatorGraphPlan requests 无效")
        if type(self.root_seed) is not int or self.root_seed < 0:
            raise ResearchPackageError("OperatorGraphPlan root_seed 无效")
        try:
            clock = datetime.fromisoformat(self.fixed_clock)
        except ValueError as exc:
            raise ResearchPackageError("OperatorGraphPlan fixed_clock 无效") from exc
        if clock.tzinfo is None or clock.utcoffset() is None:
            raise ResearchPackageError("OperatorGraphPlan fixed_clock 必须带时区")
        implementation_hashes = dict(self.project_implementation_hashes or {})
        if self.project_bundle_hashes:
            if (
                not self.project_id
                or tuple(sorted(set(self.project_bundle_hashes)))
                != self.project_bundle_hashes
                or not implementation_hashes
            ):
                raise ResearchPackageError("OperatorGraphPlan 项目准入身份无效")
        elif self.project_id is not None or implementation_hashes:
            raise ResearchPackageError("OperatorGraphPlan 项目身份不闭合")
        object.__setattr__(
            self,
            "project_implementation_hashes",
            MappingProxyType(dict(sorted(implementation_hashes.items()))),
        )
        if self.verifier_identity is not None:
            if not isinstance(self.verifier_identity, Mapping):
                raise ResearchPackageError("OperatorGraphPlan Verifier identity 无效")
            verifier_identity = dict(self.verifier_identity)
            if verifier_identity.get("project_id") != (self.project_id or self.research_id):
                raise ResearchPackageError("OperatorGraphPlan Verifier project_id 不一致")
            object.__setattr__(self, "verifier_identity", MappingProxyType(verifier_identity))
        if self.plan_hash != typed_canonical_hash(self.payload()):
            raise ResearchPackageError("OperatorGraphPlan hash 不一致")

    def payload(self) -> dict[str, object]:
        payload = {
            "research_id": self.research_id,
            "package_hash": self.package_hash,
            "requests": [
                {"request_id": request_id, "query": query.to_dict()}
                for request_id, query in zip(self.request_ids, self.queries, strict=True)
            ],
            "recipe": self.recipe.to_dict(),
            "topological_order": list(self.topological_order),
            "registry_hash": self.registry_hash,
            "admission_hash": self.admission_hash,
            "metric_proofs": [item.to_dict() for item in self.metric_proofs],
            "result_spec": self.result_spec.to_dict(),
            "root_seed": self.root_seed,
            "fixed_clock": self.fixed_clock,
            "contract_version": self.contract_version,
        }
        if self.research_semantics is not None:
            payload["research_semantics"] = self.research_semantics.to_dict()
        if self.project_bundle_hashes:
            payload["project_admission"] = {
                "project_id": self.project_id,
                "bundle_hashes": list(self.project_bundle_hashes),
                "implementation_hashes": dict(self.project_implementation_hashes),
            }
        if self.verifier_identity is not None:
            payload["verifier_admission"] = dict(self.verifier_identity)
        return payload


__all__ = [
    "OPERATOR_GRAPH_PLAN_VERSION",
    "OperatorGraphPlan",
    "QueryCompileResult",
    "RecipeCompileResult",
]
