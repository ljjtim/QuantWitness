"""ResearchPackage 编译编排；只负责选择固定的内置编译路径。"""

from __future__ import annotations

from .compiler import compile_package_queries
from .models import ResearchPackage, ResearchPackageError
from .operator_graph import (
    OPERATOR_GRAPH_BUILDER_ID,
    compile_operator_graph_package,
)
from .plan_contracts import OperatorGraphPlan


def registered_package_builders() -> tuple[str, ...]:
    """返回代码内静态绑定的 builder；声明不能注入动态实现。"""
    return (OPERATOR_GRAPH_BUILDER_ID,)


def compile_research_package(
    package: ResearchPackage,
    *,
    admission: object | None = None,
    platform_admission: object | None = None,
    verifier_admission: object | None = None,
) -> OperatorGraphPlan:
    if package.builder_id == OPERATOR_GRAPH_BUILDER_ID:
        queries = compile_package_queries(package)
        return compile_operator_graph_package(
            package,
            query_result=queries,
            admission=admission,
            platform_admission=platform_admission,
            verifier_admission=verifier_admission,
        )
    raise ResearchPackageError(f"未登记的 ResearchPackage builder: {package.builder_id}")


__all__ = ["compile_research_package", "registered_package_builders"]
