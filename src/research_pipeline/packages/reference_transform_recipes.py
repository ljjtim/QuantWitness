"""公共 Recipe 发现边界。

完整研究拓扑、资产画像、默认窗口和项目 ResultSpec 由各项目自己的
ResearchPackage 与扩展维护。当前没有满足异构复用和人工批准条件的公共 Recipe。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash


BUILTIN_WORKFLOW_PROFILE_IDENTITIES: frozenset[str] = frozenset()


class RecipeScaffoldError(ValueError):
    """公共 Recipe 不存在或不能用于 scaffold。"""

    def __init__(
        self,
        message: str,
        *,
        missing_requirements: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.missing_requirements = missing_requirements


@dataclass(frozen=True)
class ReferenceTransformRecipe:
    """保留发现层类型边界；实例只能来自以后正式批准的公共 Recipe。"""

    reference_id: str
    research_kind: str
    execution_ready: bool
    frequency: str
    supported_asset_profiles: tuple[str, ...]
    questions: tuple[object, ...]
    graph: Mapping[str, object]
    contract_version: str = "research-reference-transform-recipe-v2"


def build_reference_transform_recipes() -> tuple[ReferenceTransformRecipe, ...]:
    """返回已批准的公共 Recipe；当前没有。"""

    return ()


def scaffold_research_package(
    recipe: ReferenceTransformRecipe,
    *,
    answers: Mapping[str, object],
    output: str | Path,
    admission: object,
    dataset_versions: Mapping[str, int],
) -> dict[str, object]:
    """拒绝未注册的完整研究拓扑进入公共 scaffold。"""

    del recipe, answers, output, admission, dataset_versions
    raise RecipeScaffoldError(
        "当前没有已批准的公共 recipe；请使用 package init 或项目自有模板",
        missing_requirements=("approved_public_recipe",),
    )


def recipe_identity(recipe: ReferenceTransformRecipe) -> str:
    """计算正式公共 Recipe 的发现身份。"""

    return typed_canonical_hash({
        "reference_id": recipe.reference_id,
        "research_kind": recipe.research_kind,
        "execution_ready": recipe.execution_ready,
        "frequency": recipe.frequency,
        "supported_asset_profiles": list(recipe.supported_asset_profiles),
        "graph": dict(recipe.graph),
        "contract_version": recipe.contract_version,
    })


__all__ = [
    "BUILTIN_WORKFLOW_PROFILE_IDENTITIES",
    "RecipeScaffoldError",
    "ReferenceTransformRecipe",
    "build_reference_transform_recipes",
    "recipe_identity",
    "scaffold_research_package",
]
