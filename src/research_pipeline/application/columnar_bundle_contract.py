"""节点显式绑定的列式数据 bundle 复核。"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef


def columnar_materialization_plans(
    admitted_plans: Mapping[str, object],
) -> dict[str, object]:
    """分钟请求由引用扫描节点消费，普通列式节点只接收已物化请求。"""

    return {
        request_id: plan
        for request_id, plan in admitted_plans.items()
        if getattr(plan, "minute_dataset_semantics_hash", None) is None
    }


def verify_columnar_data_bundle(
    payload: object,
    admitted_plans: Mapping[str, object],
    artifact_root: Path,
) -> dict[str, Mapping[str, object]]:
    from research_pipeline.data_plane.research_data_bundle import (
        validate_research_data_bundle,
    )

    selected_plans = columnar_materialization_plans(admitted_plans)
    bundle = validate_research_data_bundle(payload)
    expected = {
        key: plan.plan_hash for key, plan in sorted(selected_plans.items())
    }
    actual_hashes = bundle.get("admitted_plan_hashes")
    if (
        not isinstance(actual_hashes, Mapping)
        or any(
            actual_hashes.get(request_id) != plan_hash
            for request_id, plan_hash in expected.items()
        )
    ):
        raise ValueError("columnar data bundle 缺少当前节点绑定的 admitted plan")
    references = bundle.get("references")
    if not isinstance(references, dict) or not set(selected_plans).issubset(references):
        raise ValueError("columnar data bundle 缺少当前节点绑定的 request")
    resolver = ArtifactResolver(artifact_root)
    verified_manifests = {}
    for request_id in sorted(selected_plans):
        dataset = resolver.resolve(
            DatasetArtifactRef.from_dict(references[request_id])
        )
        verified_manifests[request_id] = dict(dataset.manifest)
    return verified_manifests


__all__ = ["columnar_materialization_plans", "verify_columnar_data_bundle"]
