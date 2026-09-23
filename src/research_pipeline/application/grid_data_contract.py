"""算子身份边界：grid_data_contract。"""

from __future__ import annotations

from pathlib import Path
from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef
from typing import Mapping


def _columnar_materialization_plans(
    admitted_plans: Mapping[str, object],
) -> dict[str, object]:
    """分钟请求由引用扫描节点消费，禁止在普通数据节点复制 raw 行。"""

    return {
        request_id: plan
        for request_id, plan in admitted_plans.items()
        if getattr(plan, "minute_dataset_semantics_hash", None) is None
    }


def _verify_data_bundle(
    payload,
    admitted_plans,
    artifact_root: Path,
) -> dict[str, Mapping[str, object]]:
    from research_pipeline.data_plane.research_data_bundle import (
        validate_research_data_bundle,
    )

    admitted_plans = _columnar_materialization_plans(admitted_plans)
    payload = validate_research_data_bundle(payload)
    expected = {key: plan.plan_hash for key, plan in sorted(admitted_plans.items())}
    actual_hashes = payload.get("admitted_plan_hashes")
    if (
        not isinstance(actual_hashes, Mapping)
        or any(actual_hashes.get(request_id) != plan_hash for request_id, plan_hash in expected.items())
    ):
        raise ValueError("columnar data bundle 缺少当前节点绑定的 admitted plan")
    resolver = ArtifactResolver(artifact_root)
    references = payload.get("references")
    if not isinstance(references, dict) or not set(admitted_plans).issubset(references):
        raise ValueError("columnar data bundle 缺少当前节点绑定的 request")
    verified_manifests = {}
    for request_id in sorted(admitted_plans):
        raw = references[request_id]
        dataset = resolver.resolve(DatasetArtifactRef.from_dict(raw))
        verified_manifests[request_id] = dict(dataset.manifest)
    return verified_manifests
