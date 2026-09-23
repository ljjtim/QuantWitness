"""正式研究运行共享的数据引用 bundle。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from research_pipeline.platform.canonical import typed_canonical_hash


RESEARCH_DATA_BUNDLE_CONTRACT = "research-data-bundle-v1"


def build_research_data_bundle(
    *,
    admitted_plan_hashes: Mapping[str, str],
    references: Mapping[str, Mapping[str, object]],
    database_fingerprints: Iterable[Mapping[str, object]] = (),
) -> dict[str, object]:
    """生成同时容纳普通列式工件和分钟分区引用的唯一当前合同。"""

    hashes = {str(key): str(value) for key, value in sorted(admitted_plan_hashes.items())}
    normalized_references = {
        str(key): dict(value) for key, value in sorted(references.items())
    }
    if set(hashes) != set(normalized_references):
        raise ValueError("研究数据 bundle 的计划与引用集合不一致")
    unsigned: dict[str, object] = {
        "contract_version": RESEARCH_DATA_BUNDLE_CONTRACT,
        "admitted_plan_hashes": hashes,
        "references": normalized_references,
        "database_fingerprints": [dict(item) for item in database_fingerprints],
    }
    return {**unsigned, "bundle_hash": typed_canonical_hash(unsigned)}


def validate_research_data_bundle(payload: Mapping[str, Any]) -> dict[str, object]:
    """验证当前 bundle 的结构、集合闭合和内容身份。"""

    if payload.get("contract_version") != RESEARCH_DATA_BUNDLE_CONTRACT:
        raise ValueError("研究数据 bundle 合同不受支持")
    hashes = payload.get("admitted_plan_hashes")
    references = payload.get("references")
    fingerprints = payload.get("database_fingerprints")
    if (
        not isinstance(hashes, Mapping)
        or not isinstance(references, Mapping)
        or not isinstance(fingerprints, list)
        or any(not isinstance(item, Mapping) for item in fingerprints)
        or any(not isinstance(item, Mapping) for item in references.values())
    ):
        raise ValueError("研究数据 bundle 字段无效")
    normalized = build_research_data_bundle(
        admitted_plan_hashes={str(key): str(value) for key, value in hashes.items()},
        references={str(key): dict(value) for key, value in references.items()},
        database_fingerprints=fingerprints,
    )
    if payload.get("bundle_hash") != normalized["bundle_hash"]:
        raise ValueError("研究数据 bundle 内容身份不一致")
    return normalized


def merge_research_data_reference(
    payload: Mapping[str, Any] | None,
    *,
    request_id: str,
    admitted_plan_hash: str,
    reference: Mapping[str, object],
) -> dict[str, object]:
    """把一个已准入请求引用加入当前 bundle；同请求只允许幂等重复。"""

    current = (
        build_research_data_bundle(admitted_plan_hashes={}, references={})
        if payload is None
        else validate_research_data_bundle(payload)
    )
    hashes = dict(current["admitted_plan_hashes"])
    references = dict(current["references"])
    previous_hash = hashes.get(request_id)
    previous_reference = references.get(request_id)
    if previous_hash not in (None, admitted_plan_hash) or previous_reference not in (
        None,
        dict(reference),
    ):
        raise ValueError("研究数据 bundle 同一 request_id 出现冲突")
    hashes[request_id] = admitted_plan_hash
    references[request_id] = dict(reference)
    return build_research_data_bundle(
        admitted_plan_hashes=hashes,
        references=references,
        database_fingerprints=current["database_fingerprints"],
    )


def merge_research_data_bundles(
    payloads: Iterable[Mapping[str, Any]],
) -> dict[str, object]:
    """合并已验证 bundle；同一请求只能是完全相同的幂等引用。"""

    hashes: dict[str, str] = {}
    references: dict[str, Mapping[str, object]] = {}
    fingerprints: list[dict[str, object]] = []
    found = False
    for raw in payloads:
        found = True
        current = validate_research_data_bundle(raw)
        current_hashes = current["admitted_plan_hashes"]
        current_references = current["references"]
        for request_id in sorted(current_hashes):
            admitted_plan_hash = str(current_hashes[request_id])
            reference = dict(current_references[request_id])
            if (
                request_id in hashes
                and (
                    hashes[request_id] != admitted_plan_hash
                    or references[request_id] != reference
                )
            ):
                raise ValueError("研究数据 bundle 同一 request_id 出现冲突")
            hashes[request_id] = admitted_plan_hash
            references[request_id] = reference
        for raw_fingerprint in current["database_fingerprints"]:
            fingerprint = dict(raw_fingerprint)
            if fingerprint not in fingerprints:
                fingerprints.append(fingerprint)
    if not found:
        raise ValueError("研究数据 bundle 集合不能为空")
    return build_research_data_bundle(
        admitted_plan_hashes=hashes,
        references=references,
        database_fingerprints=fingerprints,
    )


def select_research_data_references(
    payload: Mapping[str, Any],
    *,
    request_ids: Iterable[str],
) -> dict[str, object]:
    """从混合 bundle 取出指定请求，供对应数据节点恢复和复验。"""

    current = validate_research_data_bundle(payload)
    selected = tuple(sorted(set(request_ids)))
    hashes = current["admitted_plan_hashes"]
    references = current["references"]
    missing = [request_id for request_id in selected if request_id not in references]
    if missing:
        raise ValueError(f"研究数据 bundle 缺少请求: {missing}")
    return build_research_data_bundle(
        admitted_plan_hashes={request_id: hashes[request_id] for request_id in selected},
        references={request_id: references[request_id] for request_id in selected},
        database_fingerprints=current["database_fingerprints"],
    )


__all__ = [
    "RESEARCH_DATA_BUNDLE_CONTRACT",
    "build_research_data_bundle",
    "merge_research_data_bundles",
    "merge_research_data_reference",
    "select_research_data_references",
    "validate_research_data_bundle",
]
