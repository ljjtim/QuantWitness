"""按版本化登记限制存在正确性缺陷的 Result 正式消费动作。"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
import json
from typing import Mapping

from .errors import EvidenceContractError


RESULT_DISPOSITION_REGISTRY_VERSION = "research-result-disposition-registry-v1"
_REGISTRY_RESOURCE = "result_dispositions.v1.json"
_ACTIONS = frozenset({"rerun_required"})
_CONSUMERS = frozenset({"compare", "export", "report"})


@dataclass(frozen=True)
class ResultDisposition:
    """Result 的已核实处置；不修改 Result 本体。"""

    result_id: str
    action: str
    reason: str
    claim_ceiling: str | None = None
    affected_metrics: tuple[str, ...] = ()
    blocked_consumers: tuple[str, ...] = ()


def _parse_disposition(item: object) -> ResultDisposition:
    if not isinstance(item, Mapping) or set(item) != {
        "result_id",
        "action",
        "reason",
        "claim_ceiling",
        "affected_metrics",
        "blocked_consumers",
    }:
        raise EvidenceContractError("Result 处置登记项 schema 无效")
    result_id = item["result_id"]
    action = item["action"]
    reason = item["reason"]
    claim_ceiling = item["claim_ceiling"]
    affected_metrics = item["affected_metrics"]
    blocked_consumers = item["blocked_consumers"]
    if (
        not isinstance(result_id, str)
        or len(result_id) != 64
        or any(character not in "0123456789abcdef" for character in result_id)
        or action not in _ACTIONS
        or not isinstance(reason, str)
        or not reason
        or (claim_ceiling is not None and not isinstance(claim_ceiling, str))
        or not isinstance(affected_metrics, list)
        or not all(isinstance(value, str) and value for value in affected_metrics)
        or not isinstance(blocked_consumers, list)
        or not all(value in _CONSUMERS for value in blocked_consumers)
    ):
        raise EvidenceContractError("Result 处置登记项内容无效")
    return ResultDisposition(
        result_id=result_id,
        action=action,
        reason=reason,
        claim_ceiling=claim_ceiling,
        affected_metrics=tuple(affected_metrics),
        blocked_consumers=tuple(blocked_consumers),
    )


@lru_cache(maxsize=1)
def load_result_dispositions() -> Mapping[str, ResultDisposition]:
    """读取随 evidence 发布的唯一 Result 处置登记。"""

    try:
        payload = json.loads(
            resources.files(__package__)
            .joinpath(_REGISTRY_RESOURCE)
            .read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceContractError("Result 处置登记不可读取") from exc
    if (
        not isinstance(payload, Mapping)
        or set(payload) != {"contract_version", "dispositions"}
        or payload.get("contract_version") != RESULT_DISPOSITION_REGISTRY_VERSION
        or not isinstance(payload.get("dispositions"), list)
    ):
        raise EvidenceContractError("Result 处置登记 schema 无效")
    dispositions = tuple(
        _parse_disposition(item) for item in payload["dispositions"]
    )
    indexed = {item.result_id: item for item in dispositions}
    if len(indexed) != len(dispositions):
        raise EvidenceContractError("Result 处置登记包含重复 result_id")
    return indexed


def disposition_for_result(result_id: str) -> ResultDisposition:
    """返回已登记处置；未知 Result 不做推断性降级。"""

    disposition = load_result_dispositions().get(result_id)
    if disposition is not None:
        return disposition
    return ResultDisposition(
        result_id=result_id,
        action="continue",
        reason="没有命中已登记的旧 Result 正确性问题。",
    )


def require_result_consumable(result_id: str, *, consumer: str) -> None:
    """在 report/compare/export 的直接消费边界执行旧 Result 处置。"""

    disposition = disposition_for_result(result_id)
    if consumer in disposition.blocked_consumers:
        raise EvidenceContractError(
            f"旧 Result {result_id} 暂不能用于 {consumer}：{disposition.reason}"
        )


__all__ = [
    "RESULT_DISPOSITION_REGISTRY_VERSION",
    "ResultDisposition",
    "disposition_for_result",
    "load_result_dispositions",
    "require_result_consumable",
]
