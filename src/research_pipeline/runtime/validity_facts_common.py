"""Validity facts 共用的输入、时间与 holdout 复核。"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
import json
from pathlib import Path
from typing import Mapping

from research_pipeline.data_plane import (
    DatasetArtifactRef,
    PartitionedDatasetRef,
    QueryIRInvalidError,
    parse_aware_datetime,
    resolve_as_of_cutoff,
)
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.holdout_verification import verify_holdout_access_payload
from research_pipeline.platform.claim_levels import CLAIM_LEVELS, weakest_claim_level


def _data_pit_facts(
    admitted_plans: Mapping[str, object],
    observations: list[dict[str, object]],
) -> dict[str, object]:
    """把实际输入的结论上限写入唯一 validity lineage。"""

    ceilings = []
    by_request = {}
    for request_id, plan in sorted(admitted_plans.items()):
        ceiling = getattr(plan, "input_claim_ceiling", None)
        if ceiling not in CLAIM_LEVELS:
            raise EvidenceContractError(f"输入缺少正式 claim ceiling: {request_id}")
        ceilings.append(str(ceiling))
        by_request[request_id] = str(ceiling)
    return {
        "observations": observations,
        "consumed_request_ids": list(by_request),
        "input_claim_ceilings": by_request,
        "effective_claim_ceiling": weakest_claim_level(*ceilings),
    }


def _source_revision_hash(payload: object) -> str:
    """从当前两类正式数据引用读取同一个来源 revision 事实。"""

    if not isinstance(payload, Mapping):
        raise EvidenceContractError("validity 的 data reference 不是映射")
    try:
        if payload.get("contract_version") == "partitioned-dataset-ref":
            reference = PartitionedDatasetRef.from_dict(payload)
            source_revision_hash = reference.lineage.get("source_revision_hash")
            if not _sha256_text(source_revision_hash):
                raise EvidenceContractError("分钟分区引用缺少来源 revision")
            return str(source_revision_hash)
        return DatasetArtifactRef.from_dict(payload).source_revision_hash
    except EvidenceContractError:
        raise
    except Exception as exc:
        raise EvidenceContractError("validity 的 data reference 合同无效") from exc


def _verified_holdout_access(
    root: Path,
    *,
    statistics_manifest: Mapping[str, object],
    statistics_identity: Mapping[str, object],
) -> dict[str, object]:
    plan = _read_mapping(root / "plan.json", "holdout plan")
    prepared = _read_mapping(root / "prepared.json", "holdout prepared")
    opened = _read_mapping(root / "opened.json", "holdout opened")
    terminal = _read_mapping(root / "terminal.json", "holdout terminal")
    retired = _read_mapping(root / "retired.json", "holdout retired")
    return verify_holdout_access_payload({
        "authorized": True,
        "plan": dict(plan),
        "prepared": dict(prepared),
        "opened": dict(opened),
        "terminal": dict(terminal),
        "retired": dict(retired),
    },
        expected_result_hash=str(statistics_manifest.get("semantic_hash")),
        expected_sample_count=int(statistics_manifest["inference_contract"]["sample_count"]),
        expected_candidate_ids=tuple(statistics_manifest["candidate_ids"]),
        expected_plan_hash=str(statistics_manifest.get("holdout_plan_hash")),
        expected_opened_hash=str(statistics_manifest.get("holdout_opened_hash")),
        expected_terminal_hash=str(statistics_identity.get("holdout_terminal_hash")),
    )


def _read_mapping(path: Path, label: str) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceContractError(f"{label} 不可读") from exc
    if not isinstance(payload, Mapping):
        raise EvidenceContractError(f"{label} 必须是对象")
    return payload


def _sha256_text(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _date_ns(value: str) -> int:
    parsed = date.fromisoformat(value[:10])
    return _datetime_ns(datetime.combine(parsed, time.min, tzinfo=timezone.utc))


def _as_of_ns(value: str, *, fixed_clock: str) -> int:
    """按编译器的同一语义投影 QueryIR 可见性截止。"""

    try:
        reference_clock = parse_aware_datetime(fixed_clock, "fixed_clock")
        parsed = resolve_as_of_cutoff(
            value,
            reference_clock=reference_clock,
            field="QueryIR as_of",
        )
    except QueryIRInvalidError as exc:
        raise EvidenceContractError(str(exc)) from exc
    return _datetime_ns(parsed)


def _datetime_ns(value: datetime) -> int:
    if value.tzinfo is None:
        raise EvidenceContractError("validity 时间必须带时区")
    utc_value = value.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc_value - epoch
    return (
        delta.days * 86_400 * 1_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


__all__ = [
    "_as_of_ns",
    "_data_pit_facts",
    "_date_ns",
    "_datetime_ns",
    "_read_mapping",
    "_sha256_text",
    "_source_revision_hash",
    "_verified_holdout_access",
]
