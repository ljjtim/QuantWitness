"""从事件明细独立重建二维聚类与 Holm，不调用生产估计器。"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import math
from statistics import NormalDist, fmean
from typing import Mapping, Sequence

from research_pipeline.platform import typed_canonical_hash

from .contracts import StatisticsError


EVENT_INFERENCE_RECOMPUTE_VERSION = "event-inference-independent-recompute-v1"


def verify_event_inference(
    event_rows: object,
    aggregate_rows: object,
    *,
    inference_semantics: Mapping[str, object],
    tolerance: float = 1e-12,
) -> dict[str, object]:
    """只从事件行重算推断结果，并逐字段核对正式聚合表。"""
    if (
        inference_semantics.get("inference_method")
        != "two_way_cluster_robust_mean_v1"
        or inference_semantics.get("correlation_correction")
        != "two_way_cluster"
        or inference_semantics.get("entity_cluster_field") != "code"
        or inference_semantics.get("time_cluster_field") != "session"
        or inference_semantics.get("cluster_dimensions") != 2
        or inference_semantics.get("confidence_level") != 0.95
        or inference_semantics.get("confidence_interval_method")
        != "two_sided_normal_v1"
        or inference_semantics.get("multiple_testing_method") != "holm"
    ):
        raise StatisticsError("事件推断语义不受独立重算支持")
    event_records = _records(event_rows, "事件明细")
    aggregate_records = _records(aggregate_rows, "事件聚合")
    required_event_fields = {
        "event_id", "relative_session", "abnormal_return", "code", "session",
        "window_hash",
    }
    required_aggregate_fields = {
        "relative_session", "sample_size", "average_abnormal_return",
        "cumulative_average_abnormal_return", "standard_error", "z_value",
        "confidence_low", "confidence_high", "p_value_two_sided_normal",
        "entity_cluster_count", "time_cluster_count",
        "intersection_cluster_count", "holm_adjusted_p_value",
        "rejected_at_family_alpha", "window_hash",
    }
    if (
        not event_records
        or not aggregate_records
        or any(required_event_fields - set(row) for row in event_records)
        or any(required_aggregate_fields - set(row) for row in aggregate_records)
    ):
        raise StatisticsError("事件独立重算输入缺少必需列")
    offsets = tuple(sorted(int(row["relative_session"]) for row in aggregate_records))
    if len(set(offsets)) != len(offsets):
        raise StatisticsError("事件聚合相对日重复")
    by_offset: dict[int, list[Mapping[str, object]]] = defaultdict(list)
    window_hashes = set()
    seen_keys = set()
    for row in event_records:
        offset = int(row["relative_session"])
        key = (str(row["event_id"]), offset)
        if key in seen_keys:
            raise StatisticsError("事件明细经济主键重复")
        seen_keys.add(key)
        by_offset[offset].append(row)
        window_hashes.add(str(row["window_hash"]))
    if tuple(sorted(by_offset)) != offsets or len(window_hashes) != 1:
        raise StatisticsError("事件明细与聚合窗口不闭合")

    expected = []
    raw_p_values = []
    cumulative = 0.0
    z_critical = NormalDist().inv_cdf(0.975)
    for offset in offsets:
        rows = by_offset[offset]
        values = [float(row["abnormal_return"]) for row in rows]
        mean, standard_error, counts = _two_way_cluster_mean(
            values,
            [str(row["code"]) for row in rows],
            [str(row["session"]) for row in rows],
        )
        z_value = mean / standard_error
        p_value = math.erfc(abs(z_value) / math.sqrt(2.0))
        raw_p_values.append(p_value)
        cumulative += mean
        expected.append({
            "relative_session": offset,
            "sample_size": len(values),
            "average_abnormal_return": mean,
            "cumulative_average_abnormal_return": cumulative,
            "standard_error": standard_error,
            "z_value": z_value,
            "confidence_low": mean - z_critical * standard_error,
            "confidence_high": mean + z_critical * standard_error,
            "p_value_two_sided_normal": p_value,
            "entity_cluster_count": counts[0],
            "time_cluster_count": counts[1],
            "intersection_cluster_count": counts[2],
            "window_hash": next(iter(window_hashes)),
        })
    for row, adjusted in zip(expected, _holm(raw_p_values), strict=True):
        row["holm_adjusted_p_value"] = adjusted
        row["rejected_at_family_alpha"] = adjusted <= 0.05

    actual_by_offset = {
        int(row["relative_session"]): row for row in aggregate_records
    }
    for row in expected:
        actual = actual_by_offset[row["relative_session"]]
        for field, value in row.items():
            if not _equal(value, actual[field], tolerance=tolerance):
                raise StatisticsError(
                    f"事件聚合字段与独立重算不一致: {row['relative_session']}.{field}"
                )

    family_payload = {
        "family_id": f"event-window-{next(iter(window_hashes))}",
        "candidate_ids": [str(offset) for offset in offsets],
        "metric": "average_abnormal_return",
        "direction": "two_sided",
        "alpha": 1.0 - float(inference_semantics["confidence_level"]),
        "dependency_assumption": "arbitrary",
        "frozen_at": str(inference_semantics.get("test_family_frozen_at")),
        "stage": "pre_test",
    }
    try:
        frozen_at = datetime.fromisoformat(family_payload["frozen_at"])
    except ValueError as exc:
        raise StatisticsError("事件检验族冻结时间无效") from exc
    if frozen_at.tzinfo is None or frozen_at.utcoffset() is None:
        raise StatisticsError("事件检验族冻结时间必须包含时区")
    family_hash = typed_canonical_hash(family_payload)
    if (
        inference_semantics.get("test_family_id") != family_payload["family_id"]
        or inference_semantics.get("test_family_size") != len(offsets)
        or inference_semantics.get("test_family_hash") != family_hash
    ):
        raise StatisticsError("事件检验族身份与独立重算不一致")
    identity = {
        "contract_version": EVENT_INFERENCE_RECOMPUTE_VERSION,
        "window_hash": next(iter(window_hashes)),
        "event_count": len({str(row["event_id"]) for row in event_records}),
        "aggregate_count": len(expected),
        "relative_sessions": list(offsets),
        "test_family_hash": family_hash,
        "aggregate_recompute_hash": typed_canonical_hash(expected),
        "status": "pass",
    }
    return {**identity, "recompute_hash": typed_canonical_hash(identity)}


def _records(value: object, label: str) -> list[Mapping[str, object]]:
    to_dict = getattr(value, "to_dict", None)
    rows = to_dict("records") if callable(to_dict) else value
    if not isinstance(rows, (list, tuple)) or any(
        not isinstance(row, Mapping) for row in rows
    ):
        raise StatisticsError(f"{label}必须是记录表")
    return list(rows)


def _two_way_cluster_mean(
    values: Sequence[float],
    entity_clusters: Sequence[str],
    time_clusters: Sequence[str],
) -> tuple[float, float, tuple[int, int, int]]:
    if len(values) < 3 or not (
        len(values) == len(entity_clusters) == len(time_clusters)
    ) or any(not math.isfinite(value) for value in values):
        raise StatisticsError("事件聚类样本无效")
    mean = fmean(values)
    centered = [value - mean for value in values]

    def component(clusters: Sequence[object]) -> tuple[float, int]:
        grouped: dict[object, float] = defaultdict(float)
        for cluster, value in zip(clusters, centered, strict=True):
            grouped[cluster] += value
        count = len(grouped)
        if count < 2:
            raise StatisticsError("事件聚类维度至少需要两个簇")
        variance = (
            count / (count - 1.0)
            * sum(value * value for value in grouped.values())
            / (len(values) ** 2)
        )
        return variance, count

    first, first_count = component(entity_clusters)
    second, second_count = component(time_clusters)
    intersection, intersection_count = component(
        tuple(zip(entity_clusters, time_clusters, strict=True))
    )
    variance = first + second - intersection
    if not math.isfinite(variance) or variance <= 0.0:
        raise StatisticsError("事件二维聚类方差退化")
    return mean, math.sqrt(variance), (
        first_count, second_count, intersection_count,
    )


def _holm(p_values: Sequence[float]) -> list[float]:
    order = sorted(range(len(p_values)), key=lambda index: (p_values[index], index))
    adjusted = [0.0] * len(p_values)
    running = 0.0
    for rank, index in enumerate(order):
        running = max(running, p_values[index] * (len(p_values) - rank))
        adjusted[index] = min(1.0, running)
    return adjusted


def _equal(left: object, right: object, *, tolerance: float) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is bool and type(right) is bool and left is right
    if isinstance(left, int) and not isinstance(left, bool):
        return type(right) is int and left == right
    try:
        left_number, right_number = float(left), float(right)
    except (TypeError, ValueError):
        return str(left) == str(right)
    return (
        math.isfinite(left_number)
        and math.isfinite(right_number)
        and math.isclose(left_number, right_number, rel_tol=0.0, abs_tol=tolerance)
    )


__all__ = ["EVENT_INFERENCE_RECOMPUTE_VERSION", "verify_event_inference"]
