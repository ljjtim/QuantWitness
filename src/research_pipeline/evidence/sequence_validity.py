"""从原始 Feature 和冻结末端独立重建序列窗口，不调用模型采样代码。"""
from __future__ import annotations

from collections import defaultdict
from typing import Sequence

import numpy as np
import pandas as pd

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError


class SequenceWindowVerificationError(MainlineError):
    error_code = "sequence_window_verification_failed"


def verify_sequence_window_facts(
    *, features: pd.DataFrame, targets: pd.DataFrame, calendar_sessions: Sequence,
    feature_columns: Sequence[str], step_len: int, context: pd.DataFrame,
    members: pd.DataFrame, exclusions: pd.DataFrame,
) -> dict[str, int]:
    """核对上下文值、窗口成员及排除理由；不执行模型或反序列化处理器。"""
    def require(condition, message):
        if not condition:
            raise SequenceWindowVerificationError(message)

    days = [pd.Timestamp(day).date() for day in calendar_sessions]
    require(type(step_len) is int and step_len > 0, "窗口长度无效")
    require(bool(days) and days == sorted(set(days)), "冻结日历无效")
    day_positions = {day: index for index, day in enumerate(days)}
    columns = tuple(feature_columns)
    require(bool(columns) and len(set(columns)) == len(columns), "特征顺序无效")
    by_day = defaultdict(dict)
    for row in features.to_dict("records"):
        if str(row["status"]) != "ok":
            continue
        day = pd.Timestamp(row["observation_session"]).date()
        if day not in day_positions:
            continue
        name = f'{row["feature_id"]}__w{int(row["window_sessions"])}'
        require(name in columns, "上下文包含未声明的特征")
        key = (str(row["entity_id"]), day)
        require(name not in by_day[key], "原始 Feature 证券会话特征重复")
        observed, available = pd.Timestamp(row["observation_time"]), pd.Timestamp(row["available_time"])
        require(not pd.isna(observed) and not pd.isna(available) and observed <= available,
                "原始 Feature 观察和可见时间无效")
        by_day[key][name] = row
    actual_context = {}
    for row in context.to_dict("records"):
        key = (str(row["entity_id"]), pd.Timestamp(row["observation_session"]).date())
        require(key not in actual_context, "上下文证券会话重复")
        actual_context[key] = row
    require(set(actual_context) == set(by_day), "上下文未闭合原始 Feature")
    expected_context = {}
    for key, feature_rows in by_day.items():
        facts = {"entity_id": key[0], "observation_session": key[1],
                 "observation_time": max(pd.Timestamp(row["observation_time"]) for row in feature_rows.values()),
                 "feature_available_time": max(pd.Timestamp(row["available_time"]) for row in feature_rows.values()),
                 "feature_count": len(feature_rows),
                 "feature_lineage_hash": typed_canonical_hash(sorted(str(row["lineage_hash"]) for row in feature_rows.values()))}
        actual = actual_context[key]
        require(all(actual[name] == value for name, value in facts.items()), "上下文时点或来源不一致")
        expected_values = [float(feature_rows[name]["value"]) if name in feature_rows else np.nan for name in columns]
        require(np.array_equal(np.asarray([actual[name] for name in columns], dtype=float),
                               np.asarray(expected_values), equal_nan=True), "上下文特征值与原始 Feature 不一致")
        expected_context[key] = facts
    expected_members, expected_exclusions = {}, {}
    seen = set()
    for target in targets.to_dict("records"):
        sid, entity = str(target["sample_id"]), str(target["entity_id"])
        require(sid not in seen, "末端身份重复")
        seen.add(sid)
        day = pd.Timestamp(target["observation_session"]).date()
        decision = pd.Timestamp(target["decision_time"])
        require(day in day_positions and not pd.isna(decision), "末端不在日历内或缺少决策时间")
        index = day_positions[day]
        if index < step_len - 1:
            expected_exclusions[sid] = "insufficient_history"
            continue
        window = []
        for member_day in days[index - step_len + 1:index + 1]:
            row = expected_context.get((entity, member_day))
            if row is None or row["feature_count"] < len(columns):
                expected_exclusions[sid] = "missing_feature"
                break
            if row["feature_available_time"] > decision:
                expected_exclusions[sid] = "feature_not_visible"
                break
            window.append(row)
        if sid not in expected_exclusions:
            for step, row in enumerate(window):
                expected_members[(sid, step)] = {name: value for name, value in row.items() if name != "feature_count"}
    actual_members = {}
    require(set(members.columns) == {"sample_id", "step", "entity_id", "observation_session", "observation_time",
                                     "feature_available_time", "feature_lineage_hash"}, "窗口事实列不完整")
    for row in members.to_dict("records"):
        key = (str(row.pop("sample_id")), row.pop("step"))
        require(key not in actual_members, "窗口成员重复")
        actual_members[key] = row
    require(actual_members == expected_members, "窗口成员、证券、会话、可见时间或来源不一致")
    actual_exclusions = {}
    for row in exclusions.to_dict("records"):
        sid = str(row["sample_id"])
        require(sid not in actual_exclusions, "排除末端重复")
        actual_exclusions[sid] = row["reason_code"]
    require(actual_exclusions == expected_exclusions, "窗口排除末端或理由不一致")
    return {"target_count": len(seen), "window_count": len(seen) - len(expected_exclusions),
            "excluded_count": len(expected_exclusions), "member_count": len(expected_members)}
