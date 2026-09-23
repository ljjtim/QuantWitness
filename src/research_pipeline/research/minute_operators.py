"""受控分钟特征、标签和信号算子的纯函数实现。"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
import math
from statistics import pstdev
from types import MappingProxyType
from typing import Callable, Iterable, Mapping, Sequence

import pandas as pd

from research_pipeline.domain import (
    InstrumentKey,
    PortfolioTarget,
    PortfolioTargetEntry,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.asset_taxonomy import MINUTE_TARGET_ASSET_CLASSES
from research_pipeline.platform.minute_operator_contracts import (
    MINUTE_ADJUSTMENT_MODES,
    MINUTE_ASSET_CLASSES,
    MINUTE_FEATURE_IDS,
    MINUTE_GAP_POLICIES,
    MINUTE_LABEL_EVENTS,
    MINUTE_SIGNAL_RULES,
    MINUTE_TARGET_PAYLOAD_SCHEMA_ID,
)
from research_pipeline.platform.causal_time import (
    validate_feature_time_facts,
    validate_label_time_facts,
)


MINUTE_OPERATOR_ARTIFACT_VERSION = "minute-operator-artifact-v2"
PreWindowRebaser = Callable[
    [Sequence[Mapping[str, object]], datetime],
    tuple[dict[str, object], ...],
]


class MinuteOperatorError(ValueError):
    """分钟算子输入或时间边界不符合封闭合同。"""


@dataclass(frozen=True)
class MinuteOperatorArtifact:
    artifact_type: str
    rows: tuple[Mapping[str, object], ...]
    manifest: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.artifact_type:
            raise MinuteOperatorError("分钟算子 artifact type 不能为空")
        object.__setattr__(
            self,
            "rows",
            tuple(MappingProxyType(dict(item)) for item in self.rows),
        )
        object.__setattr__(self, "manifest", MappingProxyType(dict(self.manifest)))

    @property
    def content_hash(self) -> str:
        return typed_canonical_hash(_canonical_rows(self.rows))


class IntradayFeatureStream:
    """按已排序分钟行增量计算特征，只保留各标的滚动窗口。"""

    def __init__(
        self,
        *,
        decision_time: datetime,
        feature_ids: tuple[str, ...],
        lookback_bars: int,
        warmup_bars: int,
        gap_policy: str,
        cross_session: bool,
        adjustment_mode: str,
        asset_class: str,
        interval_minutes: int,
        availability_policy_ref: str = "pit.completed-bar.v1",
        pre_window_rebaser: PreWindowRebaser | None = None,
    ) -> None:
        _validate_feature_parameters(
            decision_time=decision_time,
            feature_ids=feature_ids,
            lookback_bars=lookback_bars,
            warmup_bars=warmup_bars,
            gap_policy=gap_policy,
            cross_session=cross_session,
            adjustment_mode=adjustment_mode,
            asset_class=asset_class,
            interval_minutes=interval_minutes,
            availability_policy_ref=availability_policy_ref,
            pre_window_rebaser=pre_window_rebaser,
        )
        self.decision_time = decision_time
        self.feature_ids = feature_ids
        self.lookback_bars = lookback_bars
        self.warmup_bars = warmup_bars
        self.gap_policy = gap_policy
        self.cross_session = cross_session
        self.interval_minutes = interval_minutes
        self.pre_window_rebaser = pre_window_rebaser
        self.history: dict[
            tuple[str, str], deque[Mapping[str, object]]
        ] = defaultdict(
            lambda: deque(maxlen=max(lookback_bars + 1, warmup_bars + 1))
        )
        self._session_by_instrument: dict[str, str] = {}
        self._last_end_by_instrument: dict[str, datetime] = {}

    def consume(self, row: Mapping[str, object]) -> Mapping[str, object] | None:
        _validate_visible_completed_row(row, decision_time=self.decision_time)
        instrument = str(row.get("instrument"))
        session_id = str(row.get("session_id"))
        current_end = _time(row, "bar_end")
        previous_end = self._last_end_by_instrument.get(instrument)
        if previous_end is not None and current_end <= previous_end:
            raise MinuteOperatorError("分钟特征输入未按标的观测时间严格递增")
        self._last_end_by_instrument[instrument] = current_end
        if not self.cross_session:
            previous_session = self._session_by_instrument.get(instrument)
            if previous_session is not None and previous_session != session_id:
                self.history.pop((instrument, previous_session), None)
            self._session_by_instrument[instrument] = session_id
        key = (instrument, "*" if self.cross_session else session_id)
        window = self.history[key]
        if window:
            window_previous_end = _time(window[-1], "bar_end")
            elapsed = (current_end - window_previous_end).total_seconds() / 60
            session_changed = str(window[-1].get("session_id")) != session_id
            if elapsed != self.interval_minutes and not (
                self.cross_session and session_changed
            ):
                if self.gap_policy == "fail":
                    raise MinuteOperatorError("分钟特征窗口存在缺口")
                window.clear()
        window.append(row)
        if len(window) < self.warmup_bars:
            return None
        trailing: Sequence[Mapping[str, object]] = list(window)[
            -self.lookback_bars:
        ]
        if self.pre_window_rebaser is not None:
            selection_at = max(
                _time(row, "bar_end"), _time(row, "available_time")
            )
            trailing = self.pre_window_rebaser(trailing, selection_at)
            if len(trailing) != self.lookback_bars:
                raise MinuteOperatorError("pre 分钟特征重定基器改变了窗口行数")
        closes = [_number(item, "close") for item in trailing]
        volumes = [_number(item, "volume") for item in trailing]
        values: dict[str, float | None] = {}
        if "lagged_return" in self.feature_ids:
            values["lagged_return"] = closes[-1] / closes[-2] - 1.0
        if "rolling_volatility" in self.feature_ids:
            returns = [
                closes[index] / closes[index - 1] - 1.0
                for index in range(1, len(closes))
            ]
            values["rolling_volatility"] = (
                pstdev(returns) if len(returns) > 1 else 0.0
            )
        if "volume_ratio" in self.feature_ids:
            average = sum(volumes) / len(volumes)
            values["volume_ratio"] = None if average == 0 else volumes[-1] / average
        if "vwap_close_deviation" in self.feature_ids:
            avg = trailing[-1].get("avg")
            values["vwap_close_deviation"] = (
                None
                if avg is None or float(avg) == 0
                else closes[-1] / float(avg) - 1.0
            )
        result_row: dict[str, object] = {
            "instrument": instrument,
            "bar_end": _time(row, "bar_end"),
            "available_time": _time(row, "available_time"),
            "decision_time": max(
                _time(row, "bar_end"), _time(row, "available_time")
            ),
            "max_source_observation_time": max(
                _time(item, "bar_end") for item in trailing
            ),
            "max_source_available_time": max(
                _time(item, "available_time") for item in trailing
            ),
            "source_partition_ids": sorted(
                {
                    str(item.get("source_partition_id"))
                    for item in trailing
                    if isinstance(item.get("source_partition_id"), str)
                    and str(item.get("source_partition_id"))
                }
            ),
            "session_id": session_id,
            "source_snapshot_hash": str(row.get("source_snapshot_hash")),
            "features": dict(sorted(values.items())),
        }
        if self.pre_window_rebaser is not None:
            result_row["adjustment_anchor_factor"] = _number(
                trailing[-1], "adjustment_anchor_factor"
            )
            result_row["adjustment_snapshot_identity_hash"] = str(
                trailing[-1].get("factor_snapshot_hash")
            )
        return result_row


class IntradayLabelStream:
    """按已排序分钟行增量生成标签，只保留各标的 horizon 尾部。"""

    def __init__(
        self,
        *,
        entry_event: str,
        exit_event: str,
        horizon_bars: int,
        overlap: bool,
        adjustment_mode: str,
        asset_class: str,
        interval_minutes: int,
        availability_policy_ref: str = "pit.label-horizon.v1",
        pre_window_rebaser: PreWindowRebaser | None = None,
    ) -> None:
        _validate_label_parameters(
            entry_event=entry_event,
            exit_event=exit_event,
            horizon_bars=horizon_bars,
            adjustment_mode=adjustment_mode,
            asset_class=asset_class,
            interval_minutes=interval_minutes,
            availability_policy_ref=availability_policy_ref,
            pre_window_rebaser=pre_window_rebaser,
        )
        self.entry_event = entry_event
        self.exit_event = exit_event
        self.horizon_bars = horizon_bars
        self.overlap = overlap
        self.pre_window_rebaser = pre_window_rebaser
        self.entry_shift = 1 if entry_event == "next_bar_open" else 0
        self.span = horizon_bars + self.entry_shift
        self.history: dict[str, deque[tuple[int, Mapping[str, object]]]] = defaultdict(
            lambda: deque(maxlen=self.span + 1)
        )
        self.next_index: dict[str, int] = defaultdict(int)

    def consume(self, row: Mapping[str, object]) -> Mapping[str, object] | None:
        if row.get("completed") is not True or row.get("quality_status") != "pass":
            raise MinuteOperatorError("分钟标签要求已完成且质量通过的 bar")
        instrument = str(row.get("instrument"))
        history = self.history[instrument]
        absolute_index = self.next_index[instrument]
        self.next_index[instrument] += 1
        if history and _time(row, "bar_end") <= _time(history[-1][1], "bar_end"):
            raise MinuteOperatorError("分钟标签输入未按标的观测时间严格递增")
        history.append((absolute_index, row))
        decision_index = absolute_index - self.span
        if decision_index < 0 or (
            not self.overlap and decision_index % self.horizon_bars
        ):
            return None
        by_index = {index: item for index, item in history}
        decision_row = by_index.get(decision_index)
        entry = by_index.get(decision_index + self.entry_shift)
        exit_row = by_index.get(absolute_index)
        if decision_row is None or entry is None or exit_row is None:
            raise MinuteOperatorError("分钟标签 horizon 滚动状态不完整")
        selected_rows: Sequence[Mapping[str, object]] = (entry, exit_row)
        if self.pre_window_rebaser is not None:
            selected_rows = self.pre_window_rebaser(
                selected_rows,
                _time(exit_row, "available_time"),
            )
            if len(selected_rows) != 2:
                raise MinuteOperatorError("pre 分钟标签重定基器改变了窗口行数")
            entry, exit_row = selected_rows
        entry_price = _number(
            entry, "close" if self.entry_event == "bar_close" else "open"
        )
        exit_price = _number(
            exit_row, "close" if self.exit_event == "bar_close" else "open"
        )
        decision_time = max(
            _time(decision_row, "bar_end"),
            _time(decision_row, "available_time"),
        )
        entry_actual_time = _event_observation_time(entry, self.entry_event)
        exit_actual_time = _event_observation_time(exit_row, self.exit_event)
        first_actual_time = (
            entry_actual_time if entry_actual_time > decision_time else exit_actual_time
        )
        result_row: dict[str, object] = {
            "instrument": instrument,
            "bar_end": _time(decision_row, "bar_end"),
            "available_time": _time(exit_row, "available_time"),
            "decision_time": decision_time,
            "label_start": _time(entry, "bar_end"),
            "label_end": _time(exit_row, "bar_end"),
            "first_actual_observation_time": first_actual_time,
            "last_actual_observation_time": exit_actual_time,
            "source_snapshot_hash": str(decision_row.get("source_snapshot_hash")),
            "future_return": exit_price / entry_price - 1.0,
        }
        if self.pre_window_rebaser is not None:
            result_row["adjustment_anchor_factor"] = _number(
                exit_row, "adjustment_anchor_factor"
            )
            result_row["adjustment_snapshot_identity_hash"] = str(
                exit_row.get("factor_snapshot_hash")
            )
        return result_row


def _canonical_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value


def _canonical_rows(rows: tuple[Mapping[str, object], ...]) -> list[object]:
    return [_canonical_value(dict(item)) for item in rows]


def _time(row: Mapping[str, object], field: str) -> datetime:
    value = row.get(field)
    if not isinstance(value, datetime):
        raise MinuteOperatorError(f"分钟算子行缺少 datetime 字段: {field}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise MinuteOperatorError(f"分钟算子时间字段必须带时区: {field}")
    return value


def _number(row: Mapping[str, object], field: str) -> float:
    value = row.get(field)
    if type(value) not in {int, float} or not math.isfinite(float(value)):
        raise MinuteOperatorError(f"分钟算子行缺少有限数值字段: {field}")
    return float(value)


def _validate_visible_completed_row(
    row: Mapping[str, object], *, decision_time: datetime
) -> None:
    bar_end = _time(row, "bar_end")
    available_time = _time(row, "available_time")
    if row.get("completed") is not True:
        raise MinuteOperatorError("分钟特征和信号只能读取已完成 bar")
    if bar_end > decision_time or available_time > decision_time:
        raise MinuteOperatorError("分钟算子读取了 decision_time 尚不可见的 bar")
    if row.get("quality_status") != "pass":
        raise MinuteOperatorError("分钟算子要求分钟质量门禁为 pass")
    instrument = row.get("instrument")
    snapshot_hash = row.get("source_snapshot_hash")
    source_partition_id = row.get("source_partition_id")
    if not isinstance(instrument, str) or not instrument:
        raise MinuteOperatorError("分钟算子 instrument 不能为空")
    if (
        not isinstance(snapshot_hash, str)
        or len(snapshot_hash) != 64
        or any(char not in "0123456789abcdef" for char in snapshot_hash)
    ):
        raise MinuteOperatorError("分钟算子 source snapshot hash 无效")
    if not isinstance(source_partition_id, str) or not source_partition_id:
        raise MinuteOperatorError("分钟 Feature 输入缺少 source_partition_id")


def _validate_feature_parameters(
    *,
    decision_time: datetime,
    feature_ids: tuple[str, ...],
    lookback_bars: int,
    warmup_bars: int,
    gap_policy: str,
    cross_session: bool,
    adjustment_mode: str,
    asset_class: str,
    interval_minutes: int,
    availability_policy_ref: str,
    pre_window_rebaser: PreWindowRebaser | None,
) -> None:
    if decision_time.tzinfo is None or decision_time.utcoffset() is None:
        raise MinuteOperatorError("分钟特征 decision_time 必须带时区")
    if tuple(sorted(set(feature_ids))) != feature_ids or not set(
        feature_ids
    ).issubset(MINUTE_FEATURE_IDS):
        raise MinuteOperatorError("分钟 feature_ids 必须来自白名单并规范排序")
    if (
        type(lookback_bars) is not int
        or type(warmup_bars) is not int
        or lookback_bars < 2
        or warmup_bars < lookback_bars
    ):
        raise MinuteOperatorError("分钟特征 lookback/warmup 无效")
    if (
        gap_policy not in MINUTE_GAP_POLICIES
        or adjustment_mode not in MINUTE_ADJUSTMENT_MODES
    ):
        raise MinuteOperatorError("分钟特征缺口或复权策略不受支持")
    if (
        asset_class not in MINUTE_ASSET_CLASSES
        or interval_minutes not in {1, 5, 15, 30, 60, 120}
    ):
        raise MinuteOperatorError("分钟特征资产或周期不受支持")
    if not availability_policy_ref.strip():
        raise MinuteOperatorError("分钟特征 availability policy 不能为空")
    if (adjustment_mode == "pre") != (pre_window_rebaser is not None):
        raise MinuteOperatorError("pre 分钟特征必须且只能声明逐决策窗口重定基器")


def _validate_label_parameters(
    *,
    entry_event: str,
    exit_event: str,
    horizon_bars: int,
    adjustment_mode: str,
    asset_class: str,
    interval_minutes: int,
    availability_policy_ref: str,
    pre_window_rebaser: PreWindowRebaser | None,
) -> None:
    if entry_event not in MINUTE_LABEL_EVENTS or exit_event not in MINUTE_LABEL_EVENTS:
        raise MinuteOperatorError("分钟标签 entry/exit event 不受支持")
    if (
        type(horizon_bars) is not int
        or horizon_bars < 1
        or adjustment_mode not in MINUTE_ADJUSTMENT_MODES
        or asset_class not in MINUTE_ASSET_CLASSES
        or interval_minutes not in {1, 5, 15, 30, 60, 120}
        or not availability_policy_ref.strip()
    ):
        raise MinuteOperatorError("分钟标签 horizon 或资产类别无效")
    if (adjustment_mode == "pre") != (pre_window_rebaser is not None):
        raise MinuteOperatorError("pre 分钟标签必须且只能声明逐可用时点重定基器")


def _visible_completed_rows(
    rows: Iterable[Mapping[str, object]], *, decision_time: datetime
) -> tuple[Mapping[str, object], ...]:
    result = []
    for row in rows:
        _validate_visible_completed_row(row, decision_time=decision_time)
        result.append(row)
    return tuple(sorted(result, key=lambda item: (str(item.get("instrument")), _time(item, "bar_end"))))


def _base_manifest(
    *, operator_id: str, parameters: Mapping[str, object], rows: tuple[Mapping[str, object], ...]
) -> dict[str, object]:
    return {
        "contract_version": MINUTE_OPERATOR_ARTIFACT_VERSION,
        "operator_id": operator_id,
        "parameters_hash": typed_canonical_hash(dict(parameters)),
        "row_count": len(rows),
        "content_hash": typed_canonical_hash(_canonical_rows(rows)),
        "time_lineage_fields": [
            "instrument",
            "bar_end",
            "available_time",
            "decision_time",
            "source_snapshot_hash",
        ],
    }


def build_intraday_features(
    rows: Iterable[Mapping[str, object]],
    *,
    decision_time: datetime,
    feature_ids: tuple[str, ...],
    lookback_bars: int,
    warmup_bars: int,
    gap_policy: str,
    cross_session: bool,
    adjustment_mode: str,
    asset_class: str,
    interval_minutes: int,
    availability_policy_ref: str = "pit.completed-bar.v1",
    pre_window_rebaser: PreWindowRebaser | None = None,
) -> MinuteOperatorArtifact:
    """按 instrument/session 生成一个受控的小型分钟特征集合。"""
    visible = _visible_completed_rows(rows, decision_time=decision_time)
    stream = IntradayFeatureStream(
        decision_time=decision_time,
        feature_ids=feature_ids,
        lookback_bars=lookback_bars,
        warmup_bars=warmup_bars,
        gap_policy=gap_policy,
        cross_session=cross_session,
        adjustment_mode=adjustment_mode,
        asset_class=asset_class,
        interval_minutes=interval_minutes,
        availability_policy_ref=availability_policy_ref,
        pre_window_rebaser=pre_window_rebaser,
    )
    rows_out = tuple(
        result
        for row in visible
        if (result := stream.consume(row)) is not None
    )
    if rows_out:
        validate_feature_time_facts(pd.DataFrame(rows_out))
    parameters = {
        "feature_ids": list(feature_ids), "lookback_bars": lookback_bars,
        "warmup_bars": warmup_bars, "gap_policy": gap_policy,
        "cross_session": cross_session, "adjustment_mode": adjustment_mode,
        "asset_class": asset_class, "interval_minutes": interval_minutes,
        "availability_policy_ref": availability_policy_ref,
    }
    return MinuteOperatorArtifact(
        "research.minute-features.v1", rows_out,
        _base_manifest(operator_id="research.features.intraday", parameters=parameters, rows=rows_out),
    )


def build_intraday_labels(
    rows: Iterable[Mapping[str, object]],
    *,
    entry_event: str,
    exit_event: str,
    horizon_bars: int,
    overlap: bool,
    adjustment_mode: str,
    asset_class: str,
    interval_minutes: int,
    availability_policy_ref: str = "pit.label-horizon.v1",
    pre_window_rebaser: PreWindowRebaser | None = None,
) -> MinuteOperatorArtifact:
    """生成带 entry/exit 和可用时间的未来收益标签。"""
    ordered = tuple(sorted(rows, key=lambda item: (str(item.get("instrument")), _time(item, "bar_end"))))
    stream = IntradayLabelStream(
        entry_event=entry_event,
        exit_event=exit_event,
        horizon_bars=horizon_bars,
        overlap=overlap,
        adjustment_mode=adjustment_mode,
        asset_class=asset_class,
        interval_minutes=interval_minutes,
        availability_policy_ref=availability_policy_ref,
        pre_window_rebaser=pre_window_rebaser,
    )
    rows_out = tuple(
        result
        for row in ordered
        if (result := stream.consume(row)) is not None
    )
    if rows_out:
        validate_label_time_facts(pd.DataFrame(rows_out))
    parameters = {
        "entry_event": entry_event, "exit_event": exit_event,
        "horizon_bars": horizon_bars, "overlap": overlap,
        "adjustment_mode": adjustment_mode,
        "asset_class": asset_class, "interval_minutes": interval_minutes,
        "availability_policy_ref": availability_policy_ref,
    }
    manifest = _base_manifest(operator_id="research.labels.intraday", parameters=parameters, rows=rows_out)
    manifest["label_overlap"] = {"enabled": overlap, "horizon_bars": horizon_bars}
    return MinuteOperatorArtifact("research.minute-labels.v1", rows_out, manifest)


def _event_observation_time(
    row: Mapping[str, object],
    event: str,
) -> datetime:
    if event == "bar_close":
        return _time(row, "bar_end")
    return _time(row, "bar_start")


def build_intraday_signals(
    feature_artifact: MinuteOperatorArtifact,
    *, feature_id: str,
    rule: str,
    threshold: float,
    availability_policy_ref: str = "pit.completed-feature.v1",
) -> MinuteOperatorArtifact:
    """仅根据已可见 feature 生成离散信号。"""
    if feature_artifact.artifact_type != "research.minute-features.v1":
        raise MinuteOperatorError("分钟信号只能消费 feature 工件，禁止消费 label")
    if (
        feature_id not in MINUTE_FEATURE_IDS
        or rule not in MINUTE_SIGNAL_RULES
        or type(threshold) not in {int, float}
        or not math.isfinite(threshold)
        or not availability_policy_ref.strip()
    ):
        raise MinuteOperatorError("分钟信号规则不受支持")
    rows_out = tuple(
        build_intraday_signal_row(
            row,
            feature_id=feature_id,
            rule=rule,
            threshold=threshold,
        )
        for row in feature_artifact.rows
    )
    parameters = {"feature_id": feature_id, "rule": rule, "threshold": threshold, "availability_policy_ref": availability_policy_ref}
    return MinuteOperatorArtifact(
        "research.minute-signals.v1", rows_out,
        _base_manifest(operator_id="research.signals.intraday", parameters=parameters, rows=rows_out),
    )


def build_intraday_signal_row(
    row: Mapping[str, object],
    *,
    feature_id: str,
    rule: str,
    threshold: float,
) -> Mapping[str, object]:
    """把一行已可见特征转换为信号，供分区 Runtime 逐行复用。"""

    values = row.get("features")
    if not isinstance(values, Mapping) or feature_id not in values:
        raise MinuteOperatorError("分钟信号引用了未生成的 feature")
    value = values[feature_id]
    active = False if value is None else (
        float(value) > threshold
        if rule == "greater_than"
        else float(value) < threshold
    )
    return {
        key: row[key]
        for key in (
            "instrument",
            "bar_end",
            "available_time",
            "decision_time",
            "source_snapshot_hash",
        )
    } | {"signal": 1 if active else 0}


def build_intraday_targets(
    signal_artifact: MinuteOperatorArtifact,
    *,
    asset_class: str,
    target_quantity_per_signal: int,
    leverage_limit: float,
    availability_policy_ref: str = "pit.completed-signal.v1",
) -> MinuteOperatorArtifact:
    """把离散分钟信号显式转换为数量目标，不在仿真层猜买卖方向。"""

    if signal_artifact.artifact_type != "research.minute-signals.v1":
        raise MinuteOperatorError("分钟目标只能消费 signal 工件")
    if asset_class not in MINUTE_TARGET_ASSET_CLASSES:
        raise MinuteOperatorError("分钟目标资产类别不受支持")
    if (
        type(target_quantity_per_signal) is not int
        or target_quantity_per_signal <= 0
        or type(leverage_limit) not in {int, float}
        or not math.isfinite(float(leverage_limit))
        or float(leverage_limit) < 1.0
        or not availability_policy_ref.strip()
    ):
        raise MinuteOperatorError("分钟目标数量、杠杆或可见性策略无效")
    if asset_class in {"cn_stock", "cn_etf"} and target_quantity_per_signal % 100:
        raise MinuteOperatorError("股票和 ETF 分钟目标必须按 100 股/份交易单位声明")

    result_rows = tuple(
        build_intraday_target_row(
            row,
            asset_class=asset_class,
            target_quantity_per_signal=target_quantity_per_signal,
            leverage_limit=leverage_limit,
        )
        for row in signal_artifact.rows
    )
    parameters = {
        "asset_class": asset_class,
        "target_quantity_per_signal": target_quantity_per_signal,
        "leverage_limit": float(leverage_limit),
        "availability_policy_ref": availability_policy_ref,
    }
    return MinuteOperatorArtifact(
        "research.minute-targets.v1",
        result_rows,
        _base_manifest(
            operator_id="research.targets.intraday",
            parameters=parameters,
            rows=result_rows,
        ),
    )


def build_intraday_target_row(
    row: Mapping[str, object],
    *,
    asset_class: str,
    target_quantity_per_signal: int,
    leverage_limit: float,
) -> Mapping[str, object]:
    """把一行信号转换为规范 PortfolioTarget。"""

    signal = row.get("signal")
    allowed = {-1, 0, 1} if asset_class == "cn_future" else {0, 1}
    if type(signal) is not int or signal not in allowed:
        raise MinuteOperatorError("分钟信号不能映射到当前资产数量目标")
    decision_time = _time(row, "decision_time")
    available_time = _time(row, "available_time")
    if available_time > decision_time:
        raise MinuteOperatorError("分钟目标读取了决策时尚不可见的 signal")
    instrument_id = row.get("instrument")
    source_snapshot_hash = row.get("source_snapshot_hash")
    if not isinstance(instrument_id, str) or "." not in instrument_id:
        raise MinuteOperatorError("分钟目标 instrument 必须是规范证券代码")
    if (
        not isinstance(source_snapshot_hash, str)
        or len(source_snapshot_hash) != 64
        or any(char not in "0123456789abcdef" for char in source_snapshot_hash)
    ):
        raise MinuteOperatorError("分钟目标 source snapshot hash 无效")
    venue = instrument_id.rpartition(".")[2]
    instrument = InstrumentKey(
        instrument_id=instrument_id,
        asset_class=asset_class,
        venue=venue,
        currency="CNY",
        contract_kind=(
            "future_contract"
            if asset_class == "cn_future"
            else "stock" if asset_class == "cn_stock" else "etf"
        ),
    )
    signal_hash = typed_canonical_hash(_canonical_value(dict(row)))
    target_quantity = int(signal) * target_quantity_per_signal
    target = PortfolioTarget(
        decision_time=decision_time,
        target_type="quantity",
        entries=(PortfolioTargetEntry(instrument, "quantity", target_quantity),),
        base_currency="CNY",
        cash_weight=None,
        short_allowed=asset_class == "cn_future",
        leverage_limit=float(leverage_limit),
        source_hashes=tuple(sorted({source_snapshot_hash, signal_hash})),
    )
    return {
        key: row[key]
        for key in (
            "instrument",
            "bar_end",
            "available_time",
            "decision_time",
            "source_snapshot_hash",
        )
    } | {
        "signal": signal,
        "target_quantity": target_quantity,
        "target_hash": target.target_hash,
        "portfolio_target": target.to_dict(),
    }


__all__ = [
    "MINUTE_ADJUSTMENT_MODES", "MINUTE_ASSET_CLASSES", "MINUTE_FEATURE_IDS",
    "MINUTE_GAP_POLICIES", "MINUTE_LABEL_EVENTS", "MINUTE_OPERATOR_ARTIFACT_VERSION",
    "MINUTE_TARGET_PAYLOAD_SCHEMA_ID",
    "MINUTE_SIGNAL_RULES", "IntradayFeatureStream", "IntradayLabelStream",
    "MinuteOperatorArtifact", "MinuteOperatorError", "PreWindowRebaser",
    "build_intraday_features", "build_intraday_labels", "build_intraday_signals",
    "build_intraday_signal_row", "build_intraday_target_row",
    "build_intraday_targets",
]
