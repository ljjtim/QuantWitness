"""分钟数据集与物理来源的严格语义合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping

from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    normalize_asset_class,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from .errors import CatalogParseError, CatalogReferenceError


MINUTE_DATASET_SEMANTICS_VERSION = "minute-dataset-semantics-v1"
MINUTE_SOURCE_SEMANTICS_VERSION = "minute-source-semantics-v1"
ADJUSTMENT_FACTOR_SNAPSHOT_VERSION = "adjustment-factor-snapshot-v2"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
MINUTE_REFERENCE_SCOPE_VALIDATOR_ID = "catalog.minute.reference-scope.v1"
MINUTE_CAPABILITY_MANIFEST_VALIDATOR_ID = "catalog.minute.capability-manifest.v1"


def validate_minute_reference_scope_policy(
    rules: Mapping[str, Any],
    applicability: Mapping[str, Any],
) -> None:
    expected = {
        "consumer_id",
        "minute_reference_scope_hash",
        "contract_version",
        "binding_hash",
    }
    _exact(rules, expected, "reference_scope policy")
    if rules["consumer_id"] != "catalog.minute.contracts":
        raise CatalogParseError("reference_scope consumer 必须是分钟 Catalog")
    if dict(applicability) != {"frequency": "minute", "market": "cn"}:
        raise CatalogParseError("分钟 reference_scope applicability 无效")
    body = {
        "consumer_id": rules["consumer_id"],
        "minute_reference_scope_hash": rules["minute_reference_scope_hash"],
        "contract_version": rules["contract_version"],
    }
    if rules["binding_hash"] != typed_canonical_hash(body):
        raise CatalogParseError("reference_scope binding hash 不一致")
    # PolicyContract 也用于解析历史 Lock 和 removed tombstone。这里仅校验合同
    # 自身结构；“必须绑定当前 Scope”由 Catalog 编译器只对活动合同执行。


def validate_minute_capability_manifest_policy(
    rules: Mapping[str, Any],
    applicability: Mapping[str, Any],
) -> None:
    expected = {
        "consumer_id",
        "minute_capability_manifest_hash",
        "contract_version",
        "binding_hash",
    }
    _exact(rules, expected, "minute capability policy")
    if rules["consumer_id"] != "catalog.minute.contracts":
        raise CatalogParseError("minute capability consumer 必须是分钟 Catalog")
    if dict(applicability) != {"frequency": "minute", "market": "cn"}:
        raise CatalogParseError("minute capability applicability 无效")
    body = {
        "consumer_id": rules["consumer_id"],
        "minute_capability_manifest_hash": rules["minute_capability_manifest_hash"],
        "contract_version": rules["contract_version"],
    }
    if rules["binding_hash"] != typed_canonical_hash(body):
        raise CatalogParseError("minute capability binding hash 不一致")


def register_minute_policy_validators() -> None:
    from .policy_validators import register_policy_validator

    register_policy_validator(
        MINUTE_REFERENCE_SCOPE_VALIDATOR_ID,
        validate_minute_reference_scope_policy,
    )
    register_policy_validator(
        MINUTE_CAPABILITY_MANIFEST_VALIDATOR_ID,
        validate_minute_capability_manifest_policy,
    )


def _exact(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise CatalogParseError(
            f"{name} schema 不匹配；缺失={sorted(expected-set(value))}，"
            f"未知={sorted(set(value)-expected)}"
        )


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CatalogParseError(f"{name} 必须是非空字符串")
    return value


def _hash(value: object, name: str) -> str:
    text = _text(value, name)
    if _SHA256.fullmatch(text) is None:
        raise CatalogParseError(f"{name} 必须是 sha256 小写摘要")
    return text


def _sorted_strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise CatalogParseError(f"{name} 必须是字符串序列")
    values = tuple(value)
    if (
        not values
        or any(not isinstance(item, str) or not item for item in values)
        or values != tuple(sorted(set(values)))
    ):
        raise CatalogParseError(f"{name} 必须非空、排序且唯一")
    return values


@dataclass(frozen=True)
class MinuteDatasetSemantics:
    asset_class: str
    instrument_role: str
    bar_interval: str
    timezone: str
    timestamp_storage: str
    timestamp_role: str
    session_policy_ref: str
    availability_policy_ref: str
    scope_policy_ref: str
    source_delivery: str
    quality_policy_refs: tuple[str, ...]
    semantics_version: str = MINUTE_DATASET_SEMANTICS_VERSION

    def __post_init__(self) -> None:
        try:
            object.__setattr__(
                self,
                "asset_class",
                normalize_asset_class(self.asset_class, field="minute asset_class"),
            )
        except AssetTaxonomyError as exc:
            raise CatalogParseError(str(exc)) from exc
        expected_role = "benchmark_only" if self.asset_class == "cn_index" else "tradable"
        if self.instrument_role != expected_role:
            raise CatalogParseError("分钟资产可交易角色与资产类型不一致")
        if self.bar_interval != "1m":
            raise CatalogParseError("分钟源合同只允许 1m")
        if self.timezone != "Asia/Shanghai":
            raise CatalogParseError("中国分钟数据必须声明 Asia/Shanghai")
        if self.timestamp_storage != "naive_local_wall_clock":
            raise CatalogParseError("分钟 dt 必须声明为无时区本地墙钟")
        if self.timestamp_role != "completed_bar_end":
            raise CatalogParseError("分钟 dt 必须声明为已完成 bar 右端时间")
        for name in (
            "session_policy_ref",
            "availability_policy_ref",
            "scope_policy_ref",
        ):
            _text(getattr(self, name), name)
        if self.source_delivery != "historical_batch":
            raise CatalogParseError("当前分钟物理交付只能声明 historical_batch")
        _sorted_strings(self.quality_policy_refs, "quality_policy_refs")
        if self.semantics_version != MINUTE_DATASET_SEMANTICS_VERSION:
            raise CatalogParseError("minute dataset semantics version 不受支持")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MinuteDatasetSemantics":
        expected = {
            "asset_class", "instrument_role", "bar_interval", "timezone",
            "timestamp_storage", "timestamp_role", "session_policy_ref",
            "availability_policy_ref", "scope_policy_ref", "source_delivery",
            "quality_policy_refs", "semantics_version",
        }
        _exact(value, expected, "minute_semantics")
        return cls(
            *(
                _text(value[name], name)
                for name in (
                    "asset_class", "instrument_role", "bar_interval", "timezone",
                    "timestamp_storage", "timestamp_role", "session_policy_ref",
                    "availability_policy_ref", "scope_policy_ref", "source_delivery",
                )
            ),
            _sorted_strings(value["quality_policy_refs"], "quality_policy_refs"),
            _text(value["semantics_version"], "semantics_version"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "asset_class": self.asset_class,
            "instrument_role": self.instrument_role,
            "bar_interval": self.bar_interval,
            "timezone": self.timezone,
            "timestamp_storage": self.timestamp_storage,
            "timestamp_role": self.timestamp_role,
            "session_policy_ref": self.session_policy_ref,
            "availability_policy_ref": self.availability_policy_ref,
            "scope_policy_ref": self.scope_policy_ref,
            "source_delivery": self.source_delivery,
            "quality_policy_refs": list(self.quality_policy_refs),
            "semantics_version": self.semantics_version,
        }

    @property
    def content_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


@dataclass(frozen=True)
class MinuteSourceSemantics:
    adjustment_mode: str
    adjustment_usage: str
    source_kind: str
    adjustment_anchor: str
    factor_snapshot_policy: str
    scope_binding_hash: str
    evidence_refs: tuple[str, ...]
    semantics_version: str = MINUTE_SOURCE_SEMANTICS_VERSION

    def __post_init__(self) -> None:
        if self.adjustment_mode not in {"raw", "pre", "post"}:
            raise CatalogParseError("分钟来源 adjustment_mode 不受支持")
        if self.adjustment_usage not in {"pit_allowed", "analysis_only", "unsupported"}:
            raise CatalogParseError("分钟来源 adjustment_usage 不受支持")
        if self.source_kind not in {
            "collected", "dynamic_reconstruction", "dynamic_adjusted_view"
        }:
            raise CatalogParseError("分钟 source_kind 不受支持")
        if self.adjustment_anchor not in {
            "none", "collection_time_mixed_unknown", "latest_full_history"
        }:
            raise CatalogParseError("分钟 adjustment_anchor 不受支持")
        if self.factor_snapshot_policy not in {
            "not_applicable", "required_for_pit", "missing"
        }:
            raise CatalogParseError("分钟 factor_snapshot_policy 不受支持")
        _hash(self.scope_binding_hash, "scope_binding_hash")
        _sorted_strings(self.evidence_refs, "evidence_refs")
        if self.semantics_version != MINUTE_SOURCE_SEMANTICS_VERSION:
            raise CatalogParseError("minute source semantics version 不受支持")
        if self.adjustment_usage == "pit_allowed" and (
            self.adjustment_mode != "raw"
            or self.source_kind != "collected"
            or self.adjustment_anchor != "none"
            or self.factor_snapshot_policy != "not_applicable"
        ):
            raise CatalogParseError("当前只有直接采集 raw 来源可声明 pit_allowed")
        if self.adjustment_mode in {"pre", "post"} and self.adjustment_usage == "pit_allowed":
            raise CatalogParseError("调整价格没有 PIT 因子快照时不能准入 signal")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MinuteSourceSemantics":
        expected = {
            "adjustment_mode", "adjustment_usage", "source_kind",
            "adjustment_anchor", "factor_snapshot_policy", "scope_binding_hash",
            "evidence_refs", "semantics_version",
        }
        _exact(value, expected, "minute_source_semantics")
        return cls(
            *(
                _text(value[name], name)
                for name in (
                    "adjustment_mode", "adjustment_usage", "source_kind",
                    "adjustment_anchor", "factor_snapshot_policy",
                )
            ),
            _hash(value["scope_binding_hash"], "scope_binding_hash"),
            _sorted_strings(value["evidence_refs"], "evidence_refs"),
            _text(value["semantics_version"], "semantics_version"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "adjustment_mode": self.adjustment_mode,
            "adjustment_usage": self.adjustment_usage,
            "source_kind": self.source_kind,
            "adjustment_anchor": self.adjustment_anchor,
            "factor_snapshot_policy": self.factor_snapshot_policy,
            "scope_binding_hash": self.scope_binding_hash,
            "evidence_refs": list(self.evidence_refs),
            "semantics_version": self.semantics_version,
        }

    @property
    def content_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())


@dataclass(frozen=True)
class AdjustmentFactorSegment:
    """一个标的在半开时间段内实际使用的绝对复权因子。"""

    starts_at: str
    ends_at: str
    factor: str
    available_at: str
    adjustment_ratio: str
    action_hashes: tuple[str, ...] = ()
    unrounded_ex_reference: str | None = None
    ex_reference_price: str | None = None

    def __post_init__(self) -> None:
        start = _aware(self.starts_at, "segment.starts_at")
        end = _aware(self.ends_at, "segment.ends_at")
        available = _aware(self.available_at, "segment.available_at")
        if start >= end:
            raise CatalogParseError("factor segment 必须是非空半开区间")
        if available > start:
            raise CatalogReferenceError("factor segment 在生效时尚不可见")
        _positive_decimal(self.factor, "segment.factor")
        _positive_decimal(self.adjustment_ratio, "segment.adjustment_ratio")
        if tuple(sorted(set(self.action_hashes))) != self.action_hashes:
            raise CatalogParseError("factor segment action_hashes 必须排序且唯一")
        for value in self.action_hashes:
            _hash(value, "segment.action_hash")
        references = (self.unrounded_ex_reference, self.ex_reference_price)
        if (references[0] is None) != (references[1] is None):
            raise CatalogParseError("除权参考价的未舍位值和规则值必须同时存在")
        if references[0] is not None:
            _positive_decimal(references[0], "segment.unrounded_ex_reference")
            _positive_decimal(references[1], "segment.ex_reference_price")

    def to_dict(self) -> dict[str, object]:
        return {
            "starts_at": self.starts_at,
            "ends_at": self.ends_at,
            "factor": self.factor,
            "available_at": self.available_at,
            "adjustment_ratio": self.adjustment_ratio,
            "action_hashes": list(self.action_hashes),
            "unrounded_ex_reference": self.unrounded_ex_reference,
            "ex_reference_price": self.ex_reference_price,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AdjustmentFactorSegment":
        expected = {
            "starts_at", "ends_at", "factor", "available_at",
            "adjustment_ratio", "action_hashes", "unrounded_ex_reference",
            "ex_reference_price",
        }
        _exact(value, expected, "factor segment")
        hashes = value["action_hashes"]
        if not isinstance(hashes, (list, tuple)):
            raise CatalogParseError("factor segment action_hashes 必须是序列")
        return cls(
            _text(value["starts_at"], "segment.starts_at"),
            _text(value["ends_at"], "segment.ends_at"),
            _text(value["factor"], "segment.factor"),
            _text(value["available_at"], "segment.available_at"),
            _text(value["adjustment_ratio"], "segment.adjustment_ratio"),
            tuple(str(item) for item in hashes),
            None if value["unrounded_ex_reference"] is None else _text(
                value["unrounded_ex_reference"], "segment.unrounded_ex_reference"
            ),
            None if value["ex_reference_price"] is None else _text(
                value["ex_reference_price"], "segment.ex_reference_price"
            ),
        )


@dataclass(frozen=True)
class AdjustmentDecisionAnchor:
    """Feature 或 Label 在自身可用时点选择的 pre 锚点。"""

    consumer_id: str
    consumer_kind: str
    decision_at: str
    available_at: str
    selected_at: str
    anchor_factor: str

    def __post_init__(self) -> None:
        _text(self.consumer_id, "anchor.consumer_id")
        if self.consumer_kind not in {"feature", "label"}:
            raise CatalogParseError("pre 锚点 consumer_kind 只支持 feature/label")
        decision = _aware(self.decision_at, "anchor.decision_at")
        available = _aware(self.available_at, "anchor.available_at")
        selected = _aware(self.selected_at, "anchor.selected_at")
        if available < decision:
            raise CatalogParseError("pre 锚点可用时间不能早于决定时间")
        expected_selected = decision if self.consumer_kind == "feature" else available
        if selected != expected_selected:
            raise CatalogParseError("pre 锚点选择时点与消费者类型不一致")
        _positive_decimal(self.anchor_factor, "anchor.anchor_factor")

    def to_dict(self) -> dict[str, str]:
        return {
            "consumer_id": self.consumer_id,
            "consumer_kind": self.consumer_kind,
            "decision_at": self.decision_at,
            "available_at": self.available_at,
            "selected_at": self.selected_at,
            "anchor_factor": self.anchor_factor,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AdjustmentDecisionAnchor":
        expected = {
            "consumer_id", "consumer_kind", "decision_at", "available_at",
            "selected_at", "anchor_factor",
        }
        _exact(value, expected, "adjustment anchor")
        return cls(*(_text(value[name], f"anchor.{name}") for name in (
            "consumer_id", "consumer_kind", "decision_at", "available_at",
            "selected_at", "anchor_factor",
        )))


@dataclass(frozen=True)
class AdjustmentFactorSnapshot:
    factor_snapshot_hash: str
    as_of: str
    anchor_at: str
    event_available_at: str
    applicable_start: str
    applicable_end: str
    source_revision_hash: str
    instrument_id: str
    asset_class: str
    initial_factor_date: str
    initial_factor: str
    factor_segments: tuple[AdjustmentFactorSegment, ...]
    included_action_hashes: tuple[str, ...]
    decision_anchors: tuple[AdjustmentDecisionAnchor, ...] = ()
    contract_version: str = ADJUSTMENT_FACTOR_SNAPSHOT_VERSION

    def __post_init__(self) -> None:
        _hash(self.factor_snapshot_hash, "factor_snapshot_hash")
        _hash(self.source_revision_hash, "source_revision_hash")
        as_of = _aware(self.as_of, "as_of")
        anchor = _aware(self.anchor_at, "anchor_at")
        available = _aware(self.event_available_at, "event_available_at")
        start = _aware(self.applicable_start, "applicable_start")
        end = _aware(self.applicable_end, "applicable_end")
        if available > as_of or anchor > as_of:
            raise CatalogReferenceError("未来公司行为或未来锚点不能进入 PIT 因子快照")
        if start >= end:
            raise CatalogParseError("factor snapshot applicable interval 必须非空")
        _text(self.instrument_id, "instrument_id")
        if self.asset_class not in {"cn_stock", "cn_etf"}:
            raise CatalogParseError("复权快照只支持股票或 ETF")
        try:
            date.fromisoformat(self.initial_factor_date)
        except ValueError as exc:
            raise CatalogParseError("initial_factor_date 必须是 ISO 日期") from exc
        _positive_decimal(self.initial_factor, "initial_factor")
        if not self.factor_segments:
            raise CatalogParseError("复权快照必须包含 factor segments")
        previous_end = start
        for segment in self.factor_segments:
            segment_start = _aware(segment.starts_at, "segment.starts_at")
            segment_end = _aware(segment.ends_at, "segment.ends_at")
            if segment_start != previous_end:
                raise CatalogParseError("factor segments 必须连续覆盖适用区间")
            if not start <= segment_start < segment_end <= end:
                raise CatalogParseError("factor segment 超出快照适用区间")
            previous_end = segment_end
        if previous_end != end:
            raise CatalogParseError("factor segments 未完整覆盖快照适用区间")
        if tuple(sorted(set(self.included_action_hashes))) != self.included_action_hashes:
            raise CatalogParseError("included_action_hashes 必须排序且唯一")
        observed_hashes = tuple(sorted({
            action_hash
            for segment in self.factor_segments
            for action_hash in segment.action_hashes
        }))
        if observed_hashes != self.included_action_hashes:
            raise CatalogParseError("factor segments 与纳入公司行动身份不一致")
        for anchor_item in self.decision_anchors:
            anchor_time = _aware(anchor_item.selected_at, "anchor.selected_at")
            if anchor_time > as_of:
                raise CatalogReferenceError("pre 锚点晚于快照研究时钟")
            segment = self.segment_at(anchor_time)
            if Decimal(segment.factor) != Decimal(anchor_item.anchor_factor):
                raise CatalogParseError("pre 锚点因子与对应时点 segment 不一致")
        if self.contract_version != ADJUSTMENT_FACTOR_SNAPSHOT_VERSION:
            raise CatalogParseError("AdjustmentFactorSnapshot version 不受支持")

    def segment_at(self, observed_at: datetime) -> AdjustmentFactorSegment:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise CatalogParseError("factor 查询时点必须带时区")
        for segment in self.factor_segments:
            if _aware(segment.starts_at, "segment.starts_at") <= observed_at < _aware(
                segment.ends_at, "segment.ends_at"
            ):
                return segment
        raise CatalogReferenceError("factor snapshot 未覆盖消费时点")

    def anchor_for(self, consumer_id: str) -> AdjustmentDecisionAnchor:
        matches = tuple(
            item for item in self.decision_anchors if item.consumer_id == consumer_id
        )
        if len(matches) != 1:
            raise CatalogReferenceError("pre 消费者没有唯一决定时点锚点")
        return matches[0]

    @property
    def snapshot_identity_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "factor_snapshot_hash": self.factor_snapshot_hash,
            "as_of": self.as_of,
            "anchor_at": self.anchor_at,
            "event_available_at": self.event_available_at,
            "applicable_start": self.applicable_start,
            "applicable_end": self.applicable_end,
            "source_revision_hash": self.source_revision_hash,
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "initial_factor_date": self.initial_factor_date,
            "initial_factor": self.initial_factor,
            "factor_segments": [item.to_dict() for item in self.factor_segments],
            "included_action_hashes": list(self.included_action_hashes),
            "decision_anchors": [item.to_dict() for item in self.decision_anchors],
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AdjustmentFactorSnapshot":
        expected = {
            "factor_snapshot_hash", "as_of", "anchor_at", "event_available_at",
            "applicable_start", "applicable_end", "source_revision_hash",
            "instrument_id", "asset_class", "initial_factor_date", "initial_factor",
            "factor_segments", "included_action_hashes", "decision_anchors",
            "contract_version",
        }
        _exact(value, expected, "AdjustmentFactorSnapshot")
        segment_values = value["factor_segments"]
        anchor_values = value["decision_anchors"]
        action_hashes = value["included_action_hashes"]
        if (
            not isinstance(segment_values, (list, tuple))
            or any(not isinstance(item, Mapping) for item in segment_values)
            or not isinstance(anchor_values, (list, tuple))
            or any(not isinstance(item, Mapping) for item in anchor_values)
            or not isinstance(action_hashes, (list, tuple))
        ):
            raise CatalogParseError("AdjustmentFactorSnapshot 列表字段无效")
        return cls(
            *(
                _text(value[name], name)
                for name in (
                    "factor_snapshot_hash", "as_of", "anchor_at",
                    "event_available_at", "applicable_start", "applicable_end",
                    "source_revision_hash", "instrument_id", "asset_class",
                    "initial_factor_date", "initial_factor",
                )
            ),
            tuple(AdjustmentFactorSegment.from_mapping(item) for item in segment_values),
            tuple(str(item) for item in action_hashes),
            tuple(AdjustmentDecisionAnchor.from_mapping(item) for item in anchor_values),
            _text(value["contract_version"], "contract_version"),
        )


def _positive_decimal(value: object, name: str) -> Decimal:
    text = _text(value, name)
    try:
        result = Decimal(text)
    except InvalidOperation as exc:
        raise CatalogParseError(f"{name} 必须是十进制数") from exc
    if not result.is_finite() or result <= 0:
        raise CatalogParseError(f"{name} 必须是正有限十进制数")
    return result


def _aware(value: object, name: str) -> datetime:
    text = _text(value, name)
    try:
        result = datetime.fromisoformat(text)
    except ValueError as exc:
        raise CatalogParseError(f"{name} 必须是带时区 ISO 时间") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise CatalogParseError(f"{name} 必须带时区")
    if text != result.isoformat(timespec="seconds"):
        raise CatalogParseError(f"{name} 必须使用规范秒精度 ISO 时间")
    return result


__all__ = [
    "ADJUSTMENT_FACTOR_SNAPSHOT_VERSION",
    "MINUTE_DATASET_SEMANTICS_VERSION",
    "MINUTE_SOURCE_SEMANTICS_VERSION",
    "AdjustmentDecisionAnchor",
    "AdjustmentFactorSegment",
    "AdjustmentFactorSnapshot",
    "MinuteDatasetSemantics",
    "MinuteSourceSemantics",
]
