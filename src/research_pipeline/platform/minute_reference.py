"""平台分钟数据能力与资产覆盖合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from importlib.resources import files
import json
from pathlib import Path
import re
from typing import Mapping

from .asset_taxonomy import (
    AssetTaxonomyError,
    normalize_asset_class,
    require_canonical_asset_class,
    require_instrument_type,
)
from .canonical import typed_canonical_hash
from .errors import MainlineError


MINUTE_CAPABILITY_MANIFEST_VERSION = "minute-capability-manifest-v1"
MINUTE_CAPABILITY_BINDING_VERSION = "minute-capability-binding-v1"
CURRENT_MINUTE_CAPABILITY_MANIFEST_REVISION = 1
MINUTE_INTERVALS = ("1m", "5m", "15m", "30m", "60m", "120m")
MINUTE_CAPABILITY_REQUIRED_CONSUMERS = (
    "catalog.minute.contracts",
    "domain.minute.market_rule_snapshots",
    "domain.minute.session_calendar",
    "reference.minute.acceptance",
)
CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH = "73a37c34c20e6cc25fd249098e396c6d676b9ee79e496755c61010d42f0cf033"

_DATASET_ASSET_PAIRS = {
    ("cn_equity.minute_bar", "cn_stock"),
    ("cn_fund.minute_bar", "cn_etf"),
    ("cn_index.minute_bar", "cn_index"),
    ("cn_futures.minute_bar", "cn_future"),
}
_ROLES = {"tradable", "benchmark", "signal_only"}
_STABLE_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ACTUAL_FUTURES_ID = re.compile(r"^[A-Z]{1,3}\d{3,4}\.[A-Z]{4}$")
_FORBIDDEN_SELECTION_FIELDS = {
    "feature", "features", "label", "labels", "p_value", "pvalue", "return",
    "returns", "significance", "strategy", "strategy_output", "strategy_return",
}


class MinuteCapabilityManifestError(MainlineError):
    """分钟能力覆盖不完整、读取了策略结果或身份不一致。"""

    error_code = "minute_capability_manifest_invalid"


def _normalize_asset_class(value: object) -> str:
    try:
        return normalize_asset_class(value)
    except AssetTaxonomyError as exc:
        raise MinuteCapabilityManifestError(str(exc)) from exc


@dataclass(frozen=True)
class MinuteInventoryObservation:
    dataset_id: str
    instrument_id: str
    start_at: str
    end_at: str
    trading_date_start: str
    trading_date_end: str
    observed_row_count: int
    bars_per_trading_date: tuple[tuple[str, int], ...]
    observed_features: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_id(self.dataset_id, "dataset_id")
        _require_text(self.instrument_id, "instrument_id")
        start = _aware_datetime(self.start_at, "start_at")
        end = _aware_datetime(self.end_at, "end_at")
        if (
            start.utcoffset().total_seconds() != 8 * 3600
            or end.utcoffset().total_seconds() != 8 * 3600
        ):
            raise MinuteCapabilityManifestError("中国市场分钟能力窗口必须使用 +08:00")
        if start >= end:
            raise MinuteCapabilityManifestError("分钟观察窗口必须是非空半开区间")
        first_date = _date(self.trading_date_start, "trading_date_start")
        last_date = _date(self.trading_date_end, "trading_date_end")
        if first_date > last_date:
            raise MinuteCapabilityManifestError("trading_date 范围倒置")
        if type(self.observed_row_count) is not int or self.observed_row_count <= 0:
            raise MinuteCapabilityManifestError("observed_row_count 必须是正整数")
        if not self.bars_per_trading_date:
            raise MinuteCapabilityManifestError("bars_per_trading_date 不能为空")
        dates = tuple(item[0] for item in self.bars_per_trading_date)
        if dates != tuple(sorted(set(dates))):
            raise MinuteCapabilityManifestError("bars_per_trading_date 必须按交易日排序且唯一")
        for trading_date, count in self.bars_per_trading_date:
            _date(trading_date, "bars_per_trading_date.trading_date")
            if type(count) is not int or count <= 0:
                raise MinuteCapabilityManifestError("bars_per_trading_date.count 必须是正整数")
        if sum(item[1] for item in self.bars_per_trading_date) != self.observed_row_count:
            raise MinuteCapabilityManifestError("逐交易日 bar 数与观察总行数不一致")
        if dates[0] != self.trading_date_start or dates[-1] != self.trading_date_end:
            raise MinuteCapabilityManifestError("逐交易日 bar 范围与 trading_date 起止不一致")
        _require_sorted_ids(self.observed_features, "observed_features")

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "instrument_id": self.instrument_id,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "trading_date_start": self.trading_date_start,
            "trading_date_end": self.trading_date_end,
            "observed_row_count": self.observed_row_count,
            "bars_per_trading_date": [
                {"trading_date": day, "count": count}
                for day, count in self.bars_per_trading_date
            ],
            "observed_features": list(self.observed_features),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "MinuteInventoryObservation":
        payload = _strict(
            value,
            {
                "dataset_id", "instrument_id", "start_at", "end_at",
                "trading_date_start", "trading_date_end", "observed_row_count",
                "bars_per_trading_date", "observed_features",
            },
            "MinuteInventoryObservation",
        )
        counts = []
        for item in _list(payload["bars_per_trading_date"], "bars_per_trading_date"):
            count_payload = _strict(
                item, {"trading_date", "count"}, "bars_per_trading_date item"
            )
            counts.append(
                (
                    _string(count_payload["trading_date"], "trading_date"),
                    _integer(count_payload["count"], "count"),
                )
            )
        return cls(
            _string(payload["dataset_id"], "dataset_id"),
            _string(payload["instrument_id"], "instrument_id"),
            _string(payload["start_at"], "start_at"),
            _string(payload["end_at"], "end_at"),
            _string(payload["trading_date_start"], "trading_date_start"),
            _string(payload["trading_date_end"], "trading_date_end"),
            _integer(payload["observed_row_count"], "observed_row_count"),
            tuple(counts),
            _strings(payload["observed_features"], "observed_features"),
        )


@dataclass(frozen=True)
class MinuteCapabilityInstrument:
    instrument_id: str
    asset_class: str
    asset_subtype: str
    dataset_id: str
    role: str
    adjustment_mode: str
    observation: MinuteInventoryObservation
    source_adjustment_mode: str
    source_usage: str
    source_semantics_version: str

    def __post_init__(self) -> None:
        _require_text(self.instrument_id, "instrument_id")
        try:
            require_canonical_asset_class(self.asset_class)
            require_instrument_type(self.asset_class, self.asset_subtype)
        except AssetTaxonomyError as exc:
            raise MinuteCapabilityManifestError(str(exc)) from exc
        _require_id(self.dataset_id, "dataset_id")
        if self.role not in _ROLES:
            raise MinuteCapabilityManifestError("分钟能力 instrument role 不受支持")
        if self.adjustment_mode != "raw":
            raise MinuteCapabilityManifestError("分钟 PIT 能力当前只允许 raw bar")
        if self.source_semantics_version != "minute-source-semantics-v1":
            raise MinuteCapabilityManifestError("分钟来源复权语义版本不受支持")
        if self.source_adjustment_mode not in {"raw", "pre"}:
            raise MinuteCapabilityManifestError("分钟来源复权模式不受支持")
        if self.source_usage not in {"pit_eligible", "analysis_only"}:
            raise MinuteCapabilityManifestError("分钟来源用途不受支持")
        if self.source_adjustment_mode == "pre" and self.source_usage != "analysis_only":
            raise MinuteCapabilityManifestError("无 PIT 因子快照的前复权来源只能 analysis_only")
        if self.asset_class in {"cn_index", "cn_future"} and (
            self.source_adjustment_mode != "raw" or self.source_usage != "pit_eligible"
        ):
            raise MinuteCapabilityManifestError("指数和期货能力来源必须是可准入 raw")
        if (
            self.observation.dataset_id != self.dataset_id
            or self.observation.instrument_id != self.instrument_id
        ):
            raise MinuteCapabilityManifestError("分钟观察与 instrument 身份不一致")
        if (self.dataset_id, self.asset_class) not in _DATASET_ASSET_PAIRS:
            raise MinuteCapabilityManifestError("分钟 dataset 与资产类型不匹配")
        if self.asset_class == "cn_index" and self.role == "tradable":
            raise MinuteCapabilityManifestError("指数不能声明为可交易资产")
        if self.asset_class != "cn_index" and self.role != "tradable":
            raise MinuteCapabilityManifestError("股票、ETF 和实际期货能力标的必须可交易")
        if self.asset_class == "cn_future":
            contract_code = self.instrument_id.split(".", 1)[0]
            if (
                not _ACTUAL_FUTURES_ID.fullmatch(self.instrument_id)
                or contract_code.endswith(("8888", "9999"))
            ):
                raise MinuteCapabilityManifestError("期货能力范围必须使用实际到期合约")

    @property
    def identity_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "asset_class": self.asset_class,
            "asset_subtype": self.asset_subtype,
            "dataset_id": self.dataset_id,
            "role": self.role,
            "adjustment_mode": self.adjustment_mode,
            "observation": self.observation.to_dict(),
            "source_adjustment_mode": self.source_adjustment_mode,
            "source_usage": self.source_usage,
            "source_semantics_version": self.source_semantics_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "MinuteCapabilityInstrument":
        payload = _strict(
            value,
            {
                "instrument_id", "asset_class", "asset_subtype", "dataset_id",
                "role", "adjustment_mode", "observation", "source_adjustment_mode",
                "source_usage", "source_semantics_version",
            },
            "MinuteCapabilityInstrument",
        )
        return cls(
            _string(payload["instrument_id"], "instrument_id"),
            _normalize_asset_class(payload["asset_class"]),
            _string(payload["asset_subtype"], "asset_subtype"),
            _string(payload["dataset_id"], "dataset_id"),
            _string(payload["role"], "role"),
            _string(payload["adjustment_mode"], "adjustment_mode"),
            MinuteInventoryObservation.from_dict(
                _mapping(payload["observation"], "observation")
            ),
            _string(payload["source_adjustment_mode"], "source_adjustment_mode"),
            _string(payload["source_usage"], "source_usage"),
            _string(payload["source_semantics_version"], "source_semantics_version"),
        )


@dataclass(frozen=True)
class MinuteAssetCoverage:
    coverage_id: str
    instrument: MinuteCapabilityInstrument
    required_intervals: tuple[str, ...]
    session_features: tuple[str, ...]
    adjustment_requirement: str
    required_rule_ids: tuple[str, ...]
    quality_expectations: tuple[str, ...]
    claim_ceiling: str
    unsupported_boundaries: tuple[str, ...]
    selection_reason: str

    def __post_init__(self) -> None:
        _require_id(self.coverage_id, "coverage_id")
        if self.required_intervals != MINUTE_INTERVALS:
            raise MinuteCapabilityManifestError("每项分钟能力必须覆盖冻结的六个分钟周期")
        _require_sorted_ids(self.session_features, "session_features")
        if self.adjustment_requirement != "pit_raw_only_v1":
            raise MinuteCapabilityManifestError("分钟能力复权要求必须是 pit_raw_only_v1")
        _require_sorted_ids(self.required_rule_ids, "required_rule_ids")
        _require_sorted_ids(self.quality_expectations, "quality_expectations")
        if self.claim_ceiling != "research_observation":
            raise MinuteCapabilityManifestError("分钟能力 claim ceiling 只能是 research_observation")
        _require_sorted_ids(self.unsupported_boundaries, "unsupported_boundaries")
        _require_text(self.selection_reason, "selection_reason")

    def to_dict(self) -> dict[str, object]:
        return {
            "coverage_id": self.coverage_id,
            "instrument": self.instrument.to_dict(),
            "required_intervals": list(self.required_intervals),
            "session_features": list(self.session_features),
            "adjustment_requirement": self.adjustment_requirement,
            "required_rule_ids": list(self.required_rule_ids),
            "quality_expectations": list(self.quality_expectations),
            "claim_ceiling": self.claim_ceiling,
            "unsupported_boundaries": list(self.unsupported_boundaries),
            "selection_reason": self.selection_reason,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "MinuteAssetCoverage":
        payload = _strict(
            value,
            {
                "coverage_id", "instrument", "required_intervals", "session_features",
                "adjustment_requirement", "required_rule_ids", "quality_expectations",
                "claim_ceiling", "unsupported_boundaries", "selection_reason",
            },
            "MinuteAssetCoverage",
        )
        return cls(
            _string(payload["coverage_id"], "coverage_id"),
            MinuteCapabilityInstrument.from_dict(
                _mapping(payload["instrument"], "instrument")
            ),
            _strings(payload["required_intervals"], "required_intervals"),
            _strings(payload["session_features"], "session_features"),
            _string(payload["adjustment_requirement"], "adjustment_requirement"),
            _strings(payload["required_rule_ids"], "required_rule_ids"),
            _strings(payload["quality_expectations"], "quality_expectations"),
            _string(payload["claim_ceiling"], "claim_ceiling"),
            _strings(payload["unsupported_boundaries"], "unsupported_boundaries"),
            _string(payload["selection_reason"], "selection_reason"),
        )


@dataclass(frozen=True)
class MinuteCapabilityManifest:
    manifest_id: str
    revision: int
    previous_manifest_hash: str | None
    inventory_evidence_hash: str
    required_consumers: tuple[str, ...]
    required_asset_classes: tuple[str, ...]
    coverages: tuple[MinuteAssetCoverage, ...]
    manifest_hash: str
    contract_version: str = MINUTE_CAPABILITY_MANIFEST_VERSION

    def __post_init__(self) -> None:
        _require_id(self.manifest_id, "manifest_id")
        if type(self.revision) is not int or self.revision <= 0:
            raise MinuteCapabilityManifestError("manifest revision 必须是正整数")
        if self.revision == 1 and self.previous_manifest_hash is not None:
            raise MinuteCapabilityManifestError("首个 manifest revision 不得声明前序摘要")
        if self.revision > 1:
            _require_hash(self.previous_manifest_hash, "previous_manifest_hash")
        _require_hash(self.inventory_evidence_hash, "inventory_evidence_hash")
        if self.required_consumers != MINUTE_CAPABILITY_REQUIRED_CONSUMERS:
            raise MinuteCapabilityManifestError("分钟能力下游消费者集合不完整")
        normalized_required = tuple(
            sorted(_normalize_asset_class(item) for item in self.required_asset_classes)
        )
        if not normalized_required or normalized_required != self.required_asset_classes:
            raise MinuteCapabilityManifestError("required_asset_classes 必须排序且唯一")
        coverage_ids = tuple(item.coverage_id for item in self.coverages)
        if not coverage_ids or coverage_ids != tuple(sorted(set(coverage_ids))):
            raise MinuteCapabilityManifestError("分钟能力覆盖必须按 coverage_id 排序且唯一")
        instrument_ids = tuple(item.instrument.instrument_id for item in self.coverages)
        if len(set(instrument_ids)) != len(instrument_ids):
            raise MinuteCapabilityManifestError("分钟能力 instrument 必须唯一")
        covered_assets = {item.instrument.asset_class for item in self.coverages}
        if covered_assets != set(self.required_asset_classes):
            raise MinuteCapabilityManifestError("分钟能力缺少 required_asset_classes 覆盖")
        if self.contract_version != MINUTE_CAPABILITY_MANIFEST_VERSION:
            raise MinuteCapabilityManifestError("分钟能力 manifest version 不受支持")
        if self.manifest_hash != typed_canonical_hash(self.payload()):
            raise MinuteCapabilityManifestError("分钟能力 manifest hash 与内容不一致")

    def payload(self) -> dict[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "revision": self.revision,
            "previous_manifest_hash": self.previous_manifest_hash,
            "inventory_evidence_hash": self.inventory_evidence_hash,
            "required_consumers": list(self.required_consumers),
            "required_asset_classes": list(self.required_asset_classes),
            "coverages": [item.to_dict() for item in self.coverages],
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "manifest_hash": self.manifest_hash}

    @property
    def instruments(self) -> tuple[MinuteCapabilityInstrument, ...]:
        return tuple(item.instrument for item in self.coverages)

    def coverage_for_instrument(self, instrument_id: str) -> MinuteAssetCoverage:
        matches = tuple(
            item for item in self.coverages
            if item.instrument.instrument_id == instrument_id
        )
        if len(matches) != 1:
            raise MinuteCapabilityManifestError("instrument 没有唯一分钟能力覆盖")
        return matches[0]

    def downstream_identity(self, consumer_id: str) -> dict[str, object]:
        _require_id(consumer_id, "consumer_id")
        if consumer_id not in self.required_consumers:
            raise MinuteCapabilityManifestError("consumer_id 不在分钟能力消费者中")
        payload = {
            "consumer_id": consumer_id,
            "minute_capability_manifest_hash": self.manifest_hash,
            "contract_version": MINUTE_CAPABILITY_BINDING_VERSION,
        }
        return {**payload, "binding_hash": typed_canonical_hash(payload)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "MinuteCapabilityManifest":
        payload = _strict(
            value,
            {
                "manifest_id", "revision", "previous_manifest_hash",
                "inventory_evidence_hash", "required_consumers",
                "required_asset_classes", "coverages", "manifest_hash",
                "contract_version",
            },
            "MinuteCapabilityManifest",
        )
        previous = payload["previous_manifest_hash"]
        if previous is not None and not isinstance(previous, str):
            raise MinuteCapabilityManifestError("previous_manifest_hash 类型无效")
        return cls(
            _string(payload["manifest_id"], "manifest_id"),
            _integer(payload["revision"], "revision"),
            previous,
            _string(payload["inventory_evidence_hash"], "inventory_evidence_hash"),
            _strings(payload["required_consumers"], "required_consumers"),
            _strings(payload["required_asset_classes"], "required_asset_classes"),
            tuple(
                MinuteAssetCoverage.from_dict(_mapping(item, "coverage"))
                for item in _list(payload["coverages"], "coverages")
            ),
            _string(payload["manifest_hash"], "manifest_hash"),
            _string(payload["contract_version"], "contract_version"),
        )


def load_minute_capability_manifest(
    path: str | Path | None = None,
    *,
    expected_manifest_hash: str | None = None,
) -> MinuteCapabilityManifest:
    """读取平台分钟能力 manifest，并核对同目录只读 inventory。"""

    is_default = path is None
    if is_default:
        resource = files("research_pipeline.platform").joinpath(
            "minute_capabilities/minute_capability_manifest.v1.json"
        )
        raw = resource.read_text(encoding="utf-8")
        inventory_raw = files("research_pipeline.platform").joinpath(
            "minute_capabilities/minute_inventory.v1.json"
        ).read_bytes()
    else:
        manifest_path = Path(path)
        try:
            raw = manifest_path.read_text(encoding="utf-8")
            inventory_raw = manifest_path.with_name("minute_inventory.v1.json").read_bytes()
        except (OSError, UnicodeError) as exc:
            raise MinuteCapabilityManifestError("分钟能力 manifest 或 inventory 无法读取") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MinuteCapabilityManifestError("分钟能力 manifest 不是有效 JSON") from exc
    manifest = MinuteCapabilityManifest.from_dict(
        _mapping(payload, "MinuteCapabilityManifest")
    )
    anchor = CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH if is_default else expected_manifest_hash
    if anchor is None or manifest.manifest_hash != _require_hash(anchor, "expected_manifest_hash"):
        raise MinuteCapabilityManifestError("分钟能力 manifest 与预期发布锚点不一致")
    if manifest.revision != CURRENT_MINUTE_CAPABILITY_MANIFEST_REVISION:
        raise MinuteCapabilityManifestError("分钟能力 manifest revision 不受支持")
    if hashlib.sha256(inventory_raw).hexdigest() != manifest.inventory_evidence_hash:
        raise MinuteCapabilityManifestError("分钟能力只读 inventory 摘要不一致")
    return manifest


def require_current_minute_capability_binding(
    *,
    consumer_id: str,
    manifest_hash: str,
    contract_version: str,
    binding_hash: str,
) -> None:
    """核对消费者 binding 确实指向当前平台分钟能力 manifest。"""

    if consumer_id not in MINUTE_CAPABILITY_REQUIRED_CONSUMERS:
        raise MinuteCapabilityManifestError("分钟能力 consumer identity 不受支持")
    if manifest_hash != CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH:
        raise MinuteCapabilityManifestError("分钟消费者必须绑定当前能力 manifest")
    if contract_version != MINUTE_CAPABILITY_BINDING_VERSION:
        raise MinuteCapabilityManifestError("分钟能力 binding version 不受支持")
    expected = typed_canonical_hash(
        {
            "consumer_id": consumer_id,
            "minute_capability_manifest_hash": manifest_hash,
            "contract_version": contract_version,
        }
    )
    if binding_hash != expected:
        raise MinuteCapabilityManifestError("分钟能力 consumer binding hash 不一致")


def require_current_minute_catalog_capability_binding(**kwargs: str) -> None:
    if kwargs.get("consumer_id") != "catalog.minute.contracts":
        raise MinuteCapabilityManifestError("分钟 Catalog consumer identity 不一致")
    require_current_minute_capability_binding(**kwargs)


def _strict(value: object, expected: set[str], field: str) -> Mapping[str, object]:
    payload = _mapping(value, field)
    unknown = set(payload) - expected
    if unknown & _FORBIDDEN_SELECTION_FIELDS:
        raise MinuteCapabilityManifestError("分钟能力选择禁止读取策略收益、标签或显著性输出")
    if set(payload) != expected:
        raise MinuteCapabilityManifestError(
            f"{field} schema 不匹配；缺失={sorted(expected-set(payload))}，"
            f"未知={sorted(unknown)}"
        )
    return payload


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise MinuteCapabilityManifestError(f"{field} 必须是字符串键映射")
    return value


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list) or not value:
        raise MinuteCapabilityManifestError(f"{field} 必须是非空列表")
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    values = _list(value, field)
    if any(not isinstance(item, str) for item in values):
        raise MinuteCapabilityManifestError(f"{field} 必须是字符串列表")
    return tuple(values)


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise MinuteCapabilityManifestError(f"{field} 必须是整数")
    return value


def _require_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MinuteCapabilityManifestError(f"{field} 必须是非空字符串")
    return value


def _string(value: object, field: str) -> str:
    return _require_text(value, field)


def _require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _STABLE_ID.fullmatch(value):
        raise MinuteCapabilityManifestError(f"{field} 必须是稳定 ID")
    return value


def _require_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise MinuteCapabilityManifestError(f"{field} 必须是 sha256 小写摘要")
    return value


def _require_sorted_ids(values: tuple[str, ...], field: str) -> None:
    if not values or values != tuple(sorted(set(values))):
        raise MinuteCapabilityManifestError(f"{field} 必须非空、排序且唯一")
    for value in values:
        _require_id(value, field)


def _aware_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise MinuteCapabilityManifestError(f"{field} 必须是带时区 ISO 时间")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise MinuteCapabilityManifestError(f"{field} 必须是带时区 ISO 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MinuteCapabilityManifestError(f"{field} 必须带时区")
    if value != parsed.isoformat(timespec="seconds"):
        raise MinuteCapabilityManifestError(f"{field} 必须使用规范秒精度 ISO 时间")
    return parsed


def _date(value: object, field: str) -> date:
    if not isinstance(value, str):
        raise MinuteCapabilityManifestError(f"{field} 必须是 ISO 日期")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise MinuteCapabilityManifestError(f"{field} 必须是 ISO 日期") from exc


__all__ = [
    "CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH",
    "CURRENT_MINUTE_CAPABILITY_MANIFEST_REVISION",
    "MINUTE_CAPABILITY_BINDING_VERSION",
    "MINUTE_CAPABILITY_MANIFEST_VERSION",
    "MINUTE_CAPABILITY_REQUIRED_CONSUMERS",
    "MINUTE_INTERVALS",
    "MinuteAssetCoverage",
    "MinuteCapabilityInstrument",
    "MinuteCapabilityManifest",
    "MinuteCapabilityManifestError",
    "MinuteInventoryObservation",
    "load_minute_capability_manifest",
    "require_current_minute_capability_binding",
    "require_current_minute_catalog_capability_binding",
]
