"""股票、ETF 与期货共用的正式成交 Bar TCA 归因。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP, localcontext
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Mapping, Sequence
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform import (
    canonical_json,
    typed_canonical_bytes,
    typed_canonical_hash,
)
from research_pipeline.platform.asset_taxonomy import (
    AssetTaxonomyError,
    require_frequency,
    require_simulation_entry,
)

from .orders import SimulationContractError


BAR_TCA_POLICY_VERSION = "research-bar-tca-policy-v2"
BAR_TCA_RESULT_VERSION = "research-bar-tca-attribution-result-v2"
BAR_TCA_ARTIFACT_VERSION = "research-bar-tca-attribution-artifact-v2"
BAR_TCA_ORACLE_INPUT_VERSION = "research-bar-tca-attribution-oracle-input-v2"
BAR_TCA_STREAM_RESULT_VERSION = "research-bar-tca-attribution-result-v3"
BAR_TCA_STREAM_ARTIFACT_VERSION = "research-bar-tca-attribution-artifact-v3"
BAR_TCA_TABLE_REFERENCE_VERSION = "research-bar-tca-table-references-v1"
_IMPACT_MODELS = {"fixed_bps_v1", "sqrt_participation_v1"}
_CLAIM_CEILINGS = {"analysis_only", "sealed"}
_ORDER_STATUSES = {"filled", "partially_filled", "rejected"}


def _require_hash(value: str, field: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise SimulationContractError(f"{field} 必须是小写 sha256")


def _require_bar_tca_asset(asset_class: str, *, frequency: str | None = None) -> None:
    try:
        require_simulation_entry(asset_class, "bar_tca")
        if frequency is not None:
            require_frequency(asset_class, frequency)
    except AssetTaxonomyError as exc:
        raise SimulationContractError(str(exc)) from exc


@dataclass(frozen=True)
class BarTcaPolicy:
    """模型参数只用于可计算的流动性归因，不得生成正式成交或正式费用。"""

    asset_class: str
    bar_frequency: str
    impact_model: str
    spread_slippage_bps: int
    fixed_impact_bps: int
    sqrt_impact_coefficient_bps: int
    participation_cap_ppm: int
    delay_benchmark: str
    rounding_rule: str
    contract_multiplier: int
    price_scale: int
    available_at: datetime
    rule_snapshot_hash: str
    claim_ceiling: str
    contract_version: str = BAR_TCA_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != BAR_TCA_POLICY_VERSION:
            raise SimulationContractError("Bar TCA policy 版本不受支持")
        _require_bar_tca_asset(self.asset_class, frequency=self.bar_frequency)
        if self.bar_frequency not in {"daily", "minute"}:
            raise SimulationContractError("Bar TCA 频率无效")
        if self.impact_model not in _IMPACT_MODELS:
            raise SimulationContractError("Bar TCA 冲击模型不受支持")
        integer_fields = (
            "spread_slippage_bps", "fixed_impact_bps",
            "sqrt_impact_coefficient_bps",
        )
        if any(type(getattr(self, field)) is not int or getattr(self, field) < 0 for field in integer_fields):
            raise SimulationContractError("Bar TCA 参数必须是非负整数")
        if type(self.participation_cap_ppm) is not int or not 1 <= self.participation_cap_ppm <= 1_000_000:
            raise SimulationContractError("Bar TCA participation cap 必须位于 (0,100%]")
        if type(self.contract_multiplier) is not int or self.contract_multiplier <= 0:
            raise SimulationContractError("Bar TCA 合约乘数必须是正整数")
        if type(self.price_scale) is not int or self.price_scale < 0:
            raise SimulationContractError("Bar TCA 价格精度无效")
        if self.delay_benchmark != "decision_price_v1":
            raise SimulationContractError("Bar TCA benchmark 未注册")
        if self.rounding_rule != "price_half_up_cost_ceil_v1":
            raise SimulationContractError("Bar TCA 舍入规则未注册")
        if self.claim_ceiling not in _CLAIM_CEILINGS:
            raise SimulationContractError("Bar TCA claim ceiling 无效")
        if self.bar_frequency == "minute" and self.asset_class in {"cn_stock", "cn_etf"} and self.claim_ceiling != "analysis_only":
            raise SimulationContractError("股票/ETF 分钟 TCA 缺 PIT/复权证明时只能 analysis_only")
        if self.impact_model == "fixed_bps_v1" and self.sqrt_impact_coefficient_bps:
            raise SimulationContractError("固定基点模型不得混入平方根冲击参数")
        if self.impact_model == "sqrt_participation_v1" and self.fixed_impact_bps:
            raise SimulationContractError("平方根模型不得混入固定冲击参数")
        require_aware_datetime(self.available_at, "available_at")
        _require_hash(self.rule_snapshot_hash, "rule_snapshot_hash")

    @property
    def implementation_digest(self) -> str:
        return typed_canonical_hash({
            "formula": "formal-fill-attribution-with-optional-visible-liquidity-v2",
            "impact_model": self.impact_model,
            "rounding": self.rounding_rule,
            "contract_version": self.contract_version,
        })

    @property
    def policy_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            **{
                field: getattr(self, field)
                for field in (
                    "asset_class", "bar_frequency", "impact_model",
                    "spread_slippage_bps", "fixed_impact_bps",
                    "sqrt_impact_coefficient_bps", "participation_cap_ppm",
                    "delay_benchmark", "rounding_rule", "contract_multiplier", "price_scale",
                    "rule_snapshot_hash", "claim_ceiling", "contract_version",
                )
            },
            "available_at": self.available_at.isoformat(),
            "implementation_digest": self.implementation_digest,
        }


@dataclass(frozen=True)
class BarTcaOrder:
    order_id: str
    instrument_id: str
    asset_class: str
    side: str
    requested_quantity: int
    filled_quantity: int
    status: str
    terminal_reason: str | None
    decision_time: datetime
    submitted_at: datetime
    decision_price_units: int
    decision_price_available_at: datetime
    decision_price_source_hash: str
    source_order_hash: str

    def __post_init__(self) -> None:
        if not self.order_id.strip() or not self.instrument_id.strip():
            raise SimulationContractError("Bar TCA 正式订单身份不能为空")
        _require_bar_tca_asset(self.asset_class)
        if self.side not in {"buy", "sell"}:
            raise SimulationContractError("Bar TCA 正式订单方向无效")
        if type(self.requested_quantity) is not int or self.requested_quantity <= 0:
            raise SimulationContractError("Bar TCA 正式订单数量必须为正整数")
        if type(self.filled_quantity) is not int or not 0 <= self.filled_quantity <= self.requested_quantity:
            raise SimulationContractError("Bar TCA 正式订单成交数量无效")
        if self.status not in _ORDER_STATUSES:
            raise SimulationContractError("Bar TCA 正式订单状态无效")
        expected = (
            "filled" if self.filled_quantity == self.requested_quantity
            else "rejected" if self.filled_quantity == 0
            else "partially_filled"
        )
        if self.status != expected:
            raise SimulationContractError("Bar TCA 正式订单状态与成交数量不一致")
        if self.status == "filled" and self.terminal_reason is not None:
            raise SimulationContractError("已成交正式订单不得有终止原因")
        if self.status != "filled" and not self.terminal_reason:
            raise SimulationContractError("未完全成交正式订单必须有终止原因")
        for field in ("decision_time", "submitted_at", "decision_price_available_at"):
            require_aware_datetime(getattr(self, field), field)
        if self.decision_price_available_at > self.decision_time or self.decision_time > self.submitted_at:
            raise SimulationContractError("Bar TCA 正式订单使用了决策后基准或时间顺序无效")
        if type(self.decision_price_units) is not int or self.decision_price_units <= 0:
            raise SimulationContractError("Bar TCA 决策基准必须是正整数定点值")
        _require_hash(self.decision_price_source_hash, "decision_price_source_hash")
        _require_hash(self.source_order_hash, "source_order_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            **self.__dict__,
            "decision_time": self.decision_time.isoformat(),
            "submitted_at": self.submitted_at.isoformat(),
            "decision_price_available_at": self.decision_price_available_at.isoformat(),
        }


@dataclass(frozen=True)
class BarTcaFormalFill:
    source_fill_id: str
    order_id: str
    instrument_id: str
    asset_class: str
    side: str
    fill_time: datetime
    quantity: int
    execution_price_units: int
    formal_fee_units: int
    source_fill_hash: str
    source_ledger_hash: str
    arrival_price_units: int | None = None
    arrival_price_available_at: datetime | None = None
    visible_capacity: int | None = None
    capacity_available_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.source_fill_id.strip() or not self.order_id.strip() or not self.instrument_id.strip():
            raise SimulationContractError("Bar TCA 正式成交身份不能为空")
        _require_bar_tca_asset(self.asset_class)
        if self.side not in {"buy", "sell"}:
            raise SimulationContractError("Bar TCA 正式成交方向无效")
        require_aware_datetime(self.fill_time, "fill_time")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise SimulationContractError("Bar TCA 正式成交数量必须为正整数")
        if type(self.execution_price_units) is not int or self.execution_price_units <= 0:
            raise SimulationContractError("Bar TCA 正式成交价必须为正整数定点值")
        if type(self.formal_fee_units) is not int or self.formal_fee_units < 0:
            raise SimulationContractError("Bar TCA 正式费用必须是非负整数")
        for field in ("source_fill_hash", "source_ledger_hash"):
            _require_hash(getattr(self, field), field)
        if (self.arrival_price_units is None) != (self.arrival_price_available_at is None):
            raise SimulationContractError("Bar TCA arrival 基准值和可见时间必须同时提供")
        if self.arrival_price_units is not None:
            if type(self.arrival_price_units) is not int or self.arrival_price_units <= 0:
                raise SimulationContractError("Bar TCA arrival 基准必须为正整数定点值")
            require_aware_datetime(self.arrival_price_available_at, "arrival_price_available_at")
            if self.arrival_price_available_at > self.fill_time:
                raise SimulationContractError("Bar TCA arrival 基准在成交时尚不可见")
        if (self.visible_capacity is None) != (self.capacity_available_at is None):
            raise SimulationContractError("Bar TCA 可见容量和值的可见时间必须同时提供")
        if self.visible_capacity is not None:
            if type(self.visible_capacity) is not int or self.visible_capacity <= 0:
                raise SimulationContractError("Bar TCA 可见容量必须是正整数")
            require_aware_datetime(self.capacity_available_at, "capacity_available_at")
            if self.capacity_available_at > self.fill_time:
                raise SimulationContractError("Bar TCA 容量在成交时尚不可见")

    def to_dict(self) -> dict[str, object]:
        return {
            **self.__dict__,
            "fill_time": self.fill_time.isoformat(),
            "arrival_price_available_at": (
                None if self.arrival_price_available_at is None
                else self.arrival_price_available_at.isoformat()
            ),
            "capacity_available_at": (
                None if self.capacity_available_at is None
                else self.capacity_available_at.isoformat()
            ),
        }


@dataclass(frozen=True)
class BarTcaFill:
    source_fill_id: str
    source_fill_hash: str
    source_ledger_hash: str
    order_id: str
    instrument_id: str
    fill_time: datetime
    quantity: int
    execution_price_units: int
    decision_price_units: int
    observed_price_shortfall_units: int
    formal_fee_units: int
    observed_implementation_shortfall_units: int
    liquidity_attribution_status: str
    participation_ppm: int | None
    modeled_participation_limit_exceeded: bool | None
    modeled_spread_slippage_units: int | None
    modeled_impact_units: int | None

    @property
    def fill_id(self) -> str:
        return self.source_fill_id

    def to_dict(self) -> dict[str, object]:
        return {**self.__dict__, "fill_time": self.fill_time.isoformat()}


@dataclass(frozen=True)
class BarTcaOrderResult:
    order_id: str
    status: str
    terminal_reason: str | None
    requested_quantity: int
    filled_quantity: int
    remaining_quantity: int
    fill_count: int
    observed_price_shortfall_units: int
    formal_fee_units: int
    observed_implementation_shortfall_units: int
    liquidity_attribution_status: str
    modeled_spread_slippage_units: int | None
    modeled_impact_units: int | None

    def to_dict(self) -> dict[str, object]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class BarTcaResult:
    orders: tuple[BarTcaOrderResult, ...]
    fills: tuple[BarTcaFill, ...]
    daily_rows: tuple[Mapping[str, object], ...]
    research_rows: tuple[Mapping[str, object], ...]
    policy_hash: str
    input_hash: str
    source_simulation_hash: str
    source_ledger_hash: str
    contract_version: str = BAR_TCA_RESULT_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != BAR_TCA_RESULT_VERSION:
            raise SimulationContractError("Bar TCA result 版本不受支持")
        if tuple(item.order_id for item in self.orders) != tuple(sorted(item.order_id for item in self.orders)):
            raise SimulationContractError("Bar TCA order result 必须按 order_id 排序")
        if tuple(item.source_fill_id for item in self.fills) != tuple(sorted(item.source_fill_id for item in self.fills)):
            raise SimulationContractError("Bar TCA fill 必须按正式 fill identity 排序")
        for field in ("policy_hash", "input_hash", "source_simulation_hash", "source_ledger_hash"):
            _require_hash(getattr(self, field), field)

    @property
    def result_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "orders": [item.to_dict() for item in self.orders],
            "fills": [item.to_dict() for item in self.fills],
            "daily_rows": [dict(item) for item in self.daily_rows],
            "research_rows": [dict(item) for item in self.research_rows],
            "policy_hash": self.policy_hash,
            "input_hash": self.input_hash,
            "source_simulation_hash": self.source_simulation_hash,
            "source_ledger_hash": self.source_ledger_hash,
            "contract_version": self.contract_version,
        }


class BarTcaAccumulator:
    """按已闭合交易日归因 Bar TCA，只保留研究汇总和当前日结果。"""

    def __init__(
        self,
        *,
        policy: BarTcaPolicy,
        source_simulation_hash: str,
        source_ledger_hash: str,
    ) -> None:
        _require_hash(source_simulation_hash, "source_simulation_hash")
        _require_hash(source_ledger_hash, "source_ledger_hash")
        self.policy = policy
        self.source_simulation_hash = source_simulation_hash
        self.source_ledger_hash = source_ledger_hash
        self._input_digest = hashlib.sha256(b"bar-tca-stream-input-v1\0")
        self._source_fill_digest = hashlib.sha256(b"bar-tca-fill-manifest-v1\0")
        self._order_count = 0
        self._fill_count = 0
        self._observed_price_shortfall_units = 0
        self._formal_fee_units = 0
        self._observed_implementation_shortfall_units = 0
        self._modeled_spread_slippage_units = 0
        self._modeled_impact_units = 0
        self._all_fills_computed = True

    def consume_session(
        self,
        *,
        orders: Sequence[BarTcaOrder],
        formal_fills: Sequence[BarTcaFormalFill],
    ) -> tuple[
        tuple[BarTcaOrderResult, ...],
        tuple[BarTcaFill, ...],
        tuple[Mapping[str, object], ...],
    ]:
        ordered_orders = tuple(sorted(orders, key=lambda item: item.order_id))
        ordered_fills = tuple(
            sorted(formal_fills, key=lambda item: item.source_fill_id)
        )
        if len({item.order_id for item in ordered_orders}) != len(ordered_orders):
            raise SimulationContractError("Bar TCA 正式 order_id 重复")
        if len({item.source_fill_id for item in ordered_fills}) != len(ordered_fills):
            raise SimulationContractError("Bar TCA 正式 source_fill_id 重复")
        for order in ordered_orders:
            if order.asset_class != self.policy.asset_class:
                raise SimulationContractError("Bar TCA 输入资产类别与 policy 不一致")
            if self.policy.available_at > order.decision_time:
                raise SimulationContractError("Bar TCA policy 在订单决策时尚不可见")
        order_by_id = {item.order_id: item for item in ordered_orders}
        attributed = []
        for fill in ordered_fills:
            if fill.asset_class != self.policy.asset_class:
                raise SimulationContractError("Bar TCA 输入资产类别与 policy 不一致")
            if fill.source_ledger_hash != self.source_ledger_hash:
                raise SimulationContractError("Bar TCA 正式成交与 ledger 身份漂移")
            order = order_by_id.get(fill.order_id)
            if order is None:
                raise SimulationContractError("Bar TCA 正式成交引用未知订单")
            if (
                fill.instrument_id != order.instrument_id
                or fill.side != order.side
                or fill.fill_time < order.submitted_at
            ):
                raise SimulationContractError("Bar TCA 正式成交与订单身份或时间不一致")
            attributed.append(_attribute_fill(order, fill, self.policy))
            self._source_fill_digest.update(typed_canonical_bytes({
                "source_fill_id": fill.source_fill_id,
                "source_fill_hash": fill.source_fill_hash,
            }))
        output_fills = tuple(attributed)
        results = []
        for order in ordered_orders:
            selected = tuple(
                item for item in output_fills if item.order_id == order.order_id
            )
            quantity = sum(item.quantity for item in selected)
            if quantity != order.filled_quantity:
                raise SimulationContractError(
                    "Bar TCA 正式成交数量与订单事实不一致"
                )
            computed = bool(selected) and all(
                item.liquidity_attribution_status == "computed"
                for item in selected
            )
            results.append(BarTcaOrderResult(
                order_id=order.order_id,
                status=order.status,
                terminal_reason=order.terminal_reason,
                requested_quantity=order.requested_quantity,
                filled_quantity=order.filled_quantity,
                remaining_quantity=(
                    order.requested_quantity - order.filled_quantity
                ),
                fill_count=len(selected),
                observed_price_shortfall_units=sum(
                    item.observed_price_shortfall_units for item in selected
                ),
                formal_fee_units=sum(item.formal_fee_units for item in selected),
                observed_implementation_shortfall_units=sum(
                    item.observed_implementation_shortfall_units
                    for item in selected
                ),
                liquidity_attribution_status=(
                    "computed" if computed else "not_computable"
                ),
                modeled_spread_slippage_units=(
                    sum(int(item.modeled_spread_slippage_units) for item in selected)
                    if computed else None
                ),
                modeled_impact_units=(
                    sum(int(item.modeled_impact_units) for item in selected)
                    if computed else None
                ),
            ))
        self._input_digest.update(typed_canonical_bytes({
            "orders": [item.to_dict() for item in ordered_orders],
            "formal_fills": [item.to_dict() for item in ordered_fills],
        }))
        self._order_count += len(results)
        self._fill_count += len(output_fills)
        self._observed_price_shortfall_units += sum(
            item.observed_price_shortfall_units for item in output_fills
        )
        self._formal_fee_units += sum(
            item.formal_fee_units for item in output_fills
        )
        self._observed_implementation_shortfall_units += sum(
            item.observed_implementation_shortfall_units for item in output_fills
        )
        for item in output_fills:
            if item.liquidity_attribution_status != "computed":
                self._all_fills_computed = False
            else:
                self._modeled_spread_slippage_units += int(
                    item.modeled_spread_slippage_units
                )
                self._modeled_impact_units += int(item.modeled_impact_units)
        return tuple(results), output_fills, _daily_rows(output_fills)

    @property
    def input_hash(self) -> str:
        return typed_canonical_hash({
            "stream_contract": "bar-tca-stream-input-v1",
            "stream_sha256": self._input_digest.hexdigest(),
            "policy_hash": self.policy.policy_hash,
            "source_simulation_hash": self.source_simulation_hash,
            "source_ledger_hash": self.source_ledger_hash,
        })

    def research_row(self) -> Mapping[str, object]:
        computed = self._fill_count > 0 and self._all_fills_computed
        return {
            "order_count": self._order_count,
            "fill_count": self._fill_count,
            "observed_price_shortfall_units": self._observed_price_shortfall_units,
            "formal_fee_units": self._formal_fee_units,
            "observed_implementation_shortfall_units": (
                self._observed_implementation_shortfall_units
            ),
            "liquidity_attribution_status": (
                "computed" if computed else "not_computable"
            ),
            "policy_hash": self.policy.policy_hash,
            "implementation_digest": self.policy.implementation_digest,
            "rule_snapshot_hash": self.policy.rule_snapshot_hash,
            "claim_ceiling": self.policy.claim_ceiling if computed else "analysis_only",
            "source_simulation_hash": self.source_simulation_hash,
            "source_ledger_hash": self.source_ledger_hash,
            "source_fill_manifest_hash": typed_canonical_hash({
                "stream_contract": "bar-tca-fill-manifest-v1",
                "stream_sha256": self._source_fill_digest.hexdigest(),
                "fill_count": self._fill_count,
            }),
            "input_hash": self.input_hash,
        }


@dataclass(frozen=True)
class BarTcaArtifactSummary:
    """流式 TCA 发布后供 Runtime 写小型元数据使用。"""

    result_hash: str
    policy_hash: str
    input_hash: str
    source_simulation_hash: str
    source_ledger_hash: str
    research_rows: tuple[Mapping[str, object], ...]


def _bar_tca_arrow_schemas() -> Mapping[str, pa.Schema]:
    return {
        "orders": pa.schema([
            pa.field("order_id", pa.string()),
            pa.field("status", pa.string()),
            pa.field("terminal_reason", pa.string()),
            pa.field("requested_quantity", pa.int64()),
            pa.field("filled_quantity", pa.int64()),
            pa.field("remaining_quantity", pa.int64()),
            pa.field("fill_count", pa.int64()),
            pa.field("observed_price_shortfall_units", pa.int64()),
            pa.field("formal_fee_units", pa.int64()),
            pa.field("observed_implementation_shortfall_units", pa.int64()),
            pa.field("liquidity_attribution_status", pa.string()),
            pa.field("modeled_spread_slippage_units", pa.int64()),
            pa.field("modeled_impact_units", pa.int64()),
        ]),
        "fills": pa.schema([
            pa.field("source_fill_id", pa.string()),
            pa.field("source_fill_hash", pa.string()),
            pa.field("source_ledger_hash", pa.string()),
            pa.field("order_id", pa.string()),
            pa.field("instrument_id", pa.string()),
            pa.field("fill_time", pa.string()),
            pa.field("quantity", pa.int64()),
            pa.field("execution_price_units", pa.int64()),
            pa.field("decision_price_units", pa.int64()),
            pa.field("observed_price_shortfall_units", pa.int64()),
            pa.field("formal_fee_units", pa.int64()),
            pa.field("observed_implementation_shortfall_units", pa.int64()),
            pa.field("liquidity_attribution_status", pa.string()),
            pa.field("participation_ppm", pa.int64()),
            pa.field("modeled_participation_limit_exceeded", pa.bool_()),
            pa.field("modeled_spread_slippage_units", pa.int64()),
            pa.field("modeled_impact_units", pa.int64()),
        ]),
        "daily": pa.schema([
            pa.field("date", pa.string()),
            pa.field("fill_count", pa.int64()),
            pa.field("observed_price_shortfall_units", pa.int64()),
            pa.field("formal_fee_units", pa.int64()),
            pa.field("observed_implementation_shortfall_units", pa.int64()),
            pa.field("liquidity_attribution_status", pa.string()),
            pa.field("modeled_spread_slippage_units", pa.int64()),
            pa.field("modeled_impact_units", pa.int64()),
        ]),
        "research": pa.schema([
            pa.field("order_count", pa.int64()),
            pa.field("fill_count", pa.int64()),
            pa.field("observed_price_shortfall_units", pa.int64()),
            pa.field("formal_fee_units", pa.int64()),
            pa.field("observed_implementation_shortfall_units", pa.int64()),
            pa.field("liquidity_attribution_status", pa.string()),
            pa.field("policy_hash", pa.string()),
            pa.field("implementation_digest", pa.string()),
            pa.field("rule_snapshot_hash", pa.string()),
            pa.field("claim_ceiling", pa.string()),
            pa.field("source_simulation_hash", pa.string()),
            pa.field("source_ledger_hash", pa.string()),
            pa.field("source_fill_manifest_hash", pa.string()),
            pa.field("input_hash", pa.string()),
        ]),
    }


class BarTcaArtifactWriter:
    """按交易日写 TCA 四表，oracle 只保存正式表引用。"""

    def __init__(
        self,
        output_root: str | Path,
        *,
        policy: BarTcaPolicy,
        source_simulation_hash: str,
        source_ledger_hash: str,
        table_references: Mapping[str, Mapping[str, object]],
    ) -> None:
        self.root = Path(output_root)
        if self.root.exists():
            raise FileExistsError(f"Bar TCA 工件已存在: {self.root}")
        self._staging = (
            self.root.parent / f".{self.root.name}.staging-{uuid.uuid4().hex}"
        )
        self._staging.mkdir(parents=True)
        self.policy = policy
        self.accumulator = BarTcaAccumulator(
            policy=policy,
            source_simulation_hash=source_simulation_hash,
            source_ledger_hash=source_ledger_hash,
        )
        self._schemas = _bar_tca_arrow_schemas()
        self._digests = {}
        self._row_counts = {name: 0 for name in self._schemas}
        self._files: dict[str, str] = {}
        self._closed = False
        required_references = {
            "formal_orders", "formal_fills", "decision_benchmarks",
            "execution_observations",
        }
        if set(table_references) != required_references:
            raise SimulationContractError("Bar TCA 表引用集合不闭合")
        self._table_references = {
            str(name): dict(reference)
            for name, reference in sorted(table_references.items())
        }
        for name, schema in self._schemas.items():
            (self._staging / name).mkdir()
            digest = hashlib.sha256()
            digest.update(typed_canonical_bytes({
                "table_hash_contract": "research-bar-tca-table-hash-v1",
                "schema": str(schema),
            }))
            self._digests[name] = digest

    def append_session(
        self,
        session: date,
        *,
        orders: Sequence[BarTcaOrder],
        formal_fills: Sequence[BarTcaFormalFill],
    ) -> None:
        if self._closed:
            raise SimulationContractError("Bar TCA writer 已经关闭")
        order_rows, fill_rows, daily_rows = self.accumulator.consume_session(
            orders=orders, formal_fills=formal_fills
        )
        rows_by_table = {
            "orders": [item.to_dict() for item in order_rows],
            "fills": [item.to_dict() for item in fill_rows],
            "daily": [dict(item) for item in daily_rows],
        }
        for name, rows in rows_by_table.items():
            if not rows:
                continue
            relative = f"{name}/session={session.isoformat()}/data.parquet"
            self._write_partition(name, relative, rows)

    def finalize(self) -> tuple[BarTcaArtifactSummary, dict[str, object]]:
        if self._closed:
            raise SimulationContractError("Bar TCA writer 只能 finalize 一次")
        self._closed = True
        research_row = dict(self.accumulator.research_row())
        self._write_partition(
            "research", "research/part-00000.parquet", [research_row]
        )
        for name, schema in self._schemas.items():
            if self._row_counts[name] != 0:
                continue
            relative = f"{name}/part-00000.parquet"
            path = self._staging / relative
            pq.write_table(pa.Table.from_pylist([], schema=schema), path)
            self._files[relative] = _sha256(path)
        table_hashes = {
            name: self._digests[name].hexdigest() for name in self._schemas
        }
        result_hash = typed_canonical_hash({
            "contract_version": BAR_TCA_STREAM_RESULT_VERSION,
            "policy_hash": self.policy.policy_hash,
            "input_hash": self.accumulator.input_hash,
            "source_simulation_hash": self.accumulator.source_simulation_hash,
            "source_ledger_hash": self.accumulator.source_ledger_hash,
            "table_hashes": table_hashes,
        })
        oracle = {
            "contract_version": BAR_TCA_TABLE_REFERENCE_VERSION,
            "policy": self.policy.to_dict(),
            "table_references": self._table_references,
            "partitioning": "session",
            "source_simulation_hash": self.accumulator.source_simulation_hash,
            "source_ledger_hash": self.accumulator.source_ledger_hash,
        }
        oracle_path = self._staging / "oracle-input.json"
        oracle_path.write_text(canonical_json(oracle), encoding="utf-8")
        self._files["oracle-input.json"] = _sha256(oracle_path)
        body = {
            "contract_version": BAR_TCA_STREAM_ARTIFACT_VERSION,
            "result_hash": result_hash,
            "policy_hash": self.policy.policy_hash,
            "input_hash": self.accumulator.input_hash,
            "source_simulation_hash": self.accumulator.source_simulation_hash,
            "source_ledger_hash": self.accumulator.source_ledger_hash,
            "table_rows": dict(self._row_counts),
            "schema_hashes": {
                name: typed_canonical_hash(str(schema))
                for name, schema in self._schemas.items()
            },
            "table_hashes": table_hashes,
            "files": dict(sorted(self._files.items())),
        }
        manifest = {**body, "manifest_hash": typed_canonical_hash(body)}
        (self._staging / "manifest.json").write_text(
            canonical_json(manifest), encoding="utf-8"
        )
        (self._staging / "COMMITTED").write_text(
            str(manifest["manifest_hash"]), encoding="ascii"
        )
        os.replace(self._staging, self.root)
        summary = BarTcaArtifactSummary(
            result_hash=result_hash,
            policy_hash=self.policy.policy_hash,
            input_hash=self.accumulator.input_hash,
            source_simulation_hash=self.accumulator.source_simulation_hash,
            source_ledger_hash=self.accumulator.source_ledger_hash,
            research_rows=(research_row,),
        )
        return summary, manifest

    def _write_partition(
        self,
        name: str,
        relative: str,
        rows: Sequence[Mapping[str, object]],
    ) -> None:
        path = self._staging / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(
            [dict(row) for row in rows], schema=self._schemas[name]
        )
        pq.write_table(table, path)
        self._files[relative] = _sha256(path)
        digest = self._digests[name]
        for row in table.to_pylist():
            digest.update(typed_canonical_bytes(row))
        self._row_counts[name] += int(table.num_rows)


def run_bar_tca(
    orders: Sequence[BarTcaOrder],
    formal_fills: Sequence[BarTcaFormalFill],
    *,
    policy: BarTcaPolicy,
    source_simulation_hash: str,
    source_ledger_hash: str,
) -> BarTcaResult:
    """只归因正式成交，不决定成交量、价格、拒单、顺延或强平。"""

    _require_hash(source_simulation_hash, "source_simulation_hash")
    _require_hash(source_ledger_hash, "source_ledger_hash")
    ordered_orders = tuple(sorted(orders, key=lambda item: item.order_id))
    ordered_fills = tuple(sorted(formal_fills, key=lambda item: item.source_fill_id))
    if not ordered_orders:
        raise SimulationContractError("Bar TCA 至少需要一个正式订单")
    if len({item.order_id for item in ordered_orders}) != len(ordered_orders):
        raise SimulationContractError("Bar TCA 正式 order_id 重复")
    if len({item.source_fill_id for item in ordered_fills}) != len(ordered_fills):
        raise SimulationContractError("Bar TCA 正式 source_fill_id 重复")
    if len({item.source_fill_hash for item in ordered_fills}) != len(ordered_fills):
        raise SimulationContractError("Bar TCA 正式 source_fill_hash 重复")
    if any(item.asset_class != policy.asset_class for item in (*ordered_orders, *ordered_fills)):
        raise SimulationContractError("Bar TCA 输入资产类别与 policy 不一致")
    if any(policy.available_at > item.decision_time for item in ordered_orders):
        raise SimulationContractError("Bar TCA policy 在订单决策时尚不可见")
    if any(item.source_ledger_hash != source_ledger_hash for item in ordered_fills):
        raise SimulationContractError("Bar TCA 正式成交与 ledger 身份漂移")
    order_by_id = {item.order_id: item for item in ordered_orders}
    attributed: list[BarTcaFill] = []
    for fill in ordered_fills:
        order = order_by_id.get(fill.order_id)
        if order is None:
            raise SimulationContractError("Bar TCA 正式成交引用未知订单")
        if (
            fill.instrument_id != order.instrument_id
            or fill.side != order.side
            or fill.fill_time < order.submitted_at
        ):
            raise SimulationContractError("Bar TCA 正式成交与订单身份或时间不一致")
        attributed.append(_attribute_fill(order, fill, policy))
    result_orders = []
    for order in ordered_orders:
        fills = tuple(item for item in attributed if item.order_id == order.order_id)
        quantity = sum(item.quantity for item in fills)
        if quantity != order.filled_quantity:
            raise SimulationContractError("Bar TCA 正式成交数量与订单事实不一致")
        computed = bool(fills) and all(item.liquidity_attribution_status == "computed" for item in fills)
        result_orders.append(BarTcaOrderResult(
            order_id=order.order_id,
            status=order.status,
            terminal_reason=order.terminal_reason,
            requested_quantity=order.requested_quantity,
            filled_quantity=order.filled_quantity,
            remaining_quantity=order.requested_quantity - order.filled_quantity,
            fill_count=len(fills),
            observed_price_shortfall_units=sum(item.observed_price_shortfall_units for item in fills),
            formal_fee_units=sum(item.formal_fee_units for item in fills),
            observed_implementation_shortfall_units=sum(
                item.observed_implementation_shortfall_units for item in fills
            ),
            liquidity_attribution_status="computed" if computed else "not_computable",
            modeled_spread_slippage_units=(
                sum(int(item.modeled_spread_slippage_units) for item in fills) if computed else None
            ),
            modeled_impact_units=(
                sum(int(item.modeled_impact_units) for item in fills) if computed else None
            ),
        ))
    input_hash = typed_canonical_hash({
        "orders": [item.to_dict() for item in ordered_orders],
        "formal_fills": [item.to_dict() for item in ordered_fills],
        "policy_hash": policy.policy_hash,
        "source_simulation_hash": source_simulation_hash,
        "source_ledger_hash": source_ledger_hash,
    })
    output_fills = tuple(sorted(attributed, key=lambda item: item.source_fill_id))
    results = tuple(sorted(result_orders, key=lambda item: item.order_id))
    daily = _daily_rows(output_fills)
    effective_claim = (
        policy.claim_ceiling
        if output_fills and all(item.liquidity_attribution_status == "computed" for item in output_fills)
        else "analysis_only"
    )
    research = ({
        "order_count": len(results),
        "fill_count": len(output_fills),
        "observed_price_shortfall_units": sum(item.observed_price_shortfall_units for item in output_fills),
        "formal_fee_units": sum(item.formal_fee_units for item in output_fills),
        "observed_implementation_shortfall_units": sum(
            item.observed_implementation_shortfall_units for item in output_fills
        ),
        "liquidity_attribution_status": (
            "computed" if output_fills and all(
                item.liquidity_attribution_status == "computed" for item in output_fills
            ) else "not_computable"
        ),
        "policy_hash": policy.policy_hash,
        "implementation_digest": policy.implementation_digest,
        "rule_snapshot_hash": policy.rule_snapshot_hash,
        "claim_ceiling": effective_claim,
        "source_simulation_hash": source_simulation_hash,
        "source_ledger_hash": source_ledger_hash,
        "source_fill_manifest_hash": typed_canonical_hash([
            {"source_fill_id": item.source_fill_id, "source_fill_hash": item.source_fill_hash}
            for item in output_fills
        ]),
        "input_hash": input_hash,
    },)
    return BarTcaResult(
        results,
        output_fills,
        daily,
        research,
        policy.policy_hash,
        input_hash,
        source_simulation_hash,
        source_ledger_hash,
    )


def _attribute_fill(
    order: BarTcaOrder,
    fill: BarTcaFormalFill,
    policy: BarTcaPolicy,
) -> BarTcaFill:
    direction = 1 if order.side == "buy" else -1
    shortfall = (
        direction
        * (fill.execution_price_units - order.decision_price_units)
        * fill.quantity
        * policy.contract_multiplier
    )
    computed = fill.arrival_price_units is not None and fill.visible_capacity is not None
    participation = None
    spread = None
    impact = None
    if computed:
        participation = fill.quantity * 1_000_000 // int(fill.visible_capacity)
        spread_delta = _price_bps_units(
            int(fill.arrival_price_units), policy.spread_slippage_bps
        )
        if policy.impact_model == "fixed_bps_v1":
            impact_bps = Decimal(policy.fixed_impact_bps)
        else:
            with localcontext() as context:
                context.prec = 40
                impact_bps = Decimal(policy.sqrt_impact_coefficient_bps) * (
                    Decimal(fill.quantity) / Decimal(int(fill.visible_capacity))
                ).sqrt()
        impact_delta = _price_bps_units(int(fill.arrival_price_units), impact_bps)
        spread = spread_delta * fill.quantity * policy.contract_multiplier
        impact = impact_delta * fill.quantity * policy.contract_multiplier
    return BarTcaFill(
        source_fill_id=fill.source_fill_id,
        source_fill_hash=fill.source_fill_hash,
        source_ledger_hash=fill.source_ledger_hash,
        order_id=fill.order_id,
        instrument_id=fill.instrument_id,
        fill_time=fill.fill_time,
        quantity=fill.quantity,
        execution_price_units=fill.execution_price_units,
        decision_price_units=order.decision_price_units,
        observed_price_shortfall_units=shortfall,
        formal_fee_units=fill.formal_fee_units,
        observed_implementation_shortfall_units=shortfall + fill.formal_fee_units,
        liquidity_attribution_status="computed" if computed else "not_computable",
        participation_ppm=participation,
        modeled_participation_limit_exceeded=(
            participation > policy.participation_cap_ppm if participation is not None else None
        ),
        modeled_spread_slippage_units=spread,
        modeled_impact_units=impact,
    )


def _daily_rows(fills: Sequence[BarTcaFill]) -> tuple[Mapping[str, object], ...]:
    rows = []
    for day in sorted({item.fill_time.date() for item in fills}):
        selected = tuple(item for item in fills if item.fill_time.date() == day)
        computed = all(item.liquidity_attribution_status == "computed" for item in selected)
        rows.append({
            "date": day.isoformat(),
            "fill_count": len(selected),
            "observed_price_shortfall_units": sum(item.observed_price_shortfall_units for item in selected),
            "formal_fee_units": sum(item.formal_fee_units for item in selected),
            "observed_implementation_shortfall_units": sum(
                item.observed_implementation_shortfall_units for item in selected
            ),
            "liquidity_attribution_status": "computed" if computed else "not_computable",
            "modeled_spread_slippage_units": (
                sum(int(item.modeled_spread_slippage_units) for item in selected) if computed else None
            ),
            "modeled_impact_units": (
                sum(int(item.modeled_impact_units) for item in selected) if computed else None
            ),
        })
    return tuple(rows)


def bar_tca_policy_from_parameters(
    parameters: Mapping[str, object],
    *,
    asset_class: str,
    bar_frequency: str,
    rule_snapshot_hash: str,
    contract_multiplier: int,
    price_scale: int,
) -> BarTcaPolicy:
    names = (
        "tca_impact_model", "tca_spread_slippage_bps", "tca_fixed_impact_bps",
        "tca_sqrt_impact_coefficient_bps", "tca_participation_cap_ppm",
        "tca_delay_benchmark", "tca_rounding_rule", "tca_policy_available_at",
        "tca_claim_ceiling",
    )
    missing = [name for name in names if name not in parameters]
    if missing:
        raise SimulationContractError(f"Bar TCA 参数缺失: {missing}")
    try:
        available_at = datetime.fromisoformat(str(parameters["tca_policy_available_at"]))
    except ValueError as exc:
        raise SimulationContractError("Bar TCA policy available_at 无效") from exc
    integer_names = {
        "spread_slippage_bps": "tca_spread_slippage_bps",
        "fixed_impact_bps": "tca_fixed_impact_bps",
        "sqrt_impact_coefficient_bps": "tca_sqrt_impact_coefficient_bps",
        "participation_cap_ppm": "tca_participation_cap_ppm",
    }
    for source in integer_names.values():
        if type(parameters[source]) is not int:
            raise SimulationContractError(f"Bar TCA 参数 {source} 必须是整数")
    return BarTcaPolicy(
        asset_class=asset_class,
        bar_frequency=bar_frequency,
        impact_model=str(parameters["tca_impact_model"]),
        **{target: int(parameters[source]) for target, source in integer_names.items()},
        delay_benchmark=str(parameters["tca_delay_benchmark"]),
        rounding_rule=str(parameters["tca_rounding_rule"]),
        contract_multiplier=contract_multiplier,
        price_scale=price_scale,
        available_at=available_at,
        rule_snapshot_hash=rule_snapshot_hash,
        claim_ceiling=str(parameters["tca_claim_ceiling"]),
    )


def write_bar_tca_artifact(
    result: BarTcaResult,
    output_root: str | Path,
    *,
    policy: BarTcaPolicy | None = None,
    orders: Sequence[BarTcaOrder] | None = None,
    formal_fills: Sequence[BarTcaFormalFill] | None = None,
) -> dict[str, object]:
    root = Path(output_root)
    if root.exists():
        existing = verify_bar_tca_artifact(root)
        if existing["result_hash"] != result.result_hash:
            raise SimulationContractError("Bar TCA 目标目录已包含不同结果")
        return existing
    staging = root.parent / f".{root.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir(parents=True)
    try:
        tables = {
            "orders": [item.to_dict() for item in result.orders],
            "fills": [item.to_dict() for item in result.fills],
            "daily": [dict(item) for item in result.daily_rows],
            "research": [dict(item) for item in result.research_rows],
        }
        files: dict[str, str] = {}
        schemas: dict[str, str] = {}
        for name, rows in tables.items():
            directory = staging / name
            directory.mkdir()
            path = directory / "part-00000.parquet"
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, path)
            files[f"{name}/part-00000.parquet"] = _sha256(path)
            schemas[name] = typed_canonical_hash(str(table.schema))
        if policy is not None or orders is not None or formal_fills is not None:
            if policy is None or orders is None or formal_fills is None:
                raise SimulationContractError("Bar TCA oracle 输入必须同时提供 policy/orders/formal_fills")
            oracle = {
                "contract_version": BAR_TCA_ORACLE_INPUT_VERSION,
                "policy": policy.to_dict(),
                "orders": [item.to_dict() for item in orders],
                "formal_fills": [item.to_dict() for item in formal_fills],
                "source_simulation_hash": result.source_simulation_hash,
                "source_ledger_hash": result.source_ledger_hash,
            }
            oracle_path = staging / "oracle-input.json"
            oracle_path.write_text(canonical_json(oracle), encoding="utf-8")
            files["oracle-input.json"] = _sha256(oracle_path)
        manifest = {
            "contract_version": BAR_TCA_ARTIFACT_VERSION,
            "result_hash": result.result_hash,
            "policy_hash": result.policy_hash,
            "input_hash": result.input_hash,
            "source_simulation_hash": result.source_simulation_hash,
            "source_ledger_hash": result.source_ledger_hash,
            "table_rows": {name: len(rows) for name, rows in tables.items()},
            "schema_hashes": schemas,
            "files": files,
        }
        manifest["manifest_hash"] = typed_canonical_hash(manifest)
        (staging / "manifest.json").write_text(canonical_json(manifest), encoding="utf-8")
        (staging / "COMMITTED").write_text(str(manifest["manifest_hash"]), encoding="ascii")
        os.replace(staging, root)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return verify_bar_tca_artifact(root)


def verify_bar_tca_artifact(output_root: str | Path) -> dict[str, object]:
    root = Path(output_root)
    manifest_path = root / "manifest.json"
    marker_path = root / "COMMITTED"
    if not manifest_path.is_file() or not marker_path.is_file():
        raise SimulationContractError("Bar TCA 工件缺少 manifest 或 COMMITTED")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("contract_version") != BAR_TCA_ARTIFACT_VERSION:
        raise SimulationContractError("Bar TCA 工件版本无效")
    expected_hash = typed_canonical_hash({
        key: value for key, value in manifest.items() if key != "manifest_hash"
    })
    if manifest.get("manifest_hash") != expected_hash or marker_path.read_text(encoding="ascii") != expected_hash:
        raise SimulationContractError("Bar TCA manifest 或提交标记漂移")
    for relative, expected in manifest.get("files", {}).items():
        path = root / str(relative)
        if not path.is_file() or _sha256(path) != expected:
            raise SimulationContractError(f"Bar TCA 文件损坏: {relative}")
    for name, expected_rows in manifest.get("table_rows", {}).items():
        path = root / str(name) / "part-00000.parquet"
        if not path.is_file() or pq.read_metadata(path).num_rows != expected_rows:
            raise SimulationContractError(f"Bar TCA 表行数漂移: {name}")
    return manifest


def _price_bps_units(price_units: int, bps: int | Decimal) -> int:
    value = Decimal(price_units) * Decimal(bps) / Decimal(10_000)
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "BAR_TCA_ARTIFACT_VERSION",
    "BAR_TCA_ORACLE_INPUT_VERSION",
    "BAR_TCA_POLICY_VERSION",
    "BAR_TCA_RESULT_VERSION",
    "BAR_TCA_STREAM_ARTIFACT_VERSION",
    "BAR_TCA_STREAM_RESULT_VERSION",
    "BAR_TCA_TABLE_REFERENCE_VERSION",
    "BarTcaArtifactSummary",
    "BarTcaArtifactWriter",
    "BarTcaFill",
    "BarTcaFormalFill",
    "BarTcaAccumulator",
    "BarTcaOrder",
    "BarTcaOrderResult",
    "BarTcaPolicy",
    "BarTcaResult",
    "bar_tca_policy_from_parameters",
    "run_bar_tca",
    "verify_bar_tca_artifact",
    "write_bar_tca_artifact",
]
