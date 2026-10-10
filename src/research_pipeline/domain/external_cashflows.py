"""普通现货账户的外部出入金计划，不包含融资本金交易。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Mapping, Sequence

from research_pipeline.domain.time import require_aware_datetime


@dataclass(frozen=True)
class ExternalCashflow:
    event_id: str
    account_id: str
    currency: str
    direction: str
    amount_units: int
    requested_at: datetime
    available_at: datetime
    effective_at: datetime
    status: str = "settled"
    reason: str = ""
    source_ref: str = ""

    def __post_init__(self) -> None:
        for field in ("event_id", "account_id", "currency", "source_ref"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"资金流 {field} 必须为非空字符串")
        if self.currency != "CNY":
            raise ValueError("普通日频外部资金流只支持 CNY")
        if not isinstance(self.direction, str) or self.direction not in {"deposit", "withdrawal"}:
            raise ValueError("资金流 direction 只支持 deposit 或 withdrawal")
        if type(self.amount_units) is not int or self.amount_units <= 0:
            raise ValueError("资金流 amount_units 必须为正整数分")
        for field in ("requested_at", "available_at", "effective_at"):
            value = getattr(self, field)
            if not isinstance(value, datetime):
                raise ValueError(f"资金流 {field} 必须为带时区时点")
            require_aware_datetime(value, field)
        if max(self.requested_at, self.available_at) > self.effective_at:
            raise ValueError("资金流生效不能早于申请或可见时点")
        if not isinstance(self.status, str) or self.status not in {"settled", "cancelled", "failed"}:
            raise ValueError("资金流 status 只支持 settled、cancelled 或 failed")
        if not isinstance(self.reason, str) or (self.status != "settled" and not self.reason.strip()):
            raise ValueError("失败或取消资金流必须声明 reason")

    @property
    def accepted_at(self) -> datetime:
        return max(self.requested_at, self.available_at)

    @property
    def signed_units(self) -> int:
        return self.amount_units if self.direction == "deposit" else -self.amount_units

    def to_dict(self) -> dict[str, object]:
        return {**self.__dict__, **{field: getattr(self, field).isoformat()
                for field in ("requested_at", "available_at", "effective_at")}}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExternalCashflow:
        fields = set(cls.__dataclass_fields__)
        required = fields - {"status", "reason"}
        if set(value) - fields or required - set(value):
            raise ValueError("资金流计划字段缺失或包含未知字段")
        payload = dict(value)
        for field in ("requested_at", "available_at", "effective_at"):
            try:
                payload[field] = datetime.fromisoformat(payload[field])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"资金流 {field} 必须为 ISO 时点") from exc
        return cls(**payload)


def parse_external_cashflows(value: str | Sequence[ExternalCashflow | Mapping[str, object]]) -> tuple[ExternalCashflow, ...]:
    """解析原生参数；同刻稳定顺序由输入数组位置确定。"""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("external_cashflows 必须为完整 JSON 数组") from exc
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("external_cashflows 必须为数组或序列")
    plans = []
    for item in value:
        if isinstance(item, ExternalCashflow):
            plans.append(item)
        elif isinstance(item, Mapping):
            plans.append(ExternalCashflow.from_dict(item))
        else:
            raise ValueError("external_cashflows 每笔必须为资金流对象")
    if len({plan.event_id for plan in plans}) != len(plans):
        raise ValueError("资金流 event_id 不能重复")
    return tuple(plans)


__all__ = ["ExternalCashflow", "parse_external_cashflows"]
