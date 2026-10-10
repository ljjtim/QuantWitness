"""具备公告可见时间的公司行为合同。

v1 保持历史封存字段和内容身份；v2 显式区分股份到账与可卖日期，
并以来源、终止交易日和清算价格可见时点约束非交易退市现金清算。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping

from research_pipeline.platform.canonical import typed_canonical_hash

from .models import DomainContractError
from .time import require_aware_datetime


CORPORATE_ACTION_KINDS = frozenset({
    "cash_dividend", "stock_dividend", "split", "reverse_split", "rights", "delisting_cash", "code_change",
})


@dataclass(frozen=True)
class CorporateAction:
    action_id: str
    revision: int
    kind: str
    instrument_hash: str
    announcement_available_time: datetime
    record_date: date
    ex_date: date
    pay_date: date
    effective_date: date
    cash_per_share_units: int = 0
    cash_per_share_microunits: int = 0
    ratio_numerator: int = 0
    ratio_denominator: int = 1
    exercise_price_units: int = 0
    new_code: str | None = None
    contract_version: int = 1
    shares_arrival_date: date | None = None
    shares_sellable_date: date | None = None
    source_ref: str | None = None
    settlement_available_time: datetime | None = None
    trading_termination_date: date | None = None

    def __post_init__(self) -> None:
        if not self.action_id.strip() or self.revision < 1 or self.kind not in CORPORATE_ACTION_KINDS:
            raise DomainContractError("公司行为身份或类型无效")
        if len(self.instrument_hash) != 64:
            raise DomainContractError("instrument_hash 必须是 sha256")
        require_aware_datetime(self.announcement_available_time, "announcement_available_time")
        if self.contract_version not in {1, 2}:
            raise DomainContractError("公司行为 contract_version 不受支持")
        if not (self.record_date <= self.ex_date <= self.effective_date <= self.pay_date):
            raise DomainContractError("公司行为 record/ex/effective/pay 日期顺序无效")
        if min(self.cash_per_share_units, self.cash_per_share_microunits, self.ratio_numerator, self.exercise_price_units) < 0 or self.ratio_denominator < 1:
            raise DomainContractError("公司行为金额或比例无效")
        if self.cash_per_share_units and self.cash_per_share_microunits:
            raise DomainContractError("公司行为现金比例不能同时使用分和万分币精度")
        if self.kind in {"stock_dividend", "split", "reverse_split", "rights"} and self.ratio_numerator < 1:
            raise DomainContractError("数量类公司行为必须声明正比例")
        if self.kind == "code_change" and not self.new_code:
            raise DomainContractError("代码变更必须声明 new_code")

        extensions = (
            self.shares_arrival_date, self.shares_sellable_date, self.source_ref,
            self.settlement_available_time, self.trading_termination_date,
        )
        if self.contract_version == 1 and any(value is not None for value in extensions):
            raise DomainContractError("扩展公司行为事实必须显式使用 contract_version=2")
        if self.contract_version == 2:
            if not self.source_ref or not self.source_ref.strip():
                raise DomainContractError("v2 公司行为必须声明 source_ref")
            if self.kind in {"stock_dividend", "split"} and self.ratio_numerator < self.ratio_denominator:
                raise DomainContractError("送转或拆股的总股份比例不能小于一")
            if self.kind == "reverse_split" and self.ratio_numerator > self.ratio_denominator:
                raise DomainContractError("并股的总股份比例不能大于一")
            if self.kind in {"stock_dividend", "split", "reverse_split"}:
                if self.shares_arrival_date is None or self.shares_sellable_date is None:
                    raise DomainContractError("v2 数量类公司行为必须声明股份到账日和可卖日")
                if not (self.effective_date <= self.shares_arrival_date <= self.shares_sellable_date):
                    raise DomainContractError("公司行为生效、股份到账、可卖日期顺序无效")
            elif self.shares_arrival_date is not None or self.shares_sellable_date is not None:
                raise DomainContractError("非数量类公司行为不能声明股份到账日和可卖日")
            if self.kind == "delisting_cash":
                if self.trading_termination_date is None or self.settlement_available_time is None:
                    raise DomainContractError("退市现金清算必须声明终止交易日和清算价格可见时间")
                require_aware_datetime(self.settlement_available_time, "settlement_available_time")
                if self.trading_termination_date > self.effective_date:
                    raise DomainContractError("退市清算不能早于终止交易日")
                if not (self.cash_per_share_units or self.cash_per_share_microunits):
                    raise DomainContractError("退市现金清算必须有来源明确的正清算价格")
            elif self.settlement_available_time is not None or self.trading_termination_date is not None:
                raise DomainContractError("非退市现金清算不能声明退市清算事实")

    @property
    def action_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        payload = dict(self.__dict__)
        extension_fields = {
            "contract_version", "shares_arrival_date", "shares_sellable_date", "source_ref",
            "settlement_available_time", "trading_termination_date",
        }
        # v1 保持原始封存字段，旧行动及输入快照的内容身份不变。
        if self.contract_version == 1:
            for key in extension_fields:
                payload.pop(key)
        else:
            for key in ("shares_arrival_date", "shares_sellable_date", "trading_termination_date"):
                value = getattr(self, key)
                payload[key] = value.isoformat() if value is not None else None
            payload["settlement_available_time"] = (
                self.settlement_available_time.isoformat()
                if self.settlement_available_time is not None else None
            )
        payload["announcement_available_time"] = self.announcement_available_time.isoformat()
        for key in ("record_date", "ex_date", "pay_date", "effective_date"):
            payload[key] = getattr(self, key).isoformat()
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "CorporateAction":
        extension_fields = {
            "contract_version", "shares_arrival_date", "shares_sellable_date", "source_ref",
            "settlement_available_time", "trading_termination_date",
        }
        expected = set(cls.__dataclass_fields__)
        if "contract_version" not in payload:
            expected -= extension_fields
        elif payload["contract_version"] != 2:
            raise DomainContractError("带版本字段的公司行为载荷必须使用 v2")
        if set(payload) != expected:
            raise DomainContractError("公司行为载荷字段不完整或含未知字段")
        values = dict(payload)
        try:
            values["announcement_available_time"] = datetime.fromisoformat(
                str(values["announcement_available_time"])
            )
            for key in ("record_date", "ex_date", "pay_date", "effective_date"):
                values[key] = date.fromisoformat(str(values[key]))
            for key in ("shares_arrival_date", "shares_sellable_date", "trading_termination_date"):
                if values.get(key) is not None:
                    values[key] = date.fromisoformat(str(values[key]))
            if values.get("settlement_available_time") is not None:
                values["settlement_available_time"] = datetime.fromisoformat(
                    str(values["settlement_available_time"])
                )
        except ValueError as exc:
            raise DomainContractError("公司行为载荷日期无效") from exc
        return cls(**values)


def resolve_corporate_actions(actions: tuple[CorporateAction, ...], *, as_of: datetime, effective_date: date) -> tuple[CorporateAction, ...]:
    require_aware_datetime(as_of, "as_of")
    by_id: dict[str, CorporateAction] = {}
    visible_revisions: dict[tuple[str, int], CorporateAction] = {}
    for action in actions:
        if action.announcement_available_time > as_of:
            continue
        revision_key = (action.action_id, action.revision)
        previous_revision = visible_revisions.get(revision_key)
        if previous_revision is not None and previous_revision.action_hash != action.action_hash:
            raise DomainContractError("同一公司行为修订内容冲突")
        visible_revisions[revision_key] = action
        previous = by_id.get(action.action_id)
        if previous is None or action.revision > previous.revision:
            by_id[action.action_id] = action
    # 修订可能改动生效日，先确定当时最新事实，再判断本会话是否生效。
    return tuple(sorted(
        (item for item in by_id.values() if item.effective_date == effective_date),
        key=lambda item: (item.action_id, item.revision),
    ))


__all__ = ["CORPORATE_ACTION_KINDS", "CorporateAction", "resolve_corporate_actions"]
