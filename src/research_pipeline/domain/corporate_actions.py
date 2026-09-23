"""具备公告可见时间的公司行为合同。"""

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

    def __post_init__(self) -> None:
        if not self.action_id.strip() or self.revision < 1 or self.kind not in CORPORATE_ACTION_KINDS:
            raise DomainContractError("公司行为身份或类型无效")
        if len(self.instrument_hash) != 64:
            raise DomainContractError("instrument_hash 必须是 sha256")
        require_aware_datetime(self.announcement_available_time, "announcement_available_time")
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

    @property
    def action_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        payload = dict(self.__dict__)
        payload["announcement_available_time"] = self.announcement_available_time.isoformat()
        for key in ("record_date", "ex_date", "pay_date", "effective_date"):
            payload[key] = getattr(self, key).isoformat()
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "CorporateAction":
        expected = set(cls.__dataclass_fields__)
        if set(payload) != expected:
            raise DomainContractError("公司行为载荷字段不完整或含未知字段")
        values = dict(payload)
        try:
            values["announcement_available_time"] = datetime.fromisoformat(
                str(values["announcement_available_time"])
            )
            for key in ("record_date", "ex_date", "pay_date", "effective_date"):
                values[key] = date.fromisoformat(str(values[key]))
        except ValueError as exc:
            raise DomainContractError("公司行为载荷日期无效") from exc
        return cls(**values)


def resolve_corporate_actions(actions: tuple[CorporateAction, ...], *, as_of: datetime, effective_date: date) -> tuple[CorporateAction, ...]:
    require_aware_datetime(as_of, "as_of")
    by_id: dict[str, CorporateAction] = {}
    for action in actions:
        if action.announcement_available_time > as_of or action.effective_date != effective_date:
            continue
        previous = by_id.get(action.action_id)
        if previous is not None and previous.revision == action.revision and previous.action_hash != action.action_hash:
            raise DomainContractError("同一公司行为修订内容冲突")
        if previous is None or action.revision > previous.revision:
            by_id[action.action_id] = action
    return tuple(sorted(by_id.values(), key=lambda item: (item.action_id, item.revision)))


__all__ = ["CORPORATE_ACTION_KINDS", "CorporateAction", "resolve_corporate_actions"]
