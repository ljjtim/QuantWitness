"""公司行为到真实现金和数量账本事件的编译器。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction
from math import gcd
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction, InstrumentKey
from research_pipeline.platform import typed_canonical_hash

from .events import FinancialEvent
from .orders import SimulationContractError


@dataclass(frozen=True)
class RightsExerciseInstruction:
    action_hash: str
    quantity: int
    available_time: datetime


def compile_cn_stock_corporate_action_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    trading_sessions: Sequence[date],
    allow_post_effective_visibility: bool = False,
) -> tuple[CorporateAction, ...]:
    """把聚宽 A 股实施方案行编译成带可见时间和到账日的通用事件。"""
    sessions = tuple(sorted(set(trading_sessions)))
    if not sessions:
        raise SimulationContractError("公司行动编译缺少交易日历")
    actions: list[CorporateAction] = []
    observed_ids: set[tuple[str, int]] = set()
    for row in rows:
        source_id = str(row.get("action_id", "")).strip()
        revision = int(row.get("revision", 1))
        source_identity = (source_id, revision)
        if not source_id or source_identity in observed_ids:
            raise SimulationContractError("公司行动源 id 为空或重复")
        observed_ids.add(source_identity)
        if row.get("plan_progress") != "实施方案":
            raise SimulationContractError("公司行动只接受实施方案")
        cash_per_ten = _nonnegative_decimal(row.get("cash_per_ten"), "cash_per_ten")
        stock_per_ten = _nonnegative_decimal(row.get("stock_dividend_per_ten"), "stock_dividend_per_ten")
        transfer_per_ten = _nonnegative_decimal(row.get("transfer_per_ten"), "transfer_per_ten")
        if cash_per_ten == stock_per_ten == transfer_per_ten == 0:
            continue
        code = str(row.get("code", "")).strip()
        if not code:
            raise SimulationContractError("公司行动证券代码为空")
        publication = _required_date(row.get("announcement_date"), "announcement_date")
        record = _required_date(row.get("record_date"), "record_date")
        ex_date = _required_date(row.get("ex_date"), "ex_date")
        available_session = next((item for item in sessions if item > publication), None)
        if available_session is None:
            raise SimulationContractError("公司行动实施方案缺少公告后的下一交易日")
        available_time = datetime.combine(
            available_session,
            time(9, 15),
            ZoneInfo("Asia/Shanghai"),
        )
        for field in ("source_added_at", "source_revised_at"):
            source_time = row.get(field)
            if source_time is not None:
                available_time = max(
                    available_time,
                    _required_local_datetime(
                        source_time,
                        field,
                        ZoneInfo("Asia/Shanghai"),
                    ),
                )
        if available_time.date() > ex_date and not allow_post_effective_visibility:
            raise SimulationContractError("公司行动实施方案在除权日前不可见")
        if revision < 1:
            raise SimulationContractError("公司行动修订序号必须为正整数")
        venue = code.rpartition(".")[2]
        if not venue:
            raise SimulationContractError("公司行动证券代码缺少 venue 后缀")
        instrument_hash = InstrumentKey(
            code,
            "cn_stock",
            venue,
            "CNY",
            "stock",
        ).instrument_hash
        if cash_per_ten:
            cash_due = max(
                ex_date,
                _optional_date(row.get("cash_arrival_date")) or ex_date,
            )
            per_share_microunits = _exact_scaled_integer(
                cash_per_ten,
                100_000,
                "cash_per_ten",
            )
            actions.append(CorporateAction(
                f"{source_id}:cash",
                revision,
                "cash_dividend",
                instrument_hash,
                available_time,
                record,
                ex_date,
                cash_due,
                ex_date,
                cash_per_share_microunits=per_share_microunits,
            ))
        extra_shares = stock_per_ten + transfer_per_ten
        if extra_shares:
            extra_scaled = _exact_scaled_integer(
                extra_shares,
                10_000,
                "share_per_ten",
            )
            numerator, denominator = 100_000 + extra_scaled, 100_000
            divisor = gcd(numerator, denominator)
            stock_due = max((
                ex_date,
                *(
                    value
                    for value in (
                        _optional_date(row.get("stock_arrival_date")),
                        _optional_date(row.get("listing_date")),
                    )
                    if value is not None
                ),
            ))
            actions.append(CorporateAction(
                f"{source_id}:stock",
                revision,
                "stock_dividend",
                instrument_hash,
                available_time,
                record,
                ex_date,
                stock_due,
                ex_date,
                ratio_numerator=numerator // divisor,
                ratio_denominator=denominator // divisor,
            ))
    return tuple(sorted(actions, key=lambda item: (item.effective_date, item.action_id)))


def compile_cn_etf_corporate_action_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    trading_sessions: Sequence[date],
    as_of: datetime,
    allow_post_effective_visibility: bool = False,
) -> tuple[CorporateAction, ...]:
    """把基金分红实施记录编译为 ETF 现金应收或数量调整事件。"""
    sessions = tuple(sorted(set(trading_sessions)))
    if not sessions:
        raise SimulationContractError("ETF 公司行动编译缺少交易日历")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise SimulationContractError("ETF 公司行动 as_of 必须带时区")
    timezone = ZoneInfo("Asia/Shanghai")
    local_as_of = as_of.astimezone(timezone)
    actions: list[CorporateAction] = []
    observed_ids: set[tuple[str, int]] = set()
    for row in rows:
        source_id = str(row.get("action_id", "")).strip()
        revision = int(row.get("revision", 1))
        source_identity = (source_id, revision)
        if not source_id or source_identity in observed_ids:
            raise SimulationContractError("ETF 公司行动源 id 为空或重复")
        observed_ids.add(source_identity)
        process_id = row.get("process_id")
        if process_id is not None and int(process_id) != 405002:
            raise SimulationContractError("ETF 公司行动只接受实施方案")
        status = row.get("status")
        if status is not None and int(status) != 0:
            raise SimulationContractError("ETF 公司行动状态不是当前有效记录")
        publication = _required_date(row.get("publication_date"), "publication_date")
        revised_at = _required_local_datetime(
            row.get("source_revised_at"), "source_revised_at", timezone
        )
        added_at = _required_local_datetime(
            row.get("source_added_at"), "source_added_at", timezone
        )
        if revised_at > local_as_of or added_at > local_as_of:
            raise SimulationContractError("ETF 公司行动包含 as_of 之后的源修订")
        visible_after_date = publication
        available_session = next(
            (session for session in sessions if session > visible_after_date), None
        )
        if available_session is None:
            raise SimulationContractError("ETF 公司行动缺少公告后的下一交易日")
        available_time = max(
            datetime.combine(available_session, time(9, 15), timezone),
            revised_at,
            added_at,
        )
        if available_time > local_as_of:
            raise SimulationContractError("ETF 公司行动在 as_of 时尚不可见")
        record = _required_date(row.get("record_date"), "record_date")
        ex_date = _required_date(row.get("ex_date"), "ex_date")
        if available_session > ex_date and not allow_post_effective_visibility:
            raise SimulationContractError("ETF 公司行动在除息日前不可见")
        if revision < 1:
            raise SimulationContractError("ETF 公司行动修订序号必须为正整数")
        instrument = _etf_instrument(str(row.get("code", "")).strip())
        cash_per_share = _nonnegative_decimal(
            row.get("cash_per_share"), "cash_per_share"
        )
        split_ratio = _nonnegative_decimal(row.get("split_ratio"), "split_ratio")
        if cash_per_share == split_ratio == 0:
            continue
        if cash_per_share:
            pay_date = _optional_date(row.get("pay_date"))
            if pay_date is None:
                raise SimulationContractError("ETF 现金分红缺少到账日")
            actions.append(CorporateAction(
                f"fund-dividend:{source_id}:cash",
                revision,
                "cash_dividend",
                instrument.instrument_hash,
                available_time,
                record,
                ex_date,
                max(ex_date, pay_date),
                ex_date,
                cash_per_share_microunits=_exact_scaled_integer(
                    cash_per_share, 1_000_000, "cash_per_share"
                ),
            ))
        if split_ratio:
            ratio = Fraction(split_ratio)
            split_kind = _etf_split_kind(row)
            actions.append(CorporateAction(
                f"fund-dividend:{source_id}:split",
                revision,
                split_kind,
                instrument.instrument_hash,
                available_time,
                record,
                ex_date,
                ex_date,
                ex_date,
                ratio_numerator=ratio.numerator,
                ratio_denominator=ratio.denominator,
            ))
    return tuple(sorted(actions, key=lambda item: (item.effective_date, item.action_id)))


def corporate_action_snapshot_hash(actions: Sequence[CorporateAction]) -> str:
    return typed_canonical_hash(
        [item.to_dict() for item in sorted(actions, key=lambda item: (item.action_id, item.revision))]
    )


def compile_corporate_action(
    action: CorporateAction,
    *,
    held_quantity: int,
    effective_time: datetime,
    group_id: str,
    rule_hash: str,
    instruction: RightsExerciseInstruction | None = None,
) -> tuple[FinancialEvent, ...]:
    if action.announcement_available_time > effective_time:
        raise SimulationContractError("公司行为在落账时尚不可见")
    if held_quantity < 0:
        raise SimulationContractError("held_quantity 不能为负")
    cash_delta = 0
    sellable_delta = 0
    if action.kind == "cash_dividend":
        cash_delta = held_quantity * action.cash_per_share_units
        if action.cash_per_share_microunits:
            cash_delta = int(
                (Decimal(held_quantity * action.cash_per_share_microunits) / Decimal(10_000))
                .quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )
    elif action.kind in {"stock_dividend", "split", "reverse_split"}:
        new_quantity = held_quantity * action.ratio_numerator // action.ratio_denominator
        sellable_delta = new_quantity - held_quantity
    elif action.kind == "rights":
        if instruction is None:
            return ()
        if instruction.action_hash != action.action_hash or instruction.available_time > effective_time:
            raise SimulationContractError("配股指令与公司行为不匹配或尚不可见")
        entitlement = held_quantity * action.ratio_numerator // action.ratio_denominator
        if instruction.quantity < 0 or instruction.quantity > entitlement:
            raise SimulationContractError("配股数量超过 entitlement")
        cash_delta = -instruction.quantity * action.exercise_price_units
        sellable_delta = instruction.quantity
    elif action.kind == "delisting_cash":
        cash_delta = held_quantity * action.cash_per_share_units
        sellable_delta = -held_quantity
    elif action.kind == "code_change":
        return ()
    values = {
        "cash_delta_units": cash_delta,
        "instrument_hash": action.instrument_hash,
        "sellable_delta": sellable_delta,
        "unsettled_delta": 0,
    }
    if action.kind == "cash_dividend" and cash_delta and action.pay_date > action.effective_date:
        values.update({
            "cash_delta_units": 0,
            "cash_receivable_units": cash_delta,
            "cash_due_date": action.pay_date.isoformat(),
            "receivable_id": f"cash:{action.action_hash}",
        })
    if (
        action.kind in {"stock_dividend", "split"}
        and sellable_delta > 0
        and action.pay_date > action.effective_date
    ):
        values.update({
            "sellable_delta": 0,
            "position_entitlement_quantity": sellable_delta,
            "position_due_date": action.pay_date.isoformat(),
            "entitlement_id": f"position:{action.action_hash}",
        })
    event = FinancialEvent(
        f"corporate:{action.action_hash}", "corporate_action", effective_time,
        action.effective_date.isoformat(), group_id, rule_hash,
        tuple(sorted(values.items())),
        parent_id=action.action_id,
    )
    return (event,)


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise SimulationContractError(f"{field} 不是有效数值") from exc
    if not result.is_finite() or result < 0:
        raise SimulationContractError(f"{field} 必须是非负有限数值")
    return result


def _exact_scaled_integer(value: Decimal, multiplier: int, field: str) -> int:
    scaled = value * multiplier
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise SimulationContractError(f"{field} 精度超过合同")
    return int(integral)


def _required_date(value: object, field: str) -> date:
    result = _optional_date(value)
    if result is None:
        raise SimulationContractError(f"{field} 不能为空")
    return result


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise SimulationContractError("公司行动日期无效") from exc


def _required_local_datetime(
    value: object,
    field: str,
    timezone: ZoneInfo,
) -> datetime:
    if not isinstance(value, datetime):
        raise SimulationContractError(f"{field} 必须是 datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone)
    return value.astimezone(timezone)


def _etf_instrument(code: str) -> InstrumentKey:
    if not code:
        raise SimulationContractError("ETF 公司行动证券代码为空")
    if "." in code:
        instrument_id, _, venue = code.rpartition(".")
    elif code.startswith("5"):
        instrument_id, venue = code, "XSHG"
    elif code.startswith("1"):
        instrument_id, venue = code, "XSHE"
    else:
        raise SimulationContractError("ETF 公司行动证券代码无法确定交易所")
    if not instrument_id or venue not in {"XSHG", "XSHE"}:
        raise SimulationContractError("ETF 公司行动证券代码或交易所无效")
    return InstrumentKey(
        f"{instrument_id}.{venue}", "cn_etf", venue, "CNY", "etf"
    )


def canonical_cn_etf_instrument_id(code: str) -> str:
    """把公司行动源的纯数字代码解析为研究主链的 ETF 标的身份。"""

    return _etf_instrument(code).instrument_id


def _etf_split_kind(row: Mapping[str, object]) -> str:
    """拆分方向只读显式事件类型，比例本身不参与猜测。"""

    values = tuple(
        str(row.get(field, "")).strip().lower()
        for field in ("event", "event_id")
        if row.get(field) is not None
    )
    split_names = {
        "split", "fund_split", "份额拆分", "基金份额拆分", "拆分", "份额折算增加",
    }
    reverse_names = {
        "reverse_split", "fund_reverse_split", "份额合并", "基金份额合并", "合并", "份额折算减少",
    }
    matches = {
        "split" if value in split_names else "reverse_split"
        for value in values
        if value in split_names or value in reverse_names
    }
    if len(matches) != 1:
        raise SimulationContractError("ETF 拆分/合并缺少唯一明确 event/event_id 类型")
    return matches.pop()


__all__ = [
    "RightsExerciseInstruction",
    "canonical_cn_etf_instrument_id",
    "compile_cn_etf_corporate_action_rows",
    "compile_cn_stock_corporate_action_rows",
    "compile_corporate_action",
    "corporate_action_snapshot_hash",
]
