"""公司行为到真实现金和数量账本事件的编译器。

v2 分红及数量权益使用登记日收盘快照，到账事件仅记录股份交付事实；
可卖日继续由权益结算转桶。退市清算扣除三个持仓桶并注销待可卖权益，
清算到账前记应收现金。事件不提供行情替代价或市场成交。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction
from math import gcd
from typing import TYPE_CHECKING, Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction, InstrumentKey
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.platform import typed_canonical_hash

from .events import FinancialEvent
from .orders import SimulationContractError

if TYPE_CHECKING:
    from .ledger import SpotLedgerState


@dataclass(frozen=True)
class CorporateActionRecordPosition:
    """登记日收盘的持仓事实，独立于后续行动修订及当前持仓。"""

    instrument_hash: str
    record_time: datetime
    quantity: int
    source_ref: str

    def __post_init__(self) -> None:
        require_aware_datetime(self.record_time, "record_time")
        local_time = self.record_time.astimezone(ZoneInfo("Asia/Shanghai"))
        if len(self.instrument_hash) != 64 or type(self.quantity) is not int or self.quantity < 0:
            raise SimulationContractError("登记持仓身份或数量无效")
        if local_time.time() != time(15):
            raise SimulationContractError("登记持仓必须来自登记日收盘时点")
        if not isinstance(self.source_ref, str) or not self.source_ref.strip():
            raise SimulationContractError("登记持仓必须有 source_ref")

    @property
    def record_date(self) -> date:
        return self.record_time.astimezone(ZoneInfo("Asia/Shanghai")).date()

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_hash": self.instrument_hash,
            "record_time": self.record_time.isoformat(),
            "quantity": self.quantity,
            "source_ref": self.source_ref,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "CorporateActionRecordPosition":
        if set(payload) != {"instrument_hash", "record_time", "quantity", "source_ref"}:
            raise SimulationContractError("登记持仓载荷字段无效")
        try:
            record_time = datetime.fromisoformat(str(payload["record_time"]))
        except ValueError as exc:
            raise SimulationContractError("登记持仓日期无效") from exc
        return cls(
            str(payload["instrument_hash"]), record_time,
            payload["quantity"], str(payload["source_ref"]),
        )


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
    contract_version: int = 1,
) -> tuple[CorporateAction, ...]:
    """编译聚宽 A 股实施方案。

    v2 的现金、红股、转增分别生成 :cash、:stock、:transfer 身份；
    cash_arrival_date、stock_arrival_date、transfer_arrival_date 分别来自
    a_bonus_date、dividend_arrival_date、a_transfer_arrival_date。
    只要求非零分配对应的到账字段，股份可卖日使用 listing_date。
    """
    if contract_version not in {1, 2}:
        raise SimulationContractError("公司行动编译 contract_version 不受支持")
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
        visibility_deadline = datetime.combine(ex_date, time(9, 15), ZoneInfo("Asia/Shanghai"))
        if (
            available_time > visibility_deadline if contract_version == 2
            else available_time.date() > ex_date
        ) and not allow_post_effective_visibility:
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
            cash_due = (
                _required_date(row.get("cash_arrival_date"), "cash_arrival_date")
                if contract_version == 2 else max(
                    ex_date, _optional_date(row.get("cash_arrival_date")) or ex_date,
                )
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
                **_row_contract_fields(row, contract_version),
            ))
        if contract_version == 2:
            # 红股和转增分别按登记数量确认，不能把不同来源与到账日合并。
            share_distributions = (
                ("stock", stock_per_ten, "stock_arrival_date"),
                ("transfer", transfer_per_ten, "transfer_arrival_date"),
            )
        else:
            share_distributions = (("stock", stock_per_ten + transfer_per_ten, "stock_arrival_date"),)
        for suffix, extra_shares, arrival_field in share_distributions:
            if not extra_shares:
                continue
            extra_scaled = _exact_scaled_integer(extra_shares, 10_000, "share_per_ten")
            numerator, denominator = 100_000 + extra_scaled, 100_000
            divisor = gcd(numerator, denominator)
            if contract_version == 2:
                arrival = _required_date(row.get(arrival_field), arrival_field)
                sellable = _required_date(row.get("listing_date"), "listing_date")
                stock_due = arrival
            else:
                arrival = _optional_date(row.get("stock_arrival_date"))
                sellable = _optional_date(row.get("listing_date"))
                stock_due = max(ex_date, arrival or ex_date, sellable or ex_date)
            actions.append(CorporateAction(
                f"{source_id}:{suffix}",
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
                **_row_contract_fields(row, contract_version, arrival=arrival, sellable=sellable),
            ))
    return tuple(sorted(actions, key=lambda item: (item.effective_date, item.action_id)))


def compile_cn_etf_corporate_action_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    trading_sessions: Sequence[date],
    as_of: datetime,
    allow_post_effective_visibility: bool = False,
    contract_version: int = 1,
) -> tuple[CorporateAction, ...]:
    """把基金分红实施记录编译为 ETF 现金应收或数量调整事件。"""
    if contract_version not in {1, 2}:
        raise SimulationContractError("公司行动编译 contract_version 不受支持")
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
        visibility_deadline = datetime.combine(ex_date, time(9, 15), timezone)
        if (
            available_time > visibility_deadline if contract_version == 2
            else available_session > ex_date
        ) and not allow_post_effective_visibility:
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
                pay_date if contract_version == 2 else max(ex_date, pay_date),
                ex_date,
                cash_per_share_microunits=_exact_scaled_integer(
                    cash_per_share, 1_000_000, "cash_per_share"
                ),
                **_row_contract_fields(row, contract_version),
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
                **_row_contract_fields(
                    row, contract_version,
                    arrival=_required_date(row.get("shares_arrival_date"), "shares_arrival_date")
                    if contract_version == 2 else None,
                    sellable=_required_date(row.get("shares_sellable_date"), "shares_sellable_date")
                    if contract_version == 2 else None,
                ),
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
    record_position: CorporateActionRecordPosition | None = None,
    position_buckets: Mapping[str, int] | None = None,
) -> tuple[FinancialEvent, ...]:
    require_aware_datetime(effective_time, "effective_time")
    if action.contract_version == 2:
        return _compile_v2_corporate_action(
            action, held_quantity=held_quantity, effective_time=effective_time,
            group_id=group_id, rule_hash=rule_hash, instruction=instruction,
            record_position=record_position, position_buckets=position_buckets,
        )
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
        raise SimulationContractError("退市现金清算必须使用有清算来源的 v2 合同")
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



def capture_corporate_action_record_positions(
    state: "SpotLedgerState", *, record_time: datetime, source_ref: str,
    instrument_hashes: Sequence[str] = (),
) -> tuple[CorporateActionRecordPosition, ...]:
    """收盘交易和结算完成后封存登记事实，显式标的还保留零持仓。"""
    quantities = {key: 0 for key in instrument_hashes}
    for lot in state.positions:
        quantities[lot.instrument_hash] = quantities.get(lot.instrument_hash, 0) + (
            lot.sellable + lot.unsettled + lot.frozen
        )
    return tuple(
        CorporateActionRecordPosition(key, record_time, quantity, source_ref)
        for key, quantity in sorted(quantities.items())
    )


def _row_contract_fields(
    row: Mapping[str, object], contract_version: int,
    *, arrival: date | None = None, sellable: date | None = None,
) -> dict[str, object]:
    if contract_version == 1:
        return {}
    source_ref = str(row.get("source_ref") or "").strip()
    if not source_ref:
        raise SimulationContractError("v2 公司行动源记录必须提供 source_ref")
    return {
        "contract_version": 2, "source_ref": source_ref,
        "shares_arrival_date": arrival, "shares_sellable_date": sellable,
    }


def _record_quantity(
    action: CorporateAction, record_position: CorporateActionRecordPosition | None,
    effective_time: datetime,
) -> int:
    if record_position is None:
        raise SimulationContractError("v2 公司行动缺少登记持仓，不能以生效时持仓代替")
    if (
        record_position.instrument_hash != action.instrument_hash
        or record_position.record_date != action.record_date
        or record_position.record_time > effective_time
    ):
        raise SimulationContractError("登记持仓标的、登记日或可见时点与公司行动不一致")
    return record_position.quantity


def _position_buckets(
    held_quantity: int, position_buckets: Mapping[str, int] | None,
) -> dict[str, int]:
    if position_buckets is None or set(position_buckets) != {"sellable", "unsettled", "frozen"}:
        raise SimulationContractError("数量转换或退市清算必须提供三个当前持仓桶")
    result = dict(position_buckets)
    if any(type(value) is not int or value < 0 for value in result.values()):
        raise SimulationContractError("当前持仓桶必须是非负整数")
    if sum(result.values()) != held_quantity:
        raise SimulationContractError("当前持仓桶与 held_quantity 不一致")
    return result


def _cash_entitlement_units(action: CorporateAction, quantity: int) -> int:
    if action.cash_per_share_microunits:
        return int(
            (Decimal(quantity * action.cash_per_share_microunits) / Decimal(10_000))
            .quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
    return quantity * action.cash_per_share_units


def _compile_v2_corporate_action(
    action: CorporateAction, *, held_quantity: int, effective_time: datetime,
    group_id: str, rule_hash: str, instruction: RightsExerciseInstruction | None,
    record_position: CorporateActionRecordPosition | None,
    position_buckets: Mapping[str, int] | None,
) -> tuple[FinancialEvent, ...]:
    local_date = effective_time.astimezone(ZoneInfo("Asia/Shanghai")).date()
    if local_date != action.effective_date:
        raise SimulationContractError("v2 公司行动落账日与生效日不一致")
    if action.announcement_available_time > effective_time:
        raise SimulationContractError("公司行为在落账时尚不可见")
    if type(held_quantity) is not int or held_quantity < 0:
        raise SimulationContractError("held_quantity 必须是非负整数")
    if action.kind in {"rights", "code_change"}:
        raise SimulationContractError("v2 配股或证券承接必须由专属指令与换股内核处理")
    registered = (
        held_quantity if action.kind == "delisting_cash"
        else _record_quantity(action, record_position, effective_time)
    )
    values: dict[str, object] = {
        "contract_version": 2, "action_hash": action.action_hash,
        "action_kind": action.kind, "action_revision": action.revision,
        "action_phase": "effective", "source_ref": action.source_ref,
        "instrument_hash": action.instrument_hash,
        "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0,
        "frozen_delta": 0, "registered_quantity": registered,
        "record_date": action.record_date.isoformat(),
        "ex_date": action.ex_date.isoformat(),
        "pay_date": action.pay_date.isoformat(),
    }
    if record_position is not None and action.kind != "delisting_cash":
        values["record_position"] = record_position.to_dict()
    cash_amount = 0
    entitlement_quantity = 0
    if action.kind == "cash_dividend":
        cash_amount = _cash_entitlement_units(action, registered)
    elif action.kind == "delisting_cash":
        if action.settlement_available_time > effective_time:
            raise SimulationContractError("退市清算价格在落账时尚不可见")
        buckets = _position_buckets(held_quantity, position_buckets)
        for key, quantity in buckets.items():
            values[f"{key}_delta"] = -quantity
        values["cancel_position_entitlements"] = True
        values["trading_termination_date"] = action.trading_termination_date.isoformat()
        values["settlement_available_time"] = action.settlement_available_time.isoformat()
        cash_amount = _cash_entitlement_units(action, held_quantity)
    elif action.kind == "stock_dividend":
        entitlement_quantity = (
            registered * action.ratio_numerator // action.ratio_denominator - registered
        )
    elif action.kind in {"split", "reverse_split"}:
        buckets = _position_buckets(held_quantity, position_buckets)
        if registered != held_quantity:
            raise SimulationContractError("拆并股当前数量与登记持仓不一致，需先解释中间数量变动")
        target = registered * action.ratio_numerator // action.ratio_denominator
        if target >= held_quantity:
            entitlement_quantity = target - held_quantity
        elif action.shares_sellable_date > local_date:
            for key, quantity in buckets.items():
                values[f"{key}_delta"] = -quantity
            entitlement_quantity = target
        else:
            converted = {
                key: quantity * action.ratio_numerator // action.ratio_denominator
                for key, quantity in buckets.items()
            }
            remaining = target - sum(converted.values())
            allocation = sorted(
                buckets,
                key=lambda key: (-(buckets[key] * action.ratio_numerator % action.ratio_denominator), key),
            )
            for key in allocation[:remaining]:
                converted[key] += 1
            for key, quantity in converted.items():
                values[f"{key}_delta"] = quantity - buckets[key]
    if cash_amount:
        if action.pay_date > local_date:
            values.update({
                "cash_receivable_units": cash_amount,
                "cash_due_date": action.pay_date.isoformat(),
                "receivable_id": f"cash:{action.action_hash}",
            })
        else:
            values["cash_delta_units"] = cash_amount
    if action.shares_arrival_date is not None:
        values["shares_arrival_date"] = action.shares_arrival_date.isoformat()
        values["shares_sellable_date"] = action.shares_sellable_date.isoformat()
    if entitlement_quantity:
        if action.shares_sellable_date > local_date:
            values.update({
                "position_entitlement_quantity": entitlement_quantity,
                "position_due_date": action.shares_sellable_date.isoformat(),
                "entitlement_id": f"position:{action.action_hash}",
            })
        else:
            values["sellable_delta"] = int(values["sellable_delta"]) + entitlement_quantity
    return (FinancialEvent(
        f"corporate:{action.action_hash}", "corporate_action", effective_time,
        local_date.isoformat(), group_id, rule_hash, tuple(sorted(values.items())),
        parent_id=action.action_id,
    ),)


def compile_corporate_action_share_arrival(
    action: CorporateAction, *, record_position: CorporateActionRecordPosition,
    effective_time: datetime, group_id: str, rule_hash: str,
) -> tuple[FinancialEvent, ...]:
    """登记股份到账事实；除权时已确认经济权益，到账不再增加数量或估值。"""
    require_aware_datetime(effective_time, "effective_time")
    local_date = effective_time.astimezone(ZoneInfo("Asia/Shanghai")).date()
    if action.contract_version != 2 or action.shares_arrival_date != local_date:
        raise SimulationContractError("股份到账事件必须匹配 v2 股份到账日")
    if action.announcement_available_time > effective_time:
        raise SimulationContractError("股份到账行动在当时尚不可见")
    registered = _record_quantity(action, record_position, effective_time)
    quantity = registered * action.ratio_numerator // action.ratio_denominator
    if action.kind == "stock_dividend" or action.ratio_numerator >= action.ratio_denominator:
        quantity -= registered
    values = {
        "contract_version": 2, "action_hash": action.action_hash,
        "action_kind": action.kind, "action_revision": action.revision,
        "action_phase": "shares_arrival", "source_ref": action.source_ref,
        "instrument_hash": action.instrument_hash, "arrived_quantity": quantity,
        "shares_arrival_date": action.shares_arrival_date.isoformat(),
        "shares_sellable_date": action.shares_sellable_date.isoformat(),
        "record_position": record_position.to_dict(),
        "cash_delta_units": 0, "sellable_delta": 0, "unsettled_delta": 0,
        "frozen_delta": 0,
    }
    return (FinancialEvent(
        f"corporate:{action.action_hash}:shares-arrival", "corporate_action",
        effective_time, local_date.isoformat(), group_id, rule_hash,
        tuple(sorted(values.items())), parent_id=action.action_id,
    ),)


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
