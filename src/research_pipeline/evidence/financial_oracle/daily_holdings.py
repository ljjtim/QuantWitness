"""从成交与公司行动原始事实独立还原 ETF 每日持仓桶。"""

from __future__ import annotations

from datetime import datetime, time
from heapq import merge
from itertools import groupby
from zoneinfo import ZoneInfo

from ..errors import EvidenceContractError
from .common import aware_datetime, ceil_ratio, date_value, integer, ordered_rows


def verify_daily_etf_holdings(
    *, canonical, category_by_code, profile, commission_ppm,
    min_commission_units, corporate_actions,
) -> None:
    """按标的、会话流式归并成交和持仓，不调用生产账本或公司行动编译器。"""
    actions_by_hash = {}
    for action in corporate_actions:
        actions_by_hash.setdefault(action.instrument_hash, []).append(action)
    observed_hashes = set()

    def records(name, phase):
        suffix = ("fill_time", "fill_id") if name == "fills" else ("valuation_time",)
        for row in ordered_rows(canonical[name], order_by=("instrument_id", "session", *suffix)):
            yield (str(row["instrument_id"]), date_value(row["session"], "session"), phase, row)

    events = merge(records("fills", 0), records("positions", 1), key=lambda item: item[:3])
    for code, instrument_events in groupby(events, key=lambda item: item[0]):
        category = category_by_code.get(code)
        if category is None:
            raise EvidenceContractError("ETF 日频成交或持仓引用未分类标的")
        settlement_days = profile.settlement_days_for(category)
        sellable = trade_unsettled = 0
        entitlements = []
        instrument_hash = None
        actions = []
        for session, session_events in groupby(instrument_events, key=lambda item: item[1]):
            sellable += trade_unsettled
            trade_unsettled = 0
            pending = []
            for due, quantity in entitlements:
                if due <= session:
                    sellable += quantity
                else:
                    pending.append((due, quantity))
            entitlements = pending
            non_trade = 0
            initialized = False
            seen_position = False
            for _, _, phase, row in session_events:
                row_hash = str(row["instrument_hash"])
                if instrument_hash is None:
                    instrument_hash = row_hash
                    observed_hashes.add(row_hash)
                    actions = actions_by_hash.get(row_hash, ())
                elif row_hash != instrument_hash:
                    raise EvidenceContractError("ETF 日频标的与公司行动身份不一致")
                if not initialized:
                    preopen = datetime.combine(session, time(9, 15), ZoneInfo("Asia/Shanghai"))
                    for action in sorted(actions, key=lambda item: item.action_id):
                        if action.effective_date != session:
                            continue
                        if action.announcement_available_time > preopen:
                            raise EvidenceContractError("ETF 日频公司行动在生效盘前尚不可见")
                        held = sellable + sum(value for _, value in entitlements)
                        if action.kind in {"stock_dividend", "split", "reverse_split"}:
                            delta = held * action.ratio_numerator // action.ratio_denominator - held
                        elif action.kind == "delisting_cash":
                            delta = -held
                        else:
                            # 日频现金引擎没有配股行权指令；现金分红不改变份额。
                            delta = 0
                        non_trade += delta
                        if delta > 0 and action.kind in {"stock_dividend", "split"} and action.pay_date > session:
                            entitlements.append((action.pay_date, delta))
                        else:
                            sellable += delta
                        if sellable < 0:
                            raise EvidenceContractError("ETF 日频公司行动消耗了尚未到账的持仓")
                    initialized = True
                if phase == 0:
                    quantity = integer(row["quantity"], "fill.quantity", minimum=1)
                    fill_time = aware_datetime(row["fill_time"], "fill.fill_time")
                    if not (profile.effective_start <= session <= profile.effective_end and profile.rule_available_at <= fill_time):
                        raise EvidenceContractError("ETF 日频 fill 使用了无效或尚不可见的规则")
                    if fill_time < preopen or fill_time.astimezone(ZoneInfo("Asia/Shanghai")).date() != session:
                        raise EvidenceContractError("ETF 日频 fill 时间早于盘前公司行动或不属于该会话")
                    notional = integer(row["notional_units"], "fill.notional_units", minimum=1)
                    side = str(row["side"])
                    fee = max(min_commission_units, ceil_ratio(notional * commission_ppm, 1_000_000))
                    fee += ceil_ratio(notional * profile.transfer_fee_ppm, 1_000_000)
                    if side == "sell":
                        fee += ceil_ratio(notional * profile.sell_tax_ppm, 1_000_000)
                    if int(row["fee_units"]) != fee:
                        raise EvidenceContractError("ETF 日频 fill 费用与研究假设不一致")
                    remainder = quantity % profile.lot_size
                    if side == "buy":
                        if remainder:
                            raise EvidenceContractError("ETF 日频买入 fill 不符合受控交易单位")
                        if settlement_days:
                            trade_unsettled += quantity
                        else:
                            sellable += quantity
                    elif side == "sell":
                        if quantity > sellable:
                            raise EvidenceContractError("ETF 日频卖出 fill 绕过 T+0/T+1 或公司行动到账约束")
                        if remainder and remainder != sellable % profile.lot_size:
                            raise EvidenceContractError("ETF 日频卖出 fill 拆分零股，交易单位无效")
                        sellable -= quantity
                    else:
                        raise EvidenceContractError("ETF 日频 fill 买卖方向无效")
                else:
                    if seen_position:
                        raise EvidenceContractError("ETF 日频同一标的会话出现重复持仓桶")
                    seen_position = True
                    unsettled = trade_unsettled + sum(value for _, value in entitlements)
                    expected = {
                        "sellable_quantity": sellable,
                        "unsettled_quantity": unsettled,
                        "frozen_quantity": 0,
                        "quantity": sellable + unsettled,
                        "non_trade_quantity_change": non_trade,
                    }
                    if any(int(row[key]) != value for key, value in expected.items()):
                        raise EvidenceContractError("ETF 日频持仓桶与成交、公司行动或到账日不一致")
            if not seen_position:
                raise EvidenceContractError("ETF 日频成交会话缺少持仓桶")
    if set(actions_by_hash) - observed_hashes:
        raise EvidenceContractError("ETF 日频公司行动引用了未声明的持仓标的")
