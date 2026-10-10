"""从成交与公司行动原始事实独立还原现货每日持仓桶。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, time
from heapq import merge
from itertools import groupby
from zoneinfo import ZoneInfo

from ..errors import EvidenceContractError
from .common import aware_datetime, ceil_ratio, date_value, integer, ordered_rows


def verify_daily_etf_holdings(
    *, canonical, category_by_code, profile, commission_ppm,
    min_commission_units, corporate_actions, rule_parameters_for=None, corporate_action_records=(),
    verified_fee_units: Mapping[str, int] | None = None,
) -> None:
    """复核成交及持仓；登记事实、v2 公司行动或历史现金复核另需 cash 表。"""
    if verified_fee_units is not None and (
        not isinstance(verified_fee_units, Mapping)
        or any(type(key) is not str or type(value) is not int or value < 0
               for key, value in verified_fee_units.items())
    ):
        raise EvidenceContractError("独立费用映射无效")
    observed_fee_ids = set()
    actions_by_hash = {}
    for action in corporate_actions:
        actions_by_hash.setdefault(action.instrument_hash, []).append(action)
    observed_hashes = set()
    records_by_key = index_corporate_action_records(corporate_action_records)
    observed_records = set()
    cash_entitlements = []
    needs_cash = (rule_parameters_for is not None or bool(records_by_key)
                  or any(action.contract_version == 2 for action in corporate_actions))
    if needs_cash and "cash" not in canonical:
        raise EvidenceContractError("日频公司行动登记或现金复核缺少 cash 表")
    for cash_row in ordered_rows(canonical.get("cash", ()), order_by=("session",)):
        session = date_value(cash_row["session"], "cash.session")
        preopen = datetime.combine(session, time(9, 15), ZoneInfo("Asia/Shanghai"))
        for action in effective_daily_actions(corporate_actions, session, preopen):
            if action.contract_version == 2 and action.kind != "delisting_cash":
                registered = registered_quantity(action, records_by_key, preopen)
                if action.kind == "cash_dividend":
                    numerator = registered * action.cash_per_share_microunits
                    amount = (numerator + 5000) // 10000 if action.cash_per_share_microunits else registered * action.cash_per_share_units
                    cash_entitlements.append((action.effective_date, action.pay_date, amount))

    def records(name, phase):
        suffix = ("fill_time", "fill_id") if name == "fills" else ("valuation_time",)
        for row in ordered_rows(canonical[name], order_by=("instrument_id", "session", *suffix)):
            yield (str(row["instrument_id"]), date_value(row["session"], "session"), phase, row)

    events = merge(records("fills", 0), records("positions", 1), key=lambda item: item[:3])
    for code, instrument_events in groupby(events, key=lambda item: item[0]):
        category = category_by_code.get(code)
        if category is None:
            raise EvidenceContractError("ETF 日频成交或持仓引用未分类标的")
        if rule_parameters_for is None:
            parameters = {
                "settlement_days": profile.settlement_days_for(category),
                "commission_ppm": commission_ppm, "min_commission_units": min_commission_units,
                "transfer_fee_ppm": profile.transfer_fee_ppm, "sell_tax_ppm": profile.sell_tax_ppm,
                "lot_size": profile.lot_size, "sell_remainder_allowed": True,
            }
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
            preopen = datetime.combine(session, time(9, 15), ZoneInfo("Asia/Shanghai"))
            if rule_parameters_for is not None:
                parameters = rule_parameters_for(code, session, datetime.combine(session, time(9, 30), ZoneInfo("Asia/Shanghai")))
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
                    for action in effective_daily_actions(actions, session, preopen):
                        if action.announcement_available_time > preopen:
                            raise EvidenceContractError("ETF 日频公司行动在生效盘前尚不可见")
                        held = sellable + sum(value for _, value in entitlements)
                        registered = held
                        if action.contract_version == 2 and action.kind != "delisting_cash":
                            registered = registered_quantity(action, records_by_key, preopen)
                        if action.kind in {"rights", "code_change"}:
                            raise EvidenceContractError("日频现货该公司行动需要专属账户事实")
                        if action.kind == "cash_dividend":
                            numerator = registered * action.cash_per_share_microunits
                            amount = (numerator + 5000) // 10000 if action.cash_per_share_microunits else registered * action.cash_per_share_units
                            if action.contract_version != 2:
                                cash_entitlements.append((session, action.pay_date, amount))
                            delta = 0
                        elif action.kind == "delisting_cash":
                            if action.contract_version == 2 and action.settlement_available_time > preopen:
                                raise EvidenceContractError("日频退市清算价格在生效时尚不可见")
                            numerator = held * action.cash_per_share_microunits
                            amount = (numerator + 5000) // 10000 if action.cash_per_share_microunits else held * action.cash_per_share_units
                            cash_entitlements.append((session, action.pay_date, amount))
                            delta = -held
                        elif action.kind in {"stock_dividend", "split", "reverse_split"}:
                            if action.contract_version == 2 and action.kind in {"split", "reverse_split"} and registered != held:
                                raise EvidenceContractError("日频拆并股当前数量与登记事实不一致")
                            delta = registered * action.ratio_numerator // action.ratio_denominator - registered
                        else:
                            delta = 0
                        non_trade += delta
                        due = action.shares_sellable_date if action.contract_version == 2 else action.pay_date
                        if action.kind == "delisting_cash":
                            sellable = 0
                            entitlements = []
                        elif action.contract_version == 2 and action.kind == "reverse_split" and due > session:
                            sellable = 0
                            entitlements = [(due, held + delta)]
                        elif delta > 0 and due > session:
                            entitlements.append((due, delta))
                        else:
                            sellable += delta
                        if sellable < 0:
                            raise EvidenceContractError("日频现货公司行动消耗了尚未到账的持仓")
                    initialized = True
                if phase == 0:
                    quantity = integer(row["quantity"], "fill.quantity", minimum=1)
                    fill_time = aware_datetime(row["fill_time"], "fill.fill_time")
                    if rule_parameters_for is None and not (profile.effective_start <= session <= profile.effective_end and profile.rule_available_at <= fill_time):
                        raise EvidenceContractError("ETF 日频 fill 使用了无效或尚不可见的规则")
                    if fill_time < preopen or fill_time.astimezone(ZoneInfo("Asia/Shanghai")).date() != session:
                        raise EvidenceContractError("ETF 日频 fill 时间早于盘前公司行动或不属于该会话")
                    notional = integer(row["notional_units"], "fill.notional_units", minimum=1)
                    side = str(row["side"])
                    if verified_fee_units is None:
                        fee = max(parameters["min_commission_units"], ceil_ratio(notional * parameters["commission_ppm"], 1_000_000))
                        fee += ceil_ratio(notional * parameters["transfer_fee_ppm"], 1_000_000)
                        if side == "sell":
                            fee += ceil_ratio(notional * parameters["sell_tax_ppm"], 1_000_000)
                    else:
                        fill_id = str(row["fill_id"])
                        if fill_id not in verified_fee_units:
                            raise EvidenceContractError("日频显式成交缺少独立费用事实")
                        fee = verified_fee_units[fill_id]
                        observed_fee_ids.add(fill_id)
                    if int(row["fee_units"]) != fee:
                        raise EvidenceContractError("ETF 日频 fill 费用与独立费用事实不一致")
                    if side not in {"buy", "sell"}:
                        raise EvidenceContractError("日频现货 fill 买卖方向无效")
                    if not is_daily_quantity_allowed(quantity, side, parameters, sellable):
                        raise EvidenceContractError("日频现货 fill 违反交易单位、申报数量格点或零股规则")
                    if side == "buy":
                        if parameters["settlement_days"]:
                            trade_unsettled += quantity
                        else:
                            sellable += quantity
                    elif side == "sell":
                        if quantity > sellable:
                            raise EvidenceContractError("ETF 日频卖出 fill 绕过 T+0/T+1 或公司行动到账约束")
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
                    record_key = (row_hash, session)
                    if record_key in records_by_key:
                        if records_by_key[record_key]["quantity"] != expected["quantity"]:
                            raise EvidenceContractError("日频公司行动登记数量与登记日收盘持仓不一致")
                        observed_records.add(record_key)
                    if any(int(row[key]) != value for key, value in expected.items()):
                        raise EvidenceContractError("ETF 日频持仓桶与成交、公司行动或到账日不一致")
            if not seen_position:
                raise EvidenceContractError("ETF 日频成交会话缺少持仓桶")
    if records_by_key:
        first_session = min((date_value(row["session"], "cash.session") for row in canonical["cash"]), default=None)
        if any(key not in observed_records and first_session is not None and key[1] >= first_session for key in records_by_key):
            raise EvidenceContractError("日频公司行动登记事实缺少对应收盘持仓")
    if verified_fee_units is not None:
        expected_fee_ids = {str(row["fill_id"]) for row in canonical["fills"]}
        if observed_fee_ids != expected_fee_ids or set(verified_fee_units) != expected_fee_ids:
            raise EvidenceContractError("独立费用映射与正式成交集合不一致")
    if rule_parameters_for is not None:
        _verify_corporate_cash(canonical, cash_entitlements)
    if set(actions_by_hash) - observed_hashes:
        raise EvidenceContractError("ETF 日频公司行动引用了未声明的持仓标的")


def is_daily_quantity_allowed(quantity, side, parameters, sellable):
    """独立检查最低申报量、数量步长、上限及一次性零股余额。"""
    minimum = parameters.get(f"{side}_min_quantity", parameters["lot_size"])
    step = parameters.get(f"{side}_quantity_step", parameters["lot_size"])
    maximum = parameters.get(f"{side}_max_quantity")
    if maximum is not None and quantity > maximum:
        return False
    if quantity >= minimum and (quantity - minimum) % step == 0:
        return True
    if side != "sell" or parameters.get("sell_remainder_allowed") is not True or quantity > sellable:
        return False
    if sellable < minimum:
        return quantity == sellable
    remainder = (sellable - minimum) % step
    return remainder > 0 and quantity % step == remainder


def index_corporate_action_records(raw):
    """校验登记事实，并按标的及登记会话建立唯一索引。"""
    if not isinstance(raw, (list, tuple)):
        raise EvidenceContractError("公司行动登记事实必须为列表")
    output = {}
    for row in raw:
        if not isinstance(row, dict) or set(row) != {"instrument_hash", "record_time", "quantity", "source_ref"}:
            raise EvidenceContractError("公司行动登记事实 schema 无效")
        stamp = aware_datetime(row["record_time"], "record_time").astimezone(ZoneInfo("Asia/Shanghai"))
        if stamp.time() != time(15) or not isinstance(row["source_ref"], str) or not row["source_ref"].strip():
            raise EvidenceContractError("公司行动登记事实必须绑定收盘时点和来源")
        if not isinstance(row["instrument_hash"], str) or len(row["instrument_hash"]) != 64:
            raise EvidenceContractError("公司行动登记标的身份无效")
        integer(row["quantity"], "registered.quantity", minimum=0)
        key = (row["instrument_hash"], stamp.date())
        if key in output:
            raise EvidenceContractError("公司行动登记事实重复")
        output[key] = row
    return output


def registered_quantity(action, records, effective_time):
    row = records.get((action.instrument_hash, action.record_date))
    if row is None or aware_datetime(row["record_time"], "record_time") > effective_time:
        raise EvidenceContractError("v2 公司行动缺少生效前可见的登记日持仓事实")
    return row["quantity"]


def _verify_corporate_cash(canonical, entitlements):
    """从登记金额和到账日独立核对非交易现金与应收；不读取生产事件金额。"""
    rows = ordered_rows(canonical["cash"], order_by=("session",))
    previous_session = None
    for row in rows:
        session = date_value(row["session"], "cash.session")
        increase = sum(amount for effective, _, amount in entitlements
                       if effective <= session and (previous_session is None or effective > previous_session))
        receivable = sum(amount for effective, due, amount in entitlements if effective <= session < due)
        if integer(row["non_trade_cash_change_units"], "cash.non_trade") != increase:
            raise EvidenceContractError("日频公司行动非交易现金与登记权益不一致")
        if integer(row["receivable_cash_units"], "cash.receivable", minimum=0) != receivable:
            raise EvidenceContractError("日频公司行动应收或到账时间不一致")
        previous_session = session


def effective_daily_actions(actions, session, as_of):
    """按当前可见修订选择生效事实；未来修订不能回改过去权益。"""
    selected = {}
    for action in actions:
        if action.announcement_available_time > as_of:
            continue
        prior = selected.get(action.action_id)
        if prior is None or action.revision > prior.revision:
            selected[action.action_id] = action
    return tuple(sorted((action for action in selected.values() if action.effective_date == session),
                        key=lambda action: (action.action_id, action.revision)))
