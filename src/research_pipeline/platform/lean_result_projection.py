"""正式LEAN结果的纯研究权益及成交事实投影。"""
from datetime import datetime,timezone,timedelta
from decimal import Decimal

LEAN_METRIC_VERSION = "2.0.0"

def _number(value, label):
    try:
        number = Decimal(str(value))
    except (ValueError, ArithmeticError) as exc:
        raise ValueError(f"LEAN指标数值无效：{label}") from exc
    if not number.is_finite():
        raise ValueError(f"LEAN指标数值必须有限：{label}")
    return number


def _timestamp(value, label):
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError(f"LEAN时间必须带时区：{label}")
    return stamp


def lean_research_daily(daily, *, account_events=(), final_account=None):
    """保留原始账户字段，仅为研究收益增加权益与实际估值时间投影。"""
    days = [dict(row) for row in daily]
    if not days:
        return days
    futures = [row.get("asset_class") == "cn_future" or "margin" in row for row in days]
    if any(futures) and not all(futures):
        raise ValueError("LEAN日终不能混用期货与现货账户")
    if not all(futures):
        if final_account is None or not final_account.get("terminal_observation_at"):
            return days
        at = _timestamp(final_account["valuation_at"], "account.valuation_at")
        if at != _timestamp(final_account["terminal_observation_at"], "account.terminal_observation_at"):
            raise ValueError("LEAN现货终态与冻结观察时点不一致")
        if at <= _timestamp(days[-1]["valuation_at"], "daily.valuation_at"):
            raise ValueError("LEAN现货尾部观察必须晚于最后交易窗口")
        for row in days:
            row["research_equity"] = row["equity"]
            row["research_cash"] = row["cash"]
            row["research_valuation_at"] = row["valuation_at"]
            row["research_equity_semantics"] = "trading_date_equity_with_terminal_delisting_observation"
        days[-1].update(research_equity=final_account["equity"], research_cash=final_account["cash"],
            research_valuation_at=final_account["valuation_at"],
            research_valuation_marks=final_account["valuation_marks"])
        return days
    for row in days:
        row["research_equity"] = row["equity"]
        row["research_margin"] = row["margin"]
        row["research_frozen_funds"] = row["frozen_funds"]
        row["research_available_funds"] = row["available_funds"]
        row["research_settled_through"] = row["settled_through"]
        row["research_valuation_at"] = row["valuation_at"]
        row["research_equity_semantics"] = "trading_date_equity_with_terminal_settlement_tail"
    last = days[-1]
    latest = None
    previous_sequence = None
    for event in account_events:
        sequence = event["event_sequence"]
        if previous_sequence is not None and sequence <= previous_sequence:
            raise ValueError("LEAN账户事件序号必须严格递增")
        previous_sequence = sequence
        if event.get("account_id") != last.get("account_id"):
            raise ValueError("LEAN账户事件与日终账户不一致")
        if event.get("reason") not in {"settlement", "terminal"}:
            continue
        if event.get("trading_date") != last.get("trading_date", last["date"]):
            continue
        stamp = _timestamp(event.get("valuation_at", event["available_at"]), "account_event.valuation_at")
        if stamp >= _timestamp(last["valuation_at"], "daily.valuation_at"):
            latest = event
    if final_account is not None:
        if final_account.get("account_id") != last.get("account_id"):
            raise ValueError("LEAN终态账户与日终账户不一致")
        if final_account.get("trading_date") != last.get("trading_date", last["date"]):
            raise ValueError("LEAN终态账户必须归属末交易日，观察尾段不能增加交易日")
        final_at = _timestamp(final_account["valuation_at"], "account.valuation_at")
        baseline = latest or last
        if final_at < _timestamp(baseline.get("valuation_at", baseline.get("available_at")), "latest.valuation_at"):
            raise ValueError("LEAN终态账户估值时间早于正式账户序列")
        if latest is not None and latest.get("reason") == "terminal" and _number(latest["equity"], "terminal.equity") != _number(final_account["equity"], "account.equity"):
            raise ValueError("LEAN终态账户与terminal事件权益不一致")
        latest = final_account
    if latest is not None:
        last["research_equity"] = latest["equity"]
        for field in ("margin", "frozen_funds", "available_funds"):
            last["research_" + field] = latest[field]
        last["research_settled_through"] = latest["settled_through"]
        last["research_valuation_at"] = latest.get("valuation_at", latest.get("available_at"))
    return days


def lean_trade_totals(dates, orders, *, fills=None, futures=False):
    """期货按交易日归属成交腿，现货沿用原生订单成交事件。"""
    gross = dict.fromkeys(dates, Decimal(0))
    fees_by_day = dict.fromkeys(dates, Decimal(0))
    order_ids, filled_ids, fill_ids, leg_ids = set(), set(), set(), set()
    if futures and fills is None:
        raise ValueError("LEAN期货指标必须提供正式fills表，空表也须显式提供")
    if futures:
        for event in orders:
            order_ids.add(event["order_id"])
    count = 0
    for event in fills if futures else orders:
        order_ids.add(event["order_id"])
        quantity = _number(event["quantity"], "fill quantity")
        if not futures and quantity == 0:
            continue
        if futures:
            if quantity <= 0 or quantity != quantity.to_integral_value():
                raise ValueError("LEAN期货成交腿数量必须为正整数手")
            if event["leg_id"] in leg_ids:
                raise ValueError("LEAN期货成交腿重复")
            leg_ids.add(event["leg_id"])
            fill_ids.add(event["fill_id"])
            day = str(event["trading_date"])
            multiplier = _number(event["multiplier"], "multiplier")
            if multiplier <= 0:
                raise ValueError("LEAN期货合约乘数必须为正")
        else:
            if event["status"] not in {"Filled", "PartiallyFilled"}:
                raise ValueError("LEAN非成交订单事件带有成交数量")
            day = _timestamp(event["at"], "fill.at").astimezone(timezone(timedelta(hours=8))).date().isoformat()
            multiplier = Decimal(1)
        _timestamp(event["at"], "fill.at")
        if day not in gross:
            raise ValueError("LEAN成交日期超出正式日终样本")
        price, fee = _number(event["price"], "fill price"), _number(event["fee"], "fee")
        if price <= 0 or fee < 0:
            raise ValueError("LEAN成交价格或费用无效")
        gross[day] += abs(quantity) * price * multiplier
        fees_by_day[day] += fee
        filled_ids.add(event["order_id"])
        order_ids.add(event["order_id"])
        count += 1
    return {"gross_by_day": gross, "fees_by_day": fees_by_day,
            "fee_total": sum(fees_by_day.values()), "fill_count": len(fill_ids) if futures else count,
            "filled_order_count": len(filled_ids), "order_count": len(order_ids)}


