"""日频期货按独立资金分配复核费用、仓位桶、盯市和强平。

入口只接收普通行映射。tables 包含 fills、settlements、nav、rule_snapshots、
market、source_settlements、tick_size_inputs；
context 保存初始资金与执行政策；market、source_settlements 保存原始来源行。
所有表按会话排序，fills 同会话再按 fill_sequence 排序；每次只保留一个会话。
来源行必须来自已封存的执行与结算输入，不能由待验证余额或成交价反推。
规则引用沿用 Result 已有身份，本模块不生成身份或读取数据库。
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from itertools import groupby
from fractions import Fraction
from typing import Iterable, Iterator, Mapping
from zoneinfo import ZoneInfo

from ..errors import EvidenceContractError


Row = Mapping[str, object]


def _field(row: Row, name: str) -> object:
    if name not in row or row[name] is None:
        raise EvidenceContractError(f"日频期货缺少必要事实：{name}")
    return row[name]


def _integer(value: object, label: str, minimum: int | None = None) -> int:
    if type(value) is not int or (minimum is not None and value < minimum):
        raise EvidenceContractError(f"日频期货 {label} 必须是合法整数")
    return value


def _decimal(value: object, label: str, minimum: Decimal = Decimal(0)) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise EvidenceContractError(f"日频期货 {label} 数值无效") from exc
    if not number.is_finite() or number < minimum:
        raise EvidenceContractError(f"日频期货 {label} 数值无效")
    return number


def _price(value: object, label: str) -> Decimal:
    number = _decimal(value, label)
    if number == 0:
        raise EvidenceContractError(f"日频期货 {label} 必须为正")
    return number


def _day(value: object) -> date:
    if isinstance(value, datetime):
        raise EvidenceContractError("日频期货交易会话必须是日期")
    try:
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    except ValueError as exc:
        raise EvidenceContractError("日频期货交易会话无效") from exc


def _time(value: object) -> datetime:
    try:
        result = (
            value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        )
    except ValueError as exc:
        raise EvidenceContractError("日频期货事实时间无效") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise EvidenceContractError("日频期货事实时间必须带时区")
    return result


def _fen(amount_cny: Decimal | Fraction) -> int:
    if isinstance(amount_cny, Fraction):
        # 有理基价的逐笔金额直接判定半分边界，避免除法精度改变舍入方向。
        scaled = amount_cny * 100
        magnitude = abs(scaled)
        rounded = (2 * magnitude.numerator + magnitude.denominator) // (
            2 * magnitude.denominator
        )
        return rounded if scaled >= 0 else -rounded
    return int((amount_cny * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _equal(row: Row, name: str, expected: object) -> None:
    actual = _field(row, name)
    if isinstance(expected, bool):
        valid = type(actual) is bool and actual == expected
    elif isinstance(expected, int):
        valid = _integer(actual, name) == expected
    else:
        valid = actual == expected
    if not valid:
        raise EvidenceContractError(f"日频期货 {name} 与独立复算不一致")


def _basis(row: Row, name: str, expected: Decimal | Fraction | None) -> None:
    if name not in row:
        raise EvidenceContractError(f"日频期货缺少必要事实：{name}")
    if expected is None:
        if row[name] is not None:
            raise EvidenceContractError(f"日频期货 {name} 应为空")
    elif row[name] is None or _price(row[name], name) != (
        Decimal(str(float(expected))) if isinstance(expected, Fraction) else expected
    ):
        raise EvidenceContractError(f"日频期货 {name} 与独立复算不一致")


def _index(rows: Iterable[Row], name: str) -> dict[object, Row]:
    result: dict[object, Row] = {}
    for row in rows:
        key = _field(row, name)
        if key in result:
            raise EvidenceContractError(f"日频期货 {name} 重复")
        result[key] = row
    return result


def _exact_basis(
    row: Row, prefix: str, expected: Fraction | None, scale_field: str
) -> None:
    names = (prefix + "_numerator", prefix + "_denominator", scale_field)
    if expected is None:
        if any(name not in row or row[name] is not None for name in names[:2]):
            raise EvidenceContractError(f"日频期货 {prefix} 零仓基价事实不一致")
        return
    numerator = _integer(_field(row, names[0]), names[0], 1)
    denominator = _integer(_field(row, names[1]), names[1], 1)
    scale = _integer(_field(row, scale_field), scale_field, 0)
    if Fraction(numerator, denominator * 10**scale) != expected:
        raise EvidenceContractError(f"日频期货 {prefix} 有理基价与独立复算不一致")


def _rule_index(rows: Iterable[Row]) -> dict[object, Row]:
    result: dict[object, Row] = {}
    fields = (
        "contract_code",
        "trading_date",
        "available_at",
        "multiplier",
        "fee_unit",
        "open_fee_permyriad",
        "close_fee_permyriad",
        "close_today_fee_permyriad",
    )
    for row in rows:
        reference = _field(row, "rule_snapshot_hash")
        if reference in result:
            previous = result[reference]
            if any(_field(previous, field) != _field(row, field) for field in fields):
                raise EvidenceContractError("日频期货同一引用的规则事实冲突")
            for field in (
                "settlement_margin_rate_pct",
                "settlement_margin_available_at",
            ):
                if field in row and field in previous and row[field] != previous[field]:
                    raise EvidenceContractError("日频期货同一引用的结算规则事实冲突")
            result[reference] = {**previous, **row}
        else:
            result[reference] = row
    return result


def _session_groups(
    table: Iterable[Row], columns: tuple[str, ...], session_field: str
) -> Iterator[tuple[date, list[Row]]]:
    # OracleTable 在扫描侧排序；普通迭代器由调用方提供同样的顺序。
    row_iterator = (
        table.iter_rows(order_by=columns)
        if hasattr(table, "iter_rows")
        else iter(table)
    )
    previous: date | None = None
    for session, rows in groupby(
        row_iterator, key=lambda row: _day(_field(row, session_field))
    ):
        if previous is not None and session <= previous:
            raise EvidenceContractError("日频期货输入必须按交易会话严格排序")
        previous = session
        yield session, list(rows)


def _rule(
    rules: Mapping[object, Row],
    reference: object,
    contract: str,
    session: date,
    application_time: datetime,
) -> Row:
    if reference not in rules:
        raise EvidenceContractError("日频期货缺少引用的规则快照")
    row = rules[reference]
    if (
        str(_field(row, "contract_code")) != contract
        or _day(_field(row, "trading_date")) != session
    ):
        raise EvidenceContractError("日频期货规则合约或会话不一致")
    if _time(_field(row, "available_at")) > application_time:
        raise EvidenceContractError("日频期货规则在应用时点尚不可见")
    _integer(_field(row, "multiplier"), "multiplier", 1)
    for field in (
        "open_fee_permyriad",
        "close_fee_permyriad",
        "close_today_fee_permyriad",
    ):
        _decimal(_field(row, field), field)
    if _field(row, "fee_unit") not in {"per_lot_cny", "notional_permyriad"}:
        raise EvidenceContractError("日频期货费用单位未支持")
    return row


def verify_daily_futures_financial_context(
    *,
    tables: Mapping[str, Iterable[Row]],
    context: Row,
    verified_execution_prices: Mapping[str, Decimal] | None = None,
) -> dict[str, object]:
    """从普通结果与可见来源重建单一资金分配；任一必要事实缺失即拒绝。

    持仓从零开始，一个账户同时只持有一个真实合约的单一净方向。换月必须
    先平旧再开新。普通 close 先昨后今，仍按普通平仓费率计取整笔费用；
    强平拆成平昨、平今两腿，各腿单独舍入。多品种聚合须在调用方独立核对。
    market 必须提供 date/code/open/open_available_at，同一开盘价可供多笔订单使用。
    source_settlements 必须提供 date/code/settle_price/settlement_time/available_at
    和 ledger_settlement_time，依次核验来源事件、可见时间与账本应用顺序。非零滑点消费
    tick_size_inputs 的 date/code/tick_size/available_at。规则行保留现有
    rule_snapshot_hash 引用与 settlement_margin_available_at；不补造可见时间。
    有理基价用已有 numerator/denominator/price_scale 字段精确比较。
    返回复算汇总，不发布 VerificationResult，也不替代 Result 的身份复验。
    """
    with localcontext() as decimal_context:
        decimal_context.prec = 28
        return _verify(tables, context, verified_execution_prices)


def _verify(tables: Mapping[str, Iterable[Row]], context: Row,
            verified_execution_prices: Mapping[str, Decimal] | None = None) -> dict[str, object]:
    policies = {
        "cash_scale": 2,
        "initial_position": 0,
        "account_model": "independent_allocation",
        "margin_check_policy": "settlement_margin_check",
        "forced_execution_policy": "settlement_price_full_close",
    }
    for name, expected in policies.items():
        _equal(context, name, expected)
    if _field(context, "close_bucket_order") != "yesterday_then_today":
        raise EvidenceContractError("日频期货普通 close 必须声明先昨后今")
    equity = _integer(_field(context, "initial_cash_fen"), "initial_cash_fen", 1)
    slippage_ticks = _integer(_field(context, "slippage_ticks"), "slippage_ticks", 0)
    orders = {
        "fills": (("trading_date", "fill_sequence"), "trading_date"),
        "nav": (("trading_date",), "trading_date"),
        "rule_snapshots": (("trading_date", "rule_snapshot_hash"), "trading_date"),
        "market": (("date", "code"), "date"),
        "source_settlements": (("date", "code"), "date"),
        "tick_size_inputs": (("date", "code"), "date"),
    }
    streams = {
        name: _session_groups(_field(tables, name), columns, session_field)
        for name, (columns, session_field) in orders.items()
    }
    heads = {name: next(stream, None) for name, stream in streams.items()}
    settlement_stream = _session_groups(
        _field(tables, "settlements"), ("trading_date",), "trading_date"
    )
    sessions_verified = fills_verified = 0
    position = today = yesterday = 0
    held_contract: str | None = None
    basis: Fraction | None = None
    last_settlements: dict[str, Decimal] = {}
    total_fees = total_realized = total_mtm = forced_sessions = 0

    previous_session_time: datetime | None = None
    for session, settlement_rows in settlement_stream:
        if len(settlement_rows) != 1:
            raise EvidenceContractError("日频期货同一会话必须有唯一结算")
        rows: dict[str, list[Row]] = {}
        for name, head in heads.items():
            if head is not None and head[0] < session:
                raise EvidenceContractError(f"日频期货 {name} 存在没有对应结算的会话")
            rows[name] = head[1] if head is not None and head[0] == session else []
        if len(rows["nav"]) != 1:
            raise EvidenceContractError("日频期货净值会话事实缺失或重复")
        rules = _rule_index(rows["rule_snapshots"])
        markets = _index(rows["market"], "code")
        sources = _index(rows["source_settlements"], "code")
        ticks = _index(rows["tick_size_inputs"], "code")
        fill_ids: set[object] = set()
        today, yesterday = 0, abs(position)
        settlement = settlement_rows[0]
        _equal(settlement, "opening_equity_fen", equity)
        _equal(settlement, "margin_check_policy", "settlement_margin_check")
        _equal(settlement, "close_bucket_order", "yesterday_then_today")
        _equal(settlement, "pnl_rounding_policy", "half_up_per_event")
        contract = str(_field(settlement, "actual_contract"))
        if contract not in sources:
            raise EvidenceContractError("日频期货结算缺少原始来源事实")
        source = sources[contract]
        settlement_time = _time(_field(source, "ledger_settlement_time"))
        source_event_time = _time(_field(source, "settlement_time"))
        source_available_time = _time(_field(source, "available_at"))
        if settlement_time.astimezone(ZoneInfo("Asia/Shanghai")).date() != session:
            raise EvidenceContractError("日频期货结算时点不属于声明会话")
        if (
            previous_session_time is not None
            and settlement_time <= previous_session_time
        ):
            raise EvidenceContractError("日频期货结算时点未递增")
        if _time(_field(settlement, "settlement_time")) != settlement_time:
            raise EvidenceContractError("日频期货结算时点与来源不一致")
        if not source_event_time <= source_available_time <= settlement_time:
            raise EvidenceContractError(
                "日频期货结算来源事件、可见时间与账本应用顺序不一致"
            )
        settle_price = _price(_field(source, "settle_price"), "settle_price")
        _basis(settlement, "settlement_price", settle_price)
        _basis(settlement, "previous_settlement_price", last_settlements.get(contract))
        settlement_rule = _rule(
            rules,
            _field(settlement, "settlement_rule_snapshot_hash"),
            contract,
            session,
            settlement_time,
        )
        if (
            _time(_field(settlement_rule, "settlement_margin_available_at"))
            > settlement_time
        ):
            raise EvidenceContractError("日频期货结算保证金规则尚不可见")
        rate = _decimal(
            _field(settlement_rule, "settlement_margin_rate_pct"),
            "settlement_margin_rate_pct",
        )
        if rate == 0:
            raise EvidenceContractError("日频期货结算保证金比例必须为正")
        if _decimal(_field(settlement, "margin_rate_pct"), "margin_rate_pct") != rate:
            raise EvidenceContractError("日频期货结算保证金比例与规则不一致")
        multiplier = _integer(_field(settlement_rule, "multiplier"), "multiplier", 1)
        ordered = rows["fills"]
        sequences = [
            _integer(_field(row, "fill_sequence"), "fill_sequence", 0)
            for row in ordered
        ]
        if any(right <= left for left, right in zip(sequences, sequences[1:])):
            raise EvidenceContractError("日频期货成交顺序重复")
        normal: list[Row] = []
        forced: list[Row] = []
        for fill in ordered:
            fill_id = _field(fill, "fill_id")
            if fill_id in fill_ids:
                raise EvidenceContractError("日频期货会话成交重复")
            fill_ids.add(fill_id)
            reason = _field(fill, "execution_reason")
            if reason not in {"target_order", "settlement_margin_shortfall"} and not (
                reason == "explicit_order" and verified_execution_prices is not None
                and fill_id in verified_execution_prices
            ):
                raise EvidenceContractError("日频期货成交执行原因未支持")
            if reason == "settlement_margin_shortfall":
                forced.append(fill)
            elif forced:
                raise EvidenceContractError("日频期货强平后不能继续普通交易")
            else:
                normal.append(fill)

        def apply_fill(fill: Row, *, is_forced: bool) -> None:
            nonlocal equity, position, today, yesterday, basis, held_contract
            nonlocal total_fees, total_realized
            leg_contract = str(_field(fill, "actual_contract"))
            fill_time = _time(_field(fill, "fill_time"))
            if fill_time > settlement_time or (
                is_forced and fill_time != settlement_time
            ):
                raise EvidenceContractError("日频期货成交不符合执行／结算顺序")
            rule = _rule(
                rules,
                _field(fill, "rule_snapshot_hash"),
                leg_contract,
                session,
                fill_time,
            )
            leg_multiplier = _integer(_field(rule, "multiplier"), "multiplier", 1)
            _equal(fill, "multiplier", leg_multiplier)
            price = _price(_field(fill, "fill_price"), "fill_price")
            if is_forced:
                if (
                    leg_contract != contract
                    or price != settle_price
                    or rule is not settlement_rule
                ):
                    raise EvidenceContractError(
                        "日频期货强平没有使用当前结算事实与规则"
                    )
            else:
                if leg_contract not in markets:
                    raise EvidenceContractError("日频期货成交缺少原始执行行情")
                fact = markets[leg_contract]
                if _time(_field(fact, "open_available_at")) > fill_time or (
                    previous_session_time is not None
                    and fill_time <= previous_session_time
                ):
                    raise EvidenceContractError("日频期货可见执行事实或交易会话不一致")
                expected_price = _price(_field(fact, "open"), "market.open")
                if _field(fill, "execution_reason") == "explicit_order":
                    expected_price = verified_execution_prices[_field(fill, "fill_id")]
                elif slippage_ticks:
                    if leg_contract not in ticks:
                        raise EvidenceContractError("日频期货缺少 tick 来源事实")
                    tick_fact = ticks[leg_contract]
                    tick = _price(_field(tick_fact, "tick_size"), "tick_size")
                    if _time(_field(tick_fact, "available_at")) > fill_time:
                        raise EvidenceContractError("日频期货 tick 规则尚不可见")
                    expected_price += (
                        tick
                        * slippage_ticks
                        * (1 if _field(fill, "side") == "buy" else -1)
                    )
                if expected_price <= 0 or price != expected_price:
                    raise EvidenceContractError("日频期货成交价与原始执行行情不一致")
            quantity = _integer(_field(fill, "quantity"), "quantity", 1)
            effect = _field(fill, "position_effect")
            side = _field(fill, "side")
            if side not in {"buy", "sell"}:
                raise EvidenceContractError("日频期货成交方向无效")
            direction = 1 if side == "buy" else -1
            _equal(fill, "position_before", position)
            _equal(fill, "opened_today_before", today)
            _basis(fill, "basis_before", basis)
            _exact_basis(fill, "basis_before", basis, "basis_before_price_scale")
            _equal(fill, "pnl_rounding_policy", "half_up_per_event")
            realized = 0
            if effect == "open":
                if (
                    is_forced
                    or held_contract not in {None, leg_contract}
                    or (position and position * direction < 0)
                ):
                    raise EvidenceContractError("日频期货开仓绕过旧仓或方向限制")
                basis = (
                    Fraction(price)
                    if position == 0
                    else (basis * abs(position) + Fraction(price) * quantity)
                    / (abs(position) + quantity)
                )
                today += quantity
                held_contract = leg_contract
            elif effect in {"close", "close_today", "close_yesterday"}:
                if (
                    held_contract != leg_contract
                    or position * direction >= 0
                    or quantity > abs(position)
                ):
                    raise EvidenceContractError("日频期货平仓合约、方向或数量非法")
                if effect == "close_today":
                    consume_today, consume_yesterday = quantity, 0
                elif effect == "close_yesterday":
                    consume_today, consume_yesterday = 0, quantity
                else:
                    consume_yesterday = min(quantity, yesterday)
                    consume_today = quantity - consume_yesterday
                if consume_today > today or consume_yesterday > yesterday:
                    raise EvidenceContractError("日频期货平今／平昨超出对应仓位桶")
                realized = _fen(
                    (Fraction(price) - basis)
                    * (1 if position > 0 else -1)
                    * quantity
                    * leg_multiplier
                )
                today -= consume_today
                yesterday -= consume_yesterday
            else:
                raise EvidenceContractError("日频期货持仓效果未支持")
            position += direction * quantity
            if position == 0:
                basis = None
                held_contract = None
            fee_field = (
                "open_fee_permyriad"
                if effect == "open"
                else (
                    "close_today_fee_permyriad"
                    if effect == "close_today"
                    else "close_fee_permyriad"
                )
            )
            fee_rate = _decimal(_field(rule, fee_field), fee_field)
            fee_cny = (
                fee_rate * quantity
                if _field(rule, "fee_unit") == "per_lot_cny"
                else (price * leg_multiplier * quantity * fee_rate / 10000)
            )
            fee = _fen(fee_cny)
            _equal(fill, "fee_fen", fee)
            _equal(fill, "realized_pnl_fen", realized)
            _equal(fill, "position_after", position)
            _equal(fill, "opened_today_after", today)
            _basis(fill, "basis_after", basis)
            if basis is None:
                _exact_basis(fill, "basis_after", None, "basis_after_price_scale")
            else:
                _exact_basis(fill, "basis_after", basis, "price_scale")
            equity += realized - fee
            total_fees += fee
            total_realized += realized

        prior_time: datetime | None = None
        for fill in normal:
            fill_time = _time(_field(fill, "fill_time"))
            if prior_time is not None and fill_time < prior_time:
                raise EvidenceContractError("日频期货成交时间与顺序不一致")
            apply_fill(fill, is_forced=False)
            prior_time = fill_time
        if held_contract not in {None, contract}:
            raise EvidenceContractError("日频期货持仓缺少所属合约结算事实")
        _basis(settlement, "basis_before_settlement", basis)
        _exact_basis(
            settlement,
            "basis_before_settlement",
            basis,
            "basis_before_settlement_price_scale",
        )
        mtm = (
            0
            if position == 0
            else _fen((Fraction(settle_price) - basis) * position * multiplier)
        )
        equity += mtm
        total_mtm += mtm
        basis = Fraction(settle_price) if position else None
        required_margin = _fen(settle_price * multiplier * abs(position) * rate / 100)
        _equal(settlement, "mtm_pnl_fen", mtm)
        _equal(settlement, "position_before_settlement", position)
        _equal(settlement, "opened_today_before_liquidation", today)
        _equal(settlement, "equity_before_liquidation_fen", equity)
        _equal(settlement, "required_margin_before_liquidation_fen", required_margin)
        needs_liquidation = required_margin > equity and position != 0
        _equal(settlement, "forced_liquidation", needs_liquidation)
        expected_legs = []
        if needs_liquidation:
            if yesterday:
                expected_legs.append(("close_yesterday", yesterday))
            if today:
                expected_legs.append(("close_today", today))
        observed_legs = [
            (_field(row, "position_effect"), _field(row, "quantity")) for row in forced
        ]
        if observed_legs != expected_legs:
            raise EvidenceContractError("日频期货强平腿与强平前需求、今昨仓不一致")
        fees_before_forced = total_fees
        for fill in forced:
            apply_fill(fill, is_forced=True)
        _equal(
            settlement, "forced_liquidation_fee_fen", total_fees - fees_before_forced
        )
        expected_reason = (
            "required_margin_exceeds_equity" if needs_liquidation else None
        )
        if (
            "forced_liquidation_reason" not in settlement
            or settlement["forced_liquidation_reason"] != expected_reason
        ):
            raise EvidenceContractError("日频期货强平触发原因不一致")
        final_margin = 0 if needs_liquidation else required_margin
        if equity < 0 or final_margin > equity:
            raise EvidenceContractError("日频期货批次结束权益或保证金不合法")
        for name, expected in {
            "position": position,
            "position_opened_today": today,
            "required_margin_fen": final_margin,
            "equity_fen": equity,
            "free_equity_fen": equity - final_margin,
        }.items():
            _equal(settlement, name, expected)
        _basis(settlement, "basis_after_settlement", basis)
        for name, expected in {
            "position": position,
            "nav_fen": equity,
            "margin_fen": final_margin,
            "free_equity_fen": equity - final_margin,
        }.items():
            _equal(rows["nav"][0], name, expected)
        forced_sessions += int(needs_liquidation)
        last_settlements[contract] = settle_price
        sessions_verified += 1
        fills_verified += len(fill_ids)
        previous_session_time = settlement_time
        for name, head in list(heads.items()):
            if head is not None and head[0] == session:
                heads[name] = next(streams[name], None)
    if not sessions_verified or any(head is not None for head in heads.values()):
        raise EvidenceContractError("日频期货结果为空或存在没有对应结算的会话")
    return {
        "sessions_verified": sessions_verified,
        "fills_verified": fills_verified,
        "forced_liquidation_sessions": forced_sessions,
        "total_fee_fen": total_fees,
        "total_realized_pnl_fen": total_realized,
        "total_mtm_pnl_fen": total_mtm,
        "final_equity_fen": equity,
        "final_position": position,
        "final_today_quantity": today,
        "final_yesterday_quantity": yesterday,
    }
