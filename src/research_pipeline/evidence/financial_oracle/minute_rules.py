"""分钟现货与期货交易规则、价格限制、保证金和结算独立复核。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Mapping, Sequence

from research_pipeline.domain.minute_rule_snapshots import MinuteRuleSnapshotBundle
from research_pipeline.domain.session_calendar import (
    SessionCalendarResolver,
    SessionPolicyBundle,
)
from research_pipeline.platform import typed_canonical_hash

from ..errors import EvidenceContractError
from ..oracle_workspace import OracleTable, quoted_identifier
from .common import (
    aware_datetime as _aware_datetime,
    ceil_ratio as _ceil_ratio,
    date_value as _date_value,
    integer as _integer,
    normalized as _normalized,
    ordered_rows as _ordered_rows,
    require_no_rows as _require_no_external_rows,
    require_unique as _unique,
)


class MinuteIndexedRows(Mapping):
    """正式分钟事实的关系索引；仅当前取出的行进入 Python。"""

    def __init__(self, rows: OracleTable, keys: tuple[str, ...], label: str) -> None:
        self.rows = rows.workspace.minute_table(rows, keys)
        self.keys_fields = keys
        _unique(self.rows, keys, label)

    def __len__(self):
        return len(self.rows)

    def __iter__(self):
        for row in self.rows:
            values = tuple(row[key] for key in self.keys_fields)
            yield values[0] if len(values) == 1 else values

    def __getitem__(self, key):
        values = (key,) if len(self.keys_fields) == 1 else tuple(key)
        found = _minute_filtered_rows(self.rows, dict(zip(self.keys_fields, values, strict=True)))
        row = next(found, None)
        if row is None:
            raise KeyError(key)
        return row

    def items(self):
        for row in self.rows:
            values = tuple(row[key] for key in self.keys_fields)
            yield (values[0] if len(values) == 1 else values), row

    def values(self):
        return iter(self.rows)


def minute_index(rows, keys: tuple[str, ...], label: str):
    if isinstance(rows, OracleTable):
        return MinuteIndexedRows(rows, keys, label)
    result = {
        (row[keys[0]] if len(keys) == 1 else tuple(row[key] for key in keys)): row
        for row in rows
    }
    if len(result) != len(rows):
        raise EvidenceContractError(f"{label} 主键重复")
    return result


def _minute_filtered_rows(rows, filters: Mapping[str, object], *, order_by=()):
    if isinstance(rows, OracleTable):
        table = rows.workspace.minute_table(rows, tuple(filters))
        where = " AND ".join(f"{quoted_identifier(key)} = ?" for key in filters)
        order = (
            " ORDER BY " + ", ".join(quoted_identifier(key) for key in order_by)
            if order_by else ""
        )
        yield from table.workspace.iter_query(
            f"SELECT * FROM {table.name} WHERE {where}{order}", tuple(filters.values())
        )
        return
    selected = (
        row for row in rows
        if all(_normalized(row.get(key)) == _normalized(value) for key, value in filters.items())
    )
    if order_by:
        selected = sorted(selected, key=lambda row: tuple(_normalized(row[key]) for key in order_by))
    yield from selected


def minute_single_bar(bars, filters: Mapping[str, object], label: str):
    selected = _minute_filtered_rows(bars, {**filters, "completed": True, "quality_status": "pass"})
    first = next(selected, None)
    if first is None or next(selected, None) is not None:
        raise EvidenceContractError(label)
    return first


def _minute_execution_bar(bars, order):
    return minute_single_bar(bars, {
        "instrument_id": str(order["instrument_id"]),
        "bar_start": _normalized(order["submitted_at"]),
    }, "分钟 order 无法回指唯一执行 bar")


def _minute_order_rules(order, bundle, rule_ids):
    return {
        name: _visible_minute_rule(
            bundle, rule_id, str(order["instrument_id"]),
            _date_value(order["session"], "order.session"),
            _aware_datetime(order["submitted_at"], "order.submitted_at"),
        )
        for name, rule_id in rule_ids.items()
    }


def minute_decode_settlement(row):
    try:
        event = json.loads(str(row["event_json"]))
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError("分钟期货结算事件列式载荷无效") from exc
    return {**{key: value for key, value in row.items() if key not in {"event_json", "event_session"}}, "event": event}


class MinuteSettlementRows(Sequence):
    def __init__(self, rows: OracleTable):
        workspace = rows.workspace
        name = "minute_settlement_sessions"
        workspace.execute(
            f"CREATE VIEW {name} AS SELECT *, "
            f"CAST(json_extract_string(event_json, '$.session') AS DATE) AS event_session FROM {rows.name}"
        )
        self.rows = workspace.minute_table(OracleTable(workspace, name, len(rows)), ("event_session",))

    def __len__(self):
        return len(self.rows)

    def __iter__(self):
        for row in self.rows:
            yield minute_decode_settlement(row)

    def __getitem__(self, index):
        return minute_decode_settlement(self.rows[index])

    def for_session(self, session):
        for row in _minute_filtered_rows(self.rows, {"event_session": session}, order_by=("instrument_id",)):
            yield minute_decode_settlement(row)


def _minute_events_for_session(events, session):
    if isinstance(events, MinuteSettlementRows):
        yield from events.for_session(session)
    else:
        yield from (
            event for event in events
            if _date_value(event["event"].get("session"), "settlement.session") == session
        )


def verify_minute_execution_rules(
    *,
    asset_class: str,
    canonical: Mapping[str, list[dict[str, object]]],
    bars: list[Mapping[str, object]],
    rule_bundle: MinuteRuleSnapshotBundle,
    policy: Mapping[str, object],
    settlement_events: object,
    session_bundle: SessionPolicyBundle,
) -> None:
    """独立消费分钟规则，复算现货成交资格、费用和 T+1。"""

    if asset_class == "cn_future":
        _verify_minute_futures_execution_rules(
            canonical=canonical,
            bars=bars,
            rule_bundle=rule_bundle,
            policy=policy,
            settlement_events=settlement_events,
            session_bundle=session_bundle,
        )
        return
    if asset_class not in {"cn_stock", "cn_etf"}:
        raise EvidenceContractError("分钟金融上下文资产类别不受支持")
    if len(settlement_events) != 0:
        raise EvidenceContractError("分钟现货金融上下文不得声明期货结算事件")
    price_rule_id = (
        "rule.cn_stock.price_limit.v1"
        if asset_class == "cn_stock"
        else "rule.cn_fund.price_limit.v1"
    )
    lot_rule_id = (
        "rule.cn_stock.lot_size.v1"
        if asset_class == "cn_stock"
        else "rule.cn_fund.lot_size.v1"
    )
    settlement_rule_id = (
        "rule.cn_stock.settlement.v1"
        if asset_class == "cn_stock"
        else "rule.cn_fund.settlement.v1"
    )
    fee_rule_id = (
        "rule.cn_stock.trading_fee.v1"
        if asset_class == "cn_stock"
        else "rule.cn_fund.trading_fee.v1"
    )
    lifecycle_rule_id = (
        "rule.cn_stock.instrument_lifecycle.v1"
        if asset_class == "cn_stock"
        else "rule.cn_fund.instrument_lifecycle.v1"
    )
    cap_ppm = _integer(
        policy.get("participation_cap_ppm"),
        "分钟 participation_cap_ppm",
        minimum=1,
    )
    if cap_ppm > 1_000_000:
        raise EvidenceContractError("分钟 participation cap 超出支持范围")
    orders = minute_index(canonical["orders"], ("order_id",), "分钟订单")
    order_rule_ids = {
        "price": price_rule_id, "lot": lot_rule_id,
        "settlement": settlement_rule_id, "fee": fee_rule_id,
        "lifecycle": lifecycle_rule_id,
    }
    for order_id, order in orders.items():
        submitted = _aware_datetime(order["submitted_at"], "order.submitted_at")
        bar = _minute_execution_bar(bars, order)
        session = _date_value(order["session"], "order.session")
        if str(bar.get("trading_date")) != session.isoformat():
            raise EvidenceContractError("分钟 order 会话与执行 bar 不一致")
        selected = {
            "price": _visible_minute_rule(
                rule_bundle, price_rule_id, str(order["instrument_id"]),
                session, submitted,
            ),
            "lot": _visible_minute_rule(
                rule_bundle, lot_rule_id, str(order["instrument_id"]),
                session, submitted,
            ),
            "settlement": _visible_minute_rule(
                rule_bundle, settlement_rule_id, str(order["instrument_id"]),
                session, submitted,
            ),
            "fee": _visible_minute_rule(
                rule_bundle, fee_rule_id, str(order["instrument_id"]),
                session, submitted,
            ),
            "lifecycle": _visible_minute_rule(
                rule_bundle, lifecycle_rule_id, str(order["instrument_id"]),
                session, submitted,
            ),
        }
        _verify_minute_price_rule(selected["price"], bar=bar, submitted=submitted)
        lifecycle = dict(selected["lifecycle"].parameters)
        if asset_class == "cn_etf":
            if lifecycle.get("product_class") != "equity_etf":
                raise EvidenceContractError("分钟 ETF 产品类别规则无效")
            listed = _date_value(lifecycle.get("listed_date"), "listed_date")
            delisted = _date_value(lifecycle.get("delisted_date"), "delisted_date")
            if not listed <= session <= delisted:
                raise EvidenceContractError("分钟 ETF 成交日不在生命周期内")
            if dict(selected["fee"].parameters).get("cost_model_scope") != (
                "research_assumption"
            ):
                raise EvidenceContractError("分钟 ETF 费用未声明为研究成本假设")
    bought_total: dict[str, int] = {}
    bought_today: dict[str, int] = {}
    active_session = None
    active_fill_time = None
    sold_total: dict[str, int] = {}
    used_capacity_by_bar: dict[str, int] = {}
    fills = canonical["fills"]
    for fill in _ordered_rows(
        fills,
        order_by=("session", "fill_time", "fill_id"),
    ):
        order_id = str(fill["order_id"])
        order = orders.get(order_id)
        if order is None:
            raise EvidenceContractError("分钟 fill 引用未知规则绑定 order")
        bar = _minute_execution_bar(bars, order)
        selected = _minute_order_rules(order, rule_bundle, order_rule_ids)
        if _normalized(fill["fill_time"]) != _normalized(bar.get("available_time")):
            raise EvidenceContractError("分钟 fill 时间与执行 bar 不一致")
        expected_price = (
            bar.get("avg_units")
            if bar.get("avg_units") is not None
            else bar.get("close_units")
        )
        if int(fill["execution_price_units"]) != int(expected_price):
            raise EvidenceContractError("分钟 fill 价格不是执行 bar 的正式成交价")
        if int(fill["price_scale"]) != int(
            dict(selected["price"].parameters).get("price_scale", -1)
        ):
            raise EvidenceContractError("分钟 fill 价格精度与规则不一致")
        quantity = _integer(fill["quantity"], "fill.quantity", minimum=1)
        capacity = _integer(bar.get("volume"), "执行 bar volume", minimum=1)
        bar_hash = str(bar.get("bar_hash"))
        if active_fill_time != fill["fill_time"]:
            used_capacity_by_bar.clear()
            active_fill_time = fill["fill_time"]
        used_capacity = used_capacity_by_bar.get(bar_hash, 0) + quantity
        if used_capacity * 1_000_000 > capacity * cap_ppm:
            raise EvidenceContractError("分钟 fill 超过已完成执行 bar 的参与率上限")
        used_capacity_by_bar[bar_hash] = used_capacity
        lot_parameters = dict(selected["lot"].parameters)
        lot_key = "buy_lot_shares" if asset_class == "cn_stock" else "buy_lot_units"
        lot_size = _integer(lot_parameters.get(lot_key), lot_key, minimum=1)
        if str(fill["side"]) == "buy" and quantity % lot_size:
            raise EvidenceContractError("分钟买入 fill 不符合交易单位")
        fee_parameters = dict(selected["fee"].parameters)
        notional = _integer(fill["notional_units"], "fill.notional_units", minimum=1)
        commission = max(
            _integer(fee_parameters.get("min_commission_units"), "min_commission_units", minimum=0),
            _ceil_ratio(
                notional
                * _integer(fee_parameters.get("commission_ppm"), "commission_ppm", minimum=0),
                1_000_000,
            ),
        )
        transfer = _ceil_ratio(
            notional
            * _integer(fee_parameters.get("transfer_fee_ppm"), "transfer_fee_ppm", minimum=0),
            1_000_000,
        )
        tax = (
            _ceil_ratio(
                notional
                * _integer(fee_parameters.get("sell_tax_ppm"), "sell_tax_ppm", minimum=0),
                1_000_000,
            )
            if str(fill["side"]) == "sell"
            else 0
        )
        if int(fill["fee_units"]) != commission + transfer + tax:
            raise EvidenceContractError("分钟 fill 费用与当时有效规则不一致")
        instrument_id = str(fill["instrument_id"])
        session = _date_value(fill["session"], "fill.session")
        if active_session != session:
            bought_today.clear()
            active_session = session
        settlement_days = _integer(
            dict(selected["settlement"].parameters).get("settlement_days"),
            "settlement_days",
            minimum=0,
        )
        if settlement_days not in {0, 1}:
            raise EvidenceContractError("分钟现货结算天数不受支持")
        if str(fill["side"]) == "buy":
            bought_total[instrument_id] = bought_total.get(instrument_id, 0) + quantity
            bought_today[instrument_id] = bought_today.get(instrument_id, 0) + quantity
        else:
            eligible = bought_total.get(instrument_id, 0) - sold_total.get(instrument_id, 0)
            if settlement_days == 1:
                eligible -= bought_today.get(instrument_id, 0)
            if quantity > eligible:
                raise EvidenceContractError("分钟卖出 fill 绕过 T+1 可卖数量")
            sold_total[instrument_id] = sold_total.get(instrument_id, 0) + quantity
    _verify_minute_spot_position_buckets(
        positions=canonical["positions"],
        fills=fills,
        rule_bundle=rule_bundle,
        settlement_rule_id=settlement_rule_id,
    )


def _verify_minute_futures_execution_rules(
    *,
    canonical: Mapping[str, list[dict[str, object]]],
    bars: list[Mapping[str, object]],
    rule_bundle: MinuteRuleSnapshotBundle,
    policy: Mapping[str, object],
    settlement_events: object,
    session_bundle: SessionPolicyBundle,
) -> None:
    """从 ResultStore 直接输入独立复算期货费率、保证金和逐日结算。"""

    if not isinstance(settlement_events, Sequence) or any(
        not isinstance(item, Mapping) for item in settlement_events
    ):
        raise EvidenceContractError("分钟期货结算事件载荷无效")
    cap_ppm = _integer(
        policy.get("participation_cap_ppm"),
        "分钟 participation_cap_ppm",
        minimum=1,
    )
    if cap_ppm > 1_000_000:
        raise EvidenceContractError("分钟 participation cap 超出支持范围")
    metadata_by_id = {item.instrument_id: item for item in rule_bundle.instruments}
    session_resolver = SessionCalendarResolver(session_bundle)
    orders = minute_index(canonical["orders"], ("order_id",), "分钟期货订单")
    order_rule_ids = {
        "mapping": "rule.cn_futures.actual_contract_mapping.v1",
        "lifecycle": "rule.cn_futures.contract_lifecycle.v1",
        "multiplier": "rule.cn_futures.contract_multiplier.v1",
        "fee": "rule.cn_futures.fee_schedule.v1",
        "margin": "rule.cn_futures.margin.v1",
        "price": "rule.cn_futures.price_limit.v1",
        "tick": "rule.cn_futures.price_tick.v1",
        "session": "rule.cn_futures.session.v1",
    }
    for order_id, order in orders.items():
        instrument_id = str(order["instrument_id"])
        submitted = _aware_datetime(order["submitted_at"], "order.submitted_at")
        session_date = _date_value(order["session"], "order.session")
        bar = _minute_execution_bar(bars, order)
        if str(bar.get("trading_date")) != session_date.isoformat():
            raise EvidenceContractError("分钟期货 order 会话与执行 bar 不一致")
        selected = {
            name: _visible_minute_rule(
                rule_bundle, rule_id, instrument_id, session_date, submitted
            )
            for name, rule_id in order_rule_ids.items()
        }
        _verify_minute_price_rule(selected["price"], bar=bar, submitted=submitted)
        parameters = {
            key: value
            for rule in selected.values()
            for key, value in rule.parameters
        }
        if parameters.get("actual_contract_id") != instrument_id:
            raise EvidenceContractError("分钟期货连续合约进入了正式成交")
        listed = _date_value(parameters.get("listed_date"), "listed_date")
        last_trade = _date_value(parameters.get("last_trade_date"), "last_trade_date")
        if not listed <= session_date <= last_trade:
            raise EvidenceContractError("分钟期货成交日不在合约生命周期内")
        tick = _integer(parameters.get("price_tick_units"), "price_tick_units", minimum=1)
        execution_price = int(
            bar.get("avg_units")
            if bar.get("avg_units") is not None else bar.get("close_units")
        )
        if execution_price % tick:
            raise EvidenceContractError("分钟期货成交价不符合最小变动价位")
        if parameters.get("margin_account_role") != "speculative":
            raise EvidenceContractError("分钟期货保证金账户角色不受支持")
        margin_ppm = _integer(
            parameters.get("speculative_initial_margin_ppm"),
            "speculative_initial_margin_ppm",
            minimum=1,
        )
        _integer(parameters.get("hedge_margin_ppm"), "hedge_margin_ppm", minimum=1)
        if margin_ppm > 1_000_000:
            raise EvidenceContractError("分钟期货保证金率超出支持范围")
        if parameters.get("fee_unit") != "notional_permyriad" or (
            parameters.get("opening_charge_null_semantics")
            != "use_common_clearance_charge"
        ):
            raise EvidenceContractError("分钟期货手续费口径不受支持")
        close_rate = _integer(
            parameters.get("close_fee_ppm"), "close_fee_ppm", minimum=0
        )
        close_today_rate = _integer(
            parameters.get("close_today_fee_ppm"),
            "close_today_fee_ppm",
            minimum=0,
        )
        if close_rate != close_today_rate:
            raise EvidenceContractError("分钟期货平今费率未按首版合同失败关闭")
        metadata = metadata_by_id.get(instrument_id)
        if metadata is None:
            raise EvidenceContractError("分钟期货缺少 session classification")
        revision = _integer(
            parameters.get("session_policy_revision"),
            "session_policy_revision",
            minimum=1,
        )
        trading_session = session_resolver.resolve_trading_date(
            metadata, session_date, policy_revision=revision
        )
        if (
            trading_session.calendar_policy_id
            != parameters.get("session_policy_id")
            or str(bar.get("session_id")) != trading_session.session_id
            or not any(
                item.contains_completed_bar_end(
                    _aware_datetime(bar.get("bar_end"), "bar.bar_end")
                )
                for item in trading_session.segments
            )
        ):
            raise EvidenceContractError("分钟期货执行 bar 与 session policy 不一致")
    used_capacity_by_bar: dict[str, int] = {}
    active_fill_time = None
    for fill in _ordered_rows(canonical["fills"], order_by=("session", "fill_time", "fill_id")):
        order_id = str(fill["order_id"])
        order = orders.get(order_id)
        if order is None:
            raise EvidenceContractError("分钟期货 fill 引用未知规则绑定 order")
        bar = _minute_execution_bar(bars, order)
        selected = _minute_order_rules(order, rule_bundle, order_rule_ids)
        if _normalized(fill["fill_time"]) != _normalized(bar.get("available_time")):
            raise EvidenceContractError("分钟期货 fill 时间与执行 bar 不一致")
        expected_price = (
            bar.get("avg_units")
            if bar.get("avg_units") is not None else bar.get("close_units")
        )
        if int(fill["execution_price_units"]) != int(expected_price):
            raise EvidenceContractError("分钟期货 fill 价格不是执行 bar 正式成交价")
        parameters = {
            key: value
            for rule in selected.values()
            for key, value in rule.parameters
        }
        multiplier = _integer(
            parameters.get("contract_unit_kg"), "contract_unit_kg", minimum=1
        )
        if int(fill["contract_multiplier"]) != multiplier:
            raise EvidenceContractError("分钟期货 fill 合约乘数与规则不一致")
        quantity = _integer(fill["quantity"], "fill.quantity", minimum=1)
        bar_hash = str(bar.get("bar_hash"))
        if active_fill_time != fill["fill_time"]:
            used_capacity_by_bar.clear()
            active_fill_time = fill["fill_time"]
        used = used_capacity_by_bar.get(bar_hash, 0) + quantity
        volume_limit = (
            _integer(bar.get("volume"), "执行 bar volume", minimum=0)
            * cap_ppm // 1_000_000
        )
        open_interest = _integer(
            bar.get("open_interest"), "执行 bar open_interest", minimum=0
        )
        if used > min(volume_limit, open_interest):
            raise EvidenceContractError("分钟期货 fill 超过可见容量或持仓量上限")
        used_capacity_by_bar[bar_hash] = used
        position_effect = str(fill["position_effect"])
        if position_effect not in {"open", "close"}:
            raise EvidenceContractError("分钟期货 position_effect 无效")
        rate = _integer(
            parameters.get(
                "open_fee_ppm" if position_effect == "open" else "close_fee_ppm"
            ),
            "期货 fee_ppm",
            minimum=0,
        )
        expected_fee = _ceil_ratio(int(fill["notional_units"]) * rate, 1_000_000)
        if int(fill["fee_units"]) != expected_fee:
            raise EvidenceContractError("分钟期货 fill 费用与当时有效规则不一致")

    cash_by_session = minute_index(canonical["cash"], ("session",), "分钟期货现金会话")
    for declared in settlement_events:
        event = declared.get("event")
        if not isinstance(event, Mapping):
            raise EvidenceContractError("分钟期货结算事件缺少完整 FinancialEvent")
        session_date = _date_value(event.get("session"), "settlement.session")
        if session_date not in cash_by_session:
            raise EvidenceContractError("分钟期货结算事件引用未知现金快照会话")
    positions: dict[str, tuple[int, int]] = {}
    instrument_hashes = {
        str(row["instrument_id"]): str(row["instrument_hash"])
        for row in canonical["orders"]
    }
    settlement_rule_ids = (
        "rule.cn_futures.contract_multiplier.v1",
        "rule.cn_futures.margin.v1",
        "rule.cn_futures.price_tick.v1",
        "rule.cn_futures.session.v1",
        "rule.cn_futures.settlement.v1",
    )
    opening_cash_values = {
        _integer(row["opening_cash_units"], "cash.opening_cash_units", minimum=1)
        for row in canonical["cash"]
    }
    if len(opening_cash_values) > 1:
        raise EvidenceContractError("分钟期货初始权益身份漂移")
    running_equity = next(iter(opening_cash_values), 0)
    running_margin = 0
    for cash in _ordered_rows(canonical["cash"], order_by=("session",)):
        session_date = _date_value(cash["session"], "cash.session")
        for fill in _minute_filtered_rows(
            canonical["fills"], {"session": session_date}, order_by=("fill_time", "fill_id"),
        ):
            instrument_id = str(fill["instrument_id"])
            price = int(fill["execution_price_units"])
            old_quantity, old_basis = positions.get(instrument_id, (0, price))
            quantity = int(fill["quantity"])
            side = str(fill["side"])
            direction = 1 if side == "buy" else -1
            expected_sign = 1 if side == "sell" else -1
            if str(fill["position_effect"]) == "close":
                if old_quantity * expected_sign <= 0 or quantity > abs(old_quantity):
                    raise EvidenceContractError("分钟期货平仓与当时持仓不一致")
                realized = (
                    (price - old_basis) * quantity
                    * int(fill["contract_multiplier"]) * expected_sign
                )
            else:
                if old_quantity and old_quantity * direction < 0:
                    raise EvidenceContractError("分钟期货反向开仓未先平仓")
                realized = 0
            if int(fill["realized_pnl_units"]) != realized:
                raise EvidenceContractError("分钟期货 fill 已实现盈亏复算不一致")
            order = orders.get(str(fill["order_id"]))
            if order is None:
                raise EvidenceContractError("分钟期货 fill 缺少订单时点规则")
            selected = _minute_order_rules(order, rule_bundle, order_rule_ids)
            parameters = {
                key: value
                for rule in selected.values()
                for key, value in rule.parameters
            }
            margin_ppm = _integer(
                parameters.get("speculative_initial_margin_ppm"),
                "speculative_initial_margin_ppm",
                minimum=1,
            )
            multiplier = int(fill["contract_multiplier"])
            new_quantity = old_quantity + direction * quantity
            old_margin = _ceil_ratio(
                old_basis * multiplier * abs(old_quantity) * margin_ppm,
                1_000_000,
            )
            new_margin = _ceil_ratio(
                price * multiplier * abs(new_quantity) * margin_ppm,
                1_000_000,
            )
            next_margin = running_margin - old_margin + new_margin
            next_equity = (
                running_equity + realized - int(fill["fee_units"])
            )
            if next_margin < 0 or next_margin > next_equity:
                raise EvidenceContractError("分钟期货 fill 绕过盘中投机保证金约束")
            running_margin = next_margin
            running_equity = next_equity
            positions[instrument_id] = (
                new_quantity,
                price,
            )

        instrument_ids = {
            instrument_id
            for instrument_id, (quantity, _basis) in positions.items()
            if quantity != 0
        }
        if not instrument_ids:
            cash = cash_by_session[session_date]
            if (
                next(_minute_events_for_session(settlement_events, session_date), None) is not None
                or int(cash["non_trade_cash_change_units"]) != 0
                or int(cash["margin_units"]) != 0
                or running_margin != 0
                or int(cash["total_cash_units"]) != running_equity
                or str(cash["non_trade_source_hash"]) != typed_canonical_hash([])
            ):
                raise EvidenceContractError("分钟期货空仓会话包含伪结算或保证金")
            continue
        expected_facts = []
        for instrument_id in sorted(instrument_ids):
            settlement_rule = _visible_minute_rule(
                rule_bundle,
                "rule.cn_futures.settlement.v1",
                instrument_id,
                session_date,
                datetime.max.replace(tzinfo=_aware_datetime(
                    next(iter(bars))["available_time"], "bar.available_time"
                ).tzinfo),
            )
            settlement_time = settlement_rule.available_at
            assert settlement_time is not None
            rules = tuple(
                _visible_minute_rule(
                    rule_bundle, rule_id, instrument_id,
                    session_date, settlement_time,
                )
                for rule_id in settlement_rule_ids
            )
            parameters = {
                key: value for rule in rules for key, value in rule.parameters
            }
            metadata = metadata_by_id.get(instrument_id)
            if metadata is None:
                raise EvidenceContractError("分钟期货结算缺少 session classification")
            trading_session = session_resolver.resolve_trading_date(
                metadata,
                session_date,
                policy_revision=_integer(
                    parameters.get("session_policy_revision"),
                    "session_policy_revision",
                    minimum=1,
                ),
            )
            close_time = max(
                item.ends_at
                for item in trading_session.segments
                if item.bar_eligible is True
            )
            minute_single_bar(bars, {
                "instrument_id": instrument_id,
                "trading_date": session_date,
                "session_id": trading_session.session_id,
                "bar_end": close_time,
            }, "分钟期货结算缺少完整 session 收盘触发")
            if settlement_time < close_time:
                raise EvidenceContractError("分钟期货结算缺少完整 session 收盘触发")
            settlement_price = _integer(
                parameters.get("settlement_price_units"),
                "settlement_price_units",
                minimum=1,
            )
            multiplier = _integer(
                parameters.get("contract_unit_kg"),
                "contract_unit_kg",
                minimum=1,
            )
            margin_ppm = _integer(
                parameters.get("speculative_initial_margin_ppm"),
                "speculative_initial_margin_ppm",
                minimum=1,
            )
            quantity, basis = positions.get(
                instrument_id, (0, settlement_price)
            )
            pnl = (settlement_price - basis) * quantity * multiplier
            margin = _ceil_ratio(
                settlement_price * multiplier * abs(quantity) * margin_ppm,
                1_000_000,
            )
            rule_hash = _minute_rules_identity(rule_bundle, rules)
            expected_facts.append({
                "instrument_id": instrument_id,
                "instrument_hash": instrument_hashes.get(instrument_id),
                "settlement_time": settlement_time,
                "settlement_price_units": settlement_price,
                "price_scale": _integer(
                    parameters.get("price_scale"), "price_scale", minimum=0
                ),
                "position_contracts_before": quantity,
                "previous_settlement_price_units": basis,
                "contract_multiplier": multiplier,
                "speculative_margin_ppm": margin_ppm,
                "pnl_units": pnl,
                "required_margin_units": margin,
                "rule_hash": rule_hash,
                "rule_snapshot_hashes": [item.snapshot_hash for item in rules],
            })
        if len({item["settlement_time"] for item in expected_facts}) != 1:
            raise EvidenceContractError("分钟期货同一账户结算时点不一致")
        aggregate_margin = sum(
            int(item["required_margin_units"]) for item in expected_facts
        )
        expected_events = []
        for fact in expected_facts:
            event = {
                "event_id": (
                    f"minute-settlement:{fact['instrument_id']}:"
                    f"{session_date.isoformat()}"
                ),
                "kind": "mark_to_market",
                "effective_time": fact["settlement_time"].isoformat(),
                "session": session_date.isoformat(),
                "group_id": "minute-default-cn-futures",
                "rule_hash": fact["rule_hash"],
                "payload": [
                    ["pnl_units", int(fact["pnl_units"])],
                    ["required_margin_units", aggregate_margin],
                ],
                "parent_id": None,
            }
            expected_events.append({
                **fact,
                "settlement_time": fact["settlement_time"].isoformat(),
                "aggregate_required_margin_units": aggregate_margin,
                "event": event,
                "event_hash": typed_canonical_hash(event),
            })
            if fact["instrument_id"] in positions:
                positions[str(fact["instrument_id"])] = (
                    int(fact["position_contracts_before"]),
                    int(fact["settlement_price_units"]),
                )
        declared = sorted(
            _minute_events_for_session(settlement_events, session_date),
            key=lambda item: str(item.get("instrument_id")),
        )
        normalized_expected = [
            {key: _normalized(value) for key, value in item.items()}
            for item in expected_events
        ]
        normalized_declared = [
            {str(key): _normalized(value) for key, value in item.items()}
            for item in declared
        ]
        if normalized_declared != normalized_expected:
            raise EvidenceContractError("分钟期货结算事件与独立复算不一致")
        event_hashes = [str(item["event_hash"]) for item in expected_events]
        cash = cash_by_session[session_date]
        running_equity += sum(
            int(item["pnl_units"]) for item in expected_events
        )
        running_margin = aggregate_margin
        if (
            int(cash["non_trade_cash_change_units"])
            != sum(int(item["pnl_units"]) for item in expected_events)
            or int(cash["margin_units"]) != aggregate_margin
            or int(cash["total_cash_units"]) != running_equity
            or str(cash["non_trade_source_hash"])
            != typed_canonical_hash(sorted(event_hashes))
            or _normalized(cash["valuation_time"])
            != _normalized(expected_facts[0]["settlement_time"])
        ):
            raise EvidenceContractError("分钟期货现金、保证金或结算来源不闭合")


def _minute_rules_identity(
    bundle: MinuteRuleSnapshotBundle,
    rules: tuple[object, ...],
) -> str:
    source_hashes = {item.source_id: item.source_hash for item in bundle.sources}
    bindings = []
    for rule in rules:
        identity = typed_canonical_hash({
            "snapshot_hash": rule.snapshot_hash,
            "source_hashes": {
                source_id: source_hashes[source_id]
                for source_id in rule.source_ids
            },
        })
        bindings.append(identity)
    return typed_canonical_hash({
        "bundle_hash": bundle.bundle_hash,
        "bindings": bindings,
    })


def _verify_minute_spot_position_buckets(
    *,
    positions: list[dict[str, object]],
    fills: list[dict[str, object]],
    rule_bundle: MinuteRuleSnapshotBundle,
    settlement_rule_id: str,
) -> None:
    """按成交日重建分钟现货的可卖、未结算和冻结数量。"""

    if isinstance(positions, OracleTable) and isinstance(fills, OracleTable):
        workspace = positions.workspace
        _unique(positions, ("instrument_id", "session"), "分钟现货持仓快照")
        _require_no_external_rows(
            workspace,
            f"SELECT instrument_id, session FROM {fills.name} EXCEPT "
            f"SELECT instrument_id, session FROM {positions.name}",
            "分钟现货 fill 缺少对应持仓快照",
        )
        rows = workspace.iter_query(f"""
            WITH daily_fills AS (
                SELECT instrument_id, session,
                    sum(CASE WHEN side = 'buy' THEN quantity ELSE 0 END) AS buys,
                    sum(CASE WHEN side = 'sell' THEN quantity ELSE 0 END) AS sells
                FROM {fills.name} GROUP BY instrument_id, session
            ), matched AS (
                SELECT p.*, coalesce(f.buys, 0) AS buys, coalesce(f.sells, 0) AS sells
                FROM {positions.name} p LEFT JOIN daily_fills f USING (instrument_id, session)
            )
            SELECT *, sum(buys - sells) OVER (
                PARTITION BY instrument_id ORDER BY session ROWS UNBOUNDED PRECEDING
            ) AS closing_quantity FROM matched ORDER BY instrument_id, session
        """)
        for position in rows:
            rule = _visible_minute_rule(
                rule_bundle, settlement_rule_id, str(position["instrument_id"]),
                _date_value(position["session"], "position.session"),
                _aware_datetime(position["valuation_time"], "position.valuation_time"),
            )
            days = _integer(dict(rule.parameters).get("settlement_days"), "settlement_days", minimum=0)
            if days not in {0, 1}:
                raise EvidenceContractError("分钟现货结算天数不受支持")
            if int(position["non_trade_quantity_change"]) != 0:
                raise EvidenceContractError("当前分钟现货不支持非交易持仓数量变化")
            unsettled = int(position["buys"]) if days == 1 else 0
            if (
                int(position["sellable_quantity"]) != int(position["closing_quantity"]) - unsettled
                or int(position["unsettled_quantity"]) != unsettled
                or int(position["frozen_quantity"]) != 0
            ):
                raise EvidenceContractError("分钟现货持仓 bucket 与结算规则不一致")
        return

    buys_by_session: dict[tuple[str, date], int] = {}
    sells_by_session: dict[tuple[str, date], int] = {}
    for fill in fills:
        key = (
            str(fill["instrument_id"]),
            _date_value(fill["session"], "fill.session"),
        )
        destination = buys_by_session if str(fill["side"]) == "buy" else sells_by_session
        destination[key] = destination.get(key, 0) + int(fill["quantity"])
    fill_sessions = {
        (str(row["instrument_id"]), _date_value(row["session"], "fill.session"))
        for row in fills
    }
    position_sessions = {
        (str(row["instrument_id"]), _date_value(row["session"], "position.session"))
        for row in positions
    }
    if len(position_sessions) != len(positions):
        raise EvidenceContractError("分钟现货同一标的同一交易日持仓快照重复")
    if not fill_sessions <= position_sessions:
        raise EvidenceContractError("分钟现货 fill 缺少对应持仓快照")
    cumulative_buys: dict[str, int] = {}
    cumulative_sells: dict[str, int] = {}
    for position in sorted(
        positions,
        key=lambda row: (
            str(row["instrument_id"]),
            _date_value(row["session"], "position.session"),
        ),
    ):
        instrument_id = str(position["instrument_id"])
        session = _date_value(position["session"], "position.session")
        valuation_time = _aware_datetime(
            position["valuation_time"], "position.valuation_time"
        )
        settlement_rule = _visible_minute_rule(
            rule_bundle,
            settlement_rule_id,
            instrument_id,
            session,
            valuation_time,
        )
        settlement_days = _integer(
            dict(settlement_rule.parameters).get("settlement_days"),
            "settlement_days",
            minimum=0,
        )
        if settlement_days not in {0, 1}:
            raise EvidenceContractError("分钟现货结算天数不受支持")
        if int(position["non_trade_quantity_change"]) != 0:
            raise EvidenceContractError("当前分钟现货不支持非交易持仓数量变化")
        key = instrument_id, session
        current_buys = buys_by_session.get(key, 0)
        cumulative_buys[instrument_id] = (
            cumulative_buys.get(instrument_id, 0) + current_buys
        )
        cumulative_sells[instrument_id] = (
            cumulative_sells.get(instrument_id, 0) + sells_by_session.get(key, 0)
        )
        expected_unsettled = current_buys if settlement_days == 1 else 0
        expected_sellable = (
            cumulative_buys[instrument_id]
            - cumulative_sells[instrument_id]
            - expected_unsettled
        )
        if (
            int(position["sellable_quantity"]) != expected_sellable
            or int(position["unsettled_quantity"]) != expected_unsettled
            or int(position["frozen_quantity"]) != 0
        ):
            raise EvidenceContractError("分钟现货持仓 bucket 与结算规则不一致")


def _visible_minute_rule(
    bundle: MinuteRuleSnapshotBundle,
    rule_id: str,
    instrument_id: str,
    session: date,
    as_of: datetime,
):
    matches = [
        item for item in bundle.rules
        if item.rule_id == rule_id
        and item.instrument_id == instrument_id
        and item.effective_from <= session <= item.effective_to
    ]
    if (
        len(matches) != 1
        or matches[0].status != "supported"
        or matches[0].available_at is None
        or matches[0].available_at > as_of
    ):
        raise EvidenceContractError(f"分钟规则不可唯一且及时使用: {rule_id}")
    return matches[0]


def _verify_minute_price_rule(rule, *, bar: Mapping[str, object], submitted: datetime) -> None:
    parameters = dict(rule.parameters)
    futures = rule.rule_id == "rule.cn_futures.price_limit.v1"
    try:
        reference_available = datetime.fromisoformat(
            str(parameters["reference_price_available_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise EvidenceContractError("分钟价格限制参考时间无效") from exc
    reference = _integer(
        parameters.get(
            "reference_previous_settlement_units"
            if futures else "reference_previous_close_units"
        ),
        (
            "reference_previous_settlement_units"
            if futures else "reference_previous_close_units"
        ),
        minimum=1,
    )
    ratio_ppm = _integer(
        parameters.get("price_limit_ratio_ppm"),
        "price_limit_ratio_ppm",
        minimum=1,
    )
    if reference_available.tzinfo is None or reference_available > submitted:
        raise EvidenceContractError("分钟价格限制参考在提交时尚不可见")
    ratio = Decimal(ratio_ppm) / Decimal(1_000_000)
    if futures:
        tick = _integer(
            parameters.get("price_tick_units"), "price_tick_units", minimum=1
        )
        expected_high = (
            int(Decimal(reference) * (Decimal(1) + ratio)) // tick * tick
        )
        expected_low = (
            int(Decimal(reference) * (Decimal(1) - ratio)) // tick * tick
        )
    else:
        expected_high = int(
            (Decimal(reference) * (Decimal(1) + ratio)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
        expected_low = int(
            (Decimal(reference) * (Decimal(1) - ratio)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
    high = _integer(parameters.get("high_limit_units"), "high_limit_units", minimum=1)
    low = _integer(parameters.get("low_limit_units"), "low_limit_units", minimum=1)
    if (high, low) != (expected_high, expected_low):
        raise EvidenceContractError("分钟价格限制与前收和比例不一致")
    execution_price = int(
        bar.get("avg_units")
        if bar.get("avg_units") is not None
        else bar.get("close_units")
    )
    if not low <= execution_price <= high:
        raise EvidenceContractError("分钟执行价格超出当时有效涨跌停")


__all__ = [
    "MinuteIndexedRows",
    "MinuteSettlementRows",
    "minute_decode_settlement",
    "minute_index",
    "minute_single_bar",
    "verify_minute_execution_rules",
]
