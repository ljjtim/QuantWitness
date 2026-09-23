"""canonical orders、fills、positions、cash、costs 与 valuations 独立复核。"""

from __future__ import annotations

from typing import Mapping, Sequence

from research_pipeline.results import CANONICAL_SIMULATION_SCHEMA_IDS

from ..errors import EvidenceContractError
from ..oracle_workspace import OracleTable as _OracleTable
from .common import (
    integer as _integer,
    iso as _iso,
    key as _key,
    require_no_rows as _require_no_external_rows,
    require_unique as _unique,
)


def verify_canonical_tables(
    tables: Mapping[str, Sequence[dict[str, object]]],
) -> None:
    if all(isinstance(rows, _OracleTable) for rows in tables.values()):
        _verify_external_canonical_tables(tables)
        return
    orders = tables["orders"]
    fills = tables["fills"]
    positions = tables["positions"]
    cash = tables["cash"]
    costs = tables["costs"]
    valuations = tables["valuations"]
    _unique(orders, ("portfolio_id", "order_id"), "orders")
    _unique(fills, ("portfolio_id", "fill_id"), "fills")
    _unique(positions, ("portfolio_id", "snapshot_id", "instrument_hash"), "positions")
    _unique(cash, ("portfolio_id", "snapshot_id"), "cash")
    _unique(costs, ("portfolio_id", "cost_id"), "costs")
    _unique(valuations, ("portfolio_id", "snapshot_id"), "valuations")
    order_by_key = {_key(row, "portfolio_id", "order_id"): row for row in orders}
    fill_quantity: dict[tuple[str, str], int] = {}
    for fill in fills:
        order_key = _key(fill, "portfolio_id", "order_id")
        order = order_by_key.get(order_key)
        if order is None:
            raise EvidenceContractError("canonical fill 引用未知 order")
        for field in ("instrument_id", "instrument_hash", "asset_class", "side", "session"):
            if str(fill[field]) != str(order[field]):
                raise EvidenceContractError("canonical fill 与 order 身份不一致")
        quantity = _integer(fill["quantity"], "fill.quantity", minimum=1)
        price = _integer(fill["execution_price_units"], "fill.execution_price_units", minimum=1)
        multiplier = _integer(fill["contract_multiplier"], "fill.contract_multiplier", minimum=1)
        if _integer(fill["notional_units"], "fill.notional_units", minimum=0) != price * quantity * multiplier:
            raise EvidenceContractError("canonical fill 成交额不守恒")
        _integer(fill["fee_units"], "fill.fee_units", minimum=0)
        fill_quantity[order_key] = fill_quantity.get(order_key, 0) + quantity
    for key, order in order_by_key.items():
        requested = _integer(order["requested_quantity"], "order.requested_quantity", minimum=1)
        declared = _integer(order["filled_quantity"], "order.filled_quantity", minimum=0)
        actual = fill_quantity.get(key, 0)
        if declared != actual or declared > requested:
            raise EvidenceContractError("canonical order 与 fills 数量不闭合")
        expected_status = "filled" if declared == requested else (
            "rejected" if declared == 0 else "partially_filled"
        )
        if order["status"] != expected_status:
            raise EvidenceContractError("canonical order 终态与成交数量不一致")
    fill_by_key = {_key(row, "portfolio_id", "fill_id"): row for row in fills}
    fee_by_fill: dict[tuple[str, str], int] = {}
    for cost in costs:
        key = _key(cost, "portfolio_id", "fill_id")
        fill = fill_by_key.get(key)
        if fill is None or str(cost["session"]) != str(fill["session"]):
            raise EvidenceContractError("canonical cost 引用未知或错会话 fill")
        if str(cost["currency"]) != "CNY":
            raise EvidenceContractError("canonical cost 币种无效")
        fee_by_fill[key] = fee_by_fill.get(key, 0) + _integer(
            cost["amount_units"], "cost.amount_units", minimum=0,
        )
    expected_fees = {key: int(row["fee_units"]) for key, row in fill_by_key.items()}
    if fee_by_fill != expected_fees:
        raise EvidenceContractError("canonical costs 与 fills 正式费用不闭合")
    _verify_cash(fills, cash)
    _verify_positions(fills, positions)
    _verify_valuations(positions, cash, valuations)


def _verify_external_canonical_tables(
    tables: Mapping[str, Sequence[dict[str, object]]],
) -> None:
    """用可 spill 的关系扫描复核六表，不保留全量 Python 主键或行。"""

    external = {
        name: rows for name, rows in tables.items()
        if isinstance(rows, _OracleTable)
    }
    if set(external) != set(CANONICAL_SIMULATION_SCHEMA_IDS):
        raise EvidenceContractError("SimulationResult canonical 输入不闭合")
    workspace = next(iter(external.values())).workspace
    if any(rows.workspace is not workspace for rows in external.values()):
        raise EvidenceContractError("SimulationResult canonical 表不属于同一复核快照")
    orders = external["orders"].name
    fills = external["fills"].name
    positions = external["positions"].name
    cash = external["cash"].name
    costs = external["costs"].name
    valuations = external["valuations"].name
    for name, fields, label in (
        (orders, ("portfolio_id", "order_id"), "orders"),
        (fills, ("portfolio_id", "fill_id"), "fills"),
        (
            positions,
            ("portfolio_id", "snapshot_id", "instrument_hash"),
            "positions",
        ),
        (cash, ("portfolio_id", "snapshot_id"), "cash"),
        (costs, ("portfolio_id", "cost_id"), "costs"),
        (valuations, ("portfolio_id", "snapshot_id"), "valuations"),
    ):
        _unique(_OracleTable(workspace, name, 0), fields, label)

    _require_no_external_rows(
        workspace,
        f"""
        SELECT 1
        FROM {fills} AS f
        LEFT JOIN {orders} AS o
          ON f.portfolio_id = o.portfolio_id AND f.order_id = o.order_id
        WHERE o.order_id IS NULL
           OR f.instrument_id IS DISTINCT FROM o.instrument_id
           OR f.instrument_hash IS DISTINCT FROM o.instrument_hash
           OR f.asset_class IS DISTINCT FROM o.asset_class
           OR f.side IS DISTINCT FROM o.side
           OR f.session IS DISTINCT FROM o.session
           OR f.quantity IS NULL OR f.quantity < 1
           OR f.execution_price_units IS NULL OR f.execution_price_units < 1
           OR f.contract_multiplier IS NULL OR f.contract_multiplier < 1
           OR f.notional_units IS NULL OR f.notional_units < 0
           OR CAST(f.notional_units AS HUGEINT) !=
              CAST(f.execution_price_units AS HUGEINT)
              * CAST(f.quantity AS HUGEINT)
              * CAST(f.contract_multiplier AS HUGEINT)
           OR f.fee_units IS NULL OR f.fee_units < 0
           OR f.realized_pnl_units IS NULL
        LIMIT 1
        """,
        "canonical fill 引用、身份、数值或成交额不守恒",
    )
    _require_no_external_rows(
        workspace,
        f"""
        WITH fill_totals AS (
          SELECT portfolio_id, order_id, sum(quantity) AS actual_quantity
          FROM {fills}
          GROUP BY portfolio_id, order_id
        )
        SELECT 1
        FROM {orders} AS o
        LEFT JOIN fill_totals AS f
          ON o.portfolio_id = f.portfolio_id AND o.order_id = f.order_id
        WHERE o.requested_quantity IS NULL OR o.requested_quantity < 1
           OR o.filled_quantity IS NULL OR o.filled_quantity < 0
           OR o.filled_quantity != coalesce(f.actual_quantity, 0)
           OR o.filled_quantity > o.requested_quantity
           OR o.status IS DISTINCT FROM CASE
                WHEN o.filled_quantity = o.requested_quantity THEN 'filled'
                WHEN o.filled_quantity = 0 THEN 'rejected'
                ELSE 'partially_filled'
              END
        LIMIT 1
        """,
        "canonical order 与 fills 数量或终态不闭合",
    )
    _require_no_external_rows(
        workspace,
        f"""
        WITH fee_totals AS (
          SELECT portfolio_id, fill_id, sum(amount_units) AS fee_units,
                 count(*) AS cost_rows
          FROM {costs}
          WHERE amount_units IS NOT NULL AND amount_units >= 0
            AND currency = 'CNY'
          GROUP BY portfolio_id, fill_id
        ), invalid_cost AS (
          SELECT 1
          FROM {costs} AS c
          LEFT JOIN {fills} AS f
            ON c.portfolio_id = f.portfolio_id AND c.fill_id = f.fill_id
          WHERE f.fill_id IS NULL
             OR c.session IS DISTINCT FROM f.session
             OR c.currency IS DISTINCT FROM 'CNY'
             OR c.amount_units IS NULL OR c.amount_units < 0
          LIMIT 1
        ), invalid_fill AS (
          SELECT 1
          FROM {fills} AS f
          LEFT JOIN fee_totals AS c
            ON f.portfolio_id = c.portfolio_id AND f.fill_id = c.fill_id
          WHERE c.cost_rows IS NULL OR c.fee_units IS DISTINCT FROM f.fee_units
          LIMIT 1
        )
        SELECT 1 FROM invalid_cost
        UNION ALL
        SELECT 1 FROM invalid_fill
        LIMIT 1
        """,
        "canonical costs 与 fills 正式费用不闭合",
    )

    _require_no_external_rows(
        workspace,
        f"""
        WITH trade_changes AS (
          SELECT portfolio_id, session,
                 sum(CASE WHEN asset_class = 'cn_future'
                          THEN realized_pnl_units - fee_units
                          ELSE (CASE WHEN side = 'buy' THEN -1 ELSE 1 END)
                               * notional_units - fee_units END) AS amount
          FROM {fills}
          GROUP BY portfolio_id, session
        ), asset_classes AS (
          SELECT portfolio_id,
                 count(DISTINCT asset_class) AS class_count,
                 min(asset_class) AS only_class
          FROM {fills}
          GROUP BY portfolio_id
        ), sequenced AS (
          SELECT c.*,
                 first_value(opening_cash_units) OVER sequence AS first_opening,
                 count(DISTINCT opening_cash_units) OVER (
                   PARTITION BY c.portfolio_id
                 ) AS opening_count,
                 coalesce(t.amount, 0) AS expected_trade,
                 first_value(opening_cash_units) OVER sequence
                   + sum(coalesce(t.amount, 0) + non_trade_cash_change_units)
                     OVER sequence AS expected_total,
                 a.class_count, a.only_class
          FROM {cash} AS c
          LEFT JOIN trade_changes AS t
            ON c.portfolio_id = t.portfolio_id AND c.session = t.session
          LEFT JOIN asset_classes AS a ON c.portfolio_id = a.portfolio_id
          WINDOW sequence AS (
            PARTITION BY c.portfolio_id
            ORDER BY c.valuation_time, c.snapshot_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )
        )
        SELECT 1
        FROM sequenced
        WHERE opening_cash_units IS NULL OR opening_cash_units < 1
           OR opening_count != 1
           OR trade_cash_change_units IS DISTINCT FROM expected_trade
           OR total_cash_units IS DISTINCT FROM expected_total
           OR non_trade_cash_change_units IS NULL
           OR available_cash_units IS NULL
           OR expected_total < 0 OR available_cash_units < 0
           OR currency IS DISTINCT FROM 'CNY'
           OR margin_units IS NULL OR margin_units < 0
           OR receivable_cash_units IS NULL OR receivable_cash_units < 0
           OR (class_count = 1 AND only_class = 'cn_future' AND (
                receivable_cash_units != 0
                OR available_cash_units + margin_units != expected_total
              ))
           OR (coalesce(class_count = 1 AND only_class = 'cn_future', false) = false
               AND (margin_units != 0
                    OR available_cash_units + receivable_cash_units > expected_total))
        LIMIT 1
        """,
        "canonical cash 与 fills 或逐期现金 bucket 不闭合",
    )

    _require_no_external_rows(
        workspace,
        f"""
        WITH fill_changes AS (
          SELECT portfolio_id, instrument_hash, session,
                 sum(CASE WHEN side = 'buy' THEN quantity ELSE -quantity END) AS quantity
          FROM {fills}
          GROUP BY portfolio_id, instrument_hash, session
        ), missing_snapshots AS (
          SELECT 1
          FROM fill_changes AS f
          LEFT JOIN {positions} AS p
            ON f.portfolio_id = p.portfolio_id
           AND f.instrument_hash = p.instrument_hash
           AND f.session = p.session
          WHERE p.snapshot_id IS NULL
          LIMIT 1
        ), sequenced AS (
          SELECT p.*, coalesce(f.quantity, 0) AS expected_trade,
                 sum(p.trade_quantity_change + p.non_trade_quantity_change)
                   OVER (
                     PARTITION BY p.portfolio_id, p.instrument_hash
                     ORDER BY p.valuation_time, p.snapshot_id
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS expected_quantity
          FROM {positions} AS p
          LEFT JOIN fill_changes AS f
            ON p.portfolio_id = f.portfolio_id
           AND p.instrument_hash = f.instrument_hash
           AND p.session = f.session
        ), invalid_positions AS (
          SELECT 1
          FROM sequenced
          WHERE trade_quantity_change IS DISTINCT FROM expected_trade
             OR non_trade_quantity_change IS NULL
             OR market_value_units IS NULL
             OR quantity IS DISTINCT FROM expected_quantity
             OR (asset_class IS DISTINCT FROM 'cn_future' AND (
                  quantity < 0
                  OR sellable_quantity IS NULL OR unsettled_quantity IS NULL
                  OR frozen_quantity IS NULL
                  OR sellable_quantity + unsettled_quantity + frozen_quantity
                     != quantity
                ))
          LIMIT 1
        )
        SELECT 1 FROM missing_snapshots
        UNION ALL
        SELECT 1 FROM invalid_positions
        LIMIT 1
        """,
        "canonical positions 与 fills 或逐期持仓 bucket 不闭合",
    )

    _require_no_external_rows(
        workspace,
        f"""
        WITH position_values AS (
          SELECT portfolio_id, snapshot_id, sum(market_value_units) AS market_value
          FROM {positions}
          GROUP BY portfolio_id, snapshot_id
        ), missing_cash AS (
          SELECT 1
          FROM {positions} AS p
          LEFT JOIN {cash} AS c
            ON p.portfolio_id = c.portfolio_id AND p.snapshot_id = c.snapshot_id
          WHERE c.snapshot_id IS NULL
          LIMIT 1
        ), mismatched AS (
          SELECT 1
          FROM {cash} AS c
          FULL OUTER JOIN {valuations} AS v
            ON c.portfolio_id = v.portfolio_id AND c.snapshot_id = v.snapshot_id
          LEFT JOIN position_values AS p
            ON c.portfolio_id = p.portfolio_id AND c.snapshot_id = p.snapshot_id
          WHERE c.snapshot_id IS NULL OR v.snapshot_id IS NULL
             OR v.session IS DISTINCT FROM c.session
             OR v.valuation_time IS DISTINCT FROM c.valuation_time
             OR v.currency IS DISTINCT FROM c.currency
             OR v.source_state_hash IS DISTINCT FROM c.source_state_hash
             OR v.valuation_model IS NULL
             OR v.valuation_model NOT IN (
                  'cash_plus_position_market_value', 'futures_settlement_equity'
                )
             OR v.nav_units IS DISTINCT FROM CASE
                  WHEN v.valuation_model = 'cash_plus_position_market_value'
                    THEN c.total_cash_units + coalesce(p.market_value, 0)
                  ELSE c.total_cash_units
                END
          LIMIT 1
        )
        SELECT 1 FROM missing_cash
        UNION ALL
        SELECT 1 FROM mismatched
        LIMIT 1
        """,
        "canonical valuations 与现金、持仓或 NAV 不闭合",
    )


def _verify_cash(
    fills: list[dict[str, object]],
    cash: list[dict[str, object]],
) -> None:
    trade_changes: dict[tuple[str, str], int] = {}
    asset_classes: dict[str, set[str]] = {}
    for fill in fills:
        portfolio = str(fill["portfolio_id"])
        asset_classes.setdefault(portfolio, set()).add(str(fill["asset_class"]))
        if str(fill["asset_class"]) == "cn_future":
            change = int(fill["realized_pnl_units"]) - int(fill["fee_units"])
        else:
            direction = -1 if str(fill["side"]) == "buy" else 1
            change = direction * int(fill["notional_units"]) - int(fill["fee_units"])
        key = portfolio, str(fill["session"])
        trade_changes[key] = trade_changes.get(key, 0) + change
    by_portfolio: dict[str, list[dict[str, object]]] = {}
    for row in cash:
        by_portfolio.setdefault(str(row["portfolio_id"]), []).append(row)
    for portfolio, rows in by_portfolio.items():
        previous: int | None = None
        opening: int | None = None
        for row in sorted(rows, key=lambda item: (_iso(item["valuation_time"]), str(item["snapshot_id"]))):
            current_opening = _integer(row["opening_cash_units"], "cash.opening", minimum=1)
            if opening is None:
                opening = current_opening
                previous = opening
            elif opening != current_opening:
                raise EvidenceContractError("canonical cash 初始资金身份漂移")
            declared_trade = _integer(row["trade_cash_change_units"], "cash.trade_change")
            if declared_trade != trade_changes.get((portfolio, str(row["session"])), 0):
                raise EvidenceContractError("canonical cash 与 fills 交易变化不闭合")
            assert previous is not None
            expected = previous + declared_trade + int(row["non_trade_cash_change_units"])
            if int(row["total_cash_units"]) != expected or min(
                expected, int(row["available_cash_units"]),
            ) < 0:
                raise EvidenceContractError("canonical cash 逐期不守恒")
            if str(row["currency"]) != "CNY":
                raise EvidenceContractError("canonical cash 币种无效")
            available = int(row["available_cash_units"])
            margin = _integer(row["margin_units"], "cash.margin", minimum=0)
            receivable = _integer(row["receivable_cash_units"], "cash.receivable", minimum=0)
            if asset_classes.get(portfolio) == {"cn_future"}:
                if receivable != 0 or available + margin != expected:
                    raise EvidenceContractError("期货现金、可用权益与保证金不闭合")
            elif margin != 0 or available + receivable > expected:
                raise EvidenceContractError("现货现金 bucket 不闭合")
            previous = expected


def _verify_positions(
    fills: list[dict[str, object]],
    positions: list[dict[str, object]],
) -> None:
    fill_changes: dict[tuple[str, str, str], int] = {}
    for fill in fills:
        signed = int(fill["quantity"]) if fill["side"] == "buy" else -int(fill["quantity"])
        key = str(fill["portfolio_id"]), str(fill["instrument_hash"]), str(fill["session"])
        fill_changes[key] = fill_changes.get(key, 0) + signed
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in positions:
        grouped.setdefault(_key(row, "portfolio_id", "instrument_hash"), []).append(row)
    fill_position_keys = {
        (str(row["portfolio_id"]), str(row["instrument_hash"]), str(row["session"]))
        for row in fills
    }
    position_keys = {
        (str(row["portfolio_id"]), str(row["instrument_hash"]), str(row["session"]))
        for row in positions
    }
    if not fill_position_keys <= position_keys:
        raise EvidenceContractError("canonical fill 缺少同会话持仓终止快照")
    for key, rows in grouped.items():
        previous = 0
        for row in sorted(rows, key=lambda item: (_iso(item["valuation_time"]), str(item["snapshot_id"]))):
            declared = int(row["trade_quantity_change"])
            actual = fill_changes.get((key[0], key[1], str(row["session"])), 0)
            if declared != actual:
                raise EvidenceContractError("canonical positions 与 fills 数量变化不闭合")
            quantity = previous + declared + int(row["non_trade_quantity_change"])
            if int(row["quantity"]) != quantity:
                raise EvidenceContractError("canonical positions 逐期不守恒")
            if str(row["asset_class"]) != "cn_future":
                buckets = sum(int(row[name]) for name in (
                    "sellable_quantity", "unsettled_quantity", "frozen_quantity",
                ))
                if quantity < 0 or buckets != quantity:
                    raise EvidenceContractError("现货持仓 bucket 不闭合")
            previous = quantity


def _verify_valuations(
    positions: list[dict[str, object]],
    cash: list[dict[str, object]],
    valuations: list[dict[str, object]],
) -> None:
    cash_by_key = {_key(row, "portfolio_id", "snapshot_id"): row for row in cash}
    value_by_key: dict[tuple[str, str], int] = {}
    for row in positions:
        key = _key(row, "portfolio_id", "snapshot_id")
        if key not in cash_by_key:
            raise EvidenceContractError("canonical position 引用未知 cash snapshot")
        value_by_key[key] = value_by_key.get(key, 0) + int(row["market_value_units"])
    if {_key(row, "portfolio_id", "snapshot_id") for row in valuations} != set(cash_by_key):
        raise EvidenceContractError("canonical valuations 与 cash 快照集合不闭合")
    for row in valuations:
        key = _key(row, "portfolio_id", "snapshot_id")
        cash_row = cash_by_key[key]
        if any(str(row[field]) != str(cash_row[field]) for field in (
            "session", "valuation_time", "currency", "source_state_hash",
        )):
            raise EvidenceContractError("canonical valuation 与 cash 身份不一致")
        expected = int(cash_row["total_cash_units"])
        if row["valuation_model"] == "cash_plus_position_market_value":
            expected += value_by_key.get(key, 0)
        elif row["valuation_model"] != "futures_settlement_equity":
            raise EvidenceContractError("canonical valuation_model 不受支持")
        if int(row["nav_units"]) != expected:
            raise EvidenceContractError("canonical NAV 与现金、持仓市值不守恒")


__all__ = ["verify_canonical_tables"]
