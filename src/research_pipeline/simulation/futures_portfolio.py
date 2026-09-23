"""复用单品种期货账本的多品种组合编排与贡献对账。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
import re
from typing import Mapping

import pandas as pd

from .futures_daily import (
    FUTURES_PORTFOLIO_SIMULATION_VERSION,
    FuturesRuleBook,
    FuturesSimulationResult,
    build_futures_roll_order_intents,
    cny_to_fen,
    run_futures_daily_simulation,
)
from .orders import SimulationContractError


_PORTFOLIO_INTENT_COLUMNS = (
    "product", "category", "execution_session", "intent_hash", "intent_json",
    "order_id", "actual_contract", "side", "quantity", "position_effect",
    "target_sequence", "leg_ordinal", "target_hash", "rule_snapshot_hash",
)


def build_futures_portfolio_roll_intents(
    targets: pd.DataFrame,
    *,
    market_data_artifact_hash: str,
    rule_book: FuturesRuleBook,
) -> pd.DataFrame:
    """逐品种复用双腿换月订单生成器，禁止跨品种状态串线。"""

    required = {"product", "category", "execution_session", "execution_time"}
    if required - set(targets.columns):
        raise SimulationContractError("多品种目标缺少组合或执行字段")
    rows = []
    for (product, category), frame in targets.groupby(["product", "category"], sort=True):
        sessions = tuple(sorted(pd.to_datetime(frame["execution_session"]).dt.date.unique()))
        execution_times = {
            pd.Timestamp(row.execution_session).date(): pd.Timestamp(row.execution_time).to_pydatetime()
            for row in frame.itertuples(index=False)
        }
        intents = build_futures_roll_order_intents(
            frame,
            execution_sessions=sessions,
            execution_times=execution_times,
            market_data_artifact_hash=market_data_artifact_hash,
            rule_book=rule_book,
        )
        if not intents.empty:
            intents.insert(0, "category", str(category))
            intents.insert(0, "product", str(product))
            rows.append(intents)
    if not rows:
        return pd.DataFrame(columns=_PORTFOLIO_INTENT_COLUMNS)
    return pd.concat(rows, ignore_index=True).sort_values(
        ["execution_session", "product", "target_sequence", "leg_ordinal"], kind="stable",
    ).reset_index(drop=True)


def run_futures_portfolio_simulation(
    *,
    market: pd.DataFrame,
    settlements: pd.DataFrame,
    intents: pd.DataFrame,
    mappings: pd.DataFrame,
    rule_book: FuturesRuleBook,
    initial_cash_cny: Decimal,
    category_by_product: Mapping[str, str],
    slippage_ticks: int,
    tick_sizes: pd.DataFrame,
) -> FuturesSimulationResult:
    """独立重放每个品种账本，再按真实 PnL/费用/保证金合成组合。"""

    required_mapping = {"product", "execution_session", "actual_contract"}
    required_tick = {"date", "code", "tick_size", "available_time", "source_hash"}
    if required_mapping - set(mappings.columns) or required_tick - set(tick_sizes.columns):
        raise SimulationContractError("多品种仿真缺少映射或 tick size 字段")
    products = tuple(sorted(mappings["product"].astype(str).unique()))
    if set(products) != set(category_by_product):
        raise SimulationContractError("组合类别必须精确覆盖映射品种")
    if not intents.empty:
        required_intent = {"product", "category"}
        if required_intent - set(intents.columns):
            raise SimulationContractError("多品种订单缺少品种或类别")
        observed_categories = {
            str(product): set(frame["category"].astype(str))
            for product, frame in intents.groupby("product", sort=True)
        }
        if any(
            observed_categories.get(product) != {str(category_by_product[product])}
            for product in products
        ):
            raise SimulationContractError("多品种订单类别与组合类别映射不一致")
    tick = tick_sizes.copy()
    tick["date"] = pd.to_datetime(tick["date"]).dt.date
    tick["available_time"] = pd.to_datetime(tick["available_time"], utc=True, errors="raise")
    if tick.duplicated(["date", "code"]).any():
        raise SimulationContractError("tick size 规则主键重复")
    product_results: dict[str, FuturesSimulationResult] = {}
    initial_fen = cny_to_fen(initial_cash_cny)
    for product in products:
        mapping = mappings.loc[mappings["product"].astype(str) == product].copy()
        active = {
            pd.Timestamp(row.execution_session).date(): str(row.actual_contract)
            for row in mapping.itertuples(index=False)
        }
        if len(active) != len(mapping):
            raise SimulationContractError("同一品种执行会话存在多个主动合约")
        allowed_codes = set(mapping["actual_contract"].astype(str))
        product_market = market.loc[
            market["code"].astype(str).map(lambda code: _product(code) == product)
            & pd.to_datetime(market["date"]).dt.date.isin(active)
        ].copy()
        product_settlement = settlements.loc[
            settlements["code"].astype(str).isin(allowed_codes)
            & pd.to_datetime(settlements["date"]).dt.date.isin(active)
        ].copy()
        product_intents = intents.loc[intents["product"].astype(str) == product].drop(
            columns=["product", "category"], errors="ignore",
        )
        order_times = {
            (pd.Timestamp(row.execution_session).date(), str(row.actual_contract)): pd.Timestamp(
                json.loads(str(row.intent_json))["order_time"]
            )
            for row in product_intents.itertuples(index=False)
        }
        relevant_tick = tick.loc[
            tick.apply(lambda row: (row["date"], str(row["code"])) in order_times, axis=1)
        ]
        tick_map: dict[tuple[date, str], Decimal] = {}
        for row in relevant_tick.itertuples(index=False):
            key = (row.date, str(row.code))
            order_time = order_times.get(key)
            if order_time is None or pd.Timestamp(row.available_time) > order_time:
                raise SimulationContractError("tick size 在订单时点尚不可见")
            tick_map[key] = Decimal(str(row.tick_size))
        if slippage_ticks and set(order_times) != set(tick_map):
            raise SimulationContractError("非零滑点的订单没有完整 tick size")
        product_results[product] = run_futures_daily_simulation(
            market=product_market,
            settlements=product_settlement,
            intents=product_intents,
            rule_book=rule_book,
            initial_cash_cny=initial_cash_cny,
            active_contract_by_session=active,
            slippage_ticks=slippage_ticks,
            tick_size_by_session_contract=tick_map,
        )
    sessions = sorted({pd.Timestamp(value).date() for result in product_results.values() for value in result.nav["trading_date"]})
    contribution_rows = []
    portfolio_rows = []
    cumulative = 0
    for session in sessions:
        day_pnl = 0
        day_margin = 0
        for product, result in product_results.items():
            nav = result.nav.copy()
            nav["trading_date"] = pd.to_datetime(nav["trading_date"]).dt.date
            current_rows = nav.loc[nav["trading_date"] <= session]
            current = initial_fen if current_rows.empty else int(current_rows.iloc[-1]["nav_fen"])
            prior_rows = nav.loc[nav["trading_date"] < session]
            prior = initial_fen if prior_rows.empty else int(prior_rows.iloc[-1]["nav_fen"])
            contribution = current - prior
            margin_rows = nav.loc[nav["trading_date"] == session, "margin_fen"]
            margin = 0 if margin_rows.empty else int(margin_rows.iloc[0])
            fees = result.fills.copy()
            fee = 0 if fees.empty else int(fees.loc[pd.to_datetime(fees["trading_date"]).dt.date == session, "fee_fen"].sum())
            contribution_rows.append({
                "trading_date": session,
                "year": session.year,
                "product": product,
                "category": str(category_by_product[product]),
                "pnl_fen": contribution,
                "fee_fen": fee,
                "margin_fen": margin,
            })
            day_pnl += contribution
            day_margin += margin
        cumulative += day_pnl
        nav_fen = initial_fen + cumulative
        if day_margin > nav_fen:
            raise SimulationContractError("多品种组合保证金合计超过权益；本版本不猜测强平顺序")
        portfolio_rows.append({
            "trading_date": session,
            "nav_fen": nav_fen,
            "margin_fen": day_margin,
            "free_equity_fen": nav_fen - day_margin,
            "position": 0,
        })
    fills = _concat(product_results, "fills", category_by_product)
    settlements_frame = _concat(product_results, "settlements", category_by_product)
    rules = _concat(product_results, "rule_snapshots", category_by_product)
    rejections = _concat(product_results, "rejections", category_by_product)
    combined_intents = intents.reset_index(drop=True)
    result = FuturesSimulationResult(
        intents=combined_intents,
        fills=fills,
        settlements=settlements_frame,
        nav=pd.DataFrame(portfolio_rows),
        rule_snapshots=rules,
        backend_id="cn-futures-portfolio-daily-v1",
        contract_version=FUTURES_PORTFOLIO_SIMULATION_VERSION,
        rejections=rejections,
        contributions=pd.DataFrame(contribution_rows),
        portfolio=pd.DataFrame(portfolio_rows),
    )
    realized = 0 if result.fills.empty else int(result.fills["realized_pnl_fen"].sum())
    fees = 0 if result.fills.empty else int(result.fills["fee_fen"].sum())
    expected = initial_fen + int(result.settlements["mtm_pnl_fen"].sum()) + realized - fees
    if int(result.nav.iloc[-1]["nav_fen"]) != expected:
        raise SimulationContractError("多品种组合现金、逐日盯市与费用不守恒")
    return result


def _concat(
    results: Mapping[str, FuturesSimulationResult],
    field: str,
    category_by_product: Mapping[str, str],
) -> pd.DataFrame:
    frames = []
    for product, result in results.items():
        frame = getattr(result, field).copy()
        if not frame.empty:
            frame.insert(0, "category", str(category_by_product[product]))
            frame.insert(0, "product", product)
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _product(code: str) -> str:
    base = code.split(".", 1)[0]
    match = re.match(r"[A-Za-z]+", base)
    if match is None:
        raise SimulationContractError("无法从实际合约识别品种")
    return match.group(0).upper()


__all__ = ["build_futures_portfolio_roll_intents", "run_futures_portfolio_simulation"]
