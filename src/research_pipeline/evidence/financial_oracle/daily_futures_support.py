"""把封存的日频期货支持分区交给独立金融复核。"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from itertools import zip_longest
from pathlib import Path

import pyarrow.parquet as pq

from research_pipeline.platform import typed_canonical_hash
from ..errors import EvidenceContractError
from ..oracle_workspace import FINANCIAL_ORACLE_BATCH_SIZE, ResultTableSource
from .common import require_no_rows, aware_datetime, date_value
from .daily_futures import verify_daily_futures_financial_context
from .explicit_futures_daily import verify_explicit_futures_daily_order_execution

FUTURES_DECLARATION = {
    "contract_version": "research-futures-daily-context-v1",
    "path": "simulation/futures-context.json",
    "path_prefix": "simulation/futures-context-tables",
}
FUTURES_ROLES = (
    "intents", "fills", "settlements", "nav", "rule_snapshots", "rejections",
    "contributions", "portfolio", "market_inputs", "settlement_inputs", "tick_size_inputs",
)


def futures_support_sources(snapshot, manifest, context):
    semantics = manifest.get("semantics", {})
    required = (semantics.get("asset_class") == "cn_future" and semantics.get("frequency") == "daily"
                and semantics.get("contract_version") == "research-simulation-result-semantics-v2")
    declaration = manifest.get("futures_context_contract")
    if not required and declaration is None:
        if context is not None:
            raise EvidenceContractError("日频期货金融上下文缺少正式声明")
        return {}
    version = declaration.get("contract_version") if isinstance(declaration, dict) else None
    if (version not in {"research-futures-daily-context-v1", "research-futures-daily-context-v2", "research-futures-daily-context-v3"}
            or declaration != {**FUTURES_DECLARATION, "contract_version": version}
            or context is None or not required):
        raise EvidenceContractError("日频期货金融上下文声明缺失或无效")
    unsigned = {key: value for key, value in context.items() if key != "context_hash"}
    if (context.get("contract_version") != version
            or context.get("context_hash") != typed_canonical_hash(unsigned)
            or context.get("source_simulation_hash") != manifest.get("source_simulation_hash")
            or context.get("simulation_result_hash") != manifest.get("result_hash")
            or set(context.get("tables", {})) != set(FUTURES_ROLES)):
        raise EvidenceContractError("日频期货金融上下文与正式结果身份不一致")
    control = [item for item in snapshot.bundle.support_files
               if item.source_path == "simulation/result-contract/manifest.json"]
    if len(control) != 1:
        raise EvidenceContractError("日频期货支持事实缺少唯一仿真来源")
    sources = {}
    selected_all = set()
    for role in FUTURES_ROLES:
        prefix = f"simulation/futures-context-tables/{role}"
        metadata = context["tables"][role]
        selected = sorted((item for item in snapshot.bundle.support_files
                           if item.source_path.startswith(prefix + "/")), key=lambda item: item.source_path)
        if (metadata.get("path_prefix") != prefix or len(selected) != metadata.get("partition_count")
                or any(item.artifact_key != control[0].artifact_key for item in selected)):
            raise EvidenceContractError("日频期货支持分区与声明不一致")
        paths: list[Path] = []
        schema = None
        rows = size = 0
        for partition_index, item in enumerate(selected):
            relative = item.source_path.removeprefix(prefix + "/")
            if version == "research-futures-daily-context-v1":
                parts = relative.split("/")
                valid_path = len(parts) == 2 and parts[0].startswith("session=") and parts[1] == "data.parquet"
            else:
                valid_path = relative == f"part-{partition_index:05d}.parquet"
            if not valid_path:
                raise EvidenceContractError("日频期货支持分区路径无效")
            selected_all.add(item.source_path)
            target = snapshot.directory / item.relative_path
            parquet = pq.ParquetFile(target)
            if schema is None:
                schema = parquet.schema_arrow
            elif schema != parquet.schema_arrow:
                raise EvidenceContractError("日频期货支持表分区 schema 不一致")
            rows += parquet.metadata.num_rows
            for index in range(parquet.metadata.num_row_groups):
                group = parquet.metadata.row_group(index)
                size += sum(group.column(column).total_uncompressed_size for column in range(group.num_columns))
            paths.append(target)
        if rows != metadata.get("row_count") or (rows == 0) != (not selected):
            raise EvidenceContractError("日频期货支持表行数不一致")
        if paths:
            def batches(paths=tuple(paths)):
                for target in paths:
                    yield from pq.ParquetFile(target).iter_batches(batch_size=FINANCIAL_ORACLE_BATCH_SIZE)
            sources[f"futures.{role}"] = ResultTableSource(schema, rows, size, batches, tuple(paths))
    present = {item.source_path for item in snapshot.bundle.support_files
               if item.source_path.startswith("simulation/futures-context-tables/")}
    if present != selected_all:
        raise EvidenceContractError("日频期货支持事实含未声明的表")
    return sources


def _rows_for_verification(value):
    if value is None:
        return []
    if hasattr(value, "iter_rows"):
        return [dict(row) for row in value.iter_rows()]
    return [dict(row) for row in value]


def verify_sealed_futures(*, workspace, context, canonical):
    def table(role):
        return workspace.tables.get(f"futures.{role}")
    def rows(role, order_by=()):
        value = table(role)
        return () if value is None else value.iter_rows(order_by=order_by)
    policy = {key: context[key] for key in (
        "cash_scale", "initial_position", "account_model", "forced_execution_policy",
        "initial_cash_fen", "slippage_ticks", "margin_check_policy", "close_bucket_order",
    )}
    verified_prices = {}
    if context.get("contract_version") == "research-futures-daily-context-v3":
        if context.get("execution_mode") != "explicit_orders" or not isinstance(context.get("explicit_order_context"), dict):
            raise EvidenceContractError("日频期货 v3 必须绑定显式订单上下文")
        support_tables = {
            role: table(role) or ()
            for role in ("fills", "market_inputs", "rule_snapshots", "tick_size_inputs", "settlements")
        }
        explicit_context = context["explicit_order_context"]
        if context.get("account_mode") == "single_account":
            verified_fees = verify_explicit_futures_daily_order_execution(
                context=explicit_context, canonical=canonical, support_tables=support_tables,
                global_slippage_ticks=int(context["slippage_ticks"]),
            )
            verified_prices = {
                str(row["fill_id"]): Decimal(int(row["execution_price_units"])).scaleb(-int(row["price_scale"]))
                for row in _rows_for_verification(canonical["fills"])
                if str(row["fill_id"]) in verified_fees
            }
        elif context.get("account_mode") == "independent_product_accounts":
            if (explicit_context.get("contract_version") != "research-explicit-futures-daily-portfolio-v1"
                    or explicit_context.get("account_model") != "independent_product_accounts"
                    or not isinstance(explicit_context.get("products"), dict)):
                raise EvidenceContractError("日频期货组合 v3 显式上下文无效")
            for product, product_context in explicit_context["products"].items():
                product_tables = {
                    role: [row for row in _rows_for_verification(value)
                          if str(row.get("product")) == str(product)]
                    for role, value in support_tables.items()
                }
                product_market_keys = {
                    (str(row["code"]), date_value(row["date"], "market.date"))
                    for row in product_tables["market_inputs"]
                }
                product_tables["tick_size_inputs"] = [
                    row for row in _rows_for_verification(support_tables["tick_size_inputs"])
                    if (str(row["code"]), date_value(row["date"], "tick.date")) in product_market_keys
                ]
                product_canonical = {
                    name: [row for row in _rows_for_verification(value)
                          if str(row.get("portfolio_id")) in {str(product), "default"}]
                    for name, value in canonical.items()
                }
                verified_fees = verify_explicit_futures_daily_order_execution(
                    context=product_context, canonical=product_canonical,
                    support_tables=product_tables,
                    global_slippage_ticks=int(context["slippage_ticks"]),
                )
                verified_prices.update({
                    str(row["fill_id"]): Decimal(int(row["execution_price_units"])).scaleb(-int(row["price_scale"]))
                    for row in product_canonical["fills"] if str(row["fill_id"]) in verified_fees
                })
        else:
            raise EvidenceContractError("日频期货 v3 账户模式无效")
    expected_timing = {
        "policy_id": "daily-futures-research-clock-v1", "timezone": "Asia/Shanghai",
        "execution": "explicit_order_time_at_daily_open_price",
        "missing_open_time": "earliest_explicit_order_time_research_assumption",
        "settlement_time": "17:00",
        "missing_settlement_availability": "settlement_time_research_assumption",
        "missing_tick_availability": "earliest_explicit_order_time_research_assumption",
    }
    if context.get("contract_version") == "research-futures-daily-context-v3":
        expected_timing.update(
            policy_id="daily-futures-explicit-opening-clock-v1",
            execution="strictly_after_submission_at_daily_open",
            missing_open_time="09:00_research_assumption",
            missing_tick_availability="daily_open_research_assumption",
        )
    if context.get("timing_policy") != expected_timing:
        raise EvidenceContractError("日频期货研究执行时序声明不一致")
    mode = context.get("account_mode")
    if mode == "single_account":
        report = verify_daily_futures_financial_context(context=policy, verified_execution_prices=verified_prices, tables={
            "fills": table("fills") or (), "settlements": table("settlements") or (),
            "nav": table("nav") or (), "rule_snapshots": table("rule_snapshots") or (),
            "market": table("market_inputs") or (),
            "source_settlements": table("settlement_inputs") or (),
            "tick_size_inputs": table("tick_size_inputs") or (),
        })
        reports = [report]
    elif mode == "independent_product_accounts":
        reports = _verify_products(workspace, table, policy, verified_prices)
    else:
        raise EvidenceContractError("日频期货账户聚合模式无效")
    for fact, fill in zip_longest(rows("fills", ("fill_id",)), canonical["fills"].iter_rows(order_by=("fill_id",))):
        if fact is None or fill is None:
            raise EvidenceContractError("日频期货支持成交与规范成交集合不一致")
        fields = {
            "fill_id": "fill_id", "order_id": "order_id", "actual_contract": "instrument_id",
            "side": "side", "quantity": "quantity", "position_effect": "position_effect",
            "fee_fen": "fee_units", "realized_pnl_fen": "realized_pnl_units",
            "execution_price_units": "execution_price_units", "price_scale": "price_scale",
            "multiplier": "contract_multiplier",
        }
        if any(fact.get(left) != fill.get(right) for left, right in fields.items()):
            raise EvidenceContractError("日频期货支持成交与规范成交价量费用不一致")
        exact_price = Decimal(int(fact["execution_price_units"])) / (Decimal(10) ** int(fact["price_scale"]))
        if exact_price != Decimal(str(fact["fill_price"])):
            raise EvidenceContractError("日频期货定点报价与独立复算价格不一致")
        if (aware_datetime(fact["fill_time"], "fill_time") != aware_datetime(fill["fill_time"], "canonical.fill_time")
                or date_value(fact["trading_date"], "trading_date") != date_value(fill["session"], "canonical.session")):
            raise EvidenceContractError("日频期货规范成交时点或会话不一致")
        notional = int((Decimal(str(fact["fill_price"])) * fact["quantity"] * fact["multiplier"] * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))
        if int(fill["notional_units"]) != notional:
            raise EvidenceContractError("日频期货规范成交金额不一致")
    for fact, cash in zip_longest(rows("nav", ("trading_date",)), canonical["cash"].iter_rows(order_by=("session",))):
        if fact is None or cash is None or str(fact["trading_date"]) != str(cash["session"]):
            raise EvidenceContractError("日频期货规范现金会话不一致")
        if any(int(fact[left]) != int(cash[right]) for left, right in (
            ("nav_fen", "total_cash_units"), ("margin_fen", "margin_units"),
            ("free_equity_fen", "available_cash_units"),
        )) or int(cash["opening_cash_units"]) != int(context["initial_cash_fen"]):
            raise EvidenceContractError("日频期货规范现金与独立账本不一致")
    settlements = table("settlements")
    positions, cash = canonical["positions"], canonical["cash"]
    require_no_rows(workspace, f"""
        SELECT p.instrument_id FROM {positions.name} p LEFT JOIN {settlements.name} s
          ON p.instrument_id = s.actual_contract AND p.session = s.trading_date
        WHERE p.quantity != coalesce(s.position, 0) OR p.non_trade_quantity_change != 0
        UNION ALL
        SELECT s.actual_contract FROM {settlements.name} s LEFT JOIN {positions.name} p
          ON p.instrument_id = s.actual_contract AND p.session = s.trading_date
        WHERE s.position != 0 AND (p.instrument_id IS NULL OR p.quantity != s.position)
    """, "日频期货规范仓位与独立结算事实不一致")
    require_no_rows(workspace, f"""
        SELECT c.session FROM {cash.name} c JOIN (
            SELECT trading_date, max(settlement_time) AS settlement_time FROM {settlements.name} GROUP BY trading_date
        ) s ON c.session = s.trading_date
        WHERE CAST(c.valuation_time AS TIMESTAMPTZ) != CAST(s.settlement_time AS TIMESTAMPTZ)
    """, "日频期货规范估值时点与结算不一致")
    return reports


def _verify_products(workspace, table, policy, verified_prices):
    settlements, rules = table("settlements"), table("rule_snapshots")
    if settlements is None or rules is None or table("contributions") is None or table("portfolio") is None:
        raise EvidenceContractError("日频期货组合支持事实不完整")
    reports = []
    for row in workspace.iter_query(f"SELECT DISTINCT product FROM {settlements.name} ORDER BY product"):
        product = row["product"]
        def product_rows(role, order_by):
            source = table(role)
            if source is None:
                return ()
            return workspace.iter_query(
                f"SELECT * FROM {source.name} WHERE product = ? ORDER BY {order_by}", (product,))
        def source_rows(role):
            source = table(role)
            if source is None:
                return ()
            return workspace.iter_query(f"""
                SELECT x.* FROM {source.name} x
                WHERE x.code IN (SELECT contract_code FROM {rules.name} WHERE product = ?)
                  AND CAST(x.date AS DATE) IN (SELECT trading_date FROM {settlements.name} WHERE product = ?)
                ORDER BY x.date, x.code
            """, (product, product))
        nav = workspace.iter_query(f"""
            SELECT trading_date, equity_fen AS nav_fen, required_margin_fen AS margin_fen,
                   free_equity_fen, position FROM {settlements.name}
            WHERE product = ? ORDER BY trading_date
        """, (product,))
        reports.append(verify_daily_futures_financial_context(context=policy, verified_execution_prices=verified_prices, tables={
            "fills": product_rows("fills", "trading_date, fill_sequence"),
            "settlements": product_rows("settlements", "trading_date"), "nav": nav,
            "rule_snapshots": product_rows("rule_snapshots", "trading_date, rule_snapshot_hash"),
            "market": source_rows("market_inputs"), "source_settlements": source_rows("settlement_inputs"),
            "tick_size_inputs": source_rows("tick_size_inputs"),
        }))
    contributions, nav, portfolio = table("contributions"), table("nav"), table("portfolio")
    initial = int(policy["initial_cash_fen"])
    require_no_rows(workspace, f"""
        SELECT product, trading_date FROM {contributions.name} GROUP BY product, trading_date HAVING count(*) != 1
    """, "日频期货品种贡献主键重复")
    require_no_rows(workspace, f"""
        WITH expected AS (SELECT p.product, n.trading_date
            FROM (SELECT DISTINCT product FROM {settlements.name}) p
            CROSS JOIN {nav.name} n)
        (SELECT * FROM expected EXCEPT SELECT product, trading_date FROM {contributions.name})
        UNION ALL
        (SELECT product, trading_date FROM {contributions.name} EXCEPT SELECT * FROM expected)
    """, "日频期货品种贡献未完整覆盖交易会话")
    require_no_rows(workspace, f"""
        SELECT c.product FROM {contributions.name} c
        LEFT JOIN LATERAL (SELECT position, required_margin_fen, trading_date FROM {settlements.name} s
            WHERE s.product = c.product AND s.trading_date <= c.trading_date
            ORDER BY s.trading_date DESC LIMIT 1) prior ON TRUE
        WHERE prior.trading_date < c.trading_date AND (prior.position != 0 OR prior.required_margin_fen != 0)
    """, "日频期货持仓品种缺少当前会话结算，不能忽略保证金")
    fills = table("fills")
    if fills is None:
        require_no_rows(workspace, f"SELECT product FROM {contributions.name} WHERE fee_fen != 0", "日频期货品种贡献费用不一致")
    else:
        require_no_rows(workspace, f"""
            SELECT c.product FROM {contributions.name} c LEFT JOIN (
                SELECT product, trading_date, sum(fee_fen) AS fee FROM {fills.name} GROUP BY product, trading_date
            ) f USING (product, trading_date) WHERE c.fee_fen != coalesce(f.fee, 0)
        """, "日频期货品种贡献费用不一致")
    require_no_rows(workspace, f"""
        SELECT c.product FROM {contributions.name} c
        LEFT JOIN LATERAL (SELECT equity_fen, required_margin_fen, trading_date FROM {settlements.name} s
            WHERE s.product = c.product AND s.trading_date <= c.trading_date ORDER BY s.trading_date DESC LIMIT 1) now ON TRUE
        LEFT JOIN LATERAL (SELECT equity_fen FROM {settlements.name} s
            WHERE s.product = c.product AND s.trading_date < c.trading_date ORDER BY s.trading_date DESC LIMIT 1) prev ON TRUE
        WHERE c.pnl_fen != coalesce(now.equity_fen, {initial}) - coalesce(prev.equity_fen, {initial})
           OR c.margin_fen != CASE WHEN now.trading_date = c.trading_date THEN now.required_margin_fen ELSE 0 END
    """, "日频期货逐品种贡献不一致")
    require_no_rows(workspace, f"""
        WITH daily AS (SELECT trading_date, sum(pnl_fen) AS pnl, sum(margin_fen) AS margin
            FROM {contributions.name} GROUP BY trading_date), expected AS (
            SELECT trading_date, {initial} + sum(pnl) OVER (ORDER BY trading_date) AS equity, margin FROM daily)
        SELECT n.trading_date FROM {nav.name} n FULL JOIN expected e USING (trading_date)
        WHERE n.trading_date IS NULL OR e.trading_date IS NULL OR n.nav_fen != e.equity
           OR n.margin_fen != e.margin OR n.free_equity_fen != e.equity - e.margin
           OR e.margin > e.equity
    """, "日频期货组合净值或保证金不一致")
    require_no_rows(workspace, f"(SELECT * FROM {nav.name} EXCEPT SELECT * FROM {portfolio.name}) UNION ALL (SELECT * FROM {portfolio.name} EXCEPT SELECT * FROM {nav.name})", "日频期货组合事实与净值不一致")
    return reports
