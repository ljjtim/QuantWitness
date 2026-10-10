"""把显式日频目标接入既有现金引擎、规范账本和独立复核上下文。"""
from bisect import bisect_right
from dataclasses import replace
from datetime import date, datetime, time
from pathlib import Path
from collections.abc import Mapping
from zoneinfo import ZoneInfo
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.domain import CorporateAction, MarketRuleSnapshot, PortfolioTarget, Price
from research_pipeline.domain.rules import build_cn_etf_daily_rule_snapshots
from research_pipeline.domain.non_trading_sessions import parse_non_trading_sessions
from research_pipeline.domain.order_stream import parse_order_commands
from research_pipeline.domain.external_cashflows import parse_external_cashflows
from research_pipeline.domain.credit_account import parse_credit_account
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.market_rule_defaults import resolve_cn_etf_daily_market_rule_profile
from research_pipeline.research.dataframe_budget import PandasFrameBudget, parquet_uncompressed_bytes
from research_pipeline.simulation import BarTcaPolicy
from research_pipeline.simulation.corporate_actions import CorporateActionRecordPosition
from research_pipeline.simulation.daily_event import DAILY_CASH_PRICE_SCALE, run_daily_cash_event_simulation
from research_pipeline.simulation.result_contract import (build_simulation_result_contract, canonical_simulation_table,
    project_cash_daily_result, write_simulation_result_contract)
from .bar_tca_adapter import execute_simulation_result_bar_tca, tca_metadata


def _read(root, prefix, columns, budget, optional_columns=()):
    paths = sorted((Path(root) / prefix).glob('*.parquet'))
    if parquet_uncompressed_bytes(paths, columns=columns) > budget.max_memory_bytes:
        raise ValueError('日频仿真输入超过节点内存预算')
    def batches():
        for path in paths:
            parquet = pq.ParquetFile(path)
            selected = [*columns, *(name for name in optional_columns if name in parquet.schema_arrow.names)]
            if parquet.metadata.num_rows == 0:
                yield pa.RecordBatch.from_pylist([], schema=pa.schema([
                    parquet.schema_arrow.field(name) for name in selected]))
            else:
                yield from parquet.iter_batches(columns=selected, batch_size=8192)
    return budget.collect_arrow_batches(batches(), label=prefix)


def execute_daily_cash_artifact(*, target_root, market_root, parameters, output_root,
                                max_memory_bytes, market_artifact_hash, target_artifact_hash):
    """读取显式目标、行情及有效日可见的股票或 ETF 规则。"""
    declaration = parse_non_trading_sessions(parameters.get("non_trading_sessions"))
    account_model = parameters.get("account_model", "cash")
    credit_account = parameters.get("credit_account")
    if account_model not in {"cash", "financing_credit"}:
        raise ValueError("account_model 只能是 cash 或 financing_credit")
    if account_model == "cash" and credit_account is not None:
        raise ValueError("普通现金账户不能携带信用账户输入")
    if account_model == "financing_credit" and (
        not isinstance(credit_account, Mapping) or not isinstance(parameters.get("account"), Mapping)
        or parameters.get("execution_mode") != "explicit_orders"
    ):
        raise ValueError("融资信用账户必须声明 credit_account、account 及显式订单模式")
    if credit_account is not None:
        credit_account = parse_credit_account(credit_account)
    budget = PandasFrameBudget(max_memory_bytes)
    market = _read(market_root, 'market', ['date', 'code', 'open', 'close', 'high_limit', 'low_limit', 'paused'], budget, optional_columns=('visible_capacity',))
    targets = _read(target_root, 'targets', ['order_time', 'decision_time', 'target_json', 'intent_plan_hash'], budget, optional_columns=('target_hash',))
    benchmarks = _read(target_root, 'benchmarks', ['code', 'decision_time', 'price_units', 'price_scale', 'available_at'], budget)
    execution_mode = parameters.get("execution_mode", "target")
    encoded_commands = parameters.get("order_commands", "")
    if not isinstance(encoded_commands, str):
        raise ValueError("order_commands 必须是完整 JSON 字符串")
    commands = parse_order_commands([] if encoded_commands == "" else encoded_commands)
    encoded_cashflows = parameters.get("external_cashflows", "")
    if not isinstance(encoded_cashflows, str):
        raise ValueError("external_cashflows 必须是完整 JSON 数组字符串")
    cashflows = parse_external_cashflows([] if encoded_cashflows == "" else encoded_cashflows)
    if declaration is None and execution_mode == "explicit_orders" and not any(command.action == "submit" for command in commands):
        raise ValueError("explicit_orders 模式至少需要一条 submit 命令")
    if execution_mode not in {"target", "explicit_orders"}:
        raise ValueError("execution_mode 只能是 target 或 explicit_orders")
    if not targets.empty and commands:
        raise ValueError("正式日频参数禁止非空目标和显式命令同时执行")
    if execution_mode == "target" and commands:
        raise ValueError("target 模式不能携带显式命令")
    if execution_mode == "explicit_orders" and not targets.empty:
        raise ValueError("explicit_orders 模式不能携带非空目标")
    if benchmarks.duplicated(['code', 'decision_time']).any():
        raise ValueError('决策价格基准键重复')
    if declaration is not None:
        if not market.empty or not targets.empty or commands:
            raise ValueError("non_trading_sessions 只允许空行情、空目标且无订单命令")
        if account_model != "cash" or parameters.get("account") is None or credit_account is not None or cashflows:
            raise ValueError("non_trading_sessions 必须提供普通 cash 期初账户且不能携带信用或外部资金流")
        sessions = [date.fromisoformat(item) for item in declaration["dates"]]
    else:
        sessions = sorted(set(pd.to_datetime(market["date"]).dt.date))
    target_hashes = []
    for row in targets.itertuples(index=False):
        target = PortfolioTarget.from_dict(json.loads(row.target_json))
        decision, execution = pd.Timestamp(row.decision_time), pd.Timestamp(row.order_time)
        if (decision.tzinfo is None or execution.tzinfo is None or decision >= execution
                or target.decision_time != decision.to_pydatetime()
                or (hasattr(row, "target_hash") and target.target_hash != row.target_hash)):
            raise ValueError('目标身份或决策执行时点不一致')
        next_session = bisect_right(sessions, decision.tz_convert("Asia/Shanghai").date())
        if next_session == len(sessions) or sessions[next_session] != execution.tz_convert("Asia/Shanghai").date():
            raise ValueError("目标必须在行情日历的下一交易日开盘执行")
        target_hashes.append(target.target_hash)
    targets["target_hash"] = target_hashes
    history = parameters.get("market_rules")
    profile = None
    if history is not None:
        if any(name in parameters for name in (
            "market_rule_profile_id", "bond_etf_codes", "equity_etf_codes",
            "commission_ppm", "min_commission_units",
        )):
            raise ValueError("历史规则快照与 ETF profile/固定费用参数不能同时声明")
        rules = _historical_rule_snapshots(history, parameters["instrument_codes"], market, non_trading_sessions=declaration)
        asset_class = next(iter(rules.values()))[0].market
        engine_rules = {"rule_by_code": {code: snapshots[0] for code, snapshots in rules.items()},
                        "rule_history_by_code": rules}
    else:
        profile = resolve_cn_etf_daily_market_rule_profile(parameters['market_rule_profile_id'])
        rules = build_cn_etf_daily_rule_snapshots(
            instrument_codes=tuple(parameters['instrument_codes']), bond_etf_codes=tuple(parameters['bond_etf_codes']),
            equity_etf_codes=tuple(parameters['equity_etf_codes']), profile=profile,
            commission_ppm=parameters['commission_ppm'], min_commission_units=parameters['min_commission_units'],
        )
        asset_class = "cn_etf"
        engine_rules = {"rule_by_code": rules}
    rule_bundle = {
        code: [rule.to_dict() for rule in snapshots] if history is not None else snapshots.to_dict()
        for code, snapshots in sorted(rules.items())
    }
    actions = tuple(CorporateAction.from_dict(item) for item in parameters['corporate_actions'])
    result = run_daily_cash_event_simulation(
        market=market, intent_plans=targets, corporate_actions=actions, **engine_rules,
        initial_cash_cny=parameters['initial_cash_cny'], market_data_artifact_hash=market_artifact_hash,
        columns={name: name for name in market.columns},
        account=parameters.get("account"), execution_mode=execution_mode, order_commands=commands,
        external_cashflows=cashflows, credit_account=credit_account,
        non_trading_sessions=declaration,
        corporate_action_records=tuple(CorporateActionRecordPosition.from_dict(item)
                                       for item in parameters.get("corporate_action_records", ())),
    )
    uses_cash_context = credit_account is not None or bool(cashflows) or execution_mode == "explicit_orders" or profile is None or getattr(result, "account_context", None) is not None or bool(result.corporate_action_records) or any(action.contract_version == 2 for action in actions)
    if uses_cash_context and history is None:
        rule_bundle = {code: [rule.to_dict()] for code, rule in sorted(rules.items())}
    projection_intents = targets
    if execution_mode == "explicit_orders":
        credit_context = getattr(result, "credit_context", None)
        risk_commands = () if credit_context is None else parse_order_commands(credit_context["risk_commands"])
        by_order = {command.order_id: command for command in (*commands, *risk_commands) if command.action == "submit"}
        projection_intents = pd.DataFrame([{
            "target_hash": row["source_order_hash"],
            "decision_time": by_order[row["order_id"]].decision_time,
            "order_time": by_order[row["order_id"]].submitted_at,
        } for row in result.explicit_order_rows], columns=["target_hash", "decision_time", "order_time"])
    contract = project_cash_daily_result(
        result=result, intents=projection_intents, asset_class=asset_class,
        timeline_semantics={'decision': 'declared_command' if execution_mode == 'explicit_orders' else 'declared_target', 'execution': 'next_open', 'valuation': 'close'},
        fee_model_version='cash-fee-v1', calendar_id=parameters['calendar_id'], settlement_policy_id='cn.stock.t1.v1' if asset_class == 'cn_stock' else 'cn.etf.mixed-t0-t1.v1',
    )
    if execution_mode == "explicit_orders":
        contract = build_simulation_result_contract(
            tables={**contract.tables,
                "orders": canonical_simulation_table("orders", list(result.explicit_order_rows)),
                "fills": canonical_simulation_table("fills", list(result.explicit_fill_rows)),
                "costs": canonical_simulation_table("costs", list(result.explicit_cost_rows))},
            semantics=replace(contract.semantics, decision_time_convention="declared_explicit_order_time"),
            source_simulation_hash=result.simulation_hash, order_lifecycle=result.order_lifecycle,
            account_valuation_adjustments=contract.account_valuation_adjustments)
    ledger_hash = typed_canonical_hash(result.ledger.to_json(orient='records', date_format='iso', double_precision=15))
    root = Path(output_root)
    financial_context = {
        "result": result, "profile": profile, "rules": rules,
        "bond_etf_codes": tuple(parameters.get("bond_etf_codes", ())),
        "equity_etf_codes": tuple(parameters.get("equity_etf_codes", ())),
        "commission_ppm": parameters.get("commission_ppm"),
        "min_commission_units": parameters.get("min_commission_units"),
        "source_ledger_hash": ledger_hash,
    }
    if uses_cash_context:
        financial_context.update(market=market, market_artifact_hash=market_artifact_hash)
    write_simulation_result_contract(
        contract, root / 'simulation/result-contract', daily_etf_context=financial_context,
    )
    lookup = {(str(row.code), pd.Timestamp(row.decision_time)): row for row in benchmarks.itertuples(index=False)}
    benchmark_rows = []
    for order in contract.tables['orders'].itertuples(index=False):
        if execution_mode == "explicit_orders":
            command = by_order[str(order.order_id)]
            if command.reference_price.scale != DAILY_CASH_PRICE_SCALE:
                raise ValueError("日频显式订单决策价格必须使用 price_scale=3")
            if command.reference_price_available_at > command.decision_time:
                raise ValueError("显式订单决策基准在决策时尚不可见")
            benchmark_rows.append({"portfolio_id": order.portfolio_id, "order_id": order.order_id,
                "decision_price_units": command.reference_price.units,
                "available_at": command.reference_price_available_at, "source_hash": command.command_hash})
            continue
        key = (str(order.instrument_id), pd.Timestamp(order.decision_time))
        if key not in lookup:
            raise ValueError('正式订单缺少决策时价格基准')
        observed = lookup[key]
        price = Price(observed.price_units, observed.price_scale, "CNY")
        if price.scale != DAILY_CASH_PRICE_SCALE:
            raise ValueError("日频组合决策价格必须显式使用 price_scale=3")
        benchmark_rows.append({'portfolio_id': order.portfolio_id, 'order_id': order.order_id,
            'decision_price_units': int(price.units), 'available_at': observed.available_at,
            'source_hash': target_artifact_hash})
    policy = BarTcaPolicy(asset_class=asset_class, bar_frequency='daily', impact_model='fixed_bps_v1',
        spread_slippage_bps=0, fixed_impact_bps=0, sqrt_impact_coefficient_bps=0, participation_cap_ppm=100000,
        delay_benchmark='decision_price_v1', rounding_rule='price_half_up_cost_ceil_v1', contract_multiplier=1,
        price_scale=DAILY_CASH_PRICE_SCALE, available_at=datetime.fromisoformat(parameters['policy_available_at']),
        rule_snapshot_hash=typed_canonical_hash(rule_bundle),
        claim_ceiling='analysis_only')
    tca, tca_manifest = execute_simulation_result_bar_tca(simulation_result=contract,
        decision_benchmarks=pd.DataFrame(benchmark_rows, columns=['portfolio_id','order_id','decision_price_units','available_at','source_hash']),
        execution_observations=(None if execution_mode == "target" or "visible_capacity" not in market else pd.DataFrame(
            result.explicit_execution_observations, columns=[
                "source_fill_id", "arrival_price_units", "arrival_price_available_at",
                "visible_capacity", "capacity_available_at"])),
        policy=policy, source_ledger_hash=ledger_hash, output_root=root)
    metrics = result.metrics.copy()
    metrics['sample_start'] = sessions[0].isoformat()
    metrics['sample_end'] = sessions[-1].isoformat()
    metrics['sample_size'] = len(result.nav)
    metrics['status'] = 'computed'
    folder = root / 'simulation/metrics'
    folder.mkdir(parents=True)
    pq.write_table(pa.Table.from_pandas(metrics, preserve_index=False), folder / 'part-00000.parquet')
    payload = {'simulation_result_hash': contract.result_hash, 'source_ledger_hash': ledger_hash,
        'target_artifact_hash': target_artifact_hash, 'market_artifact_hash': market_artifact_hash,
        'claim_level': 'research_observation', **tca_metadata(tca, tca_manifest)}
    (root / 'result.json').write_text(canonical_json(payload), encoding='utf-8')
    return payload


def _historical_rule_snapshots(raw, instrument_codes, market, *, non_trading_sessions=None):
    """绑定逐标的历史区间，禁止缺失日期回退到当前规则。"""
    codes = tuple(instrument_codes)
    if not codes or len(set(codes)) != len(codes):
        raise ValueError("历史规则必须声明唯一非空标的列表")
    if not isinstance(raw, Mapping) or set(raw) != set(codes) or (non_trading_sessions is None and set(market["code"]) != set(codes)):
        raise ValueError("历史规则、声明标的与行情必须精确对应")
    fields = {"rule_id", "version", "market", "instrument_type", "effective_start",
              "effective_end", "available_time", "official_source_id", "evidence_url", "parameters"}
    rules = {}
    for code in sorted(codes):
        entries = raw[code]
        if not isinstance(entries, (list, tuple)) or not entries:
            raise ValueError("每个标的的历史规则必须是非空列表")
        snapshots = []
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != fields:
                raise ValueError(f"历史规则 schema 无效: {code}")
            rule = MarketRuleSnapshot(
                rule_id=entry["rule_id"], version=entry["version"], market=entry["market"],
                instrument_type=entry["instrument_type"],
                effective_start=date.fromisoformat(entry["effective_start"]),
                effective_end=None if entry["effective_end"] is None else date.fromisoformat(entry["effective_end"]),
                available_time=datetime.fromisoformat(entry["available_time"]),
                official_source_id=entry["official_source_id"], evidence_url=entry["evidence_url"],
                parameters=tuple((item[0], item[1]) for item in entry["parameters"]),
            )
            if (rule.market, rule.instrument_type) not in {("cn_stock", "stock"), ("cn_etf", "etf")}:
                raise ValueError("日频历史规则仅支持股票与 ETF")
            if dict(rule.parameters).get("source_instrument_id") != code:
                raise ValueError("历史规则必须明确绑定 source_instrument_id")
            if rule.market == "cn_stock" and "stock_board" not in dict(rule.parameters):
                raise ValueError("正式股票历史规则必须声明板块、上市阶段和交易状态")
            snapshots.append(rule)
        snapshots.sort(key=lambda rule: (rule.effective_start, rule.available_time, rule.rule_id, rule.version))
        for left, right in zip(snapshots, snapshots[1:]):
            if left.effective_end is None or left.effective_end >= right.effective_start:
                raise ValueError(f"历史规则有效区间重叠: {code}")
        sessions = (tuple(date.fromisoformat(item) for item in non_trading_sessions["dates"])
                    if non_trading_sessions is not None else pd.to_datetime(market.loc[market["code"] == code, "date"]).dt.date)
        for session in sessions:
            preopen = datetime.combine(session, time(9, 15) if non_trading_sessions is not None else time(9, 30), ZoneInfo("Asia/Shanghai"))
            visible = [rule for rule in snapshots if rule.effective_start <= session
                       and (rule.effective_end is None or session <= rule.effective_end)
                       and rule.available_time <= preopen]
            if len(visible) != 1:
                raise ValueError(f"历史规则缺失或在开盘尚不可见: {code} {session}")
        rules[code] = tuple(snapshots)
    if len({rule.market for snapshots in rules.values() for rule in snapshots}) != 1:
        raise ValueError("同一日频现金账户不能混用股票与 ETF 市场规则")
    return rules
