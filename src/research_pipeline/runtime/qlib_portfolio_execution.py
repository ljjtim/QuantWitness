"""把显式日频目标接入既有现金引擎、规范账本和独立复核上下文。"""
from bisect import bisect_right
from datetime import datetime
from pathlib import Path
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.domain import CorporateAction, PortfolioTarget, Price
from research_pipeline.domain.rules import build_cn_etf_daily_rule_snapshots
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.market_rule_defaults import resolve_cn_etf_daily_market_rule_profile
from research_pipeline.research.dataframe_budget import PandasFrameBudget, parquet_uncompressed_bytes
from research_pipeline.simulation import BarTcaPolicy
from research_pipeline.simulation.daily_event import DAILY_CASH_PRICE_SCALE, run_daily_cash_event_simulation
from research_pipeline.simulation.result_contract import project_cash_daily_result, write_simulation_result_contract
from .bar_tca_adapter import execute_simulation_result_bar_tca, tca_metadata


def _read(root, prefix, columns, budget, optional_columns=()):
    paths = sorted((Path(root) / prefix).glob('*.parquet'))
    if parquet_uncompressed_bytes(paths, columns=columns) > budget.max_memory_bytes:
        raise ValueError('日频仿真输入超过节点内存预算')
    def batches():
        for path in paths:
            parquet = pq.ParquetFile(path)
            selected = [*columns, *(name for name in optional_columns if name in parquet.schema_arrow.names)]
            yield from parquet.iter_batches(columns=selected, batch_size=8192)
    return budget.collect_arrow_batches(batches(), label=prefix)


def execute_daily_cash_artifact(*, target_root, market_root, parameters, output_root,
                                max_memory_bytes, market_artifact_hash, target_artifact_hash):
    """读取显式输入端口；当前适用范围为声明了分类和成本的ETF日频研究。"""
    budget = PandasFrameBudget(max_memory_bytes)
    market = _read(market_root, 'market', ['date', 'code', 'open', 'close', 'high_limit', 'low_limit', 'paused'], budget)
    targets = _read(target_root, 'targets', ['order_time', 'decision_time', 'target_json', 'intent_plan_hash'], budget, optional_columns=('target_hash',))
    benchmarks = _read(target_root, 'benchmarks', ['code', 'decision_time', 'price_units', 'price_scale', 'available_at'], budget)
    if benchmarks.duplicated(['code', 'decision_time']).any():
        raise ValueError('决策价格基准键重复')
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
    profile = resolve_cn_etf_daily_market_rule_profile(parameters['market_rule_profile_id'])
    rules = build_cn_etf_daily_rule_snapshots(
        instrument_codes=tuple(parameters['instrument_codes']), bond_etf_codes=tuple(parameters['bond_etf_codes']),
        equity_etf_codes=tuple(parameters['equity_etf_codes']), profile=profile,
        commission_ppm=parameters['commission_ppm'], min_commission_units=parameters['min_commission_units'],
    )
    actions = tuple(CorporateAction.from_dict(item) for item in parameters['corporate_actions'])
    result = run_daily_cash_event_simulation(
        market=market, intent_plans=targets, corporate_actions=actions, rule_by_code=rules,
        initial_cash_cny=parameters['initial_cash_cny'], market_data_artifact_hash=market_artifact_hash,
        columns={name: name for name in market.columns},
    )
    contract = project_cash_daily_result(
        result=result, intents=targets, asset_class='cn_etf',
        timeline_semantics={'decision': 'declared_target', 'execution': 'next_open', 'valuation': 'close'},
        fee_model_version='cash-fee-v1', calendar_id=parameters['calendar_id'], settlement_policy_id='cn.etf.mixed-t0-t1.v1',
    )
    ledger_hash = typed_canonical_hash(result.ledger.to_json(orient='records', date_format='iso', double_precision=15))
    root = Path(output_root)
    write_simulation_result_contract(contract, root / 'simulation/result-contract', daily_etf_context={
        'result': result, 'profile': profile, 'rules': rules,
        'bond_etf_codes': tuple(parameters['bond_etf_codes']), 'equity_etf_codes': tuple(parameters['equity_etf_codes']),
        'commission_ppm': parameters['commission_ppm'], 'min_commission_units': parameters['min_commission_units'],
        'source_ledger_hash': ledger_hash,
    })
    lookup = {(str(row.code), pd.Timestamp(row.decision_time)): row for row in benchmarks.itertuples(index=False)}
    benchmark_rows = []
    for order in contract.tables['orders'].itertuples(index=False):
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
    policy = BarTcaPolicy(asset_class='cn_etf', bar_frequency='daily', impact_model='fixed_bps_v1',
        spread_slippage_bps=0, fixed_impact_bps=0, sqrt_impact_coefficient_bps=0, participation_cap_ppm=100000,
        delay_benchmark='decision_price_v1', rounding_rule='price_half_up_cost_ceil_v1', contract_multiplier=1,
        price_scale=DAILY_CASH_PRICE_SCALE, available_at=datetime.fromisoformat(parameters['policy_available_at']),
        rule_snapshot_hash=typed_canonical_hash({code: rule.to_dict() for code, rule in sorted(rules.items())}),
        claim_ceiling='analysis_only')
    tca, tca_manifest = execute_simulation_result_bar_tca(simulation_result=contract,
        decision_benchmarks=pd.DataFrame(benchmark_rows, columns=['portfolio_id','order_id','decision_price_units','available_at','source_hash']),
        policy=policy, source_ledger_hash=ledger_hash, output_root=root)
    metrics = result.metrics.copy()
    metrics['sample_start'] = min(market['date']).isoformat()
    metrics['sample_end'] = max(market['date']).isoformat()
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
