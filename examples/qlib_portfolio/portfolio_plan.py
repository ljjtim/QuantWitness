"""把冻结组合目标接到 LEAN 日频执行和正式结果。"""
from research_pipeline.domain.strategy_protocol import StrategyDeclaration, StrategyInputBinding, StrategyLimits
from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE, LEAN_SCHEMA_IDS, LEAN_PATHS, LEAN_METRICS_SCHEMA_ID, LEAN_METRICS_PATH


PORTFOLIO_METRICS = {
    'project.lean.total_return@1.0.0': ('区间总收益', 'ratio', 'higher_is_better'),
    'project.lean.max_drawdown@1.0.0': ('区间最大回撤', 'ratio', 'lower_is_better'),
    'project.lean.turnover_mean@1.0.0': ('日均双边换手率', 'ratio', 'neutral'),
    'project.lean.fee_total@1.0.0': ('成交费用合计', 'CNY', 'lower_is_better'),
}


def extend_portfolio(nodes, declarations, tables, design, node, declaration):
    execution = design['lean_execution']
    target_type = 'research.portfolio-targets.v1'
    instruments = [dict(instrument_id=code, asset_class='cn_etf', venue=code.rsplit('.', 1)[-1],
        currency='CNY', contract_kind='etf', contract_version='research-instrument-key-v1') for code in design['entities']]
    strategy = StrategyDeclaration(strategy_id='qlib.portfolio', bundle_id='pending', state_schema_id='qlib.portfolio.state.v1',
        subscriptions=('bar', 'execution_feedback'), schedules=(), asset_class='cn_etf', frequency='1d',
        input_bindings=(StrategyInputBinding(binding_id='prices', columns=('close',), lookback_bars=1),
            StrategyInputBinding(binding_id='portfolio_targets', source='signals', columns=('weight_ppb',), lookback_bars=1)),
        limits=StrategyLimits(max_state_bytes=65536, max_context_bytes=4*1024**2,
            max_response_bytes=1024**2, max_intents_per_event=max(32, len(instruments)*2), callback_wall_seconds=60))
    parameters = dict(execution_backend='lean', execution_mode='strategy', frequency='1d', price_scale=3,
        execution_model='lean_next_session_open_v1', claim_ceiling='bar_level_research_only',
        strategy_declaration=strategy.to_dict(), instruments=instruments,
        bars_request_id=execution['bars_request_id'], open_request_id=execution['open_request_id'],
        minute_scope=execution['minute_scope'], rule_bundle_hash=execution['minute_scope']['rules_hash'],
        lean_runtime=execution['lean_runtime'], initial_cash_units=execution['initial_cash_units'],
        slippage_bps=execution['slippage_bps'], slippage_ticks=execution['slippage_ticks'],
        participation_ppm=execution['participation_ppm'],
        strategy_worker_projection={'parameters': ['strategy_declaration', 'frequency', 'price_scale'],
            'rule_ids': ['rule.cn_fund.lot_size.v1']})
    nodes.extend([
        node('portfolio_targets', 'project.qlib_demo.targets',
            [('selection', 'model_selection', 'selection'), ('data', 'data_plane', 'data')],
            {'design': design, 'price_request_id': 'daily_feature'}, '1.0.0'),
        node('portfolio_snapshot', 'data.minute.adjustment_snapshot', [('data', 'data_plane', 'data')],
            dict(factor_request_id=execution['factor_request_id'], corporate_action_request_id=execution['corporate_action_request_id'],
                instrument_ids=design['entities'], asset_class='cn_etf', as_of=execution['as_of'],
                applicable_start=execution['applicable_start'], applicable_end=execution['applicable_end'],
                availability_policy_ref=execution['availability_policy_ref']), '1.0.0'),
        node('portfolio_simulation', 'project.qlib_demo.lean_portfolio',
            [('bars', 'data_plane', 'data'), ('snapshot', 'portfolio_snapshot', 'snapshot'),
             ('targets', 'portfolio_targets', 'targets')], parameters, '1.0.0'),
    ])
    strategy_parameters = [(name, 'string' if isinstance(value, str) else 'integer' if type(value) is int else 'json')
                           for name, value in parameters.items()]
    strategy_declaration = declaration('lean_portfolio',
        [('bars', 'data.columnar-bundle.v1'), ('snapshot', 'data.adjustment-factor-snapshot.v1'), ('targets', target_type)],
        [('simulation', LEAN_ARTIFACT_TYPE)], strategy_parameters, (target_type, LEAN_ARTIFACT_TYPE), module='lean_strategy', function='run')
    strategy_declaration['operator']['resource_profile'].update(memory_bytes=4*1024**3, cpu_slots=1,
        process_slots=6, temp_bytes=1024**3, wall_seconds=3600)
    # LEAN回调只运行标准库策略；模型训练依赖留在模型Worker。
    strategy_declaration['dependency_lock'] = {}
    declarations.extend([
        ('targets', declaration('targets', [('selection', 'research.model-selection.v2'), ('data', 'data.columnar-bundle.v1')],
            [('targets', target_type)], [('design', 'json'), ('price_request_id', 'string')], (target_type,), module='portfolio', function='targets')),
        ('lean_portfolio', strategy_declaration),
    ])
    outputs = [
        ('portfolio_targets', 'portfolio_targets', 'targets', target_type, 'targets', 'project.qlib_demo.portfolio-targets.v1'),
        ('decision_benchmarks', 'portfolio_targets', 'targets', target_type, 'benchmarks', 'project.qlib_demo.decision-benchmarks.v1'),
        ('portfolio_metrics', 'portfolio_simulation', 'simulation', LEAN_ARTIFACT_TYPE, LEAN_METRICS_PATH, LEAN_METRICS_SCHEMA_ID),
        *[('portfolio_'+name, 'portfolio_simulation', 'simulation', LEAN_ARTIFACT_TYPE, LEAN_PATHS[name], schema)
          for name, schema in LEAN_SCHEMA_IDS.items()],
    ]
    for table_id, source, port, kind, prefix, schema in outputs:
        tables.append(dict(table_id=table_id, role='metrics' if table_id == 'portfolio_metrics' else 'diagnostic',
            source_node_id=source, source_port=port, artifact_type=kind, schema_id=schema, path_prefix=prefix))


def metric_definitions():
    # 核心按策略频率注入正式LEAN指标，项目只声明所消费的指标引用。
    return ()
