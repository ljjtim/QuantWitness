"""把冻结组合目标接到 LEAN 日频执行和正式结果。"""
from research_pipeline.domain.lean_algorithm import LeanAlgorithm, LeanStrategyDeclaration, LeanDataBinding, LeanAlgorithmLimits
from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE, LEAN_SCHEMA_IDS, LEAN_PATHS, LEAN_METRICS_SCHEMA_ID, LEAN_METRICS_PATH


PORTFOLIO_METRICS = {
    'project.lean.total_return@2.0.0': ('区间总收益', 'ratio', 'higher_is_better'),
    'project.lean.max_drawdown@2.0.0': ('区间最大回撤', 'ratio', 'lower_is_better'),
    'project.lean.turnover_mean@2.0.0': ('日均双边换手率', 'ratio', 'neutral'),
    'project.lean.fee_total@2.0.0': ('成交费用合计', 'CNY', 'lower_is_better'),
}


def extend_portfolio(nodes, declarations, tables, design, node, declaration):
    execution = design['lean_execution']
    target_type = 'research.portfolio-targets.v1'
    instruments = [dict(instrument_id=code, asset_class='cn_etf', venue=code.rsplit('.', 1)[-1],
        currency='CNY', contract_kind='etf', contract_version='research-instrument-key-v1') for code in design['entities']]
    algorithm = LeanAlgorithm(language='Python', entry_file='algorithm.py', type_name='QlibPortfolioAlgorithm')
    strategy = LeanStrategyDeclaration(strategy_id='qlib.portfolio', bundle_id='pending', algorithm=algorithm,
        asset_class='cn_etf', frequency='1d',
        input_bindings=(LeanDataBinding(binding_id='prices', input_port='bars', request_id=execution['bars_request_id'],
            kind='bars', resolution='1d', columns=('fld_etf_date','fld_etf_code','fld_etf_open','fld_etf_high','fld_etf_low','fld_etf_close','fld_etf_volume'),
            daily_time=dict(rule='next_session_open', timezone='Asia/Shanghai', sessions=design['calendar_sessions'])),
            LeanDataBinding(binding_id='portfolio_targets', input_port='targets', request_id=None,
                kind='targets', resolution='event', columns=('weight_ppb', 'batch_id'))),
        limits=LeanAlgorithmLimits(initialize_seconds=60, time_step_seconds=60))
    parameters = dict(execution_backend='lean', execution_mode='strategy', frequency='1d', price_scale=3,
        execution_model='lean_next_session_open_v1', claim_ceiling='bar_level_research_only',
        strategy_declaration=strategy.to_dict(), instruments=instruments,
        bars_request_id=execution['bars_request_id'], open_request_id=execution['open_request_id'],
        minute_scope=execution['minute_scope'], rule_bundle_hash=execution['minute_scope']['rules_hash'],
        lean_runtime=execution['lean_runtime'], initial_cash_units=execution['initial_cash_units'],
        slippage_bps=execution['slippage_bps'], slippage_ticks=execution['slippage_ticks'],
        participation_ppm=execution['participation_ppm'])
    nodes.extend([
        node('portfolio_targets', 'project.qlib_demo.targets',
            [('selection', 'model_selection', 'selection'), ('data', 'data_plane', 'data')],
            {'design': design, 'price_request_id': 'daily_feature'}, '1.0.0'),
        node('portfolio_snapshot', 'data.minute.adjustment_snapshot', [('data', 'data_plane', 'data')],
            dict(factor_request_id=execution['factor_request_id'], corporate_action_request_id=execution['corporate_action_request_id'],
                instrument_ids=design['entities'], asset_class='cn_etf', as_of=execution['as_of'],
                applicable_start=execution['applicable_start'], applicable_end=execution['applicable_end'],
                availability_policy_ref=execution['availability_policy_ref'],
                **({'trading_calendar_request_id': execution['trading_calendar_request_id']}
                   if 'trading_calendar_request_id' in execution else {})), '1.0.0'),
        node('portfolio_simulation', 'project.qlib_demo.lean_portfolio',
            [('bars', 'data_plane', 'data'), ('snapshot', 'portfolio_snapshot', 'snapshot'),
             ('targets', 'portfolio_targets', 'targets')], parameters, '1.0.0'),
    ])
    strategy_parameters = [(name, 'string' if isinstance(value, str) else 'integer' if type(value) is int else 'json')
                           for name, value in parameters.items()]
    strategy_declaration = declaration('lean_portfolio',
        [('bars', 'data.columnar-bundle.v1'), ('snapshot', 'data.adjustment-factor-snapshot.v1'), ('targets', target_type)],
        [('simulation', LEAN_ARTIFACT_TYPE)], strategy_parameters, (target_type, LEAN_ARTIFACT_TYPE), module='algorithm', function='run')
    strategy_declaration['entry'] = algorithm.to_dict()
    strategy_declaration['operator']['resource_profile'].update(memory_bytes=4*1024**3, cpu_slots=1,
        process_slots=6, temp_bytes=1024**3, wall_seconds=3600)
    # 原生算法依赖由LEAN安装提供；模型训练依赖留在模型Worker。
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
