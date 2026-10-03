"""把固定组合和日频现金引擎加入公开模型研究声明。"""
from research_pipeline.results import BAR_TCA_SCHEMA_IDS, CANONICAL_SIMULATION_SCHEMA_IDS


def extend_portfolio(nodes, declarations, tables, design, node, declaration):
    target_type = 'research.portfolio-targets.v1'
    market_type = 'data.daily-market.v1'
    simulation_type = 'research.daily-simulation.v1'
    finance = design.get('finance', {})
    simulation_parameters = {'market_rule_profile_id': 'cn_etf.daily.curated.v1',
        'instrument_codes': list(design['entities']), 'bond_etf_codes': [],
        'equity_etf_codes': list(design['entities']), 'commission_ppm': 300,
        'min_commission_units': 5, 'corporate_actions': [], 'initial_cash_cny': 100000.,
        'calendar_id': design.get('calendar_id', 'public_synthetic_weekdays'),
        'policy_available_at': design['calendar_sessions'][0] + 'T00:00:00+08:00'}
    simulation_parameters.update({key: value for key, value in finance.items()
        if key not in {'classification_evidence', 'corporate_action_evidence', 'price_scale'}})
    nodes.extend([
        node('portfolio_targets', 'project.qlib_demo.targets',
            [('selection', 'model_selection', 'selection'), ('data', 'data_plane', 'data')],
            {'design': design, 'price_request_id': 'daily_feature'}, '1.0.0'),
        node('portfolio_market', 'project.qlib_demo.market',
            [('targets', 'portfolio_targets', 'targets'), ('data', 'data_plane', 'data')], {'price_request_id': 'daily_feature', 'design': design}, '1.0.0'),
        node('portfolio_simulation', 'finance.simulation.daily-cash',
            [('targets', 'portfolio_targets', 'targets'), ('market', 'portfolio_market', 'market')],
            simulation_parameters, '1.0.0'),
    ])
    declarations.extend([
        ('targets', declaration('targets', [('selection', 'research.model-selection.v2'), ('data', 'data.columnar-bundle.v1')],
            [('targets', target_type)], [('design', 'json'), ('price_request_id', 'string')], module='portfolio', function='targets')),
        ('market', declaration('market', [('targets', target_type), ('data', 'data.columnar-bundle.v1')],
            [('market', market_type)], [('price_request_id', 'string'), ('design', 'json')], module='portfolio', function='market')),
    ])
    for table_id, source, port, kind, prefix, schema in [
        ('portfolio_targets', 'portfolio_targets', 'targets', target_type, 'targets', 'project.qlib_demo.portfolio-targets.v1'),
        ('decision_benchmarks', 'portfolio_targets', 'targets', target_type, 'benchmarks', 'project.qlib_demo.decision-benchmarks.v1'),
        ('execution_market', 'portfolio_market', 'market', market_type, 'market', 'project.qlib_demo.execution-market.v1'),
        ('portfolio_metrics', 'portfolio_simulation', 'simulation', simulation_type, 'simulation/metrics', 'research.daily-simulation.metrics.v1'),
        *[(f'canonical_{name}', 'portfolio_simulation', 'simulation', simulation_type, f'simulation/result-contract/{name}', schema)
          for name, schema in CANONICAL_SIMULATION_SCHEMA_IDS.items()],
        *[(f'tca_{name}', 'portfolio_simulation', 'simulation', simulation_type, f'simulation/tca/{name}', schema)
          for name, schema in BAR_TCA_SCHEMA_IDS.items()],
    ]:
        tables.append({'table_id': table_id, 'role': 'metrics' if table_id == 'portfolio_metrics' else 'diagnostic', 'source_node_id': source,
            'source_port': port, 'artifact_type': kind, 'schema_id': schema, 'path_prefix': prefix})
    validity = next(item for item in nodes if item['node_id'] == 'validity')
    validity['inputs'].append({'input_port': 'simulation', 'source_node_id': 'portfolio_simulation', 'source_output_port': 'simulation'})
    validity_declaration = next(payload for name, payload in declarations if name == 'validity')
    validity_declaration['operator']['input_ports'].append({'port': 'simulation', 'artifact_type': simulation_type})


PORTFOLIO_METRICS = {
    'portfolio.total_return@1.0.0': ('区间总收益', 'decimal_return', 'higher_is_better'),
    'portfolio.max_drawdown@1.0.0': ('区间最大回撤', 'decimal_return', 'higher_is_better'),
    'portfolio.turnover@1.0.0': ('累计成交金额与初始现金之比', 'ratio', 'lower_is_better'),
    'portfolio.transaction_cost@1.0.0': ('总成交费用', 'CNY', 'lower_is_better'),
}


def metric_definitions(verifier_path):
    import hashlib
    from research_pipeline.platform.metric_contracts import MetricDefinition
    return tuple(MetricDefinition.build(metric_id=ref.split('@')[0], version='1.0.0',
        input_artifact_type='research.daily-simulation.v1', result_schema_id='research.daily-simulation.metrics.v1',
        output_schema={'value': 'float64'}, unit=unit, frequency='daily', annualization_policy='none',
        risk_free_rate_policy='not_applicable', null_policy='forbid', direction=direction,
        implementation_ref='public.qlib.independent_portfolio',
        implementation_digest=hashlib.sha256(verifier_path.read_bytes()).hexdigest(),
        measurement_semantics={'quantity': ref.split('@')[0], 'observation_timing': 'after_execution_session_close',
            'aggregation': 'whole_simulation_interval',
            'numerator': {'portfolio.total_return@1.0.0': 'final_nav_minus_initial_cash',
                'portfolio.max_drawdown@1.0.0': 'minimum_nav_minus_running_peak',
                'portfolio.turnover@1.0.0': 'sum_fill_notional',
                'portfolio.transaction_cost@1.0.0': 'sum_fill_fees'}[ref],
            'denominator': 'one' if ref == 'portfolio.transaction_cost@1.0.0' else 'running_peak_nav' if ref == 'portfolio.max_drawdown@1.0.0' else 'initial_cash'})
        for ref, (_, unit, direction) in PORTFOLIO_METRICS.items())
