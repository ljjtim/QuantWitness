"""生成无需个人数据库的期货日线或一分钟完整研究包。"""
import argparse
import json
from pathlib import Path
import shutil
import yaml
from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.strategy_protocol import StrategyDeclaration, StrategyInputBinding, StrategyLimits
from research_pipeline.extensions import OperatorSpec, PortSpec, ParameterSpec, ParameterType, compile_project_operator_bundle, project_source_hash
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE
from research_pipeline.platform.minute_reference import load_minute_capability_manifest
from research_pipeline.platform import typed_canonical_hash
from inputs import prepare_inputs, MINUTE_AVAILABILITY
from scope import write_scope
import synthetic


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding='utf-8')


def node(name, operator, parameters, inputs=()):
    return dict(node_id=name, operator_id=operator, operator_version='1.0.0', parameters=parameters,
        inputs=[dict(input_port=p, source_node_id=n, source_output_port=o) for p, n, o in inputs], strategies=[])


def build(output, frequency):
    root = Path(output).resolve()
    if root.exists():
        raise ValueError('输出目录必须不存在')
    root.mkdir(parents=True)
    package = root/'package'
    reference = write_scope(package)
    requests, archive = prepare_inputs(root, frequency)
    instruments = [InstrumentKey(code, 'cn_future', 'CCFX', 'CNY', 'future').to_dict() for code in synthetic.CODES]
    declaration = StrategyDeclaration(strategy_id='public.lean.futures', bundle_id='pending',
        state_schema_id='public.lean.futures.state.v1', subscriptions=('bar',), schedules=(), asset_class='cn_future', frequency=frequency,
        input_bindings=(StrategyInputBinding(binding_id='prices', columns=('close',), lookback_bars=1),),
        limits=StrategyLimits(max_state_bytes=4096, max_context_bytes=8*1024**2, max_response_bytes=65536, max_intents_per_event=4, callback_wall_seconds=30))
    parameters = dict(execution_mode='strategy', frequency=frequency, price_scale=0,
        execution_model='lean_next_session_open_v1' if frequency == '1d' else 'lean_next_completed_close_v1',
        claim_ceiling='bar_level_research_only', strategy_declaration=declaration.to_dict(), instruments=instruments,
        minute_scope=reference, rule_bundle_hash=reference['rules_hash'], initial_cash_units=10000000,
        slippage_bps=0, slippage_ticks=0, participation_ppm=1000000)
    if frequency == '1d':
        parameters.update(bars_request_id='bars', open_request_id='opening')
    source = root/'strategy-source'
    source.mkdir()
    shutil.copyfile(Path(__file__).with_name('strategy.py'), source/'strategy.py')
    spec = OperatorSpec.build(operator_id='project.public.lean_futures', operator_version='1.0.0',
        input_ports=(PortSpec('bars', 'data.columnar-bundle.v1' if frequency == '1d' else 'data.minute-bars.v1'),),
        output_ports=(PortSpec('simulation', LEAN_ARTIFACT_TYPE),),
        parameters=tuple(ParameterSpec(key, ParameterType.STRING if isinstance(value, str) else ParameterType.INTEGER if type(value) is int else ParameterType.JSON) for key, value in parameters.items()),
        strategy_roles=(), resource_profile=dict(memory_bytes=12*1024**3, cpu_slots=4, process_slots=6, temp_bytes=32*1024**3, wall_seconds=120),
        determinism_mode='deterministic', seed_policy='none', code_hash=project_source_hash(source), pit_capabilities=('pit.as_of.v1',))
    bundle = compile_project_operator_bundle(source_root=source, output_root=root/'bundles', project_id='public-lean-futures',
        operator_spec=spec, entry_module='strategy', entry_function='run', dependency_lock={},
        registered_operator_specs=build_mainline_operator_registry().operator_specs, project_artifact_types=(LEAN_ARTIFACT_TYPE,))
    parameters['strategy_declaration']['bundle_id'] = bundle.name
    nodes = [node('data', 'data.columnar.materialize', {})] if frequency == '1d' else [
        node('scan', 'data.minute.scan', dict(request_ids=['minute_bars'],
            scope_binding_hash=load_minute_capability_manifest().manifest_hash, max_source_bytes=16*1024**2,
            max_returned_rows=5000, max_batch_bytes=16*1024**2, availability_policy_ref=MINUTE_AVAILABILITY)),
        node('bars', 'research.bars.minute_resample', dict(interval_minutes=1, session_policy_ref='session.public.synthetic.future.v1',
            quality_policy_refs=['quality.minute.common_fields.v1'], adjustment_mode='none', asset_class='cn_future',
            availability_policy_ref=MINUTE_AVAILABILITY, minute_scope=reference), [('minute_1m', 'scan', 'minute_1m')])]
    nodes.append(node('strategy', spec.operator_id, parameters, [('bars', 'data', 'data') if frequency == '1d' else ('bars', 'bars', 'bars')]))
    save(package/'package.yaml', dict(package_slug='lean_futures_'+frequency, display_name='公开LEAN双向期货示例', package_version='1.0.0', builder_id='operator_graph_plan_v1',
        metric_contract=dict(contract_id='public.lean.futures.metrics', version='2.0.0', metrics=['project.lean.total_return@2.0.0'],
            semantics={'project.lean.total_return@2.0.0': 'last_equity_div_initial_equity_minus_one'}),
        claim_contract=dict(allowed_claim_levels=['research_observation'], max_claim_level='research_observation')))
    save(package/'sources/sources.yaml', {'sources': [dict(source_id='public_synthetic', source_type='dataset', title='公开确定性合成期货行情与规则',
        url='https://github.com/ljjtim/QuantWitness', accessed_at='2024-01-05', content_hash=typed_canonical_hash({'days': synthetic.DAYS, 'codes': synthetic.CODES}),
        license_id='Apache-2.0', status='available', limitation='合成价格与规则，仅验证双向期货执行合同',
        provenance=dict(mode='citation_only', content_digest=None, media_type=None, snapshot_artifact_id=None, snapshot_manifest_hash=None,
            importer_id=None, imported_at=None, contract_version='research-source-provenance-v1'))]})
    save(package/'localization.yaml', {'decisions': [dict(decision_id='synthetic', original_assumption='历史市场行情与规则', local_market='cn_future',
        local_adaptation='预先定义的合成市场及人民币期货账户', evidence_source_ids=['public_synthetic'], status='proxy', claim_effect='downgrade_to_research_observation')]})
    from research_pipeline.platform.lean_contract import LEAN_SCHEMA_IDS, LEAN_PATHS
    tables = [dict(table_id='lean_'+name, role='primary' if name == 'daily' else 'diagnostic', source_node_id='strategy', source_port='simulation',
        artifact_type=LEAN_ARTIFACT_TYPE, schema_id=schema, path_prefix=LEAN_PATHS[name]) for name, schema in LEAN_SCHEMA_IDS.items()]
    save(package/'spec/research.yaml', dict(contract_version='research-operator-graph-package-v2', research_id='public.lean.futures.'+frequency,
        as_of=synthetic.AS_OF, root_seed=1703, fixed_clock=synthetic.CLOCK, requests=requests,
        result=dict(contract_version='research-result-spec-v1', tables=tables),
        graph=dict(contract_version='research-operator-graph-recipe-v1', graph_id='public.lean.futures', nodes=nodes)))
    from research_pipeline.packages import load_research_package
    load_research_package(package)
    summary = dict(package=str(package), bundles=[str(bundle)], catalog_lock=str(root/'catalog'), input_snapshot_manifest=str(archive),
        minute_root=str(root/'minute') if frequency == '1m' else None, frequency=frequency, fixed_clock=synthetic.CLOCK)
    (root/'generated.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--frequency', choices=('1d', '1m'), default='1d')
    args = parser.parse_args()
    print(json.dumps(build(args.output, args.frequency), ensure_ascii=False, indent=2))
