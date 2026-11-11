"""合成Parquet经ResearchPackage准入、Runtime和正式Result交付自定义指标。"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import yaml

from research_pipeline.platform.metric_contracts import MetricDefinition
from research_pipeline.results import load_result_context, render_result_report

if __package__:
    from .inputs import prepare_inputs
    from .source.operator import ARTIFACT, SCHEMA, METRIC_REF, METRIC_SCHEMA, calculate_metric
else:
    from inputs import prepare_inputs
    from source.operator import ARTIFACT, SCHEMA, METRIC_REF, METRIC_SCHEMA, calculate_metric

HERE = Path(__file__).resolve().parent
FRAMEWORK = HERE.parents[1]
CLOCK = '2024-01-09T00:00:00+08:00'


def definition() -> MetricDefinition:
    """完整测量语义同时说明分子、分母和观察时点。"""
    return MetricDefinition.build(
        metric_id='project.custom_metric.mean_return', version='1.0.0',
        input_artifact_type=ARTIFACT, result_schema_id=SCHEMA,
        output_schema={'value': 'float64'}, unit='decimal_return', frequency='daily',
        annualization_policy='none', risk_free_rate_policy='not_applicable',
        null_policy='forbid', direction='neutral',
        implementation_ref='project.custom_metric.mean.run',
        implementation_digest=hashlib.sha256((HERE / 'source/operator.py').read_bytes()).hexdigest(),
        measurement_semantics={
            'quantity': 'mean_observed_return', 'numerator': 'sum_observed_returns',
            'denominator': 'observed_session_count',
            'observation_timing': 'after_each_observed_session_close', 'aggregation': 'arithmetic_mean',
        },
    )


def save_yaml(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding='utf-8')


def cli(arguments):
    completed = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-m', 'research_pipeline', *map(str, arguments)],
                               cwd=FRAMEWORK, capture_output=True, text=True, encoding='utf-8', timeout=180)
    if completed.returncode:
        raise RuntimeError(completed.stdout + completed.stderr)
    return json.loads(completed.stdout)


def prepare(output):
    request, archive = prepare_inputs(output)
    for field in ('ir_version', 'as_of'):
        request.pop(field, None)
    source = HERE / 'source'
    operator = {
        'contract_version': 'project-operator-declaration-v2', 'project_id': 'custom-metric-example',
        'project_artifact_types': [ARTIFACT],
        'operator': {'contract_version': 'research-operator-contract-v1',
            'operator_id': 'project.custom_metric.mean', 'operator_version': '1.0.0',
            'input_ports': [{'port': 'data', 'artifact_type': 'data.columnar-bundle.v1'}],
            'output_ports': [{'port': 'result', 'artifact_type': ARTIFACT}],
            'parameters': [{'name': 'data_request_id', 'value_type': 'string', 'required': True, 'allowed_values': []}],
            'strategy_roles': [], 'resource_profile': {'memory_bytes': 1024**3, 'cpu_slots': 1,
                'process_slots': 4, 'temp_bytes': 64*1024**2, 'wall_seconds': 60},
            'determinism_mode': 'deterministic', 'seed_policy': 'none', 'pit_capabilities': ['pit.as_of.v1']},
        'entry': {'module': 'operator', 'function': 'run'}, 'dependency_lock': {'pyarrow': '21.0.0'},
        'permissions': {'artifact_write_scope': 'output_only'},
    }
    save_yaml(output / 'operator.yaml', operator)
    bundle = cli(['operator', 'build', '--spec', output/'operator.yaml', '--source', source,
                  '--output', output/'bundle', '--format', 'json'])['data']['bundle_path']
    metric = definition()
    package = output / 'package'
    save_yaml(package/'package.yaml', {'package_slug': 'custom_metric_example', 'display_name': '自定义观察均值指标',
        'package_version': '1.0.0', 'builder_id': 'operator_graph_plan_v1',
        'metric_contract': {'contract_id': 'custom_metric', 'version': '1.0.0', 'metrics': [METRIC_REF],
                            'semantics': {METRIC_REF: '三个合成观察交易日收益的算术均值'}},
        'claim_contract': {'allowed_claim_levels': ['research_observation'], 'max_claim_level': 'research_observation'}})
    save_yaml(package/'spec/research.yaml', {'contract_version': 'research-operator-graph-package-v2',
        'research_id': 'custom.metric.example', 'as_of': '2024-01-08', 'root_seed': 0, 'fixed_clock': CLOCK,
        'requests': [request], 'metric_definitions': [metric.to_dict()],
        'result': {'contract_version': 'research-result-spec-v1', 'tables': [
            {'table_id': name, 'role': 'primary' if name == 'metrics' else 'diagnostic',
             'source_node_id': 'metric', 'source_port': 'result', 'artifact_type': ARTIFACT,
             'schema_id': SCHEMA if name == 'metrics' else 'project.custom_metric.observations.v1',
             'path_prefix': 'result/' + name} for name in ('metrics', 'observations')]},
        'graph': {'contract_version': 'research-operator-graph-recipe-v1', 'graph_id': 'custom.metric.example', 'nodes': [
            {'node_id': 'data_plane', 'operator_id': 'data.columnar.materialize', 'operator_version': '1.0.0', 'inputs': [], 'parameters': {}, 'strategies': []},
            {'node_id': 'metric', 'operator_id': 'project.custom_metric.mean', 'operator_version': '1.0.0',
             'inputs': [{'input_port': 'data', 'source_node_id': 'data_plane', 'source_output_port': 'data'}],
             'parameters': {'data_request_id': 'observations'}, 'strategies': []}]}})
    save_yaml(package/'sources/sources.yaml', {'sources': [{'source_id': 'synthetic', 'source_type': 'dataset',
        'title': '三个确定性合成观察日', 'url': 'https://github.com/ljjtim/QuantWitness',
        'accessed_at': '2024-01-08', 'content_hash': hashlib.sha256((HERE/'inputs.py').read_bytes()).hexdigest(),
        'license_id': 'Apache-2.0', 'status': 'available', 'limitation': '合成数据，不代表真实市场',
        'provenance': {'mode': 'citation_only', 'content_digest': None, 'media_type': None,
            'snapshot_artifact_id': None, 'snapshot_manifest_hash': None, 'importer_id': None,
            'imported_at': None, 'contract_version': 'research-source-provenance-v1'}}]})
    save_yaml(package/'localization.yaml', {'decisions': [{'decision_id': 'synthetic_scope',
        'original_assumption': '观察日收益统计', 'local_market': 'cn_etf',
        'local_adaptation': '只使用三个合成观察日，不生成交易信号', 'evidence_source_ids': ['synthetic'],
        'status': 'proxy', 'claim_effect': 'downgrade_to_research_observation'}]})
    (output/'metric-definition.json').write_text(json.dumps(metric.to_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
    return package, bundle, archive


def run_example(output: Path) -> dict[str, str]:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    package, bundle, archive = prepare(output)
    common = ['--package', package, '--catalog-lock', output/'catalog', '--extension-bundle', bundle]
    admission = cli(['package', 'admit', *common, '--input-snapshot-manifest', archive, '--output', output/'plan', '--json'])
    (output/'admission.json').write_text(json.dumps(admission, ensure_ascii=False, indent=2), encoding='utf-8')
    result = cli(['run', '--plan', output/'plan', '--input-snapshot-manifest', archive,
        '--artifact-root', output/'artifacts', '--run-root', output/'run', '--result-store', output/'results',
        '--clock', CLOCK, '--root-seed', '0', '--workers', '1', '--resource-memory-bytes', str(8*1024**3),
        '--resource-cpu-slots', '4', '--resource-process-slots', '4', '--resource-scratch-bytes', str(32*1024**3),
        '--resource-timeout-seconds', '30', '--resource-state-dir', output/'resources', '--json'])
    (output/'run-receipt.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    payload = result['data']
    if payload['status'] != 'result_finalized':
        raise RuntimeError('研究未发布正式Result，请检查run-receipt.json')
    directory = payload['result_directory']
    context = load_result_context(directory, result_store=output/'results', additional_table_ids=('observations',))
    report = output/'report.md'
    report.write_text(render_result_report(context), encoding='utf-8')
    receipt = {'result_id': context.snapshot.bundle.result_id, 'result_directory': str(directory),
               'result_store': str(output/'results'), 'report': str(report), 'metric_ref': METRIC_REF}
    (output/'receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding='utf-8')
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='尚不存在的输出目录')
    print(json.dumps(run_example(parser.parse_args().output), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
