"""公开自定义指标示例的正式发布与消费者验收，不访问数据库。"""
import json
from pathlib import Path

import pyarrow as pa
import pytest

from examples.custom_metric.run import METRIC_REF, METRIC_SCHEMA, calculate_metric, definition, run_example
from research_pipeline.cli import main
from research_pipeline.results import build_result_report, load_result_context


def test_custom_metric_definition_and_arrow_contract():
    metric_definition = definition()
    semantics = metric_definition.to_dict()['measurement_semantics']
    assert semantics['numerator'] == 'sum_observed_returns'
    assert semantics['denominator'] == 'observed_session_count'
    observations = pa.table({'session': ['2024-01-05', '2024-01-03'], 'return': [0.04, -0.02]})
    table = calculate_metric(observations)
    assert table.schema == METRIC_SCHEMA
    row, = table.to_pylist()
    assert row == {'metric_ref': METRIC_REF, 'value': 0.01, 'unit': 'decimal_return',
                   'sample_start': '2024-01-03', 'sample_end': '2024-01-05',
                   'sample_size': 2, 'status': 'computed'}


def test_custom_metric_example_publishes_and_reports(tmp_path, capsys):
    receipt = run_example(tmp_path / 'example')
    context = load_result_context(receipt['result_directory'], result_store=receipt['result_store'],
                                  additional_table_ids=('observations',))
    metric, = context.metrics
    assert metric.metric_ref == METRIC_REF
    assert metric.value == pytest.approx(0.01)
    assert metric.sample_size == 3
    assert (metric.sample_start, metric.sample_end) == ('2024-01-03', '2024-01-05')
    assert metric.status == 'computed'
    assert context.claim_level == 'research_observation'
    assert {'data_plane', 'metric'} <= set(context.snapshot.bundle.run.node_statuses)
    assert set(context.snapshot.bundle.run.node_statuses.values()) == {'succeeded'}
    run_receipt = json.loads((Path(receipt['report']).parent / 'run-receipt.json').read_text(encoding='utf-8'))
    assert run_receipt['data']['status'] == 'result_finalized'
    assert (Path(receipt['report']).parent / 'package/spec/research.yaml').is_file()
    assert (Path(receipt['report']).parent / 'plan').is_dir()
    original = context.snapshot.read_table('project.custom_metric.observations.v1')
    assert original.num_rows == 3
    report = build_result_report(context).to_dict()
    assert report['claim_level'] == 'research_observation'
    python_report = Path(receipt['report']).read_text(encoding='utf-8')
    assert METRIC_REF in python_report and receipt['result_id'] in python_report
    cli_report = tmp_path / 'cli-report.md'
    assert main(['report', '--result', receipt['result_directory'], '--result-store', receipt['result_store'],
                 '--output', str(cli_report), '--format', 'markdown', '--json']) == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload['status'] == 'pass'
    assert METRIC_REF in cli_report.read_text(encoding='utf-8')

