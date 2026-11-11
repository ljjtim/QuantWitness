"""合成换手率分组的手算、时间隔离与正式Result验收。"""
from copy import deepcopy
from pathlib import Path

import pytest

from examples.turnover_factor.inputs import synthetic_table
from examples.turnover_factor.run import run_example
from examples.turnover_factor.source.operator import build_groups, evaluate
from research_pipeline.results import load_result_context


def test_turnover_groups_match_hand_calculation_and_ignore_future():
    rows = synthetic_table().to_pylist()
    groups = build_groups(rows)
    first = [row for row in groups if row['session'] == '2024-01-05']
    assert len(first) == 20
    assert [row['group'] for row in first] == [1]*4 + [2]*4 + [3]*4 + [4]*4 + [5]*4
    assert first[0]['mean_turnover'] == pytest.approx((0.00102+0.00103+0.00104)/3)
    changed = deepcopy(rows)
    for row in changed:
        if row['fld_metric_session'].isoformat() >= '2024-01-05':
            row['fld_metric_turnover'] = 1000.0
            row['fld_metric_close'] *= 1.5
    assert [row for row in build_groups(changed) if row['session'] == '2024-01-05'] == first
    labels_changed = deepcopy(rows)
    for row in labels_changed:
        row['fld_metric_close'] *= 1 + row['fld_metric_session'].day
    assert build_groups(labels_changed) == groups
    observations, metrics = evaluate(rows)
    metric, = metrics.to_pylist()
    assert metric['value'] == pytest.approx(0.016)
    assert metric['sample_size'] == 5
    assert observations.num_rows == 120
    assert observations['forward_return'].null_count == 20


def test_turnover_example_publishes_result(tmp_path):
    receipt = run_example(tmp_path / 'example')
    context = load_result_context(receipt['result_directory'], result_store=receipt['result_store'],
                                  additional_table_ids=('observations',))
    metric, = context.metrics
    assert metric.value == pytest.approx(0.016)
    assert metric.sample_size == 5
    assert context.claim_level == 'research_observation'
    assert set(context.snapshot.bundle.run.node_statuses.values()) == {'succeeded'}
    assert receipt['result_id'] in Path(receipt['report']).read_text(encoding='utf-8')
