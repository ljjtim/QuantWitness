"""合成换手率分组的手算、时间隔离与正式Result验收。"""
from copy import deepcopy
from pathlib import Path

import pytest

from examples.turnover_factor.inputs import synthetic_table
from examples.turnover_factor.run import run_example
from examples.turnover_factor.source.operator import build_groups, build_labels, evaluate
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


@pytest.mark.parametrize('whole_session_missing', [False, True])
def test_turnover_label_requires_the_next_calendar_session(whole_session_missing):
    rows = [row for row in synthetic_table().to_pylist()
            if not (row['fld_metric_session'].day == 9
                    and (whole_session_missing or row['fld_metric_code'] == 'SYN.000'))]
    labels = build_labels(rows)
    assert ('2024-01-08', 'SYN.000') not in labels
    if whole_session_missing:
        assert ('2024-01-08', 'SYN.001') not in labels
    else:
        assert labels['2024-01-08', 'SYN.001'] == pytest.approx(0.019)
    assert labels['2024-01-05', 'SYN.000'] == pytest.approx(0.020)
    assert labels['2024-01-10', 'SYN.000'] == pytest.approx(0.020)


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


def test_groups_require_three_consecutive_calendar_sessions():
    from examples.turnover_factor.source.operator import SYNTHETIC_SESSIONS

    rows = [row for row in synthetic_table().to_pylist() if row['fld_metric_session'].day != 5]
    groups = build_groups(rows)
    assert {row['session'] for row in groups} == {'2024-01-05', '2024-01-11', '2024-01-12'}
    # 同一批行情若配的是明确休市的日历，下一交易日和过去三日均随日历变化。
    sessions = tuple(day for day in SYNTHETIC_SESSIONS if day.day != 5)
    observations, _ = evaluate(rows, sessions=sessions)
    first = observations.to_pylist()[0]
    assert (first['session'], first['mean_turnover']) == ('2024-01-08', pytest.approx(0.00103))
    assert build_labels(rows, sessions=sessions)['2024-01-04', 'SYN.000'] == pytest.approx(1.02 ** 2 - 1)


def test_short_declarations_match_explicit_package_and_query(tmp_path):
    import yaml
    from examples.turnover_factor.run import prepare
    from research_pipeline.packages import load_research_package
    from research_pipeline.packages.compiler import compile_package_queries
    from research_pipeline.packages.lint import diagnose_research_package
    from research_pipeline.packages.store import PACKAGE_FILES, PackageDocuments

    root, _, _ = prepare(tmp_path)
    short = load_research_package(root)
    raw = {name: yaml.safe_load((root / name).read_bytes()) for name in PACKAGE_FILES}
    documents = PackageDocuments(root, raw, {})
    assert load_research_package(root, documents=documents).package_hash == short.package_hash
    assert diagnose_research_package(root, documents=documents) == []
    assert 'builder_id' not in raw['package.yaml']
    assert 'filters' not in raw['spec/research.yaml']['requests'][0]
    package_file = root / 'package.yaml'
    package = yaml.safe_load(package_file.read_bytes())
    assert 'builder_id' not in package
    package['builder_id'] = short.builder_id
    package_file.write_text(yaml.safe_dump(package), encoding='utf-8')
    spec_file = root / 'spec/research.yaml'
    spec = yaml.safe_load(spec_file.read_bytes())
    for query in spec['requests']:
        query.update(filters=[], limit=None)
    spec['graph']['nodes'][0].update(inputs=[], parameters={}, strategies=[])
    spec_file.write_text(yaml.safe_dump(spec), encoding='utf-8')
    explicit = load_research_package(root)
    assert explicit.package_hash == short.package_hash
    assert compile_package_queries(explicit) == compile_package_queries(short)
