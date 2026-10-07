"""公开组合的test来源、决策基准和下一会话成交合同。"""
from copy import deepcopy
from datetime import date, datetime
import importlib.util
import json
from pathlib import Path

import pytest

from research_pipeline.domain import PortfolioTarget


SOURCE = Path(__file__).resolve().parents[1] / 'examples/qlib_portfolio/extension/portfolio.py'
spec = importlib.util.spec_from_file_location('public_qlib_portfolio', SOURCE)
portfolio = importlib.util.module_from_spec(spec)
spec.loader.exec_module(portfolio)


def _case():
    days = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    codes = [f'SYN{i:03d}.XSHG' for i in range(10)]
    design = {'calendar_sessions': days, 'entities': codes, 'holdout_start': '2024-01-05T00:00:00+08:00'}
    predictions = [{'entity_id': code, 'observation_session': date(2024, 1, 3),
        'prediction': i / 100, 'stage': 'test',
        'decision_time': portfolio.at(days[1], '09:30:00'),
        'feature_available_time': portfolio.at(days[1], '09:30:00')}
        for i, code in enumerate(codes)]
    prices = [{portfolio.PRICE_COLUMNS[0]: date(2024, 1, 2), portfolio.PRICE_COLUMNS[1]: code,
        portfolio.PRICE_COLUMNS[2]: 10. + i} for i, code in enumerate(codes)]
    return predictions, prices, design


def test_fixed_targets_use_test_ranks_and_prior_close():
    predictions, prices, design = _case()
    targets, benchmarks = portfolio.build_targets(predictions, prices, design, 'a' * 64)
    target = json.loads(targets[0]['target_json'])
    assert PortfolioTarget.from_dict(target).cash_weight == .1
    assert [entry['instrument']['instrument_id'] for entry in target['entries']] == design['entities'][-3:]
    assert [entry['value'] for entry in target['entries']] == [.3, .3, .3]
    assert target['cash_weight'] == .1
    assert targets[0]['order_time'] == datetime.fromisoformat('2024-01-04T09:30:00+08:00')
    assert targets[0]['decision_time'] < targets[0]['order_time']
    assert [row['price_units'] for row in benchmarks] == list(range(10000, 20000, 1000))
    assert all(row['available_at'] < row['decision_time'] for row in benchmarks)
    later = deepcopy(prices)
    for row in later:
        row[portfolio.PRICE_COLUMNS[0]] = date(2024, 1, 3)
        row[portfolio.PRICE_COLUMNS[2]] = 10000.
    assert portfolio.build_targets(predictions, prices + later, design, 'a' * 64) == (targets, benchmarks)


@pytest.mark.parametrize('attack', ['holdout', 'future_prediction', 'future_feature', 'missing', 'duplicate', 'nan'])
def test_targets_reject_ineligible_predictions(attack):
    predictions, prices, design = _case()
    if attack == 'holdout':
        predictions[0]['stage'] = 'holdout'
    elif attack == 'future_prediction':
        predictions[0]['decision_time'] = portfolio.at('2024-01-04', '09:30:00')
    elif attack == 'future_feature':
        predictions[0]['feature_available_time'] = portfolio.at('2024-01-03', '10:00:00')
    elif attack == 'missing':
        predictions.pop()
    elif attack == 'duplicate':
        predictions.append(predictions[0])
    else:
        predictions[0]['prediction'] = float('nan')
    with pytest.raises(ValueError):
        portfolio.build_targets(predictions, prices, design, 'a' * 64)


def test_decision_benchmark_preserves_milli_price():
    predictions, prices, design = _case()
    prices[0][portfolio.PRICE_COLUMNS[2]] = 3.947
    _, benchmarks = portfolio.build_targets(predictions, prices, design, 'a' * 64)
    assert benchmarks[0]['price_units'] == 3947
    assert benchmarks[0]['price_scale'] == 3
    prices[0][portfolio.PRICE_COLUMNS[2]] = 3.9471
    with pytest.raises(ValueError, match='三位小数'):
        portfolio.build_targets(predictions, prices, design, 'a' * 64)
