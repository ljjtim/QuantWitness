"""风险目标、实际现金执行与独立金融oracle。"""
from copy import deepcopy
import importlib.util
import json
import math
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
ROOT = Path(__file__).resolve().parents[1] / 'project_extensions/qlib_portfolio_risk'
with pytest.MonkeyPatch.context() as imports:
    imports.syspath_prepend(str(ROOT))
    spec = importlib.util.spec_from_file_location('risk_extension', ROOT / 'extension.py')
    extension = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(extension)
    import oracle as risk_oracle

def case(method='inv'):
    days = pd.bdate_range('2024-01-02', periods=32).strftime('%Y-%m-%d').tolist()
    assets = ['SYN000.XSHG', 'SYN001.XSHG', 'SYN002.XSHG']
    design = {'calendar_sessions': days, 'entities': assets, 'holdout_start': days[-2] + 'T00:00:00+08:00', 'price_fields': ['date', 'code', 'close'], 'finance': {'initial_cash_cny': 100000.0}, 'portfolio_risk': {'method': method, 'estimator': 'empirical', 'lookback': 20, 'shrink_alpha': 0.1}}
    prices = [{'date': pd.Timestamp(day).date(), 'code': code, 'close': round(10 + number * 3 + 0.03 * i + 0.2 * math.sin(i / 3 + number), 3)} for (i, day) in enumerate(days) for (number, code) in enumerate(assets)]
    predictions = [{'entity_id': code, 'observation_session': pd.Timestamp(days[23]).date(), 'prediction': 0.001 * (number - 1), 'stage': 'test', 'decision_time': extension._at(days[23], '09:30:00'), 'feature_available_time': extension._at(days[23], '09:30:00')} for (number, code) in enumerate(assets)]
    return (predictions, prices, design)

@pytest.mark.parametrize('attack', ['future_prediction', 'holdout', 'duplicate', 'nan'])
def test_target_rejects_ineligible_prediction(attack):
    (predictions, prices, design) = case()
    if attack == 'future_prediction':
        predictions[0]['decision_time'] = extension._at(design['calendar_sessions'][24], '09:30:00')
    elif attack == 'holdout':
        predictions[0]['stage'] = 'holdout'
    elif attack == 'duplicate':
        predictions.append(predictions[0])
    else:
        predictions[0]['prediction'] = float('nan')
    with pytest.raises(ValueError):
        extension.build_targets(predictions, prices, design, 'a' * 64)

def test_future_prices_cannot_change_earlier_risk_or_target():
    (predictions, prices, design) = case()
    baseline = extension.build_targets(predictions, prices, design, 'a' * 64)
    later = deepcopy(prices)
    for row in later:
        if row['date'] >= predictions[0]['observation_session']:
            row['close'] *= 3
    assert extension.build_targets(predictions, later, design, 'a' * 64) == baseline
