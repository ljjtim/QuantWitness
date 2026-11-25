"""验证原生金融样本的可见时间、冻结输入与规则流。"""
from datetime import datetime
from pathlib import Path
import sys

import pyarrow.parquet as pq
import pytest

sys.path.insert(0,str(Path(__file__).parent/'tests'))
from native_fixture import native_rows


def sample(frequency='1d'):
    days=['2024-01-08','2024-01-09']
    payload=dict(dates=days,frequency=frequency,price_scale=3,symbols=['510300.XSHG'])
    packets={}
    for day in days:
        start=datetime.fromisoformat(day+'T09:30:00+08:00')
        end=datetime.fromisoformat(day+('T15:00:00+08:00' if frequency=='1d' else 'T09:31:00+08:00'))
        content=[(0,start,end,10007,11007,9007,10507,100)]
        packets[day]=(dict(signals=dict(available_at=start.isoformat())),content)
    return payload,packets


def test_daily_close_visible_only_at_following_open():
    rows=native_rows(*sample())
    closes=[row for row in rows if not row.get('execution_open') and row['available_at'] is not None]
    assert len(closes)==1
    assert closes[0]['available_at'].isoformat()=='2024-01-09T09:30:00+08:00'
    assert closes[0]['end_time'].isoformat()=='2024-01-08T15:00:00+08:00'
    assert closes[0]['close']==10.507


def test_minute_rows_retain_completed_bar_times():
    rows=native_rows(*sample('1m'))
    assert len(rows)==2 and all(row['available_at']==row['end_time'] for row in rows)


def test_empty_day_cannot_produce_a_visible_daily_close():
    payload,packets=sample()
    packets['2024-01-08']=(packets['2024-01-08'][0],[])
    rows=native_rows(payload,packets)
    assert not [row for row in rows if not row.get('execution_open') and row['available_at'] is not None]


@pytest.mark.parametrize('frequency', ['1d', '1m'])
def test_native_fixture_separates_financial_stream_and_native_inputs(tmp_path, frequency):
    import json
    from native_fixture import prepare_native_fixture
    from test_direct_launcher import rule
    payload,packets=sample(frequency)
    payload['execution_windows'] = {day: dict(start_at=day+'T09:30:00+08:00', end_at=day+'T15:00:00+08:00',
        session_open_at=day+'T09:30:00+08:00', session_close_at=day+'T15:00:00+08:00') for day in payload['dates']}
    for day, (header, bars) in packets.items():
        header['rules'] = [rule(day, 3, payload['symbols'][0])]
    (tmp_path/'input.json').write_text(json.dumps(payload))
    (tmp_path/'strategy-input.json').write_text(json.dumps({'declaration': {'asset_class': 'cn_etf'}}))
    config=dict(environments=dict(backtesting={}))
    prepare_native_fixture(tmp_path,config,packets)
    current=json.loads((tmp_path/'input.json').read_text())
    binding = current['native_data']['bindings'][0]
    assert binding['resolution'] == frequency
    assert 'endpoint' not in current['native_data']
    if frequency == '1m':
        assert binding['direct_minute'] and current['source_kind'] == 'frozen_minute_parquet'
        assert 'endpoint' not in current
        sources = current['rules_parquet']
        rules = pq.read_table(sources['rules_path'], columns=['rule_id', 'effective_from']).to_pylist()
        dates = {r['effective_from'] for r in rules}
        kinds = {r['rule_id'] for r in rules}
        if 'daily_reference_path' in sources:
            columns = ['trading_date', 'lifecycle_rule_id', 'prices_rule_id', 'suspension_rule_id']
            daily = pq.read_table(sources['daily_reference_path'], columns=columns).to_pylist()
            assert len(daily) == len(payload['dates'])
            dates.update(row['trading_date'] for row in daily)
            kinds.update(row[key] for row in daily for key in columns[1:] if row[key])
        assert dates == set(payload['dates'])
        assert kinds == {
            f'rule.cn_fund.{kind}.v1' for kind in ('instrument_lifecycle', 'lot_size', 'price_limit',
                                                   'settlement', 'suspension', 'trading_fee', 'session', 'adjustment_factor_snapshot')}
        partition, = current['minute_parquet']['partitions']
        minute = pq.read_table(partition['path']).to_pylist()
        assert [row['dt'].isoformat() for row in minute] == [day + 'T09:31:00' for day in payload['dates']]
        assert [row['close'] for row in minute] == [10.507, 10.507]
    else:
        assert 'direct' not in binding
        from research_pipeline.runtime.lean_native_direct import _binding

        sidecar = tmp_path / 'native-bindings.json'
        assert json.loads(sidecar.read_text())['bindings'][0]['direct']
        reader = _binding(str(sidecar), 'bars')
        request = dict(symbol='510300.XSHG', purpose='history', start_at=binding['history_start_at'],
                       end_at='2024-01-09T15:00:00+08:00', consumer_time='2024-01-08T09:30:00+08:00')
        assert list(reader.rows(request)) == []
        request['consumer_time'] = '2024-01-09T09:30:00+08:00'
        row, = list(reader.rows(request))
        assert row['close'] == 10.507 and not row.get('execution_open')
        assert list(reader.rows(request)) == [row]
        with pytest.raises(ValueError, match='研究数据域'):
            list(reader.rows({**request, 'symbol': '000001.XSHE'}))
    assert config['environments']['backtesting']['setup-handler'].endswith('NativeSetupHandler')
    assert 'endpoint' not in current
