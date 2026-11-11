"""只读核对公开期货示例的正式Result与独立手算。"""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import pyarrow.parquet as pq
from research_pipeline.platform.lean_contract import LEAN_SCHEMA_IDS, LEAN_METRICS_SCHEMA_ID


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify(root, frequency):
    root = Path(root).resolve()
    require((root/'COMMITTED').is_file(), '缺少正式Result提交标记')
    manifest = json.loads((root/'result.json').read_text(encoding='utf-8'))
    tables = {}
    for name, schema in LEAN_SCHEMA_IDS.items():
        selected = [row for row in manifest['tables'] if row['schema_id'] == schema]
        require(len(selected) == 1, 'Result须恰好包含一张'+name+'表')
        tables[name] = [json.loads(value) for path in sorted(selected[0]['files'])
                       for value in pq.ParquetFile(root/path).read(columns=['payload_json'])['payload_json'].to_pylist()]
    fills = tables['fills']
    expected = {'1m': (6, 6, 100094, 8300, 0), '1d': (3, 3, 100077, 4300, 454)}[frequency]
    require(len(fills) == expected[0], '实际成交腿数量与手算不符')
    require(sum(Decimal(str(row['fee'])) for row in fills) == expected[1], '总费用与手算不符')
    require(sum(Decimal(str(row['price']))*row['quantity']*row['multiplier'] for row in fills) == expected[3], '名义成交金额未按手数乘乘数计算')
    require({row['direction'] for row in fills[:3]} == {'long', 'short'}, '缺少真实双向成交')
    terminal = [row for row in tables['account_events'] if row['reason'] == 'terminal']
    require(len(terminal) == 1, '缺少唯一终态账户事件')
    final = terminal[0]
    require(final['equity'] == final['cash'] == expected[2], '期末结算权益与手算不符')
    require(final['margin'] == expected[4] and final['frozen_funds'] == 0, '期末保证金或预占不符')
    require(final['available_funds'] == expected[2]-expected[4], '期末可用资金不符')
    require(final['trading_date'] == '2024-01-04', '结算观察尾段不能成为额外交易日')
    require(final['valuation_at'].startswith('2024-01-05T09:30:'), '末日结算须保留实际可见时刻')
    require([row['trading_date'] for row in tables['daily']] == ['2024-01-03', '2024-01-04'], '日终交易日不完整')
    require(all(row['active_orders'] == 0 for row in tables['daily']), '日终不能保留活动订单')
    if frequency == '1m':
        require(all(row['bucket'] == 'yesterday' for row in fills[3:]), '跨日平仓必须使用昨仓')
    else:
        require([row['price'] for row in fills] == [110, 110, 210], '日频成交须使用下一会话独立开盘')
    metric_table = next(row for row in manifest['tables'] if row['schema_id'] == LEAN_METRICS_SCHEMA_ID)
    metrics = [row for path in metric_table['files'] for row in pq.ParquetFile(root/path).read().to_pylist()]
    value = next(row['value'] for row in metrics if row['metric_ref'].startswith('project.lean.final_equity@'))
    require(value == expected[2], '正式指标未消费期末结算权益')
    return dict(result_id=manifest['result_id'], frequency=frequency, fills=expected[0], fee_total=expected[1], final_equity=expected[2])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result', type=Path, required=True)
    parser.add_argument('--frequency', choices=('1d', '1m'), required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.result, args.frequency), ensure_ascii=False, indent=2))
