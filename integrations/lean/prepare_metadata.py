"""从输入包冻结日历创建 LEAN 市场元数据，不连接数据库。"""
import argparse
from bisect import bisect_left, bisect_right
import csv
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import shutil
import yaml
import pyarrow.parquet as pq


def prepare(package, upstream, target):
    content = (package/'spec/research.yaml').read_text(encoding='utf-8-sig')
    try:
        research = json.loads(content)
    except json.JSONDecodeError:
        research = yaml.safe_load(content)
    node = next(n for n in research['graph']['nodes'] if 'strategy_declaration' in n['parameters'])
    prepare_strategy_metadata(package, upstream, target, node['parameters'])


def prepare_strategy_metadata(package, upstream, target, parameters):
    """从已解析的策略参数及冻结日历生成本次研究的市场元数据。"""
    if parameters['strategy_declaration']['asset_class'] == 'cn_future':
        for name in ('market-hours', 'symbol-properties'):
            shutil.copytree(upstream/'Data'/name, target/name, dirs_exist_ok=True)
        # 期货规格与历史会话随正式输入冻结，宿主据此注册真实合约。
        print(json.dumps(dict(symbols=len(parameters['instruments']), metadata=str(target),
                              contract_source='admitted_futures_inputs')))
        return
    scope = package/'minute-scopes'/parameters['minute_scope']['name']
    policies = pq.read_table(scope/'policies.parquet', columns=[
        'instrument_index', 'revision', 'available_at', 'effective_from', 'effective_to', 'trading_dates'
    ]).to_pylist()
    dates = sorted({day for policy in policies for day in policy['trading_dates']})
    first, last = dates[0], dates[-1]
    segments = pq.read_table(scope/'segments.parquet').to_pylist()
    if len({json.dumps(row['segments'],sort_keys=True) for row in segments}) != 1:
        raise ValueError('当前安装入口要求单一A股连续交易时段模板')
    sessions = [dict(start=s['start_time'], end=s['end_time'], state='market')
                for s in segments[0]['segments'] if s['bar_eligible']]
    if any(s['start_day_offset'] or s['end_day_offset'] for s in segments[0]['segments']):
        raise ValueError('当前安装入口不支持跨日时段')
    for name in ('market-hours', 'symbol-properties'):
        shutil.copytree(upstream/'Data'/name, target/name, dirs_exist_ok=True)
    path = target/'market-hours/market-hours-database.json'
    hours = json.loads(path.read_text(encoding='utf-8-sig'))
    entry = dict(dataTimeZone='Asia/Shanghai', exchangeTimeZone='Asia/Shanghai')
    for weekday in ('monday','tuesday','wednesday','thursday','friday'):
        entry[weekday] = sessions
    entry.update(saturday=[], sunday=[])
    current = date.fromisoformat(first)
    last_day = date.fromisoformat(last)
    civil_days = {}
    while current <= last_day:
        civil_days[current.isoformat()] = current.strftime('%m/%d/%Y')
        current += timedelta(days=1)
    all_dates = frozenset(dates)
    entry['holidays'] = [text for day, text in civil_days.items() if day not in all_dates]
    holidays_by_calendar = {all_dates: entry['holidays']}
    hours['entries']['Equity-jqchina-[*]'] = entry
    # 上市日期使证券日历起点不同，逐证券保留冻结日期，不用全市场并集替代。
    instrument_rows = pq.read_table(scope/'instruments.parquet',columns=['instrument']).to_pylist()
    by_code = {}
    for policy in policies:
        code = instrument_rows[policy['instrument_index']]['instrument']['instrument_id']
        by_code.setdefault(code,[]).append(policy)
    shanghai = timezone(timedelta(hours=8))
    for instrument in parameters['instruments']:
        code = instrument['instrument_id']
        permitted, covered = set(), set()
        for policy in sorted(by_code.get(code, ()), key=lambda row: row['revision'], reverse=True):
            announced = datetime.fromisoformat(policy['available_at'].replace('Z', '+00:00')).astimezone(shanghai)
            first_visible = announced.date() + timedelta(days=announced.time() > time(9, 30))
            start = bisect_left(dates, max(first_visible.isoformat(), policy['effective_from']))
            end = bisect_right(dates, policy['effective_to'])
            # 高修订在已可见且生效的区间内优先，包括该修订取消的交易日。
            window = set(dates[start:end]).difference(covered)
            permitted.update(window.intersection(policy['trading_dates']))
            covered.update(window)
        calendar = frozenset(permitted)
        if calendar not in holidays_by_calendar:
            holidays_by_calendar[calendar] = [text for day, text in civil_days.items() if day not in calendar]
        hours['entries']['Equity-jqchina-'+code.split('.')[0]] = dict(entry, holidays=holidays_by_calendar[calendar])
    path.write_text(json.dumps(hours, ensure_ascii=False), encoding='utf-8')
    path = target/'symbol-properties/symbol-properties-database.csv'
    with path.open(encoding='utf-8-sig', newline='') as stream:
        rows = [r for r in csv.reader(stream) if not r or r[0] != 'jqchina']
    for instrument in parameters['instruments']:
        code = instrument['instrument_id']
        rows.append(['jqchina',code.split('.')[0],'equity',code,'CNY','1','0.01','1',code.split('.')[0],'1','1','1'])
    with path.open('w', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerows(rows)
    print(json.dumps(dict(symbols=len(parameters['instruments']), days=len(dates), metadata=str(target))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('package','upstream','target'):
        parser.add_argument('--'+name, type=Path, required=True)
    args = parser.parse_args()
    prepare(args.package, args.upstream, args.target)
