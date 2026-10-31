"""从输入包冻结日历创建 LEAN 市场元数据，不连接数据库。"""
import argparse
import csv
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import shutil
import yaml
import pyarrow.parquet as pq


def prepare(package, upstream, target):
    research = yaml.safe_load((package/'spec/research.yaml').read_text(encoding='utf-8'))
    node = next(n for n in research['graph']['nodes'] if 'strategy_declaration' in n['parameters'])
    parameters = node['parameters']
    scope = package/'minute-scopes'/parameters['minute_scope']['name']
    policies = pq.read_table(scope/'policies.parquet').to_pylist()
    dates = {day for policy in policies for day in policy['trading_dates']}
    first, last = min(dates), max(dates)
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
    holidays = []
    while current <= date.fromisoformat(last):
        if current.isoformat() not in dates:
            holidays.append(current.strftime('%m/%d/%Y'))
        current += timedelta(days=1)
    entry['holidays'] = holidays
    hours['entries']['Equity-jqchina-[*]'] = entry
    # 上市日期使证券日历起点不同，逐证券保留冻结日期，不用全市场并集替代。
    instrument_rows = pq.read_table(scope/'instruments.parquet',columns=['instrument']).to_pylist()
    by_code = {}
    for policy in policies:
        code = instrument_rows[policy['instrument_index']]['instrument']['instrument_id']
        by_code.setdefault(code,[]).append(policy)
    for instrument in parameters['instruments']:
        code=instrument['instrument_id']
        permitted=set()
        for day in sorted(dates):
            at=datetime.fromisoformat(day+'T09:30:00+08:00')
            candidates=[p for p in by_code.get(code,()) if p['effective_from'] <= day <= p['effective_to']
                        and datetime.fromisoformat(p['available_at']) <= at]
            if candidates:
                selected=max(candidates,key=lambda p:p['revision'])
                if day in selected['trading_dates']:
                    permitted.add(day)
        individual=dict(entry)
        current=date.fromisoformat(first)
        individual['holidays']=[]
        while current<=date.fromisoformat(last):
            if current.isoformat() not in permitted:
                individual['holidays'].append(current.strftime('%m/%d/%Y'))
            current+=timedelta(days=1)
        hours['entries']['Equity-jqchina-'+code.split('.')[0]]=individual
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
