"""将小型金融流fixture同时交给原生数据服务与账户，不连接数据库。"""
from datetime import datetime, timedelta
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from research_pipeline.runtime.lean_minute_parquet import _encode_schedules
from research_pipeline.runtime.lean_native_inputs import _calendar_events, _calendar_windows
from fixture_native_sources import daily_source, universe_target_sources
from fixture_financial_scope import frozen_financial_day


def native_rows(payload, packets):
    rows=[]
    days=payload['dates']
    daily=payload['frequency']=='1d'
    scale=10**payload['price_scale']
    for index,day in enumerate(days):
        header,content=packets[day]
        for symbol, first, last, opening, high, low, close, volume in content:
            if daily:
                sessions=[value for value in payload.get('sessions',()) if value['trading_date']==day]
                point=datetime.fromisoformat(sessions[0]['start_at']) if sessions else first
                rows.append(dict(symbol=payload['symbols'][symbol],time=point,end_time=point,available_at=point,
                                 execution_open=True,open=opening/scale))
                following=[value for value in payload.get('sessions',()) if index+1 < len(days) and value['trading_date']==days[index+1]]
                available=datetime.fromisoformat(following[0]['start_at']) if following else (datetime.fromisoformat(days[index+1]+'T09:30:00+08:00') if index+1 < len(days) else None)
            else: available=last
            rows.append(dict(symbol=payload['symbols'][symbol],time=first,end_time=last,available_at=available,
                             open=opening/scale,high=high/scale,low=low/scale,close=close/scale,volume=volume))
    return sorted(rows,key=lambda row:(row['available_at'] is None,row['available_at'],row.get('execution_open',False),row['symbol']))


def minute_parquet(output, payload, rows):
    schedules = []
    if payload.get('sessions'):
        for symbol in payload['symbols']:
            segments = [[session['trading_date'], segment['start_at'], segment['end_at']]
                        for session in payload['sessions'] if session['instrument_id'] == symbol
                        for segment in session['segments']]
            schedules.append(dict(symbols=[symbol], segments=segments))
    else:
        segments = [[day, window['session_open_at'], window['session_close_at']]
                    for day, window in payload['execution_windows'].items()]
        schedules.append(dict(symbols=payload['symbols'], segments=segments))
    by_month = {}
    columns = ('open', 'high', 'low', 'close', 'volume')
    for row in rows:
        minute = row['end_time'].replace(tzinfo=None)
        by_month.setdefault(minute.strftime('%Y-%m'), []).append(
            dict(code=row['symbol'], dt=minute, money=float(row['close'] * row['volume']),
                 avg=float(row['close']) if row['volume'] else None, open_interest=None,
                 **{name: float(row[name]) for name in columns}))
    schema = pa.schema([('code', pa.string()), ('dt', pa.timestamp('us')),
                        *((name, pa.float64()) for name in (*columns, 'money', 'avg', 'open_interest'))])
    partitions = []
    zone = datetime.fromisoformat(next(segment[1] for schedule in schedules for segment in schedule['segments'])).tzinfo
    directory = output / 'fixture-inputs'
    directory.mkdir(exist_ok=True)
    for month, records in sorted(by_month.items()):
        path = (directory / f'minute-{month}.parquet').resolve()
        ordered = sorted(records, key=lambda row: (row['code'], row['dt']))
        pq.write_table(pa.Table.from_pylist(ordered, schema=schema), path)
        begin = datetime.fromisoformat(month + '-01').replace(tzinfo=zone)
        end = (begin + timedelta(days=32)).replace(day=1)
        stat = path.stat()
        partitions.append(dict(path=str(path), month=month, start_at=begin.isoformat(), end_at=end.isoformat(),
                               size=stat.st_size, mtime_ticks=621355968000000000 + stat.st_mtime_ns // 100,
                               row_count=len(records), row_groups=1, timestamp_utc=False, groups=[0]))
    quality = ['avg', 'open_interest'] if payload.get('asset_class') == 'cn_future' else ['avg']
    by_schedule = {}
    for schedule in schedules:
        key = tuple(sorted(((datetime.fromisoformat(day).date(), datetime.fromisoformat(start),
                            datetime.fromisoformat(end)) for day, start, end in schedule['segments']),
                           key=lambda segment: segment[1]))
        by_schedule.setdefault(key, []).extend(schedule['symbols'])
    return dict(memory_bytes=64 * 1024 * 1024, partitions=partitions,
                schedules=_encode_schedules(by_schedule), quality_columns=quality)


def prepare_native_fixture(output, config, samples, *, universe_targets=False):
    output=Path(output)
    payload=json.loads((output/'input.json').read_text())
    settings=json.loads((output/'strategy-input.json').read_text())
    windows=list(payload.get('execution_windows',{}).values()) or payload['sessions']
    starts=[datetime.fromisoformat(value['start_at']) for value in windows]
    ends=[datetime.fromisoformat(value['end_at']) for value in windows]
    start=min(starts)-timedelta(days=1)
    end=max(ends+[datetime.fromisoformat(payload.get('terminal_observation_at',payload.get('end_at',max(ends).isoformat())))])
    rows=native_rows(payload,samples)
    binding=dict(binding_id='bars',kind='bars',resolution=payload['frequency'],symbols=payload['symbols'],
                 trade_symbols=payload['symbols'],history_start_at=start.isoformat(),start_at=min(starts).isoformat(),end_at=end.isoformat(),page_size=1 if payload['frequency']=='1d' else 240)
    fixture_inputs = output / 'fixture-inputs'
    fixture_inputs.mkdir(exist_ok=True)
    if payload['frequency'] == '1m':
        payload['minute_parquet'] = minute_parquet(output, payload, rows)
        binding['direct_minute'] = True
    else:
        binding = daily_source(fixture_inputs, binding, payload['dates'], rows,
                               futures=payload.get('asset_class') == 'cn_future')
    frozen_financial_day(output, settings, payload, samples, binding)
    (output / 'strategy-input.json').write_text(json.dumps(settings), encoding='utf-8')
    bindings = [binding]
    if universe_targets:
        bindings.extend(universe_target_sources(fixture_inputs, binding, payload['dates']))
    clock={**payload,**settings,'end_at':end.isoformat()}
    from research_pipeline.runtime.lean_native_inputs import _native_manifests
    from research_pipeline.runtime.lean_strategy_inputs import _compact_execution_windows, _write_json
    native, direct = _native_manifests(clock, bindings, _calendar_events(clock), _calendar_windows(clock))
    _write_json(output / 'native-bindings.json', {'bindings': direct})
    payload['native_data'] = native
    if 'execution_windows' in payload:
        payload['execution_windows'] = _compact_execution_windows(payload['execution_windows'])
    config.update({'research-strategy-input':str(output/'strategy-input.json'),'research-lifecycle-handler':'QuantWitness.Lean.Research.NativeMarketLifecycle','history-provider':'QuantWitness.LeanInput.NativeHistoryProvider'})
    config['environments']['backtesting'].update({'setup-handler':'QuantWitness.Lean.Research.NativeSetupHandler','result-handler':'QuantConnect.Lean.Engine.Results.BacktestingResultHandler','history-provider':'QuantWitness.LeanInput.NativeHistoryProvider'})
    _write_json(output / 'input.json', payload)
