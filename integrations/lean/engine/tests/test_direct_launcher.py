"""用合成行情及冻结规则检查真实 LEAN 订单链。"""
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest


DATES = ('2021-10-08', '2021-10-11', '2021-10-12')
CODE = '000002.XSHE'
EMPTY_CODE = '000001.XSHE'


def timestamp(day, clock):
    return datetime.fromisoformat(f'{day}T{clock}+08:00')


def launcher_environment(installation, output):
    python_root = next((installation / 'python-managed').glob('cpython-*'))
    environment = os.environ.copy()
    environment.update(DOTNET_ROOT=str(installation / 'sdk'), PYTHONNET_PYDLL=str(python_root / 'python311.dll'),
        PYTHONHOME=str(python_root), PYTHONNOUSERSITE='1', PYTHONPATH=str(installation / 'python-env/Lib/site-packages'),
        PYTHONDONTWRITEBYTECODE='1', TEMP=str(output / 'temp'), TMP=str(output / 'temp'),
        DOTNET_CLI_HOME=str(output / 'temp/dotnet'))
    environment['PATH'] = os.pathsep.join((str(python_root), str(python_root / 'DLLs'),
                                         str(installation / 'sdk'), environment.get('PATH', '')))
    return environment


def rule(day, price_scale=2, code=CODE):
    value = dict(instrument_id=code, effective_from=day, effective_to=day,
                available_at=timestamp(day, '09:00:00').isoformat(), parameters=dict(
                    settlement_days=0 if price_scale == 3 else 1, paused=False, price_limit_mode='unbounded', market_protection_required=False,
                    market_order_allowed=True,
                    price_scale=price_scale, commission_ppm=300, min_commission_units=500,
                    sell_tax_ppm=0 if price_scale == 3 else 1000,
                    transfer_fee_ppm=0 if price_scale == 3 else 10, buy_min_quantity=100, buy_quantity_step=100, buy_max_quantity=1000000,
                    sell_min_quantity=100, sell_quantity_step=100, sell_max_quantity=1000000, sell_remainder_allowed=True))
    if price_scale == 3:
        for name in ('paused', 'market_protection_required', 'buy_min_quantity', 'buy_quantity_step',
                     'buy_max_quantity', 'sell_min_quantity', 'sell_quantity_step', 'sell_max_quantity'):
            value['parameters'].pop(name)
        value['parameters']['buy_lot_units'] = 100
    return value


@pytest.mark.parametrize(('frequency', 'hold', 'missing_last', 'cash_action', 'price_scale', 'scenario'), [
    ('1d', False, False, False, 2, None), ('1m', False, False, False, 2, None), ('1d', True, False, True, 2, None),
    ('1d', True, True, False, 2, None), ('1d', True, True, True, 2, None),
    ('1d', False, False, False, 3, None), ('1m', False, False, False, 3, None),
    ('1m', True, False, True, 2, 'partial'), ('1m', True, False, False, 2, 'empty'),
    ('1m', True, True, True, 2, 'missing_mark'), ('1m', True, True, True, 2, 'missing_rules'),
    ('1m', True, False, False, 2, 'no_close_bar'),
    ('1m', True, True, False, 2, 'opening_cutoff'), ('1m', True, True, True, 2, 'opening_cash'),
    ('1m', True, False, False, 2, 'point_only'),
    ('1d', True, True, True, 2, 'delisted'), ('1m', True, True, True, 2, 'delisted'),
    ('1d', True, True, True, 2, 'delisted_tail'), ('1m', True, True, True, 2, 'delisted_tail'),
    ('1m', False, False, False, 2, 'universe_targets'),
    ('1m', False, False, False, 2, 'closed_orders'), ('1m', False, False, False, 3, 'closed_orders'),
    ('1m', False, False, False, 2, 'order_trace'), ('1m', False, False, False, 2, 'order_trace_strict'),
    ('1m', False, False, True, 2, 'zero_lifecycle'),
    ('1m', False, False, False, 2, 'current_rules'),
    ('1m', False, False, False, 2, 'changed_fee_model')])
def test_direct_native_orders(tmp_path, frequency, hold, missing_last, cash_action, price_scale, scenario):
    universe_targets = scenario == 'universe_targets'
    if universe_targets:
        scenario = None
    code = '510300.XSHG' if price_scale == 3 else CODE
    tail = scenario == 'delisted_tail'
    stream_dates = DATES[:-1] if tail else DATES
    symbols = [code] if tail else [code, EMPTY_CODE]
    divisor = 10 ** price_scale
    fraction = 7 if price_scale == 3 else 0
    windows = {day: dict(start_at=timestamp(day, '09:30:00').isoformat(),
        end_at=timestamp(day, '15:00:00').isoformat(), session_open_at=timestamp(day, '09:30:00').isoformat(),
        session_close_at=timestamp(day, '15:00:00').isoformat()) for day in DATES}
    if scenario and scenario != 'closed_orders':
        windows[DATES[0]]['start_at'] = timestamp(DATES[0], '09:31:00').isoformat()
        if scenario not in {'no_close_bar', 'missing_mark', 'delisted', 'delisted_tail'}:
            windows[DATES[-1]]['end_at'] = timestamp(DATES[-1], '09:33:00').isoformat()
        if scenario in {'opening_cutoff', 'opening_cash'}:
            windows[DATES[-1]]['end_at'] = windows[DATES[-1]]['start_at']
        if scenario in {'empty', 'point_only'}:
            for day in DATES:
                windows[day].update(start_at=timestamp(day, '10:00:00').isoformat(),
                                    end_at=timestamp(day, '10:00:00' if scenario == 'point_only' else '10:05:00').isoformat())
    if tail:
        windows.pop(DATES[-1])
    installation = os.environ.get('QW_ENGINE_TEST_INSTALLATION')
    build = os.environ.get('QW_ENGINE_TEST_BUILD')
    if not installation or not build:
        pytest.skip('需显式提供只读LEAN依赖目录和仓库外新构建目录')
    installation, build = Path(installation), Path(build)
    source_launcher = next((installation / 'src').glob('Lean-*/Launcher/bin/Release'))
    launcher = tmp_path / 'launcher'
    shutil.copytree(source_launcher, launcher)
    for name in ('QuantWitnessLeanHost', 'ChinaRules', 'CorporateActions', 'ParquetInput'):
        shutil.copy2(build / f'bin/{name}/net10.0/{name}.dll', launcher / f'{name}.dll')
    output = tmp_path / 'run'
    output.mkdir()
    (output / 'temp').mkdir()
    metadata = output / 'metadata'
    source_metadata = installation / 'rp-integration/metadata'
    for directory in ('market-hours', 'symbol-properties'):
        shutil.copytree(source_metadata / directory, metadata / directory)
    if price_scale == 3:
        hours_path = metadata / 'market-hours/market-hours-database.json'
        hours = json.loads(hours_path.read_text())
        hours['entries']['Equity-jqchina-510300'] = hours['entries']['Equity-jqchina-[*]']
        hours_path.write_text(json.dumps(hours), encoding='utf-8')
        properties = metadata / 'symbol-properties/symbol-properties-database.csv'
        with properties.open('a', encoding='utf-8') as stream:
            stream.write('\njqchina,510300,equity,510300.XSHG,CNY,1,0.001,1,510300,1,1,1\n')
    for market in ('jqchina', 'usa'):
        for name in ('map_files', 'factor_files'):
            (metadata / f'equity/{market}/{name}').mkdir(parents=True)

    samples = {}
    for index, day in enumerate(stream_dates):
        clocks = [('09:30:00', '15:00:00')] if frequency == '1d' else [
            ('09:30:00', '09:31:00'), ('09:31:00', '09:32:00'), ('14:59:00', '15:00:00')]
        if scenario == 'closed_orders' and day == DATES[0]:
            clocks.insert(2, ('11:29:00', '11:30:00'))
        if scenario in {'order_trace', 'order_trace_strict'} and day == DATES[0]:
            clocks.insert(2, ('09:32:00', '09:33:00'))
        if missing_last and day == DATES[-1]:
            clocks = []
        if scenario in {'empty', 'point_only'}:
            clocks = []
        elif scenario and scenario != 'closed_orders' and day == DATES[-1]:
            clocks = [row for row in clocks if row[1] != '15:00:00']
        rules = [] if scenario in {'missing_rules', 'delisted'} and day == DATES[-1] else [rule(day, price_scale, code)]
        if scenario == 'current_rules' and index == 0:
            for minute, ratio, step in [(30, 100000, 100), (32, 200000, 200)]:
                available = timestamp(day, f'09:{minute}:00').isoformat()
                row = rule(day, price_scale, code)
                row['available_at'] = available
                row['parameters'].update(price_limit_mode='bounded',
                    reference_previous_close_units=1000, reference_price_available_at=available,
                    price_limit_ratio_ppm=ratio, price_limit_rounding='half_up_to_quote_unit',
                    low_limit_units=1000 - ratio // 1000, high_limit_units=1000 + ratio // 1000,
                    buy_min_quantity=step, buy_quantity_step=step)
                if minute == 30:
                    rules = [row]
                else:
                    rules.append(row)
        header = dict(date=day, frequency=frequency, availability='next_session_open',
            rules=rules,
            signals=dict(available_at=timestamp(day, '09:30:00').isoformat(), selection=[]),
            row_count=len(clocks))
        bars = []
        for begin, finish in clocks:
            start, end = timestamp(day, begin), timestamp(day, finish)
            row = (0, start,
                              end,
                              (10 + index) * divisor + fraction, (100 + index) * divisor + fraction,
                              divisor, (20 + index) * divisor + fraction,
                              1 if frequency == '1d' else 1000000)
            bars.append(row)
        samples[day] = header, bars
    (output / 'input.json').write_text(json.dumps(dict(frequency=frequency,
        symbols=symbols, dates=stream_dates, price_scale=price_scale, execution_windows=windows,
        **({'terminal_observation_at': timestamp(DATES[-1], '15:00:00').isoformat()} if tail else {}))), encoding='utf-8')
    instrument = dict(instrument_id=code, asset_class='cn_etf' if price_scale == 3 else 'cn_stock',
                      venue='XSHG' if price_scale == 3 else 'XSHE', currency='CNY',
                      contract_kind='etf' if price_scale == 3 else 'stock', contract_version='research-instrument-key-v1')
    declaration = dict(asset_class=instrument['asset_class'])
    source = output / 'strategy-source'
    source.mkdir()
    sample = 'fixture_universe_algorithm.py' if universe_targets else 'fixture_spot_algorithm.py'
    shutil.copy2(Path(__file__).with_name(sample), source / 'fixture_strategy.py')
    settings = dict(frequency=frequency, declaration=declaration, algorithm=dict(language='Python', entry_file='fixture_strategy.py', type_name='NativeFixture'),
        parameters=dict(slippage_bps=0, slippage_ticks=0,
                        participation_cap_ppm=1 if frequency == '1d' else 1000000, frequency=frequency, hold=hold,
                        price_scale=price_scale, asset_class=instrument['asset_class'], fraction=fraction, scenario=scenario),
        execution_windows=windows,
        instruments={symbol: dict(instrument, instrument_id=symbol) for symbol in symbols}, initial_cash_units=1000000)
    if scenario in {'delisted', 'delisted_tail'}:
        settings['lifecycle'] = {code: dict(delisted_date=DATES[-2],
            available_at=timestamp(DATES[-1], '00:00:00').isoformat(), source_ref='all_stocks:synthetic:end_date')}
    if tail:
        settings['terminal_observation_at'] = timestamp(DATES[-1], '15:00:00').isoformat()
    (output / 'strategy-input.json').write_text(json.dumps(settings), encoding='utf-8')
    actions = [dict(action_id='cash-fixture', revision=1, kind='cash_dividend', symbol=CODE.split('.')[0],
        available_at=timestamp(DATES[0], '09:00:00').isoformat(), record_at=timestamp(DATES[1], '15:00:00').isoformat(),
        ex_at=timestamp(DATES[2], '09:30:00').isoformat(), cash_pay_at=timestamp(DATES[2], '09:30:00').isoformat(),
        cash_per_share=0.1, source_ref='synthetic-cash')] if cash_action else []
    if scenario and actions:
        actions[0]['cash_pay_at'] = timestamp(DATES[2], '09:30:00' if scenario in {'opening_cash', 'delisted'} else '16:00:00').isoformat()
        actions[0]['ex_reference_price'] = None
        if tail:
            actions[0]['cash_pay_at'] = timestamp(DATES[-1], '11:00:00').isoformat()
    if scenario in {'missing_mark', 'missing_rules'}:
        actions.append(dict(actions[0], revision=2, cash_per_share=0.2,
            available_at=timestamp(DATES[2], '09:15:00').isoformat(), source_ref='synthetic-cash-revision'))
    if scenario == 'zero_lifecycle':
        actions[0].update(record_at=timestamp(DATES[0], '15:00:00').isoformat(),
            cash_pay_at=timestamp(DATES[2], '09:30:00').isoformat())
        actions.append(dict(actions[0], action_id='cash-unheld', symbol=EMPTY_CODE.split('.')[0]))
    (output / 'actions.json').write_text(json.dumps(actions), encoding='utf-8')

    template = (installation / 'rp-integration/config-template.json').read_text(encoding='utf-8-sig')
    template = re.sub(r'("(?:\\.|[^"\\])*")|//[^\n]*|/\*[\s\S]*?\*/', lambda match: match[1] or '', template)
    config = json.loads(re.sub(r',\s*([}\]])', r'\1', template))
    config.update({'environment': 'backtesting', 'algorithm-type-name': 'NativeFixture',
        'algorithm-language': 'Python', 'algorithm-location': str(source / 'fixture_strategy.py'),
        'results-destination-folder': str(output), 'parquet-input-manifest': str(output / 'input.json'),
        'data-folder': str(metadata), 'python-venv': str(installation / 'python-env'),
        'object-store-root': str(output / 'storage'), 'close-automatically': True})
    config['environments']['backtesting']['data-feed-handler'] = 'QuantWitness.LeanInput.ParquetDataFeed'
    from native_fixture import prepare_native_fixture
    prepare_native_fixture(output, config, samples, universe_targets=universe_targets)
    (output / 'config.json').write_text(json.dumps(config), encoding='utf-8')
    environment = launcher_environment(installation, output)
    if scenario in {'order_trace', 'order_trace_strict'}:
        environment['QUANTWITNESS_STRICT_AUDIT'] = '1' if scenario == 'order_trace_strict' else '0'
    if frequency == '1m':
        manifest = json.loads((output / 'input.json').read_text(encoding='utf-8'))
        assert manifest['source_kind'] == 'frozen_minute_parquet'
        assert 'endpoint' not in manifest
    with (output / 'engine.log').open('wb') as log:
        result = subprocess.run([str(installation / 'sdk/dotnet.exe'), str(launcher / 'QuantConnect.Lean.Launcher.dll'),
            '--config', str(output / 'config.json')], cwd=launcher, env=environment, stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT, timeout=90)
    status_path = output / 'engine-status.json'
    expected_status = 'failed' if scenario in {'missing_mark', 'missing_rules', 'changed_fee_model'} else 'completed'
    expected_returncode = 1 if expected_status == 'failed' else 0
    assert result.returncode == expected_returncode and status_path.is_file(), (output / 'engine.log').read_text(errors='replace')[-6000:]
    status = json.loads(status_path.read_text())
    assert status['status'] == expected_status, (status, (output / 'engine.log').read_text(errors='replace')[-6000:])
    assert status['host_finished'] == (expected_status == 'completed')
    engine_log = (output / 'engine.log').read_text(encoding='utf-8', errors='replace')
    shutdown = engine_log.index('PythonInitializer.Shutdown(): ended')
    assert shutdown < engine_log.index('Program.Main(): Exiting Lean...')
    if scenario == 'partial':
        account_timing = json.loads((output / 'spot-account-view.json').read_text(encoding='utf-8'))
        assert 0 < sum(row['cache_hits'] for row in account_timing) < sum(row['calls'] for row in account_timing)
    if scenario == 'changed_fee_model':
        assert '原生研究不能更换已冻结的成交或费用模型' in engine_log
        assert (output / 'orders.jsonl').read_text(encoding='utf-8') == ''
        return
    if frequency == '1m' and expected_status == 'completed':
        profile = json.loads((output / 'parquet-read.json').read_text(encoding='utf-8'))
        assert math.isfinite(profile['rule_source_load_seconds']) and profile['rule_source_load_seconds'] > 0
        expected_rules = {
            day: (2 if scenario == 'current_rules' and day == DATES[0] else int(scenario != 'delisted' or day != DATES[-1]))
            for day in stream_dates
        }
        assert profile['date_load_counts'] == dict.fromkeys(stream_dates, 1)
        materialized = {day: info['rows'] for day, info in profile['rules_by_date'].items()}
        assert set(materialized) == set(expected_rules)
        assert all(0 <= count <= expected_rules[day] for day, count in materialized.items())
        if scenario == 'current_rules':
            assert materialized[DATES[0]] == 2
        assert all(opens == 1 for opens in profile['partition_open_counts'].values())
        assert all(math.isfinite(info['seconds']) and info['seconds'] > 0 for info in profile['rules_by_date'].values())
    if scenario in {'order_trace', 'order_trace_strict'}:
        trace = [json.loads(line) for line in (output / 'strategy-trace.jsonl').read_text(encoding='utf-8').splitlines()]
        assert [row['event_seq'] for row in trace] == list(range(1, len(trace) + 1))
        assert trace[-1]['kind'] == 'algorithm_completed'
        assert {row['kind'] for row in trace} == {
            'algorithm_initialized', 'algorithm_completed', 'order_event', 'order_request'}
        requests_by_tag = {}
        for row in trace:
            if row['kind'] == 'order_request':
                request = row['payload']
                requests_by_tag.setdefault(request['tag'], []).append(request)
        assert set(requests_by_tag) == {'zero-submit', 'open-limit', 'filled-market', 'terminal-update',
            'terminal-cancel', 'unknown-cancel', 'update-limit', 'cancel-limit', 'duplicate-cancel',
            'duplicate-ticket-cancel'}
        for tag, kind, error in (
            ('zero-submit', 'Submit', 'OrderQuantityZero'),
            ('terminal-update', 'Update', 'InvalidOrderStatus'),
            ('terminal-cancel', 'Cancel', 'InvalidOrderStatus'),
            ('unknown-cancel', 'Cancel', 'UnableToFindOrder'),
            ('duplicate-cancel', 'Cancel', 'InvalidRequest'),
            ('duplicate-ticket-cancel', 'Cancel', 'RequestCanceled'),
        ):
            assert [(row['request_type'], row['response_error_code'], row['response_is_error'])
                    for row in requests_by_tag[tag]] == [(kind, error, True)] * (2 if tag == 'duplicate-cancel' else 1)
        assert requests_by_tag['unknown-cancel'][0]['order_id'] == 999999
        for tag in ('update-limit', 'cancel-limit'):
            assert [(row['request_status'], row['response_is_error']) for row in requests_by_tag[tag]] == [
                ('Processing', False), ('Processed', False)]
        assert requests_by_tag['update-limit'][-1]['limit_price'] == 0.75
        assert requests_by_tag['terminal-update'][-1]['quantity'] == 200
        return
    if scenario == 'zero_lifecycle':
        lifecycle = [json.loads(line) for line in (output / 'lifecycle.jsonl').read_text(encoding='utf-8').splitlines()]
        assert [(row['action_id'], row['phase'], row['registered_quantity']) for row in lifecycle] == [
            ('cash-fixture', 'ex', 0)]
        account = json.loads((output / 'account.json').read_text())
        assert account['positions'] == {CODE: 100}
        assert account['cash'] == pytest.approx(10000 - 2100 - 5.03)
        return
    if scenario == 'current_rules':
        observed = json.loads((output / 'native-observations.json').read_text())['events']
        assert [(row['lower'], row['upper'], row['minimum'], row['step']) for row in observed] == [
            (9, 11, 100, 100), (8, 12, 200, 200)]
        return
    if scenario == 'closed_orders':
        orders = [json.loads(line) for line in (output / 'orders.jsonl').read_text(encoding='utf-8').splitlines()]
        assert len(orders) == 4
        assert {(row['at'][11:16], row['order_type']) for row in orders} == {
            (clock, kind) for clock in ('11:30', '15:00') for kind in ('Market', 'Limit')
        }
        assert all(row['status'] == 'Invalid' and row['quantity'] == 0
                   and '常规交易时段' in row['message'] for row in orders)
        assert json.loads((output / 'account.json').read_text())['active_orders'] == []
        return
    if universe_targets:
        observed = json.loads((output / 'native-observations.json').read_text())
        assert observed['selections'] == [
            [timestamp(DATES[0], '09:30:00').replace(tzinfo=None).isoformat(), 1],
            [timestamp(DATES[2], '09:30:00').replace(tzinfo=None).isoformat(), 0],
        ]
        assert observed['before_publication'] == [0]
        assert observed['after_publication'] == [1]
        assert observed['held_after_exit'] == [True]
        assert observed['targets'] == [timestamp(day, '15:00:00').isoformat() for day in DATES[:2]]
        orders = [json.loads(line) for line in (output / 'orders.jsonl').read_text().splitlines()]
        assert [(row['at'], row['quantity']) for row in orders if row['quantity']] == [
            (timestamp(DATES[1], '09:32:00').isoformat(), 100),
            (timestamp(DATES[2], '09:32:00').isoformat(), -100),
        ]
        assert json.loads((output / 'account.json').read_text())['positions'] == {}
        return
    if scenario:
        if scenario in {'delisted', 'delisted_tail'}:
            account = json.loads((output / 'account.json').read_text())
            daily = [json.loads(line) for line in (output / 'daily.jsonl').read_text().splitlines()]
            orders = [json.loads(line) for line in (output / 'orders.jsonl').read_text().splitlines()]
            events = [json.loads(line) for line in (output / 'lifecycle.jsonl').read_text().splitlines()]
            assert account['positions'] == {code: 100}
            assert account['equity'] == account['cash']
            assert daily[-1 if tail else -2]['positions'][0]['price'] > 0
            if tail:
                assert account['terminal_observation_at'] == timestamp(DATES[-1], '15:00:00').isoformat()
                assert account['valuation_at'] == account['terminal_observation_at']
                assert len(daily) == 2 and daily[-1]['date'] == DATES[-2]
                trace = [json.loads(line) for line in (output / 'strategy-trace.jsonl').read_text().splitlines()]
                assert not any(row['kind'] == 'order_request' and row['at'].startswith(DATES[-1]) for row in trace)
                terminal = [json.loads(line) for line in (output / 'account_events.jsonl').read_text().splitlines()]
                assert len([row for row in terminal if row['reason'] == 'terminal']) == 1
            else:
                assert daily[-1]['positions'][0]['price'] == 0
            mark = account['valuation_marks'][0]
            assert mark['source'] == 'delisted_zero' and mark['price'] == 0
            assert mark['delisted_date'] == DATES[-2]
            assert mark['price_available_at'] == timestamp(DATES[-1], '00:00:00').isoformat()
            zero = [row for row in events if row['kind'] == 'delisted_zero_mark']
            assert len(zero) == 1 and zero[0]['cash_delta'] == 0 and zero[0]['quantity'] == 100
            assert [(row['phase'], row['registered_quantity'], row['cash_delta'])
                    for row in events if row['kind'] == 'corporate_action'] == [
                ('registered', 100, 0), ('ex', 100, 0), ('cash_paid', 100, 10)]
            assert [(row['Phase'], row['CashDelta']) for row in account['transitions']] == [
                ('registered', 0), ('ex', 0), ('cash_paid', 10)]
            fills = [row for row in orders if row['quantity']]
            assert len(fills) == 1 and fills[0]['quantity'] == 100
            assert account['cash'] == pytest.approx(10000 - fills[0]['quantity'] * fills[0]['price'] - fills[0]['fee'] + 10)
            return
        _assert_window_outcome(output, scenario, windows)
        return
    state = json.loads((output / 'native-observations.json').read_text())
    if frequency == '1d':
        assert [event['at'] for event in state['events']] == ['2021-10-11T09:30:00+08:00', '2021-10-12T09:30:00+08:00']
        assert [len(event['history']) for event in state['events']] == [1, 2]
    else:
        assert [event['at'] for event in state['events']] == [f'{day}T{clock}+08:00'
            for day in DATES for clock in ('09:31:00', '09:32:00', '15:00:00')]
        assert [len(event['history']) for event in state['events']] == [1, 2, 1, 2, 3, 1, 2, 3, 1]
    orders = [json.loads(line) for line in (output / 'orders.jsonl').read_text().splitlines()]
    fills = [row for row in orders if row['quantity']]
    expected_fills = ([
        ('2021-10-11T09:30:00+08:00', 100, 11), ('2021-10-12T09:30:00+08:00', -100, 12)] if frequency == '1d' else [
        ('2021-10-08T09:32:00+08:00', 100, 20), ('2021-10-11T09:32:00+08:00', -100, 21)])
    expected_fills = [(at, quantity, price + fraction / divisor) for at, quantity, price in expected_fills]
    assert [(row['at'], row['quantity'], row['price']) for row in fills] == (expected_fills[:1] if hold else expected_fills)
    if frequency == '1d':
        assert all(datetime.fromisoformat(row['submitted_at']) == datetime.fromisoformat(row['at']) for row in fills)
    else:
        assert all(datetime.fromisoformat(row['submitted_at']) < datetime.fromisoformat(row['at']) for row in fills)
    account = json.loads((output / 'account.json').read_text())
    assert account['positions'] == ({code: 100} if hold else {})
    daily = [json.loads(line) for line in (output / 'daily.jsonl').read_text().splitlines()]
    semantics = 'latest_visible_mark_not_session_close' if frequency == '1d' else 'session_close_mark'
    assert account['equity_semantics'] == semantics
    assert all(row['equity_semantics'] == semantics for row in daily)
    if frequency == '1d':
        assert daily[1]['positions'][0]['price'] == 11 + fraction / divisor
        assert daily[1]['valuation_marks'][0] == dict(instrument_id=code, price=11 + fraction / divisor, source='session_open',
            price_available_at='2021-10-11T09:30:00+08:00', price_source_at='2021-10-11T09:30:00+08:00',
            is_current_session=True, is_current_session_close=False)
    if hold:
        price = (20.9 if cash_action else 21) if missing_last else 12
        assert account['valuation_marks'][0]['price'] == price
        assert account['valuation_marks'][0]['source'] == (
            'corporate_action_reference' if cash_action and missing_last else 'completed_daily_close' if missing_last else 'session_open')
        assert account['valuation_marks'][0]['price_available_at'] == '2021-10-12T09:30:00+08:00'
        assert account['valuation_marks'][0]['is_current_session_close'] is False
        assert account['cash'] == (8904.98 if cash_action else 8894.98)
        assert account['equity'] == pytest.approx(account['cash'] + 100 * price)
        assert account['valuation_marks'][0]['is_current_session'] == (not missing_last or cash_action)
        assert account['valuation_marks'][0]['price_source_at'] == (
            '2021-10-11T15:00:00+08:00' if missing_last and not cash_action else '2021-10-12T09:30:00+08:00')
        assert [transition['Phase'] for transition in account['transitions']] == (['registered', 'ex', 'cash_paid'] if cash_action else [])
    assert not (output / 'intents.jsonl').read_text().strip()
    assert not list(output.glob('*.parquet'))
    assert not list(output.glob('*rule*.json'))
    if price_scale == 3:
        assert all(row['fee'] == 5 for row in fills)


def _assert_window_outcome(output, scenario, windows):
    trace = [json.loads(line) for line in (output / 'strategy-trace.jsonl').read_text().splitlines()]
    assert trace[0]['kind'] == 'algorithm_initialized'
    observations = output / 'native-observations.json'
    if scenario in {'empty', 'point_only'}:
        assert not observations.exists()
    else:
        assert json.loads(observations.read_text())['events']
    assert not (output / 'intents.jsonl').read_text().strip()
    if scenario in {'missing_mark', 'missing_rules'}:
        diagnostic = json.loads((output / 'failure-account.json').read_text())
        assert diagnostic['is_result'] is False
        assert diagnostic['positions'] == {CODE: 100}
        assert diagnostic['cash'] > 0
        assert 'equity' not in diagnostic
        assert CODE in diagnostic['reason'] and DATES[-1] in diagnostic['reason']
        entitlement = diagnostic['registered_entitlements'][0]
        assert entitlement == dict(action_id='cash-fixture', revision=2, instrument_id=CODE,
            registered_quantity=100, cash_entitlement=20, bonus_quantity=0, ex_applied=True,
            cash_paid=False, shares_arrived=False, shares_sellable=False,
            share_settlement_policy='actual_arrival', shares_arrival_date=None,
            shares_credited=False, shares_sellable_at=None, source_shares_arrival_at=None)
        assert [(row['Phase'], row['Revision']) for row in diagnostic['transitions']] == [('registered', 1), ('ex', 2)]
        assert not (output / 'account.json').exists()
        return
    account = json.loads((output / 'account.json').read_text())
    daily = [json.loads(line) for line in (output / 'daily.jsonl').read_text().splitlines()]
    assert account['valuation_at'] == windows[DATES[-1]]['end_at']
    assert account['equity_semantics'] == 'execution_window_mark'
    assert account['session_complete'] == (scenario == 'no_close_bar')
    assert [row['valuation_at'] for row in daily] == [windows[day]['end_at'] for day in DATES]
    if scenario == 'partial':
        assert account['positions'] == {CODE: 100}
        assert account['valuation_marks'][0]['price_source_at'] == timestamp(DATES[-1], '09:32:00').isoformat()
        assert account['valuation_marks'][0]['is_current_session_close'] is False
        assert len(account['active_orders']) == 1
        assert account['active_orders'][0]['remaining_quantity'] == 100
        assert account['active_orders'][0]['filled_quantity'] == 0
        assert daily[-1]['active_orders'] == 1
        assert account['additional_equity'] == 10
        assert [row['Phase'] for row in account['transitions']] == ['registered', 'ex']
    elif scenario == 'opening_cutoff':
        assert account['positions'] == {CODE: 100}
        assert account['valuation_marks'][0]['price_source_at'] == timestamp(DATES[-2], '15:00:00').isoformat()
        assert account['valuation_marks'][0]['is_current_session'] is False
    elif scenario == 'opening_cash':
        assert account['positions'] == {CODE: 100}
        assert account['cash'] == pytest.approx(8004.98)
        assert account['additional_equity'] == 0
        assert [row['Phase'] for row in account['transitions']] == ['registered', 'ex', 'cash_paid']
