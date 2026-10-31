"""按需报价的纯 Python 定向测试，不加载 LEAN 或写运行目录。"""
import ast
from copy import deepcopy
from datetime import datetime
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).with_name('ResearchLeanAlgorithm.py')
TREE = ast.parse(SOURCE.read_text(encoding='utf-8-sig'))
NAMESPACE = dict(deepcopy=deepcopy, datetime=datetime, json=json, QCAlgorithm=object)
NODES = [node for node in TREE.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
         and node.name in {'json_bytes', 'write_json_line', 'VisibleMarket', 'ResearchLeanAlgorithm'}]
exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), 'exec'), NAMESPACE)
VisibleMarket = NAMESPACE['VisibleMarket']
ResearchLeanAlgorithm = NAMESPACE['ResearchLeanAlgorithm']
json_bytes = NAMESPACE['json_bytes']

FIRST = '600000.XSHG'
SECOND = '600001.XSHG'
NOW = '2021-10-08T09:31:00+08:00'


def quote_row(code, available_at=NOW):
    return dict(instrument={'instrument_id': code}, available_at=available_at,
                price={'units': 1234, 'scale': 2, 'currency': 'CNY'}, values=[])


class FlushedBuffer(io.StringIO):
    def __init__(self):
        super().__init__()
        self.flushed = []

    def flush(self):
        self.flushed.append(self.getvalue())


class SnapshotHost:
    """复现 Host 的按证券、按当前完成时刻返回报价的边界。"""

    NowText = NOW

    def __init__(self, rows):
        self.rows = rows
        self.calls = []
        self.order_event_count = 0

    def Snapshot(self, codes_json):
        codes = json.loads(codes_json)
        self.calls.append(codes)
        quotes = {code: deepcopy(self.rows[code]) for code in codes if code in self.rows
                  and datetime.fromisoformat(self.rows[code]['available_at']) <= datetime.fromisoformat(self.NowText)}
        return json.dumps(dict(order_event_count=self.order_event_count,
                               account={'positions': [], 'active_orders': []}, quotes=quotes,
                               missing_instruments=[code for code in codes if code not in quotes]))


class VisibleMarketTest(unittest.TestCase):
    def test_direct_signals_are_requested_for_current_day_and_filtered_by_visibility(self):
        algorithm = ResearchLeanAlgorithm()
        algorithm._signal_binding = dict(binding_id='signals', columns=['rank'])
        algorithm._spec = {}
        calls = []
        rows = [dict(binding_id='signals', available_at=f'2021-10-08T09:{minute}:00+08:00',
                     values=[['rank', rank]]) for minute, rank in [('30', 1), ('31', 2), ('32', 3)]]
        payload = dict(available_at='2021-10-08T09:30:00+08:00', selection=rows)
        def daily_signals(day):
            calls.append(day)
            return json.dumps(payload)
        algorithm._host = SimpleNamespace(HasDirectInputs=True, DailySignals=daily_signals)
        self.assertEqual(algorithm._visible_inputs('2021-10-08', datetime.fromisoformat(NOW)), [rows[1]])
        self.assertEqual(calls, ['2021-10-08'])
        payload['available_at'] = '2021-10-08T09:32:00+08:00'
        self.assertEqual(algorithm._visible_inputs('2021-10-08', datetime.fromisoformat(NOW)), [])
        algorithm._host = SimpleNamespace(HasDirectInputs=False)
        algorithm._spec = dict(daily={'2021-10-08': dict(payload, available_at=NOW)})
        self.assertEqual(algorithm._visible_inputs('2021-10-08', datetime.fromisoformat(NOW)), [rows[1]])

    def test_declared_columns_and_history_window(self):
        host = SnapshotHost({FIRST: quote_row(FIRST)})
        rows = [quote_row(FIRST, f'2021-10-08T09:{minute}:00+08:00') for minute in ('29', '30', '31')]
        for index, row in enumerate(rows):
            row['values'] = {'open': 1200 + index, 'high': 1250, 'low': 1150, 'close': 1234, 'volume': 100 + index}
        host.History = lambda code, count: json.dumps(rows[-count:])
        market = VisibleMarket('prices', dict(quotes={}, missing_instruments=[]), host, {FIRST},
                               0, 100000, columns=('open', 'volume'), lookback_bars=3)
        history = market.history('prices', FIRST, 3)
        self.assertEqual([row['values'] for row in history],
                         [dict(open=1200 + index, volume=100 + index) for index in range(3)])
        size = market._context_size
        history[0]['values']['open'] = 0
        self.assertEqual(market.history('prices', FIRST, 3)[0]['values']['open'], 1200)
        self.assertEqual(market._context_size, size)
        with self.assertRaisesRegex(ValueError, 'lookback_bars'):
            market.history('prices', FIRST, 4)

    def test_history_future_rows_and_budget_rejected(self):
        host = SnapshotHost({})
        host.History = lambda code, count: json.dumps([quote_row(FIRST, '2021-10-08T09:32:00+08:00')])
        market = VisibleMarket('prices', dict(quotes={}, missing_instruments=[]), host, {FIRST}, 0, 100000, lookback_bars=3)
        with self.assertRaisesRegex(ValueError, '未来数据'):
            market.history('prices', FIRST, 2)
        host.History = lambda code, count: json.dumps([quote_row(FIRST), quote_row(FIRST)])
        limited = VisibleMarket('prices', dict(quotes={}, missing_instruments=[]), host, {FIRST}, 0, 1, lookback_bars=3)
        with self.assertRaisesRegex(ValueError, 'max_context_bytes'):
            limited.history('prices', FIRST, 2)
        self.assertEqual(limited._histories, {})

    def make_market(self, rows=None, limit=100000, snapshot=None):
        host = SnapshotHost(rows if rows is not None else {FIRST: quote_row(FIRST)})
        if snapshot is None:
            snapshot = dict(quotes={}, missing_instruments=[])
        event = dict(inputs=[], account={'positions': []}, missing_instruments=snapshot['missing_instruments'])
        context = dict(strategy={}, parameters={})
        baseline = len(json_bytes(dict(context=context, event=event, quotes=snapshot['quotes'])))
        market = VisibleMarket('prices', snapshot, host, {FIRST, SECOND}, baseline, limit)
        return market, snapshot, host, event, context, baseline

    def assert_context_size(self, market, snapshot, event, context):
        self.assertEqual(market._context_size,
                         len(json_bytes(dict(context=context, event=event, quotes=snapshot['quotes']))))

    def test_no_signals_empty_positions_can_read_first_quote(self):
        market, snapshot, host, event, context, _ = self.make_market()
        row = market.quote('prices', FIRST)
        self.assertEqual(host.calls, [[FIRST]])
        self.assertEqual(row['values'], {'close': 1234})
        self.assertEqual(row['binding_id'], 'prices')
        self.assertEqual(row['input_id'], f'prices:{FIRST}:{NOW}')
        self.assertEqual(row, snapshot['quotes'][FIRST])
        row['values']['close'] = 0
        self.assertEqual(snapshot['quotes'][FIRST]['values']['close'], 1234)
        self.assert_context_size(market, snapshot, event, context)

    def test_undeclared_instrument_rejected_before_host_query(self):
        market, _, host, _, _, _ = self.make_market()
        with self.assertRaisesRegex(ValueError, '证券不在冻结范围'):
            market.quote('prices', '600999.XSHG')
        self.assertEqual(host.calls, [])
        with self.assertRaisesRegex(ValueError, 'binding 未声明'):
            market.quote('undeclared', FIRST)

    def test_future_quote_is_missing_and_history_stays_empty(self):
        market, snapshot, host, event, context, baseline = self.make_market(
            {FIRST: quote_row(FIRST, '2021-10-08T09:32:00+08:00')})
        self.assertIsNone(market.quote('prices', FIRST))
        self.assertEqual(market.history('prices', FIRST, 1), ())
        self.assertEqual(snapshot['quotes'], {})
        self.assertEqual(event['missing_instruments'], [FIRST])
        self.assertEqual(host.calls, [[FIRST]])
        self.assertGreater(market._context_size, baseline)
        self.assert_context_size(market, snapshot, event, context)

    def test_context_limit_rejects_quote_before_delivery(self):
        probe, _, _, _, _, baseline = self.make_market()
        probe.quote('prices', FIRST)
        exact_limit = probe._context_size
        market, snapshot, _, _, _, _ = self.make_market(limit=exact_limit - 1)
        with self.assertRaisesRegex(ValueError, 'max_context_bytes'):
            market.quote('prices', FIRST)
        self.assertEqual(snapshot['quotes'], {})
        self.assertEqual(market._context_size, baseline)
        boundary, _, _, _, _, _ = self.make_market(limit=exact_limit)
        self.assertIsNotNone(boundary.quote('prices', FIRST))

    def test_repeated_quote_and_history_do_not_count_twice(self):
        market, snapshot, host, event, context, _ = self.make_market()
        first = market.quote('prices', FIRST)
        size = market._context_size
        self.assertEqual(market.quote('prices', FIRST), first)
        self.assertEqual(market.history('prices', FIRST, 1), (first,))
        self.assertEqual(market._context_size, size)
        self.assertEqual(host.calls, [[FIRST]])
        self.assert_context_size(market, snapshot, event, context)

    def test_cumulative_quote_and_missing_bytes_include_commas(self):
        market, snapshot, _, event, context, _ = self.make_market(
            {FIRST: quote_row(FIRST), SECOND: quote_row(SECOND)})
        market.quote('prices', FIRST)
        market.quote('prices', SECOND)
        self.assert_context_size(market, snapshot, event, context)
        missing, snapshot, _, event, context, _ = self.make_market({})
        missing.quote('prices', FIRST)
        missing.quote('prices', SECOND)
        self.assert_context_size(missing, snapshot, event, context)
        _, _, _, _, _, baseline = self.make_market({})
        limited, snapshot, _, _, _, _ = self.make_market({}, limit=baseline + len(json_bytes(FIRST)) - 1)
        with self.assertRaisesRegex(ValueError, 'max_context_bytes'):
            limited.quote('prices', FIRST)
        self.assertEqual(snapshot['missing_instruments'], [])
        self.assertEqual(limited._context_size, baseline)

    def make_algorithm(self):
        host = SnapshotHost({FIRST: quote_row(FIRST)})
        trace = io.StringIO()
        algorithm = ResearchLeanAlgorithm()
        algorithm._subscriptions = {'bar'}
        algorithm._in_dispatch = False
        algorithm._closing = False
        algorithm._seq = 0
        algorithm.time = datetime.fromisoformat(NOW)
        algorithm._host = host
        algorithm._bar_binding = 'prices'
        algorithm._spec = dict(declaration={}, parameters={}, instruments={FIRST: {'instrument_id': FIRST}})
        algorithm._limits = dict(max_context_bytes=100000, callback_wall_seconds=60)
        algorithm._context_bytes = len(json_bytes(dict(strategy={}, parameters={})))
        algorithm._visible_inputs = lambda day, now: []
        algorithm._state = {}
        algorithm._trace = trace
        algorithm._callback_events = FlushedBuffer()
        algorithm._failed = False
        algorithm._host_finished = False
        algorithm._check_response = lambda response, kind: None
        algorithm._statuses = []
        algorithm._status = lambda status, error=None: algorithm._statuses.append((status, error))
        class Output:
            def __truediv__(self, name):
                return SimpleNamespace(write_bytes=lambda data: None)
        algorithm._output = Output()
        NAMESPACE.update(os=SimpleNamespace(getpid=lambda: 1), time=SimpleNamespace(time=lambda: 0))
        return algorithm, trace, host

    def test_callback_trace_contains_on_demand_quote(self):
        algorithm, trace, host = self.make_algorithm()
        host.order_event_count = 17
        def entry(context, event, state):
            started = json.loads(algorithm._callback_events.flushed[-1].splitlines()[-1])
            self.assertEqual(started, dict(status='started', event_seq=event['event_seq'],
                pid=1, kind='bar', started_at_unix=0, wall_seconds=60))
            self.assertEqual(event['inputs'], [])
            self.assertEqual(event['account']['positions'], [])
            self.assertEqual(context['market'].quote('prices', FIRST)['values'], {'close': 1234})
            return dict(state={'read': FIRST}, intents=[])
        algorithm._entry = entry
        algorithm._call_strategy('bar')
        host.order_event_count = 18
        algorithm._call_strategy('bar')
        records = [json.loads(line) for line in trace.getvalue().splitlines()]
        self.assertEqual([record['order_event_count'] for record in records], [17, 18])
        self.assertEqual(records[0]['event']['decision_time'], records[1]['event']['decision_time'])
        for record in records:
            self.assertNotIn('order_event_count', record['event'])
            self.assertEqual(record['quotes'][FIRST]['values'], {'close': 1234})
            self.assertEqual(record['response']['state'], {'read': FIRST})
        self.assertEqual(host.calls, [[], [FIRST], [], [FIRST]])
        callback_records = [json.loads(line) for line in algorithm._callback_events.getvalue().splitlines()]
        self.assertEqual([(row['status'], row['event_seq']) for row in callback_records],
                         [('started', 1), ('finished', 1), ('started', 2), ('finished', 2)])
        self.assertEqual(len(algorithm._callback_events.flushed), 4)
        self.assertTrue(all(value.endswith('\n') for value in algorithm._callback_events.flushed))
        self.assertFalse(algorithm._callback_events.closed)

    def test_callback_exception_flushes_finished_record(self):
        algorithm, trace, _ = self.make_algorithm()
        def entry(context, event, state):
            self.assertEqual(len(algorithm._callback_events.flushed), 1)
            raise ValueError('策略执行失败')
        algorithm._entry = entry
        with self.assertRaisesRegex(ValueError, '策略执行失败'):
            algorithm._call_strategy('bar')
        self.assertEqual([json.loads(line) for line in algorithm._callback_events.getvalue().splitlines()],
            [dict(status='started', event_seq=1, pid=1, kind='bar', started_at_unix=0, wall_seconds=60),
             dict(status='finished', event_seq=1)])
        self.assertEqual(len(algorithm._callback_events.flushed), 2)
        self.assertEqual(trace.getvalue(), '')
        self.assertEqual(algorithm._statuses, [('failed', '策略执行失败')])

    def test_algorithm_end_closes_callback_log(self):
        algorithm, trace, host = self.make_algorithm()
        host.HasRuntimeError = False
        host.EngineStatus = 'Running'
        host.Finish = lambda: None
        algorithm.on_end_of_algorithm()
        self.assertTrue(algorithm._callback_events.closed)
        self.assertTrue(trace.closed)
        self.assertTrue(algorithm._host_finished)
        self.assertEqual(algorithm._statuses, [('completed', None)])

    def test_failed_algorithm_end_also_closes_callback_log(self):
        algorithm, trace, _ = self.make_algorithm()
        algorithm._failed = True
        with self.assertRaisesRegex(RuntimeError, '不能发布成功终态'):
            algorithm.on_end_of_algorithm()
        self.assertTrue(algorithm._callback_events.closed)
        self.assertTrue(trace.closed)
        self.assertFalse(algorithm._host_finished)


if __name__ == '__main__':
    unittest.main()
