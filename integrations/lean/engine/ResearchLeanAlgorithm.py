"""将已准入的A股日线和一分钟策略接到LEAN原生账户与订单。"""
from copy import deepcopy
from datetime import datetime
import importlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time

import clr
clr.AddReference('QuantConnect.Algorithm')
clr.AddReference('QuantConnect.Common')
clr.AddReference('QuantConnect.Configuration')
clr.AddReference('QuantWitnessLeanHost')
from QuantConnect.Algorithm import QCAlgorithm
from QuantConnect.Orders import OrderStatus
from QuantWitness.Lean.Research import LeanStrategyHost
from QuantConnect.Configuration import Config


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':')).encode('utf-8')


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_bytes(json_bytes(value))
    temporary.replace(path)


def write_json_line(stream, **value):
    stream.write(json_bytes(value).decode('utf-8') + '\n')
    stream.flush()


def load_entry(spec, directory):
    entry = spec['entry']
    name = entry['module']
    source = spec.get('execution_source')
    source_root = directory / 'strategy-source'
    if source_root.is_dir():
        # 源码由主链从准入 bundle 封存到本次运行，不回退到项目工作目录。
        sys.path.insert(0, str(source_root))
        from research_pipeline.runtime.project_operator_worker import _load_project_entry_module
        module = _load_project_entry_module(source_root=source_root,
            entry_module=name, synthetic_root='_quantwitness_lean_strategy')
        if not Path(module.__file__).resolve().is_relative_to(source_root.resolve()):
            raise ValueError('策略模块必须来自 run/strategy-source')
    elif source is not None:
        path = Path(source)
        if not path.is_absolute():
            raise ValueError('execution_source 必须是已冻结的绝对路径')
        sys.path.insert(0, str(path.parent))
        definition = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(definition)
        sys.modules[name] = module
        definition.loader.exec_module(module)
    else:
        raise ValueError('缺少 run/strategy-source 或已冻结的 execution_source')
    function = getattr(module, entry['function'])
    if not callable(function):
        raise ValueError('策略入口必须可调用')
    return function


class VisibleMarket:
    def __init__(self, binding_id, snapshot, host, instruments, context_size, max_context_bytes,
                 columns=('close',), lookback_bars=1):
        self._binding_id = binding_id
        self._quotes = snapshot['quotes']
        self._missing = snapshot['missing_instruments']
        self._host = host
        self._instruments = instruments
        self._requested = set(self._quotes) | set(self._missing)
        self._context_size = context_size
        self._max_context_bytes = max_context_bytes
        self._columns = tuple(columns)
        self._lookback = lookback_bars
        self._histories = {}

    @staticmethod
    def prepare_quote(binding_id, code, quote, columns=('close',)):
        quote['binding_id'] = binding_id
        quote['input_id'] = quote['price_input_id'] = f"{binding_id}:{code}:{quote['available_at']}"
        values = quote['values'] if isinstance(quote['values'], dict) else {'close': quote['price']['units']}
        if not set(columns) <= set(values):
            raise ValueError('引擎报价缺少声明字段')
        quote['values'] = {column: values[column] for column in columns}

    def quote(self, binding_id, instrument_id):
        if binding_id != self._binding_id:
            raise ValueError('行情 binding 未声明')
        if instrument_id not in self._instruments:
            raise ValueError('行情证券不在冻结范围内')
        if instrument_id not in self._requested:
            snapshot = json.loads(str(self._host.Snapshot(json.dumps([instrument_id]))))
            quote = snapshot['quotes'].get(instrument_id)
            if quote is None:
                extra_bytes = len(json_bytes(instrument_id)) + bool(self._missing)
            else:
                self.prepare_quote(binding_id, instrument_id, quote, self._columns)
                extra_bytes = len(json_bytes({instrument_id: quote})) - 2 + bool(self._quotes)
            if self._context_size + extra_bytes > self._max_context_bytes:
                raise ValueError('context 超过 max_context_bytes')
            self._context_size += extra_bytes
            self._requested.add(instrument_id)
            # 共享当前事件的报价与缺失记录，让按需交付进入同一份 trace。
            if quote is None:
                self._missing.append(instrument_id)
            else:
                self._quotes[instrument_id] = quote
        return deepcopy(self._quotes.get(instrument_id))

    def history(self, binding_id, instrument_id, count):
        if type(count) is not int or not 0 < count <= self._lookback:
            raise ValueError('history count 超出声明 lookback_bars')
        if count == 1:
            row = self.quote(binding_id, instrument_id)
            return () if row is None else (row,)
        if binding_id != self._binding_id or instrument_id not in self._instruments:
            raise ValueError('历史行情 binding 或证券未声明')
        key = f'{instrument_id}:{count}'
        if key not in self._histories:
            rows = json.loads(str(self._host.History(instrument_id, count)))
            now = datetime.fromisoformat(str(self._host.NowText))
            for row in rows:
                if datetime.fromisoformat(row['available_at']) > now:
                    raise ValueError('历史行情包含未来数据')
                self.prepare_quote(binding_id, instrument_id, row, self._columns)
            extra = len(json_bytes({key: rows})) - 2 + bool(self._histories)
            if self._context_size + extra > self._max_context_bytes:
                raise ValueError('history 超过 max_context_bytes')
            self._context_size += extra
            self._histories[key] = rows
        return tuple(deepcopy(self._histories[key]))


class ResearchLeanAlgorithm(QCAlgorithm):
    def initialize(self):
        self._output = Path(Config.Get('results-destination-folder'))
        self._seq = 0
        self._failed = False
        self._ready = False
        self._host_finished = False
        self._state = {}
        self._initialized = False
        self._session_day = None
        self._in_dispatch = False
        self._feedback_pending = False
        self._closing = False
        try:
            self._spec = json.loads((self._output / 'strategy-input.json').read_text(encoding='utf-8-sig'))
            self._status('running')
            declaration = self._spec['declaration']
            bars = [row for row in declaration['input_bindings'] if row['source'] == 'bars']
            signals = [row for row in declaration['input_bindings'] if row['source'] == 'signals']
            if (len(bars) != 1 or not bars[0]['columns']
                    or not set(bars[0]['columns']) <= {'open', 'high', 'low', 'close', 'volume'}
                    or type(bars[0]['lookback_bars']) is not int or bars[0]['lookback_bars'] < 1 or bars[0]['delivery'] != 'columnar'
                    or len(signals) > 1 or any(row['lookback_bars'] != 1 or row['delivery'] != 'inline' for row in signals)
                    or len(bars) + len(signals) != len(declaration['input_bindings'])):
                raise ValueError('需要一个OHLCV bars声明及至多一个 signals/lookback1 binding')
            self._bar_binding = bars[0]['binding_id']
            self._bar_columns = bars[0]['columns']
            self._bar_lookback = bars[0]['lookback_bars']
            self._signal_binding = signals[0] if signals else None
            if len({row['binding_id'] for row in declaration['input_bindings']}) != len(declaration['input_bindings']):
                raise ValueError('binding_id 不能重复')
            self._subscriptions = set(declaration['subscriptions'])
            if declaration['schedules'] or self._subscriptions - {
                    'initialize', 'before_trading', 'bar', 'execution_feedback', 'after_trading'}:
                raise ValueError('当前有限 profile 不支持 scheduled 或其他事件')
            self._limits = declaration['limits']
            for key in ('max_state_bytes', 'max_context_bytes', 'max_response_bytes',
                        'max_intents_per_event', 'callback_wall_seconds'):
                if type(self._limits[key]) is not int or self._limits[key] <= 0:
                    raise ValueError(f'{key} 必须为正整数')
            self._context_bytes = len(json_bytes(dict(strategy=declaration, parameters=self._spec['parameters'])))
            self._entry = load_entry(self._spec, self._output)
            self._trace = (self._output / 'strategy-trace.jsonl').open('w', encoding='utf-8')
            self._callback_events = (self._output / 'callback-events.jsonl').open('w', encoding='utf-8')
            self._host = LeanStrategyHost(self, str(self._output))
            self.schedule.on(self.date_rules.every_day(self._host.ClockSymbol),
                             self.time_rules.at(9, 30), self._session_open)
            self._ready = True
        except Exception as error:
            self._status('failed', str(error))
            raise

    def _status(self, status, error=None):
        if status == 'failed':
            self._failed = True
        payload = dict(status=status, host_finished=self._host_finished,
                       event_seq=self._seq, execution_model=(
                           'lean_next_session_open_v1' if getattr(self, '_spec', {}).get('frequency') == '1d'
                           else 'lean_next_completed_close_v1'),
                       artifact_type='research.lean-simulation.v1', error=error)
        write_json(self._output / 'engine-status.json', payload)

    def _session_open(self):
        day = self.time.date().isoformat()
        if self._session_day == day:
            return
        self._host.OpenSession()
        self._session_day = day
        if not self._initialized:
            self._call_strategy('initialize')
            self._initialized = True
        self._call_strategy('before_trading')

    def _visible_inputs(self, day, now):
        daily = (json.loads(str(self._host.DailySignals(day))) if self._host.HasDirectInputs
                 else self._spec['daily'][day])
        if datetime.fromisoformat(daily['available_at']) > now:
            return []
        rows = daily['selection']
        if rows and self._signal_binding is None:
            raise ValueError('存在每日 signals 输入但未声明 signals binding')
        for row in rows:
            if (row['binding_id'] != self._signal_binding['binding_id']
                    or not set(dict(row['values'])) <= set(self._signal_binding['columns'])):
                raise ValueError('每日 signals 与冻结 binding 不一致')
        visible = [row for row in rows if datetime.fromisoformat(row['available_at']) <= now]
        if not visible:
            return []
        latest = max(datetime.fromisoformat(row['available_at']) for row in visible)
        return [row for row in visible if datetime.fromisoformat(row['available_at']) == latest]

    def _check_response(self, response, kind):
        declaration = self._spec['declaration']
        if set(response) != {'protocol_version', 'event_seq', 'state_version', 'state_schema_id', 'state', 'intents'}:
            raise ValueError('策略应答字段不匹配')
        for key in ('protocol_version', 'state_version', 'state_schema_id'):
            if response[key] != declaration[key]:
                raise ValueError(f'策略应答 {key} 与准入声明不一致')
        if type(response['event_seq']) is not int or response['event_seq'] != self._seq:
            raise ValueError('策略应答事件序号不匹配')
        if type(response['state']) is not dict:
            raise ValueError('state 必须为 JSON 对象')
        if len(json_bytes(response['state'])) > self._limits['max_state_bytes']:
            raise ValueError('state 超过 max_state_bytes')
        if len(json_bytes(response)) > self._limits['max_response_bytes']:
            raise ValueError('response 超过 max_response_bytes')
        intents = response['intents']
        if not isinstance(intents, (list, tuple)) or len(intents) > self._limits['max_intents_per_event']:
            raise ValueError('intents 超过 max_intents_per_event 或不是数组')
        if intents and kind in {'initialize', 'before_trading', 'after_trading'}:
            raise ValueError('初始化、盘前和盘后不允许提交意图')
        for index, intent in enumerate(intents):
            if intent['intent_id'] != f'{self._seq}:{index}':
                raise ValueError('intent_id 必须由事件序号和批内顺序组成')
            self._host.ValidateIntent(json_bytes(intent).decode('utf-8'))

    def _call_strategy(self, kind):
        if kind not in self._subscriptions:
            return
        if self._in_dispatch:
            self._feedback_pending = True
            return
        self._in_dispatch = True
        try:
            current_kind = kind
            # 同步订单反馈在当前调用结束后交付，避免递归改写策略状态。
            for _ in range(32):
                self._feedback_pending = False
                day = self.time.date().isoformat()
                now = str(self._host.NowText)
                selection = self._visible_inputs(day, datetime.fromisoformat(now))
                codes = sorted({row['instrument']['instrument_id'] for row in selection if row['instrument'] is not None})
                snapshot = json.loads(str(self._host.Snapshot(json.dumps(codes))))
                for code, quote in snapshot['quotes'].items():
                    VisibleMarket.prepare_quote(self._bar_binding, code, quote, getattr(self, '_bar_columns', ('close',)))
                self._seq += 1
                event = dict(event_seq=self._seq, trading_date=day, decision_time=now, kind=current_kind,
                             inputs=selection, missing_instruments=snapshot['missing_instruments'], account=snapshot['account'])
                context = dict(strategy=self._spec['declaration'], parameters=self._spec['parameters'])
                context_size = self._context_bytes + len(json_bytes(dict(context=None, event=event, quotes=snapshot['quotes']))) - 4
                if context_size > self._limits['max_context_bytes']:
                    raise ValueError('context 超过 max_context_bytes')
                context['market'] = VisibleMarket(self._bar_binding, snapshot, self._host,
                    self._spec['instruments'], context_size, self._limits['max_context_bytes'],
                    getattr(self, '_bar_columns', ('close',)), getattr(self, '_bar_lookback', 1))
                write_json_line(self._callback_events, status='started', pid=os.getpid(),
                    event_seq=self._seq, kind=current_kind, started_at_unix=time.time(),
                    wall_seconds=self._limits['callback_wall_seconds'])
                # 策略在当前回调线程同步执行；超时由主会话终止整个子进程。
                try:
                    response = self._entry(context, event, self._state)
                finally:
                    write_json_line(self._callback_events, status='finished', event_seq=self._seq)
                self._check_response(response, current_kind)
                self._state = response['state']
                self._trace.write(json_bytes(dict(order_event_count=snapshot['order_event_count'],
                    event=event, quotes=snapshot['quotes'], histories=context['market']._histories,
                    response=response)).decode('utf-8') + '\n')
                self._trace.flush()
                for intent in response['intents']:
                    self._host.Submit(json_bytes(intent).decode('utf-8'))
                if not self._feedback_pending or self._closing:
                    break
                current_kind = 'execution_feedback'
            else:
                raise RuntimeError('同一时点的策略反馈超过32次')
        except Exception as error:
            self._status('failed', str(error))
            raise
        finally:
            self._in_dispatch = False

    def on_data(self, data):
        if data.bars.count == 0 and not (
                self.time.hour == 15 and self.time.minute == 0
                or self._host.IsDaily and self.time.hour == 9 and self.time.minute == 30):
            return
        if self._session_day != self.time.date().isoformat():
            raise RuntimeError('行情事件之前必须先交付当日09:30盘前阶段')
        count = self._host.ObserveBars(data)
        if self._host.IsDaily and self.time.hour == 9 and self.time.minute == 30 and self.time.second == 1:
            self._host.CloseOrders()
        if count:
            self._call_strategy('bar')
        if self.time.hour == 15 and self.time.minute == 0:
            self._closing = True
            try:
                self._host.CloseOrders()
                self._call_strategy('after_trading')
                self._host.CloseSession()
                self._trace.flush()
            finally:
                self._closing = False

    def on_order_event(self, order_event):
        self._host.OnOrderEvent(order_event)
        if self._ready and self._host.IsDaily and self._session_day != self.time.date().isoformat():
            self._session_open()
        if self._ready and self._session_day is not None and not self._closing and order_event.status in (
                OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELED, OrderStatus.INVALID):
            self._call_strategy('execution_feedback')

    def on_end_of_algorithm(self):
        try:
            self._callback_events.close()
            self._trace.close()
            if self._failed or self._host.HasRuntimeError or str(self._host.EngineStatus) not in {'Running', 'Completed'}:
                raise RuntimeError('LEAN 未正常完成，不能发布成功终态')
            (self._output / 'strategy-state.json').write_bytes(json_bytes(self._state))
            self._host.Finish()
            self._host_finished = True
            self._status('completed')
        except Exception as error:
            self._status('failed', str(error))
            raise
