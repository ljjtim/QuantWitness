"""现货金融场景直接使用原生订阅、History和订单。"""
from AlgorithmImports import *
from QuantConnect.Configuration import Config
from QuantWitness.Lean.Research import NativeMarketLifecycle
from QuantWitness.LeanInput import ResearchTradeBar
from System import Decimal as ClrDecimal
from datetime import datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

class NativeFixture(QCAlgorithm):
    def initialize(self):
        self.root=Path(Config.Get('results-destination-folder'))
        self.spec=json.loads((self.root/'strategy-input.json').read_text())
        self.params=self.spec['parameters']
        self.daily=self.params['frequency']=='1d'
        self.code=next(iter(self.spec['instruments']))
        self.symbol=self.add_equity(self.code.split('.')[0],Resolution.DAILY if self.daily else Resolution.MINUTE,
            'jqchina',fill_forward=False,data_normalization_mode=DataNormalizationMode.RAW).symbol
        self.events=[]
        self.open_ticket=None
        self.fill_ticket=None

    def on_data(self,data):
        if self.is_warming_up or not data.bars.contains_key(self.symbol): return
        now=self.time.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
        window=self.spec['execution_windows'].get(now.date().isoformat())
        if window is None or not datetime.fromisoformat(window['start_at'])<=now<=datetime.fromisoformat(window['end_at']): return
        if self.params['scenario']=='changed_fee_model':
            self.securities[self.symbol].set_fee_model(ConstantFeeModel(0))
            self.market_order(self.symbol, 100)
            return
        if self.params['scenario']=='current_rules':
            if now.date().isoformat()=='2021-10-08' and now.strftime('%H:%M') in ('09:31','09:32'):
                rule=NativeMarketLifecycle.CurrentSpotOrderRules(self.symbol)
                self.events.append(dict(lower=int(rule.LowerLimit),upper=int(rule.UpperLimit),
                    minimum=int(rule.MarketBuy.Minimum),step=int(rule.MarketBuy.Step)))
                (self.root/'native-observations.json').write_text(json.dumps(dict(events=self.events)))
            return
        if self.params['scenario']=='closed_orders':
            if now.date().isoformat()=='2021-10-08' and now.strftime('%H:%M') in ('11:30', '15:00'):
                self.market_order(self.symbol, 100, asynchronous=True, tag='closed-market')
                self.limit_order(self.symbol, 100, 10, tag='closed-limit')
            return
        if self.params['scenario'] in ('order_trace', 'order_trace_strict'):
            if now.date().isoformat() != '2021-10-08':
                return
            if now.strftime('%H:%M') == '09:31':
                self.market_order(self.symbol, 0, tag='zero-submit')
                self.open_ticket = self.limit_order(self.symbol, 100, 0.50, tag='open-limit')
                self.fill_ticket = self.market_order(self.symbol, 100, asynchronous=True, tag='filled-market')
            elif now.strftime('%H:%M') == '09:32':
                assert self.fill_ticket.status == OrderStatus.FILLED
                self.fill_ticket.update_quantity(200, 'terminal-update')
                self.fill_ticket.cancel('terminal-cancel')
                self.transactions.cancel_order(999999, 'unknown-cancel')
                self.open_ticket.update_limit_price(0.75, 'update-limit')
            elif now.strftime('%H:%M') == '09:33':
                self.open_ticket.cancel('cancel-limit')
                self.transactions.cancel_order(self.open_ticket.order_id, 'duplicate-cancel')
                self.transactions.cancel_order(self.open_ticket.order_id, 'duplicate-cancel')
                self.open_ticket.cancel('duplicate-ticket-cancel')
                if os.environ.get('QUANTWITNESS_STRICT_AUDIT') == '1':
                    with (self.root / 'strategy-trace.jsonl').open() as trace:
                        assert 'duplicate-ticket-cancel' in trace.read()
            return
        if self.params['scenario'] == 'zero_lifecycle':
            if now.isoformat() == '2021-10-11T09:31:00+08:00':
                self.market_order(self.symbol, 100, asynchronous=True)
            return
        bars=list(self.history[TradeBar](self.symbol,3,Resolution.DAILY if self.daily else Resolution.MINUTE))
        if self.daily and not bars: return
        assert bars and all(bar.end_time<=self.time for bar in bars)
        if self.daily: assert all(bar.ObservationEndTime.date()<self.time.date() for bar in bars)
        account=json.loads(NativeMarketLifecycle.AccountView())
        assert account['available_cash']['scale']==2
        if self.params['scenario']=='partial':
            assert json.loads(NativeMarketLifecycle.AccountView()) == account
            registered = json.loads(NativeMarketLifecycle.AccountView(True))
            assert registered['unsettled_registration_codes'] is not None
            assert {key: value for key, value in registered.items() if key != 'unsettled_registration_codes'} == {
                key: value for key, value in account.items() if key != 'unsettled_registration_codes'}
        self.events.append(dict(at=now.isoformat(),history=[dict(available_at=bar.end_time.isoformat()) for bar in bars]))
        (self.root/'native-observations.json').write_text(json.dumps(dict(events=self.events)))
        pending=self.params['scenario']=='partial' and now.isoformat()=='2021-10-12T09:32:00+08:00'
        if pending or ((self.daily or now.strftime('%H:%M:%S')=='09:31:00' and now.date().isoformat()!='2021-10-12')
            and not (self.params['hold'] and len(self.events)>1)):
            buy=pending or now.date().isoformat()==('2021-10-11' if self.daily else '2021-10-08')
            quantity=100 if buy else -100
            if self.params['price_scale']==3 and buy:
                budget=1105.70 if self.daily else 2005.70
                quantity=NativeMarketLifecycle.QuantityForBudget(self.symbol,ClrDecimal.Parse(str(budget)))
            self.market_order(self.symbol,quantity,asynchronous=True)
