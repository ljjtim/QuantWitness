"""期货金融场景直接使用原生显式开平委托。"""
from AlgorithmImports import *
from QuantConnect.Configuration import Config
from QuantWitness.Lean.China import ChinaFutureOrderProperties, ChinaFutureTimeInForce
from QuantWitness.Lean.Research import NativeMarketLifecycle
from QuantWitness.LeanInput import ResearchTradeBar
from System import DateTime,DateTimeKind,Decimal as ClrDecimal
from datetime import datetime,timezone
import json
from pathlib import Path

class NativeFixture(QCAlgorithm):
    def initialize(self):
        self.root=Path(Config.Get('results-destination-folder'))
        self.spec=json.loads((self.root/'strategy-input.json').read_text())
        self.market=json.loads((self.root/'input.json').read_text())
        self.scenario=self.spec['parameters']['scenario']
        self.daily=self.spec['frequency']=='1d'
        self.universe_settings.data_normalization_mode=DataNormalizationMode.RAW
        self.symbols={}
        for code,contract in self.market['contracts'].items():
            symbol=Symbol.CreateFuture(contract['product'],contract['market'],datetime.fromisoformat(contract['expiry_date']),code)
            self.symbols[code]=self.add_security(symbol,Resolution.DAILY if self.daily else Resolution.MINUTE,fill_forward=False,data_normalization_mode=DataNormalizationMode.RAW).symbol
        self.seen=[]

    def on_data(self,data):
        if self.is_warming_up or not data.bars.count:return
        if self.daily and not list(self.history[TradeBar](next(iter(self.symbols.values())),1,Resolution.DAILY)): return
        sessions=[row for row in self.market['sessions'] if datetime.fromisoformat(row['start_at']).replace(tzinfo=None)<=self.time<=datetime.fromisoformat(row['end_at']).replace(tzinfo=None)]
        if not sessions:return
        account=json.loads(NativeMarketLifecycle.AccountView())
        assert account['equity']['units']==account['settled_cash']['units']+account['unrealized_pnl']['units']
        assert account['available_funds']['units']==account['equity']['units']-account['margin']['units']-account['frozen_funds']['units']
        self.seen.append(dict(at=self.time.isoformat(),account=account))
        if self.scenario=='night':
            rule=NativeMarketLifecycle.CurrentFutureOrderRules(next(iter(self.symbols.values())))
            self.seen[-1].update(rule_trading_date=rule.TradingDate,upper_limit=int(rule.Rule.UpperLimit))
        legs=[]
        day=sessions[0]['trading_date']
        if self.daily:
            if self.scenario in ('risk','risk_rule_wait'):
                if len(self.seen)==1:legs=[('IF2401.CCFX',1,'open')]
            else:legs=[('IF2401.CCFX',1,'open'),('IF2401.CCFX',-1,'open')]
        elif day=='2024-01-03':
            if len(self.seen)==1:legs=[('IF2401.CCFX',1,'open')]
            if len(self.seen)==2 and self.scenario in ('hedge','night'):legs=[('IF2401.CCFX',-1,'open')]
        elif self.time.strftime('%H:%M:%S') in ('09:31:00','23:59:00') and self.scenario in ('hedge','night'):
            legs=[('IF2401.CCFX',1,'close_yesterday')]
        if self.scenario.startswith('blocked_risk') and len(self.seen)==1:
            legs=[(code,1,'open') for code in self.symbols]
        for code,side,effect in legs:
            quantity=450 if self.scenario.startswith('blocked_risk') else 900 if self.scenario in ('risk','risk_rule_wait') else 3 if self.scenario=='roll' else 1
            properties=ChinaFutureOrderProperties()
            properties.PositionEffect=effect
            properties.ClosePriority='yesterday_first'
            properties.Duration='DAY'
            properties.TimeInForce=ChinaFutureTimeInForce()
            properties.DailyOpenDecision=self.daily
            end=max(datetime.fromisoformat(row['end_at']) for row in sessions).astimezone(timezone.utc)
            properties.ValidUntilUtc=DateTime(end.year,end.month,end.day,end.hour,end.minute,end.second,DateTimeKind.Utc)
            if self.scenario=='protected_resize':
                quantity=min(1000,account['available_funds']['units']//15100)
                properties.ProtectionPrice=ClrDecimal.Parse('100')
                self.seen[-1].update(requested_quantity=1000,submitted_quantity=quantity)
            self.market_order(self.symbols[code],side*quantity,asynchronous=True,order_properties=properties)
        (self.root/'native-observations.json').write_text(json.dumps(dict(seen=self.seen)))

