"""验证原生 Universe 退出、事件可见性和按目标调仓。"""

from AlgorithmImports import *
from QuantConnect.Configuration import Config
from QuantWitness.LeanInput import ResearchData
from datetime import datetime
import json
from pathlib import Path


class NativeFixture(QCAlgorithm):
    def initialize(self):
        self.universe_settings.resolution = Resolution.MINUTE
        self.universe_settings.fill_forward = False
        self.universe_settings.data_normalization_mode = DataNormalizationMode.RAW
        self.symbol = Symbol.create('000002', SecurityType.EQUITY, 'jqchina')
        self.announcements = self.add_data(ResearchData, 'announcements:000002.XSHE', Resolution.DAILY).symbol
        self.add_data(ResearchData, 'publication_probe:000002.XSHE', Resolution.DAILY)
        self.add_data(ResearchData, 'portfolio_targets:000002.XSHE', Resolution.DAILY)
        self.add_universe(ResearchData, 'selection', Resolution.DAILY, self.select)
        self.schedule.on(self.date_rules.every_day(self.symbol), self.time_rules.at(9, 33), self.verify)
        self.observations = dict(selections=[], before_publication=[], after_publication=[], held_after_exit=[], targets=[])
        self.pending_weight = None

    def select(self, rows):
        selected = any(row.Fields['selected'].GetBoolean() for row in rows)
        self.observations['selections'].append([self.time.isoformat(), int(selected)])
        return [self.symbol] if selected else []

    def verify(self):
        if self.time.day == 11:
            published = self._announcement_history()
            self.observations['after_publication'].append(len(published))
            assert all(row.AvailableAt <= self.time for row in published)

    def _announcement_history(self):
        return list(self.history[ResearchData](self.announcements, datetime(2021, 10, 8), self.time,
                                               Resolution.DAILY, fill_forward=False))

    def on_data(self, data):
        if self.is_warming_up:
            return
        for row in data.get(ResearchData).values():
            if row.BindingId == 'publication_probe':
                assert self.time == datetime(2021, 10, 11)
                self.observations['before_publication'].append(len(self._announcement_history()))
            if row.BindingId != 'portfolio_targets':
                continue
            assert row.AvailableAt == self.time
            batch = row.Fields['batch_id'].GetString()
            assert batch not in self.observations['targets']
            self.pending_weight = row.Fields['weight_ppb'].GetInt64() / 1_000_000_000
            self.observations['targets'].append(batch)
        if self.time.hour == 9 and self.time.minute == 31 and self.pending_weight is not None:
            if self.pending_weight == 0:
                assert self.observations['selections'][-1][1] == 0
                self.observations['held_after_exit'].append(self.portfolio[self.symbol].invested)
            target = PortfolioTarget(self.symbol, self.pending_weight)
            self.set_holdings([target], liquidate_existing_holdings=True)
            self.pending_weight = None

    def on_end_of_algorithm(self):
        root = Path(Config.Get('results-destination-folder'))
        (root / 'native-observations.json').write_text(json.dumps(self.observations), encoding='utf-8')
