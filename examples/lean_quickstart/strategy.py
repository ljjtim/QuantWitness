"""原生算法在首个可见bar买入，下一会话卖出。"""
import json
from pathlib import Path

from AlgorithmImports import *
from QuantConnect.Configuration import Config


class QuickstartAlgorithm(QCAlgorithm):
    def initialize(self):
        root = Path(Config.Get("results-destination-folder"))
        spec = json.loads((root / "strategy-input.json").read_text(encoding="utf-8"))
        self._frequency = spec["frequency"]
        self._bought = False
        self._sold = False
        self._buy_day = None
        self._symbol = self.add_equity("599999", Resolution.DAILY if self._frequency == "1d" else Resolution.MINUTE,
            "jqchina", fill_forward=False, leverage=1, extended_market_hours=False,
            data_normalization_mode=DataNormalizationMode.RAW).symbol
        self.set_warm_up(1, Resolution.DAILY if self._frequency == "1d" else Resolution.MINUTE)
        self.sma(self._symbol, 1, Resolution.DAILY if self._frequency == "1d" else Resolution.MINUTE)
        self.object_store.save("quickstart/algorithm", "QuickstartAlgorithm")

    def on_data(self, data):
        if self.is_warming_up or self._sold or not data.bars.contains_key(self._symbol):
            return
        if self.transactions.get_open_orders(self._symbol):
            return
        self.plot("原生账户", "总权益", self.portfolio.total_portfolio_value)
        if not self._bought:
            self._record_history()
            # 日频已可见的前日日线通过宿主默认属性参与本次开盘决策。
            self.market_order(self._symbol, 100, asynchronous=True, tag="首个可见行情买入")
            self._bought = True
            self._buy_day = self.time.date()
        elif self.time.date() > self._buy_day and self.portfolio[self._symbol].quantity >= 100:
            self._record_history()
            self.market_order(self._symbol, -100, asynchronous=True, tag="下一交易日卖出")
            self._sold = True

    def _record_history(self):
        resolution = Resolution.DAILY if self._frequency == "1d" else Resolution.MINUTE
        history = self.history(self._symbol, 1, resolution)
        closes = [] if history.empty else history["close"].tolist()
        self.object_store.save(
            f"quickstart/history-{self.time.date().isoformat()}.json",
            json.dumps({"at": self.time.isoformat(), "close": closes}),
        )
