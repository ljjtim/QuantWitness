"""原生期货算法显式开平，保留四桶账户。"""
from datetime import datetime
import json
from pathlib import Path

from AlgorithmImports import *
from QuantConnect.Configuration import Config
from QuantWitness.Lean.China import ChinaFutureOrderProperties, ChinaFutureTimeInForce
from System import DateTime, DateTimeKind


class FuturesAlgorithm(QCAlgorithm):
    def initialize(self):
        spec = json.loads((Path(Config.Get("results-destination-folder")) / "strategy-input.json").read_text(encoding="utf-8"))
        market_input = json.loads((Path(Config.Get("results-destination-folder")) / "input.json").read_text(encoding="utf-8"))
        self._sessions = market_input["sessions"]
        self._daily = spec["frequency"] == "1d"
        self._opened = None
        self._closed = False
        self._symbols = {}
        self.universe_settings.data_normalization_mode = DataNormalizationMode.RAW
        for code, contract in market_input["contracts"].items():
            expiry = datetime.fromisoformat(contract["expiry_date"])
            symbol = Symbol.CreateFuture(contract["product"], contract["market"], expiry, code)
            security = self.add_security(symbol, Resolution.DAILY if self._daily else Resolution.MINUTE,
                fill_forward=False, data_normalization_mode=DataNormalizationMode.RAW)
            self._symbols[code] = security.symbol
        self.object_store.save("futures/algorithm", "FuturesAlgorithm")

    def _submit(self, code, quantity, effect, session_end):
        properties = ChinaFutureOrderProperties()
        properties.PositionEffect = effect
        properties.ClosePriority = "yesterday_first"
        properties.Duration = "DAY"
        properties.TimeInForce = ChinaFutureTimeInForce()
        properties.ValidUntilUtc = DateTime(session_end.year, session_end.month, session_end.day,
            session_end.hour, session_end.minute, session_end.second, DateTimeKind.Utc)
        properties.DailyOpenDecision = self._daily
        self.market_order(self._symbols[code], quantity, asynchronous=True,
            tag=effect, order_properties=properties)

    def on_data(self, data):
        if self.is_warming_up or self._closed or not data.bars.count:
            return
        sessions = [row for row in self._sessions
                    if datetime.fromisoformat(row["start_at"]).replace(tzinfo=None) <= self.time
                    <= datetime.fromisoformat(row["end_at"]).replace(tzinfo=None)]
        if not sessions:
            return
        day = sessions[0]["trading_date"]
        # 委托截止时刻是UTC，交易日归属沿冻结会话。
        from datetime import timezone
        end = max(datetime.fromisoformat(row["end_at"]) for row in sessions).astimezone(timezone.utc).replace(tzinfo=None)
        if self._opened is None:
            for code, quantity in (("IF2401.CCFX", 1), ("IF2401.CCFX", -1), ("IF2403.CCFX", 1)):
                self._submit(code, quantity, "open", end)
            self._opened = day
        elif day != self._opened:
            for code, quantity in (("IF2401.CCFX", -1), ("IF2401.CCFX", 1), ("IF2403.CCFX", -1)):
                self._submit(code, quantity, "close_yesterday", end)
            self._closed = True
        self.plot("期货账户", "权益", self.portfolio.total_portfolio_value)
