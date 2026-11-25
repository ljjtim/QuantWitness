"""用原生LEAN目标组合消费正式Qlib开发目标。"""
import json
from pathlib import Path

from AlgorithmImports import *
from QuantConnect.Configuration import Config
from QuantWitness.LeanInput import ResearchData


class QlibPortfolioAlgorithm(QCAlgorithm):
    def initialize(self):
        spec = json.loads((Path(Config.Get("results-destination-folder")) / "strategy-input.json").read_text(encoding="utf-8"))
        self._codes = tuple(sorted(spec["instruments"]))
        self._symbols = {}
        self._pending = {}
        self._completed = None
        for code in self._codes:
            self._symbols[code] = self.add_equity(code.split(".")[0], Resolution.DAILY, "jqchina",
                fill_forward=False, leverage=1, extended_market_hours=False,
                data_normalization_mode=DataNormalizationMode.RAW).symbol
            self.add_data(ResearchData, code, Resolution.DAILY, fill_forward=False)

    def on_data(self, data):
        if self.is_warming_up:
            return
        for row in data.get(ResearchData).values():
            if row.BindingId != "portfolio_targets":
                continue
            batch = row.Fields["batch_id"].GetString()
            if batch == self._completed:
                continue
            if row.AvailableAt > self.time:
                raise ValueError("组合目标尚不可见")
            code = row.symbol.value
            if code not in self._symbols:
                raise ValueError("组合目标证券不在冻结范围")
            weight = row.Fields["weight_ppb"].GetInt64()
            if not 0 <= weight <= 1_000_000_000:
                raise ValueError("组合目标权重超出范围")
            self._pending.setdefault(batch, {})[code] = weight
        for batch in sorted(self._pending):
            weights = self._pending[batch]
            if set(weights) != set(self._codes):
                continue
            if sum(weights.values()) > 1_000_000_000:
                raise ValueError("组合目标超过全部权益")
            if self.transactions.get_open_orders():
                return
            targets = [PortfolioTarget(self._symbols[code], weights[code] / 1_000_000_000)
                       for code in self._codes]
            self.set_holdings(targets, liquidate_existing_holdings=True)
            self._completed = batch
            self.object_store.save("portfolio/completed_target", batch)
            self.plot("组合权益", "权益", self.portfolio.total_portfolio_value)
            del self._pending[batch]
            return
