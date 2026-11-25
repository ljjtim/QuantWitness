"""原生目标批次的确定性组合案例，不启动引擎。"""
from datetime import datetime
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace


def load_algorithm(monkeypatch):
    imports = ModuleType("AlgorithmImports")
    imports.QCAlgorithm = object
    imports.PortfolioTarget = lambda symbol, weight: (symbol, weight)
    configuration = ModuleType("QuantConnect.Configuration")
    configuration.Config = object
    lean_input = ModuleType("QuantWitness.LeanInput")
    lean_input.ResearchData = object
    for name, module in (("AlgorithmImports", imports), ("QuantConnect.Configuration", configuration),
                         ("QuantWitness.LeanInput", lean_input)):
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("native_qlib_algorithm", Path(__file__).parent/"lean_algorithm/algorithm.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.QlibPortfolioAlgorithm()


def row(code, weight, batch="2024-01-02T15:00:00+08:00"):
    return SimpleNamespace(BindingId="portfolio_targets", AvailableAt=datetime(2024, 1, 3, 9, 30),
        symbol=SimpleNamespace(value=code), Fields={
            "batch_id": SimpleNamespace(GetString=lambda: batch),
            "weight_ppb": SimpleNamespace(GetInt64=lambda: weight)})


def test_native_batch_waits_for_all_symbols_and_keeps_zero_exits(monkeypatch):
    algorithm = load_algorithm(monkeypatch)
    algorithm._codes = ("A.XSHG", "B.XSHG")
    algorithm._symbols = {"A.XSHG": "A", "B.XSHG": "B"}
    algorithm._pending = {}
    algorithm._completed = None
    algorithm.is_warming_up = False
    algorithm.time = datetime(2024, 1, 3, 9, 30)
    algorithm.transactions = SimpleNamespace(get_open_orders=lambda: [])
    submitted = []
    algorithm.set_holdings = lambda targets, **kwargs: submitted.append((targets, kwargs))
    algorithm.object_store = SimpleNamespace(save=lambda *args: None)
    algorithm.portfolio = SimpleNamespace(total_portfolio_value=1000)
    algorithm.plot = lambda *args: None
    algorithm.on_data(SimpleNamespace(get=lambda _: {"A": row("A.XSHG", 500_000_000)}))
    assert submitted == []
    algorithm.on_data(SimpleNamespace(get=lambda _: {"B": row("B.XSHG", 0)}))
    assert submitted == [([("A", 0.5), ("B", 0.0)], {"liquidate_existing_holdings": True})]
    algorithm.on_data(SimpleNamespace(get=lambda _: {"A": row("A.XSHG", 500_000_000), "B": row("B.XSHG", 0)}))
    assert len(submitted) == 1
