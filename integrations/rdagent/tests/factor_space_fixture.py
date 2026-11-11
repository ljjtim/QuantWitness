import importlib.util
from pathlib import Path


def price_space():
    path = Path(__file__).resolve().parents[1] / "examples/factor_research/prepare.py"
    spec = importlib.util.spec_from_file_location("test_price_space_example", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.factor_space()
