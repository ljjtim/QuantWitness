"""原生LEAN结果封存与真实请求统计。"""
import json

from research_pipeline.platform.lean_contract import LEAN_SUPPORT_PATHS
from research_pipeline.platform.lean_native_support import copy_lean_support
from research_pipeline.platform.lean_result_projection import lean_trade_totals


def test_native_outputs_charts_object_store_and_algorithm_are_preserved(tmp_path):
    root, staging = tmp_path / "run", tmp_path / "result"
    root.mkdir()
    staging.mkdir()
    for relative in LEAN_SUPPORT_PATHS:
        (root / relative.removeprefix("lean/")).write_text("{}", encoding="utf-8")
    for relative, content in {
        "strategy-source/algo.py": "class Strategy: pass",
        "storage/model/data.bin": "model",
        "Charts/Equity.json": '{"chart":"Equity"}',
        "run-result.json": '{"orders":{}}',
        "config.json": '{"algorithm-type-name":"Strategy"}',
        "orders.jsonl": "order",
        "input.json": "input",
        "native-data/events/run-0.ipc": "临时归并索引",
    }.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    manifest = copy_lean_support(root, staging)
    assert (staging / "lean/strategy-source/algo.py").read_text() == "class Strategy: pass"
    assert (staging / "lean/storage/model/data.bin").read_text() == "model"
    assert json.loads((staging / "lean/native-output/Charts/Equity.json").read_text()) == {"chart": "Equity"}
    assert (staging / "lean/native-output/run-result.json").is_file()
    assert (staging / "lean/native-output/config.json").is_file()
    assert not (staging / "lean/native-output/orders.jsonl").exists()
    assert not (staging / "lean/native-output/input.json").exists()
    assert not (staging / "lean/native-output/native-data").exists()
    assert "native-data" not in manifest["native_output"]
    assert manifest["object_store"] == "lean/storage"


def test_native_unfilled_and_rejected_orders_count_without_synthetic_fills():
    orders = [
        {"order_id": 1, "quantity": 0, "status": "Submitted"},
        {"order_id": 1, "quantity": 0, "status": "Canceled"},
        {"order_id": 2, "quantity": 0, "status": "Invalid"},
        {"order_id": 3, "quantity": 10, "status": "Filled", "at": "2024-01-08T10:00:00+08:00", "price": 10, "fee": 5},
    ]
    totals = lean_trade_totals(["2024-01-08"], iter(orders))
    assert totals["fee_total"] == 5
    assert totals["order_count"] == 3
    assert totals["fill_count"] == 1
    assert totals["filled_order_count"] == 1
    assert totals["fee_total"] == 5
