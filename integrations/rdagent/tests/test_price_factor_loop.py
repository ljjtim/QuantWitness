"""两类新增价格特征的正式研究结果、独立验证与恢复。"""
import importlib.util
import json
from pathlib import Path
import sys

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="正式RD研究在Linux验收")


def test_price_families_formal_results_and_resume(tmp_path, monkeypatch):
    import duckdb
    import sqlite3
    from test_package_campaign_loop import readonly_sort_factory
    from quantwitness_rdagent import model_client, package_execution
    from quantwitness_rdagent.contracts import write_json
    def forbidden(*args, **kwargs):
        raise AssertionError("本验收不得打开数据库文件、付费或重复已完成实验")
    sorts = []
    monkeypatch.setattr(duckdb, "connect", readonly_sort_factory(duckdb.connect, sorts))
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(model_client, "request_text", forbidden)
    example = Path(__file__).resolve().parents[1] / "examples/factor_research/prepare.py"
    spec = importlib.util.spec_from_file_location("price_example", example)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path / "research"
    module.build(root, campaign_id="synthetic_price_families")
    payload = json.loads((root / "request.json").read_text(encoding="utf-8"))
    volatility = "Std($close, 3) / Mean($close, 3)"
    position = "($close - Min($close, 3)) / (Max($close, 3) - Min($close, 3))"
    confirmation = json.loads((root / "confirmed-definition.json").read_text(encoding="utf-8"))
    confirmation.update(formula=volatility, title="收盘价相对波动的教学定义",
        text="使用决策前一会话及更早三期收盘价，样本标准差除以均价。仅作合成研究基线：" + volatility)
    write_json(root / "confirmed-definition.json", confirmation)
    (root / "confirmed-definition.md").write_text(confirmation["text"] + "\n", encoding="utf-8")
    payload["baseline"] = {"expression": volatility, "hypothesis": "研究收盘价格的相对离散程度", "reason": "按确认的教学定义计算"}
    payload["budget"].update(rounds=2, model_calls=4, output_tokens=8192)
    def reflection(next_idea):
        return {"Observations": "实验结论以已验证开发指标为准", "Feedback for Hypothesis": "保留结果，比较不同价格信息",
            "New Hypothesis": next_idea, "Reasoning": "使用同一冻结范围", "Replace Best Result": False}
    responses = [reflection("检验价格在收盘区间的位置"),
        {"hypothesis": "相对区间位置可以提供不同的预测信息", "reason": "与离散程度对照"},
        {"range_position": {"description": "三期收盘区间位置", "formulation": position,
            "variables": {"close": "决策前已完成会话的收盘价"}, "expression": position}},
        reflection("两类价格信息均有正式验证记录")]
    paths = []
    for index, response in enumerate(responses):
        path = root / "price-responses" / f"{index:02d}.json"
        write_json(path, response)
        paths.append(str(path))
    payload["proposer"]["responses"] = paths
    write_json(root / "request.json", payload)
    from quantwitness_rdagent.factor_loop import run_factor_research
    outcome = run_factor_research(payload)
    assert outcome["status"] == "completed", outcome
    assert [row["expression"] for row in outcome["rounds"]] == [volatility, position]
    assert all(row["metrics"]["verification_status"] == "pass" for row in outcome["rounds"])
    assert len({row["metrics"]["result_id"] for row in outcome["rounds"]}) == 2
    assert len(list((root / "session/calls").glob("*.json"))) == 4
    assert outcome["paid_model_calls"] == 0 and outcome["holdout_evaluated"] is False
    before = {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in (root / "session").rglob("*") if p.is_file()}
    monkeypatch.setattr(package_execution, "execute_package", forbidden)
    assert run_factor_research(payload, resume=True) == outcome
    assert before == {p: (Path(p).stat().st_size, Path(p).stat().st_mtime_ns) for p in before}
    write_json(root / "acceptance.json", {"status": "pass", "outcome": outcome,
        "database_file_connections": 0, "paid_model_calls": 0,
        "completed_resume_unchanged": True, "readonly_memory_sorts": len(sorts)})
