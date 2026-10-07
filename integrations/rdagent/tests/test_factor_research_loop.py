"""合成输入上的正式三轮因子研究与中断恢复。"""
import importlib.util
import json
from pathlib import Path
import sys

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="完整RD研究在Linux验收")


def test_three_formal_rounds_and_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    import duckdb
    import sqlite3
    from test_package_campaign_loop import readonly_sort_factory
    from quantwitness_rdagent import model_client
    sorts = []
    monkeypatch.setattr(duckdb, "connect", readonly_sort_factory(duckdb.connect, sorts))
    def forbidden(*args, **kwargs):
        raise AssertionError("固定文本研究不得写SQLite或调用付费模型")
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(model_client, "request_text", forbidden)
    example = Path(__file__).resolve().parents[1] / "examples/factor_research/prepare.py"
    spec = importlib.util.spec_from_file_location("factor_example", example)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path / "research"
    module.build(root)
    payload = json.loads((root / "request.json").read_text(encoding="utf-8"))
    assert len(payload["package_template"]["candidates"]) == 1
    from quantwitness_rdagent.factor_loop import run_factor_research
    from quantwitness_rdagent import package_execution
    paused = run_factor_research(payload, step_n=4)
    assert paused["status"] == "incomplete" and paused["completed_rounds"] == 1, paused
    baseline = root / "session/rounds/0000/record.json"
    before = baseline.read_bytes(), baseline.stat().st_mtime_ns
    original = package_execution.execute_package
    calls = []
    def execute(**kwargs):
        assert kwargs["allocation_label"] != "factor_0000", "恢复不得重评基线"
        calls.append(kwargs["allocation_label"])
        return original(**kwargs)
    monkeypatch.setattr(package_execution, "execute_package", execute)
    result = run_factor_research(payload, resume=True)
    assert result["status"] == "completed", result
    assert calls == ["factor_0001", "factor_0002"]
    assert before == (baseline.read_bytes(), baseline.stat().st_mtime_ns)
    assert result["generated_candidates"] == 2
    assert result["paid_model_calls"] == 0 and result["holdout_evaluated"] is False
    assert len({row["metrics"]["result_id"] for row in result["rounds"]}) == 3
    assert all(row["metrics"]["verification_status"] == "pass" and row["reflection"] for row in result["rounds"])
    assert len({row["expression"] for row in result["rounds"]}) == 3
    receipts = [json.loads(p.read_text(encoding="utf-8")) for p in sorted((root / "session/calls").glob("*.json"))]
    assert len(receipts) == 7
    assert "检验较短动量" in json.dumps(receipts[1]["request"], ensure_ascii=False)
    assert "检验相对均价偏离" in json.dumps(receipts[4]["request"], ensure_ascii=False)
    snapshot = {str(p): (p.stat().st_mtime_ns, p.stat().st_size) for p in (root / "session").rglob("*") if p.is_file()}
    assert run_factor_research(payload, resume=True) == result
    assert snapshot == {name: (Path(name).stat().st_mtime_ns, Path(name).stat().st_size) for name in snapshot}
    (root / "acceptance.json").write_text(json.dumps({"status": "pass", "outcome": result,
        "resume_preserved_baseline": True, "completed_resume_unchanged": True,
        "read_only_memory_sorts": len(sorts), "database_file_connections": 0, "paid_model_calls": 0}, ensure_ascii=False, indent=2), encoding="utf-8")
