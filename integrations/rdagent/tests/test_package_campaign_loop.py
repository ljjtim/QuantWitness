"""真实RD调度、两份正式Result与恢复；只读Parquet，不打开数据库文件。"""
import asyncio
import importlib.util
import json
from pathlib import Path
import sys

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="真实RD调度在Linux验收")


class ReadOnlySortConnection:
    """允许独立验证的内存排序；禁止数据库文件和写表语句。"""
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.close()

    def execute(self, sql):
        assert sql.startswith("SET max_temp_directory_size = "), "不得写入数据库表"
        self.connection.execute(sql)
        return self

    def from_parquet(self, *args, **kwargs):
        return self.connection.from_parquet(*args, **kwargs)

    def from_arrow(self, *args, **kwargs):
        return self.connection.from_arrow(*args, **kwargs)


def readonly_sort_factory(connect, calls):
    def open_sort(database, **kwargs):
        assert database == ":memory:", "不得打开数据库文件"
        calls.append("memory_sort")
        return ReadOnlySortConnection(connect(database, **kwargs))
    return open_sort


@pytest.mark.parametrize("example_name,baseline_id,next_id,expected_rows", [
    ("prepare.py", "window_3", "window_2", 6),
    ("prepare_qlib.py", "ridge_0_1", "ridge_1_0", 1),
], ids=["formula", "qlib"])
def test_real_package_loop_and_recovery(tmp_path, monkeypatch, example_name, baseline_id, next_id, expected_rows):
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    import duckdb
    import sqlite3
    from rdagent.oai.backend.base import APIBackend
    def forbidden(*args, **kwargs):
        raise AssertionError("合成正式研究不得打开数据库文件或调用语言模型")
    sorts = []
    connection = readonly_sort_factory(duckdb.connect, sorts) if example_name == "prepare_qlib.py" else forbidden
    monkeypatch.setattr(duckdb, "connect", connection)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(APIBackend, "__init__", forbidden)
    example = Path(__file__).resolve().parents[1] / "examples/package_campaign" / example_name
    spec = importlib.util.spec_from_file_location("real_package_example", example)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path / "research"
    module.build(root)
    payload = json.loads((root / "request.json").read_text(encoding="utf-8"))
    from quantwitness_rdagent.package_campaign import PackageCampaign
    from quantwitness_rdagent.campaign_loop import ResearchCampaignLoop, run_campaign
    from quantwitness_rdagent import package_execution
    from rdagent.core.conf import RD_AGENT_SETTINGS
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger
    session = PackageCampaign(payload)
    monkeypatch.setattr(RD_AGENT_SETTINGS, "artifact_signing_key_path", session.root / "rd-signing.key")
    monkeypatch.setattr(RD_AGENT_SETTINGS, "workspace_path", session.root / "rd-workspace")
    monkeypatch.setattr(LOG_SETTINGS, "trace_path", str(session.root / "rd-logs"))
    rdagent_logger.set_storages_path(session.root / "rd-logs")
    loop = ResearchCampaignLoop(session)
    asyncio.run(loop.run(step_n=2, loop_n=2))
    baseline = session.root / "rounds/0000/evaluation.json"
    assert baseline.exists()
    completed = json.loads(baseline.read_text(encoding="utf-8"))
    assert completed["status"] == "evaluated", completed
    before = baseline.read_bytes(), baseline.stat().st_mtime_ns
    original = package_execution.execute_package
    calls = []
    def resume_execute(**kwargs):
        assert kwargs["allocation_label"] != baseline_id, "恢复不可重评已完成候选"
        calls.append(kwargs["allocation_label"])
        return original(**kwargs)
    monkeypatch.setattr(package_execution, "execute_package", resume_execute)
    outcome = run_campaign(payload, resume=True)
    assert outcome["status"] == "completed", outcome
    assert calls == [next_id]
    assert before == (baseline.read_bytes(), baseline.stat().st_mtime_ns)
    assert len({row["metrics"]["result_id"] for row in outcome["rounds"]}) == 2
    assert all(row["metrics"]["verification_status"] == "pass" and row["metrics"]["rows"] == expected_rows for row in outcome["rounds"])
    assert len({row["metrics"]["value"] for row in outcome["rounds"]}) == 2
    assert outcome["new_formal_result"] is True and outcome["holdout_evaluated"] is False
    files = {str(p.relative_to(session.root)): (p.stat().st_mtime_ns, p.stat().st_size) for p in session.root.rglob("*") if p.is_file()}
    assert run_campaign(payload, resume=True) == outcome
    assert files == {str(p.relative_to(session.root)): (p.stat().st_mtime_ns, p.stat().st_size) for p in session.root.rglob("*") if p.is_file()}
    from quantwitness_rdagent.contracts import write_json
    write_json(root / "acceptance.json", {"status": "pass", "outcome": outcome, "unchanged_files": len(files),
        "two_formal_results": True, "baseline_not_reevaluated": True, "database_file_connections": 0, "database_writes": 0, "readonly_memory_sorts": len(sorts), "model_calls": 0})
