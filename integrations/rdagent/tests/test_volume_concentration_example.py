"""合成教学公式的数值、窗口、可见时点及公开准备入口。"""
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pyarrow as pa
import pytest

ROOT = Path(__file__).resolve().parents[1] / "examples/volume_concentration"
RP_ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(RP_ROOT / "src"), str(RP_ROOT / "integrations/rdagent/src")]


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


prepare = load("volume_example_prepare", ROOT / "prepare.py")
parent = ModuleType("volume_example_candidate")
parent.__path__ = [str(ROOT / "candidate")]
sys.modules[parent.__name__] = parent
compute = load("volume_example_candidate.compute", ROOT / "candidate/compute.py")
adapter = load("volume_example_candidate.adapter", ROOT / "candidate/adapter.py")


def test_daily_and_three_session_contract():
    assert compute.daily_value({i: 10.0 for i in range(240)}, 240)["value"] == pytest.approx(1 / 240)
    assert compute.daily_value({}, 240) == {"value": None, "status": "missing_bars"}
    assert compute.daily_value({i: 0.0 for i in range(240)}, 240)["status"] == "zero_volume"
    for invalid in (-1.0, float("nan"), float("inf"), None):
        values = {i: 10.0 for i in range(240)}
        values[1] = invalid
        assert compute.daily_value(values, 240)["status"] == "invalid_volume"
    history = [{"value": 1.0, "status": "computed"}, {"value": 2.0, "status": "computed"}, {"value": 3.0, "status": "computed"}]
    assert compute.rolling_value(history[:2])["status"] == "warmup"
    assert compute.rolling_value(history) == {"value": 2.0, "status": "computed"}
    history[1] = {"value": None, "status": "missing_bars"}
    assert compute.rolling_value(history)["status"] == "invalid_window"
    assert compute.rolling_value([{"value": 999, "status": "computed"}, *history])["status"] == "invalid_window"


class Bars:
    port = "bars"
    def __init__(self, calendar):
        self.complete = False
        self.rows = [{"code": entity, "dt": stamp, "volume": float(1 + (index + entity_index * 3) % 17)}
                     for entity_index, entity in enumerate(prepare.ENTITIES)
                     for day in calendar for index, stamp in enumerate(day["expected_bars"])]
    def iter_batches(self, **kwargs):
        return pa.Table.from_pylist(self.rows).to_batches()
    def assert_complete(self):
        self.complete = True


class Output:
    def __init__(self):
        self.tables = {}
    def write_batches(self, *, port, batches, **kwargs):
        self.tables[port] = pa.Table.from_batches(batches).to_pylist()
        return port


def test_adapter_preserves_formula_window_and_completed_bar_time():
    days = prepare.calendar_rows()
    context = SimpleNamespace(parameters={"calendar_json": json.dumps(days), "entities_json": json.dumps(prepare.ENTITIES), "decision_time": prepare.CLOCK})
    bars, output = Bars(days), Output()
    adapter.run(context, [bars], output)
    assert bars.complete
    for entity in prepare.ENTITIES:
        history = []
        for day in days:
            rows = [row for row in bars.rows if row["code"] == entity and row["dt"] in day["expected_bars"]]
            volumes = [row["volume"] for row in rows]
            expected = sum(value ** 2 for value in volumes) / sum(volumes) ** 2
            history.append(expected)
            row = next(item for item in output.tables["daily"] if item["entity"] == entity and item["session"] == day["session"])
            assert row["value"] == pytest.approx(expected)
            assert row["available_at"] == day["expected_bars"][-1]
            if len(history) >= 3:
                assert row["rolling_value"] == pytest.approx(sum(history[-3:]) / 3)
    context.parameters["decision_time"] = "2025-01-10T14:59:00+08:00"
    with pytest.raises(ValueError, match="已完成"):
        adapter.preflight(context)


def test_prepare_is_database_free_and_pending_review(tmp_path, monkeypatch):
    import duckdb
    import sqlite3
    def forbidden(*args, **kwargs):
        raise AssertionError("教学准备入口禁止连接数据库")
    monkeypatch.setattr(duckdb, "connect", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    root = tmp_path / "prepared"
    receipt = prepare.build(root, RP_ROOT, sys.executable, "/mnt/i/source", "/mnt/i/output")
    assert receipt["status"] == "prepared_pending_review"
    assert receipt["raw_rows"] == 2400
    assert receipt["entity_sessions"] == 10
    import yaml
    from research_pipeline.platform import load_minute_capability_manifest
    catalog = yaml.safe_load((root / "synthetic-catalog.yaml").read_text(encoding="utf-8"))
    identity = load_minute_capability_manifest().downstream_identity("catalog.minute.contracts")
    scope = next(policy for policy in catalog["policies"]
                 if policy["policy_id"] == "scope.minute.catalog.v4")
    assert scope["rules"] == identity
    assert catalog["datasets"][0]["minute_source_semantics"]["scope_binding_hash"] == identity["binding_hash"]
    assert not (root / "confirmation.json").exists()
    assert not (root / "request.json").exists()
    from quantwitness_rdagent.contracts import FrozenRequest
    request = FrozenRequest.load(root / "request-template.json")
    assert request.payload["budget"]["live_llm_calls"] == 0
    from quantwitness_rdagent.formula_spec import review_spec
    review = review_spec(root / "draft.json", root / "materials.json")
    assert "待人工确认" in review
    assert len(request.payload["runtime_binding"]["fixed_responses"]) == 2
    from quantwitness_rdagent.generation import validate_generated_source
    for response in request.payload["runtime_binding"]["fixed_responses"]:
        validate_generated_source(response)
    with pytest.raises(FileExistsError):
        prepare.build(root, RP_ROOT, sys.executable, "/mnt/i/source", "/mnt/i/output")
