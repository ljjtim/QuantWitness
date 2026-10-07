"""知识筛选与证据重验的局部合同；正式工件另做跨会话验收。"""
import copy
import json
from types import SimpleNamespace

import pytest

from quantwitness_rdagent import research_knowledge as knowledge
from quantwitness_rdagent.contracts import write_json


def test_only_empty_holdout_structure_is_allowed():
    knowledge._check_result_tables([SimpleNamespace(table_id="holdout_index", row_counts={"index.parquet": 0})])
    for name, rows in [("holdout_index", 1), ("holdout_predictions", 0), ("test_predictions", 1)]:
        with pytest.raises(ValueError, match="最终评价"):
            knowledge._check_result_tables([SimpleNamespace(table_id=name, row_counts={"table.parquet": rows})])


def facts():
    development = {"start": "2020-01-01", "end": "2020-01-10", "as_of": "2020-01-11T00:00:00+08:00"}
    metric = dict(zip(knowledge._METRIC_FIELDS, ("mse", "squared_ratio", "daily", "lower_is_better", "none", "validation mse")))
    design = {"mode": "development", "calendar_id": "calendar", "snapshot_scope": "synthetic", "price_basis": "raw",
        "entities": ["A"], "research_sessions": ["2020-01-02"]}
    scope = knowledge._scope(design, development)
    record = {"record_id": "session/a/result", "usable_for_proposal": True, "kind": "negative", "status": "no_improvement",
        "objective": {"direction": "minimize"}, "metric_description": metric,
        "development_scope": development, "as_of": development["as_of"], "design_scope": scope}
    return dict(development=development, objective=record["objective"], metric_description=metric, design=design), record


@pytest.mark.parametrize("fault", ["future", "date", "entity", "metric", "frequency", "market", "holdout"])
def test_out_of_scope_records_are_excluded_before_loading_sources(tmp_path, monkeypatch, fault):
    args, record = facts()
    row = copy.deepcopy(record)
    if fault == "future": row["as_of"] = "2021-01-01T00:00:00+08:00"
    if fault == "date": row["development_scope"]["start"] = "2019-01-01"
    if fault == "entity": row["design_scope"]["entities"] = ["OTHER"]
    if fault == "metric": row["objective"]["direction"] = "maximize"
    if fault == "frequency": row["metric_description"]["frequency"] = "minute"
    if fault == "market": row["design_scope"]["snapshot_scope"] = "other_market"
    if fault == "holdout": row["usable_for_proposal"] = False
    path = tmp_path / "index.json"
    write_json(path, {"contract_version": knowledge.VERSION, "session_root": "unopened", "records": [row]})
    def forbidden(*args): raise AssertionError("范围外的来源不得加载")
    monkeypatch.setattr(knowledge, "_build_index", forbidden)
    assert knowledge.query_index(path, **args) == []


def test_selected_records_are_reverified_and_negative_results_preserved(tmp_path, monkeypatch):
    args, row = facts()
    index = {"contract_version": knowledge.VERSION, "session_root": "source", "records": [row]}
    path = tmp_path / "index.json"
    write_json(path, index)
    monkeypatch.setattr(knowledge, "_build_index", lambda root: copy.deepcopy(index))
    assert knowledge.query_index(path, **args)[0]["kind"] == "negative"
    changed = copy.deepcopy(index)
    changed["records"][0]["kind"] = "observation"
    write_json(path, changed)
    with pytest.raises(ValueError, match="来源不一致"):
        knowledge.query_index(path, **args)


def test_export_is_immutable_and_does_not_modify_source(tmp_path, monkeypatch):
    source = tmp_path / "session"
    source.mkdir()
    value = {"contract_version": knowledge.VERSION, "records": []}
    monkeypatch.setattr(knowledge, "_build_index", lambda root: copy.deepcopy(value))
    output = tmp_path / "index.json"
    knowledge.export_session(source, output)
    stamp = output.stat().st_mtime_ns
    knowledge.export_session(source, output)
    assert output.stat().st_mtime_ns == stamp
    value["records"].append({"record_id": "new"})
    with pytest.raises(ValueError, match="冻结"):
        knowledge.export_session(source, output)
    with pytest.raises(ValueError, match="之外"):
        knowledge.export_session(source, source / "request.json")


def test_cumulative_index_keeps_earlier_negative_records_and_rechecks_source(tmp_path, monkeypatch):
    prior = {"contract_version": knowledge.VERSION, "session_root": "prior", "records": [
        {"record_id": "prior/negative", "kind": "negative"},
        {"record_id": "prior/failed", "status": "technical_failure"}]}
    path = tmp_path / "prior.json"
    write_json(path, prior)
    write_json(tmp_path / "knowledge-input.json", {"index": prior, "records": prior["records"][:1]})
    monkeypatch.setattr(knowledge, "_build_index", lambda root: copy.deepcopy(prior))
    inherited = knowledge._inherited_records(tmp_path, {"knowledge": {"index": str(path)}})
    current = [{"record_id": "current/result", "kind": "observation"}]
    assert knowledge._merge_records(inherited, current) == prior["records"] + current
    assert knowledge._merge_records(inherited, inherited) == inherited
    with pytest.raises(ValueError, match="ID冲突"):
        knowledge._merge_records(inherited, [{"record_id": "prior/negative", "kind": "observation"}])
    changed = copy.deepcopy(prior)
    changed["records"].pop()
    monkeypatch.setattr(knowledge, "_build_index", lambda root: changed)
    with pytest.raises(ValueError, match="冻结来源"):
        knowledge._inherited_records(tmp_path, {"knowledge": {"index": str(path)}})
