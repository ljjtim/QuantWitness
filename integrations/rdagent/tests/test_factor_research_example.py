"""教学公式的前日可见性、独立手算和正式基包声明。"""
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples/qlib_portfolio"
PRICE_EXPRESSIONS = [
    "$close / Ref($close, 5) - 1", "$close / Mean($close, 3) - 1",
    "Std($close, 3) / Mean($close, 3)",
    "($close - Min($close, 3)) / (Max($close, 3) - Min($close, 3))",
]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def example_modules(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE / "extension"))
    return load_module("factor_example_operator", EXAMPLE / "extension/operator.py")


@pytest.mark.parametrize("expression", PRICE_EXPRESSIONS)
def test_expression_uses_only_previous_close(monkeypatch, expression):
    pytest.importorskip("qlib")
    operator = example_modules(monkeypatch)
    sessions = [f"2025-01-{i:02d}" for i in range(1, 16)]
    prices = {("ETF", session): float(100+i) for i, session in enumerate(sessions)}
    day = sessions[11]
    keys = [["ETF", day, 5, "historical_return"], ["ETF", day, 10, "historical_return"]]
    columns = ["entity_id", "observation_session", "window_sessions", "feature_id"]
    design = {"calendar_sessions": sessions, "feature_expressions": {"historical_return": expression}}
    expected = dict(zip(PRICE_EXPRESSIONS, (110/105-1, 110/109-1, 1/109, 1)))[expression]
    before = operator._expression_values(design, prices, keys, columns)
    assert before[("ETF", day)] == pytest.approx(expected)
    for session in sessions[11:]:
        prices[("ETF", session)] = 1e8
    assert operator._expression_values(design, prices, keys, columns) == before


def test_prepare_base_freezes_development_formula(tmp_path, monkeypatch):
    import sqlite3
    def forbidden(*args, **kwargs):
        raise AssertionError("准备教学基包不得打开数据库")
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    duckdb = pytest.importorskip("duckdb")
    monkeypatch.setattr(duckdb, "connect", forbidden)
    prepare = load_module("factor_example_prepare", ROOT / "integrations/rdagent/examples/factor_research/prepare.py")
    root = tmp_path / "example"
    template = prepare.prepare_base(root)
    assert template["baseline_id"] == "baseline"
    assert template["budget"]["evaluations"] == 1
    assert template["proposer"] == {"mode": "fixed_policy"}
    import yaml
    package = yaml.safe_load((Path(template["source"]["path"]) / "spec/research.yaml").read_text(encoding="utf-8"))
    assert package["metric_definitions"]
    facts = json.loads((root / "input/request.json").read_text(encoding="utf-8"))
    assert facts["design"]["mode"] == "development"
    assert facts["design"]["feature_expressions"] == {"historical_return": prepare.BASELINE_EXPRESSION}
    confirmation = json.loads((root / "confirmed-definition.json").read_text(encoding="utf-8"))
    assert confirmation["kind"] == "synthetic"
    assert confirmation["confirmed_by"] == "example_definition"
    assert not list(root.rglob("*.duckdb"))


@pytest.mark.parametrize("expression", PRICE_EXPRESSIONS)
def test_published_features_are_independently_checked(monkeypatch, expression):
    from datetime import date
    from types import SimpleNamespace
    import pyarrow as pa
    pytest.importorskip("qlib")
    operator = example_modules(monkeypatch)
    sessions = [f"2025-01-{i:02d}" for i in range(1, 16)]
    day = sessions[11]
    rows = [{"session": date.fromisoformat(d), "entity": "ETF", "close": float(100+i)}
            for i, d in enumerate(sessions)]
    class Input:
        def iter_batches(self, **kwargs):
            return pa.Table.from_pylist(rows).to_batches()
    class Output:
        def write_batches(self, **kwargs):
            return pa.Table.from_batches(kwargs["batches"]).to_pylist()
    design = {"calendar_sessions": sessions, "research_sessions": [day], "entities": ["ETF"],
              "price_fields": ["session", "entity", "close"],
              "feature_expressions": {"historical_return": expression}}
    plan = {"kind": "feature", "output_port": "features",
            "key_columns": ["entity_id", "observation_session", "window_sessions", "feature_id"],
            "work_items": [{"decision_time": day+"T09:30:00+08:00",
                            "key_rows": [["ETF", day, w, f] for w in (5,10)
                                         for f in ("historical_return", "volatility")]}]}
    context = SimpleNamespace(parameters={"design": design, "causal_plan": plan, "lineage_ref": "test"})
    features = operator.run(context, [Input()], Output())
    expected = dict(zip(PRICE_EXPRESSIONS, (110/105-1, 110/109-1, 1/109, 1)))[expression]
    selected = [row for row in features if row["feature_id"] == "historical_return"]
    assert selected and all(row["value"] == pytest.approx(expected) for row in selected)


def test_confirmed_document_is_bound_to_executed_baseline(tmp_path, monkeypatch):
    from quantwitness_rdagent import formula_spec
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.factor_research import FactorResearch
    prepare = load_module("confirmed_factor_prepare", ROOT / "integrations/rdagent/examples/factor_research/prepare.py")
    root = tmp_path / "example"
    prepare.build(root)
    payload = json.loads((root / "request.json").read_text(encoding="utf-8"))
    materials = {"source_title": "测试文档", "source_id": "test-source", "snapshot_artifact_id": "test-snapshot",
        "snapshot_manifest_hash": "existing-identity", "pages": [{"pdf_page": 1, "lines": ["使用历史收盘动量"]}], "review_notes": []}
    draft = {"schema_version": "paper-formula-draft-v1", "title": "已确认公式",
        "rules": [{"rule_id": "r1", "origin": "paper_explicit", "statement": "使用历史收盘动量",
                   "evidence_refs": [{"pdf_page": 1, "line_start": 1, "line_end": 1, "quote": "使用历史收盘动量"}]}],
        "ambiguities": [], "limitations": []}
    decisions = {"accepted_rule_ids": ["r1"], "resolutions": [],
        "interface": prepare.BASELINE_EXPRESSION, "review_notes": "测试已确认定义"}
    paths = {name: str(root / ("formula-" + name + ".json")) for name in ("draft", "materials", "decisions", "path")}
    for name, value in (("draft", draft), ("materials", materials), ("decisions", decisions)):
        write_json(paths[name], value)
    monkeypatch.setattr(formula_spec, "verify_materials", lambda value: None)
    formula_spec.confirm_spec(paths["draft"], paths["materials"], paths["decisions"], paths["path"], confirmed_by="test", approve=True)
    payload["confirmed_spec"] = {"kind": "confirmed_formula", **paths}
    session = FactorResearch(payload)
    assert prepare.BASELINE_EXPRESSION in session.context()["confirmed_spec"]
    assert session.context()["objective"]["metric_definition"]["unit"] == "squared_decimal_price_change"
    changed = json.loads(json.dumps(payload))
    changed["baseline"]["expression"] = "$close / Mean($close, 3) - 1"
    changed["session_root"] = str(root / "other-session")
    with pytest.raises(ValueError, match="基线表达式"):
        FactorResearch(changed)
    draft["rules"][0]["statement"] = "改变后的解释"
    write_json(paths["draft"], draft)
    with pytest.raises(ValueError, match="changed_or_missing"):
        FactorResearch(payload)


def test_knowledge_demo_uses_supplied_record_ids_and_new_expression():
    prepare = load_module("knowledge_example_prepare", ROOT / "integrations/rdagent/examples/factor_research/prepare.py")
    records = [{"record_id": "user-session/verified-result", "usable_for_proposal": True,
                "expression": "$close / Mean($close, 4) - 1", "metrics": {"value": 1, "rows": 1}}]
    responses = prepare.knowledge_responses(records, lambda a, b: {"observation": a, "next": b})
    assert responses[1]["knowledge_refs"] == ["user-session/verified-result"]
    factor = next(iter(responses[3].values()))
    assert factor["knowledge_refs"] == responses[1]["knowledge_refs"]
    assert factor["expression"] == "$close / Mean($close, 2) - 1"
    with pytest.raises(ValueError, match="正式开发指标"):
        prepare.knowledge_responses([], lambda a, b: {})


@pytest.mark.parametrize("count", [2, 3, 4, 5])
@pytest.mark.parametrize("kind", ["dispersion", "position"])
def test_new_price_formulas_match_independent_arithmetic(monkeypatch, count, kind):
    import math
    pytest.importorskip("qlib")
    operator = example_modules(monkeypatch)
    expression = (f"Std($close, {count}) / Mean($close, {count})" if kind == "dispersion"
                  else f"($close - Min($close, {count})) / (Max($close, {count}) - Min($close, {count}))")
    values = [91, 99, 95, 101, 106, 98, 110, 103, 113, 108, 111]
    history = values[-count:]
    mean = sum(history) / count
    expected = (math.sqrt(sum((x-mean)**2 for x in history)/(count-1))/mean if kind == "dispersion"
                else (history[-1]-min(history))/(max(history)-min(history)))
    sessions = [f"2025-01-{i:02d}" for i in range(1, 13)]
    prices = {("ETF", d): v for d, v in zip(sessions, values)}
    design = {"calendar_sessions": sessions, "feature_expressions": {"historical_return": expression}}
    actual = operator._expression_values(design, prices, [["ETF", sessions[-1]]], ["entity_id", "observation_session"])
    assert actual[("ETF", sessions[-1])] == pytest.approx(expected)


@pytest.mark.parametrize("expression", PRICE_EXPRESSIONS[2:])
@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), 0, -1])
def test_new_price_formulas_require_complete_positive_window(monkeypatch, expression, bad):
    pytest.importorskip("qlib")
    operator = example_modules(monkeypatch)
    sessions = [f"2025-01-{i:02d}" for i in range(1, 13)]
    values = [100.0] * 8 + [101.0, bad, 103.0]
    prices = {("ETF", d): v for d, v in zip(sessions, values)}
    design = {"calendar_sessions": sessions, "feature_expressions": {"historical_return": expression}}
    actual = operator._expression_values(design, prices, [["ETF", sessions[-1]]], ["entity_id", "observation_session"])
    assert actual[("ETF", sessions[-1])] is None


@pytest.mark.parametrize("expression,expected", [(PRICE_EXPRESSIONS[2], 0), (PRICE_EXPRESSIONS[3], None)])
def test_flat_price_and_short_history(monkeypatch, expression, expected):
    pytest.importorskip("qlib")
    operator = example_modules(monkeypatch)
    sessions = [f"2025-01-{i:02d}" for i in range(1, 13)]
    prices = {("ETF", d): 100.0 for d in sessions}
    design = {"calendar_sessions": sessions, "feature_expressions": {"historical_return": expression}}
    actual = operator._expression_values(design, prices, [["ETF", sessions[-1]]], ["entity_id", "observation_session"])
    assert actual[("ETF", sessions[-1])] == expected


def test_flat_final_window_is_independent_of_earlier_price_changes(monkeypatch):
    pytest.importorskip("qlib")
    operator = example_modules(monkeypatch)
    sessions = [f"2025-01-{i:02d}" for i in range(1, 13)]
    values = [9.44, 9.47, 9.51, 9.54, 9.57, 9.59, 9.61, 9.62, 9.63, 9.63, 9.63]
    expression = "Std($close, 3) / Mean($close, 3)"
    prices = {("ETF", day): value for day, value in zip(sessions, values)}
    design = {"calendar_sessions": sessions, "feature_expressions": {"historical_return": expression}}
    keys, columns = [["ETF", sessions[-1]]], ["entity_id", "observation_session"]
    expected = 0.0
    assert expected == 0.0
    assert operator._expression_values(design, prices, keys, columns)[("ETF", sessions[-1])] == expected
    for day in sessions[:8]:
        prices[("ETF", day)] *= 1000
    assert operator._expression_values(design, prices, keys, columns)[("ETF", sessions[-1])] == expected
