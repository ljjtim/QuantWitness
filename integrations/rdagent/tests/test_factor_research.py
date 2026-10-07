"""因子研究的冻结、预算和开发反馈边界；不执行模型或数据库。"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from quantwitness_rdagent.factor_research import (
    FactorResearch, ResearchCalls, ResearchBudgetExhausted, canonical_expression, _freeze,
)
from quantwitness_rdagent.contracts import write_json


def session(tmp_path, responses=None):
    value = FactorResearch.__new__(FactorResearch)
    value.root = tmp_path
    value.model_env_file = None
    value.metric_description = {"unit": "squared_decimal_price_change", "metric_id": "validation_mse"}
    value.materials = {"text": "确认的教学定义", "responses": responses or ['{"ok":true}']}
    value.payload = {"campaign_id": "demo", "budget": {"rounds": 3, "model_calls": 2,
        "output_tokens": 100, "max_output_tokens_per_call": 50},
        "proposer": {"mode": "fixed_responses"},
        "package_template": {"objective": {"direction": "minimize"},
                             "development": {"end": "2025-01-10"}}}
    return value


def test_call_recovery_does_not_repeat_response_or_spend(tmp_path):
    value = session(tmp_path)
    assert ResearchCalls(value, "proposal")("任务", instructions="规则") == '{"ok":true}'
    calls = list((tmp_path / "calls").glob("*.json"))
    before = calls[0].read_bytes(), calls[0].stat().st_mtime_ns
    value.materials["responses"] = []
    assert ResearchCalls(value, "proposal")("任务", instructions="规则") == '{"ok":true}'
    assert before == (calls[0].read_bytes(), calls[0].stat().st_mtime_ns)
    with pytest.raises(ValueError, match="请求"):
        ResearchCalls(value, "proposal")("另一任务", instructions="规则")


def test_unknown_model_outcome_is_not_retried(tmp_path):
    value = session(tmp_path)
    write_json(tmp_path / "calls/proposal-00.json", {"status": "reserved",
        "request": {"prompt": "任务", "instructions": "规则", "max_output_tokens": 50}})
    with pytest.raises(RuntimeError, match="outcome_unknown"):
        ResearchCalls(value, "proposal")("任务", instructions="规则")


def test_reserved_calls_enforce_both_budgets(tmp_path):
    value = session(tmp_path, ["one", "two"])
    assert ResearchCalls(value, "first")("a", instructions="b") == "one"
    value.payload["budget"]["output_tokens"] = 99
    with pytest.raises(ResearchBudgetExhausted):
        ResearchCalls(value, "second")("c", instructions="d")
    value.payload["budget"]["output_tokens"] = 100
    assert ResearchCalls(value, "second")("c", instructions="d") == "two"
    with pytest.raises(ResearchBudgetExhausted):
        ResearchCalls(value, "third")("e", instructions="f")


@pytest.mark.parametrize("expression", ["Ref($close,-1)", "$close / Ref($close, 0) - 1",
    "$close / Mean($close, 6) - 1", "$close / Ref($close,1) - 1 + $label"])
def test_formula_scope_rejects_future_or_unverified_method(expression):
    with pytest.raises(ValueError):
        canonical_expression(expression)


def test_formula_canonicalization_identifies_duplicate():
    assert canonical_expression(" $close/Ref($close, 3)-1.0 ") == "$close / Ref($close, 3) - 1"


def test_context_never_exposes_result_paths_or_unverified_metric(tmp_path):
    value = session(tmp_path)
    write_json(tmp_path / "rounds/0000/record.json", {"candidate_id": "x", "status": "failed",
        "expression": "$close / Ref($close, 5) - 1", "hypothesis": "h", "reason": "r",
        "metrics": {"value": 999, "holdout": 999, "result_ref": "PRIVATE_RESULT"},
        "diagnostic": "PRIVATE_DIAGNOSTIC"})
    write_json(tmp_path / "rounds/0001/record.json", {"candidate_id": "y", "status": "evaluated",
        "expression": "$close / Ref($close, 3) - 1", "hypothesis": "h", "reason": "r",
        "metrics": {"value": 0.5, "rows": 3, "result_ref": "PRIVATE_RESULT", "test": 999},
        "reflection": {"reason": "有界开发结论"}})
    result = value.context()
    assert "metrics" not in result["history"][0]
    assert result["history"][1]["metrics"] == {"value": 0.5, "rows": 3}
    text = json.dumps(result)
    assert "PRIVATE" not in text and "holdout" not in text and '"test"' not in text


def test_freezing_refuses_changed_confirmation(tmp_path):
    path = tmp_path / "frozen.json"
    _freeze(path, {"definition": "past"})
    with pytest.raises(ValueError, match="冻结材料"):
        _freeze(path, {"definition": "future"})


def test_finish_does_not_claim_failed_or_unreflected_round_completed(tmp_path):
    value = session(tmp_path)
    write_json(tmp_path / "rounds/0000/record.json", {"round": 0, "candidate_id": "a", "status": "evaluated",
        "metrics": {"value": 1, "result_ref": "r", "verification_ref": "v"}})
    result = value.finish("model_budget")
    assert result["status"] == "incomplete"
    assert result["generated_candidates"] == 0
    assert result["holdout_evaluated"] is False


def test_live_identity_change_is_refused_before_reserving(tmp_path, monkeypatch):
    from quantwitness_rdagent import model_client
    value = session(tmp_path)
    value.payload["proposer"] = {"mode": "live", "model": "frozen", "base_url": "https://example.test/v1"}
    value.model_env_file = "unused.env"
    monkeypatch.setattr(model_client, "public_config", lambda path: {"model": "changed", "base_url": "https://example.test/v1"})
    with pytest.raises(ValueError, match="身份"):
        ResearchCalls(value, "proposal")("任务", instructions="规则")
    assert not list((tmp_path / "calls").glob("*.json"))


def live_session(tmp_path, monkeypatch, response):
    from quantwitness_rdagent import model_client
    value = session(tmp_path)
    identity = {"model": "frozen", "base_url": "https://example.test/v1"}
    value.payload["proposer"] = {"mode": "live", **identity}
    value.model_env_file = "explicit.env"
    monkeypatch.setattr(model_client, "public_config", lambda path: identity)
    calls = []
    def request(path, prompt, limit, *, instructions):
        assert path == "explicit.env" and limit == 50
        calls.append(prompt)
        return response
    monkeypatch.setattr(model_client, "request_text", request)
    return value, calls


def test_live_client_receipt_is_consumed_and_replayed(tmp_path, monkeypatch):
    value, calls = live_session(tmp_path, monkeypatch, {"model": "frozen", "text": '{"ok":true}',
        "usage": {"input_tokens": 20, "output_tokens": 12, "total_tokens": 32, "unrelated": "private"}})
    for _ in range(2):
        assert ResearchCalls(value, "proposal")("任务", instructions="规则") == '{"ok":true}'
    assert calls == ["任务"]
    receipt = json.loads((tmp_path / "calls/proposal-00.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "completed"
    assert receipt["usage"] == {"input_tokens": 20, "output_tokens": 12, "total_tokens": 32}


@pytest.mark.parametrize("response", [
    {"model": "different", "text": "{}"},
    {"model": "frozen", "text": "{}", "usage": {"output_tokens": 51}},
    {"model": "frozen", "text": ""},
])
def test_invalid_live_response_preserves_spend_without_retry(tmp_path, monkeypatch, response):
    value, calls = live_session(tmp_path, monkeypatch, response)
    with pytest.raises(RuntimeError, match="预算已占用"):
        ResearchCalls(value, "proposal")("任务", instructions="规则")
    with pytest.raises(RuntimeError, match="outcome_unknown"):
        ResearchCalls(value, "proposal")("任务", instructions="规则")
    assert calls == ["任务"]
    receipt = json.loads((tmp_path / "calls/proposal-00.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "failed"


@pytest.mark.parametrize("expression,canonical,direction", [
    (" Std($close,3)/ Mean($close,3) ", "Std($close, 3) / Mean($close, 3)", "relative_volatility"),
    ("($close-Min($close,4))/(Max($close,4)-Min($close,4))", "($close - Min($close, 4)) / (Max($close, 4) - Min($close, 4))", "range_position"),
])
def test_price_family_normalization_and_direction(expression, canonical, direction):
    from quantwitness_rdagent.factor_research import expression_direction
    assert canonical_expression(expression) == canonical
    assert expression_direction(expression) == direction


@pytest.mark.parametrize("expression", [
    "Std($close,1)/Mean($close,1)", "Std($close,3)/Mean($close,4)",
    "Std($close,-3)/Mean($close,-3)", "Std($close,6)/Mean($close,6)",
    "($close-Min($close,3))/(Max($close,4)-Min($close,3))",
    "($close-Min($close,3))/(Max($close,3)-Min($close,4))",
    "Std($volume,3)/Mean($volume,3)", "Ref(Std($close,3),-1)/Mean($close,3)",
])
def test_price_families_reject_mixed_windows_future_and_unbound_fields(expression):
    with pytest.raises(ValueError):
        canonical_expression(expression)
