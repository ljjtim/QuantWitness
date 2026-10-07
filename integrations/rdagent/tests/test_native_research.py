"""固定响应验证上游研究组件的调用、开发投影和无缓存边界。"""
import copy
import importlib.util
import json
import sqlite3

import pytest

from quantwitness_rdagent.native_backend import ResearchTextBackend
from quantwitness_rdagent.native_research import propose_factor, reflect_factor


NATIVE_AVAILABLE = importlib.util.find_spec("rdagent") is not None
native = pytest.mark.skipif(not NATIVE_AVAILABLE, reason="需要固定RD-Agent源码及Linux场景依赖")


def context():
    return {
        "confirmed_spec": "使用当前收盘已知价格研究下一期收益；只修改历史收益表达式。",
        "fields": ["close"], "max_window": 5,
        "expression_contract": {"operators": ["Ref", "Mean"], "metric": "开发预测MAE", "unit": "ratio"},
        "max_output_tokens": 512,
        "history": [{
            "candidate_id": "baseline", "hypothesis": "五日动量可以预测收益",
            "expression": "$close / Ref($close, 5) - 1",
            "metrics": {"value": 0.2, "rows": 20},
            "reflection": {
                "observations": "动量效果有限", "hypothesis_evaluation": "没有支持原假设",
                "new_hypothesis": "NEXT_ROUND_USE_SMOOTHING", "reason": "尝试减少噪声", "decision": False,
            },
        }],
    }


def hypothesis():
    return {"hypothesis": "平滑价格变化可以减少噪声", "reason": "降低短期价格冲击"}


def factor(expression="Mean($close, 3) / Ref($close, 3) - 1"):
    return {"smoothed_momentum": {
        "description": "三期平滑动量", "formulation": "mean(close,3)/lag(close,3)-1",
        "variables": {"close": "当时已知收盘价格"}, "expression": expression,
    }}


class FixedRequest:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def __call__(self, prompt, *, instructions, max_output_tokens):
        self.calls.append({"prompt": prompt, "instructions": instructions, "max_output_tokens": max_output_tokens})
        return json.dumps(next(self.responses), ensure_ascii=False)


def forbid_database(*args, **kwargs):
    raise AssertionError("原生研究文本调用不得连接数据库")


@native
def test_native_proposal_uses_upstream_templates_and_prior_reflection(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", forbid_database)
    complete = FixedRequest([hypothesis(), factor()])
    actual = propose_factor(context(), complete)
    assert actual["expression"] == factor()["smoothed_momentum"]["expression"]
    assert set(actual) == {"hypothesis", "reason", "expression"}
    assert len(complete.calls) == 2
    assert all(call["max_output_tokens"] == 512 for call in complete.calls)
    assert "NEXT_ROUND_USE_SMOOTHING" in complete.calls[0]["prompt"]
    assert "NEXT_ROUND_USE_SMOOTHING" in complete.calls[1]["prompt"]
    assert "hypotheses" in complete.calls[0]["instructions"]
    assert "不是预测样本数" in complete.calls[0]["instructions"]
    assert "formulation" in complete.calls[1]["instructions"]
    assert "expression" in complete.calls[1]["instructions"]
    assert "$close / Ref($close, 5) - 1" in complete.calls[1]["prompt"]


@native
def test_native_reflection_preserves_actual_metric_and_returns_feedback(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", forbid_database)
    value = context()
    value["current"] = {
        "candidate_id": "new_candidate", "hypothesis": "平滑后误差会下降",
        "expression": "Mean($close, 3) / Ref($close, 3) - 1",
        "metrics": {"value": 0.21, "rows": 20},
    }
    complete = FixedRequest([{
        "Observations": "开发误差增加", "Feedback for Hypothesis": "未支持假设",
        "New Hypothesis": "尝试短期反转", "Reasoning": "当前平滑可能丢失信息", "Replace Best Result": "no",
    }])
    result = reflect_factor(value, complete)
    assert result["decision"] is False
    assert result["new_hypothesis"] == "尝试短期反转"
    assert len(complete.calls) == 1
    assert '"value": 0.21' in complete.calls[0]["prompt"]
    assert "开发预测MAE" in complete.calls[0]["instructions"]
    assert "Current Result is" not in complete.calls[0]["prompt"]


@native
def test_duplicate_expression_is_rejected_without_hidden_retries(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", forbid_database)
    complete = FixedRequest([hypothesis(), factor("$close/Ref($close,5)-1")])
    with pytest.raises(ValueError, match="已经研究过"):
        propose_factor(context(), complete)
    assert len(complete.calls) == 2


@native
def test_failed_model_call_restores_global_backend(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", forbid_database)
    from rdagent.oai.llm_conf import LLM_SETTINGS
    before = LLM_SETTINGS.backend

    def fail(*args, **kwargs):
        raise RuntimeError("预算已用尽")

    with pytest.raises(RuntimeError, match="预算已用尽"):
        propose_factor(context(), fail)
    assert LLM_SETTINGS.backend == before
    with pytest.raises(RuntimeError, match="尚未绑定"):
        ResearchTextBackend().build_messages_and_create_chat_completion("user", "system")


@pytest.mark.parametrize("fault", ["report", "metric_extra", "nonfinite", "unverified_shape"])
def test_reject_unprojected_feedback_before_upstream_import(fault):
    value = copy.deepcopy(context())
    if fault == "report":
        value["report"] = "最终隔离集报告"
    elif fault == "metric_extra":
        value["history"][0]["metrics"]["holdout"] = 0.99
    elif fault == "nonfinite":
        value["history"][0]["metrics"]["value"] = float("nan")
    else:
        value["history"][0]["result"] = {"status": "unverified"}
    complete = FixedRequest([])
    with pytest.raises(ValueError):
        propose_factor(value, complete)
    assert not complete.calls


def test_embedding_and_chat_session_are_explicitly_unavailable():
    backend = ResearchTextBackend()
    with pytest.raises(NotImplementedError, match="embedding"):
        backend.create_embedding(["factor"])
    with pytest.raises(NotImplementedError, match="无缓存"):
        backend.build_chat_session()


@native
def test_technical_failure_history_is_not_promoted_to_metric(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", forbid_database)
    value = context()
    value["development"] = {"start": "2020-01-01", "end": "2020-01-10", "as_of": "2020-01-11T00:00:00Z"}
    value["objective"] = {"direction": "minimize", "value_column": "mae"}
    value["history"].append({"candidate_id": "invalid", "status": "rejected", "reason": "表达式不支持"})
    complete = FixedRequest([hypothesis(), factor()])
    propose_factor(value, complete)
    assert '"status": "rejected"' in complete.calls[0]["prompt"]
    assert '"direction": "minimize"' in complete.calls[0]["instructions"]
    value["current"] = value["history"][-1]
    with pytest.raises(ValueError, match="已执行验证"):
        reflect_factor(value, FixedRequest([]))


@pytest.mark.parametrize("json_mode", [False, True])
def test_json_mode_declares_output_format_in_recorded_request(json_mode):
    from quantwitness_rdagent.native_backend import _REQUEST
    complete = FixedRequest([{"answer": "完成"}])
    token = _REQUEST.set((complete, 512))
    try:
        actual = ResearchTextBackend().build_messages_and_create_chat_completion(
            "研究请求", "正式开发指标", json_mode=json_mode,
        )
    finally:
        _REQUEST.reset(token)
    assert json.loads(actual) == {"answer": "完成"}
    assert len(complete.calls) == 1
    prompt = complete.calls[0]["prompt"]
    assert prompt.startswith("研究请求")
    assert ("只返回一个有效JSON对象" in prompt) is json_mode
    assert ("本次任务约束：\n正式开发指标" in prompt) is json_mode
    assert complete.calls[0]["instructions"] == "正式开发指标"


def test_invalid_json_does_not_trigger_hidden_retry():
    from quantwitness_rdagent.native_backend import _REQUEST
    calls = []
    def complete(prompt, **kwargs):
        calls.append(prompt)
        return "普通文字"
    token = _REQUEST.set((complete, 512))
    try:
        with pytest.raises(json.JSONDecodeError):
            ResearchTextBackend().build_messages_and_create_chat_completion(
                "研究请求", "正式开发指标", json_mode=True,
            )
    finally:
        _REQUEST.reset(token)
    assert len(calls) == 1
