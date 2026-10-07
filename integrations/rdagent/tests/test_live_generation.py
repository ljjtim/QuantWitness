"""受预算约束的代码生成、技术反馈与恢复；不调用真实模型。"""
import json
import sys
import types
from pathlib import Path

import pytest

from quantwitness_rdagent.contracts import FrozenRequest, write_json
from quantwitness_rdagent.generation import (
    build_prompt, generate_source, model_environment, validate_generated_source,
)
from test_request_and_recovery import request_payload

STUB = "def daily_value(rows):\n    raise NotImplementedError\n\ndef rolling_value(rows):\n    pass\n"
SOURCE = "import math\ndef daily_value(rows):\n    return None\n\ndef rolling_value(rows):\n    return None\n"


def live_payload(tmp_path):
    payload = request_payload(tmp_path)
    payload["runtime_binding"]["fixed_responses"] = []
    payload["budget"].update(live_llm_calls=3, max_output_tokens=240)
    payload["code_generation"] = {
        "mode": "live", "model": "gpt-6.1-sol", "base_url": "https://model.example/v1", "interface": "daily_value(rows), rolling_value(rows)",
        "formula": "固定公式及交易窗口", "initial_stub": STUB, "max_output_tokens_per_call": 100,
    }
    return payload


def fake_model(monkeypatch, fn):
    module = types.ModuleType("quantwitness_rdagent.model_client")
    module.generate = fn
    module.public_config = lambda path: {"model": "gpt-6.1-sol", "base_url": "https://model.example/v1"}
    module.ModelCallError = type("ModelCallError", (RuntimeError,), {})
    monkeypatch.setitem(sys.modules, module.__name__, module)


def response(text=SOURCE, **usage):
    return {"model": "gpt-6.1-sol", "text": text, "usage": usage or {"output_tokens": 10}}


def test_live_request_accepts_bounded_generation(tmp_path):
    request = FrozenRequest.from_dict(live_payload(tmp_path))
    request.freeze()
    assert request.payload["runtime_binding"]["fixed_responses"] == []


@pytest.mark.parametrize("change", ["implemented_stub", "credential", "fixed_answer", "too_many_calls", "per_call", "empty_model"])
def test_live_request_rejects_invalid_contract(tmp_path, change):
    payload = live_payload(tmp_path)
    if change == "implemented_stub":
        payload["code_generation"]["initial_stub"] = SOURCE
    elif change == "credential":
        payload["code_generation"]["api_key"] = "not-a-real-secret"
    elif change == "fixed_answer":
        payload["runtime_binding"]["fixed_responses"] = [SOURCE]
    elif change == "too_many_calls":
        payload["budget"]["live_llm_calls"] = 4
    elif change == "per_call":
        payload["code_generation"]["max_output_tokens_per_call"] = 241
    else:
        payload["code_generation"]["model"] = ""
    with pytest.raises(ValueError):
        FrozenRequest.from_dict(payload)


def test_prompt_contains_only_formula_stub_and_safe_diagnostics(tmp_path):
    request = FrozenRequest.from_dict(live_payload(tmp_path))
    initial = build_prompt(request)
    assert STUB in json.loads(initial.split("\n", 1)[1])["source"]
    evidence = {"execution_status": "failed", "formula_status": "fail", "formula_coverage": {"raw_rows": 99119},
                "diagnostics": [{"stage": "build", "error_type": "SyntaxError", "message": "I:/secret 600000 price=13.27"},
                                "formula.monthly_mismatch entity=600000 value=13.27"], "allowed_metrics": {"ic": 0.98}}
    repaired = build_prompt(request, SOURCE, evidence)
    assert "formula.monthly_mismatch" in repaired and "SyntaxError" in repaired
    assert SOURCE in json.loads(repaired.split("\n", 1)[1])["source"]
    for forbidden in ("600000", "13.27", "99119", "0.98", "I:/secret", str(tmp_path)):
        assert forbidden not in repaired


def test_call_reserved_before_api_and_completed_response_reused(tmp_path, monkeypatch):
    request = FrozenRequest.from_dict(live_payload(tmp_path / "session"))
    request.freeze()
    calls = []
    def generate(env_path, prompt, maximum):
        receipt = json.loads((request.session_root / "model-calls/call-0000.json").read_text(encoding="utf-8"))
        assert receipt["status"] == "reserved" and maximum == 100
        assert env_path.endswith("private.env")
        calls.append(prompt)
        return response(output_tokens=10, input_tokens=60, total_tokens=70, private_field="never-save")
    fake_model(monkeypatch, generate)
    env_file = tmp_path / "private.env"
    with model_environment(env_file):
        assert generate_source(request, 0) == SOURCE
    assert generate_source(request, 0) == SOURCE
    assert len(calls) == 1
    receipt_text = (request.session_root / "model-calls/call-0000.json").read_text(encoding="utf-8")
    assert "private.env" not in receipt_text and "private_field" not in receipt_text
    assert json.loads(receipt_text)["usage"] == {"output_tokens": 10, "input_tokens": 60, "total_tokens": 70}


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_failed_or_interrupted_call_stops_without_repayment(tmp_path, monkeypatch, failure):
    request = FrozenRequest.from_dict(live_payload(tmp_path / "session"))
    request.freeze()
    calls = []
    def generate(*args):
        calls.append(1)
        raise failure("sensitive diagnostic must not persist")
    fake_model(monkeypatch, generate)
    with model_environment(tmp_path / "private.env"):
        with pytest.raises(failure):
            generate_source(request, 0)
        with pytest.raises(RuntimeError, match="预算已占用"):
            generate_source(request, 0)
    assert len(calls) == 1
    receipt_text = (request.session_root / "model-calls/call-0000.json").read_text(encoding="utf-8")
    assert "sensitive diagnostic" not in receipt_text
    assert json.loads(receipt_text)["status"] == ("failed" if failure is RuntimeError else "reserved")


def test_output_budget_is_reserved_without_usage_refund(tmp_path, monkeypatch):
    request = FrozenRequest.from_dict(live_payload(tmp_path / "session"))
    request.freeze()
    maxima = []
    def generate(env_path, prompt, maximum):
        maxima.append(maximum)
        return response(output_tokens=1)
    fake_model(monkeypatch, generate)
    with model_environment(tmp_path / "private.env"):
        for index in range(3):
            generate_source(request, index)
        with pytest.raises(RuntimeError, match="预算已耗尽"):
            generate_source(request, 3)
    assert maxima == [100, 100, 40]


def test_call_count_budget_stops_before_token_total(tmp_path, monkeypatch):
    payload = live_payload(tmp_path / "session")
    payload["budget"]["live_llm_calls"] = 1
    request = FrozenRequest.from_dict(payload)
    request.freeze()
    fake_model(monkeypatch, lambda *args: response())
    with model_environment(tmp_path / "private.env"):
        generate_source(request, 0)
        with pytest.raises(RuntimeError, match="预算已耗尽"):
            generate_source(request, 1)


def test_env_path_is_explicit_and_context_not_persistent(tmp_path, monkeypatch):
    request = FrozenRequest.from_dict(live_payload(tmp_path / "session"))
    request.freeze()
    with pytest.raises(ValueError, match="model-env-file"):
        generate_source(request, 0)
    fake_model(monkeypatch, lambda *args: response())
    with model_environment(tmp_path / "private.env"):
        generate_source(request, 0)
    with pytest.raises(ValueError, match="model-env-file"):
        generate_source(request, 1)
    assert "private.env" not in json.dumps(request.payload)


@pytest.mark.parametrize("source", ["import os\n" + SOURCE, SOURCE + "\nopen('a')", SOURCE.replace("return None", "return eval('1')"),
                                   SOURCE.replace("return None", "return rows.__class__"),
                                   SOURCE.replace("return None", "import math\n    return None")])
def test_generated_code_rejects_external_access(source):
    with pytest.raises(ValueError, match="code\\."):
        validate_generated_source(source)


def test_generated_code_supports_formula_helpers():
    validate_generated_source("from __future__ import annotations\nfrom statistics import mean\n" + SOURCE +
                              "\ndef _mean(values):\n    return sum(values) / len(values)\n")


def test_cli_requires_env_before_loading_rd(tmp_path, monkeypatch):
    from quantwitness_rdagent.__main__ import main
    path = tmp_path / "request.json"
    write_json(path, live_payload(tmp_path / "session"))
    monkeypatch.setattr(sys, "argv", ["rd", "run", "--request", str(path)])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2


def test_changed_model_endpoint_rejected_before_reservation(tmp_path, monkeypatch):
    request = FrozenRequest.from_dict(live_payload(tmp_path / "session"))
    request.freeze()
    def forbidden(*args):
        raise AssertionError("配置不符不得调用API")
    fake_model(monkeypatch, forbidden)
    sys.modules["quantwitness_rdagent.model_client"].public_config = lambda path: {"model": "changed", "base_url": "https://model.example/v1"}
    with model_environment(tmp_path / "private.env"):
        with pytest.raises(ValueError, match="冻结请求不一致"):
            generate_source(request, 0)
    assert not list((request.session_root / "model-calls").glob("call-*.json"))


def test_safe_client_error_code_is_retained(tmp_path, monkeypatch):
    request = FrozenRequest.from_dict(live_payload(tmp_path / "session"))
    request.freeze()
    def failure(*args):
        raise sys.modules["quantwitness_rdagent.model_client"].ModelCallError("model_http_status_429")
    fake_model(monkeypatch, failure)
    with model_environment(tmp_path / "private.env"):
        with pytest.raises(RuntimeError):
            generate_source(request, 0)
    receipt = json.loads((request.session_root / "model-calls/call-0000.json").read_text(encoding="utf-8"))
    assert receipt["error_code"] == "model_http_status_429"
