"""CoSTEER 精确编码经验、正式来源约束和实际生成提示。"""
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from quantwitness_rdagent.contracts import FrozenRequest, write_json
from quantwitness_rdagent.generation import generate_source, model_environment
from test_live_generation import live_payload, fake_model, response, SOURCE

NATIVE = pytest.mark.skipif(sys.platform != "linux", reason="原生 CoSTEER 使用 Linux 环境")


def payload(root):
    value = live_payload(root)
    value["confirmed_formula"] = {key: value["code_generation"][key] for key in ("formula", "interface")}
    value["coding_knowledge"] = {"max_records": 3, "max_source_chars": 12000}
    return value


@pytest.mark.parametrize("change", ["confirmation", "limit", "source", "unknown"])
def test_reject_invalid_knowledge_request(tmp_path, change):
    value = payload(tmp_path)
    if change == "confirmation":
        del value["confirmed_formula"]
    elif change == "limit":
        value["coding_knowledge"]["max_records"] = 0
    elif change == "source":
        value["coding_knowledge"]["source_session"] = ""
    else:
        value["coding_knowledge"]["embedding"] = True
    with pytest.raises(ValueError):
        FrozenRequest.from_dict(value)


def test_source_path_cannot_overlap_session(tmp_path):
    value = payload(tmp_path / "session")
    value["coding_knowledge"]["source_session"] = str(tmp_path / "session/old")
    with pytest.raises(ValueError, match="不能重叠"):
        FrozenRequest.from_dict(value).freeze()


@pytest.fixture
def native_runtime(tmp_path, monkeypatch):
    if sys.platform != "linux":
        pytest.skip("原生 CoSTEER 使用 Linux 环境")
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    from rdagent.core.conf import RD_AGENT_SETTINGS
    from rdagent.log import rdagent_logger
    from rdagent.oai.backend.base import APIBackend
    monkeypatch.setattr(RD_AGENT_SETTINGS, "artifact_signing_key_path", tmp_path / "rd-signing.key")
    rdagent_logger.set_storages_path(tmp_path / "logs")
    def forbidden(*args, **kwargs):
        raise AssertionError("编码知识不得访问模型后端、embedding或数据库")
    monkeypatch.setattr(APIBackend, "__init__", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    return RD_AGENT_SETTINGS


def make_coder(request):
    from test_linux_live_generation import SyntaxTestBridge
    from quantwitness_rdagent.coder import build_coder
    from quantwitness_rdagent.scenario import RPScenario
    return build_coder(RPScenario(request), SyntaxTestBridge(request))


def develop(coder, request):
    from quantwitness_rdagent.scenario import RPTask, RPExperiment
    return coder.develop(RPExperiment([RPTask(request)]))


def make_source(tmp_path, monkeypatch):
    value = payload(tmp_path / "source")
    value["request_id"] = "source-definition"
    request = FrozenRequest.from_dict(value)
    calls = []
    def generate(*args):
        calls.append(args)
        return response("def invalid(:" if len(calls) == 1 else SOURCE)
    fake_model(monkeypatch, generate)
    coder = make_coder(request)
    with model_environment(tmp_path / "private.env"):
        result = develop(coder, request)
    write_json(request.session_root / "outcome.json", result.sub_workspace_list[0].feedback_ref)
    return request, coder, calls


@NATIVE
def test_costeer_persists_error_repair_and_reuses_actual_prompt(tmp_path, monkeypatch, native_runtime):
    from quantwitness_rdagent import coding_knowledge as ck
    source, coder, calls = make_source(tmp_path, monkeypatch)
    assert len(calls) == 2
    assert (source.session_root / "coding-knowledge/knowledge.pkl").is_file()
    checked = []
    monkeypatch.setattr(ck, "_validate_formal_success", lambda *args: checked.append(args[1].name))
    value = payload(tmp_path / "next")
    value["request_id"] = "next-definition"
    value["coding_knowledge"]["source_session"] = str(source.session_root)
    request = FrozenRequest.from_dict(value)
    prompts = []
    fake_model(monkeypatch, lambda env, prompt, maximum: (prompts.append(prompt), response())[1])
    with model_environment(tmp_path / "private.env"):
        result = develop(make_coder(request), request)
    assert checked == ["candidate-0001"]
    assert result.sub_workspace_list[0].candidate_id == "candidate-0000"
    records = json.loads(prompts[0].split("\n", 1)[1])["coding_knowledge"]
    assert {record["success"] for record in records} == {True, False}
    assert "source-definition/candidate-0000" in prompts[0] and "SyntaxError" in prompts[0]
    assert "def invalid(:" in prompts[0] and SOURCE == records[0]["source"]
    assert str(tmp_path) not in prompts[0]
    receipt = json.loads((request.session_root / "model-calls/call-0000.json").read_text())
    assert receipt["prompt"] == prompts[0]
    monkeypatch.setattr(ck, "_source_records", lambda *args: pytest.fail("恢复不能重检索来源"))
    with model_environment(tmp_path / "private.env"):
        develop(make_coder(request), request)
    assert len(prompts) == 1
    restored = make_coder(request).rag.knowledgebase
    traces = next(iter(restored.working_trace_knowledge.values()))
    assert len(traces) == 3 and len({item.implementation.record_id for item in traces}) == 3
    assert generate_source(request, 0) == SOURCE


@NATIVE
def test_source_requires_formal_verification_and_same_definition(tmp_path, monkeypatch, native_runtime):
    from quantwitness_rdagent import coding_knowledge as ck
    source, _, _ = make_source(tmp_path, monkeypatch)
    value = payload(tmp_path / "next")
    value["coding_knowledge"]["source_session"] = str(source.session_root)
    def reject(*args):
        raise ValueError("未通过正式独立验证")
    monkeypatch.setattr(ck, "_validate_formal_success", reject)
    with pytest.raises(ValueError, match="独立验证"):
        make_coder(FrozenRequest.from_dict(value))
    value = payload(tmp_path / "different")
    value["coding_knowledge"]["source_session"] = str(source.session_root)
    value["confirmed_formula"]["formula"] = value["code_generation"]["formula"] = "另一公式"
    with pytest.raises(ValueError, match="公式或接口"):
        make_coder(FrozenRequest.from_dict(value))


@NATIVE
def test_knowledge_projection_excludes_full_diagnostics_and_resource_failures(tmp_path, monkeypatch, native_runtime):
    from quantwitness_rdagent import coding_knowledge as ck
    from quantwitness_rdagent.scenario import RPTask
    from rdagent.core.evolving_framework import EvoStep
    request = FrozenRequest.from_dict(payload(tmp_path / "session"))
    coder = make_coder(request)
    task = RPTask(request)
    evidence = {"command_status": "succeeded", "execution_status": "succeeded",
        "verification_status": "fail", "formula_status": "fail",
        "diagnostics": ["formula.baseline_mismatch I:/secret price=13.27 label=999"]}
    ws = SimpleNamespace(candidate_id="candidate-0000", feedback_ref=evidence, all_codes=SOURCE)
    step = EvoStep(SimpleNamespace(sub_tasks=[task], sub_workspace_list=[ws]))
    coder.rag.generate_knowledge([step])
    queried = coder.rag.query(SimpleNamespace(sub_tasks=[task]), [])
    view = ck.query_view(request, task, queried, 0)
    text = json.dumps(view)
    assert "formula.baseline_mismatch" in text
    assert "13.27" not in text and "secret" not in text and "999" not in text
    ws.candidate_id = "candidate-0001"
    ws.feedback_ref = {**evidence, "diagnostics": [{"stage": "run", "error_code": "worker_crash"}]}
    coder.rag.generate_knowledge([step])
    assert len(coder.rag.knowledgebase.working_trace_knowledge[task.get_task_information()]) == 1


@NATIVE
def test_knowledge_source_character_budget_keeps_complete_functions(tmp_path, monkeypatch, native_runtime):
    from quantwitness_rdagent import coding_knowledge as ck
    from quantwitness_rdagent.scenario import RPTask
    from rdagent.components.coder.CoSTEER.knowledge_management import CoSTEERQueriedKnowledgeV2
    request = FrozenRequest.from_dict(payload(tmp_path / "session"))
    request.payload["coding_knowledge"]["max_source_chars"] = 1
    request.freeze()
    task = RPTask(request)
    item = ck._knowledge(task, {"record_id": "one", "source": SOURCE, "technical_feedback": {}, "success": True})
    queried = CoSTEERQueriedKnowledgeV2(success_task_to_knowledge_dict={task.get_task_information(): item},
        task_to_former_failed_traces={})
    assert ck.query_view(request, task, queried, 0) == []



@NATIVE
def test_cross_formula_transfer_consumes_source_error_repair_in_real_prompt(tmp_path, monkeypatch, native_runtime):
    from quantwitness_rdagent import coding_knowledge as ck
    source, _, _ = make_source(tmp_path, monkeypatch)
    checked = []
    monkeypatch.setattr(ck, "_validate_formal_success", lambda *args: checked.append(args[1].name))
    value = payload(tmp_path / "different-formula")
    value["request_id"] = "different-formula"
    value["coding_knowledge"]["source_session"] = str(source.session_root)
    value["confirmed_formula"]["formula"] = value["code_generation"]["formula"] = "另一确认公式，固定交易窗口"
    value["coding_knowledge"]["retrieval_scope"] = "technical_transfer"
    request = FrozenRequest.from_dict(value)
    prompts = []
    fake_model(monkeypatch, lambda env, prompt, maximum: (prompts.append(prompt), response())[1])
    with model_environment(tmp_path / "private.env"):
        result = develop(make_coder(request), request)
    assert checked == ["candidate-0001"]
    assert result.sub_workspace_list[0].candidate_id == "candidate-0000"
    projection = json.loads(prompts[0].split("\n", 1)[1])
    assert projection["formula"] == "另一确认公式，固定交易窗口"
    records = projection["coding_knowledge"]
    assert [record["success"] for record in records] == [True, False]
    assert all(record["source_task"]["formula"] == source.payload["confirmed_formula"]["formula"] for record in records)
    assert all(record["match_reason"]["kind"] == "technical_transfer" for record in records)
    assert all(record["repair_record_id"] == "source-definition/candidate-0001" for record in records)
    assert "SyntaxError" in prompts[0] and "def invalid(:" in prompts[0]
    assert str(tmp_path) not in prompts[0]
    attempts = json.loads((request.session_root / "coder-attempts.json").read_text(encoding="utf-8"))
    assert attempts[0]["coding_knowledge_records"] == [record["record_id"] for record in records]


@NATIVE
def test_cross_formula_transfer_keeps_interface_boundary(tmp_path, monkeypatch, native_runtime):
    from quantwitness_rdagent import coding_knowledge as ck
    source, _, _ = make_source(tmp_path, monkeypatch)
    monkeypatch.setattr(ck, "_validate_formal_success", lambda *args: None)
    value = payload(tmp_path / "different-interface")
    value["coding_knowledge"]["source_session"] = str(source.session_root)
    value["confirmed_formula"]["interface"] = value["code_generation"]["interface"] = "daily_value(frame), rolling_value(frame)"
    value["coding_knowledge"]["retrieval_scope"] = "technical_transfer"
    request = FrozenRequest.from_dict(value)
    with pytest.raises(ValueError, match="接口不一致"):
        make_coder(request)


def test_technical_transfer_formula_selection_retains_source_definition():
    from quantwitness_rdagent.coding_transfer import select_records
    task = {"kind": "formula", "interface": "pure-functions-v1", "formula": "Mean(close, 3)"}
    source = "import statistics\ndef rolling_value(window):\n    return statistics.mean(window)\n"
    target = {**task, "formula": "Std(close, 3) / Mean(close, 3)"}
    records = [{"record_id": "mean/candidate-0000", "task": task, "source": "def invalid(:", "technical_feedback": {"diagnostic_codes": ["error_type:SyntaxError"]}, "success": False},
               {"record_id": "mean/candidate-0001", "task": task, "source": source, "technical_feedback": {}, "success": True}]
    result = select_records(records, target, "", max_records=3, max_source_chars=12000)
    assert [record["success"] for record in result] == [True, False]
    assert result[0]["source_task"]["formula"] == "Mean(close, 3)"
    assert result[0]["match_reason"]["shared_operations"] == ["aggregation"]
    assert all(record["repair_record_id"] == "mean/candidate-0001" for record in result)


def test_technical_transfer_only_exports_approved_error_codes():
    from quantwitness_rdagent.coding_transfer import select_records
    task = {"kind": "formula", "interface": "pure-functions-v1", "formula": "Mean(close, 3)"}
    record = {"record_id": "mean/candidate-0001", "task": task, "source": "def value(): return 1", "success": True,
              "technical_feedback": {"holdout_ic": 0.87, "price": 13.27, "diagnostic_codes": ["code.bad_shape", "test_ic:0.87", "I:/private"]}}
    result = select_records([record], task, "", max_records=1, max_source_chars=12000)
    assert result[0]["technical_feedback"] == {"diagnostic_codes": ["code.bad_shape"]}


@pytest.mark.parametrize("scope", ["exact_formula", "technical_transfer"])
def test_public_knowledge_scope_is_accepted_and_frozen(tmp_path, scope):
    value = payload(tmp_path / "session")
    value["coding_knowledge"]["retrieval_scope"] = scope
    request = FrozenRequest.from_dict(value)
    request.freeze()
    assert FrozenRequest.load(request.session_root / "request.json").payload["coding_knowledge"]["retrieval_scope"] == scope
    changed = payload(tmp_path / "session")
    changed["coding_knowledge"]["retrieval_scope"] = "exact_formula" if scope == "technical_transfer" else "technical_transfer"
    with pytest.raises(ValueError, match="请求已冻结"):
        FrozenRequest.from_dict(changed).freeze()


@pytest.mark.parametrize("scope", ["embedding", "", [], {}, 1, None])
def test_public_knowledge_scope_rejects_unsupported_values(tmp_path, scope):
    value = payload(tmp_path)
    value["coding_knowledge"]["retrieval_scope"] = scope
    with pytest.raises(ValueError, match="检索范围"):
        FrozenRequest.from_dict(value)
