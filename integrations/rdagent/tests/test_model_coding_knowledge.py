"""模型技术经验的筛选、来源核验、持久化与金融反馈隔离。"""
import json
from pathlib import Path

import pytest

from quantwitness_rdagent.coding_transfer import matching_reason, select_records, technical_feedback
from quantwitness_rdagent.model_knowledge import ModelCodingKnowledge

TASK = {"interface": "fit-predict-tabular-v1", "model_family": "LinearModel"}
SOURCE = "import numpy as np\nclass LinearModel:\n    def fit(self, x, y):\n        self.x = np.asarray(x)\n    def predict(self, x):\n        return np.asarray(x)\n"
BROKEN = "class LinearModel(:"


def evidence(success=True):
    value = {"command_status": "succeeded", "execution_status": "succeeded", "verification_status": "pass",
             "model_status": "pass", "diagnostics": [],
             "execution_ref": "formal/execution", "result_ref": "formal/result",
             "verification_ref": "formal/verification", "bundle_ref": "formal/bundle"}
    if not success:
        value.update(command_status="failed", execution_status="not_run", verification_status="not_run", model_status="not_run",
                     diagnostics=[{"stage": "code_validation", "error_type": "SyntaxError", "message": "I:/private/test_auc=0.987 holdout=99"}])
    return value


def source_session(tmp_path):
    root = tmp_path / "source-linear"
    knowledge = ModelCodingKnowledge(root, task=TASK)
    knowledge.record(candidate_id="candidate-0000", source=BROKEN, evidence=evidence(False))
    knowledge.record(candidate_id="candidate-0001", source=SOURCE, evidence=evidence())
    return root


def test_cross_model_query_preserves_error_repair_and_excludes_metrics(tmp_path):
    source = source_session(tmp_path)
    calls = []
    def validate(root, record):
        calls.append((root, record["record_id"]))
        assert record["source"] in {SOURCE, BROKEN}
    knowledge = ModelCodingKnowledge(tmp_path / "next-ridge", task={**TASK, "model_family": "RidgeModel"},
        source_sessions=[source], validate_source=validate, source_template=SOURCE)
    records = knowledge.query(0)
    assert len(calls) == 2
    assert [record["success"] for record in records] == [True, False]
    assert all(record["match_reason"]["kind"] == "technical_transfer" for record in records)
    assert all(record["source_task"]["model_family"] == "LinearModel" for record in records)
    text = json.dumps(records)
    assert "SyntaxError" in text and BROKEN in text and "source-linear/candidate-0001" in text
    assert all(value not in text for value in ("test_auc", "holdout", "0.987", "I:/private", "formal/"))
    frozen = json.loads((knowledge.root / "input.json").read_text(encoding="utf-8"))
    assert frozen["records"][1]["formal_refs"]["verification_ref"] == "formal/verification"


def test_model_resume_uses_frozen_source_and_query_without_revalidation(tmp_path):
    source = source_session(tmp_path)
    current = tmp_path / "next"
    knowledge = ModelCodingKnowledge(current, task=TASK, source_sessions=[source], validate_source=lambda *args: None)
    records = knowledge.query(0)
    (source / "model-coding-knowledge/records/candidate-0001.json").unlink()
    def reject(*args):
        pytest.fail("恢复不能重新验证或扫描已冻结来源")
    restored = ModelCodingKnowledge(current, task=TASK, source_sessions=[source], validate_source=reject)
    assert restored.query(0) == records
    assert restored.query(1) == records


@pytest.mark.parametrize("change", ["interface", "model_family", "source", "budget", "template"])
def test_model_resume_rejects_contract_changes(tmp_path, change):
    source = source_session(tmp_path)
    current = tmp_path / "next"
    ModelCodingKnowledge(current, task=TASK, source_sessions=[source], validate_source=lambda *args: None)
    args = dict(task=TASK, source_sessions=[source], validate_source=lambda *args: None)
    if change in {"interface", "model_family"}:
        args["task"] = {**TASK, change: "Different"}
    elif change == "source":
        args["source_sessions"] = []
    elif change == "budget":
        args["max_records"] = 2
    else:
        args["source_template"] = SOURCE
    with pytest.raises(ValueError, match="冻结模型"):
        ModelCodingKnowledge(current, **args)


def test_model_source_requires_validator_and_actual_validation(tmp_path):
    source = source_session(tmp_path)
    with pytest.raises(ValueError, match="正式模型验证"):
        ModelCodingKnowledge(tmp_path / "missing-validator", task=TASK, source_sessions=[source])
    def reject(*args):
        raise ValueError("来源源码与正式模型不一致")
    with pytest.raises(ValueError, match="正式模型不一致"):
        ModelCodingKnowledge(tmp_path / "bad-source", task=TASK, source_sessions=[source], validate_source=reject)


def test_unrepaired_source_is_not_reusable(tmp_path):
    source = tmp_path / "only-error"
    knowledge = ModelCodingKnowledge(source, task=TASK)
    knowledge.record(candidate_id="candidate-0000", source=BROKEN, evidence=evidence(False))
    with pytest.raises(ValueError, match="缺少正式验证"):
        ModelCodingKnowledge(tmp_path / "next", task=TASK, source_sessions=[source], validate_source=lambda *args: None)


@pytest.mark.parametrize("code", ["worker_crash", "heartbeat_timeout", "resource_memory_exhausted", "project_worker_transport"])
def test_resource_failures_are_not_model_code_experience(tmp_path, code):
    knowledge = ModelCodingKnowledge(tmp_path, task=TASK)
    feedback = evidence(False)
    feedback["diagnostics"] = [{"stage": "run", "error_code": code, "error_type": "TypeError", "message": "model.bad_shape"}]
    assert knowledge.record(candidate_id="candidate-0000", source=SOURCE, evidence=feedback) is None
    assert not list((knowledge.root / "records").glob("*.json"))


def test_success_requires_all_formal_references_and_candidate_is_immutable(tmp_path):
    knowledge = ModelCodingKnowledge(tmp_path, task=TASK)
    incomplete = evidence()
    del incomplete["result_ref"]
    with pytest.raises(ValueError, match="完整正式来源"):
        knowledge.record(candidate_id="candidate-0000", source=SOURCE, evidence=incomplete)
    knowledge.record(candidate_id="candidate-0000", source=SOURCE, evidence=evidence())
    with pytest.raises(ValueError, match="候选内容改变"):
        knowledge.record(candidate_id="candidate-0000", source=BROKEN, evidence=evidence())


def test_different_interface_and_unrelated_structure_are_not_transferred(tmp_path):
    source = source_session(tmp_path)
    knowledge = ModelCodingKnowledge(tmp_path / "next", task={**TASK, "interface": "sequence-fit-v1"},
        source_sessions=[source], validate_source=lambda *args: None, source_template=SOURCE)
    assert knowledge.query(0) == []
    assert matching_reason({"kind": "model", **TASK}, {"kind": "model", **{**TASK, "model_family": "OtherModel"}},
                           "def value(): return 1", "def value(): return 2") is None


def test_query_budget_preserves_complete_files_and_success_priority(tmp_path):
    source = source_session(tmp_path)
    short = ModelCodingKnowledge(tmp_path / "short", task=TASK, source_sessions=[source], validate_source=lambda *args: None,
                                max_source_chars=len(SOURCE) - 1)
    assert short.query(0) == []
    one = ModelCodingKnowledge(tmp_path / "one", task=TASK, source_sessions=[source], validate_source=lambda *args: None,
                              max_records=1)
    assert one.query(0)[0]["source"] == SOURCE


def test_model_local_records_are_available_to_later_queries(tmp_path):
    knowledge = ModelCodingKnowledge(tmp_path, task=TASK)
    assert knowledge.query(0) == []
    knowledge.record(candidate_id="candidate-0000", source=BROKEN, evidence=evidence(False))
    knowledge.record(candidate_id="candidate-0001", source=SOURCE, evidence=evidence())
    assert [record["success"] for record in knowledge.query(1)] == [True, False]
    assert knowledge.query(0) == []


def test_technical_projection_drops_test_holdout_and_diagnostic_values():
    feedback = evidence(False)
    feedback.update(test={"auc": 0.91}, holdout={"ic": 0.52}, allowed_metrics={"Sharpe": 9.8})
    assert technical_feedback(feedback)["diagnostic_codes"] == ["error_type:SyntaxError", "stage:code_validation"]
    text = json.dumps(technical_feedback(feedback))
    assert all(value not in text for value in ("test", "holdout", "Sharpe", "0.91", "0.52", "9.8", "I:/private"))


def test_exact_model_precedes_cross_model_without_financial_ranking():
    target = {"kind": "model", **TASK}
    records = [{"record_id": "cross/candidate-0001", "task": {**target, "model_family": "RidgeModel"}, "source": SOURCE,
                "technical_feedback": {}, "success": True},
               {"record_id": "exact/candidate-0001", "task": target, "source": SOURCE, "technical_feedback": {}, "success": True}]
    result = select_records(records, target, SOURCE, max_records=2, max_source_chars=60000)
    assert [record["record_id"].split("/")[0] for record in result] == ["exact", "cross"]


def test_source_task_must_match_frozen_declaration(tmp_path):
    source = source_session(tmp_path)
    path = source / "model-coding-knowledge/records/candidate-0001.json"
    record = json.loads(path.read_text(encoding="utf-8"))
    record["task"]["model_family"] = "ChangedModel"
    path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(ValueError, match="冻结任务"):
        ModelCodingKnowledge(tmp_path / "next", task=TASK, source_sessions=[source], validate_source=lambda *args: None)


GENERATED = {"interface": "generated-dag-v1", "model_family": "GeneratedModel"}
DEFINITION = {"nodes": [{"inputs": [-1], "width": 1, "activation": "identity"}]}


def test_current_model_compile_error_is_consumed_before_any_success(tmp_path):
    knowledge = ModelCodingKnowledge(tmp_path, task=GENERATED)
    invalid = '{"definition":{"nodes":[{"inputs":[-1],"width":0,"activation":"identity"}]}}'
    feedback = {"model_status": "fail", "diagnostics": [{"stage": "model_validation", "error_code": "model.definition_invalid", "error_type": "ValueError"}]}
    knowledge.record(candidate_id="model_0001_attempt_0", source=invalid, evidence=feedback)
    records = knowledge.query(0)
    assert records[0]["source"] == invalid
    assert records[0]["match_reason"]["kind"] == "current_failure"
    assert not records[0]["success"]


def test_generated_model_definition_is_per_candidate_and_cross_structure(tmp_path):
    root = tmp_path / "source"
    knowledge = ModelCodingKnowledge(root, task=GENERATED)
    invalid = '{"definition":{"nodes":[{"inputs":[-1],"width":0,"activation":"identity"}]}}'
    feedback = {"model_status": "fail", "diagnostics": [{"stage": "model_validation", "error_code": "model.definition_invalid"}]}
    knowledge.record(candidate_id="model_0001_attempt_0", source=invalid, evidence=feedback)
    source = "import torch\nfrom torch import nn\nclass Net(nn.Module):\n    def forward(self, x):\n        return self.layer(x)\n"
    knowledge.record(candidate_id="model_0001", source=source, task={**GENERATED, "definition": DEFINITION}, evidence=evidence())
    next_task = {**GENERATED, "definition": {"nodes": [{"inputs": [-1], "width": 3, "activation": "relu"}, {"inputs": [0], "width": 1, "activation": "identity"}]}}
    next_knowledge = ModelCodingKnowledge(tmp_path / "next", task=next_task, source_sessions=[root], validate_source=lambda *args: None)
    records = next_knowledge.query(0)
    assert [record["success"] for record in records] == [True, False]
    assert all(record["repair_record_id"] == "source/model_0001" for record in records)
    assert all(record["match_reason"]["kind"] == "technical_transfer" for record in records)
    assert records[1]["source"] == invalid
    assert records[0]["source_task"]["definition"] == DEFINITION


def test_generated_query_target_is_frozen_per_attempt(tmp_path):
    knowledge = ModelCodingKnowledge(tmp_path, task=GENERATED)
    assert knowledge.query(0, task={**GENERATED, "definition": DEFINITION}) == []
    changed = {**GENERATED, "definition": {"nodes": [{"inputs": [-1], "width": 3, "activation": "identity"}]}}
    with pytest.raises(ValueError, match="查询目标改变"):
        knowledge.query(0, task=changed)


def test_model_session_directories_preserve_distinct_source_ids(tmp_path):
    first = ModelCodingKnowledge(tmp_path / "source" / "session", task=TASK)
    second = ModelCodingKnowledge(tmp_path / "next" / "session", task=TASK)
    one = first.record(candidate_id="candidate-0000", source=SOURCE, evidence=evidence())
    two = second.record(candidate_id="candidate-0000", source=SOURCE, evidence=evidence())
    assert one["record_id"] == "source/session/candidate-0000"
    assert two["record_id"] == "next/session/candidate-0000"


def test_actual_generated_compile_receipt_is_required(tmp_path):
    from quantwitness_rdagent.model_research_evidence import validate_coding_source
    knowledge = ModelCodingKnowledge(tmp_path, task=GENERATED)
    response = '{"definition":{"nodes":[]}}'
    record = knowledge.record(candidate_id="model_0001_attempt_0", source=response,
        evidence={"model_status": "fail", "diagnostics": [{"stage": "model_validation", "error_code": "model.definition_invalid"}]})
    receipt = tmp_path / "rounds/0001/compile-00.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"status": "fail", "response": response, "error": "生成网络需要1至8个节点"}), encoding="utf-8")
    validate_coding_source(tmp_path, record)
    receipt.write_text(json.dumps({"status": "fail", "response": "另一响应", "error": "结构错误"}), encoding="utf-8")
    with pytest.raises(ValueError, match="真实编译诊断"):
        validate_coding_source(tmp_path, record)


@pytest.fixture
def formal_network_context(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace
    source_root = tmp_path / "source"
    execution = tmp_path / "execution"
    result_root = execution / "results/formal-result"
    result_root.mkdir(parents=True)
    plan = execution / "plan/admitted/research-plan.json"
    plan.parent.mkdir(parents=True)
    plan.write_text(json.dumps({"package_hash": "package", "package_plan_hash": "plan"}), encoding="utf-8")
    config = {"candidate": {"model": {"class": "GeneratedModel"}}, "generated": {"definition": DEFINITION, "source_path": "fold/network.py"}}
    model = {"mode": "walk_forward_development_v1", "design": {"mode": "development"},
             "table_bindings": {"models": "model.models.v1"}, "model_configs": {"fold/config.json": config}}
    facts = {"model_diagnostics": model}
    support_bytes = {"validity.json": json.dumps(facts).encode(), "support/config.json": json.dumps(config).encode(), "support/network.py": SOURCE.encode()}
    bundle = SimpleNamespace(project_id="project", run_id="run", result_id="result", package_hash="package", plan_hash="plan",
        verification=SimpleNamespace(validity_source_path="validity.json"), tables=[SimpleNamespace(schema_id="model.models.v1", artifact_key="models")],
        support_files=[SimpleNamespace(artifact_key="models", source_path="fold/config.json", relative_path="support/config.json"),
                       SimpleNamespace(artifact_key="models", source_path="fold/network.py", relative_path="support/network.py")])
    snapshot = SimpleNamespace(bundle=bundle, support_bytes=support_bytes)
    context = SimpleNamespace(verification=SimpleNamespace(status="pass"), snapshot=snapshot)
    class Store:
        def __init__(self, *args, **kwargs):
            pass
        def result_directory(self, value):
            assert value is bundle
            return result_root
        def load_snapshot_by_identity(self, **kwargs):
            assert kwargs["support_paths"] == ("support/config.json", "support/network.py")
            return snapshot
    monkeypatch.setitem(sys.modules, "research_pipeline.evidence", SimpleNamespace(load_verified_result_context=lambda *args, **kwargs: context))
    monkeypatch.setitem(sys.modules, "research_pipeline.results", SimpleNamespace(ResultStore=Store))
    knowledge = ModelCodingKnowledge(source_root, task=GENERATED)
    source_file = source_root / "rounds/0000/verified-network.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text(SOURCE, encoding="utf-8")
    metric = {"result_id": "result", "result_ref": str(result_root), "verification_ref": str(execution / "verification/result.json")}
    feedback = {**evidence(), "execution_ref": str(execution), **{key: metric[key] for key in ("result_ref", "verification_ref")}, "bundle_ref": str(source_file)}
    record = knowledge.record(candidate_id="model_0000", source=SOURCE, task={**GENERATED, "definition": DEFINITION}, evidence=feedback)
    return SimpleNamespace(root=source_root, execution=execution, result_root=result_root, plan=plan, facts=facts, model=model,
        config=config, context=context, snapshot=snapshot, source_file=source_file, record=record, metric=metric, knowledge=knowledge)


def test_generated_source_validator_binds_formal_context_and_sealed_network(formal_network_context):
    from quantwitness_rdagent.model_knowledge import validate_generated_model_source
    fixture = formal_network_context
    assert validate_generated_model_source(fixture.root, fixture.record) == {"result_id": "result"}


@pytest.mark.parametrize("change", ["verification", "result_ref", "plan", "definition", "source", "sealed_source", "holdout"])
def test_generated_source_validator_rejects_binding_failures(formal_network_context, change):
    from quantwitness_rdagent.model_knowledge import validate_generated_model_source
    fixture = formal_network_context
    if change == "verification":
        fixture.context.verification.status = "fail"
    elif change == "result_ref":
        fixture.result_root = fixture.execution / "other"
        fixture.record["formal_refs"]["result_ref"] = str(fixture.result_root)
        path = fixture.knowledge.root / "records/model_0000.json"
        path.write_text(json.dumps(fixture.record), encoding="utf-8")
    elif change == "plan":
        fixture.plan.write_text(json.dumps({"package_hash": "other", "package_plan_hash": "plan"}), encoding="utf-8")
    elif change == "definition":
        fixture.config["generated"]["definition"] = {"nodes": []}
        fixture.snapshot.support_bytes["validity.json"] = json.dumps(fixture.facts).encode()
    elif change == "source":
        fixture.source_file.write_text("另一源码", encoding="utf-8")
    elif change == "sealed_source":
        fixture.snapshot.support_bytes["support/network.py"] = b"other source"
    else:
        fixture.model["mode"] = "walk_forward_prediction_v1"
        fixture.snapshot.support_bytes["validity.json"] = json.dumps(fixture.facts).encode()
    with pytest.raises(ValueError):
        validate_generated_model_source(fixture.root, fixture.record)


def test_model_research_success_copies_actual_formal_source(formal_network_context):
    from types import SimpleNamespace
    from quantwitness_rdagent.model_research_evidence import record_coding_success
    fixture = formal_network_context
    reference = fixture.root / "experiments/model_0001/candidates/model_0001/execution-ref.json"
    reference.parent.mkdir(parents=True)
    reference.write_text(json.dumps({"execution_root": str(fixture.execution)}), encoding="utf-8")
    session = SimpleNamespace(root=fixture.root, knowledge=fixture.knowledge)
    proposal = {"candidate_id": "model_0001", "round": 1, "definition": DEFINITION}
    record = record_coding_success(session, proposal, {"metrics": {**fixture.metric, "value": 0.91, "holdout_auc": 0.98}})
    assert record["source"] == SOURCE
    assert record["task"]["definition"] == DEFINITION
    assert "value" not in json.dumps(record) and "holdout_auc" not in json.dumps(record)
    assert Path(record["formal_refs"]["bundle_ref"]).read_text(encoding="utf-8") == SOURCE


@pytest.mark.parametrize("broken_node", [
    {"inputs": [0], "width": 1, "activation": "identity"},
    {"inputs": [[]], "width": 1, "activation": "identity"},
    {"inputs": [-1], "width": 1, "activation": {}},
], ids=["future_input", "nested_input_type", "activation_type"])
def test_model_repair_prompt_consumes_actual_current_compile_record(tmp_path, monkeypatch, broken_node):
    from test_model_research import session, GENERATED as repaired_definition
    from quantwitness_rdagent import native_model_research
    value = session(tmp_path)
    value.knowledge = ModelCodingKnowledge(tmp_path, task=GENERATED)
    response = json.dumps({"definition": {"nodes": [broken_node]}})
    monkeypatch.setattr(native_model_research, "propose_model", lambda *args: {"hypothesis": "跳连", "reason": "结构依据", "response": response})
    proposal = value.propose(1)
    assert proposal["definition"] == repaired_definition
    calls = list((tmp_path / "calls").glob("*.json"))
    prompt = json.loads(calls[0].read_text(encoding="utf-8"))["request"]["prompt"]
    payload = json.loads(prompt)
    records = payload["coding_knowledge"]
    receipt = json.loads((tmp_path / "rounds/0001/compile-00.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "fail" and receipt["response"] == response
    assert payload["technical_error"] == receipt["error"]
    assert len(calls) == 1
    assert records[0]["source"] == response
    assert records[0]["match_reason"]["kind"] == "current_failure"
    assert "model.definition_invalid" in prompt and "ValueError" in prompt
    assert "formal_refs" not in records[0]
