"""摘要、输出和异常投影使用内存载荷，不连接数据库或执行研究。"""

from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from research_pipeline.cli import result as cli_result
from research_pipeline.cli.summary import build_summary
from research_pipeline.platform import MainlineError


def arguments(command="run", **kwargs):
    return SimpleNamespace(command=command, json=False, summary=False, **kwargs)


@pytest.mark.parametrize("command", ["run", "resume", "retry-node", "rerun-from"])
@pytest.mark.parametrize("workspace", [False, True])
def test_run_and_recovery_require_finalized_result(command, workspace):
    args = arguments("workspace", workspace_command=command) if workspace else arguments(command)
    finalized = build_summary(args, status="pass", data={"status": "result_finalized"})
    assert finalized["execution_status"] == "succeeded"
    assert finalized["verification_status"] is None
    unfinished = build_summary(args, status="pass", data={"status": "succeeded"})
    assert unfinished["execution_status"] == "finalize_pending"
    assert build_summary(args, status="fail", data=None)["execution_status"] == "failed"


@pytest.mark.parametrize(
    "run_status,finalize_status,expected",
    [
        ("not_started", "unknown", "not_started"),
        ("running", "pending", "running"),
        ("failed", "unknown", "failed"),
        ("paused", "unknown", "paused"),
        ("succeeded", "unknown", "finalize_pending"),
        ("succeeded", "pending", "finalize_pending"),
        ("succeeded", "failed", "finalize_failed"),
        ("succeeded", "succeeded", "succeeded"),
    ],
)
def test_inspect_distinguishes_runtime_and_finalize(run_status, finalize_status, expected):
    summary = build_summary(arguments("inspect"), status="pass", data={
        "run_status": run_status, "finalize": {"status": finalize_status},
    })
    assert summary["command_status"] == "pass"
    assert summary["execution_status"] == expected
    assert summary["verification_status"] is None


def test_inspect_keeps_published_result_and_finalize_error():
    error = {"error_code": "io_error", "message": "封存后续记录失败"}
    summary = build_summary(arguments("inspect"), status="pass", data={
        "run_status": "succeeded", "finalize": {
            "status": "failed", "result_published": True,
            "result_id": "r1", "result_directory": "I:/results/r1", "error": error,
        },
    })
    assert summary["execution_status"] == "finalize_failed"
    assert summary["result_directory"] == "I:/results/r1"
    assert summary["finalize_error"] == error


@pytest.mark.parametrize("verdict", ["pass", "fail", None])
def test_verify_reads_verdict_instead_of_exit_code(capfd, verdict):
    args = arguments("verify")
    args.json = True
    data = {} if verdict is None else {"status": verdict}
    assert cli_result.emit(args, status="pass", data=data, code=0) == 0
    payload = json.loads(capfd.readouterr().out)
    assert payload["status"] == "pass"
    assert payload["summary"]["verification_status"] == verdict
    assert payload["summary"]["execution_status"] is None


def test_non_execution_command_has_null_execution_status():
    summary = build_summary(arguments("package"), status="pass", data={"status": "pass"})
    assert summary == {"command_status": "pass", "execution_status": None, "verification_status": None}


def test_full_json_preserves_envelope_and_original_data(capfd):
    args = arguments()
    args.json = True
    data = {"status": "result_finalized", "private_detail": {"large": [1, 2, 3]}}
    before = deepcopy(data)
    cli_result.emit(args, status="pass", data=data, code=0)
    payload = json.loads(capfd.readouterr().out)
    assert set(payload) == {"contract_version", "status", "error_code", "message", "data", "summary"}
    assert payload["data"] == before == data
    assert payload["error_code"] is None
    assert payload["message"] is None
    assert "private_detail" not in payload["summary"]


def test_summary_json_keeps_all_lint_issues_and_recovery_inputs(capfd):
    args = arguments("package")
    args.summary = True
    issues = [
        {"code": "missing", "file": "sources.yaml", "field": "sources", "message": "缺少来源", "action": "填写来源"},
        {"code": "missing", "file": "spec.yaml", "field": "clock", "message": "缺少时点", "action": "填写时点"},
    ]
    data = {
        "issues": issues, "required_inputs": ["clock", "sources"],
        "next_command_argv": ["python", "-m", "research_pipeline", "package", "lint", "--help"],
        "large_detail": ["不可倾倒"],
    }
    cli_result.emit(args, status="fail", data=data, code=1, error_code="invalid", message="声明不完整")
    payload = json.loads(capfd.readouterr().out)
    assert set(payload) == {"contract_version", "summary", "error_code", "message", "required_inputs", "next_command_argv"}
    assert payload["summary"]["issues"] == issues
    assert payload["required_inputs"] == data["required_inputs"]
    assert payload["next_command_argv"] == data["next_command_argv"]
    assert "large_detail" not in payload["summary"]


def test_default_text_keeps_paths_issues_actions_and_quotes_command(capfd):
    data = {
        "result_directory": "I:/结果目录/r1", "status": "result_finalized",
        "issues": [{"code": "missing", "file": "spec.yaml", "field": "clock", "message": "缺少时点", "action": "填写时点"}],
        "next_action": "补齐声明", "required_inputs": ["clock"],
        "next_command_argv": ["python", "-m", "research_pipeline", "inspect", "--run-root", "I:/研究 空格"],
        "large_detail": "不可倾倒的明细",
    }
    cli_result.emit(arguments(), status="pass", data=data, code=0)
    output = capfd.readouterr().out
    for expected in ("执行状态: succeeded", "I:/结果目录/r1", "缺少时点", "填写时点", "补齐声明", "需要补充", "'I:/研究 空格'"):
        assert expected in output
    assert "不可倾倒的明细" not in output


def test_workspace_verification_failure_keeps_completed_execution_and_report(capfd):
    args = arguments("workspace", workspace_command="execute")
    args.summary = True
    stages = {"lint": "pass", "run": "pass", "verify": "fail", "report": "pass"}
    data = {
        "stages": stages, "execution_id": "e1", "execution_root": "I:/执行/e1",
        "execution_status": "succeeded", "result_id": "r1", "result_directory": "I:/执行/e1/results/r1",
        "verification_status": "fail", "report_path": "I:/执行/e1/report.md", "failed_stage": "verify",
        "resource_advice": [{"status": "configuration_insufficient", "message": "建议声明 3 槽"}],
    }

    def fail(_args):
        error = MainlineError("验证未通过")
        error.error_code = "workspace_verification_failed"
        error.failure_payload = data
        raise error

    assert cli_result.execute_guarded(args, fail) == 1
    payload = json.loads(capfd.readouterr().out)
    summary = payload["summary"]
    assert summary["command_status"] == "fail"
    assert summary["execution_status"] == "succeeded"
    assert summary["verification_status"] == "fail"
    for key in ("report_path", "result_directory", "failed_stage", "resource_advice", "stages"):
        assert summary[key] == data[key]


def test_workspace_lint_failure_does_not_mark_unexecuted_stages_success():
    summary = build_summary(arguments("workspace", workspace_command="execute"), status="fail", data={
        "stages": {"lint": "fail", "run": "not_run", "verify": "not_run"},
        "failed_stage": "lint", "execution_status": None, "verification_status": None,
        "issues": [{"message": "缺少来源"}],
    })
    assert summary["execution_status"] is None
    assert summary["verification_status"] is None
    assert summary["issues"] == [{"message": "缺少来源"}]


def test_execute_guarded_preserves_resource_failure_and_root_diagnostic(monkeypatch, capfd):
    args = arguments(run_root="I:/run")
    args.summary = True
    data = {"exceeded": {"process_slots": {"actual": 4, "limit": 2}}}
    monkeypatch.setattr(cli_result, "_runtime_failure_details", lambda *_: {
        "failed_node": "analysis", "root_error": {"error_code": "resource_exceeded", "message": "进程超限"},
    })

    def fail(_args):
        error = MainlineError("节点执行失败")
        error.error_code = "resource_exceeded"
        error.failure_payload = data
        raise error

    assert cli_result.execute_guarded(args, fail) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["summary"]["failed_node"] == "analysis"
    assert payload["summary"]["root_error"]["message"] == "进程超限"
    advice = payload["summary"]["resource_advice"][0]
    assert advice["status"] == "measured_exceeded"
    assert advice["exceeded"]["process_slots"] == {"actual": 4, "limit": 2, "suggested_limit": 4}
    assert payload["next_command_argv"][-3:] == ["--run-root", "I:/run", "--json"]
    assert data == {"exceeded": {"process_slots": {"actual": 4, "limit": 2}}}


def test_execute_guarded_success_and_plain_error(capfd):
    args = arguments("package")
    args.json = True
    assert cli_result.execute_guarded(args, lambda _: {"package_path": "I:/pkg"}) == 0
    assert json.loads(capfd.readouterr().out)["summary"]["package_path"] == "I:/pkg"

    def fail(_args):
        raise ValueError("参数缺失")

    assert cli_result.execute_guarded(args, fail) == 1
    payload = json.loads(capfd.readouterr().out)
    assert payload["data"] is None
    assert payload["error_code"] == "value_error"
    assert payload["summary"]["execution_status"] is None


def test_report_without_output_keeps_requested_report_body(capfd):
    cli_result.emit(arguments("report"), status="pass", code=0, data={"report": "研究报告正文"})
    assert "研究报告正文" in capfd.readouterr().out


@pytest.mark.parametrize("command", ["operator", "artifact", "recipe", "catalog", "capabilities"])
def test_discovery_text_keeps_requested_items(command, capfd):
    data = {"items": [{"id": "example", "fields": [{"name": "close", "unit": "元"}]}]}
    assert cli_result.execute_guarded(arguments(command, format="text"), lambda _: data) == 0
    output = capfd.readouterr().out
    assert '"items"' in output
    assert '"close"' in output
    assert "元" in output


def test_catalog_docs_text_keeps_documentation_and_counts(capfd):
    data = {"markdown": "# 字段说明\n可见日期", "output": "I:/目录.md", "dataset_count": 3}
    cli_result.emit(arguments("catalog", catalog_command="docs"), status="pass", code=0, data=data)
    output = capfd.readouterr().out
    assert "字段说明" in output
    assert '"dataset_count": 3' in output
    assert "I:/目录.md" in output


def test_workspace_execute_explicit_null_is_not_inferred_from_stage():
    summary = build_summary(arguments("workspace", workspace_command="execute"), status="fail", data={
        "execution_status": None, "stages": {"run": "fail"},
    })
    assert summary["execution_status"] is None


def test_run_success_requires_result_finalized_even_with_runtime_fields():
    summary = build_summary(arguments("run"), status="pass", data={
        "run_status": "succeeded", "finalize": {"status": "succeeded"},
    })
    assert summary["execution_status"] == "finalize_pending"


def test_resource_advice_text_identifies_budget_parameter(capfd):
    data = {
        "resource_advice": [{
            "purpose": "verifier", "status": "configuration_insufficient",
            "parameter": "--verification-process-slots", "message": "建议声明 3 槽",
        }],
        "exceeded": {"process_slots": {"actual": 4, "limit": 2}},
    }
    cli_result.emit(arguments("verify"), status="fail", data=data, code=1, error_code="resource_exceeded")
    output = capfd.readouterr().out
    assert "--verification-process-slots" in output
    assert "actual=4, limit=2, 建议=4" in output
    assert "建议声明 3 槽" in output


@pytest.mark.parametrize("mode", ["text", "summary", "json"])
def test_inspect_node_errors_keep_reason_context_and_recovery_without_normal_nodes(mode, capfd):
    args = arguments("inspect")
    args.summary = mode == "summary"
    args.json = mode == "json"
    context = {
        "contract_version": "data-plane-request-failure-v1",
        "request_status": "opened", "request_id": "prices",
        "dataset_id": "daily_prices", "binding_id": "local_prices",
        "object_name": "prices_table", "provider": "duckdb",
        "output_budget": {"max_rows": 100, "max_bytes": 4096, "batch_size": 10},
        "execution_budget": {"memory_bytes": 2048, "temp_bytes": 1024, "cpu_slots": 1},
        "completed_request_ids": [], "underlying_exception_type": "MemoryError",
    }
    data = {
        "run_status": "paused", "finalize": {"status": "unknown"},
        "nodes": {
            "normal_node": {"status": "succeeded", "last_error": None, "checkpoint": {"large": "正常节点细节"}},
            "prices": {
                "status": "retryable_failed", "attempts_remaining": 1,
                "last_error": {
                    "error_code": "data_plane_memory_exceeded", "exception_type": "MemoryError",
                    "message": "价格请求内存超限", "failure_context": context,
                },
            },
            "unstarted_node": {"status": "not_started", "last_error": None},
        },
        "recommended_action": "retry-node", "recommendation_reason": "本节点仍有重试次数",
        "required_inputs": [],
        "next_command_argv": ["python", "-m", "research_pipeline", "retry-node", "--run-root", "I:/研究 运行", "--node", "prices"],
    }
    before = deepcopy(data)
    assert cli_result.execute_guarded(args, lambda _: data) == 0
    output = capfd.readouterr().out
    assert data == before
    if mode == "text":
        for expected in ("prices", "retryable_failed", "data_plane_memory_exceeded", "MemoryError",
                         "价格请求内存超限", "prices_table", "max_bytes", "retry-node", "本节点仍有重试次数"):
            assert expected in output
        assert "'I:/研究 运行'" in output
    else:
        payload = json.loads(output)
        assert payload["summary"]["node_errors"] == [{
            "node_id": "prices", "status": "retryable_failed",
            "error_code": "data_plane_memory_exceeded", "exception_type": "MemoryError",
            "message": "价格请求内存超限", "failure_context": context,
        }]
        assert payload["summary"]["recommended_action"] == "retry-node"
        if mode == "json":
            assert payload["data"] == before
        else:
            assert "data" not in payload
            assert payload["next_command_argv"] == data["next_command_argv"]
            assert payload["required_inputs"] == []
    if mode != "json":
        assert "normal_node" not in output
        assert "unstarted_node" not in output
        assert "正常节点细节" not in output


def test_inspect_node_errors_without_context_and_empty_errors():
    data = {
        "run_status": "paused", "nodes": {
            "interrupted": {"status": "running", "last_error": {
                "error_code": "runtime_attempt_interrupted", "exception_type": "RuntimeInterruption",
                "message": "节点未形成终态",
            }},
        },
    }
    errors = build_summary(arguments("inspect"), status="pass", data=data)["node_errors"]
    assert errors == [{
        "node_id": "interrupted", "status": "running", "error_code": "runtime_attempt_interrupted",
        "exception_type": "RuntimeInterruption", "message": "节点未形成终态",
    }]
    assert build_summary(arguments("inspect"), status="pass", data={"nodes": {}})["node_errors"] == []


@pytest.mark.parametrize("single_execution", [False, True])
def test_workspace_inspect_default_text_keeps_execution_navigation(single_execution, capfd):
    entry = {
        "execution_id": "execution-001", "status": "allocated",
        "execution_path": "generated/executions/execution-001", "label": "研究一",
        "clock": "2026-09-30T09:00:00+08:00", "root_seed": 17,
    }
    data = entry if single_execution else {"workspace_id": "research-workspace", "executions": [entry]}
    before = deepcopy(data)
    args = arguments("workspace", workspace_command="inspect", execution="execution-001" if single_execution else None)
    assert cli_result.execute_guarded(args, lambda _: data) == 0
    output = capfd.readouterr().out
    for expected in ("execution-001", "generated/executions/execution-001", "研究一", "allocated", '"root_seed": 17'):
        assert expected in output
    if not single_execution:
        assert '"executions"' in output
        assert "research-workspace" in output
    assert data == before
