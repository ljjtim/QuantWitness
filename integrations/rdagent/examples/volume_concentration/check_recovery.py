"""核对已完成的公开合成案例，并从编码前快照恢复同一候选。"""
import argparse
import asyncio
import json
import os
from pathlib import Path
from unittest.mock import patch

from quantwitness_rdagent.contracts import FrozenRequest
from quantwitness_rdagent.feedback import acceptable


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def check(request_file, output):
    request = FrozenRequest.load(request_file)
    if request.payload["development_scope"].get("synthetic") is not True:
        raise ValueError("此检查仅适用于明确的合成教学请求")
    if request.payload["budget"]["live_llm_calls"] != 0 or "code_generation" in request.payload:
        raise ValueError("合成恢复检查不接受实时模型调用")
    if Path(output).exists():
        raise FileExistsError(output)
    root = request.session_root
    if read(root / "request.json") != request.payload:
        raise ValueError("检查请求与原冻结会话不一致")
    outcome = read(root / "outcome.json")
    bad = read(root / "candidates/candidate-0000/feedback.json")
    if bad["execution_status"] != "succeeded" or bad["formula_status"] != "fail" or bad["verification_status"] != "fail":
        raise ValueError("错误公式未被正式独立验证拒绝")
    if not acceptable(outcome) or outcome["candidate_id"] != "candidate-0001":
        raise ValueError("第二候选未完成正式公式验收")
    coverage = outcome["formula_coverage"]
    if coverage["entity_sessions"] != 10 or coverage["raw_rows"] != 2400 or len(coverage["entities"]) != 2:
        raise ValueError("正式覆盖与两证券五session不符")
    attempts_path = root / "coder-attempts.json"
    attempts = attempts_path.read_bytes()
    if len(json.loads(attempts)) != 2 or outcome["allowed_metrics"] != {}:
        raise ValueError("候选次数或技术反馈范围不符")
    candidate = root / "candidates/candidate-0001"
    execution = request._local_path(outcome["execution_ref"])
    result = request._local_path(outcome["result_ref"])
    def files():
        return {path for folder in (execution / "run", result) for path in folder.rglob("*") if path.is_file()}

    originals = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in files()}

    os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
    from rdagent.core.conf import RD_AGENT_SETTINGS
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger
    RD_AGENT_SETTINGS.workspace_path = root / "rd-workspace"
    RD_AGENT_SETTINGS.artifact_signing_key_path = root / "rd-signing.key"
    LOG_SETTINGS.trace_path = str(root / "rd-logs")
    rdagent_logger.set_storages_path(root / "rd-logs")
    from rdagent.oai.backend.base import APIBackend
    from quantwitness_rdagent.loop import RPLoop

    def forbidden(*args, **kwargs):
        raise AssertionError("离线恢复不得建立实时模型客户端或数据库连接")

    with patch.object(APIBackend, "__init__", forbidden), patch("sqlite3.connect", forbidden):
        from quantwitness_rdagent.__main__ import main as cli_main
        with patch("sys.argv", ["quantwitness_rdagent", "resume", "--request", str(request_file)]):
            cli_main()
        latest = RPLoop.load(root / "rd-logs/__session__", checkout=False)
        if latest.step_idx[0] != 5:
            raise ValueError("RD缺少完成阶段快照")
        restored = RPLoop.load(root / "rd-logs/__session__/0/0_propose", checkout=False)
        if restored.request.payload != request.payload or latest.request.payload != request.payload:
            raise ValueError("恢复快照与检查请求不一致")
        if restored.step_idx[0] != 1:
            raise ValueError("编码前快照阶段不符")
        asyncio.run(restored._run_step(0))
        asyncio.run(restored._run_step(0))
        if not acceptable(restored.loop_prev_out[0]["running"]):
            raise ValueError("快照恢复没有复用成功候选")
    if attempts_path.read_bytes() != attempts:
        raise ValueError("恢复新增了编码尝试")
    if len(list((candidate / "workspace/.research/executions").glob("*/execution.json"))) != 1:
        raise ValueError("恢复新增了正式execution")
    if files() != set(originals):
        raise ValueError("恢复新增或移除了运行及Result文件")
    if any(not path.is_file() or path.stat().st_mtime_ns != stamp or path.read_bytes() != content
           for path, (stamp, content) in originals.items()):
        raise ValueError("恢复改变了原运行或Result")
    record = {
        "status": "pass", "scope": "public_synthetic_second_formula", "live_model_calls": 0,
        "candidate_count": 2, "failed_candidate_formula_status": bad["formula_status"],
        "successful_candidate": outcome["candidate_id"], "verification_status": outcome["verification_status"],
        "formula_coverage": coverage, "execution_ref": outcome["execution_ref"],
        "result_ref": outcome["result_ref"], "verification_ref": outcome["verification_ref"],
        "public_resume_passed": True, "restored_from": "propose", "runtime_and_result_files_unchanged": True,
        "checked_files": len(originals), "successful_execution_count": 1,
    }
    with Path(output).open("x", encoding="utf-8") as stream:
        json.dump(record, stream, ensure_ascii=False, indent=2)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.request, args.output), ensure_ascii=False))


if __name__ == "__main__":
    main()
