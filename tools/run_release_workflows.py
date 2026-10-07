"""在安装态执行公开合成工作流，生成 Gate A、I/B 与 Gate C 输入回执。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


PROTOCOL_VERSION = "research-release-workflow-protocol-v1"
EVIDENCE_VERSION = "research-release-workflow-evidence-v1"
RECEIPT_VERSION = "research-release-workflow-receipt-v1"
PROJECTS = ("equity_cross_section", "etf_time_series", "event_study", "futures_term_structure")
CHECKPOINTS = {
    "admission": "platform.catalog.admission",
    "data_plane": "data_plane",
    "feature": "project_analysis",
}
SCOPE_DESCRIPTION = "确定性 CLI 与合成数据验收，不证明真人或 LLM 的研究能力，也不代表真实市场验收。"


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON 顶层必须是对象: {path}")
    return value


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def require(condition: object, message: str) -> None:
    if not condition:
        raise ValueError(message)


def database_state(path: Path) -> dict:
    stat = path.stat()
    return {"size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "wal_exists": Path(str(path) + ".wal").exists()}


def database_evidence(path: Path, before: dict) -> dict:
    after = database_state(path)
    unchanged = before == after and not before["wal_exists"] and not after["wal_exists"]
    return {"database": str(path), "before": before, "after": after,
            "unchanged": unchanged, "status": "pass" if unchanged else "fail"}


class Commands:
    """每次子进程调用保留输出、退出码和实际 cwd。"""

    def __init__(self, python: Path, output: Path, timeout: float = 300):
        self.python = python
        self.cwd = output / "cwd"
        self.cwd.mkdir()
        self.logs = output / "commands"
        self.logs.mkdir()
        self.scratch = output / "tmp"
        self.scratch.mkdir()
        self.timeout = timeout
        self.records: list[dict] = []
        self.environment = dict(os.environ)
        for name in ("PYTHONPATH", "PYTHONHOME"):
            self.environment.pop(name, None)
        self.environment.update(PYTHONUTF8="1", PYTHONIOENCODING="utf-8",
                                PYTHONDONTWRITEBYTECODE="1", TEMP=str(self.scratch), TMP=str(self.scratch))

    def run(self, label: str, arguments: list, *, expected: int = 0, parse: bool = True) -> dict:
        argv = [str(self.python), "-I", "-B", "-X", "utf8", *map(str, arguments)]
        started = time.monotonic()
        record = {"label": label, "argv": argv, "cwd": str(self.cwd), "expected_exit": expected}
        try:
            completed = subprocess.run(argv, cwd=self.cwd, env=self.environment,
                                       capture_output=True, text=True, encoding="utf-8",
                                       errors="replace", timeout=self.timeout, check=False)
            record.update(exit_code=completed.returncode, stdout=completed.stdout, stderr=completed.stderr)
        except subprocess.TimeoutExpired as exc:
            def text(value):
                return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value or ""
            record.update(exit_code=None, timed_out=True, stdout=text(exc.stdout), stderr=text(exc.stderr))
        except OSError as exc:
            record.update(exit_code=None, stdout="", stderr=str(exc))
        record["seconds"] = time.monotonic() - started
        path = self.logs / f"{len(self.records) + 1:03d}-{label}.json"
        write_json(path, record)
        self.records.append({"label": label, "path": str(path), "exit_code": record["exit_code"]})
        require(record["exit_code"] == expected, f"{label} 退出码不符，详见 {path}")
        if not parse:
            return record
        try:
            payload = json.loads(record["stdout"])
        except json.JSONDecodeError as exc:
            raise ValueError(f"{label} 未返回机器 JSON，详见 {path}") from exc
        require(isinstance(payload, dict), f"{label} 未返回 JSON 对象")
        if expected == 0:
            if arguments[:3] == ["-m", "research_pipeline", "capabilities"]:
                require(payload.get("contract_version") == "research-capability-discovery-v1"
                        and payload.get("capabilities"), "能力发现返回空清单或错误合同")
            else:
                require(payload.get("status") == "pass", f"{label} 未明确通过，详见 {path}")
        return payload

    def cli(self, label: str, arguments: list, *, expected: int = 0) -> dict:
        return self.run(label, ["-m", "research_pipeline", *arguments], expected=expected)

    def worker(self, label: str, action: str, *arguments, expected: int = 0, parse: bool = True) -> dict:
        return self.run(label, [str(Path(__file__).resolve()), "_worker", action, *arguments],
                        expected=expected, parse=parse)


def validate_installation(payload: dict, project: Path) -> dict:
    prefix = Path(payload["prefix"]).resolve()
    require(prefix != Path(payload["base_prefix"]).resolve(), "--python 必须指向安装态 venv")
    for name, raw in payload["imports"].items():
        path = Path(raw).resolve()
        require(path.is_relative_to(prefix) and not path.is_relative_to(project),
                f"{name} 导入来源不属于安装态 venv: {path}")
    require(payload["imports"], "安装态导入来源为空")
    require(not payload.get("editable"), "不接受 editable 安装")
    return payload


def require_verification(path: Path) -> dict:
    payload = read_json(path)
    require(payload.get("status") == "pass" and payload.get("validity_status") == "pass",
            f"独立 VerificationResult 未通过: {path}")
    return {"verification_result": str(path), "status": payload["status"],
            "validity_status": payload["validity_status"], "verification_hash": payload.get("verification_hash")}


def require_workflow(payload: dict) -> dict:
    data = payload["data"]
    require(data.get("execution_status") == "succeeded" and data.get("verification_status") == "pass",
            "workspace execute 没有完成运行与独立验证")
    require(all(data.get("stages", {}).get(name) == "pass"
                for name in ("lint", "prepare", "allocate", "admit", "run", "verify", "report")),
            "workspace execute 有未完成阶段")
    require_verification(Path(data["verification_result"]))
    require(Path(data["report_path"]).is_file(), "workspace 报告文件缺失")
    directory = Path(data["result_directory"])
    require({path.name for path in directory.parent.iterdir() if path.is_dir()} == {directory.name},
            "成功 Run 必须只有一个正式 Result")
    return data


def require_draft_rejection(payload: dict) -> dict:
    data = payload.get("data", {})
    issues = data.get("issues", [])
    require(payload.get("status") == "fail" and payload.get("error_code") == "research_package_invalid",
            "中性草稿未按研究包诊断拒绝")
    require(data.get("execution_ready") is False and issues, "中性草稿缺少明确未准入诊断")
    for issue in issues:
        require(all(isinstance(issue.get(key), str) and issue[key]
                    for key in ("code", "file", "field", "message", "action")), "草稿诊断字段不完整")
    required = {("package.yaml", "package_slug"), ("sources/sources.yaml", "sources"),
                ("spec/research.yaml", "requests")}
    require(required <= {(issue["file"], issue["field"]) for issue in issues}, "中性草稿缺少研究事实诊断")
    return {"error_code": payload["error_code"], "issues": issues, "execution_ready": False}


def discover(commands: Commands, output: Path) -> dict:
    capabilities = commands.cli("capabilities", ["capabilities", "--format", "json"])
    operators = commands.cli("operator-list", ["operator", "list", "--format", "json"])
    items = operators["data"]["items"]
    require(items, "公开算子发现为空")
    operator_id = items[0]["operator_id"]
    described = commands.cli("operator-describe", ["operator", "describe", operator_id, "--format", "json"])
    draft = output / "neutral-draft"
    commands.cli("package-init", ["package", "init", draft, "--json"])
    first = require_draft_rejection(commands.cli("draft-lint", ["package", "lint", "--package", draft, "--json"], expected=1))
    second = require_draft_rejection(commands.cli("draft-lint-repeat", ["package", "lint", "--package", draft, "--json"], expected=1))
    require(first == second, "中性草稿重复 lint 的诊断不稳定")
    return {"capabilities": capabilities, "operator": described, "draft": str(draft), "diagnostics": first}


def workspace_flow(project: dict, environment: dict, workspace: Path, slots: int) -> list:
    return ["workspace", "execute", "--workspace", workspace,
            "--catalog-lock", environment["catalog_lock"], "--data-db", environment["database"],
            "--extension-bundle", project["operator_bundle"], "--verifier-bundle", project["verifier_bundle"],
            "--verification-process-slots", str(slots), "--json"]



def require_reused_checkpoints(runtime: dict, checkpoint: str) -> list[str]:
    nodes = list(CHECKPOINTS.values())
    required = nodes[:list(CHECKPOINTS).index(checkpoint) + 1]
    reused = runtime.get("reused_nodes", [])
    require(runtime.get("status") == "succeeded" and set(required) <= set(reused),
            f"恢复没有复用必需 checkpoint: {required}")
    return reused

def recover(commands: Commands, project: dict, environment: dict, baseline: dict,
            output: Path, slots: int) -> dict:
    recovered = {}
    workspace = Path(baseline["workspace"])
    for checkpoint, node_id in CHECKPOINTS.items():
        receipt = output / f"interruption-{checkpoint}.json"
        commands.worker(f"interrupt-{checkpoint}", "interrupt", node_id, receipt,
                        *workspace_flow(project, environment, workspace, slots), expected=95, parse=False)
        interrupted = read_json(receipt)
        require(interrupted.get("node_id") == node_id and interrupted.get("phase") == "committed_before_event",
                f"{checkpoint} 未在已提交 checkpoint 上中断")
        run_root = Path(interrupted["run_root"])
        require(Path(interrupted["committed_marker"]).is_file(), "中断回执缺少 COMMITTED")
        inspected = commands.cli(f"inspect-{checkpoint}", ["inspect", "--run-root", run_root, "--json"])
        require(inspected["summary"].get("recommended_action") == "resume", "中断后没有建议原 resume")
        execution = run_root.parent
        resumed = commands.cli(f"resume-{checkpoint}", ["workspace", "resume", "--workspace", workspace,
                                                         "--execution", execution.name, "--json"])
        require(resumed["summary"].get("execution_status") == "succeeded", "恢复没有成功结束")
        runtime = read_json(run_root / "operator-dag-run.json")
        reused = require_reused_checkpoints(runtime, checkpoint)
        result = resumed["data"]["result_directory"]
        store = execution / "results"
        verification = execution / "verification" / "result.json"
        commands.cli(f"verify-{checkpoint}", ["verify", "--result", result, "--result-store", store,
                     "--verifier-bundle", project["verifier_bundle"], "--output", verification,
                     "--verification-process-slots", str(slots), "--verification-scratch-root", commands.scratch, "--json"])
        verified = require_verification(verification)
        report = execution / "report.md"
        commands.cli(f"report-{checkpoint}", ["report", "--verification-result", verification,
                     "--result-store", store, "--output", report, "--json"])
        comparison = commands.worker(f"compare-{checkpoint}", "compare", baseline["result_directory"], result)
        recovered[checkpoint] = {"node_id": node_id, "interruption": interrupted,
            "result_directory": result, "result_store": str(store), **verified,
            "report": str(report), "tables": comparison["tables"],
            "runtime_record": str(run_root / "operator-dag-run.json"), "reused_nodes": reused}
    return recovered


def tamper(commands: Commands, baseline: dict, project: dict, output: Path, slots: int) -> dict:
    store = Path(baseline["result_store"])
    copied = output / "tampered-results"
    shutil.copytree(store, copied)
    result = copied / Path(baseline["result_directory"]).relative_to(store)
    manifest = read_json(result / "result.json")
    table = manifest["tables"][0]
    target = result / next(iter(table["files"]))
    with target.open("r+b") as handle:
        first = handle.read(1)
        require(first, "Result 表为空，无法执行篡改负例")
        handle.seek(0)
        handle.write(bytes([first[0] ^ 1]))
    verification = output / "tampered-verification.json"
    rejected = commands.cli("tamper-verify", ["verify", "--result", result, "--result-store", copied,
                 "--verifier-bundle", project["verifier_bundle"], "--output", verification,
                 "--verification-process-slots", str(slots), "--json"], expected=1)
    require(rejected.get("status") == "fail" and rejected.get("error_code") == "result_contract_invalid",
            "Result 篡改没有被完整性验证明确拒绝")
    require(not verification.exists() or read_json(verification).get("status") != "pass",
            "篡改 Result 获得了通过回执")
    return {"result_directory": str(result), "modified_table": table["table_id"],
            "error_code": rejected["error_code"], "message": rejected.get("message")}


def run_workflows(*, python: Path, project: Path, output: Path, release_candidate_id: str,
                  build_manifest: Path, verification_process_slots: int = 3, timeout: float = 300) -> dict:
    project, output, python = project.resolve(), output.resolve(), python.resolve()
    require(not output.exists(), "--output 必须是新目录")
    source_root = next((p for p in (project, *project.parents) if (p / ".git").exists()), project)
    require(not output.is_relative_to(source_root), "--output 与 CLI cwd 必须在源码仓库外")
    require(all((project / path).is_file() for path in
                ("pyproject.toml", "examples/build_bundles.py", "examples/prepare_synthetic_environment.py")),
            "--project 必须指向包含公开示例的完整源码项目")
    require(verification_process_slots > 0 and timeout > 0, "进程槽与命令超时必须大于零")
    output.mkdir(parents=True)
    commands = Commands(python, output, timeout)
    checks = {}
    receipt = {"contract_version": RECEIPT_VERSION, "protocol_version": PROTOCOL_VERSION,
               "status": "fail", "scope": "synthetic", "scope_description": SCOPE_DESCRIPTION,
               "projects": [], "python": str(python), "project": str(project),
               "build_manifest": str(build_manifest.resolve())}
    error = None
    database = before = None

    def check(name, action):
        try:
            value = action()
            checks[name] = {"status": "pass", "details": value}
            return value
        except Exception as exc:
            checks[name] = {"status": "fail", "error": str(exc)}
            raise

    try:
        # tools 目录不含框架包；所有研究代码只由显式 venv 子进程导入。
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from release_evidence_binding import release_evidence_binding
        from wheel_source_inventory import verify_installed_source_inventory

        def binding():
            require(bool(release_candidate_id) and build_manifest is not None, "必须提供候选 ID 与 BuildManifest")
            return release_evidence_binding(release_candidate_id=release_candidate_id,
                                           build_manifest_path=build_manifest, project=project)

        receipt.update(check("candidate_binding", binding))
        installation = commands.worker("installed-origin", "installation")
        check("installed_origin", lambda: validate_installation(installation, source_root))
        receipt["installation"] = installation
        check("installed_candidate_content", lambda: verify_installed_source_inventory(
            python=python, project=project, cwd=commands.cwd))
        check("discovery_and_neutral_draft", lambda: discover(commands, output))
        environment = check("synthetic_environment", lambda: commands.worker(
            "prepare-synthetic", "prepare", project, output / "environment"))
        database = Path(environment["database"])
        before = database_state(database)
        require(not before["wal_exists"], "生成后源数据库残留 WAL")
        receipt["environment"] = environment
        bundles = check("project_bundles", lambda: commands.worker("build-bundles", "bundles", project, output / "bundles"))
        require(tuple(item["name"] for item in bundles["projects"]) == PROJECTS, "公开示例清单不完整")
        for item in bundles["projects"]:
            name = item["name"]
            workspace = output / "workspaces" / name
            commands.cli(f"init-{name}", ["workspace", "init", workspace, "--from-package", item["package"], "--json"])
            data = check(f"workflow:{name}", lambda: require_workflow(commands.cli(
                f"execute-{name}", workspace_flow(item, environment, workspace, verification_process_slots))))
            plan_path = Path(data["plan_directory"]) / "operator-graph-plan.json"
            plan = read_json(plan_path)
            entry = {"name": name, "scope": "synthetic", "workspace": str(workspace),
                     "package": str(workspace / "package"), "plan": str(plan_path),
                     "result_id": data["result_id"], "graph_id": plan["recipe"]["graph_id"],
                     "result_directory": data["result_directory"], "result_store": data["result_store"],
                     "verification_result": data["verification_result"], "report": data["report_path"],
                     "operator_bundle": item["operator_bundle"], "verifier_bundle": item["verifier_bundle"],
                     "verifier_source": str(project / "examples" / name / "verifier/source/check.py"),
                     "data_range": [{"request_id": request["request_id"],
                                     "dataset_id": request["query"]["dataset_id"],
                                     "time_range": request["query"]["time_range"]}
                                    for request in plan["requests"]],
                     "database_evidence": database_evidence(database, before)}
            require(entry["database_evidence"]["unchanged"], "workspace 流程修改了源数据库")
            receipt["projects"].append(entry)
        equity = receipt["projects"][0]
        recovery = check("checkpoint_recovery", lambda: recover(commands, bundles["projects"][0], environment,
                          equity, output, verification_process_slots))
        receipt["recovery"] = recovery
        check("result_tamper_rejected", lambda: tamper(commands, equity, bundles["projects"][0], output, verification_process_slots))
        require(check("candidate_binding_after", binding) == checks["candidate_binding"]["details"], "验收期间候选身份改变")
    except Exception as exc:
        error = str(exc)
    finally:
        if database is not None and before is not None:
            try:
                evidence = database_evidence(database, before)
                checks["database_unchanged"] = evidence
                receipt["database_evidence"] = evidence
                for item in receipt["projects"]:
                    item["database_evidence"] = evidence
                if not evidence["unchanged"]:
                    error = error or "源数据库 size/mtime 改变或出现 WAL"
            except OSError as exc:
                error = error or str(exc)
                checks["database_unchanged"] = {"status": "fail", "error": str(exc)}
        if error is None:
            try:
                gate_c = gate_c_input(receipt["projects"])
                gate_c_path = output / "gate-c-input.json"
                write_json(gate_c_path, gate_c)
                receipt["gate_c_input"] = str(gate_c_path)
                checks["gate_c_input"] = {"status": "pass", "path": str(gate_c_path)}
            except Exception as exc:
                error = str(exc)
                checks["gate_c_input"] = {"status": "fail", "error": error}
        receipt["status"] = "pass" if error is None else "fail"
        receipt["commands"] = commands.records
        receipt["checks"] = checks
        if error is not None:
            receipt["error"] = error
        receipt_path = output / "workflow-receipt.json"
        write_json(receipt_path, receipt)
        for gate in ("gate-a", "gate-i-b"):
            write_json(output / f"{gate}.json", {
                "contract_version": EVIDENCE_VERSION, "protocol_version": PROTOCOL_VERSION,
                "gate_id": gate, "status": receipt["status"], "scope": "synthetic",
                "scope_description": SCOPE_DESCRIPTION,
                **{key: receipt.get(key) for key in ("evidence_scope", "release_candidate_id", "build_manifest_hash")},
                "workflow_receipt": str(receipt_path), "checks": checks,
                "checkpoint_mapping": CHECKPOINTS if gate == "gate-a" else {},
                "gate_protocol_version": "research-gate-a-protocol-v2" if gate == "gate-a" else PROTOCOL_VERSION,
                "commands": commands.records, "error": error,
            })
    return receipt


def gate_c_input(projects: list[dict]) -> dict:
    """将实测回执投影为 Gate C v2 输入，正式证据仍由 Gate C 验证。"""
    families = dict(zip(PROJECTS, ("cross_section", "time_series", "event_study", "term_structure")))
    require(len(projects) == 4 and {item["name"] for item in projects} == set(PROJECTS),
            "Gate C 必须收到四个公开项目")
    references = []
    for item in projects:
        evidence = item["database_evidence"]
        require(evidence["status"] == "pass" and evidence["unchanged"], "不能为变化的数据库生成 Gate C 输入")
        result = read_json(Path(item["result_directory"]) / "result.json")
        require(result["result_id"] == item["result_id"], "工作流与 Result 身份不一致")
        references.append({
            "reference": item["name"], "dag_family": families[item["name"]],
            **{key: item[key] for key in ("package", "plan", "result_directory", "result_store", "verification_result")},
            "expected_graph_id": item["graph_id"], "expected_result_id": item["result_id"],
            "study_request_ids": [request["request_id"] for request in item["data_range"]],
            "oracle_script": item["verifier_source"],
            "database_evidence": {
                "data_scope": "synthetic", "database_path": evidence["database"],
                "result_id": item["result_id"], "read_only": True, "database_unchanged": True,
                **{phase: {key: evidence[phase][key] for key in ("size_bytes", "mtime_ns")}
                   for phase in ("before", "after")},
            },
        })
    return {"contract_version": "research-gate-c-input-v2", "data_scope": "synthetic",
            "minimum_dag_families": 3, "maximum_window_days": 400, "references": references}


def compare_tables(left: Path, right: Path) -> dict:
    import pyarrow.parquet as pq

    def tables(root):
        manifests = read_json(root / "result.json")["tables"]
        require(manifests, "Result 未包含结果表")
        result = {item["table_id"]: pq.read_table([root / name for name in item["files"]]) for item in manifests}
        require(len(result) == len(manifests), "Result table_id 重复")
        return result

    first, second = tables(left), tables(right)
    require(first.keys() == second.keys(), "恢复结果表集合不同")
    details = {}
    for name in first:
        require(first[name].equals(second[name], check_metadata=True), f"恢复结果表不精确一致: {name}")
        details[name] = {"status": "pass", "rows": first[name].num_rows, "columns": first[name].column_names,
                         "comparison": "arrow_exact_with_schema_metadata"}
    return {"status": "pass", "tables": details}


def worker(action: str, arguments: list[str]) -> int:
    if action == "installation":
        import importlib
        import importlib.metadata
        modules = ("research_pipeline", "research_pipeline.cli", "research_pipeline.runtime.checkpoint",
                   "research_pipeline.packages", "research_pipeline.results")
        distribution = importlib.metadata.distribution("quantwitness")
        direct = json.loads(distribution.read_text("direct_url.json") or "{}")
        payload = {"status": "pass", "prefix": sys.prefix, "base_prefix": sys.base_prefix,
                   "executable": sys.executable, "version": distribution.version,
                   "editable": direct.get("dir_info", {}).get("editable", False),
                   "imports": {name: str(Path(importlib.import_module(name).__file__).resolve()) for name in modules}}
    elif action in {"prepare", "bundles"}:
        import runpy
        project, output = map(Path, arguments)
        examples = project / "examples"
        sys.path.insert(0, str(examples))
        filename = "prepare_synthetic_environment.py" if action == "prepare" else "build_bundles.py"
        module = runpy.run_path(str(examples / filename))
        payload = module["prepare"](output) if action == "prepare" else module["build_all"](examples, output)
    elif action == "compare":
        payload = compare_tables(*map(Path, arguments))
    elif action == "interrupt":
        from research_pipeline.cli import main as cli_main
        from research_pipeline.runtime.checkpoint import CheckpointStore
        node_id, receipt, *cli_arguments = arguments
        original = CheckpointStore.commit_bytes

        def commit(store, **kwargs):
            manifest = original(store, **kwargs)
            if kwargs["attempt_id"].startswith(node_id + "-attempt-"):
                marker = store.checkpoints_root / kwargs["expectation"].node_execution_id / "COMMITTED"
                require(marker.is_file(), "checkpoint 提交没有产生 COMMITTED")
                write_json(Path(receipt), {"phase": "committed_before_event", "node_id": node_id,
                    "attempt_id": kwargs["attempt_id"], "run_root": str(store.run_root), "committed_marker": str(marker)})
                os._exit(95)
            return manifest

        CheckpointStore.commit_bytes = commit
        return cli_main(cli_arguments)
    else:
        raise ValueError(f"未知内部动作: {action}")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "_worker":
        return worker(sys.argv[2], sys.argv[3:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True, help="已安装发行包的 venv Python")
    parser.add_argument("--project", type=Path, required=True, help="含 examples 的完整源码项目根目录")
    parser.add_argument("--output", type=Path, required=True, help="源码仓库外尚不存在的输出目录")
    parser.add_argument("--release-candidate-id", required=True)
    parser.add_argument("--build-manifest", type=Path, required=True)
    parser.add_argument("--verification-process-slots", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=300, help="每个命令的超时秒数")
    args = parser.parse_args()
    receipt = run_workflows(**vars(args))
    print(json.dumps({"status": receipt["status"], "workflow_receipt": str(args.output.resolve() / "workflow-receipt.json"),
                      "error": receipt.get("error")}, ensure_ascii=False))
    return 0 if receipt["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
