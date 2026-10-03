"""Windows RP 执行桥：候选关联持久化，执行事实由 Workspace 持有。"""
import importlib
import json
from pathlib import Path
import shutil
import sys
from .contracts import FrozenRequest, write_json


def _command(argv):
    from research_pipeline.cli.parser import build_parser
    from research_pipeline.cli.commands import COMMAND_MODULES
    args = build_parser().parse_args(argv)
    module = importlib.import_module(COMMAND_MODULES[args.command])
    return module._execute(args)


def find_or_allocate(workspace, label, *, clock, root_seed):
    """allocation.label 随 execution.json 落盘，覆盖 allocate 尚未返回的空档。"""
    from research_pipeline.workspace import allocate_execution, load_workspace, rebuild_workspace_index
    config = load_workspace(workspace)
    index = rebuild_workspace_index(workspace)
    matches = [item for item in index["executions"] if item.get("label") == label]
    if len(matches) > 1:
        raise ValueError("候选标识对应多个 execution")
    if matches:
        item = dict(matches[0])
        item["execution_path"] = str(config.root / item["execution_path"])
        return item
    return allocate_execution(workspace, clock=clock, root_seed=root_seed, label=label)


def find_or_build(root, source, spec):
    """从已提交 bundle 恢复，覆盖编译完成但候选关联尚未落盘的空档。"""
    from research_pipeline.extensions.project_bundle import project_source_hash, verify_project_operator_bundle
    committed = sorted((root / "bundles").glob("*/COMMITTED"))
    if len(committed) > 1:
        raise ValueError("冻结候选对应多个 bundle")
    if committed:
        bundle = committed[0].parent
        manifest = verify_project_operator_bundle(bundle)
        if manifest.source_tree_hash != project_source_hash(source):
            raise ValueError("已提交 bundle 与冻结候选源码不一致")
        return {"bundle_path": str(bundle)}
    return _command(["operator", "build", "--spec", spec, "--source", str(source),
                     "--output", str(root / "bundles"), "--format", "json"])


def _options(mapping):
    argv = []
    for name, value in mapping.items():
        if name in {"data_db", "source_db", "plan", "run_root", "artifact_root", "result_store", "handoff_out"}:
            raise ValueError("runtime_options 不得覆盖来源或执行位置")
        if value is None:
            continue
        for part in value if isinstance(value, list) else [value]:
            argv.extend(["--" + name.replace("_", "-"), str(part)])
    return argv


def runtime_options(mapping, *, plan, execution, clock):
    """沿用正式 run 的解析默认值，执行位置仍由 Workspace 分配。"""
    from research_pipeline.cli.parser import build_parser
    from research_pipeline.cli.commands.workspace_flow import _RUNTIME_OPTIONS
    allowed = set(_RUNTIME_OPTIONS) - {"source_db", "input_snapshot_manifest"}
    if set(mapping) - allowed:
        raise ValueError("runtime_options 含非运行参数或覆盖冻结来源")
    args = build_parser().parse_args([
        "run", "--plan", str(plan), "--clock", clock,
        "--artifact-root", str(execution / "artifacts"), "--handoff-out", str(execution / "handoff.json"),
        "--run-root", str(execution / "run"), "--result-store", str(execution / "results"),
        *_options(mapping),
    ])
    return {name: getattr(args, name) for name in sorted(allowed)}



def failure_diagnostics(error, *, stage, execution=None, bundle=None):
    """保存正式异常分类，并从inspect定位失败节点与其候选身份。"""
    from research_pipeline.runtime.diagnostics import safe_error_summary
    summary = safe_error_summary(error, default_error_code="rp_command_failed")
    result = [{"stage": stage, "error_type": summary["exception_type"],
               "error_code": summary["error_code"], "message": summary["message"]}]
    if stage != "run" or execution is None:
        return result
    run = Path(execution) / "run"
    if not (run / "operator-dag-invocation.json").exists():
        return result
    try:
        inspection = _command(["inspect", "--run-root", str(run), "--json"])
        candidate_nodes = set()
        if bundle is not None:
            manifest = json.loads((Path(bundle) / "manifest.json").read_text(encoding="utf-8"))
            plan = json.loads((Path(execution) / "plan/admitted/operator-graph-plan.json").read_text(encoding="utf-8"))
            identity = manifest["operator_spec"]
            candidate_nodes = {node["node_id"] for node in plan["recipe"]["nodes"]
                               if (node["operator_id"], node["operator_version"]) ==
                               (identity["operator_id"], identity["operator_version"])}
        result[0]["recommended_action"] = inspection["recommended_action"]
        for node_id, node in inspection.get("nodes", {}).items():
            last_error = node.get("last_error")
            if node.get("status") == "succeeded" or not isinstance(last_error, dict):
                continue
            result.append({"stage": "run", "node_id": node_id,
                           "error_type": last_error.get("exception_type"),
                           "error_code": last_error.get("error_code"), "message": last_error.get("message"),
                           "candidate_operator": node_id in candidate_nodes,
                           "recommended_action": inspection["recommended_action"]})
    except Exception as diagnostic_error:
        safe = safe_error_summary(diagnostic_error, default_error_code="rp_inspect_failed")
        result.append({"stage": "inspect", "error_type": safe["exception_type"],
                       "error_code": safe["error_code"], "message": safe["message"]})
    return result

def evaluate(request, candidate_id):
    from research_pipeline.workspace import initialize_workspace, load_workspace
    from research_pipeline.packages import load_research_package
    from research_pipeline.workspace import run_workspace_execution, resume_workspace_execution
    binding = request.payload["runtime_binding"]
    from research_pipeline.data_plane import PathRolePolicy
    from research_pipeline.data_plane.archived_inputs import load_archived_input_manifest
    archived = load_archived_input_manifest(request.payload["input_snapshot_manifest"])
    sources = {f"archive_{key}_input": entry["root"] for key, entry in archived["requests"].items()}
    sources.update({f"{key}_input": request.payload[key] for key in
                    ("base_package", "source_archive_root", "editable_source_root", "reference_bundle")})
    PathRolePolicy().validate({"rd_session_output": binding["windows_session_root"], **sources},
                              read_only_roles=tuple(sources))
    frozen_path = Path(binding["windows_session_root"]) / "frozen-inputs.json"
    if json.loads(frozen_path.read_text(encoding="utf-8")) != request._materials():
        raise ValueError("冻结输入在执行前已改变")
    root = Path(binding["windows_session_root"]) / "candidates" / candidate_id
    candidate = json.loads((root / "candidate.json").read_text(encoding="utf-8"))
    if candidate["candidate_id"] != candidate_id:
        raise ValueError("候选引用不一致")
    feedback = dict(bundle_ref=None, execution_ref=None, command_status="failed", execution_status="not_run",
                    verification_status="not_run", formula_status="not_run", formula_coverage={},
                    claim_level="research_observation", diagnostics=[], result_ref=None, verification_ref=None)
    stage = "build"
    try:
        if "code_generation" in request.payload:
            from .generation import validate_generated_source
            stage = "code_validation"
            validate_generated_source((root / "compute.py").read_text(encoding="utf-8"))
        stage = "build"
        source = root / "source"
        if not source.exists():
            pending = root / "source.pending"
            shutil.copytree(request.payload["editable_source_root"], pending, dirs_exist_ok=True)
            shutil.copyfile(root / "compute.py", pending / "compute.py")
            pending.rename(source)
        elif (source / "compute.py").read_bytes() != (root / "compute.py").read_bytes():
            raise ValueError("冻结候选代码已改变")
        built = find_or_build(root, source, binding["operator_spec"])
        feedback["bundle_ref"] = built["bundle_path"]
        from .package_execution import execute_package
        feedback.update(execute_package(
            root=root, workspace_id=request.payload["request_id"] + "-" + candidate_id,
            allocation_label=candidate["allocation_label"], base_package=request.payload["base_package"],
            source_archive_root=request.payload["source_archive_root"],
            input_snapshot_manifest=request.payload["input_snapshot_manifest"],
            verifier_bundle=request.payload["reference_bundle"],
            binding={**binding, "extension_bundles": [*binding["extension_bundles"], built["bundle_path"]]}))
        stage = "verify"
        from research_pipeline.evidence import load_verified_result_context
        from research_pipeline.results import ResultStore
        from .feedback import project_formula_feedback
        store = Path(feedback["execution_ref"]) / "results"
        verified = load_verified_result_context(feedback["verification_ref"], result_store=store)
        feedback.update(project_formula_feedback(verified, ResultStore(store),
                                                request.payload.get("formula_evaluation")))
        feedback["command_status"] = "succeeded"
    except Exception as exc:
        execution_failure = getattr(exc, "package_execution_feedback", {})
        feedback.update(execution_failure)
        stage = execution_failure.get("failed_stage", stage)
        feedback["command_status"] = "failed"
        feedback["diagnostics"] = failure_diagnostics(exc, stage=stage,
            execution=feedback.get("execution_ref"), bundle=feedback.get("bundle_ref"))
        if stage == "run":
            feedback["execution_status"] = "failed"
    write_json(root / "feedback.json", feedback)
    return feedback


def main():
    payload = json.load(sys.stdin)
    request = FrozenRequest.from_dict(payload["request"])
    result = evaluate(request, payload["candidate_id"])
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
