"""生成模型编码经验与正式开发研究记录的绑定。"""
import json
from pathlib import Path
import re


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _formal_network(execution, metrics, definition):
    from research_pipeline.evidence import load_verified_result_context
    from research_pipeline.results import ResultStore

    execution = Path(execution)
    store = ResultStore(execution / "results", create=False)
    verified = load_verified_result_context(metrics["verification_ref"], result_store=execution / "results")
    bundle = verified.snapshot.bundle
    if (verified.verification.status != "pass" or (metrics.get("result_id") is not None and metrics["result_id"] != bundle.result_id)
            or Path(metrics["result_ref"]).resolve() != store.result_directory(bundle).resolve()):
        raise ValueError("模型编码成功必须绑定正式Result和通过的独立验证")
    plan = _read(execution / "plan/admitted/research-plan.json")
    if plan["package_hash"] != bundle.package_hash or plan["package_plan_hash"] != bundle.plan_hash:
        raise ValueError("模型编码来源与已准入计划不一致")
    facts = json.loads(verified.snapshot.support_bytes[bundle.verification.validity_source_path])
    model = facts.get("model_diagnostics", {})
    if model.get("mode") != "walk_forward_development_v1" or model.get("design", {}).get("mode") != "development":
        raise ValueError("模型编码经验只接受正式开发结果")
    schema = model["table_bindings"]["models"]
    table = next(item for item in bundle.tables if item.schema_id == schema)
    supports = {(item.artifact_key, item.source_path): item for item in bundle.support_files}
    matches, paths = [], set()
    for path, config in model.get("model_configs", {}).items():
        if (config.get("candidate", {}).get("model", {}).get("class") != "GeneratedModel"
                or config.get("generated", {}).get("definition") != definition):
            continue
        config_item = supports.get((table.artifact_key, path))
        source_item = supports.get((table.artifact_key, config["generated"]["source_path"]))
        if config_item is None or source_item is None:
            raise ValueError("模型编码来源缺少封存配置或network.py")
        paths.update((config_item.relative_path, source_item.relative_path))
        matches.append((config, config_item, source_item))
    if not matches:
        raise ValueError("模型提案结构与正式封存配置不一致")
    snapshot = store.load_snapshot_by_identity(project_id=bundle.project_id, run_id=bundle.run_id,
        result_id=bundle.result_id, support_paths=tuple(sorted(paths)), verify_all_files=False)
    sources = []
    for config, config_item, source_item in matches:
        if json.loads(snapshot.support_bytes[config_item.relative_path]) != config:
            raise ValueError("模型编码来源配置与封存文件不一致")
        source = snapshot.support_bytes[source_item.relative_path].decode("utf-8")
        if source not in sources:
            sources.append(source)
    if len(sources) != 1:
        raise ValueError("模型编码来源包含多个不同网络源码；需分别记录实现")
    return sources[0], bundle.result_id


def record_coding_success(session, proposal, result):
    """只复制正式Result已封存源码，不把开发指标写入编码经验。"""
    candidate_id = proposal["candidate_id"]
    reference = session.root / "experiments" / candidate_id / "candidates" / candidate_id / "execution-ref.json"
    execution = Path(_read(reference)["execution_root"])
    source, _ = _formal_network(execution, result["metrics"], proposal["definition"])
    source_path = session.root / "rounds" / ("%04d" % proposal["round"]) / "verified-network.py"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.exists():
        if source_path.read_text(encoding="utf-8") != source:
            raise ValueError("正式模型源码副本改变；须使用新会话")
    else:
        source_path.write_text(source, encoding="utf-8")
    task = {key: session.knowledge.task[key] for key in ("interface", "model_family")}
    task["definition"] = proposal["definition"]
    return session.knowledge.record(candidate_id=candidate_id, source=source, task=task, evidence={
        "command_status": "succeeded", "execution_status": "succeeded", "verification_status": "pass", "model_status": "pass",
        "execution_ref": str(execution), "result_ref": result["metrics"]["result_ref"],
        "verification_ref": result["metrics"]["verification_ref"], "bundle_ref": str(source_path), "diagnostics": []})


def validate_coding_source(source_root, record):
    """核对真实编译失败、成功提案与正式模型，不以文件存在代替验证。"""
    from .model_knowledge import validate_generated_model_source

    source_root = Path(source_root)
    candidate_id = record["record_id"].rsplit("/", 1)[1]
    match = re.fullmatch(r"model_([0-9]{4})(?:_attempt_([0-9]+))?", candidate_id)
    if match is None:
        raise ValueError("模型编码来源候选标识不符合研究轮次")
    round_root = source_root / "rounds" / match[1]
    if not record["success"]:
        if match[2] is None:
            raise ValueError("失败模型编码来源缺少编译尝试标识")
        receipt = _read(round_root / ("compile-%02d.json" % int(match[2])))
        if receipt.get("status") != "fail" or receipt.get("response") != record["source"] or not receipt.get("error"):
            raise ValueError("失败模型源码与真实编译诊断不一致")
        validate_generated_model_source(source_root, record)
        return
    if match[2] is not None:
        raise ValueError("成功模型编码来源必须绑定正式候选")
    proposal = _read(round_root / "proposal.json")
    evaluation = _read(round_root / "evaluation.json")
    if (proposal.get("candidate_id") != candidate_id or evaluation.get("status") != "evaluated"
            or proposal.get("definition") != record["task"].get("definition")):
        raise ValueError("模型知识定义与正式轮次提案不一致")
    reference = _read(source_root / "experiments" / candidate_id / "candidates" / candidate_id / "execution-ref.json")
    refs, metrics = record["formal_refs"], evaluation["metrics"]
    if Path(refs["execution_ref"]).resolve() != Path(reference["execution_root"]).resolve():
        raise ValueError("模型知识执行引用与正式研究轮次不一致")
    for field in ("result_ref", "verification_ref"):
        if Path(refs[field]).resolve() != Path(metrics[field]).resolve():
            raise ValueError("模型知识Result或VerificationResult与正式轮次不一致")
    binding = validate_generated_model_source(source_root, record)
    if binding["result_id"] != metrics["result_id"]:
        raise ValueError("模型知识Result身份与正式研究轮次不一致")
