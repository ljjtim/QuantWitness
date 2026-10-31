"""生成模型编码经验与正式开发研究记录的绑定。"""
import json
from pathlib import Path
import re


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _formal_network(execution, metrics, definition):
    from research_pipeline.results import ResultStore, load_result_context

    execution = Path(execution)
    store = ResultStore(execution / "results", create=False)
    context = load_result_context(metrics["result_ref"], result_store=execution / "results",
                                  additional_table_ids=("models", "study_design"))
    bundle = context.snapshot.bundle
    if (bundle.run.status != "succeeded" or (metrics.get("result_id") is not None and metrics["result_id"] != bundle.result_id)
            or Path(metrics["result_ref"]).resolve() != store.result_directory(bundle).resolve()):
        raise ValueError("模型编码成功必须绑定已完成的正式Result")
    plan = _read(execution / "plan/admitted/research-plan.json")
    if plan["package_hash"] != bundle.package_hash or plan["package_plan_hash"] != bundle.plan_hash:
        raise ValueError("模型编码来源与已准入计划不一致")
    design_table = next(item for item in bundle.tables if item.table_id == "study_design")
    design_rows = [row for batch in context.snapshot.iter_table_batches(design_table.schema_id, columns=("design_json",), batch_size=2)
                   for row in batch.to_pylist()]
    if len(design_rows) != 1 or json.loads(design_rows[0]["design_json"]).get("mode") != "development":
        raise ValueError("模型编码经验只接受正式开发结果")
    table = next(item for item in bundle.tables if item.table_id == "models")
    supports = {(item.artifact_key, item.source_path): item for item in bundle.support_files}
    paths = set()
    for batch in context.snapshot.iter_table_batches(table.schema_id, columns=("status", "config_path"), batch_size=128):
        for row in batch.to_pylist():
            if row["status"] != "fitted":
                continue
            item = supports.get((table.artifact_key, row["config_path"]))
            if item is None:
                raise ValueError("模型编码来源缺少封存配置")
            paths.add(item.relative_path)
    snapshot = store.load_snapshot_by_identity(project_id=bundle.project_id, run_id=bundle.run_id,
        result_id=bundle.result_id, support_paths=tuple(sorted(paths)), verify_all_files=False)
    sources = set()
    for path in sorted(paths):
        config = json.loads(snapshot.support_bytes[path])
        if (config.get("candidate", {}).get("model", {}).get("class") != "GeneratedModel"
                or config.get("generated", {}).get("definition") != definition):
            continue
        source_item = supports.get((table.artifact_key, config["generated"]["source_path"]))
        if source_item is None:
            raise ValueError("模型编码来源缺少封存network.py")
        sources.add(source_item.relative_path)
    if not sources:
        raise ValueError("模型提案结构与正式封存配置不一致")
    snapshot = store.load_snapshot_by_identity(project_id=bundle.project_id, run_id=bundle.run_id,
        result_id=bundle.result_id, support_paths=tuple(sorted(sources)), verify_all_files=False)
    source_texts = {snapshot.support_bytes[path].decode("utf-8") for path in sources}
    if len(source_texts) != 1:
        raise ValueError("模型编码来源包含多个不同网络源码")
    return source_texts.pop(), bundle.result_id


def record_coding_success(session, proposal, result):
    """只复制正式Result已封存源码，不把开发指标写入编码经验。"""
    candidate_id = proposal["candidate_id"]
    reference = session.root / "experiments" / candidate_id / "candidates" / candidate_id / "execution-ref.json"
    execution = Path(_read(reference)["execution_root"])
    source, _ = _formal_network(execution, result["metrics"], proposal["definition"])
    source_path = session.root / "rounds" / ("%04d" % proposal["round"]) / "result-network.py"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.exists():
        if source_path.read_text(encoding="utf-8") != source:
            raise ValueError("正式模型源码副本改变；须使用新会话")
    else:
        source_path.write_text(source, encoding="utf-8")
    task = {key: session.knowledge.task[key] for key in ("interface", "model_family")}
    task["definition"] = proposal["definition"]
    return session.knowledge.record(candidate_id=candidate_id, source=source, task=task, evidence={
        "command_status": "succeeded", "execution_status": "succeeded", "execution_ref": str(execution), "result_ref": result["metrics"]["result_ref"],
        "bundle_ref": str(source_path), "diagnostics": []})


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
    for field in ("result_ref",):
        if Path(refs[field]).resolve() != Path(metrics[field]).resolve():
            raise ValueError("模型知识Result与正式轮次不一致")
    binding = validate_generated_model_source(source_root, record)
    if binding["result_id"] != metrics["result_id"]:
        raise ValueError("模型知识Result身份与正式研究轮次不一致")
