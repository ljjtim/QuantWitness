"""从正式开发结果导出可追溯研究知识，并按完整研究范围检索。"""
from copy import deepcopy
from datetime import date, datetime
import json
from pathlib import Path

from .contracts import write_json
from .factor_research import DAI_FACTOR_CONTRACT, canonical_expression, factor_contract, feature_expression_slot, validate_factor_research
from .package_campaign import _check_development_package, project_development_metric

VERSION = "rd-research-knowledge-v1"
_METRIC_FIELDS = ("metric_id", "unit", "frequency", "direction", "annualization_policy", "measurement_semantics")
_DOMAIN_FIELDS = ("calendar_id", "snapshot_scope", "price_basis")
_REFLECTION_FIELDS = ("observations", "hypothesis_evaluation", "new_hypothesis", "reason", "decision")


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _plain(value):
    if isinstance(value, dict) or hasattr(value, "items"):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _time(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("研究知识时点必须包含时区")
    return result


def _scope(design, development):
    if design.get("mode") != "development":
        raise ValueError("研究知识仅接受 development")
    start, end = date.fromisoformat(development["start"]), date.fromisoformat(development["end"])
    if not start <= end <= _time(development["as_of"]).date():
        raise ValueError("开发范围或时点无效")
    result = {key: design[key] for key in _DOMAIN_FIELDS}
    if any(not isinstance(value, str) or not value for value in result.values()):
        raise ValueError("资产来源、日历和价格口径不能为空")
    entities, sessions = design["entities"], design["research_sessions"]
    if not entities or not sessions or any(not isinstance(x, str) or not x for x in entities):
        raise ValueError("研究知识必须声明资产和会话范围")
    if len(set(entities)) != len(entities) or len(set(sessions)) != len(sessions):
        raise ValueError("资产和会话范围不得重复")
    if any(not start <= date.fromisoformat(day) <= end for day in sessions):
        raise ValueError("研究会话超出开发范围")
    if factor_contract(design) == DAI_FACTOR_CONTRACT:
        result["factor_contract"] = DAI_FACTOR_CONTRACT
    result.update(entities=sorted(entities), research_sessions=sorted(sessions))
    return result


def _package_design(package):
    nodes = {node["node_id"]: node for node in package.spec_payload["graph"]["nodes"]}
    design = _plain(nodes["feature"]["parameters"]["design"])
    if any(_plain(nodes[key]["parameters"]["design"]) != design for key in ("label", "summary")):
        raise ValueError("Feature、Label与Summary设计不一致")
    return design


def _metric_description(verifier, objective, design):
    definitions = [item for item in verifier.metric_definitions if item.metric_ref == design["metric_ref"]
                   and item.result_schema_id == objective["schema_id"]]
    if len(definitions) != 1:
        raise ValueError("知识来源没有唯一正式指标定义")
    definition = definitions[0].payload()
    result = {key: _plain(definition[key]) for key in _METRIC_FIELDS}
    if (objective["direction"] == "minimize") != (result["direction"] == "lower_is_better"):
        raise ValueError("知识指标方向与开发目标不一致")
    return result


def _formal_record(root, candidate, record, template, base_design, verifier):
    """仅正式 Result 的设计表和已绑定变体包决定金融上下文。"""
    from research_pipeline.evidence import load_verified_result_context
    from research_pipeline.packages import load_research_package
    from research_pipeline.results import ResultStore

    experiment = root / "experiments" / candidate
    package = load_research_package(experiment / "packages" / ("candidate_" + candidate))
    _check_development_package(package, template["development"])
    design = _package_design(package)
    expected = deepcopy(base_design)
    expected["feature_expressions"] = {feature_expression_slot(base_design): canonical_expression(record["expression"], contract=factor_contract(base_design))}
    if design != expected:
        raise ValueError("轮次表达式或研究范围与正式变体包不符")
    execution = Path(_read(experiment / "candidates" / candidate / "execution-ref.json")["execution_root"])
    result = {"execution_ref": str(execution), "verification_ref": str(execution / "verification/result.json"),
              "result_ref": record["metrics"]["result_ref"]}
    context = load_verified_result_context(result["verification_ref"], result_store=execution / "results",
        additional_table_ids=(template["objective"]["table_id"], "study_design"))
    bundle = context.snapshot.bundle
    if context.verification.status != "pass" or bundle.package_hash != package.package_hash:
        raise ValueError("知识 Result 未通过验证或研究包身份不符")
    if dict(bundle.verification.verifier_identity) != verifier.identity():
        raise ValueError("知识 Result 与指标 Verifier 身份不符")
    if Path(result["result_ref"]).resolve() != ResultStore(execution / "results", create=False).result_directory(bundle).resolve():
        raise ValueError("轮次 Result 路径与独立验证引用不符")
    if record["metrics"].get("result_id") != bundle.result_id:
        raise ValueError("轮次 Result 身份与独立验证引用不符")
    if Path(record["metrics"]["verification_ref"]).resolve() != Path(result["verification_ref"]).resolve():
        raise ValueError("轮次 VerificationResult 路径不符")
    tables = [table for table in bundle.tables if table.table_id == "study_design"]
    if len(tables) != 1:
        raise ValueError("Result 缺少唯一设计表")
    rows = []
    for batch in context.snapshot.iter_table_batches(tables[0].schema_id, columns=("design_json",), batch_size=2):
        rows.extend(batch.to_pylist())
        if len(rows) > 1:
            raise ValueError("Result 设计表必须只有一行")
    if len(rows) != 1 or json.loads(rows[0]["design_json"]) != design:
        raise ValueError("Result 正式设计与表达式声明不符")
    _check_result_tables(bundle.tables)
    metrics = project_development_metric(result, template)
    return {"metrics": {key: metrics[key] for key in ("value", "rows")},
        "claim_level": context.verification.claim_level, "design_scope": _scope(design, template["development"]),
        "metric_description": _metric_description(verifier, template["objective"], design),
        "source": {"result_id": bundle.result_id, "package_hash": bundle.package_hash,
                   "result_ref": result["result_ref"], "verification_ref": result["verification_ref"],
                   "execution_ref": str(execution), "verifier_identity": verifier.identity()}}


def _check_result_tables(tables):
    for table in tables:
        # 开发分割保留空的隔离集索引，不能将结构声明视为已读取隔离集。
        if table.table_id == "holdout_index" and not sum(table.row_counts.values()):
            continue
        if "holdout" in table.table_id.lower() or "test" in table.table_id.lower():
            raise ValueError("知识来源 Result 包含最终评价表")


def _inherited_records(root, payload):
    if "knowledge" not in payload:
        return []
    snapshot = _read(root / "knowledge-input.json")
    index = _read(payload["knowledge"]["index"])
    if snapshot["index"] != index or _build_index(index["session_root"]) != index:
        raise ValueError("历史知识与冻结来源不一致")
    return deepcopy(index["records"])


def _merge_records(inherited, current):
    records = {record["record_id"]: record for record in inherited}
    for record in current:
        key = record["record_id"]
        if key in records and records[key] != record:
            raise ValueError("知识记录ID冲突；独立会话须使用不同campaign_id")
        records[key] = record
    return list(records.values())


def _build_index(session_root):
    from research_pipeline.packages import load_research_package
    from research_pipeline.extensions.verifier_bundle import verify_project_verifier_bundle

    root = Path(session_root).resolve()
    payload = validate_factor_research(_read(root / "request.json"))
    if Path(payload["session_root"]).resolve() != root:
        raise ValueError("知识来源会话目录与冻结请求不同")
    outcome = _read(root / "outcome.json")
    if outcome.get("status") not in {"completed", "incomplete"} or outcome.get("holdout_evaluated") is not False:
        raise ValueError("知识只从已终止的开发研究会话导出")
    template = payload["package_template"]
    package = load_research_package(template["source"]["path"])
    _check_development_package(package, template["development"])
    design = _package_design(package)
    design_scope = _scope(design, template["development"])
    verifier = verify_project_verifier_bundle(template["source"]["verifier_bundle"])
    metric_description = _metric_description(verifier, template["objective"], design)
    records, best = [], None
    paths = sorted((root / "rounds").glob("*/record.json"))
    if not paths:
        raise ValueError("知识来源没有完成轮次")
    raw_records = [_read(path) for path in paths]
    if outcome.get("campaign_id") != payload["campaign_id"] or outcome.get("rounds") != raw_records:
        raise ValueError("会话完成记录与已冻结轮次不同")
    seen = set()
    for path, record in zip(paths, raw_records):
        candidate = record["candidate_id"]
        if candidate != f"factor_{int(path.parent.name):04d}" or candidate in seen:
            raise ValueError("知识轮次身份无效或重复")
        seen.add(candidate)
        status = record["status"]
        if status not in {"evaluated", "failed", "rejected", "stopped"}:
            raise ValueError("知识轮次状态无效")
        if status == "stopped":
            continue
        entry = {"record_id": payload["campaign_id"] + "/" + candidate,
            "candidate_id": candidate, "parent_id": record.get("parent_id"), "kind": "negative",
            "status": "technical_failure", "claim_level": "technical_status", "expression": None,
            "hypothesis": record.get("hypothesis", ""), "reason": record.get("reason", "正式执行未获得可用于研究的验证结果"),
            "reflection": {}, "development_scope": template["development"], "as_of": template["development"]["as_of"],
            "objective": template["objective"], "metric_description": metric_description, "design_scope": design_scope,
            "usable_for_proposal": True, "session_root": str(root),
            "source": {"record_ref": str(path), "request_ref": str(root / "request.json")}}
        if "expression" in record:
            try:
                entry["expression"] = canonical_expression(record["expression"], contract=factor_contract(design))
            except ValueError:
                if status == "evaluated":
                    raise
        if status == "evaluated":
            formal = _formal_record(root, candidate, record, template, design, verifier)
            entry.update({key: value for key, value in formal.items() if key != "source"})
            entry["source"].update(formal["source"])
            entry["record_id"] += "/" + formal["source"]["result_id"]
            loss = entry["metrics"]["value"] * (1 if template["objective"]["direction"] == "minimize" else -1)
            entry["status"] = "baseline" if best is None else "improved" if loss < best else "no_improvement"
            entry["kind"] = "negative" if entry["status"] == "no_improvement" else "observation"
            best = loss if best is None else min(best, loss)
            entry["reason"] = record.get("reason", "")
            entry["reflection"] = {key: record["reflection"][key] for key in _REFLECTION_FIELDS if key in record.get("reflection", {})}
        records.append(entry)
    records = _merge_records(_inherited_records(root, payload), records)
    return {"contract_version": VERSION, "session_root": str(root), "campaign_id": payload["campaign_id"], "records": records}


def export_session(session_root, output):
    """输出不可变索引；同样正文重复导出不改写文件。"""
    payload = _build_index(session_root)
    output = Path(output).resolve()
    if output.is_relative_to(Path(session_root).resolve()):
        raise ValueError("知识索引须保存到来源会话之外")
    if output.exists():
        if _read(output) != payload:
            raise ValueError("知识索引已经冻结，不能覆盖不同内容")
    else:
        write_json(output, payload)
    return payload


def _compatible(record, development, objective, metric_description, scope):
    if record.get("usable_for_proposal") is not True or record.get("objective") != objective or record.get("metric_description") != metric_description:
        return False
    source = record["development_scope"]
    if not development["start"] <= source["start"] <= source["end"] <= development["end"]:
        return False
    if _time(record["as_of"]) > _time(development["as_of"]):
        return False
    prior = record["design_scope"]
    if factor_contract(prior) != factor_contract(scope):
        return False
    if any(prior[key] != scope[key] for key in _DOMAIN_FIELDS):
        return False
    return (set(prior["entities"]) <= set(scope["entities"]) and
            set(prior["research_sessions"]) <= set(scope["research_sessions"]))


def query_index(index_path, *, development, objective, metric_description, design, max_records=8):
    """先过滤完整研究范围，再重验入选来源；索引不替代正式证据。"""
    if type(max_records) is not int or not 1 <= max_records <= 100:
        raise ValueError("max_records 必须为1至100的整数")
    if set(metric_description) != set(_METRIC_FIELDS):
        raise ValueError("检索必须声明完整正式指标语义")
    scope = _scope(design, development)
    index = _read(index_path)
    if index.get("contract_version") != VERSION:
        raise ValueError("不支持的研究知识索引")
    selected = [record for record in index["records"] if _compatible(record, development, objective, metric_description, scope)]
    if not selected:
        return []
    fresh = _build_index(index["session_root"])
    if fresh != index:
        raise ValueError("知识索引与当前正式来源不一致")
    selected.sort(key=lambda item: (item["as_of"], item["record_id"]), reverse=True)
    return deepcopy(selected[:max_records])
