"""复用公开模型ResearchPackage，接入风险组合目标和独立oracle。"""
from importlib.metadata import version
import json
from pathlib import Path
import shutil
import sys

import yaml

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / "qlib_portfolio"
SOURCE = HERE.parents[1] / "project_extensions/qlib_portfolio_risk"
sys.path.insert(0, str(BASE))
import prepare as model_prepare
import portfolio_plan


def prepare(root, *, method="inv", estimator="empirical", lookback=20):
    from research_pipeline.extensions import (load_project_operator_declaration, compile_project_operator_bundle,
        compile_project_verifier_bundle, verify_project_verifier_bundle)
    from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
    from research_pipeline.platform import typed_canonical_hash
    root = Path(root).resolve()
    if method not in {"inv", "gmv", "rp", "mvo", "enhanced", "topk_dropout"}:
        raise ValueError("风险组合方法无效")
    if estimator not in {"empirical", "shrink"}:
        raise ValueError("风险估计方法无效")
    bundle = model_prepare.prepare(root, mode="portfolio")
    package = Path(bundle["package"])
    path = package / "spec/research.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    nodes = payload["graph"]["nodes"]
    config = {"method": method, "estimator": estimator, "lookback": lookback, "shrink_alpha": .1,
        "rebalance_policy": "one_rebalance_from_actual_initial_cash", "expected_return_semantics": "raw_price_change_prediction",
        "enhanced_solver": "CLARABEL", "turnover_policy": "require_original_constraints"}
    for item in nodes:
        parameters = item.get("parameters", {})
        if "design" in parameters:
            parameters["design"]["portfolio_risk"] = config
    design = next(item["parameters"]["design"] for item in nodes if item["node_id"] == "summary")
    identity = typed_canonical_hash(design)
    for item in nodes:
        parameters = item.get("parameters", {})
        if "research_identity_hash" in parameters:
            parameters["research_identity_hash"] = identity
        if item["node_id"] == "model_holdout":
            parameters["package_hash"] = identity
    request_path = root / "request.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request["design"] = design
    request["model_parameters"]["research_identity_hash"] = identity
    request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")
    sources_path = package / "sources/sources.yaml"
    sources = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
    sources["sources"][0]["content_hash"] = identity
    sources_path.write_text(yaml.safe_dump(sources, allow_unicode=True, sort_keys=False), encoding="utf-8")
    node = next(item for item in nodes if item["node_id"] == "portfolio_targets")
    node["operator_id"] = "project.qlib_risk.targets"
    declaration_path = root / "declarations/targets-risk.yaml"
    declaration = yaml.safe_load((root / "declarations/targets.yaml").read_text(encoding="utf-8"))
    declaration["operator"]["operator_id"] = "project.qlib_risk.targets"
    declaration["entry"] = {"module": "extension", "function": "targets"}
    declaration["dependency_lock"].update(scipy=version("scipy"), cvxpy=version("cvxpy"), clarabel=version("clarabel"))
    declaration_path.write_text(yaml.safe_dump(declaration, allow_unicode=True, sort_keys=False), encoding="utf-8")
    operator_source = root / "risk-operator-source"
    operator_source.mkdir()
    for name in ("optimizer.py", "extension.py"):
        shutil.copyfile(SOURCE/name, operator_source/name)
    declared = load_project_operator_declaration(declaration_path, source_root=operator_source)
    built = compile_project_operator_bundle(source_root=operator_source, output_root=root/"bundles/targets-risk",
        project_id=declared.project_id, operator_spec=declared.operator_spec, entry_module=declared.entry_module,
        entry_function=declared.entry_function, dependency_lock=declared.dependency_lock,
        project_artifact_types=declared.project_artifact_types, permissions=declared.permissions,
        registered_operator_specs=build_mainline_operator_registry().operator_specs)
    # 原目标bundle不再用于本研究；其余正式节点与现金执行合同继续复用。
    bundle["extensions"] = [value for value in bundle["extensions"] if Path(value).parent.name != "targets"] + [str(built)]
    tables = payload["result"]["tables"]
    for name in ("risk_inputs", "risk_optimization_results", "risk_optimization_attempts", "risk_constraint_residuals"):
        tables.append({"table_id": name, "role": "diagnostic", "source_node_id": "portfolio_targets", "source_port": "targets",
            "artifact_type": "research.portfolio-targets.v1", "schema_id": "project.qlib_demo."+name+".v1", "path_prefix": name})
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    verifier_source = root / "risk-verifier-source"
    verifier_source.mkdir()
    for source in (BASE/"verifier").glob("*.py"):
        shutil.copyfile(source, verifier_source/source.name)
    shutil.copyfile(BASE/"extension/factor_baseline_oracle.py", verifier_source/"factor_baseline_oracle.py")
    shutil.copyfile(SOURCE/"oracle.py", verifier_source/"portfolio.py")
    old = verify_project_verifier_bundle(bundle["verifier"])
    metrics = tuple(item for item in old.metric_definitions if not item.metric_id.startswith("portfolio."))
    metrics += portfolio_plan.metric_definitions(verifier_source/"portfolio.py")
    bundle["verifier"] = str(compile_project_verifier_bundle(source_root=verifier_source, output_root=root/"risk-verifier-bundles",
        project_id="qlib-public", verifier_id="public-qlib-risk-independent", verifier_version="1.0.0",
        entry_module="check", entry_function="verify", authorized_schema_ids=tuple(item["schema_id"] for item in tables),
        metric_definitions=metrics, dependency_lock={"pyarrow": "21.0.0"}))
    bundle["portfolio_risk"] = config
    (root/"bundle-paths.json").write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    return bundle
