"""从合成或自有封存ETF输入生成模型研究包和独立Verifier。"""
from copy import deepcopy
from datetime import datetime
import argparse
import hashlib
import json
from pathlib import Path
import yaml

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.metric_contracts import MetricDefinition
from research_pipeline.research import (ResearchSemantics, ResearchTimeWindow, FeatureSetArtifact,
    LabelArtifact, EstimandSpec, HypothesisSpec)
from research_pipeline.research.validation import build_search_manifest
from research_pipeline.extensions import load_project_operator_declaration, compile_project_operator_bundle, compile_project_verifier_bundle
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
from inputs import prepare_inputs, PRICE_FIELDS
import synthetic

HERE = Path(__file__).resolve().parent
METRIC = "project.qlib_demo.prediction_mse@1.0.0"
CLOCK = "2026-10-03T09:00:00+08:00"
SEED = 1703


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding="utf-8")


def stamp(day, at="09:30:00"):
    return datetime.fromisoformat(str(day) + "T" + at + "+08:00")


def candidate(alpha):
    return {"candidate_id": "ridge_"+str(alpha), "model": {"class": "LinearModel",
        "module_path": "qlib.contrib.model.linear", "kwargs": {"estimator": "ridge", "alpha": alpha,
        "fit_intercept": True, "include_valid": False}}, "processors": {"infer": [
        {"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature", "clip_outlier": True}},
        {"class": "Fillna", "kwargs": {"fields_group": "feature", "fill_value": 0}}],
        "learn": [{"class": "DropnaLabel", "kwargs": {}}]}, "fit": {}}


def edge(port, node, output):
    return {"input_port": port, "source_node_id": node, "source_output_port": output}


def node(name, operator, inputs, parameters=None, version="2.0.0"):
    return {"node_id": name, "operator_id": operator, "operator_version": version,
        "inputs": [edge(*item) for item in inputs], "parameters": parameters or {}, "strategies": []}


def declaration(name, inputs, outputs, parameters, types=(), module="operator", function="run"):
    return {"contract_version": "project-operator-declaration-v2", "project_id": "qlib-public",
        "project_artifact_types": list(types), "operator": {"contract_version": "research-operator-contract-v1",
        "operator_id": "project.qlib_demo."+name, "operator_version": "1.0.0",
        "input_ports": [{"port": port, "artifact_type": kind} for port, kind in inputs],
        "output_ports": [{"port": port, "artifact_type": kind} for port, kind in outputs],
        "parameters": [{"name": key, "value_type": kind, "required": True, "allowed_values": []} for key, kind in parameters],
        "strategy_roles": [], "resource_profile": {"memory_bytes": 1024**3, "cpu_slots": 1,
            "process_slots": 4, "temp_bytes": 1024**3, "wall_seconds": 1800},
        "determinism_mode": "deterministic", "seed_policy": "none", "pit_capabilities": ["pit.as_of.v1"]},
        "entry": {"module": module, "function": function}, "dependency_lock": {"pyarrow": "21.0.0"},
        "permissions": {"artifact_write_scope": "output_only"}}


def prepare(root, mode="model", input_config=None):
    root = Path(root).resolve()
    if mode not in {"model", "development", "portfolio"}:
        raise ValueError("不支持的公开起点")
    if (root / "bundle-paths.json").exists():
        raise ValueError("该输出目录已有冻结研究，请使用新目录")
    metric_ref = "project.qlib_demo.validation_mse@1.0.0" if mode == "development" else METRIC
    own = None
    if input_config is not None:
        from input_config import load_input_config
        own, requests, archive_payload = load_input_config(input_config, mode=mode)
        root.mkdir(parents=True, exist_ok=True)
        archive = root / "inputs.json"
        archive.write_text(json.dumps(archive_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        days, entities = own["calendar_sessions"], own["entities"]
        fields = tuple(own["columns"][key] for key in ("date", "code", "close"))
        catalog_lock = own["catalog_lock"]
        (root / "input-config.json").write_text(json.dumps(own, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        calendar = synthetic.sessions()
        _, requests, archive = prepare_inputs(root, end_session=calendar[78] if mode == "development" else None)
        days, entities = [day.isoformat() for day in calendar], list(synthetic.instruments())
        fields, catalog_lock = PRICE_FIELDS, str(root / "catalog")
    for request in requests:
        request.pop("ir_version", None)
        request.pop("as_of", None)
    study = days[11:-3]
    boundary = study[-20]
    cutoff = days[days.index(boundary)-1]
    clock = stamp(boundary, "16:00:00").isoformat() if mode == "development" else own["fixed_clock"] if own else CLOCK
    if mode == "development":
        study = [day for day in study if day < cutoff]
    research_id = own["research_id"] if own else "public.qlib.etf"
    design = {"calendar_sessions": days, "research_sessions": study,
        "split_calendar_sessions": [day for day in study if day < boundary], "entities": entities,
        "holdout_start": boundary+"T00:00:00+08:00", "train_sessions": 30, "validation_sessions": 10,
        "test_sessions": 10, "step_sessions": 10, "embargo_sessions": 1, "expanding": True, "root_seed": SEED,
        "price_basis": "unadjusted_price_change", "calendar_source": own["calendar_source"] if own else "synthetic.py deterministic weekdays",
        "calendar_id": own["calendar_id"] if own else "public_synthetic_weekdays",
        "snapshot_scope": own["snapshot_scope"] if own else "public_synthetic_no_market_claim",
        "price_fields": list(fields),
        "market_fields": [own["columns"][key] for key in ("date", "code", "close", "open", "high_limit", "low_limit", "paused")] if own else [*PRICE_FIELDS, "fld_demo_open", "fld_demo_high_limit", "fld_demo_low_limit", "fld_demo_paused"]}
    design["finance"] = own["finance"] if own else {"initial_cash_cny": 100000., "price_scale": 3}
    candidates = [candidate(0.1), candidate(1.0)]
    search = build_search_manifest(search_id=research_id, candidates=candidates, method="grid",
        max_trials=len(candidates), max_parallel=1, stopping_condition="complete_declared_candidate_universe",
        objective="neg_mean_squared_error", direction="maximize", frozen_at=stamp(days[0], "09:00:00"))
    design["mode"] = mode
    design["metric_ref"] = metric_ref
    design["candidate_ids"] = [item.candidate_id for item in search.candidates]
    design["candidate_parameters_json"] = canonical_json({item.candidate_id: dict(item.parameters) for item in search.candidates})
    identity = typed_canonical_hash(design)
    parameters = {"candidate_jsons": [canonical_json(item) for item in candidates], "target_kind": "regression",
        "objective": "neg_mean_squared_error", "direction": "maximize", "search_id": research_id,
        "search_frozen_at": stamp(days[0], "09:00:00").isoformat(), "research_identity_hash": identity, "thread_count": 1}
    (root / "request.json").write_text(json.dumps({"design": design, "model_parameters": parameters, "root_seed": SEED}, indent=2), encoding="utf-8")
    package = root / "package"
    metric_semantics = {metric_ref: "开发validation价格变化预测均方误差" if mode == "development" else "冻结选择后单次holdout的价格变化预测均方误差"}
    if mode == "portfolio":
        from portfolio_plan import PORTFOLIO_METRICS
        metric_semantics.update({ref: value[0] for ref, value in PORTFOLIO_METRICS.items()})
    save(package / "package.yaml", {"package_slug": research_id.replace(".", "_"), "display_name": own["display_name"] if own else "合成ETF日频Qlib研究",
        "package_version": "1.0.0", "builder_id": "operator_graph_plan_v1", "metric_contract": {
        "contract_id": "public.qlib.metrics", "version": "1.0.0", "metrics": list(metric_semantics),
        "semantics": metric_semantics},
        "claim_contract": {"allowed_claim_levels": ["research_observation"], "max_claim_level": "research_observation"}})
    save(package / "sources/sources.yaml", {"sources": [{"source_id": "public_synthetic", "source_type": "dataset",
        "title": "公开确定性合成ETF行情", "url": "https://github.com/ljjtim/QuantWitness", "accessed_at": "2026-10-03",
        "content_hash": identity, "license_id": "Apache-2.0", "status": "available", "limitation": "全部数据为合成，不对应真实行情或投资表现",
        "provenance": {"mode": "citation_only", "content_digest": None, "media_type": None, "snapshot_artifact_id": None,
        "snapshot_manifest_hash": None, "importer_id": None, "imported_at": None, "contract_version": "research-source-provenance-v1"}}]})
    save(package / "localization.yaml", {"decisions": [{"decision_id": "synthetic_scope", "original_assumption": "研究使用真实市场历史",
        "local_market": "cn_etf", "local_adaptation": "10只虚构ETF与确定性价格，仅验证模型时间合同和复现",
        "evidence_source_ids": ["public_synthetic"], "status": "proxy", "claim_effect": "downgrade_to_research_observation"}]})
    if own:
        save(package / "sources/sources.yaml", {"sources": own["sources"]})
        save(package / "localization.yaml", {"decisions": own["localization"]})
    if not own:
        from input_config import write_input_template
        write_input_template(root, design=design, research_id=research_id, display_name="合成ETF日频Qlib研究",
            catalog_lock=catalog_lock, archive=archive, fixed_clock=CLOCK,
            sources=yaml.safe_load((package / "sources/sources.yaml").read_text(encoding="utf-8"))["sources"],
            localization=yaml.safe_load((package / "localization.yaml").read_text(encoding="utf-8"))["decisions"])
    first = study[0]
    feature = FeatureSetArtifact.build(feature_id="qlib_demo.features", artifact_id="qlib_demo.feature.rows",
        entity_keys=("entity_id", "observation_session"), value_fields=("value",),
        time_window=ResearchTimeWindow(stamp(first,"00:00:00"), stamp(first), stamp(days[0],"15:00:00"), stamp(first,"00:00:00")),
        schema_hash=typed_canonical_hash(["feature_id","window_sessions","value"]), transform_lineage_hash=identity,
        source_revision_hash=identity, preprocessing_order=("historical_return","volatility"))
    label = LabelArtifact.build(label_id="qlib_demo.labels", artifact_id="qlib_demo.label.rows",
        entity_keys=("entity_id","observation_session"), value_field="forward_return",
        time_window=ResearchTimeWindow(stamp(first), stamp(days[13]), stamp(first,"15:00:00"), stamp(days[12],"15:00:00")),
        schema_hash=typed_canonical_hash(["horizon_sessions","forward_return"]), source_lineage_hash=identity,
        source_revision_hash=identity, visibility_policy_hash=typed_canonical_hash("available.daily.v1/next_session_open"),
        revision_policy="point_in_time", training_only=True)
    estimand = EstimandSpec.build(estimand_id="qlib_demo.mse", label_id=label.label_id, population="fixed_declared_etf_pool" if own else "fixed_synthetic_ten",
        sample_policy="point_in_time", statistic="mean_squared_error", unit="squared_decimal_price_change", direction="two_sided", metric_refs=(metric_ref,))
    hypothesis = HypothesisSpec.build(hypothesis_id="qlib_demo.integration", estimand_id=estimand.estimand_id,
        hypothesis_kind="exploratory", direction="two_sided", primary_metric_ref=metric_ref, preregistration_hash=None,
        requested_claim_level="research_observation")
    semantics = ResearchSemantics.build(decision_at=stamp(first), features=(feature,), labels=(label,), estimands=(estimand,), hypotheses=(hypothesis,))
    nodes = [node("data_plane", "data.columnar.materialize", [], version="1.0.0")]
    declarations = []
    for kind in ("feature", "label"):
        output = "features" if kind == "feature" else "labels"
        artifact = "research.feature-set.v1" if kind == "feature" else "research.label.v1"
        keys = ["entity_id","observation_session",*(["window_sessions","feature_id"] if kind == "feature" else ["horizon_sessions"])]
        work = []
        for day in study:
            i = days.index(day)
            start, end = (days[i-11],days[i-1]) if kind == "feature" else (day,days[i+1])
            rows = [[code,day,w,f] for code in entities for w in (5,10) for f in ("historical_return","volatility")] if kind == "feature" else [[code,day,1] for code in entities]
            work.append({"key_rows": rows, "decision_time": stamp(day).isoformat(), "window_start": stamp(start,"15:00:00").isoformat(),
                "window_end": stamp(end,"15:00:00").isoformat(), "source_partitions": {"bars": sorted({d[:7] for d in days if start <= d <= end})}})
        causal = {"kind": kind, "output_port": output, "key_columns": keys, "state_scope": "independent",
            "sources": [{"port": "bars", "request_id": "daily_"+kind, "columns": list(fields), "observation_column": fields[0],
                "available_column": fields[0], "daily_time": {"rule": "next_session_open", "timezone": "Asia/Shanghai",
                "calendar_id": design["calendar_id"], "calendar_source": design["calendar_source"], "sessions": days}}], "work_items": work}
        nodes.append(node(kind,"project.qlib_demo."+kind,[("bars","data_plane","data")],
            {"causal_plan": causal,"design": design,"lineage_ref": identity},"1.0.0"))
        declarations.append((kind, declaration(kind,[("bars","data.columnar-bundle.v1")],[(output,artifact)],
            [("causal_plan","json"),("design","json"),("lineage_ref","string")])))
    split_params = {key: design[key] for key in ("holdout_start","train_sessions","validation_sessions","test_sessions","step_sessions","embargo_sessions","expanding")}
    split_params.update(horizon_sessions=1,target_field="forward_return",calendar_sessions=design["split_calendar_sessions"])
    split_params["evaluation_scope"] = "development" if mode == "development" else "final"
    nodes.extend([
        node("model_split","research.model.split-manifest",[("features","feature","features"),("labels","label","labels")],split_params),
        node("model_fit","research.model.fit",[("splits","model_split","splits")],parameters),
        node("model_predict","research.model.predict",[("splits","model_split","splits"),("models","model_fit","models")]),
        node("model_metrics","research.model.fold-metrics",[("predictions","model_predict","predictions"),("models","model_fit","models")],{"target_kind":"regression","objective":"neg_mean_squared_error"}),
        node("model_selection","research.model.selection",[("metrics","model_metrics","metrics"),("splits","model_split","splits"),("models","model_fit","models")],{"target_kind":"regression","objective":"neg_mean_squared_error","direction":"maximize"}),
        node("model_holdout","research.model.locked-holdout",[("features","feature","features"),("labels","label","labels"),("selection","model_selection","selection"),("splits","model_split","splits")],
            {**parameters,"package_hash":identity,"implementation_hash":typed_canonical_hash((HERE/"extension/operator.py").read_text(encoding="utf-8")),
             "holdout_actor":research_id,"holdout_reason":"final_locked_holdout","holdout_unlock_at":clock,"fixed_clock":clock}),
        node("summary","project.qlib_demo.summary",[("data","data_plane","data"),("fit","model_fit","models"),("holdout","model_holdout","holdout")],{"design":design,"price_request_id":"daily_feature"},"1.0.0"),
    ])
    summary_type="project.qlib_demo.summary.v1"
    if mode == "development":
        nodes = [item for item in nodes if item["node_id"] not in {"model_selection", "model_holdout"}]
        nodes[-1]["inputs"] = [edge("data", "data_plane", "data"), edge("fit", "model_fit", "models"), edge("predictions", "model_predict", "predictions")]
    declarations.append(("summary",declaration("summary",[("data","data.columnar-bundle.v1"),("fit","research.model-fits.v2"),("holdout","research.model-locked-holdout.v2")],[("result",summary_type)],[("design","json"),("price_request_id","string")],(summary_type,),function="summarize")))
    if mode == "development":
        declarations[-1][1]["operator"]["input_ports"][-1] = {"port":"predictions","artifact_type":"research.model-validation-predictions.v2"}
    tables=[]
    def table(name, source, port, kind, schema=None, role="diagnostic"):
        tables.append({"table_id":name,"role":role,"source_node_id":source,"source_port":port,"artifact_type":kind,"schema_id":schema or "project.qlib_demo."+name+".v1","path_prefix":name})
    for name,source,port,kind in [("features","feature","features","research.feature-set.v1"),("labels","label","labels","research.label.v1")]: table(name,source,port,kind)
    for source,port,kind,names in [
        ("model_split","splits","research.model-split-manifest.v2",("samples","split_audit","holdout_index")),
        ("model_fit","models","research.model-fits.v2",("fit_audit",)),
        ("model_predict","predictions","research.model-validation-predictions.v2",("validation_predictions",)),
        ("model_selection","selection","research.model-selection.v2",("selection","test_predictions","fold_selections")),
        ("model_holdout","holdout","research.model-locked-holdout.v2",("holdout_predictions","holdout_receipt")),
        ("summary","result",summary_type,("raw_prices","study_design"))]:
        for name in names: table(name,source,port,kind)
    table("metrics","validity","metrics","project.qlib_demo.prediction-metrics.v1",role="primary")
    table("models","summary","result",summary_type,"research.qlib-model-inventory.v1")
    validity_ports=[("data","data_plane","data","data.columnar-bundle.v1"),("features","feature","features","research.feature-set.v1"),
        ("labels","label","labels","research.label.v1"),("splits","model_split","splits","research.model-split-manifest.v2"),
        ("fits","model_fit","models","research.model-fits.v2"),("predictions","model_predict","predictions","research.model-validation-predictions.v2"),
        ("selection","model_selection","selection","research.model-selection.v2"),("holdout","model_holdout","holdout","research.model-locked-holdout.v2"),
        ("summary","summary","result",summary_type)]
    if mode == "development":
        tables = [item for item in tables if item["source_node_id"] not in {"model_selection", "model_holdout"}]
        validity_ports = [item for item in validity_ports if item[0] not in {"selection", "holdout"}]
    nodes.append(node("validity","project.qlib_demo.validity",[(a,b,c) for a,b,c,_ in validity_ports],
        {"data_request_ids":["daily_feature","daily_label"],"table_bindings":{item["table_id"]:item["schema_id"] for item in tables},"metric_ref":metric_ref},"1.0.0"))
    declarations.append(("validity",declaration("validity",[(a,d) for a,_,_,d in validity_ports],[("validity","research.validity-facts.v1"),("metrics","project.qlib_demo.prediction-metrics.v1")],
        [("data_request_ids","string_list"),("table_bindings","json"),("metric_ref","string")],(summary_type,"project.qlib_demo.prediction-metrics.v1"),module="validity")))
    if mode == "portfolio":
        from portfolio_plan import extend_portfolio
        extend_portfolio(nodes, declarations, tables, design, node, declaration)
    save(package/"spec/research.yaml",{"contract_version":"research-operator-graph-package-v2","research_id":research_id,"as_of":cutoff if mode == "development" else days[-1],"root_seed":SEED,"fixed_clock":clock,
        "requests":requests,"research_semantics":semantics.to_dict(),"result":{"contract_version":"research-result-spec-v1","tables":tables},
        "graph":{"contract_version":"research-operator-graph-recipe-v1","graph_id":research_id,"nodes":nodes}})
    built=[]
    builtin=build_mainline_operator_registry()
    for name, payload in declarations:
        path=root/"declarations"/(name+".yaml")
        save(path,payload)
        decl=load_project_operator_declaration(path,source_root=HERE/"extension")
        built.append(str(compile_project_operator_bundle(source_root=HERE/"extension",output_root=root/"bundles"/name,project_id=decl.project_id,
            operator_spec=decl.operator_spec,entry_module=decl.entry_module,entry_function=decl.entry_function,dependency_lock=decl.dependency_lock,
            project_artifact_types=decl.project_artifact_types,permissions=decl.permissions,registered_operator_specs=builtin.operator_specs)))
    metric=MetricDefinition.build(metric_id=metric_ref.split("@")[0],version="1.0.0",input_artifact_type="project.qlib_demo.prediction-metrics.v1",
        result_schema_id="project.qlib_demo.metrics.v1",output_schema={"value":"float64"},unit="squared_decimal_price_change",frequency="daily",
        annualization_policy="none",risk_free_rate_policy="not_applicable",null_policy="forbid",direction="lower_is_better",
        implementation_ref="public.qlib.independent_mse",implementation_digest=hashlib.sha256((HERE/"verifier/check.py").read_bytes()).hexdigest(),
        measurement_semantics={"quantity":"prediction_error","numerator":"sum_squared_prediction_error","denominator":"validation_prediction_count" if mode == "development" else "holdout_count","observation_timing":"after_label_available","aggregation":"mean"})
    definitions = (metric,)
    if mode == "portfolio":
        from portfolio_plan import metric_definitions
        definitions += metric_definitions(HERE/"verifier/portfolio.py")
    verifier=compile_project_verifier_bundle(source_root=HERE/"verifier",output_root=root/"verifier-bundles",project_id="qlib-public",
        verifier_id="public-qlib-independent",verifier_version="1.0.0",entry_module="check",entry_function="verify",
        authorized_schema_ids=tuple(item["schema_id"] for item in tables),metric_definitions=definitions,dependency_lock={"pyarrow":"21.0.0"})
    result={"package":str(package),"extensions":built,"verifier":str(verifier),"catalog_lock":catalog_lock,"input_snapshot_manifest":str(archive),"mode":mode,"fixed_clock":clock}
    (root/"bundle-paths.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    return result


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",required=True)
    parser.add_argument("--mode",choices=("development","model","portfolio"),default="model")
    parser.add_argument("--input-config")
    args=parser.parse_args()
    print(json.dumps(prepare(args.output,args.mode,args.input_config),ensure_ascii=False,indent=2))
