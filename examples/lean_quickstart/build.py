"""生成从零安装所需的完整合成研究包、bundle和输入归档。"""
import argparse
import json
from pathlib import Path
import shutil
import yaml
from research_pipeline.domain import InstrumentKey
from research_pipeline.domain.strategy_protocol import StrategyDeclaration, StrategyInputBinding, StrategyLimits
from research_pipeline.extensions import OperatorSpec, PortSpec, ParameterSpec, ParameterType, compile_project_operator_bundle, project_source_hash
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE, LEAN_SCHEMA_IDS, LEAN_PATHS, LEAN_METRICS_SCHEMA_ID, LEAN_METRICS_PATH
from research_pipeline.platform import typed_canonical_hash
from inputs import prepare_inputs
from lean_scope import write_synthetic_scope
import synthetic
HERE=Path(__file__).resolve().parent


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(yaml.safe_dump(value,allow_unicode=True,sort_keys=False),encoding="utf-8")


def node(name,operator,parameters,inputs=()):
    return dict(node_id=name,operator_id=operator,operator_version="1.0.0",parameters=parameters,
        inputs=[dict(input_port=p,source_node_id=n,source_output_port=o) for p,n,o in inputs],strategies=[])


def build(output,frequency):
    root=Path(output).resolve()
    if root.exists():
        raise ValueError("输出目录必须不存在")
    root.mkdir(parents=True)
    package=root/"package"
    reference=write_synthetic_scope(package,days=list(synthetic.DAYS),entities=[synthetic.CODE],clock=synthetic.CLOCK)
    catalog,requests,archive=prepare_inputs(root,frequency)
    instrument=InstrumentKey(synthetic.CODE,"cn_etf","XSHG","CNY","etf").to_dict()
    declaration=StrategyDeclaration(strategy_id="public.lean.quickstart",bundle_id="pending",state_schema_id="public.lean.quickstart.state.v1",
        subscriptions=("bar",),schedules=(),asset_class="cn_etf",frequency=frequency,
        input_bindings=(StrategyInputBinding(binding_id="prices",columns=("close",),lookback_bars=1),),
        limits=StrategyLimits(max_state_bytes=4096,max_context_bytes=8*1024**2,max_response_bytes=65536,max_intents_per_event=1,callback_wall_seconds=30))
    parameters=dict(execution_mode="strategy",frequency=frequency,price_scale=3,
        execution_model="lean_next_session_open_v1" if frequency=="1d" else "lean_next_completed_close_v1",
        claim_ceiling="bar_level_research_only",strategy_declaration=declaration.to_dict(),instruments=[instrument],
        minute_scope=reference,rule_bundle_hash=reference["rules_hash"],initial_cash_units=1000000,
        slippage_bps=0,slippage_ticks=0,participation_ppm=1000000)
    if frequency=="1d":
        parameters.update(bars_request_id="portfolio_bars",open_request_id="portfolio_open")
    source=root/"strategy-source"; source.mkdir()
    shutil.copyfile(HERE/"strategy.py",source/"strategy.py")
    spec=OperatorSpec.build(operator_id="project.public.lean_quickstart",operator_version="1.0.0",
        input_ports=(PortSpec("bars","data.columnar-bundle.v1" if frequency=="1d" else "data.minute-bars.v1"),PortSpec("snapshot","data.adjustment-factor-snapshot.v1")),
        output_ports=(PortSpec("simulation",LEAN_ARTIFACT_TYPE),),
        parameters=tuple(ParameterSpec(key,ParameterType.STRING if isinstance(value,str) else ParameterType.INTEGER if type(value)is int else ParameterType.JSON) for key,value in parameters.items()),
        strategy_roles=(),resource_profile=dict(memory_bytes=12*1024**3,cpu_slots=4,process_slots=6,temp_bytes=32*1024**3,wall_seconds=120),
        determinism_mode="deterministic",seed_policy="none",code_hash=project_source_hash(source),pit_capabilities=("pit.as_of.v1",))
    bundle=compile_project_operator_bundle(source_root=source,output_root=root/"bundles",project_id="public-lean-quickstart",
        operator_spec=spec,entry_module="strategy",entry_function="run",dependency_lock={},
        registered_operator_specs=build_mainline_operator_registry().operator_specs,project_artifact_types=(LEAN_ARTIFACT_TYPE,))
    parameters["strategy_declaration"]["bundle_id"]=bundle.name
    daily_ids=[q["request_id"] for q in requests if q["request_id"]!="minute_bars"]
    nodes=[node("data","data.columnar.materialize",{}),
        node("snapshot","data.minute.adjustment_snapshot",dict(factor_request_id="portfolio_factors",corporate_action_request_id="portfolio_actions",
            instrument_ids=[synthetic.CODE],asset_class="cn_etf",as_of=synthetic.CLOCK,
            applicable_start=synthetic.DAYS[1]+"T09:30:00+08:00",applicable_end=synthetic.DAYS[-1]+"T15:01:00+08:00",
            availability_policy_ref="available.etf.corporate_action.v1"),[("data","data","data")])]
    bars=("bars","data","data")
    if frequency=="1m":
        from minute_inputs import scan_parameters
        nodes += [node("scan","data.minute.scan",scan_parameters()),
            node("bars","research.bars.minute_resample",dict(interval_minutes=1,session_policy_ref="session.public.synthetic.etf.v1",
                quality_policy_refs=["quality.minute.common_fields.v1","quality.minute.fund.v1"],adjustment_mode="none",asset_class="cn_etf",
                availability_policy_ref="available.minute.completed_bar.v1",minute_scope=reference),[("minute_1m","scan","minute_1m")])]
        bars=("bars","bars","bars")
    nodes.append(node("strategy",spec.operator_id,parameters,[bars,("snapshot","snapshot","snapshot")]))
    tables=[dict(table_id="source_snapshot",role="primary",source_node_id="snapshot",source_port="snapshot",
        artifact_type="data.adjustment-factor-snapshot.v1",schema_id="data.adjustment-factor-snapshot.payload.v1",path_prefix="snapshot")]
    save(package/"package.yaml",dict(package_slug="lean_quickstart_"+frequency,display_name="公开LEAN合成买卖示例",package_version="1.0.0",builder_id="operator_graph_plan_v1",
        metric_contract=dict(contract_id="public.lean.quickstart.metrics",version="1.0.0",metrics=["project.lean.total_return@1.0.0"],semantics={"project.lean.total_return@1.0.0":"last_equity_div_initial_equity_minus_one"}),
        claim_contract=dict(allowed_claim_levels=["research_observation"],max_claim_level="research_observation")))
    save(package/"sources/sources.yaml",{"sources":[dict(source_id="public_synthetic",source_type="dataset",title="公开确定性合成ETF行情",
        url="https://github.com/ljjtim/QuantWitness",accessed_at="2024-01-12",content_hash=typed_canonical_hash({"days":synthetic.DAYS,"prices":synthetic.PRICES}),license_id="Apache-2.0",status="available",limitation="虚构证券与合成价格，仅验证执行合同",
        provenance=dict(mode="citation_only",content_digest=None,media_type=None,snapshot_artifact_id=None,snapshot_manifest_hash=None,importer_id=None,imported_at=None,contract_version="research-source-provenance-v1"))]})
    save(package/"localization.yaml",{"decisions":[dict(decision_id="synthetic",original_assumption="真实市场行情",local_market="cn_etf",
        local_adaptation="虚构ETF及预先定义的合成市场",evidence_source_ids=["public_synthetic"],status="proxy",claim_effect="downgrade_to_research_observation")]})
    save(package/"spec/research.yaml",dict(contract_version="research-operator-graph-package-v2",research_id="public.lean.quickstart."+frequency,
        as_of="2024-01-11T23:59:59+08:00",root_seed=1703,fixed_clock=synthetic.CLOCK,requests=requests,
        result=dict(contract_version="research-result-spec-v1",tables=tables),graph=dict(contract_version="research-operator-graph-recipe-v1",graph_id="public.lean.quickstart",nodes=nodes)))
    from research_pipeline.packages import load_research_package
    load_research_package(package)
    summary=dict(package=str(package),bundles=[str(bundle)],catalog_lock=str(root/"catalog"),input_snapshot_manifest=str(archive),
        minute_root=str(root/"minute") if frequency=="1m" else None,frequency=frequency,fixed_clock=synthetic.CLOCK)
    (root/"generated.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    return summary

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",required=True,type=Path)
    parser.add_argument("--frequency",choices=("1d","1m"),default="1d")
    args=parser.parse_args()
    print(json.dumps(build(args.output,args.frequency),ensure_ascii=False,indent=2))
