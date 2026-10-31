"""将完整 ResearchPackage 的唯一项目策略转换为正式 LEAN 后端声明。"""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sys

import yaml


def read_mapping(path):
    text = Path(path).read_text(encoding="utf-8")
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = yaml.safe_load(text)
    if not isinstance(value, dict):
        raise ValueError(f"声明必须是对象：{path}")
    return value


def write_mapping(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def bundle_catalog(package):
    """只查找包的配套 bundle，不使用运行目录或历史结果。"""
    from research_pipeline.extensions import verify_project_operator_bundle

    catalog = {}
    for root in (package / "bundles", package.parent / "bundles"):
        if root.is_dir():
            for path in root.iterdir():
                if path.is_dir() and (path / "manifest.json").is_file():
                    manifest = verify_project_operator_bundle(path)
                    key = manifest.operator_spec.operator_id, manifest.operator_spec.operator_version
                    if key in catalog and catalog[key][1].bundle_hash != manifest.bundle_hash:
                        raise ValueError(f"配套 bundle 有多个实现：{key}")
                    catalog[key] = path, manifest
    return catalog


def rebuild_bundle(path, manifest, spec, output, registered_specs, *, source_root=None):
    from research_pipeline.extensions import compile_project_operator_bundle

    preflight = manifest.to_dict().get("parameter_preflight")
    options = {} if not preflight else {
        "parameter_preflight_module": preflight["module"],
        "parameter_preflight_function": preflight["function"],
        "parameter_preflight_parameters": tuple(preflight["parameters"]),
    }
    raw = manifest.to_dict()
    return compile_project_operator_bundle(
        source_root=source_root or path / "sources", output_root=output,
        project_id=manifest.project_id, operator_spec=spec,
        entry_module=raw["entry"]["module"], entry_function=raw["entry"]["function"],
        dependency_lock=dict(manifest.dependency_lock), registered_operator_specs=registered_specs,
        project_artifact_types=("research.lean-simulation.v1",),
        permissions=manifest.permissions, reuse_scope=manifest.reuse_scope, **options,
    )


def prepare_package(from_package, output, lean_home):
    from research_pipeline.extensions import (
        OperatorSpec, ParameterSpec, ParameterType, PortSpec,
    )
    from research_pipeline.packages import load_research_package
    from research_pipeline.packages.models import MetricContract, ResearchPackage
    from research_pipeline.packages.store import PACKAGE_FILES, PackageDocuments
    from research_pipeline.platform.lean_runtime import freeze_lean_runtime, validate_lean_runtime
    from research_pipeline.platform.lean_contract import (
        LEAN_ARTIFACT_TYPE, LEAN_PATHS, LEAN_SCHEMA_IDS, LEAN_SIGNAL_SOURCE_SCHEMA_ID,
        LEAN_METRICS_SCHEMA_ID, LEAN_METRICS_PATH,
    )
    from research_pipeline.runtime.operator_registry import build_mainline_operator_registry

    package, output, lean_home = Path(from_package).resolve(), Path(output).resolve(), Path(lean_home).resolve()
    if output.exists():
        raise ValueError("输出目录必须不存在")
    payloads = {name: read_mapping(package / name) for name in PACKAGE_FILES}
    # 迁移来源只验证完整声明与输入闭包；执行准入在转换完成后进行。
    source_package = load_research_package(package, documents=PackageDocuments(package, payloads, {}))
    research = payloads["spec/research.yaml"]
    original = deepcopy(research)
    strategies = [node for node in research["graph"]["nodes"] if "strategy_declaration" in node["parameters"]]
    if len(strategies) != 1:
        raise ValueError("原完整包必须恰好包含一个项目策略节点")
    strategy = strategies[0]
    initial_cash = strategy["parameters"].get("initial_cash_units")
    if type(initial_cash) is not int or initial_cash <= 0:
        raise ValueError("LEAN研究初始资金必须为正整数分")
    if {i["input_port"] for i in strategy["inputs"]} not in ({"bars", "snapshot"}, {"bars", "snapshot", "signals"}):
        raise ValueError("转换要求原策略直接绑定bars、snapshot及可选signals")
    catalog = bundle_catalog(package)
    path, manifest = catalog[strategy["operator_id"], strategy["operator_version"]]
    if manifest.bundle_hash != strategy["parameters"]["strategy_declaration"]["bundle_id"]:
        raise ValueError("原策略声明与配套bundle身份不一致")
    from research_pipeline.packages.project_strategy_admission import strategy_bars_frequency
    frequency = strategy_bars_frequency(strategy["parameters"], {
        port.port: port.artifact_type for port in manifest.operator_spec.input_ports})
    execution_model = "lean_next_session_open_v1" if frequency == "1d" else "lean_next_completed_close_v1"
    declaration_frequency = strategy["parameters"]["strategy_declaration"].get("frequency", "1m")
    budget = dict(manifest.operator_spec.resource_profile)
    minimum_slots = 3 + int(sys.platform == "win32" and sys.prefix != sys.base_prefix)
    if budget["process_slots"] < minimum_slots:
        raise ValueError(f"原包策略预算须至少{minimum_slots}个进程槽；转换不自动扩大额度")
    if strategy["parameters"].get("execution_backend") == "lean":
        runtime = validate_lean_runtime(strategy["parameters"]["lean_runtime"], verify_files=False)
        if Path(runtime["installation_root"]).resolve() != lean_home:
            raise ValueError("研究声明绑定的 LEAN 安装与本次 lean-home 不一致")
        if declaration_frequency == frequency:
            output.mkdir(parents=True)
            summary = {"package": str(package), "bundles": [str(path) for path, _ in catalog.values()],
                       "from_package": str(package), "execution_backend": "lean", "run_budget": budget,
                       "database_access": "not_opened", "research_executed": False}
            write_mapping(output / "conversion.json", summary)
            return summary, source_package
    registry = build_mainline_operator_registry()
    runtime = (runtime if strategy["parameters"].get("execution_backend") == "lean"
               else freeze_lean_runtime(lean_home))
    parameters = tuple(replace(item, allowed_values=(execution_model,))
                       if item.name == "execution_model" else item for item in manifest.operator_spec.parameters
                       if item.name not in {"execution_backend", "lean_runtime"})
    parameters += (ParameterSpec("execution_backend", ParameterType.STRING, allowed_values=("lean",)),
                   ParameterSpec("lean_runtime", ParameterType.JSON))
    spec = OperatorSpec.build(
        operator_id=manifest.operator_spec.operator_id, operator_version=manifest.operator_spec.operator_version,
        input_ports=manifest.operator_spec.input_ports,
        output_ports=(PortSpec("simulation", LEAN_ARTIFACT_TYPE),), parameters=parameters,
        strategy_roles=manifest.operator_spec.strategy_roles, resource_profile=budget,
        determinism_mode=manifest.operator_spec.determinism_mode, seed_policy=manifest.operator_spec.seed_policy,
        code_hash=manifest.operator_spec.code_hash, pit_capabilities=manifest.operator_spec.pit_capabilities,
    )
    output.mkdir(parents=True)
    bundle_output = output / "bundles"
    registered_specs = (*registry.operator_specs, *(m.operator_spec for _, m in catalog.values()))
    execution_bundle = rebuild_bundle(path, manifest, spec, bundle_output, registered_specs)
    strategy["parameters"].update(execution_backend="lean", lean_runtime=runtime,
                                  execution_model=execution_model)
    strategy["parameters"]["strategy_declaration"]["bundle_id"] = execution_bundle.name
    strategy["parameters"]["strategy_declaration"]["frequency"] = frequency
    bundles = [execution_bundle]
    retired = set()
    for node in research["graph"]["nodes"]:
        entry = catalog.get((node["operator_id"], node["operator_version"]))
        if node["operator_id"].startswith("research.validity.") or (
            entry is not None and any(port.artifact_type == "research.validity-facts.v1"
                                      for port in entry[1].operator_spec.output_ports)
        ):
            retired.add(node["node_id"])
    research["graph"]["nodes"] = [node for node in research["graph"]["nodes"] if node["node_id"] not in retired]
    research["result"]["tables"] = [table for table in research["result"]["tables"] if table["source_node_id"] not in retired]
    replaced = {strategy["operator_id"]}
    for node in research["graph"]["nodes"]:
        key = node["operator_id"], node["operator_version"]
        if key in catalog and node["operator_id"] not in replaced:
            old_path, _ = catalog[key]
            destination = bundle_output / old_path.name
            shutil.copytree(old_path, destination)
            bundles.append(destination)
    research["result"]["tables"] = [t for t in research["result"]["tables"] if t["source_node_id"] != strategy["node_id"]]
    signal = next((binding for binding in strategy["inputs"] if binding["input_port"] == "signals"), None)
    for table in research["result"]["tables"]:
        if (signal is not None and table["schema_id"] == "research.strategy.signal-source.v1"
                and table["source_node_id"] == signal["source_node_id"]
                and table["source_port"] == signal["source_output_port"]
                and table["artifact_type"] == "research.feature-set.v1"):
            table["schema_id"] = LEAN_SIGNAL_SOURCE_SCHEMA_ID
    research["result"]["tables"].extend({
        "table_id": "lean_" + name, "role": "diagnostic", "source_node_id": strategy["node_id"],
        "source_port": "simulation", "artifact_type": LEAN_ARTIFACT_TYPE,
        "schema_id": schema, "path_prefix": LEAN_PATHS[name],
    } for name, schema in LEAN_SCHEMA_IDS.items())
    research["result"]["tables"].append({
        "table_id": "lean_metrics", "role": "diagnostic", "source_node_id": strategy["node_id"],
        "source_port": "simulation", "artifact_type": LEAN_ARTIFACT_TYPE,
        "schema_id": LEAN_METRICS_SCHEMA_ID, "path_prefix": LEAN_METRICS_PATH,
    })
    if research["requests"] != original["requests"]:
        raise ValueError("转换不得改变原数据请求")
    destination = output / "package"
    shutil.copytree(package, destination)
    write_mapping(destination / "spec/research.yaml", research)
    from research_pipeline.research.lean_metrics import lean_metric_definitions
    metadata = deepcopy(payloads["package.yaml"])
    for definition in lean_metric_definitions():
        if definition.metric_ref not in metadata["metric_contract"]["metrics"]:
            metadata["metric_contract"]["metrics"].append(definition.metric_ref)
        metadata["metric_contract"]["semantics"][definition.metric_ref] = definition.measurement_semantics.aggregation
    write_mapping(destination / "package.yaml", metadata)
    # 同次转换复用已验证的完整来源；新声明仍经过正式构造器与图编译。
    metric = metadata["metric_contract"]
    converted = ResearchPackage.build(
        package_slug=source_package.package_slug, display_name=source_package.display_name,
        package_version=source_package.package_version, builder_id=source_package.builder_id,
        sources=source_package.sources, localizations=source_package.localizations,
        metric_contract=MetricContract.build(
            contract_id=metric["contract_id"], version=metric["version"],
            metrics=tuple(metric["metrics"]), semantics=metric["semantics"],
        ),
        claim_contract=source_package.claim_contract, spec_payload=research,
        minute_scope_inputs={name: replace(scope, root=destination)
                             for name, scope in source_package.minute_scope_inputs.items()},
    )
    summary = {"package": str(destination), "bundles": [str(p) for p in bundles],
               "from_package": str(package), "execution_backend": "lean", "run_budget": budget,
               "database_access": "not_opened", "research_executed": False}
    write_mapping(output / "conversion.json", summary)
    return summary, converted


def convert(from_package, output, lean_home):
    """准备可单独准入的声明，命令行只返回持久路径。"""
    summary, _ = prepare_package(from_package, output, lean_home)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-package", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lean-home", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(convert(args.from_package, args.output, args.lean_home), ensure_ascii=False))
