"""风险项目算子的实际bundle源码闭包与端口准入。"""
from importlib.metadata import version
import importlib.util
from pathlib import Path
import shutil
import sys

import yaml


def test_risk_target_bundle_compiles_with_registered_cash_ports(tmp_path):
    from research_pipeline.extensions import load_project_operator_declaration, compile_project_operator_bundle, verify_project_operator_bundle
    from research_pipeline.runtime.operator_registry import build_mainline_operator_registry
    root = Path(__file__).resolve().parents[1]
    source = tmp_path / "source"
    source.mkdir()
    for name in ("optimizer.py", "extension.py"):
        shutil.copyfile(root/"project_extensions/qlib_portfolio_risk"/name, source/name)
    sys.path.insert(0, str(root/"examples/qlib_portfolio"))
    import prepare
    declaration = prepare.declaration("targets", [("selection", "research.model-selection.v2"), ("data", "data.columnar-bundle.v1")],
        [("targets", "research.portfolio-targets.v1")], [("design", "json"), ("price_request_id", "string")], module="extension", function="targets")
    declaration["operator"]["operator_id"] = "project.qlib_risk.targets"
    declaration["dependency_lock"].update(scipy=version("scipy"), cvxpy=version("cvxpy"), clarabel=version("clarabel"))
    path = tmp_path/"declaration.yaml"
    path.write_text(yaml.safe_dump(declaration, allow_unicode=True), encoding="utf-8")
    declared = load_project_operator_declaration(path, source_root=source)
    output = compile_project_operator_bundle(source_root=source, output_root=tmp_path/"bundles", project_id=declared.project_id,
        operator_spec=declared.operator_spec, entry_module=declared.entry_module, entry_function=declared.entry_function,
        dependency_lock=declared.dependency_lock, project_artifact_types=declared.project_artifact_types,
        permissions=declared.permissions, registered_operator_specs=build_mainline_operator_registry().operator_specs)
    manifest = verify_project_operator_bundle(output)
    assert manifest.entry_module == "extension" and manifest.entry_function == "targets"
    assert manifest.operator_spec.output_ports[0].artifact_type == "research.portfolio-targets.v1"


def test_risk_package_binds_complete_design_to_holdout(tmp_path):
    import json
    from research_pipeline.platform import typed_canonical_hash
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("risk_prepare_identity", root / "examples/qlib_risk/prepare.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    prepared = module.prepare(tmp_path / "study", method="enhanced", estimator="shrink")
    package = Path(prepared["package"])
    payload = yaml.safe_load((package / "spec/research.yaml").read_text(encoding="utf-8"))
    nodes = {item["node_id"]: item for item in payload["graph"]["nodes"]}
    design = nodes["summary"]["parameters"]["design"]
    identity = typed_canonical_hash(design)
    assert design["portfolio_risk"]["method"] == "enhanced"
    assert design["portfolio_risk"]["estimator"] == "shrink"
    assert nodes["portfolio_targets"]["parameters"]["design"] == design
    assert nodes["model_fit"]["parameters"]["research_identity_hash"] == identity
    assert nodes["model_holdout"]["parameters"]["research_identity_hash"] == identity
    assert nodes["model_holdout"]["parameters"]["package_hash"] == identity
    request = json.loads((tmp_path / "study/request.json").read_text(encoding="utf-8"))
    assert request["design"] == design
    assert request["model_parameters"]["research_identity_hash"] == identity
    sources = yaml.safe_load((package / "sources/sources.yaml").read_text(encoding="utf-8"))
    assert sources["sources"][0]["content_hash"] == identity
