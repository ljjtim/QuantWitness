"""从四个公开示例的源码和薄声明构建临时 Operator/Verifier bundles。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import yaml

from research_pipeline.extensions import (
    compile_project_operator_bundle,
    compile_project_verifier_bundle,
    load_project_operator_declaration,
)
from research_pipeline.platform.metric_contracts import MetricDefinition
from research_pipeline.runtime.operator_registry import build_mainline_operator_registry


PROJECTS = (
    "equity_cross_section",
    "etf_time_series",
    "event_study",
    "futures_term_structure",
)


def build_all(examples_root: Path, output_root: Path) -> dict[str, object]:
    """构建到调用方提供的临时目录，不在示例源码旁保留 bundle。"""
    root = examples_root.resolve(strict=True)
    output = output_root.resolve()
    if output.exists():
        raise ValueError("bundle 输出目录必须不存在")
    operator_root = output / "operators"
    verifier_root = output / "verifiers"
    builtin = build_mainline_operator_registry()
    projects = []
    for name in PROJECTS:
        project = root / name
        source = project / "extension/source"
        declaration = load_project_operator_declaration(
            project / "extension/operator.yaml", source_root=source
        )
        operator_bundle = compile_project_operator_bundle(
            source_root=source,
            output_root=operator_root / name,
            project_id=declaration.project_id,
            operator_spec=declaration.operator_spec,
            entry_module=declaration.entry_module,
            entry_function=declaration.entry_function,
            dependency_lock=declaration.dependency_lock,
            registered_operator_specs=builtin.operator_specs,
            project_artifact_types=declaration.project_artifact_types,
            permissions=declaration.permissions,
        )
        definition = yaml.safe_load(
            (project / "verifier/definition.yaml").read_text(encoding="utf-8")
        )
        verifier_source = project / "verifier/source"
        implementation_digest = hashlib.sha256(
            (verifier_source / "check.py").read_bytes()
        ).hexdigest()
        metric = MetricDefinition.build(
            metric_id=definition["metric_id"],
            version="1.0.0",
            input_artifact_type=definition["artifact_type"],
            result_schema_id=definition["metric_schema_id"],
            output_schema={"value": "float64"},
            unit=definition["unit"],
            frequency="bounded_synthetic_sample",
            annualization_policy="none",
            risk_free_rate_policy="not_applicable",
            null_policy="forbid",
            direction=definition["direction"],
            implementation_ref=f"{definition['metric_id']}.independent_value",
            implementation_digest=implementation_digest,
        )
        verifier_bundle = compile_project_verifier_bundle(
            source_root=verifier_source,
            output_root=verifier_root / name,
            project_id=definition["project_id"],
            verifier_id=definition["verifier_id"],
            verifier_version="1.0.0",
            entry_module="check",
            entry_function="verify",
            authorized_schema_ids=(
                definition["metric_schema_id"],
                definition["observation_schema_id"],
            ),
            metric_definitions=(metric,),
            dependency_lock={"pyarrow": "21.0.0"},
        )
        projects.append({
            "name": name,
            "package": str(project / "package"),
            "operator_bundle": str(operator_bundle),
            "verifier_bundle": str(verifier_bundle),
            "metric_ref": metric.metric_ref,
        })
    return {
        "schema_version": "quantwitness-example-bundles-v1",
        "status": "pass",
        "projects": projects,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="构建 QuantWitness 公开示例 bundles")
    parser.add_argument("--examples-root", default=str(Path(__file__).parent))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    payload = build_all(Path(args.examples_root), Path(args.output))
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
