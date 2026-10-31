"""构建公开示例的 Operator，并复制带生产指标定义的研究包。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil

import yaml

from research_pipeline.extensions import (
    compile_project_operator_bundle,
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

METRIC_SEMANTICS = {
    "equity_cross_section": {
        "quantity": "cross_section_outcome_spread",
        "numerator": "top_signal_outcome_return_minus_bottom_signal_outcome_return",
        "denominator": "one",
        "observation_timing": "after_outcome_session_available",
        "aggregation": "top_minus_bottom",
    },
    "etf_time_series": {
        "quantity": "signal_weighted_intraday_return",
        "numerator": "sum_signal_times_next_session_intraday_return",
        "denominator": "complete_instrument_count",
        "observation_timing": "after_outcome_session_available",
        "aggregation": "mean",
    },
    "event_study": {
        "quantity": "event_window_return",
        "numerator": "sum_eligible_event_window_returns",
        "denominator": "eligible_nonoverlapping_event_count",
        "observation_timing": "after_event_window_available",
        "aggregation": "mean",
    },
    "futures_term_structure": {
        "quantity": "annualized_term_structure_slope",
        "numerator": "deferred_settlement_over_near_settlement_minus_one_times_365",
        "denominator": "days_between_selected_expiries",
        "observation_timing": "decision_at",
        "aggregation": "selected_contract_pair",
    },
}


def build_all(examples_root: Path, output_root: Path) -> dict[str, object]:
    """构建到调用方提供的临时目录，不在示例源码旁保留 bundle。"""
    root = examples_root.resolve(strict=True)
    output = output_root.resolve()
    if output.exists():
        raise ValueError("bundle 输出目录必须不存在")
    operator_root = output / "operators"
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
            parameter_preflight_module=declaration.parameter_preflight_module,
            parameter_preflight_function=declaration.parameter_preflight_function,
        )
        package = output / name / "package"
        shutil.copytree(project / "package", package)
        specification = package / "spec/research.yaml"
        research = yaml.safe_load(specification.read_text(encoding="utf-8"))
        metadata = yaml.safe_load((package / "package.yaml").read_text(encoding="utf-8"))
        metric_ref = metadata["metric_contract"]["metrics"][0]
        metric_table = next(item for item in research["result"]["tables"] if item["role"] == "primary")
        implementation_digest = hashlib.sha256(
            (source / "operator.py").read_bytes()
        ).hexdigest()
        metric = MetricDefinition.build(
            metric_id=metric_ref.split("@")[0],
            version="1.0.0",
            input_artifact_type=metric_table["artifact_type"],
            result_schema_id=metric_table["schema_id"],
            output_schema={"value": "float64"},
            unit="annualized_decimal_slope" if name == "futures_term_structure" else "decimal_return",
            frequency="bounded_synthetic_sample",
            annualization_policy="365_days_between_expiries" if name == "futures_term_structure" else "none",
            risk_free_rate_policy="not_applicable",
            null_policy="forbid",
            direction="higher_is_better" if name in {"equity_cross_section", "etf_time_series"} else "neutral",
            implementation_ref=f"{declaration.operator_spec.operator_id}.run",
            implementation_digest=implementation_digest,
            measurement_semantics=METRIC_SEMANTICS[name],
        )
        research["metric_definitions"] = [metric.to_dict()]
        specification.write_text(yaml.safe_dump(research, allow_unicode=True, sort_keys=False), encoding="utf-8")
        projects.append({
            "name": name,
            "package": str(package),
            "operator_bundle": str(operator_bundle),
            "metric_ref": metric.metric_ref,
        })
    return {
        "schema_version": "quantwitness-example-bundles-v2",
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
