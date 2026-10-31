"""公开合成研究的正式 Result、计划绑定与只读源库回归。"""

from __future__ import annotations

from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml

from examples.build_bundles import build_all
from examples.prepare_synthetic_environment import prepare
from research_pipeline.cli import main
from research_pipeline.results import (
    ResultContractError, ResultMetric, ResultStore, compare_results, load_result_context, render_result_report,
)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from build_gate_c_evidence import build_gate_c_evidence, _numeric_acceptance  # noqa: E402


EXPECTED_METRICS = {
    "equity_cross_section": {
        "metric_ref": "project.quantwitness.equity_cross_section.spread@1.0.0",
        "value": (12.3 / 12.0 - 1) - (8.8 / 9.0 - 1), "unit": "decimal_return", "sample_size": 4,
    },
    "etf_time_series": {
        "metric_ref": "project.quantwitness.etf_time_series.signal_return@1.0.0",
        "value": 0.05 / 2, "unit": "decimal_return", "sample_size": 2,
    },
    "event_study": {
        "metric_ref": "project.quantwitness.event_study.mean_window_return@1.0.0",
        "value": (0.05 - 0.01) / 2, "unit": "decimal_return", "sample_size": 2,
    },
    "futures_term_structure": {
        "metric_ref": "project.quantwitness.futures_term_structure.annualized_slope@1.0.0",
        "value": 0.03 * 365 / 29, "unit": "annualized_decimal_slope", "sample_size": 2,
    },
}


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _cli(*args):
    capture = io.StringIO()
    with redirect_stdout(capture):
        code = main([*args, "--json"])
    assert code == 0, capture.getvalue()
    return json.loads(capture.getvalue().strip().splitlines()[-1])["data"]


@pytest.fixture(scope="module")
def gate_inputs(tmp_path_factory):
    work = tmp_path_factory.mktemp("gate-c-formal")
    environment = prepare(work / "synthetic-environment")
    projects = build_all(ROOT / "examples", work / "bundles")["projects"]
    database = Path(environment["database"])
    before = {"size_bytes": database.stat().st_size, "mtime_ns": database.stat().st_mtime_ns}
    references = []
    for project in projects:
        name = project["name"]
        target = work / name
        target.mkdir()
        plan_root = target / "plan"
        _cli(
            "package", "admit", "--package", project["package"],
            "--extension-bundle", project["operator_bundle"],
            "--catalog-lock", environment["catalog_lock"],
            "--data-db", str(database), "--output", str(plan_root),
        )
        specification = yaml.safe_load((Path(project["package"]) / "spec/research.yaml").read_text(encoding="utf-8"))
        store = target / "results"
        process = subprocess.run([
            sys.executable, "-B", "-m", "research_pipeline", "run",
            "--plan", str(plan_root), "--data-db", str(database),
            "--artifact-root", str(target / "artifacts"),
            "--handoff-out", str(target / "handoff.json"),
            "--run-root", str(target / "run"), "--result-store", str(store),
            "--clock", specification["fixed_clock"], "--root-seed", str(specification["root_seed"]), "--json",
        ], cwd=target, env={**os.environ, "PYTHONPATH": str(ROOT / "src")}, capture_output=True, text=True, encoding="utf-8", timeout=120)
        assert process.returncode == 0, process.stdout + process.stderr
        result = Path(json.loads(process.stdout)["data"]["result_directory"])
        context = load_result_context(result, result_store=store)
        assert context.snapshot.bundle.run.status == "succeeded"
        after = {"size_bytes": database.stat().st_size, "mtime_ns": database.stat().st_mtime_ns}
        assert before == after
        plan = _json(plan_root / "operator-graph-plan.json")
        references.append({
            "reference": name, "dag_family": name,
            "package": project["package"], "plan": str(plan_root / "operator-graph-plan.json"),
            "result_directory": str(result), "result_store": str(store),
            "expected_graph_id": plan["recipe"]["graph_id"],
            "expected_result_id": context.snapshot.bundle.result_id,
            "expected_metrics": [dict(EXPECTED_METRICS[name])],
            "study_request_ids": [row["request_id"] for row in plan["requests"]],
            "database_evidence": {
                "data_scope": "synthetic", "database_path": str(database),
                "result_id": context.snapshot.bundle.result_id,
                "read_only": True, "database_unchanged": True, "before": before, "after": after,
            },
        })
    return references


def test_four_public_results_keep_plan_identity_and_readonly_sources(gate_inputs):
    assert len(gate_inputs) == 4
    result_ids = set()
    for reference in gate_inputs:
        context = load_result_context(reference["result_directory"], result_store=reference["result_store"])
        bundle = context.snapshot.bundle
        plan = _json(Path(reference["plan"]))
        assert bundle.contract_version == "research-result-v4"
        assert bundle.status == "finalized" and bundle.run.status == "succeeded"
        assert bundle.result_id == reference["expected_result_id"]
        assert bundle.plan_hash == plan["plan_hash"]
        assert bundle.package_hash == plan["package_hash"]
        assert bundle.run.fixed_clock == plan["fixed_clock"]
        assert bundle.result_spec.to_dict() == plan["result_spec"]
        assert {item.request_id for item in bundle.input_revisions} == set(reference["study_request_ids"])
        assert reference["database_evidence"]["before"] == reference["database_evidence"]["after"]
        assert bundle.metric_definitions and context.metrics
        report = render_result_report(context)
        assert bundle.result_id in report
        assert all(metric.metric_ref in report for metric in context.metrics)
        result_ids.add(bundle.result_id)
    assert len(result_ids) == 4


def test_public_result_metrics_read_after_relocation(tmp_path, gate_inputs):
    for index, reference in enumerate(gate_inputs):
        source = Path(reference["result_directory"])
        original = load_result_context(source, result_store=reference["result_store"])
        store = tmp_path / f"results-{index}"
        destination = store / source.relative_to(reference["result_store"])
        shutil.copytree(source, destination)
        copied = load_result_context(destination, result_store=store)
        assert copied.snapshot.bundle == original.snapshot.bundle
        assert copied.metrics == original.metrics
        comparison = compare_results(original, copied)
        assert comparison.comparable
        assert comparison.left_result_id == comparison.right_result_id
        assert all(row["delta"] == 0 for row in comparison.metric_comparisons)


def test_requested_diagnostic_table_rejects_modified_content(tmp_path, gate_inputs):
    reference = gate_inputs[0]
    source = Path(reference["result_directory"])
    store = tmp_path / "results"
    result = store / source.relative_to(reference["result_store"])
    shutil.copytree(source, result)
    manifest = _json(result / "result.json")
    table = next(row for row in manifest["tables"] if row["role"] == "diagnostic")
    with (result / next(iter(table["files"]))).open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(ResultContractError):
        load_result_context(result, result_store=store, additional_table_ids=(table["table_id"],))
    references = deepcopy(gate_inputs)
    references[0].update(result_directory=str(result), result_store=str(store))
    with pytest.raises(ResultContractError):
        build_gate_c_evidence(input_path=_gate_input_file(tmp_path, references), output=tmp_path / "evidence")


def test_result_rejects_modified_plan_identity(tmp_path, gate_inputs):
    reference = gate_inputs[0]
    source = Path(reference["result_directory"])
    store = tmp_path / "results"
    result = store / source.relative_to(reference["result_store"])
    shutil.copytree(source, result)
    manifest = _json(result / "result.json")
    manifest["plan_hash"] = "f" * 64
    (result / "result.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ResultContractError):
        load_result_context(result, result_store=store)


def test_result_identity_lookup_rejects_other_result(gate_inputs):
    first, second = gate_inputs[:2]
    context = load_result_context(first["result_directory"], result_store=first["result_store"])
    bundle = context.snapshot.bundle
    with pytest.raises(ResultContractError):
        ResultStore(first["result_store"], create=False).load_by_identity(
            project_id=bundle.project_id, run_id=bundle.run_id,
            result_id=second["expected_result_id"],
        )


def _gate_input_file(root: Path, references: list[dict]) -> Path:
    path = root / "gate-c-input.json"
    path.write_text(json.dumps({
        "contract_version": "research-gate-c-input-v2", "data_scope": "synthetic",
        "minimum_dag_families": 3, "maximum_window_days": 400, "references": references,
    }), encoding="utf-8")
    return path


def test_gate_c_accepts_four_result_outputs_and_hand_calculated_metrics(tmp_path, gate_inputs):
    output = tmp_path / "evidence"
    evidence = build_gate_c_evidence(input_path=_gate_input_file(tmp_path, gate_inputs), output=output)
    assert evidence == _json(output / "gate-c-evidence.json")
    assert evidence["contract_version"] == "research-gate-c-evidence-v3"
    assert evidence["status"] == "pass"
    assert evidence["evidence_scope"] == "diagnostic_unbound"
    assert evidence["acceptance_scope"] == "engineering"
    assert evidence["acceptance_basis"] == "result_contract_and_metric_values"
    assert evidence["reference_count"] == evidence["accepted_metric_count"] == 4
    assert evidence["all_results_contract"] == "research-result-v4"
    assert evidence["dag_family_count"] == 4
    assert "all_verifications_contract" not in evidence and "independent_oracle_count" not in evidence
    for reference in evidence["references"]:
        assert reference["execution_status"] == "succeeded"
        assert reference["metric_count"] == 1
        assert "verification_status" not in reference and "oracle_identity" not in reference
        expected = EXPECTED_METRICS[reference["reference"]]
        metric, = reference["metrics"]
        assert metric["metric_ref"] == expected["metric_ref"]
        assert metric["expected_value"] == expected["value"]
        assert metric["absolute_error"] <= metric["absolute_tolerance"] == 1e-12
        assert metric["sample_start"] and metric["sample_end"]
        assert metric["unit"] == expected["unit"]
        assert metric["sample_size"] == expected["sample_size"]


@pytest.mark.parametrize("field,value,match", [
    ("value", 0.1, "指标数值"),
    ("unit", "percent_return", "单位或样本量"),
    ("sample_size", 1, "单位或样本量"),
    ("metric_ref", "project.unrelated@1.0.0", "预期指标集合"),
])
def test_gate_c_rejects_different_numeric_expectation(tmp_path, gate_inputs, field, value, match):
    references = deepcopy(gate_inputs)
    references[0]["expected_metrics"][0][field] = value
    output = tmp_path / "evidence"
    with pytest.raises(ValueError, match=match):
        build_gate_c_evidence(input_path=_gate_input_file(tmp_path, references), output=output)
    assert not output.exists()


@pytest.mark.parametrize("change,match", [
    ({"expected_graph_id": "other_graph"}, "DAG 身份"),
    ({"expected_result_id": "other_result"}, "Result 身份"),
    ({"study_request_ids": ["other_request"]}, "正式输入"),
    ({"expected_metrics": []}, "非空指标预期"),
])
def test_gate_c_keeps_reference_bindings(tmp_path, gate_inputs, change, match):
    references = deepcopy(gate_inputs)
    references[0].update(change)
    with pytest.raises(ValueError, match=match):
        build_gate_c_evidence(input_path=_gate_input_file(tmp_path, references), output=tmp_path / "evidence")


@pytest.mark.parametrize("field,value,match", [
    ("value", float("nan"), "有限数值"),
    ("value", float("inf"), "有限数值"),
    ("value", True, "有限数值"),
    ("sample_size", 0, "正整数"),
])
def test_numeric_acceptance_rejects_invalid_expectations(field, value, match):
    expected = dict(EXPECTED_METRICS["event_study"])
    metric = ResultMetric(metric_ref=expected["metric_ref"], value=expected["value"], unit=expected["unit"],
        sample_start="2024-01-03", sample_end="2024-01-05", sample_size=2, status="computed", table_id="metrics")
    expected[field] = value
    with pytest.raises(ValueError, match=match):
        _numeric_acceptance({"expected_metrics": [expected]}, (metric,))
