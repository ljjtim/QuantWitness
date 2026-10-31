from __future__ import annotations

import hashlib
import json
import runpy
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from examples.build_bundles import PROJECTS, build_all
from examples.prepare_synthetic_environment import prepare
from research_pipeline.catalog import CompiledCatalog
from research_pipeline.cli import main
from research_pipeline.results import ResultContext, load_result_context
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest


ROOT = Path(__file__).resolve().parents[1]


def test_public_qlib_prediction_metric_matches_hand_calculated_mse() -> None:
    calculate = runpy.run_path(
        ROOT / "examples/qlib_portfolio/extension/operator.py"
    )["prediction_metric"]
    metric = calculate([
        {"observation_session": "2024-01-05", "prediction": 0.04, "actual": 0.01},
        {"observation_session": "2024-01-03", "prediction": -0.01, "actual": 0.01},
    ], "project.qlib_demo.validation_mse@1.0.0")
    assert abs(metric["value"] - 0.00065) < 1e-12
    assert metric["unit"] == "squared_decimal_price_change"
    assert (metric["sample_start"], metric["sample_end"], metric["sample_size"]) == (
        "2024-01-03", "2024-01-05", 2,
    )


def test_public_qlib_development_complete_cli_workflow(tmp_path: Path) -> None:
    pytest.importorskip("qlib")
    output = tmp_path / "development"
    completed = subprocess.run([
        sys.executable, "-B", str(ROOT / "examples/qlib_portfolio/run.py"),
        "--mode", "development", "--output", str(output),
    ], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=600, check=False)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    receipt = json.loads((output / "run-receipt.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "result_finalized"
    context = load_result_context(receipt["result_directory"], result_store=output / "results",
        additional_table_ids=("validation_predictions", "samples", "holdout_index"))
    tables = {table.table_id: table for table in context.snapshot.bundle.tables}
    assert {"selection", "test_predictions", "holdout_predictions", "holdout_receipt"}.isdisjoint(tables)
    assert context.snapshot.read_table(tables["holdout_index"].schema_id).num_rows == 0
    predictions = context.snapshot.read_table(tables["validation_predictions"].schema_id).to_pylist()
    assert predictions
    assert {row["stage"] for row in predictions} == {"validation"}
    metric, = context.metrics
    assert metric.metric_ref == "project.qlib_demo.validation_mse@1.0.0"
    expected_mse = sum((row["prediction"] - row["actual"]) ** 2 for row in predictions) / len(predictions)
    assert metric.value == pytest.approx(expected_mse, rel=0, abs=1e-12)
    assert metric.sample_size == len(predictions)
    request = json.loads((output / "request.json").read_text(encoding="utf-8"))
    boundary = datetime.fromisoformat(request["design"]["holdout_start"])
    samples = context.snapshot.read_table(tables["samples"].schema_id).to_pylist()
    assert all(row["label_end_time"] < boundary and row["label_available_time"] <= boundary for row in samples)
    assert not list(output.rglob("opened.json"))
    report = (output / "report.md").read_text(encoding="utf-8")
    assert receipt["result_id"] in report
    assert metric.metric_ref in report


def _last_payload(capsys) -> dict[str, object]:
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_public_examples_have_licensed_deterministic_sources() -> None:
    for name in PROJECTS:
        project = ROOT / "examples" / name
        source = project / "synthetic.py"
        payload = yaml.safe_load(
            (project / "package/sources/sources.yaml").read_text(encoding="utf-8")
        )["sources"][0]
        assert payload["license_id"] == "Apache-2.0"
        assert payload["status"] == "available"
        canonical_source = (
            source.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        )
        assert payload["content_hash"] == hashlib.sha256(canonical_source).hexdigest()
        assert (project / "package/spec/research.yaml").is_file()
        assert (project / "extension/operator.yaml").is_file()
        assert (project / "extension/source/operator.py").is_file()


def test_public_examples_build_and_lint_without_public_registry_entries(
    tmp_path: Path,
    capsys,
) -> None:
    templates = {
        name: (ROOT / "examples" / name / "package/spec/research.yaml").read_bytes()
        for name in PROJECTS
    }
    output = tmp_path / "bundles"
    payload = build_all(ROOT / "examples", output)
    assert payload["status"] == "pass"
    public_operator_ids = {
        item.name for item in build_mainline_operator_manifest().definitions
    }
    for project in payload["projects"]:
        name = project["name"]
        declaration = yaml.safe_load(
            (ROOT / "examples" / name / "extension/operator.yaml").read_text(
                encoding="utf-8"
            )
        )
        assert declaration["operator"]["operator_id"] not in public_operator_ids
        package = Path(project["package"])
        assert package == output / name / "package"
        specification = yaml.safe_load((package / "spec/research.yaml").read_text(encoding="utf-8"))
        definition, = specification["metric_definitions"]
        assert definition["implementation_ref"] == declaration["operator"]["operator_id"] + ".run"
        source = ROOT / "examples" / name / "extension/source/operator.py"
        assert definition["implementation_digest"] == hashlib.sha256(source.read_bytes()).hexdigest()
        assert (ROOT / "examples" / name / "package/spec/research.yaml").read_bytes() == templates[name]
        assert main([
            "package", "lint",
            "--package", project["package"],
            "--extension-bundle", project["operator_bundle"],
            "--json",
        ]) == 0
        lint = _last_payload(capsys)["data"]
        assert lint["missing_requirements"] == [
            "catalog_lock", "data_source", "output",
        ]
        assert lint["checks"]["metrics"]["status"] == "pass"


def test_public_examples_prepare_isolated_synthetic_environment(
    tmp_path: Path,
) -> None:
    payload = prepare(tmp_path / "synthetic-environment")
    database = Path(payload["database"])
    catalog_lock = Path(payload["catalog_lock"])

    assert payload["status"] == "pass"
    assert payload["row_counts"] == {
        "quantwitness.synthetic_equity_daily": 16,
        "quantwitness.synthetic_etf_daily": 8,
        "quantwitness.synthetic_event_records": 9,
        "quantwitness.synthetic_futures_curve": 3,
    }
    assert database.is_file()
    assert CompiledCatalog.load(catalog_lock).catalog_hash == payload["catalog_hash"]


def test_public_examples_complete_formal_cli_workflow(
    tmp_path: Path, capsys, monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    environment = prepare(tmp_path / "synthetic-environment")
    bundles = build_all(ROOT / "examples", tmp_path / "bundles")
    database = Path(environment["database"])
    before = (database.stat().st_size, database.stat().st_mtime_ns)

    for project in bundles["projects"]:
        name = project["name"]
        work = tmp_path / name
        work.mkdir()
        common = [
            "--package", project["package"],
            "--extension-bundle", project["operator_bundle"],
        ]
        assert main(["package", "lint", *common, "--json"]) == 0, name
        assert _last_payload(capsys)["data"]["checks"]["metrics"]["status"] == "pass"

        plan = work / "plan"
        assert main([
            "package", "admit", *common,
            "--catalog-lock", environment["catalog_lock"],
            "--data-db", str(database), "--output", str(plan), "--json",
        ]) == 0, name
        assert _last_payload(capsys)["data"]["execution_ready"] is True

        specification = yaml.safe_load(
            (Path(project["package"]) / "spec/research.yaml").read_text(encoding="utf-8")
        )
        store_path = work / "results"
        run = subprocess.run([
            sys.executable, "-B", "-m", "research_pipeline",
            "run", "--plan", str(plan), "--data-db", str(database),
            "--artifact-root", str(work / "artifacts"),
            "--run-root", str(work / "run"),
            "--result-store", str(store_path),
            "--clock", specification["fixed_clock"],
            "--root-seed", str(specification["root_seed"]), "--json",
        ], capture_output=True, text=True, encoding="utf-8", timeout=120, check=False)
        assert run.returncode == 0, run.stdout + run.stderr
        result = Path(json.loads(run.stdout)["data"]["result_directory"])
        context = load_result_context(result, result_store=store_path)
        assert isinstance(context, ResultContext)
        assert context.snapshot.bundle.run.status == "succeeded", name
        assert context.claim_level == "research_observation", name
        assert len(context.metrics) == 1, name
        assert context.metrics[0].metric_ref == project["metric_ref"], name

        assert main([
            "report", "--result", str(result),
            "--result-store", str(store_path), "--json",
        ]) == 0, name
        assert _last_payload(capsys)["data"]["report"]
        assert (database.stat().st_size, database.stat().st_mtime_ns) == before
