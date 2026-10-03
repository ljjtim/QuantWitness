"""用公开合成研究的正式 Result 验证 Gate C 证据消费。"""

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
from research_pipeline.platform import canonical_json, typed_canonical_hash

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from build_gate_c_evidence import build_gate_c_evidence, _request_windows  # noqa: E402


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, payload):
    path.write_text(canonical_json(payload), encoding="utf-8")
    return path


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
            "--verifier-bundle", project["verifier_bundle"],
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
        verification = target / "verification-result.json"
        _cli("verify", "--verification-process-slots", "3" if os.name == "nt" else "2", "--result", str(result), "--result-store", str(store), "--output", str(verification))
        verified = _json(verification)
        assert verified["status"] == "pass"
        after = {"size_bytes": database.stat().st_size, "mtime_ns": database.stat().st_mtime_ns}
        assert before == after
        plan = _json(plan_root / "operator-graph-plan.json")
        references.append({
            "reference": name, "dag_family": name,
            "package": project["package"], "plan": str(plan_root / "operator-graph-plan.json"),
            "result_directory": str(result), "result_store": str(store),
            "verification_result": str(verification),
            "expected_graph_id": plan["recipe"]["graph_id"],
            "expected_result_id": verified["result_reference"]["result_id"],
            "study_request_ids": [row["request_id"] for row in plan["requests"]],
            "oracle_script": str(ROOT / "examples" / name / "verifier/source/check.py"),
            "database_evidence": {
                "data_scope": "synthetic", "database_path": str(database),
                "result_id": verified["result_reference"]["result_id"],
                "read_only": True, "database_unchanged": True, "before": before, "after": after,
            },
        })
    return {
        "contract_version": "research-gate-c-input-v2", "data_scope": "synthetic",
        "minimum_dag_families": 3, "maximum_window_days": 400, "references": references,
    }


def _build(tmp_path, inputs, **kwargs):
    return build_gate_c_evidence(
        input_path=_write(tmp_path / "input.json", inputs), output=tmp_path / "gate-c", **kwargs,
    )


def test_gate_c_accepts_four_self_contained_public_results(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    for index, reference in enumerate(inputs["references"]):
        source = Path(reference["result_directory"])
        store = tmp_path / f"results-{index}"
        destination = store / source.relative_to(reference["result_store"])
        shutil.copytree(source, destination)
        reference["result_store"] = str(store)
        reference["result_directory"] = str(destination)
    evidence = _build(tmp_path, inputs)
    assert evidence["contract_version"] == "research-gate-c-evidence-v2"
    assert evidence["gate_id"] == "gate-c"
    assert evidence["all_results_contract"] == "research-result-v3"
    assert evidence["all_verifications_contract"] == "research-verification-result-v4"
    assert evidence["reference_count"] == evidence["independent_oracle_count"] == 4
    assert evidence["dag_family_count"] == 4
    assert evidence["data_scope"] == "synthetic"
    assert evidence["acceptance_scope"] == "engineering"
    assert evidence["evidence_scope"] == "diagnostic_unbound"
    assert all(row["database_evidence"]["database_unchanged"] for row in evidence["references"])


def test_gate_c_rejects_tampered_result_diagnostic_table(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    reference = inputs["references"][0]
    original = Path(reference["result_directory"])
    store = tmp_path / "results"
    result = store / original.relative_to(reference["result_store"])
    shutil.copytree(original, result)
    manifest = _json(result / "result.json")
    table = next(row for row in manifest["tables"] if row["role"] == "diagnostic")
    with (result / next(iter(table["files"]))).open("ab") as handle:
        handle.write(b"tampered")
    reference.update(result_store=str(store), result_directory=str(result))
    with pytest.raises(ValueError, match="hash|内容|Parquet|Result"):
        _build(tmp_path, inputs)
    assert not (tmp_path / "gate-c").exists()


def test_gate_c_rejects_other_valid_verification_result(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    store = tmp_path / "combined-results"
    for reference in inputs["references"][:2]:
        source = Path(reference["result_directory"])
        destination = store / source.relative_to(reference["result_store"])
        shutil.copytree(source, destination)
        reference.update(result_directory=str(destination), result_store=str(store))
    inputs["references"][0]["verification_result"] = inputs["references"][1]["verification_result"]
    with pytest.raises(ValueError, match="VerificationResult.*不匹配"):
        _build(tmp_path, inputs)


def test_gate_c_rejects_rehashed_verification_with_wrong_plan(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    reference = inputs["references"][0]
    verification = _json(Path(reference["verification_result"]))
    verification["source_plan_hash"] = "f" * 64
    verification["verification_hash"] = typed_canonical_hash({key: value for key, value in verification.items() if key != "verification_hash"})
    reference["verification_result"] = str(_write(tmp_path / "wrong-verification.json", verification))
    with pytest.raises(ValueError, match="绑定|身份"):
        _build(tmp_path, inputs)


@pytest.mark.parametrize("change", ["missing", "different_source", "missing_outcome"])
def test_gate_c_rejects_missing_or_unbound_oracle(tmp_path, gate_inputs, change):
    inputs = deepcopy(gate_inputs)
    reference = inputs["references"][0]
    if change == "missing":
        del reference["oracle_script"]
    elif change == "different_source":
        reference["oracle_script"] = inputs["references"][1]["oracle_script"]
    else:
        verification = _json(Path(reference["verification_result"]))
        del verification["project_verifier_identity"]
        del verification["project_verifier_outcome_hash"]
        verification["verification_hash"] = typed_canonical_hash({key: value for key, value in verification.items() if key != "verification_hash"})
        reference["verification_result"] = str(_write(tmp_path / "no-oracle.json", verification))
    with pytest.raises(ValueError, match="oracle|绑定|身份"):
        _build(tmp_path, inputs)


@pytest.mark.parametrize("start,end", [("2024-02-01", "2024-01-01"), ("2024-01-01", "2026-01-01"), ("bad", "2024-01-01")])
def test_gate_c_rejects_invalid_request_dates(start, end):
    plan = {"requests": [{"request_id": "study", "query": {"dataset_id": "synthetic", "time_range": {"start": start, "end": end}}}]}
    with pytest.raises(ValueError, match="窗口|日期"):
        _request_windows(plan, 400, {"study"})


def test_gate_c_rejects_window_limit_and_missing_request(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    inputs["maximum_window_days"] = 1
    with pytest.raises(ValueError, match="窗口"):
        _build(tmp_path, inputs)
    inputs["maximum_window_days"] = 400
    inputs["references"][0]["study_request_ids"] = ["not-in-result"]
    with pytest.raises(ValueError, match="正式输入"):
        _build(tmp_path, inputs)


@pytest.mark.parametrize("rehash", [False, True])
def test_gate_c_rejects_changed_plan_dates(tmp_path, gate_inputs, rehash):
    inputs = deepcopy(gate_inputs)
    reference = inputs["references"][0]
    plan = _json(Path(reference["plan"]))
    plan["requests"][0]["query"]["time_range"]["start"] = "2024-01-01"
    if rehash:
        plan["plan_hash"] = typed_canonical_hash({key: value for key, value in plan.items() if key != "plan_hash"})
    reference["plan"] = str(_write(tmp_path / "wrong-plan.json", plan))
    with pytest.raises(ValueError, match="plan"):
        _build(tmp_path, inputs)


@pytest.mark.parametrize("change", ["scope", "readonly", "changed", "result"])
def test_gate_c_rejects_database_evidence_mismatch(tmp_path, gate_inputs, change):
    inputs = deepcopy(gate_inputs)
    database = inputs["references"][0]["database_evidence"]
    if change == "scope":
        database["data_scope"] = "real"
    elif change == "readonly":
        database["read_only"] = False
    elif change == "changed":
        database["after"] = {**database["after"], "size_bytes": 1}
    else:
        database["result_id"] = "f" * 64
    with pytest.raises(ValueError, match="数据库"):
        _build(tmp_path, inputs)


def test_gate_c_rejects_synthetic_relabelled_as_real(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    inputs["data_scope"] = "real"
    for item in inputs["references"]:
        item["database_evidence"]["data_scope"] = "real"
    with pytest.raises(ValueError, match="合成数据"):
        _build(tmp_path, inputs)


def test_gate_c_requires_four_researches_and_three_families(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    inputs["references"].pop()
    with pytest.raises(ValueError, match="四项"):
        _build(tmp_path, inputs)
    inputs = deepcopy(gate_inputs)
    for item in inputs["references"]:
        item["dag_family"] = "same"
    with pytest.raises(ValueError, match="三种"):
        _build(tmp_path, inputs)


def test_gate_c_rejects_partial_candidate_binding_before_output(tmp_path, gate_inputs):
    with pytest.raises(ValueError, match="BuildManifest"):
        _build(tmp_path, gate_inputs, release_candidate_id="candidate")
    assert not (tmp_path / "gate-c").exists()


def test_gate_c_keeps_long_auxiliary_window_separate_from_study():
    plan = {"requests": [
        {"request_id": "study", "query": {"dataset_id": "prices", "time_range": {"start": "2024-01-01", "end": "2024-01-03"}}},
        {"request_id": "history", "query": {"dataset_id": "calendar", "time_range": {"start": "2000-01-01", "end": "2024-01-03"}}},
    ]}
    windows = _request_windows(plan, 10, {"study"})
    assert windows[0]["study_window"] is True
    assert windows[1]["study_window"] is False
    assert windows[1]["calendar_days"] > 400


def test_gate_c_rejects_extra_result_for_successful_run(tmp_path, gate_inputs):
    inputs = deepcopy(gate_inputs)
    reference = inputs["references"][0]
    original = Path(reference["result_directory"])
    store = tmp_path / "results"
    result = store / original.relative_to(reference["result_store"])
    shutil.copytree(original, result)
    (result.parent / "another-result").mkdir()
    reference.update(result_store=str(store), result_directory=str(result))
    with pytest.raises(ValueError, match="恰好只有一个"):
        _build(tmp_path, inputs)
