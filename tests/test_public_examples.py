from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

from examples.build_bundles import PROJECTS, build_all
from examples.prepare_synthetic_environment import prepare
from research_pipeline.catalog import CompiledCatalog
from research_pipeline.cli import main
from research_pipeline.results import ResultStore
from research_pipeline.runtime.operator_definitions import build_mainline_operator_manifest


ROOT = Path(__file__).resolve().parents[1]


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
        assert (project / "verifier/source/check.py").is_file()


def test_public_examples_build_and_lint_without_public_registry_entries(
    tmp_path: Path,
    capsys,
) -> None:
    payload = build_all(ROOT / "examples", tmp_path / "bundles")
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
        assert main([
            "package", "lint",
            "--package", project["package"],
            "--extension-bundle", project["operator_bundle"],
            "--verifier-bundle", project["verifier_bundle"],
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
            "--verifier-bundle", project["verifier_bundle"],
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
        assert main([
            "run", "--plan", str(plan), "--data-db", str(database),
            "--artifact-root", str(work / "artifacts"),
            "--handoff-out", str(work / "handoff.json"),
            "--run-root", str(work / "run"),
            "--result-store", str(store_path),
            "--clock", specification["fixed_clock"],
            "--root-seed", str(specification["root_seed"]), "--json",
        ]) == 0, name
        result = Path(_last_payload(capsys)["data"]["result_directory"])
        ResultStore(store_path, create=False).verify(result)

        verification = work / "verification-result.json"
        assert main([
            "verify", "--result", str(result),
            "--result-store", str(store_path),
            "--verifier-bundle", project["verifier_bundle"],
            "--output", str(verification), "--json",
        ]) == 0, name
        _last_payload(capsys)
        verified = json.loads(verification.read_text(encoding="utf-8"))
        assert verified["status"] == "pass", name
        assert verified["validity_status"] == "pass", name

        assert main([
            "report", "--verification-result", str(verification),
            "--result-store", str(store_path), "--json",
        ]) == 0, name
        assert _last_payload(capsys)["data"]["report"]
        assert main(["package", "lint", *common, "--json"]) == 0, name
        assert _last_payload(capsys)["data"]["checks"]["metrics"]["status"] == "pass"
        assert (database.stat().st_size, database.stat().st_mtime_ns) == before
