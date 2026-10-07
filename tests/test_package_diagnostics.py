"""静态声明诊断只使用临时 YAML，任何数据库连接都令测试失败。"""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
import sqlite3

import duckdb
import pytest
import yaml

from research_pipeline.cli import main
from research_pipeline.cli.commands import research_package as command
from research_pipeline.packages import store
from research_pipeline.packages.lint import diagnose_research_package, validate_lint_declarations
from research_pipeline.packages.models import ResearchPackageError


@pytest.fixture(autouse=True)
def forbid_database(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("静态 lint 不得访问或创建数据库")

    for name in ("connect", "sql", "query", "execute"):
        monkeypatch.setattr(duckdb, name, forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)


@pytest.fixture
def draft(tmp_path):
    return store.initialize_research_package(tmp_path / "draft")


@pytest.fixture
def complete(tmp_path):
    root = tmp_path / "complete"
    source = Path(__file__).resolve().parents[1] / "examples" / "equity_cross_section" / "package"
    for file in store.PACKAGE_FILES:
        path = root / file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text((source / file).read_text(encoding="utf-8"), encoding="utf-8")
    return root


def update_yaml(root, file, change):
    path = root / file
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    change(value)
    path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding="utf-8")


def test_draft_cli_aggregates_before_load_or_compile(draft, monkeypatch, capsys):
    import research_pipeline.packages as packages

    def forbidden(*args, **kwargs):
        pytest.fail("有静态缺口时不得加载、核验来源或编译")

    monkeypatch.setattr(packages, "load_research_package", forbidden)
    monkeypatch.setattr(packages, "verify_package_source_provenance", forbidden)
    monkeypatch.setattr(command, "_compile_lint", forbidden)
    assert main(["package", "lint", "--package", str(draft), "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == "research_package_invalid"
    data = payload["data"]
    issues = data["issues"]
    fields = {(issue["file"], issue["field"]) for issue in issues}
    assert {
        ("sources/sources.yaml", "sources"),
        ("localization.yaml", "decisions"),
        ("package.yaml", "package_slug"),
        ("package.yaml", "metric_contract"),
        ("package.yaml", "claim_contract"),
        ("spec/research.yaml", "requests"),
    } <= fields
    assert all(set(issue) == {"code", "file", "field", "message", "action"} for issue in issues)
    assert all(all(isinstance(value, str) and value for value in issue.values()) for issue in issues)
    assert issues == sorted(issues, key=lambda item: (item["file"], item["field"], item["code"], item["message"]))
    assert data["execution_ready"] is False
    assert data["checks"]["compilation"]["status"] == "pending"


@pytest.mark.parametrize("file", store.PACKAGE_FILES)
def test_unparseable_yaml_reports_only_that_file_parse_error(draft, file):
    (draft / file).write_text("broken: [", encoding="utf-8")
    issues = diagnose_research_package(draft)
    local = [issue for issue in issues if issue["file"] == file]
    assert len(local) == 1
    assert local[0]["code"] == "package_yaml_invalid"
    assert local[0]["field"] == "$"
    assert any(issue["file"] != file for issue in issues)


@pytest.mark.parametrize("content", ["[]", "null"])
def test_non_mapping_yaml_does_not_trigger_field_checks(draft, content):
    (draft / "package.yaml").write_text(content, encoding="utf-8")
    issues = diagnose_research_package(draft)
    assert [(issue["code"], issue["field"]) for issue in issues if issue["file"] == "package.yaml"] == [
        ("package_yaml_invalid", "$"),
    ]


def test_missing_files_are_independent(tmp_path):
    root = tmp_path / "missing"
    issues = diagnose_research_package(root)
    assert len(issues) == len(store.PACKAGE_FILES)
    assert {issue["file"] for issue in issues} == set(store.PACKAGE_FILES)
    assert all(issue["code"] == "package_yaml_invalid" for issue in issues)
    assert not root.exists()


def test_missing_fields_keep_other_contract_checks(complete):
    def change(value):
        del value["package_slug"]
        del value["display_name"]
        value["claim_contract"]["allowed_claim_levels"] = []
        value["unexpected"] = True

    update_yaml(complete, "package.yaml", change)
    issues = diagnose_research_package(complete)
    assert {(issue["field"], issue["code"]) for issue in issues} == {
        ("package_slug", "package_field_missing"),
        ("display_name", "package_field_missing"),
        ("claim_contract", "package_field_invalid"),
        ("unexpected", "package_field_unknown"),
    }
    assert diagnose_research_package(complete) == issues


def test_independent_source_and_localization_entries(complete):
    def damage_sources(value):
        source = value["sources"][0]
        value["sources"] = [{**source, "status": "invalid"}, {**source, "source_type": "invalid"}]

    def damage_localizations(value):
        item = value["decisions"][0]
        value["decisions"] = [{**item, "status": "invalid"}, {**item, "evidence_source_ids": []}]

    update_yaml(complete, "sources/sources.yaml", damage_sources)
    update_yaml(complete, "localization.yaml", damage_localizations)
    assert {(issue["file"], issue["field"]) for issue in diagnose_research_package(complete)} == {
        ("sources/sources.yaml", "sources[0]"),
        ("sources/sources.yaml", "sources[1]"),
        ("localization.yaml", "decisions[0]"),
        ("localization.yaml", "decisions[1]"),
    }


def test_missing_as_of_does_not_compile_dependent_queries(complete, monkeypatch):
    from research_pipeline.packages import compiler

    def forbidden(*args, **kwargs):
        pytest.fail("缺少 as_of 时不得推断查询可见性")

    monkeypatch.setattr(compiler, "compile_query_requests", forbidden)
    update_yaml(complete, "spec/research.yaml", lambda value: value.update(as_of=None, graph={}))
    issues = diagnose_research_package(complete)
    assert {issue["field"] for issue in issues} == {"as_of", "graph"}


def test_source_decoder_preserves_input_for_shared_validation(complete):
    value = yaml.safe_load((complete / "sources/sources.yaml").read_text(encoding="utf-8"))["sources"][0]
    value["provenance"] = store.SourceProvenance.citation_only().to_dict()
    original = deepcopy(value)
    assert store._load_source(value) == store._load_source(value)
    assert value == original


def test_strict_loader_and_admit_still_reject_draft(draft, capsys):
    with pytest.raises(ResearchPackageError):
        store.load_research_package(draft)
    with pytest.raises(ResearchPackageError) as caught:
        validate_lint_declarations(draft)
    assert len(caught.value.failure_payload["issues"]) >= 2
    assert main(["package", "admit", "--package", str(draft), "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error_code"] == "research_package_invalid"


def test_complete_lint_delegates_to_original_compiler_and_report(complete, capsys, monkeypatch):
    from research_pipeline.packages import lint

    package = store.load_research_package(complete)
    assert diagnose_research_package(complete) == []
    calls = []
    plan, registry = object(), object()
    resources = {"status": "declared"}
    report = {"status": "linted", "package_hash": package.package_hash, "execution_ready": False}

    def compile_package(loaded, args):
        assert loaded.package_hash == package.package_hash
        assert Path(args.package) == complete
        calls.append("compile")
        return plan, registry

    def resource_summary(compiled, admitted):
        assert compiled is plan and admitted is registry
        calls.append("resources")
        return resources

    def build_report(loaded, compiled, **kwargs):
        assert loaded.package_hash == package.package_hash
        assert compiled is plan
        assert kwargs["resource_summary"] is resources
        assert kwargs["catalog_lock"] is None
        calls.append("report")
        return report

    monkeypatch.setattr(command, "_compile_lint", compile_package)
    monkeypatch.setattr(command, "_declared_resource_summary", resource_summary)
    monkeypatch.setattr(lint, "build_lint_report", build_report)
    assert main(["package", "lint", "--package", str(complete), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["data"] == report
    assert calls == ["compile", "resources", "report"]


@pytest.mark.parametrize("scope", ["core", "project"])
def test_resource_summary_reads_real_implementation_scope(scope, monkeypatch):
    from research_pipeline.extensions.project_admission import ProjectOperatorImplementationToken
    from research_pipeline.runtime import operator_definitions

    node = SimpleNamespace(node_id="analysis", operator_id="shared.analysis", operator_version="1.0.0")
    profile = {"memory_bytes": 1024, "temp_bytes": 0, "cpu_slots": 1, "process_slots": 1, "wall_seconds": 30}
    specification = SimpleNamespace(
        operator_id=node.operator_id, operator_version=node.operator_version,
        resource_profile=profile,
    )
    definition = SimpleNamespace(implementation_ref=SimpleNamespace(implementation_scope="core"))
    manifest = SimpleNamespace(require_operator=lambda *args: definition)
    monkeypatch.setattr(operator_definitions, "build_mainline_operator_manifest", lambda: manifest)
    token = ProjectOperatorImplementationToken(SimpleNamespace(), "project.worker") if scope == "project" else object()
    registry = SimpleNamespace(
        operator_specs=[specification],
        binding=lambda *args: SimpleNamespace(implementation_token=token),
    )
    summary = command._declared_resource_summary(
        SimpleNamespace(recipe=SimpleNamespace(nodes=[node])), registry,
    )
    assert summary["nodes"][0]["implementation_scope"] == scope
    assert summary["nodes"][0]["resource_profile"] == profile
