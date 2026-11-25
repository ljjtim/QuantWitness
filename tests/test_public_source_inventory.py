from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

from public_source_inventory import (  # noqa: E402
    _INTERNAL_REFERENCE,
    export_public_source,
    inspect_public_source,
    main,
)
from release_allowlist import (  # noqa: E402
    PUBLIC_EXAMPLE_PROJECTS,
    PUBLIC_INTEGRATION_FILES,
    package_file_paths,
    public_source_paths,
)


def test_public_source_inventory_reuses_package_inventory() -> None:
    public = set(public_source_paths(ROOT))
    assert set(package_file_paths(ROOT)) <= public
    assert {
        "LICENSE",
        "README.md",
        "CONTRIBUTING.md",
        "SECURITY.md",
        "THIRD_PARTY_NOTICES.md",
        ".github/CODEOWNERS.template",
        ".github/workflows/ci.yml",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/pull_request_template.md",
        "tools/verify_rdagent_install.py",
        "integrations/rdagent/docs/installation.md",
        "integrations/rdagent/MANIFEST.in",
        ".github/workflows/publish.yml",
        "tools/plan_version_release.py",
        "tests/test_version_release_plan.py",
        "docs/qlib-expressions.md",
        "tests/test_qlib_sequence_windows.py",
        "tests/test_qlib_gru_contract.py",
        "tests/test_qlib_gru_integration.py",
        "tests/test_qlib_lstm_contract.py",
        "tests/test_qlib_lstm_integration.py",
        "tests/test_qlib_transformer_contract.py",
        "tests/test_qlib_transformer_integration.py",
        "src/research_pipeline/research/modeling/generated.py",
        "src/research_pipeline/research/modeling/generated_definition.py",
        "integrations/rdagent/src/quantwitness_rdagent/model_research.py",
        "integrations/rdagent/src/quantwitness_rdagent/model_loop.py",
        "integrations/rdagent/src/quantwitness_rdagent/native_model_research.py",
        "integrations/rdagent/src/quantwitness_rdagent/model_research_evidence.py",
        "integrations/rdagent/examples/model_research/prepare.py",
        "integrations/rdagent/docs/model-research.md",
        "examples/qlib_portfolio/extension/factor_baselines.py",
        "examples/qlib_portfolio/extension/factor_baseline_oracle.py",
        "project_extensions/qlib_portfolio_risk/optimizer.py",
        "examples/qlib_risk/prepare.py",
        "src/research_pipeline/research/modeling/gru.py",
        "src/research_pipeline/research/modeling/inputs.py",
        "src/research_pipeline/runtime/model_sequence.py",
        "src/research_pipeline/packages/model_sequence_admission.py",
        "tests/test_model_sequence_validity.py",
        "tests/test_qlib_sequence_example.py",
        "tests/test_qlib_rolling_horizons.py",
        "tests/test_qlib_horizon_example.py",
        "tests/test_qlib_horizon_group.py",
        "tests/test_qlib_sequence_admission.py",
        "src/research_pipeline/research/modeling/sequence.py",
        "src/research_pipeline/research/validation/holdout.py",
        "src/research_pipeline/research/validation/holdout_series.py",
        "examples/qlib_portfolio/extension/expressions.py",
        "examples/qlib_portfolio/horizons.py",
        "integrations/rdagent/docs/factor-research.md",
        "integrations/rdagent/examples/factor_research/prepare.py",
        "integrations/rdagent/src/quantwitness_rdagent/native_research.py",
        "integrations/rdagent/src/quantwitness_rdagent/native_backend.py",
        "integrations/rdagent/src/quantwitness_rdagent/factor_loop.py",
        "integrations/rdagent/src/quantwitness_rdagent/factor_research.py",
        "integrations/rdagent/src/quantwitness_rdagent/research_knowledge.py",
        "integrations/rdagent/src/quantwitness_rdagent/direction_selection.py",
    } <= public
    for project_name in PUBLIC_EXAMPLE_PROJECTS:
        prefix = f"examples/{project_name}/"
        if project_name in {"custom_metric", "turnover_factor"}:
            assert {f"{prefix}{name}" for name in (
                "run.py", "inputs.py", "source/operator.py", "README.md",
            )} <= public
            continue
        if project_name == "lean_quickstart":
            assert {f"{prefix}{name}" for name in (
                "build.py", "inputs.py", "lean_scope.py", "strategy.py",
                "verify.py", "expected.yaml", "README.md",
            )} <= public
            continue
        if project_name == "lean_futures":
            assert {f"{prefix}{name}" for name in (
                "build.py", "inputs.py", "scope.py", "strategy.py",
                "synthetic.py", "verify.py", "README.md",
            )} <= public
            continue
        assert f"{prefix}package/package.yaml" in public
        assert f"{prefix}package/spec/research.yaml" in public
        assert f"{prefix}extension/operator.yaml" in public
        assert f"{prefix}extension/source/operator.py" in public
        assert f"{prefix}synthetic.py" in public
        assert f"{prefix}test_operator.py" in public


def test_public_source_inventory_excludes_internal_and_generated_trees() -> None:
    paths = public_source_paths(ROOT)
    forbidden = (
        ".trellis/",
        "replacement_acceptance/",
        "research_packages/",
        "retirement_migration/",
        "dist/",
        "release-candidate/",
    )
    assert not any(path.startswith(forbidden) for path in paths)
    assert inspect_public_source(ROOT)["status"] == "pass"


def test_public_source_export_copies_exact_inventory(tmp_path: Path) -> None:
    output = tmp_path / "quantwitness-public"
    payload = export_public_source(ROOT, output)
    expected = public_source_paths(ROOT)
    actual = tuple(
        sorted(
            path.relative_to(output).as_posix()
            for path in output.rglob("*")
            if path.is_file()
        )
    )

    assert payload["status"] == "pass"
    assert tuple(payload["paths"]) == expected
    assert actual == expected


def test_rdagent_public_source_contains_all_runtime_modules() -> None:
    source = ROOT / "integrations/rdagent/src/quantwitness_rdagent"
    modules = {path.relative_to(ROOT).as_posix() for path in source.rglob("*.py")}
    assert modules <= set(PUBLIC_INTEGRATION_FILES)
    assert "integrations/rdagent/docs/formula-reproduction.md" in PUBLIC_INTEGRATION_FILES
    example = ROOT / "integrations/rdagent/examples/volume_concentration"
    example_sources = {path.relative_to(ROOT).as_posix() for path in example.rglob("*")
                       if path.is_file() and path.suffix in {".py", ".md", ".yaml"}}
    assert example_sources <= set(PUBLIC_INTEGRATION_FILES)
    assert "integrations/rdagent/src/quantwitness_rdagent/request_builder.py" in PUBLIC_INTEGRATION_FILES
    for name in ("package_campaign", "prediction_campaign"):
        example = ROOT / "integrations/rdagent/examples" / name
        expected = {path.relative_to(ROOT).as_posix() for path in example.rglob("*")
                    if path.is_file() and path.suffix in {".py", ".md", ".yaml"}}
        assert expected <= set(PUBLIC_INTEGRATION_FILES)


def test_public_qlib_example_sources_are_complete():
    example = ROOT / "examples/qlib_portfolio"
    expected = {path.relative_to(ROOT).as_posix() for path in example.rglob("*")
                if path.is_file() and path.suffix in {".py", ".md", ".yaml"}}
    assert expected <= set(public_source_paths(ROOT))


def test_public_checkout_rejects_extra_tracked_file(tmp_path: Path) -> None:
    output = tmp_path / "quantwitness-public"
    export_public_source(ROOT, output)
    owners = output / ".github" / "CODEOWNERS"
    owners.unlink(missing_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=output, check=True)
    subprocess.run(["git", "config", "core.longpaths", "true"], cwd=output, check=True)
    subprocess.run(["git", "add", "--all"], cwd=output, check=True)
    missing = inspect_public_source(output, check_tracked=True)
    assert {"kind": "missing_governance_file", "path": ".github/CODEOWNERS"} in missing["issues"]

    owners.write_text(
        "\n".join(("* @owner", "/.github/CODEOWNERS @owner", "")),
        encoding="utf-8",
    )
    subprocess.run(["git", "add", ".github/CODEOWNERS"], cwd=output, check=True)
    assert inspect_public_source(output, check_tracked=True)["status"] == "pass"
    owners.write_text("-----BEGIN " + "PRIVATE KEY-----", encoding="utf-8")
    assert any(
        issue["kind"] == "forbidden_text" and issue["path"] == ".github/CODEOWNERS"
        for issue in inspect_public_source(output, check_tracked=True)["issues"]
    )
    owners.write_text("\n".join(("* @owner", "")), encoding="utf-8")

    (output / "extra.txt").write_text("额外文件", encoding="utf-8")
    subprocess.run(["git", "add", "extra.txt"], cwd=output, check=True)
    result = inspect_public_source(output, check_tracked=True)
    assert result["status"] == "fail"
    assert {"kind": "unexpected_tracked_file", "path": "extra.txt"} in result["issues"]
    assert main(["--project", str(output), "--check"]) == 1


def test_public_source_export_rejects_existing_output(tmp_path: Path) -> None:
    output = tmp_path / "quantwitness-public"
    output.mkdir()

    try:
        export_public_source(ROOT, output)
    except ValueError as exc:
        assert "已存在" in str(exc)
    else:
        raise AssertionError("已存在的输出目录必须被拒绝")

    assert main(["--project", str(ROOT), "--output", str(output)]) == 1


def test_internal_project_references_are_rejected_in_public_documents() -> None:
    assert _INTERNAL_REFERENCE.search(
        "research_pipeline/replacement_acceptance/final-acceptance.json"
    )
    assert _INTERNAL_REFERENCE.search("research_packages/references/study/")
    assert not _INTERNAL_REFERENCE.search("project_extensions/README.md")


def test_public_tests_include_imported_local_support_modules() -> None:
    public = set(public_source_paths(ROOT))
    missing = []
    for relative in sorted(public):
        path = ROOT / relative
        if "tests" not in path.parts or path.suffix != ".py":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = [node.module]
            elif isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            else:
                continue
            for module in modules:
                # pytest把当前测试目录加入导入路径，支持文件必须一起发行。
                module_path = Path(*module.split(".")).with_suffix(".py")
                for base in (path.parent, ROOT):
                    dependency = base / module_path
                    if dependency.is_file():
                        dependency_name = dependency.relative_to(ROOT).as_posix()
                        if dependency_name not in public:
                            missing.append((relative, dependency_name))
                if module.startswith("test_") and "." not in module:
                    dependency_name = (path.parent / module_path).relative_to(ROOT).as_posix()
                    if dependency_name not in public:
                        missing.append((relative, dependency_name))
    assert not missing, missing
