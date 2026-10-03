from __future__ import annotations

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
    } <= public
    for project_name in PUBLIC_EXAMPLE_PROJECTS:
        prefix = f"examples/{project_name}/"
        assert f"{prefix}package/package.yaml" in public
        assert f"{prefix}extension/operator.yaml" in public
        assert f"{prefix}extension/source/operator.py" in public
        assert f"{prefix}verifier/source/check.py" in public
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


def test_public_test_helper_imports_are_included():
    """发行测试不能引用未随公开源码交付的本仓库测试helper。"""
    import ast
    paths = set(public_source_paths(ROOT))
    for relative in sorted(paths):
        if not relative.startswith("tests/") or not relative.endswith(".py"):
            continue
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("test_"):
                dependency = "tests/" + node.module.replace(".", "/") + ".py"
                assert dependency in paths, (relative, dependency)
