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
