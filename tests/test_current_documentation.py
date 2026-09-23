from __future__ import annotations

from pathlib import Path
import re
import shlex

import pytest

from research_pipeline.cli.parser import FINAL_COMMANDS, build_parser
from research_pipeline.operations.documentation import (
    REQUIRED_DOCS,
    audit_documentation,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
COMMAND_PATTERN = re.compile(
    r"python\s+-m\s+research_pipeline(?:\s+([a-z][a-z0-9-]*))?"
)
COMMAND_PREFIX = "python -m research_pipeline "


def test_current_documentation_passes_repository_audit() -> None:
    result = audit_documentation(PACKAGE_ROOT)

    assert result["status"] == "pass", result["errors"]
    assert result["database_accessed"] is False
    assert set(result["current_docs"]) == set(REQUIRED_DOCS)


def test_documentation_audit_allows_unindexed_history_but_rejects_current_link(
    tmp_path: Path,
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "README.md").write_text("# 当前入口\n", encoding="utf-8")
    for name in REQUIRED_DOCS:
        (docs / name).write_text("# 当前文档\n", encoding="utf-8")
    (docs / "index.md").write_text(
        "\n".join(f"[{name}]({name})" for name in sorted(REQUIRED_DOCS - {"index.md"})),
        encoding="utf-8",
    )
    (docs / "old_phase.md").write_text("# 旧阶段\n", encoding="utf-8")

    assert audit_documentation(tmp_path)["status"] == "pass"

    with (docs / "index.md").open("a", encoding="utf-8") as handle:
        handle.write("\n[旧阶段](old_phase.md)\n")
    result = audit_documentation(tmp_path)
    assert result["status"] == "fail"
    assert {item["kind"] for item in result["errors"]} == {
        "non_current_doc_link"
    }


def test_documentation_audit_rejects_old_command(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "README.md").write_text(
        "python -m research_pipeline project validate old\n", encoding="utf-8"
    )
    for name in REQUIRED_DOCS:
        (docs / name).write_text("# 当前文档\n", encoding="utf-8")
    (docs / "index.md").write_text(
        "\n".join(f"[{name}]({name})" for name in sorted(REQUIRED_DOCS - {"index.md"})),
        encoding="utf-8",
    )

    result = audit_documentation(tmp_path)

    assert result["status"] == "fail"
    assert any(item["kind"] == "forbidden_text" for item in result["errors"])


@pytest.mark.parametrize(
    "command",
    (
        "python -m research_pipeline catalog dataset search --query daily --format json",
        "python -m research_pipeline verify --result-id old --result-store results "
        "--output verification.json --json",
    ),
)
def test_documentation_audit_rejects_retired_argument_shapes(
    tmp_path: Path, command: str
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "README.md").write_text(command + "\n", encoding="utf-8")
    for name in REQUIRED_DOCS:
        (docs / name).write_text("# 当前文档\n", encoding="utf-8")
    (docs / "index.md").write_text(
        "\n".join(f"[{name}]({name})" for name in sorted(REQUIRED_DOCS - {"index.md"})),
        encoding="utf-8",
    )

    result = audit_documentation(tmp_path)

    assert result["status"] == "fail"
    assert any(item["kind"] == "forbidden_text" for item in result["errors"])


def test_documented_top_level_commands_belong_to_current_cli() -> None:
    markdown = [
        PACKAGE_ROOT / "README.md",
        *(PACKAGE_ROOT / "docs" / name for name in sorted(REQUIRED_DOCS)),
    ]
    commands = {
        match.group(1)
        for path in markdown
        for match in COMMAND_PATTERN.finditer(path.read_text(encoding="utf-8"))
        if match.group(1)
    }

    assert commands <= set(FINAL_COMMANDS)
    assert {"package", "run", "verify", "report"} <= commands


def test_getting_started_single_line_commands_parse_with_current_cli() -> None:
    document = (PACKAGE_ROOT / "docs" / "getting-started.md").read_text(
        encoding="utf-8"
    )
    commands = [
        shlex.split(line)[3:]
        for line in document.splitlines()
        if line.startswith(COMMAND_PREFIX) and not line.rstrip().endswith("`")
    ]

    assert commands
    for arguments in commands:
        assert build_parser().parse_args(arguments).command == arguments[0]


def test_retired_getting_started_arguments_do_not_parse() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "catalog",
                "dataset",
                "search",
                "--query",
                "daily",
                "--format",
                "json",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "verify",
                "--result-id",
                "old",
                "--result-store",
                "results",
                "--output",
                "verification.json",
                "--json",
            ]
        )


@pytest.mark.parametrize(
    "arguments",
    (
        ["catalog", "dataset", "search", "daily", "--format", "json"],
        [
            "recipe",
            "scaffold",
            "generic.daily-time-series",
            "--output",
            "work/package",
            "--format",
            "json",
        ],
    ),
)
def test_public_catalog_consumers_require_explicit_lock(
    arguments: list[str],
) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(arguments)


def test_public_guides_use_current_research_pipeline_workflow() -> None:
    current_flow = (
        "package init",
        "package lint",
        "package admit",
        "run",
        "verify",
        "report",
    )
    retired_fragments = (
        "`package validate` → `plan`",
        "可信证据封存",
        "`catalog`、`package`、`plan`、`run`",
    )

    for name in ("README.md", "CONTRIBUTING.md", "EXTENSIONS.md"):
        content = (PACKAGE_ROOT / name).read_text(encoding="utf-8")
        if name == "README.md":
            flow_match = re.search(
                r"```text\n研究问题与口径冻结(?P<flow>.*?)```",
                content,
                flags=re.DOTALL,
            )
            assert flow_match is not None
            positions = [flow_match.group("flow").index(stage) for stage in current_flow]
            assert positions == sorted(positions)
        assert "--source-visibility PROFILE=PATH" not in content
        assert all(fragment not in content for fragment in retired_fragments)
