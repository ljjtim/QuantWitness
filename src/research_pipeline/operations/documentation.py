"""现行文档集合的只读审计。"""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
from urllib.parse import unquote

from research_pipeline.platform.canonical import fingerprint


REQUIRED_DOCS = frozenset(
    {
        "architecture.md",
        "ai_workflow.md",
        "catalog.md",
        "cli.md",
        "data_plane.md",
        "evidence.md",
        "getting-started.md",
        "index.md",
        "minute_rule_provenance.md",
        "operations.md",
        "release.md",
        "research_package.md",
        "runtime.md",
    }
)

FORBIDDEN_PATTERNS = (
    re.compile(r"python\s+-m\s+research_pipeline\s+(?:v2|project|run-study|init-study)\b"),
    re.compile(
        r"python\s+-m\s+research_pipeline\s+catalog\s+"
        r"(?:dataset|field)\s+search\s+--query\b"
    ),
    re.compile(
        r"python\s+-m\s+research_pipeline\s+verify\b[^\n]*--result-id\b"
    ),
    re.compile(r"--source-visibility\b"),
    re.compile(r"minute-source-visibility-v1\b"),
    re.compile(r"cn_equity\.minute_bar\.raw_reconstructed\b"),
    re.compile(r"(?:开发中|尚未完成|等待\s+G)"),
)
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
IGNORED_LINK_PREFIXES = ("http://", "https://", "mailto:", "#")
def _markdown_files(root: Path) -> tuple[Path, ...]:
    files = [root / "README.md"]
    files.extend(sorted(root / "docs" / name for name in REQUIRED_DOCS))
    return tuple(path for path in files if path.is_file())


def _relative_link_target(source: Path, raw_target: str) -> Path | None:
    target = raw_target.strip().split("#", 1)[0]
    if not target or target.startswith(IGNORED_LINK_PREFIXES):
        return None
    return (source.parent / unquote(target)).resolve()


def audit_documentation(root: Path) -> dict[str, object]:
    """检查现行文档集合、索引、相对链接和退役入口。"""

    root = root.resolve()
    docs_root = root / "docs"
    errors: list[dict[str, str]] = []
    available_docs = {path.name for path in docs_root.glob("*.md") if path.is_file()}
    for name in sorted(REQUIRED_DOCS - available_docs):
        errors.append({"kind": "missing_doc", "path": f"docs/{name}"})

    indexed_text = (docs_root / "index.md").read_text(encoding="utf-8")
    for name in sorted(REQUIRED_DOCS - {"index.md"}):
        if f"({name})" not in indexed_text:
            errors.append({"kind": "missing_current_doc_link", "path": f"docs/{name}"})
    for raw_target in LINK_PATTERN.findall(indexed_text):
        target = _relative_link_target(docs_root / "index.md", raw_target)
        if (
            target is not None
            and target.parent == docs_root.resolve()
            and target.suffix.lower() == ".md"
            and target.name not in REQUIRED_DOCS
        ):
            errors.append(
                {
                    "kind": "non_current_doc_link",
                    "path": "docs/index.md",
                    "target": raw_target,
                }
            )

    scanned_files = _markdown_files(root)
    audited_documents: list[dict[str, object]] = []
    for path in scanned_files:
        content = path.read_bytes()
        text = content.decode("utf-8")
        relative = path.relative_to(root).as_posix()
        audited_documents.append(
            {
                "path": relative,
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(text):
                errors.append(
                    {
                        "kind": "forbidden_text",
                        "path": relative,
                        "pattern": pattern.pattern,
                    }
                )
        for raw_target in LINK_PATTERN.findall(text):
            target = _relative_link_target(path, raw_target)
            if target is not None and not target.exists():
                errors.append(
                    {
                        "kind": "broken_link",
                        "path": relative,
                        "target": raw_target,
                    }
                )

    identity = {
        "current_docs": sorted(REQUIRED_DOCS),
        "audited_documents": audited_documents,
        "errors": errors,
    }
    return {
        "schema_version": "research-documentation-audit-v1",
        "status": "pass" if not errors else "fail",
        "database_accessed": False,
        "current_docs": sorted(REQUIRED_DOCS),
        "audited_documents": audited_documents,
        "scanned_file_count": len(scanned_files),
        "errors": errors,
        "audit_hash": fingerprint(identity),
    }


__all__ = ["REQUIRED_DOCS", "audit_documentation"]
