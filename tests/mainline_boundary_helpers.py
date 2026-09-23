"""主链架构测试共用的 Python import 解析器。"""

from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parents[1] / "src" / "research_pipeline"
INVALID_RELATIVE_IMPORT = "<invalid-relative-import>"


def iter_imported_modules(path: Path, tree: ast.AST) -> tuple[str, ...]:
    """把绝对/相对 import 统一解析为 research_pipeline 包内绝对路径。"""
    imported: list[str] = []
    current_package = (
        "research_pipeline",
        *path.relative_to(PACKAGE_DIR).parts[:-1],
    )
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            parent_count = node.level - 1
            if parent_count >= len(current_package):
                imported.append(INVALID_RELATIVE_IMPORT)
                continue
            base_parts = current_package[: len(current_package) - parent_count]
        else:
            base_parts = ()
        if node.module:
            base_parts = (*base_parts, *node.module.split("."))
        base_module = ".".join(base_parts)
        if base_module:
            imported.append(base_module)
        imported.extend(
            ".".join((*base_parts, alias.name))
            for alias in node.names
            if base_parts
        )
    return tuple(imported)


def matches_module_prefix(module: str, allowed: tuple[str, ...]) -> bool:
    return any(
        module == prefix or module.startswith(f"{prefix}.") for prefix in allowed
    )


__all__ = [
    "INVALID_RELATIVE_IMPORT",
    "PACKAGE_DIR",
    "iter_imported_modules",
    "matches_module_prefix",
]
