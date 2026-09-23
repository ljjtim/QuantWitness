"""从同一编译发布生成目录文档。"""

from __future__ import annotations

import json
from pathlib import Path

from .compiler import CompiledCatalog
from .errors import CatalogReferenceError


def render_catalog_docs(release_root: str | Path, output: str | Path) -> dict[str, object]:
    root = Path(release_root)
    compiled = CompiledCatalog.load(root)
    compile_id = (root / "CURRENT").read_text(encoding="utf-8").strip()
    release = root / compile_id
    lock = dict(compiled.payload)
    audit = json.loads((release / "catalog.audit.json").read_text(encoding="utf-8"))
    manifest = json.loads(
        (release / "catalog.source-manifest.json").read_text(encoding="utf-8")
    )
    if lock.get("compile_id") != audit.get("compile_id") or lock.get("compile_id") != compile_id:
        raise CatalogReferenceError("lock 与 audit 的 compile ID 不匹配")
    source_hash = manifest.get("manifest_core_hash")
    if lock.get("source_manifest_hash") != source_hash or audit.get("source_manifest_hash") != source_hash:
        raise CatalogReferenceError("lock 与 audit 的 source manifest hash 不匹配")

    lines = [
        "# 可编译数据目录",
        "",
        f"- compile ID：`{compile_id}`",
        f"- catalog hash：`{lock['catalog_hash']}`",
        f"- 运行数据集：{len(lock.get('datasets', []))}",
        f"- 运行字段：{len(lock.get('fields', []))}",
        f"- blocked：{len(audit.get('blocked_entries', []))}",
        f"- rejected：{len(audit.get('rejected_entries', []))}",
        "",
        "## 运行数据集",
        "",
    ]
    for item in sorted(lock.get("datasets", []), key=lambda value: value["dataset_id"]):
        lines.append(
            f"- `{item['dataset_id']}` v{item['dataset_version']}："
            f"{item['market']} / {item['instrument_type']} / {item['frequency']}"
        )
    lines.extend(("", "## 未开放条目", ""))
    blocked = sorted(
        audit.get("blocked_entries", []),
        key=lambda value: (value["target_kind"], value["target_id"]),
    )
    if blocked:
        for item in blocked:
            lines.append(f"- blocked `{item['target_kind']}/{item['target_id']}`：{item['reason']}")
    else:
        lines.append("- 无")
    rejected = sorted(
        audit.get("rejected_entries", []),
        key=lambda value: (value["target_kind"], value["target_id"]),
    )
    for item in rejected:
        lines.append(f"- rejected `{item['target_kind']}/{item['target_id']}`：{item['reason']}")
    lines.extend(("", "## 旧目录迁移决定", ""))
    legacy = sorted(
        (
            item
            for item in audit.get("decisions", [])
            if item.get("target_kind") == "legacy_table"
        ),
        key=lambda value: value["target_id"],
    )
    for item in legacy:
        lines.append(f"- `{item['target_id']}`：{item['decision']}")

    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "compile_id": compile_id,
        "catalog_hash": lock["catalog_hash"],
        "dataset_count": len(lock.get("datasets", [])),
        "blocked_count": len(blocked),
        "rejected_count": len(rejected),
        "legacy_table_count": len(legacy),
        "output": str(destination),
    }


__all__ = ["render_catalog_docs"]
