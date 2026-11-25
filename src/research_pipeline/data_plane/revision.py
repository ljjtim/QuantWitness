"""来源修订探针；元数据证据不冒充全文哈希。"""

from __future__ import annotations

from functools import cached_property

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import SourceChangedError


SOURCE_REVISION_VERSION = "source-revision-v1"


def _safe_path(path: str | Path, root: str | Path) -> tuple[Path, str]:
    original = Path(path)
    if original.is_symlink():
        raise SourceChangedError("来源路径不接受符号链接")
    resolved = original.resolve()
    allowed = Path(root).resolve()
    try:
        relative = resolved.relative_to(allowed)
    except ValueError as exc:
        raise SourceChangedError("来源路径超出允许根目录") from exc
    return resolved, relative.as_posix()


@dataclass(frozen=True)
class SourceFileEvidence:
    relative_path: str
    size: int
    mtime_ns: int
    content_hash: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True)
class SourceRevision:
    source_id: str
    source_kind: str
    files: tuple[SourceFileEvidence, ...]
    schema_hash: str
    watermark: str | None
    evidence_strength: str
    probe_version: str = SOURCE_REVISION_VERSION

    @cached_property
    def revision_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "probe_version": self.probe_version,
            "source_id": self.source_id,
            "source_kind": self.source_kind,
            "files": [item.to_dict() for item in self.files],
            "schema_hash": self.schema_hash,
            "watermark": self.watermark,
            "evidence_strength": self.evidence_strength,
            "limitation": (
                "metadata 证据未变化时，不承诺发现逐字节内容变化"
                if self.evidence_strength == "metadata"
                else "文件内容 hash 只覆盖 manifest 声明的已提交文件"
            ),
        }


def probe_duckdb_source(
    path: str | Path,
    *,
    allowed_root: str | Path,
    object_name: str,
    source_id: str,
    watermark: str | None = None,
    connection=None,
) -> SourceRevision:
    import duckdb

    database, relative = _safe_path(path, allowed_root)
    stat = database.stat()
    context = (duckdb.connect(str(database), read_only=True)
               if connection is None else nullcontext(connection))
    with context as connection:
        rows = connection.execute(
            """SELECT column_name, data_type, is_nullable, column_index
               FROM duckdb_columns()
               WHERE schema_name='main' AND table_name=? ORDER BY column_index""",
            [object_name],
        ).fetchall()
    if not rows:
        raise SourceChangedError(f"物理对象不存在: {object_name}")
    schema_hash = typed_canonical_hash(
        [{"name": row[0], "type": row[1], "nullable": row[2], "position": row[3]} for row in rows]
    )
    return SourceRevision(
        source_id,
        "duckdb",
        (SourceFileEvidence(relative, stat.st_size, stat.st_mtime_ns),),
        schema_hash,
        watermark,
        "metadata",
    )


def require_source_unchanged(before: SourceRevision, after: SourceRevision) -> None:
    if before.revision_hash != after.revision_hash:
        raise SourceChangedError(
            f"读取期间来源发生变化: before={before.revision_hash[:12]}, after={after.revision_hash[:12]}"
        )


__all__ = [
    "SOURCE_REVISION_VERSION",
    "SourceFileEvidence",
    "SourceRevision",
    "probe_duckdb_source",
    "require_source_unchanged",
]
