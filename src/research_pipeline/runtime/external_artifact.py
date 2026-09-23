"""大型目录工件的同盘 staging、内容校验与原子提交。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
from types import MappingProxyType
from typing import Callable, Mapping
import uuid

from research_pipeline.data_plane.path_policy import PathRolePolicy
from research_pipeline.data_plane.errors import DataPlaneError
from research_pipeline.data_plane.verification_lifecycle import (
    RunScopedArtifactVerification,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.causal_time import (
    CORE_FEATURE_TIME_COLUMNS,
    CORE_LABEL_TIME_COLUMNS,
    attach_core_feature_time_facts,
    attach_core_label_time_facts,
)

from .contracts import ArtifactRef
from .errors import RuntimeIntegrityError


EXTERNAL_ARTIFACT_COMMIT_VERSION = "research-external-artifact-commit-v1"

_FORMAL_CAUSAL_ARTIFACT_COLUMNS = {
    "research.feature-set.v1": frozenset(CORE_FEATURE_TIME_COLUMNS),
    "research.label.v1": frozenset(CORE_LABEL_TIME_COLUMNS),
}
_CAUSAL_LINEAGE_METADATA_KEY = b"research_pipeline_causal_lineage"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()




def _digest_mapping(value: Mapping[str, str], field: str, *, allow_empty: bool = False) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or (not allow_empty and not value):
        raise RuntimeIntegrityError(f"ExternalArtifactCommit {field} 无效")
    normalized = {}
    for key, digest in value.items():
        if (
            not isinstance(key, str)
            or not key
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise RuntimeIntegrityError(f"ExternalArtifactCommit {field} 含无效条目")
        normalized[key] = digest
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True)
class ExternalArtifactCommit:
    artifact_name: str
    artifact_type: str
    commit_token: str
    files: Mapping[str, str]
    schema_hashes: Mapping[str, str]
    row_counts: Mapping[str, int]
    semantic_hash: str
    manifest_hash: str
    contract_version: str = EXTERNAL_ARTIFACT_COMMIT_VERSION

    def __post_init__(self) -> None:
        if not self.artifact_name or not self.artifact_type or not self.commit_token:
            raise RuntimeIntegrityError("ExternalArtifactCommit 身份字段不完整")
        if self.contract_version != EXTERNAL_ARTIFACT_COMMIT_VERSION:
            raise RuntimeIntegrityError("ExternalArtifactCommit 版本不受支持")
        object.__setattr__(self, "files", _digest_mapping(self.files, "files"))
        object.__setattr__(
            self,
            "schema_hashes",
            _digest_mapping(self.schema_hashes, "schema_hashes", allow_empty=True),
        )
        rows = {
            key: value
            for key, value in self.row_counts.items()
            if isinstance(key, str) and key and type(value) is int and value >= 0
        }
        if len(rows) != len(self.row_counts) or set(rows) != set(self.schema_hashes):
            raise RuntimeIntegrityError("ExternalArtifactCommit row_count/schema 不闭合")
        object.__setattr__(self, "row_counts", MappingProxyType(dict(sorted(rows.items()))))
        if self.semantic_hash != typed_canonical_hash(self.semantic_payload()):
            raise RuntimeIntegrityError("ExternalArtifactCommit semantic hash 不一致")
        if self.manifest_hash != typed_canonical_hash(self.payload()):
            raise RuntimeIntegrityError("ExternalArtifactCommit manifest hash 不一致")

    def semantic_payload(self) -> dict[str, object]:
        return {
            "artifact_name": self.artifact_name,
            "artifact_type": self.artifact_type,
            "files": dict(self.files),
            "schema_hashes": dict(self.schema_hashes),
            "row_counts": dict(self.row_counts),
            "contract_version": self.contract_version,
        }

    def payload(self) -> dict[str, object]:
        return {
            **self.semantic_payload(),
            "commit_token": self.commit_token,
            "semantic_hash": self.semantic_hash,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "manifest_hash": self.manifest_hash}

    @property
    def artifact_ref(self) -> ArtifactRef:
        return ArtifactRef(
            self.artifact_name,
            self.artifact_type,
            self.semantic_hash,
            self.manifest_hash,
        )

    @classmethod
    def build(
        cls,
        *,
        artifact_name: str,
        artifact_type: str,
        commit_token: str,
        files: Mapping[str, str],
        schema_hashes: Mapping[str, str],
        row_counts: Mapping[str, int],
    ) -> "ExternalArtifactCommit":
        semantic = {
            "artifact_name": artifact_name,
            "artifact_type": artifact_type,
            "files": dict(sorted(files.items())),
            "schema_hashes": dict(sorted(schema_hashes.items())),
            "row_counts": dict(sorted(row_counts.items())),
            "contract_version": EXTERNAL_ARTIFACT_COMMIT_VERSION,
        }
        semantic_hash = typed_canonical_hash(semantic)
        payload = {**semantic, "commit_token": commit_token, "semantic_hash": semantic_hash}
        return cls(
            artifact_name,
            artifact_type,
            commit_token,
            files,
            schema_hashes,
            row_counts,
            semantic_hash,
            typed_canonical_hash(payload),
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExternalArtifactCommit":
        expected = {
            "artifact_name", "artifact_type", "commit_token", "files", "schema_hashes",
            "row_counts", "semantic_hash", "manifest_hash", "contract_version",
        }
        if set(payload) != expected:
            raise RuntimeIntegrityError("ExternalArtifactCommit schema 无效")
        mappings = tuple(payload[field] for field in ("files", "schema_hashes", "row_counts"))
        if any(not isinstance(item, Mapping) for item in mappings):
            raise RuntimeIntegrityError("ExternalArtifactCommit 映射字段无效")
        return cls(
            str(payload["artifact_name"]),
            str(payload["artifact_type"]),
            str(payload["commit_token"]),
            dict(payload["files"]),  # type: ignore[arg-type]
            dict(payload["schema_hashes"]),  # type: ignore[arg-type]
            dict(payload["row_counts"]),  # type: ignore[arg-type]
            str(payload["semantic_hash"]),
            str(payload["manifest_hash"]),
            str(payload["contract_version"]),
        )


class ExternalArtifactStore:
    def __init__(
        self,
        root: str | Path,
        *,
        create: bool = True,
        verification_session: RunScopedArtifactVerification | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.verification_session = verification_session
        self.staging_root = self.root / ".staging"
        self.objects_root = self.root / "objects"
        if create:
            self.staging_root.mkdir(parents=True, exist_ok=True)
            self.objects_root.mkdir(parents=True, exist_ok=True)
        elif not (
            self.root.is_dir()
            and self.staging_root.is_dir()
            and self.objects_root.is_dir()
        ):
            raise RuntimeIntegrityError("ExternalArtifactStore 只读打开时目录不存在")

    def prepare(self) -> Path:
        token = uuid.uuid4().hex
        staging = self.staging_root / token
        staging.mkdir(exist_ok=False)
        return staging

    def import_verified(
        self,
        source_store: "ExternalArtifactStore",
        commit: ExternalArtifactCommit,
    ) -> ExternalArtifactCommit:
        """复验父对象后以普通字节复制导入当前 store，保持稳定 ArtifactRef。"""
        verified = source_store.verify(commit.semantic_hash)
        if verified != commit:
            raise RuntimeIntegrityError("待导入 external artifact 引用漂移")
        staging = self.staging_root / commit.commit_token
        if staging.exists():
            raise RuntimeIntegrityError("child external artifact staging token 冲突")
        staging.mkdir()
        source_root = source_store.objects_root / commit.semantic_hash
        try:
            for relative_path in commit.files:
                source = source_root / relative_path
                target = staging / relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
            imported = self.commit(
                staging,
                artifact_name=commit.artifact_name,
                artifact_type=commit.artifact_type,
            )
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        if imported != commit:
            raise RuntimeIntegrityError("child external artifact 导入后身份漂移")
        return imported

    def commit(
        self,
        staging_root: str | Path,
        *,
        artifact_name: str,
        artifact_type: str,
        producer_scope: str = "core",
        core_time_facts: object | None = None,
        causal_time_key_columns: tuple[str, ...] = (),
        causal_lineage: object | None = None,
        phase_hook: Callable[[str], None] | None = None,
    ) -> ExternalArtifactCommit:
        staging = Path(staging_root).resolve()
        if staging.parent != self.staging_root or not staging.is_dir():
            raise RuntimeIntegrityError("ExternalArtifactCommit staging 路径越界")
        if producer_scope not in {"core", "project"}:
            raise RuntimeIntegrityError("ExternalArtifactCommit producer_scope 无效")
        self._materialize_staging_links(staging)
        if producer_scope == "project":
            self._apply_project_causal_time_boundary(
                staging,
                artifact_type,
                core_time_facts=core_time_facts,
                key_columns=causal_time_key_columns,
                causal_lineage=causal_lineage,
            )
        elif core_time_facts is not None or causal_time_key_columns or causal_lineage is not None:
            raise RuntimeIntegrityError("只有项目扩展提交可以附加核心时间上下文")
        files, schema_hashes, row_counts = self._inspect(staging, exclude_root_control_files=True)
        manifest = ExternalArtifactCommit.build(
            artifact_name=artifact_name,
            artifact_type=artifact_type,
            commit_token=staging.name,
            files=files,
            schema_hashes=schema_hashes,
            row_counts=row_counts,
        )
        (staging / "manifest.json").write_text(canonical_json(manifest.to_dict()), encoding="utf-8")
        if phase_hook:
            phase_hook("manifest_written")
        (staging / "COMMITTED").write_text(manifest.manifest_hash, encoding="ascii")
        if phase_hook:
            phase_hook("before_commit")
        target = self.objects_root / manifest.semantic_hash
        if target.exists():
            existing = self.verify(manifest.semantic_hash)
            shutil.rmtree(staging, ignore_errors=True)
            if existing.semantic_payload() != manifest.semantic_payload():
                raise RuntimeIntegrityError("同 external artifact key 出现冲突内容")
            return existing
        os.replace(staging, target)
        if self.verification_session is None:
            return self.verify(manifest.semantic_hash)
        return self.verification_session.remember(
            "external_artifact",
            str(target),
            manifest,
        )

    @staticmethod
    def _materialize_staging_links(staging: Path) -> None:
        """把随 staging 移动会断开的链接项复制为普通内容，不处理 hardlink。"""
        links: list[Path] = []
        policy = PathRolePolicy()
        for directory, directories, filenames in policy.walk(staging):
            parent = Path(directory)
            for name in (*directories, *filenames):
                path = parent / name
                resolved = policy.resolve_contained_path(
                    allowed_root=staging, candidate=path, root_role="external_staging",
                    path_role="external_staging_item", expected_kind="any",
                )
                if resolved != parent.resolve() / name and not any(link in path.parents for link in links):
                    links.append(path)
        for path in links:
            temporary = staging.parent / f".linked-content-{uuid.uuid4().hex}"
            try:
                if path.is_dir():
                    shutil.copytree(path.resolve(), temporary, symlinks=False)
                    if path.is_symlink():
                        path.unlink()
                    else:
                        # Windows junction 本身是目录项；rmdir 不递归删除其目标。
                        path.rmdir()
                else:
                    shutil.copyfile(path.resolve(), temporary)
                    path.unlink()
                os.replace(temporary, path)
            finally:
                if temporary.is_dir():
                    shutil.rmtree(temporary)
                elif temporary.exists():
                    temporary.unlink()

    @staticmethod
    def _apply_project_causal_time_boundary(
        staging: Path,
        artifact_type: str,
        *,
        core_time_facts: object | None,
        key_columns: tuple[str, ...],
        causal_lineage: object | None,
    ) -> None:
        protected = _FORMAL_CAUSAL_ARTIFACT_COLUMNS.get(artifact_type)
        if protected is None:
            if core_time_facts is not None or key_columns or causal_lineage is not None:
                raise RuntimeIntegrityError("非 Feature/Label 项目工件不能附加因果时间上下文")
            return
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        parquet_paths = sorted(staging.rglob("*.parquet"))
        if len(parquet_paths) != 1:
            raise RuntimeIntegrityError("项目正式 Feature/Label 每次提交必须恰好一个 Parquet")
        for path in parquet_paths:
            try:
                columns = set(pq.read_schema(path).names)
            except (OSError, pa.ArrowException) as exc:
                raise RuntimeIntegrityError("项目扩展正式时间工件 Parquet 无法读取") from exc
            self_reported = sorted(columns.intersection(protected))
            if self_reported:
                raise RuntimeIntegrityError(
                    f"项目扩展不得提交核心时间事实: {self_reported}"
                )
        if core_time_facts is None:
            raise RuntimeIntegrityError(
                "项目扩展缺少核心伴随时间上下文，不能直接提交正式 Feature/Label"
            )
        if not isinstance(core_time_facts, pd.DataFrame) or not key_columns:
            raise RuntimeIntegrityError("项目扩展核心时间上下文无效")
        path = parquet_paths[0]
        try:
            original = pq.read_table(path)
            metadata = dict(original.schema.metadata or {})
            if _CAUSAL_LINEAGE_METADATA_KEY in metadata:
                raise RuntimeIntegrityError("项目扩展不得提交核心来源 lineage")
            values = original.to_pandas()
            if artifact_type == "research.feature-set.v1":
                attached = attach_core_feature_time_facts(
                    values,
                    core_time_facts,
                    key_columns=key_columns,
                )
            else:
                attached = attach_core_label_time_facts(
                    values,
                    core_time_facts,
                    key_columns=key_columns,
                )
            if causal_lineage is not None:
                if not isinstance(causal_lineage, (list, tuple)) or not causal_lineage:
                    raise RuntimeIntegrityError("项目扩展核心来源 lineage 无效")
                metadata[_CAUSAL_LINEAGE_METADATA_KEY] = canonical_json(
                    list(causal_lineage)
                ).encode("utf-8")
            table = pa.Table.from_pandas(attached, preserve_index=False)
            pq.write_table(table.replace_schema_metadata(metadata), path)
        except RuntimeIntegrityError:
            raise
        except Exception as exc:
            raise RuntimeIntegrityError("项目扩展核心时间上下文合并失败") from exc

    def verify(self, artifact_key: str) -> ExternalArtifactCommit:
        target = (self.objects_root / artifact_key).resolve()
        if self.verification_session is None:
            return self._verify_uncached(target, artifact_key)
        return self.verification_session.resolve(
            "external_artifact",
            str(target),
            lambda: self._verify_uncached(target, artifact_key),
        )

    def _verify_uncached(
        self,
        target: Path,
        artifact_key: str,
    ) -> ExternalArtifactCommit:
        if target.parent != self.objects_root or not target.is_dir():
            raise RuntimeIntegrityError("ExternalArtifactCommit 对象路径越界或不存在")
        try:
            manifest_path, marker_path = PathRolePolicy().resolve_manifest_files(
                allowed_root=target, relative_paths=("manifest.json", "COMMITTED"),
                root_role="external_artifact", file_role="external_artifact_control",
            )
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest = ExternalArtifactCommit.from_dict(payload)
            marker = marker_path.read_text(encoding="ascii")
        except (DataPlaneError, OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeIntegrityError("ExternalArtifactCommit manifest 无法读取") from exc
        if manifest.semantic_hash != artifact_key or marker != manifest.manifest_hash:
            raise RuntimeIntegrityError("ExternalArtifactCommit 身份或 marker 漂移")
        files, schemas, rows = self._inspect(target, exclude_root_control_files=True)
        if files != manifest.files or schemas != manifest.schema_hashes or rows != manifest.row_counts:
            raise RuntimeIntegrityError("ExternalArtifactCommit 文件、schema 或行数漂移")
        return manifest

    def verify_snapshot(
        self,
        commit: ExternalArtifactCommit,
        source_root: str | Path,
        *,
        prefix: str,
    ) -> None:
        """证明领域函数实际读取的目录与 Runtime 输入引用逐字一致。"""
        if not prefix or "/" in prefix or "\\" in prefix or prefix in {".", ".."}:
            raise RuntimeIntegrityError("ExternalArtifactCommit snapshot prefix 无效")
        verified = self.verify(commit.semantic_hash)
        if verified != commit:
            raise RuntimeIntegrityError("ExternalArtifactCommit snapshot 引用漂移")
        if self.verification_session is not None:
            expected_root = self.objects_root / commit.semantic_hash / prefix
            try:
                source = Path(source_root).resolve(strict=True)
                expected = expected_root.resolve(strict=True)
            except OSError as exc:
                raise RuntimeIntegrityError(
                    "Runtime 实际输入目录与 ExternalArtifactCommit 不一致"
                ) from exc
            expected_files = tuple(
                key for key in commit.files if key.startswith(f"{prefix}/")
            )
            if source != expected or not expected_files:
                raise RuntimeIntegrityError(
                    "Runtime 实际输入目录与 ExternalArtifactCommit 不一致"
                )
            return
        files, schemas, rows = self._inspect(
            Path(source_root).resolve(),
            exclude_root_control_files=False,
        )
        prefixed_files = {f"{prefix}/{key}": value for key, value in files.items()}
        prefixed_schemas = {f"{prefix}/{key}": value for key, value in schemas.items()}
        prefixed_rows = {f"{prefix}/{key}": value for key, value in rows.items()}
        expected_files = {
            key: value for key, value in commit.files.items() if key.startswith(f"{prefix}/")
        }
        expected_schemas = {
            key: value for key, value in commit.schema_hashes.items() if key.startswith(f"{prefix}/")
        }
        expected_rows = {
            key: value for key, value in commit.row_counts.items() if key.startswith(f"{prefix}/")
        }
        if (
            prefixed_files != expected_files
            or prefixed_schemas != expected_schemas
            or prefixed_rows != expected_rows
        ):
            raise RuntimeIntegrityError("Runtime 实际输入目录与 ExternalArtifactCommit 不一致")

    @staticmethod
    def _inspect(
        root: Path,
        *,
        exclude_root_control_files: bool,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, int]]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        policy = PathRolePolicy()
        resolved_root = policy.resolve_root(root, role="external_artifact_root")
        files: dict[str, str] = {}
        schemas: dict[str, str] = {}
        rows: dict[str, int] = {}
        for directory, dirnames, filenames in policy.walk(resolved_root):
            directory_path = Path(directory)
            for name in sorted(dirnames):
                policy.resolve_contained_path(
                    allowed_root=resolved_root,
                    candidate=directory_path / name,
                    root_role="external_artifact_root",
                    path_role="external_artifact_directory",
                    expected_kind="directory",
                )
            for name in sorted(filenames):
                if (
                    exclude_root_control_files
                    and directory_path == resolved_root
                    and name in {"manifest.json", "COMMITTED"}
                ):
                    continue
                path = policy.resolve_contained_path(
                    allowed_root=resolved_root,
                    candidate=directory_path / name,
                    root_role="external_artifact_root",
                    path_role="external_artifact_file",
                    expected_kind="file",
                )
                relative = (directory_path / name).relative_to(resolved_root).as_posix()
                files[relative] = _sha256(path)
                if path.suffix.lower() == ".parquet":
                    try:
                        metadata = pq.read_metadata(path)
                    except (OSError, pa.ArrowException) as exc:
                        raise RuntimeIntegrityError(
                            f"ExternalArtifactCommit Parquet 无法读取: {relative}"
                        ) from exc
                    schemas[relative] = typed_canonical_hash({"schema": str(metadata.schema.to_arrow_schema())})
                    rows[relative] = metadata.num_rows
        if not files:
            raise RuntimeIntegrityError("ExternalArtifactCommit 不能提交空目录")
        return dict(sorted(files.items())), dict(sorted(schemas.items())), dict(sorted(rows.items()))


__all__ = [
    "EXTERNAL_ARTIFACT_COMMIT_VERSION",
    "ExternalArtifactCommit",
    "ExternalArtifactStore",
]
