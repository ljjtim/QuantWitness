"""Result 层对 Runtime 外部目录工件的窄只读解析器。"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.data_plane import DataPlaneError, PathRolePolicy
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.audit_mode import strict_audit_enabled
from research_pipeline.platform.immutable_files import link_readonly_file, unlink_readonly_file
from research_pipeline.platform.validation import file_sha256
from research_pipeline.runtime.errors import RuntimeIntegrityError
from research_pipeline.runtime.external_artifact import ExternalArtifactCommit, ExternalArtifactStore

from .errors import ResultContractError


class ExternalArtifactReader:
    """只读取并复验 Runtime 已原子提交的目录工件。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.objects_root = self.root / "objects"
        if not self.root.is_dir() or not self.objects_root.is_dir():
            raise ResultContractError("外部工件仓库不存在")
        try:
            PathRolePolicy().resolve_root(self.root, role="result_runtime_artifacts")
            PathRolePolicy().resolve_root(self.objects_root, role="result_runtime_objects")
        except DataPlaneError as exc:
            raise ResultContractError("外部工件仓库路径安全验证失败") from exc

    def _read_manifest(self, artifact_key: str) -> tuple[Path, ExternalArtifactCommit]:
        if not isinstance(artifact_key, str):
            raise ResultContractError("外部工件键必须是字符串")
        try:
            target = PathRolePolicy().resolve_contained_path(
                allowed_root=self.objects_root,
                candidate=self.objects_root / artifact_key,
                root_role="result_runtime_objects",
                path_role="result_runtime_object",
                expected_kind="directory",
            )
            return target, ExternalArtifactStore.read_manifest(self.objects_root, target, artifact_key)
        except (DataPlaneError, RuntimeIntegrityError, OSError) as exc:
            raise ResultContractError(f"外部工件提交无法读取: {exc}") from exc

    def verify(self, artifact_key: str) -> ExternalArtifactCommit:
        target, commit = self._read_manifest(artifact_key)
        try:
            files, schemas, rows = ExternalArtifactStore._inspect(
                target, exclude_root_control_files=True,
            )
        except (DataPlaneError, RuntimeIntegrityError, OSError) as exc:
            raise ResultContractError(f"外部工件内容无法核验: {exc}") from exc
        if files != commit.files or schemas != commit.schema_hashes or rows != commit.row_counts:
            raise ResultContractError("外部工件文件、schema 或行数漂移")
        return commit

    def inspect(self, artifact_key: str) -> ExternalArtifactCommit:
        """校验提交与目录闭包，实际消费时核验文件内容。"""
        target, commit = self._read_manifest(artifact_key)
        try:
            paths = {name for name, _ in ExternalArtifactStore._iter_payload_paths(
                target, exclude_root_control_files=True,
            )}
        except (DataPlaneError, RuntimeIntegrityError, OSError) as exc:
            raise ResultContractError(f"外部工件文件集合无法核验: {exc}") from exc
        if paths != set(commit.files):
            raise ResultContractError("外部工件文件集合漂移")
        return commit

    def copy_file(
        self,
        snapshot: ExternalArtifactCommit,
        source_relative_path: str,
        destination: str | Path,
    ) -> None:
        """在一次源文件读取中完成复制与内容身份校验。"""

        source = self._snapshot_file(snapshot, source_relative_path)
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        strict_audit = strict_audit_enabled()
        linked = False
        digest = hashlib.sha256()
        try:
            linked = not strict_audit and link_readonly_file(source, target, freeze_owned=True)
            if linked:
                actual_hash = file_sha256(target)
            else:
                with source.open("rb") as input_file, target.open("xb") as output_file:
                    for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                        digest.update(chunk)
                        output_file.write(chunk)
                    if strict_audit:
                        output_file.flush()
                        os.fsync(output_file.fileno())
                actual_hash = digest.hexdigest()
        except OSError as exc:
            if linked:
                unlink_readonly_file(target)
            raise ResultContractError(
                f"外部工件文件无法复制: {source_relative_path}"
            ) from exc
        if actual_hash != snapshot.files[source_relative_path]:
            if linked:
                unlink_readonly_file(target)
            raise ResultContractError(
                f"外部工件文件复制时发生 hash 漂移: {source_relative_path}"
            )
        if not linked and source_relative_path in snapshot.schema_hashes:
            try:
                metadata = pq.read_metadata(target)
            except (OSError, pa.ArrowException) as exc:
                raise ResultContractError(
                    f"外部工件 Parquet 无法读取: {source_relative_path}"
                ) from exc
            schema_hash = typed_canonical_hash({
                "schema": str(metadata.schema.to_arrow_schema())
            })
            if (
                schema_hash != snapshot.schema_hashes[source_relative_path]
                or metadata.num_rows != snapshot.row_counts[source_relative_path]
            ):
                raise ResultContractError(
                    f"外部工件 Parquet schema 或行数漂移: {source_relative_path}"
                )

    def read_parquet(
        self,
        snapshot: ExternalArtifactCommit,
        relative_path: str,
    ) -> pa.Table:
        """从同一已验证文件句柄读取 Parquet，避免验证后按路径重开。"""

        if (
            relative_path not in snapshot.schema_hashes
            or relative_path not in snapshot.row_counts
        ):
            raise ResultContractError(f"外部工件未声明 Parquet: {relative_path}")
        path = self._snapshot_file(snapshot, relative_path)
        try:
            with path.open("rb") as handle:
                digest = hashlib.sha256()
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
                if digest.hexdigest() != snapshot.files[relative_path]:
                    raise ResultContractError(f"外部工件文件 hash 漂移: {relative_path}")
                handle.seek(0)
                table = pq.read_table(handle)
        except (OSError, pa.ArrowException) as exc:
            raise ResultContractError(f"外部工件 Parquet 无法读取: {relative_path}") from exc
        schema_hash = typed_canonical_hash({"schema": str(table.schema)})
        if (
            schema_hash != snapshot.schema_hashes[relative_path]
            or table.num_rows != snapshot.row_counts[relative_path]
        ):
            raise ResultContractError(f"外部工件 Parquet schema 或行数漂移: {relative_path}")
        return table

    def read_bytes(
        self,
        snapshot: ExternalArtifactCommit,
        relative_path: str,
    ) -> bytes:
        """从同一已验证文件句柄读取并复验实际返回字节。"""

        path = self._snapshot_file(snapshot, relative_path)
        try:
            with path.open("rb") as handle:
                content = handle.read()
        except OSError as exc:
            raise ResultContractError(f"外部工件文件无法读取: {relative_path}") from exc
        if hashlib.sha256(content).hexdigest() != snapshot.files[relative_path]:
            raise ResultContractError(f"外部工件文件读取时发生 hash 漂移: {relative_path}")
        return content

    def _snapshot_file(
        self,
        snapshot: ExternalArtifactCommit,
        relative_path: str,
    ) -> Path:
        if relative_path not in snapshot.files:
            raise ResultContractError(f"外部工件未声明文件: {relative_path}")
        try:
            return PathRolePolicy().resolve_contained_path(
                allowed_root=self.objects_root / snapshot.semantic_hash,
                candidate=relative_path,
                root_role="result_runtime_object",
                path_role="result_runtime_file",
                expected_kind="file",
            )
        except DataPlaneError as exc:
            raise ResultContractError(f"外部工件文件路径无效: {relative_path}") from exc


__all__ = ["ExternalArtifactReader"]
