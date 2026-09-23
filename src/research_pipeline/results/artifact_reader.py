"""Result 层对 Runtime 外部目录工件的窄只读解析器。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.data_plane import DataPlaneError, PathRolePolicy
from research_pipeline.platform import typed_canonical_hash

from .errors import ResultContractError


EXTERNAL_ARTIFACT_COMMIT_VERSION = "research-external-artifact-commit-v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()






def _digest_mapping(
    value: object,
    field: str,
    *,
    allow_empty: bool = False,
) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or (not allow_empty and not value):
        raise ResultContractError(f"外部工件 {field} 无效")
    normalized: dict[str, str] = {}
    for key, digest in value.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ResultContractError(f"外部工件 {field} 含无效条目")
        normalized[key] = digest
    return MappingProxyType(dict(sorted(normalized.items())))


@dataclass(frozen=True)
class RuntimeArtifactReference:
    name: str
    artifact_type: str
    artifact_key: str
    content_hash: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "RuntimeArtifactReference":
        expected = {"name", "artifact_type", "artifact_key", "content_hash"}
        if set(payload) != expected or any(
            not isinstance(payload[field], str) or not payload[field]
            for field in expected
        ):
            raise ResultContractError("Runtime ArtifactRef schema 无效")
        return cls(*(str(payload[field]) for field in (
            "name", "artifact_type", "artifact_key", "content_hash",
        )))


@dataclass(frozen=True)
class ExternalArtifactSnapshot:
    artifact_name: str
    artifact_type: str
    commit_token: str
    files: Mapping[str, str]
    schema_hashes: Mapping[str, str]
    row_counts: Mapping[str, int]
    semantic_hash: str
    manifest_hash: str
    contract_version: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExternalArtifactSnapshot":
        expected = {
            "artifact_name", "artifact_type", "commit_token", "files", "schema_hashes",
            "row_counts", "semantic_hash", "manifest_hash", "contract_version",
        }
        if set(payload) != expected:
            raise ResultContractError("外部工件 manifest schema 无效")
        files = _digest_mapping(payload["files"], "files")
        schemas = _digest_mapping(payload["schema_hashes"], "schema_hashes", allow_empty=True)
        raw_rows = payload["row_counts"]
        if not isinstance(raw_rows, Mapping):
            raise ResultContractError("外部工件 row_counts 无效")
        rows = {
            key: value
            for key, value in raw_rows.items()
            if isinstance(key, str) and key and type(value) is int and value >= 0
        }
        if len(rows) != len(raw_rows) or set(rows) != set(schemas):
            raise ResultContractError("外部工件 row_count/schema 不闭合")
        snapshot = cls(
            str(payload["artifact_name"]),
            str(payload["artifact_type"]),
            str(payload["commit_token"]),
            files,
            schemas,
            MappingProxyType(dict(sorted(rows.items()))),
            str(payload["semantic_hash"]),
            str(payload["manifest_hash"]),
            str(payload["contract_version"]),
        )
        if snapshot.contract_version != EXTERNAL_ARTIFACT_COMMIT_VERSION:
            raise ResultContractError("外部工件版本不受支持")
        semantic_payload = {
            "artifact_name": snapshot.artifact_name,
            "artifact_type": snapshot.artifact_type,
            "files": dict(snapshot.files),
            "schema_hashes": dict(snapshot.schema_hashes),
            "row_counts": dict(snapshot.row_counts),
            "contract_version": snapshot.contract_version,
        }
        if snapshot.semantic_hash != typed_canonical_hash(semantic_payload):
            raise ResultContractError("外部工件 semantic hash 不一致")
        manifest_payload = {
            **semantic_payload,
            "commit_token": snapshot.commit_token,
            "semantic_hash": snapshot.semantic_hash,
        }
        if snapshot.manifest_hash != typed_canonical_hash(manifest_payload):
            raise ResultContractError("外部工件 manifest hash 不一致")
        return snapshot

    @property
    def artifact_reference(self) -> RuntimeArtifactReference:
        return RuntimeArtifactReference(
            self.artifact_name,
            self.artifact_type,
            self.semantic_hash,
            self.manifest_hash,
        )


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

    def verify(self, artifact_key: str) -> ExternalArtifactSnapshot:
        snapshot = self.inspect(artifact_key)
        target = self.objects_root / artifact_key
        try:
            files, schemas, rows = self._inspect(target)
        except DataPlaneError as exc:
            raise ResultContractError("外部工件内部路径安全验证失败") from exc
        if (
            files != dict(snapshot.files)
            or schemas != dict(snapshot.schema_hashes)
            or rows != dict(snapshot.row_counts)
        ):
            raise ResultContractError("外部工件文件、schema 或行数漂移")
        return snapshot

    def inspect(self, artifact_key: str) -> ExternalArtifactSnapshot:
        """只验证 commit 与目录闭包；文件字节留给复制流单次校验。"""

        if (
            not isinstance(artifact_key, str)
            or len(artifact_key) != 64
            or any(character not in "0123456789abcdef" for character in artifact_key)
        ):
            raise ResultContractError("外部工件 key 无效")
        try:
            target = PathRolePolicy().resolve_contained_path(
                allowed_root=self.objects_root,
                candidate=self.objects_root / artifact_key,
                root_role="result_runtime_objects",
                path_role="result_runtime_object",
                expected_kind="directory",
            )
        except DataPlaneError as exc:
            raise ResultContractError("外部工件对象路径越界或不存在") from exc
        try:
            manifest_path, marker_path = PathRolePolicy().resolve_manifest_files(
                allowed_root=target, relative_paths=("manifest.json", "COMMITTED"),
                root_role="result_runtime_object", file_role="result_runtime_control",
            )
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            marker = marker_path.read_text(encoding="ascii")
        except (DataPlaneError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResultContractError("外部工件 manifest 无法读取") from exc
        if not isinstance(payload, Mapping):
            raise ResultContractError("外部工件 manifest 必须是对象")
        snapshot = ExternalArtifactSnapshot.from_dict(payload)
        if snapshot.semantic_hash != artifact_key or marker != snapshot.manifest_hash:
            raise ResultContractError("外部工件身份或 COMMITTED 漂移")
        actual_paths = self._list_payload_paths(target)
        if actual_paths != set(snapshot.files):
            raise ResultContractError("外部工件文件集合漂移")
        return snapshot

    def copy_file(
        self,
        snapshot: ExternalArtifactSnapshot,
        source_relative_path: str,
        destination: str | Path,
    ) -> None:
        """在一次源文件读取中完成复制与内容身份校验。"""

        source = self._snapshot_file(snapshot, source_relative_path)
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        try:
            with source.open("rb") as input_file, target.open("xb") as output_file:
                for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                    digest.update(chunk)
                    output_file.write(chunk)
                output_file.flush()
                os.fsync(output_file.fileno())
        except OSError as exc:
            raise ResultContractError(
                f"外部工件文件无法复制: {source_relative_path}"
            ) from exc
        if digest.hexdigest() != snapshot.files[source_relative_path]:
            raise ResultContractError(
                f"外部工件文件复制时发生 hash 漂移: {source_relative_path}"
            )
        if source_relative_path in snapshot.schema_hashes:
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
        snapshot: ExternalArtifactSnapshot,
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
        snapshot: ExternalArtifactSnapshot,
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
        snapshot: ExternalArtifactSnapshot,
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

    @staticmethod
    def _inspect(root: Path) -> tuple[dict[str, str], dict[str, str], dict[str, int]]:
        files: dict[str, str] = {}
        schemas: dict[str, str] = {}
        rows: dict[str, int] = {}
        policy = PathRolePolicy()
        for directory, dirnames, filenames in policy.walk(root):
            directory_path = Path(directory)
            for name in sorted(dirnames):
                policy.resolve_contained_path(
                    allowed_root=root,
                    candidate=directory_path / name,
                    root_role="result_runtime_object",
                    path_role="result_runtime_directory",
                    expected_kind="directory",
                )
            for name in sorted(filenames):
                if directory_path == root and name in {"manifest.json", "COMMITTED"}:
                    continue
                path = policy.resolve_contained_path(
                    allowed_root=root,
                    candidate=directory_path / name,
                    root_role="result_runtime_object",
                    path_role="result_runtime_file",
                    expected_kind="file",
                )
                relative = (directory_path / name).relative_to(root).as_posix()
                files[relative] = _sha256(path)
                if path.suffix.lower() == ".parquet":
                    try:
                        metadata = pq.read_metadata(path)
                    except (OSError, pa.ArrowException) as exc:
                        raise ResultContractError(f"外部工件 Parquet 无法读取: {relative}") from exc
                    schemas[relative] = typed_canonical_hash({
                        "schema": str(metadata.schema.to_arrow_schema())
                    })
                    rows[relative] = metadata.num_rows
        if not files:
            raise ResultContractError("外部工件不能为空")
        return dict(sorted(files.items())), dict(sorted(schemas.items())), dict(sorted(rows.items()))

    @staticmethod
    def _list_payload_paths(root: Path) -> set[str]:
        paths: set[str] = set()
        policy = PathRolePolicy()
        for directory, dirnames, filenames in policy.walk(root):
            directory_path = Path(directory)
            for name in sorted(dirnames):
                policy.resolve_contained_path(
                    allowed_root=root,
                    candidate=directory_path / name,
                    root_role="result_runtime_object",
                    path_role="result_runtime_directory",
                    expected_kind="directory",
                )
            for name in sorted(filenames):
                if directory_path == root and name in {"manifest.json", "COMMITTED"}:
                    continue
                policy.resolve_contained_path(
                    allowed_root=root,
                    candidate=directory_path / name,
                    root_role="result_runtime_object",
                    path_role="result_runtime_file",
                    expected_kind="file",
                )
                paths.add((directory_path / name).relative_to(root).as_posix())
        return paths


__all__ = [
    "ExternalArtifactReader",
    "ExternalArtifactSnapshot",
    "RuntimeArtifactReference",
]
