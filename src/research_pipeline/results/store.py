"""ResultBundle 的自包含、不可覆盖目录存储。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
from types import MappingProxyType
from typing import BinaryIO, Callable, Mapping
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.data_plane import DataPlaneError, PathRolePolicy
from research_pipeline.platform import canonical_json, typed_canonical_hash
from .artifact_reader import ExternalArtifactReader, ExternalArtifactSnapshot
from .contracts import ResultBundle
from .errors import ResultContractError


RESULT_MANIFEST_NAME = "result.json"
RESULT_COMMITTED_NAME = "COMMITTED"
_HASH_CHUNK_BYTES = 1024 * 1024


def _stream_sha256(handle: BinaryIO) -> str:
    """分块计算已打开文件的摘要，不把压缩文件整体复制进内存。"""

    digest = hashlib.sha256()
    while chunk := handle.read(_HASH_CHUNK_BYTES):
        digest.update(chunk)
    return digest.hexdigest()






@dataclass(frozen=True)
class ResultSnapshot:
    """一次操作内已经完整验证、可直接消费的 Result 快照。"""

    bundle: ResultBundle
    directory: Path
    tables: Mapping[str, pa.Table]
    support_bytes: Mapping[str, bytes]
    verified_schema_ids: frozenset[str] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tables", MappingProxyType(dict(self.tables)))
        object.__setattr__(
            self, "support_bytes", MappingProxyType(dict(self.support_bytes))
        )
        verified_schema_ids = self.verified_schema_ids
        if verified_schema_ids is None:
            # 直接构造只用于测试内存快照；ResultStore 会始终显式传入本次
            # 已做摘要和 footer 校验的 schema 集合。
            verified_schema_ids = frozenset(
                table.schema_id for table in self.bundle.tables
            )
        object.__setattr__(
            self,
            "verified_schema_ids",
            frozenset({*verified_schema_ids, *self.tables}),
        )

    def _require_verified_schema(self, schema_id: str) -> None:
        if schema_id not in self.verified_schema_ids:
            raise ResultContractError(
                f"ResultBundle 表未进入本次已验证快照: {schema_id}"
            )

    def table_manifest(self, schema_id: str):
        selected = tuple(
            table for table in self.bundle.tables if table.schema_id == schema_id
        )
        if len(selected) != 1:
            raise ResultContractError(
                f"ResultBundle 必须恰好包含一个 schema: {schema_id}"
            )
        return selected[0]

    def table_schema(self, schema_id: str) -> pa.Schema:
        """读取正式表 schema；列式快照不需要为此物化数据。"""

        if schema_id in self.tables:
            return self.tables[schema_id].schema
        self._require_verified_schema(schema_id)
        manifest = self.table_manifest(schema_id)
        selected_schema = None
        for relative_path in manifest.files:
            current = pq.ParquetFile(
                self.directory / relative_path
            ).schema_arrow
            if selected_schema is None:
                selected_schema = current
            elif current != selected_schema:
                raise ResultContractError(
                    f"ResultBundle 分区 schema 不一致: {schema_id}"
                )
        if selected_schema is None:
            raise ResultContractError(f"ResultBundle 表没有分区: {schema_id}")
        return selected_schema

    def table_row_count(self, schema_id: str) -> int:
        """从 Result manifest 读取表行数，不扫描 Parquet 数据页。"""

        if schema_id in self.tables:
            return self.tables[schema_id].num_rows
        self._require_verified_schema(schema_id)
        return sum(self.table_manifest(schema_id).row_counts.values())

    def table_uncompressed_bytes(
        self,
        schema_id: str,
        *,
        columns: tuple[str, ...] | None = None,
    ) -> int:
        """从 Parquet footer 汇总实际未压缩列块字节，用于整表读取前预算。"""

        if schema_id in self.tables:
            table = self.tables[schema_id]
            if columns is not None:
                table = table.select(columns)
            return int(table.nbytes)
        self._require_verified_schema(schema_id)
        selected = None if columns is None else set(columns)
        total = 0
        for relative_path in self.table_manifest(schema_id).files:
            parquet = pq.ParquetFile(self.directory / relative_path)
            metadata = parquet.metadata
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                for column_index in range(row_group.num_columns):
                    column = row_group.column(column_index)
                    root_name = column.path_in_schema.split(".", 1)[0]
                    if selected is None or root_name in selected:
                        total += int(column.total_uncompressed_size)
        return total

    def iter_table_batches(
        self,
        schema_id: str,
        *,
        columns: tuple[str, ...] | None = None,
        batch_size: int = 8_192,
    ):
        """从已验证快照逐分区读取所需列，不默认拼成整表。"""

        if schema_id in self.tables:
            table = self.tables[schema_id]
            if columns is not None:
                table = table.select(columns)
            yield from table.to_batches(max_chunksize=batch_size)
            return
        self._require_verified_schema(schema_id)
        manifest = self.table_manifest(schema_id)
        for relative_path in manifest.files:
            try:
                yield from pq.ParquetFile(
                    self.directory / relative_path
                ).iter_batches(
                    columns=None if columns is None else list(columns),
                    batch_size=batch_size,
                )
            except (OSError, pa.ArrowException) as exc:
                raise ResultContractError(
                    f"ResultBundle 分区读取失败: {relative_path}"
                ) from exc

    def read_table(
        self,
        schema_id: str,
        *,
        columns: tuple[str, ...] | None = None,
        max_uncompressed_bytes: int | None = None,
    ) -> pa.Table:
        """仅供确实需要完整矩阵的小表消费者使用。"""

        if max_uncompressed_bytes is not None:
            if type(max_uncompressed_bytes) is not int or max_uncompressed_bytes <= 0:
                raise ResultContractError("ResultBundle 整表预算必须为正整数")
            estimated = self.table_uncompressed_bytes(schema_id, columns=columns)
            if estimated > max_uncompressed_bytes:
                raise ResultContractError(
                    f"ResultBundle 表超出整表读取预算: {schema_id} "
                    f"({estimated} > {max_uncompressed_bytes})"
                )

        if columns is None and schema_id in self.tables:
            return self.tables[schema_id]
        batches = tuple(
            self.iter_table_batches(schema_id, columns=columns)
        )
        if not batches:
            manifest = self.table_manifest(schema_id)
            first_path = self.directory / next(iter(manifest.files))
            schema = pq.ParquetFile(first_path).schema_arrow
            if columns is not None:
                schema = pa.schema([schema.field(name) for name in columns])
            return pa.Table.from_batches((), schema=schema)
        try:
            return pa.Table.from_batches(batches)
        except (pa.ArrowException, TypeError) as exc:
            raise ResultContractError(
                f"ResultBundle 分区 schema 不一致: {schema_id}"
            ) from exc


class ResultStore:
    """唯一 ResultBundle namespace：root/project/run/result_id。"""

    def __init__(self, root: str | Path, *, create: bool = True) -> None:
        self.root = Path(root).absolute()
        if create:
            self.root.mkdir(parents=True, exist_ok=True)
        elif not self.root.is_dir():
            raise ResultContractError("result store 不存在")
        try:
            self.root = PathRolePolicy().resolve_root(self.root, role="result_store")
        except DataPlaneError as exc:
            raise ResultContractError("ResultStore 路径安全验证失败") from exc
        self.staging_root = self.root / ".staging"
        if create:
            self.staging_root.mkdir(exist_ok=True)
            try:
                PathRolePolicy().resolve_root(self.staging_root, role="result_staging")
            except DataPlaneError as exc:
                raise ResultContractError("ResultStore staging 路径安全验证失败") from exc

    def result_directory(self, bundle: ResultBundle) -> Path:
        return self._namespace(bundle.project_id, bundle.run_id) / bundle.result_id

    def publish(
        self,
        bundle: ResultBundle,
        *,
        run_root: str | Path,
        phase_hook: Callable[[str], None] | None = None,
    ) -> Path:
        """从 Runtime 工件单次读取并复制，之后 Result 不再依赖 run-root。"""

        try:
            runtime_root = PathRolePolicy().resolve_root(
                run_root, role="result_runtime_root"
            )
            PathRolePolicy().validate(
                {"result_store": self.root, "runtime_root": runtime_root},
                read_only_roles=("runtime_root",),
            )
        except DataPlaneError as exc:
            raise ResultContractError("ResultBundle 发布路径安全验证失败") from exc
        namespace = self._namespace(bundle.project_id, bundle.run_id)
        namespace.mkdir(parents=True, exist_ok=True)
        target = namespace / bundle.result_id
        existing = tuple(sorted(path for path in namespace.iterdir()))
        if existing:
            if existing != (target,) or not target.is_dir():
                raise ResultContractError("同一成功 run 已存在另一份 canonical ResultBundle")
            if self.verify(target) != bundle:
                raise ResultContractError("既有 ResultBundle 与待发布内容冲突")
            return target

        staging = self.staging_root / uuid.uuid4().hex
        staging.mkdir(exist_ok=False)
        try:
            if staging.stat().st_dev != namespace.stat().st_dev:
                raise ResultContractError("ResultBundle staging 与目标不在同一卷")
            self._materialize_files(staging, bundle, runtime_root)
            self._write_controls(staging, bundle)
            staged = self._read_controls(staging, require_namespace=False)
            if staged != bundle:
                raise ResultContractError("staged ResultBundle 与待发布对象不一致")
            if phase_hook:
                phase_hook("prepared")
            if tuple(namespace.iterdir()):
                raise ResultContractError("ResultBundle 发布前 namespace 被并发占用")
            os.replace(staging, target)
            if phase_hook:
                phase_hook("renamed")
            # 复制流已经逐文件完成内容/schema/行数校验；rename 后只复核小型控制文件。
            if self._read_controls(target, require_namespace=True) != bundle:
                raise ResultContractError("ResultBundle 原子写后验证失败")
            return target
        except Exception:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
            raise

    def verify(self, result_directory: str | Path) -> ResultBundle:
        """完整验证自包含 Result。"""

        return self.open_snapshot(result_directory).bundle

    def open_snapshot(
        self,
        result_directory: str | Path,
        *,
        schema_ids: tuple[str, ...] = (),
        verify_schema_ids: tuple[str, ...] = (),
        support_paths: tuple[str, ...] = (),
        verify_all_files: bool = True,
    ) -> ResultSnapshot:
        directory = Path(result_directory).absolute()
        try:
            PathRolePolicy().resolve_contained_path(
                allowed_root=self.root,
                candidate=directory,
                root_role="result_store",
                path_role="result_bundle",
                expected_kind="directory",
            )
        except DataPlaneError as exc:
            raise ResultContractError("ResultBundle 路径安全验证失败") from exc
        return self._verify_directory(
            directory.resolve(),
            requested_schema_ids=schema_ids,
            requested_lazy_schema_ids=verify_schema_ids,
            requested_support_paths=support_paths,
            verify_all_files=verify_all_files,
        )

    def load_by_identity(
        self,
        *,
        project_id: str,
        run_id: str,
        result_id: str,
    ) -> ResultBundle:
        return self.load_snapshot_by_identity(
            project_id=project_id, run_id=run_id, result_id=result_id
        ).bundle

    def inspect_by_identity(
        self,
        *,
        project_id: str,
        run_id: str,
        result_id: str,
    ) -> ResultBundle:
        """只读取小型控制文件，用于决定一次完整快照要加载哪些表。"""

        target = self._identity_path(project_id, run_id, result_id)
        return self._read_controls(target, require_namespace=True)

    def inspect_directory(self, directory: str | Path) -> ResultBundle:
        """只读 Result 控制文件，用于从显式目录解析内容身份。"""

        return self._read_controls(Path(directory).resolve(), require_namespace=True)

    def load_snapshot_by_identity(
        self,
        *,
        project_id: str,
        run_id: str,
        result_id: str,
        schema_ids: tuple[str, ...] = (),
        verify_schema_ids: tuple[str, ...] = (),
        support_paths: tuple[str, ...] = (),
        verify_all_files: bool = True,
    ) -> ResultSnapshot:
        target = self._identity_path(project_id, run_id, result_id)
        return self.open_snapshot(
            target,
            schema_ids=schema_ids,
            verify_schema_ids=verify_schema_ids,
            support_paths=support_paths,
            verify_all_files=verify_all_files,
        )

    def _identity_path(self, project_id: str, run_id: str, result_id: str) -> Path:
        from .contracts import ResultReference

        reference_payload = {
            "project_id": project_id,
            "run_id": run_id,
            "result_id": result_id,
            "contract_version": "research-result-ref-v1",
        }
        reference = ResultReference(
            project_id,
            run_id,
            result_id,
            typed_canonical_hash(reference_payload),
        )
        return self._namespace(reference.project_id, reference.run_id) / reference.result_id

    def read_table_by_schema_id(
        self,
        bundle: ResultBundle,
        *,
        schema_id: str,
    ) -> pa.Table:
        snapshot = self.open_snapshot(
            self.result_directory(bundle), schema_ids=(schema_id,)
        )
        self._require_expected_bundle(snapshot, bundle)
        return snapshot.tables[schema_id]

    def read_tables_by_schema_ids(
        self,
        bundle: ResultBundle,
        *,
        schema_ids: tuple[str, ...],
    ) -> dict[str, pa.Table]:
        snapshot = self.open_snapshot(
            self.result_directory(bundle), schema_ids=schema_ids
        )
        self._require_expected_bundle(snapshot, bundle)
        return dict(snapshot.tables)

    def read_artifact_bytes(
        self,
        bundle: ResultBundle,
        *,
        relative_path: str,
    ) -> bytes:
        return self.read_artifact_bytes_many(
            bundle, relative_paths=(relative_path,)
        )[relative_path]

    def read_artifact_bytes_many(
        self,
        bundle: ResultBundle,
        *,
        relative_paths: tuple[str, ...],
    ) -> dict[str, bytes]:
        snapshot = self.open_snapshot(
            self.result_directory(bundle), support_paths=relative_paths
        )
        self._require_expected_bundle(snapshot, bundle)
        return dict(snapshot.support_bytes)

    @staticmethod
    def _require_expected_bundle(snapshot: ResultSnapshot, bundle: ResultBundle) -> None:
        if snapshot.bundle != bundle:
            raise ResultContractError("待读取 ResultBundle 与已验证内容不一致")

    def _materialize_files(
        self,
        staging: Path,
        bundle: ResultBundle,
        runtime_root: Path,
    ) -> None:
        reader = ExternalArtifactReader(runtime_root / "external-artifacts")
        snapshots: dict[str, ExternalArtifactSnapshot] = {}

        def commit(artifact_key: str) -> ExternalArtifactSnapshot:
            selected = snapshots.get(artifact_key)
            if selected is None:
                selected = reader.inspect(artifact_key)
                snapshots[artifact_key] = selected
            return selected

        for table in bundle.tables:
            source = commit(table.artifact_key)
            if (
                source.manifest_hash != table.artifact_manifest_hash
                or source.artifact_type != table.artifact_type
            ):
                raise ResultContractError("ResultBundle table 的 Artifact 引用漂移")
            internal_prefix = f"tables/{table.table_id}/"
            source_prefix = f"{table.path_prefix}/"
            for relative_path, expected_hash in table.files.items():
                suffix = relative_path.removeprefix(internal_prefix)
                if suffix == relative_path:
                    raise ResultContractError("ResultBundle table 未使用内部相对路径")
                source_path = f"{source_prefix}{suffix}"
                if (
                    source.files.get(source_path) != expected_hash
                    or source.schema_hashes.get(source_path)
                    != table.schema_hashes[relative_path]
                    or source.row_counts.get(source_path) != table.row_counts[relative_path]
                ):
                    raise ResultContractError("ResultBundle table 文件/schema/row_count 漂移")
                reader.copy_file(source, source_path, staging / relative_path)
        for support in bundle.support_files:
            source = commit(support.artifact_key)
            if source.files.get(support.source_path) != support.content_hash:
                raise ResultContractError("ResultBundle support file 引用漂移")
            reader.copy_file(
                source, support.source_path, staging / support.relative_path
            )

    @staticmethod
    def _write_controls(directory: Path, bundle: ResultBundle) -> None:
        manifest_path = directory / RESULT_MANIFEST_NAME
        marker_path = directory / RESULT_COMMITTED_NAME
        with manifest_path.open("x", encoding="utf-8") as handle:
            handle.write(canonical_json(bundle.to_dict()))
            handle.flush()
            os.fsync(handle.fileno())
        with marker_path.open("x", encoding="ascii") as handle:
            handle.write(bundle.result_id)
            handle.flush()
            os.fsync(handle.fileno())

    def _read_controls(self, directory: Path, *, require_namespace: bool) -> ResultBundle:
        try:
            PathRolePolicy().resolve_contained_path(
                allowed_root=self.root,
                candidate=directory,
                root_role="result_store",
                path_role="result_bundle",
                expected_kind="directory",
            )
        except DataPlaneError as exc:
            raise ResultContractError("ResultBundle 路径安全验证失败") from exc
        try:
            manifest_path, marker_path = PathRolePolicy().resolve_manifest_files(
                allowed_root=directory, relative_paths=(RESULT_MANIFEST_NAME, RESULT_COMMITTED_NAME),
                root_role="result_bundle", file_role="result_control",
            )
            raw = manifest_path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            marker = marker_path.read_text(encoding="ascii")
        except (DataPlaneError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResultContractError("ResultBundle manifest 无法读取") from exc
        if not isinstance(payload, dict):
            raise ResultContractError("ResultBundle manifest 必须是对象")
        bundle = ResultBundle.from_dict(payload)
        if raw != canonical_json(bundle.to_dict()) or marker != bundle.result_id:
            raise ResultContractError("ResultBundle manifest 或 COMMITTED 非规范/漂移")
        relative = directory.relative_to(self.root)
        if require_namespace and relative.parts[-3:] != (
            bundle.project_id, bundle.run_id, bundle.result_id,
        ):
            raise ResultContractError("ResultBundle namespace 与内容身份不一致")
        return bundle

    def _verify_directory(
        self,
        directory: Path,
        *,
        requested_schema_ids: tuple[str, ...],
        requested_lazy_schema_ids: tuple[str, ...],
        requested_support_paths: tuple[str, ...],
        verify_all_files: bool,
    ) -> ResultSnapshot:
        bundle = self._read_controls(directory, require_namespace=True)
        manifests = {}
        for schema_id in (*requested_schema_ids, *requested_lazy_schema_ids):
            selected = tuple(table for table in bundle.tables if table.schema_id == schema_id)
            if len(selected) != 1:
                raise ResultContractError(f"ResultBundle 必须恰好包含一个 schema: {schema_id}")
            manifests[schema_id] = selected[0]
        support_matches = {}
        for source_path in requested_support_paths:
            matches = tuple(
                item for item in bundle.support_files if item.source_path == source_path
            )
            if len(matches) != 1:
                raise ResultContractError(f"ResultBundle 控制文件必须唯一: {source_path}")
            support_matches[source_path] = matches[0]

        expected_files = {
            RESULT_MANIFEST_NAME,
            RESULT_COMMITTED_NAME,
            *(path for table in bundle.tables for path in table.files),
            *(item.relative_path for item in bundle.support_files),
        }
        actual_files = self._result_payload_paths(directory)
        if actual_files != expected_files:
            raise ResultContractError("ResultBundle 文件集合不精确或含 extra file")

        requested_internal_paths = {
            path
            for manifest in manifests.values()
            for path in manifest.files
        }
        materialized_internal_paths = {
            path
            for schema_id in requested_schema_ids
            for path in manifests[schema_id].files
        }
        table_parts: dict[str, list[pa.Table]] = {
            schema_id: [] for schema_id in requested_schema_ids
        }
        support_bytes: dict[str, bytes] = {}
        schema_by_path = {
            path: schema_id
            for schema_id, manifest in manifests.items()
            for path in manifest.files
        }
        for table in bundle.tables:
            for relative_path, expected_hash in table.files.items():
                requested = relative_path in requested_internal_paths
                load_table = relative_path in materialized_internal_paths
                if not verify_all_files and not requested:
                    continue
                _, parsed = self._read_verified_file(
                    directory / relative_path,
                    relative_path=relative_path,
                    expected_hash=expected_hash,
                    expected_schema_hash=table.schema_hashes[relative_path],
                    expected_rows=table.row_counts[relative_path],
                    load_table=load_table,
                )
                if parsed is not None:
                    table_parts[schema_by_path[relative_path]].append(parsed)
        for support in bundle.support_files:
            requested_source_path = next(
                (
                    source_path
                    for source_path, selected in support_matches.items()
                    if selected == support
                ),
                None,
            )
            if not verify_all_files and requested_source_path is None:
                continue
            content, _ = self._read_verified_file(
                directory / support.relative_path,
                relative_path=support.relative_path,
                expected_hash=support.content_hash,
                load_content=requested_source_path is not None,
            )
            if requested_source_path is not None:
                if content is None:
                    raise ResultContractError("ResultBundle 控制文件读取结果为空")
                support_bytes[requested_source_path] = content

        tables = {}
        for schema_id, parts in table_parts.items():
            try:
                tables[schema_id] = pa.concat_tables(parts, promote_options="none")
            except (pa.ArrowException, TypeError) as exc:
                raise ResultContractError(
                    f"ResultBundle 分区 schema 不一致: {schema_id}"
                ) from exc
        verified_schema_ids = (
            frozenset(table.schema_id for table in bundle.tables)
            if verify_all_files
            else frozenset(manifests)
        )
        return ResultSnapshot(
            bundle,
            directory,
            tables,
            support_bytes,
            verified_schema_ids,
        )

    @staticmethod
    def _read_verified_file(
        path: Path,
        *,
        relative_path: str,
        expected_hash: str,
        expected_schema_hash: str | None = None,
        expected_rows: int | None = None,
        load_table: bool = False,
        load_content: bool = False,
    ) -> tuple[bytes | None, pa.Table | None]:
        try:
            with path.open("rb") as handle:
                digest = _stream_sha256(handle)
                content = None
                if load_content:
                    handle.seek(0)
                    content = handle.read()
        except OSError as exc:
            raise ResultContractError(f"ResultBundle 文件无法读取: {relative_path}") from exc
        if digest != expected_hash:
            raise ResultContractError(
                f"ResultBundle 文件读取时发生 hash 漂移: {relative_path}"
            )
        table = None
        if expected_schema_hash is not None:
            try:
                parquet = pq.ParquetFile(path)
                if load_table:
                    table = parquet.read()
                    schema = table.schema
                    rows = table.num_rows
                else:
                    metadata = parquet.metadata
                    schema = metadata.schema.to_arrow_schema()
                    rows = metadata.num_rows
            except pa.ArrowException as exc:
                raise ResultContractError(
                    f"ResultBundle Parquet 无法读取: {relative_path}"
                ) from exc
            if (
                typed_canonical_hash({"schema": str(schema)}) != expected_schema_hash
                or rows != expected_rows
            ):
                raise ResultContractError(
                    f"ResultBundle Parquet schema 或行数漂移: {relative_path}"
                )
        return content, table

    @staticmethod
    def _result_payload_paths(root: Path) -> set[str]:
        paths: set[str] = set()
        policy = PathRolePolicy()
        for directory, dirnames, filenames in policy.walk(root):
            directory_path = Path(directory)
            for name in sorted(dirnames):
                policy.resolve_contained_path(
                    allowed_root=root,
                    candidate=directory_path / name,
                    root_role="result_bundle",
                    path_role="result_directory",
                    expected_kind="directory",
                )
            for name in sorted(filenames):
                policy.resolve_contained_path(
                    allowed_root=root,
                    candidate=directory_path / name,
                    root_role="result_bundle",
                    path_role="result_file",
                    expected_kind="file",
                )
                paths.add((directory_path / name).relative_to(root).as_posix())
        return paths

    def _namespace(self, project_id: str, run_id: str) -> Path:
        if not project_id or any(
            char not in "abcdefghijklmnopqrstuvwxyz0123456789_.-" for char in project_id
        ):
            raise ResultContractError("result project_id 不是安全路径组件")
        if len(run_id) != 64 or any(char not in "0123456789abcdef" for char in run_id):
            raise ResultContractError("result run_id 不是 sha256")
        target = self.root / project_id / run_id
        parent = self.root / project_id
        if parent.exists():
            try:
                PathRolePolicy().resolve_contained_path(
                    allowed_root=self.root,
                    candidate=parent,
                    root_role="result_store",
                    path_role="result_project_namespace",
                    expected_kind="directory",
                )
            except DataPlaneError as exc:
                raise ResultContractError("ResultBundle namespace 路径安全验证失败") from exc
        return target


__all__ = [
    "RESULT_COMMITTED_NAME", "RESULT_MANIFEST_NAME", "ResultSnapshot", "ResultStore",
]
