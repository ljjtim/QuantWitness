"""Supervisor 已验证的非分钟多文件输入。"""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


class ProjectTableInput:
    """按提交清单读取表格或命名 JSON，不扫描相邻文件。"""

    def __init__(
        self,
        *,
        port: str,
        artifact_type: str,
        source_identity: str,
        source_root: Path,
        files: tuple[Mapping[str, object], ...],
        max_batch_rows: int = 65_536,
        max_batch_bytes: int = 32 * 1024 * 1024,
        homogeneous_parquet: bool = True,
    ) -> None:
        self.port = port
        self.artifact_type = artifact_type
        self.source_identity = source_identity
        self._paths: dict[str, Path] = {}
        self._parquet_paths: tuple[str, ...] = ()
        self._complete = False
        self._started = False
        self._active_streams = 0
        self._opened_files: set[str] = set()
        self._consumed_columns: tuple[str, ...] = ()
        self._consumed_rows = 0
        self._max_batch_rows = max_batch_rows
        self._max_batch_bytes = max_batch_bytes
        for item in files:
            relative_path = str(item["relative_path"])
            path = (source_root / relative_path).resolve(strict=True)
            if not path.is_relative_to(source_root) or not path.is_file():
                raise ValueError("project_table_input_path_escape")
            if path.stat().st_size != item["byte_size"]:
                raise ValueError("project_table_input_size_mismatch")
            if relative_path in self._paths:
                raise ValueError("project_table_input_path_duplicate")
            self._paths[relative_path] = path
        if not self._paths:
            raise ValueError("project_table_input_empty")
        self._parquet_paths = tuple(
            path for path in self._paths if path.endswith(".parquet")
        )
        if homogeneous_parquet and self._parquet_paths:
            self._schema_for(
                tuple(self._paths[path] for path in self._parquet_paths)
            )

    @property
    def file_paths(self) -> tuple[str, ...]:
        """返回 Supervisor 已验证的提交清单，不扫描目录。"""

        return tuple(self._paths)

    def _path(self, relative_path: str) -> Path:
        if not isinstance(relative_path, str) or relative_path not in self._paths:
            raise ValueError("project_table_input_unknown_file")
        return self._paths[relative_path]

    def read_bytes(self, relative_path: str, *, max_bytes: int = 32 * 1024 * 1024) -> bytes:
        """读取明确命名的小文件；大表仍必须走批次接口。"""

        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("project_table_input_max_bytes_invalid")
        path = self._path(relative_path)
        if path.stat().st_size > max_bytes:
            raise ValueError("project_table_input_file_bytes_exceeded")
        self._started = True
        self._opened_files.add(relative_path)
        self._complete = self._active_streams == 0
        return path.read_bytes()

    def read_json(self, relative_path: str, *, max_bytes: int = 32 * 1024 * 1024):
        """读取提交清单中明确命名的 JSON。"""

        try:
            return json.loads(self.read_bytes(relative_path, max_bytes=max_bytes))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("project_table_input_json_invalid") from exc

    @staticmethod
    def _schema_for(paths: tuple[Path, ...]):
        import pyarrow.parquet as pq

        schema = None
        for path in paths:
            with pq.ParquetFile(path) as reader:
                current = reader.schema_arrow
            if schema is not None and not schema.equals(current, check_metadata=False):
                raise ValueError("project_table_input_schema_mismatch")
            schema = current
        return schema

    def iter_batches(self, *, columns: tuple[str, ...], batch_size: int = 65_536):
        if self._started:
            raise ValueError("project_table_input_already_consumed")
        if not self._parquet_paths:
            raise ValueError("project_table_input_no_parquet")
        paths = tuple(self._paths[path] for path in self._parquet_paths)
        schema = self._schema_for(paths)
        assert schema is not None
        if (
            type(batch_size) is not int or not 0 < batch_size <= self._max_batch_rows
            or not columns or len(set(columns)) != len(columns)
            or not set(columns).issubset(schema.names)
        ):
            raise ValueError("project_table_input_projection_or_batch_invalid")
        self._started = True
        self._consumed_columns = tuple(columns)
        self._active_streams += 1
        completed = False
        try:
            for relative_path in self._parquet_paths:
                yield from self._iter_path(
                    relative_path, columns=columns, batch_size=batch_size,
                )
            completed = True
        finally:
            self._active_streams -= 1
            self._complete = completed and self._active_streams == 0

    def iter_file_batches(
        self,
        relative_path: str,
        *,
        columns: tuple[str, ...],
        batch_size: int = 65_536,
    ):
        """按确切提交路径读取一张 Parquet，支持同端口异构表。"""

        if relative_path in self._opened_files:
            raise ValueError("project_table_input_file_already_consumed")
        path = self._path(relative_path)
        if path.suffix != ".parquet":
            raise ValueError("project_table_input_file_not_parquet")
        schema = self._schema_for((path,))
        assert schema is not None
        if (
            type(batch_size) is not int
            or not 0 < batch_size <= self._max_batch_rows
            or not columns
            or len(set(columns)) != len(columns)
            or not set(columns).issubset(schema.names)
        ):
            raise ValueError("project_table_input_projection_or_batch_invalid")
        self._started = True
        self._opened_files.add(relative_path)
        self._active_streams += 1
        completed = False
        try:
            yield from self._iter_path(
                relative_path, columns=columns, batch_size=batch_size,
            )
            completed = True
        finally:
            self._active_streams -= 1
            self._complete = completed and self._active_streams == 0

    def _iter_path(
        self,
        relative_path: str,
        *,
        columns: tuple[str, ...],
        batch_size: int,
    ):
        import pyarrow.parquet as pq

        path = self._path(relative_path)
        with pq.ParquetFile(path) as reader:
            for batch in reader.iter_batches(
                batch_size=batch_size,
                columns=list(columns),
                use_threads=False,
            ):
                if batch.nbytes > self._max_batch_bytes:
                    raise ValueError("project_table_input_batch_bytes_exceeded")
                self._consumed_rows += batch.num_rows
                yield batch

    def assert_complete(self) -> None:
        if not self._started or not self._complete or self._active_streams:
            raise ValueError("project_table_input_not_fully_consumed")

    @property
    def consumption(self) -> dict[str, object]:
        self.assert_complete()
        return {
            "columns": list(self._consumed_columns),
            "row_count": self._consumed_rows,
        }


class ProjectRequestTableInput:
    """只暴露节点参数显式绑定的数据 request，不暴露 bundle 目录。"""

    def __init__(
        self,
        *,
        port: str,
        artifact_type: str,
        source_identity: str,
        source_root: Path,
        requests: Mapping[str, Mapping[str, object]],
    ) -> None:
        self.port = port
        self.artifact_type = artifact_type
        self.source_identity = source_identity
        self._source_root = source_root
        self._requests = MappingProxyType(dict(sorted(requests.items())))
        self._opened: dict[str, ProjectTableInput] = {}
        self._admission_reads: set[str] = set()

    def admission(self, request_id: str) -> Mapping[str, object]:
        """读取 Supervisor 绑定的当前 request 准入事实。"""
        if request_id not in self._requests:
            raise ValueError("project_request_table_unknown_request")
        facts = self._requests[request_id].get("admission")
        if not isinstance(facts, Mapping):
            raise ValueError("project_request_table_admission_unavailable")
        self._admission_reads.add(request_id)
        return MappingProxyType(dict(facts))

    @property
    def request_ids(self) -> tuple[str, ...]:
        return tuple(self._requests)

    def request(self, request_id: str) -> ProjectTableInput:
        """取得一个声明过的 request；同一 request 只能创建一个消费句柄。"""

        if not isinstance(request_id, str) or request_id not in self._requests:
            raise ValueError("project_request_table_unknown_request")
        if request_id in self._opened:
            raise ValueError("project_request_table_already_opened")
        descriptor = self._requests[request_id]
        files = descriptor.get("files")
        if not isinstance(files, tuple):
            raise ValueError("project_request_table_descriptor_invalid")
        table = ProjectTableInput(
            port=f"{self.port}:{request_id}",
            artifact_type=self.artifact_type,
            source_identity=str(descriptor["source_identity"]),
            source_root=self._source_root,
            files=files,
        )
        self._opened[request_id] = table
        return table

    def assert_complete(self) -> None:
        if set(self._opened) | self._admission_reads != set(self._requests):
            raise ValueError("project_request_table_not_all_requested")
        for table in self._opened.values():
            table.assert_complete()

    def consumption_trace(self) -> dict[str, object]:
        self.assert_complete()
        return {
            request_id: {
                "source_identity": self._requests[request_id]["source_identity"],
                "schema_hash": self._requests[request_id]["schema_hash"],
                **(
                    self._opened[request_id].consumption
                    if request_id in self._opened
                    else {"metadata_only": True}
                ),
            }
            for request_id in self._requests
        }
