"""项目输出根与有界 staging 写入；仅返回现有提交描述。"""

from __future__ import annotations

import hashlib
from pathlib import Path

from research_pipeline.data_plane.snapshots import _sha256


MAX_PROJECT_WRITE_BYTES = 32 * 1024 * 1024
PROJECT_OUTPUT_ROW_GROUP_ROWS = 8192


class ProjectOutputRoot(type(Path())):
    """保留 Path 操作，并提供不随输入批大小改变文件身份的表格 writer。"""

    def _target(self, relative_path: str) -> Path:
        root = self.resolve(strict=True)
        target = (root / relative_path).resolve()
        if not target.is_relative_to(root) or target == root:
            raise ValueError("project_output_path_escape")
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def write_batches(self, *, port, artifact_type, relative_path, schema, batches):
        import pyarrow as pa
        import pyarrow.parquet as pq

        target = self._target(relative_path)
        if target.suffix != ".parquet":
            raise ValueError("project_output_parquet_suffix_required")
        pending = []
        rows = 0
        buffered_bytes = 0
        with pq.ParquetWriter(target, schema, compression="zstd") as writer:
            for batch in batches:
                if not isinstance(batch, pa.RecordBatch) or not batch.schema.equals(schema):
                    raise ValueError("project_output_batch_schema_invalid")
                if batch.nbytes > MAX_PROJECT_WRITE_BYTES:
                    raise ValueError("project_output_batch_bytes_exceeded")
                offset = 0
                while offset < batch.num_rows:
                    count = min(PROJECT_OUTPUT_ROW_GROUP_ROWS - rows, batch.num_rows - offset)
                    part = batch.slice(offset, count)
                    pending.append(part)
                    buffered_bytes += part.nbytes
                    if buffered_bytes > MAX_PROJECT_WRITE_BYTES:
                        raise ValueError("project_output_row_group_bytes_exceeded")
                    rows += count
                    offset += count
                    if rows == PROJECT_OUTPUT_ROW_GROUP_ROWS:
                        writer.write_table(pa.Table.from_batches(pending, schema=schema).combine_chunks(),
                                           row_group_size=PROJECT_OUTPUT_ROW_GROUP_ROWS)
                        pending, rows, buffered_bytes = [], 0, 0
            if pending:
                writer.write_table(pa.Table.from_batches(pending, schema=schema).combine_chunks(),
                                   row_group_size=PROJECT_OUTPUT_ROW_GROUP_ROWS)
        return self._commit(target, port, artifact_type,
                            hashlib.sha256(schema.serialize().to_pybytes()).hexdigest())

    def write_state(self, *, relative_path, schema_hash, chunks):
        target = self._target(relative_path)
        with target.open("wb") as handle:
            for chunk in chunks:
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise ValueError("project_state_chunk_invalid")
                if (chunk.nbytes if isinstance(chunk, memoryview) else len(chunk)) > MAX_PROJECT_WRITE_BYTES:
                    raise ValueError("project_state_chunk_bytes_exceeded")
                handle.write(chunk)
        return self._commit(target, "runtime_state", "runtime.project-state", schema_hash)

    def commit_directory(self, *, port, artifact_type, relative_path, files):
        """把一个端口的全部已写文件作为同一工件提交。"""
        root = self.resolve(strict=True)
        directory = (root / relative_path).resolve(strict=True)
        if not directory.is_relative_to(root) or directory == root or not directory.is_dir():
            raise ValueError("project_output_directory_invalid")
        declared = tuple(files)
        actual = tuple(sorted(
            path.relative_to(directory).as_posix()
            for path in directory.rglob("*") if path.is_file()
        ))
        if not actual or tuple(sorted(set(declared))) != actual or len(declared) != len(actual):
            raise ValueError("project_output_directory_file_closure_invalid")
        return {
            "port": port,
            "artifact_type": artifact_type,
            "relative_path": directory.relative_to(root).as_posix(),
            "files": [
                {
                    "relative_path": path,
                    "content_hash": _sha256(directory / path),
                    "byte_size": (directory / path).stat().st_size,
                }
                for path in actual
            ],
        }

    def _commit(self, target: Path, port: str, artifact_type: str, schema_hash: str):
        return {
            "port": port, "artifact_type": artifact_type,
            "relative_path": target.relative_to(self.resolve()).as_posix(),
            "content_hash": _sha256(target), "schema_hash": schema_hash,
            "byte_size": target.stat().st_size,
        }
