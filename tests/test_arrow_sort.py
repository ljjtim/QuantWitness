"""列式排序保持时间、整数、空值和稳定次序，并受暂存额度约束。"""
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pytest

from research_pipeline.data_plane.arrow_sort import sorted_record_batches


def test_sort_preserves_schema_and_stable_ties(tmp_path):
    schema = pa.schema([("time", pa.timestamp("us", tz="Asia/Shanghai")),
                        ("code", pa.string()), ("value", pa.int64()), ("optional", pa.float64())])
    at = datetime(2021, 10, 8, 9, 31, tzinfo=ZoneInfo("Asia/Shanghai"))
    values = [dict(time=at, code=code, value=value, optional=None)
              for code, value in [("B", 2**60), ("A", 2**60+1), ("A", 2**60+2)]]
    batches = [pa.RecordBatch.from_pylist(values[:2], schema=schema),
               pa.RecordBatch.from_pylist(values[2:], schema=schema)]
    root = tmp_path / "sort"
    with sorted_record_batches(batches, root, sort_columns=("time", "code"),
            memory_bytes=128*1024**2, temp_bytes=64*1024**2, cpu_slots=2) as output:
        result = list(output)
        assert all(batch.schema == schema for batch in result)
        assert pa.Table.from_batches(result).to_pylist() == [values[1], values[2], values[0]]
    assert not root.exists()


def test_sort_closes_source_and_removes_scratch_on_input_error(tmp_path):
    closed = []
    def source():
        try:
            yield pa.record_batch([pa.array([2, 1])], names=["key"])
            raise ValueError("输入读取失败")
        finally:
            closed.append(True)
    root = tmp_path / "sort"
    with pytest.raises(Exception, match="输入读取失败"):
        with sorted_record_batches(source(), root, sort_columns=("key",),
                memory_bytes=128*1024**2, temp_bytes=64*1024**2, cpu_slots=1) as output:
            list(output)
    assert closed == [True]
    assert not root.exists()


def _spill_batches():
    for part in reversed(range(200)):
        yield pa.record_batch([
            pa.array(range((part+1)*4096-1, part*4096-1, -1), type=pa.int64()),
            pa.array([f"{value:064x}" for value in range(part*4096, (part+1)*4096)]),
            pa.repeat(pa.scalar("b"*64), 4096),
        ], names=["key", "source_snapshot_hash", "source_partition_hash"])


def test_sort_refuses_spill_above_temporary_budget(tmp_path):
    root = tmp_path / "sort"
    with pytest.raises(duckdb.OutOfMemoryException):
        with sorted_record_batches(_spill_batches(), root, sort_columns=("key",),
                memory_bytes=128*1024**2, temp_bytes=1024**2, cpu_slots=1) as output:
            list(output)
    assert not root.exists()


def test_sort_spills_and_streams_with_bounded_memory(tmp_path):
    root = tmp_path / "sort"
    previous = None
    count = 0
    with sorted_record_batches(_spill_batches(), root, sort_columns=("key",),
            memory_bytes=128*1024**2, temp_bytes=256*1024**2, cpu_slots=1) as output:
        assert sum(path.stat().st_size for path in root.iterdir()) > 0
        for batch in output:
            for row in batch.column(0).to_pylist():
                assert previous is None or previous < row
                previous = row
                count += 1
    assert count == 200*4096
    assert not root.exists()


@pytest.mark.parametrize("zone", ["UTC", "Asia/Shanghai"])
def test_batch_rows_preserve_timestamp_values_and_nulls(zone):
    from research_pipeline.platform.arrow_rows import record_batch_rows
    batch = pa.RecordBatch.from_arrays([
        pa.array([1729000000000000, None, 1729000000000000, 1729000000000123],
                 type=pa.timestamp("us", tz=zone)),
        pa.array(["甲", None, "乙", "丙"]),
        pa.array([1, 2, 3, 4], type=pa.int16()),
    ], names=["at", "code", "quantity"])
    assert list(record_batch_rows(batch)) == batch.to_pylist()
    assert list(record_batch_rows(batch.slice(1, 2))) == batch.slice(1, 2).to_pylist()


@pytest.mark.parametrize("value_type,value", [
    (pa.string(), "a" * 64),
    (pa.large_string(), "a" * 64),
    (pa.binary(), b"a" * 64),
    (pa.large_binary(), b"a" * 64),
])
def test_sort_reuses_constant_buffers_and_reorders_varying_values(tmp_path, monkeypatch, value_type, value):
    def unexpected_connection(*args, **kwargs):
        pytest.fail("有界内存排序不应打开数据库连接")

    monkeypatch.setattr(duckdb, "connect", unexpected_connection)
    schema = pa.schema([
        pa.field("key", pa.string()),
        pa.field("sequence", pa.int16()),
        pa.field("source_snapshot_hash", value_type, metadata={b"role": b"source"}),
        pa.field("source_partition_id", pa.string()),
        pa.field("all_null", pa.string()),
        pa.field("nullable", pa.string()),
    ], metadata={b"contract": b"bars"})
    batches = [pa.RecordBatch.from_arrays([
        pa.array(keys), pa.array(sequence, type=pa.int16()),
        pa.repeat(pa.scalar(value, type=value_type), len(keys)),
        pa.repeat(pa.scalar(partition), len(keys)),
        pa.nulls(len(keys), type=pa.string()), pa.array(nullable),
    ], schema=schema) for keys, sequence, partition, nullable in [
        (["B", "A", "A"], [0, 1, 2], "first", ["x", None, "x"]),
        (["B", "A"], [3, 4], "second", ["x", "x"]),
    ]]
    table = pa.Table.from_batches(batches)
    expected = table.take(pa.array([1, 2, 4, 0, 3]))
    root = tmp_path / "sort"
    with sorted_record_batches(batches, root, sort_columns=("key",),
            memory_bytes=128*1024**2, temp_bytes=0, cpu_slots=1) as output:
        result = pa.Table.from_batches(list(output))
        assert result.schema.equals(schema, check_metadata=True)
        assert result.equals(expected)
        for name in ("source_snapshot_hash", "all_null"):
            assert [
                [None if buffer is None else buffer.address for buffer in chunk.buffers()]
                for chunk in result.column(name).chunks
            ] == [
                [None if buffer is None else buffer.address for buffer in chunk.buffers()]
                for chunk in table.column(name).chunks
            ]
    assert not root.exists()


def test_spill_releases_consumed_probe_batches(tmp_path):
    import weakref
    references = []

    def source():
        for _ in range(20):
            batch = pa.record_batch([
                pa.array(range(16384)), pa.repeat("x" * 128, 16384),
            ], names=["key", "value"])
            references.append(weakref.ref(batch))
            yield batch

    with sorted_record_batches(source(), tmp_path / "sort", sort_columns=("key",),
            memory_bytes=128 * 1024**2, temp_bytes=256 * 1024**2, cpu_slots=1) as output:
        next(output)
        assert len(references) == 20
        assert all(reference() is None for reference in references)


def test_memory_sort_slices_only_retain_one_output_block(tmp_path):
    def source():
        for part in reversed(range(4)):
            yield pa.record_batch([pa.array(range(part * 65536, (part + 1) * 65536))],
                                  names=["key"])

    last = None
    for partition in range(3):
        with sorted_record_batches(source(), tmp_path / f"sort-{partition}", sort_columns=("key",),
                memory_bytes=128 * 1024**2, temp_bytes=0, cpu_slots=1) as output:
            count = 0
            for batch in output:
                assert batch.column(0)[0].as_py() == count
                count += batch.num_rows
                last = batch.slice(batch.num_rows - 1, 1)
                assert last.get_total_buffer_size() <= 65536 * 8
            assert count == 4 * 65536
        assert last.column(0)[0].as_py() == 4 * 65536 - 1
