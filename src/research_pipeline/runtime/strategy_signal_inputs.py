"""正式causal Feature的有界排序、核心可见性裁剪与策略列投影。"""
from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from dataclasses import asdict
import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from research_pipeline.domain.strategy_protocol import StrategyInputRef
from research_pipeline.platform.causal_time import CORE_FEATURE_TIME_COLUMNS, validate_feature_time_facts
from .adapters.common import _input_external_root


SIGNAL_REQUIRED_COLUMNS = ("event_id", "entity_id", *CORE_FEATURE_TIME_COLUMNS)


def signal_payload(value):
    """保留原始Feature列值，时间与日期使用Result中的规范JSON表示。"""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: signal_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [signal_payload(item) for item in value]
    return value


class StrategySignalInputs:
    """窗口单位为核心decision_time批次，批内保留所有声明证券和空证券行。"""

    def __init__(self, rows, *, bindings, instruments, max_context_bytes):
        self.rows = iter(rows)
        self.bindings = tuple(binding for binding in bindings if binding.source == "signals")
        self.instruments = {item.instrument_id: item for item in instruments}
        self.history = deque(maxlen=max(binding.lookback_bars for binding in self.bindings))
        self.history_bytes = deque(maxlen=self.history.maxlen)
        self.max_context_bytes = max_context_bytes
        self.next_row = next(self.rows, None)
        self.last_at = None

    def visible(self, at):
        if self.last_at is not None and at < self.last_at:
            raise ValueError("策略signals裁剪时点不能倒退")
        self.last_at = at
        while self.next_row is not None and self.next_row["decision_time"] <= at:
            decision = self.next_row["decision_time"]
            cohort, identities = [], set()
            sizes = {binding.binding_id: 0 for binding in self.bindings}
            while self.next_row is not None and self.next_row["decision_time"] == decision:
                row = self.next_row
                if row["event_id"] in identities:
                    raise ValueError("signals同一时点的event_id重复")
                identities.add(row["event_id"])
                cohort.append(row)
                # 限制完整批次的实际交付大小，不能用截断改变候选集合。
                for binding in self.bindings:
                    sizes[binding.binding_id] += len(json.dumps(signal_payload(asdict(self._reference(row, binding))), ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                if sum(sizes.values()) > self.max_context_bytes:
                    raise ValueError("策略signals时点批超过上下文预算")
                self.next_row = next(self.rows, None)
            self.history.append(tuple(cohort))
            self.history_bytes.append(sizes)
            actual_bytes = sum(sizes[binding.binding_id] for binding in self.bindings
                               for sizes in list(self.history_bytes)[-binding.lookback_bars:])
            if actual_bytes > self.max_context_bytes:
                raise ValueError("策略signals历史窗口超过上下文预算")
        result = []
        for binding in self.bindings:
            for cohort in list(self.history)[-binding.lookback_bars:]:
                result.extend(self._reference(row, binding) for row in cohort)
        return tuple(result)

    def _reference(self, row, binding):
        entity = row["entity_id"]
        if entity is not None and entity not in self.instruments:
            raise ValueError("signals证券超出策略声明")
        values = tuple((column, row[column]) for column in binding.columns)
        if any(value is not None and type(value) is not int for _, value in values):
            raise ValueError("signals业务列必须是整数或空值")
        return StrategyInputRef(input_id=f"{binding.binding_id}:signal:{row['event_id']}",
            binding_id=binding.binding_id, available_at=row["decision_time"],
            instrument=None if entity is None else self.instruments[entity], values=values)


@contextmanager
def open_strategy_signal_inputs(context, *, declaration, instruments, evidence_sink=None, day: date | None = None):
    """按UTC+8决策日下推读取，只在声明内存内排序，不生成中间文件。"""
    bindings = tuple(binding for binding in declaration.input_bindings if binding.source == "signals")
    if not bindings:
        yield None
        return
    if 'targets' in context.inputs:
        from .portfolio_target_inputs import portfolio_target_rows
        if 'signals' in context.inputs or len(bindings) != 1 or bindings[0].columns != ('weight_ppb',):
            raise ValueError('组合目标须独占一个weight_ppb信号binding')
        yield StrategySignalInputs(portfolio_target_rows(context, instruments=instruments, day=day),
            bindings=bindings, instruments=instruments, max_context_bytes=declaration.limits.max_context_bytes)
        return
    root = _input_external_root(context, "signals")
    commit = context.inputs["signals"].external_commit
    if commit.artifact_type != "research.feature-set.v1":
        raise ValueError("策略signals必须绑定正式Feature工件")
    columns = tuple(dict.fromkeys(column for binding in bindings for column in binding.columns))
    codes = {item.instrument_id for item in instruments}
    budget = context.effective_resource_budget

    limit = budget.memory_bytes // 8
    retained = 0
    batches = []
    source = None
    ordered = None
    projected = tuple(dict.fromkeys((*SIGNAL_REQUIRED_COLUMNS, *columns)))

    def require_memory(size):
        if size > limit:
            raise ValueError("策略signals超过声明内存预算，禁止spill；请提高声明内存或使用显式保存路径")

    try:
        for relative in sorted(commit.files):
            if not relative.endswith(".parquet"):
                continue
            parquet = pq.ParquetFile(root / relative)
            schema = parquet.schema_arrow
            if not set((*SIGNAL_REQUIRED_COLUMNS, *columns)) <= set(schema.names):
                raise ValueError("策略signals缺少稳定键、证券、核心时间或声明列")
            if any(not pa.types.is_integer(schema.field(column).type) for column in columns):
                raise ValueError("策略signals业务投影只能使用整数列")
            if not pa.types.is_string(schema.field("event_id").type) or not (pa.types.is_string(schema.field("entity_id").type) or pa.types.is_null(schema.field("entity_id").type)):
                raise ValueError("策略signals event_id须为字符串，entity_id须为字符串或空值")
            expression = None
            if day is not None:
                start = datetime.combine(day, time.min, timezone(timedelta(hours=8)))
                end = start + timedelta(days=1)
                stamp = schema.field("decision_time").type
                if pa.types.is_timestamp(stamp) and stamp.tz is None:
                    start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
                expression = (ds.field("decision_time") >= pa.scalar(start, type=stamp)) & (ds.field("decision_time") < pa.scalar(end, type=stamp))
            scanner = ds.dataset(str(root / relative), format="parquet").scanner(
                columns=list(projected), filter=expression, batch_size=256,
                batch_readahead=0, fragment_readahead=0, use_threads=False)
            for batch in scanner.to_batches():
                if not batch.num_rows:
                    continue
                size = batch.get_total_buffer_size()
                require_memory(retained + size * 4)
                validate_feature_time_facts(pa.Table.from_batches([batch]).to_pandas())
                for row in batch.to_pylist():
                    if not isinstance(row["event_id"], str) or not row["event_id"] or (row["entity_id"] is not None and row["entity_id"] not in codes):
                        raise ValueError("策略signals行身份或证券声明不合法")
                    if evidence_sink is not None:
                        evidence_sink.signal(signal_payload(row))
                batches.append(batch)
                retained += size
        output_columns = tuple(dict.fromkeys(("event_id", "entity_id", "decision_time", *columns)))
        if batches:
            table = pa.Table.from_batches(batches)
            require_memory(retained * 2 + table.num_rows * 8)
            ordered = table.take(pc.sort_indices(table, sort_keys=[("decision_time", "ascending"), ("event_id", "ascending")])).select(output_columns)
            del table
            batches.clear()

            def rows():
                for batch in ordered.to_batches(max_chunksize=256):
                    yield from batch.to_pylist()
            source = rows()
        else:
            source = iter(())
        selected = StrategySignalInputs(source, bindings=bindings, instruments=instruments,
            max_context_bytes=declaration.limits.max_context_bytes)
        selected.source_artifact_key = commit.semantic_hash
        yield selected
    finally:
        if source is not None:
            close = getattr(source, "close", None)
            if close is not None:
                close()
        batches.clear()
        ordered = None
