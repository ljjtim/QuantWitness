"""把冻结组合目标按实际执行时点交给 LEAN 策略，不冒充模型 Feature。"""
from datetime import datetime, time
from decimal import Decimal, ROUND_FLOOR
import json
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from research_pipeline.domain import PortfolioTarget
from .adapters.common import _input_external_root


class PortfolioTargetReader:
    """运行内顺序读取；全流时间校验一次，目标载荷只消费一次。"""

    def __init__(self, context):
        root = _input_external_root(context, 'targets')
        value = context.inputs['targets']
        if value.external_commit.artifact_type != 'research.portfolio-targets.v1':
            raise ValueError('组合目标须绑定正式portfolio-targets工件')
        files = tuple(relative for relative in sorted(value.external_commit.files)
                           if relative.startswith('targets/') and relative.endswith('.parquet'))
        self._initialize(root, files, context.effective_resource_budget.memory_bytes // 8)

    @classmethod
    def from_files(cls, root, files, memory_bytes):
        reader = cls.__new__(cls)
        reader._initialize(root, files, memory_bytes)
        return reader

    def _initialize(self, root, files, memory_bytes):
        self.root = root
        self.files = files
        self.limit = memory_bytes
        self._rows = None
        self._pending = None
        self._last_day = None

    def _batches(self, columns):
        for relative in self.files:
            with pq.ParquetFile(self.root / relative) as parquet:
                for batch in parquet.iter_batches(columns=columns, batch_size=256):
                    if batch.nbytes > self.limit:
                        raise ValueError('组合目标批超过内存预算')
                    yield batch

    def _validated_stream(self, columns):
        last = None
        for batch in self._batches(columns):
            for row in batch.to_pylist():
                decision, execution = row['decision_time'], row['order_time']
                if not isinstance(decision, datetime) or not isinstance(execution, datetime) or decision.utcoffset() is None or execution.utcoffset() is None:
                    raise ValueError('组合决策与执行时点必须带时区')
                execution = execution.astimezone(ZoneInfo('Asia/Shanghai'))
                if decision >= execution or execution.time() != time(9, 30):
                    raise ValueError('组合目标须在执行会话开盘前可见')
                if last is not None and execution <= last:
                    raise ValueError('组合执行时点重复或倒退')
                last = execution
                yield row, execution

    def read_index_rows(self):
        return self._validated_stream(('decision_time', 'order_time', 'target_json'))

    def _validated_rows(self):
        # 顺序消费者开始执行前，先核对全流时点。
        for _ in self._validated_stream(('decision_time', 'order_time')):
            pass
        for batch in self._batches(('decision_time', 'order_time', 'target_json')):
            for row in batch.to_pylist():
                yield row, row['order_time'].astimezone(ZoneInfo('Asia/Shanghai'))


    def rows_for_day(self, day):
        if day is not None and self._last_day is not None and day <= self._last_day:
            raise ValueError('组合目标消费日期必须递增')
        self._last_day = day
        if self._rows is None:
            self._rows = self._validated_rows()
            self._pending = next(self._rows, None)
        while self._pending is not None:
            row, execution = self._pending
            if day is not None and execution.date() > day:
                break
            self._pending = next(self._rows, None)
            if day is None or execution.date() == day:
                yield row, execution

    def close(self):
        if self._rows is not None:
            self._rows.close()
        self._pending = None


def portfolio_target_rows(context, *, instruments, day, reader=None):
    """执行会话才展开完整证券权重；保留零权重清仓事实。"""
    owned = reader is None
    reader = reader or PortfolioTargetReader(context)
    try:
        yield from _target_rows(reader.rows_for_day(day), instruments)
    finally:
        if owned:
            reader.close()


def _target_weights(row, identities):
    target = PortfolioTarget.from_dict(json.loads(row['target_json']))
    if target.decision_time != row['decision_time'] or target.target_type != 'weight':
        raise ValueError('组合目标类型或决策时点与生产表不一致')
    weights = {}
    for entry in target.entries:
        code = entry.instrument.instrument_id
        weight = Decimal(str(entry.value))
        if (identities.get(code) != entry.instrument or code in weights
                or not weight.is_finite() or not 0 <= weight <= 1):
            raise ValueError('组合目标证券或权重无效')
        weights[code] = int((weight * 1_000_000_000).to_integral_value(rounding=ROUND_FLOOR))
    if sum(weights.values()) > 1_000_000_000:
        raise ValueError('组合目标权重超过全额权益')
    return weights


def _target_rows(rows, instruments):
    identities = {item.instrument_id: item for item in instruments}
    codes = sorted(identities)
    for row, execution in rows:
        weights = _target_weights(row, identities)
        for code in codes:
            yield dict(event_id=f"{execution.isoformat()}:{code}", entity_id=code,
                       decision_time=execution, weight_ppb=weights.get(code, 0))
