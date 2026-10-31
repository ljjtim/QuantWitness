"""把冻结组合目标按实际执行时点交给 LEAN 策略，不冒充模型 Feature。"""
from datetime import datetime, time
from decimal import Decimal, ROUND_FLOOR
import json
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from research_pipeline.domain import PortfolioTarget
from .adapters.common import _input_external_root


def portfolio_target_rows(context, *, instruments, day):
    """逐批读取目标，执行会话才展开完整证券权重；保留零权重清仓事实。"""
    root = _input_external_root(context, 'targets')
    value = context.inputs['targets']
    if value.external_commit.artifact_type != 'research.portfolio-targets.v1':
        raise ValueError('组合目标须绑定正式portfolio-targets工件')
    codes = {item.instrument_id for item in instruments}
    identities = {item.instrument_id: item for item in instruments}
    selected = []
    last = None
    for relative in sorted(value.external_commit.files):
        if not relative.startswith('targets/') or not relative.endswith('.parquet'):
            continue
        parquet = pq.ParquetFile(root/relative)
        for batch in parquet.iter_batches(columns=('decision_time', 'order_time', 'target_json'), batch_size=256):
            if batch.nbytes > context.effective_resource_budget.memory_bytes//8:
                raise ValueError('组合目标批超过内存预算')
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
                if day is None or execution.date() == day:
                    selected.append((row, execution))
    if day is not None and len(selected) > 1:
        raise ValueError('同一执行会话只能交付一批组合目标')
    for row, execution in selected:
        target = PortfolioTarget.from_dict(json.loads(row['target_json']))
        if target.decision_time != row['decision_time'] or target.target_type != 'weight':
            raise ValueError('组合目标类型或决策时点与生产表不一致')
        weights = {}
        for entry in target.entries:
            code = entry.instrument.instrument_id
            weight = Decimal(str(entry.value))
            if (code not in codes or entry.instrument != identities[code] or code in weights
                    or not weight.is_finite() or not 0 <= weight <= 1):
                raise ValueError('组合目标证券或权重无效')
            weights[code] = int((weight*1_000_000_000).to_integral_value(rounding=ROUND_FLOOR))
        if sum(weights.values()) > 1_000_000_000:
            raise ValueError('组合目标权重超过全额权益')
        for code in sorted(codes):
            yield dict(event_id=f"{execution.isoformat()}:{code}", entity_id=code,
                       decision_time=execution, weight_ppb=weights.get(code, 0))
