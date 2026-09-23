"""Worker 核心为单个冻结键批提供输入并记录实际交付。"""

from __future__ import annotations

from dataclasses import replace
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.platform.causal_time import CausalTimeContractError
from research_pipeline.platform.project_causal_contract import parse_causal_plan
from .project_causal import CausalReadTrace
from .project_table_input import ProjectTableInput
from .project_causal_minute import ProjectCausalMinuteInput


def run_causal_entry(entry, context, inputs, output_root, causal_context):
    plan = parse_causal_plan(causal_context["plan"])
    if len(plan.work_items) != 1:
        raise CausalTimeContractError("Worker 每次必须只执行一个核心键批")
    item = plan.work_items[0]
    trace = CausalReadTrace(
        plan, item,
        inherited_lineage=causal_context["inherited_lineage"],
        source_partition_identities=causal_context["partition_identities"],
    )
    by_port = {value.port: value for value in inputs}
    restricted = []
    for source in plan.sources:
        original = by_port[source.port]
        if not isinstance(original, (ProjectTableInput, ProjectCausalMinuteInput)):
            raise CausalTimeContractError("正式因果输入必须是核心解析的已验证列式工件")
        source_timezone = causal_context["source_timezones"][source.port]

        def factory(*, columns, partition_ids, batch_size, original=original,
                    source=source, source_timezone=source_timezone):
            if isinstance(original, ProjectCausalMinuteInput):
                for partition, batch in original.iter_partition_batches(
                    columns=columns, partition_ids=partition_ids, batch_size=batch_size
                ):
                    for name in {source.observation_column, source.available_column}:
                        index = batch.schema.get_field_index(name)
                        array = batch.column(index)
                        if pa.types.is_timestamp(array.type) and array.type.tz is None:
                            array = pa.Array.from_pandas(pd.Series(array.to_pandas()).dt.tz_localize(source_timezone))
                            batch = batch.set_column(index, name, array)
                    yield partition, batch
                return
            # 原始路径只留在核心闭包，项目只接收 restricted wrapper。
            for path in original._paths.values():
                with pq.ParquetFile(path) as parquet:
                    for batch in parquet.iter_batches(columns=list(columns), batch_size=batch_size, use_threads=False):
                        if batch.nbytes > original._max_batch_bytes:
                            raise CausalTimeContractError("因果输入批次超过内存预算")
                        for name in {source.observation_column, source.available_column}:
                            index = batch.schema.get_field_index(name)
                            array = batch.column(index)
                            if pa.types.is_timestamp(array.type) and array.type.tz is None:
                                if source_timezone is None:
                                    raise CausalTimeContractError("因果输入缺少核心时区")
                                array = pa.Array.from_pandas(pd.Series(array.to_pandas()).dt.tz_localize(source_timezone))
                                batch = batch.set_column(index, name, array)
                        observations = batch.column(batch.schema.get_field_index(source.observation_column))
                        # 逻辑月份使用源的时区；UTC 归一只用于窗口不等式。
                        local_observations = pd.Series(observations.to_pandas())
                        if source_timezone is not None:
                            local_observations = local_observations.dt.tz_convert(source_timezone)
                        months = local_observations.dt.strftime("%Y-%m")
                        for partition in partition_ids:
                            selected = batch.filter(pa.array((months == partition).to_numpy()))
                            if selected.num_rows:
                                yield partition, selected

        restricted.append(trace.wrap_input(source.port, factory))
    state = by_port.get("runtime_state")
    if state is not None:
        restricted.append(state)
    worker_parameters = dict(context.parameters)
    worker_parameters["causal_plan"] = plan.to_dict()
    result = entry(replace(context, parameters=worker_parameters), tuple(restricted), output_root)
    for value in inputs:
        if isinstance(value, ProjectCausalMinuteInput):
            value.assert_complete()
    facts = trace.core_time_facts()
    records = json.loads(facts.to_json(orient="records", date_format="iso", date_unit="ns"))
    # JSON 仅承载当前有界键批的核心事实，不携带输出/state bytes。
    return result, {"key_columns": list(plan.key_columns), "records": records,
                    "lineage": list(trace.lineage())}
