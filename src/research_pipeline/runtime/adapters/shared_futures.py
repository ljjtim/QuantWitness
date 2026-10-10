"""共享期货账户从正式准入输入到独立金融事实的 Runtime 接线。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Mapping
import sys

from research_pipeline.data_plane import (
    ArtifactResolver,
    DatasetArtifactRef,
    require_minute_price_mode,
)
from research_pipeline.domain.shared_futures import (
    aware_time,
    validate_shared_futures_events,
    validate_shared_futures_spec,
)
from research_pipeline.platform import canonical_json
from research_pipeline.platform.shared_futures_contracts import (
    SHARED_FUTURES_RESULT_PATHS,
)
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs
from .common import (
    _environment,
    _factor_external_result,
    _input_admitted_plans,
    _input_data_bundle,
    _input_external_payload,
    _input_external_root,
    _json_ready,
    _parameters,
)
from .minute_io import _operator_bar_partitions


def _market_events(context: OperatorRuntimeContext, parameters: Mapping) -> list[dict]:
    """只读取当前市场入边的已物化请求，不从参数接收行情。"""
    bundle = _input_data_bundle(context, "market")
    plans = _input_admitted_plans(_environment(context), bundle)
    request_id = str(parameters["market_request_id"])
    references = bundle["references"]
    if request_id not in plans or request_id not in references:
        raise ValueError("共享期货 market_request_id 未绑定市场入边的准入请求")
    plan = plans[request_id]
    if str(getattr(plan.query.purpose, "value", plan.query.purpose)) != "feature":
        raise ValueError("共享期货只接受 feature 市场事实，AUDIT 与未来标签不能进入仿真")
    if plan.temporal_selection.requires_consumer_binding:
        raise ValueError(
            "共享期货市场事件必须先按可见时点物化，不能直接读取时态候选全集"
        )
    bindings = parameters["event_field_bindings"]
    columns = tuple(dict.fromkeys(bindings.values()))
    if not set(columns) <= set(plan.query.field_ids):
        raise ValueError("共享期货事件字段未被 QueryIR 显式请求")
    reference = DatasetArtifactRef.from_dict(references[request_id])
    dataset = ArtifactResolver(
        _input_external_root(context, "market") / "data",
        max_batch_rows=8_192,
        max_batch_bytes=min(
            64 * 1024 * 1024, context.effective_resource_budget.memory_bytes // 4
        ),
    ).resolve(reference)
    if dataset.manifest.get("admitted_plan_hash") != plan.plan_hash:
        raise ValueError("共享期货事件与已准入计划不一致")
    if (
        dataset.parquet_uncompressed_bytes(columns=columns)
        > context.effective_resource_budget.memory_bytes // 4
    ):
        raise ValueError("共享期货事件工作集超过节点内存预算")
    rows = []
    retained_bytes = sys.getsizeof(rows)
    input_budget = context.effective_resource_budget.memory_bytes // 8
    for batch in dataset.iter_batches(columns=columns, batch_size=8_192):
        # 输入还会被领域校验和上下文封存复制；转换前为字典及标量预留空间。
        conversion_bytes = 8 * int(batch.nbytes) + int(batch.num_rows) * (
            128 + 72 * len(columns)
        )
        if retained_bytes + conversion_bytes > input_budget:
            raise ValueError("共享期货 Arrow 转事件对象的峰值超过输入内存预算")
        for source in batch.to_pylist():
            if len(rows) >= parameters["max_event_rows"]:
                raise ValueError("共享期货事件行数超过 max_event_rows")
            row = {
                field: source[column]
                for field, column in bindings.items()
                if source[column] is not None
            }
            row = _json_ready(row)
            retained_bytes += (
                sys.getsizeof(row)
                + sum(
                    sys.getsizeof(key) + sys.getsizeof(value)
                    for key, value in row.items()
                )
                + 8
            )
            if retained_bytes > input_budget:
                raise ValueError("共享期货常驻事件对象超过输入内存预算")
            rows.append(row)
    return rows


def _bind_completed_bars(
    context: OperatorRuntimeContext, spec: Mapping, events: list[dict]
) -> list[dict]:
    """分钟价格与时间来自完成bar；容量和涨跌停来自同一时点的准入事实。"""
    payload = _input_external_payload(context, "bars")
    require_minute_price_mode(payload, consumer="共享期货分钟成交", expected_mode="raw")
    request = _environment(context).admitted_plans.get(
        str(payload.get("request_id", ""))
    )
    if (
        request is None
        or request.minute_asset_class != "cn_future"
        or str(getattr(request.query.purpose, "value", request.query.purpose))
        != "feature"
        or int(payload.get("interval_minutes", 0)) != 1
    ):
        raise ValueError("共享期货分钟算子只接受真实期货的一分钟完成bar")
    by_key = {}
    for index, event in enumerate(events):
        if event["kind"] == "bar":
            key = (
                event["instrument_id"],
                aware_time(event["event_time"], "event_time"),
            )
            if key in by_key:
                raise ValueError("同一合约完成bar只能绑定一条容量事实")
            by_key[key] = index
    instruments = {row["instrument_id"]: row for row in spec["instruments"]}
    sessions = {
        (row["instrument_id"], row["trading_date"]): row for row in spec["sessions"]
    }
    consumed = set()
    _, partitions = _operator_bar_partitions(context, payload)
    for _partition, rows in partitions:
        for row in rows:
            code = str(row["instrument"])
            key = (code, aware_time(row["bar_end"], "bar_end"))
            if code not in instruments:
                raise ValueError("分钟工件包含未声明的共享合约")
            session = sessions.get((code, row["trading_date"].isoformat()))
            if session is None:
                raise ValueError("分钟bar交易日不属于冻结的共享合约会话")
            start = aware_time(row["bar_start"], "bar_start")
            end = aware_time(row["bar_end"], "bar_end")
            if not any(
                aware_time(segment["starts_at"], "starts_at")
                <= start
                < end
                <= aware_time(segment["ends_at"], "ends_at")
                for segment in session["segments"]
            ):
                raise ValueError("完成bar起止不在冻结的共享合约会话段内")
            if row.get("completed") is not True or row.get("quality_status") != "pass":
                raise ValueError("共享期货不能消费未完成或质量失败的分钟bar")
            if key not in by_key:
                raise ValueError("完成bar缺少已准入容量及涨跌停事实")
            if key in consumed:
                raise ValueError("共享期货完成bar重复")
            index = by_key[key]
            event = dict(events[index])
            price = Decimal(str(row["open"])) * Decimal(
                str(instruments[code]["price_scale"])
            )
            if price != price.to_integral_value():
                raise ValueError("完成bar价格不能精确映射到合约价格尺度")
            event.update(
                price_units=int(price), bar_start=row["bar_start"], completed=True
            )
            if aware_time(event["available_at"], "available_at") < aware_time(
                row["available_time"], "available_time"
            ):
                raise ValueError("分钟事件不能早于完成bar可见时点")
            if date.fromisoformat(str(event["trading_date"])) != row["trading_date"]:
                raise ValueError("分钟事件交易日与完成bar会话不一致")
            events[index] = _json_ready(event)
            consumed.add(key)
    if consumed != set(by_key):
        raise ValueError("准入分钟执行事件缺少对应的完成bar")
    return events


def _write_artifact(
    context: OperatorRuntimeContext, frequency: str, *, output_root: Path
) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    parameters = _parameters(context)
    spec = validate_shared_futures_spec(_json_ready(parameters["spec"]))
    if spec["frequency"] != frequency:
        raise ValueError("共享期货算子与 spec.frequency 不一致")
    events = _market_events(context, parameters)
    if frequency == "1m":
        events = _bind_completed_bars(context, spec, events)
    events = validate_shared_futures_events(spec, events)
    clock = aware_time(_environment(context).fixed_clock, "fixed_clock")
    if any(aware_time(event["event_time"], "event_time") > clock for event in events):
        raise ValueError("共享期货市场事件晚于固定研究时钟")
    from research_pipeline.simulation.shared_futures import (
        run_shared_futures_simulation,
    )
    from research_pipeline.simulation.shared_futures_result import (
        write_shared_futures_result,
    )

    result = run_shared_futures_simulation(spec, events)
    build_root = output_root / "shared_futures"
    manifest = write_shared_futures_result(result, build_root)
    for name, declaration in manifest["tables"].items():
        target = output_root / SHARED_FUTURES_RESULT_PATHS[name] / "part-00000.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        (build_root / declaration["path"]).replace(target)
        declaration["path"] = f"{SHARED_FUTURES_RESULT_PATHS[name]}/part-00000.parquet"
    context_root = output_root / SHARED_FUTURES_RESULT_PATHS["context"]
    context_root.mkdir(parents=True, exist_ok=True)
    context_json = canonical_json(_json_ready(result.context))
    pq.write_table(
        pa.Table.from_pylist(
            [{"payload": context_json}], schema=pa.schema([("payload", pa.string())])
        ),
        context_root / "part-00000.parquet",
    )
    # 正式目录工件使用 ResultSpec 路径，金融上下文仅封存列式表。
    (build_root / "manifest.json").unlink()
    (build_root / "context.json").unlink()
    payload = {
        "version": "research-shared-futures-artifact-v1",
        "frequency": frequency,
        "tables": manifest["tables"],
        "context_path": SHARED_FUTURES_RESULT_PATHS["context"],
    }
    (output_root / "result.json").write_text(canonical_json(payload), encoding="utf-8")
    return payload


def execute_finance_simulation_shared_futures_daily_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    _, outputs = _factor_external_result(
        context, lambda **kwargs: _write_artifact(context, "1d", **kwargs)
    )
    return outputs


def execute_finance_simulation_shared_futures_intraday_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    _, outputs = _factor_external_result(
        context, lambda **kwargs: _write_artifact(context, "1m", **kwargs)
    )
    return outputs
