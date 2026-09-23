"""minute_simulation 算子族及其直接共享实现。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import shutil
from typing import Iterable, Iterator, Mapping, MutableMapping
import pandas as pd
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.data_plane import require_minute_price_mode
from research_pipeline.domain import MinuteRuleResolver, PortfolioTarget, load_minute_rule_snapshot_bundle, load_session_policy_bundle
from research_pipeline.platform.minute_operator_contracts import MINUTE_TARGET_PAYLOAD_SCHEMA_ID
from research_pipeline.simulation import BarTcaFormalFill, BarTcaOrder, IntradayExecutionPolicy, MinuteExecutionBar, bar_tca_policy_from_parameters
from research_pipeline.simulation.bar_tca import BarTcaArtifactWriter
from research_pipeline.simulation.events import FinancialEvent
from research_pipeline.simulation.minute_execution import MinuteEventSimulationStateMachine, PreparedMinuteTarget
from research_pipeline.simulation.result_contract import SimulationResultArtifactWriter
from ..bar_tca_adapter import tca_metadata
from ..operator_runtime import OperatorRuntimeContext, RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _capture, _input_external_payload, _input_external_root, _json_ready, _parameters
from .minute_io import _aware, _minute_adjustment_context, _minute_artifact_partitions, _minute_timestamp_type, _operator_bar_partitions, _write_minute_row_partition


def _minute_execution_bar_context_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("instrument_id", pa.string()),
            pa.field("asset_class", pa.string()),
            pa.field("trading_date", pa.date32()),
            pa.field("session_id", pa.string()),
            pa.field("bar_start", _minute_timestamp_type(pa)),
            pa.field("bar_end", _minute_timestamp_type(pa)),
            pa.field("available_time", _minute_timestamp_type(pa)),
            pa.field("receipt_time", _minute_timestamp_type(pa)),
            pa.field("interval_minutes", pa.int64()),
            pa.field("open_units", pa.int64()),
            pa.field("high_units", pa.int64()),
            pa.field("low_units", pa.int64()),
            pa.field("close_units", pa.int64()),
            pa.field("avg_units", pa.int64()),
            pa.field("volume", pa.int64()),
            pa.field("open_interest", pa.int64()),
            pa.field("completed", pa.bool_()),
            pa.field("quality_status", pa.string()),
            pa.field("source_snapshot_hash", pa.string()),
            pa.field("source_sequence", pa.int64()),
            pa.field("bar_hash", pa.string()),
        ]
    )


def _minute_decision_benchmark_context_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("portfolio_id", pa.string()),
            pa.field("order_id", pa.string()),
            pa.field("decision_price_units", pa.int64()),
            pa.field("available_at", _minute_timestamp_type(pa)),
            pa.field("source_hash", pa.string()),
        ]
    )


def _minute_execution_observation_context_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("source_fill_id", pa.string()),
            pa.field("arrival_price_units", pa.int64()),
            pa.field("arrival_price_available_at", _minute_timestamp_type(pa)),
            pa.field("visible_capacity", pa.int64()),
            pa.field("capacity_available_at", _minute_timestamp_type(pa)),
        ]
    )


def _minute_settlement_event_context_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("instrument_id", pa.string()),
            pa.field("instrument_hash", pa.string()),
            pa.field("settlement_time", _minute_timestamp_type(pa)),
            pa.field("settlement_price_units", pa.int64()),
            pa.field("price_scale", pa.int64()),
            pa.field("position_contracts_before", pa.int64()),
            pa.field("previous_settlement_price_units", pa.int64()),
            pa.field("contract_multiplier", pa.int64()),
            pa.field("speculative_margin_ppm", pa.int64()),
            pa.field("pnl_units", pa.int64()),
            pa.field("required_margin_units", pa.int64()),
            pa.field("rule_hash", pa.string()),
            pa.field("rule_snapshot_hashes", pa.list_(pa.string())),
            pa.field("aggregate_required_margin_units", pa.int64()),
            pa.field("event_json", pa.string()),
            pa.field("event_hash", pa.string()),
        ]
    )


_MINUTE_CONTEXT_TABLES = {
    "execution_bars": (
        "simulation/context-tables/execution-bars",
        "research.minute-financial-context.execution-bars.v1",
        _minute_execution_bar_context_schema,
    ),
    "decision_benchmarks": (
        "simulation/context-tables/decision-benchmarks",
        "research.minute-financial-context.decision-benchmarks.v1",
        _minute_decision_benchmark_context_schema,
    ),
    "execution_observations": (
        "simulation/context-tables/execution-observations",
        "research.minute-financial-context.execution-observations.v1",
        _minute_execution_observation_context_schema,
    ),
    "settlement_events": (
        "simulation/context-tables/settlement-events",
        "research.minute-financial-context.settlement-events.v1",
        _minute_settlement_event_context_schema,
    ),
}


def _write_minute_context_session(
    staging: Path,
    *,
    session: date,
    rows_by_table: Mapping[str, Iterable[Mapping[str, object]]],
    table_counts: MutableMapping[str, int],
    partition_counts: MutableMapping[str, int],
) -> None:
    """把一个已闭合交易日的金融上下文立即写入列式分区。"""

    for name, rows in rows_by_table.items():
        values = tuple(rows)
        if not values:
            continue
        prefix, _schema_id, schema_factory = _MINUTE_CONTEXT_TABLES[name]
        row_count = _write_minute_row_partition(
            staging / prefix / f"session={session.isoformat()}" / "data.parquet",
            rows=values,
            schema=schema_factory(),
        )
        table_counts[name] += row_count
        partition_counts[name] += 1


def _minute_execution_bar_context_row(
    bar: MinuteExecutionBar,
) -> dict[str, object]:
    """保留列式时间类型，并显式绑定原始 MinuteExecutionBar 身份。"""

    return {
        "instrument_id": bar.instrument_id,
        "asset_class": bar.asset_class,
        "trading_date": bar.trading_date,
        "session_id": bar.session_id,
        "bar_start": bar.bar_start,
        "bar_end": bar.bar_end,
        "available_time": bar.available_time,
        "receipt_time": bar.receipt_time,
        "interval_minutes": bar.interval_minutes,
        "open_units": bar.open_units,
        "high_units": bar.high_units,
        "low_units": bar.low_units,
        "close_units": bar.close_units,
        "avg_units": bar.avg_units,
        "volume": bar.volume,
        "open_interest": bar.open_interest,
        "completed": bar.completed,
        "quality_status": bar.quality_status,
        "source_snapshot_hash": bar.source_snapshot_hash,
        "source_sequence": bar.source_sequence,
        "bar_hash": bar.bar_hash,
    }


def _minute_settlement_event_context_row(
    row: Mapping[str, object],
) -> dict[str, object]:
    result = dict(row)
    result["settlement_time"] = _aware(result["settlement_time"])
    result["event_json"] = canonical_json(_json_ready(result.pop("event")))
    return result


def _finalize_empty_minute_context_tables(
    staging: Path,
    *,
    partition_counts: MutableMapping[str, int],
) -> None:
    """零订单/零成交时仍发布可由 ResultSpec 选择的带类型空表。"""

    for name, (prefix, _schema_id, schema_factory) in _MINUTE_CONTEXT_TABLES.items():
        if partition_counts[name] != 0:
            continue
        _write_minute_row_partition(
            staging / prefix / "part-00000.parquet",
            rows=(),
            schema=schema_factory(),
        )
        partition_counts[name] = 1


def _minute_canonical_session_groups(
    staging: Path,
    table_name: str,
) -> Iterator[tuple[date, list[dict[str, object]]]]:
    """六表 writer 的每个非空分区都只对应一个已闭合交易日。"""

    import pyarrow.parquet as pq

    root = staging / "simulation/result-contract" / table_name
    for path in sorted(root.glob("part-*.parquet")):
        rows = [dict(row) for row in pq.read_table(path).to_pylist()]
        if not rows:
            continue
        sessions = {row.get("session") for row in rows}
        if len(sessions) != 1:
            raise ValueError(f"分钟 canonical {table_name} 分区混入多个交易日")
        session = next(iter(sessions))
        if not isinstance(session, date):
            raise ValueError(f"分钟 canonical {table_name} 缺少交易日")
        yield session, rows


def _minute_context_session_rows(
    staging: Path,
    *,
    table_name: str,
    session: date,
) -> list[dict[str, object]]:
    import pyarrow.parquet as pq

    prefix = _MINUTE_CONTEXT_TABLES[table_name][0]
    path = staging / prefix / f"session={session.isoformat()}" / "data.parquet"
    if not path.is_file():
        return []
    return [dict(row) for row in pq.read_table(path).to_pylist()]


def _minute_tca_policy_dimensions(
    staging: Path,
    *,
    expected_price_scale: int,
) -> int:
    """只投影两个定长列，确认 TCA 的唯一合约乘数。"""

    import pyarrow.parquet as pq

    price_scales: set[int] = set()
    multipliers: set[int] = set()
    root = staging / "simulation/result-contract/fills"
    for path in sorted(root.glob("part-*.parquet")):
        table = pq.read_table(
            path, columns=["price_scale", "contract_multiplier"]
        )
        price_scales.update(int(value.as_py()) for value in table["price_scale"])
        multipliers.update(
            int(value.as_py()) for value in table["contract_multiplier"]
        )
    if price_scales and price_scales != {expected_price_scale}:
        raise ValueError("分钟正式 fill 的价格精度与资产类别不一致")
    if len(multipliers) > 1:
        raise ValueError("分钟 Bar TCA 当前只支持唯一合约乘数")
    return next(iter(multipliers), 1)


def _minute_tca_session_inputs(
    *,
    order_rows: Iterable[Mapping[str, object]],
    fill_rows: Iterable[Mapping[str, object]],
    benchmark_rows: Iterable[Mapping[str, object]],
    observation_rows: Iterable[Mapping[str, object]],
    source_ledger_hash: str,
) -> tuple[tuple[BarTcaOrder, ...], tuple[BarTcaFormalFill, ...]]:
    benchmarks = {
        (str(row["portfolio_id"]), str(row["order_id"])): row
        for row in benchmark_rows
    }
    observations = {
        str(row["source_fill_id"]): row for row in observation_rows
    }
    orders = []
    for row in order_rows:
        key = (str(row["portfolio_id"]), str(row["order_id"]))
        benchmark = benchmarks.pop(key, None)
        if benchmark is None:
            raise ValueError("TCA 正式订单缺少决策时可见基准")
        decision_at = _aware(row["decision_time"])
        available_at = _aware(benchmark["available_at"])
        if available_at > decision_at:
            raise ValueError("TCA 决策基准在决策时尚不可见")
        orders.append(BarTcaOrder(
            order_id=str(row["order_id"]),
            instrument_id=str(row["instrument_id"]),
            asset_class=str(row["asset_class"]),
            side=str(row["side"]),
            requested_quantity=int(row["requested_quantity"]),
            filled_quantity=int(row["filled_quantity"]),
            status=str(row["status"]),
            terminal_reason=(
                None if row.get("terminal_reason") is None
                else str(row["terminal_reason"])
            ),
            decision_time=decision_at,
            submitted_at=_aware(row["submitted_at"]),
            decision_price_units=int(benchmark["decision_price_units"]),
            decision_price_available_at=available_at,
            decision_price_source_hash=str(benchmark["source_hash"]),
            source_order_hash=str(row["source_order_hash"]),
        ))
    if benchmarks:
        raise ValueError("TCA 决策基准引用未知正式订单")
    fills = []
    for row in fill_rows:
        fill_id = str(row["fill_id"])
        observation = observations.pop(fill_id, None)
        if observation is None:
            raise ValueError("TCA 正式 fill 缺少对应成交时观察")
        fills.append(BarTcaFormalFill(
            source_fill_id=fill_id,
            order_id=str(row["order_id"]),
            instrument_id=str(row["instrument_id"]),
            asset_class=str(row["asset_class"]),
            side=str(row["side"]),
            fill_time=_aware(row["fill_time"]),
            quantity=int(row["quantity"]),
            execution_price_units=int(row["execution_price_units"]),
            formal_fee_units=int(row["fee_units"]),
            source_fill_hash=str(row["source_fill_hash"]),
            source_ledger_hash=source_ledger_hash,
            arrival_price_units=int(observation["arrival_price_units"]),
            arrival_price_available_at=_aware(
                observation["arrival_price_available_at"]
            ),
            visible_capacity=int(observation["visible_capacity"]),
            capacity_available_at=_aware(observation["capacity_available_at"]),
        ))
    if observations:
        raise ValueError("TCA 成交时观察引用未知正式 fill")
    return tuple(orders), tuple(fills)


def _write_minute_bar_tca(
    staging: Path,
    *,
    parameters: Mapping[str, object],
    asset_class: str,
    price_scale: int,
    rule_snapshot_hash: str,
    source_simulation_hash: str,
    source_ledger_hash: str,
):
    multiplier = _minute_tca_policy_dimensions(
        staging, expected_price_scale=price_scale
    )
    policy = bar_tca_policy_from_parameters(
        parameters,
        asset_class=asset_class,
        bar_frequency="minute",
        rule_snapshot_hash=rule_snapshot_hash,
        contract_multiplier=multiplier,
        price_scale=price_scale,
    )
    writer = BarTcaArtifactWriter(
        staging / "simulation/tca",
        policy=policy,
        source_simulation_hash=source_simulation_hash,
        source_ledger_hash=source_ledger_hash,
        table_references={
            "formal_orders": {
                "schema_id": "research.simulation.orders.v1",
                "path_prefix": "simulation/result-contract/orders",
            },
            "formal_fills": {
                "schema_id": "research.simulation.fills.v1",
                "path_prefix": "simulation/result-contract/fills",
            },
            "decision_benchmarks": {
                "schema_id": (
                    "research.minute-financial-context.decision-benchmarks.v1"
                ),
                "path_prefix": (
                    "simulation/context-tables/decision-benchmarks"
                ),
            },
            "execution_observations": {
                "schema_id": (
                    "research.minute-financial-context.execution-observations.v1"
                ),
                "path_prefix": (
                    "simulation/context-tables/execution-observations"
                ),
            },
        },
    )
    fill_groups = iter(_minute_canonical_session_groups(staging, "fills"))
    pending_fill = next(fill_groups, None)
    for session, order_rows in _minute_canonical_session_groups(staging, "orders"):
        if pending_fill is not None and pending_fill[0] < session:
            raise ValueError("分钟 canonical fill 缺少同会话正式订单")
        fill_rows: list[dict[str, object]] = []
        if pending_fill is not None and pending_fill[0] == session:
            fill_rows = pending_fill[1]
            pending_fill = next(fill_groups, None)
        orders, fills = _minute_tca_session_inputs(
            order_rows=order_rows,
            fill_rows=fill_rows,
            benchmark_rows=_minute_context_session_rows(
                staging, table_name="decision_benchmarks", session=session
            ),
            observation_rows=_minute_context_session_rows(
                staging, table_name="execution_observations", session=session
            ),
            source_ledger_hash=source_ledger_hash,
        )
        writer.append_session(session, orders=orders, formal_fills=fills)
    if pending_fill is not None or next(fill_groups, None) is not None:
        raise ValueError("分钟 canonical fill 引用了未知会话订单")
    return writer.finalize()


def execute_finance_simulation_intraday_v3(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    bars_payload = _input_external_payload(context, "bars")
    require_minute_price_mode(
        bars_payload, consumer="分钟仿真成交", expected_mode="raw"
    )
    target_payload = _input_external_payload(context, "targets")
    parameters = _parameters(context)
    bundle = load_minute_rule_snapshot_bundle()
    if parameters["rule_bundle_hash"] != bundle.bundle_hash:
        raise ValueError("分钟 simulation 声明的 rule bundle hash 与受信发布锚点不一致")

    staging = context.external_store.prepare()
    try:
        target_dataset, target_partitions = _minute_artifact_partitions(
            context, target_payload, port="targets"
        )
        target_source_root = _input_external_root(context, "targets")
        target_partition_receipts = []
        target_table_root = staging / "simulation/targets"
        for index, partition in enumerate(target_dataset.partitions):
            source_path = target_source_root / Path(partition.relative_path)
            target_path = target_table_root / f"part-{index:05d}.parquet"
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target_path)
            target_partition_receipts.append({
                "partition_key": partition.partition_key,
                "row_count": partition.row_count,
                "source_partition_identity": typed_canonical_hash(
                    partition.to_dict()
                ),
            })
        target_artifact_row_count = sum(
            int(item["row_count"]) for item in target_partition_receipts
        )

        target_instrument_hashes: dict[str, str] = {}

        def prepared_targets() -> Iterator[PreparedMinuteTarget]:
            for _partition, rows in target_partitions:
                for row in rows:
                    raw_target_json = row.get("portfolio_target_json")
                    try:
                        raw_target = json.loads(str(raw_target_json))
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            "分钟 target 工件缺少 PortfolioTarget 完整载荷"
                        ) from exc
                    if not isinstance(raw_target, Mapping):
                        raise ValueError("分钟 target 工件缺少 PortfolioTarget 完整载荷")
                    target = PortfolioTarget.from_dict(raw_target)
                    if row.get("target_hash") != target.target_hash:
                        raise ValueError("分钟 target 行与 PortfolioTarget 身份不一致")
                    if len(target.entries) != 1:
                        raise ValueError("分钟仿真 target 必须恰好包含一个标的")
                    instrument = target.entries[0].instrument
                    if (
                        str(row.get("instrument")) != instrument.instrument_id
                        or _aware(row.get("decision_time")) != target.decision_time
                    ):
                        raise ValueError("分钟 target 行与 PortfolioTarget 时间或标的不一致")
                    existing_hash = target_instrument_hashes.setdefault(
                        instrument.instrument_id, instrument.instrument_hash
                    )
                    if existing_hash != instrument.instrument_hash:
                        raise ValueError("分钟 target 同一标的 InstrumentKey 身份漂移")
                    yield PreparedMinuteTarget(
                        target=target,
                        instrument=instrument,
                        desired_quantity=int(target.entries[0].value),
                        eligible_after=_aware(row.get("bar_end")),
                    )

        policy = IntradayExecutionPolicy(
            model_id=str(parameters["execution_model"]),
            participation_ppm=int(parameters["participation_ppm"]),
            claim_ceiling=str(parameters["claim_ceiling"]),
        )
        machine = MinuteEventSimulationStateMachine(
            prepared_targets(),
            rule_bundle=bundle,
            policy=policy,
            initial_cash_units=int(parameters["initial_cash_units"]),
        )
        asset_class = str(machine.asset_class)
        price_scale = 3 if asset_class == "cn_etf" else 2
        result_writer = SimulationResultArtifactWriter(
            staging / "simulation/result-contract"
        )
        context_table_counts = {name: 0 for name in _MINUTE_CONTEXT_TABLES}
        context_partition_counts = {name: 0 for name in _MINUTE_CONTEXT_TABLES}

        def consume_session(output) -> None:
            result_writer.append_rows(output.rows)
            _write_minute_context_session(
                staging,
                session=output.session,
                rows_by_table={
                    "execution_bars": (
                        _minute_execution_bar_context_row(item)
                        for item in output.execution_bars
                    ),
                    "decision_benchmarks": output.decision_benchmarks,
                    "execution_observations": output.execution_observations,
                    "settlement_events": (
                        _minute_settlement_event_context_row(item)
                        for item in output.settlement_events
                    ),
                },
                table_counts=context_table_counts,
                partition_counts=context_partition_counts,
            )

        _bar_dataset, bar_partitions = _operator_bar_partitions(
            context, bars_payload
        )
        source_sequence = 0
        for _partition, rows in bar_partitions:
            for row in rows:
                bar = MinuteExecutionBar(
                    str(row["instrument"]),
                    asset_class,
                    row["trading_date"],
                    str(row["session_id"]),
                    row["bar_start"],
                    row["bar_end"],
                    row["available_time"],
                    row["available_time"],
                    int(row["interval_minutes"]),
                    *(
                        _price_to_units(row[name], price_scale)
                        for name in ("open", "high", "low", "close")
                    ),
                    (
                        None
                        if row["avg"] is None
                        else _price_to_units(row["avg"], price_scale)
                    ),
                    int(row["volume"] or 0),
                    (
                        None
                        if row["open_interest"] is None
                        else int(row["open_interest"])
                    ),
                    bool(row["completed"]),
                    str(row["quality_status"]),
                    str(row["source_snapshot_hash"]),
                    source_sequence,
                )
                source_sequence += 1
                for session_output in machine.consume_bar(bar):
                    consume_session(session_output)
        for session_output in machine.finish():
            consume_session(session_output)

        _finalize_empty_minute_context_tables(
            staging,
            partition_counts=context_partition_counts,
        )
        result_manifest = result_writer.finalize(
            semantics=machine.semantics,
            source_simulation_hash=machine.source_simulation_hash,
        )
        result_hash = str(result_manifest["result_hash"])
        ledger_hash = typed_canonical_hash({
            "simulation_result_hash": result_hash,
            "ledger_table_hashes": {
                name: result_manifest["table_hashes"][name]
                for name in ("costs", "cash", "positions", "valuations")
            },
        })
        tca_result, tca_manifest = _write_minute_bar_tca(
            staging,
            parameters=parameters,
            asset_class=asset_class,
            rule_snapshot_hash=bundle.bundle_hash,
            price_scale=price_scale,
            source_simulation_hash=result_hash,
            source_ledger_hash=ledger_hash,
        )
        context_tables = {
            name: {
                "schema_id": schema_id,
                "path_prefix": prefix,
                "row_count": context_table_counts[name],
                "partition_count": context_partition_counts[name],
            }
            for name, (prefix, schema_id, _schema_factory)
            in _MINUTE_CONTEXT_TABLES.items()
        }
        financial_context = {
            "contract_version": "research-minute-financial-context-v3",
            "asset_class": asset_class,
            "price_scale": price_scale,
            "rule_bundle": bundle.to_dict(),
            "target_artifact": {
                "schema_id": MINUTE_TARGET_PAYLOAD_SCHEMA_ID,
                "manifest": target_payload.get("manifest"),
                "source_dataset_reference_id": target_dataset.reference_id,
                "partition_count": len(target_partition_receipts),
                "row_count": sum(
                    int(item["row_count"]) for item in target_partition_receipts
                ),
                "partitions": target_partition_receipts,
                **_minute_adjustment_context(target_payload),
            },
            "context_tables": context_tables,
            "price_limit_references": _minute_price_limit_references(
                bundle=bundle,
                instrument_ids=set(target_instrument_hashes),
            ),
            "session_policy_bundle": load_session_policy_bundle().to_dict(),
            "source_simulation_hash": machine.source_simulation_hash,
            "simulation_result_hash": result_hash,
            "source_ledger_hash": ledger_hash,
        }
        financial_context["context_hash"] = typed_canonical_hash(
            _json_ready(financial_context)
        )
        simulation_directory = staging / "simulation"
        simulation_directory.mkdir(exist_ok=True)
        (simulation_directory / "context.json").write_text(
            canonical_json(_json_ready(financial_context)), encoding="utf-8"
        )
        payload = {
            "status": "simulation_succeeded",
            "result_hash": result_hash,
            "source_simulation_hash": machine.source_simulation_hash,
            "source_ledger_hash": ledger_hash,
            "simulation_result_contract": result_manifest,
            "tca": tca_metadata(tca_result, tca_manifest),
            "financial_context_hash": financial_context["context_hash"],
            "rule_bundle_hash": bundle.bundle_hash,
            "target_count": int(target_artifact_row_count),
            "order_count": int(result_manifest["table_rows"]["orders"]),
            "fill_count": int(result_manifest["table_rows"]["fills"]),
            "limitations": [
                "分钟执行只按下一根已完成 bar 研究级参与，不声明 Tick/LOB 或实盘排队",
                "ETF 费用是显式研究成本假设，不代表所有券商账户",
            ],
        }
        (staging / "result.json").write_text(
            canonical_json(_json_ready(payload)), encoding="utf-8"
        )
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    _capture(context, "minute_simulation", payload)
    return RuntimeNodeOutputs(
        {commit.artifact_name: RuntimeNodeValue.external(commit)},
        completion_metadata=RuntimeCompletionMetadata(
            artifact_hashes={
                "simulation": result_hash,
                "bar_tca": tca_result.result_hash,
                "financial_context": str(financial_context["context_hash"]),
            },
            counts={
                "target_count": int(target_artifact_row_count),
                "order_count": int(result_manifest["table_rows"]["orders"]),
                "fill_count": int(result_manifest["table_rows"]["fills"]),
            },
            backend_id="minute_event_ledger_v1",
            fidelity="completed_bar_next_event",
            limitations=tuple(payload["limitations"]),
        ),
    )


def _price_to_units(value: object, scale: int) -> int:
    amount = Decimal(str(value))
    units = int(
        (amount * (Decimal(10) ** scale)).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )
    if units <= 0:
        raise ValueError("分钟正式成交价格必须为正")
    return units


def _minute_tca_inputs(
    *,
    result,
    bars: tuple[MinuteExecutionBar, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    benchmark_columns = (
        "portfolio_id", "order_id", "decision_price_units",
        "available_at", "source_hash",
    )
    observation_columns = (
        "source_fill_id", "arrival_price_units",
        "arrival_price_available_at", "visible_capacity",
        "capacity_available_at",
    )
    benchmarks = []
    for order in result.tables["orders"].itertuples(index=False):
        decision_time = _aware(order.decision_time)
        candidates = [
            item for item in bars
            if item.instrument_id == str(order.instrument_id)
            and item.bar_end <= decision_time
            and item.available_time <= decision_time
            and item.completed
            and item.quality_status == "pass"
        ]
        if not candidates:
            raise ValueError("分钟 TCA 订单缺少决策时已完成的基准 bar")
        selected = max(
            candidates, key=lambda item: (item.bar_end, item.source_sequence)
        )
        benchmarks.append({
            "portfolio_id": str(order.portfolio_id),
            "order_id": str(order.order_id),
            "decision_price_units": selected.close_units,
            "available_at": selected.available_time,
            "source_hash": selected.bar_hash,
        })

    observations = []
    for fill in result.tables["fills"].itertuples(index=False):
        fill_time = _aware(fill.fill_time)
        matches = [
            item for item in bars
            if item.instrument_id == str(fill.instrument_id)
            and item.available_time == fill_time
            and item.completed
            and item.quality_status == "pass"
        ]
        if len(matches) != 1:
            raise ValueError("分钟 TCA fill 无法唯一绑定正式执行 bar")
        selected = matches[0]
        if selected.volume <= 0:
            raise ValueError("分钟正式 fill 对应执行 bar 缺少正的可见容量")
        observations.append({
            "source_fill_id": str(fill.fill_id),
            "arrival_price_units": selected.open_units,
            "arrival_price_available_at": selected.available_time,
            "visible_capacity": selected.volume,
            "capacity_available_at": selected.available_time,
        })
    # 零订单或零成交也是正式合同；显式列让 TCA 消费者能区分
    # “合法空集合”和“上游漏写 schema”。
    return (
        pd.DataFrame.from_records(benchmarks, columns=benchmark_columns),
        pd.DataFrame.from_records(observations, columns=observation_columns),
    )


def _minute_futures_settlement_events(
    *,
    result,
    bundle,
    targets: tuple[PortfolioTarget, ...],
    instrument_hashes: Mapping[str, str] | None = None,
) -> list[dict[str, object]]:
    """把仿真已消费的逐日结算事实完整写入 ResultStore 上下文。"""

    if result.semantics.asset_class != "cn_future":
        return []
    resolver = MinuteRuleResolver(bundle)
    settlement_rule_ids = (
        "rule.cn_futures.contract_multiplier.v1",
        "rule.cn_futures.margin.v1",
        "rule.cn_futures.price_tick.v1",
        "rule.cn_futures.session.v1",
        "rule.cn_futures.settlement.v1",
    )
    positions: dict[str, tuple[int, int]] = {}
    resolved_instrument_hashes = dict(instrument_hashes or {})
    for item in targets:
        instrument = item.entries[0].instrument
        existing = resolved_instrument_hashes.setdefault(
            instrument.instrument_id, instrument.instrument_hash
        )
        if existing != instrument.instrument_hash:
            raise ValueError("分钟期货结算标的身份漂移")
    fills_by_session: dict[date, list[object]] = {}
    for fill in result.tables["fills"].itertuples(index=False):
        fills_by_session.setdefault(fill.session, []).append(fill)
    events = []
    for cash in result.tables["cash"].itertuples(index=False):
        session = cash.session
        session_fills = fills_by_session.get(session, [])
        for fill in session_fills:
            instrument_id = str(fill.instrument_id)
            old_quantity, old_basis = positions.get(
                instrument_id,
                (0, int(fill.execution_price_units)),
            )
            direction = 1 if str(fill.side) == "buy" else -1
            expected_sign = 1 if str(fill.side) == "sell" else -1
            expected_realized = (
                (int(fill.execution_price_units) - old_basis)
                * int(fill.quantity)
                * int(fill.contract_multiplier)
                * expected_sign
                if str(fill.position_effect) == "close"
                else 0
            )
            if int(fill.realized_pnl_units) != expected_realized:
                raise ValueError("分钟期货 fill 已实现盈亏与结算基价不一致")
            positions[instrument_id] = (
                old_quantity + direction * int(fill.quantity),
                int(fill.execution_price_units),
            )

        instrument_ids = {
            instrument_id
            for instrument_id, (quantity, _basis) in positions.items()
            if quantity != 0
        }
        if not instrument_ids:
            if (
                int(cash.non_trade_cash_change_units) != 0
                or int(cash.margin_units) != 0
                or str(cash.non_trade_source_hash) != typed_canonical_hash([])
            ):
                raise ValueError("分钟期货空仓会话包含伪结算或保证金")
            continue
        facts = []
        for instrument_id in sorted(instrument_ids):
            settlement_candidates = [
                item for item in bundle.rules
                if item.instrument_id == instrument_id
                and item.rule_id == "rule.cn_futures.settlement.v1"
                and item.effective_from <= session <= item.effective_to
            ]
            if (
                len(settlement_candidates) != 1
                or settlement_candidates[0].available_at is None
            ):
                raise ValueError("分钟期货结算上下文缺少唯一收盘事件")
            settlement_time = settlement_candidates[0].available_at
            bindings = tuple(
                resolver.resolve(
                    rule_id=rule_id,
                    instrument_id=instrument_id,
                    effective_on=session,
                    as_of=settlement_time,
                )
                for rule_id in settlement_rule_ids
            )
            parameters = {
                key: value
                for binding in bindings
                for key, value in binding.rule.parameters
            }
            settlement_price = int(parameters["settlement_price_units"])
            multiplier = int(parameters["contract_unit_kg"])
            margin_ppm = int(parameters["speculative_initial_margin_ppm"])
            quantity, basis = positions.get(
                instrument_id, (0, settlement_price)
            )
            pnl_units = (settlement_price - basis) * quantity * multiplier
            required_margin = (
                settlement_price * multiplier * abs(quantity) * margin_ppm
                + 999_999
            ) // 1_000_000
            rule_hash = typed_canonical_hash({
                "bundle_hash": bundle.bundle_hash,
                "bindings": [item.identity_hash for item in bindings],
            })
            facts.append({
                "instrument_id": instrument_id,
                "instrument_hash": resolved_instrument_hashes.get(instrument_id),
                "settlement_time": settlement_time,
                "settlement_price_units": settlement_price,
                "price_scale": int(parameters["price_scale"]),
                "position_contracts_before": quantity,
                "previous_settlement_price_units": basis,
                "contract_multiplier": multiplier,
                "speculative_margin_ppm": margin_ppm,
                "pnl_units": pnl_units,
                "required_margin_units": required_margin,
                "rule_hash": rule_hash,
                "rule_snapshot_hashes": [
                    item.rule.snapshot_hash for item in bindings
                ],
            })
        settlement_times = {item["settlement_time"] for item in facts}
        if len(settlement_times) != 1:
            raise ValueError("分钟期货同一账户结算时点不一致")
        aggregate_margin = sum(int(item["required_margin_units"]) for item in facts)
        session_events = []
        for fact in facts:
            event = FinancialEvent(
                f"minute-settlement:{fact['instrument_id']}:{session.isoformat()}",
                "mark_to_market",
                fact["settlement_time"],
                session.isoformat(),
                "minute-default-cn-futures",
                str(fact["rule_hash"]),
                (
                    ("pnl_units", int(fact["pnl_units"])),
                    ("required_margin_units", aggregate_margin),
                ),
            )
            positions[str(fact["instrument_id"])] = (
                int(fact["position_contracts_before"]),
                int(fact["settlement_price_units"]),
            )
            session_events.append({
                **fact,
                "settlement_time": fact["settlement_time"].isoformat(),
                "aggregate_required_margin_units": aggregate_margin,
                "event": event.to_dict(),
                "event_hash": event.event_hash,
            })
        event_hashes = [str(item["event_hash"]) for item in session_events]
        if (
            typed_canonical_hash(sorted(event_hashes))
            != str(cash.non_trade_source_hash)
            or sum(int(item["pnl_units"]) for item in session_events)
            != int(cash.non_trade_cash_change_units)
            or aggregate_margin != int(cash.margin_units)
        ):
            raise ValueError("分钟期货结算上下文与正式现金账本不一致")
        events.extend(session_events)
    return events


def _minute_price_limit_references(
    *,
    bundle,
    instrument_ids: set[str],
) -> list[dict[str, object]]:
    references = [
        item.to_dict()
        for item in bundle.rules
        if item.instrument_id in instrument_ids
        and item.rule_id in {
            "rule.cn_stock.price_limit.v1",
            "rule.cn_fund.price_limit.v1",
            "rule.cn_futures.price_limit.v1",
        }
    ]
    if not references:
        raise ValueError("分钟正式仿真缺少价格限制参考载荷")
    return references
