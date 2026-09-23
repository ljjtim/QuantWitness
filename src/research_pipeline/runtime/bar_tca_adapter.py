"""把正式仿真结果接到唯一 Bar TCA 归因实现。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Mapping

import pandas as pd

from research_pipeline.simulation import (
    BarTcaFormalFill,
    BarTcaOrder,
    BarTcaPolicy,
    BarTcaResult,
    run_bar_tca,
    write_bar_tca_artifact,
)
from research_pipeline.simulation.result_contract import SimulationResultContract


def execute_simulation_result_bar_tca(
    *,
    simulation_result: SimulationResultContract,
    decision_benchmarks: pd.DataFrame,
    execution_observations: pd.DataFrame | None = None,
    policy: BarTcaPolicy,
    source_ledger_hash: str,
    output_root: str | Path,
) -> tuple[BarTcaResult, dict[str, object]]:
    """只读取公共结果合同；具体仿真引擎身份不进入 TCA 分派。"""

    if policy.asset_class != simulation_result.semantics.asset_class:
        raise ValueError("TCA policy 与仿真结果资产类别不一致")
    if policy.bar_frequency != simulation_result.semantics.frequency:
        raise ValueError("TCA policy 与仿真结果频率不一致")
    required_benchmarks = {
        "portfolio_id", "order_id", "decision_price_units",
        "available_at", "source_hash",
    }
    missing = required_benchmarks - set(decision_benchmarks.columns)
    if missing:
        raise ValueError(f"TCA 决策基准缺少字段: {sorted(missing)}")
    if decision_benchmarks.duplicated(["portfolio_id", "order_id"]).any():
        raise ValueError("TCA 决策基准订单主键重复")
    benchmark_by_order = {
        (str(row.portfolio_id), str(row.order_id)): row
        for row in decision_benchmarks.itertuples(index=False)
    }
    observation_by_fill: dict[str, object] = {}
    if execution_observations is not None:
        required_observations = {
            "source_fill_id", "arrival_price_units",
            "arrival_price_available_at", "visible_capacity",
            "capacity_available_at",
        }
        missing = required_observations - set(execution_observations.columns)
        if missing:
            raise ValueError(f"TCA 成交时观察缺少字段: {sorted(missing)}")
        if execution_observations.duplicated(["source_fill_id"]).any():
            raise ValueError("TCA 成交时观察的 source_fill_id 重复")
        observation_by_fill = {
            str(row.source_fill_id): row
            for row in execution_observations.itertuples(index=False)
        }
    formal_orders = []
    for row in simulation_result.tables["orders"].itertuples(index=False):
        key = (str(row.portfolio_id), str(row.order_id))
        benchmark = benchmark_by_order.get(key)
        if benchmark is None:
            raise ValueError("TCA 正式订单缺少决策时可见基准")
        decision_at = _aware(row.decision_time, "decision_time")
        available_at = _aware(benchmark.available_at, "benchmark.available_at")
        if available_at > decision_at:
            raise ValueError("TCA 决策基准在决策时尚不可见")
        formal_orders.append(BarTcaOrder(
            order_id=str(row.order_id),
            instrument_id=str(row.instrument_id),
            asset_class=str(row.asset_class),
            side=str(row.side),
            requested_quantity=int(row.requested_quantity),
            filled_quantity=int(row.filled_quantity),
            status=str(row.status),
            terminal_reason=(None if pd.isna(row.terminal_reason) else str(row.terminal_reason)),
            decision_time=decision_at,
            submitted_at=_aware(row.submitted_at, "submitted_at"),
            decision_price_units=int(benchmark.decision_price_units),
            decision_price_available_at=available_at,
            decision_price_source_hash=str(benchmark.source_hash),
            source_order_hash=str(row.source_order_hash),
        ))
    formal_fills = []
    for row in simulation_result.tables["fills"].itertuples(index=False):
        observation = observation_by_fill.get(str(row.fill_id))
        if execution_observations is not None and observation is None:
            raise ValueError("TCA 正式 fill 缺少对应成交时观察")
        formal_fills.append(BarTcaFormalFill(
            source_fill_id=str(row.fill_id),
            order_id=str(row.order_id),
            instrument_id=str(row.instrument_id),
            asset_class=str(row.asset_class),
            side=str(row.side),
            fill_time=_aware(row.fill_time, "fill_time"),
            quantity=int(row.quantity),
            execution_price_units=int(row.execution_price_units),
            formal_fee_units=int(row.fee_units),
            source_fill_hash=str(row.source_fill_hash),
            source_ledger_hash=source_ledger_hash,
            arrival_price_units=(
                None if observation is None else int(observation.arrival_price_units)
            ),
            arrival_price_available_at=(
                None if observation is None
                else _aware(
                    observation.arrival_price_available_at,
                    "arrival_price_available_at",
                )
            ),
            visible_capacity=(
                None if observation is None else int(observation.visible_capacity)
            ),
            capacity_available_at=(
                None if observation is None
                else _aware(
                    observation.capacity_available_at,
                    "capacity_available_at",
                )
            ),
        ))
    unknown_observations = set(observation_by_fill) - {
        str(item.source_fill_id) for item in formal_fills
    }
    if unknown_observations:
        raise ValueError("TCA 成交时观察引用未知正式 fill")
    formal_fill_tuple = tuple(formal_fills)
    result = run_bar_tca(
        tuple(formal_orders), formal_fill_tuple, policy=policy,
        source_simulation_hash=simulation_result.result_hash,
        source_ledger_hash=source_ledger_hash,
    )
    manifest = write_bar_tca_artifact(
        result, Path(output_root) / "simulation/tca", policy=policy,
        orders=tuple(formal_orders), formal_fills=formal_fill_tuple,
    )
    return result, manifest


def tca_metadata(result: BarTcaResult, manifest: Mapping[str, object]) -> dict[str, object]:
    row = result.research_rows[0]
    return {
        "tca_result_hash": result.result_hash,
        "tca_artifact_manifest_hash": manifest["manifest_hash"],
        "tca_policy_hash": result.policy_hash,
        "tca_input_hash": result.input_hash,
        "tca_implementation_digest": row["implementation_digest"],
        "tca_rule_snapshot_hash": row["rule_snapshot_hash"],
        "tca_claim_ceiling": row["claim_ceiling"],
        "tca_reconciliation_delta_units": 0,
        "tca_source_simulation_hash": result.source_simulation_hash,
        "tca_source_ledger_hash": result.source_ledger_hash,
        "tca_source_fill_manifest_hash": row["source_fill_manifest_hash"],
        "tca_liquidity_attribution_status": row["liquidity_attribution_status"],
        "tca_table_rows": manifest["table_rows"],
    }


def _aware(value: object, field: str) -> datetime:
    result = pd.Timestamp(value).to_pydatetime()
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field} 必须带时区")
    return result


__all__ = [
    "execute_simulation_result_bar_tca",
    "tca_metadata",
]
