"""Broker 生命周期的分区支持文件合同。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .orders import SimulationContractError

ORDER_LIFECYCLE_CONTRACT_VERSION = "research-order-lifecycle-v1"
ORDER_LIFECYCLE_PATH_PREFIX = "simulation/context-tables/order-lifecycle"
ORDER_LIFECYCLE_DECLARATION = {
    "contract_version": ORDER_LIFECYCLE_CONTRACT_VERSION,
    "path_prefix": ORDER_LIFECYCLE_PATH_PREFIX,
}
ORDER_LIFECYCLE_SCHEMA = pa.schema([
    pa.field("portfolio_id", pa.string(), nullable=False),
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("trading_session", pa.date32(), nullable=False),
    pa.field("time_in_force", pa.string(), nullable=False),
    pa.field("order_type", pa.string(), nullable=False),
    pa.field("sequence", pa.int64(), nullable=False),
    pa.field("event_time", pa.timestamp("ns", tz="Asia/Shanghai"), nullable=False),
    pa.field("from_state", pa.string(), nullable=False),
    pa.field("to_state", pa.string(), nullable=False),
    pa.field("cumulative_filled_quantity", pa.int64(), nullable=False),
    pa.field("remaining_quantity", pa.int64(), nullable=False),
    pa.field("reason", pa.string(), nullable=True),
])


def lifecycle_table(rows: Iterable[Mapping[str, object]]) -> pa.Table:
    """只转换 Broker 原始事实，拒绝缺列、空值和无时区时点。"""

    values = [dict(row) for row in rows]
    for row in values:
        if set(row) != set(ORDER_LIFECYCLE_SCHEMA.names):
            raise SimulationContractError("订单生命周期字段不完整")
        for field in ORDER_LIFECYCLE_SCHEMA:
            if not field.nullable and row[field.name] is None:
                raise SimulationContractError(f"订单生命周期 {field.name} 不能为空")
        at = row["event_time"]
        if getattr(at, "tzinfo", None) is None or at.utcoffset() is None:
            raise SimulationContractError("订单生命周期 event_time 必须带时区")
        for name in ("sequence", "cumulative_filled_quantity", "remaining_quantity"):
            if type(row[name]) is not int:
                raise SimulationContractError(f"订单生命周期 {name} 必须是整数")
    try:
        return pa.Table.from_pylist(values, schema=ORDER_LIFECYCLE_SCHEMA)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise SimulationContractError("订单生命周期物理类型无效") from exc


def write_order_lifecycle_session(
    simulation_root: str | Path,
    session: date | None,
    rows: Iterable[Mapping[str, object]],
) -> Path:
    """每会话一个分区；零会话结果用 session=empty 保留空表 schema。"""

    table = lifecycle_table(rows)
    if any(value != session for value in table.column("trading_session").to_pylist()):
        raise SimulationContractError("订单生命周期交易会话与分区不一致")
    label = "empty" if session is None else session.isoformat()
    path = Path(simulation_root) / "context-tables/order-lifecycle" / f"session={label}" / "data.parquet"
    if path.exists():
        raise SimulationContractError("订单生命周期会话分区不能覆盖")
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def write_order_lifecycle(
    simulation_root: str | Path,
    rows: Iterable[Mapping[str, object]],
) -> None:
    groups: dict[date, list[Mapping[str, object]]] = {}
    for row in rows:
        groups.setdefault(row["trading_session"], []).append(row)
    if not groups:
        write_order_lifecycle_session(simulation_root, None, ())
    for session, values in sorted(groups.items()):
        write_order_lifecycle_session(simulation_root, session, values)


def read_order_lifecycle(simulation_root: str | Path) -> tuple[dict[str, object], ...]:
    root = Path(simulation_root) / "context-tables/order-lifecycle"
    paths = sorted(root.glob("session=*/data.parquet"))
    if not paths:
        raise SimulationContractError("新仿真合同缺少订单生命周期分区")
    rows = []
    for path in paths:
        table = pq.ParquetFile(path).read()
        if table.schema != ORDER_LIFECYCLE_SCHEMA:
            raise SimulationContractError("订单生命周期物理 schema 无效")
        values = table.to_pylist()
        label = path.parent.name.removeprefix("session=")
        if (label == "empty" and values) or any(
            row["trading_session"].isoformat() != label for row in values
        ):
            raise SimulationContractError("订单生命周期交易会话与分区不一致")
        rows.extend(values)
    return tuple(rows)
