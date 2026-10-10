"""共享期货九表、原始上下文及空表一致的列式读写。"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.domain.shared_futures import SHARED_FUTURES_CONTEXT_VERSION
from research_pipeline.domain.shared_futures_result import (
    SHARED_FUTURES_COLUMNS, SHARED_FUTURES_INTEGER_COLUMNS, SHARED_FUTURES_KEYS,
    SHARED_FUTURES_RESULT_VERSION, SHARED_FUTURES_SCHEMA_IDS,
    SHARED_FUTURES_TIMESTAMP_COLUMNS,
)
from .orders import SimulationContractError


def shared_futures_arrow_schema(name: str) -> pa.Schema:
    fields = []
    for column in SHARED_FUTURES_COLUMNS[name]:
        if column in SHARED_FUTURES_INTEGER_COLUMNS:
            dtype = pa.int64()
        elif column in SHARED_FUTURES_TIMESTAMP_COLUMNS:
            dtype = pa.timestamp("ns", tz="Asia/Shanghai")
        elif column == "session":
            dtype = pa.date32()
        else:
            dtype = pa.string()
        fields.append(pa.field(column, dtype, nullable=True))
    return pa.schema(fields)


def shared_futures_tables(rows: Mapping[str, list[dict]]) -> dict[str, pd.DataFrame]:
    tables = {name: pd.DataFrame(rows.get(name, []), columns=columns)
              for name, columns in SHARED_FUTURES_COLUMNS.items()}
    for column in ("cost_numerator", "cost_denominator"):
        tables["positions"][column] = tables["positions"][column].map(str)
    return tables


@dataclass(frozen=True)
class SharedFuturesResult:
    tables: Mapping[str, pd.DataFrame]
    context: Mapping[str, object]

    def __post_init__(self) -> None:
        if set(self.tables) != set(SHARED_FUTURES_COLUMNS):
            raise SimulationContractError("共享期货结果必须包含完整九表")
        if self.context.get("version") != SHARED_FUTURES_CONTEXT_VERSION:
            raise SimulationContractError("共享期货结果上下文版本无效")
        spec = self.context.get("spec")
        if not isinstance(spec, Mapping) or "market_events" not in self.context:
            raise SimulationContractError("共享期货结果缺少原始输入上下文")
        for name, columns in SHARED_FUTURES_COLUMNS.items():
            table = self.tables[name]
            if set(table.columns) != set(columns):
                raise SimulationContractError(f"共享期货 {name} 字段不符合独立schema")
            if table.duplicated(list(SHARED_FUTURES_KEYS[name])).any():
                raise SimulationContractError(f"共享期货 {name} 唯一键重复")
            for field in ("portfolio_id", "account_id", "currency"):
                if not table.empty and not table[field].eq(spec[field]).all():
                    raise SimulationContractError(f"共享期货 {name} 账户绑定错误")
        cash = self.tables["cash"]
        if cash.empty:
            raise SimulationContractError("共享期货结果缺少账户快照")
        if not (cash["equity_units"] == cash["cash_units"] + cash["unrealized_pnl_units"]).all():
            raise SimulationContractError("共享期货权益必须等于现金加未实现损益")
        if not (cash["available_units"] == cash["equity_units"] - cash["margin_units"] - cash["frozen_units"]).all():
            raise SimulationContractError("共享期货可用资金必须扣除持仓保证金和订单冻结")
        last = cash.sort_values(["event_time", "sequence"]).iloc[-1]
        if last["equity_units"] < 0 or last["available_units"] < 0 or last["frozen_units"] != 0:
            raise SimulationContractError("共享期货终态仍有资金缺口或订单冻结，不能发布结果")


def write_shared_futures_result(result: SharedFuturesResult, root: str | Path) -> dict[str, object]:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name, columns in SHARED_FUTURES_COLUMNS.items():
        frame = result.tables[name].loc[:, list(columns)].copy()
        if name == "positions":
            for column in ("cost_numerator", "cost_denominator"):
                frame[column] = frame[column].map(str)
        for column in set(columns) & SHARED_FUTURES_TIMESTAMP_COLUMNS:
            frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_convert("Asia/Shanghai")
        if "session" in columns:
            frame["session"] = pd.to_datetime(frame["session"]).dt.date
        arrow = pa.Table.from_pandas(frame, schema=shared_futures_arrow_schema(name), preserve_index=False, safe=True)
        pq.write_table(arrow, root / f"{name}.parquet")
        tables[name] = {"path": f"{name}.parquet", "schema_id": SHARED_FUTURES_SCHEMA_IDS[name], "rows": len(frame)}
    (root / "context.json").write_text(json.dumps(result.context, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    manifest = {"version": SHARED_FUTURES_RESULT_VERSION, "tables": tables, "context_path": "context.json"}
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    return manifest


def read_shared_futures_result(root: str | Path) -> SharedFuturesResult:
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("version") != SHARED_FUTURES_RESULT_VERSION or set(manifest.get("tables", {})) != set(SHARED_FUTURES_COLUMNS):
        raise SimulationContractError("共享期货结果清单版本或表声明无效")
    tables = {}
    for name in SHARED_FUTURES_COLUMNS:
        declaration = manifest["tables"][name]
        if declaration["path"] != f"{name}.parquet" or declaration["schema_id"] != SHARED_FUTURES_SCHEMA_IDS[name]:
            raise SimulationContractError("共享期货清单必须绑定固定表schema和路径")
        table = pq.read_table(root / declaration["path"])
        if not table.schema.equals(shared_futures_arrow_schema(name), check_metadata=False) or table.num_rows != declaration["rows"]:
            raise SimulationContractError(f"共享期货 {name} 物理schema或行数无效")
        tables[name] = table.to_pandas()
    if manifest.get("context_path") != "context.json":
        raise SimulationContractError("共享期货上下文路径无效")
    context = json.loads((root / "context.json").read_text(encoding="utf-8"))
    return SharedFuturesResult(tables, context)
