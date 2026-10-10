"""日频期货金融支持事实的列式封存。"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research_pipeline.platform import canonical_json, typed_canonical_hash
from .orders import SimulationContractError

FUTURES_CONTEXT_DECLARATION = {
    "contract_version": "research-futures-daily-context-v2",
    "path": "simulation/futures-context.json",
    "path_prefix": "simulation/futures-context-tables",
}
FUTURES_EXPLICIT_CONTEXT_VERSION = "research-futures-daily-context-v3"
FUTURES_CONTEXT_VERSIONS = ("research-futures-daily-context-v1", "research-futures-daily-context-v2", FUTURES_EXPLICIT_CONTEXT_VERSION)
FUTURES_CONTEXT_PART_ROWS = 65_536

FUTURES_CONTEXT_ROLES = (
    "intents", "fills", "settlements", "nav", "rule_snapshots", "rejections",
    "contributions", "portfolio", "market_inputs", "settlement_inputs", "tick_size_inputs",
)


@dataclass(frozen=True)
class FuturesFinancialContext:
    source_simulation_hash: str
    initial_cash_fen: int
    slippage_ticks: int
    account_mode: str
    timing_policy: Mapping[str, object]
    tables: Mapping[str, pd.DataFrame]
    contract_version: str = FUTURES_CONTEXT_DECLARATION["contract_version"]
    execution_mode: str = "intents"
    explicit_order_context: Mapping[str, object] | None = None

    @property
    def declaration(self) -> dict[str, str]:
        if self.contract_version not in FUTURES_CONTEXT_VERSIONS:
            raise SimulationContractError("日频期货金融上下文版本无效")
        return {**FUTURES_CONTEXT_DECLARATION, "contract_version": self.contract_version}

    @classmethod
    def from_result(cls, result) -> "FuturesFinancialContext":
        if int(result.initial_cash_fen) <= 0:
            raise SimulationContractError("日频期货金融上下文缺少期初资金")
        return cls(
            result.result_hash, int(result.initial_cash_fen), int(result.slippage_ticks),
            "independent_product_accounts" if not result.portfolio.empty else "single_account",
            dict(result.timing_policy),
            {role: getattr(result, role).copy() for role in FUTURES_CONTEXT_ROLES},
            FUTURES_EXPLICIT_CONTEXT_VERSION if result.explicit_order_context is not None else FUTURES_CONTEXT_DECLARATION["contract_version"],
            "explicit_orders" if result.explicit_order_context is not None else "intents",
            result.explicit_order_context,
        )


def write_futures_financial_context(
    simulation_root: Path, context: FuturesFinancialContext, *, simulation_result_hash: str,
) -> dict[str, object]:
    expected_version = (FUTURES_EXPLICIT_CONTEXT_VERSION if context.execution_mode == "explicit_orders"
                        else FUTURES_CONTEXT_DECLARATION["contract_version"])
    if context.contract_version != expected_version:
        raise SimulationContractError("新写出的日频期货金融上下文必须使用当前版本")
    if (context.execution_mode not in {"intents", "explicit_orders"}
            or (context.explicit_order_context is not None) != (context.execution_mode == "explicit_orders")):
        raise SimulationContractError("日频期货上下文与执行模式不一致")
    simulation_root.mkdir(parents=True, exist_ok=True)
    metadata = {}
    for role, frame in context.tables.items():
        prefix = f"simulation/futures-context-tables/{role}"
        count = 0
        if not frame.empty:
            frame = frame.copy(deep=False)
            for column in ("date", "trading_date", "execution_session"):
                if column in frame.columns:
                    frame[column] = pd.to_datetime(frame[column]).dt.date
            schema = pa.Schema.from_pandas(frame, preserve_index=False)
            integer_names = {
                "quantity", "position", "position_before", "position_after", "position_before_settlement",
                "position_opened_today", "opened_today_before", "opened_today_after",
                "opened_today_before_liquidation", "multiplier", "slippage_ticks", "year",
                "target_sequence", "leg_ordinal", "execution_price_units",
            }
            schema = pa.schema([
                pa.field(field.name, pa.int64(), nullable=True)
                if field.name in integer_names or field.name.endswith(("_fen", "_numerator", "_denominator", "_price_scale", "_sequence"))
                   or field.name == "price_scale"
                else field
                for field in schema
            ])
            session_column = next((key for key in ("trading_date", "execution_session", "date")
                                   if key in frame.columns), None)
            if session_column is None:
                raise SimulationContractError(f"日频期货支持表缺少交易日: {role}")
            sessions = frame[session_column]
            if sessions.isna().any():
                raise SimulationContractError(f"日频期货支持表交易日缺失: {role}")
            # 合并物理分区，仍按交易日稳定排序，行内时间及金融事实保持不变。
            frame = frame.sort_values(session_column, kind="stable")
            table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
            directory = simulation_root / "futures-context-tables" / role
            directory.mkdir(parents=True, exist_ok=True)
            for start in range(0, len(frame), FUTURES_CONTEXT_PART_ROWS):
                part = table.slice(start, FUTURES_CONTEXT_PART_ROWS)
                pq.write_table(part, directory / f"part-{count:05d}.parquet")
                count += 1
            del part, table
        metadata[role] = {"path_prefix": prefix, "row_count": len(frame), "partition_count": count}
    body = {
        "contract_version": context.contract_version,
        "source_simulation_hash": context.source_simulation_hash,
        "simulation_result_hash": simulation_result_hash,
        "initial_cash_fen": context.initial_cash_fen,
        "slippage_ticks": context.slippage_ticks,
        "account_mode": context.account_mode,
        "timing_policy": dict(context.timing_policy),
        "cash_scale": 2,
        "initial_position": 0,
        "account_model": "independent_allocation",
        "forced_execution_policy": "settlement_price_full_close",
        "margin_check_policy": "settlement_margin_check",
        "close_bucket_order": "yesterday_then_today",
        "pnl_rounding_policy": "half_up_per_event",
        "tables": metadata,
    }
    if context.execution_mode == "explicit_orders":
        body.update(execution_mode=context.execution_mode, explicit_order_context=context.explicit_order_context)
    payload = {**body, "context_hash": typed_canonical_hash(body)}
    (simulation_root / "futures-context.json").write_text(canonical_json(payload), encoding="utf-8")
    return payload


def read_futures_financial_context(simulation_root: Path) -> FuturesFinancialContext:
    payload = json.loads((simulation_root / "futures-context.json").read_text(encoding="utf-8"))
    body = {key: value for key, value in payload.items() if key != "context_hash"}
    if payload.get("context_hash") != typed_canonical_hash(body):
        raise SimulationContractError("日频期货金融上下文身份不一致")
    version = payload.get("contract_version")
    if version not in FUTURES_CONTEXT_VERSIONS:
        raise SimulationContractError("日频期货金融上下文版本无效")
    pattern = "session=*/data.parquet" if version == FUTURES_CONTEXT_VERSIONS[0] else "part-*.parquet"
    frames = {}
    for role in FUTURES_CONTEXT_ROLES:
        pieces = sorted((simulation_root / "futures-context-tables" / role).glob(pattern))
        if len(pieces) != int(payload["tables"][role]["partition_count"]):
            raise SimulationContractError("日频期货金融支持表分区数不一致")
        frames[role] = pd.concat([pd.read_parquet(item) for item in pieces], ignore_index=True) if pieces else pd.DataFrame()
        if len(frames[role]) != int(payload["tables"][role]["row_count"]):
            raise SimulationContractError("日频期货金融支持表行数不一致")
    return FuturesFinancialContext(
        str(payload["source_simulation_hash"]), int(payload["initial_cash_fen"]),
        int(payload["slippage_ticks"]), str(payload["account_mode"]), dict(payload["timing_policy"]), frames, version,
        str(payload.get("execution_mode", "intents")), payload.get("explicit_order_context"),
    )
