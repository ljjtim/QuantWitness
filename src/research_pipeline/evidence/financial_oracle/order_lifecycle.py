"""从封存支持分区独立核验订单生命周期与规范成交。"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, time
from zoneinfo import ZoneInfo
from itertools import groupby
from pathlib import Path
from typing import Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from ..errors import EvidenceContractError
from ..oracle_workspace import FINANCIAL_ORACLE_BATCH_SIZE, OracleTable, ResultTableSource
from .common import aware_datetime, date_value, integer, key, ordered_rows

LIFECYCLE_SOURCE_ID = "research.order-lifecycle.v1"
LIFECYCLE_DECLARATION = {
    "contract_version": "research-order-lifecycle-v1",
    "path_prefix": "simulation/context-tables/order-lifecycle",
}
# 独立描述物理合同，不能从生产转换函数或生产 schema 推导验证预期。
LIFECYCLE_SCHEMA = pa.schema([
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


def lifecycle_support_source(snapshot, manifest: Mapping[str, object]) -> ResultTableSource | None:
    semantics = manifest.get("semantics", {})
    if not isinstance(semantics, Mapping) or semantics.get("contract_version") not in {
        "research-simulation-result-semantics-v1", "research-simulation-result-semantics-v2",
    }:
        raise EvidenceContractError("仿真语义合同版本无效")
    if (semantics["contract_version"] == "research-simulation-result-semantics-v2"
            and "order_lifecycle_contract" not in manifest):
        raise EvidenceContractError("新仿真语义合同缺少订单生命周期声明")
    declaration = manifest.get("order_lifecycle_contract")
    prefix = LIFECYCLE_DECLARATION["path_prefix"] + "/"
    selected = [item for item in snapshot.bundle.support_files if item.source_path.startswith(prefix)]
    if "order_lifecycle_contract" not in manifest:
        if selected:
            raise EvidenceContractError("订单生命周期分区缺少合同声明")
        return None
    if (declaration != LIFECYCLE_DECLARATION or not selected
            or semantics["contract_version"] != "research-simulation-result-semantics-v2"):
        raise EvidenceContractError("新仿真 Result 缺少订单生命周期分区或合同无效")
    control = [item for item in snapshot.bundle.support_files
               if item.source_path == "simulation/result-contract/manifest.json"]
    if len(control) != 1 or any(item.artifact_key != control[0].artifact_key for item in selected):
        raise EvidenceContractError("订单生命周期必须来自正式仿真工件")
    paths: list[Path] = []
    labels: set[str] = set()
    rows = size = 0
    for item in sorted(selected, key=lambda value: value.source_path):
        parts = item.source_path.removeprefix(prefix).split("/")
        if len(parts) != 2 or not parts[0].startswith("session=") or parts[1] != "data.parquet":
            raise EvidenceContractError("订单生命周期分区路径无效")
        label = parts[0].removeprefix("session=")
        if label in labels:
            raise EvidenceContractError("订单生命周期交易会话分区重复")
        labels.add(label)
        if label != "empty":
            session = date_value(label, "订单生命周期分区日期")
            if session.isoformat() != label:
                raise EvidenceContractError("订单生命周期分区日期无效")
        path = snapshot.directory / item.relative_path
        parquet = pq.ParquetFile(path)
        if parquet.schema_arrow != LIFECYCLE_SCHEMA:
            raise EvidenceContractError("订单生命周期物理 schema 无效")
        for batch in parquet.iter_batches(batch_size=FINANCIAL_ORACLE_BATCH_SIZE, columns=["trading_session"]):
            if any(value is None or value.isoformat() != label for value in batch.column(0).to_pylist()):
                raise EvidenceContractError("订单生命周期交易会话与分区不一致")
        rows += parquet.metadata.num_rows
        for index in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(index)
            size += sum(group.column(column).total_uncompressed_size for column in range(group.num_columns))
        paths.append(path)
    if "empty" in labels and len(labels) != 1:
        raise EvidenceContractError("订单生命周期空分区不能混用交易会话分区")

    def batches():
        for path in paths:
            yield from pq.ParquetFile(path).iter_batches(batch_size=FINANCIAL_ORACLE_BATCH_SIZE)

    return ResultTableSource(LIFECYCLE_SCHEMA, rows, size, batches, tuple(paths))


def verify_order_lifecycle(
    *, lifecycle: Sequence[dict[str, object]],
    canonical: Mapping[str, Sequence[dict[str, object]]],
    frequency: str,
    explicit_order_context: Mapping[str, object] | None = None,
    session_policy_bundle=None,
) -> None:
    """按订单流式合并，复核转换、成交增量和声明会话结束时点。"""

    if frequency not in {"minute", "daily"}:
        raise EvidenceContractError("生命周期正式执行频率无效")
    expected_tif = "IOC" if frequency == "minute" else "DAY"
    explicit_commands = None
    explicit_closes = None
    daily_explicit_futures = explicit_order_context is not None and str(explicit_order_context.get("contract_version", "")).startswith("research-explicit-futures-daily-")
    if explicit_order_context is not None:
        from .explicit_orders import explicit_order_session_ends
        if daily_explicit_futures:
            explicit_order_context, explicit_closes = _daily_futures_lifecycle_context(explicit_order_context)
        explicit_commands = {item["order_id"]: item for item in explicit_order_context["commands"]
                             if item["action"] == "submit"}
        if not daily_explicit_futures:
            explicit_closes = explicit_order_session_ends(explicit_order_context,
                session_policy_bundle=session_policy_bundle, frequency=frequency)

    def order_key(row):
        return key(row, "portfolio_id", "order_id")
    lifecycle_groups = iter(groupby(ordered_rows(
        lifecycle, order_by=("portfolio_id", "order_id", "sequence"),
    ), order_key))
    fill_groups = iter(groupby(ordered_rows(
        canonical["fills"], order_by=("portfolio_id", "order_id", "fill_time", "fill_id"),
    ), order_key))
    lifecycle_group = next(lifecycle_groups, None)
    fill_group = next(fill_groups, None)
    orders = canonical["orders"]
    valuations = canonical["valuations"]
    close_times = {}
    if isinstance(orders, OracleTable) and isinstance(valuations, OracleTable):
        order_rows = orders.workspace.iter_query(f"""
            SELECT o.*, c.lifecycle_close_time, c.lifecycle_last_close_time
            FROM {orders.name} o
            LEFT JOIN (
                SELECT portfolio_id, session,
                       MIN(valuation_time) AS lifecycle_close_time,
                       MAX(valuation_time) AS lifecycle_last_close_time
                FROM {valuations.name}
                GROUP BY portfolio_id, session
            ) c ON o.portfolio_id = c.portfolio_id AND o.session = c.session
            ORDER BY o.portfolio_id, o.order_id
        """)
    else:
        order_rows = ordered_rows(orders, order_by=("portfolio_id", "order_id"))
        for row in valuations:
            close_key = (str(row["portfolio_id"]), date_value(row["session"], "估值会话"))
            at = aware_datetime(row["valuation_time"], "会话结束时点")
            if close_key in close_times and close_times[close_key] != at:
                raise EvidenceContractError("同一会话存在不同结束时点")
            close_times[close_key] = at
    transitions = {
        "created": {"submitted"},
        "submitted": {"accepted", "rejected", "cancelled", "expired"},
        "accepted": {"filled", "partially_filled", "rejected", "cancelled", "expired"},
        "partially_filled": {"filled", "partially_filled", "cancelled", "expired"},
    }
    terminal_rows = {}
    for order in order_rows:
        identity = order_key(order)
        if lifecycle_group is None or lifecycle_group[0] != identity:
            raise EvidenceContractError("订单生命周期缺失或包含未知订单")
        fills = []
        if fill_group is not None and fill_group[0] < identity:
            raise EvidenceContractError("生命周期成交包含未知订单")
        if fill_group is not None and fill_group[0] == identity:
            fills = list(fill_group[1])
            fill_group = next(fill_groups, None)
        expected_fills = Counter((aware_datetime(row["fill_time"], "成交时点"),
                                  integer(row["quantity"], "成交数量", minimum=1)) for row in fills)
        session = date_value(order["session"], "订单会话")
        if isinstance(orders, OracleTable) and isinstance(valuations, OracleTable):
            closing = order["lifecycle_close_time"]
            if closing != order["lifecycle_last_close_time"]:
                raise EvidenceContractError("同一会话存在不同结束时点")
        else:
            closing = close_times.get((str(order["portfolio_id"]), session))
        command = None if explicit_commands is None else explicit_commands.get(str(order["order_id"]))
        if explicit_commands is not None:
            if command is None and not (daily_explicit_futures and fills
                    and all(row["position_effect"] in {"close", "close_today", "close_yesterday"}
                            and aware_datetime(row["fill_time"], "结算强平成交时点") == explicit_closes.get(session.isoformat()) for row in fills)
                    and aware_datetime(order["submitted_at"], "结算强平提交时点") == explicit_closes.get(session.isoformat())):
                raise EvidenceContractError("显式生命周期缺少原始提交命令")
            closing = explicit_closes.get(session.isoformat())
        if closing is None:
            raise EvidenceContractError("生命周期订单缺少声明会话结束时点")
        submitted = aware_datetime(order["submitted_at"], "订单提交时点")
        requested = integer(order["requested_quantity"], "订单数量", minimum=1)
        previous_state, previous_quantity, previous_time = "created", 0, submitted
        tif = order_type = None
        observed_fills = Counter()
        last_execution_time = None
        sequence = 0
        last = None
        lifecycle_rows = list(lifecycle_group[1])
        terminal_rows[identity] = lifecycle_rows
        for sequence, row in enumerate(lifecycle_rows, start=1):
            if set(row) != set(LIFECYCLE_SCHEMA.names) or any(
                row[field.name] is None for field in LIFECYCLE_SCHEMA if not field.nullable
            ):
                raise EvidenceContractError("订单生命周期字段缺失或空值")
            if integer(row["sequence"], "生命周期序号", minimum=1) != sequence:
                raise EvidenceContractError("订单生命周期序号缺失或重复")
            if date_value(row["trading_session"], "生命周期会话") != session:
                raise EvidenceContractError("订单生命周期与订单交易会话不一致")
            required_tif = expected_tif if command is None else command["time_in_force"]
            required_type = "market" if command is None else command["order_type"]
            if row["time_in_force"] != required_tif or row["order_type"] != required_type:
                raise EvidenceContractError("订单生命周期订单类型或 TIF 无效")
            if sequence == 1:
                tif, order_type = row["time_in_force"], row["order_type"]
            if (row["time_in_force"], row["order_type"]) != (tif, order_type):
                raise EvidenceContractError("订单生命周期订单属性发生变化")
            at = aware_datetime(row["event_time"], "生命周期事件时点")
            if at < previous_time or at > closing or (sequence == 1 and at != submitted):
                raise EvidenceContractError("订单生命周期事件时点无效")
            destination = row["to_state"]
            if row["from_state"] != previous_state or destination not in transitions.get(previous_state, set()):
                raise EvidenceContractError("订单生命周期转换非法或终态再次转换")
            quantity = integer(row["cumulative_filled_quantity"], "生命周期累计成交", minimum=0)
            remaining = integer(row["remaining_quantity"], "生命周期剩余数量", minimum=0)
            if quantity + remaining != requested or quantity < previous_quantity:
                raise EvidenceContractError("订单生命周期累计数量或剩余数量不一致")
            delta = quantity - previous_quantity
            if destination in {"filled", "partially_filled"}:
                if delta <= 0 or (destination == "filled") != (remaining == 0):
                    raise EvidenceContractError("订单生命周期成交转换与数量不一致")
                observed_fills[(at, delta)] += 1
                last_execution_time = at
            elif delta:
                raise EvidenceContractError("订单生命周期非成交转换改变累计数量")
            if destination in {"rejected", "cancelled", "expired"} and not row["reason"]:
                raise EvidenceContractError("订单生命周期结束原因缺失")
            if (command is not None and previous_state == "accepted"
                    and destination == "rejected"
                    and row["reason"] not in {"insufficient_cash", "insufficient_margin", "insufficient_free_equity"}):
                raise EvidenceContractError("显式订单 accepted→rejected 原因无效")
            if destination == "expired" and (tif != "DAY" or at != closing):
                raise EvidenceContractError("DAY 生命周期未在声明会话结束时点到期")
            if destination == "cancelled" and tif == "IOC":
                if row["reason"] == "invalid_lot":
                    if command is None or quantity != 0:
                        raise EvidenceContractError("IOC 非法数量原因只能用于显式零成交订单")
                    matching = [item for item in explicit_order_context["observations"]
                                if item["instrument"] == command["instrument"]
                                and aware_datetime(item["event_time"], "数量检查时点") == at]
                    if not matching:
                        raise EvidenceContractError("IOC 非法数量原因缺少对应执行观察")
                # 数量原因由显式订单oracle按当时余额复算，生命周期绑定规范结果。
                if ((row["reason"] == "invalid_lot" or order.get("terminal_reason") == "invalid_lot")
                        and row["reason"] != order.get("terminal_reason")):
                    raise EvidenceContractError("IOC 非法数量原因与规范订单终态不符")
                expected_at = last_execution_time or previous_time
                if command is not None:
                    eligible = [aware_datetime(item["event_time"], "显式执行时点")
                                for item in explicit_order_context["observations"]
                                if item["instrument"] == command["instrument"]
                                and date_value(item["session"], "显式观察会话") == session
                                and aware_datetime(item["event_start"], "显式执行开始") >= submitted
                                and aware_datetime(item["event_time"], "显式执行时点") > submitted]
                    explicit_cancels = [aware_datetime(item["submitted_at"], "撤单时点")
                                        for item in explicit_order_context["commands"]
                                        if item["action"] == "cancel" and item["order_id"] == command["order_id"]]
                    expected_at = min([closing, *eligible, *explicit_cancels])
                if remaining <= 0 or at != expected_at or (command is None and row["reason"] != "ioc_remainder_cancelled"):
                    raise EvidenceContractError("IOC 余量未在执行事件内取消")
            previous_state, previous_quantity, previous_time = destination, quantity, at
            last = row
        if last is None or previous_state not in {"filled", "rejected", "cancelled", "expired"}:
            raise EvidenceContractError("IOC 未终结或 DAY 会话结束后仍活动")
        if observed_fills != expected_fills or previous_quantity != integer(order["filled_quantity"], "订单成交摘要", minimum=0):
            raise EvidenceContractError("订单生命周期累计成交或成交时点与 fills 不一致")
        summary = "filled" if previous_quantity == requested else "partially_filled" if previous_quantity else "rejected"
        if order["status"] != summary:
            raise EvidenceContractError("订单生命周期与成交结果摘要不一致")
        if command is not None and previous_state == "cancelled":
            explicit_cancels = [aware_datetime(item["submitted_at"], "撤单时点")
                                for item in explicit_order_context["commands"]
                                if item["action"] == "cancel" and item["order_id"] == command["order_id"]]
            automatic = {"remaining_reservation_unaffordable", "insufficient_cash", "insufficient_margin"}
            if daily_explicit_futures:
                automatic.update({"insufficient_remainder_equity", "funds_resized", "insufficient_free_equity"})
            if tif == "DAY" and last["reason"] in automatic:
                matching = [item for item in explicit_order_context["observations"]
                            if item["instrument"] == command["instrument"]
                            and aware_datetime(item["event_time"], "资金检查时点") == previous_time]
                if not matching or (last["reason"] in {"insufficient_cash", "insufficient_margin"}
                                    and command["funds_policy"] != "reject"):
                    raise EvidenceContractError("资金不足余量撤销缺少执行观察或声明策略")
            if tif == "DAY" and last["reason"] not in automatic:
                if not explicit_cancels or previous_time != min(explicit_cancels):
                    raise EvidenceContractError("DAY 撤单缺少原始命令或时点不一致")
        lifecycle_group = next(lifecycle_groups, None)
    if lifecycle_group is not None or fill_group is not None:
        raise EvidenceContractError("订单生命周期或成交包含未知订单")
    if explicit_order_context is not None:
        _verify_lifecycle_cancel_results(
            explicit_order_context, terminal_rows, explicit_commands,
        )


def _verify_lifecycle_cancel_results(context, terminal_rows, submit_commands):
    """用生命周期实际终态核对显式撤单结果，不信订单状态自报。"""
    requests = [row for row in context.get("commands", ())
                if isinstance(row, Mapping) and row.get("action") == "cancel"]
    results = {}
    for row in context.get("cancel_results", ()):
        if not isinstance(row, Mapping) or set(row) != {
            "command_id", "order_id", "status", "reason",
        } or row["command_id"] in results:
            raise EvidenceContractError("生命周期撤单结果 schema 无效或重复")
        results[row["command_id"]] = dict(row)
    if set(results) != {row.get("command_id") for row in requests}:
        raise EvidenceContractError("生命周期撤单结果与撤单命令集合不一致")
    for request in requests:
        command_id = request["command_id"]
        identity = (str(request.get("portfolio_id", "default")), str(request["order_id"]))
        rows = terminal_rows.get(identity)
        if not rows:
            raise EvidenceContractError("撤单命令没有对应生命周期")
        at = aware_datetime(request["submitted_at"], "生命周期撤单时点")
        explicit_cancel = [
            row for row in rows
            if row["to_state"] == "cancelled"
            and row.get("reason") in {"user_cancel", "explicit_cancel"}
            and aware_datetime(row["event_time"], "生命周期撤单终态") == at
        ]
        if explicit_cancel:
            expected = {
                "command_id": command_id,
                "order_id": str(request["order_id"]),
                "status": "cancelled",
                "reason": "explicit_cancel",
            }
            if results[command_id] != expected:
                raise EvidenceContractError("生命周期有效撤单结果与真实终态不一致")
            continue
        observed = None
        for row in rows:
            event_at = aware_datetime(row["event_time"], "生命周期终态时点")
            if event_at <= at:
                observed = row
            else:
                break
        if observed is None:
            raise EvidenceContractError("撤单命令早于订单生命周期")
        if observed["to_state"] in {"accepted", "partially_filled"}:
            raise EvidenceContractError("活动订单缺少有效撤单生命周期终态")
        expected = {
            "command_id": command_id,
            "order_id": str(request["order_id"]),
            "status": "rejected",
            "reason": "order_not_active",
        }
        if results[command_id] != expected:
            raise EvidenceContractError("生命周期终态后撤单结果不一致")



def _daily_futures_lifecycle_context(context):
    """日频期货以同一会话终点发布订单事实，组合保留独立账户输入。"""
    if context.get("contract_version") == "research-explicit-futures-daily-portfolio-v1":
        if context.get("account_model") != "independent_product_accounts":
            raise EvidenceContractError("显式期货组合账户模型无效")
        accounts = context["products"].values()
    else:
        accounts = (context,)
    normalized = {"commands": [], "observations": [], "cancel_results": []}
    endings = {}
    for account in accounts:
        if account.get("contract_version") != "research-explicit-futures-daily-execution-v1":
            raise EvidenceContractError("显式日频期货上下文版本无效")
        normalized["commands"].extend(account["commands"])
        normalized["observations"].extend({**row, "session": row["trading_date"]} for row in account["observations"])
        for row in account["session_ends"]:
            session = date_value(row["trading_date"], "日频期货会话")
            closing = datetime.combine(session, time(17), ZoneInfo("Asia/Shanghai"))
            if aware_datetime(row["at"], "日频期货收尾") != closing:
                raise EvidenceContractError("日频期货 DAY 到期偏离声明的17点结算")
            endings[session.isoformat()] = closing
        by_id = {row["command_id"]: row for row in account["commands"] if row["action"] == "cancel"}
        for row in account["cancel_results"]:
            if row["reason"] not in {"cancelled", "already_terminal"}:
                raise EvidenceContractError("日频期货撤单结果无效")
            command = by_id.get(row["command_id"])
            if command is None or aware_datetime(row["at"], "撤单结果时点") != aware_datetime(command["submitted_at"], "撤单提交时点"):
                raise EvidenceContractError("日频期货撤单结果与命令时点不一致")
            cancelled = row["reason"] == "cancelled"
            normalized["cancel_results"].append({"command_id": row["command_id"], "order_id": row["order_id"],
                "status": "cancelled" if cancelled else "rejected", "reason": "explicit_cancel" if cancelled else "order_not_active"})
    if len({row["command_id"] for row in normalized["commands"]}) != len(normalized["commands"]):
        raise EvidenceContractError("日频期货组合命令身份重复")
    return normalized, endings
