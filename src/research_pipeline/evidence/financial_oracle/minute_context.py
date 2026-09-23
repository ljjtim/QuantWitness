"""分钟金融上下文、直接事实和可见性关联独立复核。"""

from __future__ import annotations

from datetime import datetime
import json
from typing import Mapping, Sequence

import pyarrow as pa

from research_pipeline.domain.minute_rule_snapshots import MinuteRuleSnapshotBundle
from research_pipeline.domain.session_calendar import SessionPolicyBundle
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.minute_operator_contracts import (
    MINUTE_TARGET_PAYLOAD_SCHEMA_ID,
)
from research_pipeline.results import MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS

from ..errors import EvidenceContractError
from ..oracle_workspace import OracleTable, ResultTableSource, arrow_table_source
from .bar_tca import minute_tca_referenced_inputs
from .common import (
    context_table_rows as _context_table_rows,
    normalized as _normalized,
    read_source_rows as _read_source_rows,
    require_no_rows as _require_no_external_rows,
)
from .minute_rules import (
    MinuteIndexedRows,
    MinuteSettlementRows,
    minute_decode_settlement,
    minute_index,
    minute_single_bar,
    verify_minute_execution_rules,
)


def verify_minute_financial_context(
    *,
    context: Mapping[str, object],
    canonical: Mapping[str, Sequence[dict[str, object]]],
    simulation_manifest: Mapping[str, object],
    oracle_input: Mapping[str, object],
    target_table: pa.Table | ResultTableSource | None = None,
    adjustment_snapshot_table: pa.Table | ResultTableSource | None = None,
    context_tables: Mapping[
        str,
        pa.Table | ResultTableSource | Sequence[dict[str, object]],
    ] | None = None,
) -> None:
    """复核分钟 Result 随附的直接输入，不回读 run-root 或数据库。"""

    expected = {
        "contract_version", "asset_class", "price_scale", "rule_bundle",
        "target_artifact", "context_tables", "price_limit_references",
        "session_policy_bundle",
        "source_simulation_hash", "simulation_result_hash",
        "source_ledger_hash", "context_hash",
    }
    if set(context) != expected or context.get("contract_version") != (
        "research-minute-financial-context-v3"
    ):
        raise EvidenceContractError("分钟金融上下文 schema 无效")
    unsigned = {key: value for key, value in context.items() if key != "context_hash"}
    if typed_canonical_hash(unsigned) != context.get("context_hash"):
        raise EvidenceContractError("分钟金融上下文身份不一致")
    try:
        rule_bundle = MinuteRuleSnapshotBundle.from_dict(context["rule_bundle"])
    except Exception as exc:
        raise EvidenceContractError("分钟金融上下文规则 bundle 无效") from exc
    try:
        session_bundle = SessionPolicyBundle.from_dict(
            context["session_policy_bundle"]
        )
    except Exception as exc:
        raise EvidenceContractError("分钟金融上下文 session policy bundle 无效") from exc
    session_sources = [
        item for item in rule_bundle.sources
        if item.source_kind == "repository_snapshot"
    ]
    if (
        len(session_sources) != 1
        or not session_sources[0].locator.endswith(f"#{session_bundle.bundle_hash}")
    ):
        raise EvidenceContractError("分钟规则未绑定 ResultStore session policy bundle")
    policy = oracle_input.get("policy")
    if not isinstance(policy, Mapping) or (
        rule_bundle.bundle_hash != policy.get("rule_snapshot_hash")
    ):
        raise EvidenceContractError("分钟规则 bundle 未绑定 TCA policy")
    if (
        context.get("simulation_result_hash") != simulation_manifest.get("result_hash")
        or context.get("source_simulation_hash")
        != simulation_manifest.get("source_simulation_hash")
        or context.get("source_ledger_hash") != oracle_input.get("source_ledger_hash")
    ):
        raise EvidenceContractError("分钟金融上下文未绑定正式仿真或账本")
    if type(context.get("price_scale")) is not int or any(
        str(row["asset_class"]) != str(context.get("asset_class"))
        or int(row["price_scale"]) != int(context["price_scale"])
        for row in canonical["fills"]
    ):
        raise EvidenceContractError("分钟金融上下文资产类别或价格精度不一致")

    target_artifact = context.get("target_artifact")
    if not isinstance(target_artifact, Mapping):
        raise EvidenceContractError("分钟金融上下文缺少 target 工件")
    required_target_metadata = {
        "schema_id", "manifest", "source_dataset_reference_id",
        "partition_count", "row_count", "partitions",
    }
    if (
        not required_target_metadata <= set(target_artifact)
        or target_artifact.get("schema_id") != MINUTE_TARGET_PAYLOAD_SCHEMA_ID
        or not isinstance(target_artifact.get("manifest"), Mapping)
        or not isinstance(target_artifact.get("source_dataset_reference_id"), str)
        or type(target_artifact.get("partition_count")) is not int
        or type(target_artifact.get("row_count")) is not int
        or not isinstance(target_artifact.get("partitions"), list)
    ):
        raise EvidenceContractError("分钟 target 表身份无效")
    partitions = target_artifact["partitions"]
    if (
        int(target_artifact["partition_count"]) != len(partitions)
        or not partitions
        or any(
            not isinstance(item, Mapping)
            or set(item) != {
                "partition_key", "row_count", "source_partition_identity"
            }
            or type(item.get("row_count")) is not int
            or int(item["row_count"]) < 0
            for item in partitions
        )
        or sum(int(item["row_count"]) for item in partitions)
        != int(target_artifact["row_count"])
    ):
        raise EvidenceContractError("分钟 target 分区 manifest 无效")
    target_source = (
        None if target_table is None else arrow_table_source(target_table)
    )
    if (
        target_source is None
        or target_source.row_count != int(target_artifact["row_count"])
    ):
        raise EvidenceContractError("分钟金融上下文缺少正式 target 表")
    required_target_columns = {
        "instrument", "bar_end", "available_time", "decision_time",
        "source_snapshot_hash", "signal", "target_quantity", "target_hash",
        "portfolio_target_json",
    }
    if set(target_source.schema.names) != required_target_columns:
        raise EvidenceContractError("分钟 target 表 schema 无效")
    target_instrument_ids: set[str] = set()
    target_row_count = 0
    for batch in target_source.iter_batches():
        target_row_count += batch.num_rows
        for row in batch.to_pylist():
            try:
                target_payload = json.loads(str(row["portfolio_target_json"]))
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise EvidenceContractError("分钟 target 完整载荷无法读取") from exc
            if not isinstance(target_payload, Mapping):
                raise EvidenceContractError("分钟 target 完整载荷 schema 无效")
            if typed_canonical_hash(dict(target_payload)) != row.get("target_hash"):
                raise EvidenceContractError("分钟 target 完整载荷与身份不一致")
            entries = target_payload.get("entries")
            if (
                not isinstance(entries, list)
                or len(entries) != 1
                or not isinstance(entries[0], Mapping)
                or not isinstance(entries[0].get("instrument"), Mapping)
            ):
                raise EvidenceContractError("分钟 target 必须包含唯一标的完整载荷")
            instrument = entries[0]["instrument"]
            instrument_id = instrument.get("instrument_id")
            if (
                not isinstance(instrument_id, str)
                or not instrument_id
                or instrument.get("asset_class") != context.get("asset_class")
                or row.get("instrument") != instrument_id
            ):
                raise EvidenceContractError("分钟 target 标的与金融上下文不一致")
            target_instrument_ids.add(instrument_id)
    if target_row_count != target_source.row_count:
        raise EvidenceContractError("分钟 target 表行数与 Result manifest 不一致")
    if context.get("asset_class") in {"cn_stock", "cn_etf"}:
        required_pit = {
            "adjustment_snapshot_identity_hash", "adjustment_candidates",
            "adjustment_effective_times", "adjustment_previous_closes",
            "adjustment_snapshot", "source_bars_snapshot_hash",
        }
        if not required_pit <= set(target_artifact):
            raise EvidenceContractError("分钟现货 target 缺少 PIT 复权或公司行动载荷")
        snapshot_payload = target_artifact["adjustment_snapshot"]
        if (
            not isinstance(snapshot_payload, Mapping)
            or typed_canonical_hash(dict(snapshot_payload))
            != target_artifact.get("adjustment_snapshot_identity_hash")
        ):
            raise EvidenceContractError("分钟 PIT 复权快照身份不一致")
        if not isinstance(target_artifact["adjustment_candidates"], list):
            raise EvidenceContractError("分钟公司行动候选载荷无效")
        adjustment_source = (
            None
            if adjustment_snapshot_table is None
            else arrow_table_source(adjustment_snapshot_table)
        )
        if adjustment_source is None or adjustment_source.row_count != 1:
            raise EvidenceContractError("分钟现货金融上下文缺少 ResultStore PIT 快照表")
        adjustment_rows = _read_source_rows(
            adjustment_source,
            expected_columns=None,
            label="分钟 ResultStore PIT 快照",
        )
        stored = adjustment_rows[0]
        try:
            snapshot_from_table = json.loads(str(stored["snapshot_json"]))
            candidates_from_table = json.loads(str(stored["candidate_actions_json"]))
            effective_from_table = json.loads(str(stored["effective_times_json"]))
            closes_from_table = json.loads(str(stored["previous_closes_json"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise EvidenceContractError("分钟 ResultStore PIT 快照载荷无法读取") from exc
        if any(
            stored_value != context_value
            for stored_value, context_value in (
                (snapshot_from_table, snapshot_payload),
                (candidates_from_table, target_artifact["adjustment_candidates"]),
                (effective_from_table, target_artifact["adjustment_effective_times"]),
                (closes_from_table, target_artifact["adjustment_previous_closes"]),
            )
        ):
            raise EvidenceContractError("分钟金融上下文与 ResultStore PIT 快照不一致")

    declared_context_tables = context.get("context_tables")
    if (
        not isinstance(declared_context_tables, Mapping)
        or set(declared_context_tables) != set(MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS)
        or context_tables is None
        or set(context_tables) != set(MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS)
    ):
        raise EvidenceContractError("分钟列式金融上下文引用无效")
    context_rows: dict[str, Sequence[dict[str, object]]] = {}
    for name, schema_id in MINUTE_FINANCIAL_CONTEXT_SCHEMA_IDS.items():
        metadata = declared_context_tables.get(name)
        table_rows = _context_table_rows(
            context_tables[name],
            expected_columns=None,
            label=f"分钟列式金融上下文 {name}",
        )
        if (
            not isinstance(metadata, Mapping)
            or set(metadata) != {
                "schema_id", "path_prefix", "row_count", "partition_count"
            }
            or metadata.get("schema_id") != schema_id
            or type(metadata.get("row_count")) is not int
            or int(metadata["row_count"]) != len(table_rows)
            or type(metadata.get("partition_count")) is not int
            or int(metadata["partition_count"]) <= 0
        ):
            raise EvidenceContractError("分钟列式金融上下文 manifest 无效")
        context_rows[name] = table_rows
    settlement_source = context_rows["settlement_events"]
    settlement_rows = (
        MinuteSettlementRows(settlement_source)
        if isinstance(settlement_source, OracleTable)
        else [minute_decode_settlement(row) for row in settlement_source]
    )

    bars = context_rows["execution_bars"]
    if not bars:
        raise EvidenceContractError("分钟金融上下文缺少执行 bar")
    bar_by_hash = minute_index(bars, ("bar_hash",), "分钟执行 bar")
    if len(bar_by_hash) != len(bars):
        raise EvidenceContractError("分钟执行 bar 身份重复")
    for row in bars:
        try:
            bar_end = datetime.fromisoformat(str(row["bar_end"]))
            available = datetime.fromisoformat(str(row["available_time"]))
        except (KeyError, ValueError) as exc:
            raise EvidenceContractError("分钟执行 bar 时间无效") from exc
        if (
            type(row.get("completed")) is not bool
            or not isinstance(row.get("quality_status"), str)
            or bar_end > available
        ):
            raise EvidenceContractError("分钟执行 bar 完成状态、质量或时间无效")
        identity_payload = {
            key: _normalized(value)
            for key, value in row.items()
            if key != "bar_hash"
        }
        if typed_canonical_hash(identity_payload) != row.get("bar_hash"):
            raise EvidenceContractError("分钟执行 bar 身份与列式内容不一致")

    benchmark_rows = context_rows["decision_benchmarks"]
    observation_rows = context_rows["execution_observations"]
    benchmark_by_order = minute_index(
        benchmark_rows, ("portfolio_id", "order_id"), "分钟决策基准"
    )
    if len(benchmark_by_order) != len(benchmark_rows):
        raise EvidenceContractError("分钟决策基准重复或 schema 无效")
    canonical_orders = minute_index(canonical["orders"], ("order_id",), "分钟订单")
    if isinstance(benchmark_by_order, MinuteIndexedRows):
        _require_no_external_rows(
            benchmark_rows.workspace,
            f"SELECT portfolio_id, order_id FROM {benchmark_rows.name} EXCEPT "
            f"SELECT portfolio_id, order_id FROM {canonical['orders'].name}",
            "分钟决策基准与正式 orders 集合不一致",
        )
        if len(benchmark_by_order) != len(canonical_orders):
            raise EvidenceContractError("分钟决策基准与正式 orders 集合不一致")
    elif set(benchmark_by_order) != {
        (str(row["portfolio_id"]), str(row["order_id"])) for row in canonical["orders"]
    }:
        raise EvidenceContractError("分钟决策基准与正式 orders 集合不一致")
    if oracle_input.get("contract_version") == (
        "research-bar-tca-table-references-v1"
    ):
        if isinstance(canonical["orders"], OracleTable) and isinstance(
            canonical["fills"], OracleTable
        ):
            workspace = canonical["orders"].workspace
            formal_orders = OracleTable(
                workspace,
                "formal_orders",
                len(canonical["orders"]),
            )
            formal_fills = OracleTable(
                workspace,
                "formal_fills",
                len(canonical["fills"]),
            )
        else:
            formal_orders, formal_fills, _input_hash, _fill_hash = (
                minute_tca_referenced_inputs(
                    canonical=canonical,
                    context_tables=context_tables,
                    source_ledger_hash=str(
                        oracle_input.get("source_ledger_hash")
                    ),
                    oracle_input=oracle_input,
                )
            )
    else:
        formal_orders = oracle_input.get("orders")
        formal_fills = oracle_input.get("formal_fills")
    if not isinstance(formal_orders, Sequence):
        raise EvidenceContractError("分钟 TCA oracle orders 无效")
    for order in formal_orders:
        if not isinstance(order, Mapping):
            raise EvidenceContractError("分钟 TCA oracle order schema 无效")
        benchmark = benchmark_by_order.get((
            str(canonical_orders[str(order.get("order_id"))]["portfolio_id"]),
            str(order.get("order_id")),
        ))
        if benchmark is None or any(
            _normalized(benchmark.get(context_field))
            != _normalized(order.get(oracle_field))
            for context_field, oracle_field in (
                ("decision_price_units", "decision_price_units"),
                ("available_at", "decision_price_available_at"),
                ("source_hash", "decision_price_source_hash"),
            )
        ):
            raise EvidenceContractError("分钟决策基准与 TCA oracle 不一致")
        benchmark_bar = bar_by_hash.get(str(benchmark.get("source_hash")))
        if (
            benchmark_bar is None
            or int(benchmark_bar.get("close_units", -1))
            != int(benchmark.get("decision_price_units", -2))
            or _normalized(benchmark_bar.get("available_time"))
            != _normalized(benchmark.get("available_at"))
            or not bool(benchmark_bar.get("completed"))
            or benchmark_bar.get("quality_status") != "pass"
        ):
            raise EvidenceContractError("分钟决策基准无法回指已完成执行 bar")

    observation_by_fill = minute_index(
        observation_rows, ("source_fill_id",), "分钟成交观察"
    )
    if len(observation_by_fill) != len(observation_rows):
        raise EvidenceContractError("分钟成交观察重复或 schema 无效")
    if not isinstance(formal_fills, Sequence):
        raise EvidenceContractError("分钟成交观察与正式 fills 集合不一致")
    if isinstance(formal_fills, OracleTable) and isinstance(
        observation_by_fill, MinuteIndexedRows
    ):
        _require_no_external_rows(
            formal_fills.workspace,
            f"SELECT source_fill_id FROM {observation_rows.name} EXCEPT "
            f"SELECT source_fill_id FROM {formal_fills.name}",
            "分钟成交观察与正式 fills 集合不一致",
        )
        if len(observation_by_fill) != len(formal_fills):
            raise EvidenceContractError("分钟成交观察与正式 fills 集合不一致")
    elif set(observation_by_fill) != {str(row.get("source_fill_id")) for row in formal_fills}:
        raise EvidenceContractError("分钟成交观察与正式 fills 集合不一致")
    for fill in formal_fills:
        if not isinstance(fill, Mapping):
            raise EvidenceContractError("分钟 TCA oracle fill schema 无效")
        observation = observation_by_fill[str(fill["source_fill_id"])]
        if any(
            _normalized(observation.get(field)) != _normalized(fill.get(field))
            for field in (
                "arrival_price_units", "arrival_price_available_at",
                "visible_capacity", "capacity_available_at",
            )
        ):
            raise EvidenceContractError("分钟成交观察与 TCA oracle 不一致")
        execution_bar = minute_single_bar(bars, {
            "instrument_id": str(fill.get("instrument_id")),
            "available_time": _normalized(fill.get("fill_time")),
        }, "分钟成交观察无法回指唯一已完成执行 bar")
        if (
            int(execution_bar.get("open_units", -1))
            != int(observation.get("arrival_price_units", -2))
            or int(execution_bar.get("volume", -1))
            != int(observation.get("visible_capacity", -2))
        ):
            raise EvidenceContractError("分钟成交观察无法回指唯一已完成执行 bar")

    price_references = context.get("price_limit_references")
    if not isinstance(price_references, list) or not price_references:
        raise EvidenceContractError("分钟金融上下文缺少价格限制参考")
    if any(
        not isinstance(row, Mapping)
        or row.get("status") != "supported"
        or not isinstance(row.get("parameters"), Mapping)
        or row["parameters"].get("price_scale") != context.get("price_scale")
        for row in price_references
    ):
        raise EvidenceContractError("分钟价格限制参考未闭合")
    bundled_rules = {
        typed_canonical_hash(item.to_dict())
        for item in rule_bundle.rules
    }
    declared_price_rules = {
        typed_canonical_hash(dict(row)) for row in price_references
    }
    expected_price_rules = {
        item.snapshot_hash
        for item in rule_bundle.rules
        if item.instrument_id in target_instrument_ids
        and item.rule_id in {
            "rule.cn_stock.price_limit.v1",
            "rule.cn_fund.price_limit.v1",
            "rule.cn_futures.price_limit.v1",
        }
    }
    if (
        not declared_price_rules <= bundled_rules
        or declared_price_rules != expected_price_rules
    ):
        raise EvidenceContractError("分钟价格限制参考不属于已封存规则 bundle")
    verify_minute_execution_rules(
        asset_class=str(context["asset_class"]),
        canonical=canonical,
        bars=bars,
        rule_bundle=rule_bundle,
        policy=policy,
        settlement_events=settlement_rows,
        session_bundle=session_bundle,
    )


__all__ = ["verify_minute_financial_context"]
