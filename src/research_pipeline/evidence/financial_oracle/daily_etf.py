"""ETF 日频规则、费用、成交和结算桶独立复核。"""

from __future__ import annotations

from datetime import date
from typing import Mapping

import pyarrow as pa

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.market_rule_defaults import (
    CnEtfDailyMarketRuleProfile,
    require_cn_etf_daily_market_rule_profile_payload,
)
from research_pipeline.results import CANONICAL_SIMULATION_SCHEMA_IDS

from ..errors import EvidenceContractError
from ..oracle_workspace import OracleTable
from .common import (
    aware_datetime as _aware_datetime,
    ceil_ratio as _ceil_ratio,
    create_mapping_table as _mapping_table,
    date_value as _date_value,
    integer as _integer,
    ordered_rows as _ordered_rows,
    require_no_rows as _require_no_external_rows,
)


def verify_daily_etf_financial_context(
    *,
    context: Mapping[str, object],
    canonical: Mapping[str, list[dict[str, object]]],
    simulation_manifest: Mapping[str, object],
    oracle_input: Mapping[str, object],
) -> None:
    """从 Result 内事实独立复核 ETF 日频受控规则与佣金假设。"""

    expected = {
        "contract_version",
        "market_rule_profile_id",
        "market_rule_profile",
        "market_rule_profile_hash",
        "rule_bundle",
        "rule_bundle_hash",
        "instrument_classification",
        "cost_assumptions",
        "cost_model_hash",
        "source_simulation_hash",
        "simulation_result_hash",
        "source_ledger_hash",
        "context_hash",
    }
    if set(context) != expected or context.get("contract_version") != (
        "research-daily-etf-financial-context-v1"
    ):
        raise EvidenceContractError("ETF 日频金融上下文 schema 无效")
    unsigned = {key: value for key, value in context.items() if key != "context_hash"}
    if typed_canonical_hash(unsigned) != context.get("context_hash"):
        raise EvidenceContractError("ETF 日频金融上下文身份不一致")
    profile_id = context.get("market_rule_profile_id")
    if not isinstance(profile_id, str):
        raise EvidenceContractError("ETF 日频金融上下文缺少 profile ID")
    try:
        profile = require_cn_etf_daily_market_rule_profile_payload(
            profile_id=profile_id,
            payload=context.get("market_rule_profile"),
            profile_hash=context.get("market_rule_profile_hash"),
        )
    except ValueError as exc:
        raise EvidenceContractError(f"ETF 日频受控规则 profile 无效: {exc}") from exc
    if (
        context.get("source_simulation_hash")
        != simulation_manifest.get("source_simulation_hash")
        or context.get("simulation_result_hash")
        != simulation_manifest.get("result_hash")
        or context.get("source_ledger_hash")
        != oracle_input.get("source_ledger_hash")
    ):
        raise EvidenceContractError("ETF 日频金融上下文未绑定正式仿真或账本")

    raw_classification = context.get("instrument_classification")
    if (
        not isinstance(raw_classification, Mapping)
        or set(raw_classification) != {"bond_etf_codes", "equity_etf_codes"}
    ):
        raise EvidenceContractError("ETF 日频品类分类 schema 无效")
    bonds = _sorted_string_list(
        raw_classification["bond_etf_codes"], "bond_etf_codes"
    )
    equities = _sorted_string_list(
        raw_classification["equity_etf_codes"], "equity_etf_codes"
    )
    if set(bonds) & set(equities) or not set(bonds) | set(equities):
        raise EvidenceContractError("ETF 日频债券与股票分类不互斥或为空")
    category_by_code = {
        **{code: "bond" for code in bonds},
        **{code: "equity" for code in equities},
    }

    raw_costs = context.get("cost_assumptions")
    if not isinstance(raw_costs, Mapping) or set(raw_costs) != {
        "assumption_id",
        "commission_ppm",
        "min_commission_units",
        "currency",
        "source_mode",
    }:
        raise EvidenceContractError("ETF 日频佣金假设 schema 无效")
    if (
        raw_costs.get("assumption_id") != "cn.etf.user-commission-assumption.v1"
        or raw_costs.get("currency") != "CNY"
        or raw_costs.get("source_mode") != "user_research_assumption"
    ):
        raise EvidenceContractError("ETF 日频佣金假设冒充市场规则来源")
    commission_ppm = _integer(
        raw_costs.get("commission_ppm"), "commission_ppm", minimum=0
    )
    min_commission_units = _integer(
        raw_costs.get("min_commission_units"),
        "min_commission_units",
        minimum=0,
    )
    expected_cost_hash = typed_canonical_hash({
        "commission_ppm": commission_ppm,
        "min_commission_units": min_commission_units,
        "sell_tax_ppm": profile.sell_tax_ppm,
        "transfer_fee_ppm": profile.transfer_fee_ppm,
    })
    if context.get("cost_model_hash") != expected_cost_hash:
        raise EvidenceContractError("ETF 日频佣金假设 hash 不一致")

    raw_rules = context.get("rule_bundle")
    if not isinstance(raw_rules, Mapping) or set(raw_rules) != set(category_by_code):
        raise EvidenceContractError("ETF 日频规则未精确覆盖声明标的")
    normalized_rules: dict[str, Mapping[str, object]] = {}
    for code, raw_rule in raw_rules.items():
        if not isinstance(raw_rule, Mapping):
            raise EvidenceContractError("ETF 日频规则条目必须是映射")
        _verify_daily_etf_rule_entry(
            code=str(code),
            rule=raw_rule,
            category=category_by_code[str(code)],
            profile=profile,
            commission_ppm=commission_ppm,
            min_commission_units=min_commission_units,
        )
        normalized_rules[str(code)] = raw_rule
    expected_rule_hash = typed_canonical_hash({
        code: dict(rule) for code, rule in sorted(normalized_rules.items())
    })
    if context.get("rule_bundle_hash") != expected_rule_hash:
        raise EvidenceContractError("ETF 日频规则 bundle hash 不一致")
    policy = oracle_input.get("policy")
    if (
        not isinstance(policy, Mapping)
        or policy.get("asset_class") != "cn_etf"
        or policy.get("bar_frequency") != "daily"
        or policy.get("rule_snapshot_hash") != expected_rule_hash
    ):
        raise EvidenceContractError("ETF 日频 TCA 未绑定同一规则 bundle")

    external_tables = {
        name: rows for name, rows in canonical.items()
        if isinstance(rows, OracleTable)
    }
    if set(external_tables) == set(CANONICAL_SIMULATION_SCHEMA_IDS):
        workspace = next(iter(external_tables.values())).workspace
        session_bounds = workspace.execute(" UNION ALL ".join(
            f"SELECT min(session), max(session) FROM {external_tables[name].name}"
            for name in ("orders", "fills", "positions")
        )).fetchall()
        observed_sessions = tuple(
            value
            for row in session_bounds
            for value in row
            if value is not None
        )
    else:
        observed_sessions = tuple(
            _date_value(row["session"], "session")
            for table in (
                canonical["orders"],
                canonical["fills"],
                canonical["positions"],
            )
            for row in table
        )
    if observed_sessions:
        try:
            profile.require_covers(min(observed_sessions), max(observed_sessions))
        except ValueError as exc:
            raise EvidenceContractError(str(exc)) from exc
    if set(external_tables) == set(CANONICAL_SIMULATION_SCHEMA_IDS):
        _verify_external_daily_etf_fills_and_settlement(
            canonical=external_tables,
            category_by_code=category_by_code,
            profile=profile,
            commission_ppm=commission_ppm,
            min_commission_units=min_commission_units,
        )
        return
    _verify_daily_etf_fills_and_settlement(
        canonical=canonical,
        category_by_code=category_by_code,
        profile=profile,
        commission_ppm=commission_ppm,
        min_commission_units=min_commission_units,
    )


def _verify_external_daily_etf_fills_and_settlement(
    *,
    canonical: Mapping[str, OracleTable],
    category_by_code: Mapping[str, str],
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
) -> None:
    """把日频 ETF 跨行规则留在受预算约束的关系扫描中。"""

    workspace = next(iter(canonical.values())).workspace
    _mapping_table(
        workspace,
        name="daily_etf_classification",
        rows=[
            {
                "instrument_id": code,
                "settlement_days": profile.settlement_days_for(category),
            }
            for code, category in sorted(category_by_code.items())
        ],
        schema=pa.schema([
            pa.field("instrument_id", pa.string()),
            pa.field("settlement_days", pa.int64()),
        ]),
    )
    fills = canonical["fills"].name
    positions = canonical["positions"].name
    start = profile.effective_start.isoformat()
    end = profile.effective_end.isoformat()
    available = profile.rule_available_at.isoformat()
    transfer_ppm = profile.transfer_fee_ppm
    sell_tax_ppm = profile.sell_tax_ppm
    lot_size = profile.lot_size
    expected_fee = f"""
        greatest(
          {min_commission_units:d},
          ((CAST(f.notional_units AS HUGEINT) * {commission_ppm:d}
             + 999999) // 1000000)
        )
        + ((CAST(f.notional_units AS HUGEINT) * {transfer_ppm:d}
             + 999999) // 1000000)
        + CASE WHEN f.side = 'sell'
               THEN ((CAST(f.notional_units AS HUGEINT) * {sell_tax_ppm:d}
                      + 999999) // 1000000)
               ELSE 0 END
    """
    _require_no_external_rows(
        workspace,
        f"""
        SELECT 1
        FROM {fills} AS f
        LEFT JOIN daily_etf_classification AS c
          ON f.instrument_id = c.instrument_id
        WHERE c.instrument_id IS NULL
           OR f.session < DATE '{start}' OR f.session > DATE '{end}'
           OR f.fill_time < TIMESTAMPTZ '{available}'
           OR f.quantity IS NULL OR f.quantity < 1
           OR f.quantity % {lot_size:d} != 0
           OR f.notional_units IS NULL OR f.notional_units < 1
           OR f.fee_units IS DISTINCT FROM ({expected_fee})
        LIMIT 1
        """,
        "ETF 日频 fill 的分类、有效期、交易单位或费用不一致",
    )
    _require_no_external_rows(
        workspace,
        f"""
        WITH ordered AS (
          SELECT f.instrument_id, f.session, f.fill_time, f.fill_id,
                 c.settlement_days,
                 sum(CASE WHEN f.side = 'buy' THEN f.quantity ELSE 0 END)
                   OVER history AS bought_through_current,
                 sum(CASE WHEN f.side = 'buy' THEN f.quantity ELSE 0 END)
                   OVER current_session AS bought_in_session_through_current,
                 sum(CASE WHEN f.side = 'sell' THEN f.quantity ELSE 0 END)
                   OVER history AS sold_through_current
          FROM {fills} AS f
          JOIN daily_etf_classification AS c
            ON f.instrument_id = c.instrument_id
          WINDOW history AS (
            PARTITION BY f.instrument_id
            ORDER BY f.session, f.fill_time, f.fill_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ), current_session AS (
            PARTITION BY f.instrument_id, f.session
            ORDER BY f.fill_time, f.fill_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )
        )
        SELECT 1 FROM ordered
        WHERE sold_through_current > CASE
          WHEN settlement_days = 1
            THEN bought_through_current - bought_in_session_through_current
          ELSE bought_through_current
        END
        LIMIT 1
        """,
        "ETF 日频卖出 fill 绕过 T+0/T+1 约束",
    )
    _require_no_external_rows(
        workspace,
        f"""
        WITH bought_today AS (
          SELECT instrument_id, session, sum(quantity) AS quantity
          FROM {fills}
          WHERE side = 'buy'
          GROUP BY instrument_id, session
        )
        SELECT 1
        FROM {positions} AS p
        LEFT JOIN daily_etf_classification AS c
          ON p.instrument_id = c.instrument_id
        LEFT JOIN bought_today AS b
          ON p.instrument_id = b.instrument_id AND p.session = b.session
        WHERE c.instrument_id IS NULL
           OR (
             p.non_trade_quantity_change = 0
             AND p.unsettled_quantity IS DISTINCT FROM CASE
               WHEN c.settlement_days = 0 THEN 0 ELSE coalesce(b.quantity, 0)
             END
           )
           OR (
             p.non_trade_quantity_change != 0
             AND p.unsettled_quantity < coalesce(b.quantity, 0)
           )
        LIMIT 1
        """,
        "ETF 日频持仓 bucket 与 T+0/T+1 规则不一致",
    )


def _verify_daily_etf_rule_entry(
    *,
    code: str,
    rule: Mapping[str, object],
    category: str,
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
) -> None:
    expected_fields = {
        "rule_id",
        "version",
        "market",
        "instrument_type",
        "effective_start",
        "effective_end",
        "available_time",
        "official_source_id",
        "evidence_url",
        "parameters",
    }
    if set(rule) != expected_fields:
        raise EvidenceContractError(f"ETF 日频规则 schema 无效: {code}")
    if (
        rule.get("rule_id")
        != f"{profile.profile_id}.{category}.execution-policy"
        or rule.get("version") != profile.profile_version
        or rule.get("market") != "cn_etf"
        or rule.get("instrument_type") != "etf"
        or rule.get("effective_start") != profile.effective_start.isoformat()
        or rule.get("effective_end") != profile.effective_end.isoformat()
        or rule.get("available_time") != profile.rule_available_at.isoformat()
        or rule.get("official_source_id") != profile.source_id
        or rule.get("evidence_url") != profile.source_reference
    ):
        raise EvidenceContractError(f"ETF 日频规则来源或有效区间漂移: {code}")
    raw_parameters = rule.get("parameters")
    if (
        not isinstance(raw_parameters, list)
        or any(not isinstance(item, list) or len(item) != 2 for item in raw_parameters)
    ):
        raise EvidenceContractError(f"ETF 日频规则 parameters 无效: {code}")
    parameters = {str(item[0]): item[1] for item in raw_parameters}
    if len(parameters) != len(raw_parameters) or list(parameters) != sorted(parameters):
        raise EvidenceContractError(f"ETF 日频规则 parameters 重复或未排序: {code}")
    expected_parameters = {
        "commission_ppm": commission_ppm,
        "etf_category": category,
        "lot_size": profile.lot_size,
        "min_commission_units": min_commission_units,
        "sell_tax_ppm": profile.sell_tax_ppm,
        "settlement_days": profile.settlement_days_for(category),
        "transfer_fee_ppm": profile.transfer_fee_ppm,
    }
    if parameters != expected_parameters:
        raise EvidenceContractError(f"ETF 日频规则内容与 profile/成本假设不一致: {code}")


def _verify_daily_etf_fills_and_settlement(
    *,
    canonical: Mapping[str, list[dict[str, object]]],
    category_by_code: Mapping[str, str],
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
) -> None:
    fills = _ordered_rows(
        canonical["fills"],
        order_by=("session", "fill_time", "fill_id"),
    )
    bought: dict[tuple[str, date], int] = {}
    sold_total: dict[str, int] = {}
    bought_today: dict[tuple[str, date], int] = {}
    for fill in fills:
        code = str(fill["instrument_id"])
        category = category_by_code.get(code)
        if category is None:
            raise EvidenceContractError("ETF 日频 fill 引用未分类标的")
        session = _date_value(fill["session"], "fill.session")
        fill_time = _aware_datetime(fill["fill_time"], "fill.fill_time")
        if not (
            profile.effective_start <= session <= profile.effective_end
            and profile.rule_available_at <= fill_time
        ):
            raise EvidenceContractError("ETF 日频 fill 使用了无效或尚不可见的规则")
        quantity = _integer(fill["quantity"], "fill.quantity", minimum=1)
        if quantity % profile.lot_size:
            raise EvidenceContractError("ETF 日频 fill 不符合受控交易单位")
        notional = _integer(
            fill["notional_units"], "fill.notional_units", minimum=1
        )
        commission = max(
            min_commission_units,
            _ceil_ratio(notional * commission_ppm, 1_000_000),
        )
        transfer = _ceil_ratio(
            notional * profile.transfer_fee_ppm, 1_000_000
        )
        tax = (
            _ceil_ratio(notional * profile.sell_tax_ppm, 1_000_000)
            if str(fill["side"]) == "sell"
            else 0
        )
        if int(fill["fee_units"]) != commission + transfer + tax:
            raise EvidenceContractError("ETF 日频 fill 费用与研究假设不一致")
        settlement_days = profile.settlement_days_for(category)
        if str(fill["side"]) == "buy":
            key = (code, session)
            bought[key] = bought.get(key, 0) + quantity
            bought_today[key] = bought_today.get(key, 0) + quantity
        else:
            eligible = sum(
                value
                for (current_code, bought_on), value in bought.items()
                if current_code == code
                and (
                    bought_on < session
                    if settlement_days == 1
                    else bought_on <= session
                )
            ) - sold_total.get(code, 0)
            if quantity > eligible:
                raise EvidenceContractError("ETF 日频卖出 fill 绕过 T+0/T+1 约束")
            sold_total[code] = sold_total.get(code, 0) + quantity
    for position in canonical["positions"]:
        code = str(position["instrument_id"])
        category = category_by_code.get(code)
        if category is None:
            raise EvidenceContractError("ETF 日频持仓引用未分类标的")
        session = _date_value(position["session"], "position.session")
        unsettled = _integer(
            position["unsettled_quantity"],
            "position.unsettled_quantity",
            minimum=0,
        )
        non_trade = int(position["non_trade_quantity_change"])
        expected_buys = bought_today.get((code, session), 0)
        if non_trade == 0:
            expected_unsettled = (
                0 if profile.settlement_days_for(category) == 0 else expected_buys
            )
            if unsettled != expected_unsettled:
                raise EvidenceContractError("ETF 日频持仓桶与 T+0/T+1 规则不一致")
        elif unsettled < expected_buys:
            raise EvidenceContractError("ETF 日频非交易变化掩盖了当日未结算买入")


def _sorted_string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise EvidenceContractError(f"{field} 必须是字符串列表")
    values = tuple(value)
    if values != tuple(sorted(set(values))):
        raise EvidenceContractError(f"{field} 必须唯一并规范排序")
    return values


__all__ = ["verify_daily_etf_financial_context"]
