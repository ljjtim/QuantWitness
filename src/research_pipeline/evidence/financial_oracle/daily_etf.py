"""ETF 日频规则、费用、成交和结算桶独立复核。"""

from __future__ import annotations

from typing import Mapping
from datetime import datetime, time
from zoneinfo import ZoneInfo

from research_pipeline.domain import CorporateAction

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.market_rule_defaults import (
    CnEtfDailyMarketRuleProfile,
    require_cn_etf_daily_market_rule_profile_payload,
)
from research_pipeline.results import CANONICAL_SIMULATION_SCHEMA_IDS

from ..errors import EvidenceContractError
from ..oracle_workspace import OracleTable
from .order_lifecycle import LIFECYCLE_DECLARATION
from .common import (
    aware_datetime as _aware_datetime,
    ceil_ratio as _ceil_ratio,
    ordered_rows as _ordered_rows,
    date_value as _date_value,
    integer as _integer,
)


def verify_daily_etf_financial_context(
    *,
    context: Mapping[str, object],
    canonical: Mapping[str, list[dict[str, object]]],
    simulation_manifest: Mapping[str, object],
    oracle_input: Mapping[str, object],
    cashflow_oracle=None,
    credit_oracle=None,
) -> None:
    """从 Result 内事实独立复核 ETF 日频受控规则与佣金假设。"""

    if context.get("contract_version") in {"research-daily-cash-financial-context-v1", "research-daily-cash-financial-context-v2", "research-daily-cash-financial-context-v3", "research-daily-cash-financial-context-v4"}:
        _verify_daily_cash_financial_context(
            context=context, canonical=canonical,
            simulation_manifest=simulation_manifest, oracle_input=oracle_input, cashflow_oracle=cashflow_oracle, credit_oracle=credit_oracle,
        )
        return
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
        "corporate_actions",
    }
    version = context.get("contract_version")
    semantics_version = simulation_manifest.get("semantics", {}).get("contract_version")
    if version == "research-daily-etf-financial-context-v3":
        expected.add("order_lifecycle_contract")
        if (semantics_version != "research-simulation-result-semantics-v2"
                or context.get("order_lifecycle_contract") != LIFECYCLE_DECLARATION
                or simulation_manifest.get("order_lifecycle_contract") != LIFECYCLE_DECLARATION):
            raise EvidenceContractError("ETF 日频 v3 金融上下文必须绑定新生命周期合同")
    elif semantics_version == "research-simulation-result-semantics-v2":
        raise EvidenceContractError("新日频 ETF 仿真语义必须绑定 v3 金融上下文")
    if set(context) != expected or version not in {
        "research-daily-etf-financial-context-v2", "research-daily-etf-financial-context-v3",
    }:
        raise EvidenceContractError("ETF 日频金融上下文 schema 或版本无效")
    raw_actions = context.get("corporate_actions")
    if not isinstance(raw_actions, list) or any(not isinstance(item, Mapping) for item in raw_actions):
        raise EvidenceContractError("ETF 日频公司行动必须是列表")
    try:
        actions = tuple(CorporateAction.from_dict(item) for item in raw_actions)
    except (ValueError, TypeError, KeyError) as exc:
        raise EvidenceContractError(f"ETF 日频公司行动事实无效: {exc}") from exc
    if len({item.action_id for item in actions}) != len(actions):
        raise EvidenceContractError("ETF 日频公司行动身份重复或修订未冻结")
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
            corporate_actions=actions,
        )
        return
    _verify_daily_etf_fills_and_settlement(
        canonical=canonical,
        category_by_code=category_by_code,
        profile=profile,
        commission_ppm=commission_ppm,
        min_commission_units=min_commission_units,
        corporate_actions=actions,
    )


def _verify_external_daily_etf_fills_and_settlement(
    *,
    canonical: Mapping[str, OracleTable],
    category_by_code: Mapping[str, str],
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
    corporate_actions: tuple[CorporateAction, ...] = (),
) -> None:
    """外部表以有界批次按会话复核，仅保留当前持仓与尚未到账权益。"""
    _verify_daily_etf_fills_and_settlement(
        canonical=canonical,
        category_by_code=category_by_code,
        profile=profile,
        commission_ppm=commission_ppm,
        min_commission_units=min_commission_units,
        corporate_actions=corporate_actions,
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
    *, canonical, category_by_code, profile, commission_ppm,
    min_commission_units, corporate_actions=(),
) -> None:
    from .daily_holdings import verify_daily_etf_holdings

    verify_daily_etf_holdings(
        canonical=canonical, category_by_code=category_by_code, profile=profile,
        commission_ppm=commission_ppm, min_commission_units=min_commission_units,
        corporate_actions=corporate_actions,
    )


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


def _verify_daily_cash_financial_context(*, context, canonical, simulation_manifest, oracle_input, cashflow_oracle=None, credit_oracle=None):
    """使用封存历史事实复核股票及账户版 ETF，不调用生产费用或账本。"""
    required = {"contract_version", "asset_class", "order_lifecycle_contract", "corporate_actions",
                "rule_bundle", "rule_bundle_hash", "source_simulation_hash", "simulation_result_hash",
                "source_ledger_hash", "context_hash"}
    optional = {"account_context", "market_observations", "market_artifact_hash", "corporate_action_records",
                "execution_mode", "explicit_order_context", "external_cashflow_context", "credit_context", "non_trading_sessions"}
    if not required <= set(context) or set(context) - required - optional:
        raise EvidenceContractError("日频现金金融上下文 schema 无效")
    cashflows = context.get("external_cashflow_context")
    has_cashflows = "external_cashflow_context" in context
    has_credit = "credit_context" in context
    if has_credit != (context["contract_version"] == "research-daily-cash-financial-context-v4"):
        raise EvidenceContractError("信用事实必须绑定v4日频上下文")
    if has_credit and (context.get("account_context") is None or context.get("execution_mode") != "explicit_orders" or not has_cashflows):
        raise EvidenceContractError("信用账户须绑定P6A、显式订单及净NAV收益上下文")
    if has_cashflows != (context["contract_version"] in {"research-daily-cash-financial-context-v3", "research-daily-cash-financial-context-v4"}):
        raise EvidenceContractError("日频外部资金流必须绑定 v3 金融上下文")
    if has_cashflows and (not isinstance(cashflows, Mapping) or not cashflows):
        raise EvidenceContractError("日频外部资金流上下文必须为非空映射")
    asset_class = context["asset_class"]
    if asset_class not in {"cn_stock", "cn_etf"}:
        raise EvidenceContractError("日频现金金融上下文资产类别无效")
    semantics = simulation_manifest.get("semantics", {})
    if (semantics.get("asset_class") != asset_class or semantics.get("frequency") != "daily"
            or semantics.get("contract_version") != "research-simulation-result-semantics-v2"
            or context["order_lifecycle_contract"] != LIFECYCLE_DECLARATION
            or simulation_manifest.get("order_lifecycle_contract") != LIFECYCLE_DECLARATION):
        raise EvidenceContractError("日频现金上下文未绑定对应资产和生命周期合同")
    if typed_canonical_hash({key: value for key, value in context.items() if key != "context_hash"}) != context["context_hash"]:
        raise EvidenceContractError("日频现金金融上下文身份不一致")
    if (context["source_simulation_hash"] != simulation_manifest.get("source_simulation_hash")
            or context["simulation_result_hash"] != simulation_manifest.get("result_hash")
            or context["source_ledger_hash"] != oracle_input.get("source_ledger_hash")):
        raise EvidenceContractError("日频现金上下文未绑定正式仿真或账本")
    rules = _historical_daily_rules(context["rule_bundle"], asset_class)
    bundle_hash = typed_canonical_hash(context["rule_bundle"])
    policy = oracle_input.get("policy")
    if (context["rule_bundle_hash"] != bundle_hash or not isinstance(policy, Mapping)
            or policy.get("asset_class") != asset_class or policy.get("bar_frequency") != "daily"
            or policy.get("rule_snapshot_hash") != bundle_hash):
        raise EvidenceContractError("日频现金规则 bundle 与 TCA 绑定不一致")

    def parameters_for(code, session, as_of):
        entries = rules.get(code, ())
        visible = [entry for entry in entries if entry["start"] <= session
                   and (entry["end"] is None or session <= entry["end"])
                   and entry["available"] <= as_of]
        if len(visible) != 1:
            raise EvidenceContractError("日频现金历史规则缺失、重叠或尚不可见")
        return visible[0]["parameters"]

    non_trading = "non_trading_sessions" in context
    observations = {} if non_trading else daily_opening_observations(context, asset_class)
    raw_actions = context["corporate_actions"]
    if not isinstance(raw_actions, list) or any(not isinstance(item, Mapping) for item in raw_actions):
        raise EvidenceContractError("日频现金公司行动必须为事实列表")
    try:
        actions = tuple(CorporateAction.from_dict(item) for item in raw_actions)
    except (ValueError, TypeError, KeyError) as exc:
        raise EvidenceContractError(f"日频现金公司行动事实无效: {exc}") from exc
    if len({(action.action_id, action.revision) for action in actions}) != len(actions):
        raise EvidenceContractError("日频现金公司行动修订身份重复")
    from .daily_holdings import index_corporate_action_records
    index_corporate_action_records(context.get("corporate_action_records", ()))
    if non_trading:
        from .non_trading_cash import verify_non_trading_cash_context
        verify_non_trading_cash_context(context=context, canonical=canonical,
                                        rule_codes=rules, corporate_actions=actions)
    explicit = context.get("execution_mode", "targets") == "explicit_orders"
    replay_credit = has_credit and credit_oracle is None
    if replay_credit:
        from .credit_account import CreditContextOracle
        credit_oracle = CreditContextOracle(context=context["credit_context"], canonical=canonical,
            account_context=context["account_context"], market_observations=observations,
            market_artifact_hash=context.get("market_artifact_hash"))
    if has_cashflows and cashflow_oracle is None:
        from .external_cashflows import ExternalCashflowOracle
        cashflow_oracle = ExternalCashflowOracle(
            context=cashflows, canonical=canonical, market_observations=observations,
            market_artifact_hash=context.get("market_artifact_hash"),
            account_context=context.get("account_context"), explicit_context=context.get("explicit_order_context"), asset_class=asset_class,
            corporate_actions=context["corporate_actions"], corporate_action_records=context.get("corporate_action_records", ()), credit_oracle=credit_oracle,
        )
        if context.get("account_context") is not None:
            from .spot_account import verify_spot_account_context
            verify_spot_account_context(context=context["account_context"], canonical=canonical,
                                        allow_zero_opening=True, cashflow_oracle=cashflow_oracle, credit_oracle=credit_oracle)
        else:
            cashflow_oracle.replay_plain_account()
    if has_cashflows:
        cashflow_oracle.finish()
    verified_fee_units = None
    if context.get("contract_version") == "research-daily-cash-financial-context-v2" and not explicit:
        raise EvidenceContractError("日频现金 v2 必须绑定显式订单模式")
    if explicit:
        from .explicit_orders import verify_explicit_order_execution
        if not observations and not non_trading:
            raise EvidenceContractError("显式日频订单缺少封存开盘行情")
        verified_fee_units = verify_explicit_order_execution(
            context=context.get("explicit_order_context"), canonical=canonical,
            rule_bundle=context["rule_bundle"], asset_class=asset_class, price_scale=3,
            market_observations=observations, account_context=context.get("account_context"),
            verified_account_events=(context["account_context"]["financial_events"]
                                     if context.get("account_context") is not None else ()),
            corporate_actions=context["corporate_actions"],
            verified_external_cashflow_events=(() if cashflow_oracle is None else cashflow_oracle.context["events"]), credit_oracle=credit_oracle,
        )
    elif "explicit_order_context" in context:
        raise EvidenceContractError("目标模式不能声明显式订单上下文")
    for name in ("orders", "positions"):
        for row in _ordered_rows(canonical[name], order_by=("instrument_id", "session")):
            session = _date_value(row["session"], "session")
            preopen = datetime.combine(session, time(9, 30), ZoneInfo("Asia/Shanghai"))
            if name == "positions" and (observations or non_trading) and (str(row["instrument_id"]), session) not in observations:
                quantity = _integer(row["quantity"], "position.quantity", minimum=0)
                value = _integer(row["market_value_units"], "position.market_value_units", minimum=0)
                close_at = datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai"))
                if quantity != 0 or value != 0 or _aware_datetime(row["valuation_time"], "position.valuation_time") != close_at:
                    raise EvidenceContractError("日频现金持仓缺少封存收盘行情")
                # 换股前后的零仓位保留结果网格，不要求未上市或已退市证券的报价。
                continue
            parameters_for(str(row["instrument_id"]), session, preopen)
            if name == "positions" and observations:
                observed = observations.get((str(row["instrument_id"]), session))
                if observed is None:
                    raise EvidenceContractError("日频现金持仓缺少封存收盘行情")
                quantity = _integer(row["quantity"], "position.quantity", minimum=0)
                expected_value = (observed["close_price_units"] * quantity + 5) // 10
                close_at = datetime.combine(session, time(15), ZoneInfo("Asia/Shanghai"))
                if _integer(row["market_value_units"], "position.market_value_units", minimum=0) != expected_value or _aware_datetime(row["valuation_time"], "position.valuation_time") != close_at:
                    raise EvidenceContractError("日频现金持仓估值与当日收盘行情不一致")
    for row in _ordered_rows(canonical["fills"], order_by=("instrument_id", "session", "fill_time", "fill_id")):
        session = _date_value(row["session"], "fill.session")
        code = str(row["instrument_id"])
        preopen = datetime.combine(session, time(9, 30), ZoneInfo("Asia/Shanghai"))
        parameters = parameters_for(code, session, preopen)
        filled_at = _aware_datetime(row["fill_time"], "fill.fill_time")
        opening = datetime.combine(session, time(9, 30), ZoneInfo("Asia/Shanghai"))
        if filled_at != opening or row["asset_class"] != asset_class:
            raise EvidenceContractError("日频现金 fill 不属于声明的开盘执行事件")
        notional = _integer(row["notional_units"], "fill.notional_units", minimum=1)
        side = str(row["side"])
        if side not in {"buy", "sell"}:
            raise EvidenceContractError("日频现金 fill 方向无效")
        fee = max(parameters["min_commission_units"], _ceil_ratio(notional * parameters["commission_ppm"], 1_000_000))
        fee += _ceil_ratio(notional * parameters["transfer_fee_ppm"], 1_000_000)
        if side == "sell":
            fee += _ceil_ratio(notional * parameters["sell_tax_ppm"], 1_000_000)
        if explicit:
            fee = verified_fee_units.get(str(row["fill_id"]))
        if _integer(row["fee_units"], "fill.fee_units", minimum=0) != fee:
            raise EvidenceContractError("日频现金 fill 费用与当日历史规则不一致")
        quantity = _integer(row["quantity"], "fill.quantity", minimum=1)
        if side == "buy":
            from .daily_holdings import is_daily_quantity_allowed
            if not is_daily_quantity_allowed(quantity, side, parameters, 0):
                raise EvidenceContractError("日频现金买入 fill 违反历史交易单位与数量格点")
        if observations:
            observed = observations.get((code, session))
            if observed is None:
                raise EvidenceContractError("日频现金 fill 缺少封存开盘行情")
            _verify_daily_opening_fill(row, parameters, observed, code, session, explicit_orders=explicit)
    if "account_context" in context:
        from decimal import Decimal, ROUND_HALF_UP
        from .common import ordered_rows
        for position in ordered_rows(canonical["positions"], order_by=("session", "instrument_hash")):
            quantity = _integer(position["quantity"], "position.quantity", minimum=0)
            observed = observations.get((str(position["instrument_id"]), _date_value(position["session"], "position.session")))
            if quantity and observed is None:
                raise EvidenceContractError("账户持仓缺少封存收盘价格")
            expected_value = 0 if not quantity else int((Decimal(observed["close_price_units"] * quantity) / 10).quantize(Decimal(1), rounding=ROUND_HALF_UP))
            if position["market_value_units"] != expected_value:
                raise EvidenceContractError("账户持仓市值与封存收盘行情不符")
        if not isinstance(context["account_context"], Mapping):
            raise EvidenceContractError("日频现金账户事实必须为映射")
        # 账户批次、税权和非空期初持仓由正式 Result 的独立账户 oracle 复核。
        return
    from .daily_holdings import verify_daily_etf_holdings
    if cashflow_oracle is not None:
        from .external_cashflows import cashflow_adjusted_holdings
        canonical = cashflow_adjusted_holdings(canonical, cashflow_oracle)
    verify_daily_etf_holdings(
        canonical=canonical, category_by_code={code: asset_class for code in rules}, profile=None,
        commission_ppm=None, min_commission_units=None, corporate_actions=actions,
        rule_parameters_for=parameters_for, corporate_action_records=context.get("corporate_action_records", ()),
        **({"verified_fee_units": verified_fee_units} if explicit else {}),
    )


def _historical_daily_rules(bundle, asset_class):
    if not isinstance(bundle, Mapping) or not bundle:
        raise EvidenceContractError("日频现金历史规则必须是非空逐标的列表")
    fields = {"rule_id", "version", "market", "instrument_type", "effective_start", "effective_end",
              "available_time", "official_source_id", "evidence_url", "parameters"}
    output = {}
    for code, entries in bundle.items():
        if not isinstance(code, str) or not code or not isinstance(entries, list) or not entries:
            raise EvidenceContractError("日频现金规则标的或序列无效")
        normalized = []
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != fields:
                raise EvidenceContractError("日频现金规则条目 schema 无效")
            if entry["market"] != asset_class or entry["instrument_type"] != ("stock" if asset_class == "cn_stock" else "etf"):
                raise EvidenceContractError("日频现金规则资产类别不一致")
            for field in ("rule_id", "official_source_id", "evidence_url"):
                if not isinstance(entry[field], str) or not entry[field].strip():
                    raise EvidenceContractError("日频现金规则缺少身份或来源证据")
            _integer(entry["version"], "rule.version", minimum=1)
            start = _date_value(entry["effective_start"], "rule.effective_start")
            end = None if entry["effective_end"] is None else _date_value(entry["effective_end"], "rule.effective_end")
            available = _aware_datetime(entry["available_time"], "rule.available_time")
            if end is not None and end < start:
                raise EvidenceContractError("日频现金历史规则有效期倒置")
            raw = entry["parameters"]
            if (not isinstance(raw, list) or any(not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str) for item in raw)):
                raise EvidenceContractError("日频现金规则参数必须为名称和值列表")
            parameters = dict(raw)
            if len(parameters) != len(raw) or list(parameters) != sorted(parameters):
                raise EvidenceContractError("日频现金历史规则参数重复或未排序")
            for field in ("commission_ppm", "min_commission_units", "sell_tax_ppm", "transfer_fee_ppm", "lot_size", "settlement_days"):
                _integer(parameters.get(field), field, minimum=1 if field == "lot_size" else 0)
            if parameters["settlement_days"] not in ({1} if asset_class == "cn_stock" else {0, 1}):
                raise EvidenceContractError("日频现金结算规则无效")
            if asset_class == "cn_stock":
                _verify_stock_daily_parameters(parameters, code)
            elif parameters.get("source_instrument_id", code) != code:
                raise EvidenceContractError("日频 ETF 历史规则标的绑定无效")
            normalized.append({"start": start, "end": end, "available": available, "parameters": parameters})
        previous = None
        for entry in normalized:
            if previous is not None and (previous["end"] is None or previous["end"] >= entry["start"]):
                raise EvidenceContractError("日频现金历史规则区间重叠或未排序")
            previous = entry
        output[code] = tuple(normalized)
    return output


def _verify_stock_daily_parameters(parameters, code):
    venues = {"sh_main": "XSHG", "sz_main": "XSHE", "chinext": "XSHE", "star": "XSHG", "bse": "XBSE"}
    if (parameters.get("source_instrument_id") != code
            or parameters.get("stock_board") not in venues
            or code.rsplit(".", 1)[-1] != venues[parameters["stock_board"]]):
        raise EvidenceContractError("股票历史规则标的、板块或交易所不一致")
    if (parameters.get("listing_phase") not in {"regular", "ipo", "relisting", "delisting"}
            or type(parameters.get("is_st")) is not bool
            or parameters.get("trading_status") not in {"trading", "suspended"}
            or parameters.get("price_limit_mode") not in {"bounded", "unbounded"}
            or type(parameters.get("sell_remainder_allowed")) is not bool):
        raise EvidenceContractError("股票历史规则缺少上市阶段、ST、交易状态或价格限制")
    listed = _date_value(parameters.get("listed_date"), "listed_date")
    delisted = None if parameters.get("delisted_date") is None else _date_value(parameters["delisted_date"], "delisted_date")
    if delisted is not None and listed > delisted:
        raise EvidenceContractError("股票上市及退市日期倒置")
    for side in ("buy", "sell"):
        minimum = _integer(parameters.get(f"{side}_min_quantity"), f"{side}_min_quantity", minimum=1)
        _integer(parameters.get(f"{side}_quantity_step"), f"{side}_quantity_step", minimum=1)
        if f"{side}_max_quantity" in parameters and _integer(parameters[f"{side}_max_quantity"], f"{side}_max_quantity", minimum=1) < minimum:
            raise EvidenceContractError("股票申报数量上限小于下限")


def daily_opening_observations(context, asset_class):
    if "non_trading_sessions" in context:
        from .non_trading_cash import require_non_trading_observations
        require_non_trading_observations(context)
        return {}
    rows = context.get("market_observations")
    if rows is None:
        if asset_class == "cn_stock":
            raise EvidenceContractError("日频股票金融上下文缺少封存开盘行情")
        return {}
    if not isinstance(rows, list) or not rows or not isinstance(context.get("market_artifact_hash"), str) or len(context["market_artifact_hash"]) != 64:
        raise EvidenceContractError("日频现金封存行情或来源身份无效")
    fields = {"session", "instrument_id", "open_price_units", "close_price_units", "high_limit_units", "low_limit_units", "price_scale", "paused"}
    output = {}
    for row in rows:
        expected_fields = fields | ({"opening_capacity", "capacity_model"} if context.get("execution_mode") == "explicit_orders" or context.get("contract_version") == "research-daily-cash-financial-context-v2" else set())
        if not isinstance(row, Mapping) or set(row) != expected_fields or row["price_scale"] != 3 or type(row["paused"]) is not bool:
            raise EvidenceContractError("日频现金封存行情 schema 无效")
        if "opening_capacity" in row:
            _integer(row["opening_capacity"], "开盘可见容量", minimum=0)
            if row["capacity_model"] not in {"visible_capacity", "assumed_unbounded"} or (row["capacity_model"] == "assumed_unbounded" and row["opening_capacity"] != 2**62):
                raise EvidenceContractError("开盘容量假设与封存容量不一致")
        for field in ("open_price_units", "close_price_units"):
            _integer(row[field], field, minimum=1)
        for field in ("high_limit_units", "low_limit_units"):
            if row[field] is not None:
                _integer(row[field], field, minimum=1)
        key = (str(row["instrument_id"]), _date_value(row["session"], "market.session"))
        if key in output:
            raise EvidenceContractError("日频现金封存行情键重复")
        output[key] = row
    return output


def _verify_daily_opening_fill(fill, parameters, observed, code, session, *, explicit_orders=False):
    if (observed["paused"] or parameters.get("trading_status") == "suspended"
            or session < _date_value(parameters.get("listed_date", session), "listed_date")
            or parameters.get("delisted_date") is not None and session > _date_value(parameters["delisted_date"], "delisted_date")):
        raise EvidenceContractError("日频现金 fill 在停牌或非交易生命周期执行")
    execution = _integer(fill["execution_price_units"], "fill.execution_price_units", minimum=1)
    if (not explicit_orders and execution != observed["open_price_units"]) or fill["price_scale"] != 3:
        raise EvidenceContractError("日频现金 fill 与封存开盘价格不一致")
    if parameters.get("price_limit_mode", "bounded") == "bounded":
        if observed["low_limit_units"] is None or observed["high_limit_units"] is None:
            raise EvidenceContractError("有限价股票缺少当日上下限")
        if observed["low_limit_units"] > observed["high_limit_units"]:
            raise EvidenceContractError("日频现金价格上下限倒置")
        if (fill["side"] == "buy" and execution >= observed["high_limit_units"]
                or fill["side"] == "sell" and execution <= observed["low_limit_units"]):
            raise EvidenceContractError("日频现金 fill 越过当日价格限制")
