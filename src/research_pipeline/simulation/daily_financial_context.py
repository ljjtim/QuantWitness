"""日频现货 Result 的历史规则、公司行动与账户事实投影。"""

from __future__ import annotations

from research_pipeline.platform import typed_canonical_hash
from .order_lifecycle import ORDER_LIFECYCLE_DECLARATION
from .orders import SimulationContractError


DAILY_ETF_FINANCIAL_CONTEXT_VERSION = "research-daily-etf-financial-context-v3"
DAILY_CASH_EXPLICIT_FINANCIAL_CONTEXT_VERSION = "research-daily-cash-financial-context-v2"


def build_daily_etf_financial_context(
    *, result, profile, rules, bond_etf_codes, equity_etf_codes,
    commission_ppm, min_commission_units, simulation_result_hash,
    source_ledger_hash, market=None, market_artifact_hash=None,
):
    """把正式仿真的公司行动原始事实连同受控规则交给独立 verifier。"""
    if result.order_lifecycle is None:
        raise SimulationContractError("新日频 ETF 金融上下文必须携带订单生命周期")
    account_context = getattr(result, "account_context", None)
    execution_mode = getattr(result, "execution_mode", "target")
    explicit_context = getattr(result, "explicit_order_context", None)
    if execution_mode not in {"target", "explicit_orders"}:
        raise SimulationContractError("日频金融上下文 execution_mode 无效")
    if (execution_mode == "explicit_orders") != (explicit_context is not None):
        raise SimulationContractError("显式模式必须封存完整订单上下文，目标模式不能携带该上下文")
    if getattr(result, "credit_context", None) is not None or getattr(result, "external_cashflow_context", None) is not None or execution_mode == "explicit_orders" or profile is None or account_context is not None or getattr(result, "corporate_action_records", ()) or any(item.contract_version == 2 for item in result.corporate_actions):
        return _build_daily_cash_financial_context(
            result=result, rules=rules, simulation_result_hash=simulation_result_hash,
            source_ledger_hash=source_ledger_hash, market=market,
            market_artifact_hash=market_artifact_hash, account_context=account_context,
            explicit_context=explicit_context,
        )
    rule_bundle = {code: rule.to_dict() for code, rule in sorted(rules.items())}
    costs = {
        "commission_ppm": commission_ppm,
        "min_commission_units": min_commission_units,
        "sell_tax_ppm": profile.sell_tax_ppm,
        "transfer_fee_ppm": profile.transfer_fee_ppm,
    }
    body = {
        "contract_version": DAILY_ETF_FINANCIAL_CONTEXT_VERSION,
        "order_lifecycle_contract": dict(ORDER_LIFECYCLE_DECLARATION),
        "corporate_actions": [
            item.to_dict() for item in sorted(result.corporate_actions, key=lambda item: item.action_id)
        ],
        "market_rule_profile_id": profile.profile_id,
        "market_rule_profile": profile.to_dict(),
        "market_rule_profile_hash": profile.profile_hash,
        "rule_bundle": rule_bundle,
        "rule_bundle_hash": typed_canonical_hash(rule_bundle),
        "instrument_classification": {
            "bond_etf_codes": sorted(bond_etf_codes),
            "equity_etf_codes": sorted(equity_etf_codes),
        },
        "cost_assumptions": {
            "assumption_id": "cn.etf.user-commission-assumption.v1",
            "commission_ppm": commission_ppm,
            "min_commission_units": min_commission_units,
            "currency": "CNY",
            "source_mode": "user_research_assumption",
        },
        "cost_model_hash": typed_canonical_hash(costs),
        "source_simulation_hash": result.simulation_hash,
        "simulation_result_hash": simulation_result_hash,
        "source_ledger_hash": source_ledger_hash,
    }
    return {**body, "context_hash": typed_canonical_hash(body)}


def _build_daily_cash_financial_context(
    *, result, rules, simulation_result_hash, source_ledger_hash,
    market, market_artifact_hash, account_context, explicit_context,
):
    rule_bundle = {
        code: [rule.to_dict() for rule in (snapshots if isinstance(snapshots, (list, tuple)) else (snapshots,))]
        for code, snapshots in sorted(rules.items())
    }
    asset_classes = {item["market"] for snapshots in rule_bundle.values() for item in snapshots}
    if len(asset_classes) != 1:
        raise SimulationContractError("日频现金金融上下文必须绑定唯一现货市场")
    asset_class = next(iter(asset_classes))
    body = {
        "contract_version": ("research-daily-cash-financial-context-v1" if explicit_context is None
                             else DAILY_CASH_EXPLICIT_FINANCIAL_CONTEXT_VERSION),
        "asset_class": asset_class,
        "order_lifecycle_contract": dict(ORDER_LIFECYCLE_DECLARATION),
        "corporate_actions": [item.to_dict() for item in sorted(result.corporate_actions, key=lambda item: (item.action_id, item.revision))],
        "corporate_action_records": list(getattr(result, "corporate_action_records", ())),
        "rule_bundle": rule_bundle,
        "rule_bundle_hash": typed_canonical_hash(rule_bundle),
        "source_simulation_hash": result.simulation_hash,
        "simulation_result_hash": simulation_result_hash,
        "source_ledger_hash": source_ledger_hash,
    }
    if explicit_context is not None:
        required = {"contract_version", "commands", "events", "observations", "fee_facts",
                    "command_rules", "initial_cash_units", "cash_scale", "session_ends", "initial_positions"}
        if not required <= set(explicit_context) or explicit_context["contract_version"] != "research-explicit-order-execution-v1":
            raise SimulationContractError("显式日频金融上下文缺少命令、预占、观察或累计费用事实")
        if market is None or market_artifact_hash is None:
            raise SimulationContractError("显式日频金融上下文必须封存原始行情及来源身份")
        body.update(execution_mode="explicit_orders", explicit_order_context=dict(explicit_context))
    if market is not None:
        from decimal import Decimal, ROUND_HALF_UP

        def price_units(value):
            import pandas as pd
            return None if pd.isna(value) else int(Decimal(str(value)).scaleb(3).quantize(Decimal(1), rounding=ROUND_HALF_UP))

        body["market_observations"] = [{
            "session": row.date.isoformat(), "instrument_id": str(row.code),
            "open_price_units": price_units(row.open), "close_price_units": price_units(row.close),
            "high_limit_units": price_units(row.high_limit), "low_limit_units": price_units(row.low_limit),
            "price_scale": 3, "paused": bool(row.paused),
            **({"opening_capacity": int(getattr(row, "visible_capacity", 2**62)),
                "capacity_model": "visible_capacity" if "visible_capacity" in market else "assumed_unbounded"}
               if explicit_context is not None else {}),
        } for row in market.sort_values(["code", "date"]).itertuples(index=False)]
        body["market_artifact_hash"] = market_artifact_hash
    declaration = getattr(result, "non_trading_sessions", None)
    if declaration is not None:
        if market is None or not market.empty or market_artifact_hash is None or account_context is None:
            raise SimulationContractError("非交易现金清算上下文必须封存原始空行情、来源身份和期初账户")
        body["non_trading_sessions"] = dict(declaration)
    cashflows = getattr(result, "external_cashflow_context", None)
    if cashflows is not None:
        if market is None or market_artifact_hash is None:
            raise SimulationContractError("资金流金融上下文必须封存可见行情及来源")
        body["contract_version"] = "research-daily-cash-financial-context-v3"
        body["external_cashflow_context"] = cashflows
    credit = getattr(result, "credit_context", None)
    if credit is not None:
        if market is None or market_artifact_hash is None or account_context is None or explicit_context is None:
            raise SimulationContractError("信用账户金融上下文必须封存行情、账户及显式订单事实")
        body["contract_version"] = "research-daily-cash-financial-context-v4"
        body["credit_context"] = credit
    if account_context is not None:
        # 账户 oracle 负责批次、税权、资金事件和期末账户；本合同只封存原始事实。
        body["account_context"] = account_context
    return {**body, "context_hash": typed_canonical_hash(body)}
