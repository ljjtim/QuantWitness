"""日频 ETF Result 的金融规则与公司行动事实投影。"""

from __future__ import annotations

from research_pipeline.platform import typed_canonical_hash


DAILY_ETF_FINANCIAL_CONTEXT_VERSION = "research-daily-etf-financial-context-v2"


def build_daily_etf_financial_context(
    *, result, profile, rules, bond_etf_codes, equity_etf_codes,
    commission_ppm, min_commission_units, simulation_result_hash,
    source_ledger_hash,
):
    """把正式仿真的公司行动原始事实连同受控规则交给独立 verifier。"""
    rule_bundle = {code: rule.to_dict() for code, rule in sorted(rules.items())}
    costs = {
        "commission_ppm": commission_ppm,
        "min_commission_units": min_commission_units,
        "sell_tax_ppm": profile.sell_tax_ppm,
        "transfer_fee_ppm": profile.transfer_fee_ppm,
    }
    body = {
        "contract_version": DAILY_ETF_FINANCIAL_CONTEXT_VERSION,
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
