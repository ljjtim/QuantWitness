"""分钟生产测试共用的历史规则声明。"""
from research_pipeline.domain import MinuteRuleSnapshotBundle, load_minute_rule_snapshot_bundle
from research_pipeline.platform import typed_canonical_hash

def _rebundle(payload):
    payload["rules"].sort(key=lambda row: (row["instrument_id"], row["rule_id"], row["revision"]))
    payload["bundle_hash"] = typed_canonical_hash({key: value for key, value in payload.items() if key != "bundle_hash"})
    return MinuteRuleSnapshotBundle.from_dict(payload)


def _bundle(asset="cn_stock", **changes):
    payload = load_minute_rule_snapshot_bundle().to_dict()
    payload["sources"].append({
        "source_id": "fixture.p6.oracle", "source_kind": "repository_evidence",
        "locator": "fixture://p6-minute-rule-oracle", "published_on": "2020-01-01",
        "claim_ceiling": "research_observation", "contract_version": "minute-rule-source-v1",
    })
    payload["sources"].sort(key=lambda row: row["source_id"])
    lifecycle = {
        "listed_date": "2020-01-01", "delisted_date": None, "trading_status": "trading",
        **({"stock_board": "sz_main", "listing_phase": "regular", "is_st": False}
           if asset == "cn_stock" else {"etf_category": "equity"}),
    }
    parameters = {
        "instrument_lifecycle": lifecycle,
        "lot_size": {"buy_min_quantity": 100, "buy_quantity_step": 100,
                     "sell_min_quantity": 100, "sell_quantity_step": 100,
                     "sell_remainder_allowed": True},
        "price_limit": {"price_limit_mode": "bounded", "price_scale": 2,
                        "price_limit_ratio_ppm": 100000, "high_limit_units": 1100,
                        "low_limit_units": 900, "reference_previous_close_units": 1000,
                        "reference_price_available_at": "2023-12-29T15:00:00+08:00",
                        "price_limit_rounding": "half_up_to_quote_unit"},
        "session": {"session_policy_id": "fixture.session"},
        "settlement": {"settlement_days": 1},
        "suspension": {"paused": False},
        "trading_fee": {"commission_ppm": 0, "min_commission_units": 0,
                        "sell_tax_ppm": 0, "transfer_fee_ppm": 0,
                        "cost_model_scope": "research_assumption"},
        "adjustment_factor_snapshot": {"adjustment_snapshot_identity_hash": "a" * 64,
                                      "corporate_action_snapshot_hash": "b" * 64},
    }
    for section, updates in changes.items():
        parameters[section].update(updates)
    selected = []
    for rule in payload["rules"]:
        if rule["asset_class"] != asset:
            selected.append(rule)
        elif rule["revision"] == 1:
            section = rule["rule_id"].split(".")[-2]
            rule.update(status="supported", source_ids=["fixture.p6.oracle"],
                        available_at="2024-01-01T00:00:00+08:00",
                        effective_from="2024-01-02", effective_to="2024-01-05",
                        parameters=parameters[section], unsupported_reason=None)
            selected.append(rule)
    payload["rules"] = selected
    return _rebundle(payload)

