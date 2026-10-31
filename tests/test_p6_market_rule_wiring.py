"""P6 现货市场规则的时间可见性、数量格点与分钟执行政策。"""

from dataclasses import replace
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import (
    InstrumentKey, MarketRuleSnapshot, MinuteRuleResolver, MinuteRuleSnapshotBundle,
    MinuteRuleSnapshotError, Price, load_minute_rule_snapshot_bundle,
)
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.simulation import OpeningSnapshot, SimulationContractError
from research_pipeline.simulation.cn_stock import stock_policy_from_contract
from research_pipeline.simulation.cn_etf import etf_policy_for_session
from research_pipeline.simulation.market_rules import CashQuantityGrid, etf_policy_from_rule, stock_policy_from_rule
from research_pipeline.simulation.minute_rules import minute_cash_price_limits, resolve_minute_cash_policy

TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2024, 1, 2)
NOW = datetime(2024, 1, 2, 9, 31, tzinfo=TZ)


def _stock_rule(board="sz_main", **updates):
    minimum, step = (200, 1) if board == "star" else (100, 1) if board == "bse" else (100, 100)
    parameters = {
        "stock_board": board, "listing_phase": "regular", "is_st": False,
        "listed_date": "2020-01-01", "delisted_date": "2200-01-01", "trading_status": "trading",
        "buy_min_quantity": minimum, "buy_quantity_step": step,
        "sell_min_quantity": minimum, "sell_quantity_step": step,
        "sell_remainder_allowed": True, "lot_size": step,
        "price_limit_mode": "bounded", "settlement_days": 1,
        "commission_ppm": 0, "min_commission_units": 0, "sell_tax_ppm": 0, "transfer_fee_ppm": 0,
    }
    parameters.update(updates)
    return MarketRuleSnapshot("fixture.stock", 1, "cn_stock", "stock", DAY, date(2024, 1, 5), NOW - timedelta(days=1), "fixture.p6", "fixture://p6-stock-rule", tuple(sorted(parameters.items())))


def _instrument(board="sz_main"):
    venue = {"sz_main": "XSHE", "chinext": "XSHE", "sh_main": "XSHG", "star": "XSHG", "bse": "XBSE"}[board]
    return InstrumentKey(f"FIXTURE.{venue}", "cn_stock", venue, "CNY", "stock")


def _snapshot(instrument, **updates):
    values = dict(instrument_hash=instrument.instrument_hash, open_price=Price(1000, 2, "CNY"), high_limit=Price(1100, 2, "CNY"), low_limit=Price(900, 2, "CNY"), paused=False, visible_capacity=1000, available_time=NOW)
    values.update(updates)
    return OpeningSnapshot(**values)


@pytest.mark.parametrize(("board", "minimum", "step", "accepted", "rejected"), [
    ("sh_main", 100, 100, 200, 201), ("sz_main", 100, 100, 100, 101),
    ("chinext", 100, 100, 200, 201), ("star", 200, 1, 201, 199), ("bse", 100, 1, 101, 99),
])
def test_board_quantity_grid_preserves_minimum_and_step(board, minimum, step, accepted, rejected):
    policy = stock_policy_from_rule(_stock_rule(board))
    grid = policy.quantity_grid("buy")
    assert (grid.minimum, grid.step) == (minimum, step)
    assert grid.accepts(accepted)
    assert not grid.accepts(rejected)
    assert grid.floor(minimum - 1) == 0


def test_remainder_sale_is_explicit_and_cannot_split_small_balance():
    grid = CashQuantityGrid(200, 1, sell_remainder_allowed=True)
    assert grid.accepts(150, sellable=150)
    assert not grid.accepts(100, sellable=150)
    assert grid.floor(100, sellable=150) == 0
    assert grid.floor(150, sellable=150) == 150
    main = CashQuantityGrid(100, 100, sell_remainder_allowed=True)
    assert main.floor(140, sellable=250) == 100
    assert main.floor(150, sellable=250) == 150
    policy = stock_policy_from_rule(_stock_rule(sell_remainder_allowed=False))
    assert not policy.quantity_grid("sell").accepts(50, sellable=50)


@pytest.mark.parametrize("bad", [True, 0, -1, 1.5, "100"])
def test_quantity_integer_parameters_do_not_coerce_missing_rules(bad):
    with pytest.raises(SimulationContractError, match="整数|无效"):
        stock_policy_from_rule(_stock_rule(lot_size=bad))


def test_quantity_maximum_and_asymmetric_sell_step():
    policy = stock_policy_from_rule(_stock_rule(buy_max_quantity=300, sell_quantity_step=1))
    assert not policy.quantity_grid("buy").accepts(400)
    assert policy.quantity_grid("sell").accepts(101, sellable=101)
    assert policy.quantity_grid("sell").floor(101, sellable=101) == 101


def _bundle(asset="cn_stock", **overrides):
    payload = load_minute_rule_snapshot_bundle().to_dict()
    payload["sources"].append({"source_id": "fixture.p6", "source_kind": "repository_evidence", "locator": "fixture://p6-synthetic-rules", "published_on": "2020-01-01", "claim_ceiling": "research_observation", "contract_version": "minute-rule-source-v1"})
    payload["sources"].sort(key=lambda item: item["source_id"])
    stock = dict(_stock_rule().parameters)
    stock.pop("lot_size")
    lifecycle = {key: stock[key] for key in ("stock_board", "listing_phase", "is_st", "listed_date", "delisted_date", "trading_status")}
    if asset == "cn_etf":
        lifecycle = {"etf_category": "bond", "listed_date": "2020-01-01", "delisted_date": "2200-01-01", "trading_status": "trading"}
    grid = {key: stock[key] for key in ("buy_min_quantity", "buy_quantity_step", "sell_min_quantity", "sell_quantity_step", "sell_remainder_allowed")}
    mappings = {
        "instrument_lifecycle": lifecycle, "lot_size": grid,
        "price_limit": {"price_limit_mode": "bounded", "price_scale": 2, "price_limit_ratio_ppm": 100000, "high_limit_units": 1100, "low_limit_units": 900, "reference_previous_close_units": 1000, "reference_price_available_at": "2023-12-29T15:00:00+08:00", "price_limit_rounding": "half_up_to_quote_unit"},
        "session": {"session_policy_id": "fixture.session"},
        "settlement": {"settlement_days": 1 if asset == "cn_stock" else 0},
        "suspension": {"paused": False},
        "trading_fee": {"commission_ppm": 0, "min_commission_units": 0, "sell_tax_ppm": 0, "transfer_fee_ppm": 0, "cost_model_scope": "research_assumption"},
        "adjustment_factor_snapshot": {"adjustment_snapshot_identity_hash": "d" * 64, "corporate_action_snapshot_hash": "c" * 64},
    }
    for section, values in overrides.items():
        mappings[section].update(values)
    selected = []
    for rule in payload["rules"]:
        target_instrument = "000001.XSHE" if asset == "cn_stock" else "510300.XSHG"
        if rule["asset_class"] != asset or rule["instrument_id"] != target_instrument:
            selected.append(rule)
            continue
        section = rule["rule_id"].split(".")[-2]
        if rule["revision"] != 1:
            continue
        rule.update(status="supported", source_ids=["fixture.p6"], available_at="2024-01-01T00:00:00+08:00", effective_to="2024-01-05", parameters=mappings[section], unsupported_reason=None)
        selected.append(rule)
    payload["rules"] = selected
    payload["bundle_hash"] = typed_canonical_hash({key: value for key, value in payload.items() if key != "bundle_hash"})
    return MinuteRuleSnapshotBundle.from_dict(payload)


def _resolve(bundle, asset="cn_stock"):
    instrument = InstrumentKey("000001.XSHE" if asset == "cn_stock" else "510300.XSHG", asset, "XSHE" if asset == "cn_stock" else "XSHG", "CNY", "stock" if asset == "cn_stock" else "etf")
    return resolve_minute_cash_policy(MinuteRuleResolver(bundle), bundle=bundle, instrument=instrument, trading_date=DAY, decision_at=NOW)


@pytest.mark.parametrize("asset", ["cn_stock", "cn_etf"])
def test_minute_compilation_preserves_sources_and_settlement_policy(asset):
    rules, policy = _resolve(_bundle(asset), asset)
    assert policy.rule.parameter("source_binding_hash") == rules.identity_hash
    assert len(policy.rule.parameter("source_rule_bindings")) == len(rules.bindings)
    assert policy.settlement_days == (1 if asset == "cn_stock" else 0)


@pytest.mark.parametrize(("section", "bad", "message"), [
    ("instrument_lifecycle", {"stock_board": None}, "板块"),
    ("instrument_lifecycle", {"listed_date": "2024-01-03"}, "生命周期"),
    ("instrument_lifecycle", {"is_st": None}, "ST"),
    ("instrument_lifecycle", {"listing_phase": None}, "阶段"),
    ("suspension", {"paused": None}, "停牌"),
    ("lot_size", {"buy_quantity_step": None}, "整数"),
    ("settlement", {"settlement_days": 0}, r"T\+1"),
    ("price_limit", {"high_limit_units": 1200}, "涨跌停"),
    ("price_limit", {"reference_price_available_at": "2024-01-02T09:32:00+08:00"}, "尚不可见"),
])
def test_minute_missing_or_future_facts_fail_before_trading(section, bad, message):
    with pytest.raises(SimulationContractError, match=message):
        _resolve(_bundle(**{section: bad}))


def test_original_reference_bundle_does_not_gain_real_trading_coverage():
    original = load_minute_rule_snapshot_bundle()
    with pytest.raises(SimulationContractError, match="unsupported"):
        _resolve(original)
    with pytest.raises(SimulationContractError, match="unsupported"):
        _resolve(original, "cn_etf")


def test_unbounded_stage_has_no_fabricated_limits():
    parameters = {"price_limit_mode": "unbounded", "price_scale": 2}
    limits = minute_cash_price_limits(parameters, decision_at=NOW)
    assert limits.high_limit_units is limits.low_limit_units is None
    with pytest.raises(SimulationContractError, match="固定涨跌停"):
        minute_cash_price_limits({**parameters, "high_limit_units": 99999999}, decision_at=NOW)


def test_future_revision_cannot_change_the_visible_historical_rule():
    bundle = _bundle()
    payload = bundle.to_dict()
    original = next(rule for rule in payload["rules"] if rule["rule_id"] == "rule.cn_stock.settlement.v1")
    future = {**original, "revision": 2, "available_at": "2024-01-03T09:30:00+08:00", "parameters": {"settlement_days": 0}}
    payload["rules"].append(future)
    payload["rules"].sort(key=lambda item: (item["instrument_id"], item["rule_id"], item["revision"]))
    payload["bundle_hash"] = typed_canonical_hash({key: value for key, value in payload.items() if key != "bundle_hash"})
    revised = MinuteRuleSnapshotBundle.from_dict(payload)
    resolver = MinuteRuleResolver(revised)
    kwargs = dict(rule_id=original["rule_id"], instrument_id=original["instrument_id"], effective_on=DAY)
    assert resolver.resolve(**kwargs, as_of=NOW).rule.revision == 1
    assert resolver.resolve(**kwargs, as_of=datetime(2024, 1, 3, 9, 31, tzinfo=TZ)).rule.revision == 2
    overlap = {**future, "revision": 3, "effective_from": "2024-01-01", "available_at": "2024-01-01T00:00:00+08:00"}
    payload["rules"].append(overlap)
    payload["rules"].sort(key=lambda item: (item["instrument_id"], item["rule_id"], item["revision"]))
    payload["bundle_hash"] = typed_canonical_hash({key: value for key, value in payload.items() if key != "bundle_hash"})
    with pytest.raises(MinuteRuleSnapshotError, match="conflicting"):
        MinuteRuleResolver(MinuteRuleSnapshotBundle.from_dict(payload)).resolve(**kwargs, as_of=NOW)


def _cost_assumption():
    return {"contract_version": "research-cost-assumption-v1", "assumption_id": "fixture.cost", "currency": "CNY", "rate_unit": "ppm_of_notional", "minimum_fee_unit": "CNY_cent", "slippage_unit": "CNY_cent_per_share", "applicable_start": "2024-01-01", "applicable_end": "2024-12-31", "commission_ppm": 300, "min_commission_units": 500, "sell_tax_ppm": 0, "transfer_fee_ppm": 0, "slippage_per_share_units": 1}


def test_cost_assumption_requires_market_facts_and_preserves_them():
    kwargs = dict(research_cost_assumption=_cost_assumption(), research_start=DAY, research_end=date(2024, 1, 5), cash_shortage_policy="reject_v1")
    with pytest.raises(SimulationContractError, match="显式历史市场规则"):
        stock_policy_from_contract(lot_size=100, market_rule=None, **kwargs)
    policy = stock_policy_from_contract(market_rule=_stock_rule("star"), **kwargs)
    assert policy.quantity_grid("buy").minimum == 200
    assert policy.quantity_grid("buy").step == 1
    assert policy.rule.parameter("market_rule_hash") == _stock_rule("star").content_hash
    assert policy.slippage_units_per_share == 1


def test_etf_category_and_session_lifecycle_are_explicit():
    stock = _stock_rule()
    for category, settlement in (("equity", 1), ("bond", 0), ("commodity", 0), ("cross_border", 0), ("money_market", 0)):
        parameters = {key: value for key, value in stock.parameters if key not in {"stock_board", "listing_phase", "is_st"}}
        parameters.update(etf_category=category, settlement_days=settlement)
        rule = replace(stock, market="cn_etf", instrument_type="etf", parameters=tuple(sorted(parameters.items())))
        assert etf_policy_for_session(rule, trading_date=DAY, decision_at=NOW).settlement_days == settlement
        with pytest.raises(SimulationContractError, match="尚不可见"):
            etf_policy_for_session(replace(rule, available_time=NOW + timedelta(minutes=1)), trading_date=DAY, decision_at=NOW)
        with pytest.raises(SimulationContractError, match="品类"):
            etf_policy_from_rule(replace(rule, parameters=tuple(sorted({**parameters, "etf_category": "unknown"}.items()))))


def test_snapshot_rejects_incomplete_price_limits():
    instrument = _instrument()
    with pytest.raises(SimulationContractError, match="同时为空"):
        _snapshot(instrument, high_limit=None)


def test_unknown_delisting_date_is_explicit_and_does_not_extend_rule_window():
    rule = _stock_rule(delisted_date=None)
    assert stock_policy_from_rule(rule).rule.parameter("delisted_date") is None
    with pytest.raises(SimulationContractError, match="不适用于成交日"):
        from research_pipeline.simulation.market_rules import require_cash_rule_applicable
        require_cash_rule_applicable(rule, trading_date=date(2024, 1, 6), decision_at=NOW)
    parameters = dict(rule.parameters)
    parameters.pop("delisted_date")
    with pytest.raises(SimulationContractError, match="delisted_date"):
        stock_policy_from_rule(replace(rule, parameters=tuple(sorted(parameters.items()))))


@pytest.mark.parametrize("capacity,expected", [(199, 0), (200, 200), (201, 201)])
def test_star_quantity_floor_preserves_minimum(capacity, expected):
    grid = stock_policy_from_rule(_stock_rule("star")).quantity_grid("buy")
    assert grid.floor(capacity) == expected


@pytest.mark.parametrize("sellable,capacity,expected", [
    (130, 29, 0), (130, 30, 30), (130, 129, 100), (130, 130, 130),
    (150, 100, 100), (150, 150, 150),
])
def test_sell_quantity_floor_preserves_whole_remainder(sellable, capacity, expected):
    grid = stock_policy_from_rule(_stock_rule()).quantity_grid("sell")
    assert grid.floor(capacity, sellable=sellable) == expected


@pytest.mark.parametrize("changes,message", [
    ({"available_time": NOW + timedelta(minutes=1)}, "尚不可见"),
    ({"effective_start": date(2023, 1, 1), "effective_end": date(2024, 1, 1)}, "不适用"),
])
def test_cash_rule_requires_visible_and_effective_version(changes, message):
    from research_pipeline.simulation.market_rules import require_cash_rule_applicable

    with pytest.raises(SimulationContractError, match=message):
        require_cash_rule_applicable(replace(_stock_rule(), **changes), trading_date=DAY, decision_at=NOW)


def test_cash_rule_cannot_trade_before_listing():
    from research_pipeline.simulation.market_rules import require_cash_rule_applicable

    with pytest.raises(SimulationContractError, match="生命周期"):
        require_cash_rule_applicable(_stock_rule(listed_date="2024-01-03"), trading_date=DAY, decision_at=NOW)
