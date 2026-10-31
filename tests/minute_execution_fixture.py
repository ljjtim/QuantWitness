"""分钟金融类型、历史规则和目标声明的纯测试夹具。"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import pytest
from research_pipeline.domain import InstrumentKey, MinuteRuleBinding, MinuteRuleResolver, MinuteRuleSnapshot, PortfolioTarget, PortfolioTargetEntry
from research_pipeline.simulation import MinuteExecutionBar
TZ = ZoneInfo("Asia/Shanghai")
HASH = "a" * 64
def _dt(day: int, hour: int, minute: int) -> datetime:
    return datetime(2024, 1, day, hour, minute, tzinfo=TZ)
def _bar(
    instrument_id: str,
    asset_class: str,
    end: datetime,
    sequence: int,
    *,
    trading_date: date | None = None,
    volume: int = 10_000,
) -> MinuteExecutionBar:
    return MinuteExecutionBar(
        instrument_id=instrument_id,
        asset_class=asset_class,
        trading_date=trading_date or end.date(),
        session_id=(
            f"session.cn_futures.minute.v1@1:{instrument_id}:"
            f"{(trading_date or end.date()).isoformat()}"
            if asset_class == "cn_future"
            else f"{asset_class}-{(trading_date or end.date()).isoformat()}"
        ),
        bar_start=end - timedelta(minutes=1),
        bar_end=end,
        available_time=end,
        receipt_time=end,
        interval_minutes=1,
        open_units=1000,
        high_units=1010,
        low_units=990,
        close_units=1002,
        avg_units=1002,
        volume=volume,
        open_interest=50_000 if asset_class == "cn_future" else None,
        completed=True,
        quality_status="pass",
        source_snapshot_hash=HASH,
        source_sequence=sequence,
    )
def _target(
    instrument_id: str,
    asset_class: str,
    decision_time: datetime,
    quantity: int,
) -> PortfolioTarget:
    instrument = InstrumentKey(
        instrument_id,
        asset_class,
        instrument_id.rpartition(".")[2],
        "CNY",
        (
            "future_contract"
            if asset_class == "cn_future"
            else "stock" if asset_class == "cn_stock" else "etf"
        ),
    )
    return PortfolioTarget(
        decision_time=decision_time,
        target_type="quantity",
        entries=(PortfolioTargetEntry(instrument, "quantity", quantity),),
        base_currency="CNY",
        cash_weight=None,
        short_allowed=asset_class == "cn_future",
        leverage_limit=20.0 if asset_class == "cn_future" else 1.0,
        source_hashes=("b" * 64,),
    )
def _install_supported_rules(
    monkeypatch: pytest.MonkeyPatch,
    parameters_by_rule: dict[str, dict[str, object]],
    *,
    asset_class: str,
    settlement_prices: dict[str, int] | None = None,
) -> None:
    def resolve(self, *, rule_id, instrument_id, effective_on, as_of):
        parameters = dict(parameters_by_rule[rule_id])
        if rule_id == "rule.cn_futures.actual_contract_mapping.v1":
            parameters["actual_contract_id"] = instrument_id
        if (
            rule_id == "rule.cn_futures.settlement.v1"
            and settlement_prices is not None
        ):
            parameters["settlement_price_units"] = settlement_prices[instrument_id]
        rule = MinuteRuleSnapshot(
            rule_id,
            1,
            instrument_id,
            asset_class,
            "supported",
            effective_on,
            effective_on,
            datetime(2024, 1, 1, 0, 0, tzinfo=TZ),
            ("source",),
            tuple(sorted(parameters.items())),
            None,
        )
        return MinuteRuleBinding(rule, (("source", "e" * 64),), as_of)

    monkeypatch.setattr(MinuteRuleResolver, "resolve", resolve)
def _etf_rules() -> dict[str, dict[str, object]]:
    return {
        "rule.cn_fund.instrument_lifecycle.v1": {
            "listed_date": "2012-05-28",
            "delisted_date": "2200-01-01",
            "product_class": "equity_etf",
            "etf_category": "equity",
            "trading_status": "trading",
        },
        "rule.cn_fund.lot_size.v1": {
            "buy_lot_units": 100,
            "buy_min_quantity": 100, "buy_quantity_step": 100,
            "sell_min_quantity": 100, "sell_quantity_step": 100,
            "sell_remainder_allowed": True,
        },
        "rule.cn_fund.suspension.v1": {"paused": False},
        "rule.cn_fund.price_limit.v1": {
            "reference_previous_close_units": 1005,
            "price_limit_mode": "bounded",
            "price_limit_rounding": "half_up_to_quote_unit",
            "reference_price_available_at": "2024-01-01T15:01:00+08:00",
            "price_limit_ratio_ppm": 100_000,
            "price_scale": 3,
            "high_limit_units": 1106,
            "low_limit_units": 905,
        },
        "rule.cn_fund.session.v1": {},
        "rule.cn_fund.settlement.v1": {"settlement_days": 1},
        "rule.cn_fund.trading_fee.v1": {
            "cost_model_scope": "research_assumption",
            "commission_ppm": 300,
            "min_commission_units": 5_000,
            "sell_tax_ppm": 0,
            "transfer_fee_ppm": 0,
        },
    }
def _stock_rules() -> dict[str, dict[str, object]]:
    return {
        "rule.cn_stock.instrument_lifecycle.v1": {
            "stock_board": "sz_main", "listing_phase": "regular", "is_st": False,
            "listed_date": "1991-04-03", "delisted_date": "2200-01-01",
            "trading_status": "trading",
        },
        "rule.cn_stock.adjustment_factor_snapshot.v1": {
            "adjustment_snapshot_identity_hash": "d" * 64,
            "corporate_action_snapshot_hash": "c" * 64,
        },
        "rule.cn_stock.lot_size.v1": {
            "buy_lot_shares": 100,
            "buy_min_quantity": 100, "buy_quantity_step": 100,
            "sell_min_quantity": 100, "sell_quantity_step": 100,
            "sell_remainder_allowed": True,
        },
        "rule.cn_stock.price_limit.v1": {
            "reference_previous_close_units": 1005,
            "price_limit_mode": "bounded",
            "price_limit_rounding": "half_up_to_quote_unit",
            "reference_price_available_at": "2024-01-01T15:01:00+08:00",
            "price_limit_ratio_ppm": 100_000,
            "price_scale": 2,
            "high_limit_units": 1106,
            "low_limit_units": 905,
        },
        "rule.cn_stock.session.v1": {},
        "rule.cn_stock.settlement.v1": {"settlement_days": 1},
        "rule.cn_stock.suspension.v1": {"paused": False},
        "rule.cn_stock.trading_fee.v1": {
            "cost_model_scope": "market_rule",
            "commission_ppm": 300,
            "min_commission_units": 500,
            "sell_tax_ppm": 1_000,
            "transfer_fee_ppm": 10,
        },
    }
def _futures_rules() -> dict[str, dict[str, object]]:
    return {
        "rule.cn_futures.actual_contract_mapping.v1": {
            "actual_contract_id": "AG2406.XSGE"
        },
        "rule.cn_futures.contract_lifecycle.v1": {
            "listed_date": "2023-06-16",
            "last_trade_date": "2024-06-17",
        },
        "rule.cn_futures.contract_multiplier.v1": {"contract_unit_kg": 15},
        "rule.cn_futures.delivery_expiry.v1": {},
        "rule.cn_futures.fee_schedule.v1": {
            "close_fee_ppm": 50,
            "close_today_fee_ppm": 50,
            "fee_unit": "notional_permyriad",
            "open_fee_ppm": 50,
            "opening_charge_null_semantics": "use_common_clearance_charge",
        },
        "rule.cn_futures.margin.v1": {
            "hedge_margin_ppm": 80_000,
            "margin_account_role": "speculative",
            "margin_policy_id": "ag-margin-v1",
            "speculative_initial_margin_ppm": 90_000,
        },
        "rule.cn_futures.price_limit.v1": {
            "high_limit_units": 1070,
            "low_limit_units": 930,
            "price_limit_ratio_ppm": 70_000,
            "price_scale": 2,
            "price_tick_units": 1,
            "reference_previous_settlement_units": 1000,
            "reference_price_available_at": "2024-01-01T15:01:00+08:00",
        },
        "rule.cn_futures.price_tick.v1": {
            "price_tick": 1,
            "price_tick_units": 1,
            "price_scale": 2,
        },
        "rule.cn_futures.session.v1": {
            "session_policy_id": "session.cn_futures.minute.v1",
            "session_policy_revision": 1,
            "settlement_policy": "daily_mark_to_market_after_session_close"
        },
        "rule.cn_futures.settlement.v1": {
            "settlement_availability_semantics": (
                "session_close_event_not_supplier_timestamp"
            ),
            "settlement_price_units": 1000,
        },
    }
