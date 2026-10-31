"""策略消息表达、资金口径与可见性合同；不连接数据库。"""

from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
import json
from types import SimpleNamespace


import pytest

from research_pipeline.domain.strategy_protocol import (
    STRATEGY_PROTOCOL_VERSION, STRATEGY_STATE_VERSION, StrategyAccountView,
    StrategyDeclaration, StrategyEvent, StrategyFeedback, StrategyInputRef, StrategyInputBinding,
    StrategyIntent, StrategyLimits, StrategyOrderView, StrategyPositionView,
    StrategyProtocolError, StrategySchedule, parse_strategy_response, validate_event, validate_event_transition,
)
from research_pipeline.domain.trading import InstrumentKey
from research_pipeline.domain.values import Money, Price


AT = datetime(2024, 1, 2, 9, 31, tzinfo=timezone(timedelta(hours=8)))
STOCK = InstrumentKey("600000.XSHG", "cn_stock", "XSHG", "CNY", "stock")
OTHER = InstrumentKey("000001.XSHE", "cn_stock", "XSHE", "CNY", "stock")


def declaration(**changes):
    values = dict(strategy_id="strategy", bundle_id="frozen-bundle", state_schema_id="project-state-v1",
                  subscriptions=("execution_feedback", "bar", "scheduled"),
                  schedules=(StrategySchedule(schedule_id="buy", basis="local_time", at="09:31:00"),),
                  input_bindings=(StrategyInputBinding(binding_id="quotes", columns=("close",), lookback_bars=1),),
                  limits=StrategyLimits(max_state_bytes=1024, max_context_bytes=16384,
                                        max_response_bytes=8192, max_intents_per_event=8,
                                        callback_wall_seconds=1))
    return StrategyDeclaration(**{**values, **changes})


def account(**changes):
    values = dict(account_seq=4, as_of=AT, total_cash=Money(110000, 2, "CNY"),
                  available_cash=Money(60000, 2, "CNY"), frozen_cash=Money(30000, 2, "CNY"),
                  withdrawable_cash=Money(10000, 2, "CNY"), net_asset=Money(500000, 2, "CNY"),
                  positions=(), active_orders=())
    return StrategyAccountView(**{**values, **changes})


def event(**changes):
    values = dict(event_seq=7, kind="bar", trading_date=date(2024, 1, 2), event_time=AT,
                  decision_time=AT, account=account(), feedback=(), inputs=(),
                  market_frame_seq=0, market_window_bytes=0, market=None)
    return StrategyEvent(**{**values, **changes})


def intent(**changes):
    values = dict(intent_id="7:0", kind="quantity", instrument=STOCK, side="buy", quantity=100,
                  order_type="market", time_in_force="DAY", funds_policy="resize", price_input_id=f"quotes:{STOCK.instrument_id}:{AT.isoformat()}")
    return StrategyIntent(**{**values, **changes})


def payload(*intents, **changes):
    values = dict(protocol_version=STRATEGY_PROTOCOL_VERSION, event_seq=7,
                  state_version=STRATEGY_STATE_VERSION, state_schema_id="project-state-v1",
                  state={"phase": "waiting"}, intents=[item.to_dict() for item in intents])
    return {**values, **changes}


def parse(data, *, declared=None, current=None):
    declared, current = declared or declaration(), current or event()
    validate_event(declared, current)
    return parse_strategy_response(json.loads(json.dumps(data)), declaration=declared, event=current)


def test_actual_available_cash_can_be_split_without_using_nav_or_receivables():
    view = account()
    first = intent(kind="amount", quantity=None, amount=Money(30000, 2, "CNY"))
    second = intent(kind="amount", intent_id="7:1", instrument=OTHER, price_input_id=f"quotes:{OTHER.instrument_id}:{AT.isoformat()}",
                    quantity=None, amount=Money(30000, 2, "CNY"))
    response = parse(payload(first, second))
    assert sum(item.amount.units for item in response.intents) == view.available_cash.units
    assert view.available_cash != view.withdrawable_cash
    assert view.total_cash.units > view.available_cash.units + view.frozen_cash.units
    wire = response.to_dict()
    wire["state"]["phase"] = "changed"
    assert response.to_dict()["state"] == {"phase": "waiting"}
    with pytest.raises(FrozenInstanceError):
        view.available_cash = Money(90000, 2, "CNY")


@pytest.mark.parametrize("kind,fields", [
    ("target_quantity", {"quantity": 0}),
    ("target_amount", {"quantity": None, "amount": Money(0, 2, "CNY")}),
    ("target_weight", {"quantity": None, "weight": "0.50"}),
])
def test_targets_remain_targets_for_execution_to_resolve(kind, fields):
    response = parse(payload(intent(kind=kind, side=None, **fields)))
    assert response.intents[0].kind == kind
    assert response.intents[0].side is None


def test_partial_fill_cancel_and_later_remaining_order_are_expressible():
    active = StrategyOrderView(order_id="o1", instrument=STOCK, side="buy", quantity=300,
                               filled_quantity=100, status="partially_filled", submitted_at=AT,
                               reserved_cash=Money(200000, 2, "CNY"))
    partial = StrategyFeedback(intent_id="6:0", order_id="o1", kind="partially_filled",
                               event_time=AT, filled_quantity=100, trade_ids=("t1",))
    current = event(kind="execution_feedback", account=account(active_orders=(active,), total_cash=Money(600000, 2, "CNY"),
                                                     frozen_cash=Money(300000, 2, "CNY")), feedback=(partial,))
    cancel = StrategyIntent(intent_id="7:0", kind="cancel", instrument=STOCK, order_id="o1")
    response = parse(payload(cancel), current=current)
    assert response.intents[0].order_id == "o1"
    assert current.account.active_orders[0].reserved_cash == Money(200000, 2, "CNY")
    cancelled = replace(partial, intent_id="7:0", kind="cancelled", trade_ids=())
    later = replace(current, event_seq=8, account=account(account_seq=5), feedback=(cancelled,))
    retry = intent(intent_id="8:0", quantity=200)
    assert parse(payload(retry, event_seq=8), current=later).intents[0].quantity == 200


@pytest.mark.parametrize("changes", [
    {"asset_class": "cn_future"}, {"asset_class": "cn_index"},
    {"frequency": "5m"}, {"currency": "USD"},
])
def test_unsupported_asset_frequency_currency_combinations(changes):
    with pytest.raises(StrategyProtocolError) as error:
        declaration(**changes)
    assert error.value.error_code == "strategy_capability_unsupported"


@pytest.mark.parametrize("changes", [{"protocol_version": "v0"}, {"state_version": "v0"}])
def test_old_protocol_and_state_versions_are_not_migrated(changes):
    with pytest.raises(StrategyProtocolError) as error:
        declaration(**changes)
    assert error.value.error_code == "strategy_version_unsupported"


def test_future_price_feedback_and_stale_account_are_rejected():
    future = AT + timedelta(minutes=1)
    with pytest.raises(StrategyProtocolError, match="不可见"):
        event(inputs=(StrategyInputRef(binding_id="quotes", input_id="future", available_at=future),))
    with pytest.raises(StrategyProtocolError, match="不可见"):
        event(feedback=(StrategyFeedback(intent_id="6:0", order_id="o1", kind="accepted",
                                        event_time=future, filled_quantity=0, trade_ids=()),))
    with pytest.raises(StrategyProtocolError, match="决策时点"):
        event(account=account(as_of=AT - timedelta(minutes=1)))


def test_response_parsing_does_not_read_market_prices():
    class Market:
        def history(self, *args):
            raise AssertionError("应答解析不得读取行情")
        def resolve_price(self, *args):
            raise AssertionError("应答解析不得读取行情")
    parsed = parse(payload(intent()), current=event(market=Market()))
    assert parsed.intents == (intent(),)


@pytest.mark.parametrize("changes", [
    {"event_seq": 8}, {"event_seq": True}, {"state_schema_id": "other"},
    {"account": {"available_cash": 999999}},
])
def test_response_binding_and_account_writeback_are_rejected(changes):
    with pytest.raises(StrategyProtocolError):
        parse(payload(intent(), **changes))


@pytest.mark.parametrize("state", [{1: "bad"}, {"x": float("nan")}, {"x": {1, 2}}, []])
def test_state_is_explicit_finite_json(state):
    with pytest.raises(StrategyProtocolError):
        parse_strategy_response(payload(state=state), declaration=declaration(), event=event())


@pytest.mark.parametrize("field,value", [
    ("max_state_bytes", 2), ("max_context_bytes", 1),
    ("max_response_bytes", 1), ("max_intents_per_event", 1),
])
def test_declared_resource_limits(field, value):
    declared = declaration()
    declared = replace(declared, limits=replace(declared.limits, **{field: value}))
    with pytest.raises(StrategyProtocolError) as error:
        parse(payload(intent(), intent(intent_id="7:1")), declared=declared)
    assert error.value.error_code == "strategy_resource_exceeded"


def test_duplicate_decision_identity_is_rejected():
    with pytest.raises(StrategyProtocolError, match="批内顺序"):
        parse(payload(intent(), intent()))


def test_schedule_identity_and_subscriptions_are_frozen():
    validate_event(declaration(), event(kind="scheduled", schedule_id="buy"))
    with pytest.raises(StrategyProtocolError, match="未声明"):
        validate_event(declaration(), event(kind="scheduled", schedule_id="unknown"))
    with pytest.raises(StrategyProtocolError, match="未订阅"):
        validate_event(declaration(), event(kind="after_trading"))


@pytest.mark.parametrize("changes", [
    {"weight": "NaN"}, {"weight": "1.1"}, {"weight": 0.5}, {"weight": "-0.1"},
])
def test_target_weight_is_exact_unlevered_decimal(changes):
    with pytest.raises(StrategyProtocolError):
        intent(kind="target_weight", side=None, quantity=None, **changes)


def test_no_trading_from_initialize_or_after_trading():
    declared = declaration(subscriptions=("initialize", "before_trading", "after_trading"), schedules=())
    for kind in declared.subscriptions:
        with pytest.raises(StrategyProtocolError, match="不提交"):
            parse(payload(intent()), declared=declared, current=event(kind=kind))


def test_t_plus_one_and_reserved_sell_are_distinct():
    position = StrategyPositionView(instrument=STOCK, quantity=300, sellable_quantity=100,
                                    reserved_sell_quantity=100, mark_price=Price(1000, 2, "CNY"),
                                    mark_available_at=AT)
    assert account(positions=(position,)).positions[0].sellable_quantity == 100
    with pytest.raises(StrategyProtocolError, match="超过"):
        replace(position, sellable_quantity=300)


def test_same_time_schedule_precedes_bar_and_cannot_return_to_feedback():
    scheduled = event(kind="scheduled", schedule_id="buy")
    bar = event(event_seq=8)
    validate_event_transition(scheduled, bar)
    with pytest.raises(StrategyProtocolError) as error:
        validate_event_transition(bar, event(event_seq=9, kind="execution_feedback"))
    assert error.value.error_code == "strategy_event_order_invalid"


@pytest.mark.parametrize("changes", [
    {"event_seq": 9},
    {"account": account(account_seq=3)},
    {"kind": "initialize"},
    {"kind": "bar"},
])
def test_event_sequence_and_stage_cannot_repeat_or_go_back(changes):
    with pytest.raises(StrategyProtocolError):
        validate_event_transition(event(), event(**{**{"event_seq": 8}, **changes}))


def test_account_reservations_cannot_exceed_frozen_cash():
    active = StrategyOrderView(order_id="o1", instrument=STOCK, side="buy", quantity=100,
                               filled_quantity=0, status="accepted", submitted_at=AT,
                               reserved_cash=Money(40000, 2, "CNY"))
    with pytest.raises(StrategyProtocolError, match="预占不能超过"):
        account(active_orders=(active,))


@pytest.mark.parametrize("changes", [
    {"kind": "amount", "amount": Money(1, 2, "CNY")},
    {"kind": "target_quantity", "side": "buy"},
    {"order_type": "limit"},
    {"time_in_force": "GTC"},
    {"quantity": True},
])
def test_invalid_intent_field_combinations_are_rejected(changes):
    with pytest.raises(ValueError):
        intent(**changes)


def test_only_declared_source_bindings_can_reach_the_strategy():
    source = StrategyInputRef(binding_id="not-admitted", input_id="other", available_at=AT)
    with pytest.raises(StrategyProtocolError) as error:
        validate_event(declaration(), event(inputs=(source,)))
    assert error.value.error_code == "strategy_input_undeclared"


def test_history_window_and_columns_are_explicit():
    with pytest.raises(ValueError):
        StrategyInputBinding(binding_id="quotes", columns=("close",), lookback_bars=0)
    with pytest.raises(StrategyProtocolError):
        StrategyInputBinding(binding_id="quotes", columns=(), lookback_bars=1)


@pytest.mark.parametrize("kind", ["accepted", "rejected"])
def test_submit_receipt_cannot_claim_a_fill(kind):
    with pytest.raises(StrategyProtocolError, match="不能携带成交"):
        StrategyFeedback(intent_id="6:0", order_id="o1", kind=kind, event_time=AT,
                         filled_quantity=100, trade_ids=("t1",), reason="declined")
