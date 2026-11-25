"""送转原始到账日期、经济交付与日频桥接；不访问数据库。"""
from dataclasses import replace
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import CorporateAction, DomainContractError
from research_pipeline.runtime.lean_strategy_inputs import _actions
from research_pipeline.domain.corporate_action_source import (
    CorporateActionRecordPosition, compile_cn_stock_corporate_action_rows,
    compile_corporate_action, compile_corporate_action_share_arrival,
)
from research_pipeline.domain.corporate_actions import CorporateActionError

TZ = ZoneInfo("Asia/Shanghai")


def row(**changes):
    value = dict(action_id="263480", code="002871.XSHE", announcement_date=date(2023,4,19),
        record_date=date(2023,4,27), ex_date=date(2023,4,28), listing_date=date(2023,4,28),
        transfer_arrival_date=date(2023,4,27), transfer_per_ten=3,
        stock_dividend_per_ten=0, cash_per_ten=0, plan_progress="实施方案",
        source_ref="snapshot:STK_XR_XD:263480")
    value.update(changes)
    return value


def compile_row(value, policy="error"):
    return compile_cn_stock_corporate_action_rows((value,),
        trading_sessions=tuple(date(2023,4,d) for d in (19,20,21,24,25,26,27,28)),
        contract_version=2, missing_date_policy=policy)[0]


def projection(action):
    context={"adjustment_snapshots":{"002871.XSHE":{"financial_corporate_actions":[action.to_dict()]}}}
    return _actions(context,["002871.XSHE"],["2023-04-27","2023-04-28"],lambda *args:1000)[0][0]


def test_real_record_day_arrival_is_preserved_and_credited_no_earlier_than_ex():
    action=compile_row(row())
    assert action.shares_arrival_date==date(2023,4,27)
    assert action.pay_date==action.ex_date==date(2023,4,28)
    event=projection(action)
    assert event["shares_arrival_date"]=="2023-04-27"
    assert event["shares_arrival_at"]==event["record_at"]=="2023-04-27T15:00:00+08:00"
    assert event["share_settlement_policy"]=="actual_arrival"
    record=CorporateActionRecordPosition(action.instrument_hash,
        datetime(2023,4,27,15,tzinfo=TZ),1000,"ledger:close")
    arrival=compile_corporate_action_share_arrival(action,record_position=record,
        effective_time=record.record_time,group_id="stock",rule_hash="a"*64)[0].values()
    assert arrival["arrived_quantity"]==300
    assert arrival["sellable_delta"]==arrival["unsettled_delta"]==arrival["cash_delta_units"]==0


def test_missing_arrival_requires_explicit_policy_and_preserves_null():
    value=row(transfer_arrival_date=None,listing_date=date(2023,5,4))
    with pytest.raises(CorporateActionError,match="transfer_arrival_date"):
        compile_row(value)
    action=compile_row(value,"sellable_date_receivable")
    assert action.shares_arrival_date is None
    assert action.pay_date==action.shares_sellable_date==date(2023,5,4)
    assert CorporateAction.from_dict(action.to_dict())==action
    event=projection(action)
    assert event["shares_arrival_date"] is event["shares_arrival_at"] is None
    assert event["share_settlement_policy"]=="sellable_date_receivable"
    record=CorporateActionRecordPosition(action.instrument_hash,
        datetime(2023,4,27,15,tzinfo=TZ),1000,"ledger:close")
    compiled=compile_corporate_action(action,held_quantity=0,
        effective_time=datetime(2023,4,28,9,30,tzinfo=TZ),group_id="stock",
        rule_hash="a"*64,record_position=record)[0].values()
    assert compiled["position_entitlement_quantity"]==300
    assert compiled["position_due_date"]=="2023-05-04"
    assert compiled["shares_arrival_date"] is None
    assert compiled["cash_delta_units"]==compiled["sellable_delta"]==0


@pytest.mark.parametrize("changes",[
    dict(transfer_arrival_date=date(2023,4,26)),
    dict(listing_date=date(2023,4,27)),
    dict(transfer_arrival_date=date(2023,5,4)),
])
def test_invalid_known_dates_remain_rejected(changes):
    with pytest.raises(DomainContractError,match="顺序"):
        compile_row(row(**changes))


@pytest.mark.parametrize("changes,match",[
    (dict(listing_date=None),"listing_date"),
    (dict(note="重整转增，其中5.06股不向原股东分配",transfer_per_ten="14.82"),"实际受益比例"),
    (dict(cash_per_ten=1,cash_arrival_date=None),"cash_arrival_date"),
])
def test_receivable_policy_does_not_invent_other_missing_facts(changes,match):
    with pytest.raises(CorporateActionError,match=match):
        compile_row(row(transfer_arrival_date=None,**changes),"sellable_date_receivable")


def test_split_contract_cannot_use_missing_arrival_policy_or_pre_ex_arrival():
    action=compile_row(row())
    with pytest.raises(DomainContractError,match="顺序"):
        replace(action,kind="split")
    missing=compile_row(row(transfer_arrival_date=None),"sellable_date_receivable")
    with pytest.raises(DomainContractError,match="普通送转"):
        replace(missing,kind="split")


def test_arrival_after_ex_date_keeps_true_delivery_date():
    action=compile_row(row(transfer_arrival_date=date(2023,5,4),listing_date=date(2023,5,5)))
    assert action.pay_date==action.shares_arrival_date==date(2023,5,4)
    assert projection(action)["shares_arrival_at"]=="2023-05-04T09:30:00+08:00"


def test_receivable_shares_do_not_round_away_fractional_entitlement():
    action=compile_row(row(transfer_arrival_date=None),"sellable_date_receivable")
    record=CorporateActionRecordPosition(action.instrument_hash,
        datetime(2023,4,27,15,tzinfo=TZ),5,"ledger:close")
    with pytest.raises(CorporateActionError,match="零碎股"):
        compile_corporate_action(action,held_quantity=5,
            effective_time=datetime(2023,4,28,9,30,tzinfo=TZ),group_id="stock",
            rule_hash="a"*64,record_position=record)


def test_fully_non_shareholder_allocation_still_creates_no_stock_right():
    value=row(note="本次转增不向原股东分配")
    assert compile_cn_stock_corporate_action_rows((value,),trading_sessions=(date(2023,4,20),),
        contract_version=2,missing_date_policy="sellable_date_receivable")==()
