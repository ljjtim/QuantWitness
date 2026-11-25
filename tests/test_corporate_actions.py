from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from research_pipeline.domain import CorporateAction, DomainContractError, resolve_corporate_actions
from research_pipeline.domain.corporate_actions import CorporateActionError
from research_pipeline.domain.corporate_action_source import (
    RightsExerciseInstruction,
    compile_cn_etf_corporate_action_rows,
    compile_cn_stock_corporate_action_rows,
    compile_corporate_action,
)


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 6, 10, 9, tzinfo=TZ)
RULE = "a" * 64
INSTRUMENT = "b" * 64


def _action(kind: str, **overrides: object) -> CorporateAction:
    values = {
        "action_id": f"a-{kind}", "revision": 1, "kind": kind, "instrument_hash": INSTRUMENT,
        "announcement_available_time": datetime(2026, 5, 1, tzinfo=TZ),
        "record_date": date(2026, 6, 5), "ex_date": date(2026, 6, 8),
        "effective_date": date(2026, 6, 8), "pay_date": date(2026, 6, 10),
    }
    values.update(overrides)
    return CorporateAction(**values)  # type: ignore[arg-type]


def test_fractional_cent_cash_ratio_rounds_only_at_total_entitlement() -> None:
    action = _action("cash_dividend", cash_per_share_microunits=7_900)
    event = compile_corporate_action(
        action,
        held_quantity=100,
        effective_time=NOW,
        group_id="stock",
        rule_hash=RULE,
    )[0]
    assert event.values()["cash_receivable_units"] == 79


def test_rights_requires_explicit_visible_instruction() -> None:
    action = _action("rights", ratio_numerator=1, ratio_denominator=10, exercise_price_units=500)
    assert compile_corporate_action(action, held_quantity=100, effective_time=NOW, group_id="stock", rule_hash=RULE) == ()
    instruction = RightsExerciseInstruction(action.action_hash, 10, NOW)
    event = compile_corporate_action(action, held_quantity=100, effective_time=NOW, group_id="stock", rule_hash=RULE, instruction=instruction)[0]
    assert event.values()["cash_delta_units"] == -5_000
    with pytest.raises(CorporateActionError, match="entitlement"):
        compile_corporate_action(action, held_quantity=100, effective_time=NOW, group_id="stock", rule_hash=RULE, instruction=RightsExerciseInstruction(action.action_hash, 11, NOW))


def test_action_visibility_and_date_order_fail_closed() -> None:
    future = _action("cash_dividend", announcement_available_time=datetime(2026, 7, 1, tzinfo=TZ))
    assert resolve_corporate_actions((future,), as_of=NOW, effective_date=date(2026, 6, 8)) == ()
    with pytest.raises(DomainContractError, match="顺序"):
        _action("cash_dividend", record_date=date(2026, 6, 9), ex_date=date(2026, 6, 8))


def test_joinquant_implementation_row_compiles_cash_and_stock_entitlements() -> None:
    actions = compile_cn_stock_corporate_action_rows(
        ({
            "action_id": 1,
            "code": "000001.XSHE",
            "announcement_date": date(2026, 6, 1),
            "record_date": date(2026, 6, 4),
            "ex_date": date(2026, 6, 5),
            "stock_arrival_date": date(2026, 6, 8),
            "cash_arrival_date": date(2026, 6, 9),
            "listing_date": date(2026, 6, 8),
            "stock_dividend_per_ten": 2,
            "transfer_per_ten": 1,
            "cash_per_ten": 0.079,
            "plan_progress": "实施方案",
        },),
        trading_sessions=(
            date(2026, 6, 1),
            date(2026, 6, 2),
            date(2026, 6, 3),
            date(2026, 6, 4),
            date(2026, 6, 5),
            date(2026, 6, 8),
            date(2026, 6, 9),
        ),
    )

    assert tuple(item.kind for item in actions) == ("cash_dividend", "stock_dividend")
    assert actions[0].cash_per_share_microunits == 7_900
    assert (actions[1].ratio_numerator, actions[1].ratio_denominator) == (13, 10)
    assert actions[0].announcement_available_time.date() == date(2026, 6, 2)


def test_stock_action_without_optional_arrival_date_uses_ex_date() -> None:
    actions = compile_cn_stock_corporate_action_rows(
        ({
            "action_id": "action-no-arrival",
            "code": "000001.XSHE",
            "announcement_date": date(2024, 6, 3),
            "record_date": date(2024, 6, 4),
            "ex_date": date(2024, 6, 5),
            "stock_arrival_date": None,
            "listing_date": None,
            "cash_arrival_date": None,
            "stock_dividend_per_ten": 1.0,
            "transfer_per_ten": 0.0,
            "cash_per_ten": 0.0,
            "plan_progress": "实施方案",
        },),
        trading_sessions=(date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)),
    )

    assert len(actions) == 1
    assert actions[0].pay_date == date(2024, 6, 5)


def test_real_c2_etf_dividend_compiles_to_explicit_receivable() -> None:
    sessions = tuple(
        date(2024, 1, day) for day in (11, 12, 15, 16, 17, 18, 19, 22, 23)
    )
    actions = compile_cn_etf_corporate_action_rows(
        ({
            "action_id": 105192,
            "code": "510300",
            "publication_date": date(2024, 1, 11),
            "process_id": 405002,
            "cash_per_share": "0.06900000",
            "split_ratio": None,
            "record_date": date(2024, 1, 17),
            "ex_date": date(2024, 1, 18),
            "pay_date": date(2024, 1, 23),
            "status": 0,
            "source_added_at": datetime(2024, 1, 11, 10, 19, 42),
            "source_revised_at": datetime(2024, 1, 11, 10, 19, 42),
        },),
        trading_sessions=sessions,
        as_of=datetime(2024, 3, 29, 15, tzinfo=TZ),
    )

    assert len(actions) == 1
    action = actions[0]
    assert action.kind == "cash_dividend"
    assert action.cash_per_share_microunits == 69_000
    assert action.announcement_available_time == datetime(2024, 1, 12, 9, 15, tzinfo=TZ)
    event = compile_corporate_action(
        action,
        held_quantity=100,
        effective_time=datetime(2024, 1, 18, 9, 15, tzinfo=TZ),
        group_id="etf",
        rule_hash=RULE,
    )[0]
    assert event.values()["cash_delta_units"] == 0
    assert event.values()["cash_receivable_units"] == 690
    assert event.values()["cash_due_date"] == "2024-01-23"


def test_etf_dividend_rejects_future_revision() -> None:
    with pytest.raises(CorporateActionError, match="as_of 之后"):
        compile_cn_etf_corporate_action_rows(
            ({
                "action_id": 1,
                "code": "510300",
                "publication_date": date(2024, 1, 11),
                "process_id": 405002,
                "cash_per_share": "0.069",
                "split_ratio": None,
                "record_date": date(2024, 1, 17),
                "ex_date": date(2024, 1, 18),
                "pay_date": date(2024, 1, 23),
                "status": 0,
                "source_added_at": datetime(2024, 1, 11, 10),
                "source_revised_at": datetime(2024, 2, 1, 10),
            },),
            trading_sessions=(date(2024, 1, 11), date(2024, 1, 12), date(2024, 1, 18)),
            as_of=datetime(2024, 1, 31, 15, tzinfo=TZ),
        )


def test_etf_split_direction_requires_explicit_event_type() -> None:
    base = {
        "action_id": 2,
        "code": "510300",
        "publication_date": date(2024, 1, 11),
        "process_id": 405002,
        "cash_per_share": None,
        "split_ratio": "0.5",
        "record_date": date(2024, 1, 17),
        "ex_date": date(2024, 1, 18),
        "pay_date": date(2024, 1, 18),
        "status": 0,
        "source_added_at": datetime(2024, 1, 11, 10),
        "source_revised_at": datetime(2024, 1, 11, 10),
    }
    sessions = (date(2024, 1, 11), date(2024, 1, 12), date(2024, 1, 18))
    with pytest.raises(CorporateActionError, match="明确 event"):
        compile_cn_etf_corporate_action_rows(
            (base,),
            trading_sessions=sessions,
            as_of=datetime(2024, 1, 18, 15, tzinfo=TZ),
        )

    actions = compile_cn_etf_corporate_action_rows(
        ({**base, "event": "份额合并"},),
        trading_sessions=sessions,
        as_of=datetime(2024, 1, 18, 15, tzinfo=TZ),
    )
    assert actions[0].kind == "reverse_split"


def test_stock_post_effective_revision_is_only_allowed_for_pit_snapshot() -> None:
    row = {
        "action_id": 99,
        "code": "000001.XSHE",
        "plan_progress": "实施方案",
        "announcement_date": date(2024, 1, 4),
        "cash_per_ten": "10",
        "stock_dividend_per_ten": "0",
        "transfer_per_ten": "0",
        "record_date": date(2024, 1, 2),
        "ex_date": date(2024, 1, 3),
        "cash_arrival_date": date(2024, 1, 3),
        "stock_arrival_date": None,
        "listing_date": None,
    }
    sessions = tuple(date(2024, 1, day) for day in (2, 3, 4, 5))

    with pytest.raises(CorporateActionError, match="除权日前不可见"):
        compile_cn_stock_corporate_action_rows((row,), trading_sessions=sessions)

    actions = compile_cn_stock_corporate_action_rows(
        (row,),
        trading_sessions=sessions,
        allow_post_effective_visibility=True,
    )
    assert actions[0].announcement_available_time == datetime(
        2024, 1, 5, 9, 15, tzinfo=TZ
    )
