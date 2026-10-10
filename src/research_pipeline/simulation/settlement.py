"""分钟期货会话结算及完成收盘事件约束。"""
from __future__ import annotations
from collections.abc import Mapping
from datetime import date, datetime
from typing import TYPE_CHECKING
from research_pipeline.domain import InstrumentKey, MinuteRuleResolver, MinuteRuleSnapshotBundle, SessionCalendarResolver, load_session_policy_bundle
from .events import FinancialEvent
from .ledger import FuturesAccountCore, FuturesLedgerState, FuturesPosition, reduce_futures
from .orders import SimulationContractError
from .margin import required_futures_margin as _required_futures_margin
from .minute_rules import _ResolvedRules, resolve_minute_execution_rules, _positive_integer, _futures_margin_ppm
if TYPE_CHECKING:
    from .execution_market import MinuteExecutionBar

_FUTURES_SETTLEMENT_RULES = (
    "rule.cn_futures.contract_multiplier.v1",
    "rule.cn_futures.margin.v1",
    "rule.cn_futures.price_tick.v1",
    "rule.cn_futures.session.v1",
    "rule.cn_futures.settlement.v1",
)


def _settle_futures_session(
    *,
    state: FuturesLedgerState,
    instruments: Mapping[str, InstrumentKey],
    bars: tuple[MinuteExecutionBar, ...],
    trading_date: date,
    resolver: MinuteRuleResolver,
    bundle: MinuteRuleSnapshotBundle,
) -> tuple[
    FuturesLedgerState,
    datetime | None,
    tuple[str, ...],
    tuple[dict[str, object], ...],
]:
    """收盘后逐合约盯市，并让每个事件携带全账户聚合保证金。"""

    instrument_by_hash = {
        item.instrument_hash: item for item in instruments.values()
    }
    selected_ids = set()
    for position in state.positions:
        if position.contracts == 0:
            continue
        instrument = instrument_by_hash.get(position.instrument_hash)
        if instrument is None:
            raise SimulationContractError("期货持仓缺少 InstrumentKey，不能完成结算")
        selected_ids.add(instrument.instrument_id)
    if not selected_ids:
        return state, None, (), ()

    facts = []
    for instrument_id in sorted(selected_ids):
        instrument = instruments.get(instrument_id)
        if instrument is None:
            raise SimulationContractError("期货结算标的缺少 InstrumentKey")
        settlement_time = _futures_settlement_event_time(
            bundle,
            instrument_id=instrument_id,
            trading_date=trading_date,
        )
        rules = resolve_minute_execution_rules(
            resolver,
            bundle=bundle,
            asset_class="cn_future",
            instrument_id=instrument_id,
            effective_on=trading_date,
            as_of=settlement_time,
            required_rule_ids=_FUTURES_SETTLEMENT_RULES,
        )
        _require_futures_session_close_trigger(
            bundle=bundle,
            rules=rules,
            instrument=instrument,
            trading_date=trading_date,
            settlement_time=settlement_time,
            bars=bars,
        )
        settlement_price = _positive_integer(
            rules.parameters, "settlement_price_units"
        )
        if rules.parameters.get("settlement_availability_semantics") != (
            "session_close_event_not_supplier_timestamp"
        ):
            raise SimulationContractError("期货结算价必须声明收盘事件可见语义")
        multiplier = _positive_integer(rules.parameters, "contract_unit_kg")
        margin_ppm = _futures_margin_ppm(rules.parameters)
        position = next((
            item for item in state.positions
            if item.instrument_hash == instrument.instrument_hash
        ), FuturesPosition(instrument.instrument_hash, 0, settlement_price))
        required_margin = _required_futures_margin(
            price_units=settlement_price,
            multiplier=multiplier,
            contracts=abs(position.contracts),
            margin_ppm=margin_ppm,
        )
        posting = FuturesAccountCore.settle(
            position, price_units=settlement_price,
            multiplier=multiplier, margin_units=required_margin,
        )
        facts.append((
            settlement_time,
            instrument,
            rules,
            settlement_price,
            posting,
            required_margin,
        ))

    settlement_times = {item[0] for item in facts}
    if len(settlement_times) != 1:
        raise SimulationContractError(
            "同一账户多合约分钟结算必须使用同一收盘事件时点"
        )
    aggregate_margin = sum(item[5] for item in facts)
    settled = state
    event_hashes = []
    event_rows = []
    for (
        settlement_time,
        instrument,
        rules,
        settlement_price,
        posting,
        _required_margin,
    ) in sorted(facts, key=lambda item: (item[0], item[1].instrument_id)):
        pnl_units = posting.realized_pnl_units
        event = FinancialEvent(
            f"minute-settlement:{instrument.instrument_id}:{trading_date.isoformat()}",
            "mark_to_market",
            settlement_time,
            trading_date.isoformat(),
            settled.group.group_id,
            rules.identity_hash,
            tuple(sorted({
                "pnl_units": pnl_units,
                "required_margin_units": aggregate_margin,
            }.items())),
        )
        settled = reduce_futures(settled, event, settlement_posting=posting)
        event_hashes.append(event.event_hash)
        event_rows.append({
            "instrument_id": instrument.instrument_id,
            "instrument_hash": instrument.instrument_hash,
            "settlement_time": settlement_time.isoformat(),
            "settlement_price_units": settlement_price,
            "price_scale": _positive_integer(rules.parameters, "price_scale"),
            "position_contracts_before": next(
                item.contracts
                for item in state.positions
                if item.instrument_hash == instrument.instrument_hash
            ),
            "previous_settlement_price_units": next(
                item.settlement_price_units
                for item in state.positions
                if item.instrument_hash == instrument.instrument_hash
            ),
            "contract_multiplier": _positive_integer(
                rules.parameters, "contract_unit_kg"
            ),
            "speculative_margin_ppm": _futures_margin_ppm(rules.parameters),
            "pnl_units": pnl_units,
            "required_margin_units": _required_margin,
            "rule_hash": rules.identity_hash,
            "rule_snapshot_hashes": [
                item.rule.snapshot_hash for item in rules.bindings
            ],
            "aggregate_required_margin_units": aggregate_margin,
            "event": event.to_dict(),
            "event_hash": event.event_hash,
        })
    return (
        settled,
        max(item[0] for item in facts),
        tuple(event_hashes),
        tuple(event_rows),
    )


def _require_futures_session_close_trigger(
    *,
    bundle: MinuteRuleSnapshotBundle,
    rules: _ResolvedRules,
    instrument: InstrumentKey,
    trading_date: date,
    settlement_time: datetime,
    bars: tuple[MinuteExecutionBar, ...],
) -> None:
    """只有完整收盘 bar 才能触发当日结算，缺尾段时失败关闭。"""

    metadata = next((
        item for item in bundle.instruments
        if item.instrument_id == instrument.instrument_id
    ), None)
    if metadata is None:
        raise SimulationContractError("期货结算标的缺少 session classification")
    policy_id = str(rules.parameters.get("session_policy_id", "")).strip()
    policy_revision = _positive_integer(rules.parameters, "session_policy_revision")
    session = SessionCalendarResolver(
        load_session_policy_bundle()
    ).resolve_trading_date(
        metadata,
        trading_date,
        policy_revision=policy_revision,
    )
    if session.calendar_policy_id != policy_id:
        raise SimulationContractError("期货结算 session policy 身份漂移")
    close_time = max(
        item.ends_at for item in session.segments if item.bar_eligible is True
    )
    if settlement_time < close_time:
        raise SimulationContractError("期货结算事件早于批准 session 收盘")
    close_bars = tuple(
        item for item in bars
        if item.instrument_id == instrument.instrument_id
        and item.trading_date == trading_date
        and item.session_id == session.session_id
        and item.bar_end == close_time
        and item.completed
        and item.quality_status == "pass"
    )
    if len(close_bars) != 1:
        raise SimulationContractError("期货结算缺少 completed/pass 的 session 收盘 bar")


def _futures_settlement_event_time(
    bundle: MinuteRuleSnapshotBundle,
    *,
    instrument_id: str,
    trading_date: date,
) -> datetime:
    candidates = tuple(
        item for item in bundle.rules
        if item.instrument_id == instrument_id
        and item.rule_id == "rule.cn_futures.settlement.v1"
        and item.effective_from <= trading_date <= item.effective_to
    )
    if (
        len(candidates) != 1
        or candidates[0].status != "supported"
        or candidates[0].available_at is None
    ):
        raise SimulationContractError("期货收盘结算规则缺失、重叠或不支持")
    return candidates[0].available_at


