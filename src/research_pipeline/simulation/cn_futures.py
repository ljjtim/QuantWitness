"""连续信号到真实合约、移仓和结算的期货适配器。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from research_pipeline.domain import Instrument, require_integer_quantity
from research_pipeline.domain.time import require_aware_datetime

from .events import FinancialEvent
from .ledger import FuturesLedgerState, reduce_futures
from .margin import MarginPolicy, deterministic_liquidation_order
from .orders import SimulationContractError


@dataclass(frozen=True)
class ContinuousMapping:
    continuous_code: str
    signal_date: date
    actual_instrument: Instrument
    source_available_time: datetime
    roll_flag: bool

    def __post_init__(self) -> None:
        if not self.continuous_code.strip() or self.actual_instrument.continuous_signal_only:
            raise SimulationContractError("mapping 必须指向真实期货合约")
        require_aware_datetime(self.source_available_time, "source_available_time")


@dataclass(frozen=True)
class FuturesFeePolicy:
    policy_id: str
    open_units: int
    close_today_units: int
    close_yesterday_units: int

    def __post_init__(self) -> None:
        if not self.policy_id.strip() or min(self.open_units, self.close_today_units, self.close_yesterday_units) < 0:
            raise SimulationContractError("期货费用 policy 无效")

    def fee(self, *, offset: str, contracts: int) -> int:
        quantity = require_integer_quantity(abs(contracts), "contracts")
        rates = {"open": self.open_units, "close_today": self.close_today_units, "close_yesterday": self.close_yesterday_units}
        try:
            return quantity * rates[offset]
        except KeyError as exc:
            raise SimulationContractError("offset 必须是 open/close_today/close_yesterday") from exc


@dataclass(frozen=True)
class FuturesLeg:
    instrument_hash: str
    side: str
    contracts: int
    offset: str


def resolve_actual_contract(mappings: tuple[ContinuousMapping, ...], *, continuous_code: str, signal_date: date, as_of: datetime) -> ContinuousMapping:
    require_aware_datetime(as_of, "as_of")
    matches = tuple(item for item in mappings if item.continuous_code == continuous_code and item.signal_date == signal_date and item.source_available_time <= as_of)
    if len(matches) != 1:
        raise SimulationContractError("连续合约 mapping 缺失、重叠或未来可见")
    return matches[0]


def decompose_roll(*, previous: ContinuousMapping | None, current: ContinuousMapping, previous_contracts: int, target_contracts: int) -> tuple[FuturesLeg, ...]:
    if previous is None:
        return () if target_contracts == 0 else (FuturesLeg(current.actual_instrument.instrument_hash, "buy" if target_contracts > 0 else "sell", abs(target_contracts), "open"),)
    old_hash = previous.actual_instrument.instrument_hash
    new_hash = current.actual_instrument.instrument_hash
    if old_hash != new_hash:
        if not current.roll_flag:
            raise SimulationContractError("真实合约变化但 mapping 未声明 roll_flag")
        legs = []
        if previous_contracts:
            legs.append(FuturesLeg(old_hash, "sell" if previous_contracts > 0 else "buy", abs(previous_contracts), "close_yesterday"))
        if target_contracts:
            legs.append(FuturesLeg(new_hash, "buy" if target_contracts > 0 else "sell", abs(target_contracts), "open"))
        return tuple(legs)
    if previous_contracts and target_contracts and (previous_contracts > 0) != (target_contracts > 0):
        return (
            FuturesLeg(new_hash, "sell" if previous_contracts > 0 else "buy", abs(previous_contracts), "close_today"),
            FuturesLeg(new_hash, "buy" if target_contracts > 0 else "sell", abs(target_contracts), "open"),
        )
    delta = target_contracts - previous_contracts
    if delta == 0:
        return ()
    opening = previous_contracts == 0 or abs(target_contracts) > abs(previous_contracts)
    return (FuturesLeg(new_hash, "buy" if delta > 0 else "sell", abs(delta), "open" if opening else "close_today"),)


def settle_futures_day(
    state: FuturesLedgerState,
    *,
    event_id: str,
    instrument: Instrument,
    contracts: int,
    previous_settlement_units: int,
    settlement_units: int,
    margin_policy: MarginPolicy,
    effective_time: datetime,
    rule_hash: str,
) -> FuturesLedgerState:
    if instrument.continuous_signal_only:
        raise SimulationContractError("连续合约不能进入结算账本")
    instrument.require_tradable_on(effective_time.date())
    multiplier = int(instrument.contract_multiplier or 0)
    pnl = contracts * (settlement_units - previous_settlement_units) * multiplier
    margin = margin_policy.required_units(price_units=settlement_units, multiplier=multiplier, contracts=contracts)
    event = FinancialEvent(event_id, "mark_to_market", effective_time, effective_time.date().isoformat(), state.group.group_id, rule_hash, (("pnl_units", pnl), ("required_margin_units", margin)))
    return reduce_futures(state, event)


def margin_call_required(state: FuturesLedgerState, *, maintenance_units: int) -> bool:
    return state.equity_units < maintenance_units


__all__ = [
    "ContinuousMapping", "FuturesFeePolicy", "FuturesLeg", "decompose_roll",
    "deterministic_liquidation_order", "margin_call_required", "resolve_actual_contract", "settle_futures_day",
]
