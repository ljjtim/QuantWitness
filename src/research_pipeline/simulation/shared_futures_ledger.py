"""共享期货账户的唯一金额、双向今昨仓及订单预占状态。"""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction

from .costs import futures_fee_fen
from .ledger import FuturesAccountCore, FuturesPosition
from .margin import futures_margin_fen
from .orders import SimulationContractError


@dataclass(frozen=True)
class SharedReservation:
    instrument_id: str
    direction: str
    cash_units: int = 0
    today: int = 0
    yesterday: int = 0
    roll_plan_id: str | None = None
    opening_quantity: int = 0


class SharedFuturesLedger:
    """订单状态属于 Broker；金融状态只在本账本内确认。"""

    def __init__(self, spec: dict) -> None:
        self.instruments = {row["instrument_id"]: row for row in spec["instruments"]}
        self.cash_units = spec["initial_cash_units"]
        self.positions: dict[tuple[str, str, str], FuturesPosition] = {}
        self.prices: dict[str, int] = {}
        self.rules: dict[str, dict] = {}
        self.reservations: dict[str, SharedReservation] = {}
        self.risk_state = "normal"
        self.risk_triggered_at = None
        self.resume_after = None
        self.rolls: dict[str, dict] = {}
        self.exiting: set[str] = set()
        self.session_activity: set[tuple[str, str]] = set()
        self.settled_sessions: set[tuple[str, str]] = set()

    def quote(self, code: str, price_units: int) -> Decimal:
        return Decimal(price_units) / Decimal(self.instruments[code]["price_scale"])

    def multiplier(self, code: str) -> Decimal:
        return Decimal(str(self.instruments[code]["contract_multiplier"]))

    def margin(self, code: str, quantity: int, rule: dict | None = None, price: int | None = None) -> int:
        rule = self.rules[code] if rule is None else rule
        price = self.prices[code] if price is None else price
        return futures_margin_fen(self.quote(code, price), self.multiplier(code), quantity,
                                  Decimal(str(rule["margin_rate"])) * 100)

    def fee(self, code: str, effect: str, bucket: str, quantity: int, price: int, rule: dict) -> int:
        field = "open_fee" if effect == "open" else "close_today_fee" if bucket == "today" else "close_fee"
        value = Decimal(str(rule[field]))
        unit = "per_lot_cny"
        if rule["fee_unit"] == "notional_rate":
            unit, value = "notional_permyriad", value * 10_000
        return futures_fee_fen(self.quote(code, price), self.multiplier(code), quantity, value, fee_unit=unit)

    def mark(self, code: str, price: int, rule: dict) -> None:
        self.prices[code] = price
        self.rules[code] = rule

    def quantity(self, code: str, direction: str, bucket: str | None = None) -> int:
        return sum(abs(position.contracts) for (instrument, side, part), position in self.positions.items()
                   if instrument == code and side == direction and (bucket is None or part == bucket))

    def position_margins(self) -> dict[tuple[str, str, str], int]:
        values = {}
        for code, direction in sorted({key[:2] for key in self.positions}):
            cumulative = previous = 0
            for bucket in ("yesterday", "today"):
                key = (code, direction, bucket)
                position = self.positions.get(key)
                if position is not None:
                    cumulative += abs(position.contracts)
                    current = self.margin(code, cumulative)
                    values[key] = current - previous
                    previous = current
        return values

    def _pnl(self, code: str, position: FuturesPosition, quantity: int, price: int) -> int:
        signed = quantity if position.contracts > 0 else -quantity
        multiplier = Fraction(self.multiplier(code)) / self.instruments[code]["price_scale"]
        # 复用独立分配路径的精确成本和每事件半升金融原语。
        return position.realize(price_units=price, contracts=signed, multiplier=multiplier)[0]

    def amounts(self) -> dict[str, int]:
        unrealized = sum(self._pnl(code, position, abs(position.contracts), self.prices[code])
                         for (code, _, _), position in self.positions.items())
        margin = sum(self.position_margins().values())
        frozen = sum(item.cash_units for item in self.reservations.values())
        equity = self.cash_units + unrealized
        return dict(cash_units=self.cash_units, unrealized_pnl_units=unrealized,
                    equity_units=equity, margin_units=margin, frozen_units=frozen,
                    available_units=FuturesAccountCore.available(
                        equity_units=equity, margin_units=margin, frozen_units=frozen))

    def close_allocation(self, code: str, direction: str, effect: str, quantity: int,
                         rule: dict, *, order_id: str | None = None) -> list[tuple[str, int]]:
        if effect in {"close_today", "close_yesterday"}:
            buckets = (effect.removeprefix("close_"),)
        else:
            buckets = ("yesterday", "today") if rule["close_order"] == "yesterday_first" else ("today", "yesterday")
        result = []
        for bucket in buckets:
            reserved = sum(getattr(item, bucket) for oid, item in self.reservations.items()
                           if oid != order_id and item.instrument_id == code and item.direction == direction)
            count = min(quantity, self.quantity(code, direction, bucket) - reserved)
            if count > 0:
                result.append((bucket, count))
                quantity -= count
        return result

    def reserve(self, order_id: str, reservation: SharedReservation | None) -> list[dict]:
        previous = self.reservations.pop(order_id, None)
        if reservation is not None:
            self.reservations[order_id] = reservation
        changes = []
        for kind, field in (("margin", "cash_units"), ("today", "today"), ("yesterday", "yesterday")):
            before = getattr(previous, field) if previous else 0
            after = getattr(reservation, field) if reservation else 0
            if after != before:
                changes.append(dict(order_id=order_id, reservation_type=kind,
                                    delta_units=after - before, remaining_units=after))
        return changes

    def fill(self, code: str, direction: str, effect: str, quantity: int, price: int,
             rule: dict, *, order_id: str | None = None) -> list[dict]:
        self.mark(code, price, rule)
        if effect == "open":
            allocations = [("today", quantity)]
        else:
            allocations = self.close_allocation(code, direction, effect, quantity, rule, order_id=order_id)
            if sum(count for _, count in allocations) != quantity:
                raise SimulationContractError("平仓量超过可平今昨仓桶")
        legs = []
        multiplier = Fraction(self.multiplier(code)) / self.instruments[code]["price_scale"]
        for bucket, count in allocations:
            key = (code, direction, bucket)
            position = self.positions.get(key)
            signed = count if direction == "long" else -count
            if position is None:
                position = FuturesPosition(code, 0, price, price_scale=0,
                                           pnl_rounding_policy="half_up_per_event")
            fee = self.fee(code, effect, bucket, count, price, rule)
            posting = FuturesAccountCore.fill(
                position, contracts_delta=signed if effect == "open" else -signed,
                price_units=price, realized_contracts=0 if effect == "open" else signed,
                multiplier=multiplier, fee_units=fee, margin_units=0,
            )
            if posting.position.contracts:
                self.positions[key] = posting.position
            else:
                del self.positions[key]
            self.cash_units += posting.cash_delta_units
            notional = int((self.quote(code, price) * self.multiplier(code) * count * 100).quantize(
                Decimal(1), rounding=ROUND_HALF_UP))
            legs.append(dict(position_bucket=bucket, quantity=count, notional_units=notional,
                             fee_units=fee, realized_pnl_units=posting.realized_pnl_units))
        return legs

    def settle(self, code: str, price: int, rule: dict) -> int:
        self.mark(code, price, rule)
        total = 0
        for key, position in tuple(self.positions.items()):
            if key[0] != code:
                continue
            posting = FuturesAccountCore.settle(
                position, price_units=price,
                multiplier=Fraction(self.multiplier(code)) / self.instruments[code]["price_scale"],
                margin_units=position.margin_units,
            )
            total += posting.cash_delta_units
            self.positions[key] = posting.position
        self.cash_units += total
        return total

    def roll_session(self, code: str) -> None:
        for direction in ("long", "short"):
            today_key, yesterday_key = (code, direction, "today"), (code, direction, "yesterday")
            today = self.positions.pop(today_key, None)
            if today is None:
                continue
            yesterday = self.positions.get(yesterday_key)
            if yesterday is None:
                self.positions[yesterday_key] = today
            else:
                quantity = abs(today.contracts) + abs(yesterday.contracts)
                basis = (today.cost_price * abs(today.contracts) + yesterday.cost_price * abs(yesterday.contracts)) / quantity
                self.positions[yesterday_key] = replace(yesterday,
                    contracts=quantity if direction == "long" else -quantity,
                    cost_numerator=basis.numerator, cost_denominator=basis.denominator)

    def reserved_new_quantity(self, plan_id: str) -> int:
        return sum(item.opening_quantity for item in self.reservations.values() if item.roll_plan_id == plan_id)

    def roll_allowance(self, plan_id: str) -> int:
        plan = self.rolls[plan_id]
        return max(0, min(plan["quantity"], plan["closed_quantity"]) - plan["opened_quantity"]
                   - self.reserved_new_quantity(plan_id))
