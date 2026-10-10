"""显式订单持续执行；订单由 Broker 持有，预占与成交只写唯一账本。"""

from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date, datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Callable

from research_pipeline.domain import Price
from research_pipeline.domain.order_stream import parse_order_commands
from .broker import Broker
from .costs import (
    CashOrderCostState,
    cash_price_amount_units,
    quote_cash_order_fee,
    confirm_cash_order_fee,
    minute_futures_fee_units,
)
from .events import FinancialEvent
from .execution_market import OpeningSnapshot
from .ledger import (
    ExecutionGroup,
    SpotLedgerState,
    FuturesLedgerState,
    reduce_spot,
    reduce_futures,
    spot_order_execution_view,
    futures_order_execution_view,
)
from .margin import required_futures_margin
from .matching import match_cash_order, match_minute_futures_order
from .orders import ORDER_TERMINAL_STATES, SimulationContractError

Ledger = SpotLedgerState | FuturesLedgerState


class ExplicitOrderExecution:
    def __init__(
        self,
        commands,
        broker: Broker,
        initial_cash_units: int,
        *,
        cash_scale: int | None = None,
        credit_hook=None,
    ):
        self.commands = parse_order_commands([item.to_dict() for item in commands])
        self.broker = broker
        self.initial_cash_units = initial_cash_units
        self.cash_scale = cash_scale
        self.credit_hook = credit_hook
        self.instruments = {
            item.instrument.instrument_id: item.instrument for item in self.commands
        }
        assets = {item.instrument.asset_class for item in self.commands}
        if not assets and credit_hook is not None:
            assets = {credit_hook.account.initial_state.group.market}
        if len(assets) != 1:
            raise SimulationContractError("显式订单必须属于同一资产账户")
        self.asset_class = next(iter(assets))
        if self.asset_class == "cn_future" and len(self.instruments) != 1:
            raise SimulationContractError("分钟期货显式订单仅支持单合约账户")
        self._cursor = 0
        self._submitted = {}
        self._costs = {}
        self._event_sequence = 0
        self._fill_sequence = {}
        self._order_rows = {}
        self.events = []
        self.observations = []
        self.fee_facts = []
        self.command_rules = []
        self.cancel_results = []
        self.session_ends = {}
        self.order_rows = []
        self.fill_rows = []
        self.cost_rows = []
        self.decision_benchmarks = []
        self.execution_observations = []
        self._session = None

    @property
    def context(self):
        return {
            "contract_version": "research-explicit-order-execution-v1",
            "commands": [item.to_dict() for item in self.commands],
            "initial_cash_units": self.initial_cash_units,
            "cash_scale": self.cash_scale,
            "events": [item.to_dict() for item in self.events],
            "observations": self.observations,
            "fee_facts": self.fee_facts,
            "command_rules": self.command_rules,
            "cancel_results": self.cancel_results,
            "session_ends": self.session_ends,
        }

    def apply_financial_event(self, state, kind, at, session, rule_hash, order_id, values):
        self._event_sequence += 1
        event_id = self._next_fill_id(order_id) if kind == "fill" else f"explicit:{order_id}:{self._event_sequence}"
        if kind == "fill" and self.credit_hook is not None:
            values = self.credit_hook.enrich_fill(state, self._submitted[order_id], values, at, event_id)
        event = FinancialEvent(
            event_id,
            kind,
            at,
            session.isoformat(),
            state.group.group_id,
            rule_hash,
            tuple(sorted({**values, "order_id": order_id}.items())),
            order_id,
        )
        updated = (
            reduce_spot(state, event)
            if isinstance(state, SpotLedgerState)
            else reduce_futures(state, event)
        )
        self.events.append(event)
        return updated

    def _state(self, state, policy, parameters):
        if state is not None:
            return state
        if policy is not None:
            return SpotLedgerState(
                ExecutionGroup(
                    f"minute-default-{self.asset_class}",
                    self.asset_class,
                    "CNY",
                    f"t{policy.settlement_days}",
                ),
                self.initial_cash_units,
            )
        return FuturesLedgerState(
            ExecutionGroup(
                "minute-default-cn-futures",
                "cn_future",
                "CNY",
                "daily-settlement",
                str(parameters["margin_policy_id"]),
            ),
            self.initial_cash_units,
        )

    def process_commands(
        self,
        *,
        through: datetime,
        session: date,
        state: Ledger | None,
        resolve: Callable,
    ):
        self._session = session
        while self._cursor < len(self.commands):
            command = self.commands[self._cursor]
            if command.submitted_at > through:
                break
            if command.trading_date > session:
                break
            if command.trading_date < session:
                raise SimulationContractError("显式订单缺少其声明交易会话的执行观察")
            self._cursor += 1
            if command.action == "cancel":
                order = self.broker.orders.get(command.order_id)
                if order is None or order.status in ORDER_TERMINAL_STATES:
                    self.cancel_results.append(
                        {
                            "command_id": command.command_id,
                            "order_id": command.order_id,
                            "status": "rejected",
                            "reason": "order_not_active",
                        }
                    )
                    continue
                self.cancel_results.append(
                    {
                        "command_id": command.command_id,
                        "order_id": command.order_id,
                        "status": "cancelled",
                        "reason": "explicit_cancel",
                    }
                )
                state = self.release_order_reservations(
                    state,
                    command.order_id,
                    command.submitted_at,
                    session,
                    self._order_rows[command.order_id]["source_order_hash"],
                )
                self.broker.advance(
                    command.order_id,
                    "cancel",
                    command.submitted_at,
                    reason="explicit_cancel",
                )
                self._sync_order(command.order_id)
                continue
            rules_hash, parameters, policy = resolve(command, command.submitted_at)
            scale = int(parameters.get("price_scale", command.reference_price.scale))
            if command.reference_price.scale != scale or (
                command.limit_price is not None and command.limit_price.scale != scale
            ):
                raise SimulationContractError("显式订单价格精度必须与当时规则一致")
            state = self._state(state, policy, parameters)
            self.command_rules.append(
                {
                    "command_id": command.command_id,
                    "rules_identity_hash": rules_hash,
                    "parameters": dict(parameters),
                    "cash_policy": None
                    if policy is None
                    else {**policy.to_dict(), "rule": policy.rule.to_dict()},
                }
            )
            order = self.broker.submit_command(command, session)
            self._submitted[order.order_id] = command
            self._costs[order.order_id] = CashOrderCostState(order.order_id, order.side)
            self._order_rows[order.order_id] = {
                "portfolio_id": "default",
                "session": session,
                "order_id": order.order_id,
                "instrument_id": command.instrument.instrument_id,
                "instrument_hash": command.instrument.instrument_hash,
                "asset_class": self.asset_class,
                "side": order.side,
                "requested_quantity": order.quantity,
                "filled_quantity": 0,
                "status": "submitted",
                "terminal_reason": None,
                "decision_time": command.decision_time,
                "submitted_at": command.submitted_at,
                "source_order_hash": command.command_hash,
            }
            self.order_rows.append(self._order_rows[order.order_id])
            self.decision_benchmarks.append(
                {
                    "portfolio_id": "default",
                    "order_id": order.order_id,
                    "decision_price_units": command.reference_price.units,
                    "available_at": command.reference_price_available_at,
                    "source_hash": command.command_hash,
                }
            )
            event_count = len(self.events)
            try:
                state = self._reserve(
                    state,
                    command,
                    order.quantity,
                    command.reference_price,
                    command.submitted_at,
                    session,
                    rules_hash,
                    parameters,
                    policy,
                )
            except SimulationContractError as exc:
                del self.events[event_count:]
                self.broker.advance(
                    order.order_id, "reject", command.submitted_at, reason=str(exc)
                )
            else:
                self.broker.advance(order.order_id, "accept", command.submitted_at)
            self._sync_order(order.order_id)
        return state

    def _reserve(
        self,
        state,
        command,
        quantity,
        reference,
        at,
        session,
        rules_hash,
        parameters,
        policy,
    ):
        if isinstance(state, SpotLedgerState):
            if self.credit_hook is not None:
                price = command.limit_price or self._price(command, reference, parameters)
                reserved = self.credit_hook.reserve(self, state, command, quantity, price, at, session, rules_hash, policy)
                if reserved is not None:
                    return reserved
            if command.side == "sell":
                price = command.limit_price or self._price(
                    command, reference, parameters
                )
                notional = cash_price_amount_units(
                    price, quantity, cash_scale=self.cash_scale
                )
                fee = quote_cash_order_fee(
                    policy,
                    self._costs[command.order_id],
                    fill_id=f"estimate:{self._event_sequence}",
                    notional_units=notional,
                ).fee_units
                state = self.apply_financial_event(
                    state,
                    "cash_reserved",
                    at,
                    session,
                    rules_hash,
                    command.order_id,
                    {"cash_units": max(0, fee - notional)},
                )
                return self.apply_financial_event(
                    state,
                    "position_reserved",
                    at,
                    session,
                    rules_hash,
                    command.order_id,
                    {
                        "instrument_hash": command.instrument.instrument_hash,
                        "quantity": quantity,
                    },
                )
            price = command.limit_price or self._price(command, reference, parameters)
            notional = cash_price_amount_units(
                price, quantity, cash_scale=self.cash_scale
            )
            fee = quote_cash_order_fee(
                policy,
                self._costs[command.order_id],
                fill_id=f"estimate:{self._event_sequence}",
                notional_units=notional,
            ).fee_units
            required = notional + fee
            if command.funds_policy == "resize":
                own = state.reservation_for(command.order_id)
                required = min(
                    required,
                    state.available_cash_units + (0 if own is None else own.cash_units),
                )
            return self.apply_financial_event(
                state,
                "cash_reserved",
                at,
                session,
                rules_hash,
                command.order_id,
                {"cash_units": required},
            )
        price = command.limit_price or self._price(command, reference, parameters)
        multiplier = int(parameters["contract_unit_kg"])
        fee = minute_futures_fee_units(
            parameters,
            position_effect=command.position_effect,
            notional_units=price.units * quantity * multiplier,
        )
        position = next(
            (
                item
                for item in state.positions
                if item.instrument_hash == command.instrument.instrument_hash
            ),
            None,
        )
        sign = 1 if command.side == "buy" else -1
        if position is not None and position.contracts:
            if (
                command.position_effect == "open"
                and position.contracts * sign < 0
                or command.position_effect != "open"
                and position.contracts * sign > 0
            ):
                raise SimulationContractError("显式开平方向与当前持仓不一致")
        if command.position_effect == "open":
            margin = required_futures_margin(
                price_units=price.units,
                multiplier=multiplier,
                contracts=quantity,
                margin_ppm=int(parameters["speculative_initial_margin_ppm"]),
            )
            if command.funds_policy == "resize":
                own = state.reservation_for(command.order_id)
                available = state.free_equity_units + (
                    0 if own is None else own.cash_units + own.margin_units
                )
                fee = min(fee, available)
                margin = min(margin, max(0, available - fee))
            return self.apply_financial_event(
                state,
                "cash_reserved",
                at,
                session,
                rules_hash,
                command.order_id,
                {"cash_units": fee, "margin_units": margin},
            )
        effects = self._close_legs(state, command, quantity)
        updated = state
        for effect, amount in effects:
            updated = self.apply_financial_event(
                updated,
                "position_reserved",
                at,
                session,
                rules_hash,
                command.order_id,
                {
                    "instrument_hash": command.instrument.instrument_hash,
                    "position_effect": effect,
                    "quantity": amount,
                },
            )
        return self.apply_financial_event(
            updated,
            "cash_reserved",
            at,
            session,
            rules_hash,
            command.order_id,
            {"cash_units": fee, "margin_units": 0},
        )

    @staticmethod
    def _price(command, reference, parameters):
        scale = int(parameters.get("price_scale", reference.scale))
        if reference.scale != scale or (
            command.limit_price is not None and command.limit_price.scale != scale
        ):
            raise SimulationContractError("显式订单价格精度必须与当时规则一致")
        tick = parameters.get("price_tick_units", 1)
        if type(tick) is not int or tick <= 0:
            raise SimulationContractError("显式订单缺少合法报价单位")
        direction = 1 if command.side == "buy" else -1
        value = Decimal(reference.units) * (
            1 + direction * Decimal(str(command.slippage_bps)) / 10000
        )
        value += direction * command.slippage_ticks * tick
        rounding = ROUND_CEILING if direction == 1 else ROUND_FLOOR
        units = int((value / tick).to_integral_value(rounding=rounding)) * tick
        if units <= 0:
            raise SimulationContractError("滑点后价格必须为正")
        return Price(units, scale, reference.currency)

    def execute(
        self,
        *,
        instrument,
        event_start,
        event_time,
        session,
        reference_price,
        visible_capacity,
        rules_identity_hash,
        parameters,
        cash_policy,
        state,
    ):
        if state is None:
            return state
        self.observations.append(
            {
                "instrument": instrument.to_dict(),
                "event_start": event_start.isoformat(),
                "event_time": event_time.isoformat(),
                "session": session.isoformat(),
                "reference_price": reference_price.to_dict(),
                "visible_capacity": visible_capacity,
                "rules_identity_hash": rules_identity_hash,
                "parameters": dict(parameters),
                "cash_policy": None
                if cash_policy is None
                else {**cash_policy.to_dict(), "rule": cash_policy.rule.to_dict()},
            }
        )
        remaining_capacity = visible_capacity
        for order_id, command in self._submitted.items():
            order = self.broker.orders.get(order_id)
            if (
                order is None
                or order.status in ORDER_TERMINAL_STATES
                or command.trading_date != session
                or command.instrument != instrument
                or command.submitted_at > event_start
                or command.submitted_at >= event_time
            ):
                continue
            price = self._price(command, reference_price, parameters)
            quantity = order.quantity - order.filled_quantity
            limited = command.limit_price is not None and (
                price.units > command.limit_price.units
                if command.side == "buy"
                else price.units < command.limit_price.units
            )
            if isinstance(state, FuturesLedgerState):
                low, high = (
                    parameters.get("low_limit_units"),
                    parameters.get("high_limit_units"),
                )
                if low is None or high is None:
                    raise SimulationContractError("期货显式订单缺少当时价格限制")
                limited = limited or not int(low) <= price.units <= int(high)
            reason = "limit_price_not_reached" if limited else None
            filled = 0
            if not limited and remaining_capacity > 0:
                if isinstance(state, SpotLedgerState):
                    state, filled, reason = self._execute_spot(
                        state,
                        command,
                        quantity,
                        price,
                        remaining_capacity,
                        event_time,
                        session,
                        rules_identity_hash,
                        parameters,
                        cash_policy,
                    )
                else:
                    state, filled, reason = self._execute_future(
                        state,
                        command,
                        quantity,
                        price,
                        remaining_capacity,
                        event_time,
                        session,
                        rules_identity_hash,
                        parameters,
                    )
            remaining_capacity -= filled
            order = self.broker.orders[order_id]
            if order.status == "filled":
                state = self.release_order_reservations(
                    state, order_id, event_time, session, rules_identity_hash
                )
            elif (
                reason in {"insufficient_cash", "insufficient_margin"}
                and command.funds_policy == "reject"
            ):
                state = self.release_order_reservations(
                    state, order_id, event_time, session, rules_identity_hash
                )
                self.broker.advance(
                    order_id,
                    "cancel" if order.filled_quantity else "reject",
                    event_time,
                    reason=reason,
                )
            elif order.time_in_force == "IOC":
                state = self.release_order_reservations(
                    state, order_id, event_time, session, rules_identity_hash
                )
                self.broker.advance(
                    order_id, "cancel", event_time,
                    reason="invalid_lot" if order.filled_quantity == 0 and reason == "invalid_lot"
                    else "ioc_remainder_cancelled",
                )
            elif filled:
                event_count = len(self.events)
                try:
                    state = self._reserve(
                        state,
                        command,
                        order.quantity - order.filled_quantity,
                        price,
                        event_time,
                        session,
                        rules_identity_hash,
                        parameters,
                        cash_policy,
                    )
                except SimulationContractError:
                    del self.events[event_count:]
                    state = self.release_order_reservations(
                        state, order_id, event_time, session, rules_identity_hash
                    )
                    self.broker.advance(
                        order_id,
                        "cancel",
                        event_time,
                        reason="remaining_reservation_unaffordable",
                    )
            self._sync_order(order_id)
        return state

    def _execute_spot(
        self,
        state,
        command,
        quantity,
        price,
        capacity,
        at,
        session,
        rules_hash,
        parameters,
        policy,
    ):
        own_view = spot_order_execution_view(state, command.order_id) if self.credit_hook is None else self.credit_hook.quote_view(state, command)
        order = replace(
            self.broker.orders[command.order_id],
            quantity=quantity,
            filled_quantity=0,
            status="created",
        )
        policy = replace(
            policy,
            slippage_units_per_share=0,
            cash_shortage_policy="clip_current_lot_continue_v1"
            if command.funds_policy == "resize"
            else "reject_v1",
        )
        high = parameters.get("high_limit_units")
        low = parameters.get("low_limit_units")
        snapshot = OpeningSnapshot(
            command.instrument.instrument_hash,
            price,
            None if high is None else Price(int(high), price.scale, "CNY"),
            None if low is None else Price(int(low), price.scale, "CNY"),
            bool(parameters.get("paused", False)),
            capacity,
            at,
        )
        cost = self._costs[command.order_id]
        fill_id = self._next_fill_id(command.order_id)

        def quote(amount):
            return quote_cash_order_fee(
                policy, cost, fill_id=fill_id, notional_units=amount
            )

        candidate = match_cash_order(
            order,
            policy=policy,
            snapshot=snapshot,
            state=own_view,
            execution_at=at,
            cash_scale=self.cash_scale,
            fee_quote=lambda amount: quote(amount).fee_units,
        )
        if not candidate.filled_quantity:
            return state, 0, candidate.reason_code
        fee = quote(candidate.notional_units)
        if (
            command.side == "sell"
            and fee.fee_units > candidate.notional_units + own_view.available_cash_units
        ):
            return state, 0, "insufficient_cash"
        state = self.apply_financial_event(
            state,
            "fill",
            at,
            session,
            rules_hash,
            command.order_id,
            {
                "instrument_hash": command.instrument.instrument_hash,
                "side": command.side,
                "quantity": candidate.filled_quantity,
                "notional_units": candidate.notional_units,
                "fee_units": fee.fee_units,
                "execution_price_units": price.units,
                "price_scale": price.scale,
            },
        )
        if self.credit_hook is not None:
            self.credit_hook.record_account_fill(self.events[-1], settlement_days=policy.settlement_days)
        self._costs[command.order_id] = confirm_cash_order_fee(cost, fee)
        self.fee_facts.append(
            {
                "fill_id": fill_id,
                "rule_hash": rules_hash,
                "before": asdict(fee.before),
                "after": asdict(fee.after),
                "fee_units": fee.fee_units,
                "commission_units": fee.commission_units,
                "transfer_units": fee.transfer_units,
                "tax_units": fee.tax_units,
            }
        )
        fill_event = self.events[-1]
        if command.side == "buy" and policy.settlement_days == 0:
            state = self.apply_financial_event(
                state,
                "settlement",
                at,
                session,
                rules_hash,
                command.order_id,
                {
                    "cash_units": 0,
                    "instrument_hash": command.instrument.instrument_hash,
                    "quantity": candidate.filled_quantity,
                },
            )
        self._fill(
            command,
            candidate.filled_quantity,
            price,
            fee.fee_units,
            0,
            at,
            session,
            fill_id,
            fill_event.event_hash,
            1,
            candidate.notional_units,
            capacity,
            command.position_effect,
        )
        return state, candidate.filled_quantity, candidate.reason_code

    def _close_legs(self, state, command, quantity):
        if command.position_effect != "close":
            return [(command.position_effect, quantity)]
        yesterday = state.available_position_quantity(
            command.instrument.instrument_hash,
            position_effect="close_yesterday",
            order_id=command.order_id,
        )
        old = min(quantity, yesterday)
        return [
            (effect, amount)
            for effect, amount in (
                ("close_yesterday", old),
                ("close_today", quantity - old),
            )
            if amount
        ]

    def _execute_future(
        self,
        state,
        command,
        quantity,
        price,
        capacity,
        at,
        session,
        rules_hash,
        parameters,
    ):
        total = 0
        reason = None
        legs = (
            [("open", quantity)]
            if command.position_effect == "open"
            else self._close_legs(state, command, quantity)
        )
        for effect, requested in legs:
            requested = min(requested, capacity - total)
            if requested <= 0:
                break
            key = command.instrument.instrument_hash
            if effect != "open":
                requested = min(
                    requested,
                    state.available_position_quantity(
                        key, position_effect=effect, order_id=command.order_id
                    ),
                )
            if not requested:
                break
            view = futures_order_execution_view(state, command.order_id)
            kwargs = dict(
                instrument_hash=key,
                side=command.side,
                position_effect="open" if effect == "open" else "close",
                visible_capacity=capacity - total,
                price_units=price.units,
                multiplier=int(parameters["contract_unit_kg"]),
                margin_ppm=int(parameters["speculative_initial_margin_ppm"]),
                parameters=parameters,
                state=view,
            )
            candidate = match_minute_futures_order(quantity=requested, **kwargs)
            if (
                not candidate.filled_quantity
                and command.funds_policy == "resize"
                and effect == "open"
            ):
                lo, hi = 0, requested
                while lo < hi:
                    mid = (lo + hi + 1) // 2
                    if match_minute_futures_order(
                        quantity=mid, **kwargs
                    ).filled_quantity:
                        lo = mid
                    else:
                        hi = mid - 1
                if lo:
                    candidate = match_minute_futures_order(quantity=lo, **kwargs)
            if not candidate.filled_quantity:
                reason = candidate.reason_code
                break
            fill_id = self._next_fill_id(command.order_id)
            state = self.apply_financial_event(
                state,
                "fill",
                at,
                session,
                rules_hash,
                command.order_id,
                {
                    "instrument_hash": key,
                    "contracts_delta": candidate.contracts_delta,
                    "position_effect": effect,
                    "settlement_price_units": price.units,
                    "fee_units": candidate.fee_units,
                    "position_margin_units": candidate.position_margin_units,
                    "multiplier": int(parameters["contract_unit_kg"]),
                },
            )
            event = self.events[-1]
            self._fill(
                command,
                candidate.filled_quantity,
                price,
                candidate.fee_units,
                candidate.realized_pnl_units,
                at,
                session,
                fill_id,
                event.event_hash,
                int(parameters["contract_unit_kg"]),
                price.units
                * candidate.filled_quantity
                * int(parameters["contract_unit_kg"]),
                capacity,
                effect,
            )
            total += candidate.filled_quantity
        return state, total, reason

    def _next_fill_id(self, order_id):
        return f"{order_id}:fill:{self._fill_sequence.get(order_id, 0) + 1}"

    def _fill(
        self,
        command,
        quantity,
        price,
        fee,
        pnl,
        at,
        session,
        fill_id,
        source_hash,
        multiplier,
        notional,
        capacity,
        effect,
    ):
        self._fill_sequence[command.order_id] = (
            self._fill_sequence.get(command.order_id, 0) + 1
        )
        self.broker.advance(command.order_id, "fill", at, quantity=quantity)
        self.fill_rows.append(
            {
                "portfolio_id": "default",
                "session": session,
                "fill_id": fill_id,
                "order_id": command.order_id,
                "instrument_id": command.instrument.instrument_id,
                "instrument_hash": command.instrument.instrument_hash,
                "asset_class": self.asset_class,
                "side": command.side,
                "quantity": quantity,
                "fill_time": at,
                "execution_price_units": price.units,
                "price_scale": price.scale,
                "contract_multiplier": multiplier,
                "notional_units": notional,
                "fee_units": fee,
                "realized_pnl_units": pnl,
                "position_effect": effect,
                "source_fill_hash": source_hash,
            }
        )
        self.cost_rows.append(
            {
                "portfolio_id": "default",
                "session": session,
                "cost_id": fill_id + ":fee",
                "fill_id": fill_id,
                "cost_type": "transaction_fee",
                "amount_units": fee,
                "currency": "CNY",
                "source_cost_hash": source_hash,
            }
        )
        self.execution_observations.append(
            {
                "source_fill_id": fill_id,
                "arrival_price_units": price.units,
                "arrival_price_available_at": at,
                "visible_capacity": capacity,
                "capacity_available_at": at,
            }
        )

    def release_order_reservations(self, state, order_id, at, session, rules_hash):
        if state is None:
            return state
        if state.reservation_for(order_id) is not None:
            state = self.apply_financial_event(state, "cash_reserved", at, session, rules_hash, order_id, {"action": "release"})
        if self.credit_hook is not None and state.credit_state.reservation_for(order_id) is not None:
            state = self.apply_financial_event(state, "credit_reserved", at, session, rules_hash, order_id, {"action": "release"})
        return state

    @property
    def submitted_commands(self):
        return tuple(self._submitted.values())

    def cost_state_for(self, order_id):
        return self._costs[order_id]

    def cancel_active_order(self, state, order_id, *, at, session, rules_hash, reason):
        """取消活动订单并同步释放该订单的现金、持仓与信用预占。"""
        order = self.broker.orders[order_id]
        if order.status in ORDER_TERMINAL_STATES:
            return state
        state = self.release_order_reservations(state, order_id, at, session, rules_hash)
        self.broker.advance(order_id, "cancel", at, reason=reason)
        self._sync_order(order_id)
        return state

    def submit_system_command(self, command, *, session, state, resolve):
        """风险命令复用普通命令的预占、状态机和撮合。"""
        commands, cursor = self.commands, self._cursor
        try:
            self.commands, self._cursor = (command,), 0
            return self.process_commands(through=command.submitted_at, session=session, state=state, resolve=resolve)
        finally:
            self.commands, self._cursor = commands, cursor

    def _sync_order(self, order_id):
        order = self.broker.orders[order_id]
        row = self._order_rows[order_id]
        summary = (
            "filled"
            if order.filled_quantity == order.quantity
            else "partially_filled"
            if order.filled_quantity
            else "rejected"
        )
        row.update(
            filled_quantity=order.filled_quantity,
            status=summary,
            terminal_reason=order.rejection_code,
        )

    def close_session(self, session, at, state):
        self.session_ends[session.isoformat()] = at.isoformat()
        for order_id, command in self._submitted.items():
            order = self.broker.orders.get(order_id)
            if (
                order is None
                or command.trading_date != session
                or order.status in ORDER_TERMINAL_STATES
            ):
                continue
            state = self.release_order_reservations(state, order_id, at, session, command.command_hash)
            action = "expire" if order.time_in_force == "DAY" else "cancel"
            self.broker.advance(order_id, action, at, reason="session_end")
            self._sync_order(order_id)
        return state

    def drain_session(self):
        self.order_rows = []
        self.fill_rows = []
        self.cost_rows = []
        self.decision_benchmarks = []
        self.execution_observations = []

    def require_finished(self):
        if self._cursor != len(self.commands):
            raise SimulationContractError("显式命令在输入行情窗口内没有可见执行会话")
