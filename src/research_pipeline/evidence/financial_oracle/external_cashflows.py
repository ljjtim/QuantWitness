"""从冻结申请、原始行情和独立账户事实复核外部资金流。"""
from __future__ import annotations

from collections.abc import Mapping
from fractions import Fraction
from math import isclose, isfinite
from numbers import Real

from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer, ordered_rows

CONTEXT_VERSION = "research-external-cashflow-context-v1"
FLOW_KINDS = frozenset({"external_cashflow_reserved", "external_cashflow", "external_cashflow_settlement"})
METRIC_SCHEMA_ID = "research.daily-simulation.metrics.v1"


def _require(condition, message):
    if not condition:
        raise EvidenceContractError(f"外部资金流：{message}")


def _same(actual, expected, message):
    _require(actual == expected, message)


def _numeric(actual, expected, message):
    _require(isinstance(actual, Real) and not isinstance(actual, bool) and isfinite(actual)
             and isclose(float(actual), float(expected), rel_tol=1e-10, abs_tol=1e-12), message)


def _payload(event):
    pairs = event["payload"]
    _require(isinstance(pairs, (list, tuple)) and all(isinstance(pair, (list, tuple)) and len(pair) == 2 for pair in pairs), "事件载荷无效")
    result = dict(pairs)
    _same(len(result), len(pairs), "事件载荷字段重复")
    return result


class _ReturnReplay:
    """以有理数积累各投资区间，不复用生产收益实现。"""

    def __init__(self, opening):
        self.opening = integer(opening, "期初 NAV", minimum=0)
        self.anchor = opening
        self.net_flow = 0
        self.wealth = Fraction(1)
        self.peak = Fraction(1)
        self.worst = Fraction(0)
        self.funded = opening > 0
        self.invalid = False
        self.previous_flow = 0
        self.rows = []

    @property
    def status(self):
        if self.invalid:
            return "not_applicable_nonpositive_nav"
        return "applicable" if self.funded else "waiting_for_funding"

    def observe(self, nav):
        integer(nav, "观测 NAV")
        if self.anchor > 0 and nav > 0 and not self.invalid:
            self.wealth *= Fraction(nav, self.anchor)
            self.peak = max(self.peak, self.wealth)
            self.worst = min(self.worst, self.wealth / self.peak - 1)
        elif nav < 0 or self.anchor < 0 or self.anchor > 0 and nav == 0 or self.anchor == 0 and nav > 0:
            self.invalid = True
        self.anchor = nav

    def flow(self, before, after, signed):
        _same(after - before, signed, "生效前后 NAV 差不等于本笔流量")
        if signed == 0:
            return
        self.observe(before)
        self.net_flow += signed
        if after < 0:
            self.invalid = True
        if before == 0 and after > 0 and signed > 0:
            self.funded = True
        self.anchor = after

    def close(self, session, nav):
        self.observe(nav)
        row = {"session": session, "nav_units": nav,
               "net_flow_units": self.net_flow - self.previous_flow,
               "cumulative_net_flow_units": self.net_flow,
               "investment_pnl_units": nav - self.opening - self.net_flow,
               "return_status": self.status}
        if self.status == "applicable":
            row.update(net_value=float(self.wealth), drawdown=float(self.wealth / self.peak - 1))
        self.rows.append(row)
        self.previous_flow = self.net_flow

    def summary(self):
        result = {"opening_nav_units": self.opening, "closing_nav_units": self.anchor,
                  "net_external_flow_units": self.net_flow,
                  "investment_pnl_units": self.anchor - self.opening - self.net_flow,
                  "return_status": self.status}
        if self.status == "applicable":
            result.update(total_return=float(self.wealth - 1), max_drawdown=float(self.worst))
        return result


def verify_cashflow_metrics(*, summary, canonical, metric_rows):
    """按收益适用状态复核正式指标集合，未定义指标不得用零占位。"""
    expected = {}
    if summary["return_status"] == "applicable":
        expected.update({"portfolio.total_return@1.0.0": summary["total_return"],
                         "portfolio.max_drawdown@1.0.0": summary["max_drawdown"]})
    total_notional = total_fees = 0
    for row in ordered_rows(canonical["fills"], order_by=("fill_time", "fill_id")):
        total_notional += integer(row["notional_units"], "成交额", minimum=1)
        total_fees += integer(row["fee_units"], "成交费用", minimum=0)
    if summary["opening_nav_units"] > 0:
        expected["portfolio.turnover@1.0.0"] = total_notional / summary["opening_nav_units"]
    expected["portfolio.transaction_cost@1.0.0"] = total_fees / 100
    seen = set()
    for row in metric_rows:
        ref = row.get("metric_ref")
        _require(ref in expected and ref not in seen, "正式指标集合含未定义指标、重复指标或伪造零收益")
        _numeric(row.get("value"), expected[ref], "正式指标数值与独立计算不一致")
        seen.add(ref)
    _same(seen, set(expected), "正式指标缺少可计算的账户指标")


class ExternalCashflowOracle:
    """先校验冻结申请，再由独立账户回放提供每个生效时点的资产事实。"""

    def __init__(self, *, context, canonical, market_observations, market_artifact_hash,
                 account_context=None, explicit_context=None, asset_class=None, corporate_actions=(), corporate_action_records=(), credit_oracle=None):
        from datetime import time
        from zoneinfo import ZoneInfo
        from research_pipeline.platform import typed_canonical_hash

        _require(isinstance(context, Mapping), "上下文必须为映射")
        self.context = context
        self.credit_oracle = credit_oracle
        self.canonical = canonical
        self.market = market_observations
        self.market_hash = market_artifact_hash
        self.account_context = account_context
        self.explicit_context = explicit_context
        self.asset_class = asset_class
        from research_pipeline.domain import CorporateAction
        from .daily_holdings import index_corporate_action_records
        self.actions = tuple(item if isinstance(item, CorporateAction) else CorporateAction.from_dict(item) for item in corporate_actions)
        self.action_records = index_corporate_action_records(corporate_action_records)
        self.plain_entitlements = None
        required = {"contract_version", "account_id", "currency", "cash_scale", "plans", "events",
                    "flow_valuations", "return_series", "opening_nav_units", "closing_nav_units",
                    "net_external_flow_units", "investment_pnl_units", "return_status"}
        if context.get("return_status") == "applicable":
            required |= {"total_return", "max_drawdown"}
        _same(set(context), required, "上下文字段或收益状态无效")
        _same((context["contract_version"], context["currency"], context["cash_scale"]),
              (CONTEXT_VERSION, "CNY", 2), "上下文版本、币种或精度无效")
        _require(context["return_status"] in {"applicable", "waiting_for_funding", "not_applicable_nonpositive_nav"}, "收益状态无效")
        _require(isinstance(context["account_id"], str) and bool(context["account_id"]), "账户身份缺失")
        for name in ("plans", "events", "flow_valuations", "return_series"):
            _require(isinstance(context[name], list) and all(isinstance(row, Mapping) for row in context[name]), f"{name} 必须为事实列表")
        _require(bool(context["plans"]) or context["opening_nav_units"] == 0 or credit_oracle is not None, "非零账户资金流计划为空")
        fields = {"event_id", "account_id", "currency", "direction", "amount_units", "requested_at",
                  "available_at", "effective_at", "status", "reason", "source_ref"}
        self.plans = {}
        self.schedule = []
        zone = ZoneInfo("Asia/Shanghai")
        sessions = {date_value(row["session"], "估值会话") for row in canonical["valuations"]}
        if account_context is not None:
            opening = account_context["opening_snapshot"]
            _same(context["account_id"], opening["account_id"], "资金流与期初账户身份不符")
            started = aware_datetime(opening["started_at"], "期初时点")
        else:
            from datetime import datetime
            started = datetime.combine(min(sessions), time(0), zone)
            _same({row["portfolio_id"] for row in canonical["cash"]}, {context["account_id"]}, "资金流账户与正式现金表身份不符")
        for index, raw in enumerate(context["plans"]):
            _same(set(raw), fields, "冻结申请字段不完整或包含融资交易")
            for field in ("event_id", "account_id", "source_ref"):
                _require(isinstance(raw[field], str) and bool(raw[field].strip()), "冻结申请身份或来源缺失")
            _same((raw["account_id"], raw["currency"]), (context["account_id"], "CNY"), "申请账户或币种不符")
            _require(raw["direction"] in {"deposit", "withdrawal"}, "融资本金或偿还不能进入外部资金流")
            integer(raw["amount_units"], "申请金额", minimum=1)
            _require(raw["status"] in {"settled", "failed", "cancelled"}, "申请终态无效")
            _require(isinstance(raw["reason"], str) and (raw["status"] == "settled" or bool(raw["reason"].strip())), "失败或取消缺少原因")
            requested = aware_datetime(raw["requested_at"], "申请时点")
            available = aware_datetime(raw["available_at"], "可见时点")
            effective = aware_datetime(raw["effective_at"], "生效时点")
            accepted = max(requested, available)
            _require(started <= accepted <= effective, "资金流在期初之前受理或生效早于可见时点")
            local = effective.astimezone(zone)
            _require(local.date() in sessions and local.time() in {time(9, 30), time(15)}, "资金流缺少合格日频估值时点")
            _require(raw["event_id"] not in self.plans, "申请 event_id 重复")
            self.plans[raw["event_id"]] = raw
            self.schedule.extend(((accepted, index, 0, raw), (effective, index, 1, raw)))
        self.schedule.sort(key=lambda row: row[:3])
        self.cursor = 0
        self.reservations = {}
        self.rejected = {}
        self.used_events = set()
        self.flow_values = []
        self.event_by_id = {}
        self.rule_hash = typed_canonical_hash({"contract_version": "research-external-cashflow-v1", "plans": context["plans"]})
        event_fields = {"event_id", "kind", "effective_time", "session", "group_id", "rule_hash", "payload", "parent_id",
                        "ledger_sequence", "available_cash_units", "withdrawal_reserved_units", "unwithdrawable_sale_units", "total_cash_units", "state_hash"}
        previous = None
        for row in context["events"]:
            _same(set(row), event_fields, "资金事件字段不完整")
            _require(row["kind"] in FLOW_KINDS and row["event_id"] not in self.event_by_id, "资金事件类型无效或重复")
            at = aware_datetime(row["effective_time"], "资金事件时点")
            sequence = integer(row["ledger_sequence"], "账本序号", minimum=1)
            _require(previous is None or at >= previous[0] and sequence > previous[1], "资金事件账本顺序倒置")
            _same(date_value(row["session"], "资金事件会话"), at.astimezone(zone).date(), "资金事件会话不符")
            _same(row["rule_hash"], self.rule_hash, "资金事件未绑定冻结申请")
            _same(row["parent_id"], None, "资金事件不能绑定交易或融资父事件")
            _payload(row)
            for field in ("available_cash_units", "withdrawal_reserved_units", "unwithdrawable_sale_units", "total_cash_units"):
                integer(row[field], field, minimum=0)
            _require(isinstance(row["state_hash"], str) and len(row["state_hash"]) == 64
                     and all(char in "0123456789abcdef" for char in row["state_hash"]), "资金事件账户状态身份无效")
            self.event_by_id[row["event_id"]] = row
            if row["kind"] == "external_cashflow_settlement":
                self.schedule.append((at, -1, -1, row))
            previous = at, sequence
        self.schedule.sort(key=lambda row: row[:3])
        self.codes = {}
        for name in ("positions", "fills"):
            for row in canonical[name]:
                key, code = str(row["instrument_hash"]), str(row["instrument_id"])
                _require(key not in self.codes or self.codes[key] == code, "证券身份映射漂移")
                self.codes[key] = code
        self.returns = _ReturnReplay(context["opening_nav_units"])

    @property
    def next_at(self):
        return None if self.cursor == len(self.schedule) else self.schedule[self.cursor][0]

    def order_reserved_at(self, at):
        reserves = {}
        if self.explicit_context is not None:
            for event in self.explicit_context["events"]:
                if aware_datetime(event["effective_time"], "订单事件时点") >= at:
                    continue
                payload = _payload(event)
                order = payload.get("order_id")
                if event["kind"] == "cash_reserved":
                    reserves[order] = 0 if payload.get("action") == "release" else integer(payload["cash_units"], "订单现金预占", minimum=0)
                elif event["kind"] == "fill":
                    spent = payload["notional_units"] + payload["fee_units"] - payload.get("credit_drawdown", {}).get("principal_units", 0) if payload["side"] == "buy" else payload["fee_units"]
                    reserves[order] = max(0, reserves.get(order, 0) - spent)
        return sum(reserves.values())

    def sale_units_at(self, at, *, before_settlement=False):
        from datetime import datetime, time
        from zoneinfo import ZoneInfo
        sessions = {date_value(row["session"], "现金会话") for row in self.canonical["cash"]}
        cutoffs = [datetime.combine(session, time(9, 15), ZoneInfo("Asia/Shanghai")) for session in sessions]
        cutoff = max((value for value in cutoffs if value < at or value == at and not before_settlement), default=None)
        changes = []
        for row in self.canonical["fills"]:
            stamp = aware_datetime(row["fill_time"], "成交时点")
            if (cutoff is None or stamp >= cutoff) and stamp < at:
                amount = row["notional_units"] - row["fee_units"] if row["side"] == "sell" else -row["notional_units"] - row["fee_units"]
                if self.credit_oracle is not None:
                    from .credit_account import event_payload
                    fact = event_payload(self.credit_oracle.events[row["fill_id"]])
                    amount += fact.get("credit_drawdown", {}).get("principal_units", 0)
                    amount -= fact.get("credit_sale", {}).get("cash_units", 0)
                changes.append((stamp, 0, amount))
        if self.account_context is not None:
            for event in self.account_context["financial_events"]:
                stamp = aware_datetime(event["effective_time"], "扣税时点")
                if event["kind"] == "tax_collected" and (cutoff is None or stamp >= cutoff) and stamp <= at:
                    changes.append((stamp, 1, -integer(_payload(event)["cash_units"], "实际扣税", minimum=0)))
        units = 0
        for _, _, amount in sorted(changes):
            units = max(0, units + amount)
        return units

    def _event(self, identity, kind, at, values, account):
        row = self.event_by_id.get(identity)
        _require(row is not None and identity not in self.used_events, "冻结申请遗漏资金事件或重复消费")
        _same((row["kind"], aware_datetime(row["effective_time"], "资金事件时点")), (kind, at), "资金事件类型或实际时点不符")
        _same(_payload(row), values, "资金事件金额、终态、原因或来源与申请不符")
        if self.account_context is not None:
            _same(row["group_id"], self.account_context["book"]["group_id"], "资金事件执行组与账户不符")
        else:
            markets = {self.asset_class} if self.asset_class is not None else {row["asset_class"] for name in ("positions", "fills") for row in self.canonical[name]}
            _require(len(markets) == 1 and next(iter(markets)) in {"cn_stock", "cn_etf"}, "资金事件缺少唯一现货市场")
            _same(row["group_id"], f"{next(iter(markets))}-cny-daily", "资金事件执行组与账户不符")
        _same(row["available_cash_units"], account.cash - self.order_reserved_at(at), "资金事件可用现金挪用他单预占或未释放本笔冻结")
        _same(row["withdrawal_reserved_units"], sum(self.reservations.values()), "资金事件出金冻结余额不符")
        _same(row["total_cash_units"], account.total_cash(), "资金事件现金总额不守恒")
        _same(row["unwithdrawable_sale_units"], self.sale_units_at(at), "资金事件未结算卖出资金不符")
        if self.account_context is not None and self.explicit_context is not None:
            preceding = set(self.used_events)
            explicit_ids = {event["event_id"] for event in self.explicit_context["events"]}
            for event in self.account_context["financial_events"]:
                stamp = aware_datetime(event["effective_time"], "账户事件时点")
                # 开盘资金先到账；同刻订单的T+0释放即使也封存在账户上下文中，仍在其后。
                if stamp < at or stamp == at and event["event_id"] not in explicit_ids:
                    preceding.add(event["event_id"])
            for event in self.explicit_context["events"]:
                if aware_datetime(event["effective_time"], "订单事件时点") < at:
                    preceding.add(event["event_id"])
            if self.credit_oracle is not None:
                preceding.update(event["event_id"] for event in self.credit_oracle.verified_events)
                preceding.update(self.credit_oracle.used_trade_events)
            _same(row["ledger_sequence"], len(preceding) + 1, "资金事件序号与账户及订单合并账本不符")
        self.used_events.add(identity)
        return row

    def value(self, account, at):
        from datetime import time
        from zoneinfo import ZoneInfo
        column = "open" if at.astimezone(ZoneInfo("Asia/Shanghai")).time() == time(9, 30) else "close"
        positions = []
        quantities = {}
        for lot in account.lots.values():
            quantities[lot["instrument_hash"]] = quantities.get(lot["instrument_hash"], 0) + lot["quantity"]
        for key, quantity in sorted(quantities.items()):
            if not quantity:
                continue
            code = self.codes.get(key)
            raw = self.market.get((code, at.astimezone(ZoneInfo("Asia/Shanghai")).date()))
            _require(raw is not None, "流量估值缺少原始行情")
            price = integer(raw[f"{column}_price_units"], "原始估值价格", minimum=1)
            market_value = (quantity * price + 5) // 10
            positions.append({"instrument_hash": key, "code": code, "quantity": quantity,
                "price_units": price, "price_scale": 3, "market_value_units": market_value,
                "price_column": column, "observed_at": at.isoformat(), "available_at": at.isoformat(),
                "source_ref": self.market_hash})
        pending_values = []
        for key, conversion in sorted(account.conversions.items()):
            if not conversion["registered"]:
                plan = conversion["plan"]
                shares = sum(link["successor_lot"]["quantity"] for link in conversion["links"] if link["successor_lot"] is not None)
                amount = Fraction(shares * plan["interim_price_numerator"], plan["interim_price_denominator"])
                units = (2 * amount.numerator + amount.denominator) // (2 * amount.denominator)
                pending_values.append([key, units])
        account_values = {"pending_successor_units": sum(row[1] for row in pending_values), "payable_units": sum(account.payables.values())}
        if self.account_context is not None:
            account_values.update(pending_successor_values=pending_values,
                                  account_source_ref=account.opening["source"]["source_ref"])
        components = {"cash_units": account.total_cash(), "positions": positions,
                      "account": account_values, "market_data_artifact_hash": self.market_hash}
        nav = components["cash_units"] + sum(row["market_value_units"] for row in positions) + account_values["pending_successor_units"] - account_values["payable_units"]
        if self.credit_oracle is not None:
            nav -= self.credit_oracle.principal_units + self.credit_oracle.interest_units
            components["account"]["credit"] = self.credit_oracle.valuation_payload(account, at)
        return nav, f"daily_raw_{column}", components

    def credit_withdrawable(self, account, at, settled, own_reservation=0):
        """独立合并信用担保、其他提款和授信预占，不增加账户现金。"""
        if self.credit_oracle is None:
            return settled
        from .credit_account import independent_withdrawal_limit

        credit = self.credit_oracle
        value = credit.values_at(account, at)
        reserved = value["margin_reserved_units"] + sum(self.reservations.values()) - own_reservation
        return independent_withdrawal_limit(valuation=value, settled_available_units=settled,
            margin_reserved_units=reserved, withdrawal_ratio_ppm=credit.rule_at(at)["withdrawal_ratio_ppm"],
            risk_status=credit.risk_status)

    def apply_next(self, account):
        at, index, phase, plan = self.schedule[self.cursor]
        if phase == -1:
            units = integer(_payload(plan).get("cash_units"), "卖出结算金额", minimum=1)
            _same(units, self.sale_units_at(at, before_settlement=True), "卖出资金结算未扣除买入或扣税消耗")
            self._event(plan["event_id"], "external_cashflow_settlement", at, {"cash_units": units}, account)
            self.cursor += 1
            return
        identity = plan["event_id"]
        units = plan["amount_units"]
        common = {"cashflow_id": identity, "account_id": plan["account_id"], "source_ref": plan["source_ref"], "cash_units": units}
        withdrawable = max(0, account.cash - self.order_reserved_at(at) - self.sale_units_at(at) - sum(account.payables.values()))
        if phase == 0:
            if plan["direction"] == "withdrawal":
                withdrawable = self.credit_withdrawable(account, at, withdrawable)
                if units > withdrawable:
                    self.rejected[identity] = "insufficient_withdrawable_cash_at_request"
                    _require(f"external-cashflow:{identity}:request" not in self.event_by_id, "不可提款的申请被伪造为成功冻结")
                else:
                    account.cash -= units
                    self.reservations[identity] = units
                    self._event(f"external-cashflow:{identity}:request", "external_cashflow_reserved", at, common, account)
        else:
            before, source, components = self.value(account, at)
            reserved = self.reservations.get(identity, 0)
            status = "failed" if identity in self.rejected else plan["status"]
            reason = self.rejected.get(identity, plan["reason"])
            if status == "settled" and plan["direction"] == "withdrawal":
                settled = max(0, account.cash - self.order_reserved_at(at) - self.sale_units_at(at) - sum(account.payables.values()) + reserved)
                if reserved != units or units > self.credit_withdrawable(account, at, settled, reserved):
                    status, reason = "failed", "withdrawal_constraints_changed"
            signed = (units if plan["direction"] == "deposit" else -units) if status == "settled" else 0
            if plan["direction"] == "withdrawal":
                if reserved:
                    del self.reservations[identity]
                    if status != "settled":
                        account.cash += reserved
            elif status == "settled":
                account.cash += units
            after, _, _ = self.value(account, at)
            values = dict(common, direction=plan["direction"], status=status, reason=reason, reserved_before_units=reserved)
            event = self._event(f"external-cashflow:{identity}:terminal", "external_cashflow", at, values, account)
            expected = {"sequence": len(self.flow_values) + 1, "input_sequence": index, "event_id": identity,
                "effective_at": at.isoformat(), "status": status, "reason": reason, "signed_flow_units": signed,
                "nav_before_units": before, "nav_after_units": after, "valuation_source": source,
                "valuation_at": at.isoformat(), "valuation_components": components,
                "ledger_sequence_before": event["ledger_sequence"] - 1, "ledger_sequence_after": event["ledger_sequence"],
                "withdrawal_reserved_before_units": reserved, "withdrawal_reserved_after_units": 0}
            self.flow_values.append(expected)
        self.cursor += 1

    def finish(self):
        _same(self.cursor, len(self.schedule), "资金流计划未全部达到终态")
        _same(self.reservations, {}, "期末仍有未释放出金冻结")
        _same(self.context["flow_valuations"], self.flow_values, "流量估值与独立资产、原始行情、PIT时点或事件序不符")
        # 卖出资金的次日结算改变提款资格，不改变现金及 NAV。
        outstanding = 0
        for row in ordered_rows(self.canonical["cash"], order_by=("session", "snapshot_id")):
            current = date_value(row["session"], "现金会话")
            if outstanding:
                identity = f"external-cashflow:settlement:{current.isoformat()}"
                event = self.event_by_id.get(identity)
                _require(event is not None and _payload(event) == {"cash_units": outstanding}, "未结算卖出资金遗漏或伪造次日释放")
                at = aware_datetime(event["effective_time"], "卖出结算时点")
                _require(at.date() == current and (at.hour, at.minute, at.second) == (9, 15, 0), "卖出资金结算提前或使用错误会话")
                _same(event["kind"], "external_cashflow_settlement", "卖出资金结算类型不符")
                _same(event["unwithdrawable_sale_units"], 0, "卖出资金结算后仍受限")
                self.used_events.add(identity)
            from datetime import datetime, time
            from zoneinfo import ZoneInfo
            outstanding = self.sale_units_at(datetime.combine(current, time(23, 59, 59), ZoneInfo("Asia/Shanghai")))
        _same(self.used_events, set(self.event_by_id), "资金流存在无申请依据的事件或本金交易")
        expected_opening = self.context["opening_nav_units"]
        self.returns = _ReturnReplay(expected_opening)
        flows = iter(self.flow_values)
        pending = next(flows, None)
        for row in ordered_rows(self.canonical["valuations"], order_by=("valuation_time", "snapshot_id")):
            at = aware_datetime(row["valuation_time"], "收盘估值时点")
            while pending is not None and aware_datetime(pending["effective_at"], "资金生效时点") <= at:
                self.returns.flow(pending["nav_before_units"], pending["nav_after_units"], pending["signed_flow_units"])
                pending = next(flows, None)
            self.returns.close(date_value(row["session"], "估值会话").isoformat(), integer(row["nav_units"], "收盘 NAV"))
        _require(pending is None, "资金流晚于最后估值")
        expected = self.returns.summary()
        for key, value in expected.items():
            if key in {"total_return", "max_drawdown"}:
                _numeric(self.context.get(key), value, "收益汇总与独立切段结果不符")
            else:
                _same(self.context.get(key), value, "收益状态、净流量或投资损益不符")
        _same(len(self.context["return_series"]), len(self.returns.rows), "资金流收益序列缺少会话")
        for actual, wanted in zip(self.context["return_series"], self.returns.rows):
            _same(set(actual), set(wanted), "收益序列含未定义净值或遗漏事实")
            for key, value in wanted.items():
                if key in {"net_value", "drawdown"}:
                    _numeric(actual[key], value, "收益序列与独立切段连乘不符")
                else:
                    _same(actual[key], value, "收益序列状态、净流量或损益不符")
        return expected


    def _session_rows(self, name, session):
        from ..oracle_workspace import OracleTable
        table = self.canonical[name]
        if isinstance(table, OracleTable):
            return table.workspace.iter_query(f"SELECT * FROM {table.name} WHERE CAST(session AS DATE) = DATE '{session.isoformat()}'")
        return (row for row in table if date_value(row["session"], "金融会话") == session)

    def _previous_cash(self, session):
        from ..oracle_workspace import OracleTable
        table = self.canonical["cash"]
        if isinstance(table, OracleTable):
            return next(table.workspace.iter_query(f"SELECT * FROM {table.name} WHERE CAST(session AS DATE) < DATE '{session.isoformat()}' ORDER BY session DESC, snapshot_id DESC LIMIT 1"), None)
        return max((row for row in table if date_value(row["session"], "现金会话") < session),
                   key=lambda row: date_value(row["session"], "现金会话"), default=None)

    def _prepare_plain_actions(self):
        from datetime import datetime, time
        from zoneinfo import ZoneInfo
        from .daily_holdings import effective_daily_actions, registered_quantity
        sessions = sorted({date_value(row["session"], "现金会话") for row in self.canonical["cash"]})
        entitlements, position_changes, applied = [], [], set()
        for session in sessions:
            preopen = datetime.combine(session, time(9, 15), ZoneInfo("Asia/Shanghai"))
            previous = self._previous_cash(session)
            quantities = {} if previous is None else {row["instrument_hash"]: row["quantity"] for row in self._session_rows("positions", date_value(previous["session"], "现金会话"))}
            for action in effective_daily_actions(self.actions, session, preopen):
                if action.action_id in applied:
                    continue
                applied.add(action.action_id)
                held = quantities.get(action.instrument_hash, 0)
                registered = registered_quantity(action, self.action_records, preopen) if action.contract_version == 2 and action.kind != "delisting_cash" else held
                if action.kind in {"cash_dividend", "delisting_cash"}:
                    quantity = held if action.kind == "delisting_cash" else registered
                    amount = (quantity * action.cash_per_share_microunits + 5000) // 10000 if action.cash_per_share_microunits else quantity * action.cash_per_share_units
                    due = next((day for day in sessions if day >= max(session, action.pay_date)), None)
                    entitlements.append((preopen, None if due is None else datetime.combine(due, time(9, 15), ZoneInfo("Asia/Shanghai")), amount))
                if action.kind == "delisting_cash":
                    delta = -held
                elif action.kind in {"stock_dividend", "split", "reverse_split"}:
                    delta = registered * action.ratio_numerator // action.ratio_denominator - registered
                else:
                    delta = 0
                quantities[action.instrument_hash] = held + delta
                position_changes.append((preopen, action.instrument_hash, delta))
        self.plain_entitlements = entitlements
        self.plain_position_changes = position_changes

    def replay_plain_account(self):
        """从上一会话状态逐时点推进交易、公司行动与资金流，不提前消费到账。"""
        from types import SimpleNamespace
        cash_table = self.canonical["cash"]
        first = next(ordered_rows(cash_table, order_by=("session", "snapshot_id")), None)
        _require(first is not None, "缺少现金快照")
        _same(first["opening_cash_units"], self.context["opening_nav_units"], "无持仓期初 NAV 与正式现金起点不符")
        self._prepare_plain_actions()
        while self.next_at is not None:
            at = self.next_at
            session = at.date()
            previous = self._previous_cash(session)
            previous_cash = self.context["opening_nav_units"] if previous is None else previous["total_cash_units"]
            quantities = {} if previous is None else {row["instrument_hash"]: row["quantity"] for row in self._session_rows("positions", date_value(previous["session"], "现金会话"))}
            day_fills = list(self._session_rows("fills", session))
            prior_trade = 0
            for fill in day_fills:
                if aware_datetime(fill["fill_time"], "成交时点") < at:
                    prior_trade += (1 if fill["side"] == "sell" else -1) * fill["notional_units"] - fill["fee_units"]
                    key = fill["instrument_hash"]
                    quantities[key] = quantities.get(key, 0) + (1 if fill["side"] == "buy" else -1) * fill["quantity"]
            action_cash = sum(amount for recognized, _, amount in self.plain_entitlements if recognized.date() == session and recognized <= at)
            receivable = sum(amount for recognized, due, amount in self.plain_entitlements if recognized <= at and (due is None or at < due))
            for effective, key, delta in self.plain_position_changes:
                if effective.date() == session and effective <= at:
                    quantities[key] = quantities.get(key, 0) + delta
            prior_flow = sum(row["signed_flow_units"] for row in self.flow_values if aware_datetime(row["effective_at"], "流量时点").date() == session)
            total_cash = previous_cash + prior_trade + action_cash + prior_flow
            available = total_cash - receivable - sum(self.reservations.values())
            lots = {key: {"instrument_hash": key, "quantity": quantity} for key, quantity in quantities.items()}
            _require(all(row["quantity"] >= 0 for row in lots.values()), "资金流时点持仓为负")
            account = SimpleNamespace(cash=available, lots=lots, conversions={}, payables={})
            account.total_cash = lambda: account.cash + receivable + sum(self.reservations.values())
            self.apply_next(account)


def cashflow_adjusted_holdings(canonical, oracle):
    """扣除已独立验证的外部净流量，让原公司行动消费者继续核验其现金变化。"""
    daily = {}
    for row in oracle.flow_values:
        session = aware_datetime(row["effective_at"], "流量时点").date().isoformat()
        daily[session] = daily.get(session, 0) + row["signed_flow_units"]
    cash = canonical["cash"]
    from ..oracle_workspace import OracleTable
    if isinstance(cash, OracleTable):
        clauses = " ".join(f"WHEN CAST(session AS VARCHAR) = '{session}' THEN {units}" for session, units in sorted(daily.items()))
        adjustment = f"CASE {clauses} ELSE 0 END" if clauses else "0"
        name = "_p7a_holdings_cash"
        cash.workspace.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * REPLACE (non_trade_cash_change_units - ({adjustment}) AS non_trade_cash_change_units) FROM {cash.name}")
        adjusted = OracleTable(cash.workspace, name=name, row_count=len(cash), schema=cash.schema)
    else:
        adjusted = [dict(row, non_trade_cash_change_units=row["non_trade_cash_change_units"] - daily.get(date_value(row["session"], "现金会话").isoformat(), 0)) for row in cash]
    return dict(canonical, cash=adjusted)
