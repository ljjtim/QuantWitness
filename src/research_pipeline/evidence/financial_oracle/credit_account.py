"""从信用声明与逐笔事实独立复算本金、利息、偿还和信用估值。"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, time, timedelta
from fractions import Fraction
from zoneinfo import ZoneInfo

from ..errors import EvidenceContractError
from .common import aware_datetime, date_value, integer

DEFINITION_VERSION = "research-credit-account-v1"
CONTEXT_VERSION = "research-credit-account-context-v1"
VALUATION_MODEL = "cash_plus_positions_and_credit_liabilities_v1"
REPAYMENT_LINK_POLICY = "proportional_principal_keep_quantity"
POLICIES = {
    "profile_id": "sse-cny-financing-cash-securities-v1",
    "repayment_link_policy": REPAYMENT_LINK_POLICY,
    "interest_policy": "actual_days_open_inclusive_repay_exclusive_floor_with_remainder",
    "risk_sell_policy": "maturity_then_contract_then_instrument",
    "sale_settlement_policy": "next_session_preopen",
}
SHANGHAI = ZoneInfo("Asia/Shanghai")
PPM = 1_000_000

FIELDS = {
    "definition": "contract_version account_id currency cash_scale as_of rules contracts position_links order_allocations instructions profile_id repayment_link_policy interest_policy risk_sell_policy sale_settlement_policy",
    "rule": "rule_id effective_start effective_end available_at source_ref assumption_source_ref eligible_instruments collateral_rates exchange_margin_ppm broker_margin_ppm maintenance_ratio_ppm withdrawal_ratio_ppm concentration_ppm credit_limit_units annual_rate_ppm interest_day_basis max_contract_days risk_grace_days repayment_order agreement_fact_kind remedy_ratio_ppm stock_risk_facts",
    "contract": "contract_id opened_at maturity_date opening_rule_id principal_units interest_units interest_remainder_numerator interest_remainder_denominator last_accrual_date source_ref",
    "link": "link_id contract_id instrument_hash lot_id quantity acquisition_units principal_units source_ref receivable_id successor_right_id",
    "allocation": "order_id contract_id financing_limit_units own_cash_limit_units maturity_date source_ref",
    "instruction": "instruction_id kind contract_id amount_units requested_at available_at effective_at status new_maturity_date approval_at approval_source_ref source_ref",
}


def require(condition, message):
    if not condition:
        raise EvidenceContractError(f"信用独立复核：{message}")


def identity(value, label):
    require(isinstance(value, str) and bool(value.strip()), f"{label}缺少身份")
    return value


def object_fields(value, fields, label):
    require(isinstance(value, Mapping) and set(value) == set(fields.split()), f"{label}字段不完整或含未知字段")
    return dict(value)


def indexed(rows, field, label):
    require(isinstance(rows, (list, tuple)), f"{label}必须为数组")
    result = {}
    for row in rows:
        require(isinstance(row, Mapping), f"{label}必须包含对象")
        key = identity(row.get(field), field)
        require(key not in result, f"{label}身份重复")
        result[key] = dict(row)
    return result


def local_time(value):
    return aware_datetime(value, "信用时点").astimezone(SHANGHAI)


def validate_credit_definition(value):
    """独立校验完整声明；不导入生产解析器、计息或风控实现。"""
    definition = object_fields(value, FIELDS["definition"], "信用声明")
    require(definition["contract_version"] == DEFINITION_VERSION, "信用声明版本无效")
    require(definition["currency"] == "CNY" and type(definition["cash_scale"]) is int and definition["cash_scale"] == 2,
            "信用币种或金额精度无效")
    identity(definition["account_id"], "account_id")
    require(all(definition[key] == value for key, value in POLICIES.items()), "未支持的信用协议")
    as_of = local_time(definition["as_of"])
    rules = indexed(definition["rules"], "rule_id", "信用规则")
    require(bool(rules), "信用规则不能为空")
    for rule in rules.values():
        object_fields(rule, FIELDS["rule"], "信用规则")
        start = date_value(rule["effective_start"], "规则起始日")
        end = None if rule["effective_end"] is None else date_value(rule["effective_end"], "规则截止日")
        require(end is None or end >= start, "规则有效区间倒置")
        local_time(rule["available_at"])
        for field in ("source_ref", "assumption_source_ref"):
            identity(rule[field], field)
        for field in ("exchange_margin_ppm", "broker_margin_ppm", "maintenance_ratio_ppm", "withdrawal_ratio_ppm",
                      "concentration_ppm", "credit_limit_units", "annual_rate_ppm", "max_contract_days", "risk_grace_days"):
            integer(rule[field], field, minimum=0)
        require(min(rule["exchange_margin_ppm"], rule["broker_margin_ppm"], rule["max_contract_days"]) > 0,
                "开仓比例及合同期限必须为正")
        require(type(rule["interest_day_basis"]) is int and rule["interest_day_basis"] in (360, 365), "计息年基准无效")
        require(0 < rule["concentration_ppm"] <= PPM, "集中度比例无效")
        require(rule["withdrawal_ratio_ppm"] >= max(3 * PPM, rule["maintenance_ratio_ppm"]), "信用提款比例低于适用门槛")
        require(isinstance(rule["repayment_order"], (list, tuple)) and len(rule["repayment_order"]) == 2
                and set(rule["repayment_order"]) == {"interest", "principal"}, "还款顺序不完整")
        eligible = rule["eligible_instruments"]
        require(isinstance(eligible, (list, tuple)), "融资标的必须为数组")
        for key in eligible:
            identity(key, "融资标的")
        require(len(set(eligible)) == len(eligible), "融资标的重复")
        rates = indexed(rule["collateral_rates"], "instrument_hash", "折算率")
        for rate in rates.values():
            object_fields(rate, "instrument_hash rate_ppm exchange_cap_ppm security_category static_pe_ppm category_source_ref", "折算率")
            verify_collateral_rate(rate)
            require(integer(rate["rate_ppm"], "折算率", minimum=0)
                    <= integer(rate["exchange_cap_ppm"], "交易所折算上限", minimum=0) <= PPM, "折算率超过交易所上限")
        require(rule["agreement_fact_kind"] in {"agreement", "explicit_research_assumption"}, "协议来源类型无效")
        require(integer(rule["remedy_ratio_ppm"], "补足比例", minimum=0) >= rule["maintenance_ratio_ppm"], "补足比例低于维持比例")
        require(isinstance(rule["stock_risk_facts"], (list, tuple)), "股票集中度事实必须为数组")
        risk_keys = set()
        for fact in rule["stock_risk_facts"]:
            object_fields(fact, "instrument_hash session previous_session available_at market_collateral_ratio_ppm static_pe_ppm source_ref client_security_value_units client_asset_units client_debt_units", "股票集中度事实")
            key = (fact["instrument_hash"], date_value(fact["session"], "股票风险会话"))
            require(key not in risk_keys, "股票集中度会话事实重复")
            risk_keys.add(key)
            require(date_value(fact["previous_session"], "前一会话") < key[1], "股票集中度引用当日或未来会话")
            local_time(fact["available_at"])
            identity(fact["source_ref"], "股票风险来源")
            require(integer(fact["market_collateral_ratio_ppm"], "全市场单股担保比例", minimum=0) <= PPM, "全市场担保比例无效")
            integer(fact["static_pe_ppm"], "静态市盈率")
    contracts = indexed(definition["contracts"], "contract_id", "期初合约")
    for contract in contracts.values():
        object_fields(contract, FIELDS["contract"], "期初合约")
        identity(contract["source_ref"], "合同来源")
        opened = local_time(contract["opened_at"])
        last = date_value(contract["last_accrual_date"], "计息边界")
        maturity = date_value(contract["maturity_date"], "到期日")
        require(opened <= as_of and opened.date() <= last <= as_of.date() and maturity >= opened.date(), "期初债务日期无效")
        for field in ("principal_units", "interest_units", "interest_remainder_numerator"):
            integer(contract[field], field, minimum=0)
        denominator = integer(contract["interest_remainder_denominator"], "余数分母", minimum=1)
        require(contract["interest_remainder_numerator"] < denominator, "计息余数应小于1分")
        rule = rules.get(contract["opening_rule_id"])
        require(rule is not None and rule_visible(rule, opened), "期初开仓规则引用或可见性无效")
        check_exchange_margin(rule, opened)
    links = indexed(definition["position_links"], "link_id", "期初债务关联")
    for link in links.values():
        object_fields(link, FIELDS["link"], "债务关联")
        for field in ("instrument_hash", "lot_id", "source_ref"):
            identity(link[field], field)
        require(link["contract_id"] in contracts, "关联引用未知期初合同")
        require(link["receivable_id"] is None or link["successor_right_id"] is None, "应收与后继权益关联不能并存")
        for field in ("receivable_id", "successor_right_id"):
            if link[field] is not None:
                identity(link[field], field)
        for field in ("quantity", "acquisition_units", "principal_units"):
            integer(link[field], field, minimum=0)
        require(not link["quantity"] or link["principal_units"] <= link["acquisition_units"], "关联本金超过取得金额")
    for key, contract in contracts.items():
        require(sum(link["principal_units"] for link in links.values() if link["contract_id"] == key) == contract["principal_units"],
                "期初关联本金与合同不一致")
    allocations = indexed(definition["order_allocations"], "order_id", "融资分配")
    for row in allocations.values():
        object_fields(row, FIELDS["allocation"], "融资分配")
        identity(row["contract_id"], "融资合同")
        identity(row["source_ref"], "分配来源")
        integer(row["financing_limit_units"], "融资限额", minimum=1)
        integer(row["own_cash_limit_units"], "自有限额", minimum=0)
        date_value(row["maturity_date"], "分配到期日")
        require(row["contract_id"] not in contracts, "新增融资不得并入期初存量合同")
    require(len({row["contract_id"] for row in allocations.values()}) == len(allocations), "多个订单不能复用新融资合同")
    instructions = indexed(definition["instructions"], "instruction_id", "信用指令")
    for row in instructions.values():
        object_fields(row, FIELDS["instruction"], "信用指令")
        identity(row["source_ref"], "指令来源")
        require(row["kind"] in {"repay", "extend"} and row["status"] in {"approved", "cancelled", "rejected"}, "指令类型或状态无效")
        if row["contract_id"] is not None:
            identity(row["contract_id"], "指令合同")
        requested, available, effective = (local_time(row[field]) for field in ("requested_at", "available_at", "effective_at"))
        require(max(requested, available) <= effective and effective >= as_of, "信用指令在受理或账户起点前生效")
        if row["kind"] == "repay":
            integer(row["amount_units"], "还款金额", minimum=1)
            require(all(row[field] is None for field in ("new_maturity_date", "approval_at", "approval_source_ref")), "还款不能含展期批准")
        else:
            require(type(row["amount_units"]) is int and row["amount_units"] == 0 and row["contract_id"] is not None,
                    "展期须指定合同且金额为零")
            date_value(row["new_maturity_date"], "展期日")
            if row["status"] == "approved":
                require(local_time(row["approval_at"]) <= effective, "展期批准尚不可见")
                identity(row["approval_source_ref"], "批准来源")
    require(len([rule for rule in rules.values() if rule_visible(rule, as_of)]) == 1, "期初规则缺失、重叠或尚不可见")
    opening_rule = next(rule for rule in rules.values() if rule_visible(rule, as_of))
    check_exchange_margin(opening_rule, as_of)
    return deepcopy(definition)


def verify_collateral_rate(rate):
    """按证券历史类别及静态PE核对交易所担保折算上限。"""
    caps = {"sse180_stock": 700000, "other_a_stock": 650000, "risk_warning_stock": 0, "delisting_stock": 0, "etf": 900000}
    category = rate["security_category"]
    require(category in caps, "担保证券类别未覆盖")
    if category != "etf":
        identity(rate["category_source_ref"], "担保历史分类来源")
    pe = rate["static_pe_ppm"]
    if category in {"sse180_stock", "other_a_stock"}:
        integer(pe, "担保静态市盈率")
    elif pe is not None:
        integer(pe, "担保静态市盈率")
    cap = caps[category]
    if category != "etf" and pe is not None and (pe >= 300000000 or pe < 0):
        cap = 0
    require(rate["exchange_cap_ppm"] == cap and 0 <= rate["rate_ppm"] <= cap, "折算上限与历史类别或静态PE不符")


def check_exchange_margin(rule, at):
    at = local_time(at)
    require(at >= datetime(2023, 2, 17, tzinfo=SHANGHAI), "信用profile未覆盖该历史日期")
    minimum = 800000 if datetime(2023, 9, 8, 15, tzinfo=SHANGHAI) <= at < datetime(2026, 1, 19, tzinfo=SHANGHAI) else PPM
    require(rule["exchange_margin_ppm"] >= minimum, "开仓比例低于当时官方最低比例")


def rule_visible(rule, at):
    at = local_time(at)
    return (local_time(rule["available_at"]) <= at
            and date_value(rule["effective_start"], "规则起始日") <= at.date()
            and (rule["effective_end"] is None or at.date() <= date_value(rule["effective_end"], "规则截止日")))


class CreditAccountOracle:
    """信用债务的独立有理数账本，不含生产Ledger状态或计算函数。"""

    def __init__(self, definition):
        self.definition = validate_credit_definition(definition)
        self.rules = indexed(self.definition["rules"], "rule_id", "规则")
        self.contracts = indexed(self.definition["contracts"], "contract_id", "合同")
        self.links = indexed(self.definition["position_links"], "link_id", "关联")
        self.allocations = indexed(self.definition["order_allocations"], "order_id", "分配")
        self.reservations = {}
        self.origins = {}
        self.instructions_used = set()
        self.interest_history = []

    @property
    def principal_units(self):
        return sum(row["principal_units"] for row in self.contracts.values())

    @property
    def interest_units(self):
        return sum(row["interest_units"] for row in self.contracts.values())

    def rule_at(self, at):
        selected = [rule for rule in self.rules.values() if rule_visible(rule, at)]
        require(len(selected) == 1, "事件规则缺失、重叠或尚不可见")
        rule = selected[0]
        check_exchange_margin(rule, at)
        return rule

    def reserve(self, *, order_id, principal_units, margin_units, own_cash_units, at):
        rule = self.rule_at(at)
        require(order_id in self.allocations, "预占缺少融资分配")
        allocation = self.allocations[order_id]
        for value in (principal_units, margin_units, own_cash_units):
            integer(value, "预占", minimum=0)
        require(principal_units <= allocation["financing_limit_units"] and own_cash_units <= allocation["own_cash_limit_units"],
                "预占超出声明分配")
        other = sum(row["principal_units"] for key, row in self.reservations.items() if key != order_id)
        require(self.principal_units + other + principal_units <= rule["credit_limit_units"], "授信预占超额")
        self.reservations[order_id] = {"order_id": order_id, "contract_id": allocation["contract_id"],
            "principal_units": principal_units, "margin_units": margin_units, "own_cash_units": own_cash_units}

    def release(self, order_id):
        self.reservations.pop(order_id, None)

    def drawdown(self, *, order_id, principal_units, own_cash_units, notional_units, quantity,
                 instrument_hash, lot_id, link_id, source_ref, at):
        at = local_time(at)
        rule = self.rule_at(at)
        reserve = self.reservations.get(order_id)
        require(reserve is not None, "融资成交没有本单预占")
        for value in (principal_units, own_cash_units, notional_units):
            integer(value, "成交金额", minimum=0)
        integer(quantity, "融资取得数量", minimum=1)
        require(instrument_hash in rule["eligible_instruments"], "成交证券不具备当时融资资格")
        require(principal_units == min(notional_units, reserve["principal_units"])
                and principal_units + own_cash_units == notional_units and own_cash_units <= reserve["own_cash_units"],
                "成交未遵守融资／自有分配或超过预占")
        require(link_id not in self.links, "融资成交关联重复")
        for value in (lot_id, link_id, source_ref):
            identity(value, "成交关联")
        allocation = self.allocations[order_id]
        self.accrue_interest(at.date())
        key = allocation["contract_id"]
        maturity = date_value(allocation["maturity_date"], "成交到期日")
        require(at.date() <= maturity <= at.date() + timedelta(days=rule["max_contract_days"]), "新增融资期限无效")
        if key in self.contracts:
            require(self.origins.get(key) == order_id and self.contracts[key]["opening_rule_id"] == rule["rule_id"],
                    "新成交并入存量或不同开仓比例合同")
            self.contracts[key]["principal_units"] += principal_units
        else:
            self.contracts[key] = {"contract_id": key, "opened_at": at.isoformat(), "maturity_date": maturity.isoformat(),
                "opening_rule_id": rule["rule_id"], "principal_units": principal_units, "interest_units": 0,
                "interest_remainder_numerator": 0, "interest_remainder_denominator": 1,
                "last_accrual_date": at.date().isoformat(), "source_ref": source_ref}
            self.origins[key] = order_id
        self.links[link_id] = {"link_id": link_id, "contract_id": key, "instrument_hash": instrument_hash,
            "lot_id": lot_id, "quantity": quantity, "acquisition_units": notional_units,
            "principal_units": principal_units, "source_ref": source_ref, "receivable_id": None, "successor_right_id": None}
        reserve["principal_units"] -= principal_units
        reserve["own_cash_units"] -= own_cash_units
        self.verify_links()

    def verify_links(self):
        require(all(row["contract_id"] in self.contracts for row in self.links.values()), "关联引用未知合同")
        for key, contract in self.contracts.items():
            require(sum(row["principal_units"] for row in self.links.values() if row["contract_id"] == key)
                    == contract["principal_units"], "关联本金与合同未还本金不守恒")

    def accrue_interest(self, through):
        """计提[last_accrual_date, through)，逐合同保留不足1分余数。"""
        through = date_value(through, "计息截止日")
        facts = []
        for key in sorted(self.contracts):
            contract = self.contracts[key]
            start = date_value(contract["last_accrual_date"], "计息边界")
            require(through >= start, "计息边界倒退")
            remainder = Fraction(contract["interest_remainder_numerator"], contract["interest_remainder_denominator"])
            for offset in range((through - start).days):
                day = start + timedelta(days=offset)
                opened = local_time(contract["opened_at"])
                at = opened if day == opened.date() else datetime.combine(day, time.min, SHANGHAI)
                rule = self.rule_at(at)
                before = remainder
                charge = Fraction(contract["principal_units"] * rule["annual_rate_ppm"], PPM * rule["interest_day_basis"])
                accumulated = charge + remainder
                units = accumulated.numerator // accumulated.denominator
                remainder = accumulated - units
                facts.append({"contract_id": key, "from_date": day.isoformat(),
                    "through_date": (day + timedelta(days=1)).isoformat(), "principal_units": contract["principal_units"],
                    "annual_rate_ppm": rule["annual_rate_ppm"], "interest_day_basis": rule["interest_day_basis"],
                    "rule_id": rule["rule_id"], "interest_units": units,
                    "remainder_before_numerator": before.numerator, "remainder_before_denominator": before.denominator,
                    "remainder_numerator": remainder.numerator, "remainder_denominator": remainder.denominator})
                contract["interest_units"] += units
            contract.update(last_accrual_date=through.isoformat(), interest_remainder_numerator=remainder.numerator,
                            interest_remainder_denominator=remainder.denominator)
        self.interest_history.extend(facts)
        return facts

    def repay(self, *, amount_units, repayment_order, available_cash_units, link_policy, at,
              contract_id=None):
        integer(amount_units, "还款金额", minimum=1)
        integer(available_cash_units, "可供本笔还款现金", minimum=0)
        require(amount_units <= available_cash_units, "还款挪用其他预占或受限现金")
        require(len(repayment_order) == 2 and set(repayment_order) == {"interest", "principal"}, "还款顺序不完整")
        require(link_policy == REPAYMENT_LINK_POLICY, "还款关联分配缺少支持的命名协议")
        require(contract_id is None or contract_id in self.contracts, "还款引用未知合同")
        self.accrue_interest(local_time(at).date())
        remaining, facts = amount_units, []
        contracts = sorted(self.contracts.values(), key=lambda row: (date_value(row["maturity_date"], "到期日"), row["contract_id"]))
        for contract in contracts:
            if not remaining or contract_id is not None and contract["contract_id"] != contract_id:
                continue
            paid = {"interest": 0, "principal": 0}
            original_principal = contract["principal_units"]
            original_interest = contract["interest_units"]
            for part in repayment_order:
                field = part + "_units"
                paid[part] = min(remaining, contract[field])
                contract[field] -= paid[part]
                remaining -= paid[part]
            links = sorted((row for row in self.links.values() if row["contract_id"] == contract["contract_id"]), key=lambda row: row["link_id"])
            releases = [paid["principal"] * row["principal_units"] // original_principal
                        if original_principal else 0 for row in links]
            remainder = paid["principal"] - sum(releases)
            for index in range(len(links) - 1, -1, -1):
                extra = min(remainder, links[index]["principal_units"] - releases[index])
                releases[index] += extra
                remainder -= extra
            require(remainder == 0, "还款整数余数无法分配")
            for row, released in zip(links, releases):
                row["principal_units"] -= released
            if paid["interest"] or paid["principal"]:
                facts.append({"contract_id": contract["contract_id"], "principal_paid_units": paid["principal"],
                    "interest_paid_units": paid["interest"], "principal_before_units": original_principal,
                    "interest_before_units": original_interest, "principal_after_units": contract["principal_units"],
                    "interest_after_units": contract["interest_units"]})
        self.links = {key: row for key, row in self.links.items() if row["quantity"] or row["principal_units"]}
        self.verify_links()
        return amount_units - remaining, facts

    def extend(self, instruction_id):
        row = next((row for row in self.definition["instructions"] if row["instruction_id"] == instruction_id), None)
        require(row is not None and row["kind"] == "extend" and instruction_id not in self.instructions_used, "展期指令未知或重复")
        require(row["contract_id"] in self.contracts, "展期引用未知合同")
        self.instructions_used.add(instruction_id)
        if row["status"] != "approved":
            return False
        at = local_time(row["effective_at"])
        rule = self.rule_at(at)
        contract = self.contracts[row["contract_id"]]
        maturity = date_value(row["new_maturity_date"], "新到期日")
        require(date_value(contract["maturity_date"], "原到期日") >= at.date()
                and at.date() < maturity <= at.date() + timedelta(days=rule["max_contract_days"]), "展期批准或期限无效")
        contract["maturity_date"] = maturity.isoformat()
        return True


def independent_credit_valuation(*, cash_units, security_value_units, receivable_units, other_liability_units,
                                principal_units, interest_units, own_collateral, financing_components,
                                formula_fee_units=0):
    """分别复算会计净资产、第41条保证金和第43条维持比例。"""
    for value in (cash_units, security_value_units, receivable_units, other_liability_units,
                  principal_units, interest_units, formula_fee_units):
        integer(value, "估值金额", minimum=0)
    own_value = Fraction(0)
    for row in own_collateral:
        value = integer(row["value_units"], "自有担保品市值", minimum=0)
        rate = integer(row["collateral_ppm"], "担保折算率", minimum=0)
        require(rate <= PPM, "担保折算率超过100%")
        own_value += Fraction(value * rate, PPM)
    gain, requirement, finance_value = Fraction(0), Fraction(0), 0
    for row in financing_components:
        value = integer(row["value_units"], "融资关联市值", minimum=0)
        basis = integer(row["acquisition_units"], "未了结融资取得额", minimum=0)
        rate = integer(row["collateral_ppm"], "融资证券折算率", minimum=0)
        margin = integer(row["margin_ppm"], "关联开仓比例", minimum=1)
        require(rate <= PPM, "融资证券折算率超过100%")
        finance_value += value
        gain += Fraction((value - basis) * rate, PPM) if value >= basis else value - basis
        requirement += Fraction(basis * margin, PPM)
    require(sum(row["value_units"] for row in own_collateral) + finance_value <= security_value_units,
            "自有担保品与融资关联重复计入证券市值")
    margin = cash_units + own_value + gain - requirement - interest_units - formula_fee_units
    assets = cash_units + security_value_units
    debt = principal_units + interest_units + formula_fee_units
    return {"net_asset_units": assets + receivable_units - principal_units - interest_units - other_liability_units,
            "margin_available": margin, "collateral_value": own_value,
            "maintenance_ratio_numerator": assets, "maintenance_ratio_denominator": debt,
            "maintenance_ratio": None if debt == 0 else Fraction(assets, debt)}


def independent_withdrawal_limit(*, valuation, settled_available_units, margin_reserved_units,
                                withdrawal_ratio_ppm, risk_status="normal"):
    integer(settled_available_units, "已结算可提款现金", minimum=0)
    integer(margin_reserved_units, "其他保证金预占", minimum=0)
    require(integer(withdrawal_ratio_ppm, "提款比例", minimum=0) >= 3 * PPM, "提款协议低于300%")
    require(risk_status in {"normal", "notified", "liquidating", "default"}, "风险状态无效")
    if risk_status != "normal":
        return 0
    assets, debt = valuation["maintenance_ratio_numerator"], valuation["maintenance_ratio_denominator"]
    margin = valuation["margin_available"] - margin_reserved_units
    if debt:
        threshold = Fraction(withdrawal_ratio_ppm * debt, PPM)
        if assets <= threshold:
            return 0
        upper = min(Fraction(settled_available_units), margin, assets - threshold)
    else:
        upper = min(Fraction(settled_available_units), margin)
    upper = max(Fraction(0), upper)
    return upper.numerator // upper.denominator


def verify_stock_concentration(*, position_value_units, account_collateral_value_units, agreement_ppm, at,
                               maintenance_ratio_numerator, maintenance_ratio_denominator, facts):
    """按静态市盈率及含边界的强制组合条件复核股票融资。"""
    integer(position_value_units, "单股市值", minimum=0)
    integer(account_collateral_value_units, "账户担保物市值", minimum=0)
    require(position_value_units <= account_collateral_value_units, "单股市值超过账户担保物市值")
    require(0 < integer(agreement_ppm, "协议集中度", minimum=1) <= PPM, "协议集中度无效")
    require(isinstance(facts, Mapping), "股票集中度缺少强制组合事实")
    fields = "instrument_hash session previous_session available_at market_collateral_ratio_ppm static_pe_ppm source_ref client_security_value_units client_asset_units client_debt_units"
    object_fields(facts, fields, "股票集中度事实")
    at = local_time(at)
    require(local_time(facts["available_at"]) <= at
            and date_value(facts["previous_session"], "集中度来源会话") < at.date()
            and date_value(facts["session"], "集中度适用会话") == at.date(),
            "股票集中度事实尚不可见或会话不匹配")
    identity(facts["source_ref"], "集中度来源")
    market_ratio = integer(facts["market_collateral_ratio_ppm"], "全市场单股担保比例", minimum=0)
    require(market_ratio <= PPM, "全市场担保比例超过100%")
    pe = Fraction(integer(facts["static_pe_ppm"], "静态市盈率"), PPM)
    customer_ratio = Fraction(position_value_units, account_collateral_value_units) if account_collateral_value_units else Fraction(0)
    require(customer_ratio <= Fraction(agreement_ppm, PPM), "超过协议集中度")
    integer(maintenance_ratio_denominator, "维持比例债务", minimum=0)
    integer(maintenance_ratio_numerator, "维持比例担保资产", minimum=0)
    prior_security = integer(facts["client_security_value_units"], "前一会话客户单证券市值", minimum=0)
    prior_assets = integer(facts["client_asset_units"], "前一会话客户现金证券总值", minimum=0)
    prior_debt = integer(facts["client_debt_units"], "前一会话客户债务", minimum=0)
    require(prior_security <= prior_assets, "前一会话单证券市值超过客户资产")
    triggered = (market_ratio >= 250000 and (pe >= 300 or pe < 0)
                 and prior_security * 10 >= prior_assets * 7 and prior_assets > 0
                 and prior_debt > 0 and prior_assets <= 3 * prior_debt)
    require(not triggered, "股票强制组合集中度触发暂停融资")


__all__ = ["CreditAccountOracle", "validate_credit_definition", "independent_credit_valuation",
           "independent_withdrawal_limit", "verify_stock_concentration", "verify_collateral_rate", "CreditContextOracle", "fill_by_id",
           "REPAYMENT_LINK_POLICY", "VALUATION_MODEL"]



def event_payload(event):
    pairs = event.get("payload")
    require(isinstance(pairs, (list, tuple)) and all(isinstance(row, (list, tuple)) and len(row) == 2 for row in pairs), "信用事件载荷无效")
    values = dict(pairs)
    require(len(values) == len(pairs), "信用事件载荷字段重复")
    return values


def fill_by_id(rows, fill_id):
    """只查询一笔已封存成交，不把OracleTable转成全量列表。"""
    from ..oracle_workspace import OracleTable
    if isinstance(rows, OracleTable):
        found = list(rows.workspace.iter_query(f"SELECT * FROM {rows.name} WHERE fill_id = ? LIMIT 2", (fill_id,)))
    else:
        found = [row for row in rows if row["fill_id"] == fill_id]
    require(len(found) == 1, "信用事件缺少唯一canonical成交")
    return found[0]


class CreditContextOracle(CreditAccountOracle):
    """从封存事件和独立权益账本复核日频信用上下文。"""

    def __init__(self, *, context, canonical, account_context, market_observations, market_artifact_hash):
        required = {"contract_version", "account_id", "currency", "cash_scale", "definition", "events", "valuations",
                    "snapshots", "opening_nav_units", "closing_nav_units", "closing_state", "risk_status",
                    "risk_commands", "interest_accrued_units", "interest_paid_units"}
        require(isinstance(context, Mapping) and set(context) == required and context["contract_version"] == CONTEXT_VERSION,
                "信用上下文schema或版本无效")
        super().__init__(context["definition"])
        require(context["account_id"] == self.definition["account_id"] == account_context["opening_snapshot"]["account_id"], "信用账户身份不一致")
        require(context["currency"] == "CNY" and type(context["cash_scale"]) is int and context["cash_scale"] == 2, "信用金额尺度无效")
        require(local_time(self.definition["as_of"]) == local_time(account_context["opening_snapshot"]["started_at"]), "信用与权益起点不一致")
        self.context, self.canonical, self.account_context = context, canonical, account_context
        self.market, self.market_hash = market_observations, market_artifact_hash
        from research_pipeline.domain import InstrumentKey
        self.codes = {InstrumentKey(code, "cn_etf" if code.startswith(("5", "1")) else "cn_stock", "XSHG" if code.endswith("XSHG") else "XSHE", "CNY", "etf" if code.startswith(("5", "1")) else "stock").instrument_hash: code for code, _ in self.market}
        self.events = indexed(context["events"], "event_id", "信用来源事件")
        previous = local_time(self.definition["as_of"])
        for event in self.events.values():
            at = local_time(event["effective_time"])
            require(at >= previous and date_value(event["session"], "信用事件会话") == at.date(), "信用事件时间倒置或会话不符")
            previous = at
            event_payload(event)
        self.schedule = [row for row in self.events.values() if row["kind"].startswith("credit_")]
        self.cursor = 0
        self.sale_claims = {}
        self.risk_status, self.notified_at, self.remedy_date = "normal", None, None
        self.used_trade_events = set()
        self.adjustments = {}
        self.interest_paid = 0
        self.opening_marks = {row["instrument_hash"]: row for row in account_context["opening_marks"]}
        self.snapshot_by_session = indexed(context["snapshots"], "session", "信用会话快照")
        self.verified_events = []
        self.risk_history = []

    @property
    def next_at(self):
        return None if self.cursor == len(self.schedule) else local_time(self.schedule[self.cursor]["effective_time"])

    @property
    def priority(self):
        if self.cursor == len(self.schedule):
            return 0
        event = self.schedule[self.cursor]
        if event["kind"] == "credit_sale_settled" and event_payload(event).get("cash_already_settled"):
            return 3.5
        if event["kind"] == "credit_reserved" and event_payload(event).get("action") == "release":
            return 6
        return {"credit_interest": -5, "credit_sale_settled": -4, "credit_reserved": 0.5,
                "credit_repayment": 3.5, "credit_extension": 3.5, "credit_risk": 7}.get(self.schedule[self.cursor]["kind"], 3.5)

    def bind_account(self, account):
        require(local_time(account.opening["started_at"]) == local_time(self.definition["as_of"]), "信用起点不符")
        net = account.opening_nav - self.principal_units - self.interest_units
        require(net >= 0 and self.context["opening_nav_units"] == net, "信用期初净NAV未独立扣除P/I")
        opening = [row for row in self.context["valuations"] if row.get("observation_kind") == "opening"]
        require(len(opening) == 1, "信用起点估值必须唯一")
        self.verify_valuation(account, opening[0])

    def price_fact_at(self, key, at):
        """从当时已完成的开盘／收盘观测和期初价格重建来源明细。"""
        at = local_time(at)
        code = self.codes.get(key)
        candidates = []
        for (instrument, day), raw in self.market.items():
            if instrument != code or day > at.date():
                continue
            for column, clock in (("open", time(9, 30)), ("close", time(15))):
                observed = datetime.combine(day, clock, SHANGHAI)
                if observed <= at:
                    units = integer(raw[f"{column}_price_units"], "信用原始估值价格", minimum=1)
                    price = Fraction(units, 10)
                    fact = {"price_units": units, "price_scale": 3,
                            "observed_at": observed.isoformat(), "available_at": observed.isoformat(),
                            "source_ref": self.market_hash,
                            "price_numerator_units": price.numerator, "price_denominator": price.denominator}
                    candidates.append((observed, observed, fact))
        mark = self.opening_marks.get(key)
        if mark is not None:
            available = local_time(mark["source"]["available_at"])
            observed = local_time(mark["observed_at"])
            if observed <= available <= at:
                price = Fraction(mark["price_numerator_units"], mark["price_denominator"])
                fact = {"observed_at": mark["observed_at"], "available_at": mark["source"]["available_at"],
                        "source_ref": mark["source"]["source_ref"],
                        "price_numerator_units": price.numerator, "price_denominator": price.denominator}
                candidates.append((observed, available, fact))
        require(bool(candidates), "信用估值没有当时可见价格")
        return max(candidates, key=lambda row: row[:2])[2]

    def price_at(self, key, at):
        fact = self.price_fact_at(key, at)
        return Fraction(fact["price_numerator_units"], fact["price_denominator"])

    def values_at(self, account, at):
        at = local_time(at)
        rule = self.rule_at(at)
        rates = {row["instrument_hash"]: row["rate_ppm"] for row in rule["collateral_rates"]}
        quantities = {}
        for lot in account.lots.values():
            quantities[lot["instrument_hash"]] = quantities.get(lot["instrument_hash"], 0) + lot["quantity"]
        positions = []
        for key, quantity in sorted(quantities.items()):
            if not quantity:
                continue
            fact = self.price_fact_at(key, at)
            amount = quantity * Fraction(fact["price_numerator_units"], fact["price_denominator"])
            positions.append({"instrument_hash": key, "quantity": quantity,
                "market_value_units": (2 * amount.numerator + amount.denominator) // (2 * amount.denominator), **fact})
        security = sum(row["market_value_units"] for row in positions)
        own = sum(Fraction(quantity) * self.price_at(key, at) * Fraction(rates.get(key, 0), PPM) for key, quantity in quantities.items())
        gain, requirement = Fraction(0), Fraction(0)
        financing_links = []
        for link in self.links.values():
            if not link["principal_units"]:
                continue
            principal = link["principal_units"]
            fraction = Fraction(principal, link["acquisition_units"]) if link["acquisition_units"] else Fraction(0)
            require(fraction <= 1 or not link["quantity"], "融资份额超出来源取得额")
            value = Fraction(0)
            if not link["receivable_id"] and not link["successor_right_id"] and link["quantity"]:
                value = link["quantity"] * self.price_at(link["instrument_hash"], at) * fraction
                own -= value * Fraction(rates.get(link["instrument_hash"], 0), PPM)
            elif link["receivable_id"]:
                claim = self.sale_claims.get(link["receivable_id"])
                require(claim is not None, "融资应收关联没有受限卖款")
                related = [row for row in self.links.values() if row["receivable_id"] == link["receivable_id"]]
                total = sum(row["principal_units"] for row in related)
                value = Fraction(claim["cash_units"] * principal, total) if total else Fraction(0)
            elif link["successor_right_id"]:
                conversion = next((row for row in account.conversions.values() if ("successor:" + row["plan"]["conversion_id"]) == link["successor_right_id"]), None)
                require(conversion is not None, "融资后继权益关联没有原始换股事实")
                value = link["quantity"] * Fraction(conversion["plan"]["interim_price_numerator"], conversion["plan"]["interim_price_denominator"]) * fraction
            collateral = Fraction(rates.get(link["instrument_hash"], 0), PPM)
            profit = (value - principal) * collateral if value >= principal else value - principal
            gain += profit
            opening = self.rules[self.contracts[link["contract_id"]]["opening_rule_id"]]
            margin_ppm = max(opening["exchange_margin_ppm"], opening["broker_margin_ppm"])
            requirement += Fraction(principal * margin_ppm, PPM)
            financing_links.append({"link_id": link["link_id"], "contract_id": link["contract_id"],
                "principal_units": principal, "value_numerator": value.numerator, "value_denominator": value.denominator,
                "profit_numerator": profit.numerator, "profit_denominator": profit.denominator,
                "margin_ppm": margin_ppm, "collateral_rate_ppm": rates.get(link["instrument_hash"], 0),
                "receivable_id": link["receivable_id"], "successor_right_id": link["successor_right_id"]})
        pending = sum(Fraction(sum(link["successor_lot"]["quantity"] for link in row["links"] if link["successor_lot"] is not None)
                               * row["plan"]["interim_price_numerator"], row["plan"]["interim_price_denominator"])
                      for row in account.conversions.values() if not row["registered"])
        pending_units = (2 * pending.numerator + pending.denominator) // (2 * pending.denominator)
        receivable = sum(account.receivables.values()) + pending_units
        cash = account.total_cash() - sum(account.receivables.values())
        liabilities = sum(account.payables.values())
        margin = cash + own + gain - requirement - self.interest_units - liabilities
        return {"at": at.isoformat(), "session": at.date().isoformat(), "rule_id": rule["rule_id"],
                "positions": positions, "financing_links": financing_links,
                "cash_units": cash, "security_value_units": security, "receivable_units": receivable,
                "principal_units": self.principal_units, "interest_units": self.interest_units,
                "other_liability_units": liabilities, "net_asset_units": cash + security + receivable - self.principal_units - self.interest_units - liabilities,
                "collateral_value_units": own.numerator // own.denominator, "margin_available_units": margin.numerator // margin.denominator,
                "margin_available": margin, "margin_numerator": margin.numerator, "margin_denominator": margin.denominator,
                "margin_reserved_units": sum(row["margin_units"] for row in self.reservations.values()),
                "credit_reserved_units": sum(row["principal_units"] for row in self.reservations.values()),
                "maintenance_ratio_numerator": cash + security, "maintenance_ratio_denominator": self.principal_units + self.interest_units,
                "own_collateral_numerator": own.numerator, "own_collateral_denominator": own.denominator,
                "financing_profit_numerator": gain.numerator, "financing_profit_denominator": gain.denominator,
                "financing_margin_numerator": requirement.numerator, "financing_margin_denominator": requirement.denominator,
                "risk_status": self.risk_status}

    def verify_valuation(self, account, row):
        values = self.values_at(account, row["at"])
        for field, expected in values.items():
            if field == "margin_available":
                continue
            if field in {"positions", "financing_links"}:
                key = "instrument_hash" if field == "positions" else "link_id"
                require(indexed(row.get(field), key, field) == indexed(expected, key, field),
                        f"信用估值{field}的逐项金额、价格来源或本金关联不符")
            else:
                require(row.get(field) == expected, f"信用估值{field}与独立事实不一致")
        return values

    def apply_next(self, account):
        event = self.schedule[self.cursor]
        values, at = event_payload(event), local_time(event["effective_time"])
        kind = event["kind"]
        if kind == "credit_interest":
            expected = self.accrue_interest(at.date())
            def accrual_key(row):
                return (row["contract_id"], row["from_date"], row["through_date"])
            require(sorted(values["accruals"], key=accrual_key) == sorted(expected, key=accrual_key),
                    "信用利息事实、可见利率或余数不符")
        elif kind == "credit_reserved":
            if values.get("action") == "release":
                self.release(values["order_id"])
            else:
                reservation = values["reservation"]
                self.reserve(at=at, **{key: reservation[key] for key in ("order_id", "principal_units", "margin_units", "own_cash_units")})
                require(values["credit_limit_units"] == self.rule_at(at)["credit_limit_units"], "授信预占引用错误额度")
        elif kind in {"credit_repayment", "credit_sale_settled"}:
            rule = self.rule_at(at)
            require(values["repayment_order"] == list(rule["repayment_order"]), "偿还顺序不符")
            requested = values["cash_units"]
            if kind == "credit_repayment":
                instruction = next((row for row in self.definition["instructions"] if row["instruction_id"] == values["instruction_id"]), None)
                require(instruction is not None and instruction["kind"] == "repay" and instruction["status"] == "approved"
                        and local_time(instruction["effective_at"]) == at and instruction["amount_units"] == requested, "直接还款缺少已可见批准指令")
                require(instruction["instruction_id"] not in self.instructions_used, "直接还款重复")
                self.instructions_used.add(instruction["instruction_id"])
                available = account.cash - (account.cashflow_oracle.order_reserved_at(at) if account.cashflow_oracle is not None else 0)
                available -= account.cashflow_oracle.sale_units_at(at) if account.cashflow_oracle is not None else 0
            else:
                claim = self.sale_claims.get(values["claim_id"])
                require(claim is not None and date_value(claim["due_date"], "卖款结算日") <= at.date()
                        and values["settled_cash_units"] == claim["cash_units"], "受限卖款提前或重复结算")
                available = claim["cash_units"]
                require(values["cash_units"] == min(available, sum(self.contracts[key]["principal_units"] + self.contracts[key]["interest_units"] for key in values["contract_ids"])), "卖款偿债金额不符")
            contract_ids = values["contract_ids"]
            require(len(contract_ids) <= 1 or kind == "credit_sale_settled", "直接还款合同选择无效")
            paid, allocations = self.repay(amount_units=requested, repayment_order=rule["repayment_order"],
                available_cash_units=max(0, available), link_policy=self.definition["repayment_link_policy"], at=at,
                contract_id=contract_ids[0] if len(contract_ids) == 1 else None)
            require(paid == requested and allocations == values["allocations"], "本息偿还金额或合同分配不符")
            self.interest_paid += sum(row["interest_paid_units"] for row in allocations)
            if kind == "credit_repayment":
                account.cash -= paid
            else:
                claim = self.sale_claims.pop(values["claim_id"])
                for link in self.links.values():
                    if link["receivable_id"] == values["claim_id"]:
                        link.update(receivable_id=None, lot_id="residual:" + link["link_id"],
                                    source_ref=event["event_id"])
                if values.get("cash_already_settled"):
                    require(values["claim_id"] not in account.receivables, "换股现金尚未按独立权益事实到账")
                    account.cash -= paid
                else:
                    require(account.receivables.pop(values["claim_id"], None) == claim["cash_units"], "受限卖款与现金应收不闭合")
                    account.cash += claim["cash_units"] - paid
        elif kind == "credit_extension":
            row = next((row for row in self.definition["instructions"] if row["instruction_id"] == values["instruction_id"]), None)
            require(row is not None and row["status"] == values["status"] and local_time(row["effective_at"]) == at
                    and row["contract_id"] == values["contract_id"] and row["new_maturity_date"] == values["new_maturity_date"], "展期事件与原始批准不符")
            self.extend(values["instruction_id"])
        elif kind == "credit_risk":
            self.verify_valuation(account, values["valuation"])
            rule = self.rule_at(at)
            current = self.values_at(account, at)
            overdue = any((row["principal_units"] or row["interest_units"]) and date_value(row["maturity_date"], "到期日") <= at.date() for row in self.contracts.values())
            low = current["maintenance_ratio_denominator"] and current["maintenance_ratio_numerator"] * PPM < current["maintenance_ratio_denominator"] * rule["maintenance_ratio_ppm"]
            remedied = not current["maintenance_ratio_denominator"] or current["maintenance_ratio_numerator"] * PPM >= current["maintenance_ratio_denominator"] * rule["remedy_ratio_ppm"]
            status = "liquidating" if overdue else "notified" if self.risk_status == "normal" and low else "normal" if remedied else self.risk_status
            if status == "notified" and self.remedy_date is not None and (at.date() > self.remedy_date or at.date() == self.remedy_date and at.time() >= time(15)):
                status = "liquidating"
            require(values["risk_status"] == status, "风险通知或处置状态无原始债务／行情依据")
            self.risk_status = status
            self.notified_at, self.remedy_date = values["notified_at"], values["remedy_date"]
            held = {}
            for lot in account.lots.values():
                held[lot["instrument_hash"]] = held.get(lot["instrument_hash"], 0) + lot["quantity"]
            self.risk_history.append((at, status, held))
        else:
            raise EvidenceContractError(f"信用独立复核未覆盖事件：{kind}")
        self.verified_events.append(event)
        self.cursor += 1

    def buy_fill(self, account, fill, lot_id):
        event = self.events.get(fill["fill_id"])
        require(event is not None and event["kind"] == "fill", "信用买入缺少原始金融事件")
        values = event_payload(event)
        self.used_trade_events.add(event["event_id"])
        fact = values.get("credit_drawdown")
        if fact is None:
            return 0
        require(fact["contract_id"] == self.allocations[fill["order_id"]]["contract_id"]
                and fact["opening_rule_id"] == self.rule_at(fill["fill_time"])["rule_id"], "融资成交合同或开仓规则不符")
        self.drawdown(order_id=fill["order_id"], principal_units=fact["principal_units"],
            own_cash_units=fill["notional_units"] - fact["principal_units"], notional_units=fill["notional_units"],
            quantity=fill["quantity"], instrument_hash=fill["instrument_hash"], lot_id=event["event_id"],
            link_id=event["event_id"], source_ref=event["event_id"], at=fill["fill_time"])
        return fact["principal_units"]

    def sell_fill(self, account, fill, matches):
        event = self.events.get(fill["fill_id"])
        require(event is not None and event["kind"] == "fill", "信用卖出缺少真实成交事件")
        values = event_payload(event)
        require(all(values.get(key) == fill[key] for key in ("order_id", "instrument_hash", "quantity", "notional_units", "fee_units", "side")), "信用卖出事实与canonical成交不符")
        self.used_trade_events.add(event["event_id"])
        consumed = {row["lot_id"]: row["quantity"] for row in matches}
        transfers, active = [], []
        claim_id = f"credit-sale:{event['event_id']}"
        candidates = sorted((row for row in self.links.values() if row["instrument_hash"] == fill["instrument_hash"] and row["quantity"] and not row["receivable_id"] and not row["successor_right_id"]), key=lambda row: (self.contracts[row["contract_id"]]["maturity_date"], row["contract_id"], row["link_id"]))
        for row in candidates:
            used = min(consumed.get(row["lot_id"], 0), row["quantity"])
            if not used:
                continue
            principal = row["principal_units"] * used // row["quantity"]
            acquisition = row["acquisition_units"] * used // row["quantity"]
            transfer = {"link_id": row["link_id"], "contract_id": row["contract_id"], "quantity": used, "principal_units": principal, "acquisition_units": acquisition}
            transfers.append(transfer)
            if principal or self.contracts[row["contract_id"]]["interest_units"]:
                active.append(transfer)
            row["quantity"] -= used
            row["principal_units"] -= principal
            row["acquisition_units"] -= acquisition
            if principal:
                detached_id = f"{claim_id}:{row['link_id']}"
                require(detached_id not in self.links, "融资卖出关联身份重复")
                self.links[detached_id] = {**row, "link_id": detached_id, "lot_id": claim_id, "quantity": 0, "principal_units": principal, "acquisition_units": acquisition, "receivable_id": claim_id}
            consumed[row["lot_id"]] -= used
        restricted = max(0, fill["notional_units"] - fill["fee_units"]) * sum(row["quantity"] for row in active) // fill["quantity"]
        sale = values.get("credit_sale")
        if active:
            require(sale is not None, "融资卖款未封存受限应收")
            calendar = account.context.get("settlement_calendar")
            sessions = sorted(date_value(day, "结算日历") for day in calendar["sessions"]) if calendar is not None else sorted(date_value(row["session"], "会话") for row in account.context["snapshots"])
            due = next((day for day in sessions if day > local_time(fill["fill_time"]).date()), None)
            if due is None:
                require(calendar is None, "融资卖款缺少未来结算会话")
                due = local_time(fill["fill_time"]).date() + timedelta(days=1)
                while due.weekday() >= 5:
                    due += timedelta(days=1)
            expected = {"claim_id": claim_id, "due_date": due.isoformat(), "cash_units": restricted, "contract_ids": sorted({row["contract_id"] for row in active}), "link_transfers": transfers, "lot_consumptions": [{"lot_id": row["lot_id"], "quantity": row["quantity"]} for row in matches]}
            require(sale == expected, "融资卖出本金关联、受限金额或结算日不符")
            self.sale_claims[claim_id] = {key: expected[key] for key in ("claim_id", "due_date", "cash_units", "contract_ids")}
            self.sale_claims[claim_id]["source_event_id"] = event["event_id"]
            require(claim_id not in account.receivables, "融资卖款应收重复")
            account.receivables[claim_id] = restricted
        else:
            require(sale is None, "普通卖款伪造信用应收")
        self.verify_links()
        return restricted

    def account_event(self, account, event, values):
        kind = event["kind"]
        if kind == "security_conversion":
            conversion = account.conversions[values["conversion_id"]]
            plan = conversion["plan"]
            originals = {row["old_lot_id"]: row for row in conversion["links"]}
            claim_id, transfers, contracts, cash = values["receivable_id"], [], set(), 0
            for row in list(self.links.values()):
                if row["instrument_hash"] != values["old_instrument_hash"] or not row["quantity"] or row["receivable_id"]:
                    continue
                source = originals.get(row["lot_id"])
                require(source is not None, "换股融资关联没有独立旧批次")
                quantity = row["quantity"] * plan["ratio_numerator"] // plan["ratio_denominator"]
                fraction = Fraction(plan["successor_cost_numerator"], plan["successor_cost_denominator"]) if quantity else Fraction(0)
                principal = (row["principal_units"] * fraction).numerator // (row["principal_units"] * fraction).denominator
                acquisition = (row["acquisition_units"] * fraction).numerator // (row["acquisition_units"] * fraction).denominator
                successor = None if not quantity else {**row, "instrument_hash": values["new_instrument_hash"], "lot_id": source["successor_lot"]["lot_id"], "quantity": quantity, "principal_units": principal, "acquisition_units": acquisition, "successor_right_id": values["successor_entitlement_id"]}
                residual = row["principal_units"] - principal
                cash_link = None if not residual else {**row, "link_id": f"{claim_id}:{row['link_id']}", "lot_id": claim_id, "quantity": 0, "principal_units": residual, "acquisition_units": row["acquisition_units"] - acquisition, "successor_right_id": None, "receivable_id": claim_id}
                if cash_link:
                    contracts.add(row["contract_id"])
                    cash += source["cash_consideration_units"] * row["quantity"] // source["old_quantity"]
                transfers.append({"before_link_id": row["link_id"], "successor_link": successor, "cash_link": cash_link})
                del self.links[row["link_id"]]
                for destination in (successor, cash_link):
                    if destination is not None:
                        self.links[destination["link_id"]] = destination
            claim = None if not contracts else {"claim_id": claim_id, "due_date": local_time(plan["cash_pay_at"]).date().isoformat(), "cash_units": cash, "contract_ids": sorted(contracts), "source_event_id": event["event_id"]}
            expected = {"links": transfers, "claim": claim, "principal_allocation_policy": "source_successor_cost_fraction"}
            require(event_payload(event).get("credit_conversion") == expected, "换股本金、后继关联或受限现金不符")
            if claim:
                self.sale_claims[claim_id] = claim
            self.verify_links()
            return expected
        if kind == "successor_registered":
            for row in self.links.values():
                if row["successor_right_id"] == values["entitlement_id"]:
                    row["successor_right_id"] = None
        return None

    def valuation_payload(self, account, at):
        values = self.values_at(account, at)
        return {key: value for key, value in values.items() if key != "margin_available"}

    def snapshot(self, account, session, positions, valuation, p6_adjustment):
        require(session in self.snapshot_by_session, "信用账户缺少会话快照")
        row = self.snapshot_by_session[session]
        values = self.verify_valuation(account, row)
        require(valuation["valuation_model"] == VALUATION_MODEL and valuation["nav_units"] == values["net_asset_units"], "信用canonical净NAV与独立账本不符")
        adjustment = p6_adjustment - self.principal_units - self.interest_units
        self.adjustments[session] = adjustment
        return adjustment

    def finish(self, account):
        require(self.cursor == len(self.schedule) and set(self.adjustments) == set(self.snapshot_by_session), "信用事件或会话快照未完整复核")
        for command in self.context["risk_commands"]:
            decision = local_time(command["decision_time"])
            history = [row for row in self.risk_history if row[0] <= decision]
            require(history and history[-1][1] == "liquidating", "系统风险命令没有当时已触发处置状态")
            code = command["instrument"]["instrument_id"]
            key = next((key for key, mapped in self.codes.items() if mapped == code), None)
            require(key is not None and command["side"] == "sell" and command["action"] == "submit" and 0 < command["quantity"] <= history[-1][2].get(key, 0), "系统风险命令没有真实待处置持仓")
            require(local_time(command["available_at"]) <= decision == local_time(command["submitted_at"]) and local_time(command["reference_price_available_at"]) <= decision, "风险命令可见性不符")
            require(Fraction(command["reference_price"]["units"], 10 ** (command["reference_price"]["scale"] - 2)) == self.price_at(key, decision), "风险命令使用未来参考价")
        state = self.context["closing_state"]
        require(indexed(state["contracts"], "contract_id", "期末合同") == self.contracts, "期末本金、利息、余数或到期日不符")
        require(indexed(state["position_links"], "link_id", "期末关联") == self.links, "期末资产债务关联不符")
        require(not self.reservations and state["reservations"] == [] and indexed(state["sale_claims"], "claim_id", "期末受限卖款") == self.sale_claims, "期末存在未验证预占或受限卖款")
        last_at = local_time(self.context["snapshots"][-1]["at"])
        require(all(date_value(row["due_date"], "期末应收结算日") > last_at.date() for row in self.sale_claims.values()), "期末存在应结算而未结算卖款")
        require(all(not (row["principal_units"] or row["interest_units"]) or date_value(row["maturity_date"], "期末到期日") > last_at.date() for row in self.contracts.values()), "期末到期债务未解决")
        require(self.context["risk_status"] == self.risk_status == state["risk_status"] and self.risk_status not in {"liquidating", "default"}, "期末融资违约未解决")
        require(self.context["interest_accrued_units"] == sum(row["interest_units"] for row in self.interest_history)
                and self.context["interest_paid_units"] == self.interest_paid, "利息损益归因不符")
        require(self.context["closing_nav_units"] == self.values_at(account, self.context["snapshots"][-1]["at"])["net_asset_units"], "期末信用净NAV不符")
