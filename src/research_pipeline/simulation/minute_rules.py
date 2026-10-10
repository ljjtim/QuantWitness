"""分钟交易与结算共用的已可见规则解析和参数校验。"""
from __future__ import annotations
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from research_pipeline.domain import (
    InstrumentKey, MarketRuleSnapshot, MinuteRuleBinding, MinuteRuleResolver,
    MinuteRuleSnapshotBundle, MinuteRuleSnapshotError,
)
from research_pipeline.domain.time import require_aware_datetime
from .market_rules import (
    CashMarketPolicy, ETF_CATEGORIES, LISTING_PHASES, STOCK_BOARDS,
    _integer_parameter, _require_cash_lifecycle, etf_policy_from_rule,
    require_cash_rule_applicable, stock_policy_from_rule,
)
from research_pipeline.platform import typed_canonical_hash
from .orders import SimulationContractError

_ORDER_REQUIRED_RULES = {
    "cn_stock": (
        "rule.cn_stock.instrument_lifecycle.v1",
        "rule.cn_stock.lot_size.v1",
        "rule.cn_stock.price_limit.v1",
        "rule.cn_stock.session.v1",
        "rule.cn_stock.settlement.v1",
        "rule.cn_stock.suspension.v1",
        "rule.cn_stock.trading_fee.v1",
    ),
    "cn_etf": (
        "rule.cn_fund.instrument_lifecycle.v1",
        "rule.cn_fund.lot_size.v1",
        "rule.cn_fund.price_limit.v1",
        "rule.cn_fund.session.v1",
        "rule.cn_fund.settlement.v1",
        "rule.cn_fund.trading_fee.v1",
    ),
    "cn_future": (
        "rule.cn_futures.actual_contract_mapping.v1",
        "rule.cn_futures.contract_lifecycle.v1",
        "rule.cn_futures.contract_multiplier.v1",
        "rule.cn_futures.delivery_expiry.v1",
        "rule.cn_futures.fee_schedule.v1",
        "rule.cn_futures.margin.v1",
        "rule.cn_futures.price_limit.v1",
        "rule.cn_futures.price_tick.v1",
        "rule.cn_futures.session.v1",
    ),
}


@dataclass(frozen=True)
class _ResolvedRules:
    bindings: tuple[MinuteRuleBinding, ...]
    parameters: Mapping[str, object]
    identity_hash: str
    available_at: datetime


def resolve_minute_execution_rules(
    resolver: MinuteRuleResolver,
    *,
    bundle: MinuteRuleSnapshotBundle,
    asset_class: str,
    instrument_id: str,
    effective_on: date,
    as_of: datetime,
    required_rule_ids: tuple[str, ...] | None = None,
) -> _ResolvedRules:
    bindings = []
    selected_rule_ids = (
        _ORDER_REQUIRED_RULES[asset_class]
        if required_rule_ids is None
        else required_rule_ids
    )
    for rule_id in selected_rule_ids:
        try:
            bindings.append(resolver.resolve(
                rule_id=rule_id,
                instrument_id=instrument_id,
                effective_on=effective_on,
                as_of=as_of,
            ))
        except MinuteRuleSnapshotError as exc:
            raise SimulationContractError(
                f"分钟正式仿真规则不完整: {rule_id}: {exc}"
            ) from exc
    result = tuple(bindings)
    identity_hash = typed_canonical_hash({
        "bundle_hash": bundle.bundle_hash,
        "bindings": [item.identity_hash for item in result],
    })
    available_at = max(
        item.rule.available_at for item in result if item.rule.available_at is not None
    )
    return _ResolvedRules(result, _parameters(result), identity_hash, available_at)



def minute_cash_bar_suspended(
    resolver: MinuteRuleResolver,
    *,
    bundle: MinuteRuleSnapshotBundle,
    asset_class: str,
    instrument_id: str,
    trading_date: date,
    bar_start: datetime,
    bar_end: datetime,
    available_at: datetime,
) -> bool:
    """整分钟跨过停牌或撮合时仍停牌时，不使用该 Bar 的价格和容量。"""
    for name, value in (("bar_start", bar_start), ("bar_end", bar_end), ("available_at", available_at)):
        require_aware_datetime(value, name)
    if not bar_start < bar_end <= available_at:
        raise SimulationContractError("分钟停牌核验要求完整且已可见的执行区间")
    if asset_class not in {"cn_stock", "cn_etf"} or resolver.bundle.bundle_hash != bundle.bundle_hash:
        raise SimulationContractError("分钟停牌核验的资产或规则身份不一致")
    namespace = "cn_stock" if asset_class == "cn_stock" else "cn_fund"
    rule_id = f"rule.{namespace}.suspension.v1"
    points = {bar_start, available_at}
    points.update(
        rule.available_at for rule in bundle.rules
        if rule.rule_id == rule_id and rule.instrument_id == instrument_id
        and rule.effective_from <= trading_date <= rule.effective_to
        and rule.available_at is not None
        and bar_start < rule.available_at <= bar_end
    )
    for observed_at in sorted(points):
        binding = resolver.resolve(
            rule_id=rule_id, instrument_id=instrument_id,
            effective_on=trading_date, as_of=observed_at,
        )
        paused = dict(binding.rule.parameters).get("paused")
        if type(paused) is not bool:
            raise SimulationContractError("分钟停牌状态必须是明确布尔值")
        if paused:
            return True
    return False


def _parameters(bindings: tuple[MinuteRuleBinding, ...]) -> dict[str, object]:
    result: dict[str, object] = {}
    for binding in bindings:
        for key, value in binding.rule.parameters:
            if key in result and result[key] != value:
                raise SimulationContractError(f"分钟规则参数冲突: {key}")
            result[key] = value
    return result


def _positive_integer(
    parameters: Mapping[str, object],
    key: str,
    *,
    default: int | None = None,
) -> int:
    value = parameters.get(key, default)
    if type(value) is not int or value <= 0:
        raise SimulationContractError(f"分钟规则缺少正整数参数: {key}")
    return value


def _futures_margin_ppm(parameters: Mapping[str, object]) -> int:
    if parameters.get("margin_account_role") != "speculative":
        raise SimulationContractError("期货分钟仿真只消费明确的投机保证金率")
    rate = _positive_integer(parameters, "speculative_initial_margin_ppm")
    if rate > 1_000_000:
        raise SimulationContractError("期货保证金率超出支持范围")
    _positive_integer(parameters, "hedge_margin_ppm")
    return rate




@dataclass(frozen=True)
class MinuteCashPriceLimits:
    """无涨跌幅限制的历史阶段保留空上下界，不能虚构极大价格。"""

    mode: str
    price_scale: int
    high_limit_units: int | None
    low_limit_units: int | None


def minute_cash_price_limits(parameters: Mapping[str, object], *, decision_at: datetime) -> MinuteCashPriceLimits:
    from decimal import Decimal, ROUND_HALF_UP

    require_aware_datetime(decision_at, "decision_at")
    mode = parameters.get("price_limit_mode")
    if mode not in {"bounded", "unbounded"}:
        raise SimulationContractError("分钟现货缺少明确价格限制模式")
    scale = _integer_parameter(parameters, "price_scale")
    if scale > 8:
        raise SimulationContractError("分钟现货报价精度超出支持范围")
    if mode == "unbounded":
        if any(parameters.get(key) is not None for key in ("high_limit_units", "low_limit_units", "price_limit_ratio_ppm")):
            raise SimulationContractError("无涨跌幅限制阶段不能携带固定涨跌停值")
        return MinuteCashPriceLimits(mode, scale, None, None)
    high = _positive_integer(parameters, "high_limit_units")
    low = _positive_integer(parameters, "low_limit_units")
    ratio = _integer_parameter(parameters, "price_limit_ratio_ppm", minimum=1)
    if ratio >= 1_000_000:
        raise SimulationContractError("分钟现货价格限制比例无效")
    reference = _positive_integer(parameters, "reference_previous_close_units")
    raw = parameters.get("reference_price_available_at")
    if not isinstance(raw, str):
        raise SimulationContractError("分钟前收参考值缺少可见时间")
    try:
        visible = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SimulationContractError("分钟前收参考值可见时间无效") from exc
    require_aware_datetime(visible, "reference_price_available_at")
    if visible > decision_at:
        raise SimulationContractError("分钟前收参考值在决策时尚不可见")
    rounding = parameters.get("price_limit_rounding")
    if rounding != "half_up_to_quote_unit":
        raise SimulationContractError("分钟现货必须明确声明报价单位四舍五入规则")
    rate = Decimal(ratio) / Decimal(1_000_000)
    expected_high = int((Decimal(reference) * (1 + rate)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    expected_low = int((Decimal(reference) * (1 - rate)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    if (high, low) != (expected_high, expected_low) or low > high:
        raise SimulationContractError("分钟涨跌停值与可见前收、比例和报价精度不一致")
    return MinuteCashPriceLimits(mode, scale, high, low)


def minute_cash_policy_from_rules(
    rules: _ResolvedRules,
    *,
    instrument: InstrumentKey,
    trading_date: date,
    decision_at: datetime,
    slippage_per_share: object = 0,
    cash_shortage_policy: str = "reject_v1",
) -> CashMarketPolicy:
    """把已绑定的历史事实完整编译到撮合 policy；费用不能替代交易资格。"""
    if instrument.asset_class not in {"cn_stock", "cn_etf"}:
        raise SimulationContractError("分钟现货只接收股票或 ETF")
    require_aware_datetime(decision_at, "decision_at")
    if not rules.bindings or rules.available_at > decision_at:
        raise SimulationContractError("分钟现货规则尚不可见或缺少来源绑定")
    for binding in rules.bindings:
        rule = binding.rule
        if rule.instrument_id != instrument.instrument_id or rule.asset_class != instrument.asset_class:
            raise SimulationContractError("分钟现货规则标的或资产类型不一致")
        if rule.status != "supported" or rule.available_at is None or rule.available_at > decision_at:
            raise SimulationContractError("分钟现货规则未支持或在决策时不可见")
        if not rule.effective_from <= trading_date <= rule.effective_to:
            raise SimulationContractError("分钟现货规则不适用于成交日")
    parameters = _parameters(rules.bindings)
    if parameters != rules.parameters:
        raise SimulationContractError("分钟现货参数与来源绑定不一致")
    namespace = "cn_stock" if instrument.asset_class == "cn_stock" else "cn_fund"
    required = set(_ORDER_REQUIRED_RULES[instrument.asset_class])
    required.add(f"rule.{namespace}.suspension.v1")
    if instrument.asset_class == "cn_stock":
        required.add("rule.cn_stock.adjustment_factor_snapshot.v1")
    actual = {binding.rule.rule_id for binding in rules.bindings}
    if not required <= actual:
        raise SimulationContractError(f"分钟现货缺少必要规则: {sorted(required - actual)}")
    _require_cash_lifecycle(parameters, trading_date=trading_date)
    paused = parameters.get("paused")
    if type(paused) is not bool or paused != (parameters["trading_status"] == "suspended"):
        raise SimulationContractError("分钟现货停牌事实缺失或与交易状态不一致")
    if instrument.asset_class == "cn_stock":
        if parameters.get("stock_board") not in STOCK_BOARDS or parameters.get("listing_phase") not in LISTING_PHASES:
            raise SimulationContractError("分钟股票缺少明确历史板块或上市阶段")
        if type(parameters.get("is_st")) is not bool:
            raise SimulationContractError("分钟股票缺少明确历史 ST 状态")
        for key in ("adjustment_snapshot_identity_hash", "corporate_action_snapshot_hash"):
            value = parameters.get(key)
            if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise SimulationContractError(f"分钟股票缺少正式 PIT 工件绑定: {key}")
    elif parameters.get("etf_category") not in ETF_CATEGORIES:
        raise SimulationContractError("分钟 ETF 缺少明确历史产品类别")
    elif parameters.get("product_class") is not None and parameters["product_class"] != f"{parameters['etf_category']}_etf":
        raise SimulationContractError("分钟 ETF 产品类别与显式品类不一致")
    for side in ("buy", "sell"):
        _integer_parameter(parameters, f"{side}_min_quantity", minimum=1)
        _integer_parameter(parameters, f"{side}_quantity_step", minimum=1)
    if type(parameters.get("sell_remainder_allowed")) is not bool:
        raise SimulationContractError("分钟现货缺少明确零股政策")
    minute_cash_price_limits(parameters, decision_at=decision_at)
    scope = parameters.get("cost_model_scope")
    if scope not in {"market_rule", "research_assumption"}:
        raise SimulationContractError("分钟现货费用须声明历史来源或研究假设")
    combined = MarketRuleSnapshot(
        f"minute-{instrument.asset_class}-derived-v1", 1,
        instrument.asset_class, instrument.contract_kind,
        max(binding.rule.effective_from for binding in rules.bindings),
        min(binding.rule.effective_to for binding in rules.bindings),
        max(binding.rule.available_at for binding in rules.bindings),
        "minute-rule-bundle", "research_pipeline/docs/minute_simulation.md",
        tuple(sorted({
            **parameters,
            "lot_size": parameters["buy_quantity_step"],
            "source_binding_hash": rules.identity_hash,
            "source_instrument_id": instrument.instrument_id,
            "source_rule_bindings": [
                {"rule_id": binding.rule.rule_id, "snapshot_hash": binding.rule.snapshot_hash,
                 "source_hashes": dict(binding.source_hashes)}
                for binding in rules.bindings
            ],
        }.items())),
    )
    require_cash_rule_applicable(combined, trading_date=trading_date, decision_at=decision_at)
    if instrument.asset_class == "cn_stock":
        return stock_policy_from_rule(combined, slippage_per_share=slippage_per_share, cash_shortage_policy=cash_shortage_policy)
    policy = etf_policy_from_rule(combined)
    return replace(policy, cash_shortage_policy=cash_shortage_policy)


def resolve_minute_cash_policy(
    resolver: MinuteRuleResolver,
    *,
    bundle: MinuteRuleSnapshotBundle,
    instrument: InstrumentKey,
    trading_date: date,
    decision_at: datetime,
    slippage_per_share: object = 0,
    cash_shortage_policy: str = "reject_v1",
) -> tuple[_ResolvedRules, CashMarketPolicy]:
    """解析完整交易资格后编译 policy；不改变平台分钟覆盖范围。"""
    if instrument.asset_class not in {"cn_stock", "cn_etf"}:
        raise SimulationContractError("分钟现货只接收股票或 ETF")
    if resolver.bundle.bundle_hash != bundle.bundle_hash:
        raise SimulationContractError("分钟现货解析器与输入 bundle 不一致")
    required = set(_ORDER_REQUIRED_RULES[instrument.asset_class])
    if instrument.asset_class == "cn_stock":
        required.add("rule.cn_stock.adjustment_factor_snapshot.v1")
    else:
        required.add("rule.cn_fund.suspension.v1")
    rules = resolve_minute_execution_rules(
        resolver, bundle=bundle, asset_class=instrument.asset_class,
        instrument_id=instrument.instrument_id, effective_on=trading_date,
        as_of=decision_at, required_rule_ids=tuple(sorted(required)),
    )
    policy = minute_cash_policy_from_rules(
        rules, instrument=instrument, trading_date=trading_date, decision_at=decision_at,
        slippage_per_share=slippage_per_share, cash_shortage_policy=cash_shortage_policy,
    )
    return rules, policy
