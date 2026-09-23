"""组合目标到执行订单之间的唯一公共转换端口。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from research_pipeline.domain import DomainContractError, Price
from research_pipeline.domain.time import require_aware_datetime
from research_pipeline.domain.trading import (
    EXECUTION_CAPABILITY_MATRIX,
    OrderIntent,
    PortfolioTarget,
    TradingRuleBinding,
    require_declared_trading_capability,
)
from research_pipeline.platform import typed_canonical_hash

from .orders import Order, SimulationContractError


CAPABILITY_MATRIX_VERSION = "research-execution-capability-matrix-v1"
ACCOUNT_CONFIG_VERSION = "research-simulation-account-config-v1"
SIMULATION_REQUEST_VERSION = "research-simulation-request-v1"


@dataclass(frozen=True)
class ExecutionBackendCapability:
    backend_id: str
    target_types: tuple[str, ...]
    asset_classes: tuple[str, ...]
    position_effects: tuple[str, ...]
    supports_short: bool
    maximum_leverage: float
    supports_fx: bool = False
    contract_version: str = CAPABILITY_MATRIX_VERSION

    def __post_init__(self) -> None:
        if not self.backend_id.strip():
            raise SimulationContractError("backend_id 不能为空")
        for values, field in (
            (self.target_types, "target_types"),
            (self.asset_classes, "asset_classes"),
            (self.position_effects, "position_effects"),
        ):
            if not values or values != tuple(sorted(set(values))):
                raise SimulationContractError(f"{field} 必须非空、唯一并排序")
        if self.maximum_leverage < 1:
            raise SimulationContractError("maximum_leverage 不能小于 1")
        if self.contract_version != CAPABILITY_MATRIX_VERSION:
            raise SimulationContractError("执行能力矩阵版本无效")

    @property
    def capability_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "backend_id": self.backend_id,
            "target_types": list(self.target_types),
            "asset_classes": list(self.asset_classes),
            "position_effects": list(self.position_effects),
            "supports_short": self.supports_short,
            "maximum_leverage": self.maximum_leverage,
            "supports_fx": self.supports_fx,
            "contract_version": self.contract_version,
        }


def _backend_from_matrix(backend_id: str) -> ExecutionBackendCapability:
    raw = EXECUTION_CAPABILITY_MATRIX[backend_id]
    return ExecutionBackendCapability(
        backend_id=backend_id,
        target_types=tuple(str(item) for item in raw["target_types"]),
        asset_classes=tuple(str(item) for item in raw["asset_classes"]),
        position_effects=tuple(str(item) for item in raw["position_effects"]),
        supports_short=raw["supports_short"] is True,
        maximum_leverage=float(raw["maximum_leverage"]),
        supports_fx=raw["supports_fx"] is True,
    )


CASH_DAILY_BACKEND = _backend_from_matrix("cash-daily-v1")
CN_FUTURES_DAILY_BACKEND = _backend_from_matrix("cn-futures-daily-v1")

EXECUTION_BACKENDS: Mapping[str, ExecutionBackendCapability] = {
    item.backend_id: item for item in (CASH_DAILY_BACKEND, CN_FUTURES_DAILY_BACKEND)
}


@dataclass(frozen=True)
class AccountConfig:
    base_currency: str
    initial_cash_units: int
    fx_model_hash: str | None = None
    contract_version: str = ACCOUNT_CONFIG_VERSION

    def __post_init__(self) -> None:
        if self.base_currency != "CNY":
            raise SimulationContractError("当前账户 base_currency 只支持 CNY")
        if type(self.initial_cash_units) is not int or self.initial_cash_units < 0:
            raise SimulationContractError("initial_cash_units 必须是非负整数")
        if self.fx_model_hash is not None:
            _hash(self.fx_model_hash, "fx_model_hash")
        if self.contract_version != ACCOUNT_CONFIG_VERSION:
            raise SimulationContractError("AccountConfig 版本无效")

    @property
    def account_config_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "base_currency": self.base_currency,
            "initial_cash_units": self.initial_cash_units,
            "fx_model_hash": self.fx_model_hash,
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class SimulationRequest:
    portfolio_target: PortfolioTarget
    market_data_artifact_hash: str
    account_config: AccountConfig
    rule_bindings: tuple[TradingRuleBinding, ...]
    cost_model_hash: str
    execution_backend: ExecutionBackendCapability
    fixed_clock: str
    root_seed: int
    contract_version: str = SIMULATION_REQUEST_VERSION

    def __post_init__(self) -> None:
        _hash(self.market_data_artifact_hash, "market_data_artifact_hash")
        _hash(self.cost_model_hash, "cost_model_hash")
        require_supported_target(self.portfolio_target, self.execution_backend)
        if self.portfolio_target.base_currency != self.account_config.base_currency:
            raise SimulationContractError("PortfolioTarget 与 AccountConfig 币种不一致")
        expected = tuple(item.instrument.instrument_hash for item in self.portfolio_target.entries)
        actual = tuple(item.instrument_hash for item in self.rule_bindings)
        if actual != tuple(sorted(set(actual))) or set(actual) != set(expected):
            raise SimulationContractError("规则绑定必须与目标标的一一对应且规范排序")
        for entry in self.portfolio_target.entries:
            binding = next(item for item in self.rule_bindings if item.instrument_hash == entry.instrument.instrument_hash)
            binding.require_for(entry.instrument, application_time=self.portfolio_target.decision_time)
        _fixed_clock(self.fixed_clock)
        if type(self.root_seed) is not int or self.root_seed < 0:
            raise SimulationContractError("root_seed 必须是非负整数")
        if self.contract_version != SIMULATION_REQUEST_VERSION:
            raise SimulationContractError("SimulationRequest 版本无效")

    @property
    def request_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "portfolio_target": self.portfolio_target.to_dict(),
            "market_data_artifact_hash": self.market_data_artifact_hash,
            "account_config": self.account_config.to_dict(),
            "rule_bindings": [item.to_dict() for item in self.rule_bindings],
            "cost_model_hash": self.cost_model_hash,
            "execution_backend": self.execution_backend.to_dict(),
            "fixed_clock": self.fixed_clock,
            "root_seed": self.root_seed,
            "contract_version": self.contract_version,
        }


def require_supported_target(
    target: PortfolioTarget,
    backend: ExecutionBackendCapability,
) -> None:
    """在 plan/admit 阶段调用；不支持的 target/asset/backend 组合失败关闭。"""
    if target.target_type not in backend.target_types:
        raise SimulationContractError("execution backend 不支持该 target_type")
    unsupported_assets = {
        item.instrument.asset_class for item in target.entries
    } - set(backend.asset_classes)
    if unsupported_assets:
        raise SimulationContractError(
            f"execution backend 不支持资产类别: {sorted(unsupported_assets)}"
        )
    if target.short_allowed and not backend.supports_short:
        raise SimulationContractError("execution backend 不支持 short")
    if target.leverage_limit > backend.maximum_leverage:
        raise SimulationContractError("目标 leverage_limit 超过 execution backend 上限")
    if any(item.instrument.currency != target.base_currency for item in target.entries):
        if not backend.supports_fx:
            raise SimulationContractError("非本位币目标缺少 FX model/backend 能力")


def require_declared_execution_capability(
    *,
    backend_id: str,
    target_type: str,
    asset_classes: tuple[str, ...],
    base_currency: str,
    short_allowed: object,
    leverage_limit: object,
) -> ExecutionBackendCapability:
    """校验 ResearchPackage plan 中尚未物化的目标能力声明。"""
    try:
        require_declared_trading_capability(
            backend_id=backend_id,
            target_type=target_type,
            asset_classes=asset_classes,
            base_currency=base_currency,
            short_allowed=short_allowed,
            leverage_limit=leverage_limit,
        )
    except DomainContractError as exc:
        raise SimulationContractError(str(exc)) from exc
    backend = EXECUTION_BACKENDS.get(backend_id)
    if backend is None:
        raise SimulationContractError("execution backend 领域声明缺少运行时实现")
    return backend


class IntentToOrderPort:
    """唯一把已验证 OrderIntent 变成执行层 Order 的端口。"""

    def __init__(self, backend: ExecutionBackendCapability) -> None:
        self._backend = backend

    @property
    def backend(self) -> ExecutionBackendCapability:
        return self._backend

    def to_order(
        self,
        intent: OrderIntent,
        *,
        ordinal: int,
        order_type: str = "market",
        time_in_force: str = "DAY",
        limit_price: Price | None = None,
    ) -> Order:
        if type(ordinal) is not int or ordinal < 0:
            raise SimulationContractError("order ordinal 必须是非负整数")
        if intent.instrument.asset_class not in self._backend.asset_classes:
            raise SimulationContractError("OrderIntent 资产类别不受 backend 支持")
        if intent.position_effect not in self._backend.position_effects:
            raise SimulationContractError("OrderIntent position_effect 不受 backend 支持")
        if intent.instrument.currency != "CNY" and not self._backend.supports_fx:
            raise SimulationContractError("非 CNY OrderIntent 缺少 FX model/backend 能力")
        identity = {
            "intent_hash": intent.intent_hash,
            "backend_hash": self._backend.capability_hash,
            "ordinal": ordinal,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "limit_price": None if limit_price is None else limit_price.to_dict(),
        }
        return Order(
            order_id=typed_canonical_hash(identity),
            instrument=intent.instrument,
            side=intent.side,
            quantity=int(intent.quantity),
            order_type=order_type,
            time_in_force=time_in_force,
            submitted_at=intent.order_time,
            position_effect=intent.position_effect,
            intent_hash=intent.intent_hash,
            limit_price=limit_price,
        )


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise SimulationContractError(f"{field} 必须是小写 sha256")
    return value


def _fixed_clock(value: object) -> str:
    if not isinstance(value, str):
        raise SimulationContractError("fixed_clock 必须是字符串")
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SimulationContractError("fixed_clock 必须是带时区 ISO 时间") from exc
    require_aware_datetime(instant, "fixed_clock")
    return value


__all__ = [
    "ACCOUNT_CONFIG_VERSION",
    "CAPABILITY_MATRIX_VERSION",
    "SIMULATION_REQUEST_VERSION",
    "AccountConfig",
    "CASH_DAILY_BACKEND",
    "CN_FUTURES_DAILY_BACKEND",
    "EXECUTION_BACKENDS",
    "ExecutionBackendCapability",
    "IntentToOrderPort",
    "SimulationRequest",
    "require_supported_target",
    "require_declared_execution_capability",
]
