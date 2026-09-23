"""仿真时钟、输入可见性、容量和成本的可验证合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
import re
from typing import Mapping
from zoneinfo import ZoneInfo

from research_pipeline.platform import typed_canonical_hash

from .orders import SimulationContractError


SIMULATION_SEMANTICS_VERSION = "research-simulation-semantics-v1"
SIMULATION_INPUT_AVAILABILITY_VERSION = "research-simulation-input-availability-v1"
SIMULATION_TIMEZONE = "Asia/Shanghai"
_STABLE_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")
_USE_STAGES = {"decision", "execution", "valuation"}
_REQUIRED_INPUT_IDS = {
    "execution.open",
    "market.rule",
    "signal.target",
    "valuation.open",
}


class CapacityMode(str, Enum):
    MODELED = "modeled"
    ASSUMED_UNBOUNDED = "assumed_unbounded"
    UNKNOWN = "unknown"


_CAPACITY_CEILINGS = {
    CapacityMode.MODELED: "liquidity_modeled",
    CapacityMode.ASSUMED_UNBOUNDED: "research_only",
    CapacityMode.UNKNOWN: "no_liquidity_claim",
}


def _require_stable_id(value: str, field: str) -> None:
    if not isinstance(value, str) or not _STABLE_ID.fullmatch(value):
        raise SimulationContractError(f"{field} 必须是稳定 ID")


def _require_hash(value: str, field: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise SimulationContractError(f"{field} 必须是 sha256 小写摘要")


def _require_cn_time(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise SimulationContractError(f"{field} 必须显式带时区")
    if getattr(value.tzinfo, "key", None) != SIMULATION_TIMEZONE:
        raise SimulationContractError(f"{field} 必须使用 {SIMULATION_TIMEZONE}")


def _parse_cn_time(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise SimulationContractError(f"{field} 不是有效时间") from exc
    if parsed.utcoffset() is None:
        raise SimulationContractError(f"{field} 必须显式带时区")
    timezone = ZoneInfo(SIMULATION_TIMEZONE)
    if parsed.utcoffset() != parsed.astimezone(timezone).utcoffset():
        raise SimulationContractError(f"{field} 必须使用 {SIMULATION_TIMEZONE} 偏移")
    return parsed.astimezone(timezone)


@dataclass(frozen=True, order=True)
class SimulationInputAvailability:
    input_id: str
    source_hash: str
    available_at: datetime
    use_stage: str
    contract_version: str = SIMULATION_INPUT_AVAILABILITY_VERSION

    def __post_init__(self) -> None:
        _require_stable_id(self.input_id, "input_id")
        _require_hash(self.source_hash, "source_hash")
        _require_cn_time(self.available_at, "available_at")
        if self.use_stage not in _USE_STAGES:
            raise SimulationContractError("input use_stage 无效")
        if self.contract_version != SIMULATION_INPUT_AVAILABILITY_VERSION:
            raise SimulationContractError("input availability 版本不受支持")

    def to_dict(self) -> dict[str, str]:
        return {
            "input_id": self.input_id,
            "source_hash": self.source_hash,
            "available_at": self.available_at.isoformat(),
            "use_stage": self.use_stage,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "SimulationInputAvailability":
        expected = {"input_id", "source_hash", "available_at", "use_stage", "contract_version"}
        if set(payload) != expected:
            raise SimulationContractError("input availability schema 无效")
        return cls(
            str(payload["input_id"]),
            str(payload["source_hash"]),
            _parse_cn_time(payload["available_at"], "available_at"),
            str(payload["use_stage"]),
            str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class SimulationSemanticsV1:
    input_availability: tuple[SimulationInputAvailability, ...]
    signal_at: datetime
    decision_at: datetime
    order_submitted_at: datetime
    execution_at: datetime
    valuation_at: datetime
    return_start_at: datetime
    return_end_at: datetime
    timezone: str
    calendar_id: str
    capacity_mode: CapacityMode
    capacity_model_id: str
    capacity_model_version: str
    cost_model_id: str
    cost_model_version: str
    claim_ceiling: str
    contract_version: str = SIMULATION_SEMANTICS_VERSION

    def __post_init__(self) -> None:
        if not self.input_availability:
            raise SimulationContractError("simulation semantics 缺少输入可见性")
        ordered = tuple(sorted(self.input_availability, key=lambda item: item.input_id))
        if self.input_availability != ordered or len({item.input_id for item in ordered}) != len(ordered):
            raise SimulationContractError("input availability 必须唯一并按 input_id 排序")
        input_ids = {item.input_id for item in ordered}
        if not _REQUIRED_INPUT_IDS <= input_ids:
            raise SimulationContractError("simulation semantics 缺少基础输入可见性")
        for field in (
            "signal_at",
            "decision_at",
            "order_submitted_at",
            "execution_at",
            "valuation_at",
            "return_start_at",
            "return_end_at",
        ):
            _require_cn_time(getattr(self, field), field)
        if self.timezone != SIMULATION_TIMEZONE:
            raise SimulationContractError("simulation timezone 必须是 Asia/Shanghai")
        _require_stable_id(self.calendar_id, "calendar_id")
        if not isinstance(self.capacity_mode, CapacityMode):
            raise SimulationContractError("capacity_mode 无效")
        for field in (
            "capacity_model_id",
            "capacity_model_version",
            "cost_model_id",
            "cost_model_version",
        ):
            _require_stable_id(getattr(self, field), field)
        expected_ceiling = _CAPACITY_CEILINGS[self.capacity_mode]
        if self.claim_ceiling != expected_ceiling:
            raise SimulationContractError("claim_ceiling 与 capacity_mode 不一致")
        if self.capacity_mode is CapacityMode.MODELED and self.capacity_model_id == "none":
            raise SimulationContractError("modeled capacity 必须绑定真实模型")
        if self.capacity_mode is CapacityMode.MODELED and "capacity.visible" not in input_ids:
            raise SimulationContractError("modeled capacity 缺少容量可见性输入")
        if (
            self.capacity_mode is CapacityMode.ASSUMED_UNBOUNDED
            and self.capacity_model_id == "none"
        ):
            raise SimulationContractError("assumed_unbounded 必须绑定假设模型身份")
        if self.capacity_mode is CapacityMode.UNKNOWN and self.capacity_model_id != "none":
            raise SimulationContractError("unknown capacity 的 model_id 必须是 none")
        if self.contract_version != SIMULATION_SEMANTICS_VERSION:
            raise SimulationContractError("SimulationSemantics 版本不受支持")
        if self.signal_at > self.decision_at:
            raise SimulationContractError("signal_at 晚于 decision_at")
        if not (
            self.decision_at
            <= self.order_submitted_at
            <= self.execution_at
            <= self.valuation_at
            <= self.return_start_at
            < self.return_end_at
        ):
            raise SimulationContractError("决策、提交、成交、估值和收益窗口顺序无效")
        stage_times = {
            "decision": self.decision_at,
            "execution": self.execution_at,
            "valuation": self.valuation_at,
        }
        for item in self.input_availability:
            if item.available_at > stage_times[item.use_stage]:
                raise SimulationContractError(
                    f"{item.input_id} 在 {item.use_stage} 时尚不可见"
                )

    @property
    def semantics_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "input_availability": [item.to_dict() for item in self.input_availability],
            "signal_at": self.signal_at.isoformat(),
            "decision_at": self.decision_at.isoformat(),
            "order_submitted_at": self.order_submitted_at.isoformat(),
            "execution_at": self.execution_at.isoformat(),
            "valuation_at": self.valuation_at.isoformat(),
            "return_start_at": self.return_start_at.isoformat(),
            "return_end_at": self.return_end_at.isoformat(),
            "timezone": self.timezone,
            "calendar_id": self.calendar_id,
            "capacity_mode": self.capacity_mode.value,
            "capacity_model_id": self.capacity_model_id,
            "capacity_model_version": self.capacity_model_version,
            "cost_model_id": self.cost_model_id,
            "cost_model_version": self.cost_model_version,
            "claim_ceiling": self.claim_ceiling,
            "contract_version": self.contract_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "SimulationSemanticsV1":
        expected = {
            "input_availability", "signal_at", "decision_at", "order_submitted_at",
            "execution_at", "valuation_at", "return_start_at", "return_end_at",
            "timezone", "calendar_id", "capacity_mode", "capacity_model_id",
            "capacity_model_version", "cost_model_id", "cost_model_version",
            "claim_ceiling", "contract_version",
        }
        inputs = payload.get("input_availability")
        if set(payload) != expected or not isinstance(inputs, list):
            raise SimulationContractError("SimulationSemantics schema 无效")
        try:
            parsed_inputs = tuple(
                SimulationInputAvailability.from_dict(item)
                for item in inputs
                if isinstance(item, Mapping)
            )
            if len(parsed_inputs) != len(inputs):
                raise SimulationContractError("input availability item 无效")
            return cls(
                parsed_inputs,
                *(_parse_cn_time(payload[field], field) for field in (
                    "signal_at", "decision_at", "order_submitted_at", "execution_at",
                    "valuation_at", "return_start_at", "return_end_at",
                )),
                str(payload["timezone"]),
                str(payload["calendar_id"]),
                CapacityMode(str(payload["capacity_mode"])),
                str(payload["capacity_model_id"]),
                str(payload["capacity_model_version"]),
                str(payload["cost_model_id"]),
                str(payload["cost_model_version"]),
                str(payload["claim_ceiling"]),
                str(payload["contract_version"]),
            )
        except SimulationContractError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise SimulationContractError("SimulationSemantics 字段无效") from exc


def build_cn_daily_simulation_semantics(
    *,
    signal_session: date,
    execution_session: date,
    return_end_session: date,
    signal_source_hash: str,
    execution_source_hash: str,
    rule_source_hash: str,
    rule_available_at: datetime,
    capacity_mode: CapacityMode,
    capacity_model_id: str,
    capacity_model_version: str,
    cost_model_id: str,
    cost_model_version: str,
    capacity_available_at: datetime | None = None,
    capacity_source_hash: str | None = None,
    calendar_id: str = "cn.exchange.calendar.v1",
) -> SimulationSemanticsV1:
    timezone = ZoneInfo(SIMULATION_TIMEZONE)
    signal_at = datetime.combine(signal_session, time(15, 0), timezone)
    decision_at = signal_at
    order_at = datetime.combine(execution_session, time(9, 25), timezone)
    execution_at = datetime.combine(execution_session, time(9, 30), timezone)
    return_end = datetime.combine(return_end_session, time(9, 30), timezone)
    inputs = [
        SimulationInputAvailability("execution.open", execution_source_hash, execution_at, "execution"),
        SimulationInputAvailability("market.rule", rule_source_hash, rule_available_at, "decision"),
        SimulationInputAvailability("signal.target", signal_source_hash, signal_at, "decision"),
        SimulationInputAvailability("valuation.open", execution_source_hash, execution_at, "valuation"),
    ]
    if capacity_mode is CapacityMode.MODELED:
        if capacity_available_at is None or capacity_source_hash is None:
            raise SimulationContractError("modeled capacity 缺少可见时间或来源摘要")
        inputs.append(
            SimulationInputAvailability(
                "capacity.visible", capacity_source_hash, capacity_available_at, "execution"
            )
        )
    ceiling = _CAPACITY_CEILINGS[capacity_mode]
    return SimulationSemanticsV1(
        tuple(sorted(inputs, key=lambda item: item.input_id)),
        signal_at,
        decision_at,
        order_at,
        execution_at,
        execution_at,
        execution_at,
        return_end,
        SIMULATION_TIMEZONE,
        calendar_id,
        capacity_mode,
        capacity_model_id,
        capacity_model_version,
        cost_model_id,
        cost_model_version,
        ceiling,
    )


def capacity_claim_ceiling(mode: CapacityMode) -> str:
    return _CAPACITY_CEILINGS[mode]


__all__ = [
    "SIMULATION_INPUT_AVAILABILITY_VERSION",
    "SIMULATION_SEMANTICS_VERSION",
    "SIMULATION_TIMEZONE",
    "CapacityMode",
    "SimulationInputAvailability",
    "SimulationSemanticsV1",
    "build_cn_daily_simulation_semantics",
    "capacity_claim_ceiling",
]
