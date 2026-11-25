"""原生LEAN算法、数据绑定和执行预算的冻结声明。"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass

from research_pipeline.platform.asset_taxonomy import ASSET_CAPABILITY_MATRIX

from research_pipeline.platform.errors import StrategyProtocolError
from research_pipeline.platform.lean_contract import LeanAlgorithm as LeanAlgorithm


LEAN_NATIVE_STRATEGY_VERSION = "research-lean-native-strategy-v1"


def _require(condition, message):
    if not condition:
        raise StrategyProtocolError("lean_native_declaration_invalid", message)


@dataclass(frozen=True, kw_only=True)
class LeanAlgorithmLimits:
    initialize_seconds: int
    time_step_seconds: int

    def __post_init__(self):
        _require(all(type(value) is int and value > 0 for value in
                     (self.initialize_seconds, self.time_step_seconds)), "初始化和时间步预算须为正整数秒")


@dataclass(frozen=True, kw_only=True)
class LeanDataBinding:
    binding_id: str
    input_port: str
    request_id: str | None
    kind: str
    resolution: str
    columns: tuple[str, ...]
    daily_time: dict | None = None

    def __post_init__(self):
        for name in ("binding_id", "input_port"):
            _require(isinstance(getattr(self, name), str) and bool(getattr(self, name).strip()),
                     f"数据绑定{name}不能为空")
        _require(self.kind in {"bars", "event", "universe", "targets", "pit"}, "原生数据绑定kind不受支持")
        _require(self.request_id is None if self.kind == "targets" else
                 isinstance(self.request_id, str) and bool(self.request_id.strip()),
                 "模型目标由输入工件绑定且request_id须为空；其他来源须绑定实际QueryIR")
        _require(self.resolution in {"1m", "1d", "event"}, "数据频率须为1m、1d或event")
        _require(self.kind != "bars" or self.resolution in {"1m", "1d"}, "行情须声明日线或一分钟")
        _require(isinstance(self.columns, tuple) and bool(self.columns)
                 and all(isinstance(value, str) and value for value in self.columns)
                 and len(self.columns) == len(set(self.columns)), "绑定列须为非空且唯一的列名")
        if self.kind == "pit":
            _require(self.daily_time is not None and self.resolution == "event", "PIT 直读须绑定日级可见规则")
        if self.daily_time is not None:
            _require({"rule", "timezone", "sessions"} <= set(self.daily_time)
                     <= {"rule", "timezone", "sessions", "calendar_id", "calendar_source"},
                     "日期绑定须声明规则、时区及冻结交易日历")
            _require(self.daily_time["rule"] in {"next_session_open", "session_open", "business_effective_date",
                                               "next_session_open_following_publication_date"}, "日期可见规则不受支持")
            _require(self.daily_time["timezone"] == "Asia/Shanghai" and bool(self.daily_time["sessions"]),
                     "日期绑定须使用中国市场时区和非空交易日历")


@dataclass(frozen=True, kw_only=True)
class LeanStrategyDeclaration:
    strategy_id: str
    bundle_id: str
    algorithm: LeanAlgorithm
    input_bindings: tuple[LeanDataBinding, ...]
    limits: LeanAlgorithmLimits
    asset_class: str = "cn_stock"
    frequency: str = "1m"
    currency: str = "CNY"
    protocol_version: str = LEAN_NATIVE_STRATEGY_VERSION

    def __post_init__(self):
        _require(self.protocol_version == LEAN_NATIVE_STRATEGY_VERSION, "原生策略声明版本不受支持")
        _require(all(isinstance(value, str) and value for value in (self.strategy_id, self.bundle_id)),
                 "策略须绑定名称和实际bundle")
        _require(isinstance(self.algorithm, LeanAlgorithm) and isinstance(self.limits, LeanAlgorithmLimits),
                 "原生算法与预算声明无效")
        _require(self.frequency in {"1m", "1d"}, "执行行情频率须为1m或1d")
        capability = ASSET_CAPABILITY_MATRIX.get(self.asset_class)
        execution = "lean_minute" if self.frequency == "1m" else "lean_daily"
        _require(capability is not None and execution in capability.simulation_entries and self.currency == "CNY",
                 "当前金融模型支持人民币股票、ETF或期货")
        _require(isinstance(self.input_bindings, tuple) and bool(self.input_bindings)
                 and all(isinstance(value, LeanDataBinding) for value in self.input_bindings),
                 "原生策略须声明数据绑定")
        ids = [value.binding_id for value in self.input_bindings]
        _require(len(ids) == len(set(ids)), "原生数据绑定ID不能重复")
        _require(any(value.kind == "bars" and value.resolution == self.frequency for value in self.input_bindings),
                 "原生策略须绑定执行频率的行情")

    def to_dict(self):
        result = asdict(self)
        result["input_bindings"] = [{**asdict(value), "columns": list(value.columns)}
                                    for value in self.input_bindings]
        return result


def parse_lean_strategy_declaration(payload):
    _require(isinstance(payload, Mapping), "原生策略声明须为对象")
    values = dict(payload)
    values["algorithm"] = LeanAlgorithm(**dict(values["algorithm"]))
    values["limits"] = LeanAlgorithmLimits(**dict(values["limits"]))
    values["input_bindings"] = tuple(LeanDataBinding(**{**dict(value), "columns": tuple(value["columns"])})
                                     for value in values["input_bindings"])
    return LeanStrategyDeclaration(**values)
