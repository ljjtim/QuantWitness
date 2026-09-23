"""按有效期和可见时间解析的市场规则快照。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping, Sequence

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.market_rule_defaults import (
    CnEtfDailyMarketRuleProfile,
    build_cn_etf_daily_rule_payloads,
)

from .instruments import Instrument
from .models import DomainContractError
from .time import parse_session, require_aware_datetime


@dataclass(frozen=True)
class MarketRuleSnapshot:
    rule_id: str
    version: int
    market: str
    instrument_type: str
    effective_start: date
    effective_end: date | None
    available_time: datetime
    official_source_id: str
    evidence_url: str
    parameters: tuple[tuple[str, object], ...]

    def __post_init__(self) -> None:
        if not self.rule_id.strip() or self.version < 1:
            raise DomainContractError("rule_id/version 无效")
        if self.effective_end is not None and self.effective_end < self.effective_start:
            raise DomainContractError("规则有效期倒置")
        require_aware_datetime(self.available_time, "available_time")
        if not self.official_source_id.strip() or not self.evidence_url.strip():
            raise DomainContractError("规则必须绑定批准来源 ID 和证据 URL")
        if self.parameters != tuple(sorted(self.parameters, key=lambda item: item[0])):
            raise DomainContractError("规则参数必须按名称排序")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise DomainContractError("规则参数不能重复")

    @property
    def content_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "rule_id": self.rule_id,
            "version": self.version,
            "market": self.market,
            "instrument_type": self.instrument_type,
            "effective_start": self.effective_start.isoformat(),
            "effective_end": None if self.effective_end is None else self.effective_end.isoformat(),
            "available_time": self.available_time.isoformat(),
            "official_source_id": self.official_source_id,
            "evidence_url": self.evidence_url,
            "parameters": [[key, value] for key, value in self.parameters],
        }

    def parameter(self, name: str) -> object:
        try:
            return dict(self.parameters)[name]
        except KeyError as exc:
            raise DomainContractError(f"规则缺少参数: {name}") from exc


@dataclass(frozen=True)
class RuleBinding:
    instrument_hash: str
    session: date
    rule: MarketRuleSnapshot
    explanation: str


def build_cn_etf_daily_rule_snapshots(
    *,
    instrument_codes: Sequence[str],
    bond_etf_codes: Sequence[str],
    equity_etf_codes: Sequence[str],
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
) -> dict[str, MarketRuleSnapshot]:
    """由受控 profile 与独立佣金假设构造 ETF 日频执行规则。"""
    try:
        payloads = build_cn_etf_daily_rule_payloads(
            instrument_codes=instrument_codes,
            bond_etf_codes=bond_etf_codes,
            equity_etf_codes=equity_etf_codes,
            profile=profile,
            commission_ppm=commission_ppm,
            min_commission_units=min_commission_units,
        )
    except ValueError as exc:
        raise DomainContractError(str(exc)) from exc
    rules: dict[str, MarketRuleSnapshot] = {}
    for code, payload in payloads.items():
        rules[code] = MarketRuleSnapshot(
            rule_id=str(payload["rule_id"]),
            version=int(payload["version"]),
            market=str(payload["market"]),
            instrument_type=str(payload["instrument_type"]),
            effective_start=date.fromisoformat(str(payload["effective_start"])),
            effective_end=date.fromisoformat(str(payload["effective_end"])),
            available_time=datetime.fromisoformat(str(payload["available_time"])),
            official_source_id=str(payload["official_source_id"]),
            evidence_url=str(payload["evidence_url"]),
            parameters=tuple(
                (str(item[0]), item[1]) for item in payload["parameters"]
            ),
        )
    return rules


def cn_etf_daily_rule_bundle_hash(rules: Mapping[str, MarketRuleSnapshot]) -> str:
    """对已规范化的 ETF 规则全集计算唯一摘要。"""
    if not rules:
        raise DomainContractError("ETF 规则集合不能为空")
    return typed_canonical_hash({code: rule.to_dict() for code, rule in sorted(rules.items())})


def resolve_market_rule(
    rules: tuple[MarketRuleSnapshot, ...],
    *,
    instrument: Instrument,
    session: object,
    as_of: datetime,
) -> RuleBinding:
    require_aware_datetime(as_of, "as_of")
    current = parse_session(session)
    instrument.require_tradable_on(current)
    matches = tuple(
        rule for rule in rules
        if rule.market == instrument.instrument_id.market
        and rule.instrument_type == instrument.instrument_id.instrument_type
        and rule.effective_start <= current
        and (rule.effective_end is None or current <= rule.effective_end)
        and rule.available_time <= as_of
    )
    if len(matches) != 1:
        raise DomainContractError("市场规则快照缺失、重叠或在决策时尚不可见")
    rule = matches[0]
    return RuleBinding(instrument.instrument_hash, current, rule, f"按 {as_of.isoformat()} 选择 {rule.rule_id}@{rule.version}")


__all__ = [
    "MarketRuleSnapshot",
    "RuleBinding",
    "build_cn_etf_daily_rule_snapshots",
    "cn_etf_daily_rule_bundle_hash",
    "resolve_market_rule",
]
