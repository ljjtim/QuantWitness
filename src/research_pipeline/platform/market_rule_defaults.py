"""ETF 日频市场规则 profile 的唯一受控事实源。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping, Sequence

from .canonical import typed_canonical_hash


CN_ETF_DAILY_MARKET_RULE_PROFILE_VERSION = (
    "research-cn-etf-daily-market-rule-profile-v1"
)


@dataclass(frozen=True)
class CnEtfDailyMarketRuleProfile:
    """本地受控的 ETF 日频规则，不冒充交易所原始发布字节。"""

    profile_id: str
    profile_version: int
    effective_start: date
    effective_end: date
    rule_available_at: datetime
    curated_at: datetime
    source_mode: str
    source_id: str
    source_reference: str
    lot_size: int
    settlement_days: tuple[tuple[str, int], ...]
    sell_tax_ppm: int
    transfer_fee_ppm: int
    evidence_level: str
    claim_ceiling: str
    limitations: tuple[str, ...]
    contract_version: str = CN_ETF_DAILY_MARKET_RULE_PROFILE_VERSION

    def __post_init__(self) -> None:
        if (
            not self.profile_id
            or self.profile_version < 1
            or self.effective_end < self.effective_start
            or self.rule_available_at.tzinfo is None
            or self.curated_at.tzinfo is None
            or self.lot_size < 1
            or min(self.sell_tax_ppm, self.transfer_fee_ppm) < 0
        ):
            raise ValueError("ETF 日频市场规则 profile 基础字段无效")
        if self.contract_version != CN_ETF_DAILY_MARKET_RULE_PROFILE_VERSION:
            raise ValueError("ETF 日频市场规则 profile 版本无效")
        if self.source_mode != "curated_local_citation":
            raise ValueError("ETF 日频规则只能使用本地受控引用模式")
        if self.evidence_level != "local_only" or self.claim_ceiling != (
            "research_observation"
        ):
            raise ValueError("ETF 日频规则结论上限必须保持 local_only")
        if not self.source_id or not self.source_reference:
            raise ValueError("ETF 日频规则必须绑定受控来源引用")
        if self.settlement_days != (("bond", 0), ("equity", 1)):
            raise ValueError("ETF 日频规则必须显式闭合债券 T+0 与股票 T+1")
        if self.limitations != tuple(sorted(set(self.limitations))):
            raise ValueError("ETF 日频规则 limitations 必须唯一并规范排序")

    @property
    def profile_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "market": "cn_etf",
            "instrument_type": "etf",
            "effective_start": self.effective_start.isoformat(),
            "effective_end": self.effective_end.isoformat(),
            "rule_available_at": self.rule_available_at.isoformat(),
            "curated_at": self.curated_at.isoformat(),
            "source_mode": self.source_mode,
            "source_id": self.source_id,
            "source_reference": self.source_reference,
            "source_content_archived": False,
            "lot_size": self.lot_size,
            "settlement_days": {
                category: days for category, days in self.settlement_days
            },
            "sell_tax_ppm": self.sell_tax_ppm,
            "transfer_fee_ppm": self.transfer_fee_ppm,
            "evidence_level": self.evidence_level,
            "claim_ceiling": self.claim_ceiling,
            "limitations": list(self.limitations),
            "contract_version": self.contract_version,
        }

    def settlement_days_for(self, category: str) -> int:
        try:
            return dict(self.settlement_days)[category]
        except KeyError as exc:
            raise ValueError(f"ETF 日频规则不支持品类: {category}") from exc

    def require_covers(self, start: date, end: date) -> None:
        if start > end or start < self.effective_start or end > self.effective_end:
            raise ValueError(
                "ETF 日频市场规则 profile 未覆盖研究窗口: "
                f"{start.isoformat()}..{end.isoformat()}"
            )
        start_boundary = datetime.combine(start, datetime.min.time()).replace(
            tzinfo=self.rule_available_at.tzinfo
        )
        if self.rule_available_at > start_boundary:
            raise ValueError("ETF 日频市场规则在研究起点尚不可见")


_CN_ETF_DAILY_MARKET_RULE_PROFILES = (
    CnEtfDailyMarketRuleProfile(
        profile_id="cn_etf.daily.curated.v1",
        profile_version=1,
        effective_start=date(2020, 1, 1),
        effective_end=date(2026, 8, 31),
        rule_available_at=datetime.fromisoformat("2019-12-31T00:00:00+08:00"),
        curated_at=datetime.fromisoformat("2026-08-31T00:00:00+08:00"),
        source_mode="curated_local_citation",
        source_id="curated.cn-etf.daily-market-rules.v1",
        source_reference="https://www.sse.com.cn/assortment/fund/etf/home/",
        lot_size=100,
        settlement_days=(("bond", 0), ("equity", 1)),
        sell_tax_ppm=0,
        transfer_fee_ppm=0,
        evidence_level="local_only",
        claim_ceiling="research_observation",
        limitations=tuple(sorted((
            "本地整理的规则引用，不是交易所发布字节归档",
            "仅支持个人历史研究，不能作为实盘可交易性证明",
        ))),
    ),
)

CN_ETF_DAILY_MARKET_RULE_PROFILE_IDS = tuple(
    item.profile_id for item in _CN_ETF_DAILY_MARKET_RULE_PROFILES
)


def resolve_cn_etf_daily_market_rule_profile(
    profile_id: str,
) -> CnEtfDailyMarketRuleProfile:
    """按稳定 ID 解析唯一受控 profile；未知 ID 不回退默认规则。"""

    matches = tuple(
        item
        for item in _CN_ETF_DAILY_MARKET_RULE_PROFILES
        if item.profile_id == profile_id
    )
    if len(matches) != 1:
        raise ValueError(f"未知 ETF 日频市场规则 profile: {profile_id}")
    return matches[0]


def require_cn_etf_daily_market_rule_profile_payload(
    *,
    profile_id: str,
    payload: object,
    profile_hash: object,
) -> CnEtfDailyMarketRuleProfile:
    """复验 package/plan 封存的 profile 内容仍等于受控事实源。"""

    profile = resolve_cn_etf_daily_market_rule_profile(profile_id)
    if not isinstance(payload, Mapping):
        raise ValueError("ETF 日频市场规则 profile 内容必须是映射")
    if typed_canonical_hash(dict(payload)) != profile.profile_hash:
        raise ValueError("ETF 日频市场规则 profile 内容与受控事实源不一致")
    if profile_hash != profile.profile_hash:
        raise ValueError("ETF 日频市场规则 profile hash 与受控事实源不一致")
    return profile


def build_cn_etf_daily_rule_payloads(
    *,
    instrument_codes: Sequence[str],
    bond_etf_codes: Sequence[str],
    equity_etf_codes: Sequence[str],
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
) -> dict[str, dict[str, object]]:
    """用受控市场规则和独立佣金假设生成执行 policy 载荷。"""

    codes = tuple(str(item) for item in instrument_codes)
    bond_codes = frozenset(str(item) for item in bond_etf_codes)
    equity_codes = frozenset(str(item) for item in equity_etf_codes)
    if (
        not codes
        or codes != tuple(sorted(set(codes)))
        or bond_codes & equity_codes
        or bond_codes | equity_codes != set(codes)
    ):
        raise ValueError("ETF 标的必须由互斥的 bond/equity 分类精确覆盖")
    if commission_ppm < 0 or min_commission_units < 0:
        raise ValueError("ETF 佣金研究假设不能为负")
    payloads: dict[str, dict[str, object]] = {}
    for code in codes:
        category = "bond" if code in bond_codes else "equity"
        parameters = {
            "commission_ppm": int(commission_ppm),
            "etf_category": category,
            "lot_size": profile.lot_size,
            "min_commission_units": int(min_commission_units),
            "sell_tax_ppm": profile.sell_tax_ppm,
            "settlement_days": profile.settlement_days_for(category),
            "transfer_fee_ppm": profile.transfer_fee_ppm,
        }
        payloads[code] = {
            "rule_id": f"{profile.profile_id}.{category}.execution-policy",
            "version": profile.profile_version,
            "market": "cn_etf",
            "instrument_type": "etf",
            "effective_start": profile.effective_start.isoformat(),
            "effective_end": profile.effective_end.isoformat(),
            "available_time": profile.rule_available_at.isoformat(),
            "official_source_id": profile.source_id,
            "evidence_url": profile.source_reference,
            "parameters": [
                [key, value] for key, value in sorted(parameters.items())
            ],
        }
    return payloads


__all__ = [
    "CN_ETF_DAILY_MARKET_RULE_PROFILE_IDS",
    "CN_ETF_DAILY_MARKET_RULE_PROFILE_VERSION",
    "CnEtfDailyMarketRuleProfile",
    "build_cn_etf_daily_rule_payloads",
    "require_cn_etf_daily_market_rule_profile_payload",
    "resolve_cn_etf_daily_market_rule_profile",
]
