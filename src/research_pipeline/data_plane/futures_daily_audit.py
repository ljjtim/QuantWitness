"""中国期货日频数据的列式 D0 审计；只报告，不修复或填充。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import json
import math
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo

from research_pipeline.platform import canonical_json, typed_canonical_hash

from .errors import QualityGateError


FUTURES_D0_PROFILE_VERSION = "futures-d0-profile-v1"
FUTURES_D0_REPORT_VERSION = "futures-d0-report-v1"
FUTURES_D0_RECEIPT_VERSION = "futures-d0-receipt-v1"
_ZONE = ZoneInfo("Asia/Shanghai")
_BASE_MARKET_FIELDS = (
    "fld_futures_daily_date",
    "fld_futures_daily_code",
    "fld_futures_daily_open",
    "fld_futures_daily_high",
    "fld_futures_daily_low",
    "fld_futures_daily_close",
)
_OBSERVATION_MARKET_FIELDS = (
    "fld_futures_daily_date",
    "fld_futures_daily_code",
    "fld_futures_daily_close",
    "fld_futures_daily_volume",
    "fld_futures_daily_open_interest",
)
_TRADING_MARKET_FIELDS = tuple(
    sorted(set(_BASE_MARKET_FIELDS) | set(_OBSERVATION_MARKET_FIELDS))
)
_PRIMARY_KEYS = {
    "market": ("fld_futures_daily_date", "fld_futures_daily_code"),
    "lifecycle": ("fld_future_code",),
    "settlement": ("fld_futures_settlement_date", "fld_futures_settlement_code"),
    "multiplier": (
        "fld_futures_multiplier_symbol",
        "fld_futures_multiplier_exchange",
        "fld_futures_multiplier_effective",
    ),
    "margin": (
        "fld_futures_margin_day",
        "fld_futures_margin_code",
        "fld_futures_margin_id",
    ),
    "fee": ("fld_futures_fee_day", "fld_futures_fee_code", "fld_futures_fee_id"),
    "tick": (
        "fld_futures_tick_product",
        "fld_futures_tick_exchange",
        "fld_futures_tick_effective_from",
    ),
}
_OFFICIAL_FIELDS = {
    "dataset_id": "fld_futures_authoritative_dataset",
    "key_json": "fld_futures_authoritative_key",
    "field_id": "fld_futures_authoritative_field",
    "expected_value": "fld_futures_authoritative_expected",
    "available_at": "fld_futures_authoritative_available_at",
    "source_hash": "fld_futures_authoritative_source_hash",
}


@dataclass(frozen=True)
class FuturesD0Profile:
    audit_mode: str
    rule_cutoff_policy: str
    trading_contracts: tuple[str, ...]
    required_market_fields: tuple[str, ...]
    start: date
    end: date
    as_of: datetime
    max_missing_ratio: float
    min_rule_coverage_ratio: float
    max_issue_samples: int = 100
    max_reconciliation_samples: int = 1_000
    contract_version: str = FUTURES_D0_PROFILE_VERSION

    def __post_init__(self) -> None:
        if (
            self.contract_version != FUTURES_D0_PROFILE_VERSION
            or self.audit_mode not in {"observation", "trading"}
            or self.rule_cutoff_policy != "day_session_open_0900"
            or not self.trading_contracts
            or self.trading_contracts != tuple(sorted(set(self.trading_contracts)))
            or not set(
                _TRADING_MARKET_FIELDS
                if self.audit_mode == "trading"
                else _OBSERVATION_MARKET_FIELDS
            )
            <= set(self.required_market_fields)
            or self.required_market_fields
            != tuple(sorted(set(self.required_market_fields)))
            or self.start > self.end
            or self.as_of.tzinfo is None
            or self.as_of.utcoffset() is None
            or self.as_of.astimezone(_ZONE).date() <= self.end
            or not 0.0 <= self.max_missing_ratio <= 1.0
            or not 0.0 <= self.min_rule_coverage_ratio <= 1.0
            or not 1 <= self.max_issue_samples <= 10_000
            or not 1 <= self.max_reconciliation_samples <= 100_000
        ):
            raise QualityGateError("期货 D0 profile 无效")

    @property
    def profile_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "audit_mode": self.audit_mode,
            "rule_cutoff_policy": self.rule_cutoff_policy,
            "trading_contracts": list(self.trading_contracts),
            "required_market_fields": list(self.required_market_fields),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "as_of": self.as_of.isoformat(),
            "max_missing_ratio": self.max_missing_ratio,
            "min_rule_coverage_ratio": self.min_rule_coverage_ratio,
            "max_issue_samples": self.max_issue_samples,
            "max_reconciliation_samples": self.max_reconciliation_samples,
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class FuturesD0Report:
    payload: Mapping[str, object]

    @property
    def audit_hash(self) -> str:
        body = dict(self.payload)
        body.pop("d0_receipt", None)
        return typed_canonical_hash(body)

    def to_dict(self) -> dict[str, object]:
        return {**self.payload, "audit_hash": self.audit_hash}


class _Issues:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.counts: dict[str, int] = {}
        self.samples: list[dict[str, object]] = []

    def add(
        self,
        code: str,
        *,
        dataset: str,
        key: str,
        detail: str,
        severity: str = "fail",
    ) -> None:
        identity = f"{severity}:{code}"
        self.counts[identity] = self.counts.get(identity, 0) + 1
        if len(self.samples) < self.limit:
            self.samples.append(
                {
                    "error_code": code,
                    "severity": severity,
                    "dataset": dataset,
                    "key": key,
                    "detail": detail,
                }
            )

    @property
    def failed(self) -> bool:
        return any(key.startswith("fail:") for key in self.counts)


def audit_futures_daily_data(
    datasets: Mapping[str, Iterable[object]],
    *,
    profile: FuturesD0Profile,
    snapshot_hashes: Mapping[str, str],
) -> FuturesD0Report:
    """流式读取 Arrow batch，审计本地质量；外部样本只作可选增强证据。"""

    required = (
        {"market", "lifecycle"}
        if profile.audit_mode == "observation"
        else {"market", "lifecycle", "settlement", "multiplier", "margin", "fee"}
    )
    if not required <= set(datasets):
        raise QualityGateError(f"期货 D0 缺少输入: {sorted(required - set(datasets))}")
    if set(snapshot_hashes) != set(datasets) or any(
        not _sha256(value) for value in snapshot_hashes.values()
    ):
        raise QualityGateError("期货 D0 snapshot hash 不闭合")

    issues = _Issues(profile.max_issue_samples)
    official_rows = _read_official_rows(
        datasets.get("official", ()),
        profile=profile,
        issues=issues,
    )
    requested_lookups = {
        (row["dataset_id"], row["key_json"], row["field_id"]) for row in official_rows
    }
    local_values: dict[tuple[str, str, str], str] = {}

    market_keys: set[tuple[date, str]] = set()
    declared_contracts = set(profile.trading_contracts)
    field_nulls = {field: 0 for field in profile.required_market_fields}
    market_rows = 0
    for row in _rows(datasets["market"], profile.required_market_fields):
        trading_date = _date(row["fld_futures_daily_date"], "market.date")
        code = _text(row["fld_futures_daily_code"], "market.code")
        if (
            code not in declared_contracts
            or not profile.start <= trading_date <= profile.end
        ):
            continue
        market_rows += 1
        key = (trading_date, code)
        key_text = f"{trading_date.isoformat()}:{code}"
        if key in market_keys:
            issues.add(
                "futures_d0.duplicate_primary_key",
                dataset="market",
                key=key_text,
                detail="date/code 重复",
            )
        market_keys.add(key)
        for field in profile.required_market_fields:
            if row.get(field) is None:
                field_nulls[field] += 1
        try:
            _positive(row["fld_futures_daily_close"], "fld_futures_daily_close")
            _nonnegative(row["fld_futures_daily_volume"], "fld_futures_daily_volume")
            _nonnegative(
                row["fld_futures_daily_open_interest"],
                "fld_futures_daily_open_interest",
            )
        except QualityGateError as exc:
            issues.add(
                "futures_d0.observation_market_invalid",
                dataset="market",
                key=key_text,
                detail=str(exc),
            )
        if set(_BASE_MARKET_FIELDS) <= set(profile.required_market_fields):
            try:
                open_price, high, low, close = (
                    _positive(row[field], field) for field in _BASE_MARKET_FIELDS[2:]
                )
                if (
                    low > min(open_price, close)
                    or high < max(open_price, close)
                    or low > high
                ):
                    raise QualityGateError("OHLC 包络关系无效")
            except QualityGateError as exc:
                issues.add(
                    "futures_d0.ohlc_invalid",
                    dataset="market",
                    key=key_text,
                    detail=str(exc),
                    severity=("fail" if profile.audit_mode == "trading" else "warning"),
                )
        _capture_requested_values(
            "cn_futures.daily_bar",
            row,
            _PRIMARY_KEYS["market"],
            requested_lookups,
            local_values,
        )

    if market_rows == 0:
        issues.add(
            "futures_d0.market_empty",
            dataset="market",
            key="*",
            detail="研究窗口没有日线",
        )
    missing_ratios = {
        field: (1.0 if market_rows == 0 else count / market_rows)
        for field, count in sorted(field_nulls.items())
    }
    for field, ratio in missing_ratios.items():
        if ratio > profile.max_missing_ratio:
            issues.add(
                "futures_d0.missing_ratio_exceeded",
                dataset="market",
                key=field,
                detail=f"missing_ratio={ratio:.12g}",
            )

    lifecycle = _materialize(
        "cn_futures.instrument_master",
        datasets["lifecycle"],
        _PRIMARY_KEYS["lifecycle"],
        requested_lookups,
        local_values,
    )
    settlement = _materialize(
        "cn_futures.settlement",
        datasets.get("settlement", ()),
        _PRIMARY_KEYS["settlement"],
        requested_lookups,
        local_values,
    )
    multiplier = _materialize(
        "cn_futures.multiplier",
        datasets.get("multiplier", ()),
        _PRIMARY_KEYS["multiplier"],
        requested_lookups,
        local_values,
    )
    margin = _materialize(
        "cn_futures.margin_rule",
        datasets.get("margin", ()),
        _PRIMARY_KEYS["margin"],
        requested_lookups,
        local_values,
    )
    fee = _materialize(
        "cn_futures.fee_rule",
        datasets.get("fee", ()),
        _PRIMARY_KEYS["fee"],
        requested_lookups,
        local_values,
    )
    tick = _materialize(
        "cn_futures.tick_rule",
        datasets.get("tick", ()),
        _PRIMARY_KEYS["tick"],
        requested_lookups,
        local_values,
    )

    trading_keys = tuple(
        sorted(
            key
            for key in market_keys
            if key[1] in declared_contracts
            and profile.start <= key[0] <= profile.end
        )
    )
    observed_contracts = {code for _, code in trading_keys}
    for code in sorted(set(profile.trading_contracts) - observed_contracts):
        issues.add(
            "futures_d0.trading_contract_missing",
            dataset="market",
            key=code,
            detail="声明的交易合约在研究窗口没有日线",
        )
    coverage_counts = {
        name: 0
        for name in ("lifecycle", "settlement", "multiplier", "margin", "fee", "tick")
    }
    observation_only = profile.audit_mode == "observation"
    lifecycle_by_code = _group_rows(lifecycle, "fld_future_code")
    if observation_only:
        settlement_by_key: Mapping[
            tuple[object, ...], tuple[Mapping[str, object], ...]
        ] = {}
        multiplier_by_identity: Mapping[
            tuple[object, ...], tuple[Mapping[str, object], ...]
        ] = {}
        margin_by_key: Mapping[
            tuple[object, ...], tuple[Mapping[str, object], ...]
        ] = {}
        fee_by_key: Mapping[tuple[object, ...], tuple[Mapping[str, object], ...]] = {}
        tick_by_identity: Mapping[
            tuple[object, ...], tuple[Mapping[str, object], ...]
        ] = {}
    else:
        settlement_by_key = _group_rows(
            settlement,
            "fld_futures_settlement_code",
            "fld_futures_settlement_date",
            date_fields={"fld_futures_settlement_date"},
            accepted_values={"fld_futures_settlement_code": declared_contracts},
        )
        multiplier_by_identity = _group_rows(
            multiplier,
            "fld_futures_multiplier_symbol",
            "fld_futures_multiplier_exchange",
            uppercase_fields={
                "fld_futures_multiplier_symbol",
                "fld_futures_multiplier_exchange",
            },
        )
        margin_by_key = _group_rows(
            margin,
            "fld_futures_margin_code",
            "fld_futures_margin_day",
            date_fields={"fld_futures_margin_day"},
            accepted_values={"fld_futures_margin_code": declared_contracts},
        )
        fee_by_key = _group_rows(
            fee,
            "fld_futures_fee_code",
            "fld_futures_fee_day",
            date_fields={"fld_futures_fee_day"},
            accepted_values={"fld_futures_fee_code": declared_contracts},
        )
        tick_by_identity = _group_rows(
            tick,
            "fld_futures_tick_product",
            "fld_futures_tick_exchange",
            uppercase_fields={
                "fld_futures_tick_product",
                "fld_futures_tick_exchange",
            },
        )
    for trading_date, code in trading_keys:
        key_text = f"{trading_date.isoformat()}:{code}"
        lifecycle_matches = [
            row
            for row in lifecycle_by_code.get((code,), ())
            if _date(row.get("fld_future_start"), "lifecycle.start")
            <= trading_date
            <= _date(row.get("fld_future_end"), "lifecycle.end")
        ]
        _count_unique(
            lifecycle_matches,
            name="lifecycle",
            code="futures_d0.lifecycle_missing_or_overlap",
            key=key_text,
            counts=coverage_counts,
            issues=issues,
        )
        if observation_only:
            continue
        settlement_matches = [
            row
            for row in settlement_by_key.get((code, trading_date), ())
            if _finite_positive(row.get("fld_futures_settlement_price"))
        ]
        _count_unique(
            settlement_matches,
            name="settlement",
            code="futures_d0.settlement_missing_or_overlap",
            key=key_text,
            counts=coverage_counts,
            issues=issues,
            required=not observation_only,
        )
        product, exchange = _instrument_identity(code)
        multiplier_matches = [
            row
            for row in multiplier_by_identity.get((product, exchange), ())
            if _date(row.get("fld_futures_multiplier_effective"), "multiplier.start")
            <= trading_date
            <= _date(row.get("fld_futures_multiplier_cancel"), "multiplier.end")
            and _finite_positive(row.get("fld_futures_multiplier_value"))
        ]
        _count_unique(
            multiplier_matches,
            name="multiplier",
            code="futures_d0.multiplier_missing_or_overlap",
            key=key_text,
            counts=coverage_counts,
            issues=issues,
            required=not observation_only,
        )
        cutoff = _rule_cutoff(trading_date, profile.rule_cutoff_policy)
        margin_matches = _visible_daily_rules(
            margin_by_key.get((code, trading_date), ()),
            code=code,
            trading_date=trading_date,
            day_field="fld_futures_margin_day",
            code_field="fld_futures_margin_code",
            added_field="fld_futures_margin_add_time",
            revised_field="fld_futures_margin_mod_time",
            cutoff=cutoff,
        )
        _count_unique(
            margin_matches,
            name="margin",
            code="futures_d0.margin_missing_future_or_overlap",
            key=key_text,
            counts=coverage_counts,
            issues=issues,
            required=not observation_only,
        )
        fee_matches = _visible_daily_rules(
            fee_by_key.get((code, trading_date), ()),
            code=code,
            trading_date=trading_date,
            day_field="fld_futures_fee_day",
            code_field="fld_futures_fee_code",
            added_field="fld_futures_fee_add_time",
            revised_field="fld_futures_fee_mod_time",
            cutoff=cutoff,
        )
        _count_unique(
            fee_matches,
            name="fee",
            code="futures_d0.fee_missing_future_or_overlap",
            key=key_text,
            counts=coverage_counts,
            issues=issues,
            required=not observation_only,
        )
        tick_matches = [
            row
            for row in tick_by_identity.get((product, exchange), ())
            if _date(row.get("fld_futures_tick_effective_from"), "tick.start")
            <= trading_date
            <= _date(row.get("fld_futures_tick_effective_to"), "tick.end")
            and _datetime(row.get("fld_futures_tick_available_at"), "tick.available")
            <= cutoff
            and _datetime(row.get("fld_futures_tick_revised_at"), "tick.revised")
            <= cutoff
            and _finite_positive(row.get("fld_futures_tick_value"))
            and _sha256(row.get("fld_futures_tick_source_hash"))
        ]
        if len(tick_matches) == 1:
            coverage_counts["tick"] += 1
        elif profile.audit_mode == "trading":
            issues.add(
                "futures_d0.tick_missing_future_or_overlap",
                dataset="tick",
                key=key_text,
                detail=f"visible_matches={len(tick_matches)}",
            )
        else:
            issues.add(
                "futures_d0.tick_unavailable_observation_only",
                dataset="tick",
                key=key_text,
                detail=f"visible_matches={len(tick_matches)}",
                severity="warning",
            )

    expected = len(trading_keys)
    coverage_matrix = []
    for dataset, observed in sorted(coverage_counts.items()):
        ratio = 1.0 if expected == 0 else observed / expected
        required_for_mode = dataset == "lifecycle" or profile.audit_mode == "trading"
        status = (
            "not_applicable"
            if not required_for_mode
            else ("pass" if ratio >= profile.min_rule_coverage_ratio else "fail")
        )
        coverage_matrix.append(
            {
                "dataset": dataset,
                "expected_count": expected,
                "covered_count": observed,
                "coverage_ratio": ratio,
                "required_for_mode": required_for_mode,
                "status": status,
            }
        )
        if status == "fail":
            issues.add(
                "futures_d0.rule_coverage_below_threshold",
                dataset=dataset,
                key="*",
                detail=f"coverage_ratio={ratio:.12g}",
            )

    reconciliation = _reconcile(
        official_rows,
        local_values=local_values,
        profile=profile,
        issues=issues,
    )
    local_status = "fail" if issues.failed else "pass"
    tick_coverage = next(
        (item for item in coverage_matrix if item.get("dataset") == "tick"),
        None,
    )
    local_trading_ready = profile.audit_mode != "trading" or (
        isinstance(tick_coverage, Mapping) and tick_coverage.get("status") == "pass"
    )
    # readiness_state 只描述本地数据是否满足所选研究模式。外部
    # 供应商/官方样本只能提升 evidence_level，不能决定本地 D0 是否就绪。
    readiness = (
        "failed"
        if local_status == "fail"
        else (
            "d0_ready"
            if (profile.audit_mode == "trading" and local_trading_ready)
            else ("local_consistent" if local_trading_ready else "local_only")
        )
    )
    evidence_level = (
        "official_cross_checked" if reconciliation["status"] == "pass" else "local_only"
    )
    claim_ceiling = (
        "bar_level_historical_research"
        if profile.audit_mode == "trading" and local_trading_ready
        else "research_observation"
    )
    limitations = []
    if reconciliation["status"] == "NOT_RUN":
        limitations.append(
            "未提供外部权威样本；结果仅具备本地一致性证据，不宣称官方交叉核对"
        )
    elif reconciliation["status"] == "fail":
        limitations.append(
            "外部样本存在差异；不影响本地观察研究，但不能宣称外部交叉核对通过"
        )
    if not tick and profile.audit_mode == "trading":
        limitations.append("没有可信 tick size 数据，不得进入交易仿真")
    data_quality = {
        "market_row_count": market_rows,
        "trading_key_count": expected,
        "field_missing_ratios": missing_ratios,
        "issue_counts": dict(sorted(issues.counts.items())),
        "issue_samples": issues.samples,
        "status": local_status,
    }
    trading_ready = (
        profile.audit_mode == "trading"
        and local_status == "pass"
        and local_trading_ready
    )
    body = {
        "contract_version": FUTURES_D0_REPORT_VERSION,
        "profile": profile.to_dict(),
        "profile_hash": profile.profile_hash,
        "data_quality": data_quality,
        "coverage_matrix": coverage_matrix,
        "data_reconciliation": reconciliation,
        "snapshot_hashes": dict(sorted(snapshot_hashes.items())),
        "status": local_status,
        "readiness_state": readiness,
        "evidence_level": evidence_level,
        "claim_ceiling": claim_ceiling,
        "trading_ready": trading_ready,
        "limitations": limitations,
    }
    report_hash = typed_canonical_hash(body)
    receipt = None
    if local_status == "pass":
        receipt_body = {
            "contract_version": FUTURES_D0_RECEIPT_VERSION,
            "status": "pass",
            "audit_hash": report_hash,
            "profile_hash": profile.profile_hash,
            "evidence_level": evidence_level,
            "claim_ceiling": claim_ceiling,
            "trading_ready": trading_ready,
            "official_snapshot_hash": reconciliation["official_snapshot_hash"],
            "snapshot_hashes": dict(sorted(snapshot_hashes.items())),
        }
        receipt = {**receipt_body, "receipt_hash": typed_canonical_hash(receipt_body)}
    return FuturesD0Report({**body, "d0_receipt": receipt})


def require_futures_d0_receipt(
    payload: Mapping[str, object],
    *,
    require_trading_rules: bool = False,
    expected_rule_cutoff_policy: str | None = None,
) -> None:
    """校验本地 D0 receipt；交易消费还须具备当时可见的交易规则。"""

    receipt = payload.get("d0_receipt")
    profile = payload.get("profile")
    readiness = payload.get("readiness_state")
    local_status = (
        payload.get("status") == "pass"
        and isinstance(payload.get("data_quality"), Mapping)
        and payload["data_quality"].get("status") == "pass"
    )
    trading_mode = (
        isinstance(profile, Mapping) and profile.get("audit_mode") == "trading"
    )
    coverage = payload.get("coverage_matrix")
    coverage_by_dataset = (
        {
            str(item.get("dataset")): item
            for item in coverage
            if isinstance(item, Mapping)
        }
        if isinstance(coverage, list)
        else {}
    )
    required_trading_input_roles = (
        "lifecycle",
        "settlement",
        "multiplier",
        "margin",
        "fee",
        "tick",
    )
    snapshots = payload.get("snapshot_hashes")
    trading_rules_ready = (
        set(coverage_by_dataset)
        == {"lifecycle", "settlement", "multiplier", "margin", "fee", "tick"}
        and isinstance(snapshots, Mapping)
        and all(
            coverage_by_dataset[role].get("status") == "pass"
            and coverage_by_dataset[role].get("required_for_mode") is True
            and _sha256(snapshots.get(role))
            for role in required_trading_input_roles
        )
    )
    if (
        not local_status
        or not isinstance(receipt, Mapping)
        or receipt.get("contract_version") != FUTURES_D0_RECEIPT_VERSION
        or receipt.get("status") != "pass"
        or not _sha256(receipt.get("receipt_hash"))
        or readiness not in {"local_only", "local_consistent", "d0_ready"}
        or (
            require_trading_rules
            and (
                not trading_mode
                or readiness != "d0_ready"
                or not trading_rules_ready
                or receipt.get("trading_ready") is not True
                or payload.get("trading_ready") is not True
            )
        )
        or (
            expected_rule_cutoff_policy is not None
            and (
                not isinstance(profile, Mapping)
                or profile.get("rule_cutoff_policy") != expected_rule_cutoff_policy
            )
        )
    ):
        raise QualityGateError("期货 D0 receipt 不满足当前数据及交易规则要求")
    identity = dict(receipt)
    observed_hash = identity.pop("receipt_hash")
    report_body = dict(payload)
    report_body.pop("audit_hash", None)
    report_body.pop("d0_receipt", None)
    if (
        typed_canonical_hash(identity) != observed_hash
        or receipt.get("audit_hash") != typed_canonical_hash(report_body)
        or receipt.get("profile_hash") != payload.get("profile_hash")
        or receipt.get("snapshot_hashes") != payload.get("snapshot_hashes")
        or not isinstance(payload.get("data_reconciliation"), Mapping)
        or not isinstance(payload["data_reconciliation"].get("sample_count"), int)
        or payload["data_reconciliation"]["sample_count"] < 0
        or receipt.get("evidence_level") != payload.get("evidence_level")
        or receipt.get("claim_ceiling") != payload.get("claim_ceiling")
        or receipt.get("official_snapshot_hash")
        != payload["data_reconciliation"].get("official_snapshot_hash")
        or not isinstance(payload.get("snapshot_hashes"), Mapping)
        or (
            payload["data_reconciliation"].get("status") == "pass"
            and (
                not _sha256(payload["snapshot_hashes"].get("official"))
                or not _sha256(receipt.get("official_snapshot_hash"))
            )
        )
    ):
        raise QualityGateError("期货 D0 receipt hash 漂移")


def _rows(batches: Iterable[object], required_fields: Iterable[str] = ()):
    required = set(required_fields)
    for batch in batches:
        names = tuple(getattr(getattr(batch, "schema", None), "names", ()))
        if required - set(names):
            raise QualityGateError(
                f"期货 D0 输入缺少字段: {sorted(required - set(names))}"
            )
        to_pylist = getattr(batch, "to_pylist", None)
        if not callable(to_pylist):
            raise QualityGateError("期货 D0 输入必须是 Arrow RecordBatch")
        for row in to_pylist():
            if not isinstance(row, Mapping):
                raise QualityGateError("期货 D0 行必须是 mapping")
            yield row


def _materialize(
    dataset_id: str,
    batches: Iterable[object],
    primary_key: tuple[str, ...],
    requested: set[tuple[str, str, str]],
    local_values: dict[tuple[str, str, str], str],
) -> list[Mapping[str, object]]:
    rows = list(_rows(batches, primary_key))
    for row in rows:
        _capture_requested_values(
            dataset_id,
            row,
            primary_key,
            requested,
            local_values,
        )
    return rows


def _capture_requested_values(
    dataset_id: str,
    row: Mapping[str, object],
    primary_key: tuple[str, ...],
    requested: set[tuple[str, str, str]],
    local_values: dict[tuple[str, str, str], str],
) -> None:
    if any(field not in row for field in primary_key):
        raise QualityGateError(f"{dataset_id} 缺少主键字段")
    key_json = canonical_json({field: _normal(row[field]) for field in primary_key})
    for field, value in row.items():
        identity = (dataset_id, key_json, str(field))
        if identity in requested:
            # 重复键由数据质量规则单独判失败；这里保留首值，让审计仍能完整出报告。
            local_values.setdefault(identity, _value_text(value))


def _group_rows(
    rows: Iterable[Mapping[str, object]],
    *fields: str,
    date_fields: set[str] | None = None,
    uppercase_fields: set[str] | None = None,
    accepted_values: Mapping[str, set[str]] | None = None,
) -> dict[tuple[object, ...], tuple[Mapping[str, object], ...]]:
    """按实际消费键一次建索引，避免每个交易日重复全表扫描。"""

    date_fields = date_fields or set()
    uppercase_fields = uppercase_fields or set()
    accepted_values = accepted_values or {}
    grouped: dict[tuple[object, ...], list[Mapping[str, object]]] = {}
    for row in rows:
        if any(
            row.get(field) not in allowed for field, allowed in accepted_values.items()
        ):
            continue
        key = tuple(
            _date(row.get(field), field)
            if field in date_fields
            else (
                str(row.get(field, "")).upper()
                if field in uppercase_fields
                else row.get(field)
            )
            for field in fields
        )
        grouped.setdefault(key, []).append(row)
    return {key: tuple(values) for key, values in grouped.items()}


def _read_official_rows(
    batches: Iterable[object],
    *,
    profile: FuturesD0Profile,
    issues: _Issues,
) -> list[dict[str, str]]:
    required = set(_OFFICIAL_FIELDS.values())
    rows = []
    try:
        for raw in _rows(batches, required):
            if len(rows) >= profile.max_reconciliation_samples:
                raise QualityGateError("官方对账样本超过 package 预算")
            try:
                key_payload = json.loads(
                    _text(raw[_OFFICIAL_FIELDS["key_json"]], "official.key_json")
                )
                if not isinstance(key_payload, Mapping) or not key_payload:
                    raise ValueError
                key_json = canonical_json(dict(key_payload))
                available = _aware_datetime(
                    raw[_OFFICIAL_FIELDS["available_at"]], "official.available_at"
                )
                source_hash = _text(
                    raw[_OFFICIAL_FIELDS["source_hash"]], "official.source_hash"
                )
                if available > profile.as_of or not _sha256(source_hash):
                    raise ValueError
            except (QualityGateError, ValueError, json.JSONDecodeError):
                issues.add(
                    "futures_d0.official_sample_invalid",
                    dataset="official",
                    key=str(raw.get(_OFFICIAL_FIELDS["key_json"], "")),
                    detail="官方样本结构、可见时间或来源 hash 无效",
                    severity="warning",
                )
                continue
            rows.append(
                {
                    "dataset_id": _text(
                        raw[_OFFICIAL_FIELDS["dataset_id"]], "official.dataset_id"
                    ),
                    "key_json": key_json,
                    "field_id": _text(
                        raw[_OFFICIAL_FIELDS["field_id"]], "official.field_id"
                    ),
                    "expected_value": _text(
                        raw[_OFFICIAL_FIELDS["expected_value"]],
                        "official.expected_value",
                    ),
                    "available_at": available.isoformat(),
                    "source_hash": source_hash,
                }
            )
    except QualityGateError as exc:
        # 官方输入本身是可选增强；格式不完整时记录为 warning，不能
        # 把本地字段、规则和 PIT 审计一起判失败。
        issues.add(
            "futures_d0.official_sample_invalid",
            dataset="official",
            key="*",
            detail=str(exc),
            severity="warning",
        )
    return rows


def _reconcile(
    rows: list[dict[str, str]],
    *,
    local_values: Mapping[tuple[str, str, str], str],
    profile: FuturesD0Profile,
    issues: _Issues,
) -> dict[str, object]:
    if not rows:
        return {
            "status": "NOT_RUN",
            "sample_count": 0,
            "matched_count": 0,
            "mismatch_count": 0,
            "mismatch_samples": [],
            "official_snapshot_hash": None,
            "reason": "external_authoritative_snapshot_not_provided",
        }
    mismatches = []
    mismatch_count = 0
    for row in rows:
        identity = (row["dataset_id"], row["key_json"], row["field_id"])
        observed = local_values.get(identity)
        if observed != row["expected_value"]:
            mismatch_count += 1
            mismatch = {
                "dataset_id": row["dataset_id"],
                "key_json": row["key_json"],
                "field_id": row["field_id"],
                "expected_value": row["expected_value"],
                "observed_value": observed,
            }
            if len(mismatches) < profile.max_issue_samples:
                mismatches.append(mismatch)
            issues.add(
                "futures_d0.reconciliation_mismatch",
                dataset=row["dataset_id"],
                key=row["key_json"],
                detail=row["field_id"],
                severity="warning",
            )
    return {
        "status": "pass" if mismatch_count == 0 else "fail",
        "sample_count": len(rows),
        "matched_count": len(rows) - mismatch_count,
        "mismatch_count": mismatch_count,
        "mismatch_samples": mismatches,
        "official_snapshot_hash": typed_canonical_hash(rows),
        "reason": None,
    }


def _visible_daily_rules(
    rows: Iterable[Mapping[str, object]],
    *,
    code: str,
    trading_date: date,
    day_field: str,
    code_field: str,
    added_field: str,
    revised_field: str,
    cutoff: datetime,
) -> list[Mapping[str, object]]:
    return [
        row
        for row in rows
        if row.get(code_field) == code
        and _date(row.get(day_field), day_field) == trading_date
        and _datetime(row.get(added_field), added_field) <= cutoff
        and _datetime(row.get(revised_field), revised_field) <= cutoff
    ]


def _rule_cutoff(trading_date: date, policy: str) -> datetime:
    if policy == "day_session_open_0900":
        return datetime.combine(trading_date, time(9, 0))
    raise QualityGateError("期货 D0 不支持该规则可见性截止策略")


def _count_unique(
    rows: list[Mapping[str, object]],
    *,
    name: str,
    code: str,
    key: str,
    counts: dict[str, int],
    issues: _Issues,
    required: bool = True,
) -> None:
    if len(rows) == 1:
        counts[name] += 1
    else:
        issues.add(
            code,
            dataset=name,
            key=key,
            detail=f"visible_matches={len(rows)}",
            severity="fail" if required else "warning",
        )


def _instrument_identity(code: str) -> tuple[str, str]:
    base, separator, exchange = code.partition(".")
    product = "".join(character for character in base if character.isalpha()).upper()
    if not separator or not product or not exchange:
        raise QualityGateError("实际期货合约无法解析品种或交易所")
    return product, exchange.upper()


def _date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise QualityGateError(f"{field} 不是有效日期") from exc


def _datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=None)
    except ValueError as exc:
        raise QualityGateError(f"{field} 不是有效时间") from exc


def _aware_datetime(value: object, field: str) -> datetime:
    parsed = (
        value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QualityGateError(f"{field} 必须带时区")
    return parsed.astimezone(_ZONE)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise QualityGateError(f"{field} 必须是非空字符串")
    return value


def _positive(value: object, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise QualityGateError(f"{field} 不是数值") from exc
    if not math.isfinite(number) or number <= 0.0:
        raise QualityGateError(f"{field} 必须为正有限数")
    return number


def _nonnegative(value: object, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise QualityGateError(f"{field} 不是数值") from exc
    if not math.isfinite(number) or number < 0.0:
        raise QualityGateError(f"{field} 必须为非负有限数")
    return number


def _finite_positive(value: object) -> bool:
    try:
        return math.isfinite(float(value)) and float(value) > 0.0
    except (TypeError, ValueError):
        return False


def _sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _normal(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _value_text(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise QualityGateError("官方对账不接受非有限数")
        try:
            return format(Decimal(str(value)).normalize(), "f")
        except InvalidOperation as exc:
            raise QualityGateError("官方对账数值无效") from exc
    return str(value)


__all__ = [
    "FUTURES_D0_PROFILE_VERSION",
    "FUTURES_D0_RECEIPT_VERSION",
    "FUTURES_D0_REPORT_VERSION",
    "FuturesD0Profile",
    "FuturesD0Report",
    "audit_futures_daily_data",
    "require_futures_d0_receipt",
]
