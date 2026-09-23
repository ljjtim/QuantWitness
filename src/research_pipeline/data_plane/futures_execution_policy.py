"""期货执行价格语义和 D0 证据降级合同。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping

from research_pipeline.platform import typed_canonical_hash

from .errors import QualityGateError
from .futures_daily_audit import require_futures_d0_receipt


FUTURES_EXECUTION_EVIDENCE_VERSION = "futures-execution-evidence-v1"
FUTURES_EXECUTION_RECEIPT_VERSION = "futures-execution-receipt-v1"


@dataclass(frozen=True)
class ExecutionPricePolicy:
    """一种执行价格的输入、可见截止和缺失原因。"""

    policy_id: str
    price_field: str
    available_time_field: str
    cutoff_policy: str
    missing_reason_code: str

    def __post_init__(self) -> None:
        if self.policy_id not in {
            "daily_open",
            "auction_open",
            "day_session_continuous_first_bar_open",
            "next_executable_session",
        }:
            raise QualityGateError("执行价格 policy_id 不受支持")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (
                self.price_field,
                self.available_time_field,
                self.cutoff_policy,
                self.missing_reason_code,
            )
        ):
            raise QualityGateError("执行价格策略字段不完整")


def default_execution_price_policies() -> tuple[ExecutionPricePolicy, ...]:
    """返回四种互不混淆的执行价格语义。"""

    return (
        ExecutionPricePolicy(
            "daily_open",
            "daily_open",
            "daily_open_available_at",
            "visible_before_declared_execution",
            "execution_price.daily_open_unavailable",
        ),
        ExecutionPricePolicy(
            "auction_open",
            "auction_open",
            "auction_open_available_at",
            "visible_before_declared_execution",
            "execution_price.auction_open_unavailable",
        ),
        ExecutionPricePolicy(
            "day_session_continuous_first_bar_open",
            "day_session_continuous_first_open",
            "day_session_continuous_first_open_available_at",
            "completed_first_day_session_continuous_bar_0901",
            "execution_price.day_session_continuous_first_open_unavailable",
        ),
        ExecutionPricePolicy(
            "next_executable_session",
            "next_session_open",
            "next_session_open_available_at",
            "visible_before_next_session_execution",
            "execution_price.next_session_open_unavailable",
        ),
    )


def audit_futures_execution_evidence(
    base_d0_report: Mapping[str, object],
    samples: Iterable[Mapping[str, object]],
    *,
    selected_policy_id: str,
    mode: str,
    decision_time: datetime,
    rule_snapshot_available_at: datetime,
    vendor_check: str,
    official_check: str,
    snapshot_hashes: Mapping[str, str],
    open_interest_policy: str = "strict",
) -> dict[str, object]:
    """在现有 D0 报告上附加执行语义，不越级声称证据等级。"""

    if mode not in {"observation", "trading"}:
        raise QualityGateError("执行价格模式只支持 observation 或 trading")
    if open_interest_policy not in {"strict", "diagnostic"}:
        raise QualityGateError("open_interest_policy 只支持 strict 或 diagnostic")
    if mode == "trading" and open_interest_policy != "strict":
        raise QualityGateError("交易模式的 open_interest_policy 必须是 strict")
    base_d0_audit_hash = base_d0_report.get("audit_hash")
    if not _sha256(base_d0_audit_hash):
        raise QualityGateError("本地 D0 audit_hash 无效")
    require_futures_d0_receipt(base_d0_report)
    if vendor_check not in {"pass", "fail", "NOT_RUN"} or official_check not in {
        "pass",
        "fail",
        "NOT_RUN",
    }:
        raise QualityGateError("外部证据状态无效")
    if set(snapshot_hashes) - {"local", "vendor", "official", "execution_rules"}:
        raise QualityGateError("执行价格 snapshot_hashes 含未知角色")
    if "local" not in snapshot_hashes or "execution_rules" not in snapshot_hashes:
        raise QualityGateError("执行价格审计缺少本地或规则快照")
    if any(not _sha256(value) for value in snapshot_hashes.values()):
        raise QualityGateError("执行价格 snapshot hash 无效")
    if vendor_check == "pass" and "vendor" not in snapshot_hashes:
        raise QualityGateError("供应商核对通过但缺少供应商快照")
    if official_check == "pass" and "official" not in snapshot_hashes:
        raise QualityGateError("官方核对通过但缺少官方快照")
    decision = _aware(decision_time, "decision_time")
    rule_available = _aware(rule_snapshot_available_at, "rule_snapshot_available_at")
    policies = {item.policy_id: item for item in default_execution_price_policies()}
    if selected_policy_id not in policies:
        raise QualityGateError("selected execution price policy 不受支持")

    reasons: set[str] = set()
    semantic_differences = []
    reconciliation_diagnostics = []
    policy_results = []
    local_failed = False
    sample_rows: list[tuple[tuple[str, str], Mapping[str, object]]] = []
    seen_sample_keys: set[tuple[str, str]] = set()
    for raw in samples:
        if not isinstance(raw, Mapping):
            raise QualityGateError("执行价格样本必须是 mapping")
        key = (
            _text(raw.get("instrument_id"), "instrument_id"),
            _text(raw.get("trading_day"), "trading_day"),
        )
        if key in seen_sample_keys:
            raise QualityGateError(f"执行价格样本键重复: {key[0]}/{key[1]}")
        seen_sample_keys.add(key)
        sample_rows.append((key, raw))
    if not sample_rows:
        raise QualityGateError("执行价格样本不能为空")
    normalized_samples = tuple(
        raw for _, raw in sorted(sample_rows, key=lambda item: item[0])
    )
    selected_values = []
    for raw in normalized_samples:
        instrument = _text(raw.get("instrument_id"), "instrument_id")
        trading_day = _text(raw.get("trading_day"), "trading_day")
        execution_at = _aware(raw.get("execution_at"), "execution_at")
        if execution_at > decision:
            raise QualityGateError("执行价格样本在 decision_time 尚不可见")

        daily_open = _optional_price(raw.get("daily_open"), "daily_open")
        continuous_open = _optional_price(
            raw.get("day_session_continuous_first_open"),
            "day_session_continuous_first_open",
        )
        if (
            daily_open is not None
            and continuous_open is not None
            and daily_open != continuous_open
        ):
            semantic_differences.append(
                {
                    "instrument_id": instrument,
                    "trading_day": trading_day,
                    "reason_code": "execution_price.daily_open_differs_from_day_session_continuous_open",
                    "daily_open": str(daily_open),
                    "day_session_continuous_first_open": str(continuous_open),
                }
            )

        for left, right, reason in (
            ("daily_volume", "minute_volume", "futures_d0.volume_mismatch"),
            ("daily_money", "minute_money", "futures_d0.money_mismatch"),
        ):
            if _decimal(raw.get(left), left) != _decimal(raw.get(right), right):
                reasons.add(reason)
                local_failed = True
        daily_open_interest = _decimal(
            raw.get("daily_open_interest"),
            "daily_open_interest",
        )
        minute_open_interest = _decimal(
            raw.get("minute_close_open_interest"),
            "minute_close_open_interest",
        )
        if daily_open_interest != minute_open_interest:
            reason = "futures_d0.open_interest_snapshot_semantic_difference"
            reasons.add(reason)
            reconciliation_diagnostics.append(
                {
                    "instrument_id": instrument,
                    "trading_day": trading_day,
                    "reason_code": reason,
                    "daily_open_interest": str(daily_open_interest),
                    "minute_close_open_interest": str(minute_open_interest),
                    "delta": str(minute_open_interest - daily_open_interest),
                    "policy": open_interest_policy,
                }
            )
            if open_interest_policy == "strict":
                local_failed = True

        for policy in policies.values():
            value = _optional_price(raw.get(policy.price_field), policy.price_field)
            available = _optional_aware(
                raw.get(policy.available_time_field),
                policy.available_time_field,
            )
            status = "pass"
            failure = None
            if value is None or available is None or available > execution_at:
                status = "NOT_RUN"
                failure = policy.missing_reason_code
                reasons.add(policy.missing_reason_code)
            if policy.policy_id == selected_policy_id and status == "pass":
                selected_values.append(
                    {
                        "instrument_id": instrument,
                        "trading_day": trading_day,
                        "price": str(value),
                        "available_at": available.isoformat(),
                    }
                )
            policy_results.append(
                {
                    **asdict(policy),
                    "instrument_id": instrument,
                    "trading_day": trading_day,
                    "status": status,
                    "failure_reason_code": failure,
                }
            )

    first_execution_at = min(
        _aware(item.get("execution_at"), "execution_at") for item in normalized_samples
    )
    historical_rule_visibility_proven = rule_available <= first_execution_at
    if not historical_rule_visibility_proven:
        reasons.add("execution_price.rule_snapshot_not_visible")
        if mode == "trading":
            local_failed = True
    if len(selected_values) != len(normalized_samples):
        reasons.add("execution_price.selected_policy_not_executable")
        if mode == "trading":
            local_failed = True
    trading_local_rules_ready = True
    if mode == "trading":
        try:
            require_futures_d0_receipt(base_d0_report, require_trading_rules=True)
        except QualityGateError:
            trading_local_rules_ready = False
            local_failed = True
            reasons.update(_missing_trading_rule_reason_codes(base_d0_report))
    # 供应商/官方抽样属于可选的增强证据。即使抽样未运行或发现差异，
    # 也不能把已经通过本地一致性审计的观察研究判成不可运行；差异仍以
    # reason code 留在收据中，避免把本地结果冒充成外部交叉核对结果。
    if vendor_check == "fail":
        reasons.add("futures_d0.vendor_cross_check_failed")
    elif vendor_check == "NOT_RUN":
        reasons.add("futures_d0.vendor_cross_check_not_run")
    if official_check == "fail":
        reasons.add("futures_d0.official_cross_check_failed")
    elif official_check == "NOT_RUN":
        reasons.add("futures_d0.official_cross_check_not_run")

    if local_failed:
        state = "failed"
        evidence_level = "local_only"
        claim_ceiling = "no_claim"
    else:
        if official_check == "pass":
            evidence_level = "official_cross_checked"
        elif vendor_check == "pass":
            evidence_level = "vendor_cross_checked"
        else:
            evidence_level = "local_only"
        state = "d0_ready" if mode == "trading" else "local_consistent"
        claim_ceiling = (
            "bar_level_historical_research"
            if mode == "trading" and trading_local_rules_ready
            else "research_observation"
        )
        if mode == "observation":
            reasons.add("execution_price.observation_mode_not_trading_ready")

    body = {
        "contract_version": FUTURES_EXECUTION_EVIDENCE_VERSION,
        "mode": mode,
        "selected_policy_id": selected_policy_id,
        "decision_time": decision.isoformat(),
        "rule_snapshot_available_at": rule_available.isoformat(),
        "historical_pit_rule_visibility_proven": historical_rule_visibility_proven,
        "open_interest_policy": open_interest_policy,
        "status": state,
        "evidence_level": evidence_level,
        "claim_ceiling": claim_ceiling,
        "reason_codes": sorted(reasons),
        "external_checks": {
            "vendor": vendor_check,
            "official": official_check,
            "required_for_local_consistency": False,
        },
        "semantic_differences": semantic_differences,
        "reconciliation_diagnostics": reconciliation_diagnostics,
        "policy_results": policy_results,
        "selected_prices": selected_values,
        "input_snapshot_hashes": dict(sorted(snapshot_hashes.items())),
        "base_d0_audit_hash": base_d0_audit_hash,
        "base_d0_receipt_hash": base_d0_report["d0_receipt"]["receipt_hash"],
    }
    receipt = None
    # 本地一致性通过即可生成 D0 receipt。receipt 的 evidence_level 和
    # claim_ceiling 明确限制了结论上限；没有外部样本时不会伪称官方证明。
    if state in {"local_consistent", "d0_ready"}:
        receipt_body = {
            "contract_version": FUTURES_EXECUTION_RECEIPT_VERSION,
            "status": "pass",
            "evidence_hash": typed_canonical_hash(body),
            "selected_policy_id": selected_policy_id,
            "evidence_level": evidence_level,
            "input_snapshot_hashes": dict(sorted(snapshot_hashes.items())),
            "claim_ceiling": claim_ceiling,
            "base_d0_receipt_hash": base_d0_report["d0_receipt"]["receipt_hash"],
        }
        receipt = {
            **receipt_body,
            "receipt_hash": typed_canonical_hash(receipt_body),
        }
    return {**body, "d0_receipt": receipt}


def _aware(value: object, field: str) -> datetime:
    current = datetime.fromisoformat(value) if isinstance(value, str) else value
    if (
        not isinstance(current, datetime)
        or current.tzinfo is None
        or current.utcoffset() is None
    ):
        raise QualityGateError(f"{field} 必须是带时区时点")
    return current


def _optional_aware(value: object, field: str) -> datetime | None:
    return None if value is None else _aware(value, field)


def _decimal(value: object, field: str) -> Decimal:
    current = _optional_decimal(value, field)
    if current is None:
        raise QualityGateError(f"{field} 必须是有限非负数")
    return current


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise QualityGateError(f"{field} 必须是有限非负数")
    try:
        current = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise QualityGateError(f"{field} 必须是有限非负数") from None
    if not current.is_finite() or current < 0:
        raise QualityGateError(f"{field} 必须是有限非负数")
    return current


def _optional_price(value: object, field: str) -> Decimal | None:
    current = _optional_decimal(value, field)
    if current is not None and current <= 0:
        raise QualityGateError(f"{field} 必须是有限正数")
    return current


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QualityGateError(f"{field} 必须是非空字符串")
    return value


def _sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _missing_trading_rule_reason_codes(
    base_d0_report: Mapping[str, object],
) -> set[str]:
    """从 D0 覆盖矩阵生成稳定的本地交易规则缺失原因。"""

    coverage = base_d0_report.get("coverage_matrix")
    by_role = (
        {
            str(item.get("dataset")): item
            for item in coverage
            if isinstance(item, Mapping)
        }
        if isinstance(coverage, list)
        else {}
    )
    snapshots = base_d0_report.get("snapshot_hashes")
    snapshot_map = snapshots if isinstance(snapshots, Mapping) else {}
    reasons = set()
    for role in ("tick", "fee", "margin"):
        item = by_role.get(role)
        if (
            not isinstance(item, Mapping)
            or item.get("status") != "pass"
            or item.get("required_for_mode") is not True
            or not _sha256(snapshot_map.get(role))
        ):
            reasons.add(f"execution_price.{role}_rule_unavailable")
    if base_d0_report.get("readiness_state") != "d0_ready":
        reasons.add("execution_price.local_d0_not_trading_ready")
    return reasons


__all__ = [
    "FUTURES_EXECUTION_EVIDENCE_VERSION",
    "FUTURES_EXECUTION_RECEIPT_VERSION",
    "ExecutionPricePolicy",
    "audit_futures_execution_evidence",
    "default_execution_price_policies",
]
