"""研究有效性门禁与结论上限政策。"""

from __future__ import annotations

from dataclasses import dataclass
from research_pipeline.platform.canonical import typed_canonical_hash

from .claims import ClaimFacet
from .errors import EvidenceContractError
from .facets import ArtifactIntegrityFacet, CLAIM_LEVELS, ResearchValidityFacet, ReproducibilityFacet, VerificationIssue, require_sha256, weakest_claim_level


VALIDITY_CONTRACT_VERSION = "research-evidence-validity-v1"
VALIDITY_GATE_IDS = (
    "data.pit",
    "label.split",
    "search.holdout",
    "statistics",
    "financial.tradability",
)
GATE_STATUSES = {"pass", "fail", "not_applicable"}
FINDING_SEVERITIES = {"warning", "error"}
VALIDITY_REASON_MESSAGES = {
    "pit.future_data": "发现当时尚不可见的数据",
    "pit.source_revision_missing": "缺少数据源修订身份",
    "pit.availability_policy_missing": "缺少可见性政策",
    "pit.minute_quality_failed": "分钟数据质量门禁未通过",
    "pit.minute_recovery_mismatch": "分钟恢复与基准运行摘要不一致",
    "label.leakage": "标签泄漏到特征或选择阶段",
    "split.overlap": "训练与评价标签区间重叠",
    "split.embargo_missing": "需要但未执行 embargo",
    "split.embargo_below_floor": "分钟 embargo 低于标签跨度推导下限",
    "split.minute_recompute_mismatch": "分钟时序拆分与独立重算不一致",
    "search.ledger_incomplete": "搜索账本不完整",
    "holdout.access_invalid": "holdout 访问不符合预声明",
    "holdout.reused": "holdout 被重复访问",
    "search.minute_trial_universe_incomplete": "分钟 trial universe 遗漏或身份不一致",
    "statistics.small_sample": "有效样本不足",
    "statistics.small_cluster": "有效簇数量不足",
    "statistics.degenerate_matrix": "统计矩阵退化",
    "statistics.method_not_applicable": "统计方法不适用",
    "multiple_testing.missing": "缺少多重检验控制",
    "statistics.hac_below_floor": "分钟 HAC lag 低于重叠标签推导下限",
    "statistics.minute_sample_invalid": "分钟检验样本不足或统计退化",
    "statistics.minute_recompute_mismatch": "分钟统计摘要与独立重算不一致",
    "financial.rule_missing": "缺少历史金融规则快照",
    "financial.order_audit_missing": "缺少订单与成交审计",
    "financial.ledger_missing": "缺少资金持仓账本审计",
    "tradability.missing": "缺少可交易性证据",
    "financial.time_semantics_invalid": "仿真时间或可见性合同无效",
    "financial.capacity_semantics_invalid": "仿真容量与结论上限合同无效",
    "financial.minute_simulation_unsupported": "分钟仿真存在不受支持的金融规则路径",
    "financial.bar_tca_invalid": "Bar TCA 身份、结论上限或费用恒等无效",
}
VALIDITY_GATE_REASON_CODES = {
    "data.pit": frozenset({"pit.future_data", "pit.source_revision_missing", "pit.availability_policy_missing", "pit.minute_quality_failed", "pit.minute_recovery_mismatch"}),
    "label.split": frozenset({"label.leakage", "split.overlap", "split.embargo_missing", "split.embargo_below_floor", "split.minute_recompute_mismatch"}),
    "search.holdout": frozenset({"search.ledger_incomplete", "holdout.access_invalid", "holdout.reused", "search.minute_trial_universe_incomplete"}),
    "statistics": frozenset({"statistics.small_sample", "statistics.small_cluster", "statistics.degenerate_matrix", "statistics.method_not_applicable", "multiple_testing.missing", "statistics.hac_below_floor", "statistics.minute_sample_invalid", "statistics.minute_recompute_mismatch"}),
    "financial.tradability": frozenset({"financial.rule_missing", "financial.order_audit_missing", "financial.ledger_missing", "tradability.missing", "financial.time_semantics_invalid", "financial.capacity_semantics_invalid", "financial.minute_simulation_unsupported", "financial.bar_tca_invalid"}),
}


@dataclass(frozen=True)
class ValidityFinding:
    code: str
    severity: str
    message: str

    def __post_init__(self) -> None:
        if self.code not in VALIDITY_REASON_MESSAGES:
            raise EvidenceContractError("validity finding code 不受支持")
        if self.severity not in FINDING_SEVERITIES:
            raise EvidenceContractError("validity finding severity 不受支持")
        if self.message != VALIDITY_REASON_MESSAGES[self.code]:
            raise EvidenceContractError("validity finding message 必须使用注册表文本")

    @classmethod
    def build(cls, code: str, severity: str = "error") -> "ValidityFinding":
        if code not in VALIDITY_REASON_MESSAGES:
            raise EvidenceContractError("validity finding code 不受支持")
        return cls(code, severity, VALIDITY_REASON_MESSAGES[code])

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "severity": self.severity, "message": self.message}


@dataclass(frozen=True)
class ValidityGateResult:
    gate_id: str
    status: str
    input_hashes: tuple[str, ...]
    findings: tuple[ValidityFinding, ...]
    result_hash: str
    contract_version: str = VALIDITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.gate_id not in VALIDITY_GATE_IDS:
            raise EvidenceContractError("validity gate_id 不受支持")
        if self.status not in GATE_STATUSES:
            raise EvidenceContractError("validity gate status 不受支持")
        if not self.input_hashes or tuple(sorted(self.input_hashes)) != self.input_hashes or len(set(self.input_hashes)) != len(self.input_hashes):
            raise EvidenceContractError("gate input_hashes 必须非空、不重复并规范排序")
        for value in self.input_hashes:
            require_sha256(value, "gate input_hash")
        if tuple(sorted(self.findings, key=lambda item: (item.severity, item.code))) != self.findings or len({item.code for item in self.findings}) != len(self.findings):
            raise EvidenceContractError("gate findings 必须唯一并规范排序")
        if any(item.code not in VALIDITY_GATE_REASON_CODES[self.gate_id] for item in self.findings):
            raise EvidenceContractError("finding code 不属于该 validity gate")
        has_error = any(item.severity == "error" for item in self.findings)
        if (self.status == "fail") != has_error or (self.status == "not_applicable" and self.findings):
            raise EvidenceContractError("gate status 与 findings 不一致")
        if self.contract_version != VALIDITY_CONTRACT_VERSION or self.result_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("gate result hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"gate_id": self.gate_id, "status": self.status, "input_hashes": list(self.input_hashes), "findings": [item.to_dict() for item in self.findings], "contract_version": self.contract_version}

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "result_hash": self.result_hash}

    @classmethod
    def build(cls, *, gate_id: str, input_hashes: tuple[str, ...], issue_codes: tuple[str, ...] = (), warning_codes: tuple[str, ...] = (), not_applicable: bool = False) -> "ValidityGateResult":
        findings = tuple(sorted(
            (ValidityFinding.build(code, "error") for code in issue_codes),
            key=lambda item: (item.severity, item.code),
        )) + tuple(sorted(
            (ValidityFinding.build(code, "warning") for code in warning_codes),
            key=lambda item: (item.severity, item.code),
        ))
        findings = tuple(sorted(findings, key=lambda item: (item.severity, item.code)))
        if not_applicable and findings:
            raise EvidenceContractError("not_applicable gate 不能有 finding")
        status = "not_applicable" if not_applicable else ("fail" if issue_codes else "pass")
        normalized_hashes = tuple(sorted(input_hashes))
        payload = {"gate_id": gate_id, "status": status, "input_hashes": list(normalized_hashes), "findings": [item.to_dict() for item in findings], "contract_version": VALIDITY_CONTRACT_VERSION}
        return cls(gate_id, status, normalized_hashes, findings, typed_canonical_hash(payload))


@dataclass(frozen=True)
class ClaimPolicy:
    requested_level: str
    required_gates: tuple[str, ...]
    allowed_not_applicable: tuple[str, ...]
    allowed_warning_codes: tuple[str, ...]
    policy_hash: str
    contract_version: str = VALIDITY_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.requested_level not in CLAIM_LEVELS:
            raise EvidenceContractError("claim policy 的等级不受支持")
        if not self.required_gates or tuple(sorted(self.required_gates)) != self.required_gates or len(set(self.required_gates)) != len(self.required_gates):
            raise EvidenceContractError("required_gates 必须非空、唯一并规范排序")
        if any(item not in VALIDITY_GATE_IDS for item in self.required_gates):
            raise EvidenceContractError("required_gates 含未知 gate")
        if tuple(sorted(self.allowed_not_applicable)) != self.allowed_not_applicable or len(set(self.allowed_not_applicable)) != len(self.allowed_not_applicable) or any(item not in self.required_gates for item in self.allowed_not_applicable):
            raise EvidenceContractError("allowed_not_applicable 必须是 required gate 子集")
        if tuple(sorted(self.allowed_warning_codes)) != self.allowed_warning_codes or len(set(self.allowed_warning_codes)) != len(self.allowed_warning_codes) or any(item not in VALIDITY_REASON_MESSAGES for item in self.allowed_warning_codes):
            raise EvidenceContractError("allowed_warning_codes 不受支持或未排序")
        if self.contract_version != VALIDITY_CONTRACT_VERSION or self.policy_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("claim policy hash 或版本不一致")

    def payload(self) -> dict[str, object]:
        return {"requested_level": self.requested_level, "required_gates": list(self.required_gates), "allowed_not_applicable": list(self.allowed_not_applicable), "allowed_warning_codes": list(self.allowed_warning_codes), "contract_version": self.contract_version}

    @classmethod
    def build(cls, *, requested_level: str, required_gates: tuple[str, ...] = VALIDITY_GATE_IDS, allowed_not_applicable: tuple[str, ...] = (), allowed_warning_codes: tuple[str, ...] = ()) -> "ClaimPolicy":
        values = (requested_level, tuple(sorted(required_gates)), tuple(sorted(allowed_not_applicable)), tuple(sorted(allowed_warning_codes)))
        payload = {"requested_level": values[0], "required_gates": list(values[1]), "allowed_not_applicable": list(values[2]), "allowed_warning_codes": list(values[3]), "contract_version": VALIDITY_CONTRACT_VERSION}
        return cls(*values, typed_canonical_hash(payload))


@dataclass(frozen=True)
class ClaimAssessment:
    policy_hash: str
    validity: ResearchValidityFacet
    claim: ClaimFacet
    verification_eligible: bool
    limitations: tuple[str, ...]
    assessment_hash: str

    def __post_init__(self) -> None:
        if type(self.verification_eligible) is not bool or tuple(sorted(set(self.limitations))) != self.limitations:
            raise EvidenceContractError("claim assessment 字段未规范化")
        require_sha256(self.policy_hash, "policy_hash")
        require_sha256(self.assessment_hash, "assessment_hash")
        if self.assessment_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("claim assessment hash 不一致")

    def payload(self) -> dict[str, object]:
        return {"policy_hash": self.policy_hash, "validity_hash": self.validity.facet_hash, "claim_hash": self.claim.claim_hash, "verification_eligible": self.verification_eligible, "limitations": list(self.limitations), "contract_version": VALIDITY_CONTRACT_VERSION}


def assess_claim_policy(
    *,
    policy: ClaimPolicy,
    gate_results: tuple[ValidityGateResult, ...],
    integrity: ArtifactIntegrityFacet,
    reproducibility: ReproducibilityFacet,
    external_claim_ceiling: str | None = None,
) -> ClaimAssessment:
    by_id = {item.gate_id: item for item in gate_results}
    if len(by_id) != len(gate_results) or set(by_id) != set(policy.required_gates):
        raise EvidenceContractError("gate result 缺失、重复或包含未知 gate")
    issues: list[VerificationIssue] = []
    limitations: list[str] = []
    for gate_id in policy.required_gates:
        result = by_id[gate_id]
        if result.status == "not_applicable" and gate_id not in policy.allowed_not_applicable:
            issues.append(VerificationIssue("validity", "gate.not_applicable_forbidden", "该门禁不允许跳过", "validity_gate", gate_id))
        for finding in result.findings:
            if finding.severity == "warning" and finding.code in policy.allowed_warning_codes:
                limitations.append(finding.code)
            else:
                issues.append(VerificationIssue("validity", finding.code, finding.message, "validity_gate", gate_id))
    if external_claim_ceiling is not None:
        if external_claim_ceiling not in CLAIM_LEVELS:
            raise EvidenceContractError("external_claim_ceiling 不受支持")
        limitations.append(f"simulation_claim_ceiling:{external_claim_ceiling}")
    normalized_issues = tuple(sorted(issues, key=lambda item: (item.code, item.object_id)))
    normalized_limitations = tuple(sorted(set(limitations)))
    ceiling = "research_observation" if normalized_issues else policy.requested_level
    if external_claim_ceiling is not None:
        ceiling = weakest_claim_level(ceiling, external_claim_ceiling)
    validity = ResearchValidityFacet.build(tuple(item.result_hash for item in gate_results), ceiling, normalized_issues)
    claim = ClaimFacet.build(requested_level=policy.requested_level, integrity=integrity, reproducibility=reproducibility, validity=validity, limitations=normalized_limitations)
    base_eligible = integrity.status == "pass" and reproducibility.status == "pass"
    verification_eligible = base_eligible and not normalized_issues and claim.claim_level == policy.requested_level
    assessment_hash = typed_canonical_hash({"policy_hash": policy.policy_hash, "validity_hash": validity.facet_hash, "claim_hash": claim.claim_hash, "verification_eligible": verification_eligible, "limitations": list(normalized_limitations), "contract_version": VALIDITY_CONTRACT_VERSION})
    return ClaimAssessment(policy.policy_hash, validity, claim, verification_eligible, normalized_limitations, assessment_hash)


__all__ = ["ClaimAssessment", "ClaimPolicy", "GATE_STATUSES", "VALIDITY_CONTRACT_VERSION", "VALIDITY_GATE_IDS", "VALIDITY_GATE_REASON_CODES", "VALIDITY_REASON_MESSAGES", "ValidityFinding", "ValidityGateResult", "assess_claim_policy"]
