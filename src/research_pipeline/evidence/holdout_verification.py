"""从 Result 内 holdout 状态事实独立复核冻结、打开和终态。"""

from __future__ import annotations

from datetime import datetime
from collections.abc import Sequence
from typing import Mapping

from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.platform import typed_canonical_hash


def verify_holdout_access_payload(
    payload: Mapping[str, object],
    *,
    expected_result_hash: str,
    expected_sample_count: int,
    expected_candidate_ids: Sequence[str] | None = None,
    expected_plan_hash: str | None = None,
    expected_opened_hash: str | None = None,
    expected_terminal_hash: str | None = None,
) -> dict[str, object]:
    """拒绝旧 v1，只接受由 Result 直接携带的当前四态/退役事实。"""

    required = {"authorized", "plan", "prepared", "opened", "terminal", "retired"}
    if set(payload) != required or payload.get("authorized") is not True:
        raise EvidenceContractError("holdout access schema 无效")
    plan = _mapping(payload["plan"], "holdout plan")
    prepared = _mapping(payload["prepared"], "holdout prepared")
    opened = _mapping(payload["opened"], "holdout opened")
    terminal = _mapping(payload["terminal"], "holdout terminal")
    retired_raw = payload["retired"]

    if set(plan) != {
        "contract_version", "state", "holdout_identity_hash", "freeze_payload",
        "actor", "reason", "unlock_at", "plan_hash", "token_hash",
    } or plan.get("contract_version") != "research-persistent-holdout-plan-v2":
        raise EvidenceContractError("holdout plan schema 无效或旧 v1 已拒绝")
    freeze = _mapping(plan["freeze_payload"], "holdout freeze payload")
    split = _mapping(freeze.get("holdout_split"), "holdout split")
    sample_ids = split.get("sample_ids")
    candidates = freeze.get("candidates")
    mode = freeze.get("mode")
    if (
        plan.get("state") != "frozen"
        or not isinstance(sample_ids, list)
        or not sample_ids
        or sample_ids != sorted(set(map(str, sample_ids)))
        or len(sample_ids) != expected_sample_count
        or not isinstance(candidates, list)
        or not candidates
        or candidates != sorted(set(map(str, candidates)))
        or mode not in {"single_candidate_confirmation", "family_wide_confirmation"}
    ):
        raise EvidenceContractError("holdout freeze payload 无效")
    if mode == "single_candidate_confirmation" and len(candidates) != 1:
        raise EvidenceContractError("single holdout 不是唯一候选")
    if mode == "family_wide_confirmation":
        correction = freeze.get("multiple_testing")
        if (
            len(candidates) < 2
            or not isinstance(correction, Mapping)
            or correction.get("family_size") != len(candidates)
            or not str(correction.get("method", "")).strip()
        ):
            raise EvidenceContractError("family-wide 候选全集或校正规则无效")
    if expected_candidate_ids is not None:
        planned_candidates = [str(item) for item in expected_candidate_ids]
        if (
            planned_candidates != sorted(set(planned_candidates))
            or candidates != planned_candidates
        ):
            raise EvidenceContractError("holdout 冻结候选集合与正式 planned candidates 不一致")
    unlock_at = _aware(plan.get("unlock_at"), "holdout unlock_at")
    expected_identity = typed_canonical_hash(
        {
            "parent_research_purpose": freeze.get("parent_research_purpose"),
            "data_snapshot": freeze.get("data_snapshot"),
            "holdout_start": split.get("start"),
            "holdout_end": split.get("end"),
            "sample_ids": sample_ids,
        }
    )
    expected_plan = typed_canonical_hash(
        {
            "freeze_payload": freeze,
            "actor": plan.get("actor"),
            "reason": plan.get("reason"),
            "unlock_at": plan.get("unlock_at"),
            "holdout_identity_hash": expected_identity,
        }
    )
    expected_token = typed_canonical_hash(
        {"domain": "locked-holdout-token-v2", "plan_hash": expected_plan}
    )
    if (
        plan.get("holdout_identity_hash") != expected_identity
        or plan.get("plan_hash") != expected_plan
        or plan.get("token_hash") != expected_token
        or (expected_plan_hash is not None and expected_plan_hash != expected_plan)
    ):
        raise EvidenceContractError("holdout plan 身份无效")

    _verify_hashed(prepared, "research-persistent-holdout-prepared-v2", "prepared_hash")
    if (
        prepared.get("state") != "prepared"
        or prepared.get("plan_hash") != expected_plan
        or not isinstance(prepared.get("preflight"), Mapping)
        or not prepared["preflight"]
    ):
        raise EvidenceContractError("holdout prepared 状态无效")
    if mode == "family_wide_confirmation" and (
        prepared["preflight"].get("candidate_ids") != candidates
        or prepared["preflight"].get("family_size") != len(candidates)
    ):
        raise EvidenceContractError("family-wide prepared 未闭合正式候选全集")
    _aware(prepared.get("prepared_at"), "holdout prepared_at")

    _verify_hashed(opened, "research-persistent-holdout-opened-v2", "opened_hash")
    opened_at = _aware(opened.get("opened_at"), "holdout opened_at")
    if (
        opened.get("state") != "opened"
        or opened.get("plan_hash") != expected_plan
        or opened.get("prepared_hash") != prepared.get("prepared_hash")
        or opened.get("token_hash") != expected_token
        or opened.get("actor") != plan.get("actor")
        or opened.get("reason") != plan.get("reason")
        or opened.get("sample_ids_hash") != typed_canonical_hash(sample_ids)
        or opened_at < unlock_at
        or (expected_opened_hash is not None and opened.get("opened_hash") != expected_opened_hash)
    ):
        raise EvidenceContractError("holdout opened 状态无效")

    _verify_hashed(terminal, "research-persistent-holdout-terminal-v2", "terminal_hash")
    if (
        terminal.get("opened_hash") != opened.get("opened_hash")
        or terminal.get("status") != "committed"
        or terminal.get("result_hash") != expected_result_hash
        or (expected_terminal_hash is not None and terminal.get("terminal_hash") != expected_terminal_hash)
    ):
        raise EvidenceContractError("holdout terminal 状态无效")

    retired: Mapping[str, object] | None = None
    if retired_raw is not None:
        retired = _mapping(retired_raw, "holdout retired")
        _verify_hashed(retired, "research-persistent-holdout-retired-v2", "retired_hash")
    if mode == "family_wide_confirmation":
        if (
            retired is None
            or retired.get("status") != "retired"
            or retired.get("final_status") != "committed"
            or retired.get("terminal_hash") != terminal.get("terminal_hash")
            or retired.get("holdout_identity_hash") != expected_identity
        ):
            raise EvidenceContractError("family-wide holdout 未正确 retired")
    elif retired is not None:
        raise EvidenceContractError("single holdout 不应声明 family retired")
    return {
        "authorized": True,
        "plan": dict(plan),
        "prepared": dict(prepared),
        "opened": dict(opened),
        "terminal": dict(terminal),
        "retired": None if retired is None else dict(retired),
    }


def _verify_hashed(payload: Mapping[str, object], contract: str, field: str) -> None:
    unsigned = dict(payload)
    digest = unsigned.pop(field, None)
    if payload.get("contract_version") != contract or typed_canonical_hash(unsigned) != digest:
        raise EvidenceContractError(f"holdout {field} 无效")


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EvidenceContractError(f"{label} 必须是对象")
    return value


def _aware(value: object, label: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise EvidenceContractError(f"{label} 无效") from exc
    if result.utcoffset() is None:
        raise EvidenceContractError(f"{label} 缺少时区")
    return result


__all__ = ["verify_holdout_access_payload"]
