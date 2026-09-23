"""搜索开始前冻结完整候选宇宙和资源预算。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from research_pipeline.platform.canonical import typed_canonical_hash

from .splits import ValidationError


@dataclass(frozen=True)
class SearchCandidate:
    candidate_id: str
    parameters: tuple[tuple[str, object], ...]
    parameter_hash: str


@dataclass(frozen=True)
class SearchManifest:
    search_id: str
    candidates: tuple[SearchCandidate, ...]
    method: str
    max_trials: int
    max_parallel: int
    stopping_condition: str
    objective: str
    direction: str
    frozen_at: datetime
    stage: str
    manifest_hash: str


def build_search_manifest(
    *,
    search_id: str,
    candidates: object,
    method: str,
    max_trials: int,
    max_parallel: int,
    stopping_condition: str,
    objective: str,
    direction: str,
    frozen_at: datetime,
    stage: str = "pre_test",
) -> SearchManifest:
    if not search_id.strip() or not objective.strip() or not stopping_condition.strip():
        raise ValidationError("search_id/objective/stopping_condition 不能为空")
    if method not in {"grid", "declared_random", "bayesian_declared"}:
        raise ValidationError("搜索 method 不受支持")
    if direction not in {"maximize", "minimize"}:
        raise ValidationError("搜索 direction 不受支持")
    if stage != "pre_test":
        raise ValidationError("搜索清单必须在读取 test/holdout 前冻结")
    if frozen_at.tzinfo is None or frozen_at.utcoffset() is None:
        raise ValidationError("frozen_at 必须包含时区")
    if isinstance(max_trials, bool) or max_trials < 1 or isinstance(max_parallel, bool) or max_parallel != 1:
        raise ValidationError("搜索预算要求 max_trials>=1 且 max_parallel=1")
    if not isinstance(candidates, (list, tuple)) or not candidates:
        raise ValidationError("候选参数空间不能为空")
    normalized: list[SearchCandidate] = []
    for raw in candidates:
        if not isinstance(raw, dict) or not raw:
            raise ValidationError("每个候选必须是非空参数对象")
        parameter_hash = typed_canonical_hash(raw)
        normalized.append(SearchCandidate(f"candidate_{parameter_hash[:16]}", tuple(sorted(raw.items())), parameter_hash))
    normalized.sort(key=lambda item: item.parameter_hash)
    if len({item.parameter_hash for item in normalized}) != len(normalized):
        raise ValidationError("候选参数空间不能包含重复试验")
    if len(normalized) > max_trials:
        raise ValidationError("声明候选数超过 max_trials")
    payload = {"search_id": search_id, "candidates": [{"candidate_id": item.candidate_id, "parameter_hash": item.parameter_hash, "parameters": dict(item.parameters)} for item in normalized], "method": method, "max_trials": max_trials, "max_parallel": max_parallel, "stopping_condition": stopping_condition, "objective": objective, "direction": direction, "frozen_at": frozen_at.isoformat(), "stage": stage}
    return SearchManifest(search_id, tuple(normalized), method, max_trials, max_parallel, stopping_condition, objective, direction, frozen_at, stage, typed_canonical_hash(payload))


__all__ = ["SearchCandidate", "SearchManifest", "build_search_manifest"]
