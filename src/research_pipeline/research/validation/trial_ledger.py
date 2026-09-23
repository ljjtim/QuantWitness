"""保留 planned/started/completed/failed/pruned 全部试验的事件账本。"""

from __future__ import annotations

from dataclasses import dataclass

from research_pipeline.platform.canonical import typed_canonical_hash

from .search_manifest import SearchManifest
from .splits import ValidationError


TERMINAL_TRIAL_STATES = {"completed", "failed", "pruned"}


@dataclass(frozen=True)
class TrialEvent:
    sequence: int
    candidate_id: str
    action: str
    stage: str
    metric_name: str | None
    metric_value: float | None
    reason_code: str | None
    previous_event_hash: str | None
    event_hash: str


class TrialLedger:
    def __init__(self, manifest: SearchManifest) -> None:
        self.manifest = manifest
        self._states = {item.candidate_id: "planned" for item in manifest.candidates}
        self._events: list[TrialEvent] = []
        for item in manifest.candidates:
            self._append(item.candidate_id, "planned", "declaration", None, None, None)

    @property
    def events(self) -> tuple[TrialEvent, ...]:
        return tuple(self._events)

    @property
    def states(self) -> dict[str, str]:
        return dict(self._states)

    def start(self, candidate_id: str) -> None:
        self._require_state(candidate_id, "planned")
        self._states[candidate_id] = "started"
        self._append(candidate_id, "started", "train", None, None, None)

    def complete(self, candidate_id: str, *, validation_metric: float) -> None:
        self._require_state(candidate_id, "started")
        if not isinstance(validation_metric, (int, float)):
            raise ValidationError("validation metric 必须为有限数")
        value = float(validation_metric)
        if value != value or value in {float("inf"), float("-inf")}:
            raise ValidationError("validation metric 必须为有限数")
        self._states[candidate_id] = "completed"
        self._append(candidate_id, "completed", "validation", self.manifest.objective, value, None)

    def fail(self, candidate_id: str, *, reason_code: str) -> None:
        self._terminal(candidate_id, "failed", reason_code)

    def prune(self, candidate_id: str, *, reason_code: str) -> None:
        self._terminal(candidate_id, "pruned", reason_code)

    def record_final_evaluation(self, candidate_id: str, *, stage: str, metric_value: float) -> None:
        if stage not in {"test", "holdout"}:
            raise ValidationError("最终评价阶段只支持 test/holdout")
        if self._states.get(candidate_id) != "completed":
            raise ValidationError("只有 validation 完成的候选可以做最终评价")
        if any(event.candidate_id == candidate_id and event.stage == stage for event in self._events):
            raise ValidationError(f"{stage} 只能读取一次")
        self._append(candidate_id, "evaluated", stage, self.manifest.objective, float(metric_value), None)

    def select_winner(self) -> str:
        self.require_terminal()
        rows = [event for event in self._events if event.action == "completed" and event.stage == "validation"]
        if not rows:
            raise ValidationError("没有 validation 完成候选")
        reverse = self.manifest.direction == "maximize"
        ordered = sorted(rows, key=lambda item: ((-item.metric_value if reverse else item.metric_value), item.candidate_id))
        return ordered[0].candidate_id

    def require_terminal(self) -> None:
        incomplete = sorted(key for key, value in self._states.items() if value not in TERMINAL_TRIAL_STATES)
        if incomplete:
            raise ValidationError(f"trial ledger 仍有非终态候选: {incomplete}")
        declared = {item.candidate_id for item in self.manifest.candidates}
        if set(self._states) != declared:
            raise ValidationError("trial ledger 与冻结候选全集不一致")

    @property
    def ledger_hash(self) -> str:
        return typed_canonical_hash({"manifest_hash": self.manifest.manifest_hash, "events": [event.__dict__ for event in self._events]})

    def _terminal(self, candidate_id: str, action: str, reason_code: str) -> None:
        if self._states.get(candidate_id) not in {"planned", "started"}:
            raise ValidationError("trial 状态不允许进入失败/剪枝终态")
        if not reason_code.strip():
            raise ValidationError("失败/剪枝必须记录 reason_code")
        self._states[candidate_id] = action
        self._append(candidate_id, action, "validation", None, None, reason_code)

    def _require_state(self, candidate_id: str, expected: str) -> None:
        if candidate_id not in self._states:
            raise ValidationError("候选不在冻结搜索清单中，禁止事后扩参")
        if self._states[candidate_id] != expected:
            raise ValidationError(f"trial 当前状态不是 {expected}")

    def _append(self, candidate_id: str, action: str, stage: str, metric_name: str | None, metric_value: float | None, reason_code: str | None) -> None:
        previous = self._events[-1].event_hash if self._events else None
        payload = {"sequence": len(self._events) + 1, "candidate_id": candidate_id, "action": action, "stage": stage, "metric_name": metric_name, "metric_value": metric_value, "reason_code": reason_code, "previous_event_hash": previous}
        self._events.append(TrialEvent(payload["sequence"], candidate_id, action, stage, metric_name, metric_value, reason_code, previous, typed_canonical_hash(payload)))


__all__ = ["TERMINAL_TRIAL_STATES", "TrialEvent", "TrialLedger"]
