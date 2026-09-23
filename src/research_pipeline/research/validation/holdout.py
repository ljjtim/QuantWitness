"""冻结 holdout 协议与跨进程一次性打开账本。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
import json
import os
from pathlib import Path
import threading
from typing import Any

from research_pipeline.platform.canonical import canonical_json, typed_canonical_hash

from .splits import ValidationError


SINGLE_CANDIDATE_CONFIRMATION = "single_candidate_confirmation"
FAMILY_WIDE_CONFIRMATION = "family_wide_confirmation"
HOLDOUT_MODES = frozenset(
    {SINGLE_CANDIDATE_CONFIRMATION, FAMILY_WIDE_CONFIRMATION}
)
_FREEZE_PAYLOAD_FIELDS = frozenset(
    {
        "parent_research_purpose",
        "data_snapshot",
        "holdout_split",
        "mode",
        "candidates",
        "selection_rule",
        "validation",
        "primary_estimand",
        "direction",
        "alpha",
        "multiple_testing",
        "random_protocol",
        "failure_policy",
        "exposed_intervals",
        "package_plan_identity",
        "implementation_identity",
        "aliases",
    }
)
_SHA256_CHARS = frozenset("0123456789abcdef")


@dataclass(frozen=True)
class HoldoutAccessPlan:
    freeze_payload: Mapping[str, object]
    actor: str
    reason: str
    unlock_at: datetime
    holdout_identity_hash: str
    plan_hash: str
    token_hash: str

    @classmethod
    def build(
        cls,
        *,
        freeze_payload: Mapping[str, object],
        actor: str,
        reason: str,
        unlock_at: datetime,
    ) -> "HoldoutAccessPlan":
        normalized = _validate_freeze_payload(freeze_payload)
        if not actor.strip() or not reason.strip():
            raise ValidationError("holdout actor/reason 不能为空")
        _require_aware(unlock_at, "unlock_at")
        split = _mapping(normalized["holdout_split"], "holdout_split")
        identity_payload = {
            "parent_research_purpose": normalized["parent_research_purpose"],
            "data_snapshot": normalized["data_snapshot"],
            "holdout_start": split["start"],
            "holdout_end": split["end"],
            "sample_ids": split["sample_ids"],
        }
        holdout_identity_hash = typed_canonical_hash(identity_payload)
        plan_payload = {
            "freeze_payload": normalized,
            "actor": actor,
            "reason": reason,
            "unlock_at": unlock_at.isoformat(),
            "holdout_identity_hash": holdout_identity_hash,
        }
        plan_hash = typed_canonical_hash(plan_payload)
        token_hash = typed_canonical_hash(
            {"domain": "locked-holdout-token-v2", "plan_hash": plan_hash}
        )
        return cls(
            freeze_payload=normalized,
            actor=actor,
            reason=reason,
            unlock_at=unlock_at,
            holdout_identity_hash=holdout_identity_hash,
            plan_hash=plan_hash,
            token_hash=token_hash,
        )

    @property
    def sample_ids(self) -> tuple[str, ...]:
        split = _mapping(self.freeze_payload["holdout_split"], "holdout_split")
        return tuple(str(item) for item in split["sample_ids"])

    @property
    def mode(self) -> str:
        return str(self.freeze_payload["mode"])


@dataclass(frozen=True)
class HoldoutAccessEvent:
    sequence: int
    plan_hash: str
    token_hash: str
    actor: str
    reason: str
    accessed_at: str
    sample_ids_hash: str
    result_hash: str
    previous_event_hash: str | None
    event_hash: str
    committed: bool


class HoldoutAccessLedger:
    """只用于单进程最小研究；跨进程正式链使用 PersistentHoldoutLedger。"""

    def __init__(self, plan: HoldoutAccessPlan) -> None:
        self._plan = plan
        self._events: list[HoldoutAccessEvent] = []
        self._lock = threading.Lock()

    @property
    def events(self) -> tuple[HoldoutAccessEvent, ...]:
        return tuple(self._events)

    def access(
        self,
        *,
        token_hash: str,
        actor: str,
        reason: str,
        fixed_clock: datetime,
        sample_ids: tuple[str, ...],
        result_hash: str,
    ) -> HoldoutAccessEvent:
        with self._lock:
            if self._events:
                raise ValidationError("locked holdout 已访问，禁止第二次读取或重新选参")
            _verify_open_authority(
                self._plan,
                token_hash=token_hash,
                actor=actor,
                reason=reason,
                fixed_clock=fixed_clock,
            )
            if tuple(sorted(sample_ids)) != self._plan.sample_ids:
                raise ValidationError("holdout 访问范围必须与预声明样本完全一致")
            if not result_hash.strip():
                raise ValidationError("holdout result_hash 不能为空")
            sample_hash = typed_canonical_hash(list(self._plan.sample_ids))
            payload = {
                "sequence": 1,
                "plan_hash": self._plan.plan_hash,
                "token_hash": token_hash,
                "actor": actor,
                "reason": reason,
                "accessed_at": fixed_clock.isoformat(),
                "sample_ids_hash": sample_hash,
                "result_hash": result_hash,
                "previous_event_hash": None,
                "committed": True,
            }
            event = HoldoutAccessEvent(
                1,
                self._plan.plan_hash,
                token_hash,
                actor,
                reason,
                fixed_clock.isoformat(),
                sample_hash,
                result_hash,
                None,
                typed_canonical_hash(payload),
                True,
            )
            self._events.append(event)
            return event

    def verify(self) -> None:
        if len(self._events) > 1:
            raise ValidationError("holdout 账本访问次数超过 1")
        for event in self._events:
            payload = {
                "sequence": event.sequence,
                "plan_hash": event.plan_hash,
                "token_hash": event.token_hash,
                "actor": event.actor,
                "reason": event.reason,
                "accessed_at": event.accessed_at,
                "sample_ids_hash": event.sample_ids_hash,
                "result_hash": event.result_hash,
                "previous_event_hash": event.previous_event_hash,
                "committed": event.committed,
            }
            if not event.committed or typed_canonical_hash(payload) != event.event_hash:
                raise ValidationError("holdout 访问账本损坏或未提交")


class PersistentHoldoutLedger:
    """用独占文件创建记录 frozen/prepared/opened/terminal 状态。"""

    def __init__(self, root: str | Path, plan: HoldoutAccessPlan) -> None:
        self.root = Path(root).resolve()
        self.plan = plan

    def initialize(self) -> str:
        self.root.mkdir(parents=True, exist_ok=True)
        payload = {
            "contract_version": "research-persistent-holdout-plan-v2",
            "state": "frozen",
            "holdout_identity_hash": self.plan.holdout_identity_hash,
            "freeze_payload": dict(self.plan.freeze_payload),
            "actor": self.plan.actor,
            "reason": self.plan.reason,
            "unlock_at": self.plan.unlock_at.isoformat(),
            "plan_hash": self.plan.plan_hash,
            "token_hash": self.plan.token_hash,
        }
        path = self.root / "plan.json"
        if path.exists():
            existing = _read_mapping(path, "holdout plan")
            if existing != payload:
                if (
                    existing.get("holdout_identity_hash")
                    == self.plan.holdout_identity_hash
                ):
                    raise ValidationError(
                        "同一 holdout 身份已冻结，修改 run/package/family 名称不能重置"
                    )
                raise ValidationError("holdout 持久计划身份漂移")
        else:
            try:
                _write_exclusive(path, payload)
            except FileExistsError:
                existing = _read_mapping(path, "holdout plan")
                if existing != payload:
                    if (
                        existing.get("holdout_identity_hash")
                        == self.plan.holdout_identity_hash
                    ):
                        raise ValidationError("同一 holdout 身份已被其他计划冻结") from None
                    raise ValidationError("holdout 持久计划身份漂移") from None
        return self.plan.plan_hash

    def prepare(
        self,
        *,
        prepared_at: datetime,
        preflight: Callable[[], Mapping[str, object]],
    ) -> dict[str, object]:
        self.initialize()
        _require_aware(prepared_at, "prepared_at")
        if (self.root / "opened.json").exists() or (self.root / "terminal.json").exists():
            raise ValidationError("holdout 已经 opened，不能重新预检")
        preflight_result = preflight()
        if not isinstance(preflight_result, Mapping) or not preflight_result:
            raise ValidationError("holdout 无值预检必须返回非空格式/schema 事实")
        if self.plan.mode == FAMILY_WIDE_CONFIRMATION:
            frozen_candidates = list(self.plan.freeze_payload["candidates"])
            if (
                preflight_result.get("candidate_ids") != frozen_candidates
                or preflight_result.get("family_size") != len(frozen_candidates)
            ):
                raise ValidationError(
                    "family-wide 无值预检未证明正式候选表覆盖冻结候选全集"
                )
        payload = {
            "contract_version": "research-persistent-holdout-prepared-v2",
            "state": "prepared",
            "plan_hash": self.plan.plan_hash,
            "prepared_at": prepared_at.isoformat(),
            "preflight": _json_mapping(preflight_result, "holdout preflight"),
        }
        payload["prepared_hash"] = typed_canonical_hash(payload)
        path = self.root / "prepared.json"
        try:
            _write_exclusive(path, payload)
        except FileExistsError:
            existing = _read_mapping(path, "holdout prepared")
            if existing != payload:
                raise ValidationError("holdout 已按不同预检事实 prepared") from None
            return existing
        return payload

    def open(
        self,
        *,
        opening_id: str,
        token_hash: str,
        actor: str,
        reason: str,
        fixed_clock: datetime,
        prepared_hash: str,
    ) -> dict[str, object]:
        self.initialize()
        _verify_open_authority(
            self.plan,
            token_hash=token_hash,
            actor=actor,
            reason=reason,
            fixed_clock=fixed_clock,
        )
        if not opening_id.strip():
            raise ValidationError("holdout opening_id 不能为空")
        prepared = self._verified_prepared()
        if prepared.get("prepared_hash") != prepared_hash:
            raise ValidationError("holdout prepared hash 不匹配")
        if (self.root / "terminal.json").exists():
            raise ValidationError("holdout 已经 opened 并进入终态")
        payload = {
            "contract_version": "research-persistent-holdout-opened-v2",
            "state": "opened",
            "opening_id": opening_id,
            "plan_hash": self.plan.plan_hash,
            "prepared_hash": prepared_hash,
            "token_hash": token_hash,
            "actor": actor,
            "reason": reason,
            "opened_at": fixed_clock.isoformat(),
            "sample_ids_hash": typed_canonical_hash(list(self.plan.sample_ids)),
        }
        payload["opened_hash"] = typed_canonical_hash(payload)
        try:
            _write_exclusive(self.root / "opened.json", payload)
        except FileExistsError:
            raise ValidationError(
                "holdout 已经 opened；并发失败者不得读取任何 holdout 值"
            ) from None
        return payload

    def finish(
        self,
        *,
        opened_hash: str,
        status: str,
        result_hash: str,
        reason: str | None = None,
    ) -> dict[str, object]:
        if status not in {"committed", "consumed_failed"}:
            raise ValidationError("holdout terminal 状态无效")
        if not result_hash.strip():
            raise ValidationError("holdout terminal result_hash 不能为空")
        opened = self._verified_opened()
        if opened.get("opened_hash") != opened_hash:
            raise ValidationError("holdout opened hash 不匹配")
        if status == "consumed_failed" and not str(reason or "").strip():
            raise ValidationError("holdout 消费失败必须记录原因")
        payload = {
            "contract_version": "research-persistent-holdout-terminal-v2",
            "opened_hash": opened_hash,
            "status": status,
            "result_hash": result_hash,
            "reason": reason,
        }
        payload["terminal_hash"] = typed_canonical_hash(payload)
        path = self.root / "terminal.json"
        try:
            _write_exclusive(path, payload)
        except FileExistsError:
            existing = _read_mapping(path, "holdout terminal")
            if existing != payload:
                raise ValidationError("holdout 已进入不同终态") from None
            payload = dict(existing)
        if self.plan.mode == FAMILY_WIDE_CONFIRMATION:
            self._retire(payload)
        return payload

    def verify(self) -> dict[str, object]:
        self.initialize()
        prepared_path = self.root / "prepared.json"
        if not prepared_path.is_file():
            return {
                "status": "frozen",
                "plan_hash": self.plan.plan_hash,
                "holdout_identity_hash": self.plan.holdout_identity_hash,
            }
        prepared = self._verified_prepared()
        opened_path = self.root / "opened.json"
        if not opened_path.is_file():
            return {
                "status": "prepared",
                "prepared_hash": prepared["prepared_hash"],
            }
        opened = self._verified_opened()
        terminal_path = self.root / "terminal.json"
        if not terminal_path.is_file():
            return {"status": "opened", "opened_hash": opened["opened_hash"]}
        terminal = self._verified_terminal()
        retired_path = self.root / "retired.json"
        if retired_path.is_file():
            retired = _verified_hashed_payload(
                retired_path,
                label="holdout retired",
                hash_field="retired_hash",
                contract="research-persistent-holdout-retired-v2",
            )
            if (
                retired.get("terminal_hash") != terminal.get("terminal_hash")
                or retired.get("final_status") != terminal.get("status")
                or self.plan.mode != FAMILY_WIDE_CONFIRMATION
            ):
                raise ValidationError("holdout retired 状态损坏")
            return {
                "status": "retired",
                "final_status": terminal["status"],
                "terminal_hash": terminal["terminal_hash"],
                "retired_hash": retired["retired_hash"],
            }
        return dict(terminal)

    def _verified_prepared(self) -> dict[str, object]:
        payload = _verified_hashed_payload(
            self.root / "prepared.json",
            label="holdout prepared",
            hash_field="prepared_hash",
            contract="research-persistent-holdout-prepared-v2",
        )
        if payload.get("state") != "prepared" or payload.get("plan_hash") != self.plan.plan_hash:
            raise ValidationError("holdout prepared 身份损坏")
        _parse_aware(payload.get("prepared_at"), "prepared_at")
        if not isinstance(payload.get("preflight"), Mapping) or not payload["preflight"]:
            raise ValidationError("holdout prepared 缺少无值预检事实")
        return payload

    def _verified_opened(self) -> dict[str, object]:
        payload = _verified_hashed_payload(
            self.root / "opened.json",
            label="holdout opened",
            hash_field="opened_hash",
            contract="research-persistent-holdout-opened-v2",
        )
        prepared = self._verified_prepared()
        if (
            payload.get("state") != "opened"
            or payload.get("plan_hash") != self.plan.plan_hash
            or payload.get("prepared_hash") != prepared.get("prepared_hash")
            or payload.get("token_hash") != self.plan.token_hash
            or payload.get("actor") != self.plan.actor
            or payload.get("reason") != self.plan.reason
            or payload.get("sample_ids_hash")
            != typed_canonical_hash(list(self.plan.sample_ids))
            or not str(payload.get("opening_id", "")).strip()
        ):
            raise ValidationError("holdout opened 身份损坏")
        opened_at = _parse_aware(payload.get("opened_at"), "opened_at")
        if opened_at < self.plan.unlock_at:
            raise ValidationError("holdout 在解锁时间前被 opened")
        return payload

    def _verified_terminal(self) -> dict[str, object]:
        payload = _verified_hashed_payload(
            self.root / "terminal.json",
            label="holdout terminal",
            hash_field="terminal_hash",
            contract="research-persistent-holdout-terminal-v2",
        )
        opened = self._verified_opened()
        if (
            payload.get("opened_hash") != opened.get("opened_hash")
            or payload.get("status") not in {"committed", "consumed_failed"}
            or not str(payload.get("result_hash", "")).strip()
            or (
                payload.get("status") == "consumed_failed"
                and not str(payload.get("reason") or "").strip()
            )
        ):
            raise ValidationError("holdout terminal 身份损坏")
        return payload

    def _retire(self, terminal: Mapping[str, object]) -> None:
        payload = {
            "contract_version": "research-persistent-holdout-retired-v2",
            "status": "retired",
            "holdout_identity_hash": self.plan.holdout_identity_hash,
            "terminal_hash": terminal["terminal_hash"],
            "final_status": terminal["status"],
        }
        payload["retired_hash"] = typed_canonical_hash(payload)
        path = self.root / "retired.json"
        try:
            _write_exclusive(path, payload)
        except FileExistsError:
            if _read_mapping(path, "holdout retired") != payload:
                raise ValidationError("holdout 已按不同终态 retired") from None


def _validate_freeze_payload(payload: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(payload, Mapping) or set(payload) != _FREEZE_PAYLOAD_FIELDS:
        raise ValidationError("holdout freeze payload 字段不闭合")
    normalized = _json_mapping(payload, "holdout freeze payload")
    for field in (
        "parent_research_purpose",
        "data_snapshot",
        "primary_estimand",
        "failure_policy",
    ):
        if not isinstance(normalized.get(field), str) or not normalized[field].strip():
            raise ValidationError(f"holdout freeze payload 的 {field} 不能为空")
    if normalized["failure_policy"] != "opened_then_failure_is_consumed":
        raise ValidationError("holdout failure_policy 必须声明 opened 后失败永久消耗")
    _require_sha256(normalized["package_plan_identity"], "package_plan_identity")
    _require_sha256(normalized["implementation_identity"], "implementation_identity")

    split = _mapping(normalized["holdout_split"], "holdout_split")
    if set(split) != {"split_id", "start", "end", "sample_ids"}:
        raise ValidationError("holdout freeze payload 的 holdout_split 字段不闭合")
    if not isinstance(split["split_id"], str) or not split["split_id"].strip():
        raise ValidationError("holdout split_id 不能为空")
    holdout_start = _iso_date(split["start"], "holdout start")
    holdout_end = _iso_date(split["end"], "holdout end")
    if holdout_end < holdout_start:
        raise ValidationError("holdout 结束日期不能早于开始日期")
    sample_ids = split["sample_ids"]
    if (
        not isinstance(sample_ids, list)
        or not sample_ids
        or any(not isinstance(item, str) or not item.strip() for item in sample_ids)
        or sample_ids != sorted(set(sample_ids))
    ):
        raise ValidationError("holdout sample_ids 必须非空、唯一并排序")

    mode = normalized["mode"]
    if mode not in HOLDOUT_MODES:
        raise ValidationError("holdout mode 无效")
    candidates = normalized["candidates"]
    if (
        not isinstance(candidates, list)
        or any(not isinstance(item, str) or not item.strip() for item in candidates)
        or candidates != sorted(set(candidates))
    ):
        raise ValidationError("holdout candidates 必须非空、唯一并排序")
    if mode == SINGLE_CANDIDATE_CONFIRMATION:
        if len(candidates) != 1:
            raise ValidationError("single_candidate_confirmation 必须冻结唯一候选")
        if normalized["multiple_testing"] is not None:
            raise ValidationError("单候选确认不得声明整族多重检验")
    else:
        if len(candidates) < 2:
            raise ValidationError("family_wide_confirmation 必须冻结完整候选族")
        correction = normalized["multiple_testing"]
        if not isinstance(correction, Mapping) or not str(correction.get("method", "")).strip():
            raise ValidationError("family_wide_confirmation 必须预声明多重检验")
        if correction.get("family_size") != len(candidates):
            raise ValidationError("整族多重检验 family_size 与候选全集不一致")

    selection_rule = _mapping(normalized["selection_rule"], "selection_rule")
    if set(selection_rule) != {"method", "uses_validation"}:
        raise ValidationError("selection_rule 字段不闭合")
    if not isinstance(selection_rule["method"], str) or not selection_rule["method"].strip():
        raise ValidationError("selection_rule method 不能为空")
    if type(selection_rule["uses_validation"]) is not bool:
        raise ValidationError("selection_rule uses_validation 必须是布尔值")
    validation = _mapping(normalized["validation"], "validation")
    if set(validation) != {"purpose", "rule"} or validation["purpose"] not in {
        "selection",
        "diagnostic",
    }:
        raise ValidationError("validation purpose 必须是 selection 或 diagnostic")
    if selection_rule["uses_validation"]:
        if validation["purpose"] != "selection" or not isinstance(validation["rule"], Mapping) or not validation["rule"]:
            raise ValidationError("参与选择的 validation 必须有预声明规则")
    elif validation["purpose"] != "diagnostic" or validation["rule"] is not None:
        raise ValidationError("未实际参与选择的 validation 必须标为 diagnostic")

    if normalized["direction"] not in {"greater", "less", "two_sided"}:
        raise ValidationError("holdout direction 无效")
    alpha = normalized["alpha"]
    if type(alpha) not in {int, float} or not 0.0 < float(alpha) < 1.0:
        raise ValidationError("holdout alpha 必须位于 0 和 1 之间")
    random_protocol = _mapping(normalized["random_protocol"], "random_protocol")
    if set(random_protocol) != {"combinations", "repetitions", "seed"}:
        raise ValidationError("random_protocol 字段不闭合")
    combinations = random_protocol["combinations"]
    if not isinstance(combinations, list) or combinations != sorted(set(combinations)):
        raise ValidationError("random_protocol combinations 必须唯一并排序")
    if type(random_protocol["repetitions"]) is not int or random_protocol["repetitions"] < 0:
        raise ValidationError("random_protocol repetitions 必须是非负整数")
    if type(random_protocol["seed"]) is not int or random_protocol["seed"] < 0:
        raise ValidationError("random_protocol seed 必须是非负整数")
    if bool(combinations) != (random_protocol["repetitions"] > 0):
        raise ValidationError("随机组合与重复次数必须共同冻结或共同为空")

    exposures = normalized["exposed_intervals"]
    if not isinstance(exposures, list):
        raise ValidationError("exposed_intervals 必须是列表")
    for exposed in exposures:
        item = _mapping(exposed, "exposed_interval")
        if set(item) != {"start", "end", "reason"} or not str(item["reason"]).strip():
            raise ValidationError("exposed_interval 字段不闭合")
        exposed_start = _iso_date(item["start"], "exposed start")
        exposed_end = _iso_date(item["end"], "exposed end")
        if exposed_end < exposed_start:
            raise ValidationError("已暴露区间结束日期不能早于开始日期")
        if max(holdout_start, exposed_start) <= min(holdout_end, exposed_end):
            raise ValidationError("holdout 与已暴露区间重叠，不能重新建立确认身份")

    aliases = _mapping(normalized["aliases"], "aliases")
    if set(aliases) != {"run", "package", "family"} or any(
        not isinstance(value, str) or not value.strip() for value in aliases.values()
    ):
        raise ValidationError("holdout aliases 必须闭合 run/package/family 名称")
    return normalized


def persistent_holdout_ledger_root(
    artifact_root: str | Path,
    *,
    scope: str,
    research_identity_hash: str,
    holdout_identity_hash: str | None = None,
) -> Path:
    """让同一工作目录中的新 run 复用同一研究 holdout 账本。"""

    if scope not in {"family", "model"}:
        raise ValidationError("holdout ledger scope 无效")
    _require_sha256(research_identity_hash, "research_identity_hash")
    root = (
        Path(artifact_root).resolve().parent
        / "holdout-ledgers"
        / scope
        / research_identity_hash
    )
    if holdout_identity_hash is None:
        return root
    _require_sha256(holdout_identity_hash, "holdout_identity_hash")
    return root / holdout_identity_hash


def _verify_open_authority(
    plan: HoldoutAccessPlan,
    *,
    token_hash: str,
    actor: str,
    reason: str,
    fixed_clock: datetime,
) -> None:
    if token_hash != plan.token_hash or actor != plan.actor or reason != plan.reason:
        raise ValidationError("holdout token、actor 或 reason 不匹配")
    _require_aware(fixed_clock, "fixed_clock")
    if fixed_clock < plan.unlock_at:
        raise ValidationError("holdout 尚未到预声明解锁时间")


def _verified_hashed_payload(
    path: Path,
    *,
    label: str,
    hash_field: str,
    contract: str,
) -> dict[str, object]:
    payload = _read_mapping(path, label)
    unsigned = dict(payload)
    digest = unsigned.pop(hash_field, None)
    if payload.get("contract_version") != contract or typed_canonical_hash(unsigned) != digest:
        raise ValidationError(f"{label} 损坏")
    return payload


def _read_mapping(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"{label} 不可读") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"{label} 必须是对象")
    return payload


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} 必须是对象")
    return value


def _json_mapping(value: Mapping[str, object], label: str) -> dict[str, object]:
    try:
        normalized = json.loads(canonical_json(dict(value)))
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} 必须能确定性序列化") from exc
    if not isinstance(normalized, dict):
        raise ValidationError(f"{label} 必须是对象")
    return normalized


def _iso_date(value: object, label: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValidationError(f"{label} 必须是 ISO 日期") from exc


def _require_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _SHA256_CHARS for char in value):
        raise ValidationError(f"{label} 必须是 sha256")


def _parse_aware(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ValidationError(f"{label} 无效") from exc
    _require_aware(parsed, label)
    return parsed


def _require_aware(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(f"{label} 必须包含时区")


def _write_exclusive(path: Path, payload: dict[str, object]) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


__all__ = [
    "FAMILY_WIDE_CONFIRMATION",
    "HOLDOUT_MODES",
    "HoldoutAccessEvent",
    "HoldoutAccessLedger",
    "HoldoutAccessPlan",
    "PersistentHoldoutLedger",
    "SINGLE_CANDIDATE_CONFIRMATION",
    "persistent_holdout_ledger_root",
]
