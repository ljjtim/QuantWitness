"""同一研究系列的最终评价登记与打开事务。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from research_pipeline.platform.canonical import canonical_json
from research_pipeline.platform.file_lock import ProcessFileLock

from .holdout import (
    HoldoutAccessPlan, PersistentHoldoutLedger, _read_mapping, _write_exclusive,
    normalize_series_declaration, series_purpose_identity,
)
from .splits import ValidationError


SERIES_REGISTRATION_VERSION = "research-holdout-series-registration-v1"
SERIES_EVIDENCE_VERSION = "research-holdout-series-evidence-v1"


def series_from_parameters(parameters: Mapping[str, object]) -> "HoldoutSeries":
    try:
        declaration = json.loads(str(parameters["research_series_json"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("最终评价缺少合法 research_series_json") from exc
    return HoldoutSeries(declaration)


def _read_opened_ledger(root: Path) -> dict[str, object] | None:
    raw = dict(_read_mapping(root / "plan.json", "系列 holdout plan"))
    try:
        plan = HoldoutAccessPlan.build(
            freeze_payload=raw["freeze_payload"], actor=raw["actor"], reason=raw["reason"],
            unlock_at=datetime.fromisoformat(raw["unlock_at"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("系列账本冻结计划无效") from exc
    if (raw.get("contract_version") != "research-persistent-holdout-plan-v2"
            or raw.get("state") != "frozen" or raw.get("plan_hash") != plan.plan_hash
            or raw.get("token_hash") != plan.token_hash or raw.get("holdout_identity_hash") != plan.holdout_identity_hash):
        raise ValidationError("系列账本计划身份损坏")
    ledger = PersistentHoldoutLedger(root, plan)
    prepared = ledger._verified_prepared() if (root / "prepared.json").exists() else None
    if not (root / "opened.json").exists():
        if (root / "terminal.json").exists():
            raise ValidationError("系列账本终态缺少 opened")
        return None
    opened = ledger._verified_opened()
    terminal = ledger._verified_terminal() if (root / "terminal.json").exists() else None
    return {"ledger_ref": str(root.resolve()), "ledger": {
        "plan": raw, "prepared": prepared, "opened": opened, "terminal": terminal,
    }}


def exposed_intervals(records: list[dict[str, object]]) -> list[dict[str, str]]:
    return [{"start": record["ledger"]["plan"]["freeze_payload"]["holdout_split"]["start"],
             "end": record["ledger"]["plan"]["freeze_payload"]["holdout_split"]["end"],
             "reason": record["ledger_ref"]} for record in records]


class HoldoutSeries:
    def __init__(self, declaration: object) -> None:
        self.declaration = normalize_series_declaration(declaration)
        self.directory = Path(self.declaration["root"]) / self.declaration["series_id"]
        self.ledger_root = self.directory / "ledgers"
        self.registration = {"contract_version": SERIES_REGISTRATION_VERSION,
                             "declaration": self.declaration}

    @property
    def purpose_identity(self) -> str:
        return series_purpose_identity(self.declaration)

    def _lock(self) -> ProcessFileLock:
        return ProcessFileLock(self.directory / "series.lock", timeout_seconds=10.0,
                               timeout_error=ValidationError("研究系列账本正在被使用，请稍后重试"))

    def _register(self) -> None:
        for raw in self.declaration["legacy_ledger_roots"]:
            root = Path(raw)
            if not root.is_dir() or not any(root.rglob("plan.json")):
                raise ValidationError(f"旧账本根不存在或没有冻结计划: {root}")
        path = self.directory / "registration.json"
        if path.exists():
            existing = dict(_read_mapping(path, "研究系列登记"))
            if (set(existing) != {"contract_version", "declaration", "registered_at"}
                    or existing["contract_version"] != SERIES_REGISTRATION_VERSION
                    or existing["declaration"] != self.declaration):
                raise ValidationError("研究系列登记与当前声明不一致，不能更换研究问题或旧账本")
            registered = datetime.fromisoformat(str(existing["registered_at"]))
            if registered.tzinfo is None or registered.utcoffset() is None:
                raise ValidationError("系列登记时间必须带时区")
            self.registration = existing
        else:
            self._scan()
            self.registration["registered_at"] = datetime.now(timezone.utc).isoformat()
            _write_exclusive(path, self.registration)

    def _scan(self, *, exclude: Path | None = None) -> list[dict[str, object]]:
        roots = {path.parent for pattern in ("*/plan.json", "*/opened.json", "*/terminal.json")
                 for path in self.ledger_root.glob(pattern)}
        for raw in self.declaration["legacy_ledger_roots"]:
            legacy = Path(raw)
            roots.update(path.parent for name in ("plan.json", "opened.json", "terminal.json")
                         for path in legacy.rglob(name))
        records = []
        for root in sorted(roots):
            if exclude is not None and root.resolve() == exclude.resolve():
                continue
            record = _read_opened_ledger(root)
            if record is not None:
                if root.parent == self.ledger_root and record["ledger"]["plan"]["freeze_payload"].get("research_series") != self.declaration:
                    raise ValidationError("系列目录包含其他研究声明的账本")
                records.append(record)
        return records

    def freeze(self) -> list[dict[str, object]]:
        with self._lock():
            self._register()
            return self._scan()

    def _save_evidence(self, ledger: PersistentHoldoutLedger, evidence: dict[str, object]) -> None:
        ledger.root.mkdir(parents=True, exist_ok=True)
        path = ledger.root / "series.json"
        temporary = ledger.root / "series.pending.json"
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(canonical_json(evidence))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    def freeze_plan(self, *, freeze_payload: Mapping[str, object], actor: str, reason: str,
                    unlock_at: datetime) -> tuple[HoldoutAccessPlan, list[dict[str, object]], PersistentHoldoutLedger, dict[str, object]]:
        with self._lock():
            self._register()
            provisional = HoldoutAccessPlan.build(freeze_payload={**freeze_payload, "exposed_intervals": []},
                                                  actor=actor, reason=reason, unlock_at=unlock_at)
            root = self.ledger_root / provisional.holdout_identity_hash
            if (root / "plan.json").exists():
                raw = _read_mapping(root / "plan.json", "系列原冻结计划")
                original = HoldoutAccessPlan.build(freeze_payload=raw["freeze_payload"], actor=raw["actor"],
                                                   reason=raw["reason"], unlock_at=datetime.fromisoformat(raw["unlock_at"]))
                if ({**original.freeze_payload, "exposed_intervals": []} != provisional.freeze_payload
                        or original.actor != actor or original.reason != reason or original.unlock_at != unlock_at):
                    raise ValidationError("同一 holdout 身份已冻结，不能更换原计划声明")
                evidence = _read_mapping(root / "series.json", "系列原冻结暴露")
                if (evidence.get("contract_version") != SERIES_EVIDENCE_VERSION
                        or evidence.get("declaration") != self.declaration
                        or evidence.get("registration") != self.registration
                        or not isinstance(evidence.get("frozen_ledgers"), list)):
                    raise ValidationError("原计划系列冻结证据无效")
                frozen = evidence["frozen_ledgers"]
                if exposed_intervals(frozen) != original.freeze_payload["exposed_intervals"]:
                    raise ValidationError("原计划冻结暴露与账本不一致")
                ledger = PersistentHoldoutLedger(root, original)
                return original, frozen, ledger, ledger.verify()
            frozen = self._scan()
            plan = HoldoutAccessPlan.build(freeze_payload={**freeze_payload, "exposed_intervals": exposed_intervals(frozen)},
                                           actor=actor, reason=reason, unlock_at=unlock_at)
            ledger = PersistentHoldoutLedger(root, plan)
            self._save_evidence(ledger, {"contract_version": SERIES_EVIDENCE_VERSION,
                "declaration": self.declaration, "registration": self.registration,
                "frozen_ledgers": frozen, "opening_ledgers": None})
            return plan, frozen, ledger, ledger.verify()

    def prepare(self, ledger: PersistentHoldoutLedger, *, prepared_at: datetime, preflight) -> dict[str, object]:
        facts = preflight()
        with self._lock():
            return ledger.prepare(prepared_at=prepared_at, preflight=lambda: facts)

    def open(self, ledger: PersistentHoldoutLedger, *, frozen_ledgers: list[dict[str, object]],
             opening_id: str, token_hash: str, actor: str, reason: str,
             fixed_clock: datetime, prepared_hash: str) -> dict[str, object]:
        if ledger.root != (self.ledger_root / ledger.plan.holdout_identity_hash).resolve():
            raise ValidationError("最终评价账本位置与冻结研究系列不一致")
        if ledger.plan.freeze_payload.get("research_series") != self.declaration:
            raise ValidationError("最终评价计划更换了研究系列")
        with self._lock():
            self._register()
            current = self._scan(exclude=ledger.root)
            current_by_ref = {record["ledger_ref"]: record for record in current}
            for frozen in frozen_ledgers:
                observed = current_by_ref.get(frozen["ledger_ref"])
                if observed is None or any(observed["ledger"][key] != frozen["ledger"][key]
                                          for key in ("plan", "prepared", "opened")):
                    raise ValidationError("研究系列既往暴露记录丢失或改变")
                if frozen["ledger"]["terminal"] is not None and observed["ledger"]["terminal"] != frozen["ledger"]["terminal"]:
                    raise ValidationError("研究系列既往终态改变")
            if ledger.plan.freeze_payload["exposed_intervals"] != exposed_intervals(frozen_ledgers):
                raise ValidationError("冻结暴露区间与系列账本不一致")
            updated = {**ledger.plan.freeze_payload, "exposed_intervals": exposed_intervals(current)}
            # 只重做重叠准入；原计划和token继续用于打开。
            HoldoutAccessPlan.build(freeze_payload=updated, actor=actor, reason=reason, unlock_at=ledger.plan.unlock_at)
            evidence = {"contract_version": SERIES_EVIDENCE_VERSION, "declaration": self.declaration,
                        "registration": self.registration, "frozen_ledgers": frozen_ledgers,
                        "opening_ledgers": current}
            if (ledger.root / "opened.json").exists():
                raise ValidationError("holdout 已经 opened，不能重复读取")
            self._save_evidence(ledger, evidence)
            return ledger.open(opening_id=opening_id, token_hash=token_hash, actor=actor,
                               reason=reason, fixed_clock=fixed_clock, prepared_hash=prepared_hash)

    def finish(self, ledger: PersistentHoldoutLedger, **kwargs) -> dict[str, object]:
        with self._lock():
            return ledger.finish(**kwargs)
