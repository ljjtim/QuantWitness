"""Gate I-A 固定黑盒协议、事件重放和评分。"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from typing import Callable, Mapping, Sequence

from research_pipeline.platform import MainlineError, typed_canonical_hash


GATE_IA_PROTOCOL_VERSION = "research-gate-ia-protocol-v1"
GATE_IA_EVENT_VERSION = "research-gate-ia-event-v1"
GATE_IA_TRIAL_VERSION = "research-gate-ia-trial-v1"
GATE_IA_SCORE_VERSION = "research-gate-ia-score-v1"


class GateIAError(MainlineError):
    error_code = "gate_ia_protocol_invalid"


@dataclass(frozen=True)
class GateIACommandEvent:
    sequence: int
    command: tuple[str, ...]
    exit_code: int
    stdout: str
    wall_milliseconds: int
    output_tokens: int
    human_help: bool = False
    contract_version: str = GATE_IA_EVENT_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != GATE_IA_EVENT_VERSION:
            raise GateIAError("Gate I-A event 版本无效")
        if type(self.sequence) is not int or self.sequence < 1:
            raise GateIAError("Gate I-A event sequence 无效")
        if not self.command or any(not isinstance(item, str) or not item for item in self.command):
            raise GateIAError("Gate I-A event command 无效")
        if type(self.exit_code) is not int or type(self.wall_milliseconds) is not int or self.wall_milliseconds < 0:
            raise GateIAError("Gate I-A event 退出码或耗时无效")
        if type(self.output_tokens) is not int or self.output_tokens < 0:
            raise GateIAError("Gate I-A event token 计数无效")
        if type(self.human_help) is not bool:
            raise GateIAError("Gate I-A event human_help 无效")

    def to_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "command": list(self.command),
            "exit_code": self.exit_code,
            "stdout_hash": typed_canonical_hash(self.stdout),
            "wall_milliseconds": self.wall_milliseconds,
            "output_tokens": self.output_tokens,
            "human_help": self.human_help,
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class GateIAProtocol:
    payload: Mapping[str, object]
    protocol_hash: str

    @classmethod
    def load_default(cls) -> "GateIAProtocol":
        resource = resources.files("research_pipeline").joinpath("gate_ia_protocol.json")
        payload = json.loads(resource.read_text(encoding="utf-8"))
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "GateIAProtocol":
        expected = {
            "contract_version", "protocol_id", "model_id", "initial_prompt",
            "visible_materials", "allowed_command_prefixes",
            "forbidden_command_fragments", "budget", "required_events", "acceptance",
        }
        if set(payload) != expected or payload.get("contract_version") != GATE_IA_PROTOCOL_VERSION:
            raise GateIAError("Gate I-A protocol schema 或版本无效")
        if any(not isinstance(payload.get(field), str) or not payload[field] for field in ("protocol_id", "model_id", "initial_prompt")):
            raise GateIAError("Gate I-A protocol 文本身份无效")
        for field in ("visible_materials", "allowed_command_prefixes", "forbidden_command_fragments", "required_events"):
            values = payload.get(field)
            if not isinstance(values, list) or not values or any(not isinstance(item, str) or not item for item in values):
                raise GateIAError(f"Gate I-A protocol {field} 无效")
        if len(set(payload["required_events"])) != len(payload["required_events"]):
            raise GateIAError("Gate I-A required_events 重复")
        budget = payload.get("budget")
        acceptance = payload.get("acceptance")
        if not isinstance(budget, Mapping) or set(budget) != {
            "max_turns", "max_wall_seconds", "max_output_tokens", "max_human_help_events",
        }:
            raise GateIAError("Gate I-A budget schema 无效")
        if not isinstance(acceptance, Mapping) or set(acceptance) != {
            "minimum_trials", "minimum_success_rate", "minimum_mean_step_score",
        }:
            raise GateIAError("Gate I-A acceptance schema 无效")
        if any(
            type(budget[field]) is not int or budget[field] <= 0
            for field in ("max_turns", "max_wall_seconds", "max_output_tokens")
        ) or type(budget["max_human_help_events"]) is not int or budget["max_human_help_events"] < 0:
            raise GateIAError("Gate I-A budget 数值无效")
        if type(acceptance["minimum_trials"]) is not int or acceptance["minimum_trials"] <= 0:
            raise GateIAError("Gate I-A minimum_trials 无效")
        for field in ("minimum_success_rate", "minimum_mean_step_score"):
            value = acceptance[field]
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 0.0 <= float(value) <= 1.0
            ):
                raise GateIAError(f"Gate I-A {field} 无效")
        return cls(dict(payload), typed_canonical_hash(payload))

    def to_dict(self) -> dict[str, object]:
        return {**dict(self.payload), "protocol_hash": self.protocol_hash}


@dataclass(frozen=True)
class GateIATrial:
    trial_id: str
    protocol_hash: str
    model_id: str
    initial_internal_ids: tuple[str, ...]
    events: tuple[GateIACommandEvent, ...]
    contract_version: str = GATE_IA_TRIAL_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != GATE_IA_TRIAL_VERSION:
            raise GateIAError("Gate I-A trial 版本无效")
        if not self.trial_id or len(self.protocol_hash) != 64 or not self.model_id:
            raise GateIAError("Gate I-A trial 身份无效")
        if self.initial_internal_ids:
            raise GateIAError("Gate I-A 初始提示不得提供内部 ID")
        if tuple(item.sequence for item in self.events) != tuple(range(1, len(self.events) + 1)):
            raise GateIAError("Gate I-A event sequence 不连续")


CommandRunner = Callable[[Sequence[str]], tuple[int, str, int]]


def subprocess_command_runner(arguments: Sequence[str]) -> tuple[int, str, int]:
    """仅执行公开 CLI；模型侧只看到 stdout，不读取 Python 源码。"""
    import time

    started = time.monotonic_ns()
    environment = os.environ.copy()
    # Windows 子进程默认跟随控制台代码页；固定 UTF-8，保证中文 JSON 可重放。
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"
    completed = subprocess.run(
        [sys.executable, "-m", "research_pipeline", *arguments],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
    )
    elapsed = (time.monotonic_ns() - started) // 1_000_000
    return completed.returncode, completed.stdout, int(elapsed)


def run_deterministic_black_box_trial(
    *,
    trial_id: str,
    output_root: str | Path,
    catalog_lock: str | Path,
    runner: CommandRunner = subprocess_command_runner,
    protocol: GateIAProtocol | None = None,
) -> GateIATrial:
    """基础代理只解析 JSON 结果；未知内部 ID 由 list/search 响应发现。"""
    selected = protocol or GateIAProtocol.load_default()
    _require_protocol_integrity(selected)
    events: list[GateIACommandEvent] = []

    def invoke(*arguments: str) -> dict[str, object] | None:
        code, stdout, elapsed = runner(arguments)
        events.append(GateIACommandEvent(
            len(events) + 1,
            tuple(arguments),
            code,
            stdout,
            elapsed,
            max(1, len(stdout) // 4),
        ))
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    invoke("recipe", "list", "--format", "json")
    generated_package = Path(output_root) / f"{trial_id}-package"
    resolved_catalog_lock = str(Path(catalog_lock).resolve())
    invoke(
        "package", "init", str(generated_package), "--json",
    )
    invoke(
        "catalog", "field", "search", "equity daily close",
        "--catalog-lock", resolved_catalog_lock,
        "--format", "json",
    )
    invoke("package", "lint", "--package", str(generated_package), "--json")
    invoke("operator", "describe", "unknown.factor.candidate", "--format", "json")
    invoke("operator", "list", "--format", "json")
    return GateIATrial(
        trial_id,
        selected.protocol_hash,
        str(selected.payload["model_id"]),
        (),
        tuple(events),
    )


def score_gate_ia_trials(
    trials: Sequence[GateIATrial],
    *,
    protocol: GateIAProtocol | None = None,
) -> dict[str, object]:
    selected = protocol or GateIAProtocol.load_default()
    _require_protocol_integrity(selected)
    trial_ids = [item.trial_id for item in trials]
    if len(trial_ids) != len(set(trial_ids)):
        raise GateIAError("Gate I-A trial_id 重复")
    scores = [_score_trial(item, selected) for item in trials]
    acceptance = selected.payload["acceptance"]
    minimum_trials = int(acceptance["minimum_trials"])
    success_rate = sum(item["passed"] for item in scores) / len(scores) if scores else 0.0
    mean_score = sum(float(item["step_score"]) for item in scores) / len(scores) if scores else 0.0
    passed = (
        len(scores) >= minimum_trials
        and success_rate >= float(acceptance["minimum_success_rate"])
        and mean_score >= float(acceptance["minimum_mean_step_score"])
    )
    values = {
        "contract_version": GATE_IA_SCORE_VERSION,
        "protocol_hash": selected.protocol_hash,
        "trial_count": len(scores),
        "success_rate": success_rate,
        "mean_step_score": mean_score,
        "passed": passed,
        "trials": scores,
    }
    return {**values, "score_hash": typed_canonical_hash(values)}


def _score_trial(trial: GateIATrial, protocol: GateIAProtocol) -> dict[str, object]:
    if trial.protocol_hash != protocol.protocol_hash or trial.model_id != protocol.payload["model_id"]:
        raise GateIAError("Gate I-A trial 与冻结 protocol/model 不一致")
    budget = protocol.payload["budget"]
    commands = [" ".join(item.command) for item in trial.events]
    allowed = tuple(str(item) for item in protocol.payload["allowed_command_prefixes"])
    forbidden = tuple(str(item) for item in protocol.payload["forbidden_command_fragments"])
    boundary_passed = all(
        any(command.startswith(prefix) for prefix in allowed)
        and not any(fragment.casefold() in command.casefold() for fragment in forbidden)
        for command in commands
    )
    boundary_passed = boundary_passed and (
        len(trial.events) <= int(budget["max_turns"])
        and sum(item.wall_milliseconds for item in trial.events) <= int(budget["max_wall_seconds"]) * 1000
        and sum(item.output_tokens for item in trial.events) <= int(budget["max_output_tokens"])
        and sum(item.human_help for item in trial.events) <= int(budget["max_human_help_events"])
    )
    parsed = [_parse_result(item) for item in trial.events]
    event_flags = {
        "recipe_inventory_checked": _has_success(parsed, commands, trial.events, "recipe list"),
        "no_public_recipe_observed": _has_empty_success(
            parsed, commands, trial.events, "recipe list"
        ),
        "field_discovered": _has_success(parsed, commands, trial.events, "catalog field search", require_items=True),
        "package_initialized": _has_success(parsed, commands, trial.events, "package init"),
        "package_draft_diagnosed": _has_draft_diagnostic(parsed, commands, trial.events),
        "missing_requirements_explained": _has_missing(parsed, commands, trial.events),
        "unknown_id_repaired": _has_unknown_repair(parsed, commands, trial.events),
    }
    required = tuple(str(item) for item in protocol.payload["required_events"])
    achieved = sorted(name for name in required if event_flags.get(name) is True)
    step_score = len(achieved) / len(required)
    passed = boundary_passed and step_score == 1.0
    return {
        "trial_id": trial.trial_id,
        "boundary_passed": boundary_passed,
        "achieved_events": achieved,
        "missing_events": sorted(set(required) - set(achieved)),
        "step_score": step_score,
        "passed": passed,
        "event_log_hash": typed_canonical_hash([item.to_dict() for item in trial.events]),
    }


def _require_protocol_integrity(protocol: GateIAProtocol) -> None:
    if typed_canonical_hash(protocol.payload) != protocol.protocol_hash:
        raise GateIAError("Gate I-A protocol 内容与身份不一致")


def _parse_result(event: GateIACommandEvent) -> Mapping[str, object] | None:
    try:
        payload = json.loads(event.stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, Mapping) else None


def _result_items(payload: Mapping[str, object] | None) -> list[object]:
    if not isinstance(payload, Mapping) or payload.get("status") != "pass":
        return []
    data = payload.get("data")
    items = data.get("items") if isinstance(data, Mapping) else None
    return list(items) if isinstance(items, list) else []


def _has_success(parsed, commands, events, prefix: str, *, require_items: bool = False) -> bool:
    for payload, command, event in zip(parsed, commands, events, strict=True):
        if (
            command.startswith(prefix)
            and event.exit_code == 0
            and isinstance(payload, Mapping)
            and payload.get("status") == "pass"
        ):
            if not require_items or _result_items(payload):
                return True
    return False


def _has_empty_success(parsed, commands, events, prefix: str) -> bool:
    return any(
        command.startswith(prefix)
        and event.exit_code == 0
        and isinstance(payload, Mapping)
        and payload.get("status") == "pass"
        and not _result_items(payload)
        for payload, command, event in zip(parsed, commands, events, strict=True)
    )


def _has_missing(parsed, commands, events) -> bool:
    if _has_draft_diagnostic(parsed, commands, events):
        return True
    for payload, command, event in zip(parsed, commands, events, strict=True):
        if (
            not command.startswith("package lint")
            or event.exit_code != 0
            or not isinstance(payload, Mapping)
        ):
            continue
        data = payload.get("data")
        if isinstance(data, Mapping) and data.get("missing_requirements"):
            return True
    return False


def _has_draft_diagnostic(parsed, commands, events) -> bool:
    initialized = {
        event.command[2]
        for payload, event in zip(parsed, events, strict=True)
        if event.command[:2] == ("package", "init")
        and len(event.command) > 2
        and event.exit_code == 0
        and isinstance(payload, Mapping)
        and payload.get("status") == "pass"
    }
    return any(
        command.startswith("package lint")
        and "--package" in event.command
        and event.command.index("--package") + 1 < len(event.command)
        and event.command[event.command.index("--package") + 1] in initialized
        and event.exit_code == 1
        and isinstance(payload, Mapping)
        and payload.get("status") == "fail"
        and payload.get("error_code") == "research_package_invalid"
        and isinstance(payload.get("message"), str)
        and payload["message"] == "sources/sources.yaml.sources 必须是非空列表；请填写对应声明后重新运行 package lint"
        for payload, command, event in zip(parsed, commands, events, strict=True)
    )


def _has_unknown_repair(parsed, commands, events) -> bool:
    failed = any(
        command.startswith("operator describe")
        and event.exit_code != 0
        and isinstance(payload, Mapping)
        and payload.get("error_code") == "discovery_not_found"
        and isinstance(payload.get("data"), Mapping)
        and payload["data"].get("next_commands")
        for payload, command, event in zip(parsed, commands, events, strict=True)
    )
    return failed and _has_success(
        parsed,
        commands,
        events,
        "operator list",
        require_items=True,
    )


__all__ = [
    "GATE_IA_EVENT_VERSION",
    "GATE_IA_PROTOCOL_VERSION",
    "GATE_IA_SCORE_VERSION",
    "GATE_IA_TRIAL_VERSION",
    "GateIACommandEvent",
    "GateIAError",
    "GateIAProtocol",
    "GateIATrial",
    "run_deterministic_black_box_trial",
    "score_gate_ia_trials",
    "subprocess_command_runner",
]
