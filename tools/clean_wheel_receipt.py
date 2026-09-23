"""复验干净 wheel 黑盒验收收据；能力不能靠脚本自报晋级。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Mapping


CLEAN_WHEEL_RECEIPT_VERSION = "research-clean-wheel-acceptance-v1"
VALIDATED_CAPABILITY_IDS = ("capability.discovery", "research_package.plan")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_BASE_COMMAND_IDS = {
    "wheel.inventory",
    "source.capabilities",
    "venv.create",
    "wheel.install",
    "installed.import-origin",
    "installed.version",
    "installed.help",
    "installed.capabilities",
    "installed.package-data",
    "installed.dependencies",
    "package.init",
    "package.lint",
    "recipe.list",
    "recipe.unknown-id",
    "installed.environment",
}
_PAYLOAD_FIELDS = {
    "contract_version",
    "status",
    "wheel_name",
    "wheel_sha256",
    "dependency_lock_sha256",
    "environment",
    "installed_version",
    "inventory",
    "capabilities_sha256",
    "package_data_probe_sha256",
    "recipe_list_sha256",
    "validated_capability_ids",
    "commands",
    "source_tree_on_pythonpath_during_installed_checks",
    "database_access",
    "network_access",
}
_COMMAND_FIELDS = {
    "command_id",
    "argument_hash",
    "expected",
    "exit_code",
    "expectation_met",
    "stdout_sha256",
    "stderr_sha256",
    "wall_milliseconds",
}


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_hash(payload: object) -> str:
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _require_digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field} 必须是 SHA-256")
    return value


def verify_clean_wheel_receipt(
    receipt_path: str | Path,
    *,
    wheel: str | Path,
    dependency_lock: str | Path,
) -> dict[str, object]:
    """复验真实输入字节、命令闭包和能力边界，拒绝孤立 status=pass。"""

    receipt = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
    if set(receipt) != {"payload", "receipt_payload_sha256"} or not isinstance(
        receipt.get("payload"), Mapping
    ):
        raise ValueError("干净 wheel receipt schema 无效")
    payload = receipt["payload"]
    if set(payload) != _PAYLOAD_FIELDS:
        raise ValueError("干净 wheel receipt payload schema 无效")
    if receipt["receipt_payload_sha256"] != _canonical_hash(payload):
        raise ValueError("干净 wheel receipt payload hash 漂移")
    if (
        payload.get("contract_version") != CLEAN_WHEEL_RECEIPT_VERSION
        or payload.get("status") != "pass"
    ):
        raise ValueError("干净 wheel receipt 版本或状态无效")

    wheel_path = Path(wheel).resolve()
    lock_path = Path(dependency_lock).resolve()
    for path, label in (
        (wheel_path, "wheel"),
        (lock_path, "dependency lock"),
    ):
        if not path.is_file():
            raise ValueError(f"干净 wheel receipt 的 {label} 输入不存在")
    expected_files = {
        "wheel_name": wheel_path.name,
        "wheel_sha256": _sha256_file(wheel_path),
        "dependency_lock_sha256": _sha256_file(lock_path),
    }
    if any(payload.get(field) != value for field, value in expected_files.items()):
        raise ValueError("干净 wheel receipt 与当前输入字节不一致")

    commands = payload.get("commands")
    if not isinstance(commands, list):
        raise ValueError("干净 wheel receipt 缺少命令记录")
    command_ids: list[str] = []
    for command in commands:
        if not isinstance(command, Mapping) or set(command) != _COMMAND_FIELDS:
            raise ValueError("干净 wheel command receipt schema 无效")
        command_id = command.get("command_id")
        expected = command.get("expected")
        exit_code = command.get("exit_code")
        if (
            not isinstance(command_id, str)
            or not command_id
            or expected not in {"success", "failure"}
            or type(exit_code) is not int
            or command.get("expectation_met") is not True
            or (expected == "success") != (exit_code == 0)
        ):
            raise ValueError("干净 wheel command receipt 结果自相矛盾")
        for field in ("argument_hash", "stdout_sha256", "stderr_sha256"):
            _require_digest(command.get(field), field)
        if type(command.get("wall_milliseconds")) is not int or command["wall_milliseconds"] <= 0:
            raise ValueError("干净 wheel command receipt 墙钟无效")
        command_ids.append(command_id)
    if len(command_ids) != len(set(command_ids)):
        raise ValueError("干净 wheel command receipt ID 重复")
    expected_command_ids = _BASE_COMMAND_IDS
    actual_command_ids = set(command_ids)
    if actual_command_ids != expected_command_ids:
        raise ValueError(
            "干净 wheel receipt 命令闭包不精确: "
            f"missing={sorted(expected_command_ids - actual_command_ids)}, "
            f"extra={sorted(actual_command_ids - expected_command_ids)}"
        )
    command_expectations = {
        command["command_id"]: command["expected"]
        for command in commands
    }
    if any(
        expectation != ("failure" if command_id == "recipe.unknown-id" else "success")
        for command_id, expectation in command_expectations.items()
    ):
        raise ValueError("干净 wheel receipt 命令预期与固定协议不一致")

    inventory = payload.get("inventory")
    environment = payload.get("environment")
    if (
        not isinstance(inventory, Mapping)
        or set(inventory)
        != {
            "status",
            "package_file_count",
            "python_file_count",
            "package_data_file_count",
            "inventory_sha256",
        }
        or inventory.get("status") != "pass"
        or type(inventory.get("package_file_count")) is not int
        or inventory["package_file_count"] <= 0
        or type(inventory.get("python_file_count")) is not int
        or inventory["python_file_count"] <= 0
        or type(inventory.get("package_data_file_count")) is not int
        or inventory["package_data_file_count"] <= 0
        or inventory.get("package_file_count")
        != inventory.get("python_file_count", 0) + inventory.get("package_data_file_count", 0)
        or not isinstance(environment, Mapping)
        or set(environment) != {"python", "cache_tag", "platform"}
        or not isinstance(environment.get("python"), str)
        or not environment["python"].startswith("3.10.")
        or environment.get("platform") != "windows"
        or environment.get("cache_tag") != "cpython-310"
        or not isinstance(payload.get("installed_version"), str)
        or not payload["installed_version"].startswith("QuantWitness 1.0.0")
    ):
        raise ValueError("干净 wheel receipt 清单或正式环境身份无效")
    for field in (
        "inventory_sha256",
        "capabilities_sha256",
        "package_data_probe_sha256",
        "recipe_list_sha256",
    ):
        source = inventory if field == "inventory_sha256" else payload
        _require_digest(source.get(field), field)
    capability_ids = payload.get("validated_capability_ids")
    if (
        not isinstance(capability_ids, list)
        or tuple(capability_ids) != VALIDATED_CAPABILITY_IDS
        or payload.get("source_tree_on_pythonpath_during_installed_checks") is not False
        or payload.get("database_access") != "none"
        or payload.get("network_access") != "none"
    ):
        raise ValueError("干净 wheel receipt 能力或隔离边界无效")
    result = {
        "contract_version": CLEAN_WHEEL_RECEIPT_VERSION,
        "status": "pass",
        "receipt_payload_sha256": receipt["receipt_payload_sha256"],
        "wheel_sha256": payload["wheel_sha256"],
        "validated_capability_ids": list(VALIDATED_CAPABILITY_IDS),
    }
    return {**result, "verification_hash": _canonical_hash(result)}


def main() -> int:
    parser = argparse.ArgumentParser(description="复验干净 wheel 验收收据")
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--dependency-lock", required=True)
    args = parser.parse_args()
    result = verify_clean_wheel_receipt(
        args.receipt,
        wheel=args.wheel,
        dependency_lock=args.dependency_lock,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
