"""运行 Gate D/F/L 的冻结测试选择器并生成机器证据。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

from release_evidence_binding import release_evidence_binding
from wheel_source_inventory import verify_installed_source_inventory


PROTOCOL_VERSION = "research-release-gate-test-protocol-v2"
INPUT_VERSION = "research-release-gate-test-input-v2"
EVIDENCE_VERSION = "research-release-gate-test-evidence-v2"
REQUIRED_GATES = ("gate-d", "gate-f", "gate-l")


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"JSON 无法读取: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON 顶层必须是对象: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _junit_counts(path: Path) -> dict[str, int]:
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as exc:
        raise ValueError("Gate pytest JUnit 无法读取") from exc
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return {
        key: sum(int(suite.attrib.get(key, 0)) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }


def _pass_evidence_hash(path: Path, binding: dict[str, object]) -> str:
    payload = _load_json(path)
    if not isinstance(payload.get("contract_version"), str) or payload.get("status") != "pass":
        raise ValueError(f"前置 Gate evidence 未通过: {path}")
    if any(payload.get(key) != value for key, value in binding.items()):
        raise ValueError("前置 Gate evidence 未绑定同一候选")
    if payload.get("gate_id") != "gate-c" or payload.get("contract_version") != "research-gate-c-evidence-v2":
        raise ValueError("Gate L 前置必须是当前 Gate C 证据")
    return _sha256(path)


def run_gate_tests(
    *,
    python: Path,
    protocol_path: Path,
    input_path: Path,
    repository: Path,
    output: Path,
    release_candidate_id: str | None = None,
    build_manifest_path: Path | None = None,
) -> dict[str, dict[str, object]]:
    if output.exists():
        raise ValueError("Gate test 输出目录必须不存在")
    if not python.is_file() or not repository.is_dir():
        raise ValueError("Gate test Python 或仓库路径不存在")
    protocol = _load_json(protocol_path)
    inputs = _load_json(input_path)
    if protocol.get("contract_version") != PROTOCOL_VERSION:
        raise ValueError("Gate test protocol 版本无效")
    if inputs.get("contract_version") != INPUT_VERSION:
        raise ValueError("Gate test input 版本无效")
    gates = protocol.get("gates")
    prerequisites = inputs.get("prerequisite_evidence")
    if not isinstance(gates, dict) or tuple(gates) != REQUIRED_GATES:
        raise ValueError("Gate test 集合或顺序无效")
    if not isinstance(prerequisites, dict):
        raise ValueError("Gate test 前置证据映射无效")

    project = repository if (repository / "pyproject.toml").is_file() else repository / "research_pipeline"
    frozen_protocol = project / "release/gate-test-protocol.json"
    if protocol_path.read_bytes() != frozen_protocol.read_bytes():
        raise ValueError("Gate protocol 必须使用候选中的冻结选择器")
    binding = release_evidence_binding(
        release_candidate_id=release_candidate_id,
        build_manifest_path=build_manifest_path, project=project,
    )
    output.mkdir(parents=True)
    environment = os.environ.copy()
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"
    environment["PYTHONPATH"] = str(project)
    environment["TEMP"] = environment["TMP"] = str(output)
    runtime = verify_installed_source_inventory(python=python, project=project, cwd=output)
    results: dict[str, dict[str, object]] = {}
    for gate_id, raw_spec in gates.items():
        if not isinstance(raw_spec, dict):
            raise ValueError(f"{gate_id} protocol schema 无效")
        selectors = raw_spec.get("selectors")
        required_files = raw_spec.get("required_files")
        if (
            not isinstance(selectors, list)
            or not selectors
            or not all(isinstance(value, str) and value for value in selectors)
            or not isinstance(required_files, list)
        ):
            raise ValueError(f"{gate_id} 测试选择器或文件清单无效")
        prerequisite_hashes = {}
        prerequisite_gate = raw_spec.get("prerequisite_gate")
        if prerequisite_gate is not None:
            path = Path(str(prerequisites.get(str(prerequisite_gate), ""))).resolve()
            prerequisite_hashes[str(prerequisite_gate)] = _pass_evidence_hash(path, binding)
        absolute_selectors = []
        for selector in selectors:
            relative, separator, symbol = selector.partition("::")
            path = (project / relative).resolve()
            if not path.is_file() or not path.is_relative_to(project):
                raise ValueError(f"{gate_id} 测试文件越界或不存在")
            absolute_selectors.append(str(path) + (separator + symbol if separator else ""))
        junit_path = output / f"{gate_id}.junit.xml"
        command = [
            str(python), "-m", "pytest", *absolute_selectors,
            "-q", "-o", "pythonpath=", "-o", f"cache_dir={output / 'pytest-cache'}",
            f"--basetemp={output / (gate_id + '-tmp')}", f"--junitxml={junit_path}",
        ]
        completed = subprocess.run(
            command,
            cwd=output,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        (output / f"{gate_id}.stdout.txt").write_text(completed.stdout, encoding="utf-8")
        (output / f"{gate_id}.stderr.txt").write_text(completed.stderr, encoding="utf-8")
        counts = _junit_counts(junit_path)
        if completed.returncode != 0 or counts["failures"] or counts["errors"] or counts["skipped"] or not counts["tests"]:
            raise ValueError(f"{gate_id} 冻结测试未通过")
        file_hashes = {
            value: _sha256(project / value)
            for value in required_files
            if isinstance(value, str)
        }
        test_file_hashes = {}
        for selector in selectors:
            relative = str(selector).split("::", 1)[0]
            path = (project / relative).resolve()
            if not path.is_file() or not path.is_relative_to(project):
                raise ValueError(f"{gate_id} 测试文件越界或不存在")
            test_file_hashes[relative] = _sha256(path)
        evidence = {
            "contract_version": EVIDENCE_VERSION,
            "gate_id": gate_id,
            "status": "pass",
            **binding,
            "scope": "synthetic_contract_tests",
            "protocol_hash": _sha256(protocol_path),
            "input_hash": _sha256(input_path),
            "python": str(python),
            "runtime": runtime,
            "command": command,
            "selectors": selectors,
            "required_file_hashes": file_hashes,
            "test_file_hashes": dict(sorted(test_file_hashes.items())),
            "prerequisite_evidence_hashes": prerequisite_hashes,
            "pytest": counts,
            "junit_path": str(junit_path),
            "junit_sha256": _sha256(junit_path),
        }
        (output / f"{gate_id}-evidence.json").write_text(
            json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        results[gate_id] = evidence
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="运行 Gate D/F/L 冻结测试")
    parser.add_argument("--python", required=True)
    parser.add_argument("--protocol", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--release-candidate-id")
    parser.add_argument("--build-manifest")
    args = parser.parse_args()
    results = run_gate_tests(
        python=Path(args.python).resolve(),
        protocol_path=Path(args.protocol).resolve(),
        input_path=Path(args.input).resolve(),
        repository=Path(args.repository).resolve(),
        output=Path(args.output).resolve(),
        release_candidate_id=args.release_candidate_id,
        build_manifest_path=(
            Path(args.build_manifest).resolve() if args.build_manifest else None
        ),
    )
    print(json.dumps({
        "status": "pass",
        "gates": {gate: result["pytest"] for gate, result in results.items()},
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
