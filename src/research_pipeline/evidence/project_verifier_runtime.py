"""独立复核已封存 Result 中显式准入的项目 Verifier bundle。"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

import psutil
from dataclasses import dataclass
from typing import Mapping

from research_pipeline.extensions.verifier_bundle import (
    ProjectVerifierBundleManifest,
    verify_project_verifier_bundle,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.results import ResultSnapshot
from research_pipeline.results.errors import ResultContractError
from research_pipeline.platform.resource_budget import ResourceBudget
from research_pipeline.platform.process_resources import (
    _measure_attempt_tree_bytes,
    _project_process_usage,
    _terminate_process_tree,
)
from .oracle_workspace import FinancialOracleBudget


PROJECT_VERIFIER_OUTPUT_VERSION = "project-verifier-output-v2"
_PROJECT_VERIFIER_OUTPUT_V1 = "project-verifier-output-v1"
_STATISTICS_MATRIX_EVIDENCE_VERSION = "research-statistics-matrix-evidence-v1"


class ProjectVerifierResourceError(ResultContractError):
    """保留复核资源失败的机器码和实际测量，不混同结果合同错误。"""

    def __init__(self, code: str, *, exceeded=None, measurement_status=None, configuration=None):
        super().__init__(code)
        self.error_code = code
        self.failure_payload = {}
        if exceeded:
            self.failure_payload["exceeded"] = exceeded
        if measurement_status is not None:
            self.failure_payload["resource_measurement_status"] = measurement_status
        if configuration is not None:
            self.failure_payload["configuration"] = configuration


def _statistics_matrix_evidence(payload: object) -> dict[str, object]:
    expected = {
        "contract_version",
        "matrix_rows",
        "matrix_effective_rows",
        "matrix_columns",
        "matrix_rank",
    }
    if not isinstance(payload, Mapping) or set(payload) != expected:
        raise ResultContractError("项目 Verifier 统计矩阵证据无效")
    if payload.get("contract_version") != _STATISTICS_MATRIX_EVIDENCE_VERSION:
        raise ResultContractError("项目 Verifier 统计矩阵证据版本不受支持")
    values = {
        name: payload[name]
        for name in (
            "matrix_rows",
            "matrix_effective_rows",
            "matrix_columns",
            "matrix_rank",
        )
    }
    if any(type(value) is not int for value in values.values()):
        raise ResultContractError("项目 Verifier 统计矩阵证据必须是整数")
    rows = int(values["matrix_rows"])
    effective_rows = int(values["matrix_effective_rows"])
    columns = int(values["matrix_columns"])
    rank = int(values["matrix_rank"])
    if (
        rows <= 0
        or columns <= 0
        or not 0 <= effective_rows <= rows
        or not 0 <= rank <= min(effective_rows, columns)
    ):
        raise ResultContractError("项目 Verifier 统计矩阵证据范围无效")
    return {
        "contract_version": _STATISTICS_MATRIX_EVIDENCE_VERSION,
        "matrix_rows": rows,
        "matrix_effective_rows": effective_rows,
        "matrix_columns": columns,
        "matrix_rank": rank,
    }


@dataclass(frozen=True)
class ProjectVerifierOutcome:
    verifier_identity: Mapping[str, object]
    result_id: str
    status: str
    findings: tuple[str, ...]
    evidence_hashes: Mapping[str, str]
    statistics_matrix_evidence: Mapping[str, object] | None
    outcome_hash: str
    contract_version: str

    def __post_init__(self) -> None:
        if self.status not in {"pass", "fail"}:
            raise ResultContractError("项目 Verifier status 无效")
        if tuple(sorted(set(self.findings))) != self.findings:
            raise ResultContractError("项目 Verifier findings 必须唯一并排序")
        hashes = dict(sorted(self.evidence_hashes.items()))
        if any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for key, value in hashes.items()
        ):
            raise ResultContractError("项目 Verifier evidence hash 无效")
        object.__setattr__(self, "evidence_hashes", hashes)
        if self.contract_version == _PROJECT_VERIFIER_OUTPUT_V1:
            if self.statistics_matrix_evidence is not None:
                raise ResultContractError("项目 Verifier v1 不接受统计矩阵证据")
        elif self.contract_version == PROJECT_VERIFIER_OUTPUT_VERSION:
            if self.statistics_matrix_evidence is not None:
                object.__setattr__(
                    self,
                    "statistics_matrix_evidence",
                    _statistics_matrix_evidence(self.statistics_matrix_evidence),
                )
        else:
            raise ResultContractError("项目 Verifier 输出版本不受支持")
        if self.outcome_hash != typed_canonical_hash(self.payload()):
            raise ResultContractError("项目 Verifier outcome hash 不一致")

    def payload(self) -> dict[str, object]:
        payload = {
            "verifier_identity": dict(self.verifier_identity),
            "result_id": self.result_id,
            "status": self.status,
            "findings": list(self.findings),
            "evidence_hashes": dict(self.evidence_hashes),
            "contract_version": self.contract_version,
        }
        if self.contract_version == PROJECT_VERIFIER_OUTPUT_VERSION:
            payload["statistics_matrix_evidence"] = (
                None
                if self.statistics_matrix_evidence is None
                else dict(self.statistics_matrix_evidence)
            )
        return payload


def execute_project_verifier(
    *,
    bundle_path: str | Path,
    snapshot: ResultSnapshot,
    expected_identity: Mapping[str, object],
    scratch_root: str | Path | None = None,
    budget: ResourceBudget | None = None,
    process_slots: int = 2,
) -> ProjectVerifierOutcome:
    """仅向项目 Verifier 暴露 Result 冻结身份中授权的表和支持工件。"""
    default_budget = FinancialOracleBudget()
    budget = budget or ResourceBudget(
        default_budget.memory_bytes, 1, default_budget.temp_bytes, 300
    )
    if type(process_slots) is not int or process_slots < 2:
        raise ProjectVerifierResourceError("project_verifier_process_slots_exceeded", configuration={"declared_slots": process_slots, "minimum_slots": 2})
    started = time.monotonic()
    manifest = verify_project_verifier_bundle(bundle_path)
    if manifest.identity() != dict(expected_identity):
        raise ResultContractError("项目 Verifier bundle 与 Result 冻结身份不一致")
    # Result 的 project_id 是内容寻址的 Package ID；逻辑项目身份由冻结的
    # verifier_identity 绑定，不把两种不同身份混为一谈。
    available_schemas = {item.schema_id for item in snapshot.bundle.tables}
    if not set(manifest.authorized_schema_ids) <= available_schemas:
        raise ResultContractError("项目 Verifier 授权表不属于当前 Result")
    support_by_path = {
        item.source_path: item
        for item in snapshot.bundle.support_files
        if item.artifact_type in manifest.authorized_support_artifact_types
    }
    if set(manifest.authorized_support_artifact_types) - {
        item.artifact_type for item in support_by_path.values()
    }:
        raise ResultContractError("项目 Verifier 授权支持工件不属于当前 Result")
    missing_support = set(support_by_path) - set(snapshot.support_bytes)
    if missing_support:
        raise ResultContractError("项目 Verifier 授权支持工件未进入已验证快照")

    scratch_parent = None if scratch_root is None else str(Path(scratch_root).resolve())
    with tempfile.TemporaryDirectory(prefix="rp-project-verifier-", dir=scratch_parent) as raw:
        root = Path(raw)
        guard = _VerifierResources(root, budget, process_slots, started)
        guard.check()
        required_bytes = sum(
            (snapshot.directory / relative_path).stat().st_size
            for schema_id in manifest.authorized_schema_ids
            for relative_path in snapshot.table_manifest(schema_id).files
        ) + sum(len(snapshot.support_bytes[path]) for path in support_by_path)
        if required_bytes > budget.temp_bytes:
            raise ProjectVerifierResourceError("project_verifier_temp_exceeded", exceeded={"temp_bytes": {"actual": required_bytes, "limit": budget.temp_bytes}}, measurement_status="measured")
        free_bytes = shutil.disk_usage(root).free
        if required_bytes > free_bytes:
            raise ProjectVerifierResourceError("project_verifier_disk_space_exceeded", exceeded={"disk_space_bytes": {"actual": required_bytes, "limit": free_bytes}}, measurement_status="measured")
        input_root = root / "input"
        input_root.mkdir()
        table_entries = []
        for schema_id in manifest.authorized_schema_ids:
            table = snapshot.table_manifest(schema_id)
            table_root = input_root / "tables" / schema_id
            table_root.mkdir(parents=True)
            files = []
            for index, relative_path in enumerate(sorted(table.files)):
                source = snapshot.directory / relative_path
                target = table_root / f"part-{index:05d}.parquet"
                _copy_verifier_input(source, target, guard)
                files.append(target.relative_to(input_root).as_posix())
            table_entries.append({
                "schema_id": schema_id,
                "artifact_type": table.artifact_type,
                "files": files,
            })
        support_entries = []
        for index, (source_path, item) in enumerate(sorted(support_by_path.items())):
            target = input_root / "support" / f"item-{index:05d}.bin"
            target.parent.mkdir(parents=True, exist_ok=True)
            guard.check()
            try:
                content = memoryview(snapshot.support_bytes[source_path])
                with target.open("wb") as writer:
                    for offset in range(0, len(content), 1024 * 1024):
                        guard.check(check_disk=False)
                        writer.write(content[offset:offset + 1024 * 1024])
                del content
            except OSError as exc:
                raise ProjectVerifierResourceError("project_verifier_copy_failed") from exc
            guard.check()
            support_entries.append({
                "artifact_type": item.artifact_type,
                "source_path": source_path,
                "file": target.relative_to(input_root).as_posix(),
                "content_hash": item.content_hash,
            })
        input_manifest = {
            "contract_version": "project-verifier-input-v1",
            "result_id": snapshot.bundle.result_id,
            "tables": table_entries,
            "support_files": support_entries,
        }
        (input_root / "manifest.json").write_text(
            canonical_json(input_manifest), encoding="utf-8"
        )
        context = {
            "contract_version": "project-verifier-context-v1",
            "project_id": manifest.project_id,
            "result_project_id": snapshot.bundle.project_id,
            "result_id": snapshot.bundle.result_id,
            "package_hash": snapshot.bundle.package_hash,
            "plan_hash": snapshot.bundle.plan_hash,
            "verifier_identity": manifest.identity(),
        }
        context_path = root / "context.json"
        context_path.write_text(canonical_json(context), encoding="utf-8")
        output_path = root / "outcome.json"
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                     "NUMEXPR_NUM_THREADS", "ARROW_NUM_THREADS"):
            environment[name] = str(budget.cpu_slots)
        guard.check()
        process = subprocess.Popen(
            [
                sys.executable, "-B", "-c",
                "import runpy, sys; sys.path.insert(0, sys.argv.pop(1)); "
                "runpy.run_module('research_pipeline.evidence.project_verifier_worker', run_name='__main__')",
                str(Path(__file__).resolve().parents[2]),
                str(Path(bundle_path).resolve()), str(context_path),
                str(input_root), str(output_path),
            ],
            env=environment, cwd=root,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        resource_error = None
        try:
            while process.poll() is None:
                guard.check(process)
                time.sleep(0.02)
            guard.check()
            if process.returncode != 0:
                raise ProjectVerifierResourceError("project_verifier_worker_failed")
        except ProjectVerifierResourceError as exc:
            resource_error = exc
            raise
        finally:
            cleanup = _terminate_process_tree(
                process, observed_descendants=guard.descendants
            )
            if resource_error is not None:
                resource_error.failure_payload["process_cleanup_status"] = cleanup
        if cleanup != "complete":
            raise ProjectVerifierResourceError("project_verifier_cleanup_failed")
        try:
            raw_outcome = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ResultContractError("项目 Verifier 未生成有效输出") from exc
        guard.check()
        outcome = _parse_outcome(raw_outcome, manifest, snapshot.bundle.result_id)
        guard.check()
    return outcome


class _VerifierResources:
    """准备和执行共用一个父进程加当前 Worker 树的资源包络。"""

    def __init__(self, root, budget, process_slots, started):
        self.root = root
        self.budget = budget
        self.process_slots = process_slots
        self.started = started
        self.descendants = {}

    def check(self, process=None, *, check_disk=True):
        elapsed = time.monotonic() - self.started
        if elapsed > self.budget.wall_seconds:
            raise ProjectVerifierResourceError("project_verifier_timeout", exceeded={"wall_seconds": {"actual": elapsed, "limit": self.budget.wall_seconds}}, measurement_status="measured")
        try:
            rss, count = _project_process_usage(
                None if process is None else process.pid, self.descendants,
            )
            disk_bytes = _measure_attempt_tree_bytes(self.root) if check_disk else 0
        except (psutil.Error, OSError, RuntimeError) as exc:
            raise ProjectVerifierResourceError("project_verifier_measurement_unavailable", measurement_status="measurement_unavailable") from exc
        exceeded = {
            name: {"actual": actual, "limit": limit}
            for name, actual, limit in (
                ("memory_bytes", rss, self.budget.memory_bytes),
                ("process_slots", count, self.process_slots),
                ("temp_bytes", disk_bytes, self.budget.temp_bytes),
            ) if actual > limit
        }
        for name, code in (
            ("memory_bytes", "project_verifier_memory_exceeded"),
            ("process_slots", "project_verifier_process_slots_exceeded"),
            ("temp_bytes", "project_verifier_temp_exceeded"),
        ):
            if name in exceeded:
                raise ProjectVerifierResourceError(code, exceeded=exceeded, measurement_status="measured")


def _copy_verifier_input(source, target, guard):
    """复制块之间检查预算，准备失败不会进入 Worker 或发布阶段。"""
    try:
        with source.open("rb") as reader, target.open("wb") as writer:
            while True:
                guard.check(check_disk=False)
                block = reader.read(1024 * 1024)
                if not block:
                    break
                writer.write(block)
            writer.flush()
        guard.check()
    except OSError as exc:
        raise ProjectVerifierResourceError("project_verifier_copy_failed") from exc


def _parse_outcome(
    payload: object,
    manifest: ProjectVerifierBundleManifest,
    result_id: str,
) -> ProjectVerifierOutcome:
    base_expected = {
        "contract_version",
        "status",
        "result_id",
        "findings",
        "evidence_hashes",
    }
    if not isinstance(payload, Mapping):
        raise ResultContractError("项目 Verifier 输出 envelope 无效")
    version = payload.get("contract_version")
    if version == _PROJECT_VERIFIER_OUTPUT_V1:
        expected = base_expected
        matrix_evidence = None
    elif version == PROJECT_VERIFIER_OUTPUT_VERSION:
        expected = {*base_expected, "statistics_matrix_evidence"}
        matrix_evidence = (
            None
            if payload.get("statistics_matrix_evidence") is None
            else _statistics_matrix_evidence(
                payload.get("statistics_matrix_evidence")
            )
        )
    else:
        raise ResultContractError("项目 Verifier 输出版本不受支持")
    if set(payload) != expected:
        raise ResultContractError("项目 Verifier 输出 envelope 无效")
    if payload["result_id"] != result_id:
        raise ResultContractError("项目 Verifier 输出错绑 Result")
    findings = payload["findings"]
    evidence = payload["evidence_hashes"]
    if (
        not isinstance(findings, list)
        or any(not isinstance(item, str) or not item for item in findings)
        or not isinstance(evidence, Mapping)
    ):
        raise ResultContractError("项目 Verifier 输出事实无效")
    values = {
        "verifier_identity": manifest.identity(),
        "result_id": result_id,
        "status": str(payload["status"]),
        "findings": tuple(findings),
        "evidence_hashes": {str(key): str(value) for key, value in evidence.items()},
        "statistics_matrix_evidence": matrix_evidence,
        "contract_version": str(version),
    }
    hash_payload = {
        key: value
        for key, value in values.items()
        if not (
            version == _PROJECT_VERIFIER_OUTPUT_V1
            and key == "statistics_matrix_evidence"
        )
    }
    return ProjectVerifierOutcome(
        **values,
        outcome_hash=typed_canonical_hash(hash_payload),
    )


__all__ = [
    "PROJECT_VERIFIER_OUTPUT_VERSION",
    "ProjectVerifierOutcome",
    "execute_project_verifier",
]
