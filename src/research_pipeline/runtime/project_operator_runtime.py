"""已准入项目算子的受控 Worker 执行、检查点与缓存身份。"""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import MappingProxyType
from typing import Mapping

import psutil

from research_pipeline.extensions import (
    AdmittedProjectOperatorRegistry,
    ExtensionError,
    ProjectArtifactInput,
    ProjectOperatorContext,
    ProjectOperatorImplementationToken,
    verify_project_operator_bundle,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.operator_contracts import validate_parameters

from .contracts import ArtifactRef, ResourceBudget
from .errors import RuntimeIntegrityError, RuntimeWorkerError
from .identity import environment_fingerprint
from research_pipeline.data_plane.snapshots import _sha256


PROJECT_RUNTIME_IDENTITY_VERSION = "project-operator-runtime-identity-v1"
PROJECT_WORKER_TASK_VERSION = "project-operator-worker-task-v4"
_UNCOMMITTED_OUTPUT_CODES = frozenset({
    "project_commit_result_invalid",
    "project_output_count_invalid",
    "project_output_contract_mismatch",
    "project_output_file_closure_invalid",
    "project_output_path_duplicate",
    "project_output_commit_mismatch",
    "project_directory_commit_invalid",
})
_SUPERVISOR_VERIFIED_INPUT = object()


def _declared_request_ids(parameters: Mapping[str, object]) -> frozenset[str]:
    """只把显式 request_id 参数授权给项目异构表输入。"""

    values: set[str] = set()
    for name, raw in parameters.items():
        if name.endswith("_request_id") and isinstance(raw, str) and raw:
            values.add(raw)
        elif name.endswith("_request_ids") and isinstance(raw, (list, tuple)):
            values.update(item for item in raw if isinstance(item, str) and item)
    return frozenset(values)


@dataclass(frozen=True)
class ProjectRuntimeIdentity:
    project_id: str
    implementation_id: str
    bundle_hash: str
    operator_spec_hash: str
    source_tree_hash: str
    dependency_lock_hash: str
    adapter_hash: str
    requires_python: str
    runtime_environment_hash: str
    abi_version: str
    identity_hash: str
    contract_version: str = PROJECT_RUNTIME_IDENTITY_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != PROJECT_RUNTIME_IDENTITY_VERSION:
            raise RuntimeIntegrityError("项目 Runtime identity 版本无效")
        hash_fields = (
            self.bundle_hash,
            self.operator_spec_hash,
            self.source_tree_hash,
            self.dependency_lock_hash,
            self.adapter_hash,
            self.runtime_environment_hash,
            self.identity_hash,
        )
        if any(
            len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
            for value in hash_fields
        ):
            raise RuntimeIntegrityError("项目 Runtime identity hash 字段无效")
        payload = self.to_dict()
        payload.pop("identity_hash")
        if self.identity_hash != typed_canonical_hash(payload):
            raise RuntimeIntegrityError("项目 Runtime identity 内容 hash 不一致")

    @classmethod
    def from_token(
        cls,
        token: ProjectOperatorImplementationToken,
    ) -> "ProjectRuntimeIdentity":
        manifest = token.manifest
        payload = {
            "project_id": manifest.project_id,
            "implementation_id": token.implementation_id,
            "bundle_hash": manifest.bundle_hash,
            "operator_spec_hash": manifest.operator_spec.spec_hash,
            "source_tree_hash": manifest.source_tree_hash,
            "dependency_lock_hash": manifest.dependency_lock_hash,
            "adapter_hash": project_worker_adapter_hash(),
            "requires_python": manifest.requires_python,
            "runtime_environment_hash": environment_fingerprint(),
            "abi_version": manifest.abi_version,
            "contract_version": PROJECT_RUNTIME_IDENTITY_VERSION,
        }
        return cls(**payload, identity_hash=typed_canonical_hash(payload))

    def to_dict(self) -> dict[str, str]:
        return {
            "project_id": self.project_id,
            "implementation_id": self.implementation_id,
            "bundle_hash": self.bundle_hash,
            "operator_spec_hash": self.operator_spec_hash,
            "source_tree_hash": self.source_tree_hash,
            "dependency_lock_hash": self.dependency_lock_hash,
            "adapter_hash": self.adapter_hash,
            "requires_python": self.requires_python,
            "runtime_environment_hash": self.runtime_environment_hash,
            "abi_version": self.abi_version,
            "identity_hash": self.identity_hash,
            "contract_version": self.contract_version,
        }


@dataclass(frozen=True)
class ProjectRuntimeInput:
    artifact: ArtifactRef
    content: bytes
    schema_hash: str
    source_root: Path | None = None
    files: Mapping[str, str] = field(default_factory=dict)
    request_tables: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    _verification_token: InitVar[object] = None

    def __post_init__(self, _verification_token: object) -> None:
        if self.request_tables:
            if (
                _verification_token is not _SUPERVISOR_VERIFIED_INPUT
                or self.source_root is None
                or self.files
                or self.content
            ):
                raise RuntimeIntegrityError(
                    "项目 request 数据输入必须来自 Supervisor 已验证描述"
                )
            object.__setattr__(
                self,
                "request_tables",
                MappingProxyType(dict(sorted(self.request_tables.items()))),
            )
            return
        if self.source_root is not None:
            if _verification_token is not _SUPERVISOR_VERIFIED_INPUT or not self.files or self.content:
                raise RuntimeIntegrityError("项目多文件输入必须来自 Supervisor 已验证描述")
            object.__setattr__(self, "files", MappingProxyType(dict(sorted(self.files.items()))))
        if (
            self.source_root is None
            and
            _verification_token is not _SUPERVISOR_VERIFIED_INPUT
            and hashlib.sha256(self.content).hexdigest() != self.artifact.content_hash
        ):
            raise RuntimeIntegrityError("项目算子输入内容 hash 不一致")
        if len(self.schema_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.schema_hash
        ):
            raise RuntimeIntegrityError("项目算子输入 schema hash 无效")

    @classmethod
    def from_verified(
        cls,
        artifact: ArtifactRef,
        content: bytes,
        schema_hash: str,
        *,
        source_root: Path | None = None,
        files: Mapping[str, str] | None = None,
        request_tables: Mapping[str, Mapping[str, object]] | None = None,
    ) -> "ProjectRuntimeInput":
        """只供 Supervisor 把已验证 Runtime 值转换为当前 Worker 输入。"""

        return cls(
            artifact,
            content,
            schema_hash,
            source_root,
            {} if files is None else files,
            {} if request_tables is None else request_tables,
            _verification_token=_SUPERVISOR_VERIFIED_INPUT,
        )


@dataclass(frozen=True)
class ProjectWorkerOutput:
    port: str
    artifact_type: str
    path: Path
    commit: Mapping[str, object]


@dataclass(frozen=True)
class ProjectWorkerState:
    path: Path
    commit: Mapping[str, object]


@dataclass(frozen=True)
class ProjectWorkerAttemptResult:
    """由正式 Runtime 外层管理状态时，单次受控 Worker 的最小结果。"""

    outputs: tuple[ProjectWorkerOutput, ...]
    runtime_identity: ProjectRuntimeIdentity
    state: ProjectWorkerState | None = None
    resource_measurement_status: str = "available"
    process_cleanup_status: str = "complete"
    causal_facts: Mapping[str, object] | None = None
    request_traces: Mapping[str, object] | None = None


def project_runtime_identity(
    registry: AdmittedProjectOperatorRegistry,
    operator_id: str,
    operator_version: str,
) -> ProjectRuntimeIdentity:
    token = registry.project_token(operator_id, operator_version)
    if not isinstance(token, ProjectOperatorImplementationToken):
        raise RuntimeIntegrityError("算子不是当前组合注册表准入的项目实现")
    identity = ProjectRuntimeIdentity.from_token(token)
    bundle_path = registry.bundle_paths[token.implementation_id]
    try:
        verified = verify_project_operator_bundle(bundle_path)
    except ExtensionError as exc:
        raise RuntimeIntegrityError("项目 bundle 与组合准入身份不一致") from exc
    if verified.bundle_hash != identity.bundle_hash:
        raise RuntimeIntegrityError("项目 bundle 与组合准入身份不一致")
    expected = registry.implementation_identities.get(token.implementation_id)
    expected_fields = {
        "project_id": identity.project_id,
        "bundle_hash": identity.bundle_hash,
        "operator_spec_hash": identity.operator_spec_hash,
        "source_tree_hash": identity.source_tree_hash,
        "dependency_lock_hash": identity.dependency_lock_hash,
        "permissions": token.manifest.permissions.to_dict(),
        "requires_python": identity.requires_python,
        "abi_version": identity.abi_version,
    }
    if expected is None or any(expected.get(key) != value for key, value in expected_fields.items()):
        raise RuntimeIntegrityError("项目 Runtime 身份与组合准入不一致")
    return identity


def project_worker_adapter_hash() -> str:
    """绑定实际 Worker adapter 与 Runtime bridge 源码，变化后旧状态不可复用。"""
    runtime_path = Path(__file__).resolve(strict=True)
    worker_path = runtime_path.with_name("project_operator_worker.py")
    return typed_canonical_hash({
        "contract_version": "project-operator-worker-adapter-code-v1",
        "runtime_bridge_sha256": hashlib.sha256(runtime_path.read_bytes()).hexdigest(),
        "worker_adapter_sha256": hashlib.sha256(worker_path.read_bytes()).hexdigest(),
        "helpers": {
            name: hashlib.sha256(runtime_path.with_name(name).read_bytes()).hexdigest()
            for name in ("project_table_input.py", "project_output.py", "project_causal.py",
                         "project_causal_worker.py", "project_causal_execution.py",
                         "project_causal_minute.py")
        },
        "causal_contract": hashlib.sha256(
            runtime_path.parent.parent.joinpath("platform", "project_causal_contract.py").read_bytes()
        ).hexdigest(),
        "causal_time_contract": hashlib.sha256(
            runtime_path.parent.parent.joinpath("platform", "causal_time.py").read_bytes()
        ).hexdigest(),
    })


def execute_project_worker_attempt(
    *,
    registry: AdmittedProjectOperatorRegistry,
    implementation_id: str,
    node_id: str,
    run_id: str,
    attempt_id: str,
    attempt_root: str | Path,
    inputs: tuple[ProjectRuntimeInput, ...],
    parameters: Mapping[str, object],
    fixed_clock: str,
    root_seed: int,
    budget: ResourceBudget,
    partition_key: str | None = None,
    dataset_roots: Mapping[str, str | Path] | None = None,
    verified_partition_id: str | None = None,
    state_in: ProjectWorkerState | None = None,
    causal_context: Mapping[str, object] | None = None,
) -> ProjectWorkerAttemptResult:
    """只执行一次受控 Worker；checkpoint、重试和缓存统一由正式 Runtime 管理。"""
    token = registry.project_token_by_implementation(implementation_id)
    if not isinstance(token, ProjectOperatorImplementationToken):
        raise RuntimeIntegrityError("项目实现未被当前组合注册表准入")
    spec = token.manifest.operator_spec
    normalized_parameters = validate_parameters(
        parameters,
        spec.parameters,
        "project.parameters",
    )
    identity = project_runtime_identity(
        registry,
        spec.operator_id,
        spec.operator_version,
    )
    bundle_path = registry.bundle_paths[implementation_id]
    clock = datetime.fromisoformat(fixed_clock)
    if (
        clock.tzinfo is None
        or clock.utcoffset() is None
        or type(root_seed) is not int
        or root_seed < 0
    ):
        raise RuntimeIntegrityError("项目 Runtime 时钟或 seed 无效")
    by_port = {item.artifact.name: item for item in inputs}
    expected_inputs = {item.port: item.artifact_type for item in spec.input_ports}
    if set(by_port) != set(expected_inputs) or any(
        by_port[port].artifact.artifact_type != artifact_type
        for port, artifact_type in expected_inputs.items()
    ):
        raise RuntimeIntegrityError("项目算子输入端口或 Artifact 不闭合")
    if partition_key is not None and dataset_roots and verified_partition_id is None:
        from research_pipeline.data_plane import (
            PartitionedDatasetRef,
            PartitionedDatasetResolver,
        )

        minute_inputs = tuple(
            item
            for item in by_port.values()
            if item.artifact.artifact_type == "data.minute-bars.v1"
        )
        if len(minute_inputs) != 1:
            raise RuntimeIntegrityError("项目分区执行要求恰好一个分钟数据输入")
        try:
            payload = json.loads(minute_inputs[0].content.decode("utf-8"))
            raw_dataset = (
                payload.get("partitioned_dataset")
                if isinstance(payload, Mapping)
                else None
            )
            if not isinstance(raw_dataset, Mapping):
                raise ValueError
            dataset = PartitionedDatasetRef.from_dict(raw_dataset)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise RuntimeIntegrityError("项目分钟输入 manifest 无法解析") from exc
        verified = PartitionedDatasetResolver(dataset_roots).resolve_partition(
            dataset,
            partition_key,
        )
        verified_partition_id = typed_canonical_hash(verified.reference.to_dict())
    result = _run_project_worker(
        attempt_root=Path(attempt_root).resolve(),
        bundle_path=bundle_path,
        identity=identity,
        token=token,
        node_id=node_id,
        run_id=run_id,
        attempt_id=attempt_id,
        inputs=tuple(by_port[key] for key in sorted(by_port)),
        parameters=normalized_parameters,
        fixed_clock=fixed_clock,
        root_seed=root_seed,
        budget=budget,
        partition_key=partition_key,
        dataset_roots=dataset_roots,
        verified_partition_id=verified_partition_id,
        state_in=state_in,
        causal_context=causal_context,
    )
    outputs, state, measurement_status, cleanup_status, causal_facts, request_traces = result
    return ProjectWorkerAttemptResult(
        outputs,
        identity,
        state,
        measurement_status,
        cleanup_status,
        causal_facts,
        request_traces,
    )


def _run_project_worker(
    *,
    attempt_root: Path,
    bundle_path: Path,
    identity: ProjectRuntimeIdentity,
    token: ProjectOperatorImplementationToken,
    node_id: str,
    run_id: str,
    attempt_id: str,
    inputs: tuple[ProjectRuntimeInput, ...],
    parameters: Mapping[str, object],
    fixed_clock: str,
    root_seed: int,
    budget: ResourceBudget,
    partition_key: str | None,
    dataset_roots: Mapping[str, str | Path] | None,
    verified_partition_id: str | None,
    state_in: ProjectWorkerState | None,
    causal_context: Mapping[str, object] | None,
) -> tuple[
    tuple[ProjectWorkerOutput, ...],
    ProjectWorkerState | None,
    str,
    str,
    Mapping[str, object] | None,
    Mapping[str, object] | None,
]:
    staged_bundle = attempt_root / identity.bundle_hash
    shutil.copytree(bundle_path, staged_bundle)
    if verify_project_operator_bundle(staged_bundle).bundle_hash != identity.bundle_hash:
        raise RuntimeIntegrityError("项目 bundle 暂存后身份不一致")
    input_contracts = []
    input_root = attempt_root / "inputs"
    invocation = {"run_id": run_id, "attempt_id": attempt_id}
    for item in inputs:
        if item.artifact.artifact_type in {"data.minute-bars.v1", "data.minute-bars.1m.v1"} and causal_context is not None:
            from .project_causal_minute import build_causal_minute_descriptor

            input_contracts.append(build_causal_minute_descriptor(
                item, invocation=invocation,
                partition_ids=causal_context["plan"]["work_items"][0]["source_partitions"][item.artifact.name],
            ))
        elif item.artifact.artifact_type == "data.minute-bars.v1" and item.source_root is None:
            if partition_key is None or not dataset_roots or verified_partition_id is None:
                raise RuntimeIntegrityError("分区项目输入缺少月份或批准数据根")
            try:
                payload = json.loads(item.content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeIntegrityError("分区项目输入无法解析") from exc
            dataset = payload.get("partitioned_dataset") if isinstance(payload, Mapping) else None
            if not isinstance(dataset, Mapping):
                raise RuntimeIntegrityError("分区项目输入缺少 dataset manifest")
            input_contracts.append({
                "input_kind": "partition",
                "port": item.artifact.name,
                "artifact_type": item.artifact.artifact_type,
                "artifact_key": item.artifact.artifact_key,
                "artifact_content_hash": item.artifact.content_hash,
                "schema_hash": item.schema_hash,
                "partition_key": partition_key,
                "invocation": invocation,
                "supervisor_verified_partition_id": verified_partition_id,
                "dataset": dict(dataset),
                "allowed_roots": {
                    str(role): str(Path(path).resolve(strict=True))
                    for role, path in sorted(dataset_roots.items())
                },
            })
        elif item.request_tables:
            declared_request_ids = _declared_request_ids(parameters)
            selected = {
                request_id: descriptor
                for request_id, descriptor in item.request_tables.items()
                if request_id in declared_request_ids
            }
            if not selected:
                raise RuntimeIntegrityError(
                    "项目异构数据输入没有绑定节点参数中的 request_id"
                )
            input_contracts.append({
                "input_kind": "request_tables",
                "invocation": invocation,
                "port": item.artifact.name,
                "artifact_type": item.artifact.artifact_type,
                "source_identity": item.artifact.artifact_key,
                "source_root": str(item.source_root.resolve(strict=True)),
                "requests": [
                    {
                        "request_id": request_id,
                        "source_identity": str(descriptor["source_identity"]),
                        "schema_hash": str(descriptor["schema_hash"]),
                        **({"admission": dict(descriptor["admission"])} if "admission" in descriptor else {}),
                        "files": [dict(file) for file in descriptor["files"]],
                    }
                    for request_id, descriptor in sorted(selected.items())
                ],
            })
        elif item.source_root is not None:
            input_contracts.append({
                "input_kind": "table",
                "invocation": invocation,
                "port": item.artifact.name,
                "artifact_type": item.artifact.artifact_type,
                "source_identity": item.artifact.artifact_key,
                "source_root": str(item.source_root.resolve(strict=True)),
                "files": [
                    {
                        "relative_path": path,
                        "byte_size": (item.source_root / path).stat().st_size,
                    }
                    for path in sorted(item.files)
                ],
            })
        else:
            target = input_root / item.artifact.name / "content.bin"
            target.parent.mkdir(parents=True)
            if item.source_root is None:
                target.write_bytes(item.content)
                content_hash = item.artifact.content_hash
            else:
                if len(item.files) != 1:
                    raise RuntimeIntegrityError("项目非表格输入必须为一个文件")
                relative_path, content_hash = next(iter(item.files.items()))
                shutil.copyfile(item.source_root / relative_path, target)
            input_contracts.append({
                "input_kind": "file",
                "invocation": invocation,
                "byte_size": target.stat().st_size,
                "artifact": ProjectArtifactInput(
                    item.artifact.name,
                    item.artifact.artifact_type,
                    target.relative_to(attempt_root).as_posix(),
                    content_hash,
                    item.schema_hash,
                ).to_dict(),
            })
    state_contract = None
    if state_in is not None:
        state_target = input_root / "runtime_state" / "content.bin"
        state_target.parent.mkdir(parents=True)
        shutil.copyfile(state_in.path, state_target)
        state_contract = ProjectArtifactInput(
            "runtime_state",
            "runtime.project-state",
            state_target.relative_to(attempt_root).as_posix(),
            str(state_in.commit["content_hash"]),
            str(state_in.commit["schema_hash"]),
        ).to_dict()
    context = ProjectOperatorContext(
        identity.project_id,
        run_id,
        node_id,
        attempt_id,
        fixed_clock,
        root_seed,
        parameters,
        budget.to_dict(),
    )
    output_ports = tuple(sorted(
        token.manifest.operator_spec.output_ports,
        key=lambda item: item.port,
    ))
    task = {
        "contract_version": PROJECT_WORKER_TASK_VERSION,
        "runtime_identity": identity.to_dict(),
        "bundle_relative_path": identity.bundle_hash,
        "context": context.to_dict(),
        "inputs": input_contracts,
        "state_input": state_contract,
        "outputs": [
            {"port": output.port, "artifact_type": output.artifact_type}
            for output in output_ports
        ],
    }
    task_path = attempt_root / "task.json"
    if causal_context is not None:
        task["causal_context"] = dict(causal_context)
    result_path = attempt_root / "result.json"
    task_path.write_text(canonical_json(task), encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "research_pipeline.runtime.project_operator_worker",
        "--attempt-root",
        str(attempt_root),
        "--task",
        str(task_path),
        "--result",
        str(result_path),
    ]
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except BaseException:
        task_path.unlink(missing_ok=True)
        raise
    started = time.monotonic()
    failure = None
    observed_descendants: dict[int, float] = {}
    measurement_status = "available"
    while process.poll() is None:
        elapsed = time.monotonic() - started
        try:
            current = psutil.Process(process.pid)
            children = current.children(recursive=True)
            for child in children:
                observed_descendants[child.pid] = child.create_time()
            rss = current.memory_info().rss + sum(child.memory_info().rss for child in children)
        except (psutil.Error, OSError, RuntimeError):
            rss = None
            measurement_status = "measurement_unavailable"
        size = sum(path.stat().st_size for path in attempt_root.rglob("*") if path.is_file())
        if elapsed > budget.wall_seconds:
            failure = "project_worker_timeout"
        elif (rss is not None and rss > budget.memory_bytes) or size > budget.temp_bytes:
            failure = "project_worker_resource_exceeded"
        if failure:
            break
        time.sleep(0.02)
    cleanup_status = _terminate_process_tree(
        process,
        observed_descendants=observed_descendants,
    )
    task_path.unlink(missing_ok=True)
    if failure is not None:
        raise RuntimeWorkerError(
            failure,
            error_code=(
                "heartbeat_timeout"
                if failure == "project_worker_timeout"
                else failure
            ),
            failure_payload={
                "resource_measurement_status": measurement_status,
                "process_cleanup_status": cleanup_status,
            },
        )
    if process.returncode != 0 or not result_path.is_file():
        if result_path.is_file():
            try:
                failed = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                failed = {}
            error_code = failed.get("error_code")
            if isinstance(error_code, str) and error_code:
                raise RuntimeWorkerError(
                    error_code,
                    error_code=error_code,
                    failure_payload=_project_worker_failure_payload(error_code),
                )
        raise RuntimeWorkerError(
            "project_worker_failed",
            error_code="worker_crash",
        )
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if result.get("status") != "succeeded" or result.get("runtime_identity_hash") != identity.identity_hash:
        error_code = str(result.get("error_code", "project_worker_result_invalid"))
        raise RuntimeWorkerError(error_code, error_code=error_code)
    commits = result.get("commits")
    if not isinstance(commits, list) or any(not isinstance(item, Mapping) for item in commits):
        raise RuntimeWorkerError(
            "project_worker_commit_invalid",
            error_code="project_worker_commit_invalid",
        )
    expected_outputs = {item.port: item.artifact_type for item in output_ports}
    actual_outputs = {
        str(item.get("port")): str(item.get("artifact_type")) for item in commits
    }
    if len(actual_outputs) != len(commits) or actual_outputs != expected_outputs:
        raise RuntimeWorkerError(
            "project_worker_commit_invalid",
            error_code="project_worker_commit_invalid",
        )
    outputs = []
    for commit in sorted(commits, key=lambda item: str(item.get("port"))):
        relative_path = commit.get("relative_path")
        if not isinstance(relative_path, str):
            raise RuntimeWorkerError(
                "project_worker_commit_invalid",
                error_code="project_worker_commit_invalid",
            )
        is_directory = "files" in commit
        path = _staged_commit_path(attempt_root, relative_path, directory=is_directory)
        if is_directory:
            entries = commit.get("files")
            if not isinstance(entries, list) or not entries:
                raise RuntimeWorkerError("project_directory_commit_invalid", error_code="project_directory_commit_invalid")
            declared = {}
            for item in entries:
                if not isinstance(item, Mapping) or set(item) != {"relative_path", "content_hash", "byte_size"}:
                    raise RuntimeWorkerError("project_directory_commit_invalid", error_code="project_directory_commit_invalid")
                file_path = (path / str(item["relative_path"])).resolve(strict=True)
                if not file_path.is_relative_to(path) or not file_path.is_file():
                    raise RuntimeWorkerError("project_directory_commit_invalid", error_code="project_directory_commit_invalid")
                declared[file_path.relative_to(path).as_posix()] = item
                if file_path.stat().st_size != item["byte_size"] or _sha256(file_path) != item["content_hash"]:
                    raise RuntimeWorkerError("project_worker_output_hash_mismatch", error_code="project_worker_output_hash_mismatch")
            if set(declared) != {file.relative_to(path).as_posix() for file in path.rglob("*") if file.is_file()} or len(declared) != len(entries):
                raise RuntimeWorkerError("project_directory_commit_invalid", error_code="project_directory_commit_invalid")
        elif path.stat().st_size != commit.get("byte_size") or _sha256(path) != commit.get("content_hash"):
            raise RuntimeWorkerError(
                "project_worker_output_hash_mismatch",
                error_code="project_worker_output_hash_mismatch",
            )
        outputs.append(ProjectWorkerOutput(
            port=str(commit["port"]),
            artifact_type=str(commit["artifact_type"]),
            path=path,
            commit=dict(commit),
        ))
    raw_state = result.get("state_commit")
    state = None
    if raw_state is not None:
        if not isinstance(raw_state, Mapping):
            raise RuntimeWorkerError(
                "project_worker_state_invalid",
                error_code="project_worker_state_invalid",
            )
        relative_path = raw_state.get("relative_path")
        if (
            raw_state.get("port") != "runtime_state"
            or raw_state.get("artifact_type") != "runtime.project-state"
            or not isinstance(relative_path, str)
        ):
            raise RuntimeWorkerError(
                "project_worker_state_invalid",
                error_code="project_worker_state_invalid",
            )
        path = _staged_commit_path(attempt_root, relative_path)
        if path.stat().st_size != raw_state.get("byte_size") or _sha256(path) != raw_state.get("content_hash"):
            raise RuntimeWorkerError(
                "project_worker_state_hash_mismatch",
                error_code="project_worker_state_hash_mismatch",
            )
        state = ProjectWorkerState(path, MappingProxyType(dict(raw_state)))
    causal_facts = result.get("causal_facts")
    if causal_context is not None and not isinstance(causal_facts, Mapping):
        raise RuntimeIntegrityError("项目因果输出缺少核心实际交付事实")
    request_traces = result.get("request_traces")
    if request_traces is not None and not isinstance(request_traces, Mapping):
        raise RuntimeIntegrityError("项目 request 消费轨迹无效")
    return (
        tuple(outputs), state, measurement_status, cleanup_status,
        causal_facts, request_traces,
    )


def _staged_commit_path(attempt_root: Path, relative_path: str, *, directory: bool = False) -> Path:
    root = (attempt_root / "outputs").resolve(strict=True)
    path = (root / relative_path).resolve(strict=True)
    if not path.is_relative_to(root) or not (path.is_dir() if directory else path.is_file()):
        raise RuntimeIntegrityError("项目输出提交路径越界")
    return path


def _project_worker_failure_payload(error_code: str) -> dict[str, object] | None:
    """把 Worker 的稳定错误码投影为 CLI 可直接消费的修复动作。"""

    if error_code not in _UNCOMMITTED_OUTPUT_CODES:
        return None
    return {
        "contract_version": "project-operator-repair-v1",
        "code": "project_output_not_committed",
        "worker_error_code": error_code,
        "missing_requirements": [
            "declared_typed_output_commit",
            "output_root_relative_path",
        ],
        "next_commands": [
            "python -m research_pipeline operator scaffold --help",
        ],
    }


def _terminate_process_tree(
    process: subprocess.Popen[bytes],
    *,
    observed_descendants: Mapping[int, float] | None = None,
) -> str:
    cleanup_status = "complete"
    try:
        descendants: dict[int, psutil.Process] = {}
        for pid, started_at in (observed_descendants or {}).items():
            try:
                item = psutil.Process(pid)
                if abs(item.create_time() - started_at) < 0.01:
                    descendants[pid] = item
            except psutil.Error:
                continue
        try:
            parent = psutil.Process(process.pid)
            for child in parent.children(recursive=True):
                descendants[child.pid] = child
        except psutil.NoSuchProcess:
            parent = None
        targets = list(descendants.values())
        if parent is not None:
            targets.append(parent)
        for item in targets:
            try:
                item.terminate()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(targets, timeout=1.0) if targets else ([], [])
        for item in alive:
            try:
                item.kill()
            except psutil.NoSuchProcess:
                pass
    except (psutil.Error, OSError, RuntimeError):
        cleanup_status = "direct_process_only"
        if process.poll() is None:
            process.kill()
    try:
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        process.kill()
        cleanup_status = "direct_process_only"
    return cleanup_status


__all__ = [
    "PROJECT_RUNTIME_IDENTITY_VERSION",
    "ProjectRuntimeIdentity",
    "ProjectRuntimeInput",
    "ProjectWorkerState",
    "execute_project_worker_attempt",
    "ProjectWorkerAttemptResult",
    "project_runtime_identity",
    "project_worker_adapter_hash",
]
