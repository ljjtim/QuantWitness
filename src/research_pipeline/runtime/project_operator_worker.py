"""项目算子 Worker adapter；只由 Runtime Supervisor 启动。"""

from __future__ import annotations

import argparse
import ctypes
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import sys
from dataclasses import dataclass
from importlib.machinery import ModuleSpec
from types import ModuleType
from typing import Mapping

from research_pipeline.data_plane import (
    PathRolePolicy,
    PartitionedDatasetRef,
    VerifiedDatasetPartition,
    VerifiedMinutePartitionStream,
)
from research_pipeline.domain import load_session_policy_bundle
from research_pipeline.extensions import (
    ProjectArtifactCommit,
    ProjectArtifactInput,
    ProjectDirectoryCommit,
    ProjectOperatorContext,
    verify_project_operator_bundle,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash

from .project_operator_runtime import (
    PROJECT_RUNTIME_IDENTITY_VERSION,
    PROJECT_WORKER_TASK_VERSION,
    project_worker_adapter_hash,
)
from .project_output import ProjectOutputRoot
from .identity import environment_fingerprint
from research_pipeline.data_plane.snapshots import _sha256
from .project_table_input import ProjectRequestTableInput, ProjectTableInput


_WINDOWS_JOB_KILL_ON_CLOSE = 0x00002000
_WINDOWS_JOB_EXTENDED_LIMIT_INFORMATION = 9
_WINDOWS_JOB_HANDLE: int | None = None


class _WindowsJobBasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("per_process_user_time_limit", ctypes.c_longlong),
        ("per_job_user_time_limit", ctypes.c_longlong),
        ("limit_flags", ctypes.c_uint32),
        ("minimum_working_set_size", ctypes.c_size_t),
        ("maximum_working_set_size", ctypes.c_size_t),
        ("active_process_limit", ctypes.c_uint32),
        ("affinity", ctypes.c_size_t),
        ("priority_class", ctypes.c_uint32),
        ("scheduling_class", ctypes.c_uint32),
    ]


class _WindowsIoCounters(ctypes.Structure):
    _fields_ = [
        ("read_operation_count", ctypes.c_uint64),
        ("write_operation_count", ctypes.c_uint64),
        ("other_operation_count", ctypes.c_uint64),
        ("read_transfer_count", ctypes.c_uint64),
        ("write_transfer_count", ctypes.c_uint64),
        ("other_transfer_count", ctypes.c_uint64),
    ]


class _WindowsJobExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("basic_limit_information", _WindowsJobBasicLimitInformation),
        ("io_info", _WindowsIoCounters),
        ("process_memory_limit", ctypes.c_size_t),
        ("job_memory_limit", ctypes.c_size_t),
        ("peak_process_memory_used", ctypes.c_size_t),
        ("peak_job_memory_used", ctypes.c_size_t),
    ]


def _bind_windows_descendants_to_worker_lifetime() -> bool:
    """让极短命 Worker 的后台后代也随 Worker 退出，不依赖轮询恰好看见。"""

    global _WINDOWS_JOB_HANDLE
    if sys.platform != "win32":
        return False
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p)
    kernel32.CreateJobObjectW.restype = ctypes.c_void_p
    kernel32.SetInformationJobObject.argtypes = (
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    )
    kernel32.SetInformationJobObject.restype = ctypes.c_int
    kernel32.AssignProcessToJobObject.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
    kernel32.AssignProcessToJobObject.restype = ctypes.c_int
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
    kernel32.CloseHandle.restype = ctypes.c_int

    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        return False
    limits = _WindowsJobExtendedLimitInformation()
    limits.basic_limit_information.limit_flags = _WINDOWS_JOB_KILL_ON_CLOSE
    configured = kernel32.SetInformationJobObject(
        handle,
        _WINDOWS_JOB_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    )
    assigned = configured and kernel32.AssignProcessToJobObject(
        handle,
        kernel32.GetCurrentProcess(),
    )
    if not assigned:
        kernel32.CloseHandle(handle)
        return False
    # 不在 Python 中主动关闭：Worker 正常或异常退出时，操作系统关闭最后一个
    # Job handle，并终止仍存活的后代；监督器的 psutil 清理继续作为跨平台兜底。
    _WINDOWS_JOB_HANDLE = int(handle)
    return True


@dataclass(frozen=True)
class ProjectPartitionInput:
    """项目代码一次只能拉取当前分区的批次。"""

    port: str
    artifact_type: str
    partition_key: str
    dataset_id: str
    source_identity: str
    _stream: VerifiedMinutePartitionStream

    def iter_batches(
        self,
        *,
        columns: tuple[str, ...],
        batch_size: int = 65_536,
    ):
        return self._stream.iter_batches(columns=columns, batch_size=batch_size)

    def assert_complete(self) -> None:
        self._stream.assert_complete()


def _load_project_entry_module(
    *,
    source_root: Path,
    entry_module: str,
    synthetic_root: str,
) -> ModuleType:
    """在唯一模块命名空间中按 Python 包语义加载项目入口。"""
    root_package = ModuleType(synthetic_root)
    root_package.__package__ = synthetic_root
    root_package.__path__ = [str(source_root)]
    root_package.__spec__ = ModuleSpec(synthetic_root, loader=None, is_package=True)
    sys.modules[synthetic_root] = root_package

    entry_parts = entry_module.split(".")
    parent_name = synthetic_root
    for depth in range(1, len(entry_parts)):
        package_name = ".".join((synthetic_root, *entry_parts[:depth]))
        package_path = source_root.joinpath(*entry_parts[:depth])
        init_path = package_path / "__init__.py"
        if init_path.is_file():
            package_spec = importlib.util.spec_from_file_location(
                package_name,
                init_path,
                submodule_search_locations=[str(package_path)],
            )
            if package_spec is None or package_spec.loader is None:
                raise ValueError("project_entry_module_invalid")
            package = importlib.util.module_from_spec(package_spec)
            sys.modules[package_name] = package
            package_spec.loader.exec_module(package)
        else:
            package = ModuleType(package_name)
            package.__package__ = package_name
            package.__path__ = [str(package_path)]
            package.__spec__ = ModuleSpec(package_name, loader=None, is_package=True)
            sys.modules[package_name] = package
        setattr(sys.modules[parent_name], entry_parts[depth - 1], package)
        parent_name = package_name

    entry_path = source_root / Path(*entry_parts).with_suffix(".py")
    synthetic_module_name = ".".join((synthetic_root, *entry_parts))
    entry_spec = importlib.util.spec_from_file_location(
        synthetic_module_name,
        entry_path,
    )
    if entry_spec is None or entry_spec.loader is None:
        raise ValueError("project_entry_module_invalid")
    module = importlib.util.module_from_spec(entry_spec)
    sys.modules[synthetic_module_name] = module
    entry_spec.loader.exec_module(module)
    setattr(sys.modules[parent_name], entry_parts[-1], module)
    return module


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt-root", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--result", required=True)
    args = parser.parse_args(argv)
    windows_job_bound = _bind_windows_descendants_to_worker_lifetime()
    attempt_root = Path(args.attempt_root).resolve(strict=True)
    task_path = Path(args.task).resolve(strict=True)
    result_path = Path(args.result).resolve()
    _require_within(task_path, attempt_root, "task")
    _require_within(result_path, attempt_root, "result")
    # 独立结果句柄让 Worker adapter 报告稳定终态。
    result_handle = result_path.open("w", encoding="utf-8")
    try:
        if sys.platform == "win32" and not windows_job_bound:
            raise ValueError("project_worker_job_binding_failed")
        task = json.loads(task_path.read_text(encoding="utf-8"))
        identity, manifest, context, inputs, output_contracts = _validate_task(
            task,
            attempt_root=attempt_root,
        )
        _validate_installed_dependencies(manifest.dependency_lock)
        output_root = ProjectOutputRoot(attempt_root / "outputs")
        output_root.mkdir()
        if "causal_context" in task:
            from .project_causal_worker import run_causal_entry
        sys.dont_write_bytecode = True
        source_root = attempt_root / str(task["bundle_relative_path"]) / "sources"
        sys.path.insert(0, str(source_root))
        synthetic_root = f"_research_project_{identity['identity_hash'][:16]}"
        module = _load_project_entry_module(
            source_root=source_root,
            entry_module=manifest.entry_module,
            synthetic_root=synthetic_root,
        )
        entry = getattr(module, manifest.entry_function, None)
        if not callable(entry):
            raise ValueError("project_entry_missing")
        causal_facts = None
        if "causal_context" in task:
            raw_commits, causal_facts = run_causal_entry(
                entry, context, inputs, output_root, task["causal_context"]
            )
        else:
            if "causal_plan" in context.parameters:
                raise ValueError("project_causal_context_missing")
            raw_commits = entry(context, inputs, output_root)
        for item in inputs:
            if "causal_context" not in task and isinstance(
                item,
                (ProjectPartitionInput, ProjectRequestTableInput, ProjectTableInput),
            ):
                item.assert_complete()
        commits, state_commit = _normalize_result(raw_commits)
        if len(commits) != len(output_contracts):
            raise ValueError("project_output_count_invalid")
        expected_outputs = {
            str(item["port"]): str(item["artifact_type"])
            for item in output_contracts
        }
        actual_outputs = {item.port: item.artifact_type for item in commits}
        if len(actual_outputs) != len(commits) or actual_outputs != expected_outputs:
            raise ValueError("project_output_contract_mismatch")
        all_commits = commits if state_commit is None else (*commits, state_commit)
        _verify_output_closure(output_root, all_commits)
        payload = {
            "status": "succeeded",
            "runtime_identity_hash": identity["identity_hash"],
            "commits": [commit.to_dict() for commit in sorted(commits, key=lambda item: item.port)],
            "state_commit": None if state_commit is None else state_commit.to_dict(),
        }
        request_traces = {
            item.port: item.consumption_trace()
            for item in inputs
            if isinstance(item, ProjectRequestTableInput)
        }
        if request_traces:
            payload["request_traces"] = request_traces
        if causal_facts is not None:
            payload["causal_facts"] = causal_facts
        result_handle.write(canonical_json(payload))
        result_handle.flush()
        os.fsync(result_handle.fileno())
        return 0
    except BaseException as exc:
        payload = {
            "status": "failed",
            "error_code": str(exc) if isinstance(exc, ValueError) else type(exc).__name__,
        }
        result_handle.seek(0)
        result_handle.truncate()
        result_handle.write(canonical_json(payload))
        result_handle.flush()
        return 1
    finally:
        result_handle.close()


def _validate_installed_dependencies(dependency_lock: Mapping[str, str]) -> None:
    """静态构建不依赖本机安装；仅 Worker 执行前核对声明版本。"""
    for import_name, expected_version in dependency_lock.items():
        try:
            actual_versions = (importlib.metadata.version(import_name),)
        except importlib.metadata.PackageNotFoundError:
            distributions = importlib.metadata.packages_distributions().get(import_name, ())
            if not distributions:
                raise ValueError("project_dependency_missing") from None
            try:
                actual_versions = tuple(importlib.metadata.version(name) for name in distributions)
            except importlib.metadata.PackageNotFoundError:
                raise ValueError("project_dependency_missing") from None
        if any(version != expected_version for version in actual_versions):
            raise ValueError("project_dependency_version_mismatch")


def _validate_task(
    task: object,
    *,
    attempt_root: Path,
) -> tuple[
    Mapping[str, object],
    object,
    ProjectOperatorContext,
    tuple[ProjectArtifactInput, ...],
    tuple[Mapping[str, str], ...],
]:
    expected = {
        "contract_version", "runtime_identity", "bundle_relative_path",
        "context", "inputs", "state_input", "outputs",
    }
    if not isinstance(task, Mapping) or set(task) - {"causal_context"} != expected:
        raise ValueError("project_worker_task_invalid")
    if task["contract_version"] != PROJECT_WORKER_TASK_VERSION:
        raise ValueError("project_worker_task_version_invalid")
    identity = task["runtime_identity"]
    if not isinstance(identity, Mapping):
        raise ValueError("project_runtime_identity_invalid")
    expected_identity = {
        "project_id", "implementation_id", "bundle_hash", "operator_spec_hash",
        "source_tree_hash", "dependency_lock_hash", "adapter_hash", "requires_python",
        "runtime_environment_hash",
        "abi_version", "identity_hash", "contract_version",
    }
    if set(identity) != expected_identity or identity["contract_version"] != PROJECT_RUNTIME_IDENTITY_VERSION:
        raise ValueError("project_runtime_identity_invalid")
    body = {key: value for key, value in identity.items() if key != "identity_hash"}
    if typed_canonical_hash(body) != identity["identity_hash"]:
        raise ValueError("project_runtime_identity_hash_invalid")
    if task["bundle_relative_path"] != identity["bundle_hash"]:
        raise ValueError("project_bundle_location_invalid")
    manifest = verify_project_operator_bundle(attempt_root / str(task["bundle_relative_path"]))
    checks = {
        "project_id": manifest.project_id,
        "bundle_hash": manifest.bundle_hash,
        "operator_spec_hash": manifest.operator_spec.spec_hash,
        "source_tree_hash": manifest.source_tree_hash,
        "dependency_lock_hash": manifest.dependency_lock_hash,
        "requires_python": manifest.requires_python,
        "abi_version": manifest.abi_version,
    }
    if any(identity[key] != value for key, value in checks.items()):
        raise ValueError("project_bundle_runtime_identity_mismatch")
    if identity["adapter_hash"] != project_worker_adapter_hash():
        raise ValueError("project_worker_adapter_drift")
    if identity["runtime_environment_hash"] != environment_fingerprint():
        raise ValueError("project_worker_environment_drift")
    if not isinstance(task["context"], Mapping):
        raise ValueError("project_context_invalid")
    context = ProjectOperatorContext.from_dict(task["context"])
    if context.project_id != manifest.project_id:
        raise ValueError("project_context_identity_mismatch")
    if not isinstance(task["inputs"], list) or any(
        not isinstance(item, Mapping) for item in task["inputs"]
    ):
        raise ValueError("project_inputs_invalid")
    inputs = tuple(
        _load_input(
            item,
            attempt_root=attempt_root,
            run_id=context.run_id,
            attempt_id=context.attempt_id,
        )
        for item in task["inputs"]
    )
    state_input = task["state_input"]
    if state_input is not None:
        if not isinstance(state_input, Mapping):
            raise ValueError("project_state_input_invalid")
        state = ProjectArtifactInput.from_dict(state_input)
        if state.port != "runtime_state" or state.artifact_type != "runtime.project-state":
            raise ValueError("project_state_input_invalid")
        _verify_state_input(state, attempt_root=attempt_root)
        inputs = (*inputs, state)
    outputs = task["outputs"]
    if (
        not isinstance(outputs, list)
        or not outputs
        or any(
            not isinstance(output, Mapping)
            or set(output) != {"port", "artifact_type"}
            or any(not isinstance(output[key], str) for key in output)
            for output in outputs
        )
    ):
        raise ValueError("project_output_contract_invalid")
    ports = [str(item["port"]) for item in outputs]
    if ports != sorted(ports) or len(ports) != len(set(ports)):
        raise ValueError("project_output_contract_invalid")
    return identity, manifest, context, inputs, tuple(dict(item) for item in outputs)


def _load_input(
    item: Mapping[str, object],
    *,
    attempt_root: Path,
    run_id: str,
    attempt_id: str,
) -> object:
    invocation = item.get("invocation")
    if (
        not isinstance(invocation, Mapping)
        or set(invocation) != {"run_id", "attempt_id"}
        or invocation.get("run_id") != run_id
        or invocation.get("attempt_id") != attempt_id
    ):
        raise ValueError("project_input_invocation_mismatch")
    kind = item.get("input_kind")
    if kind == "causal_partitions":
        from .project_causal_minute import load_causal_minute_input

        return load_causal_minute_input(item, load_partition=lambda part: _load_input(
            part, attempt_root=attempt_root, run_id=run_id, attempt_id=attempt_id
        ))
    if kind == "table":
        if (
            set(item) != {"input_kind", "invocation", "port", "artifact_type", "source_identity", "source_root", "files"}
            or not isinstance(item["files"], list)
            or any(
                not isinstance(file, Mapping)
                or set(file) != {"relative_path", "byte_size"}
                or not isinstance(file["relative_path"], str)
                or type(file["byte_size"]) is not int
                or file["byte_size"] < 0
                for file in item["files"]
            )
        ):
            raise ValueError("project_table_input_invalid")
        return ProjectTableInput(
            port=str(item["port"]), artifact_type=str(item["artifact_type"]),
            source_identity=str(item["source_identity"]),
            source_root=Path(str(item["source_root"])).resolve(strict=True),
            files=tuple(item["files"]),
            homogeneous_parquet=False,
        )
    if kind == "request_tables":
        expected_request_input = {
            "input_kind", "invocation", "port", "artifact_type",
            "source_identity", "source_root", "requests",
        }
        requests = item.get("requests")
        if (
            set(item) != expected_request_input
            or not isinstance(requests, list)
            or not requests
        ):
            raise ValueError("project_request_table_input_invalid")
        normalized: dict[str, Mapping[str, object]] = {}
        for request in requests:
            if (
                not isinstance(request, Mapping)
                or set(request) not in (
                    {"request_id", "source_identity", "schema_hash", "files"},
                    {"request_id", "source_identity", "schema_hash", "admission", "files"},
                )
                or not isinstance(request["request_id"], str)
                or not request["request_id"]
                or not isinstance(request["source_identity"], str)
                or not isinstance(request["schema_hash"], str)
                or not isinstance(request["files"], list)
                or ("admission" in request and not isinstance(request["admission"], Mapping))
                or not request["files"]
                or any(
                    not isinstance(file, Mapping)
                    or set(file) != {"relative_path", "byte_size"}
                    or not isinstance(file["relative_path"], str)
                    or type(file["byte_size"]) is not int
                    or file["byte_size"] < 0
                    for file in request["files"]
                )
            ):
                raise ValueError("project_request_table_input_invalid")
            request_id = request["request_id"]
            if request_id in normalized:
                raise ValueError("project_request_table_input_invalid")
            normalized[request_id] = {
                "source_identity": request["source_identity"],
                "schema_hash": request["schema_hash"],
                **({"admission": dict(request["admission"])} if "admission" in request else {}),
                "files": tuple(dict(file) for file in request["files"]),
            }
        if tuple(normalized) != tuple(sorted(normalized)):
            raise ValueError("project_request_table_input_invalid")
        return ProjectRequestTableInput(
            port=str(item["port"]),
            artifact_type=str(item["artifact_type"]),
            source_identity=str(item["source_identity"]),
            source_root=Path(str(item["source_root"])).resolve(strict=True),
            requests=normalized,
        )
    if kind == "file":
        if (
            set(item) != {"input_kind", "invocation", "byte_size", "artifact"}
            or not isinstance(item["artifact"], Mapping)
            or type(item["byte_size"]) is not int
            or item["byte_size"] < 0
        ):
            raise ValueError("project_file_input_invalid")
        artifact = ProjectArtifactInput.from_dict(item["artifact"])
        _verify_file_input(
            artifact,
            attempt_root=attempt_root,
            expected_size=item["byte_size"],
        )
        return artifact
    if kind != "partition":
        raise ValueError("project_input_kind_invalid")
    expected = {
        "input_kind",
        "port",
        "artifact_type",
        "artifact_key",
        "artifact_content_hash",
        "schema_hash",
        "partition_key",
        "invocation",
        "supervisor_verified_partition_id",
        "dataset",
        "allowed_roots",
    }
    if (
        set(item) != expected
        or not isinstance(item["dataset"], Mapping)
        or not isinstance(item["allowed_roots"], Mapping)
    ):
        raise ValueError("project_partition_input_invalid")
    for field in (
        "port",
        "artifact_type",
        "artifact_key",
        "artifact_content_hash",
        "schema_hash",
        "partition_key",
        "supervisor_verified_partition_id",
    ):
        if not isinstance(item[field], str) or not item[field]:
            raise ValueError("project_partition_input_invalid")
    dataset = PartitionedDatasetRef.from_dict(item["dataset"])
    policy = PathRolePolicy()
    roots = {
        str(role): policy.resolve_root(path, role=str(role))
        for role, path in item["allowed_roots"].items()
    }
    matching = tuple(
        partition
        for partition in dataset.partitions
        if partition.partition_key == str(item["partition_key"])
    )
    if len(matching) != 1:
        raise ValueError("project_partition_input_invalid")
    expected = matching[0]
    if typed_canonical_hash(expected.to_dict()) != item["supervisor_verified_partition_id"]:
        raise ValueError("project_partition_supervisor_binding_invalid")
    root = roots.get(expected.root_role)
    if root is None:
        raise ValueError("project_partition_root_role_invalid")
    path = policy.resolve_contained_path(
        allowed_root=root,
        candidate=expected.relative_path,
        root_role=expected.root_role,
        path_role="project_partition_input",
        expected_kind="file",
    )
    partition = VerifiedDatasetPartition(
        reference=expected,
        path=path,
        timestamp_field=dataset.timestamp_field,
        instrument_field=dataset.instrument_field,
        instruments=dataset.instruments,
        universe_snapshot_id=dataset.universe_snapshot_id,
        allowed_columns=dataset.allowed_columns,
        max_batch_rows=65_536,
        max_batch_bytes=32 * 1024 * 1024,
        verifier=lambda: None,
    )
    lineage = dict(dataset.lineage)
    asset_class = lineage.get("minute_asset_class")
    interval_minutes = lineage.get("interval_minutes")
    session_policy_ref = lineage.get("minute_session_policy_ref")
    if (
        not isinstance(asset_class, str)
        or type(interval_minutes) is not int
        or not isinstance(session_policy_ref, str)
    ):
        raise ValueError("project_partition_minute_lineage_invalid")
    bundle = _session_bundle_for_partition(asset_class, lineage)
    instruments = ()
    if bundle is not None:
        by_id = {
            policy.instrument.instrument_id: policy.instrument
            for policy in bundle.policies
            if policy.instrument.instrument_id in dataset.instruments
        }
        if set(by_id) != set(dataset.instruments):
            raise ValueError("project_partition_session_scope_invalid")
        instruments = tuple(by_id[key] for key in sorted(by_id))
    stream = VerifiedMinutePartitionStream(
        partition,
        asset_class=asset_class,
        interval_minutes=interval_minutes,
        session_policy_ref=session_policy_ref,
        session_bundle=bundle,
        session_instruments=instruments,
    )
    return ProjectPartitionInput(
        port=str(item["port"]),
        artifact_type=str(item["artifact_type"]),
        partition_key=str(item["partition_key"]),
        dataset_id=dataset.dataset_id,
        source_identity=dataset.reference_id,
        _stream=stream,
    )


def _session_bundle_for_partition(asset_class: str, lineage: Mapping[str, object]):
    if asset_class != "cn_future":
        return None
    bundle = load_session_policy_bundle()
    if lineage.get("minute_session_bundle_hash") != bundle.bundle_hash:
        raise ValueError("project_partition_session_bundle_mismatch")
    return bundle


def _verify_file_input(
    item: ProjectArtifactInput,
    *,
    attempt_root: Path,
    expected_size: int,
) -> None:
    path = attempt_root / item.relative_path
    _require_within(path.resolve(strict=True), attempt_root / "inputs", "input")
    if path.stat().st_size != expected_size:
        raise ValueError("project_input_size_mismatch")


def _verify_state_input(item: ProjectArtifactInput, *, attempt_root: Path) -> None:
    path = attempt_root / item.relative_path
    _require_within(path.resolve(strict=True), attempt_root / "inputs", "state input")
    if _sha256(path) != item.content_hash:
        raise ValueError("project_state_input_hash_mismatch")


def _normalize_result(
    value: object,
) -> tuple[tuple[ProjectArtifactCommit | ProjectDirectoryCommit, ...], ProjectArtifactCommit | None]:
    if isinstance(value, Mapping) and set(value) == {"outputs", "state"}:
        commits = _normalize_commits(value["outputs"])
        raw_state = value["state"]
        if not isinstance(raw_state, Mapping):
            raise ValueError("project_state_commit_invalid")
        state = ProjectArtifactCommit.from_dict(raw_state)
        if state.port != "runtime_state" or state.artifact_type != "runtime.project-state":
            raise ValueError("project_state_commit_invalid")
        return commits, state
    return _normalize_commits(value), None


def _normalize_commits(value: object) -> tuple[ProjectArtifactCommit | ProjectDirectoryCommit, ...]:
    if isinstance(value, Mapping):
        raw = (value,)
    elif isinstance(value, (list, tuple)):
        raw = tuple(value)
    else:
        raise ValueError("project_commit_result_invalid")
    if any(not isinstance(item, Mapping) for item in raw):
        raise ValueError("project_commit_result_invalid")
    return tuple(
        ProjectDirectoryCommit.from_dict(item)
        if "files" in item else ProjectArtifactCommit.from_dict(item)
        for item in raw
    )


def _verify_output_closure(
    output_root: Path,
    commits: tuple[ProjectArtifactCommit | ProjectDirectoryCommit, ...],
) -> None:
    files = {
        path.relative_to(output_root).as_posix(): path
        for path in output_root.rglob("*")
        if path.is_file()
    }
    declared: dict[str, tuple[str, int]] = {}
    for commit in commits:
        entries = (
            (
                f"{commit.relative_path}/{item['relative_path']}",
                str(item["content_hash"]),
                int(item["byte_size"]),
            )
            for item in commit.files
        ) if isinstance(commit, ProjectDirectoryCommit) else (
            (commit.relative_path, commit.content_hash, commit.byte_size),
        )
        for relative_path, content_hash, byte_size in entries:
            if relative_path in declared:
                raise ValueError("project_output_path_duplicate")
            declared[relative_path] = (content_hash, byte_size)
    if set(files) != set(declared):
        raise ValueError("project_output_file_closure_invalid")
    for relative_path, (content_hash, byte_size) in declared.items():
        path = files[relative_path]
        _require_within(path, output_root, "output")
        if (
            path.stat().st_size != byte_size
            or _sha256(path) != content_hash
        ):
            raise ValueError("project_output_commit_mismatch")


def _require_within(path: Path, root: Path, field: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"{field}_path_escape") from exc


if __name__ == "__main__":
    raise SystemExit(main())
