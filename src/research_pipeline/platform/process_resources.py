"""执行与验证共用的进程树测量、临时空间计量和进程清理。"""

from __future__ import annotations

from contextlib import contextmanager
import ctypes
import os
import sys
import threading
from pathlib import Path
import subprocess
from typing import Mapping

import psutil


def _project_process_usage(
    worker_pid: int | None, observed_descendants: dict[int, float],
    *, windows_job_name: str | None = None,
) -> tuple[int, int]:
    """租约覆盖 Supervisor、当前 Worker 和已经观测到的存活后代。"""
    supervisor = psutil.Process(os.getpid())
    processes = {os.getpid(): supervisor}
    worker = None
    if worker_pid is not None:
        try:
            worker = psutil.Process(worker_pid)
            processes[worker_pid] = worker
        except psutil.NoSuchProcess:
            pass
    job_pids = (_windows_job_process_ids(windows_job_name)
                if sys.platform == "win32" and windows_job_name else None)
    try:
        children = (worker.children(recursive=True) if worker is not None else ()) if job_pids is None else job_pids
    except psutil.NoSuchProcess:
        children = ()
    for child in children:
        try:
            if isinstance(child, int):
                child = psutil.Process(child)
            observed_descendants[child.pid] = child.create_time()
            processes[child.pid] = child
        except psutil.NoSuchProcess:
            continue
    for pid, started_at in observed_descendants.items():
        if pid in processes:
            continue
        try:
            child = psutil.Process(pid)
            if abs(child.create_time() - started_at) < 0.01:
                processes[pid] = child
        except psutil.NoSuchProcess:
            continue
    rss = count = 0
    for process in processes.values():
        try:
            rss += process.memory_info().rss
            count += 1
        except psutil.NoSuchProcess:
            continue
    return rss, count


_WINDOWS_WORKER_JOB_ENV = "QUANTWITNESS_WORKER_JOB"


def _windows_job_process_ids(name: str) -> tuple[int, ...] | None:
    """仅查询本会话Job；启动绑定前返回None，其他错误不能视为没有后代。"""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenJobObjectW.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_wchar_p)
    kernel32.OpenJobObjectW.restype = ctypes.c_void_p
    kernel32.QueryInformationJobObject.argtypes = (
        ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p)
    kernel32.QueryInformationJobObject.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
    kernel32.CloseHandle.restype = ctypes.c_int
    handle = kernel32.OpenJobObjectW(4, False, name)  # JOB_OBJECT_QUERY
    if not handle:
        error = ctypes.get_last_error()
        if error == 2:
            return None
        raise ctypes.WinError(error)
    try:
        capacity = 8
        while True:
            buffer = ctypes.create_string_buffer(8 + ctypes.sizeof(ctypes.c_size_t) * capacity)
            succeeded = kernel32.QueryInformationJobObject(handle, 3, buffer, ctypes.sizeof(buffer), None)
            assigned, returned = (ctypes.c_uint32 * 2).from_buffer(buffer)
            if succeeded and assigned == returned:
                return tuple((ctypes.c_size_t * returned).from_buffer(buffer, 8))
            if not succeeded and ctypes.get_last_error() != 234:
                raise ctypes.WinError(ctypes.get_last_error())
            capacity = max(capacity * 2, assigned)
    finally:
        # 不能持有查询句柄，否则Worker退出后会推迟KILL_ON_JOB_CLOSE清理。
        kernel32.CloseHandle(handle)



_attempt_samplers = threading.local()


def _register_attempt_sampler(sampler) -> None:
    if not hasattr(_attempt_samplers, "items"):
        _attempt_samplers.items = []
    _attempt_samplers.items.append(sampler)


def _unregister_attempt_sampler(sampler) -> None:
    items = getattr(_attempt_samplers, "items", [])
    if sampler in items:
        items.remove(sampler)


@contextmanager
def _project_sampling_scope(attempt_root: Path):
    """同线程、同 attempt 的监督循环接管采样，退出时恢复原后台采样。"""
    root = attempt_root.resolve()
    sampler = next((item for item in reversed(getattr(_attempt_samplers, "items", []))
                    if any(root.is_relative_to(scope) for scope in item.scratch_roots)), None)
    if sampler is None:
        yield None
        return
    with sampler._sampling_lock:
        try:
            yield sampler
        finally:
            sampler._sample()


def _nonoverlapping_roots(roots) -> tuple[Path, ...]:
    selected: list[Path] = []
    for root in sorted(set(Path(root).resolve() for root in roots), key=lambda path: len(path.parts)):
        if not any(root.is_relative_to(parent) for parent in selected):
            selected.append(root)
    return tuple(selected)


def _measure_scratch_scopes(roots) -> dict[Path, int]:
    """重叠根只遍历一次，复用目录枚举信息并按目录累计文件大小。"""
    scopes = dict.fromkeys((Path(root).resolve() for root in roots), 0)
    pending = list(_nonoverlapping_roots(scopes))
    while pending:
        directory = pending.pop()
        size = 0
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir():
                            if not entry.is_symlink():
                                pending.append(Path(entry.path))
                        else:
                            size += entry.stat().st_size
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            continue
        for scope in scopes:
            if directory.is_relative_to(scope):
                scopes[scope] += size
    return scopes


def _measure_attempt_tree_bytes(root: Path) -> int:
    """统计临时空间；Worker 正常删除临时路径时跳过已经消失的条目。"""
    root = root.resolve()
    return _measure_scratch_scopes((root,))[root]


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
        if alive:
            _, survivors = psutil.wait_procs(alive, timeout=2.0)
            if survivors:
                cleanup_status = "direct_process_only"
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


