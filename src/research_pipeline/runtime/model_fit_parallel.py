"""模型拟合的 spawn 进程调度与节点内资源边界。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import importlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import time
import sys

import psutil


SPAWN_RUNTIME_BYTES = 512 * 1024**2
_THREAD_ENV = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
               "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS")


def _tracker_process_slots() -> int:
    if os.name == "nt":
        return 0
    from multiprocessing import resource_tracker
    tracker = resource_tracker._resource_tracker
    # spawn 子进程继承父 tracker 的描述符；只有创建者持有 tracker PID。
    return int(tracker._fd is None or tracker._pid is not None)


def fit_parallel_capacity(*, requested: int, cpu_slots: int, thread_count: int,
                          task_count: int, max_memory_bytes: int,
                          parent_input_bytes: int, child_working_bytes: int,
                          process_slots: int, child_aux_process_slots: int = 0) -> dict[str, object]:
    """输入驻留、单任务复制和解释器开销共同限制实际并发。"""
    for name, value in (("workers", requested), ("cpu_slots", cpu_slots),
                        ("thread_count", thread_count), ("task_count", task_count),
                        ("max_memory_bytes", max_memory_bytes), ("child_working_bytes", child_working_bytes),
                        ("process_slots", process_slots)):
        if type(value) is not int or value < 1:
            raise ValueError(f"模型并行 {name} 必须为正整数")
    if type(parent_input_bytes) is not int or parent_input_bytes < 0:
        raise ValueError("模型父进程输入字节无效")
    if type(child_aux_process_slots) is not int or child_aux_process_slots < 0:
        raise ValueError("模型子任务辅助进程槽必须为非负整数")
    child_bytes = SPAWN_RUNTIME_BYTES + child_working_bytes
    cpu_limit = cpu_slots // thread_count
    parent_rss = psutil.Process().memory_info().rss
    parent_reserved = max(parent_input_bytes, parent_rss)
    tracker_slots = _tracker_process_slots()
    worker_process_slots = 2 if os.name == "nt" and sys.prefix != sys.base_prefix else 1
    wrapper_slots = worker_process_slots
    child_tree_slots = worker_process_slots + child_aux_process_slots
    process_limit = (process_slots - wrapper_slots - tracker_slots) // child_tree_slots
    memory_limit = (max_memory_bytes - parent_reserved) // child_bytes
    actual = min(requested, cpu_limit, memory_limit, process_limit, task_count)
    if actual < 1:
        raise ValueError("模型 spawn 单任务连同父输入超过 CPU、内存或进程槽预算")
    return {
        "requested_workers": requested, "actual_workers": actual,
        "cpu_limit": cpu_limit, "memory_limit": memory_limit, "task_count": task_count,
        "parent_input_bytes": parent_input_bytes, "parent_rss_bytes": parent_rss,
        "parent_reserved_bytes": parent_reserved, "child_estimated_bytes": child_bytes,
        "process_slots": process_slots, "process_limit": process_limit,
        "worker_process_slots": worker_process_slots, "wrapper_slots": wrapper_slots,
        "tracker_slots": tracker_slots, "child_aux_process_slots": child_aux_process_slots,
        "child_tree_slots": child_tree_slots,
        "spawn_runtime_reserve_bytes": SPAWN_RUNTIME_BYTES,
        "planned_memory_bytes": parent_reserved + actual * child_bytes,
        "reduced_by": [name for name, limit in (("cpu", cpu_limit), ("memory", memory_limit),
                       ("process_slots", process_limit), ("tasks", task_count)) if limit < requested],
    }


def _forbid_databases() -> None:
    import sqlite3
    import duckdb

    def forbidden(*args, **kwargs):
        raise RuntimeError("模型拟合子进程禁止数据库连接或隐式数据库执行")

    sqlite3.connect = forbidden
    sqlite3.dbapi2.connect = forbidden
    for name in ("connect", "default_connection", "sql", "query", "execute", "executemany"):
        if hasattr(duckdb, name):
            setattr(duckdb, name, forbidden)


def _worker_main(connection, worker_ref: str) -> None:
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    for name in _THREAD_ENV:
        os.environ[name] = "1"
    sys.dont_write_bytecode = True
    try:
        _forbid_databases()
        module_name, symbol = worker_ref.split(":")
        worker = getattr(importlib.import_module(module_name), symbol)
        while True:
            task = connection.recv()
            if task is None:
                return
            try:
                result = worker(task)
                connection.send(("ok", result, os.getpid()))
            except BaseException as exc:
                connection.send(("error", exc, os.getpid()))
                return
    finally:
        connection.close()


def _tree(process) -> list[psutil.Process]:
    try:
        parent = process._fit_process_identity
        return [parent, *parent.children(recursive=True)] if parent.is_running() else []
    except psutil.NoSuchProcess:
        return []


def _observe_workers(workers, observed):
    for process, _ in workers:
        for child in _tree(process):
            try:
                observed[(child.pid, child.create_time())] = child
            except psutil.NoSuchProcess:
                pass
    # is_running 核对创建时间，不能对复用同一 PID 的其他进程发信号。
    for identity, child in tuple(observed.items()):
        if not child.is_running():
            del observed[identity]
    return list(observed.values())


def _stop_workers(workers, observed) -> None:
    trees = _observe_workers(workers, observed)
    for child in reversed(trees):
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(trees, timeout=2)
    for child in alive:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(alive, timeout=2)
    for process, connection in workers:
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
        connection.close()
        process.close()


def _directory_bytes(root: Path) -> int:
    total = 0
    if root.exists():
        for path in root.rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                continue
    return total


def run_fit_tasks(tasks: Sequence[Mapping[str, object]], *, worker_ref: str,
                  capacity: Mapping[str, object], max_memory_bytes: int,
                  max_temp_bytes: int, output_root: str | Path,
                  deadline_monotonic: float | None = None,
                  execution_report_path: str | Path | None = None) -> list[object]:
    """有界提交独立任务；本批完成项全部成功后补交，结果按任务序返回。"""
    if type(max_temp_bytes) is not int or max_temp_bytes < 0:
        raise ValueError("模型临时空间预算必须是非负整数")
    root = Path(output_root)
    report = {**capacity, "status": "running", "started_tasks": 0, "completed_tasks": 0,
              "worker_pids": [], "peak_child_rss_bytes": 0, "peak_temp_bytes": 0,
              "peak_inflight": 0, "peak_total_rss_bytes": 0,
              "peak_processes": 1, "start_method": "spawn"}
    workers, active, observed = [], {}, {}
    results = [None] * len(tasks)
    started = time.monotonic()
    context = mp.get_context("spawn")

    def require_resources():
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            raise TimeoutError("模型 candidate×fold 执行超过节点 deadline")
        rss = 0
        process_tree = []
        processes = int(capacity["wrapper_slots"]) + int(capacity["tracker_slots"])
        for child in _observe_workers(workers, observed):
            try:
                rss += child.memory_info().rss
                processes += 1
                process_tree.append({"pid": child.pid, "parent_pid": child.ppid(), "name": child.name()})
            except psutil.NoSuchProcess:
                continue
        report["peak_child_rss_bytes"] = max(report["peak_child_rss_bytes"], rss)
        parent_rss = psutil.Process().memory_info().rss
        total_rss = parent_rss + rss
        report["peak_total_rss_bytes"] = max(report["peak_total_rss_bytes"], total_rss)
        if processes > report["peak_processes"]:
            report["peak_processes"] = processes
            report["peak_worker_process_tree"] = process_tree
        if total_rss > max_memory_bytes:
            raise MemoryError("模型父进程与子进程总内存超过节点额度")
        if processes > int(capacity["process_slots"]):
            raise ValueError(f"模型 spawn 实测进程树 {processes} 超过节点进程槽 {capacity['process_slots']}")
        temp = _directory_bytes(root)
        report["peak_temp_bytes"] = max(report["peak_temp_bytes"], temp)
        if temp > max_temp_bytes:
            raise ValueError("模型拟合输出与临时文件超过节点 temp 预算")

    try:
        require_resources()
        for _ in range(int(capacity["actual_workers"])):
            require_resources()
            parent, child = context.Pipe()
            process = context.Process(target=_worker_main, args=(child, worker_ref), daemon=False)
            previous_env = {name: os.environ.get(name) for name in (*_THREAD_ENV, "PYTHONDONTWRITEBYTECODE")}
            try:
                # 独占模型 wrapper 只在启动时设置；spawn 重执行 main 前即继承单线程环境。
                for name in (*_THREAD_ENV, "PYTHONDONTWRITEBYTECODE"):
                    os.environ[name] = "1"
                process.start()
                process._fit_process_identity = psutil.Process(process.pid)
                process._fit_process_identity.create_time()
            except BaseException:
                parent.close()
                child.close()
                raise
            finally:
                for name, value in previous_env.items():
                    if value is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = value
            child.close()
            workers.append((process, parent))
            report["worker_pids"].append(process.pid)
        next_task = 0
        while next_task < len(tasks) or active:
            require_resources()
            for index, (process, connection) in enumerate(workers):
                if index not in active and next_task < len(tasks):
                    if not process.is_alive():
                        raise RuntimeError("模型 spawn worker 意外退出")
                    connection.send(dict(tasks[next_task]))
                    active[index] = next_task
                    next_task += 1
                    report["started_tasks"] += 1
            report["peak_inflight"] = max(report["peak_inflight"], len(active))
            completed = []
            while not completed:
                require_resources()
                for index in tuple(active):
                    process, connection = workers[index]
                    try:
                        ready = connection.poll()
                        message = connection.recv() if ready else None
                    except (EOFError, BrokenPipeError) as exc:
                        raise RuntimeError("模型 spawn worker 未返回任务结果") from exc
                    if message is not None:
                        status, value, _pid = message
                        if status == "error":
                            raise value
                        completed.append((index, value))
                    elif not process.is_alive():
                        raise RuntimeError("模型 spawn worker 意外退出")
                if not completed:
                    time.sleep(0.01)
            # 同批结果全部成功后才补交，不能由一个成功结果掩盖另一项失败。
            for index, value in completed:
                results[active.pop(index)] = value
                report["completed_tasks"] += 1
        for _, connection in workers:
            connection.send(None)
        for process, _ in workers:
            while process.is_alive():
                require_resources()
                process.join(timeout=0.02)
            if process.exitcode != 0:
                raise RuntimeError("模型 spawn worker 退出失败")
        require_resources()
        report["status"] = "succeeded"
        return results
    except BaseException as exc:
        report["status"] = "failed"
        report["error_type"] = type(exc).__name__
        raise
    finally:
        _stop_workers(workers, observed)
        report["wall_seconds"] = time.monotonic() - started
        if execution_report_path is not None:
            path = Path(execution_report_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True), encoding="utf-8")
