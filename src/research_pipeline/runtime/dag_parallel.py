"""主 DAG 的 spawn 计算与父进程调度；发布和事件始终留在 Supervisor。"""
from __future__ import annotations

from contextvars import copy_context
from dataclasses import dataclass, replace
import importlib
import io
import multiprocessing
import os
import pickle
import sys
import time
from types import MappingProxyType

import psutil

from .errors import RuntimeAdmissionError, RuntimeIntegrityError, RuntimeWorkerError
from .operator_runtime import OperatorRuntimeContext
from .resource_governor import ResourceUsageSampler
from .scheduler import ExecutionMode, ReadyCandidate


def spawn_base_process_slots() -> int:
    """当前解释器一次 spawn 的 launcher 与计算进程总槽。"""
    return 2 if os.name == "nt" and sys.prefix != sys.base_prefix else 1


def supervisor_process_slots() -> int:
    """POSIX spawn 共用一个 resource_tracker，Windows 只有 Supervisor。"""
    return 1 if os.name == "nt" else 2


def supervisor_memory_reserve() -> int:
    """读取 Supervisor 与 POSIX 共享 tracker 的当前驻留内存。"""
    total = psutil.Process().memory_info().rss
    if os.name != "nt":
        from multiprocessing import resource_tracker
        resource_tracker.getfd()
        tracker_pid = resource_tracker._resource_tracker._pid
        if tracker_pid is not None:
            total += psutil.Process(tracker_pid).memory_info().rss
    return total


def _mapping(value):
    return dict, (dict(value),)


def encode_transport(value) -> bytes:
    """只用于当前进程树的消息；验证会话仍由自身拒绝序列化。"""
    stream = io.BytesIO()
    pickler = pickle.Pickler(stream, protocol=pickle.HIGHEST_PROTOCOL)
    pickler.dispatch_table = {MappingProxyType: _mapping}
    pickler.dump(value)
    return stream.getvalue()


def require_spawn_environment(environment) -> bytes:
    try:
        return encode_transport(environment)
    except Exception as exc:
        raise RuntimeAdmissionError("并行 environment 必须可序列化且不能包含验证会话或活动连接") from exc


class DagUsageSampler(ResourceUsageSampler):
    """只观察一个计算子进程及其后代，不采入其他节点。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process = None
        self.max_processes = 0

    def bind(self, pid):
        self.process = psutil.Process(pid)
        self._sample()

    def _sample(self):
        if self.process is not None:
            try:
                processes = [self.process, *self.process.children(recursive=True)]
                rss = 0
                count = 0
                for process in processes:
                    try:
                        rss += process.memory_info().rss
                        count += 1
                    except psutil.NoSuchProcess:
                        pass
                self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
                self.max_processes = max(self.max_processes, count)
            except psutil.NoSuchProcess:
                pass
            except (psutil.Error, OSError):
                self.measurement_status = "measurement_unavailable"
        try:
            size = sum(path.stat().st_size for root in self.scratch_roots
                       if root.exists() for path in root.rglob("*") if path.is_file())
            self.peak_scratch_bytes = max(self.peak_scratch_bytes, size)
        except FileNotFoundError:
            pass
        except OSError:
            self.measurement_status = "measurement_unavailable"


class _Channel:
    def __init__(self, pipe, run_id, attempt_id):
        self.pipe, self.run_id, self.attempt_id = pipe, run_id, attempt_id

    def request(self, operation, *args, **kwargs):
        self.pipe.send_bytes(encode_transport(("request", self.run_id, self.attempt_id,
                                               operation, args, kwargs)))
        ok, result = pickle.loads(self.pipe.recv_bytes())
        if not ok:
            raise RuntimeWorkerError(result[0], error_code=result[1])
        return result


class _ExternalProxy:
    def __init__(self, channel, root):
        self.channel, self.root = channel, root
        self.staging_root, self.objects_root = root / ".staging", root / "objects"

    def prepare(self):
        return self.channel.request("external.prepare")

    def commit(self, *args, **kwargs):
        return self.channel.request("external.commit", *args, **kwargs)

    def verify(self, *args, **kwargs):
        return self.channel.request("external.verify", *args, **kwargs)

    def verify_snapshot(self, *args, **kwargs):
        return self.channel.request("external.verify_snapshot", *args, **kwargs)


class _VerificationProxy:
    """随 run/attempt 通道销毁的消费描述，不转移 Supervisor 验证会话。"""

    def __init__(self, channel):
        self.channel = channel
        self.cache = {}

    def resolve(self, namespace, identity, verifier, *, descriptor=None):
        key = (namespace, identity)
        if key not in self.cache:
            if descriptor is None:
                raise RuntimeIntegrityError("spawn 验证缺少显式临时描述")
            self.cache[key] = self.channel.request("verify", namespace, identity, descriptor)
        return self.cache[key]

    def remember(self, namespace, identity, verified):
        if namespace != "parquet_snapshot":
            raise RuntimeIntegrityError(f"spawn 不支持发布验证命名空间: {namespace}")
        result = self.channel.request("verify", namespace, identity, {"path": identity})
        if result != verified:
            raise RuntimeIntegrityError("Supervisor 与计算进程的快照事实不一致")
        self.cache[(namespace, identity)] = result
        return result

    def __reduce__(self):
        raise TypeError("Worker 验证通道不能再次序列化")


def _compute(pipe, payload, cpu_ids):
    try:
        if cpu_ids:
            psutil.Process().cpu_affinity(cpu_ids)
        for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                     "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS"):
            os.environ[name] = str(max(1, len(cpu_ids)))
        args = pickle.loads(payload)
        module_name, symbol, context, environment, root, run_id, attempt_id = args
        channel = _Channel(pipe, run_id, attempt_id)
        context = replace(context, external_store=_ExternalProxy(channel, root))
        from research_pipeline.data_plane.verification_lifecycle import activate_artifact_verification
        # 父进程先绑定采样器，再放行真实计算。
        if pipe.recv_bytes() != b"start":
            raise RuntimeIntegrityError("spawn 启动握手无效")
        with activate_artifact_verification(_VerificationProxy(channel)):
            result = getattr(importlib.import_module(module_name), symbol)(
                OperatorRuntimeContext(context, pickle.loads(environment)))
        pipe.send_bytes(encode_transport(("result", result)))
    except BaseException as exc:
        try:
            pipe.send_bytes(encode_transport(("error", str(exc),
                                             getattr(exc, "error_code", "operator_execution_failed"),
                                             getattr(exc, "failure_payload", None))))
        except (OSError, EOFError):
            pass
    finally:
        pipe.close()


@dataclass
class SpawnCall:
    module: str
    symbol: str
    node_context: object
    environment: bytes
    run_id: str
    attempt_id: str
    external: object
    verification: object
    sampler: DagUsageSampler

    def start(self, cpu_ids=()):
        context = replace(self.node_context, external_store=None)
        payload = encode_transport((self.module, self.symbol, context, self.environment,
                                    self.external.root, self.run_id, self.attempt_id))
        multiprocessing_context = multiprocessing.get_context("spawn")
        parent, child = multiprocessing_context.Pipe()
        process = multiprocessing_context.Process(target=_compute, args=(child, payload, cpu_ids))
        running = _Running(self, process, parent, cpu_ids)
        try:
            process.start()
            child.close()
            self.sampler.bind(process.pid)
            parent.send_bytes(b"start")
            return running
        except BaseException:
            if process.pid is not None:
                running.stop()
            child.close()
            parent.close()
            raise

    def service(self, message):
        _, run_id, attempt_id, operation, args, kwargs = message
        if (run_id, attempt_id) != (self.run_id, self.attempt_id):
            raise RuntimeIntegrityError("spawn 请求不属于当前 run/attempt")
        if operation in {"external.prepare", "external.commit", "external.verify", "external.verify_snapshot"}:
            if operation == "external.commit":
                self.sampler._sample()
                if self.sampler.peak_scratch_bytes > self.node_context.effective_resource_budget.temp_bytes:
                    raise RuntimeWorkerError("节点 external 暂存超过 temp 预算", error_code="resource_exceeded")
            result = getattr(self.external, operation.split(".")[1])(*args, **kwargs)
            if operation == "external.prepare":
                self.sampler.scratch_roots += (result,)
            return result
        if operation != "verify":
            raise RuntimeIntegrityError("未知 spawn Supervisor 请求")
        namespace, identity, descriptor = args
        if namespace == "parquet_snapshot":
            from research_pipeline.data_plane.snapshots import _verify_parquet_snapshot_uncached
            def verify():
                return _verify_parquet_snapshot_uncached(descriptor["path"])
        elif namespace == "dataset_artifact":
            from research_pipeline.data_plane.dataset_artifacts import ArtifactResolver
            def verify():
                return ArtifactResolver(descriptor["root"])._resolve_reference(descriptor["reference"])
        elif namespace == "partitioned_dataset":
            from research_pipeline.data_plane.partitioned_artifacts import PartitionedDatasetResolver
            reference = descriptor["reference"]
            def verify():
                return PartitionedDatasetResolver(
                    {reference.root_role: descriptor["root"]}
                )._resolve_and_verify_uncached(reference, descriptor["root"])
        else:
            raise RuntimeIntegrityError("未知 spawn 验证描述")
        from research_pipeline.data_plane.verification_lifecycle import activate_artifact_verification
        with activate_artifact_verification(self.verification):
            return self.verification.resolve(namespace, identity, verify)


class _Running:
    def __init__(self, call, process, pipe, cpu_ids):
        self.call, self.process, self.pipe = call, process, pipe
        self.cpu_ids = tuple(cpu_ids)
        self.outcome = None
        self.descendants = {}

    def poll(self, context):
        try:
            root = psutil.Process(self.process.pid)
            for process in root.children(recursive=True):
                self.descendants[(process.pid, process.create_time())] = process
        except psutil.NoSuchProcess:
            pass
        sampler = self.call.sampler
        budget = self.call.node_context.effective_resource_budget
        sampler._sample()
        if (sampler.peak_rss_bytes > budget.memory_bytes
                or sampler.peak_scratch_bytes > budget.temp_bytes
                or sampler.max_processes > self.call.node_context.process_slots):
            self.stop()
            self.outcome = ("error", "节点超过内存、暂存或进程槽预算", "resource_exceeded", None)
        if self.outcome is None and self.pipe.poll():
            try:
                message = pickle.loads(self.pipe.recv_bytes())
            except EOFError:
                message = ("error", "spawn 计算进程未返回结果", "worker_process_failed", None)
            if message[0] == "request":
                try:
                    response = (True, context.run(self.call.service, message))
                except BaseException as exc:
                    response = (False, (str(exc), getattr(exc, "error_code", "operator_execution_failed")))
                self.pipe.send_bytes(encode_transport(response))
            else:
                self.outcome = message
        deadline = self.call.node_context.attempt_deadline_monotonic
        if deadline is not None and time.monotonic() > deadline and self.process.is_alive():
            self.stop()
            self.outcome = ("error", "节点超过 wall_seconds", "heartbeat_timeout", None)
        if self.process.is_alive():
            return None
        self.process.join()
        if self.outcome is None:
            if self.pipe.poll():
                return self.poll(context)
            self.outcome = ("error", "spawn 计算进程退出且没有结果", "worker_process_failed", None)
        if self.process.exitcode != 0 and self.outcome[0] == "result":
            self.outcome = (
                "error", f"spawn 计算进程返回结果后非零退出: {self.process.exitcode}",
                "worker_process_failed",
                {"exit_code": self.process.exitcode, "result_received": True},
            )
        self.pipe.close()
        if any(process.is_running() for process in self.descendants.values()):
            self.stop()
            self.process.close()
            return ("error", "spawn 计算进程退出后仍有子进程", "worker_process_failed", None)
        self.process.close()
        return self.outcome

    def stop(self):
        try:
            root = psutil.Process(self.process.pid)
            for process in root.children(recursive=True):
                self.descendants[(process.pid, process.create_time())] = process
        except psutil.NoSuchProcess:
            pass
        for process in reversed(tuple(self.descendants.values())):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(tuple(self.descendants.values()), timeout=2)
        if self.process.is_alive():
            self.process.terminate()
        self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join()


def drive_dag(*, dag, task, outputs, mode, workers, ledger, budgets,
              process_slots, process_capacity, exclusive, memory_capacity,
              supervisor_memory_bytes):
    """稳定 ready 顺序；首个失败后只收齐在途任务，不再派新节点。"""
    order = dag.topological_order()
    rank = {node_id: index for index, node_id in enumerate(order)}
    dependencies = {node_id: set() for node_id in order}
    for edge in dag.edges:
        dependencies[edge.target_node].add(edge.source_node)
    pending = set(order)
    active = {}
    failures = []
    batch = set()
    parent = psutil.Process()
    cpu_ids = parent.cpu_affinity() if hasattr(parent, "cpu_affinity") else []
    try:
        while pending or active:
            progressed = False
            supervisor_memory_bytes = supervisor_memory_reserve()
            if supervisor_memory_bytes + sum(budgets[key].memory_bytes for key in active) > memory_capacity:
                raise RuntimeWorkerError("Supervisor 与活动节点超过运行内存总容量", error_code="resource_exceeded")
            ready = [node_id for node_id in order if node_id in pending
                     and dependencies[node_id] <= outputs.keys()]
            may_submit = not failures and (mode != ExecutionMode.PARTITIONED_BATCH or not batch)
            if may_submit:
                for node_id in ready:
                    if len(active) >= workers:
                        break
                    if active and (exclusive(node_id) or any(exclusive(key) for key in active)):
                        break
                    if sum(process_slots[key] for key in active) + process_slots[node_id] > process_capacity:
                        continue
                    if (supervisor_memory_bytes + sum(budgets[key].memory_bytes for key in active)
                            + budgets[node_id].memory_bytes > memory_capacity):
                        continue
                    candidate = ReadyCandidate(rank[node_id], node_id, None, budgets[node_id])
                    if ledger is not None and not ledger.select((candidate,)):
                        continue
                    used_cpus = {cpu for _, _, running in active.values() for cpu in running.cpu_ids}
                    available = [cpu for cpu in cpu_ids if cpu not in used_cpus]
                    if cpu_ids and len(available) < budgets[node_id].cpu_slots:
                        continue
                    pending.remove(node_id)
                    generator = task(rank[node_id], node_id)
                    context = copy_context()
                    try:
                        call = context.run(next, generator)
                        try:
                            running = context.run(call.start, available[:budgets[node_id].cpu_slots])
                        except BaseException as exc:
                            context.run(generator.throw, exc)
                            raise
                        active[node_id] = (generator, context, running)
                        batch.add(node_id)
                        progressed = True
                    except StopIteration:
                        progressed = True
                    except BaseException as exc:
                        failures.append((rank[node_id], exc))
                        break
                    if exclusive(node_id):
                        break
            for node_id in sorted(tuple(active), key=rank.get):
                generator, context, running = active[node_id]
                try:
                    outcome = running.poll(context)
                    if outcome is None:
                        continue
                    progressed = True
                    del active[node_id]
                    batch.discard(node_id)
                    if outcome[0] == "result":
                        context.run(generator.send, outcome[1])
                    else:
                        context.run(generator.throw, RuntimeWorkerError(
                            outcome[1], error_code=outcome[2], failure_payload=outcome[3]))
                except StopIteration:
                    pass
                except BaseException as exc:
                    if node_id in active:
                        running.stop()
                        running.pipe.close()
                        del active[node_id]
                        batch.discard(node_id)
                        try:
                            context.run(generator.throw, exc)
                        except BaseException as failure:
                            exc = failure
                    failures.append((rank[node_id], exc))
            if failures and not active:
                raise min(failures, key=lambda item: item[0])[1]
            if pending and not active and not progressed:
                raise RuntimeAdmissionError("ready 节点连同 Supervisor 无法满足资源总容量")
            if active and not progressed:
                time.sleep(0.01)
    finally:
        # 驱动层中断也必须先回收计算树，再退出生成器的 lease/lifecycle 上下文。
        for node_id in sorted(tuple(active), key=rank.get):
            generator, context, running = active[node_id]
            try:
                running.stop()
            except (OSError, psutil.Error):
                # 继续释放其他活动节点；原中断仍向调用方传播。
                pass
            finally:
                running.pipe.close()
                try:
                    context.run(generator.throw, RuntimeWorkerError(
                        "DAG Supervisor 中断", error_code="worker_process_failed"))
                except BaseException:
                    pass
                finally:
                    context.run(generator.close)
