"""已准入分钟策略的LEAN节点执行、资源监督和正式工件交接。"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

from research_pipeline.domain.strategy_declaration import parse_strategy_declaration
from research_pipeline.platform.lean_runtime import validate_lean_runtime
from research_pipeline.platform.process_resources import (
    _WINDOWS_WORKER_JOB_ENV, _project_process_usage, _measure_attempt_tree_bytes,
    _project_sampling_scope, _terminate_process_tree,
)
from .errors import RuntimeIntegrityError, RuntimeWorkerError
from .operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs, RuntimeNodeValue, RuntimeCompletionMetadata
from .project_operator_runtime import project_runtime_identity


def _write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False), encoding="utf-8")


def _read_callback_events(path, offset, active):
    try:
        stream = path.open(encoding="utf-8")
    except FileNotFoundError:
        return offset, active
    with stream:
        stream.seek(offset)
        while True:
            line = stream.readline()
            if not line.endswith("\n"):
                return offset, active
            event = json.loads(line)
            if event["status"] == "started":
                if active is not None:
                    raise RuntimeIntegrityError("LEAN回调开始前未结束上次回调")
                active = event
            elif event["status"] == "finished":
                if active is None or event["event_seq"] != active["event_seq"]:
                    raise RuntimeIntegrityError("LEAN回调结束序号不一致")
                active = None
            else:
                raise RuntimeIntegrityError("LEAN回调状态不受支持")
            offset = stream.tell()


def _engine_config(runtime, root, run_id):
    installation = Path(runtime["installation_root"])
    template = installation / runtime["config_template_relative"]
    text = template.read_text(encoding="utf-8-sig")
    # LEAN配置允许行注释；字符串中的网址和转义保持原样。
    text = re.sub(r'"(?:\\.|[^"\\])*"|//[^\r\n]*|/\*.*?\*/',
        lambda match: "" if match.group(0).startswith(("//", "/*")) else match.group(0), text, flags=re.S)
    config = json.loads(text)
    config.update({"environment": "backtesting", "algorithm-type-name": "ResearchLeanAlgorithm",
        "algorithm-language": "Python", "algorithm-location": str(installation / runtime["algorithm_relative"]),
        "algorithm-id": run_id, "close-automatically": True,
        "data-folder": str(root / "metadata"), "results-destination-folder": str(root),
        "object-store-root": str(root / "storage"),
        "python-venv": str(installation / runtime["python_environment_relative"]),
        "parquet-input-manifest": str(root / "input.json")})
    config["environments"]["backtesting"]["data-feed-handler"] = "QuantWitness.LeanInput.ParquetDataFeed"
    _write_json(root / "config.json", config)


def _run_engine(root, runtime, node_context, started):
    budget = node_context.effective_resource_budget
    _write_json(root / "launch-request.json", {"runtime": runtime, "output_root": str(root), "cpu_slots": budget.cpu_slots})
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    job = "Local\\QuantWitness-lean-" + uuid.uuid4().hex if sys.platform == "win32" else None
    if job:
        environment[_WINDOWS_WORKER_JOB_ENV] = job
    observed = {}
    callback_offset, callback = 0, None
    peak_rss = peak_processes = peak_scratch = 0
    with (root / "host.log").open("wb") as log:
        process = subprocess.Popen([sys.executable, "-m", "research_pipeline.runtime.lean_engine_worker",
            "--request", str(root / "launch-request.json")], env=environment, stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
        try:
            with _project_sampling_scope(root) as sampler:
                while True:
                    if sampler is None:
                        rss, count = _project_process_usage(process.pid, observed, windows_job_name=job)
                        scratch = _measure_attempt_tree_bytes(node_context.work_dir)
                    else:
                        rss, count, scratch = sampler.sample_project_usage(process.pid, observed,
                            (node_context.work_dir,), windows_job_name=job)
                    peak_rss, peak_processes, peak_scratch = max(peak_rss, rss), max(peak_processes, count), max(peak_scratch, scratch)
                    if rss > budget.memory_bytes or count > node_context.process_slots or scratch > budget.temp_bytes:
                        raise RuntimeWorkerError("LEAN进程或临时空间超出节点额度", error_code="lean_resource_exceeded")
                    if budget.wall_seconds and time.monotonic() - started > budget.wall_seconds:
                        raise RuntimeWorkerError("LEAN节点累计运行超时", error_code="lean_node_timeout")
                    callback_offset, callback = _read_callback_events(
                        root / "callback-events.jsonl", callback_offset, callback)
                    if callback is not None and time.time() > callback["started_at_unix"] + callback["wall_seconds"]:
                        raise RuntimeWorkerError("LEAN策略回调超时", error_code="strategy_callback_timeout")
                    if process.poll() is not None:
                        break
                    time.sleep(0.1)
            if process.returncode:
                raise RuntimeWorkerError(f"LEAN引擎退出码为{process.returncode}，诊断见{root / 'engine.log'}", error_code="lean_engine_failed")
        finally:
            cleanup = _terminate_process_tree(process, observed_descendants=observed)
            _write_json(root / "resource-usage.json", {"peak_rss_bytes": peak_rss, "peak_processes": peak_processes,
                "peak_scratch_bytes": peak_scratch, "wall_seconds": time.monotonic() - started, "cleanup_status": cleanup})
            if cleanup != "complete":
                raise RuntimeWorkerError("LEAN进程树清理未完成", error_code="lean_cleanup_failed")


def _json_rows(source, destination):
    import pyarrow as pa
    import pyarrow.parquet as pq
    destination.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([pa.field("payload_json", pa.string(), nullable=False)])
    count = 0
    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        batch = []
        for record in source:
            batch.append({"payload_json": json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)})
            count += 1
            if len(batch) == 4096:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch = []
        if batch or count == 0:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    return count


def _read_json_lines(path):
    with path.open(encoding="utf-8-sig") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def execute_lean_strategy_node(service, *, node_context, run_id, attempt_id, environment):
    from .lean_strategy_inputs import prepare_lean_inputs
    parameters = service.project_parameters_by_node[node_context.node.node_id]
    registry = service.project_registry
    token = registry.project_token_by_implementation(node_context.node.implementation_id)
    project_runtime_identity(registry, token.manifest.operator_spec.operator_id, token.manifest.operator_spec.operator_version)
    runtime = validate_lean_runtime(parameters["lean_runtime"])
    declaration = parse_strategy_declaration(parameters["strategy_declaration"])
    root = node_context.work_dir / "lean"
    root.mkdir(exist_ok=False)
    started = time.monotonic()
    context = OperatorRuntimeContext(node_context, environment)
    prepared = prepare_lean_inputs(context, parameters, declaration,
        registry.bundle_paths[node_context.node.implementation_id], root)
    prepared_at = time.monotonic()
    stream = prepared.pop("_day_stream")
    market = prepared.pop("_market_source")
    try:
        _write_json(root / "runtime-lock.json", runtime)
        _engine_config(runtime, root, f"{run_id}-{attempt_id}")
        _run_engine(root, runtime, node_context, started)
        stream.require_complete()
        prepared["bars_by_date"] = dict(stream.counts)
        prepared["bars"] = sum(stream.counts.values())
        prepared["signal_rows"] = stream.signal_rows
        prepared["source_reads"] = market.statistics
    finally:
        stream.close()
    engine_finished_at = time.monotonic()
    prepared["stage_seconds"] = {"prepare": prepared_at - started,
        "execute": engine_finished_at - prepared_at}
    launcher_summary = json.loads((root / f"{run_id}-{attempt_id}-summary.json").read_text(encoding="utf-8"))
    launcher_state = launcher_summary.get("state", {})
    if launcher_state.get("Status") != "Completed" or launcher_state.get("RuntimeError"):
        raise RuntimeIntegrityError("LEAN Launcher未正常完成")
    _write_json(root / "launcher-summary.json", launcher_summary)
    status = json.loads((root / "engine-status.json").read_text(encoding="utf-8"))
    if status.get("status") != "completed" or status.get("host_finished") is not True:
        raise RuntimeIntegrityError("LEAN未交付完整引擎终态")
    return _publish_lean_output(context, root, prepared, runtime)


def _publish_lean_output(context, root, prepared, runtime):
    from research_pipeline.platform.lean_contract import LEAN_PATHS, LEAN_SUPPORT_PATHS, LEAN_METRICS_PATH
    from research_pipeline.research.lean_metrics import lean_metrics_table
    from decimal import Decimal
    import pyarrow.parquet as pq
    staging = context.external_store.prepare()
    write_started = time.monotonic()
    counts = {}
    try:
        for name, source in {"orders": "orders.jsonl", "intents": "intents.jsonl", "daily": "daily.jsonl", "trace": "strategy-trace.jsonl"}.items():
            counts[name] = _json_rows(_read_json_lines(root / source), staging / LEAN_PATHS[name] / "part-00000.parquet")
        strategy_input = json.loads((root / "strategy-input.json").read_text(encoding="utf-8"))
        metrics = lean_metrics_table(
            _read_json_lines(root / "daily.jsonl"), _read_json_lines(root / "orders.jsonl"),
            initial_equity=Decimal(strategy_input["initial_cash_units"]) / 100,
            expected_dates=strategy_input["dates"],
        )
        metric_path = staging / LEAN_METRICS_PATH / "part-00000.parquet"
        metric_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(metrics, metric_path)
        for relative in LEAN_SUPPORT_PATHS:
            destination = staging / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / Path(relative).relative_to("lean"), destination)
        prepared.setdefault("stage_seconds", {})["output_write_before_commit"] = time.monotonic() - write_started
        model = "lean_next_session_open_v1" if strategy_input.get("frequency") == "1d" else "lean_next_completed_close_v1"
        _write_json(staging / "result.json", {"contract_version": "research-lean-simulation-v1",
            "execution_backend": "lean", "execution_model": model,
            "counts": counts, "preparation": prepared, "lean_runtime": runtime})
        commit = context.external_store.commit(staging, artifact_name="simulation", artifact_type="research.lean-simulation.v1")
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs({"simulation": RuntimeNodeValue.external(commit)}, completion_metadata=RuntimeCompletionMetadata(
        counts=counts, backend_id="quantconnect.lean", fidelity="bar_level_research_only",
        limitations=(model, *(["日线权益按最新已可见价格估值，价格来源及时间保存在每日账户；不是当日收盘净值。"]
                             if model == "lean_next_session_open_v1" else []))))
