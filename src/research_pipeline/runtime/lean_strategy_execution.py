"""已准入分钟策略的LEAN节点执行、资源监督和正式工件交接。"""
from __future__ import annotations

import json
from collections import deque
from contextlib import ExitStack, closing, contextmanager
from decimal import Decimal
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

from research_pipeline.domain.strategy_declaration import parse_strategy_declaration
from research_pipeline.platform.lean_runtime import (
    lean_runtime_check_scope, read_lean_config_template, validate_lean_runtime,
)
from research_pipeline.platform.process_resources import (
    _windows_kill_job, _project_process_usage, _measure_attempt_tree_bytes,
    _project_sampling_scope, _terminate_process_tree,
)
from .errors import RuntimeIntegrityError, RuntimeWorkerError
from .operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs, RuntimeNodeValue, RuntimeCompletionMetadata
from .project_operator_runtime import project_runtime_identity


def _write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False), encoding="utf-8")


def _engine_config(runtime, root, run_id, declaration):
    installation = Path(runtime["installation_root"])
    template = installation / runtime["config_template_relative"]
    config = read_lean_config_template(template)
    algorithm = declaration.algorithm
    config.update({"environment": "backtesting", "algorithm-type-name": algorithm.type_name,
        "algorithm-language": algorithm.language,
        "algorithm-location": str(root / "strategy-source" / algorithm.entry_file),
        "algorithm-id": run_id, "close-automatically": True,
        "data-folder": str(root / "metadata"), "results-destination-folder": str(root),
        "object-store-root": str(root / "storage"),
        "python-venv": str(installation / runtime["python_environment_relative"]),
        "parquet-input-manifest": str(root / "input.json"),
        "research-strategy-input": str(root / "strategy-input.json"),
        "research-lifecycle-handler": "QuantWitness.Lean.Research.NativeMarketLifecycle",
        "algorithm-manager-time-loop-maximum": declaration.limits.time_step_seconds / 60,
        "research-initialize-seconds": declaration.limits.initialize_seconds})
    config["environments"]["backtesting"]["data-feed-handler"] = "QuantWitness.LeanInput.ParquetDataFeed"
    config["environments"]["backtesting"]["setup-handler"] = "QuantWitness.Lean.Research.NativeSetupHandler"
    config["environments"]["backtesting"]["result-handler"] = "QuantConnect.Lean.Engine.Results.BacktestingResultHandler"
    config["environments"]["backtesting"]["history-provider"] = "QuantWitness.LeanInput.NativeHistoryProvider"
    _write_json(root / "config.json", config)


@contextmanager
def _engine_process(output, runtime, cpu_slots, job):
    import psutil

    root = Path(runtime["installation_root"])
    launcher = root / runtime["launcher_relative"]
    python_root = (root / runtime["python_library_relative"]).parent
    venv = root / runtime["python_environment_relative"]
    environment = os.environ.copy()
    environment.update({
        "DOTNET_ROOT": str((root / runtime["dotnet_relative"]).parent),
        "DOTNET_CLI_TELEMETRY_OPTOUT": "1", "DOTNET_NOLOGO": "1",
        "DOTNET_CLI_HOME": str(output / "temp/dotnet"), "DOTNET_SKIP_FIRST_TIME_EXPERIENCE": "1",
        "PYTHONNET_PYDLL": str(root / runtime["python_library_relative"]),
        "PYTHONHOME": str(python_root), "PYTHONNOUSERSITE": "1",
        "PYTHONPATH": str(venv / "Lib/site-packages"), "PYTHONDONTWRITEBYTECODE": "1",
        "TEMP": str(output / "temp"), "TMP": str(output / "temp"),
        "MPLCONFIGDIR": str(output / "temp/matplotlib"),
    })
    environment["PATH"] = os.pathsep.join((str(python_root), str(python_root / "DLLs"),
        str((root / runtime["dotnet_relative"]).parent), environment.get("PATH", "")))
    (output / "temp").mkdir(exist_ok=True)
    command = [str(root / runtime["dotnet_relative"]), str(launcher), "--config", str(output / "config.json")]
    with (output / "engine.log").open("wb") as log, ExitStack() as handles:
        # Windows 先挂起主线程，绑定整个进程树及 CPU 后再执行引擎。
        flags = subprocess.CREATE_NO_WINDOW | 0x00000004 if job else 0
        process = subprocess.Popen(command, cwd=launcher.parent, env=environment, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            if job:
                import _winapi
                handles.callback(_winapi.CloseHandle, _windows_kill_job(job, int(process._handle)))
            engine = psutil.Process(process.pid)
            if hasattr(engine, "cpu_affinity"):
                engine.cpu_affinity(psutil.Process().cpu_affinity()[:cpu_slots])
            if job:
                engine.resume()
            yield process
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def _run_engine(root, node_context, started, runtime):
    budget = node_context.effective_resource_budget
    job = "Local\\QuantWitness-lean-" + uuid.uuid4().hex if sys.platform == "win32" else None
    observed = {}
    peak_rss = peak_processes = peak_scratch = 0
    process_started = time.monotonic()
    with _engine_process(root, runtime, budget.cpu_slots, job) as process:
        try:
            with _project_sampling_scope(root) as sampler:
                while True:
                    node_context.remaining_resource_budget()
                    if sampler is None:
                        rss, count = _project_process_usage(process.pid, observed, windows_job_name=job)
                        scratch = _measure_attempt_tree_bytes(node_context.work_dir)
                    else:
                        rss, count, scratch = sampler.sample_project_usage(process.pid, observed,
                            (node_context.work_dir,), windows_job_name=job)
                    peak_rss, peak_processes, peak_scratch = max(peak_rss, rss), max(peak_processes, count), max(peak_scratch, scratch)
                    if rss > budget.memory_bytes or count > node_context.process_slots or (budget.temp_bytes is not None and scratch > budget.temp_bytes):
                        raise RuntimeWorkerError("LEAN进程或临时空间超出节点额度", error_code="lean_resource_exceeded")
                    if budget.wall_seconds and time.monotonic() - started > budget.wall_seconds:
                        raise RuntimeWorkerError("LEAN节点累计运行超时", error_code="lean_node_timeout")
                    if process.poll() is not None:
                        break
                    try:
                        process.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        pass
            if process.returncode:
                raise RuntimeWorkerError(f"LEAN引擎退出码为{process.returncode}，诊断见{root / 'engine.log'}", error_code="lean_engine_failed")
        finally:
            cleanup = _terminate_process_tree(process, observed_descendants=observed)
            finished = time.monotonic()
            _write_json(root / "resource-usage.json", {"peak_rss_bytes": peak_rss, "peak_processes": peak_processes,
                "peak_scratch_bytes": peak_scratch, "wall_seconds": finished - started,
                "launcher_process_seconds": finished - process_started, "cleanup_status": cleanup})
            if cleanup != "complete":
                raise RuntimeWorkerError("LEAN进程树清理未完成", error_code="lean_cleanup_failed")


def _publish_execution_rows(name, source, destination, counts):
    """写入一张正式表并向指标计算交付同一次解码的原始行。"""
    from research_pipeline.platform.lean_execution_schema import (
        LEAN_EXECUTION_SCHEMAS, encode_lean_execution_row,
    )
    from .lean_execution_writer import LeanExecutionParquetWriter

    destination.parent.mkdir(parents=True, exist_ok=True)
    schema = LEAN_EXECUTION_SCHEMAS[name]
    count = 0
    with LeanExecutionParquetWriter(destination, schema) as writer:
        batch = []
        with source.open(encoding="utf-8-sig") as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line, parse_float=Decimal)
                batch.append(encode_lean_execution_row(name, record))
                count += 1
                if len(batch) == 4096:
                    writer.write_rows(batch)
                    batch.clear()
                yield record
        if batch or count == 0:
            writer.write_rows(batch)
    counts[name] = count


@lean_runtime_check_scope()
def execute_lean_strategy_node(service, *, node_context, run_id, attempt_id, environment):
    from .lean_strategy_inputs import _write_json as write_input_json, prepare_lean_inputs
    parameters = service.project_parameters_by_node[node_context.node.node_id]
    registry = service.project_registry
    token = registry.project_token_by_implementation(node_context.node.implementation_id)
    project_runtime_identity(registry, token.manifest.operator_spec.operator_id, token.manifest.operator_spec.operator_version)
    checking_at = time.monotonic()
    runtime = validate_lean_runtime(parameters["lean_runtime"])
    runtime_checked_at = time.monotonic()
    declaration = parse_strategy_declaration(parameters["strategy_declaration"])
    root = node_context.work_dir / "lean"
    started = time.monotonic()
    context = OperatorRuntimeContext(node_context, environment)
    prepared = prepare_lean_inputs(context, parameters, declaration,
        registry.bundle_paths[node_context.node.implementation_id], root)
    prepared_at = time.monotonic()
    market = prepared.pop("_market_source")
    input_manifest = prepared.pop("_input_manifest")
    strategy_input = prepared.pop("_strategy_input")
    try:
        from .lean_native_inputs import prepare_native_data
        input_manifest["native_data"], direct_bindings = prepare_native_data(
            context, parameters, declaration, market, strategy_input,
        )
        write_input_json(root / "input.json", input_manifest)
        write_input_json(root / "strategy-input.json", strategy_input)
        write_input_json(root / "native-bindings.json", {"bindings": direct_bindings})
        del input_manifest, direct_bindings
        writing_at = time.monotonic()
        _write_json(root / "runtime-lock.json", runtime)
        written_at = time.monotonic()
        validate_lean_runtime(runtime)
        _engine_config(runtime, root, f"{run_id}-{attempt_id}", declaration)
        _run_engine(root, node_context, started, runtime)
        reads = json.loads((root / "parquet-read.json").read_text(encoding="utf-8"))
        prepared["bars_by_date"] = {day: reads["daily_selected_rows"].get(day, 0) for day in prepared["dates"]}
        prepared["source_reads"] = reads
        prepared["bars"] = sum(prepared["bars_by_date"].values())
        prepared["signal_rows"] = 0
        if declaration.asset_class != "cn_future" and market.frequency == "1m":
            rule_timings = reads.get("rules_by_date")
            if not isinstance(rule_timings, dict):
                raise RuntimeIntegrityError("现货分钟引擎缺少逐日规则读取统计")
            prepared["daily_input_timings"] = {
                day: {"rules_seconds": row["seconds"]} for day, row in rule_timings.items()
            }
        else:
            prepared["daily_input_timings"] = reads["financial_days"]
    finally:
        market.close()
    engine_finished_at = time.monotonic()
    prepared["stage_seconds"] = {"runtime_verify": runtime_checked_at - checking_at,
        "runtime_lock_write": written_at - writing_at, "native_data_prepare": writing_at - prepared_at,
        "prepare": prepared_at - started,
        "execute": engine_finished_at - prepared_at}
    launcher_summary = json.loads((root / f"{run_id}-{attempt_id}-summary.json").read_text(encoding="utf-8"))
    launcher_state = launcher_summary.get("state", {})
    if launcher_state.get("Status") != "Completed" or launcher_state.get("RuntimeError"):
        raise RuntimeIntegrityError("LEAN Launcher未正常完成")
    _write_json(root / "launcher-summary.json", launcher_summary)
    status = json.loads((root / "engine-status.json").read_text(encoding="utf-8"))
    if status.get("status") != "completed" or status.get("host_finished") is not True:
        raise RuntimeIntegrityError("LEAN未交付完整引擎终态")
    return _publish_lean_output(context, root, prepared, runtime, strategy_input)


def _publish_lean_output(context, root, prepared, runtime, strategy_input):
    from research_pipeline.platform.lean_contract import (
        LEAN_ARTIFACT_TYPE, LEAN_JSONL_FILES, LEAN_PATHS, LEAN_METRICS_PATH, LEAN_RESULT_VERSION,
    )
    from research_pipeline.research.lean_metrics import lean_metrics_table
    import pyarrow.parquet as pq
    staging = context.external_store.prepare()
    write_started = time.monotonic()
    counts = dict.fromkeys(LEAN_JSONL_FILES, 0)
    try:
        with ExitStack() as readers:
            rows = {
                name: readers.enter_context(closing(_publish_execution_rows(
                    name, root / source, staging / LEAN_PATHS[name] / "part-00000.parquet", counts,
                ))) for name, source in LEAN_JSONL_FILES.items()
            }
            metrics = lean_metrics_table(
                rows["daily"], rows["orders"],
                initial_equity=Decimal(strategy_input["initial_cash_units"]) / 100,
                expected_dates=strategy_input["dates"], fills=rows["fills"],
                account_events=rows["account_events"],
                final_account=json.loads((root / "account.json").read_text(encoding="utf-8"), parse_float=Decimal),
            )
            # 指标没有用到的执行表仍需完整写入，适用的空表也交付固定 schema。
            for source in rows.values():
                deque(source, maxlen=0)
        metric_path = staging / LEAN_METRICS_PATH / "part-00000.parquet"
        metric_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(metrics, metric_path)
        from research_pipeline.platform.lean_native_support import copy_lean_support
        copy_lean_support(root, staging)
        prepared.setdefault("stage_seconds", {})["output_write_before_commit"] = time.monotonic() - write_started
        model = "lean_next_session_open_v1" if strategy_input.get("frequency") == "1d" else "lean_next_completed_close_v1"
        _write_json(staging / "result.json", {"contract_version": LEAN_RESULT_VERSION,
            "execution_backend": "lean", "execution_model": model,
            "counts": counts, "preparation": prepared, "lean_runtime": runtime})
        commit = context.external_store.commit(staging, artifact_name="simulation", artifact_type=LEAN_ARTIFACT_TYPE)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs({"simulation": RuntimeNodeValue.external(commit)}, completion_metadata=RuntimeCompletionMetadata(
        counts=counts, backend_id="quantconnect.lean", fidelity="bar_level_research_only",
        limitations=(model, *prepared.get("action_limitations", ()),
            *(["日线权益按最新已可见价格估值，价格来源及时间保存在每日账户；不是当日收盘净值。"]
                             if model == "lean_next_session_open_v1" else []),
            *(["已确认退市的剩余股份按零价值计入账户，保留数量且不产生卖出收入；已登记现金权益继续按事件到账。"]
              if strategy_input.get("lifecycle") else []),
            *(["执行包含盘中起止；账户按实际窗口结束时估值，年化及日频风险指标仍按交易日期计数。"]
              if any(item["start_at"] != item["session_open_at"] or item["end_at"] != item["session_close_at"]
                     for item in strategy_input.get("execution_windows", {}).values()) else []))))
