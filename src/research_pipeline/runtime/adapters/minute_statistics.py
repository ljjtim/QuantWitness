"""minute_statistics 算子族及其直接共享实现。"""

from __future__ import annotations

from datetime import datetime, timezone
import shutil
from typing import Iterator, Mapping
from research_pipeline.platform import canonical_json
from research_pipeline.platform.metric_contracts import build_mainline_metric_registry
from research_pipeline.research.statistics import MinuteLabelReturn, MinuteStatisticsProfile, stream_minute_statistics_artifact
from ..operator_graph_evidence import build_minute_intraday_validity_facts
from ..operator_runtime import OperatorRuntimeContext, RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _environment, _external_result, _input_admitted_plans, _input_external_payload, _input_external_root, _input_merged_data_bundle, _json_ready, _parameters
from .minute_io import _aware, _external_parquet_rows, _minute_artifact_partitions


def execute_research_statistics_minute_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    labels = _input_external_payload(context, "labels")
    _input_external_payload(context, "simulation")
    parameters = _parameters(context)
    candidates = tuple(str(item) for item in parameters["trial_candidate_ids"])
    if len(candidates) != 1:
        raise ValueError(
            "分钟 statistics 当前只支持单候选标签来源；多候选必须显式绑定各自标签"
        )
    label_dataset, label_partitions = _minute_artifact_partitions(
        context, labels, port="labels"
    )
    if len(label_dataset.instruments) != 1:
        raise ValueError("分钟 statistics 当前只支持单标的有序标签来源")
    profile = MinuteStatisticsProfile(
        candidates,
        int(parameters["train_end_ns"]),
        int(parameters["validation_end_ns"]),
        int(parameters["test_end_ns"]),
        int(parameters["hac_lag"]),
        int(parameters["embargo_ns"]),
        str(parameters["multiple_testing_method"]),
        float(parameters["alpha"]),
        int(parameters["min_test_samples"]),
        _environment(context).fixed_clock,
        str(parameters["claim_ceiling"]),
    )
    import pyarrow as pa
    import pyarrow.parquet as pq

    observation_schema = pa.schema((
        pa.field("candidate_id", pa.string(), nullable=False),
        pa.field("observation_id", pa.string(), nullable=False),
        pa.field("decision_at_ns", pa.int64(), nullable=False),
        pa.field("entry_at_ns", pa.int64(), nullable=False),
        pa.field("exit_at_ns", pa.int64(), nullable=False),
        pa.field("return_value", pa.float64(), nullable=False),
    ))
    split_schema = pa.schema((
        pa.field("observation_id", pa.string(), nullable=False),
        pa.field("candidate_id", pa.string(), nullable=False),
        pa.field("split", pa.string(), nullable=False),
    ))
    staging = context.external_store.prepare()
    (staging / "observations").mkdir(parents=True, exist_ok=True)
    (staging / "split-assignments").mkdir(parents=True, exist_ok=True)
    observation_writer = pq.ParquetWriter(
        staging / "observations/part-00000.parquet", observation_schema
    )
    split_writer = pq.ParquetWriter(
        staging / "split-assignments/part-00000.parquet", split_schema
    )
    observation_buffer: list[Mapping[str, object]] = []
    split_buffer: list[Mapping[str, object]] = []
    test_start_ns: int | None = None
    test_end_ns: int | None = None

    def flush_rows(
        writer: object,
        rows: list[Mapping[str, object]],
        schema: object,
    ) -> None:
        if rows:
            writer.write_table(pa.Table.from_pylist(list(rows), schema=schema))
            rows.clear()

    def emit_observation(row: Mapping[str, object]) -> None:
        observation_buffer.append(dict(row))
        if len(observation_buffer) >= 2_048:
            flush_rows(observation_writer, observation_buffer, observation_schema)

    def emit_assignment(row: Mapping[str, object]) -> None:
        nonlocal test_start_ns, test_end_ns
        split_buffer.append(dict(row))
        if row["split"] == "test":
            observation_id = str(row["observation_id"])
            index = int(observation_id.rsplit(":", 1)[1])
            current = pending_test_times.pop(index)
            test_start_ns = (
                current[0]
                if test_start_ns is None
                else min(test_start_ns, current[0])
            )
            test_end_ns = (
                current[1]
                if test_end_ns is None
                else max(test_end_ns, current[1])
            )
        if len(split_buffer) >= 2_048:
            flush_rows(split_writer, split_buffer, split_schema)

    pending_test_times: dict[int, tuple[int, int]] = {}

    def observation_stream() -> Iterator[MinuteLabelReturn]:
        index = 0
        candidate = candidates[0]
        for _partition, rows in label_partitions:
            for row in rows:
                observation = MinuteLabelReturn(
                    candidate,
                    f"{candidate}:{index:08d}",
                    int(_aware(row["decision_time"]).timestamp() * 1_000_000_000),
                    int(_aware(row["label_start"]).timestamp() * 1_000_000_000),
                    int(_aware(row["label_end"]).timestamp() * 1_000_000_000),
                    float(row["future_return"]),
                )
                pending_test_times[index] = (
                    observation.entry_at_ns,
                    observation.exit_at_ns,
                )
                yield observation
                pending_test_times.pop(index, None)
                index += 1

    try:
        result = stream_minute_statistics_artifact(
            observations=observation_stream(),
            profile=profile,
            emit_observation=emit_observation,
            emit_assignment=emit_assignment,
        )
        flush_rows(observation_writer, observation_buffer, observation_schema)
        flush_rows(split_writer, split_buffer, split_schema)
    except Exception:
        observation_writer.close()
        split_writer.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise
    observation_writer.close()
    split_writer.close()
    trial_result = result["reported"]["trial_results"][0]
    if (
        test_start_ns is None
        or test_end_ns is None
        or int(trial_result["sample_count"]) <= 0
    ):
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError("分钟 statistics 正式指标样本不闭合")
    metric_ref = "statistics.adjusted_p@1.0.0"
    metric_definition = build_mainline_metric_registry().require(metric_ref)
    table_rows = [{
        "metric_ref": metric_ref,
        "value": float(trial_result["adjusted_p_value"]),
        "unit": metric_definition.unit,
        "sample_start": datetime.fromtimestamp(
            test_start_ns / 1_000_000_000, tz=timezone.utc
        ).isoformat(),
        "sample_end": datetime.fromtimestamp(
            test_end_ns / 1_000_000_000, tz=timezone.utc
        ).isoformat(),
        "sample_size": int(trial_result["sample_count"]),
        "status": "computed",
    }]
    try:
        (staging / "result.json").write_text(
            canonical_json(_json_ready(dict(result))), encoding="utf-8"
        )
        metrics = staging / "statistics/part-00000.parquet"
        metrics.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(table_rows), metrics)
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs(
        {commit.artifact_name: RuntimeNodeValue.external(commit)},
        completion_metadata=RuntimeCompletionMetadata(
            artifact_hashes={"statistics": str(result["artifact_hash"])},
            counts={
                "metric_artifact_count": len(table_rows),
                "observation_count": int(
                    result["observation_table"]["row_count"]
                ),
            },
        ),
    )


def execute_research_validity_minute_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    environment = _environment(context)
    simulation = _input_external_payload(context, "simulation")
    statistics = _input_external_payload(context, "statistics")
    statistics_root = _input_external_root(context, "statistics")
    data_bundle = _input_merged_data_bundle(context, "data", "minute_1m")
    facts = build_minute_intraday_validity_facts(
        admitted_plans=_input_admitted_plans(environment, data_bundle),
        data_bundle=data_bundle,
        simulation=simulation,
        statistics=statistics,
        statistic_observations=_external_parquet_rows(
            statistics_root,
            "observations",
            columns=(
                "candidate_id", "observation_id", "decision_at_ns",
                "entry_at_ns", "exit_at_ns", "return_value",
            ),
        ),
        statistic_split_assignments=_external_parquet_rows(
            statistics_root,
            "split-assignments",
            columns=("observation_id", "candidate_id", "split"),
        ),
        fixed_clock=environment.fixed_clock,
    )
    return _external_result(context, facts)
