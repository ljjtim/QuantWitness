"""minute_research 算子族及其直接共享实现。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Mapping
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.catalog import AdjustmentFactorSnapshot
from research_pipeline.data_plane import rebase_pre_window, require_minute_price_mode
from research_pipeline.research.minute_operators import IntradayFeatureStream, IntradayLabelStream, MinuteOperatorArtifact, build_intraday_signal_row, build_intraday_signals, build_intraday_target_row, build_intraday_targets
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeValue
from .common import _corporate_action_from_dict, _environment, _input_external_payload, _json_ready, _parameters
from .minute_io import _aware, _commit_partitioned_minute_rows, _minute_adjustment_context, _minute_artifact_partitions, _minute_feature_schema, _minute_label_schema, _minute_signal_schema, _minute_target_schema, _operator_bar_partitions


def _pre_window_rebaser(payload: Mapping[str, object]):
    """从 post bar 工件恢复单个消费时点所需的 PIT pre 窗口。"""

    require_minute_price_mode(payload, consumer="pre 窗口重定基", expected_mode="post")
    raw_snapshot = payload.get("adjustment_snapshot")
    raw_candidates = payload.get("adjustment_candidates")
    raw_effective_times = payload.get("adjustment_effective_times")
    raw_previous_closes = payload.get("adjustment_previous_closes")
    if (
        not isinstance(raw_snapshot, Mapping)
        or not isinstance(raw_candidates, list)
        or any(not isinstance(item, Mapping) for item in raw_candidates)
        or not isinstance(raw_effective_times, Mapping)
        or not isinstance(raw_previous_closes, Mapping)
    ):
        raise ValueError("pre 窗口缺少完整 PIT 复权上下文")
    snapshot = AdjustmentFactorSnapshot.from_mapping(raw_snapshot)
    candidates = tuple(
        _corporate_action_from_dict(item) for item in raw_candidates
    )
    effective_times = {
        date.fromisoformat(str(key)): _aware(value)
        for key, value in raw_effective_times.items()
    }
    previous_closes = {
        date.fromisoformat(str(key)): Decimal(str(value))
        for key, value in raw_previous_closes.items()
    }
    source_bars_hash = str(payload.get("source_bars_snapshot_hash", ""))
    expected_source_hash = typed_canonical_hash({
        "bars": source_bars_hash,
        "adjustment_snapshot": snapshot.snapshot_identity_hash,
        "adjustment_candidates": [item.to_dict() for item in candidates],
        "adjustment_effective_times": {
            item.isoformat(): value.isoformat(timespec="seconds")
            for item, value in sorted(effective_times.items())
        },
        "adjustment_previous_closes": {
            item.isoformat(): str(value)
            for item, value in sorted(previous_closes.items())
        },
    })
    if (
        payload.get("adjustment_snapshot_identity_hash")
        != snapshot.snapshot_identity_hash
        or payload.get("source_snapshot_hash") != expected_source_hash
    ):
        raise ValueError("pre 窗口 PIT 复权上下文身份不一致")

    def transform(
        rows: tuple[Mapping[str, object], ...] | list[Mapping[str, object]],
        selection_at: datetime,
    ) -> tuple[dict[str, object], ...]:
        return rebase_pre_window(
            rows,
            snapshot=snapshot,
            candidate_actions=candidates,
            effective_times=effective_times,
            previous_closes=previous_closes,
            selection_at=selection_at,
        )

    return transform


def execute_research_features_intraday_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    bars = _input_external_payload(context, "bars")
    parameters = dict(_parameters(context))
    expected_mode = str(parameters.get("adjustment_mode", "none"))
    require_minute_price_mode(
        bars,
        consumer="分钟 Feature",
        expected_mode="raw" if expected_mode == "none" else "post",
    )
    pre_window_rebaser = (
        _pre_window_rebaser(bars) if expected_mode == "pre" else None
    )
    feature_ids = tuple(parameters["feature_ids"])
    source_dataset, source_partitions = _operator_bar_partitions(context, bars)
    stream = IntradayFeatureStream(
        decision_time=datetime.fromisoformat(_environment(context).fixed_clock),
        feature_ids=feature_ids,
        pre_window_rebaser=pre_window_rebaser,
        **{key: value for key, value in parameters.items() if key != "feature_ids"},
    )
    def output_partitions():
        for source_ref, rows in source_partitions:
            def output_rows(*, rows=rows):
                for row in rows:
                    result = stream.consume(row)
                    if result is not None:
                        yield result

            yield source_ref, output_rows()

    return _commit_partitioned_minute_rows(
        context,
        source_dataset=source_dataset,
        partition_rows=output_partitions(),
        artifact_type="research.minute-features.v1",
        operator_id="research.features.intraday",
        prefix="features",
        timestamp_field="bar_end",
        schema=_minute_feature_schema(feature_ids, pre=expected_mode == "pre"),
        parameters=parameters,
        adjustment_context=_minute_adjustment_context(bars),
    )


def execute_research_labels_intraday_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    bars = _input_external_payload(context, "bars")
    parameters = dict(_parameters(context))
    expected_mode = str(parameters.get("adjustment_mode", "none"))
    require_minute_price_mode(
        bars,
        consumer="分钟 Label",
        expected_mode="raw" if expected_mode == "none" else "post",
    )
    pre_window_rebaser = (
        _pre_window_rebaser(bars) if expected_mode == "pre" else None
    )
    source_dataset, source_partitions = _operator_bar_partitions(context, bars)
    stream = IntradayLabelStream(
        pre_window_rebaser=pre_window_rebaser,
        **parameters,
    )
    def output_partitions():
        for source_ref, rows in source_partitions:
            def output_rows(*, rows=rows):
                for row in rows:
                    result = stream.consume(row)
                    if result is not None:
                        yield result

            yield source_ref, output_rows()

    return _commit_partitioned_minute_rows(
        context,
        source_dataset=source_dataset,
        partition_rows=output_partitions(),
        artifact_type="research.minute-labels.v1",
        operator_id="research.labels.intraday",
        prefix="labels",
        timestamp_field="available_time",
        schema=_minute_label_schema(pre=expected_mode == "pre"),
        parameters=parameters,
        adjustment_context=_minute_adjustment_context(bars),
        extra_manifest={
            "label_overlap": {
                "enabled": bool(parameters["overlap"]),
                "horizon_bars": int(parameters["horizon_bars"]),
            }
        },
    )


def execute_research_signals_intraday_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    payload = _input_external_payload(context, "features")
    parameters = dict(_parameters(context))
    build_intraday_signals(
        MinuteOperatorArtifact("research.minute-features.v1", (), payload["manifest"]),
        **parameters,
    )
    source_dataset, source_partitions = _minute_artifact_partitions(
        context, payload, port="features"
    )

    def output_partitions():
        for source_ref, rows in source_partitions:
            yield source_ref, (
                build_intraday_signal_row(
                    row,
                    feature_id=str(parameters["feature_id"]),
                    rule=str(parameters["rule"]),
                    threshold=float(parameters["threshold"]),
                )
                for row in rows
            )

    return _commit_partitioned_minute_rows(
        context,
        source_dataset=source_dataset,
        partition_rows=output_partitions(),
        artifact_type="research.minute-signals.v1",
        operator_id="research.signals.intraday",
        prefix="signals",
        timestamp_field="bar_end",
        schema=_minute_signal_schema(),
        parameters=parameters,
        adjustment_context=_minute_adjustment_context(payload),
    )


def execute_research_targets_intraday_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    payload = _input_external_payload(context, "signals")
    parameters = dict(_parameters(context))
    build_intraday_targets(
        MinuteOperatorArtifact("research.minute-signals.v1", (), payload["manifest"]),
        **parameters,
    )
    source_dataset, source_partitions = _minute_artifact_partitions(
        context, payload, port="signals"
    )

    def output_partitions():
        for source_ref, rows in source_partitions:
            yield source_ref, (
                build_intraday_target_row(
                    row,
                    asset_class=str(parameters["asset_class"]),
                    target_quantity_per_signal=int(
                        parameters["target_quantity_per_signal"]
                    ),
                    leverage_limit=float(parameters["leverage_limit"]),
                )
                for row in rows
            )

    def encode_target(row: Mapping[str, object]) -> Mapping[str, object]:
        result = dict(row)
        target = result.pop("portfolio_target")
        result["portfolio_target_json"] = canonical_json(_json_ready(target))
        return result

    return _commit_partitioned_minute_rows(
        context,
        source_dataset=source_dataset,
        partition_rows=output_partitions(),
        artifact_type="research.minute-targets.v1",
        operator_id="research.targets.intraday",
        prefix="targets",
        timestamp_field="bar_end",
        schema=_minute_target_schema(),
        parameters=parameters,
        adjustment_context=_minute_adjustment_context(payload),
        transform=encode_target,
    )
