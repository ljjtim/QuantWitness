"""model 算子族及其直接共享实现。"""

from __future__ import annotations

from research_pipeline.research.validation.holdout_series import series_from_parameters
from ..walk_forward_model_execution import execute_model_fit_artifact, execute_model_fold_metrics_artifact, execute_model_locked_holdout_artifact, execute_model_predict_artifact, execute_model_selection_artifact, execute_model_split_artifact
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeValue
from .common import _environment, _factor_external_result, _input_external_root, _parameters


def execute_research_model_split_manifest_v2(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_split_artifact,
        feature_root=_input_external_root(context, "features"),
        label_root=_input_external_root(context, "labels"),
        parameters=_parameters(context),
        fixed_clock=_environment(context).fixed_clock,
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    return value


def execute_research_model_fit_v2(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_fit_artifact,
        split_root=_input_external_root(context, "splits"),
        parameters=_parameters(context),
        root_seed=_environment(context).root_seed,
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
        parallel_workers=context.node_context.parallel_workers,
        process_slots=context.node_context.process_slots,
        cpu_slots=context.effective_resource_budget.cpu_slots,
        max_temp_bytes=context.effective_resource_budget.temp_bytes,
        deadline_monotonic=context.node_context.attempt_deadline_monotonic,
        execution_report_path=context.work_dir / "model-fit-execution.json",
    )
    return value


def execute_research_model_predict_v2(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_predict_artifact,
        split_root=_input_external_root(context, "splits"),
        model_root=_input_external_root(context, "models"),
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    return value


def execute_research_model_fold_metrics_v2(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_fold_metrics_artifact,
        prediction_root=_input_external_root(context, "predictions"),
        model_root=_input_external_root(context, "models"),
        parameters=_parameters(context),
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    return value


def execute_research_model_selection_v2(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_selection_artifact,
        metrics_root=_input_external_root(context, "metrics"),
        split_root=_input_external_root(context, "splits"),
        model_root=_input_external_root(context, "models"),
        parameters=_parameters(context),
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    return value


def execute_research_model_locked_holdout_v2(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    env = _environment(context)
    parameters = _parameters(context)
    if parameters.get("fixed_clock") != env.fixed_clock:
        raise ValueError("locked holdout fixed_clock 必须与正式 Runtime 时钟一致")
    series = series_from_parameters(parameters)
    result, value = _factor_external_result(
        context,
        execute_model_locked_holdout_artifact,
        split_root=_input_external_root(context, "splits"),
        selection_root=_input_external_root(context, "selection"),
        feature_root=_input_external_root(context, "features"),
        label_root=_input_external_root(context, "labels"),
        parameters=parameters,
        ledger_root=series.ledger_root,
        root_seed=env.root_seed,
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    return value
