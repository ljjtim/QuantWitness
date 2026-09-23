"""model 算子族及其直接共享实现。"""

from __future__ import annotations

from research_pipeline.research.validation import persistent_holdout_ledger_root
from ..walk_forward_model_execution import execute_model_fit_artifact, execute_model_fold_metrics_artifact, execute_model_locked_holdout_artifact, execute_model_predict_artifact, execute_model_preprocess_artifact, execute_model_selection_artifact, execute_model_split_artifact
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeValue
from .common import _capture, _environment, _factor_external_result, _input_external_root, _parameters


def execute_research_model_split_manifest_v1(
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
    _capture(context, "walk_forward_split", result)
    return value


def execute_research_model_preprocess_fit_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_preprocess_artifact,
        split_root=_input_external_root(context, "splits"),
        parameters=_parameters(context),
        root_seed=_environment(context).root_seed,
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    _capture(context, "walk_forward_preprocess", result)
    return value


def execute_research_model_fit_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_fit_artifact,
        preprocess_root=_input_external_root(context, "preprocessed"),
        parameters=_parameters(context),
        root_seed=_environment(context).root_seed,
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    _capture(context, "walk_forward_models", result)
    return value


def execute_research_model_predict_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_predict_artifact,
        preprocess_root=_input_external_root(context, "preprocessed"),
        model_root=_input_external_root(context, "models"),
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    _capture(context, "walk_forward_predictions", result)
    return value


def execute_research_model_fold_metrics_v1(
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
    _capture(context, "walk_forward_metrics", result)
    return value


def execute_research_model_selection_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    result, value = _factor_external_result(
        context,
        execute_model_selection_artifact,
        metrics_root=_input_external_root(context, "metrics"),
        preprocess_root=_input_external_root(context, "preprocessed"),
        model_root=_input_external_root(context, "models"),
        parameters=_parameters(context),
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    _capture(context, "walk_forward_selection", result)
    return value


def execute_research_model_locked_holdout_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    env = _environment(context)
    parameters = _parameters(context)
    if parameters.get("fixed_clock") != env.fixed_clock:
        raise ValueError("locked holdout fixed_clock 必须与正式 Runtime 时钟一致")
    identity = str(parameters["research_identity_hash"])
    result, value = _factor_external_result(
        context,
        execute_model_locked_holdout_artifact,
        split_root=_input_external_root(context, "splits"),
        selection_root=_input_external_root(context, "selection"),
        feature_root=_input_external_root(context, "features"),
        label_root=_input_external_root(context, "labels"),
        parameters=parameters,
        ledger_root=persistent_holdout_ledger_root(
            env.holdout_ledger_anchor,
            scope="model",
            research_identity_hash=identity,
        ),
        root_seed=env.root_seed,
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
    )
    _capture(context, "walk_forward_holdout", result)
    return value
