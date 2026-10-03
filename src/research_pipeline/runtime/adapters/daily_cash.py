"""日频现金仿真接入统一 Runtime 的目录工件端口。"""
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs
from ..qlib_portfolio_execution import execute_daily_cash_artifact
from .common import _factor_external_result, _input_external_root, _parameters


def execute_finance_simulation_daily_cash_v1(context: OperatorRuntimeContext) -> RuntimeNodeOutputs:
    _, outputs = _factor_external_result(
        context, execute_daily_cash_artifact,
        target_root=_input_external_root(context, "targets"),
        market_root=_input_external_root(context, "market"),
        parameters=_parameters(context),
        max_memory_bytes=context.effective_resource_budget.memory_bytes,
        market_artifact_hash=context.inputs["market"].artifact_ref.content_hash,
        target_artifact_hash=context.inputs["targets"].artifact_ref.content_hash,
    )
    return outputs
