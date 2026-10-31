"""本地 LEAN 项目结果的表、支持文件和正式输出绑定。"""

from types import MappingProxyType

from .errors import MainlineError


class LeanResultContractError(MainlineError):
    """LEAN正式表绑定不满足共享合同。"""

    error_code = "result_contract_invalid"


LEAN_ARTIFACT_TYPE = "research.lean-simulation.v1"
LEAN_SIGNAL_SOURCE_SCHEMA_ID = "research.lean-signal-source.v1"
LEAN_METRICS_SCHEMA_ID = "research.lean.metrics.v1"
LEAN_METRICS_PATH = "lean/metrics"
LEAN_SCHEMA_IDS = MappingProxyType({
    name: f"research.lean.{name}.v1"
    for name in ("orders", "intents", "daily", "trace")
})
LEAN_PATHS = MappingProxyType({name: f"lean/{name}" for name in LEAN_SCHEMA_IDS})
LEAN_SUPPORT_PATHS = (
    "lean/strategy-input.json",
    "lean/engine-status.json",
    "lean/launcher-summary.json",
    "lean/account.json",
    "lean/strategy-state.json",
    "lean/runtime-lock.json",
)


def validate_lean_result_closure(node, result_spec):
    """LEAN 执行表必须完整来自同一策略节点，不能混入其他撮合结果。"""
    expected = set(LEAN_SCHEMA_IDS.values())
    selected = [table for table in result_spec.tables if table.schema_id in expected]
    if len(selected) != len(expected) or {table.schema_id for table in selected} != expected:
        raise LeanResultContractError("LEAN Result 必须恰好封存完整执行表")
    paths = {schema_id: LEAN_PATHS[name] for name, schema_id in LEAN_SCHEMA_IDS.items()}
    for table in selected:
        if (table.source_node_id, table.source_port, table.artifact_type, table.path_prefix) != (
            node.node_id, "simulation", LEAN_ARTIFACT_TYPE, paths[table.schema_id],
        ):
            raise LeanResultContractError("LEAN Result 表必须绑定同一策略节点、simulation 端口及正式路径")
    for table in result_spec.tables:
        if table.schema_id == LEAN_METRICS_SCHEMA_ID:
            if (table.source_node_id, table.source_port, table.artifact_type, table.path_prefix) != (
                node.node_id, "simulation", LEAN_ARTIFACT_TYPE, LEAN_METRICS_PATH,
            ):
                raise LeanResultContractError("LEAN正式指标必须绑定同一策略节点及指标路径")
            continue
        if table.schema_id.startswith("research.lean.") and table.schema_id not in expected:
            raise LeanResultContractError("LEAN Result schema 版本不受支持")
        if table.source_node_id == node.node_id and table.source_port == "simulation" and table.schema_id not in expected:
            raise LeanResultContractError("LEAN simulation 端口不能混入其他结果合同")
    signal_bindings = [binding for binding in node.inputs if binding.input_port == "signals"]
    source_tables = [table for table in result_spec.tables if table.schema_id == LEAN_SIGNAL_SOURCE_SCHEMA_ID]
    if signal_bindings:
        if len(signal_bindings) != 1 or len(source_tables) != 1:
            raise LeanResultContractError("LEAN signals必须封存唯一正式Feature producer源表")
        binding, table = signal_bindings[0], source_tables[0]
        if (table.source_node_id, table.source_port, table.artifact_type, table.path_prefix) != (
            binding.source_node_id, binding.source_output_port, "research.feature-set.v1", binding.source_output_port,
        ):
            raise LeanResultContractError("LEAN signals源表未绑定实际Feature producer")
    elif source_tables:
        raise LeanResultContractError("未绑定signals的LEAN节点不能封存signals源表")
