"""本地 LEAN 项目结果的表、支持文件和正式输出绑定。"""

from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
import re
from types import MappingProxyType

from .errors import MainlineError, StrategyProtocolError


_TYPE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")


@dataclass(frozen=True, kw_only=True)
class LeanAlgorithm:
    language: str
    entry_file: str
    type_name: str

    def __post_init__(self):
        _require_algorithm(self.language in {"Python", "CSharp"}, "原生算法语言须为Python或CSharp")
        _require_algorithm(isinstance(self.entry_file, str) and bool(self.entry_file), "算法入口文件不能为空")
        path = PurePosixPath(self.entry_file)
        _require_algorithm(not path.is_absolute() and ".." not in path.parts and ":" not in self.entry_file and "\\" not in self.entry_file
                 and path.as_posix() == self.entry_file, "算法入口须为源码闭包内的相对路径")
        _require_algorithm(path.suffix == (".py" if self.language == "Python" else ".dll"),
                 "Python入口须为.py，CSharp入口须为已构建的.dll")
        _require_algorithm(isinstance(self.type_name, str) and bool(_TYPE_NAME.fullmatch(self.type_name)),
                 "原生算法须声明实际类型名")

    def to_dict(self):
        return asdict(self)


def _require_algorithm(condition, message):
    if not condition:
        raise StrategyProtocolError("lean_native_declaration_invalid", message)


class LeanResultContractError(MainlineError):
    """LEAN正式表绑定不满足共享合同。"""

    error_code = "result_contract_invalid"


LEAN_ARTIFACT_TYPE = "research.lean-simulation.v4"
LEAN_RESULT_VERSION = "research-lean-simulation-v4"
LEAN_SIGNAL_SOURCE_SCHEMA_ID = "research.lean-signal-source.v1"
LEAN_METRICS_SCHEMA_ID = "research.lean.metrics.v2"
LEAN_METRICS_PATH = "lean/metrics"
LEAN_SCHEMA_IDS = MappingProxyType({
    name: f"research.lean.{name}.v4"
    for name in ("orders", "intents", "daily", "trace", "fills", "positions", "account_events", "lifecycle")
})
LEAN_PATHS = MappingProxyType({name: f"lean/{name}" for name in LEAN_SCHEMA_IDS})
LEAN_JSONL_FILES = MappingProxyType({
    name: "strategy-trace.jsonl" if name == "trace" else f"{name}.jsonl"
    for name in LEAN_SCHEMA_IDS
})
LEAN_SUPPORT_PATHS = (
    "lean/strategy-input.json",
    "lean/engine-status.json",
    "lean/launcher-summary.json",
    "lean/account.json",
    "lean/algorithm-state.json",
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
