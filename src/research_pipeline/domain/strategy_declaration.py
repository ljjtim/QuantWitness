"""策略冻结声明的JSON解析。"""
from collections.abc import Mapping

from ..platform.errors import StrategyProtocolError


def parse_strategy_declaration(payload):
    from .lean_algorithm import parse_lean_strategy_declaration
    return parse_lean_strategy_declaration(payload)


def parse_stock_corporate_actions(value):
    """解析原始股票分钟执行的公司行动来源及金融口径。"""
    ids = {"action_request_id", "calendar_request_id"}
    allowed = ids | {"financial_missing_date_policy", "financial_entitlement_exclusions"}
    if (not isinstance(value, Mapping) or not ids <= value.keys() <= allowed
            or any(not isinstance(value[key], str) or not value[key] for key in ids)
            or value["action_request_id"] == value["calendar_request_id"]):
        raise ValueError("stock_corporate_actions 须声明不同的行动及日历请求")
    if value.get("financial_missing_date_policy", "error") not in (
        "error", "omit_event", "sellable_date_receivable"
    ):
        raise ValueError("financial_missing_date_policy 不受支持")
    if not isinstance(value.get("financial_entitlement_exclusions", ()), (list, tuple)):
        raise ValueError("financial_entitlement_exclusions 须为列表")
    return value


def project_strategy_worker_parameters(parameters, operator_spec):
    """按已冻结算子参数投影Worker输入，核心执行仍持有完整参数。"""
    internal_rules = {"rule_bundle", "strategy_rules", "source_rule_bundle_hash"}
    if "strategy_worker_projection" not in parameters:
        return {name: value for name, value in parameters.items() if name not in internal_rules}
    projection = parameters["strategy_worker_projection"]

    specs = {item.name: item for item in operator_spec.parameters}
    marker = specs.get("strategy_worker_projection")
    if marker is None or marker.value_type.value != "json":
        raise StrategyProtocolError("strategy_schema_invalid", "Worker参数投影必须在OperatorSpec声明为JSON参数")
    if not isinstance(projection, Mapping) or set(projection) != {"parameters"}:
        raise StrategyProtocolError("strategy_schema_invalid", "Worker参数投影只接受parameters")
    names = projection["parameters"]
    if (not isinstance(names, (list, tuple)) or any(not isinstance(name, str) or not name for name in names)
            or len(names) != len(set(names))):
        raise StrategyProtocolError("strategy_schema_invalid", "Worker投影名称必须是唯一字符串列表")
    if internal_rules.intersection(names):
        raise StrategyProtocolError("strategy_schema_invalid", "Worker参数投影不能交付静态规则")
    if any(name not in specs or name not in parameters for name in names):
        raise StrategyProtocolError("strategy_schema_invalid", "Worker投影参数必须已声明且有冻结值")
    projected = {name: parameters[name] for name in names}
    projected["strategy_worker_projection"] = projection
    return projected
