"""策略冻结声明的JSON解析。"""
from .strategy_protocol import StrategyDeclaration, StrategyInputBinding, StrategyLimits, StrategySchedule, StrategyProtocolError


def parse_strategy_declaration(payload):
    values = dict(payload)
    if any("delivery" not in item or item["delivery"] is None for item in values["input_bindings"]):
        raise StrategyProtocolError("strategy_schema_invalid", "序列化输入binding必须显式声明delivery")
    values["subscriptions"] = tuple(values["subscriptions"])
    values["schedules"] = tuple(StrategySchedule(**dict(item)) for item in values["schedules"])
    values["input_bindings"] = tuple(StrategyInputBinding(**{**dict(item), "columns": tuple(item["columns"])})
        for item in values["input_bindings"])
    values["limits"] = StrategyLimits(**dict(values["limits"]))
    return StrategyDeclaration(**values)


def project_strategy_worker_parameters(parameters, operator_spec, *, rule_bundle=None):
    """按已冻结算子参数投影Worker输入，核心执行仍持有完整参数。"""
    if "strategy_worker_projection" not in parameters:
        return parameters
    projection = parameters["strategy_worker_projection"]
    from collections.abc import Mapping

    specs = {item.name: item for item in operator_spec.parameters}
    marker = specs.get("strategy_worker_projection")
    if marker is None or marker.value_type.value != "json":
        raise StrategyProtocolError("strategy_schema_invalid", "Worker参数投影必须在OperatorSpec声明为JSON参数")
    if not isinstance(projection, Mapping) or set(projection) != {"parameters", "rule_ids"}:
        raise StrategyProtocolError("strategy_schema_invalid", "Worker参数投影须声明parameters和rule_ids")
    names, rule_ids = projection["parameters"], projection["rule_ids"]
    for values in (names, rule_ids):
        if (not isinstance(values, (list, tuple)) or any(not isinstance(item, str) or not item for item in values)
                or len(values) != len(set(values))):
            raise StrategyProtocolError("strategy_schema_invalid", "Worker投影名称必须是唯一字符串列表")
    if any(name not in specs or name not in parameters for name in names):
        raise StrategyProtocolError("strategy_schema_invalid", "Worker投影参数必须已声明且有冻结值")
    projected = {name: parameters[name] for name in names}
    projected["strategy_worker_projection"] = projection
    if rule_ids:
        if "rule_bundle" in names:
            raise StrategyProtocolError("strategy_schema_invalid", "规则投影通过rule_ids声明，不得同时传递完整rule_bundle")
        if rule_bundle is not None:
            rules = tuple(rule.to_dict() for rule in rule_bundle.rules if rule.rule_id in rule_ids)
            source_hash = rule_bundle.bundle_hash
        elif "strategy_rules" in parameters and "source_rule_bundle_hash" in parameters:
            rules, source_hash = parameters["strategy_rules"], parameters["source_rule_bundle_hash"]
        else:
            raise StrategyProtocolError("strategy_schema_invalid", "规则投影缺少完整规则来源")
        selected = tuple(rule for rule in rules if rule["rule_id"] in rule_ids)
        if {rule["rule_id"] for rule in selected} != set(rule_ids):
            raise StrategyProtocolError("strategy_schema_invalid", "规则投影引用不存在的rule_id")
        # 来源身份单独标记；规则视图保留全部历史版本，不充当完整规则包。
        projected["strategy_rules"] = selected
        projected["source_rule_bundle_hash"] = source_hash
    return projected
