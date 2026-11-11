"""把项目日历和规则覆盖绑定到正式QueryIR请求。"""
from __future__ import annotations

from pathlib import Path

from research_pipeline.domain.minute_request_scope import minute_request_sessions, require_minute_request_coverage
from research_pipeline.domain.minute_rule_snapshots import PROJECT_RULE_BUNDLE_VERSION
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION
from .models import ResearchPackageError


def load_graph_minute_scopes(root, node_parameters):
    """每个命名来源只解析一次；节点之间共享完整历史版本。"""
    loaded = {}
    for parameters in node_parameters:
        reference = parameters.get("minute_scope")
        if reference is None:
            continue
        from research_pipeline.domain.minute_scope_input import (
            load_minute_scope_input,
            validate_minute_scope_reference,
        )

        validate_minute_scope_reference(reference)
        name = reference["name"]
        if name in loaded:
            if loaded[name].reference != reference:
                raise ResearchPackageError("同名 minute_scope 不能绑定不同来源")
        else:
            loaded[name] = load_minute_scope_input(root, reference)
    directory = Path(root) / "minute-scopes"
    if directory.exists() and {item.name for item in directory.iterdir()} != set(loaded):
        raise ResearchPackageError("minute-scopes 文件与声明的输入名不一致")
    return loaded


def minute_scan_scope_bindings(recipe, future_request_ids, *, required_node_ids=()):
    """显式消费者声明决定期货扫描日历，不从策略或相邻项目猜测。"""
    if not future_request_ids:
        return {}
    from research_pipeline.domain.minute_scope_input import validate_minute_scope_reference

    bindings = {}
    nodes = {node.node_id: node for node in recipe.nodes}

    def upstream_requests(node):
        pending = [binding.source_node_id for binding in node.inputs]
        seen = set()
        while pending:
            key = pending.pop()
            if key in seen:
                continue
            seen.add(key)
            producer = nodes[key]
            if producer.operator_id == "data.minute.scan":
                yield from (key for key in producer.parameters["request_ids"] if key in future_request_ids)
            else:
                pending.extend(binding.source_node_id for binding in producer.inputs)

    def bind(request_id, reference):
        reference = validate_minute_scope_reference(reference)
        if request_id in bindings and bindings[request_id] != reference:
            raise ResearchPackageError(f"期货分钟请求绑定不同 minute_scope: {request_id}")
        bindings[request_id] = reference

    for node in recipe.nodes:
        if node.node_id in required_node_ids or "minute_scope" in node.parameters:
            requests = tuple(upstream_requests(node))
            if requests and "minute_scope" not in node.parameters:
                raise ResearchPackageError(f"期货分钟项目消费者必须显式声明 minute_scope: {node.node_id}")
            for request_id in requests:
                bind(request_id, node.parameters["minute_scope"])
        causal = node.parameters.get("causal_plan")
        if causal is not None:
            for source in causal["sources"]:
                request_id = source["request_id"]
                if request_id in future_request_ids:
                    if "minute_scope" not in node.parameters:
                        raise ResearchPackageError(f"期货分钟因果节点必须显式声明 minute_scope: {node.node_id}")
                    bind(request_id, node.parameters["minute_scope"])
        if node.operator_id == "research.bars.minute_resample" and "minute_scope" in node.parameters:
            for binding in node.inputs:
                producer = nodes[binding.source_node_id]
                if binding.input_port == "minute_1m" and producer.operator_id == "data.minute.scan":
                    for request_id in producer.parameters["request_ids"]:
                        if request_id in future_request_ids:
                            bind(request_id, node.parameters["minute_scope"])
    return bindings


def validate_project_minute_scope_bindings(recipe, queries, admission):
    """普通项目分区消费者也须在准备时绑定自己的日历参数。"""
    project_token = getattr(admission, "project_token", None)
    required = {node.node_id for node in recipe.nodes
                if project_token is not None and project_token(node.operator_id, node.operator_version) is not None}
    minute_scan_scope_bindings(recipe,
        {key for key, query in queries.items() if query.dataset_id == "cn_futures.minute_bar"},
        required_node_ids=required)


def validate_minute_scope_graph(recipe, queries, *, scope_inputs=None):

    scope_inputs = scope_inputs or {}
    from research_pipeline.platform.minute_reference import require_current_minute_capability_binding

    for scope in scope_inputs.values():
        binding = dict(scope.rules.capability_binding)
        try:
            require_current_minute_capability_binding(
                consumer_id=binding["consumer_id"],
                manifest_hash=binding["minute_capability_manifest_hash"],
                contract_version=binding["contract_version"],
                binding_hash=binding["binding_hash"],
            )
        except ValueError as exc:
            raise ResearchPackageError("规则输入未绑定当前分钟能力 manifest") from exc

    def source(node):
        reference = node.parameters["minute_scope"]
        selected = scope_inputs.get(reference["name"])
        if selected is None or selected.reference != reference:
            raise ResearchPackageError("minute_scope 缺少已加载的完整来源")
        return selected

    future_requests = {key for key, query in queries.items() if query.dataset_id == "cn_futures.minute_bar"}
    bindings = minute_scan_scope_bindings(recipe, future_requests)
    for request_id, reference in bindings.items():
        scope = scope_inputs.get(reference["name"])
        if scope is None or scope.reference != reference:
            raise ResearchPackageError("期货分钟 minute_scope 缺少已加载的完整来源")
        if scope.calendar.contract_version != PROJECT_SESSION_BUNDLE_VERSION:
            raise ResearchPackageError("期货分钟因果输入须绑定完整项目日历")
        query = queries[request_id]
        for _ in minute_request_sessions(scope.calendar, instruments=query.universe.instruments,
                start_at=query.time_range.start_at, end_at=query.visible_end_at):
            pass

    nodes = {node.node_id: node for node in recipe.nodes}
    def ancestors(node):
        stack = [binding.source_node_id for binding in node.inputs if binding.input_port in {"bars", "minute_1m"}]
        seen = set()
        while stack:
            key = stack.pop()
            if key in seen:
                continue
            seen.add(key)
            source = nodes[key]
            yield source
            stack.extend(binding.source_node_id for binding in source.inputs if binding.input_port in {"bars", "minute_1m"})

    def requests(node):
        ids = {request for source in ancestors(node) if source.operator_id == "data.minute.scan"
               for request in source.parameters["request_ids"]}
        if not ids:
            raise ResearchPackageError(f"项目分钟范围缺少行情请求: {node.node_id}")
        return tuple(queries[key] for key in sorted(ids))

    calendars = {}
    for node in recipe.nodes:
        if node.operator_id != "research.bars.minute_resample" or "minute_scope" not in node.parameters:
            continue
        bundle = source(node).calendar
        for query in requests(node):
            for _ in minute_request_sessions(bundle, instruments=query.universe.instruments,
                  start_at=query.time_range.start_at, end_at=query.visible_end_at,
                  policy_id=None if bundle.contract_version == PROJECT_SESSION_BUNDLE_VERSION else node.parameters["session_policy_ref"]):
                pass
        calendars[node.node_id] = bundle
    for node in recipe.nodes:
        if node.operator_id != "finance.simulation.intraday" and "strategy_declaration" not in node.parameters:
            continue
        bars = next(binding for binding in node.inputs if binding.input_port == "bars")
        producer = nodes[bars.source_node_id]
        if "strategy_declaration" in node.parameters and (
                node.parameters.get("frequency") == "1d" or producer.operator_id == "data.columnar.materialize"):
            from datetime import datetime, time, timedelta
            from zoneinfo import ZoneInfo
            from .project_strategy_admission import daily_strategy_query
            query = daily_strategy_query(node, nodes, queries)
            scope = source(node)
            if scope.rules.bundle_hash != node.parameters["rule_bundle_hash"]:
                raise ResearchPackageError("日线规则输入与rule_bundle_hash不一致")
            if (scope.rules.contract_version != PROJECT_RULE_BUNDLE_VERSION
                    or scope.calendar.contract_version != PROJECT_SESSION_BUNDLE_VERSION):
                raise ResearchPackageError("日线策略须绑定完整项目交易日历和规则")
            zone = ZoneInfo("Asia/Shanghai")
            if node.parameters["strategy_declaration"].get("asset_class") == "cn_future":
                from research_pipeline.domain.session_calendar import SessionCalendarResolver
                resolver = SessionCalendarResolver(scope.calendar)
                sessions = []
                for code in query.universe.instruments:
                    policies = tuple(item for item in scope.calendar.policies if item.instrument.instrument_id == code)
                    if not policies:
                        raise ResearchPackageError(f"期货日线缺少冻结会话：{code}")
                    for day in sorted({day for item in policies for day in item.trading_dates
                                       if query.time_range.start <= day <= query.time_range.end}):
                        if resolver._visible_policies(policies, day, None):
                            sessions.append(resolver.resolve_trading_date(policies[0].instrument, day, policy_revision=None))
                if not sessions:
                    raise ResearchPackageError("期货日线窗口没有冻结交易会话")
                require_minute_request_coverage(scope.rules, scope.calendar,
                    instruments=query.universe.instruments,
                    start_at=min(item.starts_at for session in sessions for item in session.segments if item.bar_eligible),
                    end_at=max(item.ends_at for session in sessions for item in session.segments if item.bar_eligible))
                continue
            require_minute_request_coverage(scope.rules, scope.calendar,
                instruments=query.universe.instruments,
                start_at=datetime.combine(query.time_range.start, time.min, tzinfo=zone),
                end_at=datetime.combine(query.time_range.end + timedelta(days=1), time.min, tzinfo=zone))
            continue
        upstream = (nodes[bars.source_node_id], *ancestors(nodes[bars.source_node_id]))
        selected = {source.node_id: calendars[source.node_id] for source in upstream if source.node_id in calendars}
        scope = source(node) if "minute_scope" in node.parameters else None
        rules = None if scope is None else scope.rules
        if rules is not None and rules.bundle_hash != node.parameters["rule_bundle_hash"]:
            raise ResearchPackageError("分钟规则输入与 rule_bundle_hash 不一致")
        if not selected and (rules is None or rules.contract_version != PROJECT_RULE_BUNDLE_VERSION):
            if "strategy_declaration" in node.parameters:
                from research_pipeline.domain.session_calendar import load_session_policy_bundle
                from .project_strategy_admission import validate_strategy_schedules
                for query in requests(nodes[bars.source_node_id]):
                    validate_strategy_schedules(node.parameters["strategy_declaration"], load_session_policy_bundle(), query)
            continue
        if len(selected) != 1 or rules is None:
            raise ResearchPackageError("项目分钟仿真须绑定唯一行情日历及完整规则")
        calendar = next(iter(selected.values()))
        if calendar.bundle_hash != scope.calendar.bundle_hash:
            raise ResearchPackageError("分钟规则与行情须绑定同一日历来源")
        if (rules.contract_version == PROJECT_RULE_BUNDLE_VERSION) != (calendar.contract_version == PROJECT_SESSION_BUNDLE_VERSION):
            raise ResearchPackageError("项目规则与日历须使用同一范围合同")
        for query in requests(nodes[bars.source_node_id]):
            if rules.contract_version == PROJECT_RULE_BUNDLE_VERSION:
                require_minute_request_coverage(rules, calendar, instruments=query.universe.instruments,
                    start_at=query.time_range.start_at, end_at=query.visible_end_at)
            if "strategy_declaration" in node.parameters:
                from .project_strategy_admission import validate_strategy_schedules
                validate_strategy_schedules(node.parameters["strategy_declaration"], calendar, query)
