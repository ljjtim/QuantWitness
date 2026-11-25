"""项目策略绑定已准入bundle、行情输入及正式结果。"""
from research_pipeline.platform.asset_taxonomy import MINUTE_TARGET_ASSET_CLASSES
from research_pipeline.domain.trading import InstrumentKey
from research_pipeline.domain.strategy_declaration import (
    parse_stock_corporate_actions,
    parse_strategy_declaration,
    project_strategy_worker_parameters,
)
from .models import ResearchPackageError


def strategy_bars_frequency(parameters, input_ports):
    """频率由显式参数或正式bars端口确定。"""
    artifact = input_ports.get("bars")
    inferred = {"data.minute-bars.v1": "1m", "data.columnar-bundle.v1": "1d"}.get(artifact)
    frequency = parameters.get("frequency", inferred)
    if inferred is None or frequency not in {"1m", "1d"} or frequency != inferred:
        raise ResearchPackageError("策略频率只能为1m或1d且须与bars端口一致")
    return frequency


def daily_strategy_query(node, nodes, queries):
    """日线必须直接消费列式请求，不能由分钟变换生成。"""
    from research_pipeline.data_plane.query_ir import DateRangeV1, QueryPurpose
    binding = next(item for item in node.inputs if item.input_port == "bars")
    producer = nodes[binding.source_node_id]
    if producer.operator_id != "data.columnar.materialize":
        raise ResearchPackageError("日线bars必须直接绑定data.columnar.materialize")
    parents = tuple(item for item in producer.inputs if item.input_port == "admission")
    if len(parents) != 1 or nodes[parents[0].source_node_id].operator_id != "data.catalog.admission":
        raise ResearchPackageError("日线列式输入缺少唯一Catalog准入来源")
    request_id = node.parameters.get("bars_request_id")
    if not isinstance(request_id, str) or request_id not in nodes[parents[0].source_node_id].parameters["request_ids"] or request_id not in queries:
        raise ResearchPackageError("日线bars_request_id必须指定实际输入中唯一已准入请求")
    open_request_id = node.parameters.get("open_request_id")
    source_requests = nodes[parents[0].source_node_id].parameters["request_ids"]
    if (not isinstance(open_request_id, str) or open_request_id == request_id
            or open_request_id not in source_requests or open_request_id not in queries):
        raise ResearchPackageError("日线策略必须通过open_request_id绑定同一data_bundle的独立开盘请求")
    query = queries[request_id]
    opening = queries[open_request_id]
    if (opening.purpose is QueryPurpose.AUDIT or not isinstance(opening.time_range, DateRangeV1)
            or opening.time_range != query.time_range
            or set(opening.universe.instruments) != set(query.universe.instruments)
            or opening.adjustment not in {"none", "unadjusted", "raw"}):
        raise ResearchPackageError("日线开盘请求须为同证券、同日期范围的raw开盘来源")
    if query.purpose is QueryPurpose.AUDIT or not isinstance(query.time_range, DateRangeV1) or query.adjustment not in {"none", "unadjusted", "raw"}:
        raise ResearchPackageError("日线策略只接受非AUDIT、有界日期范围的raw行情")
    snapshot_bindings = tuple(item for item in node.inputs if item.input_port == "snapshot")
    if node.parameters.get("strategy_declaration", {}).get("asset_class") == "cn_future":
        if snapshot_bindings:
            raise ResearchPackageError("期货日线不使用现货公司行动snapshot")
        return query
    if len(snapshot_bindings) != 1:
        raise ResearchPackageError("日线策略必须绑定唯一snapshot")
    snapshot = nodes[snapshot_bindings[0].source_node_id]
    data_inputs = tuple(item for item in snapshot.inputs if item.input_port == "data")
    if (snapshot.operator_id != "data.minute.adjustment_snapshot" or len(data_inputs) != 1
            or data_inputs[0].source_node_id != producer.node_id
            or data_inputs[0].source_output_port != binding.source_output_port):
        raise ResearchPackageError("日线snapshot须与bars绑定同一列式data_bundle")
    return query


def validate_daily_strategy_plan(plan):
    """核对已准入日线的物理语义；返回执行端需要的物理到逻辑投影。"""
    from research_pipeline.data_plane.query_ir import DateRangeV1, QueryPurpose
    from research_pipeline.data_plane.admission import DAILY_AVAILABILITY_RULE
    query = plan.query
    if query.purpose is QueryPurpose.AUDIT or not isinstance(query.time_range, DateRangeV1) or query.adjustment not in {"none", "unadjusted", "raw"}:
        raise ResearchPackageError("日线策略只接受非AUDIT、有界日期范围的raw行情")
    from research_pipeline.platform.daily_projection import daily_projection
    columns = daily_projection(plan)
    if not {"open", "high", "low", "close", "volume", "code", "date"} <= set(columns):
        raise ResearchPackageError("日线请求必须投影raw OHLCV、code和date物理列")
    futures_visibility = (getattr(plan.query, "dataset_id", None) == "cn_futures.daily_bar"
        and getattr(plan, "session_close_binding", None) is not None
        and plan.session_close_binding.availability_policy_id == "available.futures.daily.next-session.v1")
    if (columns["date"] != plan.event_time_field or columns["code"] != plan.instrument_field
            or not (futures_visibility or plan.daily_availability_rule == DAILY_AVAILABILITY_RULE)):
        raise ResearchPackageError("日线请求须绑定证券、交易日及正式日频可见性规则")
    return columns


def validate_daily_open_plan(plan, bars_plan, *, catalog):
    """开盘价仅按批准Catalog的同日session_open规则消费。"""
    from research_pipeline.data_plane.query_ir import DateRangeV1, QueryPurpose
    from .project_causal_admission import _validate_catalog_date_rule
    query = plan.query
    bars = bars_plan.query
    if (query.purpose is QueryPurpose.AUDIT
            or not isinstance(query.time_range, DateRangeV1) or query.time_range != bars.time_range
            or set(query.universe.instruments) != set(bars.universe.instruments)
            or query.adjustment not in {"none", "unadjusted", "raw"}):
        raise ResearchPackageError("日线开盘请求须为同证券、同日期范围的raw开盘来源")
    from research_pipeline.platform.daily_projection import daily_projection
    columns = daily_projection(plan)
    if (not {"date", "code", "open"} <= set(columns)
            or columns["date"] != plan.event_time_field or columns["code"] != plan.instrument_field):
        raise ResearchPackageError("日线开盘请求必须投影并正确映射date、code、open")
    _validate_catalog_date_rule(plan, {"rule": "session_open", "timezone": "Asia/Shanghai"}, catalog)
    return columns


def validate_futures_strategy_inputs(node, nodes, queries, codes):
    """期货结算沿实际列式输入闭合，换月仅声明可见的同账户两腿。"""
    from datetime import datetime
    from research_pipeline.data_plane.query_ir import DateRangeV1, QueryPurpose

    p = node.parameters
    request_id = p.get("settlement_request_id")
    if request_id is not None:
        port = "bars" if p.get("frequency", "1m") == "1d" else "futures"
        bindings = tuple(item for item in node.inputs if item.input_port == port)
        if len(bindings) != 1:
            raise ResearchPackageError("期货结算缺少唯一正式列式输入")
        source = nodes[bindings[0].source_node_id]
        parents = tuple(item for item in source.inputs if item.input_port == "admission")
        if (source.operator_id != "data.columnar.materialize" or len(parents) != 1
                or nodes[parents[0].source_node_id].operator_id != "data.catalog.admission"
                or request_id not in nodes[parents[0].source_node_id].parameters["request_ids"]):
            raise ResearchPackageError("期货结算请求须直接绑定正式Catalog列式输入")
        query = queries.get(request_id)
        if (query is None or query.dataset_id != "cn_futures.settlement"
                or query.purpose is QueryPurpose.AUDIT or not isinstance(query.time_range, DateRangeV1)
                or set(query.universe.instruments) != codes):
            raise ResearchPackageError("期货结算必须为同真实合约范围的非AUDIT有界请求")
    plans = p.get("roll_plans", ())
    if not isinstance(plans, (tuple, list)):
        raise ResearchPackageError("roll_plans必须是数组")
    ids = set()
    for plan in plans:
        if set(plan) != {"plan_id", "old_instrument_id", "new_instrument_id", "direction", "quantity",
                        "available_at", "start_at", "end_at"}:
            raise ResearchPackageError("换月计划字段不完整")
        if not isinstance(plan["plan_id"], str) or not plan["plan_id"] or plan["plan_id"] in ids:
            raise ResearchPackageError("换月plan_id须非空且唯一")
        ids.add(plan["plan_id"])
        if (plan["old_instrument_id"] == plan["new_instrument_id"]
                or {plan["old_instrument_id"], plan["new_instrument_id"]} - codes
                or plan["direction"] not in {"long", "short"}
                or type(plan["quantity"]) is not int or plan["quantity"] <= 0):
            raise ResearchPackageError("换月须为同账户两真实合约、多空方向及正整数手数")
        try:
            available, start, end = (datetime.fromisoformat(str(plan[name]))
                                   for name in ("available_at", "start_at", "end_at"))
            if (any(value.tzinfo is None or value.utcoffset() is None for value in (available, start, end))
                    or not available <= start < end):
                raise ValueError("时间次序无效")
        except (ValueError, TypeError) as exc:
            raise ResearchPackageError("换月须以带时区时间声明available_at<=start_at<end_at") from exc


def futures_query_calendars(package, queries):
    """将本次包已冻结日历交给直接和归档准入，不读取外部默认期货窗口。"""
    result = {}
    for request_id, query in queries.items():
        if query.dataset_id not in {"cn_futures.daily_bar", "cn_futures.session_open_state", "cn_futures.settlement"}:
            continue
        matches = [scope.calendar for scope in package.minute_scope_inputs.values()
                   if set(query.universe.instruments) <= {p.instrument.instrument_id for p in scope.calendar.policies}]
        unique = {calendar.bundle_hash: calendar for calendar in matches}
        if len(unique) > 1:
            raise ResearchPackageError(f"期货请求对应多个不同历史日历：{request_id}")
        if unique:
            result[request_id] = next(iter(unique.values()))
    return result


def _native_binding_requests(binding, node, nodes, queries):
    """只沿真实输入边找到冻结请求，不执行算法或Universe。"""
    source = next((value for value in node.inputs if value.input_port == binding.input_port), None)
    if source is None:
        raise ResearchPackageError(f"原生数据绑定缺少输入端口：{binding.input_port}")
    stack, seen, requests = [source.source_node_id], set(), set()
    while stack:
        key = stack.pop()
        if key in seen:
            continue
        seen.add(key)
        producer = nodes[key]
        if producer.operator_id in {"data.catalog.admission", "data.minute.scan"}:
            requests.update(producer.parameters["request_ids"])
        if producer.operator_id == "research.bars.minute_resample" and producer.parameters.get("interval_minutes") != 1:
            raise ResearchPackageError("原生执行分钟行情须为一分钟")
        stack.extend(value.source_node_id for value in producer.inputs)
    if binding.kind != "targets" and (binding.request_id not in requests or binding.request_id not in queries):
        raise ResearchPackageError(f"原生绑定请求不属于实际输入：{binding.binding_id}")
    return requests


def validate_stock_strategy_actions(node, nodes, queries, bars_query, codes):
    """原始股票交易须从同一正式列式输入消费行动和完整日历。"""
    from datetime import date, timedelta
    from zoneinfo import ZoneInfo
    from research_pipeline.data_plane.query_ir import DateRangeV1, InstantRangeV2, QueryPurpose

    try:
        settings = parse_stock_corporate_actions(node.parameters["stock_corporate_actions"])
    except ValueError as exc:
        raise ResearchPackageError(str(exc)) from exc
    action_id, calendar_id = settings["action_request_id"], settings["calendar_request_id"]
    binding = next((item for item in node.inputs if item.input_port == "corporate_actions"), None)
    source = nodes[binding.source_node_id] if binding is not None else None
    parents = () if source is None else tuple(item for item in source.inputs if item.input_port == "admission")
    if (source is None or source.operator_id != "data.columnar.materialize" or len(parents) != 1
            or nodes[parents[0].source_node_id].operator_id != "data.catalog.admission"
            or not {action_id, calendar_id} <= set(nodes[parents[0].source_node_id].parameters["request_ids"])):
        raise ResearchPackageError("公司行动和交易日历须来自同一正式Catalog列式输入")
    action, calendar = queries.get(action_id), queries.get(calendar_id)
    for query, dataset in ((action, "cn_corporate_action.events"), (calendar, "cn_market.trading_sessions")):
        if (query is None or query.dataset_id != dataset or query.purpose in {QueryPurpose.AUDIT, QueryPurpose.LABEL}
                or not isinstance(query.time_range, DateRangeV1) or query.filters or query.limit is not None):
            raise ResearchPackageError(f"公司行动执行须完整读取正式有界来源：{dataset}")
    if (bars_query.dataset_id != "cn_equity.minute_bar" or not isinstance(bars_query.time_range, InstantRangeV2)
            or bars_query.adjustment != "raw"):
        raise ResearchPackageError("公司行动直读须使用已准入的原始一分钟行情")
    zone = ZoneInfo("Asia/Shanghai")
    start = bars_query.time_range.start_at.astimezone(zone).date()
    end = (bars_query.visible_end_at - timedelta(microseconds=1)).astimezone(zone).date()
    if (set(action.universe.instruments) != codes or action.time_range.start > start or action.time_range.end < end
            or "fld_cn_trade_day" not in calendar.field_ids
            or calendar.time_range.start > action.time_range.start or calendar.time_range.end < end
            or any(query.as_of is not None and date.fromisoformat(query.as_of) < end for query in (action, calendar))):
        raise ResearchPackageError("公司行动和日历须覆盖冻结交易证券及行情窗口")


def validate_project_strategy_recipe(recipe, admission, queries, *, query_plans=None, catalog=None):
    from .models import _thaw_mapping
    from research_pipeline.data_plane.query_ir import QueryPurpose
    from research_pipeline.platform.lean_runtime import validate_lean_runtime
    from research_pipeline.platform.lean_contract import LEAN_ARTIFACT_TYPE
    import sys
    nodes = {item.node_id: item for item in recipe.nodes}
    for node in recipe.nodes:
        if "strategy_declaration" not in node.parameters:
            continue
        p = node.parameters
        if p.get("execution_backend") != "lean":
            raise ResearchPackageError("项目策略执行后端必须显式声明lean")
        token = admission.project_token(node.operator_id, node.operator_version) if hasattr(admission, "project_token") else None
        if token is None or "causal_plan" in p:
            raise ResearchPackageError("原生策略须绑定算法bundle，不能在准入执行因果选股")
        declaration = parse_strategy_declaration(_thaw_mapping(p["strategy_declaration"], "strategy_declaration"))
        if declaration.bundle_id != token.manifest.bundle_hash or declaration.algorithm != token.manifest.lean_algorithm:
            raise ResearchPackageError("原生算法声明未绑定实际bundle与入口")
        spec = token.manifest.operator_spec
        if "strategy_worker_projection" in p:
            project_strategy_worker_parameters(p, spec)
        validate_lean_runtime(p.get("lean_runtime", {}), verify_files=False)
        ports = {item.port: item.artifact_type for item in spec.input_ports}
        frequency = strategy_bars_frequency(p, ports)
        if declaration.frequency != frequency:
            raise ResearchPackageError("原生策略执行频率与bars端口不一致")
        minimum_slots = 3 + int(sys.platform == "win32" and sys.prefix != sys.base_prefix)
        if spec.resource_profile["process_slots"] < minimum_slots:
            raise ResearchPackageError(f"LEAN策略须预留{minimum_slots}个进程槽")
        if {item.port: item.artifact_type for item in spec.output_ports} != {"simulation": LEAN_ARTIFACT_TYPE}:
            raise ResearchPackageError("原生策略须交付正式simulation输出")
        if set(ports) != {value.input_port for value in node.inputs}:
            raise ResearchPackageError("原生算法声明端口与实际输入不一致")
        future = declaration.asset_class == "cn_future"
        direct_actions = "stock_corporate_actions" in p
        if direct_actions:
            if (declaration.asset_class != "cn_stock" or frequency != "1m" or "snapshot" in ports
                    or ports.get("corporate_actions") != "data.columnar-bundle.v1"):
                raise ResearchPackageError("公司行动直读仅用于原始股票分钟，须绑定正式列式端口")
        elif not future and ports.get("snapshot") != "data.adjustment-factor-snapshot.v1":
            raise ResearchPackageError("现货策略须绑定正式公司行动snapshot")
        bound_ports = set()
        for binding in declaration.input_bindings:
            bound_ports.add(binding.input_port)
            if ports.get(binding.input_port) not in {"data.minute-bars.v1", "data.columnar-bundle.v1", "research.portfolio-targets.v1"}:
                raise ResearchPackageError("原生数据须来自已准入行情、列式数据或正式组合目标")
            _native_binding_requests(binding, node, nodes, queries)
            if ports[binding.input_port] == "data.minute-bars.v1" and (
                    binding.input_port != "bars" or binding.kind != "bars" or binding.resolution != "1m"):
                raise ResearchPackageError("原始分钟端口须绑定一分钟行情；日线、公告和证券池须使用已准入列式输入")
            if binding.kind == "targets":
                if ports[binding.input_port] != "research.portfolio-targets.v1" or future:
                    raise ResearchPackageError("组合目标绑定须为现货正式组合目标")
                continue
            if binding.kind == "pit" and (ports[binding.input_port] != "data.columnar-bundle.v1"
                                          or declaration.algorithm.language != "Python"):
                raise ResearchPackageError("算法内 PIT 直读须绑定 Python 算法的列式研究输入")
            query = queries[binding.request_id]
            if query.purpose in {QueryPurpose.AUDIT, QueryPurpose.LABEL}:
                raise ResearchPackageError("原生策略不能消费观察或未来标签来源")
            if not set(binding.columns) <= set(query.field_ids):
                raise ResearchPackageError("原生绑定列超出冻结查询列")
            if query_plans is not None and ports[binding.input_port] == "data.columnar-bundle.v1":
                plan = query_plans.get(binding.request_id)
                if plan is None or plan.query != query:
                    raise ResearchPackageError("原生数据缺少一致的正式准入计划")
                if binding.kind == "pit":
                    from research_pipeline.data_plane.admitted_plan_codec import validate_pit_binding
                    validate_pit_binding(plan, binding)
                if binding.daily_time is not None:
                    from .project_causal_admission import _validate_daily_source
                    _validate_daily_source({"daily_time": binding.daily_time,
                        "observation_column": plan.event_time_field,
                        "available_column": plan.event_time_field}, plan, catalog=catalog)
                elif (str(dict(plan.field_types).get(plan.event_time_field, "")).lower().startswith("date")
                        and plan.session_close_binding is None):
                    raise ResearchPackageError("日期粒度来源须显式绑定Catalog可见规则和冻结交易日历")
        if "corporate_actions" in bound_ports:
            raise ResearchPackageError("公司行动正式输入由账户按权益时点消费")
        unbound = set(ports) - bound_ports - {"snapshot", "futures"} - ({"corporate_actions"} if direct_actions else set())
        if unbound:
            raise ResearchPackageError(f"原生数据端口未声明用途：{sorted(unbound)}")
        if "minute_scope" not in p or p.get("execution_mode") != "strategy" or p.get("order_commands", ""):
            raise ResearchPackageError("原生策略须冻结市场规则，且不能预填订单")
        model = "lean_next_session_open_v1" if frequency == "1d" else "lean_next_completed_close_v1"
        if p.get("execution_model") != model or p.get("claim_ceiling") != "bar_level_research_only":
            raise ResearchPackageError("执行模型与实际频率或研究范围不一致")
        if any(type(p.get(key)) is not int or p[key] < 0 for key in ("slippage_bps", "slippage_ticks")):
            raise ResearchPackageError("原生策略须显式声明非负滑点")
        instruments = tuple(InstrumentKey.from_dict(_thaw_mapping(value, "instrument")) for value in p.get("instruments", ()))
        codes = {value.instrument_id for value in instruments}
        if not instruments or len(codes) != len(instruments) or any(value.asset_class != declaration.asset_class or value.currency != "CNY" for value in instruments):
            raise ResearchPackageError("冻结交易域须为同资产类别的不重复人民币证券")
        primary = [value for value in declaration.input_bindings if value.input_port == "bars" and value.kind == "bars" and value.resolution == frequency]
        if len(primary) != 1 or not codes <= set(queries[primary[0].request_id].universe.instruments):
            raise ResearchPackageError("执行行情须有唯一绑定并覆盖完整交易域")
        if direct_actions:
            validate_stock_strategy_actions(node, nodes, queries, queries[primary[0].request_id], codes)
        if future:
            if any(value.contract_kind not in {"future", "future_contract"} or any(marker in value.instrument_id.upper() for marker in ("8888", "9999", "CONTINUOUS")) for value in instruments):
                raise ResearchPackageError("期货策略只能交易真实合约")
            validate_futures_strategy_inputs(node, nodes, queries, codes)
        else:
            required_scale = 3 if declaration.asset_class == "cn_etf" else 2
            price_scale = p.get("price_scale", required_scale)
            if type(price_scale) is not int or price_scale != required_scale:
                raise ResearchPackageError("股票报价精度 price_scale 须为整数2，ETF须为整数3")
        if frequency == "1d":
            query = daily_strategy_query(node, nodes, queries)
            if query != queries[primary[0].request_id]:
                raise ResearchPackageError("执行日线与原生绑定不是同一请求")
            if query_plans is not None:
                validate_daily_strategy_plan(query_plans[p["bars_request_id"]])
                validate_daily_open_plan(query_plans[p["open_request_id"]], query_plans[p["bars_request_id"]], catalog=catalog)


def validate_daily_strategy_plans(recipe, query_plans, *, catalog=None):
    """正式数据准入阶段核对日线请求；纯图编译不持有数据准入资格。"""
    nodes = {node.node_id: node for node in recipe.nodes}
    queries = {request_id: plan.query for request_id, plan in query_plans.items()}
    for node in recipe.nodes:
        if "strategy_declaration" not in node.parameters:
            continue
        binding = next(item for item in node.inputs if item.input_port == "bars")
        if (node.parameters.get("frequency") == "1d"
                or nodes[binding.source_node_id].operator_id == "data.columnar.materialize"):
            daily_strategy_query(node, nodes, queries)
            bars_plan = query_plans[node.parameters["bars_request_id"]]
            validate_daily_strategy_plan(bars_plan)
            open_plan = query_plans[node.parameters["open_request_id"]]
            validate_daily_open_plan(open_plan, bars_plan, catalog=catalog)
            declaration = node.parameters["strategy_declaration"]
            asset_class = declaration.get("asset_class", "cn_stock")
            from research_pipeline.platform.asset_taxonomy import require_instrument_type
            for plan in (bars_plan, open_plan):
                dataset = catalog.datasets[plan.query.dataset_id]
                if dataset["market"] != asset_class:
                    raise ResearchPackageError("日线及开盘Catalog资产类型必须与策略声明一致")
                require_instrument_type(asset_class, dataset["instrument_type"])


def validate_strategy_result_closure(recipe, result_spec):
    from research_pipeline.platform.lean_contract import validate_lean_result_closure
    for node in recipe.nodes:
        if "strategy_declaration" not in node.parameters:
            continue
        if node.parameters.get("execution_backend") != "lean":
            raise ResearchPackageError("项目策略执行后端必须显式声明lean")
        validate_lean_result_closure(node, result_spec)


def validate_strategy_schedules(raw):
    """定时规则由原生算法在运行中注册；准入只验证策略声明。"""
    from .models import _thaw_mapping
    parse_strategy_declaration(_thaw_mapping(raw, "strategy_declaration"))
