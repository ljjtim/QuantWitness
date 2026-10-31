"""项目策略绑定已准入bundle、行情输入及正式结果。"""
from research_pipeline.domain.trading import InstrumentKey
from research_pipeline.domain.strategy_declaration import parse_strategy_declaration
from research_pipeline.results.contracts import (STRATEGY_SCHEMA_IDS, STRATEGY_PATHS,
    STRATEGY_INPUTS_SCHEMA_ID, STRATEGY_INPUTS_PATH, STRATEGY_SIGNAL_SOURCE_SCHEMA_ID)
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
    if columns["date"] != plan.event_time_field or columns["code"] != plan.instrument_field or plan.daily_availability_rule != DAILY_AVAILABILITY_RULE:
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


def validate_project_strategy_recipe(recipe, admission, queries, *, query_plans=None, catalog=None):
    from .models import _thaw_mapping
    nodes = {item.node_id: item for item in recipe.nodes}
    for node in recipe.nodes:
        if "strategy_declaration" not in node.parameters:
            continue
        p = node.parameters
        backend = p.get("execution_backend")
        if backend != "lean":
            raise ResearchPackageError("项目策略执行后端必须显式声明lean")
        token = admission.project_token(node.operator_id, node.operator_version) if hasattr(admission, "project_token") else None
        if token is None or "causal_plan" in p:
            raise ResearchPackageError("策略必须绑定已准入项目bundle且不能同时声明causal_plan")
        declaration = parse_strategy_declaration(_thaw_mapping(p["strategy_declaration"], "strategy_declaration"))
        if declaration.bundle_id != token.manifest.bundle_hash:
            raise ResearchPackageError("策略声明未绑定实际项目bundle")
        spec = token.manifest.operator_spec
        import sys
        from research_pipeline.platform.lean_runtime import validate_lean_runtime
        validate_lean_runtime(p.get("lean_runtime", {}), verify_files=False)
        bars_bindings = tuple(item for item in declaration.input_bindings if item.source == "bars")
        signal_inputs = tuple(item for item in declaration.input_bindings if item.source == "signals")
        if (len(bars_bindings) != 1 or bars_bindings[0].lookback_bars < 1
                or any(item.lookback_bars != 1 for item in signal_inputs)
                or len(signal_inputs) > 1 or declaration.schedules):
            raise ResearchPackageError("LEAN策略要求一个行情历史窗口、至多一个单期signals输入且无定时事件")
        declared_ports = {item.port: item.artifact_type for item in spec.input_ports}
        frequency = strategy_bars_frequency(p, declared_ports)
        if declaration.frequency != frequency:
            raise ResearchPackageError("策略声明frequency必须与节点行情频率一致")
        minimum_slots = 3 + int(sys.platform == "win32" and sys.prefix != sys.base_prefix)
        if spec.resource_profile["process_slots"] < minimum_slots:
            raise ResearchPackageError(f"LEAN策略须预留{minimum_slots}个进程槽，含Supervisor、宿主、引擎及实际Python启动进程")
        if not any(item.input_port == "snapshot" for item in node.inputs):
            raise ResearchPackageError("LEAN策略须直接绑定正式snapshot输入")
        financial_port = "snapshot" if any(item.input_port == "snapshot" for item in node.inputs) else "targets"
        expected_ports = {"bars": declared_ports["bars"], financial_port: (
            "data.adjustment-factor-snapshot.v1" if financial_port == "snapshot" else "research.minute-targets.v1")}
        signal_bindings = tuple(binding for binding in declaration.input_bindings if binding.source == "signals")
        portfolio_targets = declared_ports.get('targets') == 'research.portfolio-targets.v1'
        if portfolio_targets:
            if frequency != '1d' or len(signal_bindings) != 1 or signal_bindings[0].columns != ('weight_ppb',):
                raise ResearchPackageError('组合目标仅支持日频及唯一weight_ppb信号binding')
            expected_ports['targets'] = 'research.portfolio-targets.v1'
        elif signal_bindings:
            expected_ports["signals"] = "research.feature-set.v1"
        declared_ports = {item.port: item.artifact_type for item in spec.input_ports}
        if declared_ports not in (expected_ports, {**expected_ports, "signals": "research.feature-set.v1"}):
            raise ResearchPackageError("策略输入必须绑定bars、snapshot或targets，以及可选正式signals Feature工件")
        if "signals" in p:
            raise ResearchPackageError("策略signals只能通过正式工件端口交付")
        if spec.resource_profile["process_slots"] < 2:
            raise ResearchPackageError("策略必须预留Supervisor和Worker进程槽")
        if any(binding.source == "bars" and not set(binding.columns) <= {"open", "high", "low", "close", "avg", "volume"} for binding in declaration.input_bindings):
            raise ResearchPackageError("策略行情投影列不受支持")
        if "minute_scope" not in p:
            raise ResearchPackageError("项目策略必须显式冻结市场规则")
        output_type = "research.lean-simulation.v1"
        if ({item.port: item.artifact_type for item in spec.output_ports} != {"simulation": output_type}
                or set(item.input_port for item in node.inputs) != set(expected_ports)):
            raise ResearchPackageError("策略须按binding声明行情、金融输入、可选signals及simulation输出")
        if signal_bindings and not portfolio_targets:
            from research_pipeline.platform.project_causal_contract import parse_causal_plan
            from research_pipeline.data_plane.query_ir import QueryPurpose
            source_binding = next(item for item in node.inputs if item.input_port == "signals")
            source = nodes[source_binding.source_node_id]
            producer = admission.project_token(source.operator_id, source.operator_version)
            if producer is None or "causal_plan" not in source.parameters:
                raise ResearchPackageError("策略signals必须直接来自已准入项目causal Feature")
            plan = parse_causal_plan(source.parameters["causal_plan"])
            if plan.kind != "feature" or plan.key_columns != ("event_id",) or plan.output_port != source_binding.source_output_port:
                raise ResearchPackageError("策略signals须绑定event_id稳定键的正式causal Feature输出")
            if any(queries[item.request_id].purpose in {QueryPurpose.AUDIT, QueryPurpose.LABEL} for item in plan.sources):
                raise ResearchPackageError("策略signals不能来自AUDIT或Label来源")
            outputs = {item.port: item.artifact_type for item in producer.manifest.operator_spec.output_ports}
            if outputs.get(source_binding.source_output_port) != "research.feature-set.v1":
                raise ResearchPackageError("策略signals输出类型不是正式Feature")
        if p.get("execution_mode") != "strategy" or p.get("order_commands", ""):
            raise ResearchPackageError("项目策略须显式声明strategy模式且不能预填order_commands")
        model = "lean_next_session_open_v1" if frequency == "1d" else "lean_next_completed_close_v1"
        if p.get("execution_model") != model or p.get("claim_ceiling") != "bar_level_research_only":
            raise ResearchPackageError("策略执行模型与后端或研究声明范围不一致")
        for key in ("slippage_bps", "slippage_ticks"):
            if type(p.get(key)) is not int or p[key] < 0:
                raise ResearchPackageError("策略必须显式声明非负整数滑点")
        instruments = tuple(InstrumentKey.from_dict(_thaw_mapping(item, "instrument")) for item in p.get("instruments", ()))
        codes = {item.instrument_id for item in instruments}
        asset_class = declaration.asset_class
        if (asset_class not in {"cn_stock", "cn_etf"} or not instruments or len(codes) != len(instruments)
                or any(item.asset_class != asset_class for item in instruments)):
            raise ResearchPackageError("策略证券须为不重复的股票或ETF集合，且资产类型与策略声明一致")
        expected_scale = 2 if asset_class == "cn_stock" else 3
        price_scale = p.get("price_scale", expected_scale)
        if type(price_scale) is not int or price_scale != expected_scale:
            raise ResearchPackageError("策略price_scale须按资产类型声明：股票2、ETF3")
        if frequency == "1d":
            query = daily_strategy_query(node, nodes, queries)
            if query_plans is not None:
                plan = query_plans.get(p["bars_request_id"])
                if plan is None or plan.query != query:
                    raise ResearchPackageError("日线bars请求缺少一致的已准入计划")
                validate_daily_strategy_plan(plan)
                opening = query_plans.get(p["open_request_id"])
                if opening is None:
                    raise ResearchPackageError("日线开盘请求缺少已准入计划")
                validate_daily_open_plan(opening, plan, catalog=catalog)
            if set(query.universe.instruments) != codes:
                raise ResearchPackageError("策略证券必须与冻结日线请求一致")
            continue
        requests = set()
        stack = [item.source_node_id for item in node.inputs if item.input_port == "bars"]
        seen = set()
        while stack:
            key = stack.pop()
            if key in seen:
                continue
            seen.add(key)
            source = nodes[key]
            if source.operator_id == "data.minute.scan":
                requests.update(source.parameters["request_ids"])
            if source.operator_id == "research.bars.minute_resample" and source.parameters.get("interval_minutes") != 1:
                raise ResearchPackageError("策略行情必须为1分钟")
            stack.extend(item.source_node_id for item in source.inputs if item.input_port in {"bars", "minute_1m"})
        from research_pipeline.data_plane.query_ir import QueryPurpose
        if any(queries[key].purpose is QueryPurpose.AUDIT for key in requests):
            raise ResearchPackageError("观察用行情不能进入策略交易")
        if not requests or any(set(queries[key].universe.instruments) != codes for key in requests):
            raise ResearchPackageError("策略证券必须与冻结行情请求一致")


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


def validate_strategy_schedules(raw, calendar, query):
    from datetime import datetime, time, timedelta
    from research_pipeline.domain.minute_request_scope import minute_request_sessions
    from research_pipeline.domain.session_calendar import SessionCalendarResolver
    from .models import _thaw_mapping
    declaration = parse_strategy_declaration(_thaw_mapping(raw, "strategy_declaration"))
    common = {}
    resolver = SessionCalendarResolver(calendar)
    for policy, day, _, _ in minute_request_sessions(calendar, instruments=query.universe.instruments,
            start_at=query.time_range.start_at, end_at=query.time_range.end_at):
        segments = tuple((item.starts_at, item.ends_at)
                         for item in resolver._segments_for_date(policy, day) if item.bar_eligible)
        if day in common:
            if common[day] != segments:
                raise ResearchPackageError("同一策略账户要求共同交易时段")
            # 逐证券核对时段；相同日期和时段的定时事件只需判断一次。
            continue
        common[day] = segments
        start, end = segments[0][0], segments[-1][1]
        for item in declaration.schedules:
            at = (datetime.combine(day, time.fromisoformat(item.at), tzinfo=start.tzinfo) if item.basis == "local_time"
                  else (start if item.basis == "session_open" else end) + timedelta(seconds=item.offset_seconds))
            if at.second or at.microsecond or not any(left <= at <= right for left, right in segments):
                raise ResearchPackageError("定时事件不在合法分钟触发点")
