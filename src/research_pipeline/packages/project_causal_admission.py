"""把项目因果声明绑定到真实图输入与已准入数据时间语义。"""

from __future__ import annotations

from datetime import datetime
from typing import Mapping

from research_pipeline.data_plane.admission import (
    AdmittedQueryPlan,
    MINUTE_AVAILABILITY_RULE,
)
from research_pipeline.data_plane.query_ir import (
    InstantRangeV2,
    QueryIR,
    QueryPurpose,
    parse_aware_datetime,
)
from research_pipeline.platform.operator_contracts import OperatorGraphRecipe
from research_pipeline.platform.project_causal_contract import (
    parse_causal_plan,
)
from research_pipeline.platform.causal_time import causal_time_key_columns

from .models import ResearchPackageError


_CAUSAL_TYPES = {
    "feature": "research.feature-set.v1",
    "label": "research.label.v1",
}
_DATA_PRODUCERS = {"data.columnar.materialize", "data.minute.scan"}


def project_causal_request_ids(parameters: Mapping[str, object]) -> set[str]:
    """只解释已约定的因果声明，不递归猜测任意项目参数。"""
    if "causal_plan" not in parameters:
        return set()
    plan = parse_causal_plan(parameters["causal_plan"]).to_dict()
    return {source["request_id"] for source in plan["sources"]}


def validate_project_causal_recipe(
    recipe: OperatorGraphRecipe,
    *,
    request_queries: Mapping[str, QueryIR],
    admission: object,
    fixed_clock: datetime,
) -> None:
    """纯编译只证明图、投影和冻结窗口，不冒充 Catalog 时间角色准入。"""
    nodes = {node.node_id: node for node in recipe.nodes}
    for node in recipe.nodes:
        if "causal_plan" not in node.parameters:
            continue
        parsed = parse_causal_plan(node.parameters["causal_plan"])
        if causal_time_key_columns(parsed.key_columns, is_feature=parsed.kind == "feature") != parsed.key_columns:
            raise ResearchPackageError("项目 causal_plan 必须使用正式表完整行键")
        plan = parsed.to_dict()
        specification = admission.require_operator(node.operator_id, node.operator_version)
        outputs = {port.port: port.artifact_type for port in specification.output_ports}
        if outputs.get(plan["output_port"]) != _CAUSAL_TYPES[plan["kind"]]:
            raise ResearchPackageError("项目 causal_plan 输出端口与正式因果类型不一致")
        inputs = {binding.input_port: binding for binding in node.inputs}
        sources = {source["port"]: source for source in plan["sources"]}
        if set(inputs) != set(sources):
            raise ResearchPackageError("项目 causal_plan 必须覆盖全部实际输入端口")
        for port, source in sources.items():
            query = request_queries.get(source["request_id"])
            if query is None:
                raise ResearchPackageError("项目 causal_plan 引用未知 request_id")
            if query.purpose is QueryPurpose.AUDIT or (
                plan["kind"] == "feature" and query.purpose is QueryPurpose.LABEL
            ):
                raise ResearchPackageError("项目因果输出不能使用 AUDIT 或把 Label 请求用于 Feature")
            if not set(source["columns"]) <= set(query.field_ids):
                raise ResearchPackageError("项目 causal_plan 源列超出 QueryIR 公开投影")
            if not {source["observation_column"], source["available_column"]} <= set(query.field_ids):
                raise ResearchPackageError("项目 causal_plan 时间列必须在 QueryIR 公开投影中")
            pending = [inputs[port].source_node_id]
            visited: set[str] = set()
            actual_requests: set[str] = set()
            while pending:
                ancestor_id = pending.pop()
                if ancestor_id in visited:
                    continue
                visited.add(ancestor_id)
                ancestor = nodes[ancestor_id]
                ancestor_spec = admission.require_operator(ancestor.operator_id, ancestor.operator_version)
                if plan["kind"] == "feature" and any(
                    output.artifact_type == _CAUSAL_TYPES["label"]
                    for output in ancestor_spec.output_ports
                ):
                    raise ResearchPackageError("项目 Feature 不能具有直接或间接 Label 祖先")
                if ancestor.operator_id == "data.minute.scan":
                    actual_requests.update(ancestor.parameters["request_ids"])
                elif ancestor.operator_id == "data.columnar.materialize":
                    bindings = tuple(
                        binding for binding in ancestor.inputs if binding.input_port == "admission"
                    )
                    if len(bindings) != 1:
                        raise ResearchPackageError("列式物化必须具有唯一平台准入输入")
                    platform_node = nodes[bindings[0].source_node_id]
                    if platform_node.operator_id != "data.catalog.admission":
                        raise ResearchPackageError("列式物化输入不是平台 Catalog admission")
                    actual_requests.update(platform_node.parameters["request_ids"])
                else:
                    pending.extend(binding.source_node_id for binding in ancestor.inputs)
            if source["request_id"] not in actual_requests:
                raise ResearchPackageError("项目 causal_plan request_id 与实际输入图来源不一致")
            # 任意变换的列名不能冒充原始 request 的列语义。
            if nodes[inputs[port].source_node_id].operator_id not in _DATA_PRODUCERS:
                raise ResearchPackageError("项目因果输入必须直接绑定可证明时间列的数据请求工件")
            for item in plan["work_items"]:
                decision = parse_aware_datetime(item["decision_time"], "decision_time")
                start = parse_aware_datetime(item["window_start"], "window_start")
                end = parse_aware_datetime(item["window_end"], "window_end")
                if decision > fixed_clock or end > fixed_clock:
                    raise ResearchPackageError("项目 causal_plan 决策或观测窗口晚于固定时钟")
                _validate_query_window(query, start, end, fixed_clock=fixed_clock)
                scope = query.time_range
                if isinstance(scope, InstantRangeV2):
                    first_month = scope.start_at.strftime("%Y-%m")
                    last_month = scope.end_at.strftime("%Y-%m")
                else:
                    first_month = scope.start.strftime("%Y-%m")
                    last_month = scope.end.strftime("%Y-%m")
                for month in item["source_partitions"][port]:
                    try:
                        canonical = datetime.strptime(month, "%Y-%m").strftime("%Y-%m")
                    except ValueError as exc:
                        raise ResearchPackageError("项目 causal_plan 来源分区必须是 YYYY-MM") from exc
                    if month != canonical or month < first_month or month > last_month:
                        raise ResearchPackageError("项目 causal_plan 来源月份超出 QueryIR 范围")


def _validate_query_window(
    query: QueryIR, start: datetime, end: datetime, *, fixed_clock: datetime
) -> None:
    scope = query.time_range
    if isinstance(scope, InstantRangeV2):
        if start < scope.start_at or end > scope.end_at:
            raise ResearchPackageError("项目 causal_plan 窗口超出 QueryIR 时间范围")
    elif (
        start.astimezone(fixed_clock.tzinfo).date() < scope.start
        or end.astimezone(fixed_clock.tzinfo).date() > scope.end
    ):
        raise ResearchPackageError("项目 causal_plan 窗口超出 QueryIR 日期范围")


def validate_project_causal_admitted_sources(
    recipe: OperatorGraphRecipe,
    admitted_plans: Mapping[str, AdmittedQueryPlan],
) -> None:
    """只有已准入的时间角色能够授权正式因果输入。"""
    for node in recipe.nodes:
        if "causal_plan" not in node.parameters:
            continue
        causal = parse_causal_plan(node.parameters["causal_plan"]).to_dict()
        for source in causal["sources"]:
            admitted = admitted_plans.get(source["request_id"])
            if admitted is None:
                raise ResearchPackageError("项目 causal_plan 缺少已准入请求")
            temporal = admitted.temporal_selection
            if temporal.requires_consumer_binding or temporal.revision_selector is not None or temporal.effective_interval_selector is not None:
                raise ResearchPackageError("项目因果输入尚不支持需逐决策快照选择的数据请求")
            if admitted.daily_availability_rule is not None or admitted.session_close_binding is not None:
                raise ResearchPackageError("项目因果输入缺少日频或 session-close 核心可见时间列")
            available_column = None
            if temporal.visibility_filter is not None:
                if temporal.visibility_filter.additional_time_fields:
                    raise ResearchPackageError("项目因果输入尚不支持多重可见时间角色")
                available_column = temporal.visibility_filter.available_time_field
            elif admitted.minute_availability_rule == MINUTE_AVAILABILITY_RULE:
                available_column = admitted.event_time_field
            if source["observation_column"] != admitted.event_time_field or source["available_column"] != available_column:
                raise ResearchPackageError("项目 causal_plan 时间列角色与 Catalog 准入事实不一致")
            types = dict(admitted.field_types)
            if any(
                not str(types.get(column, "")).lower().startswith("timestamp")
                for column in (source["observation_column"], source["available_column"])
            ):
                raise ResearchPackageError("项目因果输入时间列必须是已准入 timestamp")
