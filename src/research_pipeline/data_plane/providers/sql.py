"""Provider 私有 SQL 编译器；SQL 不进入公共身份。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ..admission import (
    MINUTE_AVAILABILITY_RULE,
    MINUTE_TIME_NORMALIZATION_VERSION,
    AdmittedQueryPlan,
)
from ..errors import QueryIRInvalidError
from ..query_ir import DateRangeV1, FilterOperator, InstantRangeV2, source_local_naive


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def compile_duckdb_query(
    plan: AdmittedQueryPlan,
    *,
    source_expression: str | None = None,
    source_columns: dict[str, str] | None = None,
    materialized_snapshot: bool = False,
    preserve_temporal_facts: bool = False,
) -> tuple[str, list[Any]]:
    if materialized_snapshot and preserve_temporal_facts:
        raise QueryIRInvalidError("materialized_snapshot 与 preserve_temporal_facts 不能同时启用")
    columns = plan.column_map if source_columns is None else source_columns
    field_types = dict(plan.field_types)
    output_fields = (
        plan.temporal_selection.required_scan_fields
        if preserve_temporal_facts
        else plan.query.field_ids
    )
    public_projection = ", ".join(
        quote_identifier(field_id) for field_id in output_fields
    )
    scan_projection = ", ".join(
        f"CAST({quote_identifier(columns[field_id])} AS {_duckdb_type(field_types[field_id])}) "
        f"AS {quote_identifier(field_id)}"
        for field_id in plan.temporal_selection.required_scan_fields
    )
    source = source_expression or quote_identifier(plan.object_name)
    clauses: list[str] = []
    params: list[Any] = []
    if not materialized_snapshot:
        clauses, params = _source_filter_clauses(plan, columns)
    ordering = ", ".join(
        f"{quote_identifier(item.field_id)} {'DESC' if item.descending else 'ASC'}"
        for item in plan.query.sort
    )
    if preserve_temporal_facts:
        source_order = tuple(
            dict.fromkeys(
                (
                    *plan.primary_key,
                    *plan.temporal_selection.source_order_fields,
                )
            )
        )
        ordering = ", ".join(
            f"{quote_identifier(field_id)} ASC NULLS LAST" for field_id in source_order
        )
    if materialized_snapshot:
        sql = (
            f"SELECT {public_projection} FROM {source} "
            f"ORDER BY {ordering}"
        )
        sql += " LIMIT ?"
        params.append(_execution_limit(plan))
        return sql, params

    where = "" if not clauses else f" WHERE {' AND '.join(clauses)}"
    ctes = [f'"__scan" AS (SELECT {scan_projection} FROM {source}{where})']
    current = '"__scan"'
    if preserve_temporal_facts:
        temporal = plan.temporal_selection
        global_clauses: list[str] = []
        if plan.query.as_of is None:
            raise QueryIRInvalidError("时态版本事实物化必须声明 QueryIR as_of")
        if temporal.visibility_filter is not None:
            for field, inclusive in temporal.visibility_filter.time_fields:
                clause, value = _upper_bound_clause(
                    plan,
                    field,
                    plan.query.as_of,
                    inclusive=inclusive,
                    end_of_date=True,
                )
                global_clauses.append(clause)
                params.append(value)
        revision = temporal.revision_selector
        if revision is not None:
            revision_time_field = revision.order_fields[0]
            if (
                temporal.visibility_filter is None
                or revision_time_field not in {
                    field for field, _inclusive in temporal.visibility_filter.time_fields
                }
            ):
                clause, value = _upper_bound_clause(
                    plan,
                    revision_time_field,
                    plan.query.as_of,
                    inclusive=True,
                    end_of_date=True,
                )
                global_clauses.append(clause)
                params.append(value)
        if global_clauses:
            ctes.append(
                f'"__bounded_versions" AS (SELECT * FROM {current} WHERE '
                f"{' AND '.join(global_clauses)})"
            )
            current = '"__bounded_versions"'
        sql = (
            f"WITH {', '.join(ctes)} SELECT {public_projection} FROM {current} "
            f"ORDER BY {ordering}"
        )
        return sql, params
    temporal = plan.temporal_selection
    temporal_clauses: list[str] = []
    if temporal.session_close_selection is not None:
        clause, values = _session_close_clause(plan)
        temporal_clauses.append(clause)
        params.extend(values)
    if temporal.visibility_filter is not None:
        for field, inclusive in temporal.visibility_filter.time_fields:
            clause, values = _selection_upper_clause(
                plan,
                field,
                inclusive=inclusive,
            )
            temporal_clauses.append(clause)
            params.extend(values)
    revision = temporal.revision_selector
    if revision is not None:
        revision_time_field = revision.order_fields[0]
        if (
            temporal.visibility_filter is None
            or revision_time_field not in {
                field for field, _inclusive in temporal.visibility_filter.time_fields
            }
        ):
            clause, values = _selection_upper_clause(
                plan,
                revision_time_field,
                inclusive=True,
            )
            temporal_clauses.append(clause)
            params.extend(values)
    interval = temporal.effective_interval_selector
    if interval is not None:
        start = quote_identifier(interval.effective_from_field)
        end = quote_identifier(interval.effective_to_field)
        start_clock, start_values, start_exclusive = _selection_clock_operand(
            plan, interval.effective_from_field
        )
        end_clock, end_values, end_exclusive = _selection_clock_operand(
            plan, interval.effective_to_field
        )
        start_operator = "<" if start_exclusive else (
            "<=" if interval.left_closed else "<"
        )
        end_operator = ">=" if end_exclusive else (
            ">=" if interval.right_closed else ">"
        )
        temporal_clauses.extend(
            (
                f"{start} {start_operator} {start_clock}",
                f"({end} IS NULL OR {end} {end_operator} {end_clock})",
            )
        )
        params.extend((*start_values, *end_values))
    if temporal_clauses:
        ctes.append(
            f'"__visible" AS (SELECT * FROM {current} WHERE '
            f"{' AND '.join(temporal_clauses)})"
        )
        current = '"__visible"'
    if revision is not None:
        entity = ", ".join(quote_identifier(item) for item in revision.entity_fields)
        order = ", ".join(
            f"{quote_identifier(item)} DESC NULLS LAST"
            for item in revision.order_fields
        )
        winner = ", ".join(
            quote_identifier(item)
            for item in (*revision.entity_fields, *revision.order_fields)
        )
        ctes.append(
            f'"__revision" AS (SELECT * FROM {current} QUALIFY '
            f"ROW_NUMBER() OVER (PARTITION BY {entity} ORDER BY {order}) = 1 "
            f"AND CASE WHEN COUNT(*) OVER (PARTITION BY {winner}) = 1 "
            "THEN TRUE ELSE error('时态最新修订存在冲突') END)"
        )
        current = '"__revision"'
    if interval is not None:
        entity = ", ".join(quote_identifier(item) for item in interval.entity_fields)
        ctes.append(
            f'"__interval" AS (SELECT * FROM {current} QUALIFY '
            f"CASE WHEN COUNT(*) OVER (PARTITION BY {entity}) = 1 "
            "THEN TRUE ELSE error('时态有效区间重叠') END)"
        )
        current = '"__interval"'
    sql = (
        f"WITH {', '.join(ctes)} SELECT {public_projection} FROM {current} "
        f"ORDER BY {ordering}"
    )
    if not materialized_snapshot:
        sql += " LIMIT ?"
        params.append(_execution_limit(plan))
    return sql, params


def compile_duckdb_scope_statistics(
    plan: AdmittedQueryPlan,
    *,
    variable_fields: tuple[str, ...],
) -> tuple[str, list[Any]]:
    """只扫描已准入范围，不执行排序、窗口或时态选择。"""

    columns = plan.column_map
    clauses, params = _source_filter_clauses(plan, columns)
    aggregates = ["COUNT(*)"]
    for field_id in variable_fields:
        column = quote_identifier(columns[field_id])
        aggregates.append(
            f"MAX(OCTET_LENGTH(ENCODE(CAST({column} AS VARCHAR))))"
        )
    where = "" if not clauses else f" WHERE {' AND '.join(clauses)}"
    return (
        f"SELECT {', '.join(aggregates)} "
        f"FROM {quote_identifier(plan.object_name)}{where}",
        params,
    )


def _source_filter_clauses(
    plan: AdmittedQueryPlan,
    columns: Mapping[str, str],
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    event_column = quote_identifier(columns[plan.event_time_field])
    if isinstance(plan.query.time_range, DateRangeV1):
        clauses.append(f"{event_column} BETWEEN ? AND ?")
        params.extend((plan.query.time_range.start, plan.query.time_range.end))
    elif isinstance(plan.query.time_range, InstantRangeV2):
        minute_contract = (
            plan.minute_timezone,
            plan.minute_timestamp_storage,
            plan.minute_timestamp_role,
            plan.minute_bar_interval,
            plan.minute_availability_rule,
            plan.minute_time_normalization_version,
        )
        if minute_contract != (
            "Asia/Shanghai",
            "naive_local_wall_clock",
            "completed_bar_end",
            "1m",
            MINUTE_AVAILABILITY_RULE,
            MINUTE_TIME_NORMALIZATION_VERSION,
        ) or plan.query.as_of_instant is None:
            raise QueryIRInvalidError("分钟 SQL 编译缺少已准入时间语义或 as_of")
        clauses.extend(
            (
                f"{event_column} >= ?",
                f"{event_column} < ?",
                f"{event_column} <= ?",
            )
        )
        params.extend(
            (
                source_local_naive(plan.query.time_range.start_at),
                source_local_naive(plan.query.time_range.end_at),
                source_local_naive(plan.query.as_of_instant),
            )
        )
    else:  # pragma: no cover - QueryIR 构造已封闭类型
        raise QueryIRInvalidError("时间范围合同不受支持")
    if plan.query.universe.instruments:
        if plan.instrument_field is None:
            raise QueryIRInvalidError("非证券关系不能编译证券universe过滤")
        instrument_column = quote_identifier(columns[plan.instrument_field])
        placeholders = ",".join("?" for _ in plan.query.universe.instruments)
        clauses.append(f"{instrument_column} IN ({placeholders})")
        params.extend(plan.query.universe.instruments)
    for predicate in plan.query.filters:
        column = quote_identifier(columns[predicate.field_id])
        if predicate.operator == FilterOperator.EQ:
            clauses.append(f"{column} = ?")
            params.append(predicate.values[0])
        elif predicate.operator == FilterOperator.IN:
            placeholders = ",".join("?" for _ in predicate.values)
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(predicate.values)
        elif predicate.operator == FilterOperator.RANGE:
            clauses.append(f"{column} BETWEEN ? AND ?")
            params.extend(predicate.values)
        else:
            clauses.append(
                f"{column} IS {'NULL' if predicate.values[0] else 'NOT NULL'}"
            )
    return clauses, params


def _execution_limit(plan: AdmittedQueryPlan) -> int:
    if plan.query.limit is not None:
        return plan.query.limit
    return plan.query.budget.max_rows + 1


def _selection_clock_operand(
    plan: AdmittedQueryPlan,
    field: str,
) -> tuple[str, list[Any], bool]:
    clock = plan.temporal_selection.selection_clock
    if clock.source == "event_time":
        return quote_identifier(plan.event_time_field), [], False
    if clock.source == "query_as_of":
        if plan.query.as_of is None:
            if any(
                item is not None
                for item in (
                    plan.temporal_selection.visibility_filter,
                    plan.temporal_selection.revision_selector,
                    plan.temporal_selection.effective_interval_selector,
                    plan.temporal_selection.session_close_selection,
                )
            ):
                raise QueryIRInvalidError("时态 SQL 缺少 query as_of")
            return "NULL", [], False
        parameter, exclusive = plan.temporal_selection.source_cutoff_parameter(
            field,
            plan.query.as_of,
            end_of_date=True,
        )
        return "?", [parameter], exclusive
    if clock.bound_time is None:
        raise QueryIRInvalidError(
            "历史时态查询必须先为每个消费者绑定 observation/decision time"
        )
    parameter, exclusive = plan.temporal_selection.source_cutoff_parameter(
        field,
        clock.bound_time,
    )
    return "?", [parameter], exclusive


def _selection_upper_clause(
    plan: AdmittedQueryPlan,
    field: str,
    *,
    inclusive: bool,
) -> tuple[str, list[Any]]:
    operand, values, exclusive = _selection_clock_operand(plan, field)
    operator = "<" if exclusive else ("<=" if inclusive else "<")
    return f"{quote_identifier(field)} {operator} {operand}", values


def _session_close_clause(plan: AdmittedQueryPlan) -> tuple[str, list[Any]]:
    selection = plan.temporal_selection.session_close_selection
    if selection is None:  # pragma: no cover - 调用方已判断
        raise QueryIRInvalidError("session-close selection 缺失")
    clock = plan.temporal_selection.selection_clock
    if clock.source == "query_as_of":
        raw = plan.query.as_of
        if raw is None:
            raise QueryIRInvalidError("session-close query 缺少 as_of")
        if "T" in raw:
            cutoff = datetime.fromisoformat(raw)
        else:
            cutoff = datetime.combine(
                date.fromisoformat(raw) + timedelta(days=1),
                time.min,
                ZoneInfo(selection.instrument_bindings[0].timezone),
            )
    elif clock.bound_time is not None:
        cutoff = datetime.fromisoformat(clock.bound_time)
    else:
        raise QueryIRInvalidError("session-close 查询必须先绑定 consumer decision time")
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise QueryIRInvalidError("session-close selection clock 必须带时区")
    visible = tuple(item for item in selection.facts if item.completed_at <= cutoff)
    if not visible:
        return "FALSE", []
    instrument = quote_identifier(selection.instrument_field)
    session_date = quote_identifier(selection.session_date_field)
    clauses = []
    values: list[Any] = []
    for fact in visible:
        clauses.append(f"({instrument} = ? AND {session_date} = ?)")
        values.extend((fact.instrument_id, fact.session_date))
    return f"({' OR '.join(clauses)})", values


def _upper_bound_clause(
    plan: AdmittedQueryPlan,
    field: str,
    value: str,
    *,
    inclusive: bool,
    end_of_date: bool,
) -> tuple[str, Any]:
    parameter, exclusive = plan.temporal_selection.source_cutoff_parameter(
        field,
        value,
        end_of_date=end_of_date,
    )
    operator = "<" if exclusive else ("<=" if inclusive else "<")
    return f"{quote_identifier(field)} {operator} ?", parameter


def _duckdb_type(logical_type: str) -> str:
    normalized = logical_type.lower()
    mapping = {
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "date32": "DATE",
        "date": "DATE",
        "decimal128": "DECIMAL(38,18)",
        "float32": "FLOAT",
        "float64": "DOUBLE",
        "int8": "TINYINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "string": "VARCHAR",
        "varchar": "VARCHAR",
        "timestamp[us]": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
    }
    try:
        return mapping[normalized]
    except KeyError as exc:
        raise ValueError(f"QueryPlan 字段类型无法编译为 DuckDB: {logical_type}") from exc


__all__ = [
    "compile_duckdb_query",
    "compile_duckdb_scope_statistics",
    "quote_identifier",
]
