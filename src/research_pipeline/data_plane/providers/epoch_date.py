"""对可识别的 DuckDB 日期视图缩小物理扫描范围。"""

from __future__ import annotations

from datetime import date
import json
import re

import duckdb

from ..query_ir import DateRangeV1
from .sql import quote_identifier


_CREATE_VIEW = re.compile(
    r'^CREATE VIEW (?:"(?:[^"]|"")*"|\w+) AS (.+);$', re.DOTALL
)
_EPOCH = date(1970, 1, 1)
_SECONDS_PER_DAY = 86_400


def _parse_select(connection, sql):
    parsed = json.loads(connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
    if not parsed["error"] and len(parsed["statements"]) == 1:
        return parsed


def _scalar_projection(item):
    kind = item["type"]
    if kind in ("COLUMN_REF", "VALUE_CONSTANT"):
        return True
    if kind == "OPERATOR_CAST":
        return _scalar_projection(item["child"])
    if kind == "OPERATOR_COALESCE":
        return all(_scalar_projection(child) for child in item["children"])
    return (
        kind == "FUNCTION"
        and item["function_name"] == "to_timestamp"
        and not item["schema"]
        and not item.get("catalog")
        and not item["distinct"]
        and not item["filter"]
        and not item["order_bys"]["orders"]
        and len(item["children"]) == 1
        and item["children"][0]["type"] == "COLUMN_REF"
    )


def _find_projection(node, column):
    if (
        node["type"] != "SELECT_NODE"
        or node["modifiers"]
        or node["cte_map"]["map"]
        or node["group_expressions"]
        or node["group_sets"]
        or node["aggregate_handling"] != "STANDARD_HANDLING"
        or any(node[field] is not None for field in ("where_clause", "having", "qualify", "sample"))
        or not all(_scalar_projection(item) for item in node["select_list"])
    ):
        return None
    matches = [
        item for item in node["select_list"]
        if (item["alias"] or (
            item["column_names"][-1] if item["type"] == "COLUMN_REF" else ""
        )).casefold() == column.casefold()
    ]
    return matches[0] if len(matches) == 1 else None


class DuckDBEpochDateSource:
    """在一个只读连接内复用可参数化的视图表达式。"""

    def __init__(self, connection):
        self.connection = connection
        self._sources = {}

    def expression(self, plan, date_range=None):
        if date_range is None:
            date_range = plan.query.time_range
        if (
            not isinstance(date_range, DateRangeV1)
            or dict(plan.field_types).get(plan.event_time_field) not in ("date32", "date")
        ):
            return None, ()
        key = plan.object_name, plan.column_map[plan.event_time_field]
        if key not in self._sources:
            try:
                self._sources[key] = self._resolve(*key)
            except (duckdb.Error, KeyError, TypeError, IndexError, ValueError):
                # 当前引擎不提供可识别结构时，沿用原视图查询。
                self._sources[key] = None
        source = self._sources[key]
        if source is None:
            return None, ()
        # 使用 UTC 日期外扩一日，覆盖会话时区和夏令时。秒级浮点
        # 转换会舍入到微秒，再外扩一秒；外层原日期谓词剔除余量。
        lower = ((date_range.start - _EPOCH).days - 1) * _SECONDS_PER_DAY - 1
        upper = ((date_range.end - _EPOCH).days + 2) * _SECONDS_PER_DAY + 1
        return source, (lower, upper)

    def _read_view(self, name):
        row = self.connection.execute(
            "SELECT sql FROM duckdb_views() WHERE database_name = current_database() "
            "AND schema_name = 'main' AND current_schema() = 'main' "
            "AND lower(view_name) = lower(?) AND NOT internal",
            [name],
        ).fetchone()
        match = _CREATE_VIEW.fullmatch(row[0]) if row and row[0] else None
        return _parse_select(self.connection, match[1]) if match else None

    def _resolve(self, name, column):
        parsed = self._read_view(name)
        if parsed is None:
            return None
        node = parsed["statements"][0]["node"]
        if not self._add_predicate(node, column, {name.casefold()}):
            return None
        sql = self.connection.execute(
            "SELECT json_deserialize_sql(?)", [json.dumps(parsed)]
        ).fetchone()[0]
        return f"({sql}) AS {quote_identifier(name)}"

    def _add_predicate(self, node, column, active):
        item = _find_projection(node, column)
        if item is None:
            return False
        if (
            item["type"] == "OPERATOR_CAST"
            and item["cast_type"]["id"] == "DATE"
            and not item["try_cast"]
            and item["child"]["type"] == "FUNCTION"
            and item["child"]["function_name"] == "to_timestamp"
        ):
            # 将范围加在日期转换所在的投影层，让 DuckDB 处理底层关系。
            predicate = _parse_select(
                self.connection, "SELECT 1 WHERE epoch_value >= ? AND epoch_value < ?"
            )["statements"][0]["node"]["where_clause"]
            reference = item["child"]["children"][0]["column_names"]
            for comparison in predicate["children"]:
                comparison["left"]["column_names"] = reference
            node["where_clause"] = predicate
            return True

        table = node["from_table"]
        if (
            item["type"] != "COLUMN_REF"
            or table["type"] not in ("BASE_TABLE", "SUBQUERY")
            or table["sample"] is not None
            or table.get("column_name_alias")
            or table.get("at_clause")
        ):
            return False
        reference = item["column_names"]
        alias = table["alias"] or table.get("table_name", "")
        if len(reference) > 2 or (len(reference) == 2 and reference[0].casefold() != alias.casefold()):
            return False
        if table["type"] == "SUBQUERY":
            return self._add_predicate(table["subquery"]["node"], reference[-1], active)
        name = table["table_name"]
        if name.casefold() in active or table["catalog_name"] or table["schema_name"] not in ("", "main"):
            return False
        nested = self._read_view(name)
        if nested is None:
            return False
        child = nested["statements"][0]["node"]
        if not self._add_predicate(child, reference[-1], active | {name.casefold()}):
            return False
        node["from_table"] = {
            "type": "SUBQUERY",
            "alias": alias,
            "sample": None,
            "query_location": table["query_location"],
            "subquery": {"node": child},
            "column_name_alias": [],
        }
        return True
