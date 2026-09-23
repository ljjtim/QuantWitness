"""DuckDB/Parquet 物理结构只读发现。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import glob
from pathlib import Path
import re
from typing import Any

from research_pipeline.platform.canonical import typed_canonical_hash

from .errors import CatalogDriftError, CatalogReferenceError
from .models import ApprovalDecision, PhysicalBindingContract, PolicyContract


@dataclass(frozen=True)
class PhysicalColumn:
    name: str
    data_type: str
    nullable: bool
    position: int

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "data_type": self.data_type, "nullable": self.nullable, "position": self.position}


@dataclass(frozen=True)
class PhysicalInventory:
    source_kind: str
    source_profile: str
    environment: str
    object_name: str
    columns: tuple[PhysicalColumn, ...]
    derived: bool = True

    @property
    def schema_revision(self) -> str:
        return typed_canonical_hash({
            "source_kind": self.source_kind,
            "source_profile": self.source_profile,
            "environment": self.environment,
            "object_name": self.object_name,
            "columns": [item.to_dict() for item in self.columns],
        })


@dataclass(frozen=True)
class ObjectExecutionEvidence:
    """与本次数据库 revision 和查询范围绑定的只读对象形状证据。"""

    object_name: str
    object_kind: str
    dependency_chain: tuple[str, ...]
    source_rows_upper: int | None
    expanded_rows_upper: int | None
    variable_width_upper: tuple[tuple[str, int], ...]
    has_json_expansion: bool
    has_window: bool
    has_order_by: bool
    query_scope_hash: str
    database_revision: str
    expansion_bound_method: str | None
    dependency_edges: tuple[tuple[str, str], ...] = ()
    partition_count: int = 0
    partition_rows_upper: int | None = None
    partition_uncompressed_bytes_upper: int | None = None
    partition_key: str | None = None
    partition_bound_method: str | None = None
    method: str = "duckdb_catalog_read_only_v1"

    def __post_init__(self) -> None:
        if not self.object_name or not self.object_kind or not self.dependency_chain:
            raise CatalogDriftError("对象执行证据身份不完整")
        for field, value in (
            ("source_rows_upper", self.source_rows_upper),
            ("expanded_rows_upper", self.expanded_rows_upper),
        ):
            if value is not None and (type(value) is not int or value < 0):
                raise CatalogDriftError(f"{field} 必须是非负整数或空")
        if any(
            not field_id or type(width) is not int or width < 0
            for field_id, width in self.variable_width_upper
        ):
            raise CatalogDriftError("变长字段宽度证据无效")
        if self.variable_width_upper != tuple(sorted(self.variable_width_upper)):
            raise CatalogDriftError("变长字段宽度证据必须稳定排序")
        if len({field_id for field_id, _ in self.variable_width_upper}) != len(
            self.variable_width_upper
        ):
            raise CatalogDriftError("变长字段宽度证据包含重复字段")
        for field, value in (
            ("query_scope_hash", self.query_scope_hash),
            ("database_revision", self.database_revision),
        ):
            if (
                len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise CatalogDriftError(f"{field} 必须是 sha256 小写摘要")
        if self.expanded_rows_upper is None and self.expansion_bound_method is not None:
            raise CatalogDriftError("展开倍率方法不能脱离展开行数上界")
        if self.dependency_edges != tuple(sorted(set(self.dependency_edges))):
            raise CatalogDriftError("对象依赖边必须唯一并稳定排序")
        if any(
            parent not in self.dependency_chain or child not in self.dependency_chain
            for parent, child in self.dependency_edges
        ):
            raise CatalogDriftError("对象依赖边引用了依赖图外对象")
        partition_values = (
            self.partition_rows_upper,
            self.partition_uncompressed_bytes_upper,
        )
        if any(
            value is not None and (type(value) is not int or value < 0)
            for value in partition_values
        ):
            raise CatalogDriftError("对象分区上界必须是非负整数或空")
        if type(self.partition_count) is not int or self.partition_count < 0:
            raise CatalogDriftError("对象分区数量必须是非负整数")
        partition_fields = (
            self.partition_count,
            self.partition_rows_upper,
            self.partition_uncompressed_bytes_upper,
            self.partition_key,
            self.partition_bound_method,
        )
        if self.partition_bound_method is None:
            if any(value not in {0, None} for value in partition_fields[:-1]):
                raise CatalogDriftError("分区证据字段不能脱离分区上界方法")
        elif self.partition_count == 0:
            if partition_values != (0, 0) or self.partition_key is not None:
                raise CatalogDriftError("空分区执行证据无效")
        elif (
            self.partition_rows_upper is None
            or self.partition_uncompressed_bytes_upper is None
            or not self.partition_key
        ):
            raise CatalogDriftError("分区执行证据不完整")

    @property
    def evidence_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "object_name": self.object_name,
            "object_kind": self.object_kind,
            "dependency_chain": list(self.dependency_chain),
            "source_rows_upper": self.source_rows_upper,
            "expanded_rows_upper": self.expanded_rows_upper,
            "variable_width_upper": dict(self.variable_width_upper),
            "has_json_expansion": self.has_json_expansion,
            "has_window": self.has_window,
            "has_order_by": self.has_order_by,
            "query_scope_hash": self.query_scope_hash,
            "database_revision": self.database_revision,
            "expansion_bound_method": self.expansion_bound_method,
            "dependency_edges": [list(edge) for edge in self.dependency_edges],
            "partition_count": self.partition_count,
            "partition_rows_upper": self.partition_rows_upper,
            "partition_uncompressed_bytes_upper": self.partition_uncompressed_bytes_upper,
            "partition_key": self.partition_key,
            "partition_bound_method": self.partition_bound_method,
            "method": self.method,
        }


_JSON_EXPANSION = re.compile(r"\b(?:json_each|unnest)\s*\(", re.IGNORECASE)
_WINDOW = re.compile(r"\bover\s*\(", re.IGNORECASE)
_ORDER_BY = re.compile(r"\border\s+by\b", re.IGNORECASE)
_RELATION_REFERENCE = re.compile(
    r"\b(?:from|join)\s+(?:(?:\"?main\"?)\.)?\"?([A-Za-z_][A-Za-z0-9_]*)\"?",
    re.IGNORECASE,
)
_UNBOUNDED_SET_OPERATION = re.compile(r"\bunion\b(?!\s+all\b)", re.IGNORECASE)
_MULTI_SOURCE_SHAPE = re.compile(
    r"\b(?:union\s+all|(?:left\s+|right\s+|full\s+|inner\s+|cross\s+)?join|exists\s*\()",
    re.IGNORECASE,
)
_UNION_ALL = re.compile(r"\bunion\s+all\b", re.IGNORECASE)
_JOIN = re.compile(
    r"\b(?:left\s+|right\s+|full\s+|inner\s+|cross\s+)?join\b",
    re.IGNORECASE,
)
_EXISTS = re.compile(r"\bexists\s*\(", re.IGNORECASE)
_UNSUPPORTED_VIEW_SHAPE = re.compile(
    r"\b(?:with\s+(?:recursive\s+)?|select\s+distinct|intersect|except|"
    r"group\s+by|having|qualify|pivot|unpivot|sample|attach|detach|copy|"
    r"insert|update|delete|merge|call|pragma)\b",
    re.IGNORECASE,
)
_QUALIFIED_RELATION_REFERENCE = re.compile(
    r"\b(?:from|join)\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\.\s*"
    r"\"?([A-Za-z_][A-Za-z0-9_]*)\"?",
    re.IGNORECASE,
)
_RELATION_FUNCTION = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.IGNORECASE,
)
_SUBQUERY = re.compile(r"\(\s*select\b", re.IGNORECASE)
_READ_PARQUET_ARGUMENT = re.compile(
    r"\bread_parquet\s*\(\s*'((?:''|[^'])+)'",
    re.IGNORECASE | re.DOTALL,
)
_MINUTE_PARQUET_PARTITION = re.compile(
    r"(?:^|/)(stock|fund|index|futures)/year=(\d{4})/month=(\d{2})/data\.parquet$"
)
_JSON_SOURCE = re.compile(
    r"\bfrom\s+(?:(?:\"?main\"?)\.)?\"?([A-Za-z_][A-Za-z0-9_]*)\"?"
    r"\s+(?:as\s+)?\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*,\s*"
    r"json_each\s*\(\s*\"?\2\"?\.\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_FINITE_VALUES = re.compile(
    r"\bvalues\s+((?:\(\s*'(?:''|[^'])*'\s*\)\s*,?\s*)+)",
    re.IGNORECASE | re.DOTALL,
)
_FINITE_VALUES_DERIVED_RELATION = re.compile(
    r"\(\s*select\s+\*\s+from\s+\(\s*values\s+"
    r"(?:\(\s*'(?:''|[^'])*'\s*\)\s*,?\s*)+"
    r"\)\s+as\s+\"?[A-Za-z_][A-Za-z0-9_]*\"?\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_DERIVED_RELATION_ALIAS = re.compile(
    r"\s+as\s+\"?[A-Za-z_][A-Za-z0-9_]*\"?\s*"
    r"\(\s*\"?[A-Za-z_][A-Za-z0-9_]*\"?\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_FINITE_VALUE_ALIAS = re.compile(
    r"\)\s+as\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*"
    r"\(\s*\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_STRING_LITERAL = re.compile(r"'((?:''|[^'])*)'")


@dataclass(frozen=True)
class DuckDBObjectShape:
    object_kind: str
    dependency_order: tuple[str, ...]
    dependency_edges: tuple[tuple[str, str], ...]
    has_json_expansion: bool
    has_window: bool
    has_order_by: bool
    has_union_all: bool = False
    has_join: bool = False
    has_exists: bool = False


def _matching_parenthesis(sql: str, opening: int) -> int:
    """返回 SQL 中与 opening 配对的右括号；忽略字符串和双引号标识符。"""

    depth = 0
    quote: str | None = None
    index = opening
    while index < len(sql):
        character = sql[index]
        if quote is not None:
            if character == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if character in {"'", '"'}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
            if depth < 0:
                break
        index += 1
    raise CatalogDriftError("VIEW SQL 括号不闭合")


def _exists_spans(sql: str) -> tuple[tuple[int, int], ...]:
    spans = []
    for match in _EXISTS.finditer(sql):
        opening = sql.find("(", match.start(), match.end())
        spans.append((match.start(), _matching_parenthesis(sql, opening) + 1))
    return tuple(spans)


def _is_inside_spans(position: int, spans: tuple[tuple[int, int], ...]) -> bool:
    return any(start <= position < end for start, end in spans)


def _is_finite_values_derived_relation(sql: str, opening: int) -> bool:
    """识别 DuckDB 对 FROM 中有限 VALUES 关系的规范化结果。"""

    closing = _matching_parenthesis(sql, opening)
    prefix = sql[:opening]
    if re.search(r"(?:\bfrom|\bjoin|,)\s*$", prefix, re.IGNORECASE) is None:
        return False
    relation = sql[opening:closing + 1]
    if _FINITE_VALUES_DERIVED_RELATION.fullmatch(relation) is None:
        return False
    return _DERIVED_RELATION_ALIAS.match(sql, closing + 1) is not None


def _is_structural_query_parenthesis(sql: str, opening: int) -> bool:
    """识别根查询、集合分支或 FROM/JOIN 关系位置的括号 SELECT。"""

    prefix = sql[:opening].rstrip()
    while prefix.endswith("("):
        prefix = prefix[:-1].rstrip()
    return re.search(
        r"(?:\bexists|\bfrom|\bjoin|\bas|\bunion\s+all)\s*$",
        prefix,
        re.IGNORECASE,
    ) is not None


def _validate_supported_view_sql(
    *,
    current: str,
    definition: str,
    names: dict[str, str],
) -> None:
    """只放行本项目真实使用的投影、UNION ALL、JOIN/EXISTS 与两类扫描函数。"""

    unsupported = _UNSUPPORTED_VIEW_SHAPE.search(definition)
    if unsupported is not None:
        raise CatalogDriftError(
            f"VIEW={current} 使用了不受支持的 SQL 形状: {unsupported.group(0)}"
        )
    for schema, _relation in _QUALIFIED_RELATION_REFERENCE.findall(definition):
        if schema.casefold() != "main":
            raise CatalogDriftError(
                f"VIEW={current} 引用了不受支持的 schema: {schema}"
            )
    allowed_functions = {"read_parquet", "json_each", "unnest"}
    unsupported_functions = sorted(
        {
            function
            for function in _RELATION_FUNCTION.findall(definition)
            if function.casefold() not in allowed_functions
        }
    )
    if unsupported_functions:
        raise CatalogDriftError(
            f"VIEW={current} 使用了不受支持的 relation function: "
            f"{unsupported_functions}"
        )
    for match in _SUBQUERY.finditer(definition):
        if (
            not _is_structural_query_parenthesis(definition, match.start())
            and not _is_finite_values_derived_relation(definition, match.start())
        ):
            raise CatalogDriftError(f"VIEW={current} 使用了不受支持的标量子查询")
    unknown_relations = []
    for match in _RELATION_REFERENCE.finditer(definition):
        relation = match.group(1)
        if relation.casefold() in names or relation.casefold() in allowed_functions:
            continue
        unknown_relations.append(relation)
    if unknown_relations:
        raise CatalogDriftError(
            f"VIEW={current} 引用了未登记的关系: {sorted(set(unknown_relations))}"
        )
    if _UNION_ALL.search(definition) is not None and _JOIN.search(definition) is not None:
        raise CatalogDriftError(
            f"VIEW={current} 混合 UNION ALL 与 JOIN，当前无法给出有限执行上界"
        )


def _structural_dependency_rows_upper(
    connection: Any,
    *,
    object_name: str,
    objects: dict[str, tuple[str, str | None]],
) -> int:
    """按物理叶表给出复合 VIEW 同时驻留来源行数的保守上界。"""

    names = {name.casefold(): name for name in objects}
    cache: dict[str, int] = {}

    def upper(current: str) -> int:
        cached = cache.get(current)
        if cached is not None:
            return cached
        kind, definition = objects[current]
        if kind == "table":
            row = connection.execute(
                f"SELECT COUNT(*) FROM {_quote_identifier(current)}"
            ).fetchone()
            if row is None or len(row) != 1:
                raise CatalogDriftError(f"物理叶表行数统计无效: {current}")
            result = int(row[0])
            if result < 0:
                raise CatalogDriftError(f"物理叶表行数统计为负数: {current}")
            cache[current] = result
            return result
        if kind != "view" or not isinstance(definition, str):
            raise CatalogDriftError(f"对象类型不受支持: {current}")
        dependencies = tuple(
            dict.fromkeys(
                dependency
                for match in _RELATION_REFERENCE.finditer(definition)
                if (dependency := names.get(match.group(1).casefold())) is not None
                and dependency != current
            )
        )
        if not dependencies:
            raise CatalogDriftError(f"VIEW={current} 缺少可估算的主关系")
        child_bounds = tuple(upper(dependency) for dependency in dependencies)
        if len(child_bounds) == 1:
            result = child_bounds[0]
        elif _MULTI_SOURCE_SHAPE.search(definition) is not None:
            # JOIN 的输出行数已由本次范围 COUNT 精确给出。这里估算的是执行时
            # 可能同时驻留的输入，不应把连接两侧相乘成一个并不存在的内存表。
            # UNION ALL 通常可流式执行，求和仍是安全且有限的上界。
            result = sum(child_bounds)
        else:
            raise CatalogDriftError(
                f"VIEW={current} 的多来源关系无法给出有限执行上界"
            )
        cache[current] = result
        return result

    return upper(object_name)


def classify_duckdb_object_shape(
    object_name: str,
    objects: dict[str, tuple[str, str | None]],
) -> DuckDBObjectShape:
    """只识别有界投影、UNION ALL、连接 VIEW 及 Parquet VIEW 的依赖图。"""

    if object_name not in objects:
        raise CatalogDriftError(f"物理对象不存在: {object_name}")
    names = {name.casefold(): name for name in objects}
    order: list[str] = []
    edges: set[tuple[str, str]] = set()
    active: set[str] = set()
    visited: set[str] = set()
    flags = {
        "json": False,
        "window": False,
        "order": False,
        "parquet": False,
        "union_all": False,
        "join": False,
        "exists": False,
    }

    def visit(current: str) -> None:
        if current in active:
            raise CatalogDriftError(f"VIEW 依赖形成环: {object_name}")
        if current in visited:
            return
        active.add(current)
        visited.add(current)
        order.append(current)
        kind, definition = objects[current]
        if kind == "table":
            active.remove(current)
            return
        if kind != "view" or not isinstance(definition, str):
            raise CatalogDriftError(f"对象类型不受支持: {object_name}")
        _validate_supported_view_sql(
            current=current,
            definition=definition,
            names=names,
        )
        if _UNBOUNDED_SET_OPERATION.search(definition):
            raise CatalogDriftError(f"VIEW={current} 使用了不受支持的去重 UNION")
        flags["json"] = flags["json"] or _JSON_EXPANSION.search(definition) is not None
        flags["window"] = flags["window"] or _WINDOW.search(definition) is not None
        flags["order"] = flags["order"] or _ORDER_BY.search(definition) is not None
        flags["union_all"] = (
            flags["union_all"] or _UNION_ALL.search(definition) is not None
        )
        flags["join"] = flags["join"] or _JOIN.search(definition) is not None
        flags["exists"] = flags["exists"] or _EXISTS.search(definition) is not None
        has_parquet_source = _READ_PARQUET_ARGUMENT.search(definition) is not None
        if has_parquet_source:
            flags["parquet"] = True
        dependencies = tuple(
            dict.fromkeys(
                referenced
                for match in _RELATION_REFERENCE.finditer(definition)
                if (referenced := names.get(match.group(1).casefold())) is not None
                and referenced != current
            )
        )
        if len(dependencies) > 1 and _MULTI_SOURCE_SHAPE.search(definition) is None:
            raise CatalogDriftError(
                f"VIEW={current} 的多来源形状不在有界白名单"
            )
        if not dependencies and not has_parquet_source:
            raise CatalogDriftError(f"VIEW={current} 缺少可识别的有限来源")
        for dependency in dependencies:
            edges.add((current, dependency))
            visit(dependency)
        active.remove(current)

    visit(object_name)
    return DuckDBObjectShape(
        "parquet_view" if flags["parquet"] else "physical_table",
        tuple(order),
        tuple(sorted(edges)),
        flags["json"],
        flags["window"],
        flags["order"],
        flags["union_all"],
        flags["join"],
        flags["exists"],
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _scope_filter_values(
    filters: tuple[tuple[str, str, tuple[object, ...]], ...],
    column: str,
) -> tuple[str, ...] | None:
    selected: set[str] | None = None
    for field, operator, values in filters:
        if field.casefold() != column.casefold() or operator not in {"eq", "in"}:
            continue
        current = {str(value) for value in values}
        selected = current if selected is None else selected & current
    return None if selected is None else tuple(sorted(selected))


def _observe_json_expansion_scope(
    connection: Any,
    *,
    definition: str,
    event_column: str,
    range_start: object,
    range_end: object,
    end_inclusive: bool,
    scope_filters: tuple[tuple[str, str, tuple[object, ...]], ...],
    projected_columns: dict[str, str],
    variable_fields: tuple[str, ...],
) -> tuple[int, int, tuple[tuple[str, int], ...]]:
    """从原始 JSON 行做行内统计，不执行 set-returning VIEW。"""

    source = _JSON_SOURCE.search(definition)
    finite = _FINITE_VALUES.search(definition)
    finite_alias = _FINITE_VALUE_ALIAS.search(definition)
    if source is None or finite is None or finite_alias is None:
        raise CatalogDriftError("JSON 展开 VIEW 不属于受支持的单数组有限分类形状")
    source_object, source_alias, json_column = source.groups()
    value_text = finite.group(1)
    value_alias, value_column = finite_alias.groups()
    values = tuple(
        match.group(1).replace("''", "'")
        for match in _STRING_LITERAL.finditer(value_text)
    )
    if not values or len(values) != len(set(values)):
        raise CatalogDriftError("JSON 展开 VIEW 的有限分类集合无效")
    event_pattern = re.compile(
        rf"try_cast\s*\(\s*\"?{re.escape(source_alias)}\"?\.\"?"
        rf"([A-Za-z_][A-Za-z0-9_]*)\"?\s+as\s+\"?date\"?\s*\)\s+as\s+\"?"
        rf"{re.escape(event_column)}\"?",
        re.IGNORECASE | re.DOTALL,
    )
    event_match = event_pattern.search(definition)
    discriminator_pattern = re.compile(
        rf"(?:cast\s*\(\s*)?\"?{re.escape(value_alias)}\"?\."
        rf"\"?{re.escape(value_column)}\"?"
        rf"(?:\s+as\s+varchar\s*\))?\s+as\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?",
        re.IGNORECASE | re.DOTALL,
    )
    discriminator_match = discriminator_pattern.search(definition)
    if event_match is None or discriminator_match is None:
        raise CatalogDriftError("JSON 展开 VIEW 的日期或有限分类投影无法识别")
    discriminator_column = discriminator_match.group(1)
    selected = _scope_filter_values(scope_filters, discriminator_column)
    selected_values = values if selected is None else tuple(
        value for value in values if value in selected
    )
    multiplier = len(selected_values)
    quoted_source = _quote_identifier(source_object)
    quoted_date = _quote_identifier(event_match.group(1))
    quoted_json = _quote_identifier(json_column)
    end_operator = "<=" if end_inclusive else "<"
    try:
        statistics = connection.execute(
            f"""SELECT COUNT(*),
                        COALESCE(SUM(CASE
                            WHEN {quoted_json} IS NULL THEN 0
                            WHEN json_type(TRY_CAST({quoted_json} AS JSON)) = 'ARRAY'
                            THEN json_array_length(TRY_CAST({quoted_json} AS JSON))
                        END), 0),
                        COALESCE(MAX(CASE
                            WHEN json_type(TRY_CAST({quoted_json} AS JSON)) = 'ARRAY'
                            THEN list_max(list_transform(
                                json_extract_string(
                                    TRY_CAST({quoted_json} AS JSON), '$[*]'
                                ),
                                value -> octet_length(encode(value))
                            ))
                        END), 0),
                        COUNT(*) FILTER (
                            WHERE {quoted_json} IS NOT NULL
                              AND COALESCE(
                                  json_type(TRY_CAST({quoted_json} AS JSON)), ''
                              ) <> 'ARRAY'
                        )
                 FROM {quoted_source}
                 WHERE TRY_CAST({quoted_date} AS DATE) >= ?
                   AND TRY_CAST({quoted_date} AS DATE) {end_operator} ?""",
            [range_start, range_end],
        ).fetchone()
    except Exception as exc:
        raise CatalogDriftError("JSON 原始范围统计失败") from exc
    if statistics is None or len(statistics) != 4:
        raise CatalogDriftError("JSON 原始范围统计结果无效")
    if int(statistics[3]) != 0:
        raise CatalogDriftError("JSON 原始范围包含非法 JSON 或非数组值")
    source_rows = int(statistics[0])
    expanded_rows = int(statistics[1]) * multiplier
    element_width = int(statistics[2])
    widths: list[tuple[str, int]] = []
    for field_id in variable_fields:
        output_column = projected_columns[field_id]
        if output_column.casefold() == discriminator_column.casefold():
            width = max(
                (len(value.encode("utf-8")) for value in selected_values),
                default=0,
            )
        else:
            width = element_width
        widths.append((field_id, width))
    return source_rows, expanded_rows, tuple(sorted(widths))


def _coerce_stat_time(value: object) -> date | datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    text = str(value)
    if "T" in text or " " in text:
        return datetime.fromisoformat(text)
    return date.fromisoformat(text)


def _comparable_stat_time(
    value: date | datetime,
    *,
    use_datetime: bool,
) -> date | datetime:
    if use_datetime:
        if isinstance(value, datetime):
            return value
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, datetime):
        return value.date()
    return value


def _month_keys_between(
    start: date | datetime,
    end: date | datetime,
) -> tuple[str, ...]:
    start_day = start.date() if isinstance(start, datetime) else start
    end_day = end.date() if isinstance(end, datetime) else end
    cursor = date(start_day.year, start_day.month, 1)
    last = date(end_day.year, end_day.month, 1)
    result: list[str] = []
    while cursor <= last:
        result.append(f"{cursor.year:04d}-{cursor.month:02d}")
        cursor = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
    return tuple(result)


def _parquet_partition_scope_statistics(
    *,
    definition: str,
    event_column: str,
    scan_columns: tuple[str, ...],
    range_start: object,
    range_end: object,
    end_inclusive: bool,
) -> tuple[int, int, int, int, str]:
    """只读 Parquet footer，并按查询边界选中相交 row group。"""

    match = _READ_PARQUET_ARGUMENT.search(definition)
    if match is None:
        raise CatalogDriftError("Parquet VIEW 缺少可识别的单一文件表达式")
    pattern = match.group(1).replace("''", "'")
    files = tuple(
        sorted(Path(item).resolve() for item in glob.glob(pattern, recursive=True))
    )
    if not files:
        raise CatalogDriftError("Parquet VIEW 的文件表达式没有匹配文件")
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - 正式安装依赖
        raise CatalogDriftError("Parquet footer 检查需要 pyarrow") from exc
    raw_start = _coerce_stat_time(range_start)
    raw_end = _coerce_stat_time(range_end)
    use_datetime = isinstance(raw_start, datetime) or isinstance(raw_end, datetime)
    start = _comparable_stat_time(raw_start, use_datetime=use_datetime)
    end = _comparable_stat_time(raw_end, use_datetime=use_datetime)
    if start > end or (start == end and not end_inclusive):
        raise CatalogDriftError("Parquet 查询范围为空或倒置")
    scope_last = end
    if not end_inclusive:
        scope_last = end - (
            timedelta(microseconds=1)
            if isinstance(end, datetime)
            else timedelta(days=1)
        )
    expected_months = set(_month_keys_between(start, scope_last))
    by_month: dict[str, list[int]] = {}
    selected_paths: dict[str, Path] = {}
    total_rows = 0
    for path in files:
        partition_match = _MINUTE_PARQUET_PARTITION.search(path.as_posix())
        if partition_match is None:
            raise CatalogDriftError(
                f"Parquet VIEW 文件不符合分钟 year/month 分区合同: {path.name}"
            )
        year = int(partition_match.group(2))
        month = int(partition_match.group(3))
        if not 1 <= month <= 12:
            raise CatalogDriftError(f"Parquet 分区月份无效: {path.name}")
        partition_key = f"{year:04d}-{month:02d}"
        if partition_key not in expected_months:
            continue
        if partition_key in selected_paths:
            raise CatalogDriftError(f"分钟月份存在多个 Parquet 文件: {partition_key}")
        selected_paths[partition_key] = path
        by_month[partition_key] = [0, 0]
        parquet = pq.ParquetFile(path)
        schema_names = tuple(parquet.schema_arrow.names)
        required_columns = tuple(dict.fromkeys((event_column, *scan_columns)))
        missing_columns = tuple(
            column for column in required_columns if column not in schema_names
        )
        if missing_columns:
            raise CatalogDriftError(
                f"Parquet 分区缺少扫描列: {path.name}:{list(missing_columns)}"
            )
        column_index = schema_names.index(event_column)
        projected_indices = tuple(
            schema_names.index(column) for column in required_columns
        )
        for row_group_index in range(parquet.metadata.num_row_groups):
            row_group = parquet.metadata.row_group(row_group_index)
            column = row_group.column(column_index)
            statistics = column.statistics
            if (
                statistics is None
                or not statistics.has_min_max
                or statistics.min is None
                or statistics.max is None
            ):
                raise CatalogDriftError(
                    f"Parquet 首尾范围缺少事件时间 footer 统计: {path.name}"
                )
            minimum = _comparable_stat_time(
                _coerce_stat_time(statistics.min),
                use_datetime=use_datetime,
            )
            maximum = _comparable_stat_time(
                _coerce_stat_time(statistics.max),
                use_datetime=use_datetime,
            )
            if isinstance(start, datetime):
                awareness = (
                    start.tzinfo is not None,
                    minimum.tzinfo is not None,
                    maximum.tzinfo is not None,
                )
                if len(set(awareness)) != 1:
                    raise CatalogDriftError(
                        "Parquet footer 与 QueryIR 时间时区形状不一致"
                    )
            partition_start: date | datetime
            partition_end: date | datetime
            if use_datetime:
                partition_start = datetime(year, month, 1, tzinfo=minimum.tzinfo)
                partition_end = (
                    datetime(year + 1, 1, 1, tzinfo=minimum.tzinfo)
                    if month == 12
                    else datetime(year, month + 1, 1, tzinfo=minimum.tzinfo)
                )
            else:
                partition_start = date(year, month, 1)
                partition_end = (
                    date(year + 1, 1, 1)
                    if month == 12
                    else date(year, month + 1, 1)
                )
            if minimum < partition_start or maximum >= partition_end:
                raise CatalogDriftError(
                    f"Parquet footer 时间越出文件月份: {partition_key}"
                )
            if maximum < start or (minimum > end if end_inclusive else minimum >= end):
                continue
            row_group_bytes = sum(
                int(row_group.column(index).total_uncompressed_size)
                for index in projected_indices
            )
            total_rows += int(row_group.num_rows)
            bucket = by_month[partition_key]
            bucket[0] += int(row_group.num_rows)
            bucket[1] += row_group_bytes
    if set(selected_paths) != expected_months:
        missing = sorted(expected_months - set(selected_paths))
        raise CatalogDriftError(f"分钟 Parquet 查询月份缺少文件: {missing}")
    if not by_month:
        return 0, 0, 0, 0, ""
    peak_key, peak = max(
        sorted(by_month.items()),
        key=lambda item: (item[1][1], item[1][0], item[0]),
    )
    return total_rows, len(by_month), peak[0], peak[1], peak_key


@dataclass(frozen=True)
class DriftFinding:
    kind: str
    detail: str
    severity: str


@dataclass(frozen=True)
class DriftAttestation:
    catalog_hash: str
    binding_id: str
    source_profile: str
    environment: str
    expected_schema_revision: str
    current_schema_revision: str
    policy_hash: str
    passed: bool

    @property
    def attestation_hash(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "catalog_hash": self.catalog_hash,
            "binding_id": self.binding_id,
            "source_profile": self.source_profile,
            "environment": self.environment,
            "expected_schema_revision": self.expected_schema_revision,
            "current_schema_revision": self.current_schema_revision,
            "policy_hash": self.policy_hash,
            "passed": self.passed,
        }


def _duckdb() -> Any:
    try:
        import duckdb
    except ImportError as exc:
        raise CatalogDriftError("DuckDB 不可用") from exc
    return duckdb


class DuckDBSourceInspector:
    def __init__(self, path: str | Path, *, source_profile: str, environment: str) -> None:
        self.path = Path(path)
        self.source_profile = source_profile
        self.environment = environment

    def observe_current_schema(self, object_name: str) -> PhysicalInventory:
        with _duckdb().connect(str(self.path), read_only=True) as connection:
            rows = connection.execute(
                """SELECT column_name, data_type, is_nullable, column_index
                   FROM duckdb_columns()
                   WHERE schema_name = 'main' AND table_name = ?
                   ORDER BY column_index""",
                [object_name],
            ).fetchall()
        if not rows:
            raise CatalogDriftError(f"物理对象不存在: {object_name}")
        return PhysicalInventory(
            "duckdb", self.source_profile, self.environment, object_name,
            tuple(
                PhysicalColumn(
                    str(name),
                    str(dtype),
                    bool(nullable)
                    if isinstance(nullable, bool)
                    else str(nullable).upper() == "YES",
                    int(position),
                )
                for name, dtype, nullable, position in rows
            ),
        )

    def observe_execution_evidence(
        self,
        object_name: str,
        *,
        projected_columns: dict[str, str],
        variable_fields: tuple[str, ...],
        query_scope_hash: str,
        statistics_sql: str | None = None,
        statistics_parameters: tuple[object, ...] = (),
        event_column: str | None = None,
        range_start: object | None = None,
        range_end: object | None = None,
        range_end_inclusive: bool = True,
        scope_filters: tuple[
            tuple[str, str, tuple[object, ...]], ...
        ] = (),
    ) -> ObjectExecutionEvidence:
        """读取对象定义与有限统计；JSON 展开不会通过执行 VIEW 来猜倍率。"""

        with _duckdb().connect(str(self.path), read_only=True) as connection:
            rows = connection.execute(
                """SELECT table_name, 'table' AS object_kind, NULL AS definition
                   FROM duckdb_tables() WHERE schema_name = 'main'
                   UNION ALL
                   SELECT view_name, 'view' AS object_kind, sql AS definition
                   FROM duckdb_views() WHERE schema_name = 'main'"""
            ).fetchall()
            objects = {
                str(name): (str(kind), None if definition is None else str(definition))
                for name, kind, definition in rows
            }
            shape = classify_duckdb_object_shape(object_name, objects)
            source_rows = None
            expanded_rows = None
            variable_widths: list[tuple[str, int]] = []
            partition_count = 0
            partition_rows = None
            partition_uncompressed_bytes = None
            partition_key = None
            partition_bound_method = None
            # JSON/UNNEST 的中间倍率未知时，禁止为了取得统计而真实展开 VIEW。
            if shape.has_json_expansion:
                definitions = tuple(
                    definition
                    for dependency in shape.dependency_order
                    if isinstance((definition := objects[dependency][1]), str)
                    and _JSON_EXPANSION.search(definition) is not None
                )
                if (
                    len(definitions) != 1
                    or event_column is None
                    or range_start is None
                    or range_end is None
                ):
                    raise CatalogDriftError("JSON 展开缺少查询范围统计输入")
                definition = definitions[0]
                source_rows, expanded_rows, observed_widths = (
                    _observe_json_expansion_scope(
                        connection,
                        definition=definition,
                        event_column=event_column,
                        range_start=range_start,
                        range_end=range_end,
                        end_inclusive=range_end_inclusive,
                        scope_filters=scope_filters,
                        projected_columns=projected_columns,
                        variable_fields=variable_fields,
                    )
                )
                variable_widths.extend(observed_widths)
            elif shape.object_kind == "parquet_view":
                definitions = tuple(
                    definition
                    for dependency in shape.dependency_order
                    if isinstance((definition := objects[dependency][1]), str)
                    and _READ_PARQUET_ARGUMENT.search(definition) is not None
                )
                if (
                    len(definitions) != 1
                    or event_column is None
                    or range_start is None
                    or range_end is None
                ):
                    raise CatalogDriftError("Parquet VIEW 缺少查询范围分区输入")
                definition = definitions[0]
                (
                    source_rows,
                    partition_count,
                    partition_rows,
                    partition_uncompressed_bytes,
                    raw_partition_key,
                ) = _parquet_partition_scope_statistics(
                    definition=definition,
                    event_column=event_column,
                    scan_columns=tuple(projected_columns.values()),
                    range_start=range_start,
                    range_end=range_end,
                    end_inclusive=range_end_inclusive,
                )
                partition_key = raw_partition_key or None
                partition_bound_method = "parquet_footer_month_scope_v1"
            elif statistics_sql is not None:
                statistics = connection.execute(
                    statistics_sql,
                    list(statistics_parameters),
                ).fetchone()
                if statistics is None or len(statistics) != 1 + len(variable_fields):
                    raise CatalogDriftError("对象范围统计结果 schema 无效")
                source_rows = int(statistics[0])
                variable_widths = [
                    (field_id, 0 if maximum is None else int(maximum))
                    for field_id, maximum in zip(
                        variable_fields,
                        statistics[1:],
                        strict=True,
                    )
                ]
                if shape.has_union_all or shape.has_join:
                    structural_rows = _structural_dependency_rows_upper(
                        connection,
                        object_name=object_name,
                        objects=objects,
                    )
                    source_rows = max(source_rows, structural_rows)
        stat = self.path.stat()
        database_revision = typed_canonical_hash(
            {
                "path_name": self.path.name,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
        return ObjectExecutionEvidence(
            object_name=object_name,
            object_kind=shape.object_kind,
            dependency_chain=shape.dependency_order,
            source_rows_upper=source_rows,
            expanded_rows_upper=expanded_rows,
            variable_width_upper=tuple(sorted(variable_widths)),
            has_json_expansion=shape.has_json_expansion,
            has_window=shape.has_window,
            has_order_by=shape.has_order_by,
            query_scope_hash=query_scope_hash,
            database_revision=database_revision,
            expansion_bound_method=(
                "database_scope_json_array_length_v1"
                if expanded_rows is not None
                else None
            ),
            dependency_edges=shape.dependency_edges,
            partition_count=partition_count,
            partition_rows_upper=partition_rows,
            partition_uncompressed_bytes_upper=partition_uncompressed_bytes,
            partition_key=partition_key,
            partition_bound_method=partition_bound_method,
        )


class ParquetSourceInspector:
    def __init__(self, path: str | Path, *, source_profile: str, environment: str) -> None:
        self.path = Path(path)
        self.source_profile = source_profile
        self.environment = environment

    def observe_current_schema(self, object_name: str | None = None) -> PhysicalInventory:
        if not self.path.is_file():
            raise CatalogDriftError(f"Parquet 文件不存在: {self.path.name}")
        escaped = str(self.path).replace("'", "''")
        with _duckdb().connect(":memory:") as connection:
            rows = connection.execute(f"DESCRIBE SELECT * FROM read_parquet('{escaped}') LIMIT 0").fetchall()
        name = object_name or self.path.name
        return PhysicalInventory(
            "parquet", self.source_profile, self.environment, name,
            tuple(PhysicalColumn(str(row[0]), str(row[1]), str(row[2]).upper() == "YES", index) for index, row in enumerate(rows)),
        )


def require_approval(decision: ApprovalDecision, *, target_kind: str, target_id: str, target_hash: str) -> None:
    if (decision.target_kind, decision.target_id, decision.target_hash, decision.decision) != (target_kind, target_id, target_hash, "approved"):
        raise CatalogReferenceError(f"{target_kind} {target_id} 缺少匹配的 approved 决定")


def evaluate_current_drift(
    *,
    catalog_hash: str,
    binding: PhysicalBindingContract,
    policy: PolicyContract,
    inventory: PhysicalInventory,
) -> tuple[tuple[DriftFinding, ...], DriftAttestation]:
    if policy.policy_type != "schema_drift" or binding.drift_policy_id != policy.policy_id:
        raise CatalogDriftError("binding 与 SchemaDriftPolicy 不匹配")
    if (inventory.source_profile, inventory.environment, inventory.object_name) != (binding.source_profile, binding.environment, binding.object_name):
        raise CatalogDriftError("当前物理 inventory 与 binding 不匹配")
    findings: list[DriftFinding] = []
    if inventory.schema_revision != binding.expected_schema_revision:
        action = str(policy.rules.get("schema_changed", "reject"))
        findings.append(DriftFinding("schema_changed", "物理 schema revision 已变化", "warning" if action == "warn" else "error"))
    if any(item.severity == "error" for item in findings):
        raise CatalogDriftError("检测到阻断性 schema drift")
    attestation = DriftAttestation(
        catalog_hash, binding.binding_id, binding.source_profile, binding.environment,
        binding.expected_schema_revision, inventory.schema_revision, policy.content_hash, True,
    )
    return tuple(findings), attestation


__all__ = [
    "DriftAttestation", "DriftFinding", "DuckDBObjectShape", "DuckDBSourceInspector", "ObjectExecutionEvidence", "ParquetSourceInspector",
    "PhysicalColumn", "PhysicalInventory", "evaluate_current_drift", "require_approval",
    "classify_duckdb_object_shape",
]
