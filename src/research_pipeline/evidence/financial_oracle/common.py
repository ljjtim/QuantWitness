"""金融复核领域共用的行级原语。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Mapping, Sequence

import pyarrow as pa

from ..errors import EvidenceContractError
from ..oracle_workspace import (
    OracleTable,
    OracleWorkspace,
    ResultTableSource,
    arrow_table_source,
    quoted_identifier,
)


def key(row: Mapping[str, object], *fields: str) -> tuple[str, ...]:
    return tuple(str(row[field]) for field in fields)


def require_unique(
    rows: Sequence[dict[str, object]],
    fields: tuple[str, ...],
    label: str,
) -> None:
    if isinstance(rows, OracleTable):
        columns = ", ".join(quoted_identifier(field) for field in fields)
        previous: tuple[object, ...] | None = None
        for row in rows.workspace.iter_query(
            f"SELECT {columns} FROM {rows.name} ORDER BY {columns}"
        ):
            current = tuple(normalized(row[field]) for field in fields)
            if current == previous:
                raise EvidenceContractError(f"{label} 主键重复")
            previous = current
        return
    keys = [key(row, *fields) for row in rows]
    if len(keys) != len(set(keys)):
        raise EvidenceContractError(f"{label} 主键重复")


def ordered_rows(
    rows: Sequence[dict[str, object]],
    *,
    order_by: Sequence[str],
) -> Iterable[dict[str, object]]:
    if isinstance(rows, OracleTable):
        return rows.iter_rows(order_by=order_by)
    return iter(
        sorted(
            rows,
            key=lambda row: tuple(normalized(row[item]) for item in order_by),
        )
    )


def integer(value: object, label: str, *, minimum: int | None = None) -> int:
    if type(value) is bool or not isinstance(value, int):
        raise EvidenceContractError(f"{label} 必须是整数")
    if minimum is not None and value < minimum:
        raise EvidenceContractError(f"{label} 小于允许下限")
    return value


def aware_datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise EvidenceContractError(f"{field} 不是有效时间") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise EvidenceContractError(f"{field} 必须带时区")
    return result


def date_value(value: object, field: str) -> date:
    if isinstance(value, datetime):
        raise EvidenceContractError(f"{field} 必须是日期")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise EvidenceContractError(f"{field} 不是有效日期") from exc


def ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def create_mapping_table(
    workspace: OracleWorkspace,
    *,
    name: str,
    rows: Sequence[Mapping[str, object]],
    schema: pa.Schema,
) -> None:
    try:
        table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise EvidenceContractError(f"Bar TCA {name} 行 schema 无效") from exc
    registration = f"_{name}_arrow"
    assert workspace.connection is not None
    workspace.connection.register(registration, table)
    try:
        workspace.execute(f"CREATE TABLE {name} AS SELECT * FROM {registration}")
    finally:
        workspace.connection.unregister(registration)


def context_table_rows(
    table: pa.Table | ResultTableSource | OracleTable | list[dict[str, object]],
    *,
    expected_columns: set[str] | None,
    label: str,
) -> Sequence[dict[str, object]]:
    if isinstance(table, OracleTable):
        if (
            expected_columns is not None
            and table.schema is not None
            and set(table.schema.names) != expected_columns
        ):
            raise EvidenceContractError(f"{label} schema 不匹配")
        return table
    if isinstance(table, list):
        if expected_columns is not None and any(
            set(row) != expected_columns for row in table
        ):
            raise EvidenceContractError(f"{label} schema 不匹配")
        return table
    return read_source_rows(
        arrow_table_source(table),
        expected_columns=expected_columns,
        label=label,
    )


def read_source_rows(
    source: ResultTableSource,
    *,
    expected_columns: set[str] | None,
    label: str,
) -> list[dict[str, object]]:
    if expected_columns is not None and set(source.schema.names) != expected_columns:
        raise EvidenceContractError(f"{label} schema 不匹配")
    rows: list[dict[str, object]] = []
    for batch in source.iter_batches():
        rows.extend(dict(row) for row in batch.to_pylist())
    if len(rows) != source.row_count:
        raise EvidenceContractError(f"{label} 行数与 Result manifest 不一致")
    return rows


def require_no_rows(
    workspace: OracleWorkspace,
    sql: str,
    message: str,
) -> None:
    if workspace.execute(sql).fetchone() is not None:
        raise EvidenceContractError(message)


def iso(value: object) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def normalized(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def normalized_mapping(value: Mapping[str, object]) -> dict[str, object]:
    return {str(item_key): normalized(item) for item_key, item in value.items()}


__all__ = [
    "aware_datetime",
    "ceil_ratio",
    "context_table_rows",
    "create_mapping_table",
    "date_value",
    "integer",
    "iso",
    "key",
    "normalized",
    "normalized_mapping",
    "ordered_rows",
    "read_source_rows",
    "require_no_rows",
    "require_unique",
]
