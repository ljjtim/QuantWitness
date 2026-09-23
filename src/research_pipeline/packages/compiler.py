"""把 ResearchPackage 查询声明解析为纯 Query IR 编译结果。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Mapping

from research_pipeline.data_plane import (
    DATE_RANGE_V1_VERSION,
    QUERY_IR_V2_VERSION,
    DateRangeV1,
    FilterOperator,
    FilterPredicate,
    InstantRangeV2,
    QueryBudget,
    QueryIR,
    QueryIRInvalidError,
    QueryPurpose,
    SortKey,
    UniverseSelection,
)
from .models import ResearchPackage, ResearchPackageError
from .plan_contracts import QueryCompileResult


def compile_package_queries(package: ResearchPackage) -> QueryCompileResult:
    """编译所有 builder 共享的 requests/as_of，不解释图或平台准入。"""
    payload = package.spec_payload
    as_of = payload.get("as_of")
    if not isinstance(as_of, str) or not as_of.strip():
        raise ResearchPackageError("spec.as_of 必须是规范 ISO 日期或带时区时间")
    return compile_query_requests(payload.get("requests"), as_of=as_of)


def compile_query_requests(requests: object, *, as_of: str) -> QueryCompileResult:
    """供所有声明式 builder 复用的 Query IR 编译入口。"""
    if not isinstance(requests, (list, tuple)) or not requests:
        raise ResearchPackageError("spec.requests 必须是非空序列")
    request_ids: list[str] = []
    queries: list[QueryIR] = []
    for index, raw in enumerate(requests):
        if not isinstance(raw, Mapping):
            raise ResearchPackageError(f"spec.requests[{index}] 必须是映射")
        _exact(raw, {"request_id", "dataset_id", "dataset_version", "field_ids", "purpose", "time_range", "universe", "filters", "sort", "budget", "adjustment", "limit"}, f"spec.requests[{index}]")
        request_id = _text(raw["request_id"], f"spec.requests[{index}].request_id")
        query = _build_query(raw, as_of=as_of, index=index)
        request_ids.append(request_id)
        queries.append(query)
    if len(request_ids) != len(set(request_ids)):
        raise ResearchPackageError("request_id 不能重复")
    return QueryCompileResult(tuple(request_ids), tuple(queries))

def _build_query(raw: Mapping[str, object], *, as_of: str, index: int) -> QueryIR:
    prefix = f"spec.requests[{index}]"
    time_range, ir_version = _build_time_range(raw["time_range"], prefix)
    universe_payload = _mapping(raw["universe"], {"instruments", "snapshot_id"}, f"{prefix}.universe")
    budget_payload = _mapping(raw["budget"], {"max_rows", "max_bytes", "batch_size"}, f"{prefix}.budget")
    filters = tuple(_build_filter(item, prefix, offset) for offset, item in enumerate(_sequence(raw["filters"], f"{prefix}.filters", allow_empty=True)))
    sort = tuple(_build_sort(item, prefix, offset) for offset, item in enumerate(_sequence(raw["sort"], f"{prefix}.sort")))
    instruments = _strings(universe_payload["instruments"], f"{prefix}.universe.instruments", allow_empty=True)
    snapshot = universe_payload["snapshot_id"]
    if snapshot is not None:
        snapshot = _text(snapshot, f"{prefix}.universe.snapshot_id")
    try:
        purpose = QueryPurpose(_text(raw["purpose"], f"{prefix}.purpose"))
    except ValueError as exc:
        raise ResearchPackageError(f"{prefix} 的 purpose 无效") from exc
    dataset_version = raw["dataset_version"]
    limit = raw["limit"]
    if type(dataset_version) is not int or (limit is not None and type(limit) is not int):
        raise ResearchPackageError(f"{prefix} 的 dataset_version/limit 类型无效")
    try:
        return QueryIR(
            dataset_id=_text(raw["dataset_id"], f"{prefix}.dataset_id"),
            dataset_version=dataset_version,
            field_ids=_strings(raw["field_ids"], f"{prefix}.field_ids"),
            purpose=purpose,
            time_range=time_range,
            universe=UniverseSelection(instruments, snapshot),
            filters=filters,
            sort=sort,
            budget=QueryBudget(_integer(budget_payload["max_rows"], f"{prefix}.budget.max_rows"), _integer(budget_payload["max_bytes"], f"{prefix}.budget.max_bytes"), _integer(budget_payload["batch_size"], f"{prefix}.budget.batch_size")),
            adjustment=_text(raw["adjustment"], f"{prefix}.adjustment"),
            limit=limit,
            as_of=_request_as_of(as_of, time_range),
            ir_version=ir_version,
        )
    except QueryIRInvalidError as exc:
        raise ResearchPackageError(f"{prefix} Query IR 无效: {exc}") from exc


def _request_as_of(as_of: str, time_range: DateRangeV1 | InstantRangeV2) -> str:
    """混合频率包共用决策时刻时，日频只消费对应本地日期。"""

    if isinstance(time_range, InstantRangeV2):
        return as_of
    try:
        return date.fromisoformat(as_of).isoformat()
    except ValueError:
        try:
            return datetime.fromisoformat(as_of).date().isoformat()
        except ValueError:
            return as_of


def _build_time_range(value: object, prefix: str) -> tuple[DateRangeV1 | InstantRangeV2, str]:
    field = f"{prefix}.time_range"
    if not isinstance(value, Mapping):
        raise ResearchPackageError(f"{field} 必须是映射")
    # 未标记的旧形状只解释为 v1，绝不根据 start_at/end_at 猜测 v2。
    if "contract_version" not in value:
        payload = _mapping(value, {"start", "end"}, field)
        try:
            return (
                DateRangeV1(
                    date.fromisoformat(_text(payload["start"], f"{field}.start")),
                    date.fromisoformat(_text(payload["end"], f"{field}.end")),
                ),
                "query-ir-v1",
            )
        except (ValueError, QueryIRInvalidError) as exc:
            raise ResearchPackageError(f"{field} 日频范围无效") from exc
    version = _text(value["contract_version"], f"{field}.contract_version")
    if version == DATE_RANGE_V1_VERSION:
        payload = _mapping(
            value,
            {"contract_version", "start_date", "end_date"},
            field,
        )
        try:
            return (
                DateRangeV1(
                    date.fromisoformat(_text(payload["start_date"], f"{field}.start_date")),
                    date.fromisoformat(_text(payload["end_date"], f"{field}.end_date")),
                ),
                "query-ir-v1",
            )
        except (ValueError, QueryIRInvalidError) as exc:
            raise ResearchPackageError(f"{field} 日频范围无效") from exc
    if version == "instant-range-v2":
        try:
            return InstantRangeV2.from_dict(dict(value)), QUERY_IR_V2_VERSION
        except QueryIRInvalidError as exc:
            raise ResearchPackageError(f"{field} 分钟范围无效: {exc}") from exc
    raise ResearchPackageError(f"{field}.contract_version 不受支持: {version}")


def _build_filter(value: object, prefix: str, index: int) -> FilterPredicate:
    payload = _mapping(value, {"field_id", "operator", "values"}, f"{prefix}.filters[{index}]")
    try:
        operator = FilterOperator(_text(payload["operator"], "filter.operator"))
    except ValueError as exc:
        raise ResearchPackageError("filter.operator 不受支持") from exc
    return FilterPredicate(_text(payload["field_id"], "filter.field_id"), operator, tuple(_sequence(payload["values"], "filter.values", allow_empty=operator == FilterOperator.IS_NULL)))


def _build_sort(value: object, prefix: str, index: int) -> SortKey:
    payload = _mapping(value, {"field_id", "descending"}, f"{prefix}.sort[{index}]")
    if type(payload["descending"]) is not bool:
        raise ResearchPackageError("sort.descending 必须是布尔值")
    return SortKey(_text(payload["field_id"], "sort.field_id"), payload["descending"])


def _exact(value: Mapping[str, object], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise ResearchPackageError(f"{field} schema 不匹配；缺失={sorted(expected - set(value))}，未知={sorted(set(value) - expected)}")


def _mapping(value: object, expected: set[str], field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ResearchPackageError(f"{field} 必须是映射")
    _exact(value, expected, field)
    return value


def _sequence(value: object, field: str, *, allow_empty: bool = False) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)) or (not value and not allow_empty):
        raise ResearchPackageError(f"{field} 必须是{'可空' if allow_empty else '非空'}序列")
    return tuple(value)


def _strings(value: object, field: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    values = _sequence(value, field, allow_empty=allow_empty)
    if any(not isinstance(item, str) or not item.strip() for item in values):
        raise ResearchPackageError(f"{field} 必须是字符串序列")
    return tuple(values)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchPackageError(f"{field} 必须是非空字符串")
    return value


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise ResearchPackageError(f"{field} 必须是整数")
    return value


__all__ = [
    "compile_package_queries", "compile_query_requests",
]
