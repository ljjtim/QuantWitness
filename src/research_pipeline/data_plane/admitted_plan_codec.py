"""当前已准入 QueryPlan 的严格 JSON 重建。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from factor_contracts import FactorPublicationBinding

from .admission import AdmittedQueryPlan
from .errors import QueryIRInvalidError
from .query_ir import (
    QUERY_IR_V2_VERSION,
    QUERY_IR_VERSION,
    DateRangeV1,
    FilterOperator,
    FilterPredicate,
    InstantRangeV2,
    QueryBudget,
    QueryIR,
    QueryPurpose,
    SortKey,
    UniverseSelection,
)
from .temporal import SessionCloseSelection, TemporalSelectionPlan


_RETIRED_MINUTE_VISIBILITY_FIELDS = {
    "minute_source_visibility_hash",
    "minute_source_revisions",
    "minute_source_visibility_contract_version",
}


def admitted_plan_from_dict(payload: dict[str, Any]) -> AdmittedQueryPlan:
    retired_fields = sorted(_RETIRED_MINUTE_VISIBILITY_FIELDS & set(payload))
    if retired_fields:
        raise QueryIRInvalidError(
            f"旧分钟来源可见性 QueryPlan 不受支持: {retired_fields}"
        )
    if payload.get("plan_version") != "admitted-query-plan-v6":
        raise QueryIRInvalidError(
            f"AdmittedQueryPlan 版本不受支持: {payload.get('plan_version')}"
        )
    if "temporal_selection" not in payload or "input_claim_ceiling" not in payload:
        raise QueryIRInvalidError("旧 QueryPlan 缺少可执行时态选择或结论上限")
    query_raw = payload["query"]
    ir_version = str(query_raw["ir_version"])
    if ir_version == QUERY_IR_VERSION:
        time_raw = query_raw["time_range"]
        if set(time_raw) != {"start", "end"}:
            raise QueryIRInvalidError("Query IR v1 时间范围 schema 不匹配")
        time_range = DateRangeV1(
            date.fromisoformat(time_raw["start"]),
            date.fromisoformat(time_raw["end"]),
        )
    elif ir_version == QUERY_IR_V2_VERSION:
        time_range = InstantRangeV2.from_dict(query_raw["time_range"])
    else:
        raise QueryIRInvalidError(f"Query IR 版本不受支持: {ir_version}")
    query = QueryIR(
        str(query_raw["dataset_id"]),
        int(query_raw["dataset_version"]),
        tuple(str(item) for item in query_raw["field_ids"]),
        QueryPurpose(query_raw["purpose"]),
        time_range,
        UniverseSelection(tuple(query_raw["universe"].get("instruments", ())), query_raw["universe"].get("snapshot_id")),
        tuple(FilterPredicate(str(item["field_id"]), FilterOperator(item["operator"]), tuple(item["values"])) for item in query_raw.get("filters", ())),
        tuple(SortKey(str(item["field_id"]), bool(item.get("descending", False))) for item in query_raw["sort"]),
        QueryBudget(**query_raw["budget"]),
        adjustment=str(query_raw.get("adjustment", "none")),
        limit=query_raw.get("limit"),
        as_of=query_raw.get("as_of"),
        ir_version=ir_version,
    )
    daily_availability = payload.get("daily_availability")
    if "daily_availability" not in payload:
        daily_policy_ref = None
        daily_rule = None
    elif (
        not isinstance(daily_availability, Mapping)
        or set(daily_availability) != {"policy_ref", "available_after"}
        or not all(
            isinstance(daily_availability[field], str)
            and daily_availability[field]
            for field in ("policy_ref", "available_after")
        )
    ):
        raise QueryIRInvalidError("daily_availability schema 不匹配")
    else:
        daily_policy_ref = str(daily_availability["policy_ref"])
        daily_rule = str(daily_availability["available_after"])
    session_close_raw = payload.get("session_close")
    session_close = (
        None
        if session_close_raw is None
        else SessionCloseSelection.from_dict(session_close_raw)
    )
    plan = AdmittedQueryPlan(
        query=query, catalog_hash=str(payload["catalog_hash"]),
        binding_id=str(payload["binding_id"]), binding_version=int(payload["binding_version"]),
        attestation_hash=str(payload["attestation_hash"]), source_profile=str(payload["source_profile"]),
        environment=str(payload["environment"]), expected_schema_revision=str(payload["expected_schema_revision"]),
        availability_policy_hash=str(payload["availability_policy_hash"]), revision_policy_hash=str(payload["revision_policy_hash"]),
        object_name=str(payload["object_name"]), columns=tuple((str(key), str(value)) for key, value in payload["columns"].items()),
        field_types=tuple((str(key), str(value)) for key, value in payload["field_types"].items()),
        field_nullables=tuple((str(key), bool(value)) for key, value in payload["field_nullables"].items()),
        primary_key=tuple(str(item) for item in payload["primary_key"]), event_time_field=str(payload["event_time_field"]),
        instrument_field=(
            None
            if payload["instrument_field"] is None
            else str(payload["instrument_field"])
        ),
        temporal_selection=TemporalSelectionPlan.from_dict(payload["temporal_selection"]),
        input_claim_ceiling=str(payload["input_claim_ceiling"]),
        session_close_binding=session_close,
        plan_version=str(payload["plan_version"]),
        daily_availability_policy_ref=daily_policy_ref,
        daily_availability_rule=daily_rule,
        minute_dataset_semantics_hash=payload.get("minute_dataset_semantics_hash"),
        minute_source_semantics_hash=payload.get("minute_source_semantics_hash"),
        minute_scope_binding_hash=payload.get("minute_scope_binding_hash"),
        minute_capability_manifest_hash=payload.get(
            "minute_capability_manifest_hash"
        ),
        minute_asset_class=payload.get("minute_asset_class"),
        minute_instrument_role=payload.get("minute_instrument_role"),
        minute_session_policy_ref=payload.get("minute_session_policy_ref"),
        minute_quality_policy_refs=(
            None
            if payload.get("minute_quality_policy_refs") is None
            else tuple(str(item) for item in payload["minute_quality_policy_refs"])
        ),
        minute_timezone=payload.get("minute_timezone"),
        minute_timestamp_storage=payload.get("minute_timestamp_storage"),
        minute_timestamp_role=payload.get("minute_timestamp_role"),
        minute_bar_interval=payload.get("minute_bar_interval"),
        minute_availability_rule=payload.get("minute_availability_rule"),
        minute_time_normalization_version=payload.get("minute_time_normalization_version"),
        result_cardinality=str(payload.get("result_cardinality", "one_or_more")),
        factor_publication=(
            None
            if payload.get("factor_publication") is None
            else FactorPublicationBinding.from_dict(payload["factor_publication"])
        ),
    )
    if payload.get("plan_hash") != plan.plan_hash:
        raise QueryIRInvalidError("序列化 QueryPlan hash 校验失败")
    return plan


__all__ = ["admitted_plan_from_dict"]
