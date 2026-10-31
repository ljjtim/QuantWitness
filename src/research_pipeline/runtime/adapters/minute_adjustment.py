"""minute_adjustment 算子族及其直接共享实现。"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
import shutil
from typing import Mapping
from zoneinfo import ZoneInfo
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.catalog import AdjustmentFactorSnapshot
from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef, DatasetFilter, PartitionedDatasetResolver, PartitionedDatasetRef, audit_adjustment_gate, build_adjustment_factor_snapshot, build_minute_resample_plan, execute_minute_resample, execute_minute_adjustment, inspect_parquet_partition, require_minute_price_mode
from research_pipeline.domain import CorporateAction, InstrumentKey
from research_pipeline.simulation import compile_cn_etf_corporate_action_rows, compile_cn_stock_corporate_action_rows, corporate_action_snapshot_hash
from research_pipeline.simulation.corporate_actions import canonical_cn_etf_instrument_id
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _corporate_action_from_dict, _environment, _external_result, _input_data_bundle, _input_external_payload, _input_external_root, _json_ready, _local_date, _local_datetime, _parameters
from .minute_io import _aware, _minute_root, _minute_scan_plan_from_environment, _session_instruments


def _columnar_request_rows(
    context: OperatorRuntimeContext,
    request_id: str,
) -> tuple[tuple[dict[str, object], ...], DatasetArtifactRef]:
    """只读取 materialize 已准入的有界 request 工件。"""

    dataset, reference, _plan = _columnar_request_dataset(context, request_id)
    rows: list[dict[str, object]] = []
    for batch in dataset.iter_batches(
        columns=dataset.allowed_columns,
        batch_size=8_192,
    ):
        rows.extend(dict(item) for item in batch.to_pylist())
    return tuple(rows), reference


def _columnar_request_dataset(
    context: OperatorRuntimeContext,
    request_id: str,
):
    """解析已物化 request 及其唯一 admitted plan。"""

    bundle = _input_data_bundle(context)
    references = bundle.get("references")
    if not isinstance(references, Mapping) or not isinstance(
        references.get(request_id), Mapping
    ):
        raise ValueError(f"复权快照缺少已物化 request: {request_id}")
    reference = DatasetArtifactRef.from_dict(references[request_id])
    dataset = ArtifactResolver(
        _input_external_root(context, "data") / "data",
        max_batch_rows=8_192,
        max_batch_bytes=min(
            64 * 1024 * 1024,
            context.effective_resource_budget.memory_bytes // 4,
        ),
    ).resolve(reference)
    plan = _environment(context).admitted_plans.get(request_id)
    if plan is None:
        raise ValueError(f"复权快照缺少已准入 QueryIR: {request_id}")
    return dataset, reference, plan


def _columnar_request_rows_at(
    context: OperatorRuntimeContext,
    request_id: str,
    *,
    consumer_time: datetime,
    filters: tuple[DatasetFilter, ...] = (),
) -> tuple[tuple[dict[str, object], ...], DatasetArtifactRef, object]:
    """按单个真实消费时点选择公司行动版本。"""

    dataset, reference, plan = _columnar_request_dataset(context, request_id)
    if not plan.temporal_selection.requires_consumer_binding:
        raise ValueError("公司行动 request 必须编译为逐消费者时态计划")
    rows: list[dict[str, object]] = []
    for batch in dataset.iter_batches_at(
        plan=plan,
        consumer_time=consumer_time,
        columns=tuple(plan.query.field_ids),
        filters=filters,
        batch_size=8_192,
    ):
        rows.extend(dict(item) for item in batch.to_pylist())
    return tuple(rows), reference, plan


def _stock_action_rows(
    rows: tuple[dict[str, object], ...],
    *,
    instrument_id: str,
) -> tuple[dict[str, object], ...]:
    fields = {
        "action_id": "fld_ca_id",
        "code": "fld_ca_code",
        "announcement_date": "fld_ca_implementation_pub_date",
        "cash_per_ten": "fld_ca_cash_per_ten",
        "stock_dividend_per_ten": "fld_ca_stock_per_ten",
        "transfer_per_ten": "fld_ca_transfer_per_ten",
        "record_date": "fld_ca_record_date",
        "ex_date": "fld_ca_ex_date",
        "cash_arrival_date": "fld_ca_cash_arrival_date",
        "stock_arrival_date": "fld_ca_stock_arrival_date",
        "listing_date": "fld_ca_listing_date",
        "plan_progress": "fld_ca_plan_progress",
    }
    required = set(fields.values()) | {"fld_ca_status", "fld_ca_add_time", "fld_ca_mod_time"}
    visible = []
    for source in rows:
        missing = required - set(source)
        if missing:
            raise ValueError(f"股票公司行动工件缺少字段: {sorted(missing)}")
        if str(source["fld_ca_code"]) != instrument_id:
            continue
        if source["fld_ca_status"] not in (None, 0) or source["fld_ca_plan_progress"] != "实施方案":
            continue
        row = {logical: source[physical] for logical, physical in fields.items()}
        row["announcement_date"] = _local_date(
            source["fld_ca_implementation_pub_date"],
            "fld_ca_implementation_pub_date",
        )
        row["note"] = source.get("fld_ca_note")
        row["revision"] = int(source.get("__revision", 1))
        if "fld_ca_transfer_arrival_date" in source:
            row["transfer_arrival_date"] = source["fld_ca_transfer_arrival_date"]
        visible.append(row)
    return tuple(visible)


def _etf_action_rows(
    rows: tuple[dict[str, object], ...],
    *,
    instrument_id: str,
) -> tuple[dict[str, object], ...]:
    fields = {
        "action_id": "fld_etf_ca_id",
        "code": "fld_etf_ca_code",
        "publication_date": "fld_etf_ca_pub_date",
        "event_id": "fld_etf_ca_event_id",
        "event": "fld_etf_ca_event",
        "process_id": "fld_etf_ca_process_id",
        "cash_per_share": "fld_etf_ca_cash_per_share",
        "split_ratio": "fld_etf_ca_split_ratio",
        "record_date": "fld_etf_ca_record_date",
        "ex_date": "fld_etf_ca_ex_date",
        "pay_date": "fld_etf_ca_pay_date",
        "status": "fld_etf_ca_status",
        "source_added_at": "fld_etf_ca_add_time",
        "source_revised_at": "fld_etf_ca_mod_time",
    }
    visible = []
    for source in rows:
        missing = set(fields.values()) - set(source)
        if missing:
            raise ValueError(f"ETF 公司行动工件缺少字段: {sorted(missing)}")
        source_instrument_id = canonical_cn_etf_instrument_id(
            str(source["fld_etf_ca_code"])
        )
        if source_instrument_id != instrument_id:
            continue
        row = {logical: source[physical] for logical, physical in fields.items()}
        row["publication_date"] = _local_date(
            source["fld_etf_ca_pub_date"], "fld_etf_ca_pub_date"
        )
        row["source_added_at"] = _local_datetime(
            source["fld_etf_ca_add_time"], "fld_etf_ca_add_time"
        )
        row["source_revised_at"] = _local_datetime(
            source["fld_etf_ca_mod_time"], "fld_etf_ca_mod_time"
        )
        row["revision"] = int(source.get("__revision", 1))
        visible.append(row)
    return tuple(visible)


def _temporal_corporate_action_rows(
    context: OperatorRuntimeContext,
    request_id: str,
    *,
    instrument_id: str,
    asset_class: str,
    trading_sessions: tuple[date, ...],
    applicable_start: datetime,
    applicable_end: datetime,
    as_of: datetime,
    initial_listing_date: date | None = None,
) -> tuple[tuple[dict[str, object], ...], DatasetArtifactRef]:
    rows, reference = _temporal_corporate_action_rows_many(
        context, request_id, instrument_ids=(instrument_id,),
        asset_class=asset_class, trading_sessions=trading_sessions,
        applicable_start=applicable_start, applicable_end=applicable_end,
        as_of=as_of,
        initial_listing_dates=(
            {instrument_id: initial_listing_date}
            if initial_listing_date is not None else {}
        ),
    )
    return rows[instrument_id], reference


def _temporal_corporate_action_rows_many(
    context: OperatorRuntimeContext,
    request_id: str,
    *,
    instrument_ids: tuple[str, ...],
    asset_class: str,
    trading_sessions: tuple[date, ...],
    applicable_start: datetime,
    applicable_end: datetime,
    as_of: datetime,
    initial_listing_dates: Mapping[str, date],
) -> tuple[dict[str, tuple[dict[str, object], ...]], DatasetArtifactRef]:
    """从正式时态计划收集窗口内各次真实可见的公司行动版本。"""

    dataset, reference, plan = _columnar_request_dataset(context, request_id)
    fields = {
        "cn_stock": {
            "id": "fld_ca_id",
            "code": "fld_ca_code",
            "publication": "fld_ca_implementation_pub_date",
            "added": "fld_ca_add_time",
            "revised": "fld_ca_mod_time",
            "effective": "fld_ca_ex_date",
        },
        "cn_etf": {
            "id": "fld_etf_ca_id",
            "code": "fld_etf_ca_code",
            "publication": "fld_etf_ca_pub_date",
            "added": "fld_etf_ca_add_time",
            "revised": "fld_etf_ca_mod_time",
            "effective": "fld_etf_ca_ex_date",
        },
    }
    try:
        role = fields[asset_class]
    except KeyError as exc:
        raise ValueError("公司行动历史只支持股票和 ETF") from exc
    temporal = plan.temporal_selection
    visibility = temporal.visibility_filter
    revision = temporal.revision_selector
    if (
        not temporal.requires_consumer_binding
        or visibility is None
        or visibility.available_time_field != role["publication"]
        or visibility.inclusive
        or (asset_class == "cn_etf" and role["added"] not in visibility.additional_time_fields)
        or (asset_class == "cn_stock" and visibility.additional_time_fields)
        or revision is None
        or not revision.order_fields
        or revision.order_fields[0] != (
            role["publication"] if asset_class == "cn_stock" else role["revised"]
        )
    ):
        raise ValueError("公司行动 request 与对应市场的公告可见性 policy 不一致")
    required = tuple(dict.fromkeys(role.values()))
    if asset_class == "cn_stock":
        required += ("fld_ca_note",)
    if initial_listing_dates:
        required += ("fld_ca_status", "fld_ca_plan_progress")
    if not set(required) <= set(plan.query.field_ids):
        raise ValueError("公司行动 QueryIR 没有公开构造真实版本时钟所需字段")

    source_codes = {
        code if asset_class == "cn_stock" else code.split(".", 1)[0]: code
        for code in instrument_ids
    }

    instruments = set(instrument_ids)

    def source_code(value: object) -> str | None:
        code = str(value)
        if asset_class == "cn_etf":
            canonical = canonical_cn_etf_instrument_id(code)
            if canonical not in instruments:
                return None
            code = canonical.split(".", 1)[0]
        return code if code in source_codes else None

    def code_filters(codes) -> tuple[DatasetFilter, ...]:
        values = tuple(sorted(codes))
        return (DatasetFilter(
            role["code"], "eq" if len(values) == 1 else "in",
            values[0] if len(values) == 1 else values,
        ),)

    fact_rows = (
        dict(row)
        for batch in dataset.iter_temporal_fact_batches(
            plan=plan, columns=required,
            filters=code_filters(source_codes), batch_size=8_192,
        )
        for row in batch.to_pylist()
    )
    sessions = tuple(sorted(set(trading_sessions)))
    source_versions: dict[tuple[str, str], list[dict[str, object]]] = {}
    source_effective_dates: dict[tuple[str, str], set[date]] = {}
    version_visible_at: dict[tuple[tuple[str, str], datetime], datetime] = {}
    selection_times: dict[datetime, set[str]] = {}
    for row in fact_rows:
        code = source_code(row[role["code"]])
        if code is None:
            continue
        instrument_id = source_codes[code]
        initial_listing_date = initial_listing_dates.get(instrument_id)
        effective_date = _local_date(row[role["effective"]], role["effective"])
        if (
            initial_listing_date is not None
            and effective_date <= initial_listing_date
            and row["fld_ca_status"] in (None, 0)
            and row["fld_ca_plan_progress"] == "实施方案"
        ):
            raise ValueError("IPO 归一化原点不支持上市前或上市当日生效的公司行动")
        effective_at = datetime.combine(
            effective_date,
            time(9, 31),
            ZoneInfo("Asia/Shanghai"),
        )
        source_id = (code, str(row[role["id"]]))
        source_effective_dates.setdefault(source_id, set()).add(effective_date)
        if not applicable_start <= effective_at < applicable_end:
            continue
        source_versions.setdefault(source_id, []).append(row)
        publication = _local_date(row[role["publication"]], role["publication"])
        next_index = bisect_right(sessions, publication)
        next_session = sessions[next_index] if next_index < len(sessions) else None
        if next_session is None:
            raise ValueError("公司行动缺少公告后的下一真实交易日")
        added = _local_datetime(row[role["added"]], role["added"])
        revised = _local_datetime(row[role["revised"]], role["revised"])
        visible_at = datetime.combine(
            next_session, time(9, 15), ZoneInfo("Asia/Shanghai"),
        )
        if asset_class == "cn_etf":
            visible_at = max(visible_at, added, revised)
        version_visible_at[source_id, revised] = visible_at
        if visible_at <= as_of:
            selection_times.setdefault(visible_at, set()).add(code)

    for source_id, rows in source_versions.items():
        ordered = sorted(
            rows,
            key=lambda row: _local_datetime(row[role["revised"]], role["revised"]),
        )
        first = ordered[0]
        first_added = _local_datetime(first[role["added"]], role["added"])
        first_revised = _local_datetime(first[role["revised"]], role["revised"])
        effective_dates = source_effective_dates[source_id]
        if len(effective_dates) != 1:
            raise ValueError(
                "公司行动历史修订改变生效日，无法安全重放: "
                f"action_id={source_id}"
            )
        effective_at = datetime.combine(next(iter(effective_dates)), time(9, 31), ZoneInfo("Asia/Shanghai"))
        # 生效前已可见的实施版本足以计算该次除权；生效后的更正仍须保留此前版本。
        if asset_class == "cn_etf" and first_revised > first_added and version_visible_at[source_id, first_revised] > effective_at:
            raise ValueError(
                "公司行动当前源缺少研究窗口内可还原的修订前版本: "
                f"action_id={source_id}"
            )

    selected_history: dict[tuple[str, str], dict[str, tuple[datetime, dict[str, object]]]] = {}
    for selection_at, codes in sorted(selection_times.items()):
        filters = code_filters(codes)
        if len(instrument_ids) == 1:
            selected, _reference, _plan = _columnar_request_rows_at(
                context, request_id, consumer_time=selection_at, filters=filters,
            )
        else:
            selected = (
                dict(row)
                for batch in dataset.iter_batches_at(
                    plan=plan, consumer_time=selection_at,
                    columns=tuple(plan.query.field_ids), filters=filters,
                    batch_size=8_192,
                )
                for row in batch.to_pylist()
            )
        for row in selected:
            code = source_code(row.get(role["code"], ""))
            if code not in codes:
                continue
            effective_date = _local_date(row[role["effective"]], role["effective"])
            effective_at = datetime.combine(
                effective_date,
                time(9, 31),
                ZoneInfo("Asia/Shanghai"),
            )
            if not applicable_start <= effective_at < applicable_end:
                continue
            source_id = (code, str(row[role["id"]]))
            # 供应商 DECIMAL 保留精确十进制，不先转换成二进制浮点数。
            identity = typed_canonical_hash(_json_ready({
                key: str(value) if isinstance(value, Decimal) else value
                for key, value in row.items()
            }))
            selected_history.setdefault(source_id, {}).setdefault(
                identity,
                (selection_at, dict(row)),
            )

    missing = sorted(set(source_versions) - set(selected_history))
    if missing:
        raise ValueError(
            "公司行动在研究时钟前没有可选择的历史版本: "
            f"action_ids={missing[:5]}"
        )
    normalized = {code: [] for code in instrument_ids}
    for source_id, versions in sorted(selected_history.items()):
        for revision_number, (_visible_at, row) in enumerate(
            sorted(versions.values(), key=lambda item: item[0]),
            start=1,
        ):
            row["__revision"] = revision_number
            normalized[source_codes[source_id[0]]].append(row)
    return {code: tuple(rows) for code, rows in normalized.items()}, reference


def _actions_in_applicable_range(
    actions: tuple[CorporateAction, ...],
    *,
    applicable_start: datetime,
    applicable_end: datetime,
) -> tuple[tuple[CorporateAction, ...], dict[date, datetime]]:
    effective_by_action = {
        item.action_hash: datetime.combine(
            item.effective_date,
            time(9, 31),
            ZoneInfo("Asia/Shanghai"),
        )
        for item in actions
    }
    selected = tuple(
        item
        for item in actions
        if applicable_start
        <= effective_by_action[item.action_hash]
        < applicable_end
    )
    return selected, {
        item.effective_date: effective_by_action[item.action_hash]
        for item in selected
    }


def _initial_listing_evidence(
    value: object,
    *,
    instrument_id: str,
    asset_class: str,
    applicable_start: datetime,
) -> dict[str, str] | None:
    """上市证据只定义首日归一化原点，不提供当日收盘因子。"""

    if value is None or value == {}:
        return None
    fields = {"instrument_id", "listed_date", "available_at", "source_reference"}
    if not isinstance(value, Mapping) or set(value) != fields or any(
        not isinstance(item, str) or not item.strip() for item in value.values()
    ):
        raise ValueError("initial_listing_evidence 必须精确提供四个非空字符串字段")
    if asset_class != "cn_stock":
        raise ValueError("IPO 归一化原点只支持 cn_stock")
    if value["instrument_id"] != instrument_id:
        raise ValueError("IPO 上市证据 instrument_id 与当前标的不一致")
    listed_date = _local_date(value["listed_date"], "listed_date")
    local_start = applicable_start.astimezone(ZoneInfo("Asia/Shanghai"))
    if listed_date != local_start.date():
        raise ValueError("IPO listed_date 必须等于适用起点的本地日期")
    available_at = _local_datetime(value["available_at"], "available_at")
    market_open = datetime.combine(listed_date, time(9, 30), local_start.tzinfo)
    if available_at > applicable_start or available_at >= market_open:
        raise ValueError("IPO 上市证据必须在适用起点及当日开盘前可见")
    return dict(value)


def _build_minute_adjustment_snapshot_payload(
    context: OperatorRuntimeContext,
    parameters: Mapping[str, object],
    *,
    factor_rows=None,
    factor_reference=None,
    action_rows_and_reference=None,
    trading_sessions_override: tuple[date, ...] | None = None,
) -> dict[str, object]:
    factor_request_id = str(parameters["factor_request_id"])
    action_request_id = str(parameters["corporate_action_request_id"])
    instrument_id = str(parameters["instrument_id"])
    asset_class = str(parameters["asset_class"])
    as_of = _aware(parameters["as_of"])
    applicable_start = _aware(parameters["applicable_start"])
    applicable_end = _aware(parameters["applicable_end"])
    if applicable_end > as_of:
        raise ValueError("复权快照适用区间不能晚于研究时钟")

    listing_evidence = _initial_listing_evidence(
        parameters.get("initial_listing_evidence"),
        instrument_id=instrument_id,
        asset_class=asset_class,
        applicable_start=applicable_start,
    )
    if factor_rows is None:
        factor_rows, factor_reference = _columnar_request_rows(context, factor_request_id)
    daily_fields = {
        "cn_stock": (
            "fld_equity_daily_date",
            "fld_equity_daily_code",
            "fld_equity_daily_close",
            "fld_equity_daily_factor",
        ),
        "cn_etf": ("fld_etf_date", "fld_etf_code", "fld_etf_close", "fld_etf_factor"),
    }
    try:
        date_field, code_field, close_field, factor_field = daily_fields[asset_class]
    except KeyError as exc:
        raise ValueError("复权快照只支持股票和 ETF") from exc
    filtered_daily = tuple(
        row for row in factor_rows if str(row.get(code_field, "")) == instrument_id
    )
    if (not filtered_daily and listing_evidence is None) or any(
        field not in row
        for row in filtered_daily
        for field in (date_field, close_field, factor_field)
    ):
        raise ValueError("复权快照缺少当前标的日线 close/factor")
    daily_by_date = {}
    for row in filtered_daily:
        day = _local_date(row[date_field], date_field)
        if day in daily_by_date:
            raise ValueError(f"复权快照日线重复: {instrument_id}, {day}")
        daily_by_date[day] = row
    start_date = (
        date.fromisoformat(listing_evidence["listed_date"])
        if listing_evidence is not None else applicable_start.date()
    )
    prior_dates = tuple(item for item in sorted(daily_by_date) if item < start_date)
    if listing_evidence is not None:
        if prior_dates:
            raise ValueError("已有前日 factor，不能同时声明 IPO 归一化原点")
        initial_date = start_date
        initial_factor = Decimal(1)
    else:
        if not prior_dates:
            raise ValueError("复权快照缺少适用区间前一已完成日 factor")
        initial_date = prior_dates[-1]
        initial_factor = Decimal(str(daily_by_date[initial_date][factor_field]))

    trading_sessions = trading_sessions_override or tuple(sorted(
        set(daily_by_date) | ({initial_date} if listing_evidence is not None else set())
    ))
    if action_rows_and_reference is None:
        action_rows, action_reference = _temporal_corporate_action_rows(
            context,
            action_request_id,
            instrument_id=instrument_id,
            asset_class=asset_class,
            trading_sessions=trading_sessions,
            applicable_start=applicable_start,
            applicable_end=applicable_end,
            as_of=as_of,
            initial_listing_date=initial_date if listing_evidence is not None else None,
        )
    else:
        action_rows, action_reference = action_rows_and_reference
    if asset_class == "cn_stock":
        actions = compile_cn_stock_corporate_action_rows(
            _stock_action_rows(
                action_rows,
                instrument_id=instrument_id,
            ),
            trading_sessions=trading_sessions,
            allow_post_effective_visibility=True,
        )
    else:
        actions = compile_cn_etf_corporate_action_rows(
            _etf_action_rows(
                action_rows,
                instrument_id=instrument_id,
            ),
            trading_sessions=trading_sessions,
            as_of=as_of,
            allow_post_effective_visibility=True,
        )
    financial_rows = tuple(
        {**row, "source_ref": f"{action_reference.source_identity if action_reference.contract_version == 'direct-dataset-ref-v1' else action_reference.physical_snapshot_id}#{row['action_id']}"}
        for row in (
            _stock_action_rows(action_rows, instrument_id=instrument_id)
            if asset_class == "cn_stock"
            else _etf_action_rows(action_rows, instrument_id=instrument_id)
        )
    )
    financial_actions = (
        compile_cn_stock_corporate_action_rows(
            financial_rows, trading_sessions=trading_sessions,
            allow_post_effective_visibility=True, contract_version=2,
        )
        if asset_class == "cn_stock" else compile_cn_etf_corporate_action_rows(
            financial_rows, trading_sessions=trading_sessions, as_of=as_of,
            allow_post_effective_visibility=True, contract_version=2,
        )
    )
    financial_actions = tuple(
        action for action in financial_actions
        if applicable_start.date() <= action.effective_date <= applicable_end.date()
    )
    actions, effective_times = _actions_in_applicable_range(
        actions,
        applicable_start=applicable_start,
        applicable_end=applicable_end,
    )
    previous_closes: dict[date, Decimal] = {}
    for effective_date in sorted(effective_times):
        candidates = tuple(item for item in sorted(daily_by_date) if item < effective_date)
        if not candidates:
            raise ValueError("公司行动缺少除权日前一已完成日收盘价")
        previous_closes[effective_date] = Decimal(
            str(daily_by_date[candidates[-1]][close_field])
        )
    input_references = {
        factor_request_id: factor_reference.to_dict(),
        action_request_id: action_reference.to_dict(),
    }
    source_revision_hash = typed_canonical_hash(
        input_references if listing_evidence is None else {
            "input_references": input_references,
            "initial_listing_evidence": listing_evidence,
        }
    )
    snapshot = build_adjustment_factor_snapshot(
        instrument_id=instrument_id,
        asset_class=asset_class,
        as_of=as_of,
        applicable_start=applicable_start,
        applicable_end=applicable_end,
        initial_factor_date=initial_date,
        initial_factor=initial_factor,
        source_revision_hash=source_revision_hash,
        corporate_actions=actions,
        effective_times=effective_times,
        previous_closes=previous_closes,
    )
    included = tuple(
        item for item in actions if item.action_hash in snapshot.included_action_hashes
    )
    audit = audit_adjustment_gate(
        source_adjustment_mode="post",
        snapshot=snapshot,
        corporate_action_snapshot_hash=corporate_action_snapshot_hash(included),
        included_actions=included,
    )
    payload = {
        "contract_version": "runtime-minute-adjustment-snapshot-v1",
        "snapshot": snapshot.to_dict(),
        "included_actions": [item.to_dict() for item in included],
        "candidate_actions": [item.to_dict() for item in actions],
        "financial_corporate_actions": [item.to_dict() for item in financial_actions],
        "effective_times": {
            item.isoformat(): value.isoformat(timespec="seconds")
            for item, value in sorted(effective_times.items())
        },
        "previous_closes": {
            item.isoformat(): str(value)
            for item, value in sorted(previous_closes.items())
        },
        "input_references": input_references,
        **({"initial_listing_evidence": listing_evidence} if listing_evidence is not None else {}),
        "corporate_action_snapshot_hash": corporate_action_snapshot_hash(included),
        "adjustment_audit": audit.to_dict(),
    }
    return payload


def _adjustment_snapshot_parquet_row(payload: Mapping[str, object]) -> dict[str, object]:
    return {
        "contract_version": str(payload["contract_version"]),
        **{
            f"{field}_json": canonical_json(payload[field])
            for field in (
                "snapshot", "included_actions", "financial_corporate_actions",
                "candidate_actions", "effective_times", "previous_closes",
                "input_references", "adjustment_audit",
            )
        },
        **({"initial_listing_evidence_json": canonical_json(payload["initial_listing_evidence"])}
           if "initial_listing_evidence" in payload else {}),
        "corporate_action_snapshot_hash": str(payload["corporate_action_snapshot_hash"]),
    }


_ADJUSTMENT_INSTRUMENT_BATCH_SIZE = 128


def execute_data_minute_adjustment_snapshot_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    parameters = _parameters(context)
    instrument_id = parameters.get("instrument_id")
    instrument_ids = parameters.get("instrument_ids")
    if (instrument_id is not None) == (instrument_ids is not None):
        raise ValueError("instrument_id 与 instrument_ids 必须二选一")
    if instrument_ids is None:
        if not isinstance(instrument_id, str) or not instrument_id.strip():
            raise ValueError("instrument_id 必须是非空字符串")
        if parameters.get("initial_listing_evidence_by_instrument") is not None:
            raise ValueError("单股快照应使用 initial_listing_evidence")
        payload = _build_minute_adjustment_snapshot_payload(context, parameters)
        bundle_hash = _input_data_bundle(context).get("bundle_hash")
        if bundle_hash is not None:
            payload["source_data_bundle_hash"] = bundle_hash
        return _external_result(context, payload, parquet_rows={
            "snapshot": [_adjustment_snapshot_parquet_row(payload)],
        })
    if (
        not isinstance(instrument_ids, list) or not instrument_ids
        or any(not isinstance(code, str) or not code.strip() for code in instrument_ids)
        or len(instrument_ids) != len(set(instrument_ids))
    ):
        raise ValueError("instrument_ids 必须是非空去重字符串列表")
    if parameters.get("initial_listing_evidence") is not None:
        raise ValueError("多股快照应使用 initial_listing_evidence_by_instrument")
    return _execute_multi_adjustment_snapshot(context, parameters, tuple(sorted(instrument_ids)))


def _execute_multi_adjustment_snapshot(
    context: OperatorRuntimeContext,
    parameters: Mapping[str, object],
    instrument_ids: tuple[str, ...],
) -> RuntimeNodeOutputs:
    asset_class = str(parameters["asset_class"])
    daily_fields = {
        "cn_stock": ("fld_equity_daily_date", "fld_equity_daily_code",
                     "fld_equity_daily_close", "fld_equity_daily_factor"),
        "cn_etf": ("fld_etf_date", "fld_etf_code", "fld_etf_close", "fld_etf_factor"),
    }
    if asset_class not in daily_fields:
        raise ValueError("复权快照只支持股票和 ETF")
    date_field, code_field, close_field, factor_field = daily_fields[asset_class]
    as_of = _aware(parameters["as_of"])
    start = _aware(parameters["applicable_start"])
    end = _aware(parameters["applicable_end"])
    if end > as_of or start >= end:
        raise ValueError("复权快照适用区间无效或晚于研究时钟")
    raw_listing = parameters.get("initial_listing_evidence_by_instrument")
    if raw_listing is None:
        raw_listing = {}
    if not isinstance(raw_listing, Mapping) or not set(raw_listing) <= set(instrument_ids):
        raise ValueError("批量上市证据必须按请求证券提供映射")
    listing = {
        code: _initial_listing_evidence(
            value, instrument_id=code, asset_class=asset_class, applicable_start=start,
        )
        for code, value in raw_listing.items()
    }
    dataset, factor_reference, _plan = _columnar_request_dataset(
        context, str(parameters["factor_request_id"]),
    )
    daily = {code: {} for code in instrument_ids}
    prior_dates = {}
    sessions: set[date] = set()
    # 日线只扫描一次；每证券只保留最近前日及适用窗口内的收盘价。
    for batch in dataset.iter_batches(
        columns=(date_field, code_field, close_field, factor_field),
        filters=(DatasetFilter(code_field, "in", instrument_ids),), batch_size=8_192,
    ):
        for row in batch.to_pylist():
            code = str(row[code_field])
            if code not in daily:
                continue
            day = _local_date(row[date_field], date_field)
            sessions.add(day)
            rows = daily[code]
            if day < start.date():
                previous = prior_dates.get(code)
                if previous is not None and day < previous:
                    continue
                if previous is not None and day > previous:
                    del rows[previous]
                prior_dates[code] = day
            elif day > end.date():
                continue
            if day in rows:
                raise ValueError(f"复权快照日线重复: {code}, {day}")
            rows[day] = row
    for evidence in listing.values():
        if evidence is not None:
            sessions.add(date.fromisoformat(evidence["listed_date"]))
    trading_sessions = tuple(sorted(sessions))
    snapshots = {}
    table_rows = []
    for offset in range(0, len(instrument_ids), _ADJUSTMENT_INSTRUMENT_BATCH_SIZE):
        codes = instrument_ids[offset:offset + _ADJUSTMENT_INSTRUMENT_BATCH_SIZE]
        action_rows, action_reference = _temporal_corporate_action_rows_many(
            context, str(parameters["corporate_action_request_id"]),
            instrument_ids=codes, asset_class=asset_class,
            trading_sessions=trading_sessions, applicable_start=start,
            applicable_end=end, as_of=as_of,
            initial_listing_dates={
                code: date.fromisoformat(listing[code]["listed_date"])
                for code in codes if listing.get(code) is not None
            },
        )
        for code in codes:
            payload = _build_minute_adjustment_snapshot_payload(
                context,
                {**parameters, "instrument_id": code, "initial_listing_evidence": listing.get(code)},
                factor_rows=tuple(daily.pop(code).values()), factor_reference=factor_reference,
                action_rows_and_reference=(action_rows[code], action_reference),
                trading_sessions_override=trading_sessions,
            )
            snapshots[code] = payload
            table_rows.append({
                **_adjustment_snapshot_parquet_row(payload), "instrument_id": code,
                "contract_version": "runtime-minute-adjustment-snapshot-v2",
            })
    # 首行无 IPO 证据时，Arrow 仍需保留后续证券的可选列。
    if any("initial_listing_evidence_json" in row for row in table_rows):
        for row in table_rows:
            row.setdefault("initial_listing_evidence_json", None)
    return _external_result(context, {
        "contract_version": "runtime-minute-adjustment-snapshot-v2",
        "snapshots": snapshots,
        **({"source_data_bundle_hash": _input_data_bundle(context)["bundle_hash"]}
           if "bundle_hash" in _input_data_bundle(context) else {}),
    }, parquet_rows={"snapshot": table_rows})


def execute_research_bars_minute_adjust_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    import pyarrow.parquet as pq

    bars_payload = _input_external_payload(context, "bars")
    require_minute_price_mode(bars_payload, consumer="分钟复权", expected_mode="raw")
    snapshot_payload = _input_external_payload(context, "snapshot")
    multi = snapshot_payload.get("contract_version") == "runtime-minute-adjustment-snapshot-v2"
    if multi:
        members = snapshot_payload.get("snapshots")
        if not isinstance(members, Mapping) or not members or any(
            not isinstance(code, str) or not code or not isinstance(value, Mapping)
            for code, value in members.items()
        ):
            raise ValueError("分钟复权输入缺少逐证券 snapshots 映射")
    else:
        raw_member_snapshot = snapshot_payload.get("snapshot")
        if not isinstance(raw_member_snapshot, Mapping):
            raise ValueError("分钟复权输入缺少完整 snapshot 公司行动载荷")
        members = {str(raw_member_snapshot.get("instrument_id", "")): snapshot_payload}
    snapshots = {}
    included_actions = {}
    adjustment_contexts = {}
    parameters = _parameters(context)
    for code, member in sorted(members.items()):
        raw_snapshot = member.get("snapshot")
        raw_actions = member.get("included_actions")
        raw_candidates = member.get("candidate_actions")
        raw_effective_times = member.get("effective_times")
        raw_previous_closes = member.get("previous_closes")
        if not isinstance(raw_snapshot, Mapping) or not isinstance(raw_actions, list) or any(
            not isinstance(item, Mapping) for item in raw_actions
        ) or not isinstance(raw_candidates, list) or any(
            not isinstance(item, Mapping) for item in raw_candidates
        ) or not isinstance(raw_effective_times, Mapping) or not isinstance(raw_previous_closes, Mapping):
            raise ValueError("分钟复权输入缺少完整 snapshot 公司行动载荷")
        snapshot = AdjustmentFactorSnapshot.from_mapping(raw_snapshot)
        if code != snapshot.instrument_id:
            raise ValueError("分钟复权逐证券快照身份错配或重复")
        actions = tuple(_corporate_action_from_dict(item) for item in raw_actions)
        candidate_actions = tuple(_corporate_action_from_dict(item) for item in raw_candidates)
        expected_action_hash = corporate_action_snapshot_hash(actions)
        if member.get("corporate_action_snapshot_hash") != expected_action_hash:
            raise ValueError("分钟复权 snapshot 公司行动载荷不一致")
        financial_actions = member.get("financial_corporate_actions", [])
        if multi:
            if (
                member.get("contract_version") != "runtime-minute-adjustment-snapshot-v1"
                or not isinstance(member.get("input_references"), Mapping)
                or not isinstance(financial_actions, list)
                or "financial_corporate_actions" not in member
            ):
                raise ValueError("分钟复权逐证券载荷缺少来源或权益字段")
            instrument_hash = InstrumentKey(
                code, snapshot.asset_class, code.rpartition(".")[2], "CNY",
                "stock" if snapshot.asset_class == "cn_stock" else "etf",
            ).instrument_hash
            financial = tuple(_corporate_action_from_dict(item) for item in financial_actions)
            if any(item.instrument_hash != instrument_hash for item in (*actions, *candidate_actions, *financial)):
                raise ValueError("分钟复权公司行动属于其他证券")
        audit_adjustment_gate(
            source_adjustment_mode=str(parameters["mode"]), snapshot=snapshot,
            corporate_action_snapshot_hash=expected_action_hash, included_actions=actions,
            relative_tolerance=float(parameters["relative_tolerance"]),
        )
        snapshots[code] = snapshot
        included_actions[code] = actions
        adjustment_contexts[code] = {
            "adjustment_snapshot": snapshot.to_dict(),
            "adjustment_snapshot_identity_hash": snapshot.snapshot_identity_hash,
            "adjustment_candidates": [item.to_dict() for item in candidate_actions],
            "adjustment_effective_times": dict(raw_effective_times),
            "adjustment_previous_closes": dict(raw_previous_closes),
            "financial_corporate_actions": financial_actions,
        }
    snapshot_identity = (
        typed_canonical_hash({code: item.snapshot_identity_hash for code, item in sorted(snapshots.items())})
        if multi else snapshot.snapshot_identity_hash
    )
    raw_dataset = bars_payload.get("partitioned_dataset")
    if not isinstance(raw_dataset, Mapping):
        raise ValueError("分钟复权 bars 缺少分区引用")
    source_dataset = PartitionedDatasetRef.from_dict(raw_dataset)
    if len(source_dataset.instruments) != len(snapshots) or set(source_dataset.instruments) != set(snapshots):
        raise ValueError("分钟复权行情与快照证券集合不一致，存在缺失或重复")
    source_root = _input_external_root(context, "bars")
    roots: dict[str, Path] = {"runtime_artifact": source_root}
    if any(item.root_role == "minute_data" for item in source_dataset.partitions):
        roots["minute_data"] = _minute_root(context)
    resolver = PartitionedDatasetResolver(roots)
    raw_plan = None
    if "bar_start" not in source_dataset.allowed_columns:
        request_id = str(bars_payload.get("request_id", ""))
        scan_plan = _minute_scan_plan_from_environment(context, request_id)
        from .minute_io import _session_bundle_from_payload
        session_bundle = _session_bundle_from_payload(bars_payload, context)
        raw_plan = build_minute_resample_plan(
            scan_plan,
            interval_minutes=int(bars_payload["interval_minutes"]),
            session_bundle=session_bundle,
            instruments=_session_instruments(scan_plan, session_bundle),
        )
    parameters = _parameters(context)
    mode = str(parameters["mode"])
    if mode != "post":
        raise ValueError("分钟复权流只物化 post；pre 由消费窗口动态重定基")
    staging = context.external_store.prepare()
    output_partitions = []
    receipts = []
    try:
        for source_ref in source_dataset.partitions:
            source = resolver.resolve_partition(source_dataset, source_ref.partition_key)

            class _SourceBatches:
                source_identity = typed_canonical_hash(source_ref.to_dict())

                def __iter__(self):
                    return source.iter_batches(
                        columns=source_dataset.allowed_columns,
                        batch_size=65_536,
                    )

            normalized_source = _SourceBatches()
            if raw_plan is not None:
                resampled = execute_minute_resample(normalized_source, plan=raw_plan)

                class _NormalizedBatches:
                    source_identity = typed_canonical_hash({
                        "partition": source_ref.to_dict(),
                        "resample_plan_hash": raw_plan.plan_hash,
                    })

                    def __iter__(self):
                        return iter(resampled)

                normalized_source = _NormalizedBatches()
            stream = execute_minute_adjustment(
                normalized_source,
                snapshots=snapshots,
                included_actions=included_actions,
                mode=mode,
                relative_tolerance=float(parameters["relative_tolerance"]),
            )
            target = staging / "bars" / source_ref.partition_key / "data.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            writer = None
            try:
                for batch in stream:
                    if writer is None:
                        writer = pq.ParquetWriter(target, batch.schema)
                    writer.write_batch(batch)
            finally:
                if writer is not None:
                    writer.close()
            if writer is None:
                raise ValueError("分钟复权分区没有可消费的已完成 bar")
            manifest = stream.manifest
            lineage = {
                "input_partition_id": manifest.input_reference_id,
                "snapshot_identity_hash": snapshot_identity,
                "adjustment_mode": mode,
            }
            output_partitions.append(inspect_parquet_partition(
                target,
                allowed_root=staging,
                partition_key=source_ref.partition_key,
                logical_start=source_ref.logical_start,
                logical_end=source_ref.logical_end,
                root_role="runtime_artifact",
                source_kind="runtime_derived",
                sort_keys=("code", "dt"),
                lineage=lineage,
            ))
            receipts.append({"partition_key": source_ref.partition_key, **manifest.to_dict()})
        dataset = PartitionedDatasetRef(
            dataset_id=f"minute/adjusted/{snapshot_identity}/{mode}",
            timestamp_field="dt",
            instrument_field="code",
            instruments=source_dataset.instruments,
            allowed_columns=tuple(pq.ParquetFile(
                staging / "bars" / source_dataset.partitions[0].partition_key / "data.parquet"
            ).schema_arrow.names),
            partitions=tuple(output_partitions),
            lineage={
                "source_dataset_reference_id": source_dataset.reference_id,
                "snapshot_identity_hash": snapshot_identity,
                "adjustment_mode": mode,
            },
        )
        payload = {
            **({"minute_scope": bars_payload["minute_scope"],
                 "session_policy_bundle_hash": bars_payload["session_policy_bundle_hash"]}
               if "minute_scope" in bars_payload else {}),
            "contract_version": "runtime-minute-bars-adjusted-v1",
            "request_id": bars_payload.get("request_id"),
            "interval_minutes": bars_payload.get("interval_minutes"),
            "quality_status": "pending_stream_consumption",
            "price_mode": mode,
            "partitioned_dataset": dataset.to_dict(),
            "partition_receipts": receipts,
            **({
                "adjustment_snapshots": adjustment_contexts,
                "adjustment_snapshot_identity_hash": snapshot_identity,
                "financial_corporate_actions": [
                    item for member in adjustment_contexts.values()
                    for item in member["financial_corporate_actions"]
                ],
            } if multi else adjustment_contexts[snapshot.instrument_id]),
            "source_bars_snapshot_hash": bars_payload["source_snapshot_hash"],
            "source_snapshot_hash": typed_canonical_hash({
                "bars": bars_payload["source_snapshot_hash"],
                "adjustment_snapshots": adjustment_contexts,
            } if multi else {
                "bars": bars_payload["source_snapshot_hash"],
                "adjustment_snapshot": snapshot.snapshot_identity_hash,
                "adjustment_candidates": [
                    item.to_dict() for item in candidate_actions
                ],
                "adjustment_effective_times": {
                    str(item): str(value)
                    for item, value in sorted(raw_effective_times.items())
                },
                "adjustment_previous_closes": {
                    str(item): str(value)
                    for item, value in sorted(raw_previous_closes.items())
                },
            }),
        }
        (staging / "result.json").write_text(
            canonical_json(_json_ready(payload)), encoding="utf-8"
        )
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs.single(RuntimeNodeValue.external(commit))
