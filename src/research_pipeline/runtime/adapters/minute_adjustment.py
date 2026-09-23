"""minute_adjustment 算子族及其直接共享实现。"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
import shutil
from typing import Mapping
from zoneinfo import ZoneInfo
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.catalog import AdjustmentFactorSnapshot
from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef, DatasetFilter, PartitionedDatasetResolver, PartitionedDatasetRef, audit_adjustment_gate, build_adjustment_factor_snapshot, build_minute_resample_plan, execute_minute_resample, execute_minute_adjustment, inspect_parquet_partition, require_minute_price_mode
from research_pipeline.domain import CorporateAction, load_session_policy_bundle
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
        "source_added_at": "fld_ca_add_time",
        "source_revised_at": "fld_ca_mod_time",
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
        row["source_added_at"] = _local_datetime(
            source["fld_ca_add_time"], "fld_ca_add_time"
        )
        row["source_revised_at"] = _local_datetime(
            source["fld_ca_mod_time"], "fld_ca_mod_time"
        )
        row["revision"] = int(source.get("__revision", 1))
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
) -> tuple[tuple[dict[str, object], ...], DatasetArtifactRef]:
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
        or role["added"] not in visibility.additional_time_fields
        or revision is None
        or not revision.order_fields
        or revision.order_fields[0] != role["revised"]
    ):
        raise ValueError("公司行动 request 没有闭合公告、加入和修订可见性 policy")
    required = tuple(dict.fromkeys(role.values()))
    if not set(required) <= set(plan.query.field_ids):
        raise ValueError("公司行动 QueryIR 没有公开构造真实版本时钟所需字段")

    fact_rows = [
        dict(row)
        for batch in dataset.iter_temporal_fact_batches(
            plan=plan,
            columns=required,
            filters=(
                DatasetFilter(
                    role["code"],
                    "eq",
                    instrument_id
                    if asset_class == "cn_stock"
                    else instrument_id.split(".", 1)[0],
                ),
            ),
            batch_size=8_192,
        )
        for row in batch.to_pylist()
    ]
    sessions = tuple(sorted(set(trading_sessions)))
    source_versions: dict[str, list[dict[str, object]]] = {}
    selection_times: set[datetime] = set()
    for row in fact_rows:
        code = str(row[role["code"]])
        matches = (
            code == instrument_id
            if asset_class == "cn_stock"
            else canonical_cn_etf_instrument_id(code) == instrument_id
        )
        if not matches:
            continue
        effective_date = _local_date(row[role["effective"]], role["effective"])
        effective_at = datetime.combine(
            effective_date,
            time(9, 31),
            ZoneInfo("Asia/Shanghai"),
        )
        if not applicable_start <= effective_at < applicable_end:
            continue
        source_id = str(row[role["id"]])
        source_versions.setdefault(source_id, []).append(row)
        publication = _local_date(row[role["publication"]], role["publication"])
        next_session = next((session for session in sessions if session > publication), None)
        if next_session is None:
            raise ValueError("公司行动缺少公告后的下一真实交易日")
        added = _local_datetime(row[role["added"]], role["added"])
        revised = _local_datetime(row[role["revised"]], role["revised"])
        visible_at = max(
            datetime.combine(
                next_session,
                time(9, 15),
                ZoneInfo("Asia/Shanghai"),
            ),
            added,
            revised,
        )
        if visible_at <= as_of:
            selection_times.add(visible_at)

    for source_id, rows in source_versions.items():
        ordered = sorted(
            rows,
            key=lambda row: _local_datetime(row[role["revised"]], role["revised"]),
        )
        first = ordered[0]
        first_added = _local_datetime(first[role["added"]], role["added"])
        first_revised = _local_datetime(first[role["revised"]], role["revised"])
        if first_revised > first_added and first_revised >= applicable_start:
            raise ValueError(
                "公司行动当前源缺少研究窗口内可还原的修订前版本: "
                f"action_id={source_id}"
            )
        effective_dates = {
            _local_date(row[role["effective"]], role["effective"])
            for row in ordered
        }
        if len(effective_dates) != 1:
            raise ValueError(
                "公司行动历史修订改变生效日，无法安全重放: "
                f"action_id={source_id}"
            )

    selected_history: dict[str, dict[str, tuple[datetime, dict[str, object]]]] = {}
    for selection_at in sorted(selection_times):
        selected, _reference, _plan = _columnar_request_rows_at(
            context,
            request_id,
            consumer_time=selection_at,
            filters=(
                DatasetFilter(
                    role["code"],
                    "eq",
                    instrument_id
                    if asset_class == "cn_stock"
                    else instrument_id.split(".", 1)[0],
                ),
            ),
        )
        for row in selected:
            code = str(row.get(role["code"], ""))
            matches = (
                code == instrument_id
                if asset_class == "cn_stock"
                else canonical_cn_etf_instrument_id(code) == instrument_id
            )
            if not matches:
                continue
            effective_date = _local_date(row[role["effective"]], role["effective"])
            effective_at = datetime.combine(
                effective_date,
                time(9, 31),
                ZoneInfo("Asia/Shanghai"),
            )
            if not applicable_start <= effective_at < applicable_end:
                continue
            source_id = str(row[role["id"]])
            identity = typed_canonical_hash(_json_ready(row))
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
    normalized = []
    for source_id, versions in sorted(selected_history.items()):
        for revision_number, (_visible_at, row) in enumerate(
            sorted(versions.values(), key=lambda item: item[0]),
            start=1,
        ):
            row["__revision"] = revision_number
            normalized.append(row)
    return tuple(normalized), reference


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


def execute_data_minute_adjustment_snapshot_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    parameters = _parameters(context)
    factor_request_id = str(parameters["factor_request_id"])
    action_request_id = str(parameters["corporate_action_request_id"])
    instrument_id = str(parameters["instrument_id"])
    asset_class = str(parameters["asset_class"])
    as_of = _aware(parameters["as_of"])
    applicable_start = _aware(parameters["applicable_start"])
    applicable_end = _aware(parameters["applicable_end"])
    if applicable_end > as_of:
        raise ValueError("复权快照适用区间不能晚于研究时钟")

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
    if not filtered_daily or any(
        field not in row
        for row in filtered_daily
        for field in (date_field, close_field, factor_field)
    ):
        raise ValueError("复权快照缺少当前标的日线 close/factor")
    daily_by_date = {
        _local_date(row[date_field], date_field): row for row in filtered_daily
    }
    prior_dates = tuple(
        item for item in sorted(daily_by_date) if item < applicable_start.date()
    )
    if not prior_dates:
        raise ValueError("复权快照缺少适用区间前一已完成日 factor")
    initial_date = prior_dates[-1]
    initial_row = daily_by_date[initial_date]

    trading_sessions = tuple(sorted(daily_by_date))
    action_rows, action_reference = _temporal_corporate_action_rows(
        context,
        action_request_id,
        instrument_id=instrument_id,
        asset_class=asset_class,
        trading_sessions=trading_sessions,
        applicable_start=applicable_start,
        applicable_end=applicable_end,
        as_of=as_of,
    )
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
    source_revision_hash = typed_canonical_hash({
        factor_request_id: factor_reference.to_dict(),
        action_request_id: action_reference.to_dict(),
    })
    snapshot = build_adjustment_factor_snapshot(
        instrument_id=instrument_id,
        asset_class=asset_class,
        as_of=as_of,
        applicable_start=applicable_start,
        applicable_end=applicable_end,
        initial_factor_date=initial_date,
        initial_factor=Decimal(str(initial_row[factor_field])),
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
        "effective_times": {
            item.isoformat(): value.isoformat(timespec="seconds")
            for item, value in sorted(effective_times.items())
        },
        "previous_closes": {
            item.isoformat(): str(value)
            for item, value in sorted(previous_closes.items())
        },
        "input_references": {
            factor_request_id: factor_reference.to_dict(),
            action_request_id: action_reference.to_dict(),
        },
        "corporate_action_snapshot_hash": corporate_action_snapshot_hash(included),
        "adjustment_audit": audit.to_dict(),
    }
    return _external_result(
        context,
        payload,
        parquet_rows={
            "snapshot": [{
                "contract_version": str(payload["contract_version"]),
                "snapshot_json": canonical_json(snapshot.to_dict()),
                "included_actions_json": canonical_json(
                    [item.to_dict() for item in included]
                ),
                "candidate_actions_json": canonical_json(
                    [item.to_dict() for item in actions]
                ),
                "effective_times_json": canonical_json(
                    payload["effective_times"]
                ),
                "previous_closes_json": canonical_json(
                    payload["previous_closes"]
                ),
                "input_references_json": canonical_json(
                    payload["input_references"]
                ),
                "corporate_action_snapshot_hash": str(
                    payload["corporate_action_snapshot_hash"]
                ),
                "adjustment_audit_json": canonical_json(audit.to_dict()),
            }],
        },
    )


def execute_research_bars_minute_adjust_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    import pyarrow.parquet as pq

    bars_payload = _input_external_payload(context, "bars")
    require_minute_price_mode(bars_payload, consumer="分钟复权", expected_mode="raw")
    snapshot_payload = _input_external_payload(context, "snapshot")
    raw_snapshot = snapshot_payload.get("snapshot")
    raw_actions = snapshot_payload.get("included_actions")
    raw_candidates = snapshot_payload.get("candidate_actions")
    raw_effective_times = snapshot_payload.get("effective_times")
    raw_previous_closes = snapshot_payload.get("previous_closes")
    if not isinstance(raw_snapshot, Mapping) or not isinstance(raw_actions, list) or any(
        not isinstance(item, Mapping) for item in raw_actions
    ) or not isinstance(raw_candidates, list) or any(
        not isinstance(item, Mapping) for item in raw_candidates
    ) or not isinstance(raw_effective_times, Mapping) or not isinstance(
        raw_previous_closes, Mapping
    ):
        raise ValueError("分钟复权输入缺少完整 snapshot 公司行动载荷")
    snapshot = AdjustmentFactorSnapshot.from_mapping(raw_snapshot)
    actions = tuple(_corporate_action_from_dict(item) for item in raw_actions)
    candidate_actions = tuple(
        _corporate_action_from_dict(item) for item in raw_candidates
    )
    expected_action_hash = corporate_action_snapshot_hash(actions)
    if snapshot_payload.get("corporate_action_snapshot_hash") != expected_action_hash:
        raise ValueError("分钟复权 snapshot 公司行动载荷不一致")
    audit_adjustment_gate(
        source_adjustment_mode=str(_parameters(context)["mode"]),
        snapshot=snapshot,
        corporate_action_snapshot_hash=expected_action_hash,
        included_actions=actions,
        relative_tolerance=float(_parameters(context)["relative_tolerance"]),
    )

    raw_dataset = bars_payload.get("partitioned_dataset")
    if not isinstance(raw_dataset, Mapping):
        raise ValueError("分钟复权 bars 缺少分区引用")
    source_dataset = PartitionedDatasetRef.from_dict(raw_dataset)
    if source_dataset.instruments != (snapshot.instrument_id,):
        raise ValueError("分钟复权当前只接受与快照一致的单标的分区")
    source_root = _input_external_root(context, "bars")
    roots: dict[str, Path] = {"runtime_artifact": source_root}
    if any(item.root_role == "minute_data" for item in source_dataset.partitions):
        roots["minute_data"] = _minute_root(context)
    resolver = PartitionedDatasetResolver(roots)
    raw_plan = None
    if "bar_start" not in source_dataset.allowed_columns:
        request_id = str(bars_payload.get("request_id", ""))
        scan_plan = _minute_scan_plan_from_environment(context, request_id)
        session_bundle = load_session_policy_bundle()
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
                snapshots={snapshot.instrument_id: snapshot},
                included_actions={snapshot.instrument_id: actions},
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
                "snapshot_identity_hash": snapshot.snapshot_identity_hash,
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
            dataset_id=f"minute/adjusted/{snapshot.snapshot_identity_hash}/{mode}",
            timestamp_field="dt",
            instrument_field="code",
            instruments=source_dataset.instruments,
            allowed_columns=tuple(pq.ParquetFile(
                staging / "bars" / source_dataset.partitions[0].partition_key / "data.parquet"
            ).schema_arrow.names),
            partitions=tuple(output_partitions),
            lineage={
                "source_dataset_reference_id": source_dataset.reference_id,
                "snapshot_identity_hash": snapshot.snapshot_identity_hash,
                "adjustment_mode": mode,
            },
        )
        payload = {
            "contract_version": "runtime-minute-bars-adjusted-v1",
            "request_id": bars_payload.get("request_id"),
            "interval_minutes": bars_payload.get("interval_minutes"),
            "quality_status": "pending_stream_consumption",
            "price_mode": mode,
            "partitioned_dataset": dataset.to_dict(),
            "partition_receipts": receipts,
            "adjustment_snapshot_identity_hash": snapshot.snapshot_identity_hash,
            "adjustment_candidates": [
                item.to_dict() for item in candidate_actions
            ],
            "adjustment_effective_times": dict(raw_effective_times),
            "adjustment_previous_closes": dict(raw_previous_closes),
            "adjustment_snapshot": snapshot.to_dict(),
            "source_bars_snapshot_hash": bars_payload["source_snapshot_hash"],
            "source_snapshot_hash": typed_canonical_hash({
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
