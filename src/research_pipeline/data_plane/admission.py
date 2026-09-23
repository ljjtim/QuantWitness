"""Catalog Lock 驱动的 Query IR 静态准入。"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from types import MappingProxyType
from typing import Any

from factor_contracts import FactorPublicationBinding

from research_pipeline.catalog import (
    CompiledCatalog,
    DriftAttestation,
    MinuteDatasetSemantics,
    MinuteSourceSemantics,
    PolicyContract,
)
from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.claim_levels import CLAIM_LEVELS
from research_pipeline.domain.session_close import (
    FUTURES_SESSION_CLOSE_POLICY_IDS,
    FuturesSessionCloseError,
    load_futures_session_close_bundle,
    require_current_futures_session_close_binding,
)

from .errors import (
    QueryAttestationStaleError,
    QueryCatalogUnapprovedError,
    QueryIRInvalidError,
)
from .query_ir import DateRangeV1, InstantRangeV2, QueryIR
from .temporal import (
    EffectiveIntervalSelector,
    RevisionSelector,
    SelectionClock,
    SessionCloseFact,
    SessionCloseInstrumentBinding,
    SessionClosePolicyBinding,
    SessionCloseSelection,
    TemporalSelectionPlan,
    VisibilityFilter,
)


ADMITTED_PLAN_VERSION = "admitted-query-plan-v6"
DAILY_AVAILABILITY_POLICY_REF = "available.daily.v1"
DAILY_AVAILABILITY_RULE = "next_session_open"
MINUTE_TIME_NORMALIZATION_VERSION = "minute-time-normalization-v1"
MINUTE_AVAILABILITY_RULE = "event-time-is-completed-bar-available-time-v1"


@dataclass(frozen=True)
class AdmittedQueryPlan:
    query: QueryIR
    catalog_hash: str
    binding_id: str
    binding_version: int
    attestation_hash: str
    source_profile: str
    environment: str
    expected_schema_revision: str
    availability_policy_hash: str
    revision_policy_hash: str
    object_name: str
    columns: tuple[tuple[str, str], ...]
    field_types: tuple[tuple[str, str], ...]
    field_nullables: tuple[tuple[str, bool], ...]
    primary_key: tuple[str, ...]
    event_time_field: str
    instrument_field: str | None
    temporal_selection: TemporalSelectionPlan
    input_claim_ceiling: str
    session_close_binding: SessionCloseSelection | None = None
    daily_availability_policy_ref: str | None = None
    daily_availability_rule: str | None = None
    minute_dataset_semantics_hash: str | None = None
    minute_source_semantics_hash: str | None = None
    minute_scope_binding_hash: str | None = None
    minute_capability_manifest_hash: str | None = None
    minute_asset_class: str | None = None
    minute_instrument_role: str | None = None
    minute_session_policy_ref: str | None = None
    minute_quality_policy_refs: tuple[str, ...] | None = None
    minute_timezone: str | None = None
    minute_timestamp_storage: str | None = None
    minute_timestamp_role: str | None = None
    minute_bar_interval: str | None = None
    minute_availability_rule: str | None = None
    minute_time_normalization_version: str | None = None
    result_cardinality: str = "one_or_more"
    factor_publication: FactorPublicationBinding | None = None
    plan_version: str = ADMITTED_PLAN_VERSION

    def __post_init__(self) -> None:
        if self.plan_version != ADMITTED_PLAN_VERSION:
            raise QueryIRInvalidError(
                f"AdmittedQueryPlan 版本不受支持: {self.plan_version}"
            )
        if self.temporal_selection.public_projection != self.query.field_ids:
            raise QueryIRInvalidError("TemporalSelectionPlan 公开投影与 QueryIR 不一致")
        if self.temporal_selection.event_time_field != self.event_time_field:
            raise QueryIRInvalidError("TemporalSelectionPlan event_time 与计划不一致")
        if self.input_claim_ceiling not in CLAIM_LEVELS:
            raise QueryIRInvalidError("input_claim_ceiling 无效")
        if self.instrument_field is None and self.query.universe.instruments:
            raise QueryIRInvalidError("非证券关系不能使用证券universe过滤")
        if self.source_profile == "factor":
            if self.factor_publication is None:
                raise QueryIRInvalidError("因子计划必须绑定正式publication")
            if self.factor_publication.storage_map.get(self.object_name) is None:
                raise QueryIRInvalidError("因子计划publication未绑定当前物理表")
        elif self.factor_publication is not None:
            raise QueryIRInvalidError("非因子计划不能携带因子publication")
        if self.session_close_binding != self.temporal_selection.session_close_selection:
            raise QueryIRInvalidError("AdmittedQueryPlan session-close binding 与时态计划不一致")
        if self.session_close_binding is not None:
            if self.availability_policy_hash != (
                self.session_close_binding.availability_policy_hash
            ):
                raise QueryIRInvalidError("session-close availability policy hash 不一致")
            try:
                require_current_futures_session_close_binding(
                    bundle_hash=self.session_close_binding.bundle_hash,
                    policy_identities=tuple(
                        (
                            item.session_policy_id,
                            item.session_policy_revision,
                            item.session_policy_hash,
                        )
                        for item in self.session_close_binding.policy_bindings
                    ),
                )
            except FuturesSessionCloseError as exc:
                raise QueryIRInvalidError(str(exc)) from exc
        daily_contract = (
            self.daily_availability_policy_ref,
            self.daily_availability_rule,
        )
        if daily_contract == (None, None):
            return
        if daily_contract != (
            DAILY_AVAILABILITY_POLICY_REF,
            DAILY_AVAILABILITY_RULE,
        ):
            raise QueryIRInvalidError(
                "daily_availability 必须完整绑定 "
                "available.daily.v1/next_session_open"
            )

    @property
    def column_map(self) -> MappingProxyType:
        return MappingProxyType(dict(self.columns))

    @property
    def plan_hash(self) -> str:
        return typed_canonical_hash(self.identity_payload())

    def bind_consumer_time(
        self,
        value: str | date | datetime,
    ) -> "AdmittedQueryPlan":
        """为一个真实历史消费者绑定选择时点，形成不同的执行身份。"""

        return replace(
            self,
            temporal_selection=self.temporal_selection.bind_consumer_time(value),
        )

    def identity_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "plan_version": self.plan_version,
            "query": self.query.to_dict(),
            "catalog_hash": self.catalog_hash,
            "binding_id": self.binding_id,
            "binding_version": self.binding_version,
            "attestation_hash": self.attestation_hash,
            "availability_policy_hash": self.availability_policy_hash,
            "revision_policy_hash": self.revision_policy_hash,
            "temporal_selection": self.temporal_selection.to_dict(),
            "input_claim_ceiling": self.input_claim_ceiling,
        }
        if self.result_cardinality != "one_or_more":
            payload["result_cardinality"] = self.result_cardinality
        if self.factor_publication is not None:
            payload["factor_publication"] = self.factor_publication.to_dict()
        if self.daily_availability_policy_ref is not None:
            payload["daily_availability"] = {
                "policy_ref": self.daily_availability_policy_ref,
                "available_after": self.daily_availability_rule,
            }
        if self.session_close_binding is not None:
            payload["session_close"] = self.session_close_binding.to_dict()
        if self.minute_dataset_semantics_hash is not None:
            payload.update(
                {
                    "minute_dataset_semantics_hash": self.minute_dataset_semantics_hash,
                    "minute_source_semantics_hash": self.minute_source_semantics_hash,
                    "minute_scope_binding_hash": self.minute_scope_binding_hash,
                    "minute_capability_manifest_hash": self.minute_capability_manifest_hash,
                    "minute_asset_class": self.minute_asset_class,
                    "minute_instrument_role": self.minute_instrument_role,
                    "minute_session_policy_ref": self.minute_session_policy_ref,
                    "minute_timezone": self.minute_timezone,
                    "minute_timestamp_storage": self.minute_timestamp_storage,
                    "minute_timestamp_role": self.minute_timestamp_role,
                    "minute_bar_interval": self.minute_bar_interval,
                    "minute_availability_rule": self.minute_availability_rule,
                    "minute_time_normalization_version": self.minute_time_normalization_version,
                }
            )
            if self.minute_quality_policy_refs is not None:
                payload["minute_quality_policy_refs"] = list(
                    self.minute_quality_policy_refs
                )
        return payload

    def to_dict(self) -> dict[str, object]:
        return {
            **self.identity_payload(),
            "plan_hash": self.plan_hash,
            "source_profile": self.source_profile,
            "environment": self.environment,
            "expected_schema_revision": self.expected_schema_revision,
            "object_name": self.object_name,
            "columns": dict(self.columns),
            "field_types": dict(self.field_types),
            "field_nullables": dict(self.field_nullables),
            "primary_key": list(self.primary_key),
            "event_time_field": self.event_time_field,
            "instrument_field": self.instrument_field,
        }


def admit_query(
    query: QueryIR,
    *,
    catalog: CompiledCatalog,
    binding: Any,
    attestation: DriftAttestation,
    factor_publication: FactorPublicationBinding | None = None,
) -> AdmittedQueryPlan:
    """生成唯一静态准入结果；C6 不得重复拥有这些判断。"""
    dataset = catalog.datasets.get(query.dataset_id)
    if dataset is None or dataset.get("status") != "approved":
        raise QueryCatalogUnapprovedError(f"dataset 未获批准: {query.dataset_id}")
    if int(dataset["dataset_version"]) != query.dataset_version:
        raise QueryCatalogUnapprovedError("dataset version 与 Catalog Lock 不一致")
    raw_binding = dict(binding)
    binding_id = str(raw_binding.get("binding_id", ""))
    locked_binding = catalog.bindings.get(binding_id)
    if locked_binding is None or dict(locked_binding) != raw_binding:
        raise QueryCatalogUnapprovedError("binding 必须逐字来自当前 Catalog Lock")
    if raw_binding.get("status") != "approved":
        raise QueryCatalogUnapprovedError("binding 未获批准")
    if raw_binding.get("dataset_id") != query.dataset_id:
        raise QueryCatalogUnapprovedError("binding 与 dataset 不匹配")
    minute_dataset: MinuteDatasetSemantics | None = None
    minute_source: MinuteSourceSemantics | None = None
    minute_capability_manifest_hash: str | None = None
    if dataset.get("frequency") == "minute":
        raw_semantics = dataset.get("minute_semantics")
        raw_source_semantics = raw_binding.get("minute_source_semantics")
        if not raw_semantics or not raw_source_semantics:
            raise QueryCatalogUnapprovedError(
                "旧分钟 lock 的复权、时区或可见性语义不完整，禁止准入"
            )
        minute_dataset = MinuteDatasetSemantics.from_mapping(raw_semantics)
        minute_source = MinuteSourceSemantics.from_mapping(raw_source_semantics)
        scope_contract = catalog.policies.get(minute_dataset.scope_policy_ref)
        if scope_contract is None or (
            scope_contract.get("policy_type"),
            scope_contract.get("status"),
            scope_contract.get("validator_id"),
            scope_contract.get("rules", {}).get("binding_hash"),
        ) != (
            "reference_scope",
            "approved",
            "catalog.minute.capability-manifest.v1",
            minute_source.scope_binding_hash,
        ):
            raise QueryCatalogUnapprovedError("分钟能力 manifest 与物理来源不一致")
        minute_capability_manifest_hash = str(
            scope_contract.get("rules", {}).get(
                "minute_capability_manifest_hash", ""
            )
        )
        if not minute_capability_manifest_hash:
            raise QueryCatalogUnapprovedError("分钟能力 manifest 缺少覆盖身份")
        availability_contract = catalog.policies.get(
            minute_dataset.availability_policy_ref
        )
        if availability_contract is None or (
            availability_contract.get("policy_type"),
            availability_contract.get("status"),
            dict(availability_contract.get("rules", {})),
        ) != (
            "availability",
            "approved",
            {
                "market_visible_after": "completed_bar_end",
                "same_day_realtime": False,
                "source_delivery": "historical_batch",
                "timezone": "Asia/Shanghai",
            },
        ):
            raise QueryCatalogUnapprovedError(
                "分钟 available_time policy 不能编译为已完成 bar 可见性规则"
            )
        if not isinstance(query.time_range, InstantRangeV2):
            raise QueryIRInvalidError("分钟 dataset 只接受 InstantRangeV2")
        if query.time_range.timezone != minute_dataset.timezone:
            raise QueryIRInvalidError("分钟查询时区与 Catalog dataset 语义不一致")
        if query.adjustment not in {"raw", "pre", "post"}:
            raise QueryIRInvalidError("分钟 query adjustment 不受支持")
        if query.adjustment != minute_source.adjustment_mode:
            raise QueryIRInvalidError("分钟 query adjustment 与物理来源不一致")
        if minute_source.adjustment_usage != "pit_allowed" and query.purpose.value != "audit":
            raise QueryIRInvalidError(
                "分钟来源仅限 analysis_only；调整价格进入历史信号必须绑定 PIT 因子快照"
            )
    elif isinstance(query.time_range, InstantRangeV2):
        raise QueryIRInvalidError("InstantRangeV2 只能用于分钟 dataset")
    if not attestation.passed:
        raise QueryAttestationStaleError("漂移证明未通过")
    if (
        attestation.catalog_hash != catalog.catalog_hash
        or attestation.binding_id != raw_binding["binding_id"]
        or attestation.expected_schema_revision != raw_binding["expected_schema_revision"]
        or attestation.current_schema_revision != raw_binding["expected_schema_revision"]
    ):
        raise QueryAttestationStaleError("漂移证明与当前 Catalog Lock/binding 不一致")
    drift_policy = catalog.policies.get(str(raw_binding["drift_policy_id"]))
    if drift_policy is None or attestation.policy_hash != PolicyContract(**dict(drift_policy)).content_hash:
        raise QueryAttestationStaleError("漂移证明的 policy hash 已失效")
    dataset_fields = tuple(str(item) for item in dataset["fields"])
    requested = set(query.field_ids)
    filter_fields = {item.field_id for item in query.filters}
    sort_fields = {item.field_id for item in query.sort}
    required = requested | filter_fields | sort_fields | {
        str(dataset["event_time_field"]),
        *tuple(str(item) for item in dataset["primary_key"]),
    }
    unknown = required - set(dataset_fields)
    if unknown:
        raise QueryIRInvalidError(f"字段不属于 dataset: {sorted(unknown)}")
    if not set(dataset["primary_key"]) <= requested or str(dataset["event_time_field"]) not in requested:
        raise QueryIRInvalidError("投影必须包含 event_time 与完整 primary_key")

    normalized_fields = tuple(item for item in dataset_fields if item in requested)
    normalized_filters = tuple(
        sorted(query.filters, key=lambda item: typed_canonical_hash(item.to_dict()))
    )
    normalized_sort = tuple(query.sort)
    primary_key = tuple(str(item) for item in dataset["primary_key"])
    sort_prefix = tuple(
        item.field_id for item in normalized_sort[: len(primary_key)]
    )
    if (
        len(sort_prefix) != len(primary_key)
        or sort_prefix != primary_key
    ):
        raise QueryIRInvalidError(
            "稳定排序必须以 dataset primary_key 的原顺序开头"
        )
    if any(item.descending for item in normalized_sort[: len(primary_key)]):
        raise QueryIRInvalidError("主键稳定排序必须为升序")

    instrument_field = _instrument_identifier_field(dataset, catalog)
    if instrument_field is None and query.universe.instruments:
        raise QueryIRInvalidError("非证券关系必须使用有界关系快照，不能传股票代码")
    temporal_selection, temporal_fields, input_claim_ceiling = (
        _compile_temporal_selection(
            query=query,
            dataset=dataset,
            catalog=catalog,
            required_fields=required,
            instrument_field=instrument_field,
        )
    )
    required |= temporal_fields
    unknown = required - set(dataset_fields)
    if unknown:
        raise QueryIRInvalidError(f"时态字段不属于 dataset: {sorted(unknown)}")
    field_items = {field_id: catalog.require_field(field_id) for field_id in required}
    for field_id in normalized_fields:
        allowed = tuple(str(item) for item in field_items[field_id].get("adjustment_allowed", ()))
        if allowed and query.adjustment not in allowed:
            raise QueryIRInvalidError(f"字段 {field_id} 不允许 adjustment={query.adjustment}")
    column_bindings = raw_binding["column_bindings"]
    columns: list[tuple[str, str]] = []
    for field_id in required:
        item = column_bindings.get(field_id)
        if not isinstance(item, dict) or item.get("kind") != "direct" or not item.get("column"):
            raise QueryCatalogUnapprovedError(f"字段缺少 direct binding: {field_id}")
        columns.append((field_id, str(item["column"])))
    columns.sort(key=lambda item: dataset_fields.index(item[0]))

    availability = catalog.policies.get(str(dataset["available_time_policy"]))
    revision = catalog.policies.get(str(dataset["revision_policy_id"]))
    if availability is None or revision is None:
        raise QueryCatalogUnapprovedError("dataset 缺少 availability/revision policy")
    daily_availability_policy_ref: str | None = None
    daily_availability_rule: str | None = None
    if (
        dataset.get("frequency") == "daily"
        and dataset.get("available_time_policy") == DAILY_AVAILABILITY_POLICY_REF
    ):
        if (
            availability.get("policy_type") != "availability"
            or availability.get("status") != "approved"
            or dict(availability.get("rules", {}))
            != {"available_after": DAILY_AVAILABILITY_RULE}
        ):
            raise QueryCatalogUnapprovedError(
                "available.daily.v1 必须编译为 next_session_open；"
                "修正 Catalog policy 后重新 admit"
            )
        daily_availability_policy_ref = DAILY_AVAILABILITY_POLICY_REF
        daily_availability_rule = DAILY_AVAILABILITY_RULE
    normalized_query = replace(
        query,
        field_ids=normalized_fields,
        filters=normalized_filters,
        sort=normalized_sort,
    )
    admitted_field_types = tuple(
        (field_id, str(field_items[field_id]["data_type"]))
        for field_id in dataset_fields
        if field_id in required
    )
    return AdmittedQueryPlan(
        query=normalized_query,
        catalog_hash=catalog.catalog_hash,
        binding_id=str(raw_binding["binding_id"]),
        binding_version=int(raw_binding["binding_version"]),
        attestation_hash=attestation.attestation_hash,
        source_profile=attestation.source_profile,
        environment=attestation.environment,
        expected_schema_revision=attestation.expected_schema_revision,
        availability_policy_hash=typed_canonical_hash(dict(availability)),
        revision_policy_hash=typed_canonical_hash(dict(revision)),
        object_name=str(raw_binding["object_name"]),
        columns=tuple(columns),
        field_types=admitted_field_types,
        field_nullables=tuple(
            (field_id, bool(field_items[field_id]["nullable"]))
            for field_id in dataset_fields
            if field_id in required
        ),
        primary_key=primary_key,
        event_time_field=str(dataset["event_time_field"]),
        instrument_field=instrument_field,
        temporal_selection=replace(
            temporal_selection,
            required_scan_fields=tuple(
                field_id for field_id in dataset_fields if field_id in required
            ),
            public_projection=normalized_fields,
            field_types=admitted_field_types,
        ),
        input_claim_ceiling=input_claim_ceiling,
        session_close_binding=temporal_selection.session_close_selection,
        daily_availability_policy_ref=daily_availability_policy_ref,
        daily_availability_rule=daily_availability_rule,
        minute_dataset_semantics_hash=(
            None if minute_dataset is None else minute_dataset.content_hash
        ),
        minute_source_semantics_hash=(
            None if minute_source is None else minute_source.content_hash
        ),
        minute_scope_binding_hash=(
            None if minute_source is None else minute_source.scope_binding_hash
        ),
        minute_capability_manifest_hash=minute_capability_manifest_hash,
        minute_asset_class=(
            None if minute_dataset is None else minute_dataset.asset_class
        ),
        minute_instrument_role=(
            None if minute_dataset is None else minute_dataset.instrument_role
        ),
        minute_session_policy_ref=(
            None if minute_dataset is None else minute_dataset.session_policy_ref
        ),
        minute_quality_policy_refs=(
            None if minute_dataset is None else minute_dataset.quality_policy_refs
        ),
        minute_timezone=(None if minute_dataset is None else minute_dataset.timezone),
        minute_timestamp_storage=(
            None if minute_dataset is None else minute_dataset.timestamp_storage
        ),
        minute_timestamp_role=(
            None if minute_dataset is None else minute_dataset.timestamp_role
        ),
        minute_bar_interval=(
            None if minute_dataset is None else minute_dataset.bar_interval
        ),
        minute_availability_rule=(
            None if minute_dataset is None else MINUTE_AVAILABILITY_RULE
        ),
        minute_time_normalization_version=(
            None if minute_dataset is None else MINUTE_TIME_NORMALIZATION_VERSION
        ),
        result_cardinality=str(dataset.get("result_cardinality", "one_or_more")),
        factor_publication=factor_publication,
    )


def _compile_temporal_selection(
    *,
    query: QueryIR,
    dataset: Any,
    catalog: CompiledCatalog,
    required_fields: set[str],
    instrument_field: str | None,
) -> tuple[TemporalSelectionPlan, set[str], str]:
    """把 Catalog policy 和 observation keys 编译为 Provider 直接执行的结构。"""

    availability = catalog.policies.get(str(dataset["available_time_policy"]))
    revision = catalog.policies.get(str(dataset["revision_policy_id"]))
    if availability is None or revision is None:
        raise QueryCatalogUnapprovedError("dataset 缺少 availability/revision policy")
    if availability.get("status") != "approved" or revision.get("status") != "approved":
        raise QueryCatalogUnapprovedError("时态 policy 未获批准")
    availability_rules = dict(availability.get("rules", {}))
    revision_rules = dict(revision.get("rules", {}))
    input_claim_ceiling = str(
        availability_rules.get("claim_ceiling", "tradable_simulation")
    )
    if input_claim_ceiling not in CLAIM_LEVELS:
        raise QueryCatalogUnapprovedError("availability policy claim_ceiling 无法映射")

    observations: list[tuple[str, dict[str, str]]] = []
    for field_id in dataset["fields"]:
        field = catalog.require_field(str(field_id))
        model = str(field.get("observation_model", "static_reference"))
        if model != "static_reference":
            observations.append(
                (model, {str(key): str(value) for key, value in dict(field.get("observation_keys", {})).items()})
            )
    models = {model for model, _ in observations}
    key_sets = {tuple(sorted(keys.items())) for _, keys in observations}
    if len(models) > 1 or len(key_sets) > 1:
        raise QueryCatalogUnapprovedError("dataset observation_model/keys 不一致")
    model = next(iter(models), "static_reference")
    keys = dict(next(iter(key_sets), ()))
    temporal_fields: set[str] = set()
    visibility_filter = None
    revision_selector = None
    interval_selector = None
    session_close_selection = None
    event_time_field = str(dataset["event_time_field"])
    available_field = keys.get("available_time_field")
    factor_next_open = (
        str(dataset["dataset_id"]).startswith("factor.")
        and dataset.get("frequency") == "daily"
        and str(dataset["available_time_policy"]) == DAILY_AVAILABILITY_POLICY_REF
    )

    if factor_next_open:
        temporal_fields.add(event_time_field)
        visibility_filter = VisibilityFilter(event_time_field, False)

    availability_policy_id = str(dataset["available_time_policy"])
    if availability_policy_id in FUTURES_SESSION_CLOSE_POLICY_IDS:
        if instrument_field is None:
            raise QueryCatalogUnapprovedError("session-close数据集必须有证券实体字段")
        expected_after = {
            "available.futures.daily.session-close.v1": "completed_session_close",
            "available.futures.session_close.v1": "session_close",
        }[availability_policy_id]
        if (
            availability.get("policy_type") != "availability"
            or availability_rules.get("available_after") != expected_after
            or availability_rules.get("same_session_signal_use") != "reject"
            or availability_rules.get("available_time_field") != available_field
        ):
            raise QueryCatalogUnapprovedError(
                "期货 session-close Catalog policy 无法编译为同 session 禁用规则"
            )
        if not isinstance(query.time_range, DateRangeV1):
            raise QueryIRInvalidError("期货 session-close 只接受有界日频 QueryIR")
        if query.universe.snapshot_id is not None or not query.universe.instruments:
            raise QueryCatalogUnapprovedError(
                "期货 session-close 必须声明 bundle 可核对的显式合约 universe"
            )
        try:
            bundle = load_futures_session_close_bundle()
            policies = bundle.resolve(
                availability_policy_id=availability_policy_id,
                instruments=query.universe.instruments,
                start=query.time_range.start,
                end=query.time_range.end,
            )
        except FuturesSessionCloseError as exc:
            raise QueryCatalogUnapprovedError(str(exc)) from exc
        facts: list[SessionCloseFact] = []
        mappings: dict[str, SessionCloseInstrumentBinding] = {}
        policy_bindings: list[SessionClosePolicyBinding] = []
        for policy in policies:
            calendar = bundle.calendar_binding_for(policy)
            timezone_name = calendar.timezone
            instrument_id = policy.instrument.instrument_id
            mapping = SessionCloseInstrumentBinding(
                instrument_id,
                "cn_future",
                policy.instrument.exchange,
                "".join(
                    character
                    for character in instrument_id.split(".", 1)[0]
                    if character.isalpha()
                ),
                timezone_name,
            )
            previous_mapping = mappings.setdefault(instrument_id, mapping)
            if previous_mapping != mapping:
                raise QueryCatalogUnapprovedError(
                    "session-close 同一合约的市场映射不一致"
                )
            selected_dates = tuple(
                trading_date
                for trading_date in policy.trading_dates
                if query.time_range.start <= trading_date <= query.time_range.end
            )
            if not selected_dates:
                raise QueryCatalogUnapprovedError(
                    "session-close policy 未覆盖请求日期"
                )
            policy_bindings.append(
                SessionClosePolicyBinding(
                    instrument_id,
                    policy.policy_id,
                    policy.revision,
                    policy.policy_hash,
                    selected_dates[0],
                    selected_dates[-1],
                    selected_dates,
                )
            )
            for trading_date in selected_dates:
                session = policy.build_session(
                    trading_date,
                    scope_binding_hash=bundle.bundle_hash,
                )
                day_segments = tuple(
                    item
                    for item in session.segments
                    if item.phase == "day" and item.bar_eligible
                )
                if not day_segments:
                    raise QueryCatalogUnapprovedError(
                        "session-close policy 缺少 eligible 日盘完成时刻"
                    )
                facts.append(
                    SessionCloseFact(
                        policy.instrument.instrument_id,
                        trading_date,
                        max(item.ends_at for item in day_segments),
                    )
                )
        session_close_selection = SessionCloseSelection(
            availability_policy_id,
            typed_canonical_hash(dict(availability)),
            bundle.bundle_id,
            bundle.bundle_hash,
            bundle.source_artifact_ref,
            bundle.source_acquired_at,
            instrument_field,
            event_time_field,
            tuple(sorted(mappings.values(), key=lambda item: item.instrument_id)),
            tuple(
                sorted(
                    policy_bindings,
                    key=lambda item: (
                        item.instrument_id,
                        item.coverage_start,
                        item.session_policy_id,
                        item.session_policy_revision,
                    ),
                )
            ),
            tuple(sorted(facts, key=lambda item: (item.instrument_id, item.session_date))),
        )

    if (
        available_field is not None
        and session_close_selection is None
        and not factor_next_open
    ):
        if availability_rules.get("available_time_field") not in {None, available_field}:
            raise QueryCatalogUnapprovedError("availability policy 与 observation keys 不一致")
        additional_available_fields = tuple(
            str(field)
            for field in (availability_rules.get("source_added_time_field"),)
            if field is not None
        )
        temporal_fields.update((available_field, *additional_available_fields))
        exclusive_rules = {
            "next_session_open_after_publication_date",
            "next_session_open_following_publication_date",
        }
        visibility_filter = VisibilityFilter(
            available_field,
            str(availability_rules.get("available_after")) not in exclusive_rules
            and availability_rules.get("same_day_use") != "reject"
            and availability_rules.get("same_publication_day_use") != "reject",
            additional_available_fields,
        )

    revision_field = keys.get("revision_field") or revision_rules.get("revision_field")
    if revision_field is not None:
        revision_field = str(revision_field)
        if revision_rules.get("revision_field") != revision_field:
            raise QueryCatalogUnapprovedError("revision policy 与 observation keys 不一致")
        if revision_rules.get("future_revision") != "reject":
            raise QueryCatalogUnapprovedError("revision policy 必须拒绝未来修订")
        temporal_fields.add(revision_field)
        entity_fields = tuple(
            str(item)
            for item in dataset["primary_key"]
            if str(item) != revision_field
        )
        if not entity_fields:
            raise QueryCatalogUnapprovedError("revision selector 缺少经济实体字段")
        order_fields = tuple(
            dict.fromkeys(
                item for item in (revision_field, available_field) if item is not None
            )
        )
        revision_selector = RevisionSelector(entity_fields, order_fields)
        temporal_fields.update(entity_fields)

    effective_from = keys.get("effective_from_field")
    effective_to = keys.get("effective_to_field")
    if (effective_from is None) != (effective_to is None):
        raise QueryCatalogUnapprovedError("effective interval 字段不完整")
    if effective_from is not None and effective_to is not None:
        if availability_rules.get("effective_from_field") not in {None, effective_from}:
            raise QueryCatalogUnapprovedError("effective_from policy 与字段不一致")
        if availability_rules.get("effective_to_field") not in {None, effective_to}:
            raise QueryCatalogUnapprovedError("effective_to policy 与字段不一致")
        entity_fields = tuple(
            str(item)
            for item in dataset["primary_key"]
            if str(item) not in {effective_from, effective_to}
        )
        if not entity_fields:
            raise QueryCatalogUnapprovedError("effective interval 缺少实体字段")
        endpoints = str(availability_rules.get("interval_endpoints", "left_closed"))
        interval_selector = EffectiveIntervalSelector(
            entity_fields,
            effective_from,
            effective_to,
            True,
            endpoints == "inclusive",
        )
        temporal_fields.update((*entity_fields, effective_from, effective_to))

    if session_close_selection is not None:
        source = (
            "query_as_of"
            if query.purpose.value == "audit"
            else "consumer_decision_time"
        )
    elif factor_next_open:
        source = (
            "query_as_of"
            if query.purpose.value == "audit"
            else "consumer_decision_time"
        )
    elif model in {"point_in_time", "interval_valid"}:
        source = (
            "query_as_of"
            if query.purpose.value == "audit"
            else "consumer_decision_time"
        )
    elif model == "market_event" and available_field == event_time_field:
        source = "event_time"
    else:
        source = "query_as_of"
    if source == "query_as_of" and (
        visibility_filter
        or revision_selector
        or interval_selector
        or session_close_selection
    ) and query.as_of is None:
        raise QueryIRInvalidError("时态 query_as_of 计划必须声明 QueryIR as_of")

    scan_fields = tuple(sorted(required_fields | temporal_fields))
    field_types = tuple(
        (field_id, str(catalog.require_field(field_id)["data_type"]).lower())
        for field_id in scan_fields
    )
    comparison_fields = set(temporal_fields)
    if source == "event_time":
        comparison_fields.add(event_time_field)
    needs_timezone = any(
        dict(field_types)[field_id] in {"timestamp", "timestamp[us]"}
        for field_id in comparison_fields
    )
    source_timezone = (
        query.time_range.timezone
        if isinstance(query.time_range, InstantRangeV2)
        else availability_rules.get("timezone")
    )
    if session_close_selection is not None:
        timezones = {
            item.timezone for item in session_close_selection.instrument_bindings
        }
        if len(timezones) != 1:
            raise QueryCatalogUnapprovedError(
                "session-close 请求内合约必须使用同一来源时区"
            )
        source_timezone = next(iter(timezones))
    if source_timezone is not None:
        source_timezone = str(source_timezone)
    if needs_timezone and source_timezone is None:
        raise QueryCatalogUnapprovedError(
            "无时区 timestamp 的时态 policy 必须声明 timezone"
        )
    return (
        TemporalSelectionPlan(
            SelectionClock(source),
            event_time_field,
            scan_fields,
            tuple(query.field_ids),
            field_types,
            source_timezone,
            visibility_filter,
            revision_selector,
            interval_selector,
            session_close_selection,
        ),
        temporal_fields,
        input_claim_ceiling,
    )


def _instrument_identifier_field(
    dataset: Any,
    catalog: CompiledCatalog,
) -> str | None:
    if str(dataset.get("entity_axis", "instrument")) == "relation":
        return None
    dataset_fields = tuple(str(item) for item in dataset["fields"])
    identifier_candidates = [
        field_id
        for field_id in dataset_fields
        if str(catalog.require_field(field_id)["semantic_type"]) == "identifier"
    ]
    instrument_candidates = [
        field_id
        for field_id in identifier_candidates
        if any(
            segment.startswith("instrument")
            for segment in str(catalog.require_field(field_id)["logical_name"]).split(".")
        )
    ]
    if not instrument_candidates and len(identifier_candidates) == 1:
        instrument_candidates = identifier_candidates
    if len(instrument_candidates) != 1:
        raise QueryIRInvalidError("dataset 必须有唯一 instrument identifier")
    return instrument_candidates[0]


__all__ = [
    "ADMITTED_PLAN_VERSION",
    "DAILY_AVAILABILITY_POLICY_REF",
    "DAILY_AVAILABILITY_RULE",
    "MINUTE_AVAILABILITY_RULE",
    "MINUTE_TIME_NORMALIZATION_VERSION",
    "AdmittedQueryPlan",
    "admit_query",
]
