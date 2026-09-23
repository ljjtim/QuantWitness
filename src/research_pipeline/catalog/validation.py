"""目录合同集合的交叉引用校验。"""

from collections import Counter
from collections.abc import Iterable

from .errors import CatalogReferenceError
from .models import (
    ApprovalDecision,
    CatalogContract,
    DatasetContract,
    FieldContract,
    PhysicalBindingContract,
    PolicyContract,
    TransformContract,
)


_CROSS_SECTIONAL_FIELDS = {
    "cn_universe.industry.pit": {
        "fld_xs_industry_l1": ("category", "dimensionless"),
        "fld_xs_industry_l2": ("category", "dimensionless"),
        "fld_xs_industry_l3": ("category", "dimensionless"),
    },
    "cn_equity.market_cap.pit": {
        "fld_xs_market_cap_total": ("market_capitalization", "CNY_100_million"),
        "fld_xs_market_cap_circulating": (
            "market_capitalization",
            "CNY_100_million",
        ),
    },
    "cn_universe.index_constituent_weight.pit": {
        "fld_xs_index_weight": ("weight", "percent"),
    },
    "cn_universe.st_status.pit": {
        "fld_xs_st_flag": ("flag", "dimensionless"),
    },
    "cn_universe.suspension_status.pit": {
        "fld_xs_suspension_paused": ("flag", "dimensionless"),
    },
}


def validate_contract_set(contracts: Iterable[CatalogContract]) -> tuple[CatalogContract, ...]:
    items = tuple(contracts)
    identity_groups = {
        "field": [item.field_id for item in items if isinstance(item, FieldContract)],
        "dataset": [item.dataset_id for item in items if isinstance(item, DatasetContract)],
        "policy": [item.policy_id for item in items if isinstance(item, PolicyContract)],
        "binding": [item.binding_id for item in items if isinstance(item, PhysicalBindingContract)],
        "transform": [item.transform_id for item in items if isinstance(item, TransformContract)],
    }
    for kind, identifiers in identity_groups.items():
        duplicates = sorted(
            identifier
            for identifier, count in Counter(identifiers).items()
            if count > 1
        )
        if duplicates:
            raise CatalogReferenceError(f"{kind} 永久 ID 重复: {duplicates}")
    fields = {item.field_id: item for item in items if isinstance(item, FieldContract)}
    datasets = {item.dataset_id: item for item in items if isinstance(item, DatasetContract)}
    policies = {item.policy_id: item for item in items if isinstance(item, PolicyContract)}
    bindings = {
        item.binding_id: item
        for item in items
        if isinstance(item, PhysicalBindingContract)
    }
    aliases = [name for item in fields.values() for name in (item.logical_name, *item.aliases)]
    duplicate_aliases = sorted(name for name, count in Counter(aliases).items() if count > 1)
    if duplicate_aliases:
        raise CatalogReferenceError(f"字段逻辑名或别名重复: {duplicate_aliases}")
    for dataset in datasets.values():
        missing = set(dataset.fields) - set(fields)
        if missing:
            raise CatalogReferenceError(f"dataset {dataset.dataset_id} 引用未知字段: {sorted(missing)}")
        for field_id in dataset.fields:
            unknown_observation_fields = set(
                fields[field_id].observation_keys.values()
            ) - set(dataset.fields)
            if unknown_observation_fields:
                raise CatalogReferenceError(
                    f"field {field_id} observation_keys 引用 dataset 外字段: "
                    f"{sorted(unknown_observation_fields)}"
                )
        for policy_id in (dataset.available_time_policy, dataset.drift_policy_id, dataset.revision_policy_id):
            if policy_id not in policies:
                raise CatalogReferenceError(f"dataset {dataset.dataset_id} 引用未知 policy: {policy_id}")
        availability_policy = policies[dataset.available_time_policy]
        revision_policy = policies[dataset.revision_policy_id]
        temporal_fields = [
            fields[field_id]
            for field_id in dataset.fields
            if fields[field_id].observation_model != "static_reference"
        ]
        if temporal_fields:
            available_fields = {
                str(item.observation_keys["available_time_field"])
                for item in temporal_fields
            }
            if availability_policy.rules.get("available_time_field") not in available_fields:
                raise CatalogReferenceError(
                    f"dataset {dataset.dataset_id} availability policy 未绑定可见时间字段"
                )
            source_added_field = availability_policy.rules.get(
                "source_added_time_field"
            )
            if source_added_field is not None and source_added_field not in dataset.fields:
                raise CatalogReferenceError(
                    f"dataset {dataset.dataset_id} availability policy 的源加入时间字段不存在"
                )
        revision_fields = {
            str(item.observation_keys["revision_field"])
            for item in temporal_fields
            if item.observation_model == "point_in_time"
        }
        if revision_fields:
            if revision_policy.rules.get("future_revision") != "reject":
                raise CatalogReferenceError(
                    f"dataset {dataset.dataset_id} 必须拒绝未来修订"
                )
            if revision_policy.rules.get("revision_field") not in revision_fields:
                raise CatalogReferenceError(
                    f"dataset {dataset.dataset_id} revision policy 未绑定修订字段"
                )
        interval_fields = [
            item for item in temporal_fields if item.observation_model == "interval_valid"
        ]
        if interval_fields:
            effective_from = {
                str(item.observation_keys["effective_from_field"])
                for item in interval_fields
            }
            effective_to = {
                str(item.observation_keys["effective_to_field"])
                for item in interval_fields
            }
            if availability_policy.rules.get("effective_from_field") not in effective_from:
                raise CatalogReferenceError(
                    f"dataset {dataset.dataset_id} 未绑定 effective_from 字段"
                )
            if availability_policy.rules.get("effective_to_field") not in effective_to:
                raise CatalogReferenceError(
                    f"dataset {dataset.dataset_id} 未绑定 effective_to 字段"
                )
        if dataset.minute_semantics:
            from .minute import MinuteDatasetSemantics

            semantics = MinuteDatasetSemantics.from_mapping(dataset.minute_semantics)
            scope_policy = policies.get(semantics.scope_policy_ref)
            if scope_policy is None or scope_policy.policy_type != "reference_scope":
                raise CatalogReferenceError(
                    f"分钟 dataset {dataset.dataset_id} 缺少 reference scope policy"
                )
            expected_pair = {
                "cn_stock": ("cn_stock", "equity"),
                "cn_etf": ("cn_etf", "etf"),
                "cn_index": ("cn_index", "index"),
                "cn_future": ("cn_future", "future_contract"),
            }[semantics.asset_class]
            if (dataset.market, dataset.instrument_type) != expected_pair:
                raise CatalogReferenceError("分钟 dataset 资产身份与基础合同不一致")
    minute_binding_counts: Counter[str] = Counter()
    for binding in (item for item in items if isinstance(item, PhysicalBindingContract)):
        dataset = datasets.get(binding.dataset_id)
        if dataset is None or dataset.dataset_version != binding.dataset_version:
            raise CatalogReferenceError(f"binding {binding.binding_id} 引用未知 dataset/version")
        if binding.drift_policy_id not in policies:
            raise CatalogReferenceError(f"binding {binding.binding_id} 引用未知 drift policy")
        if set(binding.column_bindings) != set(dataset.fields):
            raise CatalogReferenceError(f"binding {binding.binding_id} 列映射必须完整覆盖 dataset fields")
        direct_field_ids = {
            str(field_id)
            for field_id, value in binding.column_bindings.items()
            if value.get("kind") == "direct" and "column" in value
        }
        temporal_key_fields = {
            str(reference)
            for field_id in dataset.fields
            for reference in fields[field_id].observation_keys.values()
        }
        if binding.status == "approved" and not temporal_key_fields <= direct_field_ids:
            raise CatalogReferenceError(
                f"binding {binding.binding_id} 缺少时态语义字段的直接物理映射"
            )
        if dataset.minute_semantics:
            from .minute import MinuteDatasetSemantics, MinuteSourceSemantics

            if not binding.minute_source_semantics:
                raise CatalogReferenceError("新分钟 dataset 的 binding 缺少来源复权语义")
            semantics = MinuteDatasetSemantics.from_mapping(dataset.minute_semantics)
            source = MinuteSourceSemantics.from_mapping(binding.minute_source_semantics)
            scope_policy = policies[semantics.scope_policy_ref]
            if source.scope_binding_hash != scope_policy.rules["binding_hash"]:
                raise CatalogReferenceError("分钟来源与 reference scope binding 不一致")
            physical_columns = {
                str(item["column"]) for item in binding.column_bindings.values()
            }
            required_columns = {
                "code", "dt", "open", "high", "low", "close",
                "volume", "money", "avg",
            }
            if semantics.asset_class == "cn_future":
                required_columns.add("open_interest")
            missing_columns = required_columns - physical_columns
            if missing_columns:
                raise CatalogReferenceError(
                    f"分钟 binding 缺少完整字段: {sorted(missing_columns)}"
                )
            minute_binding_counts[dataset.dataset_id] += 1
        elif binding.minute_source_semantics:
            raise CatalogReferenceError("非分钟语义 dataset 不能声明分钟来源语义")
    for dataset in datasets.values():
        if dataset.minute_semantics and minute_binding_counts[dataset.dataset_id] < 1:
            raise CatalogReferenceError("新分钟 dataset 必须有物理 binding")
    _validate_cross_sectional_contracts(datasets, fields, bindings)
    for transform in (item for item in items if isinstance(item, TransformContract)):
        if not transform.input_schema:
            missing = (set(transform.inputs) | set(transform.outputs)) - set(fields)
            if missing:
                raise CatalogReferenceError(f"transform {transform.transform_id} 引用未知字段: {sorted(missing)}")
    transforms = [item for item in items if isinstance(item, TransformContract)]
    output_owners: dict[str, str] = {}
    for transform in transforms:
        if transform.input_schema:
            continue
        for output in transform.outputs:
            owner = output_owners.setdefault(output, transform.transform_id)
            if owner != transform.transform_id:
                raise CatalogReferenceError(
                    f"Transform 输出字段冲突: {output}/{owner}/{transform.transform_id}"
                )
        if transform.time_policy.get("kind") == "label" and transform.time_policy.get(
            "available_after"
        ) not in {"exit", "settlement"}:
            raise CatalogReferenceError("标签 Transform 只能在退出或结算事实后可见")
    dependency_graph = {
        transform.transform_id: {
            output_owners[input_field]
            for input_field in transform.inputs
            if input_field in output_owners
            and output_owners[input_field] != transform.transform_id
        }
        for transform in transforms if not transform.input_schema
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(transform_id: str) -> None:
        if transform_id in visiting:
            raise CatalogReferenceError("Transform 依赖存在环")
        if transform_id in visited:
            return
        visiting.add(transform_id)
        for dependency in dependency_graph[transform_id]:
            visit(dependency)
        visiting.remove(transform_id)
        visited.add(transform_id)

    for transform_id in dependency_graph:
        visit(transform_id)
    decisions = [item for item in items if isinstance(item, ApprovalDecision)]
    decision_keys = [(item.target_kind, item.target_id) for item in decisions]
    if len(decision_keys) != len(set(decision_keys)):
        raise CatalogReferenceError("同一目标不能存在多个 ApprovalDecision")
    return items


def _validate_cross_sectional_contracts(datasets, fields, bindings) -> None:
    """锁死横截面单位、源对象和缺证 binding，防止草案被误批准。"""

    for dataset_id, expected_fields in _CROSS_SECTIONAL_FIELDS.items():
        if dataset_id not in datasets:
            continue
        for field_id, expected in expected_fields.items():
            field = fields.get(field_id)
            if field is None or (field.semantic_type, field.unit) != expected:
                raise CatalogReferenceError(
                    f"横截面 PIT 字段单位或语义错误: {field_id}"
                )
    blocked_datasets = {
        "cn_universe.industry.pit",
        "cn_equity.market_cap.pit",
        "cn_universe.index_constituent_weight.pit",
        "cn_universe.st_status.pit",
        "cn_instrument.listing_status.pit",
        "cn_universe.suspension_status.pit",
    }
    for binding in bindings.values():
        if binding.dataset_id in blocked_datasets and binding.status != "blocked":
            raise CatalogReferenceError(
                "横截面 PIT 缺少历史可见性证据，binding 必须 blocked: "
                f"{binding.dataset_id}"
            )
        if binding.dataset_id != "cn_universe.suspension_status.pit":
            continue
        if binding.object_name != "daily_price":
            raise CatalogReferenceError("停牌 PIT binding 必须使用 raw daily_price 投影")
        if set(binding.column_bindings) != {
            "fld_xs_suspension_date",
            "fld_xs_suspension_code",
            "fld_xs_suspension_paused",
        }:
            raise CatalogReferenceError("停牌 PIT binding 只能包含 date/code/paused")


__all__ = ["validate_contract_set"]
