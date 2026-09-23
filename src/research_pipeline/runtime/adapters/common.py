"""common 算子族及其直接共享实现。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
import shutil
from typing import Mapping, MutableMapping
from zoneinfo import ZoneInfo
from research_pipeline.platform import canonical_json
from research_pipeline.data_plane.research_data_bundle import merge_research_data_bundles
from research_pipeline.domain import CorporateAction
from research_pipeline.research.semantics import ResearchSemantics
from ..operator_runtime import OperatorRuntimeContext, RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue


@dataclass(frozen=True)
class ResearchRunEnvironment:
    """正式 run 在所有节点间共享的显式、不可变输入。"""

    plan_root: Path
    manifest: Mapping[str, object]
    admitted_plans: Mapping[str, object]
    database: Path
    source_databases: Mapping[str, Path]
    minute_data_root: Path | None
    holdout_ledger_anchor: Path
    node_parameters: Mapping[str, Mapping[str, object]]
    semantics: ResearchSemantics | None
    fixed_clock: str
    root_seed: int
    workers: int
    resource_timeout_seconds: float | None
    strategy_spec_hashes: Mapping[str, str]
    operator_graph_strategy_hash: str
    study_reproduction_proof: object | None
    captured: MutableMapping[str, Mapping[str, object]]


def _environment(context: OperatorRuntimeContext) -> ResearchRunEnvironment:
    environment = context.environment
    if not isinstance(environment, ResearchRunEnvironment):
        raise ValueError("正式 Runtime 缺少 ResearchRunEnvironment")
    return environment


def _bound_admitted_plans(
    environment: ResearchRunEnvironment,
    request_ids: tuple[str, ...],
) -> Mapping[str, object]:
    """仅把当前节点声明的 request 对应计划交给业务执行器。"""

    selected = {}
    for request_id in request_ids:
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("节点 request_id 必须是非空字符串")
        if request_id in selected or request_id not in environment.admitted_plans:
            raise ValueError(f"节点绑定的 admitted plan 缺失或重复: {request_id}")
        selected[request_id] = environment.admitted_plans[request_id]
    return selected


def _input_admitted_plans(
    environment: ResearchRunEnvironment, bundle: Mapping[str, object],
) -> Mapping[str, object]:
    """从当前节点的显式数据输入闭合计划，并核对正式 claim lineage。"""

    references = bundle.get("references")
    if not isinstance(references, Mapping) or not references:
        raise ValueError("数据输入缺少 references")
    request_ids = tuple(str(request_id) for request_id in references)
    selected = _bound_admitted_plans(environment, request_ids)
    manifest = environment.manifest
    raw_request_ids = manifest.get("consumed_request_ids") if isinstance(manifest, Mapping) else None
    raw_ceilings = manifest.get("input_claim_ceilings") if isinstance(manifest, Mapping) else None
    if raw_request_ids is None and raw_ceilings is None:
        return selected
    if (
        not isinstance(raw_request_ids, list)
        or raw_request_ids != sorted(set(raw_request_ids))
        or not isinstance(raw_ceilings, Mapping)
        or set(raw_ceilings) != set(raw_request_ids)
        or not set(request_ids) <= set(raw_request_ids)
    ):
        raise ValueError("正式 plan 的 consumed request claim lineage 无效")
    for request_id, plan in selected.items():
        if raw_ceilings[request_id] != getattr(plan, "input_claim_ceiling", None):
            raise ValueError("正式 plan 的 input claim ceiling 与准入计划不一致")
    return selected


def _bound_data_bundle(
    bundle: Mapping[str, object], request_ids: tuple[str, ...],
) -> Mapping[str, object]:
    """只向当前业务节点交付显式绑定的数据引用。"""

    references = bundle.get("references")
    if not isinstance(references, Mapping):
        raise ValueError("数据输入缺少 references")
    if len(request_ids) != len(set(request_ids)) or not set(request_ids) <= set(references):
        raise ValueError("节点绑定的数据引用缺失或重复")
    return {**bundle, "references": {request_id: references[request_id] for request_id in request_ids}}


def _parameters(context: OperatorRuntimeContext) -> Mapping[str, object]:
    environment = _environment(context)
    parameters = environment.node_parameters.get(context.node.node_id)
    if not isinstance(parameters, Mapping):
        raise ValueError(f"统一 Runtime 缺少节点参数: {context.node.node_id}")
    return parameters


def _semantics(context: OperatorRuntimeContext, label: str) -> ResearchSemantics:
    semantics = _environment(context).semantics
    if semantics is None:
        raise ValueError(f"{label} Runtime 缺少 ResearchSemantics")
    return semantics


def _strategy_spec_hash(context: OperatorRuntimeContext, strategy_id: str) -> str:
    value = _environment(context).strategy_spec_hashes.get(strategy_id)
    if not isinstance(value, str):
        raise ValueError(f"Runtime 缺少已准入 StrategySpec: {strategy_id}")
    return value


def _capture(
    context: OperatorRuntimeContext,
    name: str,
    payload: Mapping[str, object],
) -> None:
    _environment(context).captured[name] = payload


def _external_result(
    context: OperatorRuntimeContext,
    payload: Mapping[str, object],
    *,
    directories: Mapping[str, Path] | None = None,
    parquet_rows: Mapping[str, list[Mapping[str, object]]] | None = None,
    completion_metadata: RuntimeCompletionMetadata | None = None,
) -> RuntimeNodeOutputs:
    staging = context.external_store.prepare()
    try:
        (staging / "result.json").write_text(
            canonical_json(_json_ready(dict(payload))), encoding="utf-8"
        )
        for name, source in sorted((directories or {}).items()):
            resolved = source.resolve()
            if not resolved.is_dir():
                raise ValueError(f"统一 Runtime 待提交目录不存在: {name}")
            shutil.copytree(resolved, staging / name)
        if parquet_rows:
            import pyarrow as pa
            import pyarrow.parquet as pq

            for prefix, rows in sorted(parquet_rows.items()):
                destination = staging / prefix / "part-00000.parquet"
                destination.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    pa.Table.from_pylist([dict(item) for item in rows]),
                    destination,
                )
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return RuntimeNodeOutputs(
        {commit.artifact_name: RuntimeNodeValue.external(commit)},
        completion_metadata=completion_metadata,
    )


def _factor_external_result(context: OperatorRuntimeContext, executor, **kwargs):
    staging = context.external_store.prepare()
    try:
        payload = executor(output_root=staging, **kwargs)
        commit = context.external_store.commit(
            staging,
            artifact_name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return payload, RuntimeNodeOutputs.single(RuntimeNodeValue.external(commit))


def _with_completion_metadata(
    outputs: RuntimeNodeOutputs,
    metadata: RuntimeCompletionMetadata,
) -> RuntimeNodeOutputs:
    """由领域 adapter 给已提交输出附上通用收尾元数据。"""

    return RuntimeNodeOutputs(outputs.values, completion_metadata=metadata)


def _verify_input_directories(
    context: OperatorRuntimeContext,
    port: str,
    directories: Mapping[str, Path],
) -> None:
    value = context.inputs.get(port)
    if value is None or value.external_commit is None:
        raise ValueError(f"统一 Runtime 输入不是外部目录工件: {port}")
    for prefix, source in sorted(directories.items()):
        context.external_store.verify_snapshot(
            value.external_commit, source, prefix=prefix
        )


def _input_external_root(context: OperatorRuntimeContext, port: str) -> Path:
    value = context.inputs.get(port)
    if value is None or value.external_commit is None:
        raise ValueError(f"统一 Runtime 输入不是外部目录工件: {port}")
    commit = context.external_store.verify(value.external_commit.semantic_hash)
    if commit != value.external_commit:
        raise ValueError(f"统一 Runtime 输入目录工件发生漂移: {port}")
    return context.external_store.objects_root / commit.semantic_hash


def _input_external_payload(
    context: OperatorRuntimeContext, port: str
) -> Mapping[str, object]:
    root = _input_external_root(context, port)
    try:
        payload = json.loads((root / "result.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"统一 Runtime 输入结果不可读: {port}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"统一 Runtime 输入结果不是对象: {port}")
    return payload


def _input_data_bundle(
    context: OperatorRuntimeContext, port: str = "data"
) -> Mapping[str, object]:
    payload = _input_external_payload(context, port)
    bundle = payload.get("data_bundle")
    if not isinstance(bundle, Mapping):
        raise ValueError(f"统一 Runtime 数据输入缺少 data_bundle: {port}")
    return bundle


def _input_merged_data_bundle(
    context: OperatorRuntimeContext, *ports: str
) -> Mapping[str, object]:
    """从显式入边合并普通列式与 raw 分钟数据引用。"""

    return merge_research_data_bundles(
        _input_data_bundle(context, port) for port in ports
    )


def _local_datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    zone = ZoneInfo("Asia/Shanghai")
    return parsed.replace(tzinfo=zone) if parsed.tzinfo is None else parsed.astimezone(zone)


def _local_date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{field} 不是有效日期") from exc


def _corporate_action_from_dict(payload: Mapping[str, object]) -> CorporateAction:
    return CorporateAction.from_dict(payload)


def _json_ready(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
