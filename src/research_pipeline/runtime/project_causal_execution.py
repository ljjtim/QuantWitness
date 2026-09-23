"""正式项目因果输出的键批调度与 ExternalArtifact 封口。"""

from __future__ import annotations

from dataclasses import replace
from collections.abc import Sequence
from pathlib import Path
import shutil

import pandas as pd
import pyarrow.parquet as pq

from research_pipeline.data_plane import ArtifactResolver, DatasetArtifactRef, PartitionedDatasetRef
from research_pipeline.data_plane.minute_scan import build_minute_partitioned_dataset
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.project_causal_contract import parse_causal_plan
from .contracts import ArtifactRef
from .errors import RuntimeIntegrityError
from .operator_runtime import RuntimeNodeOutputs, RuntimeNodeValue, OperatorRuntimeContext
from .project_causal import CausalReadTrace
from .project_operator_runtime import ProjectRuntimeInput, execute_project_worker_attempt
from .project_output import ProjectOutputRoot


def execute_project_causal_node(service, *, node_context, run_id, attempt_id, environment):
    parameters = service.project_parameters_by_node[node_context.node.node_id]
    plan = parse_causal_plan(parameters["causal_plan"])
    token = service.project_registry.project_token_by_implementation(node_context.node.implementation_id)
    formal_type = "research.feature-set.v1" if plan.kind == "feature" else "research.label.v1"
    if {port.port: port.artifact_type for port in token.manifest.operator_spec.output_ports}.get(plan.output_port) != formal_type:
        raise RuntimeIntegrityError("项目 causal_plan 输出端口或类型未绑定")
    inputs, identities, timezones = _causal_inputs(node_context, environment, plan)
    external = node_context.external_store
    committed_by_port = {}
    inherited = ()
    state = None
    # carry 扩展可按调用次数更新状态，不能随内存预算改变其键批边界。
    max_keys = 8192
    if node_context.effective_resource_budget.memory_bytes < 256 * 1024**2:
        raise RuntimeIntegrityError("正式 causal worker memory 低于 256 MiB 支持下限")
    for index, item in enumerate(plan.iter_work_items(max_keys=max_keys)):
        state_input = state if plan.state_scope == "carry" else None
        lineage = inherited if state_input is not None else ()
        CausalReadTrace(plan, item, lineage, source_partition_identities=identities)
        work_plan = replace(plan, work_items=(item,))
        work_root = node_context.work_dir / f"causal-{index:06d}"
        work_root.mkdir()
        result = execute_project_worker_attempt(
            registry=service.project_registry, implementation_id=node_context.node.implementation_id,
            node_id=node_context.node.node_id, run_id=run_id, attempt_id=attempt_id,
            attempt_root=work_root, inputs=inputs,
            parameters={**parameters, "causal_plan": work_plan.to_dict()},
            fixed_clock=node_context.fixed_clock, root_seed=node_context.root_seed,
            budget=node_context.effective_resource_budget, state_in=state_input,
            causal_context={"plan": work_plan.to_dict(), "inherited_lineage": list(lineage),
                            "partition_identities": identities, "source_timezones": timezones},
        )
        facts = result.causal_facts
        if facts is None or tuple(facts["key_columns"]) != plan.key_columns:
            raise RuntimeIntegrityError("项目输出缺少核心键批事实")
        # 核心在提交前再次验证状态来源与本次允许窗口，再用已有 attach 合同一一合并。
        checked = CausalReadTrace(plan, item, facts["lineage"], source_partition_identities=identities) if plan.state_scope == "carry" else None
        core_facts = pd.DataFrame(facts["records"])
        for output in result.outputs:
            metadata = pq.ParquetFile(output.path).metadata
            if metadata.num_rows != len(item.key_rows):
                raise RuntimeIntegrityError("项目 causal 输出行数与冻结键批不一致")
            uncompressed = sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
            if uncompressed * 4 > node_context.effective_resource_budget.memory_bytes // 2:
                raise RuntimeIntegrityError("项目 causal 输出超过核心合并内存预算")
            staging = external.prepare()
            target = staging / output.port / "data.parquet"
            target.parent.mkdir()
            shutil.copyfile(output.path, target)
            committed = external.commit(
                staging, artifact_name=output.port, artifact_type=output.artifact_type,
                producer_scope="project",
                core_time_facts=core_facts if output.port == plan.output_port else None,
                causal_time_key_columns=plan.key_columns if output.port == plan.output_port else (),
            )
            committed_by_port.setdefault(output.port, []).append(committed)
        state = result.state
        inherited = tuple(facts["lineage"]) if state is not None else ()
        if checked is not None:
            checked.core_time_facts()
    values = {}
    for port, commits in committed_by_port.items():
        staging = external.prepare()
        writer_root = ProjectOutputRoot(staging)
        first = external.objects_root / commits[0].semantic_hash / next(iter(commits[0].files))
        schema = pq.ParquetFile(first).schema_arrow

        def batches(commits=commits):
            for commit in commits:
                external.verify(commit.semantic_hash)
                for relative in commit.files:
                    if relative.endswith(".parquet"):
                        yield from pq.ParquetFile(external.objects_root / commit.semantic_hash / relative).iter_batches(batch_size=8192)

        writer_root.write_batches(port=port, artifact_type=commits[0].artifact_type,
                                  relative_path=f"{port}/data.parquet", schema=schema, batches=batches())
        # 输入均为上面已经过核心 attach 的正式提交；这里只规范化文件布局。
        final = external.commit(staging, artifact_name=port, artifact_type=commits[0].artifact_type)
        values[port] = RuntimeNodeValue.external(final)
    return RuntimeNodeOutputs(values)


def _causal_inputs(node_context, environment, plan):
    from research_pipeline.runtime.adapters.common import _input_data_bundle, _input_external_root

    context = OperatorRuntimeContext(node_context, environment)
    inputs, identities, timezones = [], {}, {}
    if set(node_context.inputs) != {source.port for source in plan.sources}:
        raise RuntimeIntegrityError("正式 causal 输入必须全部由冻结来源覆盖")
    for source in plan.sources:
        bundle = _input_data_bundle(context, source.port)
        raw = bundle.get("references", {}).get(source.request_id)
        admitted = getattr(environment, "admitted_plans", {}).get(source.request_id)
        if raw is None or admitted is None:
            raise RuntimeIntegrityError("正式 causal 输入缺少实际 request 工件或准入计划")
        if bundle.get("admitted_plan_hashes", {}).get(source.request_id) != admitted.plan_hash:
            raise RuntimeIntegrityError("正式 causal request 与准入计划身份不一致")
        partitions = {partition for item in plan.work_items for partition in item.source_partitions[source.port]}
        if node_context.inputs[source.port].artifact_ref.artifact_type in {"data.minute-bars.v1", "data.minute-bars.1m.v1"}:
            runtime_input, source_ids, timezone = prepare_causal_minute_input(
                node_context, environment, source, raw, admitted, partition_ids=partitions
            )
            inputs.append(runtime_input)
            identities[source.port] = source_ids
            timezones[source.port] = timezone
            continue
        reference = DatasetArtifactRef.from_dict(raw)
        root = _input_external_root(context, source.port) / "data"
        verified = ArtifactResolver(root).resolve(reference)
        if verified.manifest.get("admitted_plan_hash") != admitted.plan_hash:
            raise RuntimeIntegrityError("正式 causal snapshot 与准入计划身份不一致")
        snapshot_root = root / reference.relative_path
        files = {str(item["relative_path"]): str(item["sha256"]) for item in verified.manifest["files"]}
        value = node_context.inputs[source.port]
        inputs.append(ProjectRuntimeInput.from_verified(
            ArtifactRef(source.port, value.artifact_ref.artifact_type,
                        reference.physical_snapshot_id, reference.manifest_hash),
            b"", reference.schema_hash, source_root=snapshot_root, files=files,
        ))
        identities[source.port] = {partition: [f"{reference.physical_snapshot_id}/{name}" for name in files] for partition in partitions}
        timezones[source.port] = admitted.minute_timezone or admitted.temporal_selection.source_timezone
    return tuple(inputs), identities, timezones


def prepare_causal_minute_input(
    node_context, environment, source, raw, admitted, *, partition_ids: Sequence[str]
):
    """当前准入扫描计划必须生成同一个原始分钟引用，不能借用其他来源。"""
    from research_pipeline.runtime.adapters.minute_io import _minute_scan_plan_from_environment

    dataset = PartitionedDatasetRef.from_dict(raw)
    scan = _minute_scan_plan_from_environment(
        OperatorRuntimeContext(node_context, environment), source.request_id
    )
    if dataset != build_minute_partitioned_dataset(scan):
        raise RuntimeIntegrityError("项目因果分钟输入与当前准入扫描计划不一致")
    if dataset.lineage.get("admitted_plan_hash") != admitted.plan_hash:
        raise RuntimeIntegrityError("项目因果分钟输入的准入计划身份不一致")
    selected = tuple(sorted(set(partition_ids)))
    by_key = {partition.partition_key: partition for partition in dataset.partitions}
    if not selected or not set(selected) <= set(by_key):
        raise RuntimeIntegrityError("项目因果分钟来源月份不在实际已验证分区中")
    column_map = dict(admitted.columns)
    if not set(source.columns) <= set(column_map) or not {
        column_map[column] for column in source.columns
    } <= set(dataset.allowed_columns):
        raise RuntimeIntegrityError("项目因果分钟逻辑列与物理投影不闭合")
    root = Path(environment.minute_data_root).resolve(strict=True)
    content = canonical_json({
        "partitioned_dataset": dataset.to_dict(),
        "allowed_roots": {"minute_data": str(root)},
        "column_map": {column: column_map[column] for column in source.columns},
    }).encode("utf-8")
    value = node_context.inputs[source.port]
    item = ProjectRuntimeInput.from_verified(
        ArtifactRef(source.port, value.artifact_ref.artifact_type,
                    dataset.reference_id, dataset.reference_id),
        content, typed_canonical_hash([partition.schema_hash for partition in dataset.partitions]),
    )
    identities = {
        key: [typed_canonical_hash(by_key[key].to_dict())] for key in selected
    }
    return item, identities, admitted.minute_timezone
