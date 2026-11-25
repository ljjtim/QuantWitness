"""已发布分钟金融快照的来源验收。"""

from datetime import datetime
import json
from pathlib import Path

from research_pipeline.application.grid_data_contract import _columnar_materialization_plans, _verify_data_bundle
from research_pipeline.application.research_plan_store import _recipe_ancestors, load_operator_graph_research_plan
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.audit_mode import strict_audit_enabled, strict_audit_scope
from research_pipeline.platform.operator_contracts import OperatorGraphRecipe
from research_pipeline.results import ResultStore
from research_pipeline.data_plane.minute_adjustment import ADJUSTMENT_SNAPSHOT_ARTIFACT
from research_pipeline.runtime.contracts import ArtifactRef
from research_pipeline.runtime.diagnostics import read_finalize_status
from research_pipeline.runtime.execution_service import RuntimeExecutionService
from research_pipeline.runtime.identity import AuditEnvironmentManifest, DeterminismContext
from research_pipeline.runtime.operator_runtime import RuntimeCompletionMetadata
from research_pipeline.runtime.store import EventStore


SNAPSHOT_TABLE_SCHEMA = "data.adjustment-factor-snapshot.payload.v2"


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _snapshot_table(tables):
    tables = [table for table in tables if table.schema_id == SNAPSHOT_TABLE_SCHEMA
              and table.artifact_type == ADJUSTMENT_SNAPSHOT_ARTIFACT]
    if len(tables) != 1:
        raise ValueError("来源 Result 必须发布唯一的金融快照表")
    return tables[0]


def _published_snapshot_references(table, recipe, outputs):
    snapshot = outputs.get(table.source_node_id, {}).get(table.source_port)
    if (snapshot is None or snapshot.artifact_type != ADJUSTMENT_SNAPSHOT_ARTIFACT or
            snapshot.artifact_key != table.artifact_key or snapshot.content_hash != table.artifact_manifest_hash):
        raise ValueError("金融快照 Result 未宣布对应的已提交快照输出")

    by_id = {node.node_id: node for node in recipe.nodes}
    producers = [node for node in recipe.nodes if node.operator_id == "data.minute.adjustment_snapshot"]
    data_bindings = set()
    for producer in producers:
        bindings = [binding for binding in producer.inputs if binding.input_port == "data"]
        if (len(bindings) != 1 or
                by_id[bindings[0].source_node_id].operator_id != "data.columnar.materialize"):
            raise ValueError("金融快照计算节点缺少正式列式数据来源")
        data_bindings.add((bindings[0].source_node_id, bindings[0].source_output_port))
    if len(data_bindings) != 1:
        raise ValueError("来源 Result 的金融快照须绑定唯一正式列式数据输出")
    data_node, data_port = next(iter(data_bindings))
    data = outputs.get(data_node, {}).get(data_port)
    if data is None or data.artifact_type != "data.columnar-bundle.v1":
        raise ValueError("金融快照缺少已提交的正式列式数据输出")
    return snapshot, data


def _snapshot_request_hashes(recipe, dag, plans):
    requests = set()
    for node in recipe.nodes:
        if node.operator_id not in {"data.catalog.admission", "data.columnar.materialize"}:
            requests.update(node.declared_request_ids)
    nodes = {node.node_id: node for node in recipe.nodes}
    if any(edge.artifact_type == "data.columnar-bundle.v1" and edge.target_node in nodes
           and nodes[edge.target_node].operator_id != "data.minute.adjustment_snapshot" for edge in dag.edges):
        # 扩展节点拿到整个列式输入时，声明无法证明它只读取其中几个请求。
        requests.update(_columnar_materialization_plans(plans))
    if not requests or not requests <= plans.keys():
        raise ValueError("金融快照实际使用的准入请求未闭合")
    if any(plans[key].source_profile != "source" for key in requests):
        raise ValueError("金融快照实际请求须绑定主只读数据库")
    return {key: plans[key].plan_hash for key in sorted(requests)}


def load_published_adjustment_snapshot(source_run, target_run_root, database):
    """核对正式运行、原计划、当前生产实现和 Result 中的工件引用。"""
    source_run = Path(source_run).resolve(strict=True)
    invocation = _read(source_run / "operator-dag-invocation.json")
    if invocation.pop("invocation_hash") != typed_canonical_hash(invocation):
        raise ValueError("金融快照来源调用凭证身份不一致")
    with strict_audit_scope(strict_audit_enabled(), frozen_cache_audit=invocation.get("strict_audit", False)):
        return _verify_published_snapshot(source_run, target_run_root, database, invocation)


def _verify_published_snapshot(source_run, target_run_root, database, invocation):
    finalize = read_finalize_status(source_run)
    if finalize["status"] != "succeeded" or finalize["result_published"] is not True:
        raise ValueError("金融快照来源未成功发布 Result")
    result_directory = Path(finalize["result_directory"]).resolve(strict=True)
    result = ResultStore(result_directory.parents[2], create=False).open_snapshot(
        result_directory, verify_schema_ids=(SNAPSHOT_TABLE_SCHEMA,), verify_all_files=False,
    ).bundle
    table = _snapshot_table(result.tables)
    manifest, plans, dag, recipe, registry = load_operator_graph_research_plan(
        target=invocation["plan"], published_source_node_id=table.source_node_id,
    )
    record = _read(source_run / "operator-dag-run.json")
    audit = AuditEnvironmentManifest.from_dict(record["audit_environment"])
    if audit.manifest_digest != record["audit_manifest_digest"]:
        raise ValueError("金融快照来源环境身份不一致")
    service = RuntimeExecutionService(
        audit_environment=audit, numerical_backend_names=tuple(sorted(audit.dependency_distribution_digests)),
        project_registry=registry,
    )
    source = service._open_reuse_sources(
        (source_run,), target_root=Path(target_run_root).resolve(),
        node_identity_projection=manifest["node_identity_projection"],
    )[0]
    _, run_id, checkpoints, external = source
    if record["dag"] != dag.to_dict() or record["project_id"] != manifest["package_id"]:
        raise ValueError("已发布金融快照运行与原准入计划不一致")
    if (result.result_id != finalize["result_id"] or result.run_id != run_id or
            result.plan_hash != manifest["package_plan_hash"]):
        raise ValueError("金融快照 Result 与已发布运行或原计划不一致")
    node_ids = _recipe_ancestors(recipe, (table.source_node_id,))
    recipe = OperatorGraphRecipe.build(
        graph_id=recipe.graph_id, nodes=tuple(node for node in recipe.nodes if node.node_id in node_ids),
    )
    node_outputs = {
        node_id: {port: ArtifactRef.from_dict(ref) for port, ref in ports.items()}
        for node_id, ports in record["outputs"].items()
        if node_id in node_ids
    }
    reference, data = _published_snapshot_references(table, recipe, node_outputs)
    request_hashes = _snapshot_request_hashes(recipe, dag, plans)
    events = EventStore(source_run).read_events()
    executions = {event.node_id: event.payload["node_execution_id"]
                  for event in events if event.kind == "checkpoint_committed"}
    context = DeterminismContext(manifest["root_seed"], datetime.fromisoformat(manifest["fixed_clock"]))
    limitations = set()
    for node in dag.nodes:
        if node.node_id not in node_ids:
            continue
        inputs = tuple(node_outputs[edge.source_node][edge.source_port]
                       for edge in sorted(dag.edges, key=lambda edge: edge.target_port)
                       if edge.target_node == node.node_id)
        _, expectation = service._identity(node, inputs, context)
        if executions.get(node.node_id) != expectation.node_execution_id:
            raise ValueError(f"金融快照节点 {node.node_id} 的当前实现、输入或运行环境改变，请重新准备")
        checkpoint = checkpoints.verify(expectation)
        if checkpoint.outputs != node_outputs[node.node_id]:
            raise ValueError(f"已发布金融快照节点 {node.node_id} 的输出身份不一致")
        metadata = _read(checkpoints.checkpoints_root / expectation.node_execution_id / checkpoint.content_path)[
            "completion_metadata"]
        if metadata is not None:
            limitations.update(RuntimeCompletionMetadata.from_dict(metadata).limitations)

    database = Path(database).resolve(strict=True)
    if invocation["data_db"] is None or Path(invocation["data_db"]).resolve(strict=True) != database:
        raise ValueError("金融快照数据库来源改变，请重新准备")
    facts = external.read_committed_json(data, "result.json")
    stat = database.stat()
    fingerprint = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    if not facts["database_unchanged"] or facts["database_fingerprint_after"] != fingerprint:
        raise ValueError("金融快照数据库已变化，请重新准备")
    _verify_data_bundle(
        facts["data_bundle"], {key: plans[key] for key in request_hashes},
        external.objects_root / data.artifact_key / "data",
    )
    return recipe, {
        "result_directory": str(result_directory), "run_root": str(source_run), "run_id": run_id, "result_id": result.result_id,
        "artifact_ref": reference.to_dict(), "request_hashes": request_hashes,
        "fixed_clock": manifest["fixed_clock"], "root_seed": manifest["root_seed"],
        "database": str(database), "database_fingerprint": fingerprint,
        "limitations": sorted(limitations),
    }
