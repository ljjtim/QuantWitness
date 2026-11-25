"""把已发布的分钟金融快照作为独立输入交给新的研究图。"""

from pathlib import Path

from research_pipeline.data_plane.minute_adjustment import ADJUSTMENT_SNAPSHOT_ARTIFACT
from research_pipeline.data_plane.verification_lifecycle import RunScopedArtifactVerification
from research_pipeline.platform.operator_contracts import _plain_json
from research_pipeline.runtime.adapters.common import _environment, _parameters
from research_pipeline.runtime.contracts import ArtifactRef
from research_pipeline.runtime.external_artifact import ExternalArtifactStore
from research_pipeline.runtime.operator_runtime import RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue


def execute_data_minute_adjustment_snapshot_import_v1(context):
    environment = _environment(context)
    parameters = _parameters(context)
    source = parameters["published_snapshot"]
    if sorted(parameters["request_ids"]) != sorted(source["request_hashes"]):
        raise ValueError("金融快照声明的请求与来源凭证不一致")
    for request_id, plan_hash in source["request_hashes"].items():
        if environment.manifest["admitted_plan_hashes"].get(request_id) != plan_hash:
            raise ValueError(f"金融快照请求准入身份改变: {request_id}")
    if (environment.fixed_clock, environment.root_seed) != (source["fixed_clock"], source["root_seed"]):
        raise ValueError("金融快照研究时钟或种子改变")
    if (environment.database is None or
            environment.database.resolve() != Path(source["database"]).resolve()):
        raise ValueError("金融快照数据库路径改变")
    if environment.load_published_snapshot is None:
        raise ValueError("金融快照导入缺少已发布 Result 读取入口")
    _, approved = environment.load_published_snapshot(
        source["run_root"], context.external_store.root.parent, environment.database,
    )
    if approved != _plain_json(source):
        raise ValueError("金融快照来源 Result、准入请求或已提交工件身份改变")
    reference = ArtifactRef.from_dict(dict(source["artifact_ref"]))
    store = ExternalArtifactStore(
        Path(source["run_root"]) / "external-artifacts",
        create=False,
        verification_session=RunScopedArtifactVerification(source["run_id"]),
    )
    commit = store.verify(reference.artifact_key)
    if commit.artifact_ref != reference or commit.artifact_type != ADJUSTMENT_SNAPSHOT_ARTIFACT or commit.artifact_name != "snapshot":
        raise ValueError("已发布金融快照工件身份改变")
    imported = context.external_store.import_verified(store, commit)
    return RuntimeNodeOutputs(
        {"snapshot": RuntimeNodeValue.external(imported)},
        completion_metadata=RuntimeCompletionMetadata(
            artifact_hashes={"snapshot": imported.semantic_hash},
            limitations=(*source["limitations"],
                         f"金融快照来源 Result {source['result_id']}，运行 {source['run_id']}"),
        ),
    )
