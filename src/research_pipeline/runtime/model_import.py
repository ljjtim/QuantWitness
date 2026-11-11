"""完整封存模型作为新研究输入，保留来源训练事实与原失败状态。"""
from __future__ import annotations

import json
from collections.abc import Mapping

from .errors import RuntimeIntegrityError
from .external_artifact import ExternalArtifactCommit, ExternalArtifactStore
from .operator_runtime import RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue


def freeze_completed_model_source(parameters):
    from .model_import_source import freeze_completed_model_source as freeze
    return freeze(parameters)


def verify_completed_model_source(parameters):
    from .model_import_source import verify_completed_model_source as verify
    return verify(parameters)


def execute_model_import_artifacts(*, source_manifest, external_store):
    """来源完整复验后导入原正式工件，不重写模型或split元数据。"""
    from .model_import_source import verify_completed_model_source
    if isinstance(source_manifest, str):
        try:
            source_manifest = json.loads(source_manifest)
        except (ValueError, TypeError) as exc:
            raise RuntimeIntegrityError("已训练模型来源manifest无法解析") from exc
    if not isinstance(source_manifest, Mapping):
        raise RuntimeIntegrityError("已训练模型来源manifest必须是映射")
    verify_completed_model_source({"source_manifest_json": json.dumps(source_manifest, ensure_ascii=False)})
    source = ExternalArtifactStore(source_manifest["source_run_root"] + "/external-artifacts", create=False)
    values = {}
    for port, field in (("splits", "split_commit"), ("models", "fit_commit")):
        commit = ExternalArtifactCommit.from_dict(source_manifest[field])
        if commit.artifact_name != port:
            raise RuntimeIntegrityError("已训练模型来源端口名称不一致")
        imported = external_store.import_verified(source, commit)
        values[port] = RuntimeNodeValue.external(imported)
    provenance = (f"导入模型来源run={source_manifest['source_run_id']}，"
                  f"训练节点={source_manifest['source_fit_node_id']}，"
                  f"来源运行状态={source_manifest['source_record']['status']}；本次导入不执行训练。")
    limitations = [provenance]
    for diagnostic in source_manifest['fit_execution']['diagnostics']:
        context = diagnostic.get('failure_context', {})
        limitations.append("来源训练资源验收=" + str(diagnostic.get('error_code'))
                          + "，实测与预算=" + json.dumps(context.get('exceeded', {}), ensure_ascii=False, sort_keys=True))
    return RuntimeNodeOutputs(values, completion_metadata=RuntimeCompletionMetadata(
        artifact_hashes={port:value.external_commit.semantic_hash for port,value in values.items()},
        backend_id="research.completed-model-input.v1", fidelity="source_training_preserved",
        limitations=tuple(limitations)))
