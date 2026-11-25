"""完整封存模型作为新研究输入，保留来源训练事实与原失败状态。"""
from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from pathlib import Path

from .errors import RuntimeIntegrityError
from .external_artifact import ExternalArtifactCommit, ExternalArtifactStore
from .operator_runtime import RuntimeCompletionMetadata, RuntimeNodeOutputs, RuntimeNodeValue


def freeze_completed_model_source(parameters):
    from .model_import_source import freeze_completed_model_source as freeze
    return freeze(parameters)


def verify_completed_model_source(parameters):
    from .model_import_source import verify_completed_model_source as verify
    return verify(parameters)


def _import_from_published_result(source_manifest, external_store):
    from .model_import_source import _verify_published_result_bundle
    result_dir = Path(source_manifest["source_result_directory"]).resolve(strict=True)
    store_root = Path(source_manifest["source_result_store"]).resolve(strict=True)
    bundle = _verify_published_result_bundle(result_dir, store_root)
    if bundle.result_id != source_manifest["source_result_id"]:
        raise RuntimeIntegrityError("已发布模型 Result 身份发生变化")
    values = {}
    configs = (
        ("splits", "research.model-split-manifest.v2", source_manifest["split_tables"], source_manifest["source_split_artifact"], source_manifest["split_files"]),
        ("models", "research.model-fits.v2", source_manifest["fit_tables"], source_manifest["source_fit_artifact"], source_manifest["fit_files"]),
    )
    for port, artifact_type, tables, artifact_key, expected_files in configs:
        staging = external_store.prepare()
        for table in tables:
            prefix = f"tables/{table['table_id']}/"
            for rel_path in table["files"]:
                target_rel = f"{table['path_prefix']}/{rel_path.removeprefix(prefix)}"
                dest = staging / target_rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(result_dir / rel_path, dest)
        for support in bundle.support_files:
            if support.artifact_key == artifact_key:
                dest = staging / support.source_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(result_dir / support.relative_path, dest)
        committed = external_store.commit(staging, artifact_name=port, artifact_type=artifact_type)
        if committed.files != dict(expected_files):
            raise RuntimeIntegrityError(f"导入模型工件内容与冻结Result不一致: {port}")
        values[port] = RuntimeNodeValue.external(committed)
    return values


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
    if source_manifest.get("source_kind") == "published_result":
        values = _import_from_published_result(source_manifest, external_store)
        provenance = (f"导入模型来源result={source_manifest['source_result_id']}，"
                      f"run={source_manifest['source_run_id']}，"
                      f"训练节点={source_manifest['source_fit_node_id']}；本次导入不执行训练。")
    else:
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
