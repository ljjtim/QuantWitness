"""封存模型输入的共同来源验收；不加载pickle或改写来源运行。"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from research_pipeline.data_plane import DataPlaneError, PathRolePolicy
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.research.dataframe_budget import PandasFrameBudget
from research_pipeline.research.modeling.project_model import validate_project_model_adapter
from research_pipeline.research.modeling.qlib import SEQUENCE_MODEL_CLASSES, normalize_candidates
from research_pipeline.results.contracts import QLIB_MODEL_INVENTORY_SCHEMA_ID, ResultBundle
from .checkpoint import CheckpointStore
from .errors import RuntimeIntegrityError
from .external_artifact import ExternalArtifactStore
from .liveness import require_inactive_owner
from .store import EventStore

SCHEMA = "research.completed-model-source.v1"
SOURCE_FIELDS = ("source_run_root", "source_fit_artifact", "source_split_artifact",
                 "source_fit_node_id", "source_split_node_id")


def _require(condition, message):
    if not condition:
        raise RuntimeIntegrityError("已训练模型输入: " + message)


def _json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeIntegrityError("已训练模型来源JSON无法读取: " + str(path)) from exc


def _source_execution(root, record, events, projection, fit_id, split_id, fit_commit, split_commit):
    _require(record.get("contract_version") == "research-runtime-operator-dag-run-v3", "来源运行合同不受支持")
    _require(record.get("status") in {"failed", "succeeded"} and projection.run_status == record["status"], "来源必须已终止")
    _require(projection.run_id == record.get("run_id"), "来源run身份不一致")
    require_inactive_owner(root)
    _require(projection.node_statuses.get(split_id) == "succeeded", "来源split未成功")
    completed = [event for event in events if event.node_id == fit_id and event.kind == "execution_completed"]
    _require(bool(completed), "来源fit缺少完整计算事件")
    completed = [completed[-1]]
    attempt = completed[0].attempt_id
    _require(projection.attempt_statuses.get(attempt) in {"failed","succeeded"}, "来源fit完整计算attempt未终止")
    prepared = [event for event in events if event.node_id == fit_id and event.attempt_id == attempt and event.kind == "checkpoint_prepared"]
    _require(len(prepared) == 1, "来源fit缺少prepared事实")
    diagnostics = [event.payload for event in events if event.node_id == fit_id and event.attempt_id == attempt and event.kind == "diagnostic"]
    succeeded = projection.node_statuses.get(fit_id) == "succeeded"
    _require(succeeded or projection.attempt_statuses.get(attempt)=="failed", "来源fit不是已结束失败attempt")
    _require(succeeded or (len(diagnostics) == 1 and diagnostics[0].get("error_code") == "resource_exceeded"), "仅接收成功fit或完整计算后的资源超限fit")
    execution_id = prepared[0].payload["node_execution_id"]
    store = CheckpointStore(root, create=False)
    target = store.checkpoints_root / execution_id if succeeded else store.staging_root / attempt
    checkpoint = store.verify_stored(execution_id) if succeeded else store._verify_content(target, execution_id)
    split_events = [event for event in events if event.node_id==split_id and event.kind=='checkpoint_committed']
    _require(bool(split_events),"来源split缺少提交事件")
    split_checkpoint=store.verify_stored(split_events[-1].payload['node_execution_id'])
    _require(split_checkpoint.outputs=={'splits':split_commit.artifact_ref},"来源split提交与声明工件不一致")
    _require(checkpoint.outputs == {"models": fit_commit.artifact_ref}, "来源prepared输出不是声明的模型工件")
    _require(split_commit.artifact_ref in checkpoint.inputs, "来源fit未消费声明split")
    _require(completed[0].payload.get("result_hash") == typed_canonical_hash({"models":fit_commit.artifact_ref.to_dict()}), "计算完成事件与模型工件不一致")
    _require(checkpoint.root_seed == record["root_seed"] and checkpoint.fixed_clock == record["fixed_clock"], "来源seed/clock不一致")
    _require(checkpoint.audit_environment_digest == record["audit_manifest_digest"], "来源实现环境不一致")
    nodes = {node["node_id"]:node for node in record["dag"]["nodes"]}
    _require(nodes.get(split_id, {}).get("implementation_id") == split_checkpoint.implementation_id
             == "research.model.split-manifest.v2", "来源split不是正式模型生产节点")
    _require(nodes[fit_id]["implementation_id"] == checkpoint.implementation_id, "来源fit实现身份不一致")
    if not succeeded:
        _require(prepared[0].seq > completed[0].seq and all(event.seq > prepared[0].seq for event in events if event.node_id==fit_id and event.attempt_id==attempt and event.kind=='diagnostic'), "来源失败不是完整封口后的资源验收")
    return {"node_execution_id":execution_id, "attempt_id":attempt,
            "checkpoint_manifest":checkpoint.to_dict(), "diagnostics":diagnostics,
            "execution_completed":completed[0].payload, "training_node_succeeded":succeeded}


def _validate_models(fit_root, split_root, fit_commit, fit_metadata, split_metadata, record):
    from .walk_forward_model_execution import _load_table, _split_from_payload, _table_paths
    from research_pipeline.research.validation import issue_fit_scope, build_seed_manifest
    budget = PandasFrameBudget(1024**3)
    inventory, _ = _load_table(fit_root, "models", frame_budget=budget)
    audit_columns = ("candidate_id", "fold_id", "fit_scope_ref", "fit_scope_certificate_hash", "model_path", "config_path", "train_ids_json", "validation_ids_json")
    from research_pipeline.platform.canonical import typed_canonical_hash_streamed
    import pyarrow.parquet as pq
    def audit_records():
        for path in _table_paths(fit_root,"fit_audit"):
            for batch in pq.ParquetFile(path).iter_batches(batch_size=1):
                yield from batch.to_pylist()
    _require(typed_canonical_hash_streamed(audit_records())==fit_metadata['table_hashes']['fit_audit'],"拟合审计内容漂移")
    # 审计中的成员JSON仅逐行消费；不同时物化全部候选的重复样本列表。
    import pyarrow.parquet as pq
    audit_lookup = {}
    for path in _table_paths(fit_root,"fit_audit"):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=1,columns=list(audit_columns)):
            item = batch.to_pylist()[0]
            key=(item['candidate_id'],item['fold_id'])
            _require(key not in audit_lookup,"拟合审计重复")
            audit_lookup[key]= {key:value for key,value in item.items() if key not in {'train_ids_json','validation_ids_json'}}
            fold_source = next(fold for fold in split_metadata['split_manifest']['folds'] if fold['fold_id']==key[1])
            _require(set(json.loads(item['train_ids_json']))==set(fold_source['train_ids']) and set(json.loads(item['validation_ids_json']))==set(fold_source['validation_ids']),"训练审计样本不一致")
    if fit_metadata['row_counts'].get('learning_curves',0):
        curves, _ = _load_table(fit_root, "learning_curves", frame_budget=budget)
    else:
        from research_pipeline.platform.canonical import typed_canonical_hash_streamed
        _require(fit_metadata['table_hashes']['learning_curves']==typed_canonical_hash_streamed(iter(())),"空学习曲线身份无效")
        curves=pd.DataFrame(columns=['candidate_id','fold_id'])
    _require(fit_metadata.get("status") == "model_fit_succeeded", "模型计算工件未完整结束")
    _require(fit_metadata.get("split_artifact_hash") == split_metadata["artifact_hash"], "模型split身份不一致")
    _require(fit_metadata.get("root_seed") == record["root_seed"], "模型训练seed不一致")
    dependencies = fit_metadata["preflight"]["dependencies"]
    for name, expected in dependencies.items():
        _require(importlib.metadata.version(name) == expected, "模型依赖版本不兼容: " + name)
    split = _split_from_payload(split_metadata["split_manifest"], pd.DataFrame())
    candidates = {item["candidate_id"]:item for item in fit_metadata["search_manifest"]["candidates"]}
    folds = {item.fold_id:item for item in split.folds}
    expected = {(candidate, fold) for candidate in candidates for fold in folds}
    actual = list(zip(inventory.candidate_id, inventory.fold_id))
    _require(len(actual) == len(set(actual)) and set(actual) == expected and inventory.status.eq("fitted").all(), "候选与fold模型集合不完整")
    _require(set(audit_lookup) == expected and len(audit_lookup) == len(expected), "拟合审计集合不完整")
    narrow = ds.dataset([str(path) for path in sorted((split_root/"samples").glob("*.parquet"))],format="parquet")
    features = split_metadata["feature_columns"]
    results = []
    for fold_id, fold in folds.items():
        ids = tuple(fold.train_ids) + tuple(fold.validation_ids) + tuple(fold.test_ids)
        facts = narrow.to_table(columns=["sample_id","decision_time","feature_available_time","label_available_time","label_end_time"],filter=ds.field("sample_id").isin(ids)).to_pandas()
        _require(not facts.sample_id.duplicated().any() and set(facts.sample_id) == set(ids), "训练/验证样本事实缺失")
        train = facts.loc[facts.sample_id.isin(fold.train_ids)]
        valid = facts.loc[facts.sample_id.isin(fold.validation_ids)]
        test = facts.loc[facts.sample_id.isin(fold.test_ids)]
        _require(not train.empty and not valid.empty and set(fold.train_ids).isdisjoint(fold.validation_ids), "训练验证资格无效")
        _require((pd.to_datetime(facts.feature_available_time,utc=True)<=pd.to_datetime(facts.decision_time,utc=True)).all(), "Feature晚于决策时点")
        _require(pd.to_datetime(train.decision_time,utc=True).max()<pd.to_datetime(valid.decision_time,utc=True).min(), "训练验证时序无效")
        _require(pd.to_datetime(train.label_available_time,utc=True).max()<=pd.to_datetime(valid.decision_time,utc=True).min(), "训练标签在拟合边界不可见")
        _require(pd.to_datetime(train.label_end_time,utc=True).max()<pd.to_datetime(valid.decision_time,utc=True).min(), "训练标签跨验证起点")
        if not test.empty:
            _require(pd.to_datetime(valid.label_end_time,utc=True).max()<pd.to_datetime(test.decision_time,utc=True).min() and pd.to_datetime(valid.label_available_time,utc=True).max()<=pd.to_datetime(test.decision_time,utc=True).min(), "验证标签在test起点未成熟")
        for row in inventory.loc[inventory.fold_id.eq(fold_id)].to_dict("records"):
            config_path = row["config_path"]
            _require(config_path in fit_commit.files, "模型配置未正式封存")
            config = _json(fit_root/config_path)
            candidate = normalize_candidates([config["candidate"]])[0]
            sequence = candidate["model"]["class"] in SEQUENCE_MODEL_CLASSES
            _require(config["schema"] == ("research.qlib-sequence-model-bundle.v2" if sequence else "research.qlib-model-bundle.v1"), "模型schema不是当前版本")
            _require(typed_canonical_hash(candidate) == candidates[row["candidate_id"]]["parameter_hash"], "模型候选配置替换")
            _require(config["feature_columns"] == features and json.loads(row["feature_columns_json"]) == features, "特征顺序不一致")
            _require(set(config["train_ids"]) == set(fold.train_ids) and set(config["valid_ids"]) == set(fold.validation_ids), "拟合范围不一致")
            _require(config["root_seed"] == record["root_seed"] and config["model_path"] == row["model_path"], "模型seed/路径不一致")
            for name, expected_version in config["versions"].items():
                actual_version = platform.python_version() if name == "python" else importlib.metadata.version(name)
                _require(actual_version == expected_version, "模型运行依赖不兼容: " + name)
            component = "qlib:" + row["candidate_id"] + ":" + fold_id
            seed = build_seed_manifest(root_seed=record["root_seed"], research_identity_hash=fit_metadata["research_identity_hash"],component_ids=(component,))
            certificate = issue_fit_scope(component_id=component,component_kind="transformer",split_manifest=split,fold_id=fold_id,input_columns=tuple(features),parameters=candidate,code_hash=typed_canonical_hash({"implementation":fit_metadata["contract_version"]}),environment_hash=fit_metadata["preflight"]["preflight_hash"],seed_manifest=seed)
            scope = audit_lookup[(row["candidate_id"],fold_id)]
            _require(config["fit_scope_ref"] == row["fit_scope_ref"] == scope["fit_scope_ref"] == certificate.certificate_hash, "训练scope证书不一致")
            minimum_fit = max(pd.to_datetime(valid.label_available_time,utc=True).max(),pd.to_datetime(valid.label_end_time,utc=True).max())
            _require(pd.Timestamp(config["fit_time"])>=minimum_fit and config["fit_time"]==row["fit_time"], "模型拟合可见时间无效")
            files = [config["model_path"], *[path for values in config["processor_files"].values() for path in values]]
            if sequence:
                _require(config["sequence"]["feature_columns"]==features and config["sequence"]["step_len"]==split_metadata["sequence"]["step_len"] and config["sequence"]["calendar_sessions"]==split_metadata["sequence"]["calendar_sessions"], "序列冻结事实不一致")
                files += [config["weights_path"], *config["sequence"]["files"].values(), *config["upstream_sources"]]
                _require(not curves.loc[curves.candidate_id.eq(row["candidate_id"]) & curves.fold_id.eq(fold_id)].empty, "模型学习曲线缺失")
            _require(all(path in fit_commit.files for path in files), "模型或处理器支持文件未封存")
            if candidate["model"]["class"] == "ProjectQlibModel":
                saved=config["project_model"]
                _require(saved["adapter"]==candidate["model"]["kwargs"]["adapter"], "项目模型adapter替换")
                validate_project_model_adapter(saved["adapter"],check_environment=True,bundle_path=fit_root/saved["bundle_path"])
            results.append({"candidate_id":row["candidate_id"],"fold_id":fold_id,"config_path":config_path,"fit_scope_ref":certificate.certificate_hash,"candidate":candidate})
    return results


def _source_research_facts(root, split_metadata):
    from research_pipeline.research.semantics import ResearchSemantics
    semantics = _json(root/'research.semantics.json')
    identity = _json(root/'research.identity.json')
    invocation = _json(root/'operator-dag-invocation.json')
    plan = _json(Path(invocation['plan'])/'research-plan.json')
    semantic_payload = {key:value for key,value in semantics.items() if key!='effective_claim_level'}
    parsed = ResearchSemantics.from_dict(semantic_payload)
    _require(plan.get('research_identity')==identity, '来源研究身份与原冻结计划不一致')
    _require(plan.get('research_semantics')==parsed.to_dict(), '来源研究语义与原冻结计划不一致')
    _require(semantics.get('effective_claim_level')==plan.get('effective_claim_level'), '来源研究claim不一致')
    _require(split_metadata.get('semantics_hash')==parsed.semantics_hash, '来源split未绑定研究语义')
    return semantics, identity


LEGACY_GUIDANCE = (
    "该历史 Result 未封存正式模型导入所需的 model-provenance.json 或模型元数据；"
    "历史已发布 Result 可改用 `ResultStore.open_snapshot` 与 `predict_bundle` 读取预测，"
    "如需通过 `research.model.import` 复训/导入，请使用当前版本重新运行并发布 Result。"
)


def _stream_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_published_result_bundle(result_dir: Path, store_root: Path) -> ResultBundle:
    """在不跨层依赖 ResultStore 的前提下完成已发布 Result 全量校验。"""
    try:
        policy = PathRolePolicy()
        resolved_store = policy.resolve_root(store_root, role="result_store")
        resolved_dir = policy.resolve_contained_path(
            allowed_root=resolved_store,
            candidate=result_dir,
            root_role="result_store",
            path_role="result_bundle",
            expected_kind="directory",
        )
        manifest_path, marker_path = policy.resolve_manifest_files(
            allowed_root=resolved_dir,
            relative_paths=("result.json", "COMMITTED"),
            root_role="result_bundle",
            file_role="result_control",
        )
        raw = manifest_path.read_text(encoding="utf-8")
        payload = json.loads(raw)
        marker = marker_path.read_text(encoding="ascii")
        if not isinstance(payload, dict):
            raise RuntimeIntegrityError("Result manifest 必须是对象")
        bundle = ResultBundle.from_dict(payload)
        if raw != canonical_json(bundle.to_dict()) or marker != bundle.result_id:
            raise RuntimeIntegrityError("Result manifest 或 COMMITTED 发生漂移")
        relative = resolved_dir.relative_to(resolved_store)
        if relative.parts[-3:] != (bundle.project_id, bundle.run_id, bundle.result_id):
            raise RuntimeIntegrityError("Result namespace 与内容身份不一致")
        actual_files: set[str] = set()
        for directory, dirnames, filenames in policy.walk(resolved_dir):
            directory_path = Path(directory)
            for name in sorted(dirnames):
                policy.resolve_contained_path(
                    allowed_root=resolved_dir,
                    candidate=directory_path / name,
                    root_role="result_bundle",
                    path_role="result_directory",
                    expected_kind="directory",
                )
            for name in sorted(filenames):
                policy.resolve_contained_path(
                    allowed_root=resolved_dir,
                    candidate=directory_path / name,
                    root_role="result_bundle",
                    path_role="result_file",
                    expected_kind="file",
                )
                actual_files.add((directory_path / name).relative_to(resolved_dir).as_posix())
        expected_files = {
            "result.json",
            "COMMITTED",
            *bundle.control_files,
            *(path for table in bundle.tables for path in table.files),
            *(item.relative_path for item in bundle.support_files),
        }
        missing_files = expected_files - actual_files
        extra_files = actual_files - expected_files - {"execution-cost.json"}
        if missing_files or extra_files:
            raise RuntimeIntegrityError("Result 文件集合不精确或含额外文件")
        if "execution-cost.json" in actual_files:
            cost_payload = json.loads((resolved_dir / "execution-cost.json").read_text(encoding="utf-8"))
            if not isinstance(cost_payload, dict):
                raise RuntimeIntegrityError("Result execution-cost.json 必须是对象")
        for control_name, expected_digest in bundle.control_files.items():
            digest = _stream_file_sha256(resolved_dir / control_name)
            if digest != expected_digest:
                raise RuntimeIntegrityError(f"Result 控制文件摘要漂移: {control_name}")
        for support in bundle.support_files:
            digest = _stream_file_sha256(resolved_dir / support.relative_path)
            if digest != support.content_hash:
                raise RuntimeIntegrityError(f"Result 支持文件摘要漂移: {support.relative_path}")
        for table in bundle.tables:
            for rel_path, expected_digest in table.files.items():
                file_path = resolved_dir / rel_path
                digest = _stream_file_sha256(file_path)
                if digest != expected_digest:
                    raise RuntimeIntegrityError(f"Result Parquet 摘要漂移: {rel_path}")
                parquet = pq.ParquetFile(file_path)
                schema = parquet.metadata.schema.to_arrow_schema()
                if (
                    typed_canonical_hash({"schema": str(schema)}) != table.schema_hashes[rel_path]
                    or parquet.metadata.num_rows != table.row_counts[rel_path]
                ):
                    raise RuntimeIntegrityError(f"Result Parquet schema 或行数漂移: {rel_path}")
        return bundle
    except RuntimeIntegrityError:
        raise
    except Exception as exc:
        raise RuntimeIntegrityError(f"已发布 Result 校验失败: {exc}") from exc


def _freeze_published_result_model_source(parameters):
    from research_pipeline.research.semantics import ResearchSemantics
    from .walk_forward_model_execution import _read_artifact_metadata

    raw_dir = parameters.get("source_result_directory")
    _require(isinstance(raw_dir, str) and bool(raw_dir.strip()), "缺少 source_result_directory")
    try:
        result_dir = Path(raw_dir).resolve(strict=True)
    except OSError as exc:
        raise RuntimeIntegrityError(f"已发布 Result 目录不可访问: {exc}") from exc
    raw_store = parameters.get("source_result_store")
    if raw_store:
        try:
            store_root = Path(raw_store).resolve(strict=True)
        except OSError as exc:
            raise RuntimeIntegrityError(f"已发布 ResultStore 目录不可访问: {exc}") from exc
    else:
        _require(len(result_dir.parents) >= 3, "已发布 Result 目录层级无效，无法推断 ResultStore 根目录")
        store_root = result_dir.parents[2]
    bundle = _verify_published_result_bundle(result_dir, store_root)
    if "model-provenance.json" not in bundle.control_files:
        raise RuntimeIntegrityError(LEGACY_GUIDANCE)
    try:
        provenance = json.loads((result_dir / "model-provenance.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise RuntimeIntegrityError(f"模型 Result provenance 无法读取: {exc}") from exc
    _require(isinstance(provenance, dict) and provenance.get("schema") == "research.result-model-provenance.v1", "模型 Result provenance schema 不受支持")
    fit_tables = tuple(t for t in bundle.tables if t.artifact_type == "research.model-fits.v2")
    split_tables = tuple(t for t in bundle.tables if t.artifact_type == "research.model-split-manifest.v2")
    inventory_tables = tuple(t for t in fit_tables if t.schema_id == QLIB_MODEL_INVENTORY_SCHEMA_ID)
    audit_tables = tuple(t for t in fit_tables if t.path_prefix == "fit_audit")
    sample_tables = tuple(t for t in split_tables if t.path_prefix == "samples")
    if len(inventory_tables) != 1 or len(audit_tables) != 1 or len(sample_tables) != 1:
        raise RuntimeIntegrityError(LEGACY_GUIDANCE)
    fit_keys = {t.artifact_key for t in fit_tables}
    split_keys = {t.artifact_key for t in split_tables}
    _require(len(fit_keys) == 1 and len(split_keys) == 1, "模型 Result 中的 fit/split 工件不唯一")
    fit_key = next(iter(fit_keys))
    split_key = next(iter(split_keys))
    has_fit_meta = any(s.artifact_key == fit_key and s.source_path == "artifact-metadata.json" for s in bundle.support_files)
    has_split_meta = any(s.artifact_key == split_key and s.source_path == "artifact-metadata.json" for s in bundle.support_files)
    if not (has_fit_meta and has_split_meta):
        raise RuntimeIntegrityError(LEGACY_GUIDANCE)

    semantics = provenance.get("research_semantics")
    identity = provenance.get("research_identity")
    _require(isinstance(semantics, dict) and isinstance(identity, dict), "模型 Result provenance 缺少语义或身份")
    semantic_payload = {k: v for k, v in semantics.items() if k != "effective_claim_level"}
    parsed = ResearchSemantics.from_dict(semantic_payload)
    _require(provenance.get("source_run_id") == bundle.run_id, "模型 Result 来源 run_id 不一致")
    dag_nodes = provenance.get("dag_nodes")
    _require(isinstance(dag_nodes, list), "模型 Result provenance 缺少 DAG 节点声明")
    split_node_id = sample_tables[0].source_node_id
    fit_node_id = inventory_tables[0].source_node_id
    nodes_by_id = {n.get("node_id"): n for n in dag_nodes if isinstance(n, dict)}
    _require(nodes_by_id.get(split_node_id, {}).get("implementation_id") == "research.model.split-manifest.v2", "来源split不是正式模型生产节点")
    _require(nodes_by_id.get(fit_node_id, {}).get("implementation_id") in {"research.model.fit.v2", "research.model.import.v1"}, "来源fit不是正式模型生产节点")
    outputs = {o.get("artifact_type") for n in dag_nodes if isinstance(n, dict) for o in n.get("output_types", ()) if isinstance(o, dict)}
    _require("research.feature-set.v1" in outputs and bool(outputs & {"research.label.v1", "research.event-window.v1"}), "来源缺乏Feature及Label生产证明")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        split_root = tmp / "split"
        fit_root = tmp / "fit"
        split_root.mkdir()
        fit_root.mkdir()
        split_files: dict[str, str] = {}
        fit_files: dict[str, str] = {}
        for table in split_tables:
            prefix = f"tables/{table.table_id}/"
            for rel_path, expected_digest in table.files.items():
                artifact_rel = f"{table.path_prefix}/{rel_path.removeprefix(prefix)}"
                _require(artifact_rel not in split_files, f"模型 Result split 表文件重复: {artifact_rel}")
                dest = split_root / artifact_rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(result_dir / rel_path, dest)
                split_files[artifact_rel] = expected_digest
        for table in fit_tables:
            prefix = f"tables/{table.table_id}/"
            for rel_path, expected_digest in table.files.items():
                artifact_rel = f"{table.path_prefix}/{rel_path.removeprefix(prefix)}"
                _require(artifact_rel not in fit_files, f"模型 Result fit 表文件重复: {artifact_rel}")
                dest = fit_root / artifact_rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(result_dir / rel_path, dest)
                fit_files[artifact_rel] = expected_digest
        for support in bundle.support_files:
            if support.artifact_key == split_key:
                _require(support.source_path not in split_files, f"模型 Result split 支持文件重复: {support.source_path}")
                dest = split_root / support.source_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(result_dir / support.relative_path, dest)
                split_files[support.source_path] = support.content_hash
            elif support.artifact_key == fit_key:
                _require(support.source_path not in fit_files, f"模型 Result fit 支持文件重复: {support.source_path}")
                dest = fit_root / support.source_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(result_dir / support.relative_path, dest)
                fit_files[support.source_path] = support.content_hash
        fit_metadata = _read_artifact_metadata(fit_root)
        split_metadata = _read_artifact_metadata(split_root)
        _require(split_metadata.get("semantics_hash") == parsed.semantics_hash, "来源split未绑定研究语义")
        record = {
            "run_id": bundle.run_id,
            "status": bundle.run.status,
            "root_seed": provenance.get("root_seed"),
            "fixed_clock": provenance.get("fixed_clock"),
            "audit_manifest_digest": provenance.get("audit_manifest_digest"),
            "dag": {"nodes": dag_nodes},
        }
        models = _validate_models(
            fit_root, split_root, SimpleNamespace(files=set(fit_files)),
            fit_metadata, split_metadata, record,
        )
    return {
        "schema": SCHEMA,
        "source_kind": "published_result",
        "source_result_directory": str(result_dir),
        "source_result_store": str(store_root),
        "source_result_id": bundle.result_id,
        "source_run_id": bundle.run_id,
        "source_fit_node_id": fit_node_id,
        "source_split_node_id": split_node_id,
        "source_fit_artifact": fit_key,
        "source_split_artifact": split_key,
        "source_record": record,
        "research_semantics": semantics,
        "research_identity": identity,
        "fit_execution": {
            "diagnostics": [],
            "training_node_succeeded": True,
            "source_result_id": bundle.result_id,
        },
        "fit_tables": [
            {"table_id": t.table_id, "path_prefix": t.path_prefix, "files": dict(sorted(t.files.items()))}
            for t in fit_tables
        ],
        "split_tables": [
            {"table_id": t.table_id, "path_prefix": t.path_prefix, "files": dict(sorted(t.files.items()))}
            for t in split_tables
        ],
        "fit_files": dict(sorted(fit_files.items())),
        "split_files": dict(sorted(split_files.items())),
        "models": models,
    }


def freeze_completed_model_source(parameters):
    if parameters.get("source_result_directory") not in (None, ""):
        return _freeze_published_result_model_source(parameters)
    from .walk_forward_model_execution import _read_artifact_metadata
    _require(all(isinstance(parameters.get(key),str) and parameters[key] for key in SOURCE_FIELDS), "来源声明字段不完整")
    root=Path(parameters["source_run_root"]).resolve(strict=True)
    events_store=EventStore(root);projection=events_store.replay();events=events_store.read_events()
    record=_json(root/"operator-dag-run.json")
    source=ExternalArtifactStore(root/"external-artifacts",create=False)
    fit=source.verify(parameters["source_fit_artifact"]);split=source.verify(parameters["source_split_artifact"])
    _require(fit.artifact_name=="models" and fit.artifact_type=="research.model-fits.v2" and split.artifact_name=="splits" and split.artifact_type=="research.model-split-manifest.v2", "来源工件类型不一致")
    execution=_source_execution(root,record,events,projection,parameters["source_fit_node_id"],parameters["source_split_node_id"],fit,split)
    fit_root=source.objects_root/fit.semantic_hash;split_root=source.objects_root/split.semantic_hash
    fit_metadata=_read_artifact_metadata(fit_root);split_metadata=_read_artifact_metadata(split_root)
    semantics, identity = _source_research_facts(root, split_metadata)
    models=_validate_models(fit_root,split_root,fit,fit_metadata,split_metadata,record)
    _require(events_store.replay().chain_head==projection.chain_head and _json(root/"operator-dag-run.json")==record, "来源在验收期间发生变化")
    return {"schema":SCHEMA,**{key:(str(root) if key=="source_run_root" else parameters[key]) for key in SOURCE_FIELDS},"source_run_id":record["run_id"],"event_chain_head":projection.chain_head,"source_record":record,"research_semantics":semantics,"research_identity":identity,"fit_execution":execution,"fit_commit":fit.to_dict(),"split_commit":split.to_dict(),"models":models}


def verify_completed_model_source(parameters):
    manifest=parameters.get("source_manifest_json")
    manifest=json.loads(manifest) if isinstance(manifest,str) else manifest
    _require(isinstance(manifest,dict) and manifest.get("schema")==SCHEMA,"冻结来源manifest不受支持")
    current=freeze_completed_model_source(manifest)
    _require(current==manifest,"冻结来源内容或身份发生变化")
