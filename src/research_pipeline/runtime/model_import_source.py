"""封存模型输入的共同来源验收；不加载pickle或改写来源运行。"""
from __future__ import annotations

import importlib.metadata
import json
import platform
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.research.dataframe_budget import PandasFrameBudget
from research_pipeline.research.modeling.project_model import validate_project_model_adapter
from research_pipeline.research.modeling.qlib import SEQUENCE_MODEL_CLASSES, normalize_candidates
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


def freeze_completed_model_source(parameters):
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
