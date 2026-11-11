"""已训练模型输入的声明校验，不在包编译阶段读取来源文件。"""
from collections.abc import Mapping
import json
from pathlib import Path
from .models import ResearchPackageError

SOURCE_FIELDS=('source_run_root','source_fit_artifact','source_split_artifact','source_fit_node_id','source_split_node_id')


def validate_model_import_parameters(parameters, *, require_frozen=False):
    for field in SOURCE_FIELDS:
        value=parameters.get(field)
        if not isinstance(value,str) or not value.strip():
            raise ResearchPackageError('模型导入缺少显式来源字段: '+field)
        if field.endswith('_artifact') and (len(value)!=64 or any(c not in '0123456789abcdef' for c in value)):
            raise ResearchPackageError('模型导入artifact必须为完整身份: '+field)
    if not Path(parameters['source_run_root']).is_absolute():
        raise ResearchPackageError('模型来源运行必须为绝对路径')
    raw=parameters.get('source_manifest_json')
    if raw is None:
        if require_frozen:raise ResearchPackageError('模型导入计划缺少冻结source_manifest_json')
        return
    try:manifest=json.loads(raw)
    except (TypeError,ValueError) as exc:raise ResearchPackageError('模型导入source_manifest_json无效') from exc
    if not isinstance(manifest,Mapping) or any(key not in manifest for key in ('source_run_root','split_commit','fit_commit')):
        raise ResearchPackageError('模型导入冻结来源缺少运行和工件引用')
    if Path(manifest['source_run_root']).resolve()!=Path(parameters['source_run_root']).resolve():
        raise ResearchPackageError('模型导入冻结来源运行不一致')
    for field in ('source_fit_node_id','source_split_node_id'):
        if manifest.get(field)!=parameters[field]:
            raise ResearchPackageError('模型导入冻结节点引用不一致: '+field)
    for field,parameter,port in [('fit_commit','source_fit_artifact','models'),('split_commit','source_split_artifact','splits')]:
        commit=manifest[field]
        if not isinstance(commit,Mapping) or commit.get('semantic_hash')!=parameters[parameter] or commit.get('artifact_name')!=port:
            raise ResearchPackageError('模型导入冻结工件引用不一致: '+field)


def validate_model_import_graph(recipe, *, require_frozen=False):
    for node in recipe.nodes:
        if node.operator_id=='research.model.import':
            validate_model_import_parameters(node.parameters,require_frozen=require_frozen)


def imported_research_semantics_match(recipe, semantics):
    """只承接完整冻结来源已有的研究语义，不把导入节点当新生产者。"""
    imported=[node for node in recipe.nodes if node.operator_id=='research.model.import']
    if not imported or semantics is None:
        return False
    from research_pipeline.research.semantics import ResearchSemantics
    for node in imported:
        validate_model_import_parameters(node.parameters,require_frozen=True)
        manifest=json.loads(node.parameters['source_manifest_json'])
        raw=manifest.get('research_semantics')
        if not isinstance(raw,Mapping) or not isinstance(manifest.get('research_identity'),Mapping):
            raise ResearchPackageError('模型导入缺少原冻结研究语义与身份')
        payload=dict(raw)
        payload.pop('effective_claim_level',None)
        source=ResearchSemantics.from_dict(payload)
        if source.to_dict()!=semantics.to_dict():
            raise ResearchPackageError('模型导入研究语义与来源不一致')
        outputs={output['artifact_type'] for item in manifest.get('source_record',{}).get('dag',{}).get('nodes',())
                 for output in item.get('output_types',())}
        if 'research.feature-set.v1' not in outputs or not outputs.intersection({'research.label.v1','research.event-window.v1'}):
            raise ResearchPackageError('模型导入来源缺少Feature及Label生产证明')
    return True
