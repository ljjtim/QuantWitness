"""在正式准备与恢复入口绑定完整封存模型来源。"""
from research_pipeline.packages.models import ResearchPackage, _thaw_mapping
from research_pipeline.packages.model_import_admission import validate_model_import_parameters
from research_pipeline.platform import canonical_json


def freeze_model_import_sources(package):
    if not any(node.get('operator_id')=='research.model.import'
               for node in package.spec_payload.get('graph',{}).get('nodes',())):
        return package
    spec=_thaw_mapping(package.spec_payload,'spec_payload')
    nodes=spec.get('graph',{}).get('nodes',[])
    imported=[node for node in nodes if node.get('operator_id')=='research.model.import']
    if not imported:return package
    from research_pipeline.runtime.model_import import freeze_completed_model_source,verify_completed_model_source
    for node in imported:
        parameters=node['parameters'];validate_model_import_parameters(parameters)
        if parameters.get('source_manifest_json'):
            verify_completed_model_source(parameters)
        else:
            parameters['source_manifest_json']=canonical_json(freeze_completed_model_source(parameters))
        validate_model_import_parameters(parameters,require_frozen=True)
    return ResearchPackage.build(package_slug=package.package_slug,display_name=package.display_name,
        package_version=package.package_version,builder_id=package.builder_id,sources=package.sources,
        localizations=package.localizations,metric_contract=package.metric_contract,claim_contract=package.claim_contract,
        spec_payload=spec,minute_scope_inputs=package.minute_scope_inputs)


def verify_model_import_sources(recipe):
    """读取计划只核冻结声明；来源全核验在复制消费前执行。"""
    imported=[node for node in recipe.nodes if node.operator_id=='research.model.import']
    if not imported:return
    for node in imported:
        validate_model_import_parameters(node.parameters,require_frozen=True)
