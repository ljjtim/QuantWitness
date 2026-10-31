"""已准入 Catalog 计划的运行输入。"""
from research_pipeline.platform import canonical_json
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs, RuntimeNodeValue
from .common import _parameters


def execute_data_catalog_admission_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    parameters = _parameters(context)
    configuration_hash = context.node.configuration_hash
    if not isinstance(configuration_hash, str):
        raise ValueError("Catalog admission 节点缺少配置身份")
    return RuntimeNodeOutputs.single(
        RuntimeNodeValue.inline(
            name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
            content=canonical_json(
                {
                    "contract_version": "data-catalog-admission-artifact-v2",
                    "node_configuration_hash": configuration_hash,
                    "request_ids": list(parameters["request_ids"]),
                    "catalog_hash": parameters["catalog_hash"],
                    "pit_contract_hash": parameters["pit_contract_hash"],
                    "drift_proof_hash": parameters["drift_proof_hash"],
                    "execution_estimates_hash": parameters[
                        "execution_estimates_hash"
                    ],
                }
            ).encode("utf-8"),
        )
    )
