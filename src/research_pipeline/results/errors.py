"""集中结果合同的稳定错误类型。"""

from research_pipeline.platform.errors import MainlineError


class ResultContractError(MainlineError):
    """ResultSpec、ResultBundle 或可信结果消费不符合合同。"""

    error_code = "result_contract_invalid"


__all__ = ["ResultContractError"]
