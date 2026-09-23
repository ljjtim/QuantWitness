"""新主线证据合同的稳定错误。"""

from research_pipeline.platform.errors import MainlineError


class EvidenceContractError(MainlineError):
    """证据结构、身份或结论上限不合法。"""

    error_code = "evidence_contract_invalid"


__all__ = ["EvidenceContractError"]
