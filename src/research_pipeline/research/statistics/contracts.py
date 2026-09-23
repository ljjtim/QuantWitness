"""稳健推断统一结果合同。"""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import NormalDist

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError


class StatisticsError(MainlineError):
    error_code = "research_statistics_invalid"


@dataclass(frozen=True)
class InferenceResult:
    method: str
    estimate: float
    standard_error: float
    confidence_low: float
    confidence_high: float
    p_value: float
    sample_size: int
    effective_sample_size: float
    status: str
    parameters: tuple[tuple[str, object], ...]
    assumptions: tuple[str, ...]
    limitations: tuple[str, ...]
    input_hash: str
    result_hash: str

    @classmethod
    def build(
        cls,
        *,
        method: str,
        estimate: float,
        standard_error: float,
        sample_size: int,
        effective_sample_size: float,
        parameters: dict[str, object],
        assumptions: tuple[str, ...],
        limitations: tuple[str, ...],
        input_hash: str,
        confidence: float = 0.95,
    ) -> "InferenceResult":
        if not 0 < confidence < 1 or standard_error <= 0 or not math.isfinite(standard_error):
            raise StatisticsError("置信度或标准误无效")
        z = NormalDist().inv_cdf(0.5 + confidence / 2.0)
        statistic = estimate / standard_error
        p_value = 2.0 * (1.0 - NormalDist().cdf(abs(statistic)))
        payload = {
            "method": method,
            "estimate": estimate,
            "standard_error": standard_error,
            "confidence": confidence,
            "sample_size": sample_size,
            "effective_sample_size": effective_sample_size,
            "parameters": parameters,
            "assumptions": list(assumptions),
            "limitations": list(limitations),
            "input_hash": input_hash,
        }
        return cls(
            method=method,
            estimate=float(estimate),
            standard_error=float(standard_error),
            confidence_low=float(estimate - z * standard_error),
            confidence_high=float(estimate + z * standard_error),
            p_value=float(max(0.0, min(1.0, p_value))),
            sample_size=int(sample_size),
            effective_sample_size=float(effective_sample_size),
            status="pass",
            parameters=tuple(sorted(parameters.items())),
            assumptions=assumptions,
            limitations=limitations,
            input_hash=input_hash,
            result_hash=typed_canonical_hash(payload),
        )


__all__ = ["InferenceResult", "StatisticsError"]
