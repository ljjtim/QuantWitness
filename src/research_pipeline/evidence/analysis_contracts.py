"""正式 Result 的通用时间序列分析合同。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
import re
from typing import Mapping

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.claim_levels import CLAIM_LEVELS
from research_pipeline.results import ResultReference

from .errors import EvidenceContractError


_STABLE_ID = re.compile(r"^[a-z0-9][a-z0-9_.:-]*$")


def require_sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise EvidenceContractError(f"{field} 必须是 sha256 小写十六进制")
    return value


def require_id(value: object, field: str) -> str:
    if not isinstance(value, str) or not _STABLE_ID.fullmatch(value):
        raise EvidenceContractError(f"{field} 必须是稳定 ID")
    return value


def strict_fields(payload: Mapping[str, object], expected: set[str], label: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise EvidenceContractError(f"{label} 字段不匹配: missing={sorted(expected-actual)}, unknown={sorted(actual-expected)}")



ANALYSIS_REQUEST_VERSION = "research-result-analysis-request-v2"
ANALYSIS_RESULT_VERSION = "research-result-analysis-v2"
ANALYSIS_COMPARISON_VERSION = "research-result-analysis-comparison-v2"

VALUE_SEMANTICS = frozenset({
    "observation", "level", "simple_return", "log_return",
})
INTERVAL_SEMANTICS = frozenset({"point", "non_overlapping_period"})
FREQUENCIES = frozenset({
    "daily", "weekly", "monthly", "quarterly", "annual", "irregular",
})
COST_BASES = frozenset({"gross", "net", "unspecified", "not_applicable"})
MISSING_POLICIES = frozenset({"reject", "drop"})
INCOMPLETE_YEAR_POLICIES = frozenset({"include", "exclude", "reject"})
AGGREGATIONS = frozenset({
    "count",
    "mean",
    "sign_counts",
    "best_period",
    "worst_period",
    "first_value",
    "last_value",
    "absolute_change",
    "calendar_year",
    "compounded_return",
    "geometric_annualized_return",
    "max_drawdown",
})
COMPARISON_MEASURES = frozenset({
    "overall.mean",
    "overall.absolute_change",
    "overall.compounded_return",
    "overall.geometric_annualized_return",
    "overall.max_drawdown",
})
COMPARISON_DIRECTIONS = frozenset({"higher_is_better", "lower_is_better"})
COMPARISON_REASON_CODES = frozenset({
    "analysis_spec_mismatch",
    "actual_window_mismatch",
    "date_type_mismatch",
    "value_type_mismatch",
    "claim_level_mismatch",
    "claim_ceiling_mismatch",
    "measure_unavailable",
})

_COMMON_AGGREGATIONS = frozenset({
    "count", "mean", "sign_counts", "best_period", "worst_period",
    "calendar_year",
})
_LEVEL_AGGREGATIONS = frozenset({
    "first_value", "last_value", "absolute_change",
})
_RETURN_AGGREGATIONS = frozenset({
    "compounded_return", "geometric_annualized_return", "max_drawdown",
})


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EvidenceContractError(f"{field} 必须是对象")
    return value


def _non_empty_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceContractError(f"{field} 必须是非空字符串")
    return value


def _optional_positive_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise EvidenceContractError(f"{field} 必须是正整数或 null")
    return value


def _date_text(value: object, field: str) -> str:
    text = _non_empty_text(value, field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise EvidenceContractError(f"{field} 必须是 YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        raise EvidenceContractError(f"{field} 必须使用规范 YYYY-MM-DD")
    return text


def _temporal_text(value: object, field: str) -> date | datetime:
    text = _non_empty_text(value, field)
    try:
        parsed_date = date.fromisoformat(text)
    except ValueError:
        parsed_date = None
    if parsed_date is not None and parsed_date.isoformat() == text:
        return parsed_date
    try:
        parsed_time = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EvidenceContractError(f"{field} 必须是规范 ISO 日期或时间") from exc
    if parsed_time.isoformat() != text:
        raise EvidenceContractError(f"{field} 必须是规范 ISO 日期或时间")
    return parsed_time


def _optional_finite(value: object, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceContractError(f"{field} 必须是有限数值或 null")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise EvidenceContractError(f"{field} 必须是有限数值或 null")
    return normalized


@dataclass(frozen=True)
class AnalysisSelection:
    table_id: str
    date_column: str
    value_column: str

    def __post_init__(self) -> None:
        require_id(self.table_id, "analysis table_id")
        _non_empty_text(self.date_column, "analysis date_column")
        _non_empty_text(self.value_column, "analysis value_column")
        if self.date_column == self.value_column:
            raise EvidenceContractError("日期列和值列不能相同")

    def to_dict(self) -> dict[str, str]:
        return {
            "table_id": self.table_id,
            "date_column": self.date_column,
            "value_column": self.value_column,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisSelection":
        strict_fields(
            payload,
            {"table_id", "date_column", "value_column"},
            "AnalysisSelection",
        )
        return cls(
            str(payload["table_id"]),
            str(payload["date_column"]),
            str(payload["value_column"]),
        )


@dataclass(frozen=True)
class AnalysisSemantics:
    value_semantics: str
    interval_semantics: str
    frequency: str
    periods_per_year: int | None
    unit: str
    cost_basis: str

    def __post_init__(self) -> None:
        if self.value_semantics not in VALUE_SEMANTICS:
            raise EvidenceContractError("value_semantics 不受支持")
        if self.interval_semantics not in INTERVAL_SEMANTICS:
            raise EvidenceContractError("interval_semantics 不受支持")
        if self.frequency not in FREQUENCIES:
            raise EvidenceContractError("frequency 不受支持")
        _optional_positive_int(self.periods_per_year, "periods_per_year")
        _non_empty_text(self.unit, "analysis unit")
        if self.cost_basis not in COST_BASES:
            raise EvidenceContractError("cost_basis 不受支持")
        is_return = self.value_semantics in {"simple_return", "log_return"}
        if is_return != (self.interval_semantics == "non_overlapping_period"):
            raise EvidenceContractError(
                "期间收益必须且只有期间收益可以声明 non_overlapping_period"
            )
        if self.frequency == "irregular" and self.periods_per_year is not None:
            raise EvidenceContractError("irregular frequency 不能声明 periods_per_year")

    def to_dict(self) -> dict[str, object]:
        return {
            "value_semantics": self.value_semantics,
            "interval_semantics": self.interval_semantics,
            "frequency": self.frequency,
            "periods_per_year": self.periods_per_year,
            "unit": self.unit,
            "cost_basis": self.cost_basis,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisSemantics":
        strict_fields(
            payload,
            {
                "value_semantics", "interval_semantics", "frequency",
                "periods_per_year", "unit", "cost_basis",
            },
            "AnalysisSemantics",
        )
        periods = _optional_positive_int(
            payload["periods_per_year"], "periods_per_year"
        )
        return cls(
            str(payload["value_semantics"]),
            str(payload["interval_semantics"]),
            str(payload["frequency"]),
            periods,
            str(payload["unit"]),
            str(payload["cost_basis"]),
        )


@dataclass(frozen=True)
class AnalysisWindow:
    start: str
    end: str

    def __post_init__(self) -> None:
        start = _date_text(self.start, "analysis window start")
        end = _date_text(self.end, "analysis window end")
        if start > end:
            raise EvidenceContractError("analysis window start 不能晚于 end")

    def to_dict(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisWindow":
        strict_fields(payload, {"start", "end"}, "AnalysisWindow")
        return cls(str(payload["start"]), str(payload["end"]))


@dataclass(frozen=True)
class AnalysisPolicies:
    missing: str
    incomplete_year: str
    expected_observations_per_year: int | None

    def __post_init__(self) -> None:
        if self.missing not in MISSING_POLICIES:
            raise EvidenceContractError("missing policy 不受支持")
        if self.incomplete_year not in INCOMPLETE_YEAR_POLICIES:
            raise EvidenceContractError("incomplete_year policy 不受支持")
        _optional_positive_int(
            self.expected_observations_per_year,
            "expected_observations_per_year",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "missing": self.missing,
            "incomplete_year": self.incomplete_year,
            "expected_observations_per_year": self.expected_observations_per_year,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisPolicies":
        strict_fields(
            payload,
            {"missing", "incomplete_year", "expected_observations_per_year"},
            "AnalysisPolicies",
        )
        expected = _optional_positive_int(
            payload["expected_observations_per_year"],
            "expected_observations_per_year",
        )
        return cls(
            str(payload["missing"]),
            str(payload["incomplete_year"]),
            expected,
        )


@dataclass(frozen=True)
class AnalysisRequest:
    result_id: str
    selection: AnalysisSelection
    semantics: AnalysisSemantics
    window: AnalysisWindow
    policies: AnalysisPolicies
    aggregations: tuple[str, ...]
    contract_version: str = ANALYSIS_REQUEST_VERSION

    def __post_init__(self) -> None:
        require_sha256(self.result_id, "analysis result_id")
        if self.contract_version != ANALYSIS_REQUEST_VERSION:
            raise EvidenceContractError("AnalysisRequest 版本不受支持")
        normalized = tuple(sorted(set(self.aggregations)))
        if not normalized or normalized != self.aggregations:
            raise EvidenceContractError("aggregations 必须非空、唯一并规范排序")
        unknown = set(normalized) - AGGREGATIONS
        if unknown:
            raise EvidenceContractError(f"aggregations 不受支持: {sorted(unknown)}")
        allowed = set(_COMMON_AGGREGATIONS)
        if self.semantics.value_semantics == "level":
            allowed.update(_LEVEL_AGGREGATIONS)
        elif self.semantics.value_semantics in {"simple_return", "log_return"}:
            allowed.update(_RETURN_AGGREGATIONS)
        invalid = set(normalized) - allowed
        if invalid:
            raise EvidenceContractError(
                f"当前 value_semantics 不允许聚合: {sorted(invalid)}"
            )
        if self.policies.missing == "drop" and (
            self.semantics.value_semantics != "observation"
        ):
            raise EvidenceContractError("只有 observation 允许 missing=drop")
        if "geometric_annualized_return" in normalized and (
            self.semantics.periods_per_year is None
            or self.semantics.frequency == "irregular"
        ):
            raise EvidenceContractError("几何年化必须声明规则频率和 periods_per_year")

    def spec_payload(self) -> dict[str, object]:
        return {
            "semantics": self.semantics.to_dict(),
            "window": self.window.to_dict(),
            "policies": self.policies.to_dict(),
            "aggregations": list(self.aggregations),
            "contract_version": self.contract_version,
        }

    def payload(self) -> dict[str, object]:
        return {
            "result_id": self.result_id,
            "selection": self.selection.to_dict(),
            **self.spec_payload(),
        }

    @property
    def request_hash(self) -> str:
        return typed_canonical_hash(self.payload())

    @property
    def analysis_spec_hash(self) -> str:
        return typed_canonical_hash(self.spec_payload())

    def to_dict(self) -> dict[str, object]:
        return {
            **self.payload(),
            "request_hash": self.request_hash,
            "analysis_spec_hash": self.analysis_spec_hash,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisRequest":
        base_fields = {
            "result_id", "selection", "semantics",
            "window", "policies", "aggregations", "contract_version",
        }
        with_hashes = base_fields | {"request_hash", "analysis_spec_hash"}
        if set(payload) not in {frozenset(base_fields), frozenset(with_hashes)}:
            strict_fields(payload, base_fields, "AnalysisRequest")
        for field in ("selection", "semantics", "window", "policies"):
            _mapping(payload[field], f"AnalysisRequest.{field}")
        aggregations = payload["aggregations"]
        if not isinstance(aggregations, list) or any(
            not isinstance(item, str) for item in aggregations
        ):
            raise EvidenceContractError("AnalysisRequest aggregations 必须是字符串列表")
        request = cls(
            result_id=str(payload["result_id"]),
            selection=AnalysisSelection.from_dict(_mapping(payload["selection"], "selection")),
            semantics=AnalysisSemantics.from_dict(_mapping(payload["semantics"], "semantics")),
            window=AnalysisWindow.from_dict(_mapping(payload["window"], "window")),
            policies=AnalysisPolicies.from_dict(_mapping(payload["policies"], "policies")),
            aggregations=tuple(sorted(str(item) for item in aggregations)),
            contract_version=str(payload["contract_version"]),
        )
        if set(payload) == with_hashes and (
            payload["request_hash"] != request.request_hash
            or payload["analysis_spec_hash"] != request.analysis_spec_hash
        ):
            raise EvidenceContractError("AnalysisRequest hash 不一致")
        return request


@dataclass(frozen=True)
class AnalysisSource:
    result_reference: ResultReference
    package_hash: str
    plan_hash: str
    runtime_hash: str
    implementation_manifest_hash: str
    claim_level: str
    claim_ceiling: str
    limitations: tuple[str, ...]

    def __post_init__(self) -> None:
        for field in (
            "package_hash", "plan_hash", "runtime_hash",
            "implementation_manifest_hash",
        ):
            require_sha256(getattr(self, field), f"analysis source {field}")
        if self.claim_level not in CLAIM_LEVELS or self.claim_ceiling not in CLAIM_LEVELS:
            raise EvidenceContractError("analysis source claim 不受支持")
        if (
            any(not isinstance(item, str) or not item for item in self.limitations)
            or tuple(sorted(set(self.limitations))) != self.limitations
        ):
            raise EvidenceContractError("analysis source limitations 必须唯一并规范排序")

    def to_dict(self) -> dict[str, object]:
        return {
            "result_reference": self.result_reference.to_dict(),
            "package_hash": self.package_hash,
            "plan_hash": self.plan_hash,
            "runtime_hash": self.runtime_hash,
            "implementation_manifest_hash": self.implementation_manifest_hash,
            "claim_level": self.claim_level,
            "claim_ceiling": self.claim_ceiling,
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisSource":
        expected = {
            "result_reference", "package_hash", "plan_hash", "runtime_hash",
            "implementation_manifest_hash",
            "claim_level", "claim_ceiling", "limitations",
        }
        strict_fields(payload, expected, "AnalysisSource")
        reference = _mapping(payload["result_reference"], "result_reference")
        limitations = payload["limitations"]
        if not isinstance(limitations, list) or any(
            not isinstance(item, str) for item in limitations
        ):
            raise EvidenceContractError("AnalysisSource limitations 必须是字符串列表")
        return cls(
            ResultReference.from_dict(reference),
            *(str(payload[field]) for field in (
                "package_hash", "plan_hash", "runtime_hash",
                "implementation_manifest_hash",
                "claim_level", "claim_ceiling",
            )),
            tuple(str(item) for item in limitations),
        )


@dataclass(frozen=True)
class AnalysisTableBinding:
    table_id: str
    schema_id: str
    table_manifest_hash: str
    date_column: str
    value_column: str
    date_arrow_type: str
    value_arrow_type: str

    def __post_init__(self) -> None:
        require_id(self.table_id, "analysis table table_id")
        require_id(self.schema_id, "analysis table schema_id")
        require_sha256(self.table_manifest_hash, "analysis table_manifest_hash")
        for field in (
            "date_column", "value_column", "date_arrow_type", "value_arrow_type",
        ):
            _non_empty_text(getattr(self, field), f"analysis table {field}")

    def to_dict(self) -> dict[str, str]:
        return {
            "table_id": self.table_id,
            "schema_id": self.schema_id,
            "table_manifest_hash": self.table_manifest_hash,
            "date_column": self.date_column,
            "value_column": self.value_column,
            "date_arrow_type": self.date_arrow_type,
            "value_arrow_type": self.value_arrow_type,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisTableBinding":
        expected = {
            "table_id", "schema_id", "table_manifest_hash", "date_column",
            "value_column", "date_arrow_type", "value_arrow_type",
        }
        strict_fields(payload, expected, "AnalysisTableBinding")
        return cls(*(str(payload[field]) for field in (
            "table_id", "schema_id", "table_manifest_hash", "date_column",
            "value_column", "date_arrow_type", "value_arrow_type",
        )))


@dataclass(frozen=True)
class AnalysisStatistics:
    count: int | None = None
    mean: float | None = None
    positive_count: int | None = None
    negative_count: int | None = None
    zero_count: int | None = None
    best_value: float | None = None
    best_date: str | None = None
    worst_value: float | None = None
    worst_date: str | None = None
    first_value: float | None = None
    last_value: float | None = None
    absolute_change: float | None = None
    compounded_return: float | None = None
    geometric_annualized_return: float | None = None
    max_drawdown: float | None = None

    def __post_init__(self) -> None:
        for field in ("count", "positive_count", "negative_count", "zero_count"):
            value = getattr(self, field)
            if value is not None and (type(value) is not int or value < 0):
                raise EvidenceContractError(f"AnalysisStatistics.{field} 无效")
        for field in (
            "mean", "best_value", "worst_value", "first_value", "last_value",
            "absolute_change", "compounded_return",
            "geometric_annualized_return", "max_drawdown",
        ):
            _optional_finite(getattr(self, field), f"AnalysisStatistics.{field}")
        if (self.best_value is None) != (self.best_date is None):
            raise EvidenceContractError("best_value 与 best_date 必须同时存在")
        if (self.worst_value is None) != (self.worst_date is None):
            raise EvidenceContractError("worst_value 与 worst_date 必须同时存在")

    def to_dict(self) -> dict[str, object]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisStatistics":
        expected = set(cls.__dataclass_fields__)
        strict_fields(payload, expected, "AnalysisStatistics")
        integer_fields = {"count", "positive_count", "negative_count", "zero_count"}
        values: dict[str, object] = {}
        for field in expected:
            value = payload[field]
            if field in integer_fields:
                if value is not None and type(value) is not int:
                    raise EvidenceContractError(f"AnalysisStatistics.{field} 类型无效")
                values[field] = value
            elif field in {"best_date", "worst_date"}:
                if value is not None and not isinstance(value, str):
                    raise EvidenceContractError(f"AnalysisStatistics.{field} 类型无效")
                values[field] = value
            else:
                values[field] = _optional_finite(value, f"AnalysisStatistics.{field}")
        return cls(**values)


@dataclass(frozen=True)
class AnalysisYear:
    year: int
    completeness: str
    included: bool
    observation_count: int
    statistics: AnalysisStatistics | None

    def __post_init__(self) -> None:
        if type(self.year) is not int or self.year < 1:
            raise EvidenceContractError("analysis year 无效")
        if self.completeness not in {"complete", "incomplete", "not_assessed"}:
            raise EvidenceContractError("analysis year completeness 无效")
        if type(self.included) is not bool:
            raise EvidenceContractError("analysis year included 必须是布尔值")
        if type(self.observation_count) is not int or self.observation_count < 0:
            raise EvidenceContractError("analysis year observation_count 无效")
        if self.included != (self.statistics is not None):
            raise EvidenceContractError("analysis year included 与 statistics 不闭合")

    def to_dict(self) -> dict[str, object]:
        return {
            "year": self.year,
            "completeness": self.completeness,
            "included": self.included,
            "observation_count": self.observation_count,
            "statistics": None if self.statistics is None else self.statistics.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisYear":
        strict_fields(
            payload,
            {"year", "completeness", "included", "observation_count", "statistics"},
            "AnalysisYear",
        )
        statistics = payload["statistics"]
        return cls(
            year=payload["year"] if type(payload["year"]) is int else 0,
            completeness=str(payload["completeness"]),
            included=payload["included"] if type(payload["included"]) is bool else False,
            observation_count=(
                payload["observation_count"]
                if type(payload["observation_count"]) is int
                else -1
            ),
            statistics=(
                None
                if statistics is None
                else AnalysisStatistics.from_dict(_mapping(statistics, "year statistics"))
            ),
        )


def _validate_statistics_shape(
    statistics: AnalysisStatistics,
    *,
    aggregations: tuple[str, ...],
    observation_count: int,
) -> None:
    selected = set(aggregations)
    fields_by_aggregation = {
        "count": ("count",),
        "mean": ("mean",),
        "sign_counts": ("positive_count", "negative_count", "zero_count"),
        "best_period": ("best_value", "best_date"),
        "worst_period": ("worst_value", "worst_date"),
        "first_value": ("first_value",),
        "last_value": ("last_value",),
        "absolute_change": ("absolute_change",),
        "compounded_return": ("compounded_return",),
        "geometric_annualized_return": ("geometric_annualized_return",),
        "max_drawdown": ("max_drawdown",),
    }
    for aggregation, fields in fields_by_aggregation.items():
        values = tuple(getattr(statistics, field) for field in fields)
        if aggregation not in selected:
            if any(value is not None for value in values):
                raise EvidenceContractError(
                    f"AnalysisStatistics 包含未请求聚合: {aggregation}"
                )
            continue
        if aggregation == "count":
            if statistics.count != observation_count:
                raise EvidenceContractError("AnalysisStatistics count 与观察数不一致")
        elif aggregation == "sign_counts":
            if any(value is None for value in values) or sum(values) != observation_count:
                raise EvidenceContractError("AnalysisStatistics 正负零计数不闭合")
        elif observation_count == 0:
            if any(value is not None for value in values):
                raise EvidenceContractError("空年度不能包含数值聚合")
        elif any(value is None for value in values):
            raise EvidenceContractError(
                f"AnalysisStatistics 缺少已请求聚合: {aggregation}"
            )


@dataclass(frozen=True)
class AnalysisResult:
    source: AnalysisSource
    request_hash: str
    analysis_spec_hash: str
    table: AnalysisTableBinding
    requested_window: AnalysisWindow
    actual_start: str
    actual_end: str
    semantics: AnalysisSemantics
    policies: AnalysisPolicies
    aggregations: tuple[str, ...]
    input_rows: int
    selected_rows: int
    missing_rows: int
    outside_window_rows: int
    overall: AnalysisStatistics
    yearly: tuple[AnalysisYear, ...]
    analysis_hash: str
    status: str = "pass"
    contract_version: str = ANALYSIS_RESULT_VERSION

    def __post_init__(self) -> None:
        for field in ("request_hash", "analysis_spec_hash", "analysis_hash"):
            require_sha256(getattr(self, field), f"AnalysisResult {field}")
        if self.status != "pass" or self.contract_version != ANALYSIS_RESULT_VERSION:
            raise EvidenceContractError("AnalysisResult 状态或版本无效")
        for field in (
            "input_rows", "selected_rows", "missing_rows", "outside_window_rows",
        ):
            value = getattr(self, field)
            if type(value) is not int or value < 0:
                raise EvidenceContractError(f"AnalysisResult {field} 无效")
        if self.selected_rows <= 0:
            raise EvidenceContractError("AnalysisResult 必须包含至少一行有效观察")
        if self.input_rows != (
            self.selected_rows + self.missing_rows + self.outside_window_rows
        ):
            raise EvidenceContractError("AnalysisResult 行数闭包不一致")
        if tuple(sorted(set(self.aggregations))) != self.aggregations:
            raise EvidenceContractError("AnalysisResult aggregations 未规范排序")
        actual_start = _temporal_text(self.actual_start, "AnalysisResult actual_start")
        actual_end = _temporal_text(self.actual_end, "AnalysisResult actual_end")
        try:
            if actual_start > actual_end:
                raise EvidenceContractError("AnalysisResult 实际窗口倒序")
        except TypeError as exc:
            raise EvidenceContractError("AnalysisResult 实际窗口类型不一致") from exc
        requested_start = date.fromisoformat(self.requested_window.start)
        requested_end = date.fromisoformat(self.requested_window.end)
        actual_start_date = (
            actual_start.date() if isinstance(actual_start, datetime) else actual_start
        )
        actual_end_date = (
            actual_end.date() if isinstance(actual_end, datetime) else actual_end
        )
        if not (
            requested_start <= actual_start_date <= actual_end_date <= requested_end
        ):
            raise EvidenceContractError("AnalysisResult 实际窗口越出请求窗口")
        reconstructed = AnalysisRequest(
            result_id=self.source.result_reference.result_id,
            selection=AnalysisSelection(
                self.table.table_id,
                self.table.date_column,
                self.table.value_column,
            ),
            semantics=self.semantics,
            window=self.requested_window,
            policies=self.policies,
            aggregations=self.aggregations,
        )
        if (
            reconstructed.request_hash != self.request_hash
            or reconstructed.analysis_spec_hash != self.analysis_spec_hash
        ):
            raise EvidenceContractError("AnalysisResult 与 AnalysisRequest 身份不一致")
        _validate_statistics_shape(
            self.overall,
            aggregations=self.aggregations,
            observation_count=self.selected_rows,
        )
        if "calendar_year" not in self.aggregations:
            if self.yearly:
                raise EvidenceContractError("未请求 calendar_year 时 yearly 必须为空")
        else:
            expected_years = tuple(range(requested_start.year, requested_end.year + 1))
            if tuple(item.year for item in self.yearly) != expected_years:
                raise EvidenceContractError("AnalysisResult yearly 未完整覆盖请求年份")
            if sum(item.observation_count for item in self.yearly) != self.selected_rows:
                raise EvidenceContractError("AnalysisResult yearly 行数不闭合")
            expected_count = self.policies.expected_observations_per_year
            for item in self.yearly:
                expected_completeness = (
                    "not_assessed"
                    if expected_count is None
                    else (
                        "complete"
                        if item.observation_count == expected_count
                        else "incomplete"
                    )
                )
                if item.completeness != expected_completeness:
                    raise EvidenceContractError("AnalysisResult 年度完整性不一致")
                if (
                    item.completeness == "incomplete"
                    and self.policies.incomplete_year == "reject"
                ):
                    raise EvidenceContractError("AnalysisResult 包含应被拒绝的不完整年度")
                expected_included = not (
                    item.completeness == "incomplete"
                    and self.policies.incomplete_year == "exclude"
                )
                if item.included != expected_included:
                    raise EvidenceContractError("AnalysisResult 年度包含状态不一致")
                if item.statistics is not None:
                    _validate_statistics_shape(
                        item.statistics,
                        aggregations=self.aggregations,
                        observation_count=item.observation_count,
                    )
        if self.analysis_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("AnalysisResult hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "source": self.source.to_dict(),
            "request_hash": self.request_hash,
            "analysis_spec_hash": self.analysis_spec_hash,
            "table": self.table.to_dict(),
            "requested_window": self.requested_window.to_dict(),
            "actual_start": self.actual_start,
            "actual_end": self.actual_end,
            "semantics": self.semantics.to_dict(),
            "policies": self.policies.to_dict(),
            "aggregations": list(self.aggregations),
            "input_rows": self.input_rows,
            "selected_rows": self.selected_rows,
            "missing_rows": self.missing_rows,
            "outside_window_rows": self.outside_window_rows,
            "overall": self.overall.to_dict(),
            "yearly": [item.to_dict() for item in self.yearly],
            "status": self.status,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "analysis_hash": self.analysis_hash}

    @classmethod
    def build(cls, **values: object) -> "AnalysisResult":
        payload = {
            "source": values["source"].to_dict(),
            "request_hash": values["request_hash"],
            "analysis_spec_hash": values["analysis_spec_hash"],
            "table": values["table"].to_dict(),
            "requested_window": values["requested_window"].to_dict(),
            "actual_start": values["actual_start"],
            "actual_end": values["actual_end"],
            "semantics": values["semantics"].to_dict(),
            "policies": values["policies"].to_dict(),
            "aggregations": list(values["aggregations"]),
            "input_rows": values["input_rows"],
            "selected_rows": values["selected_rows"],
            "missing_rows": values["missing_rows"],
            "outside_window_rows": values["outside_window_rows"],
            "overall": values["overall"].to_dict(),
            "yearly": [item.to_dict() for item in values["yearly"]],
            "status": "pass",
            "contract_version": ANALYSIS_RESULT_VERSION,
        }
        return cls(**values, analysis_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisResult":
        expected = {
            "source", "request_hash", "analysis_spec_hash", "table",
            "requested_window", "actual_start", "actual_end", "semantics",
            "policies", "aggregations", "input_rows", "selected_rows",
            "missing_rows", "outside_window_rows", "overall", "yearly",
            "analysis_hash", "status", "contract_version",
        }
        strict_fields(payload, expected, "AnalysisResult")
        aggregations = payload["aggregations"]
        yearly = payload["yearly"]
        if not isinstance(aggregations, list) or any(
            not isinstance(item, str) for item in aggregations
        ):
            raise EvidenceContractError("AnalysisResult aggregations 必须是字符串列表")
        if not isinstance(yearly, list) or any(
            not isinstance(item, Mapping) for item in yearly
        ):
            raise EvidenceContractError("AnalysisResult yearly 必须是对象列表")
        integer_values: dict[str, int] = {}
        for field in (
            "input_rows", "selected_rows", "missing_rows", "outside_window_rows",
        ):
            value = payload[field]
            if type(value) is not int:
                raise EvidenceContractError(f"AnalysisResult {field} 类型无效")
            integer_values[field] = value
        return cls(
            source=AnalysisSource.from_dict(_mapping(payload["source"], "source")),
            request_hash=str(payload["request_hash"]),
            analysis_spec_hash=str(payload["analysis_spec_hash"]),
            table=AnalysisTableBinding.from_dict(_mapping(payload["table"], "table")),
            requested_window=AnalysisWindow.from_dict(
                _mapping(payload["requested_window"], "requested_window")
            ),
            actual_start=str(payload["actual_start"]),
            actual_end=str(payload["actual_end"]),
            semantics=AnalysisSemantics.from_dict(
                _mapping(payload["semantics"], "semantics")
            ),
            policies=AnalysisPolicies.from_dict(_mapping(payload["policies"], "policies")),
            aggregations=tuple(str(item) for item in aggregations),
            **integer_values,
            overall=AnalysisStatistics.from_dict(_mapping(payload["overall"], "overall")),
            yearly=tuple(AnalysisYear.from_dict(item) for item in yearly),
            analysis_hash=str(payload["analysis_hash"]),
            status=str(payload["status"]),
            contract_version=str(payload["contract_version"]),
        )


@dataclass(frozen=True)
class AnalysisComparison:
    measure: str
    direction: str
    comparable: bool
    reason_codes: tuple[str, ...]
    sources: tuple[Mapping[str, object], ...]
    rankings: tuple[Mapping[str, object], ...]
    comparison_hash: str
    status: str = "pass"
    contract_version: str = ANALYSIS_COMPARISON_VERSION

    def __post_init__(self) -> None:
        if self.measure not in COMPARISON_MEASURES:
            raise EvidenceContractError("analysis comparison measure 不受支持")
        if self.direction not in COMPARISON_DIRECTIONS:
            raise EvidenceContractError("analysis comparison direction 不受支持")
        if type(self.comparable) is not bool or len(self.sources) < 2:
            raise EvidenceContractError("AnalysisComparison 来源或 comparable 无效")
        if tuple(sorted(set(self.reason_codes))) != self.reason_codes:
            raise EvidenceContractError("AnalysisComparison reason_codes 未规范排序")
        if set(self.reason_codes) - COMPARISON_REASON_CODES:
            raise EvidenceContractError("AnalysisComparison reason_codes 不受支持")
        if self.comparable == bool(self.reason_codes):
            raise EvidenceContractError("AnalysisComparison comparable 与原因不一致")
        if self.comparable != bool(self.rankings):
            raise EvidenceContractError("AnalysisComparison 排名闭包不一致")
        source_identities = tuple(
            _validate_comparison_source(item) for item in self.sources
        )
        source_hashes = tuple(item[0] for item in source_identities)
        if len(source_hashes) != len(set(source_hashes)):
            raise EvidenceContractError("AnalysisComparison sources 不能重复")
        if source_hashes != tuple(sorted(source_hashes)):
            raise EvidenceContractError("AnalysisComparison sources 必须按 analysis_hash 排序")
        if self.comparable:
            best_value = _comparison_number(
                self.rankings[0].get("value"),
                "AnalysisComparison 第一名 value",
            )
            ranking_hashes = tuple(
                _validate_comparison_ranking(
                    item,
                    direction=self.direction,
                    position=position,
                    previous=(None if position == 1 else self.rankings[position - 2]),
                    best_value=best_value,
                )
                for position, item in enumerate(self.rankings, start=1)
            )
            if set(ranking_hashes) != set(source_hashes):
                raise EvidenceContractError("AnalysisComparison ranking 来源不闭合")
            reference_by_hash = dict(source_identities)
            for item in self.rankings:
                if item["result_reference"] != reference_by_hash[item["analysis_hash"]]:
                    raise EvidenceContractError(
                        "AnalysisComparison ranking 与 source ResultReference 不一致"
                    )
        if self.status != "pass" or self.contract_version != ANALYSIS_COMPARISON_VERSION:
            raise EvidenceContractError("AnalysisComparison 状态或版本无效")
        require_sha256(self.comparison_hash, "analysis comparison_hash")
        if self.comparison_hash != typed_canonical_hash(self.payload()):
            raise EvidenceContractError("AnalysisComparison hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "measure": self.measure,
            "direction": self.direction,
            "comparable": self.comparable,
            "reason_codes": list(self.reason_codes),
            "sources": [dict(item) for item in self.sources],
            "rankings": [dict(item) for item in self.rankings],
            "status": self.status,
            "contract_version": self.contract_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "comparison_hash": self.comparison_hash}

    @classmethod
    def build(cls, **values: object) -> "AnalysisComparison":
        payload = {
            "measure": values["measure"],
            "direction": values["direction"],
            "comparable": values["comparable"],
            "reason_codes": list(values["reason_codes"]),
            "sources": [dict(item) for item in values["sources"]],
            "rankings": [dict(item) for item in values["rankings"]],
            "status": "pass",
            "contract_version": ANALYSIS_COMPARISON_VERSION,
        }
        return cls(**values, comparison_hash=typed_canonical_hash(payload))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "AnalysisComparison":
        expected = {
            "measure", "direction", "comparable", "reason_codes", "sources",
            "rankings", "comparison_hash", "status", "contract_version",
        }
        strict_fields(payload, expected, "AnalysisComparison")
        reason_codes = payload["reason_codes"]
        sources = payload["sources"]
        rankings = payload["rankings"]
        if not isinstance(reason_codes, list) or any(
            not isinstance(item, str) for item in reason_codes
        ):
            raise EvidenceContractError("AnalysisComparison reason_codes 无效")
        if not isinstance(sources, list) or any(
            not isinstance(item, Mapping) for item in sources
        ):
            raise EvidenceContractError("AnalysisComparison sources 无效")
        if not isinstance(rankings, list) or any(
            not isinstance(item, Mapping) for item in rankings
        ):
            raise EvidenceContractError("AnalysisComparison rankings 无效")
        comparable = payload["comparable"]
        if type(comparable) is not bool:
            raise EvidenceContractError("AnalysisComparison comparable 类型无效")
        return cls(
            measure=str(payload["measure"]),
            direction=str(payload["direction"]),
            comparable=comparable,
            reason_codes=tuple(str(item) for item in reason_codes),
            sources=tuple(dict(item) for item in sources),
            rankings=tuple(dict(item) for item in rankings),
            comparison_hash=str(payload["comparison_hash"]),
            status=str(payload["status"]),
            contract_version=str(payload["contract_version"]),
        )


def _validate_comparison_source(
    item: Mapping[str, object],
) -> tuple[str, Mapping[str, object]]:
    expected = {
        "analysis_hash", "result_reference", "package_hash",
        "plan_hash", "runtime_hash", "implementation_manifest_hash",
        "claim_level", "claim_ceiling", "table_id", "schema_id",
        "table_manifest_hash",
    }
    strict_fields(item, expected, "AnalysisComparison source")
    for field in (
        "analysis_hash", "package_hash", "plan_hash",
        "runtime_hash", "implementation_manifest_hash",
        "table_manifest_hash",
    ):
        require_sha256(item[field], f"AnalysisComparison source {field}")
    for field in ("table_id", "schema_id"):
        require_id(item[field], f"AnalysisComparison source {field}")
    if item["claim_level"] not in CLAIM_LEVELS or item["claim_ceiling"] not in CLAIM_LEVELS:
        raise EvidenceContractError("AnalysisComparison source claim 不受支持")
    reference = item["result_reference"]
    if not isinstance(reference, Mapping):
        raise EvidenceContractError("AnalysisComparison source result_reference 无效")
    normalized_reference = ResultReference.from_dict(reference).to_dict()
    return str(item["analysis_hash"]), normalized_reference


def _validate_comparison_ranking(
    item: Mapping[str, object],
    *,
    direction: str,
    position: int,
    previous: Mapping[str, object] | None,
    best_value: float,
) -> str:
    expected = {
        "analysis_hash", "result_reference", "value", "difference_from_best", "rank",
    }
    strict_fields(item, expected, "AnalysisComparison ranking")
    analysis_hash = require_sha256(
        item["analysis_hash"], "AnalysisComparison ranking analysis_hash"
    )
    reference = item["result_reference"]
    if not isinstance(reference, Mapping):
        raise EvidenceContractError("AnalysisComparison ranking result_reference 无效")
    ResultReference.from_dict(reference)
    value = _comparison_number(
        item["value"], "AnalysisComparison ranking value"
    )
    difference = _comparison_number(
        item["difference_from_best"],
        "AnalysisComparison ranking difference_from_best",
    )
    rank = item["rank"]
    if type(rank) is not int or rank <= 0:
        raise EvidenceContractError("AnalysisComparison ranking 数值或名次无效")
    if difference != value - best_value:
        raise EvidenceContractError("AnalysisComparison difference_from_best 不闭合")
    if position == 1:
        if rank != 1 or difference != 0.0:
            raise EvidenceContractError("AnalysisComparison 第一名结构无效")
        return analysis_hash
    if previous is None:
        raise EvidenceContractError("AnalysisComparison ranking 顺序无效")
    previous_value = float(previous["value"])
    previous_rank = int(previous["rank"])
    if direction == "higher_is_better" and value > previous_value:
        raise EvidenceContractError("AnalysisComparison ranking 未按降序排列")
    if direction == "lower_is_better" and value < previous_value:
        raise EvidenceContractError("AnalysisComparison ranking 未按升序排列")
    expected_rank = previous_rank if value == previous_value else position
    if rank != expected_rank:
        raise EvidenceContractError("AnalysisComparison 并列名次不一致")
    return analysis_hash


def _comparison_number(value: object, field: str) -> float:
    normalized = _optional_finite(value, field)
    if normalized is None:
        raise EvidenceContractError(f"{field} 不能为空")
    return normalized


__all__ = [
    "AGGREGATIONS", "ANALYSIS_COMPARISON_VERSION", "ANALYSIS_REQUEST_VERSION",
    "ANALYSIS_RESULT_VERSION", "COMPARISON_DIRECTIONS", "COMPARISON_MEASURES",
    "COMPARISON_REASON_CODES",
    "AnalysisComparison", "AnalysisPolicies", "AnalysisRequest", "AnalysisResult",
    "AnalysisSelection", "AnalysisSemantics", "AnalysisSource",
    "AnalysisStatistics", "AnalysisTableBinding", "AnalysisWindow", "AnalysisYear",
]
