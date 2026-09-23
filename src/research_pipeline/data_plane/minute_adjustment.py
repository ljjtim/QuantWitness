"""股票与 ETF 分钟线的 PIT 复权快照和流式变换。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
import json
import math
from typing import Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.catalog import (
    AdjustmentFactorSegment,
    AdjustmentFactorSnapshot,
)
from research_pipeline.domain import CorporateAction, resolve_corporate_actions
from research_pipeline.platform import typed_canonical_hash

from .errors import ProviderExecutionError, QualityGateError
from .minute_quality import audit_adjustment_gate


MINUTE_ADJUSTMENT_MANIFEST_VERSION = "minute-adjustment-manifest-v1"
_ZONE = ZoneInfo("Asia/Shanghai")
_PRICE_FIELDS = ("open", "high", "low", "close", "avg")


@dataclass(frozen=True)
class MinuteAdjustmentManifest:
    mode: str
    input_reference_id: str
    snapshot_identity_hashes: tuple[str, ...]
    output_rows: int
    output_batches: int
    contract_version: str = MINUTE_ADJUSTMENT_MANIFEST_VERSION

    @property
    def artifact_id(self) -> str:
        return typed_canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "input_reference_id": self.input_reference_id,
            "snapshot_identity_hashes": list(self.snapshot_identity_hashes),
            "output_rows": self.output_rows,
            "output_batches": self.output_batches,
            "contract_version": self.contract_version,
        }


def round_ex_reference_price(value: Decimal, *, asset_class: str) -> Decimal:
    """只对交易所除权参考价按报价位数做半入舍位。"""

    if not value.is_finite() or value <= 0:
        raise QualityGateError("未舍位除权参考价必须是正有限数")
    scale = {"cn_stock": Decimal("0.01"), "cn_etf": Decimal("0.001")}.get(
        asset_class
    )
    if scale is None:
        raise QualityGateError("除权参考价只支持股票和 ETF")
    return value.quantize(scale, rounding=ROUND_HALF_UP)


def build_adjustment_factor_snapshot(
    *,
    instrument_id: str,
    asset_class: str,
    as_of: datetime,
    applicable_start: datetime,
    applicable_end: datetime,
    initial_factor_date: date,
    initial_factor: Decimal,
    source_revision_hash: str,
    corporate_actions: Sequence[CorporateAction],
    effective_times: Mapping[date, datetime],
    previous_closes: Mapping[date, Decimal],
) -> AdjustmentFactorSnapshot:
    """从前一已完成日因子和当时可见公司行动构造单标的快照。"""

    _require_aware(as_of, "as_of")
    _require_aware(applicable_start, "applicable_start")
    _require_aware(applicable_end, "applicable_end")
    if applicable_start >= applicable_end or as_of < applicable_start:
        raise QualityGateError("复权快照时间范围无效")
    if not initial_factor.is_finite() or initial_factor <= 0:
        raise QualityGateError("初始 factor 必须是正有限数")
    if asset_class not in {"cn_stock", "cn_etf"}:
        raise QualityGateError("复权快照只支持股票和 ETF")

    actions_by_date: dict[date, tuple[CorporateAction, ...]] = {}
    for effective_date in sorted({item.effective_date for item in corporate_actions}):
        effective_at = effective_times.get(effective_date)
        if effective_at is None:
            raise QualityGateError("公司行动缺少生效时点")
        _require_aware(effective_at, "effective_at")
        if not applicable_start <= effective_at < applicable_end:
            continue
        segment_as_of = min(effective_at, as_of)
        resolved = resolve_corporate_actions(
            tuple(corporate_actions),
            as_of=segment_as_of,
            effective_date=effective_date,
        )
        if resolved:
            actions_by_date[effective_date] = resolved

    segments: list[AdjustmentFactorSegment] = []
    current_start = applicable_start
    current_factor = initial_factor
    current_action_hashes: tuple[str, ...] = ()
    current_available = applicable_start
    current_unrounded: Decimal | None = None
    current_reference: Decimal | None = None
    included: list[CorporateAction] = []

    def append_until(end: datetime) -> None:
        if current_start >= end:
            return
        segments.append(AdjustmentFactorSegment(
            current_start.isoformat(timespec="seconds"),
            end.isoformat(timespec="seconds"),
            _decimal_text(current_factor),
            current_available.isoformat(timespec="seconds"),
            _decimal_text(current_factor / initial_factor),
            current_action_hashes,
            None if current_unrounded is None else _decimal_text(current_unrounded),
            None if current_reference is None else _decimal_text(current_reference),
        ))

    for effective_date, actions in sorted(actions_by_date.items()):
        effective_at = effective_times[effective_date]
        append_until(effective_at)
        previous_close = previous_closes.get(effective_date)
        if previous_close is None or not previous_close.is_finite() or previous_close <= 0:
            raise QualityGateError("公司行动缺少正有限前收盘价")
        next_factor, unrounded, reference = _apply_actions_to_factor(
            current_factor=current_factor,
            previous_close=previous_close,
            actions=actions,
            asset_class=asset_class,
        )
        included.extend(actions)
        current_start = effective_at
        current_factor = next_factor
        current_action_hashes = tuple(sorted(item.action_hash for item in actions))
        current_available = max(item.announcement_available_time for item in actions)
        current_unrounded = unrounded
        current_reference = reference
    append_until(applicable_end)
    if not segments:
        raise QualityGateError("复权快照没有可消费 factor segment")

    included_hashes = tuple(sorted(item.action_hash for item in included))
    provisional = {
        "instrument_id": instrument_id,
        "asset_class": asset_class,
        "as_of": as_of.isoformat(timespec="seconds"),
        "initial_factor_date": initial_factor_date.isoformat(),
        "initial_factor": _decimal_text(initial_factor),
        "segments": [item.to_dict() for item in segments],
        "included_action_hashes": list(included_hashes),
        "source_revision_hash": source_revision_hash,
    }
    factor_snapshot_hash = typed_canonical_hash(provisional)
    event_available = max(
        (item.announcement_available_time for item in included),
        default=applicable_start,
    )
    return AdjustmentFactorSnapshot(
        factor_snapshot_hash,
        as_of.isoformat(timespec="seconds"),
        applicable_start.isoformat(timespec="seconds"),
        event_available.isoformat(timespec="seconds"),
        applicable_start.isoformat(timespec="seconds"),
        applicable_end.isoformat(timespec="seconds"),
        source_revision_hash,
        instrument_id,
        asset_class,
        initial_factor_date.isoformat(),
        _decimal_text(initial_factor),
        tuple(segments),
        included_hashes,
    )


def require_minute_price_mode(
    payload: Mapping[str, object], *, consumer: str, expected_mode: str
) -> str:
    """让 Feature/Label/Simulation 直接消费真实价格语义，而不是参数声明。"""

    if expected_mode not in {"raw", "pre", "post"}:
        raise QualityGateError("分钟价格消费者声明了未知模式")
    observed = payload.get("price_mode")
    if observed != expected_mode:
        raise QualityGateError(
            f"{consumer} 只接受 {expected_mode} 分钟价格，实际为 {observed!r}"
        )
    return expected_mode


def rebase_pre_window(
    rows: Sequence[Mapping[str, object]],
    *,
    snapshot: AdjustmentFactorSnapshot,
    candidate_actions: Sequence[CorporateAction],
    effective_times: Mapping[date, datetime],
    previous_closes: Mapping[date, Decimal],
    selection_at: datetime,
) -> tuple[dict[str, object], ...]:
    """按单个 Feature decision 或 Label available time 临时生成 pre 窗口。"""

    _require_aware(selection_at, "selection_at")
    if selection_at > datetime.fromisoformat(snapshot.as_of):
        raise QualityGateError("pre 窗口选择时点晚于快照研究时钟")
    if not rows:
        return ()
    factors = _decision_factor_transitions(
        snapshot=snapshot,
        candidate_actions=candidate_actions,
        effective_times=effective_times,
        previous_closes=previous_closes,
        selection_at=selection_at,
    )
    anchor_factor = _factor_at_transitions(factors, selection_at)
    output: list[dict[str, object]] = []
    for row in rows:
        observed = row.get("bar_end")
        if not isinstance(observed, datetime) or observed.tzinfo is None:
            raise QualityGateError("pre 窗口 bar_end 必须是带时区时间")
        if observed > selection_at:
            raise QualityGateError("pre 窗口不能消费选择时点之后的 bar")
        if row.get("adjustment_mode") != "post":
            raise QualityGateError("pre 窗口只能从带 factor lineage 的 post bar 重定基")
        if row.get("factor_snapshot_hash") != snapshot.snapshot_identity_hash:
            raise QualityGateError("pre 窗口 bar 与 PIT 快照身份不一致")
        post_factor = _positive_row_decimal(row.get("factor"), "factor")
        target_factor = _factor_at_transitions(factors, observed)
        ratio = target_factor / anchor_factor
        transformed = dict(row)
        for field in _PRICE_FIELDS:
            value = row.get(field)
            transformed[field] = (
                None
                if value is None
                else float(Decimal(str(value)) / post_factor * ratio)
            )
        volume = row.get("volume")
        transformed["volume"] = (
            None
            if volume is None
            else float(Decimal(str(volume)) * post_factor / ratio)
        )
        transformed["factor"] = float(target_factor)
        transformed["adjustment_ratio"] = float(ratio)
        transformed["adjustment_mode"] = "pre"
        transformed["adjustment_anchor_factor"] = float(anchor_factor)
        output.append(transformed)
    return tuple(output)


def resolve_pre_anchor_factor(
    *,
    snapshot: AdjustmentFactorSnapshot,
    candidate_actions: Sequence[CorporateAction],
    effective_times: Mapping[date, datetime],
    previous_closes: Mapping[date, Decimal],
    selection_at: datetime,
) -> Decimal:
    """按单个 Feature decision 或 Label available time 复算 pre 锚点。"""

    _require_aware(selection_at, "selection_at")
    if selection_at > datetime.fromisoformat(snapshot.as_of):
        raise QualityGateError("pre 锚点选择时点晚于快照研究时钟")
    return _factor_at_transitions(
        _decision_factor_transitions(
            snapshot=snapshot,
            candidate_actions=candidate_actions,
            effective_times=effective_times,
            previous_closes=previous_closes,
            selection_at=selection_at,
        ),
        selection_at,
    )


_RESULT_SNAPSHOT_COLUMNS = {
    "contract_version", "snapshot_json", "included_actions_json",
    "candidate_actions_json", "effective_times_json", "previous_closes_json",
    "input_references_json", "corporate_action_snapshot_hash",
    "adjustment_audit_json",
}


def verify_adjustment_snapshot_result_table(table: object) -> None:
    """只用 Result 内的候选、可见时间和前收盘价复算完整快照。"""

    if (
        getattr(table, "num_rows", None) != 1
        or set(getattr(table, "column_names", ())) != _RESULT_SNAPSHOT_COLUMNS
        or any(table[name].null_count for name in _RESULT_SNAPSHOT_COLUMNS)
    ):
        raise QualityGateError("PIT 复权快照完整载荷表 schema 无效")
    try:
        row = table.to_pylist()[0]
        if row["contract_version"] != "runtime-minute-adjustment-snapshot-v1":
            raise ValueError("快照 Runtime 版本无效")
        raw_snapshot = _json_mapping(row["snapshot_json"], "snapshot")
        raw_included = _json_mapping_list(row["included_actions_json"], "included")
        raw_candidates = _json_mapping_list(row["candidate_actions_json"], "candidates")
        effective_times = {
            date.fromisoformat(str(key)): _json_aware(value)
            for key, value in _json_mapping(
                row["effective_times_json"], "effective_times"
            ).items()
        }
        previous_closes = {
            date.fromisoformat(str(key)): Decimal(str(value))
            for key, value in _json_mapping(
                row["previous_closes_json"], "previous_closes"
            ).items()
        }
        input_references = _json_mapping(
            row["input_references_json"], "input_references"
        )
        stored_audit = _json_mapping(
            row["adjustment_audit_json"], "adjustment_audit"
        )
        snapshot = AdjustmentFactorSnapshot.from_mapping(raw_snapshot)
        included = tuple(CorporateAction.from_dict(item) for item in raw_included)
        candidates = tuple(CorporateAction.from_dict(item) for item in raw_candidates)
        if not input_references or snapshot.source_revision_hash != typed_canonical_hash(
            input_references
        ):
            raise ValueError("快照来源身份与实际输入引用不一致")
        if any(
            item.announcement_available_time > datetime.fromisoformat(snapshot.as_of)
            for item in candidates
        ):
            raise ValueError("候选公司行动晚于快照研究时钟")
        if snapshot.decision_anchors:
            raise ValueError("快照不得封存旧的全流 pre 锚点")
        recomputed = build_adjustment_factor_snapshot(
            instrument_id=snapshot.instrument_id,
            asset_class=snapshot.asset_class,
            as_of=datetime.fromisoformat(snapshot.as_of),
            applicable_start=datetime.fromisoformat(snapshot.applicable_start),
            applicable_end=datetime.fromisoformat(snapshot.applicable_end),
            initial_factor_date=date.fromisoformat(snapshot.initial_factor_date),
            initial_factor=Decimal(snapshot.initial_factor),
            source_revision_hash=snapshot.source_revision_hash,
            corporate_actions=candidates,
            effective_times=effective_times,
            previous_closes=previous_closes,
        )
        if recomputed.to_dict() != snapshot.to_dict():
            raise ValueError("快照不能由候选可见性和除权公式复算")
        expected_included = tuple(
            item for item in candidates
            if item.action_hash in recomputed.included_action_hashes
        )
        if tuple(item.to_dict() for item in included) != tuple(
            item.to_dict() for item in expected_included
        ):
            raise ValueError("快照纳入行动与候选 resolver 不一致")
        action_hash = typed_canonical_hash([
            item.to_dict() for item in sorted(
                included, key=lambda item: (item.action_id, item.revision)
            )
        ])
        expected_audit = {
            "status": "pass", "source_adjustment_mode": "post",
            "snapshot_identity_hash": snapshot.snapshot_identity_hash,
            "corporate_action_snapshot_hash": action_hash,
            "included_action_hashes": [
                item.action_hash for item in sorted(
                    included, key=lambda item: (item.action_id, item.revision)
                )
            ],
            "checked_relation_count": 0, "reason": None,
            "contract_version": "minute-adjustment-audit-v1",
        }
        if (
            row["corporate_action_snapshot_hash"] != action_hash
            or dict(stored_audit) != expected_audit
        ):
            raise ValueError("快照行动身份或审计事实不能由完整载荷复算")
    except (ArithmeticError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise QualityGateError("Result 的 PIT 复权快照完整载荷无效") from exc


def verify_adjustment_anchor_result_tables(
    snapshot_table: object,
    feature_table: object | None,
    label_table: object | None,
) -> None:
    """逐行复核 Result 中实际消费的 Feature/Label pre 锚点。"""

    verify_adjustment_snapshot_result_table(snapshot_table)
    if feature_table is None or label_table is None:
        raise QualityGateError("pre 复权 Result 必须同时封存 Feature 和 Label 锚点行")
    row = snapshot_table.to_pylist()[0]
    snapshot = AdjustmentFactorSnapshot.from_mapping(
        _json_mapping(row["snapshot_json"], "snapshot")
    )
    candidates = tuple(
        CorporateAction.from_dict(item)
        for item in _json_mapping_list(row["candidate_actions_json"], "candidates")
    )
    effective_times = {
        date.fromisoformat(str(key)): _json_aware(value)
        for key, value in _json_mapping(
            row["effective_times_json"], "effective_times"
        ).items()
    }
    previous_closes = {
        date.fromisoformat(str(key)): Decimal(str(value))
        for key, value in _json_mapping(
            row["previous_closes_json"], "previous_closes"
        ).items()
    }
    for kind, anchor_table in (("feature", feature_table), ("label", label_table)):
        _verify_anchor_rows(
            anchor_table, kind=kind, snapshot=snapshot, candidates=candidates,
            effective_times=effective_times, previous_closes=previous_closes,
        )


def _verify_anchor_rows(
    table: object, *, kind: str, snapshot: AdjustmentFactorSnapshot,
    candidates: tuple[CorporateAction, ...],
    effective_times: Mapping[date, datetime],
    previous_closes: Mapping[date, Decimal],
) -> None:
    required = {
        "instrument", "decision_time", "available_time", "source_snapshot_hash",
        "adjustment_snapshot_identity_hash", "adjustment_anchor_factor",
    }
    if getattr(table, "num_rows", 0) < 1 or not required <= set(
        getattr(table, "column_names", ())
    ):
        raise QualityGateError(f"pre {kind} 锚点表 schema 或行数无效")
    observed_rows = 0
    for batch in table.to_batches(max_chunksize=8_192):
        observed_rows += batch.num_rows
        for row in batch.to_pylist():
            decision = _json_aware(row["decision_time"])
            available = _json_aware(row["available_time"])
            selection_at = decision if kind == "feature" else available
            if available < decision or (
                row["instrument"] != snapshot.instrument_id
                or row["adjustment_snapshot_identity_hash"]
                != snapshot.snapshot_identity_hash
                or not isinstance(row["source_snapshot_hash"], str)
                or len(row["source_snapshot_hash"]) != 64
            ):
                raise QualityGateError(
                    f"pre {kind} 行时间或 snapshot lineage 无效"
                )
            expected = resolve_pre_anchor_factor(
                snapshot=snapshot, candidate_actions=candidates,
                effective_times=effective_times, previous_closes=previous_closes,
                selection_at=selection_at,
            )
            observed = Decimal(str(row["adjustment_anchor_factor"]))
            if abs(observed - expected) > max(
                Decimal("1e-12"), abs(expected) * Decimal("1e-12")
            ):
                raise QualityGateError(
                    f"pre {kind} 行的 anchor_factor 不能由快照复算"
                )
    if observed_rows != table.num_rows:
        raise QualityGateError(f"pre {kind} 锚点表行数与 manifest 不一致")


def _json_mapping(value: object, name: str) -> Mapping[str, object]:
    parsed = json.loads(str(value))
    if not isinstance(parsed, Mapping):
        raise ValueError(f"{name} 必须是 JSON 对象")
    return parsed


def _json_mapping_list(value: object, name: str) -> list[Mapping[str, object]]:
    parsed = json.loads(str(value))
    if not isinstance(parsed, list) or any(not isinstance(item, Mapping) for item in parsed):
        raise ValueError(f"{name} 必须是 JSON 对象列表")
    return parsed


def _json_aware(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("复权时间必须带时区")
    return parsed


class MinuteAdjustmentStream(Iterator[object]):
    """逐批应用 factor；只保留每个标的的当前 segment 指针。"""

    def __init__(
        self,
        source: Iterable[object],
        *,
        snapshots: Mapping[str, AdjustmentFactorSnapshot],
        included_actions: Mapping[str, tuple[CorporateAction, ...]],
        mode: str,
        relative_tolerance: float = 1e-12,
    ) -> None:
        if mode != "post":
            raise QualityGateError("分钟复权流只物化 post；pre 必须按消费窗口重定基")
        if not snapshots:
            raise QualityGateError("分钟复权流缺少快照")
        if relative_tolerance <= 0 or not math.isfinite(relative_tolerance):
            raise QualityGateError("分钟复权关系容差无效")
        if set(snapshots) != set(included_actions):
            raise QualityGateError("分钟复权快照与公司行动标的集合不一致")
        for instrument_id, snapshot in snapshots.items():
            if snapshot.instrument_id != instrument_id:
                raise QualityGateError("分钟复权快照 instrument 身份不一致")
            actions = tuple(included_actions[instrument_id])
            audit_adjustment_gate(
                source_adjustment_mode=mode,
                snapshot=snapshot,
                corporate_action_snapshot_hash=typed_canonical_hash(
                    [item.to_dict() for item in sorted(
                        actions, key=lambda item: (item.action_id, item.revision)
                    )]
                ),
                included_actions=actions,
                relative_tolerance=relative_tolerance,
            )
        self._source = source
        self._snapshots = dict(snapshots)
        self._mode = mode
        self._relative_tolerance = relative_tolerance
        self._iterator = self._iterate()
        self._rows = 0
        self._batches = 0
        self._manifest: MinuteAdjustmentManifest | None = None

    @property
    def manifest(self) -> MinuteAdjustmentManifest:
        if self._manifest is None:
            raise ProviderExecutionError("分钟复权流尚未完整消费")
        return self._manifest

    def __iter__(self) -> "MinuteAdjustmentStream":
        return self

    def __next__(self) -> object:
        return next(self._iterator)

    def _iterate(self) -> Iterator[object]:
        import pyarrow as pa

        previous_key: tuple[str, datetime] | None = None
        for batch in self._source:
            names = tuple(batch.schema.names)
            forbidden = {"factor", "adjustment_ratio", "adjustment_mode", "factor_snapshot_hash"}
            if forbidden & set(names):
                raise ProviderExecutionError("分钟 raw 输入包含复权输出保留字段")
            rows = []
            for row in batch.to_pylist():
                code = str(row.get("code", ""))
                observed = row.get("dt")
                if not code or not isinstance(observed, datetime):
                    raise ProviderExecutionError("分钟复权输入缺少 code/dt")
                aware = observed.replace(tzinfo=_ZONE) if observed.tzinfo is None else observed.astimezone(_ZONE)
                key = code, aware
                if previous_key is not None and key <= previous_key:
                    raise ProviderExecutionError("分钟复权输入必须按 code,dt 严格递增")
                previous_key = key
                snapshot = self._snapshots.get(code)
                if snapshot is None:
                    raise ProviderExecutionError("分钟复权输入标的缺少 PIT 快照")
                segment = snapshot.segment_at(aware)
                factor = Decimal(segment.factor)
                ratio = factor
                transformed = dict(row)
                for field in _PRICE_FIELDS:
                    value = row.get(field)
                    transformed[field] = None if value is None else float(value) * float(ratio)
                volume = row.get("volume")
                transformed["volume"] = None if volume is None else float(volume) / float(ratio)
                transformed["money"] = row.get("money")
                transformed.update({
                    "factor": float(factor),
                    "adjustment_ratio": float(ratio),
                    "adjustment_mode": self._mode,
                    "factor_snapshot_hash": snapshot.snapshot_identity_hash,
                })
                _audit_transformed_row(row, transformed, float(ratio), self._relative_tolerance)
                rows.append(transformed)
                self._rows += 1
            self._batches += 1
            schema = pa.schema([
                *batch.schema,
                ("factor", pa.float64()),
                ("adjustment_ratio", pa.float64()),
                ("adjustment_mode", pa.string()),
                ("factor_snapshot_hash", pa.string()),
            ])
            yield pa.RecordBatch.from_pylist(rows, schema=schema)
        self._manifest = MinuteAdjustmentManifest(
            self._mode,
            _input_reference_id(self._source),
            tuple(sorted(item.snapshot_identity_hash for item in self._snapshots.values())),
            self._rows,
            self._batches,
        )


def execute_minute_adjustment(
    source: Iterable[object],
    *,
    snapshots: Mapping[str, AdjustmentFactorSnapshot],
    included_actions: Mapping[str, tuple[CorporateAction, ...]],
    mode: str,
    relative_tolerance: float = 1e-12,
) -> MinuteAdjustmentStream:
    return MinuteAdjustmentStream(
        source,
        snapshots=snapshots,
        included_actions=included_actions,
        mode=mode,
        relative_tolerance=relative_tolerance,
    )


def _cash_per_share(action: CorporateAction) -> Decimal:
    if action.cash_per_share_units:
        return Decimal(action.cash_per_share_units) / Decimal(100)
    return Decimal(action.cash_per_share_microunits) / Decimal(1_000_000)


def _apply_actions_to_factor(
    *,
    current_factor: Decimal,
    previous_close: Decimal,
    actions: Sequence[CorporateAction],
    asset_class: str,
) -> tuple[Decimal, Decimal, Decimal]:
    cash_per_share = Decimal(0)
    quantity_multiplier = Decimal(1)
    for action in actions:
        if action.kind == "cash_dividend":
            cash_per_share += _cash_per_share(action)
        elif action.kind in {"stock_dividend", "split", "reverse_split"}:
            quantity_multiplier *= (
                Decimal(action.ratio_numerator) / Decimal(action.ratio_denominator)
            )
        elif action.kind == "rights":
            raise QualityGateError("股票来源缺少配股价/比例，不能构造复权快照")
        else:
            raise QualityGateError(f"公司行动类型不能用于分钟复权: {action.kind}")
    unrounded = (previous_close - cash_per_share) / quantity_multiplier
    reference = round_ex_reference_price(unrounded, asset_class=asset_class)
    next_factor = current_factor * previous_close / reference
    if not next_factor.is_finite() or next_factor <= 0:
        raise QualityGateError("公司行动生成了无效 factor")
    return next_factor, unrounded, reference


def _decision_factor_transitions(
    *,
    snapshot: AdjustmentFactorSnapshot,
    candidate_actions: Sequence[CorporateAction],
    effective_times: Mapping[date, datetime],
    previous_closes: Mapping[date, Decimal],
    selection_at: datetime,
) -> tuple[tuple[datetime, Decimal], ...]:
    transitions: list[tuple[datetime, Decimal]] = [
        (datetime.fromisoformat(snapshot.applicable_start), Decimal(snapshot.initial_factor))
    ]
    current_factor = Decimal(snapshot.initial_factor)
    for effective_date in sorted({item.effective_date for item in candidate_actions}):
        effective_at = effective_times.get(effective_date)
        previous_close = previous_closes.get(effective_date)
        if effective_at is None or previous_close is None:
            raise QualityGateError("decision-scoped 复权缺少生效时点或前收盘价")
        _require_aware(effective_at, "effective_at")
        if effective_at > selection_at:
            continue
        actions = resolve_corporate_actions(
            tuple(candidate_actions),
            as_of=selection_at,
            effective_date=effective_date,
        )
        if not actions:
            continue
        current_factor, _, _ = _apply_actions_to_factor(
            current_factor=current_factor,
            previous_close=previous_close,
            actions=actions,
            asset_class=snapshot.asset_class,
        )
        transitions.append((effective_at, current_factor))
    return tuple(transitions)


def _factor_at_transitions(
    transitions: Sequence[tuple[datetime, Decimal]], observed_at: datetime
) -> Decimal:
    result: Decimal | None = None
    for starts_at, factor in transitions:
        if starts_at > observed_at:
            break
        result = factor
    if result is None:
        raise QualityGateError("decision-scoped factor 未覆盖消费时点")
    return result


def _positive_row_decimal(value: object, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise QualityGateError(f"pre 窗口 {name} 不是数值") from exc
    if not result.is_finite() or result <= 0:
        raise QualityGateError(f"pre 窗口 {name} 必须为正有限数")
    return result


def _audit_transformed_row(
    raw: Mapping[str, object],
    adjusted: Mapping[str, object],
    ratio: float,
    tolerance: float,
) -> None:
    for field in _PRICE_FIELDS:
        source = raw.get(field)
        target = adjusted.get(field)
        if source is None:
            if target is not None:
                raise ProviderExecutionError("分钟复权破坏了价格 NULL 语义")
        elif not math.isclose(float(target), float(source) * ratio, rel_tol=tolerance, abs_tol=tolerance):
            raise ProviderExecutionError("分钟复权价格关系不一致")
    source_volume = raw.get("volume")
    target_volume = adjusted.get("volume")
    if source_volume is None:
        if target_volume is not None:
            raise ProviderExecutionError("分钟复权破坏了 volume NULL 语义")
    elif not math.isclose(
        float(target_volume), float(source_volume) / ratio,
        rel_tol=tolerance, abs_tol=tolerance,
    ):
        raise ProviderExecutionError("分钟复权 volume 关系不一致")
    if adjusted.get("money") != raw.get("money"):
        raise ProviderExecutionError("分钟复权不得改变 money")


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise QualityGateError(f"{name} 必须带时区")


def _input_reference_id(source: Iterable[object]) -> str:
    for field in ("source_identity", "reference_id", "content_hash"):
        value = getattr(source, field, None)
        if isinstance(value, str) and len(value) == 64:
            return value
    raise ProviderExecutionError("分钟复权输入必须提供已有来源身份")


__all__ = [
    "MINUTE_ADJUSTMENT_MANIFEST_VERSION",
    "MinuteAdjustmentManifest",
    "MinuteAdjustmentStream",
    "build_adjustment_factor_snapshot",
    "execute_minute_adjustment",
    "require_minute_price_mode",
    "rebase_pre_window",
    "resolve_pre_anchor_factor",
    "verify_adjustment_anchor_result_tables",
    "verify_adjustment_snapshot_result_table",
    "round_ex_reference_price",
]
