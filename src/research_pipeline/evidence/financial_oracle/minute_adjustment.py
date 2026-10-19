"""从封存公司行动独立复核分钟复权因子和逐决策锚点。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.domain import CorporateAction
from research_pipeline.platform import typed_canonical_hash

_ZONE = ZoneInfo("Asia/Shanghai")


_RESULT_SNAPSHOT_COLUMNS = {
    "contract_version", "snapshot_json", "included_actions_json",
    "candidate_actions_json", "effective_times_json", "previous_closes_json",
    "input_references_json", "corporate_action_snapshot_hash",
    "adjustment_audit_json",
}


def verify_adjustment_snapshot_result_table(table: object) -> None:
    """只用 Result 内的候选、可见时间和前收盘价复算完整快照。"""

    columns = set(getattr(table, "column_names", ()))
    if (
        getattr(table, "num_rows", None) != 1
        or columns not in (
            _RESULT_SNAPSHOT_COLUMNS,
            _RESULT_SNAPSHOT_COLUMNS | {"financial_corporate_actions_json"},
            _RESULT_SNAPSHOT_COLUMNS | {"financial_corporate_actions_json", "initial_listing_evidence_json"},
        )
        or any(table[name].null_count for name in columns)
    ):
        raise EvidenceContractError("PIT 复权快照完整载荷表 schema 无效")
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
        snapshot = _snapshot_payload(raw_snapshot)
        included = tuple(CorporateAction.from_dict(item) for item in raw_included)
        candidates = tuple(CorporateAction.from_dict(item) for item in raw_candidates)
        if "financial_corporate_actions_json" in columns:
            financial_actions = tuple(CorporateAction.from_dict(item) for item in
                _json_mapping_list(row["financial_corporate_actions_json"], "financial_actions"))
            if any(action.contract_version != 2 or action.announcement_available_time >
                   datetime.fromisoformat(snapshot["as_of"]) for action in financial_actions):
                raise ValueError("金融行动必须使用研究时钟可见的完整 v2 合同")
        source_identity = input_references
        if "initial_listing_evidence_json" in columns:
            evidence = _json_mapping(row["initial_listing_evidence_json"], "initial_listing_evidence")
            if set(evidence) != {"instrument_id", "listed_date", "available_at", "source_reference"} or any(
                not isinstance(value, str) or not value.strip() for value in evidence.values()
            ):
                raise ValueError("IPO 上市证据必须完整绑定四个非空字段")
            listed = date.fromisoformat(evidence["listed_date"])
            start = _json_aware(snapshot["applicable_start"]).astimezone(_ZONE)
            visible = _json_aware(evidence["available_at"])
            market_open = start.replace(hour=9, minute=30, second=0, microsecond=0)
            if (
                snapshot["asset_class"] != "cn_stock"
                or evidence["instrument_id"] != snapshot["instrument_id"]
                or listed != start.date()
                or snapshot["initial_factor_date"] != listed.isoformat()
                or Decimal(snapshot["initial_factor"]) != 1
                or visible > start or visible >= market_open
            ):
                raise ValueError("IPO 首日归一化原点或上市证据可见性不一致")
            if any(action.effective_date <= listed for action in (*candidates, *financial_actions)):
                raise ValueError("IPO 原点不能省略上市前或当日生效的公司行动")
            source_identity = {"input_references": input_references, "initial_listing_evidence": evidence}
        if not input_references or snapshot["source_revision_hash"] != typed_canonical_hash(
            source_identity
        ):
            raise ValueError("快照来源身份与实际输入引用不一致")
        if any(
            item.announcement_available_time > datetime.fromisoformat(snapshot["as_of"])
            for item in candidates
        ):
            raise ValueError("候选公司行动晚于快照研究时钟")
        if snapshot["decision_anchors"]:
            raise ValueError("快照不得封存旧的全流 pre 锚点")
        recomputed = _reconstruct_snapshot(
            snapshot, candidates, effective_times, previous_closes,
        )
        if recomputed != snapshot:
            raise ValueError("快照不能由候选可见性和除权公式复算")
        expected_included = tuple(
            item for item in candidates
            if item.action_hash in recomputed["included_action_hashes"]
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
            "snapshot_identity_hash": typed_canonical_hash(snapshot),
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
        raise EvidenceContractError("Result 的 PIT 复权快照完整载荷无效") from exc


def verify_adjustment_anchor_result_tables(
    snapshot_table: object,
    feature_table: object | None,
    label_table: object | None,
) -> None:
    """逐行复核 Result 中实际消费的 Feature/Label pre 锚点。"""

    verify_adjustment_snapshot_result_table(snapshot_table)
    if feature_table is None or label_table is None:
        raise EvidenceContractError("pre 复权 Result 必须同时封存 Feature 和 Label 锚点行")
    row = snapshot_table.to_pylist()[0]
    snapshot = _snapshot_payload(
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
    table: object, *, kind: str, snapshot: Mapping[str, object],
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
        raise EvidenceContractError(f"pre {kind} 锚点表 schema 或行数无效")
    observed_rows = 0
    for batch in table.to_batches(max_chunksize=8_192):
        observed_rows += batch.num_rows
        for row in batch.to_pylist():
            decision = _json_aware(row["decision_time"])
            available = _json_aware(row["available_time"])
            selection_at = decision if kind == "feature" else available
            if available < decision or (
                row["instrument"] != snapshot["instrument_id"]
                or row["adjustment_snapshot_identity_hash"]
                != typed_canonical_hash(snapshot)
                or not isinstance(row["source_snapshot_hash"], str)
                or len(row["source_snapshot_hash"]) != 64
            ):
                raise EvidenceContractError(
                    f"pre {kind} 行时间或 snapshot lineage 无效"
                )
            expected = _reconstruct_anchor_factor(
                snapshot=snapshot, candidate_actions=candidates,
                effective_times=effective_times, previous_closes=previous_closes,
                selection_at=selection_at,
            )
            observed = Decimal(str(row["adjustment_anchor_factor"]))
            if not observed.is_finite() or abs(observed - expected) > max(
                Decimal("1e-12"), abs(expected) * Decimal("1e-12")
            ):
                raise EvidenceContractError(
                    f"pre {kind} 行的 anchor_factor 不能由快照复算"
                )
    if observed_rows != table.num_rows:
        raise EvidenceContractError(f"pre {kind} 锚点表行数与 manifest 不一致")


def _snapshot_payload(value: Mapping[str, object]) -> Mapping[str, object]:
    """证据层读取封存字段；区间、因子及身份由独立重建的完整载荷核对。"""
    text_fields = {
        "factor_snapshot_hash", "as_of", "anchor_at", "event_available_at",
        "applicable_start", "applicable_end", "source_revision_hash",
        "instrument_id", "asset_class", "initial_factor_date", "initial_factor", "contract_version",
    }
    list_fields = {"factor_segments", "included_action_hashes", "decision_anchors"}
    if set(value) != text_fields | list_fields or any(
        not isinstance(value[name], str) or not value[name].strip() for name in text_fields
    ) or any(not isinstance(value[name], list) for name in list_fields):
        raise ValueError("复权快照字段或类型无效")
    date.fromisoformat(value["initial_factor_date"])
    return value


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


def _visible_actions(
    candidates: Sequence[CorporateAction], *, clock: datetime, effective_date: date,
) -> tuple[CorporateAction, ...]:
    # 修订可改变生效日，先在决策时钟内选版本，再判断所属日期。
    visible = sorted(
        (action for action in candidates if action.announcement_available_time <= clock),
        key=lambda action: (action.action_id, action.revision),
    )
    latest: dict[str, CorporateAction] = {}
    for action in visible:
        prior = latest.get(action.action_id)
        if prior is not None and prior.revision == action.revision and prior.action_hash != action.action_hash:
            raise EvidenceContractError("独立复核发现同一公司行动修订冲突")
        latest[action.action_id] = action
    return tuple(action for action in latest.values() if action.effective_date == effective_date)


def _reference_prices(
    actions: Sequence[CorporateAction], previous_close: Decimal, asset_class: str,
) -> tuple[Decimal, Decimal]:
    if not previous_close.is_finite() or previous_close <= 0:
        raise EvidenceContractError("独立复核要求正有限前收盘价")
    cash = Decimal(0)
    shares = Decimal(1)
    for action in actions:
        if action.kind == "cash_dividend":
            cash += (
                Decimal(action.cash_per_share_units) / 100
                if action.cash_per_share_units
                else Decimal(action.cash_per_share_microunits) / 1_000_000
            )
        elif action.kind in {"stock_dividend", "split", "reverse_split"}:
            shares *= Decimal(action.ratio_numerator) / Decimal(action.ratio_denominator)
        else:
            raise EvidenceContractError(f"独立复核不支持该复权行动: {action.kind}")
    unrounded = (previous_close - cash) / shares
    if not unrounded.is_finite() or unrounded <= 0:
        raise EvidenceContractError("独立复核得到无效除权参考价")
    quantum = {"cn_stock": Decimal("0.01"), "cn_etf": Decimal("0.001")}.get(asset_class)
    if quantum is None:
        raise EvidenceContractError("独立复核只支持股票和 ETF")
    reference = unrounded.quantize(quantum, rounding=ROUND_HALF_UP)
    if reference <= 0:
        raise EvidenceContractError("独立复核得到非正舍位参考价")
    return unrounded, reference


def _reconstruct_snapshot(
    snapshot: Mapping[str, object], candidates: Sequence[CorporateAction],
    effective_times: Mapping[date, datetime], previous_closes: Mapping[date, Decimal],
) -> dict[str, object]:
    start, end, clock = map(
        _json_aware, (snapshot["applicable_start"], snapshot["applicable_end"], snapshot["as_of"])
    )
    initial = Decimal(snapshot["initial_factor"])
    if start >= end or clock < start or not initial.is_finite() or initial <= 0:
        raise EvidenceContractError("独立复核的快照时间范围或初始因子无效")
    if snapshot["asset_class"] not in {"cn_stock", "cn_etf"}:
        raise EvidenceContractError("独立复核只支持股票和 ETF")
    points = [(start, initial, start, (), None, None)]
    included: list[CorporateAction] = []
    factor = initial
    for day in sorted({action.effective_date for action in candidates}):
        effective = effective_times[day]
        if not start <= effective < end:
            continue
        actions = _visible_actions(candidates, clock=min(clock, effective), effective_date=day)
        if not actions:
            continue
        unrounded, reference = _reference_prices(actions, previous_closes[day], snapshot["asset_class"])
        factor = factor * previous_closes[day] / reference
        included.extend(actions)
        points.append((
            effective, factor, max(action.announcement_available_time for action in actions),
            tuple(sorted(action.action_hash for action in actions)), unrounded, reference,
        ))
    segments = []
    for point, boundary in zip(points, [point[0] for point in points[1:]] + [end]):
        begins, value, visible, action_hashes, unrounded, reference = point
        if begins >= boundary:
            continue
        segments.append({
            "starts_at": begins.isoformat(timespec="seconds"),
            "ends_at": boundary.isoformat(timespec="seconds"),
            "factor": format(value, "f"), "available_at": visible.isoformat(timespec="seconds"),
            "adjustment_ratio": format(value / initial, "f"),
            "action_hashes": list(action_hashes),
            "unrounded_ex_reference": None if unrounded is None else format(unrounded, "f"),
            "ex_reference_price": None if reference is None else format(reference, "f"),
        })
    included_hashes = tuple(sorted(action.action_hash for action in included))
    covered_until = start
    segment_actions = set()
    for segment in segments:
        begins = _json_aware(segment["starts_at"])
        boundary = _json_aware(segment["ends_at"])
        if begins != covered_until or not start <= begins < boundary <= end or (
            _json_aware(segment["available_at"]) > begins
        ):
            raise EvidenceContractError("独立复核的因子分段必须连续覆盖且在生效时可见")
        covered_until = boundary
        segment_actions.update(segment["action_hashes"])
    if covered_until != end or tuple(sorted(segment_actions)) != included_hashes:
        raise EvidenceContractError("独立复核的因子分段覆盖或纳入行动集合不一致")
    identity = {
        "instrument_id": snapshot["instrument_id"], "asset_class": snapshot["asset_class"],
        "as_of": clock.isoformat(timespec="seconds"),
        "initial_factor_date": snapshot["initial_factor_date"],
        "initial_factor": format(initial, "f"),
        "segments": segments,
        "included_action_hashes": list(included_hashes),
        "source_revision_hash": snapshot["source_revision_hash"],
    }
    available = max((action.announcement_available_time for action in included), default=start)
    return {
        "factor_snapshot_hash": typed_canonical_hash(identity),
        "as_of": clock.isoformat(timespec="seconds"),
        "anchor_at": start.isoformat(timespec="seconds"),
        "event_available_at": available.isoformat(timespec="seconds"),
        "applicable_start": start.isoformat(timespec="seconds"),
        "applicable_end": end.isoformat(timespec="seconds"),
        "source_revision_hash": snapshot["source_revision_hash"],
        "instrument_id": snapshot["instrument_id"], "asset_class": snapshot["asset_class"],
        "initial_factor_date": snapshot["initial_factor_date"], "initial_factor": format(initial, "f"),
        "factor_segments": segments, "included_action_hashes": list(included_hashes),
        "decision_anchors": [], "contract_version": "adjustment-factor-snapshot-v2",
    }


def _reconstruct_anchor_factor(
    *, snapshot: Mapping[str, object], candidate_actions: Sequence[CorporateAction],
    effective_times: Mapping[date, datetime], previous_closes: Mapping[date, Decimal],
    selection_at: datetime,
) -> Decimal:
    start = _json_aware(snapshot["applicable_start"])
    if selection_at < start or selection_at > _json_aware(snapshot["as_of"]):
        raise EvidenceContractError("pre 锚点选择时点不在快照研究时钟内")
    end = _json_aware(snapshot["applicable_end"])
    factor = Decimal(snapshot["initial_factor"])
    for day in sorted({action.effective_date for action in candidate_actions}):
        effective = effective_times[day]
        if not start <= effective < end or effective > selection_at:
            continue
        actions = _visible_actions(candidate_actions, clock=selection_at, effective_date=day)
        if actions:
            _, reference = _reference_prices(actions, previous_closes[day], snapshot["asset_class"])
            factor = factor * previous_closes[day] / reference
    return factor
