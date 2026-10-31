"""项目策略直接消费正式逐证券PIT快照，订单目标由策略回调产生。"""
from collections.abc import Mapping

from research_pipeline.catalog import AdjustmentFactorSnapshot
from research_pipeline.domain import CorporateAction, InstrumentKey
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.simulation.corporate_actions import corporate_action_snapshot_hash


def strategy_snapshot_context(payload, *, source_bars_hash, frequency="1m"):
    """校验每个证券的完整快照并转为现有金融上下文。"""
    if frequency == "1d" and payload.get("source_data_bundle_hash") != source_bars_hash:
        raise ValueError("日线策略snapshot与行情data_bundle来源身份不一致")
    multiple = payload.get("contract_version") == "runtime-minute-adjustment-snapshot-v2"
    if multiple:
        snapshots = payload.get("snapshots")
        if not isinstance(snapshots, Mapping) or not snapshots:
            raise ValueError("策略批量PIT快照缺少逐证券载荷")
    else:
        if payload.get("contract_version") != "runtime-minute-adjustment-snapshot-v1":
            raise ValueError("策略PIT快照版本不受支持")
        raw = payload.get("snapshot")
        if not isinstance(raw, Mapping):
            raise ValueError("策略PIT快照缺少证券身份")
        snapshots = {raw.get("instrument_id"): payload}
    contexts = {}
    financial = []
    for code, member in snapshots.items():
        if not isinstance(code, str) or not isinstance(member, Mapping):
            raise ValueError("策略PIT快照成员身份无效")
        snapshot = AdjustmentFactorSnapshot.from_mapping(member["snapshot"])
        if snapshot.instrument_id != code:
            raise ValueError("策略PIT快照与证券键不一致")
        candidates = member.get("candidate_actions")
        included = member.get("included_actions")
        actions = member.get("financial_corporate_actions")
        if any(not isinstance(rows, list) for rows in (candidates, included, actions)):
            raise ValueError("策略PIT快照缺少完整公司行动")
        instrument_hash = InstrumentKey(code, snapshot.asset_class, code.rpartition(".")[2],
            "CNY", "stock" if snapshot.asset_class == "cn_stock" else "etf").instrument_hash
        parsed = tuple(CorporateAction.from_dict(row) for row in candidates)
        if any(item.instrument_hash != instrument_hash for item in parsed):
            raise ValueError("策略PIT公司行动跨证券")
        visible = tuple(item for item in parsed if item.action_hash in snapshot.included_action_hashes)
        if (sorted(item.action_hash for item in visible) != list(snapshot.included_action_hashes)
                or [item.to_dict() for item in visible] != included
                or corporate_action_snapshot_hash(visible) != member.get("corporate_action_snapshot_hash")):
            raise ValueError("策略PIT快照与纳入公司行动不一致")
        if any(CorporateAction.from_dict(row).instrument_hash != instrument_hash for row in actions):
            raise ValueError("策略PIT权益动作跨证券")
        effective = member.get("effective_times")
        closes = member.get("previous_closes")
        if not isinstance(effective, Mapping) or not isinstance(closes, Mapping):
            raise ValueError("策略PIT快照缺少除权时间及前收盘价")
        contexts[code] = {
            "adjustment_snapshot": snapshot.to_dict(),
            "adjustment_snapshot_identity_hash": snapshot.snapshot_identity_hash,
            "adjustment_candidates": candidates,
            "adjustment_effective_times": dict(effective),
            "adjustment_previous_closes": dict(closes),
            "financial_corporate_actions": actions,
        }
        financial.extend(actions)
    source_identity = typed_canonical_hash(payload)
    context = {"manifest": {"execution_mode": "strategy", "snapshot_identity": source_identity},
               "source_bars_snapshot_hash": source_bars_hash,
               "financial_corporate_actions": financial}
    if multiple:
        context["adjustment_snapshots"] = contexts
    else:
        context.update(next(iter(contexts.values())))
    return context
