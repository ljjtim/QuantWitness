"""历史策略证据只读夹具：独立还原声明、规则、日历与金融事件。"""
from datetime import date, datetime
import json
from pathlib import Path
from types import SimpleNamespace

from research_pipeline.domain import MinuteRuleSnapshotBundle, SessionPolicyBundle
from research_pipeline.domain.strategy_declaration import parse_strategy_declaration


def _restore(value):
    if isinstance(value, list):
        return [_restore(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"$datetime"}:
            return datetime.fromisoformat(value["$datetime"])
        if set(value) == {"$date"}:
            return date.fromisoformat(value["$date"])
        return {key: _restore(item) for key, item in value.items()}
    return value


def evidence(case=None, *, variant=None, partial=False, days=1, no_orders=False, amount=False):
    root = Path(__file__).parent / "fixtures" / "legacy_strategy"
    index = json.loads((root / "index.json").read_text(encoding="utf-8"))
    if case is None:
        case = "test_strategy_evidence_rebuilds_accounts_and_closes_state"
        options = {key: value for key, value in dict(partial=partial, days=days, no_orders=no_orders, amount=amount).items()
                   if value != dict(partial=False, days=1, no_orders=False, amount=False)[key]}
        variant = {"options": options}
    key = case + "__" + json.dumps(variant or {}, sort_keys=True, separators=(",", ":"))
    value = _restore(json.loads((root / index[key]).read_text(encoding="utf-8")))
    account_events = value.pop("$account_events")
    value["declaration"] = parse_strategy_declaration(value["declaration"])
    value["rule_bundle"] = MinuteRuleSnapshotBundle.from_dict(value["rule_bundle"])
    value["session_policy_bundle"] = SessionPolicyBundle.from_dict(value["session_policy_bundle"])
    run = SimpleNamespace(calendar=value["session_policy_bundle"], declaration=value["declaration"],
        machine=SimpleNamespace(corporate_action_context={"events": account_events}))
    return value, run
