"""从冻结交易日历和已准入的证券日期键生成日频因果键批。"""
from bisect import bisect_right
from collections.abc import Mapping
from datetime import date, time

from .causal_time import CausalTimeContractError


def schedule_requests(rule):
    return {rule[key] for key in ("membership_request_id", "presence_request_id") if key in rule}


def validate_schedule(plan):
    rule = plan["daily_schedule"]
    required = {"first_observation", "last_observation", "start_offset", "end_offset",
                "start_time", "end_time", "constant_axes", "key_order"}
    if not isinstance(rule, Mapping) or not required <= rule.keys() or set(rule) - required - {
        "instruments", "membership_request_id", "presence_request_id", "warmup_sessions",
    }:
        raise CausalTimeContractError("日频排程字段无效")
    if ("instruments" in rule) == ("membership_request_id" in rule):
        raise CausalTimeContractError("排程须选择静态证券或历史成分请求")
    if "instruments" in rule:
        codes = rule["instruments"]
        if not isinstance(codes, (tuple, list)) or not codes or any(not isinstance(c, str) or not c for c in codes) or len(set(codes)) != len(codes):
            raise CausalTimeContractError("静态排程证券必须非空且唯一")
    for field in ("membership_request_id", "presence_request_id"):
        if field in rule and (not isinstance(rule[field], str) or not rule[field]):
            raise CausalTimeContractError("排程请求标识无效")
    warmup = rule.get("warmup_sessions", 0)
    if type(warmup) is not int or warmup < 0 or (plan["kind"] != "feature" and (warmup or "presence_request_id" in rule)):
        raise CausalTimeContractError("仅特征历史上下文可扩展预热或匹配来源键")
    if warmup and "instruments" in rule:
        raise CausalTimeContractError("静态证券清单不适用入池预热，warmup_sessions 须为 0 或省略")
    if "presence_request_id" in rule and rule["presence_request_id"] not in {s["request_id"] for s in plan["sources"]}:
        raise CausalTimeContractError("特征行存在性须来自本节点已绑定的特征来源")
    if any(type(rule[k]) is not int for k in ("start_offset", "end_offset")):
        raise CausalTimeContractError("窗口偏移须为交易日整数")
    for k in ("start_time", "end_time"):
        try:
            clock = time.fromisoformat(rule[k])
        except (ValueError, TypeError) as exc:
            raise CausalTimeContractError("排程时刻使用本地 HH:MM:SS") from exc
        if len(rule[k]) != 8 or clock.isoformat() != rule[k] or clock.tzinfo:
            raise CausalTimeContractError("排程时刻使用本地 HH:MM:SS")
    start = (rule["start_offset"], rule["start_time"])
    end = (rule["end_offset"], rule["end_time"])
    decision = (1, "09:30:00")
    if start > end or (plan["kind"] == "feature" and end > decision) or (
            plan["kind"] == "label" and (start < decision or end <= decision)):
        raise CausalTimeContractError("日频特征窗口须在决策前结束，标签窗口须位于决策之后")
    sources = plan["sources"]
    if not sources or any(s.get("daily_time", {}).get("rule") != "next_session_open" for s in sources):
        raise CausalTimeContractError("日频排程须绑定下一交易日开盘可见的日线来源")
    if any(s["daily_time"].get("timezone") != "Asia/Shanghai" for s in sources):
        raise CausalTimeContractError("日频排程来源时区须为 Asia/Shanghai")
    days = tuple(sources[0]["daily_time"]["sessions"])
    if any(tuple(s["daily_time"]["sessions"]) != days for s in sources[1:]):
        raise CausalTimeContractError("日频排程的来源交易日历须相同")
    if rule["first_observation"] not in days or rule["last_observation"] not in days:
        raise CausalTimeContractError("排程观察范围不在冻结日历中")
    first, last = days.index(rule["first_observation"]), days.index(rule["last_observation"])
    if first > last or first + min(rule["start_offset"], rule["end_offset"]) < 0 or last + max(1, rule["end_offset"], rule["start_offset"]) >= len(days):
        raise CausalTimeContractError("排程窗口或决策日超出日历")
    keys = tuple(plan["key_columns"])
    if keys[:2] != ("entity_id", "observation_session") or not isinstance(rule["constant_axes"], Mapping) or set(rule["constant_axes"]) != set(keys[2:]):
        raise CausalTimeContractError("排程常量轴与正式行键不一致")
    from .project_causal_contract import CausalKeyProduct
    CausalKeyProduct([["instrument"], [days[first]], *(rule["constant_axes"][k] for k in keys[2:])], rule["key_order"])
    return days, first, last


def freeze_schedule(plan, dated_entities):
    """逐日冻结实际有序证券、时点与逻辑月份，同一证券清单只保存一次。"""
    days, first, last = validate_schedule(plan)
    rule = plan["daily_schedule"]
    if schedule_requests(rule) - dated_entities.keys():
        raise CausalTimeContractError("排程缺少已准入的证券日期键")
    members = dated_entities.get(rule.get("membership_request_id"), {})
    snapshots = sorted(members)
    presence = dated_entities.get(rule.get("presence_request_id"))
    static = rule.get("instruments")
    pools, items = {}, []
    for i in range(first, last + 1):
        if static is not None:
            codes = set(static)
        else:
            codes = set()
            for day in days[i:min(i + rule.get("warmup_sessions", 0), last) + 1]:
                pos = bisect_right(snapshots, day)
                if pos:
                    codes.update(members[snapshots[pos - 1]])
        if presence is not None:
            codes.intersection_update(presence.get(days[i], ()))
        if not codes:
            continue
        pool = pools.setdefault(tuple(sorted(codes)), len(pools))
        day = days[i+1]
        start, end = (days[i+rule[field]] for field in ("start_offset", "end_offset"))
        month = date.fromisoformat(start).replace(day=1)
        months = []
        while month.isoformat() <= end:
            months.append(month.isoformat()[:7])
            month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
        items.append({
            "key_axes": [{"pool": pool}, [day], *(rule["constant_axes"][key] for key in plan["key_columns"][2:])],
            "key_order": rule["key_order"], "decision_time": day + "T09:30:00+08:00",
            "window_start": start + "T" + rule["start_time"] + "+08:00",
            "window_end": end + "T" + rule["end_time"] + "+08:00",
            "source_partitions": {s["port"]: months for s in plan["sources"]},
        })
    if not items:
        raise CausalTimeContractError("日频排程在观察范围内没有来源键")
    return {**plan, "entity_pools": list(pools), "work_items": items}
