"""仅从 Result 封存事实独立复核研究系列与既往 holdout 暴露。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from pathlib import PurePosixPath, PureWindowsPath
import re

from research_pipeline.platform import typed_canonical_hash

from .errors import EvidenceContractError


_STAGES = {"plan", "prepared", "opened", "terminal"}
_FREEZE_FIELDS = {
    "parent_research_purpose", "data_snapshot", "holdout_split", "mode", "candidates",
    "selection_rule", "validation", "primary_estimand", "direction", "alpha",
    "multiple_testing", "random_protocol", "failure_policy", "exposed_intervals",
    "package_plan_identity", "implementation_identity", "aliases",
}


def _require(condition, message):
    if not condition:
        raise EvidenceContractError(message)


def _mapping(value, label):
    _require(isinstance(value, Mapping), f"{label}必须是对象")
    return value


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _path(value):
    _require(_text(value), "系列路径必须是绝对目录")
    path = PureWindowsPath(value) if PureWindowsPath(value).is_absolute() else PurePosixPath(value)
    _require(path.is_absolute() and ".." not in path.parts, "系列路径必须是绝对目录")
    return path


def _declaration(value):
    value = _mapping(value, "研究系列声明")
    _require(set(value) == {"series_id", "root", "purpose", "legacy_ledger_roots"}, "研究系列声明字段不完整")
    _require(isinstance(value["series_id"], str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value["series_id"]), "研究系列 ID 必须是 slug")
    _require(_text(value["purpose"]), "研究系列目的不能为空")
    _path(value["root"])
    roots = value["legacy_ledger_roots"]
    _require(isinstance(roots, list), "旧账本根必须是列表")
    paths = [_path(root) for root in roots]
    _require(len(paths) == len(set(paths)), "旧账本根重复")
    return value


def _parent(declaration):
    return typed_canonical_hash({key: declaration[key] for key in ("series_id", "purpose")})


def _day(value):
    _require(isinstance(value, str), "holdout 日期必须是日期文本")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceContractError("holdout 日期无效") from exc
    _require(result.isoformat() == value, "holdout 日期必须使用 YYYY-MM-DD")
    return result


def _time(value):
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        result = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EvidenceContractError("holdout 时间无效") from exc
    _require(result.utcoffset() is not None, "holdout 时间缺少时区")
    return result


def _interval(value):
    start, end = _day(value.get("start")), _day(value.get("end"))
    _require(start <= end, "holdout 闭日期区间倒置")
    return start, end


def _hashed(value, stage):
    value = _mapping(value, f"holdout {stage}")
    field = stage + "_hash"
    _require(value.get("contract_version") == f"research-persistent-holdout-{stage}-v2", "holdout 阶段合同无效")
    _require(typed_canonical_hash({key: item for key, item in value.items() if key != field}) == value.get(field), "holdout 阶段内容身份不符")
    return value


def _verify_ledger(ledger):
    ledger = _mapping(ledger, "holdout 账本")
    _require(set(ledger) == _STAGES, "holdout 四阶段证据不完整")
    plan = _mapping(ledger["plan"], "holdout plan")
    _require(set(plan) == {"contract_version", "state", "holdout_identity_hash", "freeze_payload", "actor", "reason", "unlock_at", "plan_hash", "token_hash"}, "holdout plan 字段不完整")
    _require(plan["contract_version"] == "research-persistent-holdout-plan-v2" and plan["state"] == "frozen", "holdout 计划合同无效")
    freeze = _mapping(plan["freeze_payload"], "holdout freeze")
    _require(set(freeze) in (_FREEZE_FIELDS, _FREEZE_FIELDS | {"research_series"}), "holdout 冻结字段不完整")
    if "research_series" in freeze:
        _require(freeze["parent_research_purpose"] == _parent(_declaration(freeze["research_series"])), "既往账本父研究身份不符")
    _require(all(_text(freeze[key]) for key in ("parent_research_purpose", "data_snapshot", "primary_estimand")), "holdout 冻结身份或估计量为空")
    _require(all(isinstance(freeze[key], str) and re.fullmatch("[0-9a-f]{64}", freeze[key]) for key in ("package_plan_identity", "implementation_identity")), "holdout 包或实现身份无效")
    _require(_text(plan["actor"]) and _text(plan["reason"]), "holdout 授权说明为空")
    split = _mapping(freeze["holdout_split"], "holdout split")
    _require(set(split) == {"split_id", "start", "end", "sample_ids"} and _text(split["split_id"]), "holdout split 字段不完整")
    interval = _interval(split)
    ids = split["sample_ids"]
    _require(isinstance(ids, list) and ids and all(_text(item) for item in ids) and ids == sorted(set(ids)), "holdout 样本必须非空、唯一并排序")
    candidates = freeze["candidates"]
    _require(isinstance(candidates, list) and candidates and all(_text(item) for item in candidates) and candidates == sorted(set(candidates)), "holdout 候选身份无效")
    mode = freeze["mode"]
    _require(mode in {"single_candidate_confirmation", "family_wide_confirmation"}, "holdout 确认模式无效")
    correction = freeze["multiple_testing"]
    if mode == "single_candidate_confirmation":
        _require(len(candidates) == 1 and correction is None, "holdout 未冻结唯一候选")
    else:
        _require(len(candidates) >= 2 and isinstance(correction, Mapping) and _text(correction.get("method")) and correction.get("family_size") == len(candidates), "holdout 整族校正规则无效")
    _require(freeze["failure_policy"] == "opened_then_failure_is_consumed", "holdout 失败消费策略无效")
    _require(freeze["direction"] in {"greater", "less", "two_sided"} and type(freeze["alpha"]) in {int, float} and 0 < freeze["alpha"] < 1, "holdout 检验方向或 alpha 无效")
    selection = _mapping(freeze["selection_rule"], "holdout selection_rule")
    validation = _mapping(freeze["validation"], "holdout validation")
    _require(set(selection) == {"method", "uses_validation"} and _text(selection["method"]) and type(selection["uses_validation"]) is bool, "holdout 选模规则无效")
    _require(set(validation) == {"purpose", "rule"}, "holdout validation 字段无效")
    if selection["uses_validation"]:
        _require(validation["purpose"] == "selection" and isinstance(validation["rule"], Mapping) and validation["rule"], "holdout 未冻结 validation 选择规则")
    else:
        _require(validation["purpose"] == "diagnostic" and validation["rule"] is None, "holdout validation 用途无效")
    random = _mapping(freeze["random_protocol"], "holdout random_protocol")
    _require(set(random) == {"combinations", "repetitions", "seed"}, "holdout 随机协议字段无效")
    combinations = random["combinations"]
    _require(isinstance(combinations, list) and combinations == sorted(set(combinations)), "holdout 随机组合无效")
    _require(type(random["seed"]) is int and random["seed"] >= 0 and type(random["repetitions"]) is int and random["repetitions"] >= 0 and bool(combinations) == (random["repetitions"] > 0), "holdout 随机协议无效")
    aliases = _mapping(freeze["aliases"], "holdout aliases")
    _require(set(aliases) == {"run", "package", "family"} and all(_text(item) for item in aliases.values()), "holdout 别名无效")
    exposures = freeze["exposed_intervals"]
    _require(isinstance(exposures, list), "holdout 已暴露区间必须是列表")
    for exposure in exposures:
        exposure = _mapping(exposure, "holdout 暴露区间")
        _require(set(exposure) == {"start", "end", "reason"} and _text(exposure["reason"]), "holdout 暴露区间字段无效")
        previous = _interval(exposure)
        _require(max(interval[0], previous[0]) > min(interval[1], previous[1]), "holdout 与已暴露闭区间重叠")
    identity = typed_canonical_hash({"parent_research_purpose": freeze["parent_research_purpose"], "data_snapshot": freeze["data_snapshot"], "holdout_start": split["start"], "holdout_end": split["end"], "sample_ids": ids})
    plan_hash = typed_canonical_hash({key: plan[key] for key in ("freeze_payload", "actor", "reason", "unlock_at", "holdout_identity_hash")})
    token = typed_canonical_hash({"domain": "locked-holdout-token-v2", "plan_hash": plan_hash})
    _require(identity == plan["holdout_identity_hash"] and plan_hash == plan["plan_hash"] and token == plan["token_hash"], "holdout 计划内容身份不符")
    prepared, opened = (_hashed(ledger[name], name) for name in ("prepared", "opened"))
    _require(set(prepared) == {"contract_version", "state", "plan_hash", "prepared_at", "preflight", "prepared_hash"}, "holdout prepared 字段无效")
    _require(set(opened) == {"contract_version", "state", "opening_id", "plan_hash", "prepared_hash", "token_hash", "actor", "reason", "opened_at", "sample_ids_hash", "opened_hash"}, "holdout opened 字段无效")
    _require(prepared["state"] == "prepared" and prepared["plan_hash"] == plan_hash and isinstance(prepared["preflight"], Mapping) and prepared["preflight"], "holdout 预检未闭合")
    if mode == "family_wide_confirmation":
        _require(prepared["preflight"].get("candidate_ids") == candidates and prepared["preflight"].get("family_size") == len(candidates), "holdout 整族预检未闭合")
    _require(opened["state"] == "opened" and opened["plan_hash"] == plan_hash and opened["prepared_hash"] == prepared["prepared_hash"] and opened["token_hash"] == token, "holdout 打开前序链不符")
    _require(opened["actor"] == plan["actor"] and opened["reason"] == plan["reason"] and _text(opened["opening_id"]) and opened["sample_ids_hash"] == typed_canonical_hash(ids), "holdout 打开授权或样本不符")
    _require(_time(opened["opened_at"]) >= max(_time(plan["unlock_at"]), _time(prepared["prepared_at"])), "holdout 提前打开")
    terminal = ledger["terminal"]
    if terminal is not None:
        terminal = _hashed(terminal, "terminal")
        _require(set(terminal) == {"contract_version", "opened_hash", "status", "result_hash", "reason", "terminal_hash"}, "holdout terminal 字段无效")
        _require(terminal["opened_hash"] == opened["opened_hash"] and terminal["status"] in {"committed", "consumed_failed"} and _text(terminal["result_hash"]), "holdout 终态链不符")
        _require(terminal["status"] == "committed" or _text(terminal["reason"]), "holdout 消费失败必须记录原因")
    return interval


def verify_holdout_series_evidence(ledger):
    """返回系列父身份；历史无系列账本维持原合同，不访问任何活目录。"""
    ledger = _mapping(ledger, "模型 holdout 账本")
    plan = _mapping(ledger.get("plan"), "模型 holdout plan")
    freeze = _mapping(plan.get("freeze_payload"), "模型 holdout freeze")
    if "research_series" not in freeze:
        _require("series" not in ledger, "历史账本不得附加未冻结系列证据")
        return None
    _require(set(ledger) == _STAGES | {"series"}, "模型缺少研究系列证据")
    declaration = _declaration(freeze["research_series"])
    parent = _parent(declaration)
    _require(freeze.get("parent_research_purpose") == parent, "系列父研究身份不符")
    evidence = _mapping(ledger["series"], "研究系列证据")
    _require(set(evidence) == {"contract_version", "declaration", "registration", "frozen_ledgers", "opening_ledgers"} and evidence["contract_version"] == "research-holdout-series-evidence-v1", "研究系列证据合同无效")
    _require(evidence["declaration"] == declaration, "系列声明与冻结计划不一致")
    registration = _mapping(evidence["registration"], "研究系列登记")
    _require(set(registration) == {"contract_version", "declaration", "registered_at"} and registration["contract_version"] == "research-holdout-series-registration-v1" and registration["declaration"] == declaration, "系列登记与冻结声明不一致")
    _require(isinstance(registration["registered_at"], str), "系列登记时间必须是文本")
    _time(registration["registered_at"])
    current_interval = _verify_ledger({key: ledger[key] for key in _STAGES})
    root = _path(declaration["root"])
    legacy_roots = [_path(value) for value in declaration["legacy_ledger_roots"]]
    snapshots = {}
    for name in ("frozen_ledgers", "opening_ledgers"):
        entries = evidence[name]
        _require(isinstance(entries, list), "系列账本快照必须是列表")
        records = {}
        for entry in entries:
            entry = _mapping(entry, "系列账本条目")
            _require(set(entry) == {"ledger_ref", "ledger"}, "系列账本条目字段无效")
            ref, previous = entry["ledger_ref"], _mapping(entry["ledger"], "既往账本")
            path = _path(ref)
            _require(ref not in records, "系列账本引用重复")
            interval = _verify_ledger(previous)
            previous_plan = previous["plan"]
            identity = previous_plan["holdout_identity_hash"]
            _require(identity != plan["holdout_identity_hash"] and previous_plan["plan_hash"] != plan["plan_hash"], "当前账本不能伪装既往暴露")
            previous_freeze = previous_plan["freeze_payload"]
            is_legacy = any(path.is_relative_to(old_root) for old_root in legacy_roots)
            if not is_legacy:
                expected_path = root / declaration["series_id"] / "ledgers" / identity
                _require(previous_freeze.get("research_series") == declaration and path == expected_path, "既往账本系列声明或固定根目录路径不符")
            _require(max(current_interval[0], interval[0]) > min(current_interval[1], interval[1]), "本次 holdout 与既往暴露闭区间重叠")
            records[ref] = previous
        snapshots[name] = records
    frozen, opening = snapshots["frozen_ledgers"], snapshots["opening_ledgers"]
    for ref, previous in frozen.items():
        _require(ref in opening, "打开快照丢失冻结暴露")
        actual = opening[ref]
        _require(all(actual[key] == previous[key] for key in ("plan", "prepared", "opened")), "打开快照改写既往打开事实")
        _require(previous["terminal"] is None or actual["terminal"] == previous["terminal"], "打开快照丢失或改写既往终态")
    expected = [{"start": value["plan"]["freeze_payload"]["holdout_split"]["start"], "end": value["plan"]["freeze_payload"]["holdout_split"]["end"], "reason": ref} for ref, value in frozen.items()]
    _require(freeze["exposed_intervals"] == expected, "冻结暴露区间与既往账本不精确对应")
    return parent
