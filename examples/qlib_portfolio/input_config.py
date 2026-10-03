"""自有封存日频行情的显式配置，不生成Catalog批准。"""
from datetime import date, datetime
import json
import math
from pathlib import Path

from research_pipeline.catalog import CompiledCatalog
from research_pipeline.data_plane.archived_inputs import load_archived_input_manifest
from research_pipeline.data_plane.admitted_plan_codec import admitted_plan_from_dict

COLUMN_ROLES = ("date", "code", "close", "open", "high_limit", "low_limit", "paused")


def load_input_config(path, *, mode):
    path = Path(path).resolve()
    config = json.loads(path.read_text(encoding="utf-8"))
    required = {"contract_version", "research_id", "display_name", "catalog_lock", "input_snapshot_manifest",
                "calendar_sessions", "calendar_id", "calendar_source", "entities", "columns", "sources",
                "localization", "snapshot_scope", "finance", "fixed_clock"}
    if set(config) != required or config["contract_version"] != "qlib-own-input-v1":
        raise ValueError("自有输入配置字段不完整或版本不受支持")
    for key in ("research_id", "display_name", "calendar_id", "calendar_source", "snapshot_scope"):
        if not isinstance(config[key], str) or not config[key].strip():
            raise ValueError("自有输入必须显式声明 " + key)
    for key in ("catalog_lock", "input_snapshot_manifest"):
        target = Path(config[key])
        config[key] = str((target if target.is_absolute() else path.parent / target).resolve())
    days = config["calendar_sessions"]
    if len(days) < 102 or days != sorted(set(days)):
        raise ValueError("日历至少需要102个严格递增会话，含标签成熟闭包")
    for day in days:
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError("日历日期必须为ISO日期")
    clock = datetime.fromisoformat(config["fixed_clock"])
    if clock.utcoffset() is None or clock.date() < date.fromisoformat(days[-1]):
        raise ValueError("冻结时钟必须带时区且覆盖标签成熟日期")
    codes = config["entities"]
    if len(codes) < 3 or codes != sorted(set(codes)) or any(not code for code in codes):
        raise ValueError("固定证券池至少三只，必须排序且不重复")
    columns = config["columns"]
    if set(columns) != set(COLUMN_ROLES) or len(set(columns.values())) != len(COLUMN_ROLES):
        raise ValueError("行情字段映射必须包含七个不同逻辑字段")
    if not config["sources"] or not config["localization"]:
        raise ValueError("自有输入必须说明来源和固定证券池的研究边界")
    finance = config["finance"]
    fields = {"market_rule_profile_id", "bond_etf_codes", "equity_etf_codes", "commission_ppm",
              "min_commission_units", "initial_cash_cny", "corporate_actions", "corporate_action_evidence",
              "classification_evidence", "policy_available_at", "price_scale"}
    if set(finance) != fields:
        raise ValueError("金融声明必须完整包含规则、分类、公司行动、费用和价格精度")
    if (set(finance["bond_etf_codes"]) & set(finance["equity_etf_codes"])
            or set(finance["bond_etf_codes"]) | set(finance["equity_etf_codes"]) != set(codes)):
        raise ValueError("ETF分类必须互斥且完整覆盖固定证券池")
    if not finance["classification_evidence"] or not finance["corporate_action_evidence"]:
        raise ValueError("分类和公司行动声明必须提供证据")
    if (not math.isfinite(finance["initial_cash_cny"]) or finance["initial_cash_cny"] <= 0
            or any(type(finance[key]) is not int or finance[key] < 0 for key in ("commission_ppm", "min_commission_units"))
            or finance["price_scale"] != 3):
        raise ValueError("资金、费用或价格精度无效")
    visible = datetime.fromisoformat(finance["policy_available_at"])
    if visible.utcoffset() is None or visible > datetime.fromisoformat(days[0] + "T00:00:00+08:00"):
        raise ValueError("金融规则在研究起点尚不可见")
    from research_pipeline.platform.market_rule_defaults import resolve_cn_etf_daily_market_rule_profile
    profile = resolve_cn_etf_daily_market_rule_profile(finance["market_rule_profile_id"])
    profile.require_covers(date.fromisoformat(days[0]), date.fromisoformat(days[-1]))
    catalog = CompiledCatalog.load(config["catalog_lock"])
    archive = load_archived_input_manifest(config["input_snapshot_manifest"])
    if set(archive["requests"]) != {"daily_feature", "daily_label"}:
        raise ValueError("归档必须恰好包含daily_feature和daily_label")
    boundary_index = len(days) - 23
    end = days[boundary_index - 1] if mode == "development" else days[-3]
    requests = []
    for request_id, entry in archive["requests"].items():
        plan = admitted_plan_from_dict(entry["original_plan"])
        query = plan.query
        if query.purpose.value != request_id.removeprefix("daily_"):
            raise ValueError("归档请求用途不匹配")
        request = query.to_dict()
        if (tuple(query.universe.instruments) != tuple(codes)
                or query.time_range.start.isoformat() != days[0] or query.time_range.end.isoformat() != end):
            raise ValueError("归档证券或时间范围与配置不一致；开发归档不得包含holdout价格")
        if query.adjustment != "unadjusted" or not set(columns.values()) <= set(query.field_ids):
            raise ValueError("归档必须包含全部映射字段且使用不复权行情")
        binding = catalog.bindings.get(entry["binding_id"])
        if not binding or binding["status"] != "approved" or binding["dataset_id"] != query.dataset_id:
            raise ValueError("持久Catalog缺少对应已批准归档binding")
        for role, field_id in columns.items():
            field = catalog.fields[field_id]
            expected = "date32" if role == "date" else "string" if role == "code" else "bool" if role == "paused" else "float64"
            if field["data_type"] != expected:
                raise ValueError("行情字段映射类型不匹配: " + role)
        dataset = catalog.datasets[query.dataset_id]
        policy = catalog.policies[dataset["available_time_policy"]]
        if policy["rules"].get("available_after") != "next_session_open":
            raise ValueError("本例要求日频行情按下一会话开盘可见")
        for key in ("contract_version", "ir_version", "as_of"):
            request.pop(key, None)
        request["request_id"] = request_id
        requests.append(request)
    return config, requests, archive


def write_input_template(root, *, design, research_id, display_name, catalog_lock, archive, fixed_clock, sources, localization):
    """保存可直接复用的完整配置，引用已生成的合成归档。"""
    root = Path(root)
    finance = {"market_rule_profile_id": "cn_etf.daily.curated.v1", "bond_etf_codes": [],
        "equity_etf_codes": list(design["entities"]), "commission_ppm": 300,
        "min_commission_units": 5, "initial_cash_cny": 100000., "corporate_actions": [],
        "corporate_action_evidence": "synthetic.py确定性生成，无分红或拆合份额",
        "classification_evidence": "synthetic.py显式定义的虚构权益ETF",
        "policy_available_at": design["calendar_sessions"][0] + "T00:00:00+08:00", "price_scale": 3}
    finance.update(design["finance"])
    payload = {"contract_version": "qlib-own-input-v1", "research_id": research_id,
        "display_name": display_name, "catalog_lock": str(Path(catalog_lock).resolve()),
        "input_snapshot_manifest": str(Path(archive).resolve()),
        "calendar_sessions": list(design["calendar_sessions"]), "calendar_id": design["calendar_id"],
        "calendar_source": design["calendar_source"], "entities": list(design["entities"]),
        "columns": dict(zip(COLUMN_ROLES, design["market_fields"])), "sources": sources,
        "localization": localization, "snapshot_scope": design["snapshot_scope"],
        "finance": finance, "fixed_clock": fixed_clock}
    path = root / "input-config.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
