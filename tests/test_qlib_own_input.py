"""自有输入必须复用批准归档、显式字段和金融事实。"""
from copy import deepcopy
import importlib
import json
from pathlib import Path

import pytest

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"


@pytest.fixture
def own_input(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    inputs = importlib.import_module("inputs")
    synthetic = importlib.import_module("synthetic")
    loader = importlib.import_module("input_config").load_input_config
    inputs.prepare_inputs(tmp_path)
    days = [x.isoformat() for x in synthetic.sessions()]
    codes = list(synthetic.instruments())
    config = {"contract_version": "qlib-own-input-v1", "research_id": "own_etf", "display_name": "自有ETF研究",
        "catalog_lock": "catalog", "input_snapshot_manifest": "inputs.json", "calendar_sessions": days,
        "calendar_id": "explicit_calendar", "calendar_source": "已声明交易日历", "entities": codes,
        "columns": dict(zip(("date", "code", "close", "open", "high_limit", "low_limit", "paused"),
            (*inputs.PRICE_FIELDS, "fld_demo_open", "fld_demo_high_limit", "fld_demo_low_limit", "fld_demo_paused"))),
        "sources": [{"source_id": "own"}], "localization": [{"decision_id": "fixed_pool"}],
        "snapshot_scope": "固定证券池验收", "fixed_clock": "2026-10-03T09:00:00+08:00",
        "finance": {"market_rule_profile_id": "cn_etf.daily.curated.v1", "bond_etf_codes": [],
            "equity_etf_codes": codes, "commission_ppm": 300, "min_commission_units": 5,
            "initial_cash_cny": 200000., "corporate_actions": [], "corporate_action_evidence": "合成无公司行动",
            "classification_evidence": "合成权益ETF", "policy_available_at": days[0]+"T00:00:00+08:00", "price_scale": 3}}
    def load(value, mode="portfolio"):
        path = tmp_path / "input-config.json"
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        return loader(path, mode=mode)
    return config, load


def test_own_input_preserves_frozen_requests_and_financial_parameters(own_input):
    config, load = own_input
    actual, requests, archive = load(config)
    assert actual["finance"]["initial_cash_cny"] == 200000.
    assert actual["finance"]["price_scale"] == 3
    assert {x["request_id"] for x in requests} == {"daily_feature", "daily_label"}
    assert all(x["dataset_id"] == "public.synthetic.etf.daily" for x in requests)
    assert Path(actual["catalog_lock"]).is_absolute()
    assert all(x["binding_id"] == "synthetic.archived" for x in archive["requests"].values())


@pytest.mark.parametrize("change, message", [
    (lambda c: c["columns"].update(open="unknown"), "映射字段"),
    (lambda c: c["columns"].update(date="fld_equity_daily_close", close="fld_equity_daily_date"), "类型不匹配"),
    (lambda c: c["finance"].update(policy_available_at="2025-01-01T00:00:00+08:00"), "尚不可见"),
    (lambda c: c["finance"].pop("classification_evidence"), "金融声明"),
    (lambda c: c["finance"].update(equity_etf_codes=[]), "完整覆盖"),
    (lambda c: c["finance"].update(price_scale=2), "价格精度"),
    (lambda c: c["calendar_sessions"].__setitem__(1, c["calendar_sessions"][0]), "严格递增"),
])
def test_own_input_rejects_incomplete_or_future_facts(own_input, change, message):
    config, load = own_input
    candidate = deepcopy(config)
    change(candidate)
    with pytest.raises(ValueError, match=message):
        load(candidate)


def test_development_cannot_import_full_holdout_prices(own_input):
    config, load = own_input
    with pytest.raises(ValueError, match="不得包含holdout"):
        load(config, "development")


def test_portfolio_graph_forwards_cash_and_calendar_but_not_input_evidence(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    graph = importlib.import_module("portfolio_plan")
    prepare = importlib.import_module("prepare")
    design = {"entities": ["510050.XSHG", "510180.XSHG", "510300.XSHG"],
        "calendar_sessions": ["2020-01-02"], "calendar_id": "cn_trade_days",
        "finance": {"initial_cash_cny": 234567., "commission_ppm": 120,
            "price_scale": 3, "classification_evidence": "固定权益ETF", "corporate_action_evidence": "窗口内无行动"}}
    nodes = [{"node_id": "validity", "inputs": []}]
    declarations = [("validity", {"operator": {"input_ports": []}})]
    graph.extend_portfolio(nodes, declarations, [], design, prepare.node, prepare.declaration)
    parameters = next(x["parameters"] for x in nodes if x["node_id"] == "portfolio_simulation")
    assert parameters["initial_cash_cny"] == 234567.
    assert parameters["commission_ppm"] == 120
    assert parameters["calendar_id"] == "cn_trade_days"
    assert not {"price_scale", "classification_evidence", "corporate_action_evidence"} & set(parameters)
    market = next(x["parameters"] for x in nodes if x["node_id"] == "portfolio_market")
    assert market["design"] is design


def test_generated_input_template_can_be_loaded(own_input, tmp_path):
    config, _ = own_input
    module = importlib.import_module("input_config")
    design = {key: config[key] for key in ("entities", "calendar_sessions", "calendar_id", "calendar_source", "snapshot_scope")}
    design["finance"] = {"initial_cash_cny": 100000., "price_scale": 3}
    design["market_fields"] = [config["columns"][key] for key in module.COLUMN_ROLES]
    path = module.write_input_template(tmp_path, design=design, research_id="public.qlib.etf",
        display_name="合成ETF日频Qlib研究", catalog_lock=tmp_path/"catalog", archive=tmp_path/"inputs.json",
        fixed_clock=config["fixed_clock"], sources=config["sources"], localization=config["localization"])
    actual, requests, archive = module.load_input_config(path, mode="portfolio")
    assert actual["columns"] == config["columns"]
    assert actual["calendar_sessions"] == config["calendar_sessions"]
    assert actual["finance"]["corporate_action_evidence"]
    assert actual["finance"]["classification_evidence"]
    assert actual["finance"]["commission_ppm"] == 300
    assert set(archive["requests"]) == {item["request_id"] for item in requests}
