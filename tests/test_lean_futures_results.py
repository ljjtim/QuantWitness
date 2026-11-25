"""期货八表发布、研究权益归属和成交腿统计的独立手算。"""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform.lean_contract import (
    LEAN_ARTIFACT_TYPE, LEAN_JSONL_FILES, LEAN_METRICS_PATH, LEAN_METRICS_SCHEMA_ID,
    LEAN_PATHS, LEAN_SCHEMA_IDS, LEAN_SUPPORT_PATHS, LeanResultContractError, validate_lean_result_closure,
)
from research_pipeline.platform.metric_contracts import compose_metric_registry
from research_pipeline.research.lean_metrics import (
    lean_metric_definitions, lean_metric_definitions_for_nodes, lean_metric_rows,
    lean_metrics_table, lean_research_daily, lean_trade_totals,
)
from research_pipeline.results import ResultSnapshot, ResultSpec, ResultTableSpec
from research_pipeline.results.metrics import metrics_from_snapshot


def futures_case():
    # 两方向各一手、乘数10、每笔费用1；先按105估值，再结算108。
    # 多头100开、空头110开：首日C=99998，U=50+50，E=100098，M=210。
    # 次日107平空：C=100107，多头U=-10，E=100097；尾部109结算后E=100117。
    common = dict(account_id="futures-CNY", asset_class="cn_future", frozen_funds=0,
                  equity_semantics="latest_visible_mark_not_session_close")
    daily = [dict(common, date="2024-01-03", trading_date="2024-01-03", cash=99998,
                  unrealized_pnl=100, equity=100098, margin=210, available_funds=99888,
                  valuation_at="2024-01-03T15:00:00+08:00", settled_through={}),
             dict(common, date="2024-01-04", trading_date="2024-01-04", cash=100107,
                  unrealized_pnl=-10, equity=100097, margin=107, available_funds=99990,
                  valuation_at="2024-01-04T15:00:00+08:00", settled_through={"TEST2401": "2024-01-03"})]
    terminal = dict(common, cash=100117, unrealized_pnl=0, equity=100117, margin=109,
                    available_funds=100008, valuation_at="2024-01-05T09:30:00+08:00",
                    settled_through={"TEST2401": "2024-01-04"}, trading_date="2024-01-04")
    events = [dict(terminal, event_sequence=41, reason="settlement", at=terminal["valuation_at"],
                   available_at=terminal["valuation_at"]),
              dict(terminal, event_sequence=42, reason="terminal", at=terminal["valuation_at"],
                   available_at=terminal["valuation_at"])]
    fills = []
    for identifier, price, direction, day in ((1, 100, "long", "2024-01-03"),
                                             (2, 110, "short", "2024-01-03"),
                                             (3, 107, "short", "2024-01-04")):
        fills.append(dict(account_id="futures-CNY", order_id=identifier, fill_id=f"{identifier}:2",
            leg_id=f"{identifier}:2:0", instrument_id="TEST2401", direction=direction,
            bucket="yesterday" if identifier == 3 else "today", position_effect="close_yesterday" if identifier == 3 else "open",
            quantity=1, price=price, price_scale=0, multiplier=10, fee=1,
            realized_pnl=10 if identifier == 3 else 0, rule_reference="synthetic-fixed-rule",
            at=day+"T09:32:00+08:00", trading_date=day, event_sequence=identifier*10))
    # orders中的费用不参与期货统计，成交事实只来自fills。
    orders = [dict(order_id=i, quantity=1, price=1, fee=999, status="Filled",
                   at="2024-01-03T09:32:00+08:00") for i in (1, 2, 3)]
    return daily, orders, fills, events, terminal


def values(rows):
    return {row["metric_ref"].split("@")[0].removeprefix("project.lean."): row["value"] for row in rows}


def test_two_direction_hand_calculation_and_terminal_settlement_return():
    daily, orders, fills, events, account = futures_case()
    original = deepcopy(daily)
    rows = lean_metric_rows(daily, orders, fills=fills, account_events=events,
                            final_account=account, initial_equity=100000)
    actual = values(rows)
    assert actual["final_equity"] == 100117
    assert actual["total_return"] == pytest.approx(117/100000)
    assert actual["fee_total"] == 3
    assert actual["gross_traded_notional"] == 3170
    assert actual["fill_count"] == 3 and actual["filled_order_count"] == 3
    assert actual["turnover_mean"] == pytest.approx((2100/200000 + 1070/200196)/2)
    assert actual["margin_utilization_mean"] == pytest.approx((210/100098 + 109/100117)/2)
    assert actual["capital_utilization_mean"] == actual["margin_utilization_mean"]
    assert actual["max_drawdown"] == 0
    assert all(row["sample_size"] == 2 and row["sample_end"] == "2024-01-04" for row in rows)
    projected = lean_research_daily(daily, account_events=events, final_account=account)
    assert daily == original
    assert projected[-1]["equity"] == 100097
    assert projected[-1]["cash"] == 100107 and projected[-1]["available_funds"] == 99990
    assert projected[-1]["research_equity"] == 100117
    assert projected[-1]["research_valuation_at"] == "2024-01-05T09:30:00+08:00"
    assert lean_research_daily(daily, account_events=events) == projected
    assert lean_research_daily(daily, final_account=account) == projected


@pytest.mark.parametrize("frequency", ["1m", "1d"])
def test_futures_planned_definitions_match_current_account_semantics(frequency):
    daily, orders, fills, events, account = futures_case()
    nodes = [SimpleNamespace(parameters={"execution_backend": "lean", "frequency": frequency,
                "strategy_declaration": {"asset_class": "cn_future"}})]
    planned = {d.metric_ref: d.to_dict() for d in lean_metric_definitions_for_nodes(nodes)}
    actual = lean_metric_rows(daily, orders, fills=fills, account_events=events, final_account=account, initial_equity=100000)
    assert {r["metric_ref"]: json.loads(r["definition_json"]) for r in actual} == planned


def test_night_fills_use_trading_date_and_split_legs_count_one_execution():
    _, orders, fills, _, _ = futures_case()
    leg = dict(fills[0], at="2024-01-02T21:32:00+08:00", quantity=2)
    second = dict(leg, leg_id="1:2:1", bucket="yesterday", quantity=1, fee=2)
    totals = lean_trade_totals(["2024-01-03"], orders[:1], fills=[leg, second], futures=True)
    assert totals["gross_by_day"] == {"2024-01-03": Decimal(3000)}
    assert totals["fee_total"] == 3 and totals["fill_count"] == 1
    assert totals["filled_order_count"] == 1


@pytest.mark.parametrize("bad", ["missing_fills", "duplicate_leg", "negative_lots", "outside_day", "terminal_mismatch", "sequence", "account", "terminal_day"])
def test_futures_incomplete_or_conflicting_result_facts_are_rejected(bad):
    daily, orders, fills, events, account = futures_case()
    if bad == "missing_fills": fills = None
    if bad == "duplicate_leg": fills.append(dict(fills[0]))
    if bad == "negative_lots": fills[0]["quantity"] = -1
    if bad == "outside_day": fills[0]["trading_date"] = "2024-01-05"
    if bad == "terminal_mismatch": account["equity"] = 100118
    if bad == "sequence": events.reverse()
    if bad == "account": events[0]["account_id"] = "other"
    if bad == "terminal_day": account["trading_date"] = "2024-01-05"
    with pytest.raises(ValueError):
        lean_metric_rows(daily, orders, fills=fills, account_events=events, final_account=account, initial_equity=100000)


def futures_report_context():
    from test_lean_report import execution_table

    daily, orders, fills, events, account = futures_case()
    metric_table = lean_metrics_table(daily, orders, fills=fills, account_events=events,
                                      final_account=account, initial_equity=100000)
    definitions = lean_metric_definitions(futures=True, equity_semantics="latest_visible_mark_not_session_close")
    manifests = [SimpleNamespace(table_id="lean_metrics", schema_id=LEAN_METRICS_SCHEMA_ID,
        source_node_id="simulation", source_port="simulation", artifact_type=LEAN_ARTIFACT_TYPE,
        path_prefix=LEAN_METRICS_PATH, artifact_key="simulation-artifact")]
    proofs = compose_metric_registry(definitions).prove(tuple(d.metric_ref for d in definitions),
        (("simulation", "simulation", LEAN_ARTIFACT_TYPE),), tuple(manifests))
    tables = {LEAN_METRICS_SCHEMA_ID: metric_table}
    records = dict(daily=daily, orders=orders, fills=fills, account_events=events,
        positions=[dict(account_id="futures-CNY", event_sequence=42, instrument_id="TEST2401",
                        direction="long", bucket="yesterday", quantity=1)],
        lifecycle=[dict(kind="settlement", at=account["valuation_at"], detail={"instrument_id":"TEST2401", "amount":10})])
    for name, schema in LEAN_SCHEMA_IDS.items():
        tables[schema] = execution_table(name, records.get(name, []))
        manifests.append(SimpleNamespace(table_id="lean_"+name, schema_id=schema, artifact_key="simulation-artifact"))
    account_file = SimpleNamespace(artifact_key="simulation-artifact", source_path="lean/account.json",
                                   relative_path="support/account.json")
    bundle = SimpleNamespace(tables=manifests, metric_proofs=proofs, result_id="result.futures", metric_definitions=(),
                             support_files=(account_file,))
    snapshot = ResultSnapshot(bundle, Path("I:/temp/quantwitness-lean-cn-futures/tests"), tables,
                              {"lean/account.json": json.dumps(account).encode()})
    context = SimpleNamespace(snapshot=snapshot, metrics=metrics_from_snapshot(snapshot))
    request = {"contract_version": "lean-research-report-v2", "result_id": "result.futures",
        "tables": {**{name: "lean_"+name for name in LEAN_SCHEMA_IDS}, "metrics":"lean_metrics"},
        "budget": {"max_rows":1000,"memory_bytes":1024*1024}}
    return context, request


def test_report_uses_formal_terminal_equity_and_displays_four_bucket_and_lifecycle():
    from research_pipeline.evidence.lean_report import read_lean_report_data, render_lean_report
    context, request = futures_report_context()
    rows, _ = read_lean_report_data(context, request)
    assert rows["research_daily"][-1]["research_equity"] == 100117
    assert rows["daily"][-1]["equity"] == 100097
    pytest.importorskip("plotly")
    report = render_lean_report(context, request, result_summary="期货研究观察")
    assert "四桶持仓快照" in report and "结算、风险与换月事件" in report
    assert "2024-01-05T09:30:00+08:00" in report
    assert "日均保证金使用率" in report and "合约乘数" in report


def test_report_rejects_old_or_incomplete_table_request():
    from research_pipeline.evidence.lean_report import validate_lean_report_request
    _, request = futures_report_context()
    request["tables"].pop("fills")
    with pytest.raises(ValueError): validate_lean_report_request(request)
    request["contract_version"] = "lean-research-report-v1"
    with pytest.raises(ValueError): validate_lean_report_request(request)


def test_result_closure_requires_all_eight_current_tables():
    node = SimpleNamespace(node_id="strategy", inputs=[])
    tables = [ResultTableSpec("lean_"+name,"primary" if name=="daily" else "diagnostic", "strategy",
        "simulation", LEAN_ARTIFACT_TYPE, schema, LEAN_PATHS[name]) for name,schema in LEAN_SCHEMA_IDS.items()]
    validate_lean_result_closure(node, ResultSpec.build(tuple(tables)))
    with pytest.raises(LeanResultContractError):
        validate_lean_result_closure(node, ResultSpec.build(tuple(tables[:-1])))


def test_publisher_seals_eight_typed_tables_and_terminal_metrics(tmp_path):
    from research_pipeline.platform.lean_execution_schema import LEAN_EXECUTION_SCHEMAS
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from research_pipeline.runtime.lean_strategy_execution import _publish_lean_output
    daily, orders, fills, events, account = futures_case()
    for path in LEAN_SUPPORT_PATHS:
        (tmp_path/Path(path).name).write_text("{}", encoding="utf-8")
    (tmp_path/"account.json").write_text(json.dumps(account),encoding="utf-8")
    (tmp_path/"strategy-input.json").write_text(json.dumps({"initial_cash_units":10000000,
        "dates":[r["date"] for r in daily]}),encoding="utf-8")
    typed_orders = [dict(row, event_id=2, instrument_id="TEST2401",
                         trading_date="2024-01-03", order_event_count=index)
                    for index, row in enumerate(orders, 1)]
    data=dict(daily=daily,orders=typed_orders,fills=fills,account_events=events)
    for name, path in LEAN_JSONL_FILES.items():
        (tmp_path/path).write_text("".join(json.dumps(r)+"\n" for r in data.get(name,[])),encoding="utf-8")
    store=ExternalArtifactStore(tmp_path/"external")
    result=_publish_lean_output(SimpleNamespace(external_store=store),tmp_path,{}, {},
                                 json.loads((tmp_path / "strategy-input.json").read_text()))
    commit=result.values["simulation"].external_commit
    assert commit.artifact_type == LEAN_ARTIFACT_TYPE
    for name in LEAN_SCHEMA_IDS:
        table=pq.read_table(store.objects_root/commit.semantic_hash/LEAN_PATHS[name]/"part-00000.parquet")
        assert table.schema.names == LEAN_EXECUTION_SCHEMAS[name].names
        if name == "daily":
            assert table.schema.field("equity").type == pa.decimal128(38, 28)
    metrics=pq.read_table(store.objects_root/commit.semantic_hash/LEAN_METRICS_PATH/"part-00000.parquet")
    assert values(metrics.to_pylist())["final_equity"] == 100117
