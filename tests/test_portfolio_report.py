"""组合报告只用内存正式表与临时 HTML，验证展示口径和消费边界。"""
from datetime import date
from types import SimpleNamespace

import pyarrow as pa
import pytest

from research_pipeline.cli.report_output import report_table_ids, write_qlib_report
from research_pipeline.evidence.qlib_report import validate_report_request
from research_pipeline.evidence.portfolio_report import (
    TABLE_SCHEMAS, _figures, portfolio_display_data, read_portfolio_frames,
)


def request():
    return {"contract_version": "qlib-portfolio-report-v1", "result_id": "result.portfolio",
            "portfolio_id": "default", "tables": {role: role for role in TABLE_SCHEMAS},
            "budget": {"max_rows": 1000, "memory_bytes": 64 * 1024 * 1024}}


def context():
    days = [date(2024, 1, day) for day in (3, 4, 5)]
    tables = {}
    cash, valuations, positions = [], [], []
    for session, nav, money in zip(days, (9900, 11000, 9900), (5900, 7000, 5900)):
        common = {"portfolio_id": "default", "session": session}
        cash.append({**common, "currency": "CNY", "opening_cash_units": 10000,
                     "total_cash_units": money, "available_cash_units": money})
        valuations.append({**common, "currency": "CNY", "nav_units": nav})
        positions.append({**common, "instrument_id": "ETF", "quantity": 10,
                          "sellable_quantity": 10, "market_value_units": 4000})
    tables["cash"] = pa.Table.from_pylist(list(reversed(cash)))
    tables["valuations"] = pa.Table.from_pylist(list(reversed(valuations)))
    tables["positions"] = pa.Table.from_pylist(list(reversed(positions)))
    common = {"portfolio_id": "default", "session": days[0]}
    tables["orders"] = pa.Table.from_pylist([{**common, "order_id": "order.1", "instrument_id": "ETF",
        "side": "buy", "requested_quantity": 10, "filled_quantity": 10, "status": "filled",
        "terminal_reason": None, "submitted_at": "2024-01-03T09:30:00+08:00"}])
    tables["fills"] = pa.Table.from_pylist([{**common, "fill_id": "fill.1", "order_id": "order.1",
        "instrument_id": "ETF", "side": "buy", "quantity": 10, "execution_price_units": 4000,
        "price_scale": 3, "notional_units": 4000, "fee_units": 100,
        "fill_time": "2024-01-03T09:30:00+08:00"}])
    tables["costs"] = pa.Table.from_pylist([{**common, "fill_id": "fill.1", "cost_type": "transaction_fee",
                                          "currency": "CNY", "amount_units": 100}])
    tables["metrics"] = pa.Table.from_pylist([{
        "metric_ref": "portfolio.max_drawdown@1.0.0", "value": -0.1, "unit": "decimal_return",
        "sample_start": str(days[0]), "sample_end": str(days[-1]), "sample_size": 3, "status": "computed"}])

    class Snapshot:
        bundle = SimpleNamespace(result_id="result.portfolio", tables=[SimpleNamespace(
            table_id=role, schema_id=schema, source_node_id="simulation", source_port="simulation"
        ) for role, schema in TABLE_SCHEMAS.items()])
        projected = {}

        def table_schema(self, schema_id):
            role = next(role for role, schema in TABLE_SCHEMAS.items() if schema == schema_id)
            return tables[role].schema

        def iter_table_batches(self, schema_id, *, columns, batch_size):
            role = next(role for role, schema in TABLE_SCHEMAS.items() if schema == schema_id)
            self.projected[role] = columns
            yield from tables[role].select(columns).to_batches(max_chunksize=batch_size)

    return SimpleNamespace(snapshot=Snapshot(), tables=tables,
                           verification=SimpleNamespace(verification_hash="verification.portfolio", status="fail"))


def test_sorted_cash_net_value_drawdown_origin_and_fill_units():
    pytest.importorskip("plotly")
    ctx = context()
    data = portfolio_display_data(read_portfolio_frames(ctx, request()))
    daily = data["daily"]
    assert list(daily.session) == [date(2024, 1, day) for day in (3, 4, 5)]
    assert list(daily.net_value) == pytest.approx([0.99, 1.1, 0.99])
    assert list(daily.drawdown) == pytest.approx([0, 0, -0.1])
    assert list(daily.cash_cny) == [59, 70, 59]
    assert list(daily.positions_cny) == [40, 40, 40]
    assert list(daily.cost_cny) == [1, 0, 0]
    assert list(daily.cumulative_cost_cny) == [1, 1, 1]
    assert data["fills"].iloc[0]["price_cny"] == 4
    assert data["fills"].iloc[0]["fee_cny"] == 1
    assert data["fills"].iloc[0]["notional_cny"] == 40
    assert "source_state_hash" not in ctx.snapshot.projected["cash"]
    charts = _figures(data)
    assert list(charts[0].data[0].y) == pytest.approx([0.99, 1.1, 0.99])
    assert list(charts[1].data[0].y) == pytest.approx([0, 0, -0.1])


def test_offline_html_keeps_failed_verification_and_trade_fields(tmp_path, monkeypatch):
    pytest.importorskip("plotly")
    import duckdb

    def forbidden(*args, **kwargs):
        pytest.fail("报告不能连接数据库")

    monkeypatch.setattr(duckdb, "connect", forbidden)
    output = tmp_path / "portfolio.html"
    write_qlib_report(output, context=context(), request=request(), markdown="独立验证状态：fail")
    text = output.read_text(encoding="utf-8")
    for field in ("独立验证状态：fail", "verification.portfolio", "result.portfolio", "成交费用（元）",
                  "首个收盘会话为 0", "不再次从净值扣减", "portfolio.max_drawdown@1.0.0", "decimal_return"):
        assert field in text
    assert text.count("plotly.js v") == 1
    assert "<script src=" not in text
    assert text.count('class="plotly-graph-div"') == 6
    with pytest.raises(FileExistsError):
        write_qlib_report(output, context=context(), request=request(), markdown="独立验证状态：fail")


def test_complete_table_request_and_shared_read_budget():
    req = validate_report_request(request())
    assert report_table_ids(req) == tuple(req["tables"].values())
    assert report_table_ids(None) == ()
    assert report_table_ids({"table_id": "predictions"}) == ("predictions",)
    req["tables"]["cash"] = "valuations"
    with pytest.raises(ValueError, match="七表"):
        validate_report_request(req)
    for budget in ({"max_rows": 10, "memory_bytes": 64 * 1024 * 1024}, {"max_rows": 1000, "memory_bytes": 10}):
        req = request()
        req["budget"] = budget
        with pytest.raises(ValueError, match="max_rows/memory_bytes"):
            read_portfolio_frames(context(), req)


@pytest.mark.parametrize("failure", ["source", "schema", "identity", "portfolio", "currency"])
def test_mixed_source_identity_and_units_are_rejected(failure):
    ctx = context()
    req = request()
    if failure == "source":
        ctx.snapshot.bundle.tables[0].source_node_id = "other"
    elif failure == "schema":
        ctx.snapshot.bundle.tables[0].schema_id = "other"
    elif failure == "identity":
        req["result_id"] = "other"
    else:
        column = "portfolio_id" if failure == "portfolio" else "currency"
        table = ctx.tables["cash"]
        ctx.tables["cash"] = table.set_column(table.schema.get_field_index(column), column, pa.array(["other"] * 3))
    with pytest.raises(ValueError):
        read_portfolio_frames(ctx, req)


def test_empty_trading_tables_remain_readable():
    pytest.importorskip("plotly")
    ctx = context()
    for role in ("orders", "fills", "costs", "positions"):
        ctx.tables[role] = ctx.tables[role].slice(0, 0)
    data = portfolio_display_data(read_portfolio_frames(ctx, request()))
    assert data["fills"].empty and data["order_status"].empty
    assert list(data["daily"].cost_cny) == [0, 0, 0]
    assert len(_figures(data)) == 6


@pytest.mark.parametrize("failure", ["duplicate", "missing", "metric_window"])
def test_inconsistent_daily_or_metric_windows_are_rejected(failure):
    frames = read_portfolio_frames(context(), request())
    if failure == "duplicate":
        import pandas as pd
        frames["cash"] = pd.concat([frames["cash"], frames["cash"].iloc[:1]])
    elif failure == "missing":
        frames["cash"] = frames["cash"].iloc[1:]
    else:
        frames["metrics"].loc[0, "sample_start"] = "2024-01-04"
    with pytest.raises(ValueError):
        portfolio_display_data(frames)
