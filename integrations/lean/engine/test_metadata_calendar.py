"""现货交易日历按北京时间开盘时已公布的修订取值。"""
import importlib.util
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def test_open_revision_can_cancel_a_trading_date(tmp_path):
    path = Path(__file__).parents[1] / "prepare_metadata.py"
    spec = importlib.util.spec_from_file_location("prepare_metadata", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    upstream = tmp_path / "upstream"
    for directory, name, content in [
        ("market-hours", "market-hours-database.json", '{"entries":{}}'),
        ("symbol-properties", "symbol-properties-database.csv", ""),
    ]:
        folder = upstream / "Data" / directory
        folder.mkdir(parents=True)
        (folder / name).write_text(content, encoding="utf-8")
    scope = tmp_path / "minute-scopes" / "market"
    scope.mkdir(parents=True)
    days = ["2024-01-08", "2024-01-09", "2024-01-10"]
    def policy(revision, at, dates):
        return dict(instrument_index=0, revision=revision, available_at=at,
                    effective_from=days[0], effective_to=days[-1], trading_dates=dates)
    pq.write_table(pa.Table.from_pylist([
        policy(1, "2024-01-07T00:00:00Z", days),
        policy(2, "2024-01-08T01:30:01Z", []),
        policy(3, "2024-01-10T01:30:00Z", [days[-1]]),
    ]), scope / "policies.parquet")
    pq.write_table(pa.Table.from_pylist([dict(instrument=dict(instrument_id="000001.XSHE"))]),
                   scope / "instruments.parquet")
    pq.write_table(pa.Table.from_pylist([dict(segments=[dict(start_time="09:30", end_time="11:30",
        bar_eligible=True, start_day_offset=0, end_day_offset=0)])]), scope / "segments.parquet")
    target = tmp_path / "metadata"
    module.prepare_strategy_metadata(tmp_path, upstream, target, dict(strategy_declaration={"asset_class": "cn_stock"},
        minute_scope={"name": "market"}, instruments=[{"instrument_id": "000001.XSHE"}]))
    entries = json.loads((target / "market-hours/market-hours-database.json").read_bytes())["entries"]
    assert entries["Equity-jqchina-[*]"]["holidays"] == []
    assert entries["Equity-jqchina-000001"]["holidays"] == ["01/09/2024"]
