"""合成期货通过真实Launcher检查双向订单、结算和夜盘账户，无数据库写入。"""
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

from test_direct_launcher import launcher_environment, timestamp


CODE = "IF2401.CCFX"
DAYS = ("2024-01-03", "2024-01-04")


@pytest.mark.parametrize(("frequency", "scenario"), [("1m", "hedge"), ("1d", "hedge"), ("1m", "risk"), ("1d", "risk"), ("1d", "risk_rule_wait"), ("1m", "roll"), ("1m", "night"), ("1m", "protected_resize"), ("1m", "blocked_risk_limit"), ("1m", "blocked_risk_volume")])
def test_futures_native_orders(tmp_path, frequency, scenario):
    days = (*DAYS, "2024-01-05") if frequency == "1d" and scenario in {"risk", "risk_rule_wait"} else DAYS
    installation = os.environ.get("QW_ENGINE_TEST_INSTALLATION")
    build = os.environ.get("QW_ENGINE_TEST_BUILD")
    if not installation or not build:
        pytest.skip("需显式提供只读LEAN安装及本任务外部构建目录")
    installation, build = Path(installation), Path(build)
    launcher = tmp_path / "launcher"
    shutil.copytree(next((installation / "src").glob("Lean-*/Launcher/bin/Release")), launcher)
    for name in ("QuantWitnessLeanHost", "ChinaRules", "CorporateActions", "ParquetInput"):
        shutil.copy2(build / f"bin/{name}/net10.0/{name}.dll", launcher / f"{name}.dll")
    output = tmp_path / "run"
    output.mkdir()
    (output / "temp").mkdir()
    metadata = output / "metadata"
    for directory in ("market-hours", "symbol-properties"):
        shutil.copytree(installation / "rp-integration/metadata" / directory, metadata / directory)
    for name in ("map_files", "factor_files"):
        (metadata / "equity/usa" / name).mkdir(parents=True)
    sessions = [dict(instrument_id=CODE, trading_date=day, start_at=timestamp(day, "09:30:00").isoformat(),
        end_at=timestamp(day, "15:00:00").isoformat(), segments=[dict(start_at=timestamp(day, "09:30:00").isoformat(),
        end_at=timestamp(day, "15:00:00").isoformat())]) for day in days]
    contracts = {CODE: dict(market="cffex", product="IF", listed_date="2023-01-01", expiry_date="2024-01-19", multiplier="10",
        price_scale=0, tick_size="1", exit_deadline="2024-01-18T15:00:00+08:00")}
    symbols = [CODE]
    if scenario == "roll" or scenario.startswith("blocked_risk"):
        symbols.append("IF2403.CCFX")
        contracts[symbols[1]] = dict(contracts[CODE], expiry_date="2024-03-15", exit_deadline="2024-03-14T15:00:00+08:00")
        sessions += [dict(row, instrument_id=symbols[1]) for row in tuple(sessions)]
    if scenario == "night":
        for index, row in enumerate(sessions):
            row["start_at"] = f"2024-01-0{2 + index}T21:00:00+08:00"
            row["segments"] = [dict(start_at=row["start_at"], end_at=f"2024-01-0{3 + index}T02:30:00+08:00"),
                dict(start_at=f"2024-01-0{3 + index}T09:00:00+08:00", end_at=row["end_at"])]
    samples = {}
    for index, day in enumerate(days):
        clocks = [("09:30:00", "15:00:00", 100 if index == 0 else 107, 105 if index == 0 else 109)] if frequency == "1d" else [
            ("09:30:00", "09:31:00", 100 if index == 0 else 107, 100 if index == 0 else 107),
            ("09:31:00", "09:32:00", 100 if index == 0 else 107, 100 if index == 0 else 107),
            ("09:32:00", "09:33:00", 110 if index == 0 else 107, 110 if index == 0 else 107),
            ("09:33:00", "09:34:00", 105 if index == 0 else 107, 105 if index == 0 else 107)]
        if scenario in {"risk", "roll"} and frequency == "1m":
            clocks = [(f"09:{30 + number:02d}:00", f"09:{31 + number:02d}:00",
                100 if scenario == "roll" or index == 0 and number < 2 else 98,
                100 if scenario == "roll" or index == 0 and number < 2 else 98) for number in range(8)]
        if scenario.startswith("blocked_risk"):
            clocks = [(f"09:{30 + number:02d}:00", f"09:{31 + number:02d}:00",
                100 if index == 0 and number < 2 else 98,
                100 if index == 0 and number < 2 else 98) for number in range(16)]
        if scenario in {"risk", "risk_rule_wait"} and frequency == "1d":
            price = 100 if scenario == "risk_rule_wait" or index < 2 else 98
            clocks = [("09:30:00", "15:00:00", price, price)]
        if scenario == "protected_resize" and index == 0:
            clocks = [(f"09:{30 + number:02d}:00", f"09:{31 + number:02d}:00", price, price)
                      for number, price in enumerate((100, 110, 100, 100))]
        rule = dict(instrument_id=CODE, effective_from=day, effective_to=day,
            available_at=timestamp(day, "09:00:00").isoformat(), source_ref="synthetic-fixed-rule",
            parameters=dict(long_margin_ppm=100000, short_margin_ppm=100000,
                open_fee_per_contract_units=100, open_fee_rate_ppm=0,
                close_fee_per_contract_units=100, close_fee_rate_ppm=0,
                close_today_fee_per_contract_units=100, close_today_fee_rate_ppm=0,
                lower_limit=50, upper_limit=150, close_priority="yesterday_first"))
        if scenario in {"risk", "risk_rule_wait"} or scenario.startswith("blocked_risk"):
            rule["parameters"]["upper_limit"] = 110
        if scenario == "roll":
            rule["parameters"]["upper_limit"] = 250
        settlement = dict(instrument_id=CODE, trading_date=day, price=108 if index == 0 else 109,
            available_at=f"2024-01-0{4 + index}T09:30:00+08:00", source_ref="synthetic-settlement")
        if scenario == "risk" or scenario.startswith("blocked_risk"):
            settlement["price"] = 98
        if scenario == "roll":
            settlement["price"] = 102
        if scenario == "night":
            settlement["available_at"] = f"{day}T21:00:00+08:00"
        settlement["price"] = str(settlement["price"])
        for field in ("lower_limit", "upper_limit"):
            rule["parameters"][field] = str(rule["parameters"][field])
        day_sessions = [row for row in sessions if row["trading_date"] == day]
        rules = [dict(rule, instrument_id=code) for code in symbols]
        if scenario == "risk_rule_wait":
            rules[0] = dict(rule, parameters=dict(rule["parameters"],
                long_margin_ppm=120000 if index == 2 else 100000))
            if index == 1:
                rules.append(dict(rule, available_at=f"{day}T09:30:01+08:00",
                    parameters=dict(rule["parameters"], long_margin_ppm=120000)))
            settlement["price"] = "100"
        if scenario == "blocked_risk_limit":
            rules[0] = dict(rules[0], parameters=dict(rule["parameters"], lower_limit="98"))
        settlements = [dict(settlement, instrument_id=code, price=202 if code != CODE else settlement["price"]) for code in symbols]
        if scenario.startswith("blocked_risk"):
            settlements = [dict(settlement, instrument_id=code) for code in symbols]
        header = dict(date=day, frequency=frequency, availability="next_session_open", rules=[rule],
            sessions=day_sessions, settlements=settlements,
            signals=dict(available_at=day_sessions[0]["start_at"], selection=[]), row_count=len(clocks) * len(symbols))
        header["rules"] = rules
        if scenario == "night":
            for row in header["rules"]:
                row["available_at"] = day_sessions[0]["start_at"]
            night_rule = header["rules"][0]
            header["rules"].append(dict(night_rule, available_at=f"{day}T00:00:00+08:00",
                parameters=dict(night_rule["parameters"], upper_limit="160")))
        bars = []
        for symbol_index, code in enumerate(symbols):
            for number, (begin, end, opening, close) in enumerate(clocks):
                first, last = timestamp(day, begin), timestamp(day, end)
                if scenario == "night":
                    from datetime import timedelta
                    first = datetime.fromisoformat(day_sessions[0]["start_at"]).replace(hour=23, minute=58) + timedelta(minutes=number)
                    last = first + timedelta(minutes=1)
                if code != CODE and not scenario.startswith("blocked_risk"):
                    opening = close = 200
                if scenario == "blocked_risk_limit" and code == CODE and index == 0 and number == 2:
                    opening = close = 99
                volume = 2000 if scenario == "risk" else 1 if scenario == "roll" and number >= 3 else 100
                if scenario.startswith("blocked_risk"):
                    volume = 2000 if index == 0 and number < 2 else 10
                    if scenario == "blocked_risk_volume" and code == CODE and (index > 0 or number >= 3):
                        volume = 0
                bars.append((symbol_index, first,
                    last, opening, max(opening, close), min(opening, close), close, volume))
        samples[day] = header, bars
    observation_end = "2024-01-06T09:30:00+08:00" if len(days) == 3 else "2024-01-05T09:30:00+08:00"
    (output / "input.json").write_text(json.dumps(dict(asset_class="cn_future",
        frequency=frequency, symbols=symbols,
        dates=days, price_scale=0, contracts=contracts, sessions=sessions,
        start_at=sessions[0]["start_at"], end_at="2024-01-04T21:00:00+08:00" if scenario == "night" else observation_end)), encoding="utf-8")
    instrument = dict(instrument_id=CODE, asset_class="cn_future", venue="CCFX", currency="CNY", contract_kind="future")
    declaration = dict(asset_class="cn_future")
    (output / "strategy-input.json").write_text(json.dumps(dict(frequency=frequency, declaration=declaration,
        algorithm=dict(language="Python", entry_file="fixture_strategy.py", type_name="NativeFixture"), initial_cash_units=10000000,
        instruments={code: dict(instrument, instrument_id=code) for code in symbols},
        roll_plans=[dict(plan_id="roll-1", old_instrument_id=CODE, new_instrument_id=symbols[-1], direction="long", quantity=3,
            available_at="2024-01-03T09:00:00+08:00", start_at="2024-01-03T09:33:00+08:00", end_at="2024-01-04T15:00:00+08:00")] if scenario == "roll" else [],
        parameters=dict(frequency=frequency, scenario=scenario, slippage_bps=0, slippage_ticks=0,
            participation_cap_ppm=1000000))), encoding="utf-8")
    source = output / "strategy-source"
    source.mkdir()
    shutil.copy2(Path(__file__).with_name("fixture_future_algorithm.py"), source / "fixture_strategy.py")
    text = (installation / "rp-integration/config-template.json").read_text(encoding="utf-8-sig")
    text = re.sub(r'("(?:\\.|[^"\\])*")|//[^\n]*|/\*[\s\S]*?\*/', lambda m: m[1] or "", text)
    config = json.loads(re.sub(r",\s*([}\]])", r"\1", text))
    config.update({"environment": "backtesting", "algorithm-type-name": "NativeFixture", "algorithm-language": "Python",
        "algorithm-location": str(source / "fixture_strategy.py"), "results-destination-folder": str(output),
        "parquet-input-manifest": str(output / "input.json"), "data-folder": str(metadata),
        "python-venv": str(installation / "python-env"), "object-store-root": str(output / "storage"), "close-automatically": True})
    config["environments"]["backtesting"]["data-feed-handler"] = "QuantWitness.LeanInput.ParquetDataFeed"
    from native_fixture import prepare_native_fixture
    prepare_native_fixture(output, config, samples)
    (output / "config.json").write_text(json.dumps(config), encoding="utf-8")
    environment = launcher_environment(installation, output)
    with (output / "engine.log").open("wb") as log:
        process = subprocess.run([str(installation / "sdk/dotnet.exe"), str(launcher / "QuantConnect.Lean.Launcher.dll"),
            "--config", str(output / "config.json")], cwd=launcher, env=environment, stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT, timeout=90)
    status_file = output / "engine-status.json"
    log_tail = (output / "engine.log").read_text(errors="replace")[-8000:]
    assert process.returncode == 0 and status_file.is_file(), log_tail
    status = json.loads(status_file.read_text())
    assert status["status"] == "completed", (status, log_tail)
    account = json.loads((output / "account.json").read_text())
    fills = [json.loads(line) for line in (output / "fills.jsonl").read_text().splitlines()]
    if scenario == "risk_rule_wait":
        risk = [row for row in fills if row["risk_order"]]
        assert [(row["at"], row["quantity"], row["price"]) for row in risk] == [("2024-01-05T09:30:00+08:00", 75, 100)]
        orders = [json.loads(line) for line in (output / "orders.jsonl").read_text().splitlines()]
        risk_submitted = [row for row in orders if row["risk_order"] and row["status"] == "Submitted"]
        assert any(row["at"] == "2024-01-04T09:31:00+08:00" for row in risk_submitted)
        first_id = risk_submitted[0]["order_id"]
        assert any(row["order_id"] == first_id and row["status"] == "Canceled"
                   and row["at"] == "2024-01-04T15:00:00+08:00" for row in orders)
        assert account["cash"] == account["equity"] == 99025
        assert account["available_funds"] == 25 and account["frozen_funds"] == 0
        return
    if scenario.startswith("blocked_risk"):
        risk = [row for row in fills if row["risk_order"]]
        assert sum(row["quantity"] for row in risk) == 74
        assert {row["instrument_id"] for row in risk} == {symbols[1]}
        assert all(row["quantity"] <= 10 for row in risk)
        assert account["cash"] == account["equity"] == 81026
        assert account["available_funds"] == 78 and account["frozen_funds"] == 0
        assert {row["instrument_id"]: row["quantity"] for row in account["positions"]} == {CODE: 450, symbols[1]: 376}
        assert all(row["reserved_quantity"] == 0 for row in account["positions"])
        orders = [json.loads(line) for line in (output / "orders.jsonl").read_text().splitlines()]
        switched = [row for row in orders if row["risk_order"] and row["instrument_id"] == CODE]
        assert len({row["order_id"] for row in switched}) == 1
        canceled = [row for row in switched if row["status"] == "Canceled"]
        assert len(canceled) == 1
        submitted = [row for row in orders if row["risk_order"] and row["instrument_id"] == symbols[1] and row["status"] == "Submitted"]
        assert len(submitted) == 1
        assert canceled[0]["event_sequence"] < submitted[0]["event_sequence"]
        assert submitted[0]["remaining_quantity"] == 74
        return
    if scenario == "protected_resize":
        seen = json.loads((output / "native-observations.json").read_text())["seen"]
        assert seen[0]["requested_quantity"] == 1000 and seen[0]["submitted_quantity"] == 662
        assert not (output / "intents.jsonl").read_text().strip()
        assert sum(row["quantity"] for row in fills) == 200
        assert all(row["price"] == 100 for row in fills)
        assert account["cash"] == account["equity"] == 117800
        assert account["margin"] == 21800 and account["frozen_funds"] == 0
        return
    if scenario == "risk":
        assert account["cash"] == account["equity"] == 81026
        assert account["margin"] == 80948
        assert account["available_funds"] == 78
        assert sum(row["quantity"] for row in fills if row["risk_order"]) == 74
        assert account["positions"][0]["quantity"] == 826
        return
    if scenario == "roll":
        assert account["cash"] == account["equity"] == 100051
        assert {row["instrument_id"] for row in account["positions"]} == {symbols[1]}
        assert account["positions"][0]["quantity"] == 3
        assert sum(row["quantity"] for row in fills if row["roll_plan_id"] and row["position_effect"] == "open") == 3
        transitions = [json.loads(line) for line in (output / "lifecycle.jsonl").read_text().splitlines()]
        credits = [row["detail"] for row in transitions if row["kind"] == "roll_fill"]
        assert all(row["opened"] <= row["closed"] <= 3 for row in credits)
        assert credits[-1]["opened"] == credits[-1]["closed"] == 3
        return
    if scenario == "night":
        seen = json.loads((output / "native-observations.json").read_text())["seen"]
        observed = {row["at"]: (row["rule_trading_date"], row["upper_limit"]) for row in seen}
        assert observed["2024-01-03T23:59:00"] == ("2024-01-04", 150)
        assert observed["2024-01-04T00:00:00"] == ("2024-01-04", 160)
    assert len(fills) == (3 if frequency == "1m" else 2)
    assert account["cash"] == (100117 if frequency == "1m" else 99998)
    assert account["equity"] == account["cash"]
    assert account["margin"] == (109 if frequency == "1m" else 218)
    assert account["frozen_funds"] == 0
    assert account["settled_through"] == {CODE: "2024-01-04"}
    assert len(account["positions"]) == (1 if frequency == "1m" else 2)
    assert all(row["bucket"] == "today" for row in fills[:2])
    if frequency == "1m":
        assert fills[-1]["bucket"] == "yesterday"
        assert fills[-1]["realized_pnl"] == 10
    else:
        assert [row["price"] for row in fills] == [107, 107]
