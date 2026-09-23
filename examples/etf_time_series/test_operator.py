"""ETF 时间序列的手算与未来数据隔离。"""

from pathlib import Path
import runpy


ROOT = Path(__file__).parent
evaluate = runpy.run_path(ROOT / "extension/source/operator.py")["evaluate"]
rows = runpy.run_path(ROOT / "synthetic.py")["rows"]


def _study(sample):
    return evaluate(
        list(sample), decision_at="2024-01-05T09:30:00+08:00",
        history_sessions=("2024-01-02", "2024-01-03", "2024-01-04"),
        outcome_session="2024-01-05",
    )


def test_each_instrument_has_own_warmup_and_signal() -> None:
    result = _study(rows())
    assert [(item["instrument"], item["signal"]) for item in result] == [
        ("SYN.ETF.A", 1), ("SYN.ETF.B", 0),
    ]
    assert abs(result[0]["momentum"] - 0.2) < 1e-12
    assert abs(result[0]["next_session_intraday_return"] - 0.05) < 1e-12


def test_future_known_close_cannot_be_warmup() -> None:
    modified = [dict(item) for item in rows()]
    for item in modified:
        if item["instrument"] == "SYN.ETF.A" and item["session"] == "2024-01-04":
            item["available_at"] = "2024-01-08T09:30:00+08:00"
    assert [item["instrument"] for item in _study(modified)] == ["SYN.ETF.B"]
