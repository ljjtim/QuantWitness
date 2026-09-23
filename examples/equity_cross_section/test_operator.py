"""横截面项目的手算与防前视检查。"""

from __future__ import annotations

from pathlib import Path
import runpy


ROOT = Path(__file__).parent
evaluate = runpy.run_path(ROOT / "extension/source/operator.py")["evaluate"]
rows = runpy.run_path(ROOT / "synthetic.py")["rows"]


DECISION = "2024-01-05T09:30:00+08:00"


def _study(sample):
    return evaluate(
        list(sample), decision_at=DECISION,
        observation_sessions=("2024-01-03", "2024-01-04"),
        outcome_session="2024-01-05",
    )


def test_ranking_uses_only_known_sessions() -> None:
    result = _study(rows())
    assert [item["instrument"] for item in result] == ["SYN.A", "SYN.D", "SYN.B", "SYN.C"]
    assert abs(result[0]["signal_return"] - (12.0 / 11.0 - 1)) < 1e-12
    assert abs(result[0]["outcome_return"] - (12.3 / 12.0 - 1)) < 1e-12


def test_future_revision_cannot_enter_ranking() -> None:
    changed = [dict(item) for item in rows()]
    for item in changed:
        if item["instrument"] == "SYN.A" and item["session"] == "2024-01-04":
            item["available_at"] = "2024-01-08T09:30:00+08:00"
    assert [item["instrument"] for item in _study(changed)] == ["SYN.D", "SYN.B", "SYN.C"]


def test_future_outcome_cannot_be_visible_at_decision() -> None:
    changed = [dict(item) for item in rows()]
    for item in changed:
        if item["instrument"] == "SYN.D" and item["session"] == "2024-01-05":
            item["available_at"] = DECISION
    try:
        _study(changed)
    except ValueError as exc:
        assert "结果记录" in str(exc)
    else:
        raise AssertionError("决策时已知的结果数据应被拒绝")
