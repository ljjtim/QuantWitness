"""项目事件修订与重叠控制。"""

from pathlib import Path
import runpy


ROOT = Path(__file__).parent
evaluate = runpy.run_path(ROOT / "extension/source/operator.py")["evaluate"]
fixture = runpy.run_path(ROOT / "synthetic.py")
events = fixture["events"]
prices = fixture["prices"]


def _study(sample):
    return evaluate(
        list(sample), list(prices()),
        fixed_clock="2024-01-08T10:00:00+08:00", minimum_gap_days=2,
    )


def test_future_revision_excluded_and_overlap_resolved() -> None:
    result = _study(events())
    assert [item["event_id"] for item in result] == ["E1", "E3"]
    assert result[0]["revision"] == 1
    assert result[0]["surprise"] == 0.2
    assert abs(result[0]["window_return"] - 0.05) < 1e-12


def test_post_decision_event_does_not_change_frozen_selection() -> None:
    future = {"event_id": "E0", "instrument": "SYN.A", "event_day": "2024-01-03",
              "revision": 1, "decision_at": "2024-01-04T09:30:00+08:00",
              "available_at": "2024-01-05T10:00:00+08:00", "surprise": 2.0}
    assert _study((*events(), future)) == _study(events())
