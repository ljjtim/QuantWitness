"""期货合约选择与可见性检查。"""

from pathlib import Path
import runpy


ROOT = Path(__file__).parent
evaluate = runpy.run_path(ROOT / "extension/source/operator.py")["evaluate"]
contracts = runpy.run_path(ROOT / "synthetic.py")["contracts"]


def _study(sample):
    return evaluate(
        list(sample), decision_at="2024-01-05T09:00:00+08:00",
        minimum_days_to_expiry=20, minimum_volume=1000,
    )


def test_selects_nearest_two_visible_liquid_contracts() -> None:
    result = _study(contracts())
    assert result["selected_contract"] == "SYN2402"
    assert result["deferred_contract"] == "SYN2403"
    assert result["signal"] == "carry_short"
    assert abs(result["annualized_slope"] - (0.03 * 365.0 / 29.0)) < 1e-12


def test_future_settlement_is_excluded() -> None:
    changed = [dict(item) for item in contracts()]
    changed[1]["available_at"] = "2024-01-05T10:00:00+08:00"
    try:
        _study(changed)
    except ValueError as exc:
        assert "至少需要两个" in str(exc)
    else:
        raise AssertionError("未来才可见的次月结算价应被排除")
