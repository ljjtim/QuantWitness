"""公开 Qlib 系列冻结、继续入口和正式结果接线。"""
from copy import deepcopy
import json
from pathlib import Path

import pytest
import yaml
import sys
sys.path.insert(0, str(Path(__file__).parent))
from test_lean_runtime_identity import installation as lean_installation

from research_pipeline.platform import canonical_json, typed_canonical_hash

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"


@pytest.fixture(autouse=True)
def database_boundary(monkeypatch):
    import duckdb
    import sqlite3

    def reject(*args, **kwargs):
        pytest.fail("系列示例测试不得连接任何数据库")

    monkeypatch.setattr(duckdb, "connect", reject)
    monkeypatch.setattr(sqlite3, "connect", reject)
    monkeypatch.syspath_prepend(str(EXAMPLE))


@pytest.fixture
def series(tmp_path):
    return {"series_id": "etf-question", "root": str(tmp_path / "persistent-series"),
        "purpose": "ETF价格预测的最终确认", "legacy_ledger_roots": [str(tmp_path / "old-ledger")]}


def cli_series(series):
    arguments = ["--research-series-id", series["series_id"],
        "--research-series-root", series["root"], "--research-series-purpose", series["purpose"]]
    for root in series["legacy_ledger_roots"]:
        arguments.extend(["--research-series-legacy-root", root])
    return arguments


@pytest.mark.parametrize("mode", ["model", "portfolio"])
def test_final_requires_explicit_series_before_preparing_files(tmp_path, mode):
    from prepare import prepare
    root = tmp_path / "study"
    with pytest.raises(ValueError, match="最终评价必须"):
        prepare(root, mode=mode)
    assert not root.exists()


@pytest.mark.parametrize("change", [
    {"series_id": ""}, {"series_id": "../study"}, {"series_id": "study/name"},
    {"purpose": " "}, {"root": "relative"}, {"legacy_ledger_roots": ["relative"]},
    {"legacy_ledger_roots": "not-a-list"},
])
def test_series_rejects_incomplete_or_nonpersistent_declarations(series, change):
    from prepare import normalize_research_series
    with pytest.raises(ValueError):
        normalize_research_series({**series, **change}, required=True)


def test_design_and_holdout_freeze_shared_series_across_variants(tmp_path, series, lean_installation):
    from prepare import prepare, candidate
    identities = []
    original = deepcopy(series)
    for mode, horizon in (("model", 1), ("model", 5), ("portfolio", 1)):
        root = tmp_path / (mode + str(horizon))
        prepare(root, mode=mode, horizon_sessions=horizon, research_series=series,
            model_candidates=[candidate(0.1 if horizon == 1 else 1.0)],
            lean_home=lean_installation if mode == 'portfolio' else None)
        request = json.loads((root / "request.json").read_text(encoding="utf-8"))
        specification = yaml.safe_load((root / "package/spec/research.yaml").read_text(encoding="utf-8"))
        nodes = specification["graph"]["nodes"]
        holdout = next(node for node in nodes if node["node_id"] == "model_holdout")
        tables = {item["table_id"]: item for item in specification["result"]["tables"]}
        assert {"features", "labels", "samples", "fit_audit", "selection", "holdout_receipt"} <= set(tables)
        assert tables["metrics"]["source_node_id"] == "summary"
        assert tables["metrics"]["source_port"] == "result"
        assert request["research_series"] == original
        assert request["design"]["research_series"] == original
        assert holdout["parameters"]["research_series_json"] == canonical_json(original)
        identity = request["model_parameters"]["research_identity_hash"]
        assert identity == typed_canonical_hash(request["design"])
        assert holdout["parameters"]["research_identity_hash"] == identity
        identities.append(identity)
    assert len(set(identities)) == 3
    assert series == original
    assert not Path(series["root"]).exists()


def test_development_needs_no_series_or_holdout(tmp_path):
    from prepare import prepare
    prepare(tmp_path, mode="development")
    request = json.loads((tmp_path / "request.json").read_text(encoding="utf-8"))
    nodes = yaml.safe_load((tmp_path / "package/spec/research.yaml").read_text(encoding="utf-8"))["graph"]["nodes"]
    assert request["research_series"] is None
    assert "research_series" not in request["design"]
    assert not any(node["node_id"] == "model_holdout" for node in nodes)
    specification = yaml.safe_load((tmp_path / "package/spec/research.yaml").read_text(encoding="utf-8"))
    tables = {item["table_id"]: item for item in specification["result"]["tables"]}
    assert {"features", "labels", "samples", "fit_audit", "validation_predictions"} <= set(tables)
    assert {"selection", "holdout_predictions", "holdout_receipt"}.isdisjoint(tables)
    assert tables["metrics"]["source_node_id"] == "summary"


def test_single_cli_and_prepare_execute_forward_complete_series(tmp_path, monkeypatch, series):
    import run
    calls = []
    series["legacy_ledger_roots"].append(str(tmp_path / "another-ledger"))
    with monkeypatch.context() as patch:
        patch.setattr(run, "execute", lambda *args, **kwargs: calls.append(kwargs) or {})
        run.main(["--output", str(tmp_path / "study"), "--stage", "prepare", *cli_series(series)])
    from prepare import normalize_research_series
    assert calls[0]["research_series"] == normalize_research_series(series)
    monkeypatch.setattr(run, "prepare", lambda *args, **kwargs: calls.append(kwargs) or {})
    run.execute(tmp_path / "direct", "prepare", research_series=series)
    assert calls[-1]["research_series"] == series


def test_single_final_cli_rejects_missing_series_even_for_factor_subprocess(tmp_path, monkeypatch):
    import run
    monkeypatch.setattr(run, "execute", lambda *args, **kwargs: pytest.fail("缺系列不能执行"))
    monkeypatch.setattr(run.subprocess, "run", lambda *args, **kwargs: pytest.fail("缺系列不能启动子进程"))
    for options in ([], ["--factor-suite", "volume_price_v1"]):
        with pytest.raises(SystemExit) as failure:
            run.main(["--output", str(tmp_path), *options])
        assert failure.value.code == 2


@pytest.mark.parametrize("stage", ["lint", "admit", "run", "resume", "report"])
def test_continuation_rejects_new_series_in_cli_and_python(tmp_path, monkeypatch, series, stage):
    import run
    with pytest.raises(ValueError, match="冻结request"):
        run.execute(tmp_path, stage, research_series=series)
    monkeypatch.setattr(run, "execute", lambda *args, **kwargs: pytest.fail("继续阶段不能换系列"))
    with pytest.raises(SystemExit) as failure:
        run.main(["--output", str(tmp_path), "--stage", stage, *cli_series(series)])
    assert failure.value.code == 2
    assert not (tmp_path / "tmp").exists()


def test_factor_prepare_subprocess_preserves_legacy_roots(tmp_path, monkeypatch, series):
    import run
    calls, stages = [], []
    monkeypatch.setattr(run.subprocess, "run", lambda args, **kwargs: calls.append(args))
    monkeypatch.setattr(run, "execute", lambda root, stage, *args, **kwargs: stages.append((stage, kwargs)) or {})
    series["legacy_ledger_roots"].append(str(tmp_path / "second-ledger"))
    run.main(["--output", str(tmp_path / "factor"), "--factor-suite", "volume_price_v1", *cli_series(series)])
    arguments = calls[0]
    assert arguments[arguments.index("--research-series-id") + 1] == series["series_id"]
    legacy = [arguments[index + 1] for index, value in enumerate(arguments) if value == "--research-series-legacy-root"]
    assert legacy == sorted(series["legacy_ledger_roots"])
    assert all(options["research_series"] is None for _, options in stages)
    assert all("--research-series-id" not in args for args in calls[1:])


def test_group_final_rejects_before_preparing_any_member(tmp_path, monkeypatch, series):
    import horizons
    monkeypatch.setattr(horizons, "prepare", lambda *args, **kwargs: pytest.fail("多期限final不应准备成员"))
    root = tmp_path / "group"
    with pytest.raises(ValueError, match="development比较"):
        horizons.prepare_horizons(root, [1, 5], mode="model", research_series=series)
    assert not root.exists()


def test_development_group_freezes_unmodified_series_and_rejects_member_change(tmp_path, series):
    import horizons
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5], mode="development", research_series=series)
    group = json.loads((root / horizons.GROUP_FILE).read_text(encoding="utf-8"))
    assert group["plan"]["research_series"] == series
    for member in group["members"]:
        path = Path(member["root"]) / "request.json"
        request = json.loads(path.read_text(encoding="utf-8"))
        assert request["design"]["research_series"] == series
    request["design"]["research_series"]["series_id"] = "changed-question"
    path.write_text(json.dumps(request), encoding="utf-8")
    with pytest.raises(ValueError, match="研究系列"):
        horizons.execute_horizons(root, "lint")


def test_readme_commands_freeze_explicit_series_for_final():
    import shlex
    import run
    from prepare import research_series_from_arguments
    parser = run.argument_parser()
    commands = [line for line in (EXAMPLE / "README.md").read_text(encoding="utf-8").splitlines()
        if line.startswith("python examples/qlib_portfolio/run.py ")]
    assert commands
    for command in commands:
        args = parser.parse_args(shlex.split(command)[2:])
        series = research_series_from_arguments(args, parser)
        if args.stage in {"all", "prepare"} and args.mode in {"model", "portfolio"}:
            assert series is not None
        if args.horizons is not None:
            assert args.mode == "development"


def test_series_freezes_resolved_windows_paths_and_unique_legacy_roots(tmp_path, series):
    from prepare import normalize_research_series
    older = str(tmp_path / "z-ledger")
    earlier = str(tmp_path / "a-ledger")
    value = {**series, "root": str(tmp_path / "parent" / ".." / "persistent-series"),
        "legacy_ledger_roots": [older.replace("\\", "/"), earlier, older]}
    normalized = normalize_research_series(value, required=True)
    assert normalized["root"] == str((tmp_path / "persistent-series").resolve())
    assert normalized["legacy_ledger_roots"] == sorted({str(Path(older).resolve()), str(Path(earlier).resolve())})
    assert normalize_research_series(normalized, required=True) == normalized


def test_normal_all_passes_series_only_to_prepare(tmp_path, monkeypatch, series):
    import run
    calls = []
    monkeypatch.setattr(run, "execute", lambda root, stage, *args, **kwargs: calls.append((stage, kwargs)) or {})
    run.main(["--output", str(tmp_path / "study"), *cli_series(series)])
    assert [stage for stage, _ in calls] == ["prepare", "lint", "admit", "run", "report"]
    assert calls[0][1]["research_series"] == series
    assert all(options["research_series"] is None for _, options in calls[1:])


def test_group_cli_passes_series_without_horizon_suffix(tmp_path, monkeypatch, series):
    import horizons
    import run
    calls = []
    monkeypatch.setattr(horizons, "prepare_horizons", lambda root, horizons, **kwargs: calls.append(kwargs) or {})
    run.main(["--output", str(tmp_path / "group"), "--stage", "prepare", "--mode", "development",
        "--horizons", "1", "5", *cli_series(series)])
    assert calls[0]["research_series"] == series
