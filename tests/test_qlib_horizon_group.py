"""公开多期限入口冻结、独立执行、恢复及结果引用。"""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys
import subprocess

import pytest

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"


def save(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def entry(monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    modules = []
    for name in ("run", "horizons"):
        spec = importlib.util.spec_from_file_location(name, EXAMPLE / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        modules.append(module)
    return modules


@pytest.fixture
def stubbed(entry, monkeypatch):
    run, horizons = entry
    events = []
    holds = {}
    days = [day.isoformat() for day in horizons.synthetic.sessions()]

    def prepare(root, mode="model", input_config=None, sequence_step_len=None,
            model_candidates=None, horizon_sessions=1, window_config=None):
        root = Path(root)
        events.append(("prepare", horizon_sessions))
        assert not (root / "bundle-paths.json").exists()
        root.mkdir(parents=True)
        design = {"mode": mode, "calendar_sessions": days,
            "holdout_start": days[-23] + "T00:00:00+08:00", "horizon_sessions": horizon_sessions,
            "root_seed": horizons.SEED, **window_config,
            "calendar_id": "public_synthetic_weekdays", "calendar_source": "synthetic.py deterministic weekdays",
            "snapshot_scope": "public_synthetic_no_market_claim", "entities": list(horizons.synthetic.instruments())}
        if sequence_step_len is not None:
            design["sequence"] = {"step_len": sequence_step_len}
        save(root / "request.json", {"design": design, "model_parameters": {
            "candidate_jsons": [json.dumps(value) for value in model_candidates]}})
        bundle = {"package": str(root / "package"), "mode": mode}
        save(root / "bundle-paths.json", bundle)
        return bundle

    def execute(root, stage, mode="model", **kwargs):
        root = Path(root)
        horizon = int(root.name[1:])
        events.append((stage, horizon))
        if stage in {"run", "resume"}:
            root.joinpath("run").mkdir(exist_ok=True)
            holds[horizon] = holds.get(horizon, 0) + 1
            result = {"status": "result_finalized", "result_id": "R" + str(horizon), "result_directory": str(root / "results" / ("R" + str(horizon)))}
            save(root / "run-receipt.json", result)
            return result
        if stage == "verify":
            result = {"status": "pass", "result_reference": {"result_id": "R" + str(horizon)}}
            save(root / "verification.json", result)
            return result
        if stage == "report":
            root.joinpath("report.md").write_text("正式研究报告", encoding="utf-8")
        return {"status": "completed"}

    monkeypatch.setattr(horizons, "prepare", prepare)
    monkeypatch.setattr(horizons, "_execute_member", execute)
    return run, horizons, events, holds, execute


def test_group_freezes_every_member_before_running_and_preserves_candidates(tmp_path, stubbed):
    _, horizons, events, holds, _ = stubbed
    first, second = horizons.candidate(0.1), horizons.candidate(1.0)
    first["candidate_id"], second["candidate_id"] = "small", "large"
    candidates = [first, second]
    original = deepcopy(candidates)
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5], model_candidates=candidates,
        window_config={"expanding": False, "train_sessions": 32})
    frozen = read(root / horizons.GROUP_FILE)
    assert events == [("prepare", 1), ("prepare", 5)]
    assert frozen["plan"]["window_config"]["expanding"] is False
    assert frozen["plan"]["window_config"]["train_sessions"] == 32
    candidates[0]["candidate_id"] = "changed"
    result = horizons.execute_horizons(root, "all")
    assert holds == {1: 1, 5: 1}
    assert result["status"] == "pass"
    assert frozen["plan"]["model_candidates"][0]["candidate_id"] == original[0]["candidate_id"]
    assert result["members"] == read(root / horizons.GROUP_FILE)["members"]
    for item in result["members"]:
        child = root / ("h" + str(item["horizon_sessions"]))
        assert item["package"] == str(child / "package")
        assert item["result_directory"] == str(child / "results" / ("R" + str(item["horizon_sessions"])))
        assert item["verification_result"] == str(child / "verification.json")
        assert item["report"] == str(child / "report.md")
    assert "metrics" not in read(root / horizons.GROUP_FILE)


def test_group_resume_only_runs_unfinished_members_then_verifies(tmp_path, stubbed, monkeypatch):
    _, horizons, events, holds, execute = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    attempt = {"failed": False}

    def interrupted(child, stage, **kwargs):
        child = Path(child)
        if child.name == "h5" and stage == "run" and not attempt["failed"]:
            attempt["failed"] = True
            child.joinpath("run").mkdir()
            save(child / "run-receipt.json", {"status": "failed"})
            events.append(("interrupted", 5))
            raise RuntimeError("成员暂停")
        return execute(child, stage, **kwargs)

    monkeypatch.setattr(horizons, "_execute_member", interrupted)
    with pytest.raises(RuntimeError, match="成员暂停"):
        horizons.execute_horizons(root, "run")
    events.clear()
    result = horizons.execute_horizons(root, "resume")
    assert result["status"] == "pass"
    assert events == [("resume", 5), ("verify", 1), ("verify", 5), ("report", 1), ("report", 5)]
    assert holds == {1: 1, 5: 1}
    events.clear()
    assert horizons.execute_horizons(root, "resume")["status"] == "pass"
    assert events == []
    assert holds == {1: 1, 5: 1}


def test_group_resume_runs_unstarted_member_without_repreparing(tmp_path, stubbed):
    _, horizons, events, holds, execute = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    execute(root / "h1", "run")
    events.clear()
    horizons.execute_horizons(root, "resume")
    assert events[0] == ("run", 5)
    assert all(stage != "prepare" for stage, _ in events)
    assert holds == {1: 1, 5: 1}


def test_group_verify_failure_never_returns_pass_or_reports(tmp_path, stubbed, monkeypatch):
    _, horizons, events, _, execute = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    horizons.execute_horizons(root, "run")

    def verify(child, stage, **kwargs):
        if Path(child).name == "h5" and stage == "verify":
            save(Path(child) / "verification.json", {"status": "fail"})
            return {"status": "fail"}
        return execute(child, stage, **kwargs)

    monkeypatch.setattr(horizons, "_execute_member", verify)
    with pytest.raises(RuntimeError, match="独立验证未通过"):
        horizons.execute_horizons(root, "verify")
    assert all(stage != "report" for stage, _ in events)
    group = read(root / horizons.GROUP_FILE)
    assert "status" not in group
    assert group["members"][1]["verification_result"] == str(root / "h5" / "verification.json")
    with pytest.raises(RuntimeError, match="未全部通过"):
        horizons.execute_horizons(root, "report")


def test_group_rejects_reused_output_and_changed_member(tmp_path, stubbed):
    _, horizons, events, _, _ = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    with pytest.raises(ValueError, match="未用目录"):
        horizons.prepare_horizons(root, [1, 5])
    request = read(root / "h5" / "request.json")
    request["design"]["horizon_sessions"] = 10
    save(root / "h5" / "request.json", request)
    events.clear()
    with pytest.raises(ValueError, match="冻结组计划"):
        horizons.execute_horizons(root, "run")
    assert events == []


@pytest.mark.parametrize("horizons_list", [[1], [1, 1], [1, 0], [1, True]])
def test_group_requires_distinct_positive_horizons(tmp_path, stubbed, horizons_list):
    _, horizons, events, _, _ = stubbed
    with pytest.raises(ValueError):
        horizons.prepare_horizons(tmp_path / "group", horizons_list)
    assert events == []


def test_group_cli_reads_frozen_mode_and_rejects_changed_declarations(tmp_path, stubbed):
    run, horizons, events, _, _ = stubbed
    root = tmp_path / "group"
    run.main(["--output", str(root), "--horizons", "1", "5", "--mode", "development", "--stage", "prepare"])
    assert read(root / horizons.GROUP_FILE)["plan"]["mode"] == "development"
    events.clear()
    run.main(["--output", str(root), "--stage", "lint"])
    assert events == [("lint", 1), ("lint", 5)]
    for declarations in (["--mode", "model"], ["--horizons", "1", "10"],
            ["--sequence-step-len", "3"], ["--window-mode", "rolling"],
            ["--horizon-sessions", "5"], ["--input-config", "changed.json"],
            ["--reuse-failed-run-root", "other"], ["--require-reused-node", "model_holdout"]):
        with pytest.raises(SystemExit) as failure:
            run.main(["--output", str(root), "--stage", "resume", *declarations])
        assert failure.value.code == 2
    assert events == [("lint", 1), ("lint", 5)]


def test_single_cli_keeps_defaults_and_passes_explicit_window(tmp_path, entry, monkeypatch):
    run, _ = entry
    calls = []
    monkeypatch.setattr(run, "execute", lambda *args, **kwargs: calls.append((args, kwargs)) or {})
    run.main(["--output", str(tmp_path / "single"), "--stage", "prepare"])
    assert calls[0][0][2] == "model"
    assert calls[0][1] == {"horizon_sessions": 1, "window_config": None}
    run.main(["--output", str(tmp_path / "other"), "--stage", "prepare", "--horizon-sessions", "5",
        "--window-mode", "rolling", "--train-sessions", "32", "--validation-sessions", "12",
        "--test-sessions", "8", "--step-sessions", "4", "--embargo-sessions", "2"])
    assert calls[1][1] == {"horizon_sessions": 5, "window_config": {
        "expanding": False, "train_sessions": 32, "validation_sessions": 12,
        "test_sessions": 8, "step_sessions": 4, "embargo_sessions": 2}}
    with pytest.raises(SystemExit):
        run.main(["--output", str(tmp_path / "other"), "--horizon-sessions", "1", "--horizons", "1", "5"])


def test_group_run_also_skips_finalized_members(tmp_path, stubbed):
    _, horizons, events, holds, execute = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    execute(root / "h1", "run")
    events.clear()
    result = horizons.execute_horizons(root, "run")
    assert result["status"] == "result_finalized"
    assert events == [("run", 5)]
    assert holds == {1: 1, 5: 1}


def test_group_does_not_accept_verification_for_another_result(tmp_path, stubbed):
    _, horizons, _, _, _ = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    horizons.execute_horizons(root, "run")
    save(root / "h1" / "verification.json", {"status": "pass", "result_reference": {"result_id": "unrelated"}})
    with pytest.raises(ValueError, match="当前正式Result"):
        horizons.execute_horizons(root, "verify")
    with pytest.raises(ValueError, match="当前正式Result"):
        horizons.execute_horizons(root, "report")


def test_group_real_prepare_freezes_shared_source_and_sequence_candidates(tmp_path, entry):
    _, horizons = entry
    from prepare import gru_candidate, lstm_candidate
    root = tmp_path / "group"
    candidates = [gru_candidate(3), lstm_candidate(3)]
    horizons.prepare_horizons(root, [1, 5], sequence_step_len=3,
        model_candidates=candidates, window_config={"expanding": False, "step_sessions": 5})
    group = read(root / horizons.GROUP_FILE)
    assert group["plan"]["horizons"] == [1, 5]
    assert [item["model"]["class"] for item in group["plan"]["model_candidates"]] == ["GRU", "LSTM"]
    requests = [read(root / ("h" + str(value)) / "request.json") for value in [1, 5]]
    for horizon, request in zip([1, 5], requests):
        design = request["design"]
        assert design["horizon_sessions"] == horizon
        assert design["expanding"] is False
        assert design["step_sessions"] == 5
        assert design["sequence"]["step_len"] == 3
        assert design["calendar_sessions"] == group["plan"]["calendar_sessions"]
        assert design["holdout_start"] == group["plan"]["holdout_start"]
        assert {json.loads(value)["model"]["class"] for value in request["model_parameters"]["candidate_jsons"]} == {"GRU", "LSTM"}
    assert requests[0]["design"]["candidate_parameters_json"] == requests[1]["design"]["candidate_parameters_json"]
    assert all(member["result_directory"] is None for member in group["members"])
    assert not (root / "h1" / "run").exists()
    assert not (root / "h5" / "run").exists()


@pytest.mark.parametrize("stage", ["lint", "admit", "run", "resume", "verify", "report"])
def test_member_stage_uses_current_python_and_explicit_source_roots(tmp_path, entry, monkeypatch, stage):
    _, horizons = entry
    import research_pipeline
    root = tmp_path / "h5"
    receipt_stage = "run" if stage == "resume" else stage
    receipt = {"status": "member_receipt", "stage": receipt_stage}
    calls = []

    def launch(command, **kwargs):
        calls.append((command, kwargs))
        save(root / (receipt_stage + "-receipt.json"), receipt)
        return subprocess.CompletedProcess(command, 0, stdout="子进程标准输出无需解析")

    monkeypatch.setattr(horizons.subprocess, "run", launch)
    assert horizons._execute_member(root, stage, mode="model") == receipt
    command, options = calls[0]
    assert command[:3] == [sys.executable, "-B", "-c"]
    assert command[3] == horizons._MEMBER_BOOTSTRAP
    assert "sys.path[:0] = [sys.argv[1], sys.argv[2]]" in command[3]
    assert "from run import execute" in command[3]
    assert command[4:] == [str(Path(research_pipeline.__file__).resolve().parent.parent),
        str(EXAMPLE), str(root.resolve()), stage, "model"]
    assert options == {"check": True}


def test_member_subprocess_failure_propagates(tmp_path, entry, monkeypatch):
    _, horizons = entry

    def launch(command, **kwargs):
        raise subprocess.CalledProcessError(19, command)

    monkeypatch.setattr(horizons.subprocess, "run", launch)
    with pytest.raises(subprocess.CalledProcessError) as failure:
        horizons._execute_member(tmp_path / "h5", "resume", mode="model")
    assert failure.value.returncode == 19


def test_group_subprocess_failure_keeps_finalized_member_for_resume(tmp_path, stubbed, monkeypatch):
    _, horizons, events, holds, execute = stubbed
    root = tmp_path / "group"
    horizons.prepare_horizons(root, [1, 5])
    interrupted = {"failed": False}

    def member(child, stage, **kwargs):
        child = Path(child)
        if child.name == "h5" and stage == "run" and not interrupted["failed"]:
            interrupted["failed"] = True
            child.joinpath("run").mkdir()
            save(child / "run-receipt.json", {"status": "failed"})
            raise subprocess.CalledProcessError(23, [sys.executable, "-B", "-c", "execute"])
        return execute(child, stage, **kwargs)

    monkeypatch.setattr(horizons, "_execute_member", member)
    with pytest.raises(subprocess.CalledProcessError):
        horizons.execute_horizons(root, "run")
    group = read(root / horizons.GROUP_FILE)
    assert group["members"][0]["result_directory"] == str(root / "h1" / "results" / "R1")
    assert group["members"][1]["result_directory"] is None
    assert not (root / "h5" / "report.md").exists()
    events.clear()
    assert horizons.execute_horizons(root, "resume")["status"] == "pass"
    assert events == [("resume", 5), ("verify", 1), ("verify", 5), ("report", 1), ("report", 5)]
    assert holds == {1: 1, 5: 1}
