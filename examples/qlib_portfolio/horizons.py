"""冻结多期限声明，按成员目录委托现有单研究入口。"""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

from prepare import prepare, candidate, gru_candidate, SEED
import synthetic

GROUP_FILE = "horizon-group.json"
_MEMBER_BOOTSTRAP = """import sys
sys.path[:0] = [sys.argv[1], sys.argv[2]]
from run import execute
execute(sys.argv[3], sys.argv[4], mode=sys.argv[5])
"""
WINDOW_DEFAULTS = {"train_sessions": 30, "validation_sessions": 10, "test_sessions": 10,
    "step_sessions": 10, "embargo_sessions": 1, "expanding": True}


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _save(path, payload):
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _execute_member(root, stage, mode):
    """每阶段独立进程释放训练内存，回执仍由单研究入口保存。"""
    import research_pipeline
    core_root = Path(research_pipeline.__file__).resolve().parent.parent
    example_root = Path(__file__).resolve().parent
    root = Path(root).resolve()
    subprocess.run([sys.executable, "-B", "-c", _MEMBER_BOOTSTRAP,
        str(core_root), str(example_root), str(root), stage, mode], check=True)
    receipt_stage = "run" if stage == "resume" else stage
    return _read(root / (receipt_stage + "-receipt.json"))


def _require_member_declaration(member, plan):
    request = _read(Path(member["root"]) / "request.json")
    design = request["design"]
    source = plan["input_source"].get("configuration", plan["input_source"])
    expected = {"mode": plan["mode"], "horizon_sessions": member["horizon_sessions"],
        "calendar_sessions": plan["calendar_sessions"], "holdout_start": plan["holdout_start"],
        "root_seed": SEED, **plan["window_config"],
        **{key: source[key] for key in ("calendar_id", "calendar_source", "snapshot_scope", "entities")}}
    if any(design.get(key) != value for key, value in expected.items()):
        raise ValueError("成员研究声明与冻结组计划不一致: h" + str(member["horizon_sessions"]))
    if design.get("sequence", {}).get("step_len") != plan["sequence_step_len"]:
        raise ValueError("成员序列窗口与冻结组计划不一致")
    candidates = [json.loads(value) for value in request["model_parameters"]["candidate_jsons"]]
    if candidates != plan["model_candidates"]:
        raise ValueError("成员候选全集与冻结组计划不一致")


def prepare_horizons(root, horizons, mode="model", input_config=None, sequence_step_len=None,
        model_candidates=None, window_config=None):
    """先冻结所有成员，组索引只保存声明及正式工件引用。"""
    root = Path(root).resolve()
    horizons = list(horizons)
    if len(horizons) < 2 or any(type(value) is not int or value < 1 for value in horizons):
        raise ValueError("多期限组至少需要两个正整数期限")
    if len(set(horizons)) != len(horizons):
        raise ValueError("多期限组的期限不能重复")
    if mode not in {"development", "model"}:
        raise ValueError("多期限组支持development和model模式")
    if root.exists() and any(root.iterdir()):
        raise ValueError("多期限组必须使用未用目录")
    windows = deepcopy(WINDOW_DEFAULTS)
    if window_config is not None:
        if not isinstance(window_config, dict) or set(window_config) - set(windows):
            raise ValueError("window_config只接受六项已支持切分参数")
        windows.update(deepcopy(window_config))
    from research_pipeline.research.modeling.walk_forward import normalize_model_candidates
    candidates = ([candidate(0.1), candidate(1.0)] if sequence_step_len is None
        else [candidate(0.1), gru_candidate(sequence_step_len)])
    candidates = normalize_model_candidates(deepcopy(candidates if model_candidates is None else model_candidates))
    if input_config is not None:
        from input_config import load_input_config
        configuration, _, _ = load_input_config(input_config, mode=mode)
        days = list(configuration["calendar_sessions"])
        source = {"kind": "archived", "configuration": deepcopy(configuration)}
    else:
        days = [day.isoformat() for day in synthetic.sessions()]
        source = {"kind": "synthetic", "generator": "synthetic.py",
            "calendar_id": "public_synthetic_weekdays", "calendar_source": "synthetic.py deterministic weekdays",
            "entities": list(synthetic.instruments()), "snapshot_scope": "public_synthetic_no_market_claim"}
    root.mkdir(parents=True, exist_ok=True)
    frozen_input = None
    if input_config is not None:
        frozen_input = root / "input-config.json"
        _save(frozen_input, configuration)
    plan = {"horizons": horizons, "mode": mode, "input_source": source,
        "input_config": str(frozen_input) if frozen_input is not None else None,
        "calendar_sessions": days, "holdout_start": days[-23] + "T00:00:00+08:00",
        "sequence_step_len": sequence_step_len, "model_candidates": candidates, "window_config": windows}
    members = []
    for horizon in horizons:
        child = root / ("h" + str(horizon))
        bundle = prepare(child, mode=mode, input_config=plan["input_config"],
            sequence_step_len=sequence_step_len, model_candidates=deepcopy(candidates),
            horizon_sessions=horizon, window_config=deepcopy(windows))
        member = {"horizon_sessions": horizon, "root": str(child), "package": bundle["package"],
            "bundle_paths": str(child / "bundle-paths.json"), "result_directory": None,
            "verification_result": None, "report": None}
        _require_member_declaration(member, plan)
        members.append(member)
    group = {"schema": "qlib-horizon-group-v1", "plan": plan, "members": members}
    _save(root / GROUP_FILE, group)
    return {"stage": "prepare", "status": "prepared", "group_manifest": str(root / GROUP_FILE),
        "mode": mode, "horizons": horizons, "members": deepcopy(members)}


def _run_receipt(member):
    path = Path(member["root"]) / "run-receipt.json"
    return _read(path) if path.exists() else {}


def _verification_pass(member):
    path = Path(member["root"]) / "verification.json"
    if not path.exists():
        return False
    verification = _read(path)
    if verification.get("status") != "pass":
        return False
    receipt = _run_receipt(member)
    if (receipt.get("status") != "result_finalized"
            or verification["result_reference"]["result_id"] != receipt["result_id"]):
        raise ValueError("成员VerificationResult与当前正式Result不一致")
    return True


def _record_references(member):
    root = Path(member["root"])
    receipt = _run_receipt(member)
    if receipt.get("status") == "result_finalized":
        member["result_directory"] = receipt["result_directory"]
    verification = root / "verification.json"
    if verification.exists():
        member["verification_result"] = str(verification)
    report = root / "report.md"
    if report.exists():
        member["report"] = str(report)


def execute_horizons(root, stage):
    """每个期限独立执行；resume跳过已封存Result并补齐验证及报告。"""
    root = Path(root).resolve()
    path = root / GROUP_FILE
    group = _read(path)
    if group["schema"] != "qlib-horizon-group-v1":
        raise ValueError("不支持的多期限组索引")
    if stage not in {"lint", "admit", "run", "resume", "verify", "report", "all"}:
        raise ValueError("不支持的组阶段")
    plan = group["plan"]
    for member in group["members"]:
        _require_member_declaration(member, plan)
    stages = (("lint", "admit", "run", "verify", "report") if stage == "all"
        else ("resume", "verify", "report") if stage == "resume" else (stage,))
    for current in stages:
        if current == "report":
            for member in group["members"]:
                if not _verification_pass(member):
                    raise RuntimeError("组成员独立验证未全部通过，请先执行verify")
        for member in group["members"]:
            child = Path(member["root"])
            receipt = _run_receipt(member)
            finalized = receipt.get("status") == "result_finalized"
            if current in {"run", "resume"} and finalized:
                _record_references(member)
                _save(path, group)
                continue
            if current == "run" and (child / "run").exists():
                raise ValueError("成员已有运行，请使用组resume继续原研究")
            if current == "verify" and _verification_pass(member):
                _record_references(member)
                _save(path, group)
                continue
            if current == "report" and (child / "report.md").exists():
                _record_references(member)
                _save(path, group)
                continue
            operation = ("resume" if (child / "run").exists() else "run") if current == "resume" else current
            try:
                result = _execute_member(child, operation, mode=plan["mode"])
                if operation in {"run", "resume"} and result.get("status") != "result_finalized":
                    raise RuntimeError("成员研究尚未封存，请恢复原目录")
                if operation == "verify" and (result.get("status") != "pass" or not _verification_pass(member)):
                    raise RuntimeError("组成员独立验证未通过")
            finally:
                _record_references(member)
                _save(path, group)
    status = "pass" if stage in {"verify", "resume", "all"} else "result_finalized" if stage == "run" else "completed"
    return {"stage": stage, "status": status, "group_manifest": str(path), "mode": plan["mode"],
        "horizons": list(plan["horizons"]), "members": deepcopy(group["members"])}
