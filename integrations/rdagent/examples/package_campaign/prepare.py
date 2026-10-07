"""生成无需数据库与模型账户的两轮正式参数研究。"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys


def build(output, repo=None):
    root = Path(output).resolve()
    rp = Path(repo).resolve() if repo else Path(__file__).resolve().parents[4]
    if not (rp / "src/research_pipeline").is_dir():
        rp = rp / "research_pipeline"
    sys.path[:0] = [str(rp / "src"), str(rp / "integrations/rdagent/src")]
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.worker import _command
    from quantwitness_rdagent.package_campaign import validate_package_campaign
    if root.exists():
        raise ValueError("输出目录必须尚不存在")
    root.mkdir(parents=True)
    example = Path(__file__).resolve().parent.parent / "volume_concentration/prepare.py"
    spec = importlib.util.spec_from_file_location("volume_teaching_prepare", example)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.build(root / "input", rp, sys.executable, str(rp), str(root / "input"))
    template = json.loads((root / "input/request-template.json").read_text(encoding="utf-8"))
    binding = template["runtime_binding"]
    built = _command(["operator", "build", "--spec", binding["operator_spec"], "--source", template["editable_source_root"],
                      "--output", str(root / "operator-bundles"), "--format", "json"])
    payload = make_request(root, template, [built["bundle_path"]])
    validate_package_campaign(payload)
    write_json(root / "request.json", payload)
    return {"status": "prepared", "request": str(root / "request.json"), "database_used": False, "model_calls": 0}


def make_request(root, template, extension_bundles):
    """将公开教学输入绑定到固定共同日期的参数研究。"""
    binding = template["runtime_binding"]
    payload = {"research_kind": "package", "campaign_id": "synthetic_window_research", "session_root": str(root / "session"),
        "source": {"kind": "research_package", "path": template["base_package"],
            "source_archive_root": template["source_archive_root"], "input_snapshot_manifest": template["input_snapshot_manifest"],
            "catalog_lock": binding["catalog_lock"], "verifier_bundle": template["reference_bundle"],
            "extension_bundles": extension_bundles, "runtime_options": binding["runtime_options"],
            "verification_process_slots": 3 if sys.platform == "win32" else 2},
        "development": {"start": "2025-01-08", "end": "2025-01-10", "as_of": "2025-01-13T16:00:00+08:00"},
        "objective": {"table_id": "daily", "schema_id": "project.volume-concentration.daily.v1", "value_column": "rolling_value",
            "date_column": "session", "availability_column": "available_at", "stage_column": None,
            "filters": {"rolling_status": "computed"}, "reduction": "mean", "direction": "minimize"},
        "candidates": [{"id": "window_" + str(window), "parameter_overrides": [
            {"node_id": "formula", "parameter_name": "window_sessions", "value": window}]} for window in (3, 2)],
        "baseline_id": "window_3", "budget": {"rounds": 2, "evaluations": 2, "model_calls": 0, "output_tokens": 0,
            "max_output_tokens_per_call": 0, "max_rows": 100, "memory_bytes": 16777216},
        "stop": {"target_value": None, "min_improvement": 0, "patience": 2}, "proposer": {"mode": "fixed_policy"}}
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repo")
    args = parser.parse_args()
    print(json.dumps(build(args.output, args.repo), ensure_ascii=False))


if __name__ == "__main__":
    main()
