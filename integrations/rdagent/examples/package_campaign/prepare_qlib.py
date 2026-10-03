"""生成只比较validation的Qlib正式模型研究循环。"""
import argparse
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys


def build(output):
    root = Path(output).resolve()
    if root.exists():
        raise ValueError("输出目录必须尚不存在")
    example = Path(__file__).resolve().parents[4] / "examples/qlib_portfolio"
    sys.path.insert(0, str(example))
    spec = importlib.util.spec_from_file_location("qlib_campaign_prepare", example / "prepare.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    from research_pipeline.platform import canonical_json, typed_canonical_hash
    from research_pipeline.research.validation import build_search_manifest
    from quantwitness_rdagent.contracts import write_json
    from quantwitness_rdagent.package_campaign import validate_package_campaign
    root.mkdir(parents=True)
    bundle = module.prepare(root / "input", "development")
    facts = json.loads((root / "input/request.json").read_text(encoding="utf-8"))
    candidates = []
    for alpha in (0.1, 1.0):
        model = module.candidate(alpha)
        design = deepcopy(facts["design"])
        search = build_search_manifest(search_id="public.qlib.etf", candidates=[model], method="grid",
            max_trials=1, max_parallel=1, stopping_condition="complete_declared_candidate_universe",
            objective="neg_mean_squared_error", direction="maximize",
            frozen_at=module.stamp(design["calendar_sessions"][0], "09:00:00"))
        design["candidate_ids"] = [item.candidate_id for item in search.candidates]
        design["candidate_parameters_json"] = canonical_json({item.candidate_id: dict(item.parameters) for item in search.candidates})
        identity = typed_canonical_hash(design)
        overrides = [
            {"node_id": "model_fit", "parameter_name": "candidate_jsons", "value": [canonical_json(model)]},
            {"node_id": "model_fit", "parameter_name": "research_identity_hash", "value": identity},
        ]
        for node in ("feature", "label", "summary"):
            overrides.append({"node_id": node, "parameter_name": "design", "value": design})
            if node != "summary":
                overrides.append({"node_id": node, "parameter_name": "lineage_ref", "value": identity})
        candidates.append({"id": "ridge_" + str(alpha).replace(".", "_"), "parameter_overrides": overrides})
    source_archive = root / "source-archive"
    source_archive.mkdir()
    payload = {
        "research_kind": "package", "campaign_id": "synthetic_qlib_research", "session_root": str(root / "session"),
        "source": {"kind": "research_package", "path": bundle["package"], "source_archive_root": str(source_archive),
            "input_snapshot_manifest": bundle["input_snapshot_manifest"], "catalog_lock": bundle["catalog_lock"],
            "verifier_bundle": bundle["verifier"], "extension_bundles": bundle["extensions"],
            "runtime_options": {"workers": 1}, "verification_process_slots": 3 if sys.platform == "win32" else 2},
        "development": {"start": facts["design"]["research_sessions"][0], "end": facts["design"]["calendar_sessions"][78], "as_of": bundle["fixed_clock"]},
        "objective": {"table_id": "metrics", "schema_id": "project.qlib_demo.metrics.v1", "value_column": "value",
            "date_column": "session", "availability_column": "available_at", "stage_column": "stage",
            "filters": {}, "reduction": "mean", "direction": "minimize"},
        "candidates": candidates, "baseline_id": candidates[0]["id"],
        "budget": {"rounds": 2, "evaluations": 2, "model_calls": 0, "output_tokens": 0,
            "max_output_tokens_per_call": 0, "max_rows": 10, "memory_bytes": 16777216},
        "stop": {"target_value": None, "min_improvement": 0, "patience": 2}, "proposer": {"mode": "fixed_policy"},
    }
    validate_package_campaign(payload)
    write_json(root / "request.json", payload)
    return {"status": "prepared", "request": str(root / "request.json"), "database_used": False, "model_calls": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    print(json.dumps(build(parser.parse_args().output), ensure_ascii=False))
