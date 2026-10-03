"""将已确认公式和显式运行配置组装为请求，不启动研究。"""
import json
from pathlib import Path

from .contracts import FrozenRequest
from .formula_spec import render_spec
from .source_materials import load_research_package, verify_source_snapshot


def _verify_target_source(request, materials):
    package = load_research_package(request._local_path(request.payload["base_package"]))
    source = next((item for item in package.sources if item.source_id == materials["source_id"]), None)
    if source is None:
        raise ValueError("request.source_missing_from_target_package")
    manifest = verify_source_snapshot(
        source, request._local_path(request.payload["source_archive_root"])
    )
    expected = (materials["snapshot_artifact_id"], materials["snapshot_manifest_hash"],
                materials["source_title"], materials["source_url"])
    actual = (manifest["artifact_id"], manifest["manifest_hash"], source.title, source.url)
    if actual != expected:
        raise ValueError("request.confirmed_source_differs_from_target_package")


def build_request(template, draft, materials, decisions, confirmation, output):
    """保留模板的输入、预算和路径；确认记录不授予执行权限。"""
    payload = json.loads(Path(template).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("request.template_must_be_object")
    formula = render_spec(draft, materials, decisions, confirmation)
    decision_record = json.loads(Path(decisions).read_text(encoding="utf-8"))
    confirmed_formula = {"formula": formula, "interface": decision_record["interface"]}
    if "confirmed_formula" in payload and payload["confirmed_formula"] != confirmed_formula:
        raise ValueError("request.template_confirmed_formula_differs_from_confirmation")
    payload["confirmed_formula"] = confirmed_formula
    if "code_generation" in payload:
        generation = payload["code_generation"]
        if not isinstance(generation, dict):
            raise ValueError("request.code_generation_must_be_object")
        for key, value in confirmed_formula.items():
            if key in generation and generation[key] != value:
                raise ValueError("request.template_" + key + "_differs_from_confirmation")
            generation[key] = value
    request = FrozenRequest.from_dict(payload)
    _verify_target_source(request, json.loads(Path(materials).read_text(encoding="utf-8")))
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(request.payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    return request
