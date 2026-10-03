"""确认公式的公共请求组装，只使用离线材料与文件。"""
import copy
import json
from types import SimpleNamespace

import pytest

from quantwitness_rdagent import formula_spec, request_builder
from quantwitness_rdagent.contracts import FrozenRequest, write_json
from quantwitness_rdagent.generation import build_prompt
from test_live_generation import live_payload
from test_request_and_recovery import request_payload


@pytest.fixture
def setup(tmp_path, monkeypatch):
    material = {
        "source_title": "第二公式", "source_url": "https://example.invalid/paper",
        "source_id": "paper", "snapshot_artifact_id": "paper-snapshot",
        "snapshot_manifest_hash": "existing-snapshot-identity",
        "pages": [{"pdf_page": 1, "lines": ["过去五日均值"]}], "review_notes": [],
    }
    draft = {
        "schema_version": "paper-formula-draft-v1", "title": "历史均值",
        "rules": [{"rule_id": "r1", "origin": "paper_explicit", "statement": "过去五日均值",
                   "evidence_refs": [{"pdf_page": 1, "line_start": 1, "line_end": 1,
                                      "quote": "过去五日均值"}]}],
        "ambiguities": [], "limitations": [],
    }
    decisions = {"accepted_rule_ids": ["r1"], "resolutions": [],
                 "interface": "daily_value(rows), rolling_value(rows)",
                 "review_notes": "历史窗口不包含未来数据"}
    paths = {key: tmp_path / (key + ".json") for key in
             ("template", "draft", "materials", "decisions", "confirmation", "output")}
    monkeypatch.setattr(formula_spec, "verify_materials", lambda value: None)
    for key, value in (("draft", draft), ("materials", material), ("decisions", decisions)):
        write_json(paths[key], value)
    formula_spec.confirm_spec(paths["draft"], paths["materials"], paths["decisions"],
                              paths["confirmation"], confirmed_by="测试审阅者", approve=True)
    payload = live_payload(tmp_path / "session")
    del payload["code_generation"]["formula"]
    del payload["code_generation"]["interface"]
    write_json(paths["template"], payload)
    source = SimpleNamespace(source_id="paper", title=material["source_title"], url=material["source_url"])
    seen = []
    monkeypatch.setattr(request_builder, "load_research_package",
                        lambda path: (seen.append(("package", path)) or SimpleNamespace(sources=[source])))
    monkeypatch.setattr(request_builder, "verify_source_snapshot",
                        lambda value, path: (seen.append(("archive", path)) or
                        {"artifact_id": material["snapshot_artifact_id"],
                         "manifest_hash": material["snapshot_manifest_hash"]}))
    return paths, payload, source, seen


def test_build_preserves_formula_template_and_does_not_freeze(setup, monkeypatch):
    paths, payload, _, seen = setup
    original = copy.deepcopy(payload)
    monkeypatch.setattr(FrozenRequest, "freeze", lambda self: pytest.fail("不得创建会话"))
    result = request_builder.build_request(**paths)
    expected_formula = formula_spec.render_spec(paths["draft"], paths["materials"],
                                               paths["decisions"], paths["confirmation"])
    original["code_generation"].update(formula=expected_formula,
                                        interface="daily_value(rows), rolling_value(rows)")
    original["confirmed_formula"] = {key: original["code_generation"][key] for key in ("formula", "interface")}
    assert result.payload == original
    assert json.loads(paths["output"].read_text(encoding="utf-8")) == original
    assert json.loads(paths["template"].read_text(encoding="utf-8")) == payload
    assert not result.session_root.exists()
    assert seen == [("package", result._local_path(payload["base_package"])),
                    ("archive", result._local_path(payload["source_archive_root"]))]
    assert json.loads(build_prompt(result).split("\n", 1)[1])["formula"] == expected_formula


@pytest.mark.parametrize("key", ["formula", "interface"])
def test_build_rejects_template_conflict(setup, key):
    paths, payload, _, _ = setup
    payload["code_generation"][key] = "不同的内容"
    write_json(paths["template"], payload)
    with pytest.raises(ValueError, match="template_" + key + "_differs"):
        request_builder.build_request(**paths)
    assert not paths["output"].exists()


@pytest.mark.parametrize("changed", ["missing", "draft", "decisions", "materials"])
def test_build_requires_current_confirmation(setup, changed):
    paths, _, _, _ = setup
    if changed == "missing":
        paths["confirmation"] = paths["confirmation"].with_name("absent.json")
    else:
        value = json.loads(paths[changed].read_text(encoding="utf-8"))
        if changed == "draft":
            value["title"] = "另一个标题"
        elif changed == "decisions":
            value["review_notes"] = "另一项决定"
        else:
            value["source_title"] = "另一份材料"
        write_json(paths[changed], value)
    with pytest.raises((ValueError, FileNotFoundError)):
        request_builder.build_request(**paths)
    assert not paths["output"].exists()


@pytest.mark.parametrize("fault", ["missing_source", "title", "url", "snapshot", "manifest"])
def test_build_rejects_different_target_source(setup, monkeypatch, fault):
    paths, _, source, _ = setup
    if fault == "missing_source":
        source.source_id = "other"
    elif fault in {"title", "url"}:
        setattr(source, fault, "other")
    else:
        manifest = {"artifact_id": "paper-snapshot", "manifest_hash": "existing-snapshot-identity"}
        manifest["artifact_id" if fault == "snapshot" else "manifest_hash"] = "other"
        monkeypatch.setattr(request_builder, "verify_source_snapshot", lambda *args: manifest)
    with pytest.raises(ValueError, match=r"request\.(source_missing|confirmed_source_differs)"):
        request_builder.build_request(**paths)
    assert not paths["output"].exists()


def test_build_rejects_invalid_budget(setup):
    paths, payload, _, _ = setup
    payload["budget"]["live_llm_calls"] = 4
    write_json(paths["template"], payload)
    with pytest.raises(ValueError, match="1至3"):
        request_builder.build_request(**paths)
    assert not paths["output"].exists()


def test_build_fixed_request_freezes_confirmed_formula_without_model(setup):
    paths, _, _, _ = setup
    payload = request_payload(paths["template"].parent / "fixed-session")
    write_json(paths["template"], payload)
    result = request_builder.build_request(**paths)
    assert result.payload["budget"]["live_llm_calls"] == 0
    assert "code_generation" not in result.payload
    assert result.payload["confirmed_formula"] == {
        "formula": formula_spec.render_spec(paths["draft"], paths["materials"],
                                            paths["decisions"], paths["confirmation"]),
        "interface": "daily_value(rows), rolling_value(rows)",
    }
    assert result.payload["runtime_binding"] == payload["runtime_binding"]
    assert not result.session_root.exists()


def test_build_rejects_conflicting_confirmed_formula(setup):
    paths, payload, _, _ = setup
    payload["confirmed_formula"] = {"formula": "其他公式", "interface": "不同接口"}
    write_json(paths["template"], payload)
    with pytest.raises(ValueError, match="template_confirmed_formula_differs"):
        request_builder.build_request(**paths)
    assert not paths["output"].exists()


def test_build_does_not_overwrite_existing_output(setup):
    paths, _, _, _ = setup
    paths["output"].write_text("已有请求", encoding="utf-8")
    with pytest.raises(FileExistsError):
        request_builder.build_request(**paths)
    assert paths["output"].read_text(encoding="utf-8") == "已有请求"


def test_build_accepts_matching_formula_and_interface(setup):
    paths, payload, _, _ = setup
    payload["code_generation"].update(
        formula=formula_spec.render_spec(paths["draft"], paths["materials"],
                                        paths["decisions"], paths["confirmation"]),
        interface="daily_value(rows), rolling_value(rows)",
    )
    write_json(paths["template"], payload)
    payload["confirmed_formula"] = {key: payload["code_generation"][key] for key in ("formula", "interface")}
    assert request_builder.build_request(**paths).payload == payload


@pytest.mark.parametrize("valid", [True, False])
def test_build_preserves_and_validates_formula_evaluation(setup, valid):
    paths, payload, _, _ = setup
    evaluation = {"verifier_id": "project.volume_concentration", "verifier_version": "1.0.0",
                  "coverage_schema_id": "project.volume_concentration.coverage.v1"}
    if not valid:
        del evaluation["verifier_id"]
    payload["formula_evaluation"] = evaluation
    write_json(paths["template"], payload)
    if valid:
        assert request_builder.build_request(**paths).payload["formula_evaluation"] == evaluation
    else:
        with pytest.raises(ValueError, match="formula_evaluation"):
            request_builder.build_request(**paths)
        assert not paths["output"].exists()
