"""离线请求和 allocation 持久关联，不运行数据库或模型。"""
import json
from pathlib import Path
import pytest
from quantwitness_rdagent.contracts import FrozenRequest
from quantwitness_rdagent.execution import RPExecutionBridge
from quantwitness_rdagent.worker import find_or_allocate


def request_payload(tmp_path):
    material_root = tmp_path.parent / (tmp_path.name + "-inputs")
    material_root.mkdir(parents=True, exist_ok=True)
    for relative in ("package/package.yaml", "package/localization.yaml", "package/sources/sources.yaml",
                     "package/spec/research.yaml", "source/adapter.py", "verifier/manifest.json", "verifier/COMMITTED",
                     "operator.yaml", "inputs.json", "catalog/CURRENT", "catalog/version/catalog.lock.json",
                     "catalog/version/catalog.source-manifest.json", "catalog/version/catalog.audit.json",
                     "catalog/version/catalog.schema.json"):
        path = material_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("{}", encoding="utf-8")
    (material_root / "catalog/CURRENT").write_text("version\n", encoding="utf-8")
    return {
        "request_id": "formula-test", "mode": "formula_reproduction",
        "base_package": str(material_root / "package"), "source_archive_root": "archive", "input_snapshot_manifest": str(material_root / "inputs.json"),
        "editable_source_root": str(material_root / "source"), "reference_bundle": str(material_root / "verifier"), "development_scope": {"role": "development", "start": "2020-01-01", "end": "2020-01-31"},
        "budget": {"outer_loops": 1, "coder_attempts": 3, "parallel": 1, "live_llm_calls": 0},
        "runtime_binding": {"windows_python": "I:/env/python.exe", "windows_repo": "I:/repo", "linux_repo": "/mnt/i/repo",
            "windows_session_root": str(tmp_path), "linux_session_root": str(tmp_path), "operator_spec": str(material_root / "operator.yaml"),
            "catalog_lock": str(material_root / "catalog"), "extension_bundles": [], "fixed_responses": ["bad", "good"], "runtime_options": {}},
    }


def test_frozen_request_and_persistent_attempt_budget(tmp_path):
    payload = request_payload(tmp_path)
    bridge = RPExecutionBridge(FrozenRequest.from_dict(payload))
    assert bridge.candidate("first") == bridge.candidate("first") == "candidate-0000"
    bridge.candidate("second")
    bridge.candidate("third")
    with pytest.raises(RuntimeError, match="次数"):
        RPExecutionBridge(FrozenRequest.from_dict(payload)).candidate("fourth")
    payload["development_scope"]["end"] = "2020-02-01"
    with pytest.raises(ValueError, match="冻结"):
        FrozenRequest.from_dict(payload).freeze()


def test_live_calls_are_not_supported(tmp_path):
    payload = request_payload(tmp_path)
    payload["budget"]["live_llm_calls"] = 1
    with pytest.raises(ValueError, match="零实时"):
        FrozenRequest.from_dict(payload)


def test_allocation_crash_after_execution_declaration_reuses_execution(tmp_path, monkeypatch):
    import research_pipeline.workspace as workspace
    root = tmp_path / "workspace"
    workspace.initialize_workspace(root, workspace_id="rd-recovery")
    original = workspace._atomic_replace_text

    def interrupt_index(path, text):
        if Path(path).name == "index.json":
            raise InterruptedError("模拟 execution.json 已写而 allocation 尚未返回")
        return original(path, text)

    monkeypatch.setattr(workspace, "_atomic_replace_text", interrupt_index)
    with pytest.raises(InterruptedError):
        workspace.allocate_execution(root, clock="2026-01-01T00:00:00+08:00", root_seed=7, label="request:candidate-0000")
    declarations = list((root / ".research/executions").glob("*/execution.json"))
    assert len(declarations) == 1
    expected = json.loads(declarations[0].read_text(encoding="utf-8"))["execution_id"]
    monkeypatch.setattr(workspace, "_atomic_replace_text", original)
    resumed = find_or_allocate(root, "request:candidate-0000", clock="2026-01-01T00:00:00+08:00", root_seed=7)
    assert resumed["execution_id"] == expected
    assert len(list((root / ".research/executions").glob("*/execution.json"))) == 1


@pytest.mark.parametrize("changed", ["base_package", "editable_source_root"])
def test_same_path_fixed_material_changes_are_rejected(tmp_path, changed):
    request = FrozenRequest.from_dict(request_payload(tmp_path / "session"))
    request.freeze()
    path = Path(request.payload[changed]) / ("package.yaml" if changed == "base_package" else "adapter.py")
    path.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="已改变"):
        request.freeze()


def test_inspect_does_not_create_session(tmp_path, monkeypatch, capsys):
    from quantwitness_rdagent.__main__ import main
    payload = request_payload(tmp_path / "session")
    request_file = tmp_path / "request.json"
    request_file.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["quantwitness_rdagent", "inspect", "--request", str(request_file)])
    main()
    assert json.loads(capsys.readouterr().out) == {"status": "incomplete"}
    assert not (tmp_path / "session").exists()


def test_nonzero_worker_never_reuses_previous_pass_feedback(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from quantwitness_rdagent.contracts import write_json
    request = FrozenRequest.from_dict(request_payload(tmp_path / "session"))
    bridge = RPExecutionBridge(request)
    cid = bridge.candidate("def compute(): pass")
    receipt = request.session_root / "candidates" / cid / "feedback.json"
    write_json(receipt, {"command_status": "succeeded", "formula_status": "pass"})
    monkeypatch.setattr("quantwitness_rdagent.execution.subprocess.run", lambda *args, **kwargs:
                        SimpleNamespace(returncode=1, stderr="中断发生在新反馈写出前"))
    with pytest.raises(RuntimeError, match="执行桥中断"):
        bridge.evaluate(cid)
    assert json.loads(receipt.read_text(encoding="utf-8"))["formula_status"] == "pass"


def test_archive_overlap_is_rejected_before_session_write(tmp_path):
    payload = request_payload(tmp_path / "session")
    manifest = Path(payload["input_snapshot_manifest"])
    manifest.write_text(json.dumps({"requests": {"input": {"root": str(tmp_path)}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="只读来源目录重叠"):
        FrozenRequest.from_dict(payload).freeze()
    assert not (tmp_path / "session").exists()


def test_candidate_declaration_is_completed_after_source_write_interrupt(tmp_path):
    bridge = RPExecutionBridge(FrozenRequest.from_dict(request_payload(tmp_path / "session")))
    partial = bridge.request.session_root / "candidates" / "candidate-0000"
    partial.mkdir(parents=True)
    (partial / "compute.py").write_text("def value(): return 1", encoding="utf-8")
    assert bridge.candidate("def value(): return 1") == "candidate-0000"
    assert json.loads((partial / "candidate.json").read_text(encoding="utf-8"))["allocation_label"] == "formula-test:candidate-0000"
    assert len(list(partial.parent.iterdir())) == 1


@pytest.mark.parametrize("control", ["CURRENT", "catalog.lock.json", "catalog.source-manifest.json", "catalog.audit.json", "catalog.schema.json"])
def test_catalog_control_change_rejected_before_worker(tmp_path, monkeypatch, control):
    request = FrozenRequest.from_dict(request_payload(tmp_path / "session"))
    bridge = RPExecutionBridge(request)
    candidate = bridge.candidate("def compute(): pass")
    catalog = Path(request.payload["runtime_binding"]["catalog_lock"])
    if control == "CURRENT":
        import shutil
        shutil.copytree(catalog / "version", catalog / "new-version")
        (catalog / "CURRENT").write_text("new-version\n", encoding="utf-8")
    else:
        (catalog / "version" / control).write_text('{"changed":true}', encoding="utf-8")
    def forbidden(*args, **kwargs):
        raise AssertionError("冻结变更不得启动Windows worker")
    monkeypatch.setattr("quantwitness_rdagent.execution.subprocess.run", forbidden)
    with pytest.raises(ValueError, match="已改变"):
        bridge.evaluate(candidate)


@pytest.mark.parametrize("path_style", ["relative", "absolute", "relative_backslash"])
def test_external_original_plan_body_frozen_before_worker(tmp_path, monkeypatch, path_style):
    payload = request_payload(tmp_path / "session")
    manifest = Path(payload["input_snapshot_manifest"])
    plan = manifest.parent / "plans/original.json"
    plan.parent.mkdir()
    plan.write_text('{"plan":"frozen"}', encoding="utf-8")
    references = {"relative": "plans/original.json", "relative_backslash": "plans\\original.json", "absolute": str(plan)}
    manifest.write_text(json.dumps({"requests": {"minute": {"root": str(manifest.parent / "minute"),
        "original_plan": references[path_style]}}}), encoding="utf-8")
    bridge = RPExecutionBridge(FrozenRequest.from_dict(payload))
    candidate = bridge.candidate("def compute(): pass")
    assert bridge.request._materials()["original_plans/minute"] == plan.read_text(encoding="utf-8")
    plan.write_text('{"plan":"changed"}', encoding="utf-8")
    def forbidden(*args, **kwargs):
        raise AssertionError("原计划变更不得启动Windows worker")
    monkeypatch.setattr("quantwitness_rdagent.execution.subprocess.run", forbidden)
    with pytest.raises(ValueError, match="已改变"):
        bridge.evaluate(candidate)


@pytest.mark.parametrize('field,value', [
    ('confirmed_formula', {'formula': 'x'}),
    ('confirmed_formula', {'formula': '', 'interface': 'daily_value(rows), rolling_value(rows)'}),
    ('formula_evaluation', {'verifier_id': 'v', 'verifier_version': '1.0.0'}),
    ('formula_evaluation', {'verifier_id': 'v', 'verifier_version': '1.0.0', 'coverage_schema_id': ''}),
])
def test_new_formula_binding_requires_complete_declaration(tmp_path, field, value):
    payload = request_payload(tmp_path / 'session')
    payload[field] = value
    with pytest.raises(ValueError, match=field):
        FrozenRequest.from_dict(payload)


@pytest.mark.parametrize('field', ['confirmed_formula', 'formula_evaluation'])
def test_formula_binding_is_frozen_across_resume(tmp_path, field):
    payload = request_payload(tmp_path / 'session')
    payload['confirmed_formula'] = {'formula': '日内成交量集中度', 'interface': 'daily_value(rows), rolling_value(rows)'}
    payload['formula_evaluation'] = {'verifier_id': 'volume-check', 'verifier_version': '1.0.0',
                                   'coverage_schema_id': 'project.volume.coverage.v1'}
    FrozenRequest.from_dict(payload).freeze()
    key = 'formula' if field == 'confirmed_formula' else 'verifier_id'
    payload[field][key] = 'changed'
    with pytest.raises(ValueError, match='冻结'):
        FrozenRequest.from_dict(payload).freeze()


@pytest.mark.parametrize('layout', ['standalone', 'monorepo'])
def test_execution_bridge_accepts_public_source_root(tmp_path, monkeypatch, layout):
    """独立公开源码与单仓库布局都可启动固定worker入口。"""
    import subprocess
    import sys
    from types import SimpleNamespace
    import quantwitness_rdagent.execution as execution_module

    repo = tmp_path / 'source'
    project = repo if layout == 'standalone' else repo / 'research_pipeline'
    (project / 'src/research_pipeline').mkdir(parents=True)
    integration = project / 'integrations/rdagent/src/quantwitness_rdagent'
    integration.mkdir(parents=True)
    (integration / '__init__.py').write_text('', encoding='utf-8')
    (integration / 'worker.py').write_text('print("public-worker-started")', encoding='utf-8')
    payload = request_payload(tmp_path / 'session')
    payload['runtime_binding']['windows_repo'] = str(repo)
    payload['runtime_binding']['windows_python'] = sys.executable
    bridge = RPExecutionBridge(FrozenRequest.from_dict(payload))
    candidate = bridge.candidate('def value(): return 1')
    real_run = subprocess.run

    def probe(argv, **kwargs):
        actual = [sys.executable, *argv[1:]]
        completed = real_run(actual, **kwargs)
        assert completed.returncode == 0, completed.stderr
        assert 'public-worker-started' in completed.stdout
        return SimpleNamespace(returncode=1, stderr='probe-complete')

    monkeypatch.setattr(execution_module.subprocess, 'run', probe)
    with pytest.raises(RuntimeError, match='probe-complete'):
        bridge.evaluate(candidate)
