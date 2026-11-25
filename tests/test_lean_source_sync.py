"""源码同步按实际变化执行，失败不放行后续研究。"""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import venv
from types import SimpleNamespace

import pytest


INTEGRATION = Path(__file__).resolve().parents[1] / "integrations/lean"
SPEC = importlib.util.spec_from_file_location("lean_source_sync", INTEGRATION / "sync_installation.py")
sync = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sync)


@pytest.fixture
def sources(tmp_path, monkeypatch):
    project, home = tmp_path / "project", tmp_path / "installation"
    home.mkdir()
    for name in sync.PACKAGE_INPUTS:
        path = project / name
        if name == "src":
            path = path / "sample.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original", encoding="utf-8")
    engine = project / "integrations/lean/engine"
    engine.mkdir()
    (engine / "Rule.cs").write_text("original", encoding="utf-8")
    (engine / "QuantWitnessLeanHost.csproj").write_text(
        '<Project><ItemGroup><Compile Include="Rule.cs" /><Compile Include="added.cs" />'
        '<ProjectReference Include="china-rules/ChinaRules.csproj" />'
        '</ItemGroup></Project>', encoding="utf-8",
    )
    china_rules = engine / "china-rules"
    china_rules.mkdir()
    (china_rules / "ChinaRules.csproj").write_text(
        '<Project><ItemGroup><Compile Include="Model.cs" /><EmbeddedResource Include="model.json" />'
        '</ItemGroup></Project>', encoding="utf-8",
    )
    monkeypatch.setattr(sync, "require_idle", lambda root: None)
    monkeypatch.setattr(sync, "publish_installation_audit", lambda root, state: None)
    return project, home


def test_first_sync_then_unchanged_skips_all_builds(sources, monkeypatch):
    project, home = sources
    calls = []
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    assert sync.synchronize(project, home)["updated"] == ["package", "engine"]
    assert calls[0][0][0][3].endswith("refresh-package.ps1")
    assert "-Install" in calls[1][0][0]
    assert sync.synchronize(project, home)["status"] == "unchanged"
    assert len(calls) == 2


def test_relative_nuget_cache_keeps_callers_directory(sources, monkeypatch, tmp_path):
    project, home = sources
    monkeypatch.chdir(tmp_path)
    calls = []
    monkeypatch.setattr(sync.subprocess, "run", lambda command, **kwargs: calls.append(command))
    sync.synchronize(project, home, nuget_cache=Path("existing-cache"))
    command = calls[1]
    assert command[command.index("-NugetCache") + 1] == str(tmp_path / "existing-cache")


@pytest.mark.parametrize("operation", ["same_size_same_time", "add", "delete"])
@pytest.mark.parametrize("group", ["package", "engine"])
def test_content_addition_and_deletion_select_only_affected_group(sources, monkeypatch, group, operation):
    project, home = sources
    sync.record_installed(project, home)
    path = project / ("src/sample.py" if group == "package" else "integrations/lean/engine/Rule.cs")
    if operation == "same_size_same_time":
        stamp = path.stat()
        path.write_text("modified", encoding="utf-8")
        os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    elif operation == "add":
        path.with_name("added" + path.suffix).write_text("added", encoding="utf-8")
    else:
        path.unlink()
    calls = []
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: calls.append(args))
    assert sync.synchronize(project, home)["updated"] == [group]
    assert len(calls) == 1


def test_cache_files_do_not_trigger_installation(sources, monkeypatch):
    project, home = sources
    sync.record_installed(project, home)
    cache = project / "src/__pycache__/sample.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"cache")
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: pytest.fail("不应重新安装"))
    assert sync.synchronize(project, home)["status"] == "unchanged"


def test_engine_documentation_tests_and_unlisted_sources_do_not_trigger_build(sources, monkeypatch):
    project, home = sources
    sync.record_installed(project, home)
    engine = project / "integrations/lean/engine"
    for name in ("README.md", "test_entry_module.py", "Unused.cs", "tests/Program.cs"):
        path = engine / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("changed", encoding="utf-8")
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: pytest.fail("不应重新编译"))
    assert sync.synchronize(project, home)["status"] == "unchanged"


def test_docs_and_root_markdown_do_not_trigger_package_build(sources, monkeypatch):
    project, home = sources
    sync.record_installed(project, home)
    (project / "README.md").write_text("updated doc", encoding="utf-8")
    doc = project / "docs/guide.md"
    doc.parent.mkdir(parents=True, exist_ok=True)
    doc.write_text("updated doc", encoding="utf-8")
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: pytest.fail("不应重新安装"))
    assert sync.synchronize(project, home)["status"] == "unchanged"


@pytest.mark.parametrize("name", [
    "build.ps1", "Directory.Build.props", "build_pythonnet.py", "install_metadata.py",
    "../patch_native_lifecycle.py", "china-rules/ChinaRules.csproj",
    "china-rules/Model.cs", "china-rules/model.json",
])
def test_build_scripts_and_referenced_project_inputs_trigger_engine_only(sources, monkeypatch, name):
    project, home = sources
    sync.record_installed(project, home)
    path = project / "integrations/lean/engine" / name
    with path.open("a", encoding="utf-8") as stream:
        stream.write("\n")
    calls = []
    monkeypatch.setattr(sync.subprocess, "run", lambda command, **kwargs: calls.append(command))
    assert sync.synchronize(project, home)["updated"] == ["engine"]
    assert len(calls) == 1
    assert "-Install" in calls[0]


def test_build_failure_invalidates_skip_record_and_retries(sources, monkeypatch):
    project, home = sources
    sync.record_installed(project, home)
    (project / "src/sample.py").write_text("changed", encoding="utf-8")
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0])
    monkeypatch.setattr(sync.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        sync.synchronize(project, home)
    assert not (home / "source-sync.json").exists()
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: None)
    assert sync.synchronize(project, home)["updated"] == ["package", "engine"]


def test_active_installation_is_not_modified(sources, monkeypatch):
    project, home = sources
    sync.record_installed(project, home)
    original = (home / "source-sync.json").read_bytes()
    def busy(root):
        raise RuntimeError("安装正在使用")
    monkeypatch.setattr(sync, "require_idle", busy)
    monkeypatch.setattr(sync.subprocess, "run", lambda *args, **kwargs: pytest.fail("不应覆盖安装"))
    with pytest.raises(RuntimeError, match="正在使用"):
        sync.synchronize(project, home, force_package=True)
    assert (home / "source-sync.json").read_bytes() == original


@pytest.mark.parametrize("arguments,busy", [
    (["python", "-m", "research_pipeline", "run", "--plan", "outside-plan"], True),
    (["python", "-m", "research_pipeline", "resume", "--plan", "outside-plan"], True),
    (["python", "-m", "research_pipeline", "workspace", "execute"], True),
    (["quantwitness.exe", "retry-node"], True),
    (["python", "-m", "research_pipeline", "report"], False),
    (["python", "-m", "research_pipeline", "workspace", "init"], False),
    (["python", "unrelated.py"], False),
])
def test_idle_check_includes_external_research_but_allows_readers(tmp_path, monkeypatch, arguments, busy):
    import psutil

    own = SimpleNamespace(pid=10, parents=lambda: [SimpleNamespace(pid=9)])
    processes = [
        SimpleNamespace(pid=10, info={"exe": "python", "cmdline": ["quantwitness.exe", "run"]}),
        SimpleNamespace(pid=9, info={"exe": "python", "cmdline": ["quantwitness.exe", "run"]}),
        SimpleNamespace(pid=20, info={"exe": "outside-python", "cmdline": arguments}),
    ]
    monkeypatch.setattr(psutil, "Process", lambda: own)
    monkeypatch.setattr(psutil, "process_iter", lambda attrs: iter(processes))
    if busy:
        with pytest.raises(RuntimeError, match="进程20"):
            sync.require_idle(tmp_path / "installation")
    else:
        sync.require_idle(tmp_path / "installation")


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="需要PowerShell 7")
def test_real_powershell_invocation_and_failure(sources):
    project, home = sources
    integration = project / "integrations/lean"
    refresh = integration / "refresh-package.ps1"
    refresh.write_text("param([string]$LeanHome)\nSet-Content (Join-Path $LeanHome 'python-updated') 'yes'\n", encoding="utf-8")
    build = integration / "engine/build.ps1"
    build.write_text("param([string]$LeanHome,[switch]$Install,[string]$NugetCache)\n"
                     "if(!$Install) { throw '必须安装' }\n"
                     "Set-Content (Join-Path $LeanHome 'engine-updated') $NugetCache\n", encoding="utf-8")
    sync.synchronize(project, home, nuget_cache=project / "带 空格的缓存")
    assert (home / "python-updated").read_text().strip() == "yes"
    assert "带 空格的缓存" in (home / "engine-updated").read_text(encoding="utf-8-sig")
    build.write_text("throw '编译失败'\n", encoding="utf-8")
    with pytest.raises(subprocess.CalledProcessError):
        sync.synchronize(project, home)
    assert not (home / "source-sync.json").exists()


def test_refresh_runtime_copies_declaration_and_bundles_without_changing_research(tmp_path, monkeypatch):
    import research_pipeline.packages as packages
    import research_pipeline.platform.lean_runtime as runtime
    from research_pipeline.packages.models import ResearchPackage
    from research_pipeline.packages.store import PACKAGE_FILES

    spec = importlib.util.spec_from_file_location("sync_prepare", INTEGRATION / "prepare.py")
    prepare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prepare)
    home, source = tmp_path / "lean", tmp_path / "package"
    old_lock = {"installation_root": str(home), "version": "old"}
    new_lock = {"installation_root": str(home), "version": "new"}
    from research_pipeline.domain.lean_algorithm import LeanAlgorithm, LeanAlgorithmLimits, LeanDataBinding, LeanStrategyDeclaration
    algorithm = LeanAlgorithm(language="Python", entry_file="main.py", type_name="Example")
    declaration = LeanStrategyDeclaration(strategy_id="example", bundle_id="bundle", algorithm=algorithm,
        frequency="1d", limits=LeanAlgorithmLimits(initialize_seconds=30, time_step_seconds=30),
        input_bindings=(LeanDataBinding(binding_id="daily", input_port="bars", request_id="original",
            kind="bars", resolution="1d", columns=("close",)),)).to_dict()
    parameters = dict(execution_backend="lean", lean_runtime=old_lock, initial_cash_units=1000000,
                      strategy_declaration=declaration)
    node = dict(operator_id="project.strategy", operator_version="1", parameters=parameters,
                inputs=[dict(input_port=port) for port in ("bars", "snapshot")])
    research = dict(graph={"nodes": [node]}, requests=[{"request_id": "original"}],
                    result={"tables": [{"table_id": "custom_daily"}]})
    for name in PACKAGE_FILES:
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(research if name == "spec/research.yaml" else {}), encoding="utf-8")
    original = (source / "spec/research.yaml").read_bytes()
    bundle = tmp_path / "bundles/bundle"
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text("{}", encoding="utf-8")
    package = SimpleNamespace(package_slug="daily", display_name="日线", package_version="1", builder_id="builder",
                              sources=(), localizations=(), metric_contract=object(), claim_contract=object(),
                              minute_scope_inputs={})
    manifest = SimpleNamespace(bundle_hash="bundle", lean_algorithm=algorithm, operator_spec=SimpleNamespace(resource_profile={"process_slots": 4},
        input_ports=(SimpleNamespace(port="bars", artifact_type="data.columnar-bundle.v1"),)))
    monkeypatch.setattr(packages, "load_research_package", lambda *args, **kwargs: package)
    monkeypatch.setattr(prepare, "bundle_catalog", lambda root: {("project.strategy", "1"): (bundle, manifest)})
    monkeypatch.setattr(runtime, "validate_lean_runtime", lambda value, **kwargs: value)
    monkeypatch.setattr(runtime, "freeze_lean_runtime", lambda root: new_lock)
    monkeypatch.setattr(ResearchPackage, "build", lambda **kwargs: SimpleNamespace(**kwargs))
    summary, prepared = prepare.prepare_package(source, tmp_path / "output", home, refresh_runtime=True)
    assert (source / "spec/research.yaml").read_bytes() == original
    assert prepared.spec_payload["graph"]["nodes"][0]["parameters"]["lean_runtime"] == new_lock
    assert prepared.spec_payload["result"] == research["result"]
    assert prepared.spec_payload["requests"] == [{"request_id": "original", "filters": [], "limit": None}]
    assert prepare.read_mapping(Path(summary["package"]) / "spec/research.yaml")["requests"] == research["requests"]
    assert prepared.metric_contract is package.metric_contract
    assert Path(summary["package"]) == tmp_path / "output/package"
    assert Path(summary["bundles"][0]) == tmp_path / "output/bundles/bundle"
    assert (Path(summary["bundles"][0]) / "manifest.json").is_file()


@pytest.mark.skipif(os.name != "nt" or shutil.which("pwsh") is None, reason="需要Windows和PowerShell 7")
@pytest.mark.parametrize("sync_exit", [0, 1])
def test_run_entry_synchronizes_before_prepare_and_stops_on_failure(tmp_path, sync_exit):
    integration, home = tmp_path / "integration", tmp_path / "installation"
    integration.mkdir()
    venv.EnvBuilder(with_pip=False).create(home / "python-env")
    shutil.copyfile(INTEGRATION / "run.ps1", integration / "run.ps1")
    (integration / "sync_installation.py").write_text(
        "from pathlib import Path\nimport sys\n"
        "Path(__file__).with_name('sync-called').write_text('yes')\n"
        f"sys.exit({sync_exit})\n", encoding="utf-8")
    (integration / "accept.py").write_text(
        "from pathlib import Path\nimport sys,json,os\n"
        "assert Path(__file__).with_name('sync-called').is_file()\n"
        "assert os.environ.get('QUANTWITNESS_STRICT_AUDIT') == '1'\n"
        "Path(__file__).with_name('accepted.json').write_text(json.dumps(sys.argv))\n", encoding="utf-8")
    completed = subprocess.run([
        "pwsh", "-NoProfile", "-File", str(integration / "run.ps1"),
        "-FromPackage", str(tmp_path / "package"), "-CatalogLock", str(tmp_path / "catalog"),
        "-InputSnapshotManifest", str(tmp_path / "inputs.json"),
        "-LeanHome", str(home), "-Output", str(tmp_path / "output"), "-StrictAudit",
    ], capture_output=True, text=True, encoding="utf-8")
    accepted = integration / "accepted.json"
    if sync_exit:
        assert completed.returncode != 0
        assert not accepted.exists()
    else:
        assert completed.returncode == 0, completed.stderr
        assert "--refresh-runtime" in json.loads(accepted.read_text())
