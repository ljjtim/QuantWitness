"""从冻结的受控项目bundle加载Qlib模型，并随模型结果保留源码。"""
from __future__ import annotations

import importlib
import importlib.metadata
import json
from pathlib import Path
import shutil
import sys
import types
import inspect

from .inputs import QlibModelError


def validate_project_model_adapter(adapter, *, check_environment=False, bundle_path=None, verify_bundle=True):
    from research_pipeline.platform.project_bundle_content import verify_project_bundle_content
    fields = {"schema", "project_bundle_path", "project_bundle_hash", "module", "class",
              "network_attribute", "batch_contract"}
    if not isinstance(adapter, dict) or set(adapter) != fields:
        raise QlibModelError("项目模型必须提供完整的受控adapter描述")
    import re
    if (adapter["schema"] != "project-qlib-model-adapter-v1"
            or not isinstance(adapter["project_bundle_hash"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", adapter["project_bundle_hash"])
            or not isinstance(adapter["project_bundle_path"], str)
            or not Path(adapter["project_bundle_path"]).is_absolute()
            or adapter["batch_contract"] not in {"sample", "daily_cross_section"}
            or any(not isinstance(adapter[key], str) or not re.fullmatch(pattern, adapter[key])
                   for key, pattern in (("module", r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*"),
                                        ("class", r"[A-Za-z_]\w*"), ("network_attribute", r"[A-Za-z_]\w*")))):
        raise QlibModelError("项目模型adapter字段无效")
    if not verify_bundle:
        return adapter
    try:
        manifest = verify_project_bundle_content(bundle_path or adapter["project_bundle_path"])
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise QlibModelError(f"项目模型bundle无效: {exc}") from exc
    module_path = Path(*adapter["module"].split(".")).with_suffix(".py").as_posix()
    if (manifest["bundle_hash"] != adapter["project_bundle_hash"]
            or module_path not in {item["path"] for item in manifest["source_files"]}):
        raise QlibModelError("项目模型adapter与冻结bundle身份不一致")
    root = Path(bundle_path or adapter["project_bundle_path"])
    if check_environment:
        lock = json.loads((root / "dependency-lock.json").read_text(encoding="utf-8"))
        for name, expected in lock.items():
            distribution = {"qlib": "pyqlib", "sklearn": "scikit-learn"}.get(name, name)
            try:
                actual_version = importlib.metadata.version(distribution)
            except importlib.metadata.PackageNotFoundError as exc:
                raise QlibModelError(f"项目模型依赖缺失: {distribution}") from exc
            if actual_version != expected:
                raise QlibModelError(f"项目模型依赖版本不符: {distribution} {actual_version} != {expected}")
    return adapter


def load_project_model_class(adapter, *, bundle_path=None):
    """按源码身份建立独立包名，保持pickle可迁移且不污染普通模块名。"""
    validate_project_model_adapter(adapter, check_environment=True, bundle_path=bundle_path)
    root = Path(bundle_path or adapter["project_bundle_path"]) / "sources"
    namespace = "_rp_model_" + adapter["project_bundle_hash"]
    package = types.ModuleType(namespace)
    package.__path__ = [str(root)]
    package.__package__ = namespace
    sys.modules[namespace] = package
    previous = sys.dont_write_bytecode
    try:
        sys.dont_write_bytecode = True
        module = importlib.import_module(namespace + "." + adapter["module"])
    finally:
        sys.dont_write_bytecode = previous
    model_class = getattr(module, adapter["class"])
    from qlib.model.base import Model
    if not isinstance(model_class, type) or not issubclass(model_class, Model):
        raise QlibModelError("项目模型入口必须为Qlib Model类")
    return model_class


def archive_project_model(root, bundle_path, adapter):
    """复用原项目bundle闭包，模型迁移无需保留开发目录。"""
    validate_project_model_adapter(adapter, check_environment=True)
    relative = Path(bundle_path) / "project-model" / adapter["project_bundle_hash"]
    shutil.copytree(adapter["project_bundle_path"], Path(root) / relative)
    return {"bundle_path": relative.as_posix(), "adapter": adapter}


def restore_project_model(root, config):
    saved = config.get("project_model")
    if not isinstance(saved, dict) or saved.get("adapter") != config["candidate"]["model"]["kwargs"].get("adapter"):
        raise QlibModelError("项目模型源码封存与候选描述不一致")
    return load_project_model_class(saved["adapter"], bundle_path=Path(root) / saved["bundle_path"])


def archive_upstream_sources(root, bundle_path, model, network):
    """保存本次实际调用的上游模型源码，保留文件内原始许可头。"""
    modules = set()
    for cls in (*type(model).__mro__, *type(network).__mro__):
        if cls.__module__.startswith("qlib.contrib.model."):
            modules.add(cls.__module__)
    for method in (model.fit, model.predict, model.train_epoch, model.test_epoch):
        if method.__module__.startswith("qlib.contrib.model."):
            modules.add(method.__module__)
    paths = []
    for name in sorted(modules):
        source = Path(inspect.getsourcefile(importlib.import_module(name)))
        relative = Path(bundle_path) / "upstream" / source.name
        destination = Path(root) / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        paths.append(relative.as_posix())
    return paths
