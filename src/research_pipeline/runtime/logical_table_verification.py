"""正式工件的同运行表逻辑验收复用。"""
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
import json

import pyarrow.parquet as pq
from research_pipeline.platform import typed_canonical_hash

from research_pipeline.data_plane.verification_lifecycle import (
    RunScopedArtifactVerification, current_artifact_verification,
)
from .errors import RuntimeIntegrityError


@dataclass
class _VerifiedInput:
    commit: object
    file_stats: tuple
    tables: dict = field(default_factory=dict)


def register_produced_tables(session, root, commit):
    """由已提交核心节点登记；任意工件绑定本身不产生生产证明。"""
    root = Path(root).resolve()
    if session.lookup("external_artifact", str(root)) != commit:
        raise RuntimeIntegrityError("生产表证明缺少本次文件完整验收")
    metadata_path = root / "artifact-metadata.json"
    if not metadata_path.is_file():
        return
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    payload = {key: value for key, value in metadata.items() if key != "artifact_hash"}
    if typed_canonical_hash(payload) != metadata.get("artifact_hash"):
        raise RuntimeIntegrityError("生产表证明元数据身份不一致")
    declarations = {}
    for name, row_count in metadata.get("row_counts", {}).items():
        _require_table_files(root, name, commit)
        paths = sorted((root / name).glob("*.parquet"))
        if not paths or sum(pq.read_metadata(path).num_rows for path in paths) != row_count:
            raise RuntimeIntegrityError("生产表证明行数不一致")
        if not isinstance(metadata.get("table_hashes", {}).get(name), str):
            raise RuntimeIntegrityError("生产表证明缺少逻辑声明")
        if metadata.get("table_hash_modes", {}).get(name) == "partition-records-v1":
            descriptors = metadata.get("table_partitions", {}).get(name, ())
            if ([item["path"] for item in descriptors] != [path.name for path in paths]
                    or [item["row_count"] for item in descriptors]
                    != [pq.read_metadata(path).num_rows for path in paths]
                    or typed_canonical_hash([item["records_hash"] for item in descriptors])
                    != metadata["table_hashes"][name]):
                raise RuntimeIntegrityError("生产表证明分区声明不一致")
        declarations[name] = _declaration(metadata, name)
    session.remember("produced_table_input", str(root),
                     (commit, _file_stats(root, commit), declarations))


def _declaration(metadata, name):
    return (metadata.get("artifact_hash"), metadata.get("row_counts", {}).get(name),
            metadata.get("table_hashes", {}).get(name),
            metadata.get("table_hash_modes", {}).get(name),
            metadata.get("table_partitions", {}).get(name))


def verified_table_descriptor(root, commit):
    """Supervisor只返回已完成文件验收且绑定生产事实的不可变声明。"""
    session = current_artifact_verification()
    root = Path(root).resolve()
    bind_verified_table_input(root, commit)
    binding = session.lookup("logical_table_input", str(root))
    return (commit, binding.file_stats, deepcopy(binding.tables))


def _file_stats(root, commit):
    try:
        names = (*commit.files, "manifest.json", "COMMITTED")
        return tuple((name, _stat(root / name)) for name in names)
    except OSError as exc:
        raise RuntimeIntegrityError("表逻辑验收来源文件缺失") from exc


def _stat(path):
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def bind_verified_table_input(root, commit):
    """仅绑定当前验证会话已验收的正式目录，重复绑定不能覆盖旧来源事实。"""
    session = current_artifact_verification()
    if session is None:
        return
    root = Path(root).resolve()
    identity = str(root)
    if not isinstance(session, RunScopedArtifactVerification):
        fact = session.resolve("logical_table_input", identity, None,
                               descriptor={"root": identity, "commit": commit.to_dict()})
        if fact[0] != commit or fact[1] != _file_stats(root, commit):
            raise RuntimeIntegrityError("表逻辑验收来源发生变化")
        return
    if session.lookup("external_artifact", identity) != commit:
        raise RuntimeIntegrityError("表逻辑验收缺少当前运行的正式工件验证")
    existing = session.lookup("logical_table_input", identity)
    stats = _file_stats(root, commit)
    if existing is None:
        produced = session.lookup("produced_table_input", identity)
        if produced is not None and (produced[0] != commit or produced[1] != stats):
            raise RuntimeIntegrityError("生产表证明来源发生变化")
        tables = deepcopy(produced[2]) if produced is not None else {}
        session.remember("logical_table_input", identity, _VerifiedInput(commit, stats, tables))
    elif existing.commit != commit or existing.file_stats != stats:
        raise RuntimeIntegrityError("表逻辑验收来源发生变化")


def _require_table_files(root, name, commit):
    # 与模型读取器的单层 glob 保持一致，不扫描其他表或源目录。
    prefix = name + "/"
    expected = {path for path in commit.files
                if path.startswith(prefix) and path.endswith(".parquet")
                and "/" not in path[len(prefix):]}
    actual = {path.relative_to(root).as_posix() for path in (root / name).glob("*.parquet")}
    if actual != expected:
        raise RuntimeIntegrityError("表逻辑验收文件集合发生变化")


def _table_binding(root):
    session = current_artifact_verification()
    root = Path(root).resolve()
    binding = (session.lookup("logical_table_input", str(root))
               if isinstance(session, RunScopedArtifactVerification) else None)
    if session is not None and not isinstance(session, RunScopedArtifactVerification):
        fact = session.lookup("logical_table_input", str(root))
        if fact is not None:
            # 此副本由当前Worker独占；完整验收成功只更新本地事实，不回传Supervisor。
            binding = _VerifiedInput(fact[0], fact[1], fact[2])
    return binding


def has_verified_table_records(root, name, metadata):
    """只查询并复验当前事实，不触发无证明表的完整扫描。"""
    root = Path(root).resolve()
    binding = _table_binding(root)
    if binding is None:
        return False
    _require_table_files(root, name, binding.commit)
    if _file_stats(root, binding.commit) != binding.file_stats:
        raise RuntimeIntegrityError("表逻辑验收来源发生变化")
    declaration = _declaration(metadata, name)
    if name in binding.tables:
        if binding.tables[name] != declaration:
            raise RuntimeIntegrityError("表逻辑验收声明发生变化")
        return True
    return False


def verify_table_records(root, name, metadata, verifier):
    """首次完整验证；成功后只在同一正式来源及声明内复用。"""
    root = Path(root).resolve()
    if has_verified_table_records(root, name, metadata):
        return
    verifier()
    binding = _table_binding(root)
    if binding is None:
        return
    # 失败验收不会进入缓存；也拒绝验收期间发生的来源修改。
    _require_table_files(root, name, binding.commit)
    if _file_stats(root, binding.commit) != binding.file_stats:
        raise RuntimeIntegrityError("表逻辑验收来源发生变化")
    binding.tables[name] = deepcopy(_declaration(metadata, name))
