"""来源正文的离线导入、真实字节校验和证明摘要。"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import yaml

from research_pipeline.data_plane import PathRolePolicy
from research_pipeline.platform import canonical_json, typed_canonical_hash

from .models import PackageSource, ResearchPackage, ResearchPackageError, SourceProvenance
from .store import load_research_package


SOURCE_SNAPSHOT_VERSION = "research-source-snapshot-v1"
SOURCE_SNAPSHOT_FILE = "snapshot.bin"
SOURCE_SNAPSHOT_MANIFEST = "manifest.json"
SOURCE_SNAPSHOT_COMMITTED = "COMMITTED"
_MANIFEST_FIELDS = {
    "contract_version", "artifact_id", "source_id", "source_url", "source_title",
    "source_accessed_at", "source_license_id", "declared_reference_hash",
    "content_digest", "byte_size", "media_type", "importer_id", "imported_at",
    "snapshot_file", "manifest_hash",
}


def verify_package_source_provenance(
    package: ResearchPackage,
    archive_root: str | Path | None = None,
) -> dict[str, object]:
    """验证全部归档来源；仅引用来源明确返回不可离线复现。"""
    archived = tuple(item for item in package.sources if item.provenance.mode == "archived_snapshot")
    if archived and archive_root is None:
        raise ResearchPackageError("ResearchPackage 含 archived_snapshot，必须提供来源归档根")
    manifests = []
    for source in package.sources:
        if source.provenance.mode == "citation_only":
            manifests.append({
                "source_id": source.source_id,
                "mode": "citation_only",
                "content_reproducible_offline": False,
                "snapshot_manifest_hash": None,
            })
            continue
        manifest = verify_source_snapshot(source, archive_root)
        manifests.append({
            "source_id": source.source_id,
            "mode": "archived_snapshot",
            "content_reproducible_offline": True,
            "snapshot_manifest_hash": manifest["manifest_hash"],
        })
    summary = {
        "source_provenance_hash": package.source_provenance_hash,
        "all_content_reproducible_offline": all(
            item["content_reproducible_offline"] for item in manifests
        ),
        "sources": manifests,
    }
    return summary


def verify_source_snapshot(
    source: PackageSource,
    archive_root: str | Path | None,
) -> dict[str, object]:
    """从受控归档根重新读取正文，拒绝摘要、路径或清单篡改。"""
    provenance = source.provenance
    if provenance.mode != "archived_snapshot" or archive_root is None:
        raise ResearchPackageError("来源不是可校验的 archived_snapshot")
    policy = PathRolePolicy()
    root = policy.resolve_root(archive_root, role="source_archive_root")
    artifact = policy.resolve_contained_path(
        allowed_root=root,
        candidate=str(provenance.snapshot_artifact_id),
        root_role="source_archive_root",
        path_role="source_snapshot_artifact",
        expected_kind="directory",
    )
    manifest_path = policy.resolve_contained_path(
        allowed_root=artifact,
        candidate=SOURCE_SNAPSHOT_MANIFEST,
        root_role="source_snapshot_artifact",
        path_role="source_snapshot_manifest",
        expected_kind="file",
    )
    snapshot_path = policy.resolve_contained_path(
        allowed_root=artifact,
        candidate=SOURCE_SNAPSHOT_FILE,
        root_role="source_snapshot_artifact",
        path_role="source_snapshot_content",
        expected_kind="file",
    )
    committed_path = policy.resolve_contained_path(
        allowed_root=artifact,
        candidate=SOURCE_SNAPSHOT_COMMITTED,
        root_role="source_snapshot_artifact",
        path_role="source_snapshot_commit_marker",
        expected_kind="file",
    )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResearchPackageError("来源快照 manifest 不是有效 JSON") from exc
    if not isinstance(manifest, dict) or set(manifest) != _MANIFEST_FIELDS:
        raise ResearchPackageError("来源快照 manifest schema 无效")
    if (
        type(manifest["byte_size"]) is not int
        or manifest["byte_size"] < 0
        or not _is_sha256(manifest["declared_reference_hash"])
    ):
        raise ResearchPackageError("来源快照 manifest 字段类型无效")
    declared_manifest_hash = manifest.pop("manifest_hash")
    computed_manifest_hash = typed_canonical_hash(manifest)
    manifest["manifest_hash"] = declared_manifest_hash
    if declared_manifest_hash != computed_manifest_hash:
        raise ResearchPackageError("来源快照 manifest hash 不一致")
    try:
        committed = committed_path.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise ResearchPackageError("来源快照提交标记不可读") from exc
    expected = {
        "contract_version": SOURCE_SNAPSHOT_VERSION,
        "artifact_id": provenance.snapshot_artifact_id,
        "source_id": source.source_id,
        "source_url": source.url,
        "source_title": source.title,
        "source_accessed_at": source.accessed_at,
        "source_license_id": source.license_id,
        "content_digest": provenance.content_digest,
        "media_type": provenance.media_type,
        "importer_id": provenance.importer_id,
        "imported_at": provenance.imported_at,
        "snapshot_file": SOURCE_SNAPSHOT_FILE,
        "manifest_hash": provenance.snapshot_manifest_hash,
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ResearchPackageError("来源快照与 ResearchPackage 声明不一致")
    if committed != declared_manifest_hash:
        raise ResearchPackageError("来源快照没有完整提交")
    content_digest, byte_size = _hash_file(snapshot_path)
    if content_digest != manifest["content_digest"] or byte_size != manifest["byte_size"]:
        raise ResearchPackageError("来源快照正文已被篡改")
    return manifest


def ingest_source_snapshot(
    *,
    package_root: str | Path,
    source_id: str,
    input_root: str | Path,
    input_file: str | Path,
    archive_root: str | Path,
    media_type: str,
    importer_id: str,
    imported_at: str | None = None,
) -> dict[str, object]:
    """只从本地受控文件导入；本模块没有 URL 获取或网络回退。"""
    policy = PathRolePolicy()
    package_directory = policy.resolve_root(package_root, role="package_output")
    source_root = policy.resolve_root(input_root, role="source_input_root")
    source_file = policy.resolve_contained_path(
        allowed_root=source_root,
        candidate=input_file,
        root_role="source_input_root",
        path_role="source_input_file",
        expected_kind="file",
    )
    archive_directory = policy.resolve_root(archive_root, role="source_archive_output")
    policy.validate(
        {
            "package_output": package_directory,
            "source_input_root": source_root,
            "source_input_file": source_file,
            "source_archive_output": archive_directory,
        },
        read_only_roles=("source_input_root", "source_input_file"),
    )
    package = load_research_package(package_directory)
    matches = [item for item in package.sources if item.source_id == source_id]
    if len(matches) != 1:
        raise ResearchPackageError("source_id 在 ResearchPackage 中不存在或不唯一")
    source = matches[0]
    if source.status != "available":
        raise ResearchPackageError("只有 available source 可以导入正文")
    if source.provenance.mode != "citation_only":
        raise ResearchPackageError("来源已经是 archived_snapshot，禁止隐式覆盖")
    imported = imported_at or datetime.now(timezone.utc).isoformat(timespec="seconds")

    digest, byte_size = _hash_file(source_file)
    artifact_id = f"source_snapshot_{hashlib.sha256(source_id.encode('utf-8')).hexdigest()[:8]}_{digest[:16]}"
    target = archive_directory / artifact_id
    temporary = archive_directory / f".{artifact_id}.tmp"
    if target.exists() or temporary.exists():
        raise ResearchPackageError("来源快照目标或临时目录已存在")
    # 先用最终值构造合同，避免在归档成功后才发现元数据无效。
    provisional = SourceProvenance(
        "archived_snapshot", digest, media_type, artifact_id, "0" * 64,
        importer_id, imported,
    )
    manifest_payload = {
        "contract_version": SOURCE_SNAPSHOT_VERSION,
        "artifact_id": artifact_id,
        "source_id": source.source_id,
        "source_url": source.url,
        "source_title": source.title,
        "source_accessed_at": source.accessed_at,
        "source_license_id": source.license_id,
        "declared_reference_hash": source.content_hash,
        "content_digest": digest,
        "byte_size": byte_size,
        "media_type": provisional.media_type,
        "importer_id": provisional.importer_id,
        "imported_at": provisional.imported_at,
        "snapshot_file": SOURCE_SNAPSHOT_FILE,
    }
    manifest_hash = typed_canonical_hash(manifest_payload)
    manifest = {**manifest_payload, "manifest_hash": manifest_hash}
    provenance = SourceProvenance(
        "archived_snapshot", digest, media_type, artifact_id, manifest_hash,
        importer_id, imported,
    )
    try:
        temporary.mkdir()
        shutil.copyfile(source_file, temporary / SOURCE_SNAPSHOT_FILE)
        copied_digest, copied_size = _hash_file(temporary / SOURCE_SNAPSHOT_FILE)
        if (copied_digest, copied_size) != (digest, byte_size):
            raise ResearchPackageError("来源文件在导入期间发生变化")
        (temporary / SOURCE_SNAPSHOT_MANIFEST).write_text(canonical_json(manifest), encoding="utf-8")
        (temporary / SOURCE_SNAPSHOT_COMMITTED).write_text(f"{manifest_hash}\n", encoding="ascii")
        os.replace(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    _replace_source_declaration(package_directory, source_id, digest, provenance)
    updated = load_research_package(package_directory)
    verification = verify_package_source_provenance(updated, archive_directory)
    return {
        "package_id": updated.package_id,
        "package_hash": updated.package_hash,
        "source_id": source_id,
        "content_digest": digest,
        "byte_size": byte_size,
        "snapshot_artifact_id": artifact_id,
        "snapshot_manifest_hash": manifest_hash,
        "source_provenance_hash": updated.source_provenance_hash,
        "source_verification": verification,
    }


def _replace_source_declaration(
    package_root: Path,
    source_id: str,
    content_digest: str,
    provenance: SourceProvenance,
) -> None:
    path = package_root / "sources" / "sources.yaml"
    original = path.read_bytes()
    payload = yaml.safe_load(original.decode("utf-8"))
    if not isinstance(payload, dict) or set(payload) != {"sources"} or not isinstance(payload["sources"], list):
        raise ResearchPackageError("sources/sources.yaml schema 无效")
    found = 0
    for raw in payload["sources"]:
        if isinstance(raw, dict) and raw.get("source_id") == source_id:
            raw["content_hash"] = content_digest
            raw["provenance"] = provenance.to_dict()
            found += 1
    if found != 1:
        raise ResearchPackageError("source_id 在 sources.yaml 中不存在或不唯一")
    temporary = path.with_name(".sources.yaml.tmp")
    if temporary.exists():
        raise ResearchPackageError("sources.yaml 临时文件已存在")
    temporary.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    try:
        os.replace(temporary, path)
        load_research_package(package_root)
    except Exception:
        path.write_bytes(original)
        temporary.unlink(missing_ok=True)
        raise


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


__all__ = [
    "SOURCE_SNAPSHOT_VERSION", "ingest_source_snapshot",
    "verify_package_source_provenance", "verify_source_snapshot",
]
