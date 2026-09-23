"""同盘 staging、原子 checkpoint 提交与分层身份验证。"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Callable, Mapping

from research_pipeline.platform.canonical import canonical_json

from .artifacts import CheckpointExpectation, CheckpointManifest
from .contracts import ArtifactRef
from .errors import RuntimeIntegrityError


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class CheckpointStore:
    def __init__(self, run_root: str | Path, *, create: bool = True) -> None:
        self.run_root = Path(run_root).resolve()
        self.checkpoints_root = self.run_root / "checkpoints"
        self.staging_root = self.run_root / "staging"
        if create:
            self.checkpoints_root.mkdir(parents=True, exist_ok=True)
            self.staging_root.mkdir(parents=True, exist_ok=True)
        elif not self.checkpoints_root.is_dir():
            raise RuntimeIntegrityError("只读 checkpoint store 缺少 checkpoints 目录")

    def commit_bytes(
        self,
        *,
        expectation: CheckpointExpectation,
        attempt_id: str,
        content: bytes,
        outputs: Mapping[str, ArtifactRef] | None = None,
        output_name: str | None = None,
        output_type: str | None = None,
        audit_environment_digest: str,
        execution_identity_digest: str,
        root_seed: int,
        fixed_clock: str,
        partition_key: str | None = None,
        phase_hook: Callable[[str], None] | None = None,
    ) -> CheckpointManifest:
        node_execution_id = expectation.node_execution_id
        if any(token in node_execution_id for token in ("/", "\\")) or node_execution_id in {".", ".."}:
            raise RuntimeIntegrityError("checkpoint key 不是安全路径组件")
        target = self.checkpoints_root / node_execution_id
        content_hash = _sha256(content)
        if outputs is None:
            if output_name is None or output_type is None:
                raise RuntimeIntegrityError("checkpoint 必须声明完整 outputs")
            outputs = {
                output_name: ArtifactRef(
                    output_name,
                    output_type,
                    node_execution_id,
                    content_hash,
                )
            }
        elif output_name is not None or output_type is not None:
            raise RuntimeIntegrityError("checkpoint outputs 与单输出参数不能混用")
        manifest = CheckpointManifest(
            node_execution_id,
            attempt_id,
            expectation.inputs,
            outputs,
            expectation.implementation_id,
            expectation.implementation_digest,
            expectation.operator_definition_digest,
            expectation.cache_profile_digest,
            audit_environment_digest,
            execution_identity_digest,
            root_seed,
            fixed_clock,
            partition_key,
            len(content),
            content_hash,
        )
        if target.exists():
            existing = self.verify(expectation)
            comparable_existing = existing.identity_payload()
            comparable_new = manifest.identity_payload()
            comparable_existing["attempt_id"] = comparable_new["attempt_id"]
            if existing.content_hash != content_hash or comparable_existing != comparable_new:
                raise RuntimeIntegrityError("同 checkpoint key 出现冲突内容")
            return existing
        stage = self.staging_root / attempt_id
        if stage.exists():
            shutil.rmtree(stage)
        stage.mkdir(parents=False)
        content_path = stage / "content.bin"
        with content_path.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        with (stage / "manifest.json").open("w", encoding="utf-8") as handle:
            handle.write(canonical_json(manifest.to_dict()))
            handle.flush()
            os.fsync(handle.fileno())
        if phase_hook:
            phase_hook("checkpoint_prepared")
        os.replace(stage, target)
        if phase_hook:
            phase_hook("renamed")
        marker = target / "COMMITTED"
        with marker.open("w", encoding="ascii") as handle:
            handle.write(manifest.manifest_hash)
            handle.flush()
            os.fsync(handle.fileno())
        if phase_hook:
            phase_hook("marker_fsynced")
        return self.verify(expectation)

    def verify(
        self,
        expectation: CheckpointExpectation,
    ) -> CheckpointManifest:
        manifest = self.verify_stored(
            expectation.node_execution_id,
        )
        manifest.require_expectation(expectation)
        return manifest

    def verify_stored(
        self,
        node_execution_id: str,
    ) -> CheckpointManifest:
        """只验证已保存对象的自洽性；恢复和缓存命中必须调用 `verify`。"""
        target = (self.checkpoints_root / node_execution_id).resolve()
        try:
            target.relative_to(self.checkpoints_root.resolve())
        except ValueError as exc:
            raise RuntimeIntegrityError("checkpoint 路径越界") from exc
        marker = target / "COMMITTED"
        if not marker.is_file():
            raise RuntimeIntegrityError("checkpoint 缺 committed marker")
        try:
            payload = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
            manifest = CheckpointManifest.from_dict(payload)
        except RuntimeIntegrityError:
            raise
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("checkpoint manifest 无法读取") from exc
        if manifest.node_execution_id != node_execution_id:
            raise RuntimeIntegrityError("checkpoint 身份非法")
        content_path = (target / manifest.content_path).resolve()
        try:
            content_path.relative_to(target)
        except ValueError as exc:
            raise RuntimeIntegrityError("checkpoint 内容路径越界") from exc
        content = content_path.read_bytes()
        if len(content) != manifest.content_size or _sha256(content) != manifest.content_hash:
            raise RuntimeIntegrityError("checkpoint 内容 hash 或大小不一致")
        if marker.read_text(encoding="ascii") != manifest.manifest_hash:
            raise RuntimeIntegrityError("checkpoint marker 与 manifest 不一致")
        return manifest

    def list_uncommitted_staging(self) -> tuple[Path, ...]:
        return tuple(sorted(path for path in self.staging_root.iterdir() if path.is_dir()))

__all__ = ["CheckpointStore"]
