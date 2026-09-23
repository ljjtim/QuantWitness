"""独立复核已封存 Result 中显式准入的项目 Verifier bundle。"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Mapping

from research_pipeline.extensions.verifier_bundle import (
    ProjectVerifierBundleManifest,
    verify_project_verifier_bundle,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.results import ResultSnapshot
from research_pipeline.results.errors import ResultContractError


PROJECT_VERIFIER_OUTPUT_VERSION = "project-verifier-output-v1"


@dataclass(frozen=True)
class ProjectVerifierOutcome:
    verifier_identity: Mapping[str, object]
    result_id: str
    status: str
    findings: tuple[str, ...]
    evidence_hashes: Mapping[str, str]
    outcome_hash: str
    contract_version: str = PROJECT_VERIFIER_OUTPUT_VERSION

    def __post_init__(self) -> None:
        if self.status not in {"pass", "fail"}:
            raise ResultContractError("项目 Verifier status 无效")
        if tuple(sorted(set(self.findings))) != self.findings:
            raise ResultContractError("项目 Verifier findings 必须唯一并排序")
        hashes = dict(sorted(self.evidence_hashes.items()))
        if any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for key, value in hashes.items()
        ):
            raise ResultContractError("项目 Verifier evidence hash 无效")
        object.__setattr__(self, "evidence_hashes", hashes)
        if self.outcome_hash != typed_canonical_hash(self.payload()):
            raise ResultContractError("项目 Verifier outcome hash 不一致")

    def payload(self) -> dict[str, object]:
        return {
            "verifier_identity": dict(self.verifier_identity),
            "result_id": self.result_id,
            "status": self.status,
            "findings": list(self.findings),
            "evidence_hashes": dict(self.evidence_hashes),
            "contract_version": self.contract_version,
        }


def execute_project_verifier(
    *,
    bundle_path: str | Path,
    snapshot: ResultSnapshot,
    expected_identity: Mapping[str, object],
    scratch_root: str | Path | None = None,
) -> ProjectVerifierOutcome:
    """仅向项目 Verifier 暴露 Result 冻结身份中授权的表和支持工件。"""
    manifest = verify_project_verifier_bundle(bundle_path)
    if manifest.identity() != dict(expected_identity):
        raise ResultContractError("项目 Verifier bundle 与 Result 冻结身份不一致")
    # Result 的 project_id 是内容寻址的 Package ID；逻辑项目身份由冻结的
    # verifier_identity 绑定，不把两种不同身份混为一谈。
    available_schemas = {item.schema_id for item in snapshot.bundle.tables}
    if not set(manifest.authorized_schema_ids) <= available_schemas:
        raise ResultContractError("项目 Verifier 授权表不属于当前 Result")
    support_by_path = {
        item.source_path: item
        for item in snapshot.bundle.support_files
        if item.artifact_type in manifest.authorized_support_artifact_types
    }
    if set(manifest.authorized_support_artifact_types) - {
        item.artifact_type for item in support_by_path.values()
    }:
        raise ResultContractError("项目 Verifier 授权支持工件不属于当前 Result")
    missing_support = set(support_by_path) - set(snapshot.support_bytes)
    if missing_support:
        raise ResultContractError("项目 Verifier 授权支持工件未进入已验证快照")

    scratch_parent = None if scratch_root is None else str(Path(scratch_root).resolve())
    with tempfile.TemporaryDirectory(prefix="rp-project-verifier-", dir=scratch_parent) as raw:
        root = Path(raw)
        input_root = root / "input"
        input_root.mkdir()
        table_entries = []
        for schema_id in manifest.authorized_schema_ids:
            table = snapshot.table_manifest(schema_id)
            table_root = input_root / "tables" / schema_id
            table_root.mkdir(parents=True)
            files = []
            for index, relative_path in enumerate(sorted(table.files)):
                source = snapshot.directory / relative_path
                target = table_root / f"part-{index:05d}.parquet"
                shutil.copyfile(source, target)
                files.append(target.relative_to(input_root).as_posix())
            table_entries.append({
                "schema_id": schema_id,
                "artifact_type": table.artifact_type,
                "files": files,
            })
        support_entries = []
        for index, (source_path, item) in enumerate(sorted(support_by_path.items())):
            target = input_root / "support" / f"item-{index:05d}.bin"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(snapshot.support_bytes[source_path])
            support_entries.append({
                "artifact_type": item.artifact_type,
                "source_path": source_path,
                "file": target.relative_to(input_root).as_posix(),
                "content_hash": item.content_hash,
            })
        input_manifest = {
            "contract_version": "project-verifier-input-v1",
            "result_id": snapshot.bundle.result_id,
            "tables": table_entries,
            "support_files": support_entries,
        }
        (input_root / "manifest.json").write_text(
            canonical_json(input_manifest), encoding="utf-8"
        )
        context = {
            "contract_version": "project-verifier-context-v1",
            "project_id": manifest.project_id,
            "result_project_id": snapshot.bundle.project_id,
            "result_id": snapshot.bundle.result_id,
            "package_hash": snapshot.bundle.package_hash,
            "plan_hash": snapshot.bundle.plan_hash,
            "verifier_identity": manifest.identity(),
        }
        context_path = root / "context.json"
        context_path.write_text(canonical_json(context), encoding="utf-8")
        output_path = root / "outcome.json"
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "research_pipeline.evidence.project_verifier_worker",
                    str(Path(bundle_path).resolve()),
                    str(context_path),
                    str(input_root),
                    str(output_path),
                ],
                check=True,
                env=environment,
                cwd=root,
                timeout=300,
                capture_output=True,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise ResultContractError("项目 Verifier 执行失败或超时") from exc
        try:
            raw_outcome = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ResultContractError("项目 Verifier 未生成有效输出") from exc
    return _parse_outcome(raw_outcome, manifest, snapshot.bundle.result_id)


def _parse_outcome(
    payload: object,
    manifest: ProjectVerifierBundleManifest,
    result_id: str,
) -> ProjectVerifierOutcome:
    expected = {"contract_version", "status", "result_id", "findings", "evidence_hashes"}
    if not isinstance(payload, Mapping) or set(payload) != expected:
        raise ResultContractError("项目 Verifier 输出 envelope 无效")
    if payload["contract_version"] != PROJECT_VERIFIER_OUTPUT_VERSION:
        raise ResultContractError("项目 Verifier 输出版本不受支持")
    if payload["result_id"] != result_id:
        raise ResultContractError("项目 Verifier 输出错绑 Result")
    findings = payload["findings"]
    evidence = payload["evidence_hashes"]
    if (
        not isinstance(findings, list)
        or any(not isinstance(item, str) or not item for item in findings)
        or not isinstance(evidence, Mapping)
    ):
        raise ResultContractError("项目 Verifier 输出事实无效")
    values = {
        "verifier_identity": manifest.identity(),
        "result_id": result_id,
        "status": str(payload["status"]),
        "findings": tuple(findings),
        "evidence_hashes": {str(key): str(value) for key, value in evidence.items()},
        "contract_version": PROJECT_VERIFIER_OUTPUT_VERSION,
    }
    return ProjectVerifierOutcome(
        **values,
        outcome_hash=typed_canonical_hash(values),
    )


__all__ = [
    "PROJECT_VERIFIER_OUTPUT_VERSION",
    "ProjectVerifierOutcome",
    "execute_project_verifier",
]
