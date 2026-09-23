"""从 clean RC 和 Gate 证据闭包生成 local ReleaseEnvelope。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from research_pipeline.platform import (  # noqa: E402
    REQUIRED_GATE_IDS,
    ReleaseAcceptanceInput,
    ReleaseEnvelope,
    ReleaseGateReceipt,
    canonical_json,
    load_build_manifest,
    verify_dependency_distribution_lock,
    verify_release_envelope,
)


def _sha256(path: Path) -> str:
    if not path.is_file():
        raise ValueError(f"证据文件不存在: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_pass_evidence(
    path: Path,
    *,
    release_candidate_id: str,
    build_manifest_hash: str,
) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Gate evidence 必须是 JSON: {path}") from exc
    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("contract_version"), str)
        or payload.get("status") != "pass"
        or payload.get("evidence_scope") != "release_candidate"
        or payload.get("release_candidate_id") != release_candidate_id
        or payload.get("build_manifest_hash") != build_manifest_hash
    ):
        raise ValueError(f"Gate evidence 未绑定当前 clean RC 或未明确通过: {path}")
    return _sha256(path)


def _gate_paths(values: list[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for value in values:
        gate_id, separator, raw_path = value.partition("=")
        if not separator or gate_id in result:
            raise ValueError("--gate-evidence 必须是唯一 gate-id=path")
        result[gate_id] = Path(raw_path).resolve()
    if tuple(sorted(result)) != REQUIRED_GATE_IDS:
        raise ValueError(f"Gate evidence 必须完整覆盖: {list(REQUIRED_GATE_IDS)}")
    return result


def build_release_envelope_files(
    *,
    candidate_id: str,
    manifest_path: Path,
    capabilities_path: Path,
    dependency_lock_path: Path,
    gate_paths: dict[str, Path],
    output: Path,
    issued_at: str,
    expires_at: str,
) -> ReleaseEnvelope:
    if output.exists():
        raise ValueError("ReleaseEnvelope 输出目录必须不存在")
    manifest = load_build_manifest(manifest_path)
    try:
        lock = json.loads(dependency_lock_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("依赖 lock 无法读取") from exc
    if not isinstance(lock, dict) or set(lock) != {
        "contract_version", "platform", "python_cache_tag", "distributions",
    }:
        raise ValueError("依赖 lock schema 无效")
    verified_dependencies = verify_dependency_distribution_lock(dependency_lock_path)
    if dict(manifest.dependency_distribution_digests) != dict(verified_dependencies):
        raise ValueError("BuildManifest 与当前依赖 distribution lock 不一致")

    receipts = tuple(
        ReleaseGateReceipt.build(
            gate_id=gate_id,
            release_candidate_id=candidate_id,
            build_manifest_hash=manifest.manifest_hash,
            evidence_hashes={
                "acceptance": _load_pass_evidence(
                    gate_paths[gate_id],
                    release_candidate_id=candidate_id,
                    build_manifest_hash=manifest.manifest_hash,
                )
            },
            issued_at=issued_at,
            expires_at=expires_at,
        )
        for gate_id in REQUIRED_GATE_IDS
    )
    acceptance = ReleaseAcceptanceInput.build(
        release_candidate_id=candidate_id,
        build_manifest_hash=manifest.manifest_hash,
        receipts=receipts,
    )
    envelope = ReleaseEnvelope.build(
        release_candidate_id=candidate_id,
        profile="local",
        manifest=manifest,
        capabilities_digest=_sha256(capabilities_path),
        dependency_lock_digest=_sha256(dependency_lock_path),
        platform=str(lock["platform"]),
        python_cache_tag=str(lock["python_cache_tag"]),
        receipts=receipts,
        acceptance=acceptance,
        issued_at=issued_at,
        expires_at=expires_at,
        revocation_policy="not_applicable_no_distribution",
    )
    verify_release_envelope(
        envelope,
        manifest=manifest,
        receipts=receipts,
        acceptance=acceptance,
        as_of=issued_at,
    )

    output.mkdir(parents=True)
    receipts_dir = output / "receipts"
    receipts_dir.mkdir()
    for receipt in receipts:
        (receipts_dir / f"{receipt.gate_id}.json").write_text(
            canonical_json(receipt.to_dict()),
            encoding="utf-8",
        )
    (output / "acceptance-input.json").write_text(
        canonical_json(acceptance.to_dict()),
        encoding="utf-8",
    )
    (output / "release-envelope.json").write_text(
        canonical_json(envelope.to_dict()),
        encoding="utf-8",
    )
    return envelope


def main() -> int:
    parser = argparse.ArgumentParser(description="生成 local ReleaseEnvelope")
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--build-manifest", required=True)
    parser.add_argument("--capabilities", required=True)
    parser.add_argument("--dependency-lock", required=True)
    parser.add_argument("--gate-evidence", action="append", default=[])
    parser.add_argument("--issued-at", required=True)
    parser.add_argument("--expires-at", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    envelope = build_release_envelope_files(
        candidate_id=args.candidate_id,
        manifest_path=Path(args.build_manifest).resolve(),
        capabilities_path=Path(args.capabilities).resolve(),
        dependency_lock_path=Path(args.dependency_lock).resolve(),
        gate_paths=_gate_paths(args.gate_evidence),
        output=Path(args.output).resolve(),
        issued_at=args.issued_at,
        expires_at=args.expires_at,
    )
    print(json.dumps({
        "status": "pass",
        "release_candidate_id": envelope.release_candidate_id,
        "release_envelope": str(Path(args.output).resolve() / "release-envelope.json"),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
