"""离线生成 wheel/sdist 与依赖 distribution 的 BuildManifest。"""

from __future__ import annotations

import argparse
from importlib import metadata
import json
from pathlib import Path
import sys

from release_evidence_binding import (
    build_input_digests,
    file_sha256,
    source_identity,
)
from wheel_source_inventory import verify_wheel_source_inventory
from release_allowlist import verify_sdist_inventory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from research_pipeline.platform import (  # noqa: E402
    BuildManifest,
    canonical_json,
    typed_canonical_hash,
    verify_dependency_distribution_lock,
)


def _toolchain(*, setuptools_version: str | None, wheel_version: str | None) -> dict[str, str]:
    result = {"python": sys.version.split()[0]}
    overrides = {"setuptools": setuptools_version, "wheel": wheel_version}
    for name in ("build", "setuptools", "wheel"):
        if overrides.get(name):
            result[name] = str(overrides[name])
            continue
        try:
            result[name] = metadata.version(name)
        except metadata.PackageNotFoundError as exc:
            raise ValueError(f"构建工具 distribution 未安装: {name}") from exc
    return result


def build_manifest(
    *,
    project: Path,
    wheel: Path,
    sdist: Path,
    dependency_lock: Path,
    setuptools_version: str | None = None,
    wheel_version: str | None = None,
    require_clean_source: bool = False,
) -> BuildManifest:
    for path, label in ((wheel, "wheel"), (sdist, "sdist"), (dependency_lock, "dependency lock")):
        if not path.is_file():
            raise ValueError(f"{label} 不存在: {path}")
    verify_wheel_source_inventory(project=project, wheel=wheel)
    verify_sdist_inventory(project, sdist)
    inputs = build_input_digests(project, dependency_lock)
    source_commit, source_dirty = source_identity(project)
    if require_clean_source and source_dirty:
        raise ValueError("release candidate 必须来自 clean source；当前 source_dirty=true")
    dependencies = verify_dependency_distribution_lock(dependency_lock)
    return BuildManifest.build(
        source_commit=source_commit,
        source_dirty=source_dirty,
        source_tree_digest=typed_canonical_hash(inputs),
        build_toolchain=_toolchain(
            setuptools_version=setuptools_version,
            wheel_version=wheel_version,
        ),
        input_digests=inputs,
        wheel_digest=file_sha256(wheel),
        sdist_digest=file_sha256(sdist),
        dependency_distribution_digests=dependencies,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="离线生成 research_pipeline BuildManifest")
    parser.add_argument("--project", default=str(PROJECT_ROOT))
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--sdist", required=True)
    parser.add_argument("--dependency-lock", default=str(PROJECT_ROOT / "release/dependency-distributions.json"))
    parser.add_argument("--setuptools-version", help="离线构建实际使用的 setuptools 版本")
    parser.add_argument("--wheel-version", help="离线构建实际使用的 wheel 版本")
    parser.add_argument(
        "--release-candidate",
        action="store_true",
        help="按 RC 规则构建：要求 clean source 且输出位于 release-candidate 目录",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    if output.exists():
        raise ValueError("BuildManifest 输出已存在")
    if args.release_candidate and "release-candidate" not in output.parts:
        raise ValueError("release candidate BuildManifest 必须写入 release-candidate 目录")
    manifest = build_manifest(
        project=Path(args.project).resolve(),
        wheel=Path(args.wheel).resolve(),
        sdist=Path(args.sdist).resolve(),
        dependency_lock=Path(args.dependency_lock).resolve(),
        setuptools_version=args.setuptools_version,
        wheel_version=args.wheel_version,
        require_clean_source=args.release_candidate,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        handle.write(canonical_json(manifest.to_dict()))
    print(json.dumps({"manifest": str(output), "manifest_hash": manifest.manifest_hash}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
