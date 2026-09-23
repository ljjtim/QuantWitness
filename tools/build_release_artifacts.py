"""从统一 allowlist 的仓库外 staging 构建 wheel、sdist 和可测试源码包。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from release_allowlist import (
    UNIT_CORE_TEST_FILES,
    copy_paths,
    core_release_paths,
    verify_sdist_inventory,
    verify_source_archive,
    write_source_archive,
)
from wheel_source_inventory import verify_wheel_source_inventory
from release_metadata import verify_distribution_metadata


def _build_distributions(staging: Path, output: Path) -> tuple[Path, Path]:
    """离线调用标准 PEP 517 frontend，由 pyproject 中的 backend 生成元数据。"""
    environment = {**os.environ, "PIP_NO_INDEX": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1"}
    completed = subprocess.run(
        [sys.executable, "-m", "build", "--no-isolation", "--wheel", "--sdist",
         "--outdir", str(output), str(staging)],
        cwd=staging,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "离线 PEP 517 构建失败；请预先安装 pyproject 的构建依赖及 build frontend：\n"
            + completed.stdout + completed.stderr
        )
    wheels = tuple(sorted(output.glob("*.whl")))
    sdists = tuple(sorted(output.glob("*.tar.gz")))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError("标准 backend 必须生成唯一 wheel 和 sdist")
    return wheels[0], sdists[0]


def build_release_artifacts(*, project: Path, output: Path) -> dict[str, object]:
    project = project.resolve(strict=True)
    destination = output.resolve()
    if destination.exists():
        raise ValueError("发布产物输出目录必须不存在")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging_output = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent)
    )
    try:
        with tempfile.TemporaryDirectory(prefix="research-pipeline-release-") as temporary:
            staging = Path(temporary) / "source"
            copy_paths(project, staging, core_release_paths(project))
            distribution_dir = Path(temporary) / "distribution"
            distribution_dir.mkdir()
            wheel, sdist = _build_distributions(staging, distribution_dir)
            verify_wheel_source_inventory(project=staging, wheel=wheel)
            verify_sdist_inventory(staging, sdist)
            verify_distribution_metadata(project=staging, wheel=wheel, sdist=sdist)

            copy_paths(project, staging, UNIT_CORE_TEST_FILES)
            source_archive = Path(temporary) / "research_pipeline-source.zip"
            write_source_archive(staging, source_archive)
            verify_source_archive(staging, source_archive)

            wheel_target = staging_output / wheel.name
            sdist_target = staging_output / sdist.name
            source_target = staging_output / source_archive.name
            shutil.copyfile(wheel, wheel_target)
            shutil.copyfile(sdist, sdist_target)
            shutil.copyfile(source_archive, source_target)
        os.replace(staging_output, destination)
    except BaseException:
        shutil.rmtree(staging_output, ignore_errors=True)
        raise
    return {
        "status": "pass",
        "wheel": str(destination / wheel_target.name),
        "sdist": str(destination / sdist_target.name),
        "source_archive": str(destination / source_target.name),
        "unit_core_tests": list(UNIT_CORE_TEST_FILES),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="从统一 allowlist 构建三类正式产物")
    parser.add_argument("--project", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = build_release_artifacts(
        project=Path(args.project),
        output=Path(args.output),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
