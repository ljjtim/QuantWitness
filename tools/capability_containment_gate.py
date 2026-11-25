"""只读生成或校验整改能力基线。"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parent
SOURCE_ROOT = PROJECT_ROOT / "src"
for location in (PROJECT_ROOT, SOURCE_ROOT):
    if str(location) not in sys.path:
        sys.path.insert(0, str(location))

from tools.capability_policy import (  # noqa: E402
    build_evidence_checklist,
    evaluate_capability_containment,
    evaluate_public_capability_release,
    load_remediation_baseline,
    refresh_remediation_baseline,
)
from research_pipeline.platform import canonical_json  # noqa: E402


DEFAULT_BASELINE = PROJECT_ROOT / "release" / "remediation-baseline" / "baseline.json"


def _configure_utf8_streams() -> None:
    """让 argparse 与普通文本在 Windows 旧代码页下保持 UTF-8。"""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="strict")


def main(argv: list[str] | None = None) -> int:
    _configure_utf8_streams()
    parser = argparse.ArgumentParser(description="只读检查 capability 整改基线")
    parser.add_argument(
        "--mode",
        choices=("check", "public-check", "snapshot", "evidence-checklist"),
        default="check",
    )
    parser.add_argument("--baseline")
    parser.add_argument("--release-evidence-root", help="独立发布证据目录；仅供 check/public-check 消费")
    parser.add_argument("--release-candidate-id", help="本次验收要求的候选 ID")
    parser.add_argument("--as-of", help="显式复验时刻，默认当前 UTC 时间")
    parser.add_argument(
        "--promotion-project", action="append", default=[], metavar="ID=PATH",
        help="晋级复用项目的显式目录；可重复传入",
    )
    parser.add_argument(
        "--promotion-evidence-root", help="晋级 oracle/攻击测试所在的目录",
    )
    args = parser.parse_args(argv)
    if args.mode != "snapshot" and (args.promotion_project or args.promotion_evidence_root):
        parser.error("晋级证据参数仅适用于 --mode snapshot")
    projects = {}
    for entry in args.promotion_project:
        project_id, separator, path = entry.partition("=")
        if not separator or not project_id or not path or project_id in projects:
            parser.error("--promotion-project 应为不重复的 ID=PATH")
        projects[project_id] = path
    if args.mode not in {"check", "public-check"} and (
        args.release_evidence_root or args.release_candidate_id or args.as_of
    ):
        parser.error("发布证据参数仅适用于 check/public-check")
    baseline_path = args.baseline or (None if args.mode == "public-check" else DEFAULT_BASELINE)
    baseline = load_remediation_baseline(baseline_path) if baseline_path is not None else None
    if args.mode == "snapshot":
        payload = refresh_remediation_baseline(
            REPOSITORY_ROOT, baseline,
            promotion_project_roots=projects or None,
            promotion_evidence_root=args.promotion_evidence_root,
        )
        code = 0
    elif args.mode == "evidence-checklist":
        payload = build_evidence_checklist(baseline)
        code = 0
    elif args.mode == "public-check":
        payload = evaluate_public_capability_release(
            project_root=PROJECT_ROOT, baseline=baseline,
            release_evidence_root=args.release_evidence_root,
            release_candidate_id=args.release_candidate_id, as_of=args.as_of,
        )
        code = 0 if payload["status"] == "pass" else 1
    else:
        payload = evaluate_capability_containment(
            repository_root=REPOSITORY_ROOT,
            baseline=baseline,
            release_evidence_root=args.release_evidence_root,
            release_candidate_id=args.release_candidate_id, as_of=args.as_of,
        )
        code = 0 if payload["status"] == "pass" else 1
    print(canonical_json(payload))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
