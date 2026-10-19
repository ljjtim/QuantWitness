"""VerificationResult 报告的规范输出。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from research_pipeline.platform.exclusive_output import write_text_exclusive_atomic


REPORT_DOCUMENT_VERSION = "research-verification-report-v1"


def write_verification_report(
    destination: str | Path,
    *,
    output_format: str,
    markdown: str,
    report: Mapping[str, object],
) -> Path:
    """在目标同目录原子发布报告，且绝不覆盖既有文件。"""

    if output_format not in {"markdown", "json"}:
        raise ValueError("报告格式仅支持 markdown 或 json")
    if output_format == "markdown":
        content = markdown.rstrip("\n") + "\n"
    else:
        content = json.dumps(
            {
                "contract_version": REPORT_DOCUMENT_VERSION,
                "report": dict(report),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
    try:
        return write_text_exclusive_atomic(destination, content)
    except FileExistsError as exc:
        raise FileExistsError(f"报告输出已存在: {Path(destination).resolve()}") from exc


__all__ = ["REPORT_DOCUMENT_VERSION", "write_verification_report"]


def prepare_qlib_request(args):
    """在读取 Result 前检查 HTML 参数，并确定需要验证的表。"""
    request_path = getattr(args, "request", None)
    if args.format == "html":
        if not args.output or not request_path:
            raise ValueError("HTML 报告必须给 --output 和 --request")
        from research_pipeline.evidence.qlib_report import load_report_request

        return load_report_request(request_path)
    if request_path:
        raise ValueError("--request 仅适用于 --format html")
    return None


def report_table_ids(request):
    """向同一 Result 消费门禁声明报告所需的正式表。"""
    if request is None:
        return ()
    if "tables" in request:
        return tuple(request["tables"].values())
    return (request["table_id"],)


def write_qlib_report(destination, *, context, request, markdown):
    """只生成新的报告文件，不覆盖 Result 或已有报告。"""
    from research_pipeline.evidence.qlib_report import render_qlib_report

    output = Path(destination).resolve()
    if output.exists():
        raise FileExistsError(f"报告输出已存在: {output}")
    return write_text_exclusive_atomic(
        output, render_qlib_report(context, request, verification_summary=markdown)
    )
