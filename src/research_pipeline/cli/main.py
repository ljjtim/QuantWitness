from __future__ import annotations

import importlib

from .commands import COMMAND_MODULES
from .parser import build_parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        if argv is not None:
            return int(exc.code)
        raise
    if args.command is None:
        parser.print_help()
        return 0
    module = importlib.import_module(
        COMMAND_MODULES[args.command]
    )
    from research_pipeline.platform.audit_mode import strict_audit_scope
    from research_pipeline.platform.directory_watch import project_bundle_check_scope
    with project_bundle_check_scope(), strict_audit_scope(getattr(args, "strict_audit", False) or args.command == "doctor"):
        if module.__name__.startswith("research_pipeline.application."):
            from .result import execute_guarded
            return execute_guarded(args, module._execute)
        return int(module.execute(args))
