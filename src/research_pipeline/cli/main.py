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
    return int(module.execute(args))
