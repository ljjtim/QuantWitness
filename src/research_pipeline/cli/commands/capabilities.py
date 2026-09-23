"""能力发现命令。"""

from __future__ import annotations

from research_pipeline.operations.capabilities import discovery_payload, render_capabilities_text

from ..result import write_machine_json


def execute(args) -> int:
    payload = discovery_payload()
    if args.format == "json":
        write_machine_json(payload)
    else:
        print(render_capabilities_text(payload))
    return 0


__all__ = ["execute"]
