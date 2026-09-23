"""独立项目 Verifier bundle 的受控子进程入口。"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import sys
from typing import Mapping

from research_pipeline.platform import canonical_json


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if len(values) != 4:
        raise SystemExit("project verifier worker 参数无效")
    bundle_root = Path(values[0]).resolve(strict=True)
    context_path = Path(values[1]).resolve(strict=True)
    input_root = Path(values[2]).resolve(strict=True)
    output_path = Path(values[3]).resolve()
    if output_path.exists() or output_path.parent != input_root.parent:
        raise SystemExit("project verifier worker 输出路径无效")
    manifest = json.loads((bundle_root / "manifest.json").read_text(encoding="utf-8"))
    context = json.loads(context_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, Mapping) or not isinstance(context, Mapping):
        raise SystemExit("project verifier worker 输入合同无效")
    entry = manifest.get("entry")
    if not isinstance(entry, Mapping):
        raise SystemExit("project verifier worker 入口合同无效")
    sys.path.insert(0, str(bundle_root / "sources"))
    module = importlib.import_module(str(entry["module"]))
    verifier = getattr(module, str(entry["function"]))
    outcome = verifier(dict(context), input_root)
    if not isinstance(outcome, Mapping):
        raise SystemExit("project verifier 必须返回映射")
    output_path.write_text(canonical_json(dict(outcome)), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
