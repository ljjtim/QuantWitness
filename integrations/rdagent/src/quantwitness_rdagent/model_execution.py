"""每轮在独立进程委托同一PackageCampaign，释放模型训练内存。"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from .contracts import write_json
from .factor_research import _freeze


def evaluate_model_package(payload):
    root = Path(payload["session_root"])
    request = root / "model-execution-request.json"
    output = root / "model-execution-metrics.json"
    _freeze(request, payload)
    if not output.exists():
        with (root / "model-execution.log").open("a", encoding="utf-8") as stream:
            completed = subprocess.run([sys.executable, "-B", "-m", "quantwitness_rdagent.model_execution",
                "--request", str(request), "--output", str(output)], stdout=stream, stderr=subprocess.STDOUT)
        if completed.returncode:
            raise RuntimeError("模型正式执行尚未完成；查看model-execution.log后恢复同一会话")
    return json.loads(output.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    from .package_campaign import PackageCampaign
    session = PackageCampaign(json.loads(Path(args.request).read_text(encoding="utf-8")))
    metrics = session.evaluate_metrics(session.payload["candidates"][0])
    write_json(args.output, metrics)


if __name__ == "__main__":
    main()
