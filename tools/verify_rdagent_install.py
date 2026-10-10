"""在已安装的隔离环境验证公开公式案例，不读取数据库或调用模型。"""
from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys


def verify_install(project: Path, output: Path) -> dict[str, object]:
    """公开源码只提供示例文件，两个业务包必须来自当前虚拟环境。"""
    import research_pipeline
    import quantwitness_rdagent

    prefix = Path(sys.prefix).resolve()
    if sys.prefix == sys.base_prefix:
        raise ValueError("验收必须使用独立虚拟环境")
    configuration = (prefix / "pyvenv.cfg").read_text(encoding="utf-8").lower()
    if "include-system-site-packages = false" not in configuration:
        raise ValueError("验收环境不得继承系统 site-packages")
    origins = {}
    for module in (research_pipeline, quantwitness_rdagent):
        origin = Path(module.__file__).resolve()
        if not origin.is_relative_to(prefix):
            raise ValueError(f"{module.__name__} 未从当前虚拟环境安装目录导入")
        origins[module.__name__] = str(origin)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project = project.resolve(strict=True)
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    commands = []

    def command(name, arguments, stdin=None):
        completed = subprocess.run(
            [sys.executable, "-I", "-X", "utf8", *arguments], input=stdin, text=True,
            encoding="utf-8", capture_output=True, cwd=output, env=environment,
            check=False,
        )
        (output / (name + ".stdout.txt")).write_text(completed.stdout, encoding="utf-8")
        (output / (name + ".stderr.txt")).write_text(completed.stderr, encoding="utf-8")
        commands.append({"name": name, "exit_code": completed.returncode})
        if completed.returncode:
            raise RuntimeError(f"{name} 失败，详见 {output / (name + '.stderr.txt')}")
        return completed

    command("dependencies", ["-m", "pip", "check"])
    command("environment", ["-m", "pip", "freeze"])
    command("core-help", ["-m", "research_pipeline", "--help"])
    command("integration-help", ["-m", "quantwitness_rdagent", "--help"])

    # 示例定义留在公开源码中；build 不向 sys.path 添加框架源码。
    location = project / "integrations/rdagent/examples/volume_concentration/prepare.py"
    spec = importlib.util.spec_from_file_location("public_volume_prepare", location)
    if spec is None or spec.loader is None:
        raise ValueError("缺少公开成交量集中度示例")
    preparation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(preparation)
    case = output / "example"
    preparation.build(case, project, sys.executable, str(project), str(case))
    decisions = json.loads((case / "decisions-template.json").read_text(encoding="utf-8"))
    decisions.update(accepted_rule_ids=["teaching_definition"],
                     review_notes="synthetic-test-fixture：已知教学公式的自动化安装验收。")
    (case / "decisions.json").write_text(json.dumps(decisions, ensure_ascii=False), encoding="utf-8")
    review = ["--draft", str(case / "draft.json"), "--materials", str(case / "materials.json"),
              "--decisions", str(case / "decisions.json")]
    command("spec-confirm", ["-m", "quantwitness_rdagent", "spec-confirm", *review,
            "--confirmed-by", "synthetic-test-fixture", "--approve", "--output", str(case / "confirmation.json")])
    command("request-build", ["-m", "quantwitness_rdagent", "request-build", *review,
            "--template", str(case / "request-template.json"), "--confirmation", str(case / "confirmation.json"),
            "--output", str(case / "request.json")])
    from quantwitness_rdagent.contracts import FrozenRequest
    from quantwitness_rdagent.execution import RPExecutionBridge
    request = FrozenRequest.load(case / "request.json")
    bridge = RPExecutionBridge(request)
    records = []
    for index, source in enumerate(request.payload["runtime_binding"]["fixed_responses"]):
        candidate = bridge.candidate(source)
        command(f"candidate-{index}", ["-m", "quantwitness_rdagent.worker"],
                json.dumps({"request": request.payload, "candidate_id": candidate}))
        feedback = json.loads((request.session_root / "candidates" / candidate / "feedback.json").read_text(encoding="utf-8"))
        expected = "fail" if index == 0 else "pass"
        if (feedback["command_status"] != "succeeded" or feedback["execution_status"] != "succeeded"
                or feedback["verification_status"] != expected
                or feedback["formula_status"] != expected):
            raise ValueError(f"{candidate} 未得到预期的独立公式验证结论：{feedback}")
        coverage = feedback["formula_coverage"]
        if coverage.get("raw_rows") != 2400 or coverage.get("entity_sessions") != 10:
            raise ValueError("公开示例的正式公式覆盖范围不完整")
        records.append({"candidate_id": candidate, **feedback})
    result = {
        "status": "pass", "python": sys.version, "platform": sys.platform,
        "prefix": str(prefix), "import_origins": origins,
        "installed_versions": {name: importlib.metadata.version(name) for name in ("quantwitness", "quantwitness-rdagent")},
        "commands": commands, "candidates": records, "raw_rows": 2400,
        "input_kind": "synthetic_parquet", "model_calls": 0,
        "confirmation_kind": "synthetic-test-fixture", "rd_loop_exercised": False,
    }
    (output / "acceptance.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = verify_install(args.project, args.output)
    print(json.dumps({"status": result["status"], "receipt": str(args.output / "acceptance.json")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
