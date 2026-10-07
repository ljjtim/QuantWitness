"""可选集成入口；inspect 不加载 RD 依赖。"""
import argparse
import asyncio
import json
import os
from pathlib import Path
from .contracts import FrozenRequest
from .generation import model_environment


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "inspect", "resume"):
        command = commands.add_parser(name)
        command.add_argument("--request", required=True)
        command.add_argument("--model-env-file", help="live生成使用的仓库外.env文件路径")
    for name in ("campaign-run", "campaign-resume", "campaign-inspect"):
        command = commands.add_parser(name, help="有界开发区研究：预测校准或正式研究包执行")
        command.add_argument("--request", required=True)
        command.add_argument("--model-env-file", help="live假设提议使用的.env文件")
    for name in ("factor-run", "factor-resume", "factor-inspect"):
        command = commands.add_parser(name, help="文档基线、新因子提案和反思的正式开发循环")
        command.add_argument("--request", required=True)
        command.add_argument("--model-env-file", help="显式模型.env配置")
    for name in ("model-run", "model-resume", "model-inspect"):
        command = commands.add_parser(name, help="运行时前馈模型结构、正式开发评价与反思")
        command.add_argument("--request", required=True)
        command.add_argument("--model-env-file", help="显式模型.env配置")
    for name in ("joint-run", "joint-resume", "joint-inspect"):
        command = commands.add_parser(name, help="联合因子与模型方向、预算和正式开发评价")
        command.add_argument("--request", required=True)
        command.add_argument("--model-env-file", help="显式模型.env配置")
    knowledge_export = commands.add_parser("knowledge-export", help="复核已完成会话并导出不可变开发知识索引")
    knowledge_export.add_argument("--session", required=True)
    knowledge_export.add_argument("--output", required=True)
    extract = commands.add_parser("spec-extract", help="从归档PDF提取待人工确认规格")
    for name in ("package", "source-archive-root", "source-id", "output", "model-env-file"):
        extract.add_argument("--" + name, required=True)
    extract.add_argument("--pages", nargs="+", type=int, required=True)
    extract.add_argument("--max-calls", type=int, default=2)
    extract.add_argument("--max-output-tokens", type=int, default=8192)
    extract.add_argument("--repair", action="store_true")
    for name in ("spec-review", "spec-confirm", "spec-render"):
        command = commands.add_parser(name)
        for field in ("draft", "materials", "output"):
            command.add_argument("--" + field, required=True)
        if name != "spec-review":
            command.add_argument("--decisions", required=True)
        if name == "spec-confirm":
            command.add_argument("--confirmed-by", required=True)
            command.add_argument("--approve", action="store_true")
        elif name == "spec-render":
            command.add_argument("--confirmation", required=True)
    builder = commands.add_parser("request-build", help="把已确认规格绑定到显式请求模板")
    for field in ("template", "draft", "materials", "decisions", "confirmation", "output"):
        builder.add_argument("--" + field, required=True)
    args = parser.parse_args()
    if args.command == "knowledge-export":
        from .research_knowledge import export_session
        result = export_session(args.session, args.output)
        print(json.dumps({"status": "exported", "records": len(result["records"]), "output": args.output}))
        return
    if args.command.startswith("joint-"):
        from .joint_research import validate_joint_research
        payload = validate_joint_research(json.loads(Path(args.request).read_text(encoding="utf-8")))
        if args.command == "joint-inspect":
            outcome = Path(payload["session_root"]) / "outcome.json"
            print(outcome.read_text(encoding="utf-8") if outcome.exists() else json.dumps({"status": "incomplete"}))
            return
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
        from .joint_loop import run_joint_research
        print(json.dumps(run_joint_research(payload, resume=args.command == "joint-resume", model_env_file=args.model_env_file), ensure_ascii=False))
        return
    if args.command.startswith("model-"):
        from .model_research import validate_model_research
        payload = validate_model_research(json.loads(Path(args.request).read_text(encoding="utf-8")))
        if args.command == "model-inspect":
            outcome = Path(payload["session_root"]) / "outcome.json"
            print(outcome.read_text(encoding="utf-8") if outcome.exists() else json.dumps({"status": "incomplete"}))
            return
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
        from .model_loop import run_model_research
        print(json.dumps(run_model_research(payload, resume=args.command == "model-resume", model_env_file=args.model_env_file), ensure_ascii=False))
        return
    if args.command.startswith("factor-"):
        from .factor_research import validate_factor_research
        payload = validate_factor_research(json.loads(Path(args.request).read_text(encoding="utf-8")))
        if args.command == "factor-inspect":
            outcome = Path(payload["session_root"]) / "outcome.json"
            print(outcome.read_text(encoding="utf-8") if outcome.exists() else json.dumps({"status": "incomplete"}))
            return
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
        from .factor_loop import run_factor_research
        result = run_factor_research(payload, resume=args.command == "factor-resume", model_env_file=args.model_env_file)
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.command.startswith("campaign-"):
        from .campaign import validate_campaign
        payload = json.loads(Path(args.request).read_text(encoding="utf-8"))
        if payload.get("research_kind") == "package":
            from .package_campaign import validate_package_campaign
            payload = validate_package_campaign(payload)
        else:
            payload = validate_campaign(payload)
        if args.command == "campaign-inspect":
            output = Path(payload["session_root"]) / "outcome.json"
            print(output.read_text(encoding="utf-8") if output.exists() else json.dumps({"status": "incomplete"}))
            return
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
        from .campaign_loop import run_campaign
        result = run_campaign(payload, resume=args.command == "campaign-resume", model_env_file=args.model_env_file)
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.command == "request-build":
        from .request_builder import build_request
        build_request(args.template, args.draft, args.materials, args.decisions, args.confirmation, args.output)
        print(json.dumps({"status": "request_ready", "output": args.output,
                          "research_executed": False, "model_calls": 0}))
        return
    if args.command.startswith("spec-"):
        from .formula_spec import extract_spec, review_spec, confirm_spec, render_spec
        if args.command == "spec-extract":
            extract_spec(args.package, args.source_archive_root, args.source_id, args.pages,
                         args.output, args.model_env_file, max_calls=args.max_calls,
                         max_output_tokens=args.max_output_tokens, repair=args.repair)
            print(json.dumps({"status": "draft_ready", "output": args.output}))
        elif args.command == "spec-confirm":
            confirm_spec(args.draft, args.materials, args.decisions, args.output,
                         confirmed_by=args.confirmed_by, approve=args.approve)
            print(json.dumps({"status": "confirmed", "output": args.output}))
        else:
            text = (review_spec(args.draft, args.materials) if args.command == "spec-review" else
                    render_spec(args.draft, args.materials, args.decisions, args.confirmation))
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("x", encoding="utf-8") as stream:
                stream.write(text)
            print(json.dumps({"status": "written", "output": args.output}))
        return
    request = FrozenRequest.load(args.request)
    if args.command == "inspect":
        outcome = request.session_root / "outcome.json"
        print(outcome.read_text(encoding="utf-8") if outcome.exists() else json.dumps({"status": "incomplete"}))
        return
    live = "code_generation" in request.payload
    if live != bool(args.model_env_file):
        parser.error("live模式必须显式提供--model-env-file，固定响应模式不接受此参数")
    request.freeze()
    # 上游导入仅使用随包价格表，避免无关元数据网络请求；模型配置不进入环境。
    os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
    from rdagent.core.conf import RD_AGENT_SETTINGS
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger
    RD_AGENT_SETTINGS.workspace_path = request.session_root / "rd-workspace"
    RD_AGENT_SETTINGS.artifact_signing_key_path = request.session_root / "rd-signing.key"
    LOG_SETTINGS.trace_path = str(request.session_root / "rd-logs")
    rdagent_logger.set_storages_path(request.session_root / "rd-logs")
    from .loop import RPLoop
    snapshots = request.session_root / "rd-logs" / "__session__"
    if args.command == "resume" and snapshots.exists() and any(snapshots.glob("*/*_*")):
        loop = RPLoop.load(snapshots, checkout=False)
        if loop.request.payload != request.payload:
            raise ValueError("会话快照与冻结请求不一致")
    else:
        loop = RPLoop(request)
    if (request.session_root / "outcome.json").exists():
        print((request.session_root / "outcome.json").read_text(encoding="utf-8"))
        return
    # 仅采用上游成功阶段快照；异常时推进过的内存索引不能覆盖最后成功快照。
    with model_environment(args.model_env_file):
        asyncio.run(loop.run(loop_n=1))



if __name__ == "__main__":
    main()
