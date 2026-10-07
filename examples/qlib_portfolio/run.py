"""从公开封存输入执行正式lint、admit、run、verify和报告。"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import subprocess

from research_pipeline.cli import build_parser
from research_pipeline.cli.commands import research_package, research_run, evidence_lifecycle, runtime_lifecycle
from prepare import prepare, CLOCK, SEED


def execute(root, stage, mode="model", reuse_failed_run_root=None, require_reused_nodes=(), input_config=None, sequence_step_len=None, horizon_sessions=1, window_config=None, model_candidates=None, feature_suite=None, resource_state_dir=None):
    root=Path(root).resolve()
    root.mkdir(parents=True,exist_ok=True)
    scratch=root/"tmp"
    scratch.mkdir(exist_ok=True)
    tempfile.tempdir=str(scratch)
    if stage=="prepare":
        return prepare(root,mode,input_config,sequence_step_len=sequence_step_len,
            horizon_sessions=horizon_sessions,window_config=window_config,model_candidates=model_candidates,feature_suite=feature_suite)
    bundle=json.loads((root/"bundle-paths.json").read_text(encoding="utf-8"))
    extensions=[arg for path in bundle["extensions"] for arg in ("--extension-bundle",path)]
    common=["--package",bundle["package"],"--catalog-lock",bundle["catalog_lock"],"--verifier-bundle",bundle["verifier"],*extensions]
    if stage=="lint":
        args=["package","lint",*common,"--json"];handler=research_package._execute
    elif stage=="admit":
        args=["package","admit",*common,"--input-snapshot-manifest",bundle["input_snapshot_manifest"],"--output",str(root/"plan"),"--json"];handler=research_package._execute
    elif stage=="run":
        args=["run","--plan",str(root/"plan"),"--input-snapshot-manifest",bundle["input_snapshot_manifest"],
            "--artifact-root",str(root/"artifacts"),"--handoff-out",str(root/"handoff.json"),"--run-root",str(root/"run"),
            "--result-store",str(root/"results"),"--clock",bundle["fixed_clock"],"--root-seed",str(SEED),"--workers","1",
            "--resource-memory-bytes",str(12*1024**3),"--resource-cpu-slots","4","--resource-process-slots","8",
            "--resource-scratch-bytes",str(64*1024**3),"--resource-timeout-seconds","300","--resource-state-dir",str(Path(resource_state_dir).resolve() if resource_state_dir else root/"resources"),"--json"]
        if reuse_failed_run_root:
            args.extend(["--reuse-failed-run-root", str(reuse_failed_run_root)])
            for node_id in require_reused_nodes:
                args.extend(["--require-reused-node", node_id])
        handler=research_run._execute
    elif stage=="resume":
        args=["resume","--run-root",str(root/"run"),"--json"];handler=runtime_lifecycle._execute
    elif stage=="verify":
        result=json.loads((root/"run-receipt.json").read_text(encoding="utf-8"))
        args=["verify","--result",result["result_directory"],"--result-store",str(root/"results"),
            "--verifier-bundle",bundle["verifier"],"--output",str(root/"verification.json"),"--verification-process-slots","3",
            "--verification-scratch-root",str(scratch),"--json"];handler=evidence_lifecycle._execute
    else:
        args=["report","--verification-result",str(root/"verification.json"),"--result-store",str(root/"results"),
            "--output",str(root/"report.md")];handler=evidence_lifecycle._execute
    # 子进程通过已安装包或当前源码根导入同一正式入口。
    import research_pipeline
    os.chdir(Path(research_pipeline.__file__).resolve().parent.parent)
    if stage in {"run", "resume"}:
        control=root/"control.json"
        arguments=vars(build_parser().parse_args(args))
        if not control.exists():
            control.write_text(json.dumps(arguments,ensure_ascii=False,indent=2),encoding="utf-8")
    result=handler(build_parser().parse_args(args))
    (root/(("run" if stage=="resume" else stage)+"-receipt.json")).write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    if stage in {"run", "resume"} and result.get("status") != "result_finalized":
        raise RuntimeError("研究尚未封存正式Result，请检查run-receipt.json并恢复原运行")
    if stage == "verify" and result.get("status") != "pass":
        raise RuntimeError("独立验证未通过，请检查verification.json")
    return result


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("development", "model", "portfolio"))
    parser.add_argument("--stage", choices=("prepare", "lint", "admit", "run", "resume", "verify", "report", "all"), default="all")
    parser.add_argument("--reuse-failed-run-root")
    parser.add_argument("--require-reused-node", action="append", default=[])
    parser.add_argument("--input-config")
    parser.add_argument("--resource-state-dir", help="仓库外的资源调度状态目录")
    parser.add_argument("--sequence-step-len", type=int)
    parser.add_argument("--factor-suite", choices=("volume_price_v1", "alpha158_selected_v1", "alpha360_selected_v1"))
    horizon = parser.add_mutually_exclusive_group()
    horizon.add_argument("--horizon-sessions", type=int, help="单研究标签期限，默认1个会话")
    horizon.add_argument("--horizons", type=int, nargs="+", help="冻结多个期限，分别运行独立研究")
    parser.add_argument("--window-mode", choices=("expanding", "rolling"))
    for name in ("train", "validation", "test", "step", "embargo"):
        parser.add_argument("--" + name + "-sessions", type=int)
    return parser


def main(argv=None):
    parser = argument_parser()
    args = parser.parse_args(argv)
    root = Path(args.output).resolve()
    window_config = {name + "_sessions": getattr(args, name + "_sessions")
        for name in ("train", "validation", "test", "step", "embargo")
        if getattr(args, name + "_sessions") is not None}
    if args.window_mode is not None:
        window_config["expanding"] = args.window_mode == "expanding"
    window_config = window_config or None
    from horizons import GROUP_FILE, prepare_horizons, execute_horizons
    group_path = root / GROUP_FILE
    stages = ("prepare", "lint", "admit", "run", "verify", "report") if args.stage == "all" else (args.stage,)
    if args.factor_suite is not None and args.stage not in {"prepare", "all"}:
        parser.error("继续研究不能重新声明因子集合；读取原冻结声明")
    if (args.factor_suite is not None or args.resource_state_dir is not None) and (args.horizons is not None or group_path.exists()):
        parser.error("固定因子或资源目录覆盖使用单期限入口；多期限组沿用原冻结配置")
    if args.horizons is not None or group_path.exists():
        if args.reuse_failed_run_root or args.require_reused_node:
            parser.error("多期限组不接受失败运行复用参数；成员恢复使用原目录的resume")
        if group_path.exists():
            if args.stage in {"prepare", "all"}:
                parser.error("该组已冻结；请使用原目录分阶段执行或resume，新组须使用未用目录")
            group = json.loads(group_path.read_text(encoding="utf-8"))
            if args.horizons is not None and args.horizons != group["plan"]["horizons"]:
                parser.error("继续组研究时不能改变已冻结期限")
            if (args.mode is not None or args.input_config is not None or args.sequence_step_len is not None
                    or args.horizon_sessions is not None or window_config is not None):
                parser.error("继续组研究时不能重新声明模式、输入、序列或窗口；这些参数读取组清单")
        elif args.stage not in {"prepare", "all"}:
            parser.error("请先在未用目录prepare多期限组")
        for stage in stages:
            result = (prepare_horizons(root, args.horizons, mode=args.mode or "model",
                input_config=args.input_config, sequence_step_len=args.sequence_step_len,
                window_config=window_config) if stage == "prepare" else execute_horizons(root, stage))
            print(json.dumps(result, ensure_ascii=True, default=str), flush=True)
        return
    if args.stage == "all" and args.factor_suite is not None:
        # 原生Alpha定义准备的依赖占用不计入后续节点预算。
        prepare_args = [sys.executable, "-B", str(Path(__file__).resolve()),
            "--output", str(root), "--mode", args.mode or "model",
            "--stage", "prepare", "--factor-suite", args.factor_suite]
        for name in ("input_config", "sequence_step_len", "horizon_sessions",
                "window_mode", "train_sessions", "validation_sessions",
                "test_sessions", "step_sessions", "embargo_sessions"):
            value = getattr(args, name)
            if value is not None:
                prepare_args.extend(["--" + name.replace("_", "-"), str(value)])
        subprocess.run(prepare_args, check=True)
        stages = stages[1:]
    for stage in stages:
        if args.stage == "all" and args.factor_suite is not None and stage in {"verify", "report"}:
            # 复核使用自己的资源预算，不继承模型训练的依赖占用。
            subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                "--output", str(root), "--mode", args.mode or "model",
                "--stage", stage], check=True)
            continue
        result = execute(root, stage, args.mode or "model", args.reuse_failed_run_root,
            args.require_reused_node, args.input_config, args.sequence_step_len,
            horizon_sessions=1 if args.horizon_sessions is None else args.horizon_sessions,
            window_config=window_config, feature_suite=args.factor_suite, resource_state_dir=args.resource_state_dir)
        print(json.dumps(result, ensure_ascii=True, default=str), flush=True)


if __name__ == "__main__":
    main()
