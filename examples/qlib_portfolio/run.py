"""从公开封存输入执行正式lint、admit、run、verify和报告。"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from research_pipeline.cli import build_parser
from research_pipeline.cli.commands import research_package, research_run, evidence_lifecycle, runtime_lifecycle
from prepare import prepare, CLOCK, SEED


def execute(root, stage, mode="model", reuse_failed_run_root=None, require_reused_nodes=(), input_config=None):
    root=Path(root).resolve()
    root.mkdir(parents=True,exist_ok=True)
    scratch=root/"tmp"
    scratch.mkdir(exist_ok=True)
    tempfile.tempdir=str(scratch)
    if stage=="prepare":
        return prepare(root,mode,input_config)
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
            "--resource-scratch-bytes",str(64*1024**3),"--resource-timeout-seconds","300","--resource-state-dir",str(root/"resources"),"--json"]
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


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",required=True)
    parser.add_argument("--mode",choices=("development","model","portfolio"),default="model")
    parser.add_argument("--stage",choices=("prepare","lint","admit","run","resume","verify","report","all"),default="all")
    parser.add_argument("--reuse-failed-run-root")
    parser.add_argument("--require-reused-node", action="append", default=[])
    parser.add_argument("--input-config")
    args=parser.parse_args()
    for stage in (("prepare","lint","admit","run","verify","report") if args.stage=="all" else (args.stage,)):
        print(json.dumps(execute(args.output,stage,args.mode,args.reuse_failed_run_root,args.require_reused_node,args.input_config),ensure_ascii=True,default=str),flush=True)
