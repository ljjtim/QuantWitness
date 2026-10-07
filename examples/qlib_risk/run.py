"""风险组合的准备、准入、运行、独立验证和报告。"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("risk_prepare", HERE/"prepare.py")
risk_prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(risk_prepare)
base_spec = importlib.util.spec_from_file_location("model_example_run", HERE.parent/"qlib_portfolio/run.py")
base_run = importlib.util.module_from_spec(base_spec)
base_spec.loader.exec_module(base_run)


def execute(root, stage, *, method="inv", estimator="empirical", lookback=20):
    if stage == "prepare":
        return risk_prepare.prepare(root, method=method, estimator=estimator, lookback=lookback)
    return base_run.execute(root, stage, mode="portfolio")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--method", choices=("inv", "gmv", "rp", "mvo", "enhanced", "topk_dropout"), default="inv")
    parser.add_argument("--estimator", choices=("empirical", "shrink"), default="empirical")
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--stage", choices=("prepare", "lint", "admit", "run", "resume", "verify", "report", "all"), default="all")
    args = parser.parse_args()
    for stage in (("prepare", "lint", "admit", "run", "verify", "report") if args.stage == "all" else (args.stage,)):
        result = execute(args.output, stage, method=args.method, estimator=args.estimator, lookback=args.lookback)
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
