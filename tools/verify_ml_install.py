"""验收已安装 ML 发行包的训练、处理器封存与跨进程恢复。"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import subprocess
import sys
import tempfile


def _installation() -> dict:
    import qlib
    import research_pipeline

    prefix = Path(sys.prefix).resolve()
    if sys.prefix == sys.base_prefix:
        raise ValueError("验收必须使用独立虚拟环境")
    configuration = (prefix / "pyvenv.cfg").read_text(encoding="utf-8").lower()
    if "include-system-site-packages = false" not in configuration:
        raise ValueError("验收环境不得继承系统 site-packages")
    origins = {}
    for module in (research_pipeline, qlib):
        origin = Path(module.__file__).resolve()
        if not origin.is_relative_to(prefix):
            raise ValueError(f"{module.__name__} 未从当前虚拟环境导入: {origin}")
        origins[module.__name__] = str(origin)
    return {"python": platform.python_version(), "platform": platform.platform(),
            "prefix": str(prefix), "imports": origins,
            "versions": {name: importlib.metadata.version(name) for name in
                         ("quantwitness", "pyqlib", "numpy", "pandas", "scikit-learn",
                          "lightgbm", "xgboost", "pyarrow", "plotly", "statsmodels")}}


def _reject_database(*args, **kwargs):
    raise RuntimeError("ML 安装验收只使用内存与文件，不得连接数据库")


def _samples():
    import numpy as np
    import pandas as pd

    sessions = pd.bdate_range("2024-01-02", periods=61, tz="UTC")
    rows = []
    for index, session in enumerate(sessions[:-1]):
        for entity in range(8):
            x1 = index / 30 + entity / 10
            decision = session + pd.Timedelta(hours=15)
            end = sessions[index + 1] + pd.Timedelta(hours=16)
            rows.append({"sample_id": f"sample-{index}-{entity}", "entity_id": f"SYN_{entity}",
                         "observation_session": session.date(), "decision_time": decision,
                         "feature_available_time": decision,
                         "label_end_time": end, "label_available_time": end,
                         "target": 0.6 * x1 - 0.2 * np.sin(index + entity), "x1": x1,
                         "x2": np.nan if (index, entity) in {(3, 0), (43, 1), (53, 2)}
                         else np.cos(index / 4 + entity)})
    return pd.DataFrame(rows)


def _candidate(name: str) -> dict:
    modules = {"LinearModel": "qlib.contrib.model.linear", "LGBModel": "qlib.contrib.model.gbdt",
               "XGBModel": "qlib.contrib.model.xgboost"}
    kwargs = {"LinearModel": {"estimator": "ridge", "alpha": 0.1, "include_valid": False},
              "LGBModel": {"num_leaves": 5, "min_data_in_leaf": 5, "learning_rate": 0.1},
              "XGBModel": {"max_depth": 2, "eta": 0.1}}
    fit = {} if name == "LinearModel" else {
        "num_boost_round": 8, "early_stopping_rounds": 3 if name == "LGBModel" else None,
        "verbose_eval": 0 if name == "LGBModel" else False}
    return {"candidate_id": name, "model": {"class": name, "module_path": modules[name],
                                           "kwargs": kwargs[name]},
            "processors": {"infer": [
                {"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature"}},
                {"class": "Fillna", "kwargs": {"fields_group": "feature", "fill_value": 0}}],
                "learn": [{"class": "DropnaLabel", "kwargs": {}}]}, "fit": fit}


def _restore(output: Path) -> None:
    import numpy as np
    import pandas as pd
    from research_pipeline.research.modeling.qlib import predict_bundle

    _installation()
    inputs = pd.read_parquet(output / "prediction-input.parquet")
    rows = json.loads((output / "model-rows.json").read_text(encoding="utf-8"))
    for name, row in rows.items():
        predictions = predict_bundle(output / "relocated" / name, row, inputs)
        np.save(output / f"{name}-restored.npy", predictions)


def verify_install(output: Path) -> dict:
    import numpy as np
    from research_pipeline.research.modeling.qlib import fit_bundle, predict_bundle

    installation = _installation()
    output.mkdir(parents=True, exist_ok=False)
    os.chdir(output)
    (output / "tmp").mkdir()
    tempfile.tempdir = str(output / "tmp")
    checked = subprocess.run([sys.executable, "-I", "-m", "pip", "check"],
                             text=True, capture_output=True)
    (output / "pip-check.log").write_text(checked.stdout + checked.stderr, encoding="utf-8")
    checked.check_returncode()
    samples = _samples()
    train, valid, prediction_input = samples.iloc[:312], samples.iloc[320:392], samples.iloc[400:]
    rows, expected = {}, {}
    for name in ("LinearModel", "LGBModel", "XGBModel"):
        root = output / "training" / name
        row = fit_bundle(train, valid, candidate=_candidate(name), feature_columns=("x1", "x2"),
                         output_root=root, bundle_path="model", root_seed=7,
                         fit_scope_ref="synthetic-training-samples")
        rows[name] = row
        expected[name] = predict_bundle(root, row, prediction_input)
        if not np.isfinite(expected[name]).all():
            raise ValueError(f"{name} 预测包含非有限值")
        config = json.loads((root / row["config_path"]).read_text(encoding="utf-8"))
        if config["train_ids"] != train.sample_id.tolist() or config["valid_ids"] != valid.sample_id.tolist():
            raise ValueError(f"{name} 封存训练与验证样本不一致")
        for path in config["processor_files"]["infer"]:
            if not (root / path).is_file():
                raise ValueError(f"{name} 缺少封存处理器: {path}")
        shutil.copytree(root, output / "relocated" / name)
    # 恢复输入没有标签，恢复进程只能从迁移后的模型目录读取。
    prediction_input.drop(columns=["target"]).to_parquet(output / "prediction-input.parquet", index=False)
    (output / "model-rows.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "training").rename(output / "training-archived")
    with (output / "restore.log").open("w", encoding="utf-8") as log:
        subprocess.run([sys.executable, "-I", str(Path(__file__).resolve()), "--restore", str(output)],
                       stdout=log, stderr=subprocess.STDOUT, cwd=output, check=True)
    models = []
    for name, values in expected.items():
        restored = np.load(output / f"{name}-restored.npy", allow_pickle=False)
        np.testing.assert_allclose(restored, values, rtol=0, atol=0)
        models.append({"model": name, "train_rows": len(train), "valid_rows": len(valid),
                       "prediction_rows": len(values), "max_absolute_difference": float(np.max(np.abs(values-restored))),
                       "processor_count": 2, "restored_without_labels": True})
    receipt = {"schema_version": "quantwitness-ml-install-acceptance-v1", "status": "pass",
               "installation": installation, "models": models, "pip_check": "pass",
               "database_access": False, "model_api_calls": 0,
               "scope": "三模型安装、训练、处理器封存和跨进程迁移恢复；不声明正式研究或投资绩效"}
    (output / "acceptance.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--output", type=Path, help="尚不存在的验收输出目录")
    group.add_argument("--restore", type=Path, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    import duckdb
    sqlite3.connect = _reject_database
    duckdb.connect = _reject_database
    if arguments.restore:
        _restore(arguments.restore.resolve())
    else:
        receipt = verify_install(arguments.output.resolve())
        print(json.dumps(receipt, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
