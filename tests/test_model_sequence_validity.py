"""序列模型的生产窗口、训练范围、恢复与留出消费验收。"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
import runpy
import shutil

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.research.dataframe_budget import PandasFrameBudget
from research_pipeline.research.modeling.qlib import predict_bundle, QlibModelError
from research_pipeline.research.validation import ValidationError
from research_pipeline.runtime.model_sequence import load_runtime_windows
import research_pipeline.runtime.walk_forward_model_execution as runtime
from test_qlib_sequence_runtime import source, split, parameters, BUDGET
from test_qlib_model_integration import _read, research_series

pytestmark = pytest.mark.skipif(importlib.util.find_spec("torch") is None, reason="需要ml-sequence可选依赖")
EXAMPLE = Path(__file__).resolve().parents[1] / "examples/qlib_portfolio"


@pytest.fixture(scope="module", params=[
    "development", "final", "lstm-development", "lstm-final",
    "transformer-development", "transformer-final",
])
def sequence_result(request, tmp_path_factory):
    root = tmp_path_factory.mktemp("sequence-result-" + request.param)
    _, _, declaration = source(root, late=True)
    scope = request.param.removeprefix("lstm-").removeprefix("transformer-")
    declaration["evaluation_scope"] = scope
    metadata = split(root, declaration)
    params = parameters()
    candidate = json.loads(params["candidate_jsons"][0])
    if request.param.startswith("lstm-"):
        candidate["model"]["class"] = "LSTM"
        candidate["model"]["module_path"] = "qlib.contrib.model.pytorch_lstm_ts"
        candidate["candidate_id"] = "lstm-contract"
    elif request.param.startswith("transformer-"):
        from test_qlib_transformer_contract import transformer_candidate
        processors = candidate["processors"]
        candidate = transformer_candidate()
        candidate["processors"] = processors
    params["candidate_jsons"] = [canonical_json(candidate)]
    series = research_series(root) if scope == "final" else None
    design = {**declaration, "root_seed": 7, "candidate": candidate, "sequence": metadata["sequence"]}
    if series is not None:
        design["research_series"] = series
    params["research_identity_hash"] = typed_canonical_hash(design)
    runtime.execute_model_fit_artifact(split_root=root / "split", parameters=params,
        output_root=root / "fit", root_seed=7, max_memory_bytes=BUDGET)
    runtime.execute_model_predict_artifact(split_root=root / "split", model_root=root / "fit",
        output_root=root / "predict", max_memory_bytes=BUDGET)
    holdout_arguments = None
    if scope == "final":
        runtime.execute_model_fold_metrics_artifact(prediction_root=root / "predict", model_root=root / "fit",
            parameters=params, output_root=root / "metrics", max_memory_bytes=BUDGET)
        runtime.execute_model_selection_artifact(metrics_root=root / "metrics", split_root=root / "split", model_root=root / "fit",
            parameters=params, output_root=root / "selection", max_memory_bytes=BUDGET)
        params.update(package_hash="4" * 64, implementation_hash="5" * 64,
            holdout_actor="test", holdout_reason="序列模型最终评价验收",
            research_series_json=canonical_json(series), holdout_unlock_at="2024-06-01T00:00:00Z", fixed_clock="2024-06-02T00:00:00Z")
        holdout_arguments = dict(split_root=root / "split", selection_root=root / "selection",
            feature_root=root / "features", label_root=root / "labels", parameters=params,
            output_root=root / "holdout", ledger_root=root / "test-model/ledgers", root_seed=7, max_memory_bytes=BUDGET)
        runtime.execute_model_locked_holdout_artifact(**holdout_arguments)
    return {"root": root, "scope": scope, "metadata": metadata, "params": params,
        "series": series, "holdout_arguments": holdout_arguments}


def runtime_windows(study):
    return load_runtime_windows(study["root"] / "split", study["metadata"], PandasFrameBudget(BUDGET), runtime._load_table)


def test_sequence_training_uses_visible_windows_and_declared_fold_members(sequence_result):
    from qlib.utils.serial import Serializable
    study = sequence_result
    root = study["root"]
    windows = runtime_windows(study)
    samples = _read(root / "split", "samples").set_index("sample_id")
    excluded = set(windows.exclusions.sample_id)
    assert excluded.isdisjoint(samples.index)
    assert "feature_not_visible" in set(windows.exclusions.reason_code)
    assert "target" not in windows.context and "forward_return" not in windows.context
    models = _read(root / "fit", "models")
    audit = _read(root / "fit", "fit_audit")
    for row in models.to_dict("records"):
        assert row["status"] == "fitted"
        config = json.loads((root / "fit" / row["config_path"]).read_text(encoding="utf-8"))
        fold = next(item for item in study["metadata"]["split_manifest"]["folds"] if item["fold_id"] == row["fold_id"])
        assert config["train_ids"] == fold["train_ids"]
        assert config["valid_ids"] == fold["validation_ids"]
        assert set(config["train_ids"]).isdisjoint(config["valid_ids"])
        train, valid = (samples.loc[ids] for ids in (config["train_ids"], config["valid_ids"]))
        assert (train.label_end_time < valid.decision_time.min()).all()
        assert (train.label_available_time <= valid.decision_time.min()).all()
        frozen = audit.loc[(audit.fold_id == row["fold_id"]) & (audit.candidate_id == row["candidate_id"])].iloc[0]
        assert json.loads(frozen.train_ids_json) == config["train_ids"]
        assert json.loads(frozen.validation_ids_json) == config["valid_ids"]
        processor = Serializable.load(root / "fit" / config["processor_files"]["infer"][0])
        np.testing.assert_allclose(processor.mean_train,
            np.nanmedian(train.loc[:, config["feature_columns"]].to_numpy(), axis=0))
        saved = {name: pd.read_parquet(root / "fit" / path) for name, path in config["sequence"]["files"].items()}
        selected = set(config["train_ids"]) | set(config["valid_ids"])
        assert set(saved["targets"].sample_id) == selected
        assert set(saved["members"].sample_id) == selected
        assert set(saved["members"].groupby("sample_id").size()) == {3}
        assert set(saved["context"].context_index) == set(saved["members"].context_index)
        context = saved["context"].set_index("context_index")
        for sid, members in saved["members"].groupby("sample_id"):
            assert members.step.tolist() == [0, 1, 2]
            assert (pd.to_datetime(context.loc[members.context_index].feature_available_time, utc=True) <= samples.loc[sid].decision_time).all()
        assert config["training_curve"]
        assert all(np.isfinite(item["value"]) and item["value"] <= 0 for item in config["training_curve"])


def test_sequence_model_restores_predictions_from_copied_files(sequence_result, tmp_path):
    study = sequence_result
    root = study["root"]
    restored = tmp_path / "restored"
    shutil.copytree(root / "fit", restored)
    row = _read(root / "fit", "models").iloc[0].to_dict()
    config = json.loads((restored / row["config_path"]).read_text(encoding="utf-8"))
    samples = _read(root / "split", "samples")
    valid = samples.set_index("sample_id", drop=False).loc[config["valid_ids"]].reset_index(drop=True)
    predictions = predict_bundle(restored, row, valid, sequence_context=runtime_windows(study))
    expected = _read(root / "predict", "validation_predictions")
    expected = expected.loc[(expected.fold_id == row["fold_id"]) & (expected.candidate_id == row["candidate_id"])].set_index("sample_id")
    assert predictions.tolist() == expected.loc[valid.sample_id, "prediction"].tolist()


@pytest.mark.parametrize("mutation", ["schema", "window_length", "future_member"])
def test_sequence_prediction_rejects_incompatible_or_future_windows(sequence_result, tmp_path, mutation):
    study = sequence_result
    root = study["root"]
    restored = tmp_path / "restored"
    shutil.copytree(root / "fit", restored)
    row = _read(root / "fit", "models").iloc[0].to_dict()
    path = restored / row["config_path"]
    config = json.loads(path.read_text(encoding="utf-8"))
    samples = _read(root / "split", "samples")
    valid = samples.loc[samples.sample_id.isin(config["valid_ids"])].copy()
    windows = runtime_windows(study)
    if mutation == "schema":
        config["schema"] = "research.qlib-sequence-model-bundle.v1"
    elif mutation == "window_length":
        config["sequence"]["step_len"] += 1
    else:
        context = windows.context.copy()
        ids = windows.members.loc[windows.members.sample_id.isin(valid.sample_id), "context_index"]
        context.loc[context.context_index.isin(ids), "feature_available_time"] = pd.Timestamp("2050-01-01", tz="UTC")
        windows = replace(windows, context=context)
    path.write_text(canonical_json(config), encoding="utf-8")
    with pytest.raises(QlibModelError):
        predict_bundle(restored, row, valid, sequence_context=windows)


def test_development_keeps_holdout_closed_and_final_cannot_reconsume(sequence_result, tmp_path):
    study = sequence_result
    root = study["root"]
    if study["scope"] == "development":
        assert _read(root / "split", "holdout_index").empty
        assert not (root / "holdout").exists()
        assert not list(root.rglob("opened.json"))
        return
    predictions = _read(root / "holdout", "holdout_predictions")
    assert set(predictions.sample_id) == set(_read(root / "split", "holdout_index").sample_id)
    assert np.array_equal(predictions.actual, predictions.raw_label)
    assert np.array_equal(predictions.actual, predictions.evaluation_label)
    ledger = {name: json.loads((root / "holdout/holdout-ledger" / (name + ".json")).read_text(encoding="utf-8"))
              for name in ("plan", "opened", "terminal", "series")}
    assert ledger["series"]["declaration"] == study["series"]
    assert ledger["plan"]["freeze_payload"]["aliases"]["run"] == study["params"]["research_identity_hash"]
    model = _read(root / "holdout", "models").iloc[0]
    config = json.loads((root / "holdout" / model.config_path).read_text(encoding="utf-8"))
    development = _read(root / "split", "samples")
    days = sorted(set(development.observation_session))
    boundary = days[-study["metadata"]["validation_sessions"]]
    valid = development.loc[development.observation_session >= boundary]
    cutoff = valid.decision_time.min()
    train = development.loc[(development.observation_session < boundary)
        & (development.label_end_time < cutoff) & (development.label_available_time <= cutoff)]
    assert set(config["train_ids"]) == set(train.sample_id)
    assert set(config["valid_ids"]) == set(valid.sample_id)
    assert set(config["train_ids"]).isdisjoint(predictions.sample_id)
    assert set(config["valid_ids"]).isdisjoint(predictions.sample_id)
    with pytest.raises(ValidationError, match="已经 prepared/opened|已暴露区间重叠"):
        runtime.execute_model_locked_holdout_artifact(**{**study["holdout_arguments"], "output_root": tmp_path / "repeat"})


def test_result_preserves_model_files_for_prediction_recovery(sequence_result, tmp_path):
    from research_pipeline.platform.metric_contracts import MetricDefinition, MetricReachabilityProof
    from research_pipeline.results import (ResultAssembler, ResultSpec, ResultTableSpec, ResultStore,
        QLIB_MODEL_INVENTORY_SCHEMA_ID, load_result_context)
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from test_result_bundle import _runtime_fixture, _data_reference, HASH_A, HASH_B, HASH_C, HASH_D
    study = sequence_result
    root = study["root"]
    run_root, result_root, _ = _runtime_fixture(tmp_path)
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    external = ExternalArtifactStore(run_root / "external-artifacts")
    staging = external.prepare()
    models = []
    for port in (("fit", "holdout") if study["scope"] == "final" else ("fit",)):
        for row in _read(root / port, "models").to_dict("records"):
            config = json.loads((root / port / row["config_path"]).read_text(encoding="utf-8"))
            files = [config["model_path"], config["weights_path"],
                     *[path for paths in config["processor_files"].values() for path in paths],
                     *config["sequence"]["files"].values()]
            for path in files:
                target = staging / port / path
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(root / port / path, target)
            config["model_path"] = port + "/" + config["model_path"]
            config["weights_path"] = port + "/" + config["weights_path"]
            config["effective_fit_kwargs"]["save_path"] = config["weights_path"]
            config["processor_files"] = {name: [port + "/" + path for path in paths] for name, paths in config["processor_files"].items()}
            config["sequence"]["files"] = {name: port + "/" + path for name, path in config["sequence"]["files"].items()}
            config_path = port + "/" + row["config_path"]
            (staging / config_path).write_text(canonical_json(config), encoding="utf-8")
            models.append({**row, "config_path": config_path, "model_path": config["model_path"]})
    production = runpy.run_path(EXAMPLE / "extension/operator.py")
    prediction_port = "predict" if study["scope"] == "development" else "holdout"
    prediction_name = "validation_predictions" if study["scope"] == "development" else "holdout_predictions"
    metric_ref = "project.sequence.mse@1.0.0"
    metric = production["prediction_metric"](_read(root / prediction_port, prediction_name).to_dict("records"), metric_ref)
    tables = {"models": pa.Table.from_pylist(models), "metrics": pa.Table.from_pylist([metric]),
              **{"sequence_" + name: pa.Table.from_pandas(_read(root / "split", "sequence_" + name), preserve_index=False)
                 for name in ("context", "targets", "members", "exclusions")}}
    specifications = []
    for name, table in tables.items():
        (staging / name).mkdir()
        pq.write_table(table, staging / name / "part-0.parquet")
        schema = QLIB_MODEL_INVENTORY_SCHEMA_ID if name == "models" else "project.sequence." + name + ".v1"
        specifications.append(ResultTableSpec(name, "primary" if name == "metrics" else "diagnostic",
            "statistics", "summary", "project.sequence.summary.v1", schema, name))
    committed = external.commit(staging, artifact_name="summary", artifact_type="project.sequence.summary.v1")
    record["outputs"]["statistics"]["summary"] = committed.artifact_ref.to_dict()
    record_path.write_text(canonical_json(record), encoding="utf-8")
    definition = MetricDefinition.build(metric_id="project.sequence.mse", version="1.0.0",
        input_artifact_type="project.sequence.summary.v1", result_schema_id="project.sequence.metrics.v1",
        output_schema={"value": "float64"}, unit="squared_decimal_price_change", frequency="daily",
        annualization_policy="none", risk_free_rate_policy="not_applicable", null_policy="forbid", direction="lower_is_better",
        implementation_ref="project.qlib_demo.operator.prediction_metric",
        implementation_digest=hashlib.sha256((EXAMPLE / "extension/operator.py").read_bytes()).hexdigest(),
        measurement_semantics={"quantity": "prediction_error", "numerator": "sum_squared_prediction_error",
            "denominator": "prediction_count", "observation_timing": "after_label_available", "aggregation": "mean"})
    proof_payload = {"metric_ref": metric_ref, "definition_digest": definition.definition_digest,
        "producer_node_id": "statistics", "producer_port": "summary", "artifact_type": "project.sequence.summary.v1",
        "result_table_id": "metrics", "result_schema_id": "project.sequence.metrics.v1", "result_path_prefix": "metrics",
        "contract_version": "research-metric-reachability-v2"}
    proof = MetricReachabilityProof(**proof_payload, proof_digest=typed_canonical_hash(proof_payload))
    bundle, directory, _ = ResultAssembler(result_root).finalize(run_root=run_root, project_id="research_package_test",
        package_hash=HASH_B, plan_hash=HASH_C, result_spec=ResultSpec.build(tuple(specifications)),
        catalog_hashes={"prices": HASH_D}, data_references={"prices": _data_reference()}, metric_proofs=(proof,),
        implementation_manifest_hash=HASH_A, claim_level="research_observation", formal_input_request_ids=("prices",),
        input_claim_ceilings={"prices": "research_observation"}, metric_definitions=(definition,))
    shutil.rmtree(run_root)
    context = load_result_context(directory, result_store=result_root)
    assert context.metrics[0].value == metric["value"]
    assert context.claim_level == "research_observation"
    store = ResultStore(result_root, create=False)
    restored = tmp_path / "restored-result"
    for item in bundle.support_files:
        target = restored / item.source_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(store.read_support_bytes(bundle, artifact_key=item.artifact_key, source_path=item.source_path))
    row = models[0]
    config = json.loads((restored / row["config_path"]).read_text(encoding="utf-8"))
    samples = _read(root / "split", "samples")
    valid = samples.set_index("sample_id", drop=False).loc[config["valid_ids"]].reset_index(drop=True)
    actual = predict_bundle(restored, row, valid, sequence_context=runtime_windows(study))
    expected = _read(root / "predict", "validation_predictions")
    expected = expected.loc[(expected.fold_id == row["fold_id"]) & (expected.candidate_id == row["candidate_id"])].set_index("sample_id")
    assert actual.tolist() == expected.loc[valid.sample_id, "prediction"].tolist()
