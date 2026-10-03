from __future__ import annotations

from datetime import datetime, timezone
import json
import inspect
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import research_pipeline.research.modeling.walk_forward as walk_forward_module
import research_pipeline.runtime.walk_forward_model_execution as model_execution_module

from research_pipeline.research.modeling import (
    CandidateFitRejected,
    ModelDependencyError,
    ModelMainlineError,
    assemble_daily_model_samples,
    evaluate_locked_holdout,
    model_dependency_preflight,
)
from research_pipeline.research.validation import (
    ValidationError,
)
from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.runtime.walk_forward_model_execution import (
    execute_model_fit_artifact,
    execute_model_fold_metrics_artifact,
    execute_model_split_artifact,
)
import research_pipeline.runtime.adapters.model as operator_adapters
from test_qlib_model_integration import candidate as qlib_candidate


def test_walk_forward_adapters_match_implementation_signatures(
    tmp_path, monkeypatch
) -> None:
    """逐个核对模型链 adapter 的必填参数，避免 Runtime 执行到后段才报 TypeError。"""

    captured: dict[str, object] = {}
    fixed_clock = "2024-06-01T00:00:00+00:00"
    environment = SimpleNamespace(
        artifact_root=tmp_path,
        holdout_ledger_anchor=tmp_path,
        fixed_clock=fixed_clock,
        root_seed=7,
    )
    parameters = {
        "fixed_clock": fixed_clock,
        "research_identity_hash": "a" * 64,
    }

    def fake_external_result(context, implementation, **kwargs):
        captured["implementation"] = implementation
        captured["kwargs"] = kwargs
        return {}, object()

    monkeypatch.setattr(
        operator_adapters, "_factor_external_result", fake_external_result
    )
    monkeypatch.setattr(operator_adapters, "_input_external_root", lambda *_: tmp_path)
    monkeypatch.setattr(operator_adapters, "_parameters", lambda *_: parameters)
    monkeypatch.setattr(operator_adapters, "_environment", lambda *_: environment)

    adapters = (
        operator_adapters.execute_research_model_split_manifest_v2,
        operator_adapters.execute_research_model_fit_v2,
        operator_adapters.execute_research_model_predict_v2,
        operator_adapters.execute_research_model_fold_metrics_v2,
        operator_adapters.execute_research_model_selection_v2,
        operator_adapters.execute_research_model_locked_holdout_v2,
    )
    for adapter in adapters:
        context = SimpleNamespace(
            effective_resource_budget=SimpleNamespace(
                memory_bytes=64 * 1024**2
            ),
            node=SimpleNamespace(
                resource_budget=SimpleNamespace(memory_bytes=64 * 1024**2)
            )
        )
        adapter(context)
        implementation = captured["implementation"]
        required = {
            name
            for name, parameter in inspect.signature(implementation).parameters.items()
            if name != "output_root" and parameter.default is inspect.Parameter.empty
        }
        assert set(captured["kwargs"]) == required, (
            adapter.__name__,
            implementation.__name__,
            set(captured["kwargs"]),
            required,
        )


def test_model_split_adapter_verifies_label_before_starting_split(
    tmp_path, monkeypatch
) -> None:
    requested_ports: list[str] = []

    def verified_root(_context, port: str):
        requested_ports.append(port)
        if port == "labels":
            raise ValueError("标签 ExternalArtifact 校验失败")
        return tmp_path

    def must_not_execute(*_args, **_kwargs):
        pytest.fail("Label 校验失败后不得启动 split 或读取目标列")

    monkeypatch.setattr(operator_adapters, "_input_external_root", verified_root)
    monkeypatch.setattr(operator_adapters, "_factor_external_result", must_not_execute)
    context = SimpleNamespace(
        effective_resource_budget=SimpleNamespace(memory_bytes=64 * 1024**2)
    )

    with pytest.raises(ValueError, match="ExternalArtifact 校验失败"):
        operator_adapters.execute_research_model_split_manifest_v2(context)
    assert requested_ports == ["features", "labels"]


def _samples(size: int = 70) -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    sessions = list(pd.bdate_range("2024-01-02", periods=size, tz="UTC"))
    values = np.linspace(-1.5, 1.5, size)
    target = 0.6 * values - 0.2 * np.sin(np.arange(size))
    frame = pd.DataFrame(
        {
            "sample_id": [f"s{index:03d}" for index in range(size)],
            "observation_time": [item + pd.Timedelta(hours=15) for item in sessions],
            "label_start_time": [
                item + pd.Timedelta(days=1, hours=9) for item in sessions
            ],
            "label_end_time": [
                item + pd.Timedelta(days=1, hours=16) for item in sessions
            ],
            "target": target,
            "x1": values,
            "x2": np.cos(np.arange(size) / 4),
        }
    )
    frame["entity_id"] = "SYN"
    frame["observation_session"] = [item.date() for item in sessions]
    frame["decision_time"] = frame["observation_time"]
    frame["feature_available_time"] = frame["observation_time"]
    frame["label_available_time"] = frame["label_end_time"]
    frame["horizon_sessions"] = 1
    frame.loc[[2, 19, 33], "x2"] = np.nan
    return frame, sessions


def _read_parts(root) -> pd.DataFrame:
    return pd.concat([
        pq.ParquetFile(path).read().to_pandas() for path in sorted(root.glob("*.parquet"))
    ], ignore_index=True)


def _write_input_artifact(
    root,
    table_name: str,
    frame: pd.DataFrame,
    semantics_hash: str,
) -> None:
    directory = root / table_name
    directory.mkdir(parents=True)
    frame.to_parquet(directory / "part-00000.parquet", index=False)
    clean = frame.astype(object).where(pd.notna(frame), None)
    records = clean.to_dict("records")
    for row in records:
        for key, value in tuple(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
            elif isinstance(value, np.generic):
                row[key] = value.item()
    metadata = {
        "table_hashes": {table_name: typed_canonical_hash(records)},
        "semantics_hash": semantics_hash,
    }
    (root / "artifact-metadata.json").write_text(
        canonical_json(metadata), encoding="utf-8"
    )














def test_locked_holdout_is_reserved_once_and_cannot_be_reused(tmp_path) -> None:
    frame, sessions = _samples(64)
    development = tuple(frame.iloc[:54]["sample_id"])
    holdout = tuple(frame.iloc[54:]["sample_id"])
    arguments = dict(
        development_samples=frame.iloc[:54].copy(),
        holdout_preflight=lambda: {"format": "dataframe", "schema": "samples-v1"},
        holdout_loader=lambda: frame.iloc[54:].copy(),
        development_ids=development,
        holdout_ids=holdout,
        holdout_start=sessions[54].date().isoformat(),
        holdout_end=sessions[-1].date().isoformat(),
        feature_columns=("x1", "x2"),
        selected_candidate=qlib_candidate(),
        target_kind="regression",
        objective="neg_mean_squared_error",
        validation_sessions=10,
        output_root=tmp_path / "model",
        research_identity_hash="2" * 64,
        data_snapshot_hash="6" * 64,
        selection_hash="3" * 64,
        package_hash="4" * 64,
        implementation_hash="5" * 64,
        actor="framework",
        reason="locked_holdout_primary",
        unlock_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        fixed_clock=datetime(2024, 6, 1, tzinfo=timezone.utc),
        ledger_root=tmp_path / "holdout-ledger",
        root_seed=7,
    )
    result = evaluate_locked_holdout(**arguments)
    assert result["status"] == "committed"
    assert len(result["predictions"]) == len(holdout)
    with pytest.raises(ValidationError, match="已经 prepared|不能重复读取"):
        evaluate_locked_holdout(**arguments)


def test_locked_holdout_opens_before_loading_target_and_failed_identity_stays_closed(
    tmp_path,
) -> None:
    frame, sessions = _samples(64)
    calls = 0

    def failing_loader() -> pd.DataFrame:
        nonlocal calls
        calls += 1
        raise ModelMainlineError("故意模拟 holdout 读取失败")

    arguments = dict(
        development_samples=frame.iloc[:54].copy(),
        holdout_preflight=lambda: {"format": "dataframe", "schema": "samples-v1"},
        holdout_loader=failing_loader,
        development_ids=tuple(frame.iloc[:54]["sample_id"]),
        holdout_ids=tuple(frame.iloc[54:]["sample_id"]),
        holdout_start=sessions[54].date().isoformat(),
        holdout_end=sessions[-1].date().isoformat(),
        feature_columns=("x1", "x2"),
        selected_candidate=qlib_candidate(),
        target_kind="regression",
        objective="neg_mean_squared_error",
        validation_sessions=10,
        output_root=tmp_path / "model",
        research_identity_hash="9" * 64,
        data_snapshot_hash="d" * 64,
        selection_hash="a" * 64,
        package_hash="b" * 64,
        implementation_hash="c" * 64,
        actor="framework",
        reason="locked_holdout_primary",
        unlock_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        fixed_clock=datetime(2024, 6, 1, tzinfo=timezone.utc),
        ledger_root=tmp_path / "lazy-holdout-ledger",
        root_seed=7,
    )
    with pytest.raises(ModelMainlineError, match="故意模拟"):
        evaluate_locked_holdout(**arguments)
    assert calls == 1
    terminal_path = next((tmp_path / "lazy-holdout-ledger").glob("*/terminal.json"))
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    assert terminal["status"] == "consumed_failed"
    with pytest.raises(ValidationError, match="不能重复读取"):
        evaluate_locked_holdout(**arguments)
    assert calls == 1






def test_daily_feature_label_join_rejects_future_available_feature() -> None:
    features = pd.DataFrame(
        {
            "entity_id": ["A"],
            "observation_session": ["2024-01-02"],
            "observation_time": ["2024-01-02T15:00:00Z"],
            "available_time": ["2024-01-03T10:00:00Z"],
            "window_sessions": [5],
            "feature_id": ["trend_slope"],
            "value": [1.0],
            "status": ["ok"],
            "lineage_hash": ["a" * 64],
        }
    )
    labels = pd.DataFrame(
        {
            "entity_id": ["A"],
            "observation_session": ["2024-01-02"],
            "decision_time": ["2024-01-02T16:00:00Z"],
            "label_start_time": ["2024-01-03T09:00:00Z"],
            "label_end_time": ["2024-01-03T15:00:00Z"],
            "available_time": ["2024-01-03T16:00:00Z"],
            "horizon_sessions": [1],
            "forward_return": [0.01],
            "lineage_hash": ["b" * 64],
        }
    )
    with pytest.raises(ModelMainlineError, match="晚于决策时点"):
        assemble_daily_model_samples(features, labels, horizon_sessions=1)


def test_daily_model_samples_accept_open_left_equal_boundary_and_reject_earlier_start() -> (
    None
):
    features = pd.DataFrame(
        {
            "entity_id": ["A"],
            "observation_session": ["2024-01-02"],
            "observation_time": ["2024-01-02T15:00:00Z"],
            "available_time": ["2024-01-02T16:00:00Z"],
            "window_sessions": [5],
            "feature_id": ["trend_slope"],
            "value": [1.0],
            "status": ["ok"],
            "lineage_hash": ["a" * 64],
        }
    )
    labels = pd.DataFrame(
        {
            "entity_id": ["A"],
            "observation_session": ["2024-01-02"],
            "decision_time": ["2024-01-02T16:00:00Z"],
            "label_start_time": ["2024-01-02T16:00:00Z"],
            "label_end_time": ["2024-01-03T15:00:00Z"],
            "available_time": ["2024-01-03T16:00:00Z"],
            "horizon_sessions": [1],
            "forward_return": [0.01],
            "lineage_hash": ["b" * 64],
        }
    )

    samples, feature_columns = assemble_daily_model_samples(
        features, labels, horizon_sessions=1
    )
    assert len(samples) == 1
    assert feature_columns == ("trend_slope__w5",)

    attacked = labels.copy()
    attacked.loc[0, "label_start_time"] = "2024-01-02T15:59:59Z"
    with pytest.raises(ModelMainlineError, match="决策之前"):
        assemble_daily_model_samples(features, attacked, horizon_sessions=1)


def test_model_split_rejects_mixed_label_row_group_before_target_scan(
    tmp_path, monkeypatch
) -> None:
    feature_root = tmp_path / "features"
    label_root = tmp_path / "labels"
    _write_input_artifact(
        feature_root,
        "features",
        pd.DataFrame({"entity_id": ["INDEX"], "feature_value": [1.0]}),
        "semantics",
    )
    _write_input_artifact(
        label_root,
        "labels",
        pd.DataFrame(
            {
                "label_end_time": pd.to_datetime(
                    ["2024-01-02T08:00:00Z", "2024-01-04T08:00:00Z"]
                ),
                "horizon_sessions": [1, 1],
                "forward_return": [0.01, 0.02],
            }
        ),
        "semantics",
    )
    scans = 0

    def forbidden_scan(*_args, **_kwargs):
        nonlocal scans
        scans += 1
        raise AssertionError("混合 row group 不应进入目标扫描")

    monkeypatch.setattr(model_execution_module.ds, "dataset", forbidden_scan)
    with pytest.raises(ModelMainlineError, match="Label row group 必须只包含单一"):
        execute_model_split_artifact(
            feature_root=feature_root,
            label_root=label_root,
            parameters={
                "holdout_start": "2024-01-03T00:00:00+00:00",
                "horizon_sessions": 1,
                "target_field": "forward_return",
            },
            output_root=tmp_path / "split",
            fixed_clock="2024-01-05T00:00:00+00:00",
            max_memory_bytes=64 * 1024**2,
        )
    assert scans == 0
