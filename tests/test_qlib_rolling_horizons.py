"""多期限滚动切分、成熟标签、序列预热和独立窗口复核。"""
from __future__ import annotations

from datetime import datetime, timezone
import json

import pandas as pd
import pyarrow.parquet as pq
import pytest

from research_pipeline.research.validation import HoldoutAccessPlan
import research_pipeline.runtime.walk_forward_model_execution as runtime
from test_qlib_horizon_example import WINDOWS, check
from test_qlib_model_integration import _read
from test_research_locked_holdout import _freeze_payload
from test_walk_forward_model_mainline import _write_input_artifact

BUDGET = 128 * 1024**2


@pytest.fixture(autouse=True)
def reject_database_connections(monkeypatch):
    import duckdb
    import sqlite3

    def reject(*args, **kwargs):
        pytest.fail("滚动多期限测试不得连接数据库")

    monkeypatch.setattr(duckdb, "connect", reject)
    monkeypatch.setattr(sqlite3, "connect", reject)


def source(root, horizon, expanding, *, late_feature=False, evaluation_scope="final"):
    days = tuple(pd.bdate_range("2024-01-02", periods=80, tz="UTC"))
    features, labels = [], []
    for index, day in enumerate(days[:74]):
        for entity in ("SYN_0", "SYN_1"):
            decision = day + pd.Timedelta(hours=9, minutes=30)
            available = decision + pd.Timedelta(hours=12) if late_feature and index == 10 and entity == "SYN_1" else decision
            for name, value in (("x1", index / 100), ("x2", index / 200 + 0.01)):
                features.append({
                    "entity_id": entity, "observation_session": day.date(),
                    "observation_time": decision, "available_time": available,
                    "feature_id": name, "window_sessions": 1, "value": value,
                    "status": "ok", "lineage_hash": "1" * 64,
                })
            labels.append({
                "entity_id": entity, "observation_session": day.date(),
                "decision_time": decision, "label_start_time": day + pd.Timedelta(hours=15),
                "label_end_time": days[index + horizon] + pd.Timedelta(hours=15),
                "available_time": days[index + horizon + 1] + pd.Timedelta(hours=9, minutes=30),
                "horizon_sessions": horizon, "forward_return": (100 + index + horizon) / (100 + index) - 1,
                "lineage_hash": "2" * 64,
            })
    for name, rows in (("features", features), ("labels", labels)):
        _write_input_artifact(root / name, name, pd.DataFrame(rows), "rolling-horizon-semantics")
    label_path = next((root / "labels/labels").glob("*.parquet"))
    table = pq.ParquetFile(label_path).read()
    pq.write_table(table, label_path, row_group_size=2)
    parameters = {
        **WINDOWS, "expanding": expanding, "horizon_sessions": horizon,
        "holdout_start": days[62].isoformat(), "target_field": "forward_return",
        "calendar_sessions": [day.date().isoformat() for day in days],
        "sequence_step_len": 3, "evaluation_scope": evaluation_scope,
    }
    metadata = runtime.execute_model_split_artifact(
        feature_root=root / "features", label_root=root / "labels", parameters=parameters,
        output_root=root / "split", fixed_clock="2025-01-01T00:00:00Z", max_memory_bytes=BUDGET,
    )
    return root, days, parameters, metadata


@pytest.fixture(scope="module", params=[(1, False), (5, False), (1, True), (5, True)])
def split_case(request, tmp_path_factory):
    horizon, expanding = request.param
    with pytest.MonkeyPatch.context() as patch:
        import duckdb
        import sqlite3
        patch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("切分不得连接数据库"))
        patch.setattr(sqlite3, "connect", lambda *a, **k: pytest.fail("切分不得连接数据库"))
        return source(tmp_path_factory.mktemp(f"rolling-h{horizon}-{expanding}"), horizon, expanding)


def test_core_three_folds_respect_label_maturity_purge_and_rolling_start(split_case):
    root, days, parameters, metadata = split_case
    samples = _read(root / "split", "samples").set_index("sample_id")
    folds = metadata["split_manifest"]["folds"]
    assert len(folds) == 3
    assert metadata["split_manifest"]["method"] == ("expanding_walk_forward" if parameters["expanding"] else "rolling_walk_forward")
    horizon = parameters["horizon_sessions"]
    boundary = pd.Timestamp(parameters["holdout_start"])
    assert samples.horizon_sessions.eq(horizon).all()
    assert samples.index.str.endswith(f":h{horizon}").all()
    assert (samples.label_end_time < boundary).all()
    assert (samples.label_available_time <= boundary).all()
    assert (samples.feature_available_time <= samples.decision_time).all()
    for index, fold in enumerate(folds):
        offset = index * WINDOWS["step_sessions"]
        train = samples.loc[fold["train_ids"]]
        valid = samples.loc[fold["validation_ids"]]
        test = samples.loc[fold["test_ids"]]
        assert train.label_end_time.max() < valid.decision_time.min()
        assert train.label_available_time.max() <= valid.decision_time.min()
        assert valid.label_end_time.max() < test.decision_time.min()
        assert valid.label_available_time.max() <= test.decision_time.min()
        expected_start = days[2] if parameters["expanding"] or index == 0 else days[offset]
        assert train.observation_session.min() == expected_start.date()
        train_end = offset + WINDOWS["train_sessions"] - horizon - 1
        assert train.observation_session.max() == days[train_end].date()
        assert len(fold["purged_ids"]) == 2 * (horizon + max(horizon - WINDOWS["embargo_sessions"], 0))
        assert len(fold["embargoed_ids"]) == 2 * WINDOWS["embargo_sessions"]
        assert not (set(fold["train_ids"]) & set(fold["validation_ids"]))
        assert not (set(fold["validation_ids"]) & set(fold["test_ids"]))


def test_sequence_warmup_keeps_history_outside_fit_endpoints(split_case):
    root, days, parameters, metadata = split_case
    samples = _read(root / "split", "samples")
    context = _read(root / "split", "sequence_context")
    members = _read(root / "split", "sequence_members")
    exclusions = _read(root / "split", "sequence_exclusions")
    assert context.observation_session.min() == days[0].date()
    assert samples.observation_session.min() == days[2].date()
    assert set(exclusions.reason_code) == {"insufficient_history"}
    assert set(exclusions.sample_id) == {
        f"{entity}:{day.date()}:h{parameters['horizon_sessions']}"
        for entity in ("SYN_0", "SYN_1") for day in days[:2]
    }
    first_id = f"SYN_0:{days[2].date()}:h{parameters['horizon_sessions']}"
    warmup = members.loc[members.sample_id.eq(first_id)].sort_values("step")
    assert warmup.step.tolist() == [0, 1, 2]
    assert warmup.observation_session.tolist() == [day.date() for day in days[:3]]
    assert (warmup.feature_available_time <= samples.loc[samples.sample_id.eq(first_id), "decision_time"].iloc[0]).all()
    if not parameters["expanding"]:
        later_id = f"SYN_0:{days[8].date()}:h{parameters['horizon_sessions']}"
        later = members.loc[members.sample_id.eq(later_id)].sort_values("step")
        assert later.observation_session.tolist() == [day.date() for day in days[6:9]]
        assert later_id in metadata["split_manifest"]["folds"][1]["train_ids"]
        assert f"SYN_0:{days[6].date()}:h{parameters['horizon_sessions']}" not in metadata["split_manifest"]["folds"][1]["train_ids"]


def verifier_case(split_case):
    root, _, parameters, metadata = split_case
    frame = _read(root / "split", "samples")
    samples = {row["sample_id"]: row for row in frame.to_dict("records")}
    fits = [{
        "fold_id": fold["fold_id"], "candidate_id": "ridge",
        "train_ids_json": json.dumps(fold["train_ids"]),
        "validation_ids_json": json.dumps(fold["validation_ids"]),
        "evidence_scope": "processor_train_sample_scope",
    } for fold in metadata["split_manifest"]["folds"]]
    audit = [row for path in sorted((root / "split" / "split_audit").glob("*.parquet"))
        for row in pq.ParquetFile(path).read().to_pylist()]
    tables = {"split_audit": audit, "fit_audit": fits}
    design = {**parameters, "split_calendar_sessions": parameters["calendar_sessions"], "candidate_ids": ["ridge"]}
    return tables, design, samples, metadata


def test_public_verifier_independently_rebuilds_rolling_and_expanding_folds(split_case, monkeypatch):
    from research_pipeline.research import validation

    tables, design, samples, metadata = verifier_case(split_case)
    monkeypatch.setattr(validation, "build_walk_forward", lambda *a, **k: pytest.fail("独立Verifier不得调用生产切分"))
    folds, candidates = check._splits(tables, design, samples)
    assert candidates == {"ridge"}
    for fold in metadata["split_manifest"]["folds"]:
        for role, field in (("train", "train_ids"), ("validation", "validation_ids"), ("test", "test_ids"), ("purged", "purged_ids"), ("embargoed", "embargoed_ids")):
            assert folds[fold["fold_id"]][role] == set(fold[field])


@pytest.mark.parametrize("change", ["expanding", "window", "purge_role", "fit_scope"])
def test_public_verifier_rejects_changed_window_or_fit_members(split_case, change):
    tables, design, samples, _ = verifier_case(split_case)
    if change == "expanding":
        design["expanding"] = not design["expanding"]
    elif change == "window":
        design["train_sessions"] += 1
    elif change == "purge_role":
        row = next(row for row in tables["split_audit"] if row["role"] == "purged")
        row["role"] = "train"
    else:
        tables["fit_audit"][0]["train_ids_json"] = "[]"
    with pytest.raises(ValueError):
        check._splits(tables, design, samples)


def test_horizon_specific_holdout_ids_create_distinct_persistent_plan_identity(tmp_path):
    plans = []
    indexes = []
    for horizon in (1, 5):
        root, days, parameters, _ = source(tmp_path / f"h{horizon}", horizon, False)
        index = _read(root / "split", "holdout_index")
        assert index.sample_id.str.endswith(f":h{horizon}").all()
        assert index.observation_time.min() == days[62]
        indexes.append(set(index.sample_id))
        payload = _freeze_payload()
        payload["holdout_split"].update(start=str(days[62].date()), end=str(days[73].date()), sample_ids=sorted(index.sample_id.tolist()))
        plans.append(HoldoutAccessPlan.build(
            freeze_payload=payload, actor="test", reason="多期限独立留出身份", unlock_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        ))
    assert not indexes[0] & indexes[1]
    assert plans[0].holdout_identity_hash != plans[1].holdout_identity_hash
    assert plans[0].plan_hash != plans[1].plan_hash


@pytest.mark.parametrize("horizon", [1, 5])
def test_late_feature_endpoint_is_excluded_without_discarding_later_warmup(tmp_path, horizon):
    root, days, parameters, _ = source(tmp_path, horizon, False, late_feature=True)
    samples = _read(root / "split", "samples")
    exclusions = _read(root / "split", "sequence_exclusions")
    delayed_id = f"SYN_1:{days[10].date()}:h{horizon}"
    assert delayed_id not in set(samples.sample_id)
    assert delayed_id in set(exclusions.sample_id)
    assert f"SYN_1:{days[11].date()}:h{horizon}" in set(samples.sample_id)


@pytest.mark.parametrize("horizon", [1, 5])
def test_development_rolling_horizon_does_not_scan_or_export_holdout(tmp_path, monkeypatch, horizon):
    seen = []
    original = runtime._load_label_slice

    def tracked(*args, **kwargs):
        seen.append(kwargs["label"])
        assert "holdout" not in kwargs["label"]
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime, "_load_label_slice", tracked)
    root, _, _, metadata = source(tmp_path, horizon, False, evaluation_scope="development")
    assert seen == ["Walk-forward development labels"]
    assert metadata["holdout_end"] is None
    assert _read(root / "split", "holdout_index").empty
