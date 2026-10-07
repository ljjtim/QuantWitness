"""无数据库的序列窗口、PIT 独立复核及 Qlib 实际采样验收。"""
from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research_pipeline.evidence.sequence_validity import (
    SequenceWindowVerificationError, verify_sequence_window_facts,
)
from research_pipeline.research.modeling.sequence import build_sequence_windows, qlib_sequence_dataset
from research_pipeline.research.modeling.walk_forward import ModelMainlineError, assemble_daily_model_samples


COLUMNS = ("x__w1", "y__w1")


def fixture():
    days = pd.bdate_range("2024-01-02", periods=9, tz="UTC")
    features, targets = [], []
    for i, day in enumerate(days):
        for j, entity in enumerate(("A", "B")):
            for k, name in enumerate(("x", "y")):
                features.append(dict(entity_id=entity, observation_session=day.date(),
                    observation_time=day + pd.Timedelta(hours=15), available_time=day + pd.Timedelta(hours=15),
                    window_sessions=1, feature_id=name, value=100*j + 10*i + k,
                    status="ok", lineage_hash=f"{entity}/{i}/{name}"))
            targets.append(dict(sample_id=f"{entity}{i}", entity_id=entity, observation_session=day.date(),
                decision_time=day + pd.Timedelta(hours=16), target=object()))
    return pd.DataFrame(features), pd.DataFrame(targets), tuple(day.date() for day in days)


def plan(features=None, targets=None, *, step_len=3, columns=COLUMNS):
    raw, ends, calendar = fixture()
    return build_sequence_windows(raw if features is None else features, ends if targets is None else targets,
                                  calendar_sessions=calendar, feature_columns=columns, step_len=step_len)


def verify(windows, *, features=None, targets=None):
    raw, ends, calendar = fixture()
    return verify_sequence_window_facts(features=raw if features is None else features,
        targets=ends if targets is None else targets, calendar_sessions=calendar,
        feature_columns=windows.feature_columns, step_len=windows.step_len,
        context=windows.context, members=windows.members, exclusions=windows.exclusions)


def test_calendar_members_preserve_purge_rows_and_holdout_warmup_without_labels():
    raw, ends, calendar = fixture()
    # 只有训练、验证、holdout 末端有标签资格，窗口仍须保留中间的历史日。
    ends = ends.loc[ends.sample_id.isin(["A2", "A5", "A7", "B7"])]
    windows = plan(targets=ends)
    assert set(windows.targets.columns) == {"sample_id", "entity_id", "observation_session", "decision_time"}
    assert "target" not in windows.context
    for sid, expected_days in [("A2", calendar[:3]), ("A5", calendar[3:6]), ("A7", calendar[5:8])]:
        members = windows.members.loc[windows.members.sample_id == sid]
        assert members.observation_session.tolist() == list(expected_days)
        assert members.entity_id.tolist() == ["A"]*3
    assert verify(windows, targets=ends) == dict(target_count=4, window_count=4, excluded_count=0, member_count=12)


def test_missing_session_does_not_shorten_or_fill_window():
    raw, _, calendar = fixture()
    raw = raw.loc[~((raw.entity_id == "B") & (raw.observation_session == calendar[1]))]
    windows = plan(features=raw)
    assert dict(windows.exclusions.values) == {"A0":"insufficient_history", "B0":"insufficient_history",
        "A1":"insufficient_history", "B1":"insufficient_history", "B2":"missing_feature", "B3":"missing_feature"}
    assert verify(windows, features=raw)["window_count"] == 12


def test_historical_feature_visibility_is_checked_at_each_endpoint():
    raw, _, calendar = fixture()
    mask = (raw.entity_id == "A") & (raw.observation_session == calendar[2])
    raw.loc[mask, "available_time"] = pd.Timestamp(calendar[3], tz="UTC") + pd.Timedelta(hours=16)
    windows = plan(features=raw)
    assert dict(windows.exclusions.values)["A2"] == "feature_not_visible"
    # 等于后一个决策时点允许使用，不能因为原会话尚不可见就永久删掉历史行。
    assert "A3" in set(windows.members.sample_id)
    assert verify(windows, features=raw)["window_count"] == 13


def test_missing_one_feature_is_distinct_from_explicit_nan():
    raw, _, calendar = fixture()
    mask = (raw.entity_id == "B") & (raw.observation_session == calendar[1]) & (raw.feature_id == "y")
    missing = plan(features=raw.loc[~mask])
    assert dict(missing.exclusions.values)["B2"] == "missing_feature"
    raw.loc[mask, "value"] = np.nan
    windows = plan(features=raw)
    assert "B2" in set(windows.members.sample_id)
    with pytest.raises(ModelMainlineError, match="非有限"):
        qlib_sequence_dataset(windows, segments={"test":["B2"]})
    values = windows.context.set_index(["observation_session", "entity_id"])[list(COLUMNS)].fillna(0)
    sampler = qlib_sequence_dataset(windows, segments={"test":["B2"]}, transformed_features=values).prepare("test", col_set=["feature", "label"])
    assert sampler[0][1, 1] == 0
    assert verify(windows, features=raw)["window_count"] == 14


def test_qlib_actual_sampler_has_only_endpoints_and_keeps_unlabelled_history():
    windows = plan()
    labels = pd.Series({"A2":0.2, "B2":0.3, "A5":0.5, "B5":0.6})
    dataset = qlib_sequence_dataset(windows, segments={"train":["A2", "B2"], "valid":["A5", "B5"]}, labels=labels)
    train = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    valid = dataset.prepare("valid", col_set=["feature", "label"], data_key="learn")
    assert len(train) == len(valid) == 2
    np.testing.assert_array_equal(train[0][:, :2], [[0,1],[10,11],[20,21]])
    np.testing.assert_array_equal(valid[0][:, :2], [[30,31],[40,41],[50,51]])
    assert np.isnan(valid[0][:-1, -1]).all()
    assert valid[0][-1, -1] == 0.5
    # 上游模型请求填补索引时，完整窗口的特征仍与手算逐项相等。
    valid.config(fillna_type="ffill+bfill")
    np.testing.assert_array_equal(valid[0][:, :2], [[30,31],[40,41],[50,51]])


def test_prediction_nan_labels_feature_order_and_endpoint_order():
    windows = plan(columns=tuple(reversed(COLUMNS)))
    sampler = qlib_sequence_dataset(windows, segments={"test":["B7", "A7"]}).prepare("test", col_set=["feature", "label"])
    assert list(sampler.get_index().get_level_values("instrument")) == ["A", "B"]
    np.testing.assert_array_equal(sampler[0][:, :2], [[51,50],[61,60],[71,70]])
    assert np.isnan(sampler[0][:, -1]).all() and np.isnan(sampler[1][:, -1]).all()


@pytest.mark.parametrize("change", ["entity", "session", "available", "lineage", "step", "omit", "duplicate", "value", "exclude"])
def test_independent_verifier_rejects_altered_window_evidence(change):
    windows = plan()
    if change == "entity": windows.members.loc[0, "entity_id"] = "B"
    elif change == "session": windows.members.loc[0, "observation_session"] = windows.calendar_sessions[1]
    elif change == "available": windows.members.loc[0, "feature_available_time"] += pd.Timedelta(minutes=1)
    elif change == "lineage": windows.members.loc[0, "feature_lineage_hash"] = "other"
    elif change == "step": windows.members.loc[0, "step"] = 9
    elif change == "omit": windows = replace(windows, members=windows.members.iloc[1:])
    elif change == "duplicate": windows = replace(windows, members=pd.concat([windows.members, windows.members.iloc[:1]]))
    elif change == "value": windows.context.loc[0, "x__w1"] += 1
    else: windows.exclusions.loc[0, "reason_code"] = "missing_feature"
    with pytest.raises(SequenceWindowVerificationError): verify(windows)


@pytest.mark.parametrize("kwargs", [
    dict(segments={"test":["A7"]}, labels=pd.Series({"A7":1.0})),
    dict(segments={"train":["A2"], "valid":["A5"]}),
    dict(segments={"test":["A0"]}),
    dict(segments={"test":["A7","A7"]}),
    dict(segments={"train":["A5"], "valid":["B5"]}, labels=pd.Series({"A5":1.0,"B5":2.0})),
    dict(segments={"train":["A2"], "valid":["A5"]}, labels=pd.Series({"A2":1.0,"A5":2.0,"A7":3.0})),
])
def test_dataset_rejects_label_leakage_and_invalid_endpoint_selection(kwargs):
    with pytest.raises(ModelMainlineError): qlib_sequence_dataset(plan(), **kwargs)


def test_window_evidence_round_trip_plain_parquet(tmp_path):
    windows = plan()
    tables = {}
    for name in ("context", "targets", "members", "exclusions"):
        path = tmp_path / f"{name}.parquet"
        getattr(windows, name).to_parquet(path, index=False)
        tables[name] = pd.read_parquet(path)
    restored = replace(windows, **tables)
    assert verify(restored)["member_count"] == 42
    dataset = qlib_sequence_dataset(restored, segments={"test":["A7"]})
    np.testing.assert_array_equal(dataset.prepare("test", col_set="feature")[0], [[50,51],[60,61],[70,71]])


def test_shared_feature_assembly_preserves_table_model_samples():
    raw, ends, _ = fixture()
    labels = ends.drop(columns=["sample_id", "target"]).copy()
    labels["label_start_time"] = labels.decision_time
    labels["label_end_time"] = labels.decision_time + pd.Timedelta(days=1)
    labels["available_time"] = labels.label_end_time
    labels["horizon_sessions"] = 1
    labels["forward_return"] = np.arange(len(labels))/100
    labels["lineage_hash"] = "label-source"
    samples, columns = assemble_daily_model_samples(raw, labels, horizon_sessions=1)
    assert columns == COLUMNS and len(samples) == 18
    assert "feature_count" not in samples
    first = samples.loc[samples.entity_id == "A"].iloc[0]
    assert first.sample_id == "A:2024-01-02:h1" and first.x__w1 == 0 and first.y__w1 == 1
    assert first.target == 0


@pytest.mark.parametrize("column", ["observation_time", "available_time"])
def test_missing_source_time_cannot_be_used_as_historical_context(column):
    raw, _, _ = fixture()
    raw.loc[0, column] = pd.NaT
    with pytest.raises(ModelMainlineError, match="时间不得缺失"):
        plan(features=raw)


def test_rejected_and_future_rows_do_not_change_earlier_qlib_window():
    raw, ends, calendar = fixture()
    earlier = ends.loc[ends.sample_id.isin(["A2", "B2"])]
    first = plan(features=raw, targets=earlier)
    raw.loc[raw.observation_session > calendar[2], "value"] = 1e12
    later = plan(features=raw, targets=earlier)
    for windows in (first, later):
        sampler = qlib_sequence_dataset(windows, segments={"test":["A2"]}).prepare("test", col_set="feature")
        np.testing.assert_array_equal(sampler[0], [[0,1],[10,11],[20,21]])


def test_verifier_does_not_call_window_builder(monkeypatch):
    windows = plan()
    def reject(*args, **kwargs):
        raise AssertionError("独立复核不得调用生产窗口构建器")
    import research_pipeline.research.modeling.sequence as sequence
    monkeypatch.setattr(sequence, "build_sequence_windows", reject)
    monkeypatch.setattr(sequence, "assemble_daily_feature_context", reject)
    assert verify(windows)["window_count"] == 14
