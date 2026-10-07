"""独立切分按真实观察时间分组，决策会话可晚一个会话。"""
from copy import deepcopy
from datetime import date
import json

import pandas as pd
import pytest

from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.model_validity import _split
from research_pipeline.research.validation import build_walk_forward
from test_model_validity import model_fixture, sample, stamp


def lagged_fixture(expanding):
    model = model_fixture()
    rows = [sample(day) for day in range(2, 15)]
    for row in rows:
        day = int(row["observation_session"][-2:])
        row["observation_time"] = stamp(day - 1, 15)
    model["tables"]["samples"] = rows
    model["design"].update(expanding=expanding,
                          split_calendar_sessions=[f"2020-01-{day:02d}" for day in range(1, 18)])
    bind_runtime_split(model, use_session=False)
    return model


def bind_runtime_split(model, *, use_session):
    rows, design = model["tables"]["samples"], model["design"]
    frame = pd.DataFrame(rows)
    if use_session:
        frame["observation_time"] = pd.to_datetime(frame["observation_session"], utc=True)
    else:
        frame["observation_time"] = pd.to_datetime(frame["observation_time"], utc=True)
    last_day = frame["observation_time"].max().date()
    calendar = [date.fromisoformat(day) for day in design["split_calendar_sessions"]
                if date.fromisoformat(day) <= last_day]
    frame = frame.rename(columns={"label_start_time": "label_start", "label_end_time": "label_end"})
    split = build_walk_forward(frame, calendar=calendar,
                               **{name: design[name] for name in ("train_sessions", "validation_sessions",
                                  "test_sessions", "step_sessions", "embargo_sessions", "expanding")})
    model["tables"]["split_audit"] = split.audit.to_dict("records")
    model["tables"]["fit_audit"] = [
        dict(fold_id=fold.fold_id, candidate_id=cid,
             train_ids_json=json.dumps(fold.train_ids), validation_ids_json=json.dumps(fold.validation_ids),
             evidence_scope="processor_train_sample_scope")
        for fold in split.folds for cid in design["candidate_ids"]
    ]
    return split


@pytest.mark.parametrize("expanding", [True, False], ids=["expanding", "rolling"])
def test_previous_close_observation_matches_runtime_split(expanding, monkeypatch):
    model = lagged_fixture(expanding)
    import research_pipeline.research.validation as production
    monkeypatch.setattr(production, "build_walk_forward",
                        lambda *args, **kwargs: pytest.fail("独立复核不能调用生产切分器"))
    samples, folds, candidates = _split(model)
    assert len(samples) == 13 and len(folds) == 3 and candidates == {"a", "b"}
    first = folds["walk_forward_001"]
    assert first["train"] == {"s2", "s3", "s4"}
    assert first["validation"] == {"s5", "s6"}
    assert first["embargoed"] == {"s7"} and first["test"] == {"s8", "s9"}
    last = folds["walk_forward_003"]
    assert last["train"] == ({f"s{day}" for day in range(2, 9)} if expanding else {"s6", "s7", "s8"})
    assert last["test"] == {"s12", "s13"}
    assert all(date.fromisoformat(row["observation_session"]) > pd.Timestamp(row["observation_time"]).date()
               for row in samples.values())


@pytest.mark.parametrize("expanding", [True, False], ids=["expanding", "rolling"])
def test_session_grouping_cannot_replace_observation_split(expanding):
    model = lagged_fixture(expanding)
    forged = bind_runtime_split(model, use_session=True)
    assert len(forged.folds) == 4
    with pytest.raises(EvidenceContractError, match="切分成员不一致"):
        _split(model)


def test_previous_close_split_rejects_forged_embargo_and_fit_scope():
    model = lagged_fixture(False)
    changed = deepcopy(model)
    row = next(row for row in changed["tables"]["split_audit"] if row["role"] == "embargoed")
    row.update(role="train", exclusion_reason=None)
    with pytest.raises(EvidenceContractError, match="purge 或 embargo 角色不一致"):
        _split(changed)
    changed = deepcopy(model)
    changed["tables"]["fit_audit"][0]["train_ids_json"] = '["s2", "s3", "s5"]'
    with pytest.raises(EvidenceContractError, match="实际拟合范围不一致"):
        _split(changed)
