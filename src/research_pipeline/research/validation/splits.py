"""按真实标签区间 purge、按正式交易日历 embargo 的切分。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from research_pipeline.platform.canonical import typed_canonical_hash
from research_pipeline.platform.errors import MainlineError


class ValidationError(MainlineError):
    error_code = "research_validation_invalid"


@dataclass(frozen=True)
class SplitFold:
    fold_id: str
    train_ids: tuple[str, ...]
    validation_ids: tuple[str, ...]
    test_ids: tuple[str, ...]
    purged_ids: tuple[str, ...]
    embargoed_ids: tuple[str, ...]


@dataclass(frozen=True)
class SplitManifest:
    method: str
    folds: tuple[SplitFold, ...]
    audit: pd.DataFrame
    calendar_hash: str
    manifest_hash: str


def build_purged_kfold(samples: pd.DataFrame, *, calendar: object, folds: int, embargo_sessions: int = 0) -> SplitManifest:
    frame = _normalize_samples(samples)
    sessions = _calendar(calendar)
    if folds < 2 or folds > len(frame):
        raise ValidationError("folds 必须在 2 和样本数之间")
    if embargo_sessions < 0:
        raise ValidationError("embargo_sessions 不能为负")
    ordered = frame.sort_values(["observation_time", "sample_id"], kind="mergesort").reset_index(drop=True)
    blocks = [block for block in _split_positions(len(ordered), folds) if len(block)]
    fold_results: list[SplitFold] = []
    rows: list[dict[str, object]] = []
    for index, positions in enumerate(blocks, start=1):
        validation = ordered.iloc[positions]
        eval_start = validation["observation_time"].min()
        eval_end = validation["observation_time"].max()
        candidates = ordered.drop(index=positions)
        overlap = (candidates["label_start"] <= eval_end) & (candidates["label_end"] >= eval_start)
        embargo_dates = set(_sessions_after(sessions, eval_end.date(), embargo_sessions))
        embargo = candidates["observation_time"].dt.date.isin(embargo_dates)
        train = candidates.loc[~overlap & ~embargo]
        if train.empty or validation.empty:
            raise ValidationError("purged fold 产生空训练集或验证集")
        fold = SplitFold(
            fold_id=f"purged_{index:03d}",
            train_ids=tuple(train["sample_id"]),
            validation_ids=tuple(validation["sample_id"]),
            test_ids=(),
            purged_ids=tuple(candidates.loc[overlap, "sample_id"]),
            embargoed_ids=tuple(candidates.loc[~overlap & embargo, "sample_id"]),
        )
        fold_results.append(fold)
        rows.extend(_audit_rows(ordered, fold))
    return _manifest("purged_kfold", tuple(fold_results), rows, sessions)


def build_walk_forward(
    samples: pd.DataFrame,
    *,
    calendar: object,
    train_sessions: int,
    validation_sessions: int,
    test_sessions: int,
    step_sessions: int,
    embargo_sessions: int = 0,
    expanding: bool = True,
) -> SplitManifest:
    frame = _normalize_samples(samples)
    sessions = _calendar(calendar)
    for name, value in {"train_sessions": train_sessions, "validation_sessions": validation_sessions, "test_sessions": test_sessions, "step_sessions": step_sessions}.items():
        if value < 1:
            raise ValidationError(f"{name} 必须为正整数")
    if embargo_sessions < 0:
        raise ValidationError("embargo_sessions 不能为负")
    results: list[SplitFold] = []
    rows: list[dict[str, object]] = []
    offset = 0
    while True:
        train_start = 0 if expanding else offset
        train_end = offset + train_sessions - 1
        validation_start = train_end + 1
        validation_end = validation_start + validation_sessions - 1
        test_start = validation_end + embargo_sessions + 1
        test_end = test_start + test_sessions - 1
        if test_end >= len(sessions):
            break
        train_dates = set(sessions[train_start:train_end + 1])
        validation_dates = set(sessions[validation_start:validation_end + 1])
        test_dates = set(sessions[test_start:test_end + 1])
        candidate_train = frame.loc[frame["observation_time"].dt.date.isin(train_dates)]
        candidate_validation = frame.loc[frame["observation_time"].dt.date.isin(validation_dates)]
        test = frame.loc[frame["observation_time"].dt.date.isin(test_dates)]
        if candidate_train.empty or candidate_validation.empty or test.empty:
            raise ValidationError("walk-forward 窗口存在空训练/验证/测试集")
        first_eval = candidate_validation["observation_time"].min()
        keep = candidate_train["label_end"] < first_eval
        train = candidate_train.loc[keep]
        first_test = test["observation_time"].min()
        validation_keep = candidate_validation["label_end"] < first_test
        validation = candidate_validation.loc[validation_keep]
        embargo_dates = set(sessions[validation_end + 1:test_start])
        embargoed = frame.loc[frame["observation_time"].dt.date.isin(embargo_dates)]
        if train.empty or validation.empty:
            raise ValidationError("标签 purge 后训练集或验证集为空")
        purged = tuple(candidate_train.loc[~keep, "sample_id"]) + tuple(
            candidate_validation.loc[~validation_keep, "sample_id"]
        )
        fold = SplitFold(
            fold_id=f"walk_forward_{len(results) + 1:03d}",
            train_ids=tuple(train["sample_id"]),
            validation_ids=tuple(validation["sample_id"]),
            test_ids=tuple(test["sample_id"]),
            purged_ids=purged,
            embargoed_ids=tuple(embargoed["sample_id"]),
        )
        results.append(fold)
        rows.extend(_audit_rows(frame, fold))
        offset += step_sessions
    if not results:
        raise ValidationError("交易日不足，无法构建 walk-forward")
    return _manifest("expanding_walk_forward" if expanding else "rolling_walk_forward", tuple(results), rows, sessions)


def _normalize_samples(samples: pd.DataFrame) -> pd.DataFrame:
    required = {"sample_id", "observation_time", "label_start", "label_end"}
    missing = required - set(samples.columns)
    if missing:
        raise ValidationError(f"切分样本缺少字段: {sorted(missing)}")
    frame = samples.loc[:, sorted(required)].copy()
    frame["sample_id"] = frame["sample_id"].astype(str)
    if frame["sample_id"].duplicated().any() or frame["sample_id"].eq("").any():
        raise ValidationError("sample_id 必须非空且唯一")
    for column in ("observation_time", "label_start", "label_end"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if ((frame["observation_time"] > frame["label_start"]) | (frame["label_start"] >= frame["label_end"])).any():
        raise ValidationError("样本时间必须满足 observation <= label_start < label_end")
    return frame.sort_values(["observation_time", "sample_id"], kind="mergesort").reset_index(drop=True)


def _calendar(calendar: object) -> tuple[date, ...]:
    if isinstance(calendar, (str, bytes)):
        raise ValidationError("交易日历必须是日期序列")
    values = tuple(pd.Timestamp(value).date() for value in calendar)
    if len(values) < 2 or values != tuple(sorted(set(values))):
        raise ValidationError("交易日历必须非空、唯一且严格递增")
    return values


def _sessions_after(sessions: tuple[date, ...], anchor: date, count: int) -> tuple[date, ...]:
    if count == 0:
        return ()
    try:
        position = sessions.index(anchor)
    except ValueError as exc:
        raise ValidationError("评估日期不在正式交易日历中") from exc
    return sessions[position + 1:position + 1 + count]


def _split_positions(size: int, groups: int) -> list[list[int]]:
    base, remainder = divmod(size, groups)
    output = []
    start = 0
    for index in range(groups):
        length = base + (1 if index < remainder else 0)
        output.append(list(range(start, start + length)))
        start += length
    return output


def _audit_rows(frame: pd.DataFrame, fold: SplitFold) -> list[dict[str, object]]:
    role = {sample_id: "train" for sample_id in fold.train_ids}
    role.update({sample_id: "validation" for sample_id in fold.validation_ids})
    role.update({sample_id: "test" for sample_id in fold.test_ids})
    role.update({sample_id: "purged" for sample_id in fold.purged_ids})
    role.update({sample_id: "embargoed" for sample_id in fold.embargoed_ids})
    rows = []
    for row in frame.itertuples(index=False):
        if row.sample_id not in role:
            continue
        rows.append({"fold_id": fold.fold_id, "sample_id": row.sample_id, "role": role[row.sample_id], "observation_time": row.observation_time.isoformat(), "label_start": row.label_start.isoformat(), "label_end": row.label_end.isoformat(), "exclusion_reason": role[row.sample_id] if role[row.sample_id] in {"purged", "embargoed"} else None})
    return rows


def _manifest(method: str, folds: tuple[SplitFold, ...], rows: list[dict[str, object]], sessions: tuple[date, ...]) -> SplitManifest:
    audit = pd.DataFrame(rows).sort_values(["fold_id", "sample_id"], kind="mergesort").reset_index(drop=True)
    calendar_hash = typed_canonical_hash([value.isoformat() for value in sessions])
    payload = {"method": method, "calendar_hash": calendar_hash, "folds": [{"fold_id": fold.fold_id, "train_ids": list(fold.train_ids), "validation_ids": list(fold.validation_ids), "test_ids": list(fold.test_ids), "purged_ids": list(fold.purged_ids), "embargoed_ids": list(fold.embargoed_ids)} for fold in folds]}
    return SplitManifest(method, folds, audit, calendar_hash, typed_canonical_hash(payload))


__all__ = ["SplitFold", "SplitManifest", "ValidationError", "build_purged_kfold", "build_walk_forward"]
