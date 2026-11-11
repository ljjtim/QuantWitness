"""按真实标签区间 purge、按正式交易日历 embargo 的切分。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import sys

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
    validation_range: tuple[str, str] | None = None
    test_range: tuple[str, str] | None = None


@dataclass(frozen=True)
class SplitManifest:
    method: str
    folds: tuple[SplitFold, ...]
    audit: pd.DataFrame
    calendar_hash: str
    manifest_hash: str


def build_purged_kfold(samples: pd.DataFrame, *, calendar: object, folds: int, embargo_sessions: int = 0, frame_budget=None) -> SplitManifest:
    frame = _normalize_samples(samples)
    sessions = _calendar(calendar)
    if folds < 2 or folds > len(frame):
        raise ValidationError("folds 必须在 2 和样本数之间")
    if embargo_sessions < 0:
        raise ValidationError("embargo_sessions 不能为负")
    ordered = frame.sort_values(["observation_time", "sample_id"], kind="mergesort").reset_index(drop=True)
    blocks = [block for block in _split_positions(len(ordered), folds) if len(block)]
    fold_results: list[SplitFold] = []
    rows: list[pd.DataFrame] = []
    audit_source = _audit_source(frame, frame_budget)
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
        rows.append(_audit_rows(audit_source, fold, frame_budget))
    manifest = _manifest("purged_kfold", tuple(fold_results), rows, sessions, frame_budget)
    if frame_budget is not None:
        frame_budget.release_bytes(audit_source.attrs["audit_source_buffer_bytes"])
    return manifest


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
    prediction_samples=None,
    frame_budget=None,
) -> SplitManifest:
    frame = _normalize_samples(samples)
    sessions = _calendar(calendar)
    for name, value in {"train_sessions": train_sessions, "validation_sessions": validation_sessions, "test_sessions": test_sessions, "step_sessions": step_sessions}.items():
        if value < 1:
            raise ValidationError(f"{name} 必须为正整数")
    if embargo_sessions < 0:
        raise ValidationError("embargo_sessions 不能为负")
    results: list[SplitFold] = []
    rows: list[pd.DataFrame] = []
    audit_source = _audit_source(frame, frame_budget)
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
        prediction_test = (prediction_samples.loc[pd.to_datetime(prediction_samples.observation_time).dt.date.isin(test_dates)]
                           if prediction_samples is not None else test)
        if candidate_train.empty or candidate_validation.empty or prediction_test.empty:
            raise ValidationError("walk-forward 窗口存在空训练/验证/测试集")
        first_eval = candidate_validation["decision_time"].min()
        keep = (candidate_train["label_end"] < first_eval) & (
            candidate_train["label_available_time"] <= first_eval
        )
        train = candidate_train.loc[keep]
        first_test = pd.to_datetime(prediction_test["decision_time"], utc=True).min()
        validation_keep = (candidate_validation["label_end"] < first_test) & (
            candidate_validation["label_available_time"] <= first_test
        )
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
            validation_range=(sessions[validation_start].isoformat(), sessions[validation_end].isoformat()),
            test_range=(sessions[test_start].isoformat(), sessions[test_end].isoformat()),
        )
        results.append(fold)
        rows.append(_audit_rows(audit_source, fold, frame_budget))
        offset += step_sessions
    if not results:
        raise ValidationError("交易日不足，无法构建 walk-forward")
    manifest = _manifest("expanding_walk_forward" if expanding else "rolling_walk_forward", tuple(results), rows, sessions, frame_budget)
    if frame_budget is not None:
        frame_budget.release_bytes(audit_source.attrs["audit_source_buffer_bytes"])
    return manifest


def build_explicit_walk_forward(samples: pd.DataFrame, *, calendar: object, folds: object, prediction_samples=None, frame_budget=None) -> SplitManifest:
    """按声明日期构建fold，沿用真实标签成熟时间purge。"""
    frame = _normalize_samples(samples)
    sessions = _calendar(calendar)
    if not isinstance(folds, (list, tuple)) or not folds:
        raise ValidationError("显式fold必须是非空列表")
    results, rows, identifiers = [], [], set()
    audit_source = _audit_source(frame, frame_budget)
    for spec in folds:
        if not isinstance(spec, dict) or set(spec) != {"fold_id", "train", "validation", "test"}:
            raise ValidationError("显式fold要求fold_id/train/validation/test")
        identifier = spec["fold_id"]
        if not isinstance(identifier, str) or not identifier or identifier in identifiers:
            raise ValidationError("显式fold_id必须非空且唯一")
        identifiers.add(identifier)
        ranges = []
        for role in ("train", "validation", "test"):
            interval = spec[role]
            if not isinstance(interval, (list, tuple)) or len(interval) != 2:
                raise ValidationError("fold日期区间必须含起止两个ISO日期")
            start, end = (date.fromisoformat(value) for value in interval)
            if start > end:
                raise ValidationError("fold区间起点晚于终点")
            ranges.append((start, end))
        if not (ranges[0][1] < ranges[1][0] and ranges[1][1] < ranges[2][0]):
            raise ValidationError("显式fold训练、验证、测试必须按时间分离")
        subsets = [frame.loc[frame.observation_time.dt.date.between(start, end)] for start, end in ranges]
        prediction_test = (prediction_samples.loc[pd.to_datetime(prediction_samples.observation_time).dt.date.between(*ranges[2])]
                           if prediction_samples is not None else subsets[2])
        if subsets[0].empty or subsets[1].empty or prediction_test.empty:
            raise ValidationError("显式fold存在空训练、验证或测试集")
        train, valid, test = subsets
        valid_boundary, test_boundary = valid.decision_time.min(), pd.to_datetime(prediction_test.decision_time, utc=True).min()
        train_keep = (train.label_end < valid_boundary) & (train.label_available_time <= valid_boundary)
        valid_keep = (valid.label_end < test_boundary) & (valid.label_available_time <= test_boundary)
        if not train_keep.any() or not valid_keep.any():
            raise ValidationError("显式fold标签purge后为空")
        fold = SplitFold(identifier, tuple(train.loc[train_keep, "sample_id"]),
            tuple(valid.loc[valid_keep, "sample_id"]), tuple(test.sample_id),
            tuple(train.loc[~train_keep, "sample_id"]) + tuple(valid.loc[~valid_keep, "sample_id"]), (),
            tuple(spec["validation"]), tuple(spec["test"]))
        results.append(fold)
        rows.append(_audit_rows(audit_source, fold, frame_budget))
    manifest = _manifest("explicit_walk_forward", tuple(results), rows, sessions, frame_budget)
    if frame_budget is not None:
        frame_budget.release_bytes(audit_source.attrs["audit_source_buffer_bytes"])
    return manifest


def _normalize_samples(samples: pd.DataFrame) -> pd.DataFrame:
    required = {"sample_id", "observation_time", "label_start", "label_end"}
    missing = required - set(samples.columns)
    if missing:
        raise ValidationError(f"切分样本缺少字段: {sorted(missing)}")
    frame = samples.loc[:, sorted(required | ({"decision_time", "label_available_time"} & set(samples.columns)))].copy()
    if "decision_time" not in frame:
        frame["decision_time"] = frame["observation_time"]
    if "label_available_time" not in frame:
        frame["label_available_time"] = frame["label_end"]
    frame["sample_id"] = frame["sample_id"].astype(str)
    if frame["sample_id"].duplicated().any() or frame["sample_id"].eq("").any():
        raise ValidationError("sample_id 必须非空且唯一")
    for column in ("observation_time", "decision_time", "label_start", "label_end", "label_available_time"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if frame[["decision_time", "label_available_time"]].isna().any().any():
        raise ValidationError("样本决策时间和标签可见时间不能为空")
    if (frame["label_available_time"] < frame["label_end"]).any():
        raise ValidationError("标签可见时间不能早于标签结束时间")
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


def _audit_source(frame: pd.DataFrame, frame_budget=None) -> pd.DataFrame:
    """每个样本的原始ISO时间只构造一次，各fold共享字符串。"""
    if frame_budget is not None:
        frame_budget.require_additional(len(frame) * (4 * 8 + 3 * 128), label="切分审计共享ISO时间及列缓冲")
    source = frame.loc[:, ["sample_id", "observation_time", "label_start", "label_end"]].copy()
    payload_bytes = 0
    def iso(value):
        nonlocal payload_bytes
        result = value.isoformat()
        payload_bytes += sys.getsizeof(result)
        return result
    for column in ("observation_time", "label_start", "label_end"):
        source[column] = source[column].map(iso)
    if frame_budget is not None:
        shallow = int(source.memory_usage(index=True, deep=False).sum())
        frame_budget.reserve_bytes(shallow + payload_bytes, label="切分审计共享时间")
        source.attrs["audit_source_buffer_bytes"] = shallow
        source.attrs["audit_time_payload_bytes"] = payload_bytes
    return source


def _audit_rows(source: pd.DataFrame, fold: SplitFold, frame_budget=None) -> pd.DataFrame:
    if frame_budget is not None:
        frame_budget.require_additional(len(source) * 12 * 8, label="切分审计fold列缓冲")
    roles = pd.Series(index=source.index, dtype=object)
    for role, identifiers in (("train", fold.train_ids), ("validation", fold.validation_ids),
                              ("test", fold.test_ids), ("purged", fold.purged_ids),
                              ("embargoed", fold.embargoed_ids)):
        roles.loc[source.sample_id.isin(identifiers)] = role
    selected = roles.notna()
    result = source.loc[selected].copy()
    result.insert(0, "fold_id", fold.fold_id)
    result.insert(2, "role", roles.loc[selected].to_numpy())
    result["exclusion_reason"] = roles.loc[selected].where(
        roles.loc[selected].isin(("purged", "embargoed")), None,
    ).to_numpy()
    if frame_budget is not None:
        size = int(result.memory_usage(index=True, deep=False).sum())
        frame_budget.reserve_bytes(size, label="切分审计fold列")
        result.attrs["audit_buffer_bytes"] = size
    return result


def _manifest(method: str, folds: tuple[SplitFold, ...], rows: list[pd.DataFrame], sessions: tuple[date, ...], frame_budget=None) -> SplitManifest:
    if frame_budget is not None:
        frame_budget.require_additional(
            sum(len(part) for part in rows) * 7 * 8 * 3,
            label="切分审计拼接排序及共享ISO时间",
        )
    audit = pd.concat(rows, ignore_index=True).sort_values(["fold_id", "sample_id"], kind="mergesort").reset_index(drop=True)
    if frame_budget is not None:
        frame_budget.reserve_bytes(int(audit.memory_usage(index=True, deep=False).sum()),
                                   label="切分审计表列缓冲")
        for part in rows:
            frame_budget.release_bytes(part.attrs["audit_buffer_bytes"])
    calendar_hash = typed_canonical_hash([value.isoformat() for value in sessions])
    payload = {"method": method, "calendar_hash": calendar_hash, "folds": [{"fold_id": fold.fold_id, "train_ids": list(fold.train_ids), "validation_ids": list(fold.validation_ids), "test_ids": list(fold.test_ids), "purged_ids": list(fold.purged_ids), "embargoed_ids": list(fold.embargoed_ids), "validation_range": fold.validation_range, "test_range": fold.test_range} for fold in folds]}
    return SplitManifest(method, folds, audit, calendar_hash, typed_canonical_hash(payload))


__all__ = ["SplitFold", "SplitManifest", "ValidationError", "build_purged_kfold", "build_walk_forward"]
