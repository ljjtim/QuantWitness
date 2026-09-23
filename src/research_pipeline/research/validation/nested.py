"""外层一次测试、内层仅训练区间选参的嵌套验证。"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from research_pipeline.platform.canonical import typed_canonical_hash

from .splits import SplitManifest, ValidationError, build_purged_kfold


@dataclass(frozen=True)
class NestedValidationManifest:
    outer: SplitManifest
    inner_by_outer_fold: tuple[tuple[str, SplitManifest], ...]
    manifest_hash: str


def build_nested_validation(samples: pd.DataFrame, *, calendar: object, outer: SplitManifest, inner_folds: int, embargo_sessions: int = 0) -> NestedValidationManifest:
    sample_index = samples.set_index(samples["sample_id"].astype(str), drop=False)
    inner_items: list[tuple[str, SplitManifest]] = []
    for fold in outer.folds:
        if not fold.test_ids:
            raise ValidationError("nested validation 的 outer fold 必须包含 test")
        train_samples = sample_index.loc[list(fold.train_ids)].reset_index(drop=True)
        inner = build_purged_kfold(train_samples, calendar=calendar, folds=inner_folds, embargo_sessions=embargo_sessions)
        allowed = set(fold.train_ids)
        used = {sample_id for item in inner.folds for sample_id in (*item.train_ids, *item.validation_ids)}
        if not used <= allowed or used & set(fold.test_ids):
            raise ValidationError("inner fold 越过 outer train 边界")
        inner_items.append((fold.fold_id, inner))
    payload = {"outer_hash": outer.manifest_hash, "inner": [{"outer_fold_id": key, "inner_hash": value.manifest_hash} for key, value in inner_items]}
    return NestedValidationManifest(outer, tuple(inner_items), typed_canonical_hash(payload))


__all__ = ["NestedValidationManifest", "build_nested_validation"]
