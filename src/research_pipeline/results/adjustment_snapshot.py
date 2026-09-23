"""ResultStore 内 PIT 复权快照的独立复核入口。"""

from __future__ import annotations

from research_pipeline.data_plane.errors import QualityGateError
from research_pipeline.data_plane.minute_adjustment import (
    verify_adjustment_anchor_result_tables as _verify_anchor_tables,
    verify_adjustment_snapshot_result_table as _verify_snapshot_table,
)


def verify_adjustment_snapshot_result_table(table: object) -> None:
    try:
        _verify_snapshot_table(table)
    except QualityGateError as exc:
        raise ValueError(str(exc)) from exc


def verify_adjustment_anchor_result_tables(
    snapshot_table: object,
    feature_table: object | None,
    label_table: object | None,
) -> None:
    try:
        _verify_anchor_tables(snapshot_table, feature_table, label_table)
    except QualityGateError as exc:
        raise ValueError(str(exc)) from exc


__all__ = [
    "verify_adjustment_anchor_result_tables",
    "verify_adjustment_snapshot_result_table",
]
