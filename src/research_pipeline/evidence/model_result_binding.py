"""模型独立复核使用的 Result 支持文件绑定。"""
from __future__ import annotations

from research_pipeline.results import QLIB_MODEL_INVENTORY_SCHEMA_ID


_MODEL_HOLDOUT_ARTIFACT_TYPE = "research.model-locked-holdout.v2"
_HOLDOUT_LEDGER_PATHS = frozenset(
    f"holdout-ledger/{name}.json" for name in ("plan", "prepared", "opened", "terminal")
)


def model_verification_support_paths(bundle) -> set[str]:
    """预载模型配置、序列窗口和 holdout 账本，不加载模型对象及权重。"""
    model_keys = {
        table.artifact_key for table in bundle.tables
        if table.schema_id == QLIB_MODEL_INVENTORY_SCHEMA_ID
    }
    sequence_suffixes = tuple(
        f"/sequence/{name}.parquet" for name in ("context", "targets", "members")
    )
    return {
        item.relative_path for item in bundle.support_files
        if (item.artifact_key in model_keys and (
            item.source_path.endswith(".json")
            or item.source_path.endswith(sequence_suffixes)
            or item.source_path.endswith(("/network.py", "/network-weights.npz"))
        ))
        or (item.artifact_type == _MODEL_HOLDOUT_ARTIFACT_TYPE
            and item.source_path in _HOLDOUT_LEDGER_PATHS)
    }
