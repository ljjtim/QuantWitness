"""模型独立复核使用的 Result 支持文件绑定。"""
from __future__ import annotations

from research_pipeline.results import QLIB_MODEL_INVENTORY_SCHEMA_ID


_MODEL_HOLDOUT_ARTIFACT_TYPE = "research.model-locked-holdout.v2"
_HOLDOUT_LEDGER_PATHS = frozenset(
    f"holdout-ledger/{name}.json" for name in ("plan", "prepared", "opened", "terminal")
)


def model_verification_support_paths(bundle) -> set[str]:
    """按模型索引工件和 holdout 合同选择配置与账本，不加载模型对象。"""
    model_keys = {
        table.artifact_key for table in bundle.tables
        if table.schema_id == QLIB_MODEL_INVENTORY_SCHEMA_ID
    }
    return {
        item.relative_path for item in bundle.support_files
        if (item.artifact_key in model_keys and item.source_path.endswith(".json"))
        or (item.artifact_type == _MODEL_HOLDOUT_ARTIFACT_TYPE
            and item.source_path in _HOLDOUT_LEDGER_PATHS)
    }
