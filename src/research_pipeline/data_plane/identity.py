"""数据平面身份辅助函数。"""

from research_pipeline.platform.canonical import typed_canonical_hash


def identity_hash(payload: object) -> str:
    return typed_canonical_hash(payload)


__all__ = ["identity_hash"]
