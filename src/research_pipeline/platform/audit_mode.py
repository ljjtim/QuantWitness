"""严格审计在当前命令及它启动的工作进程中生效。"""
from contextlib import contextmanager
import os


def strict_audit_enabled():
    return os.environ.get("QUANTWITNESS_STRICT_AUDIT") == "1"


def strict_cache_identity():
    return os.environ.get("QUANTWITNESS_FROZEN_CACHE_AUDIT", os.environ.get("QUANTWITNESS_STRICT_AUDIT")) == "1"


@contextmanager
def strict_audit_scope(enabled, *, frozen_cache_audit=None):
    updates = {}
    if enabled:
        updates["QUANTWITNESS_STRICT_AUDIT"] = "1"
    if frozen_cache_audit is not None:
        updates["QUANTWITNESS_FROZEN_CACHE_AUDIT"] = "1" if frozen_cache_audit else "0"
    previous = {key: os.environ.get(key) for key in updates}
    os.environ.update(updates)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
