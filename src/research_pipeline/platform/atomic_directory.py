"""同盘目录发布，等待Windows短暂占用释放。"""

from __future__ import annotations

import os
from pathlib import Path
import time


def publish_directory(staging: Path, target: Path) -> None:
    """保留原子重命名；临时占用最多等待0.31秒，失败保留原异常。"""
    for attempt in range(6):
        try:
            os.replace(staging, target)
            return
        except OSError as error:
            if (
                os.name != "nt"
                or getattr(error, "winerror", None) not in {5, 32}
                or attempt == 5
                or not staging.is_dir()
                or target.exists()
            ):
                raise
            time.sleep(0.01 * 2**attempt)
