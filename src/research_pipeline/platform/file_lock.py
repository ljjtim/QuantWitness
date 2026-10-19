"""本机进程间的短事务文件锁，进程退出后由操作系统释放。"""

from __future__ import annotations

import os
from pathlib import Path
import time


class ProcessFileLock:
    def __init__(self, path: Path, timeout_seconds: float, *, timeout_error: Exception | None = None) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.handle = None
        self.timeout_error = timeout_error or TimeoutError("进程文件锁超时")

    def __enter__(self) -> "ProcessFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.handle.write(b"0")
            self.handle.flush()
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self.handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    self.handle.close()
                    self.handle = None
                    raise self.timeout_error
                time.sleep(0.01)

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle is None:
            return
        self.handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()

