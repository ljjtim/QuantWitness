"""跨进程共享读租约与独占发布窗口。"""

from __future__ import annotations

import os
from pathlib import Path
import time
from typing import BinaryIO


class FactorLockTimeout(TimeoutError):
    """在期限内未取得因子库锁。"""


class FactorLockStateError(RuntimeError):
    """锁对象被重复进入或重复释放。"""


def factor_lock_paths(database_path: str | Path) -> tuple[Path, Path]:
    database_path = Path(database_path)
    return (
        Path(f"{database_path}.writer.lock"),
        Path(f"{database_path}.writer.lock.gate"),
    )


if os.name == "nt":
    import ctypes
    import msvcrt
    from ctypes import wintypes

    _LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
    _LOCKFILE_EXCLUSIVE_LOCK = 0x00000002
    _ERROR_LOCK_VIOLATION = 33
    _ERROR_IO_PENDING = 997

    class _Overlapped(ctypes.Structure):
        _fields_ = [
            ("Internal", ctypes.c_void_p),
            ("InternalHigh", ctypes.c_void_p),
            ("Offset", wintypes.DWORD),
            ("OffsetHigh", wintypes.DWORD),
            ("hEvent", wintypes.HANDLE),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _lock_file_ex = _kernel32.LockFileEx
    _lock_file_ex.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_Overlapped),
    ]
    _lock_file_ex.restype = wintypes.BOOL
    _unlock_file_ex = _kernel32.UnlockFileEx
    _unlock_file_ex.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_Overlapped),
    ]
    _unlock_file_ex.restype = wintypes.BOOL


class _FileRangeLock:
    def __init__(self, path: Path, *, exclusive: bool) -> None:
        self.path = path
        self.exclusive = exclusive
        self._file: BinaryIO | None = None
        self._platform_state = None

    def try_acquire(self) -> bool:
        if self._file is not None:
            raise FactorLockStateError(f"文件锁已经持有: {self.path}")
        file_obj = open(self.path, "a+b", buffering=0)
        if file_obj.seek(0, os.SEEK_END) == 0:
            file_obj.write(b"\0")
        file_obj.seek(0)
        try:
            acquired = self._try_platform_lock(file_obj)
        except Exception:
            file_obj.close()
            raise
        if not acquired:
            file_obj.close()
            return False
        self._file = file_obj
        return True

    def release(self) -> None:
        if self._file is None:
            raise FactorLockStateError(f"文件锁尚未持有: {self.path}")
        file_obj = self._file
        self._file = None
        try:
            self._platform_unlock(file_obj)
        finally:
            self._platform_state = None
            file_obj.close()

    def _try_platform_lock(self, file_obj: BinaryIO) -> bool:
        if os.name == "nt":
            overlapped = _Overlapped()
            handle = wintypes.HANDLE(msvcrt.get_osfhandle(file_obj.fileno()))
            flags = _LOCKFILE_FAIL_IMMEDIATELY
            if self.exclusive:
                flags |= _LOCKFILE_EXCLUSIVE_LOCK
            if _lock_file_ex(handle, flags, 0, 1, 0, ctypes.byref(overlapped)):
                self._platform_state = overlapped
                return True
            error_code = ctypes.get_last_error()
            if error_code in {_ERROR_LOCK_VIOLATION, _ERROR_IO_PENDING}:
                return False
            raise OSError(error_code, f"LockFileEx 失败: {self.path}")

        import fcntl

        operation = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
        try:
            fcntl.flock(file_obj.fileno(), operation | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        self._platform_state = True
        return True

    def _platform_unlock(self, file_obj: BinaryIO) -> None:
        if os.name == "nt":
            overlapped = self._platform_state
            handle = wintypes.HANDLE(msvcrt.get_osfhandle(file_obj.fileno()))
            if not _unlock_file_ex(
                handle,
                0,
                1,
                0,
                ctypes.byref(overlapped),
            ):
                error_code = ctypes.get_last_error()
                raise OSError(error_code, f"UnlockFileEx 失败: {self.path}")
            return

        import fcntl

        fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)


def _acquire_until(
    lock: _FileRangeLock,
    *,
    deadline: float,
    poll_interval_seconds: float,
) -> bool:
    while True:
        if lock.try_acquire():
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(poll_interval_seconds, remaining))


class _FactorLeaseBase:
    logical_mode = ""

    def __init__(
        self,
        database_path: str | Path,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds 不能小于 0")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds 必须大于 0")
        self.database_path = Path(database_path)
        self.timeout_seconds = float(timeout_seconds)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.main_lock_path, self.gate_lock_path = factor_lock_paths(
            self.database_path
        )
        self._main_lock: _FileRangeLock | None = None

    @property
    def acquired(self) -> bool:
        return self._main_lock is not None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self.release()
        return False

    def release(self) -> None:
        if self._main_lock is None:
            raise FactorLockStateError(
                f"{self.logical_mode} 因子库锁尚未持有: {self.main_lock_path}"
            )
        main_lock = self._main_lock
        self._main_lock = None
        main_lock.release()

    def _ensure_not_acquired(self) -> None:
        if self.acquired:
            raise FactorLockStateError(
                f"{self.logical_mode} 因子库锁已经持有: {self.main_lock_path}"
            )

    def _timeout_error(self) -> FactorLockTimeout:
        return FactorLockTimeout(
            f"等待因子库 {self.logical_mode} 锁超时: "
            f"path={self.main_lock_path}, "
            f"wait_seconds={self.timeout_seconds:.3f}；"
            "请等待当前租约或发布窗口释放后重试"
        )


class FactorReadLease(_FactorLeaseBase):
    """持有主锁共享段的只读租约。"""

    logical_mode = "shared"

    def __init__(
        self,
        database_path: str | Path,
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        super().__init__(
            database_path,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )

    def acquire(self) -> "FactorReadLease":
        self._ensure_not_acquired()
        deadline = time.monotonic() + self.timeout_seconds
        gate_lock = _FileRangeLock(self.gate_lock_path, exclusive=True)
        if not _acquire_until(
            gate_lock,
            deadline=deadline,
            poll_interval_seconds=self.poll_interval_seconds,
        ):
            raise self._timeout_error()
        try:
            main_lock = _FileRangeLock(self.main_lock_path, exclusive=False)
            if not _acquire_until(
                main_lock,
                deadline=deadline,
                poll_interval_seconds=self.poll_interval_seconds,
            ):
                raise self._timeout_error()
            self._main_lock = main_lock
        finally:
            gate_lock.release()
        return self


class FactorPublishWindow(_FactorLeaseBase):
    """阻止新读者后取得主锁独占段的发布窗口。"""

    logical_mode = "exclusive"

    def __init__(
        self,
        database_path: str | Path,
        timeout_seconds: float = 300.0,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        super().__init__(
            database_path,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )

    def acquire(self) -> "FactorPublishWindow":
        self._ensure_not_acquired()
        deadline = time.monotonic() + self.timeout_seconds
        gate_lock = _FileRangeLock(self.gate_lock_path, exclusive=True)
        if not _acquire_until(
            gate_lock,
            deadline=deadline,
            poll_interval_seconds=self.poll_interval_seconds,
        ):
            raise self._timeout_error()
        try:
            main_lock = _FileRangeLock(self.main_lock_path, exclusive=True)
            if not _acquire_until(
                main_lock,
                deadline=deadline,
                poll_interval_seconds=self.poll_interval_seconds,
            ):
                raise self._timeout_error()
            self._main_lock = main_lock
        finally:
            gate_lock.release()
        return self


__all__ = [
    "FactorLockStateError",
    "FactorLockTimeout",
    "FactorPublishWindow",
    "FactorReadLease",
    "factor_lock_paths",
]
