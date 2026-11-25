"""在不可变工件之间共享文件，删除目录项时保留文件权限。"""

from __future__ import annotations

import os
from pathlib import Path
import shutil


def freeze_file(path: Path) -> None:
    path.chmod(path.stat().st_mode & ~0o222)


def link_readonly_file(source: Path, target: Path, *, freeze_owned: bool = False) -> bool:
    source_info = source.stat()
    if source_info.st_dev != target.parent.stat().st_dev:
        return False
    needs_freeze = bool(source_info.st_mode & 0o222)
    if needs_freeze and (not freeze_owned or source_info.st_nlink != 1):
        return False
    try:
        os.link(source, target)
    except FileExistsError:
        raise
    except OSError:
        return False
    if needs_freeze:
        try:
            freeze_file(target)
        except OSError:
            unlink_readonly_file(target)
            return False
    return True


def unlink_readonly_file(path: Path) -> None:
    try:
        path.unlink()
    except PermissionError:
        if os.name != "nt" or path.stat().st_mode & 0o222:
            raise
        _unlink_windows_readonly_file(path)


def _unlink_windows_readonly_file(path: Path) -> None:
    from ctypes import WinDLL, WinError, byref, sizeof
    from ctypes import wintypes

    kernel = WinDLL("kernel32", use_last_error=True)
    create_file = kernel.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
        wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    set_information = kernel.SetFileInformationByHandle
    set_information.argtypes = (wintypes.HANDLE, wintypes.INT, wintypes.LPVOID, wintypes.DWORD)
    set_information.restype = wintypes.BOOL
    close_handle = kernel.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    delete_access = 0x10000
    share_read_write_delete = 0x07
    open_existing = 3
    handle = create_file(str(path), delete_access, share_read_write_delete, None, open_existing, 0, None)
    if handle == wintypes.HANDLE(-1).value:
        raise WinError()
    try:
        # FileDispositionInfoEx 可忽略只读位，仅移除当前硬链接目录项。
        file_disposition_info_ex = 21
        delete_posix_ignore_readonly = wintypes.DWORD(0x01 | 0x02 | 0x10)
        if not set_information(
            handle, file_disposition_info_ex, byref(delete_posix_ignore_readonly),
            sizeof(delete_posix_ignore_readonly),
        ):
            raise WinError()
    finally:
        close_handle(handle)


def remove_tree(root: Path, *, ignore_errors: bool = False) -> None:
    def on_error(function, path, error_info):
        error = error_info[1]
        if function is os.unlink and isinstance(error, PermissionError):
            try:
                unlink_readonly_file(Path(path))
                return
            except OSError:
                if not ignore_errors:
                    raise
        elif not ignore_errors:
            raise error

    shutil.rmtree(root, onerror=on_error)
