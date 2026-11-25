"""在一次文件验证会话内观察目录树是否变化。"""
import os
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar


def watch_directory(root, resources):
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    start = kernel.FindFirstChangeNotificationW
    start.argtypes = (wintypes.LPCWSTR, wintypes.BOOL, wintypes.DWORD)
    start.restype = wintypes.HANDLE
    # 文件与目录改名、新增、删除、长度或写入变化都会置位，恢复原时间戳也会触发。
    handle = start(str(root), True, 0x01 | 0x02 | 0x08 | 0x10)
    if handle is None or handle == ctypes.c_void_p(-1).value:
        return None
    close = kernel.FindCloseChangeNotification
    close.argtypes = (wintypes.HANDLE,)
    resources.callback(close, handle)
    poll = kernel.WaitForSingleObject
    poll.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    poll.restype = wintypes.DWORD
    return lambda: poll(handle, 0) == 258  # WAIT_TIMEOUT 表示没有变化通知。


bundle_checks = ContextVar("project_bundle_checks", default=None)


@contextmanager
def project_bundle_check_scope():
    if bundle_checks.get() is not None:
        yield
        return
    with ExitStack() as handles:
        token = bundle_checks.set(({}, handles))
        try:
            yield
        finally:
            bundle_checks.reset(token)
