"""目录提交只对真实可达的Windows暂时占用作有界重试。"""

import pytest

from research_pipeline.platform import atomic_directory
from research_pipeline.runtime.external_artifact import ExternalArtifactStore


def _staging(tmp_path):
    source = tmp_path / "staging"
    source.mkdir()
    (source / "payload.txt").write_text("sealed", encoding="utf-8")
    return source, tmp_path / "published"


def _access_error(code):
    error = PermissionError("目录被占用")
    error.winerror = code
    return error


@pytest.mark.skipif(atomic_directory.os.name != "nt", reason="Windows目录占用语义")
@pytest.mark.parametrize("code", [5, 32])
def test_directory_publication_retries_temporary_windows_conflict(tmp_path, monkeypatch, code):
    source, target = _staging(tmp_path)
    original = atomic_directory.os.replace
    calls = []
    waits = []

    def replace(left, right):
        calls.append((left, right))
        assert not target.exists()
        if len(calls) < 3:
            raise _access_error(code)
        original(left, right)

    monkeypatch.setattr(atomic_directory.os, "replace", replace)
    monkeypatch.setattr(atomic_directory.time, "sleep", waits.append)
    atomic_directory.publish_directory(source, target)
    assert waits == [0.01, 0.02]
    assert len(calls) == 3
    assert not source.exists()
    assert (target / "payload.txt").read_text(encoding="utf-8") == "sealed"


@pytest.mark.skipif(atomic_directory.os.name != "nt", reason="Windows目录占用语义")
def test_directory_publication_preserves_persistent_failure_and_staging(tmp_path, monkeypatch):
    source, target = _staging(tmp_path)
    failure = _access_error(5)
    calls = []
    waits = []

    def reject(left, right):
        calls.append((left, right))
        raise failure

    monkeypatch.setattr(atomic_directory.os, "replace", reject)
    monkeypatch.setattr(atomic_directory.time, "sleep", waits.append)
    with pytest.raises(PermissionError) as caught:
        atomic_directory.publish_directory(source, target)
    assert caught.value is failure
    assert len(calls) == 6
    assert sum(waits) == pytest.approx(0.31)
    assert source.is_dir()
    assert not target.exists()


@pytest.mark.parametrize("condition", ["other_error", "existing_target", "missing_source"])
def test_directory_publication_does_not_hide_other_failures(tmp_path, monkeypatch, condition):
    source, target = _staging(tmp_path)
    if condition == "existing_target":
        target.mkdir()
    elif condition == "missing_source":
        source = tmp_path / "absent"
    failure = _access_error(5 if condition != "other_error" else 3)
    calls = []

    def reject(left, right):
        calls.append((left, right))
        raise failure

    def no_wait(delay):
        pytest.fail("非暂时占用不得等待重试")

    monkeypatch.setattr(atomic_directory.os, "replace", reject)
    monkeypatch.setattr(atomic_directory.time, "sleep", no_wait)
    with pytest.raises(PermissionError) as caught:
        atomic_directory.publish_directory(source, target)
    assert caught.value is failure
    assert len(calls) == 1


@pytest.mark.skipif(atomic_directory.os.name != "nt", reason="Windows目录占用语义")
def test_external_commit_waits_for_real_windows_file_handle(tmp_path, monkeypatch):
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                  wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    store = ExternalArtifactStore(tmp_path / "external")
    source = store.prepare()
    payload = source / "payload.txt"
    payload.write_text("sealed", encoding="utf-8")
    # 允许读写，不共享删除权限，复现目录提交被Windows文件句柄阻塞。
    handle = kernel.CreateFileW(str(payload), 0x80000000, 3, None, 3, 0, None)
    assert handle != ctypes.c_void_p(-1).value
    released = []

    def release(delay):
        assert not released
        assert not any(store.objects_root.iterdir())
        assert kernel.CloseHandle(handle)
        released.append(delay)

    monkeypatch.setattr(atomic_directory.time, "sleep", release)
    try:
        commit = store.commit(source, artifact_name="panel", artifact_type="research.cross-sectional-panel.v1")
        assert released == [0.01]
        assert store.verify(commit.semantic_hash) == commit
        assert not source.exists()
    finally:
        if not released:
            kernel.CloseHandle(handle)
