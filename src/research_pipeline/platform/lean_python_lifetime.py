"""在嵌入式 Python 关闭前释放 DuckDB 连接和模块。"""
_open_readers = set()
_shutdown_registered = False


def _release_duckdb_module():
    # Python.NET 关闭时不执行 Py_Finalize。此时仍持有 GIL，释放绑定提供的模块清理对象。
    import sys
    for name in ("duckdb", "_duckdb"):
        module = sys.modules.get(name)
        if module is not None:
            vars(module).pop("_clean_default_connection", None)


def _shutdown_readers():
    global _shutdown_registered
    try:
        for reader in tuple(_open_readers):
            reader.close()
    finally:
        _shutdown_registered = False
        _release_duckdb_module()


def register_pit_shutdown():
    global _shutdown_registered
    if not _shutdown_registered:
        from Python.Runtime import PythonEngine
        PythonEngine.AddShutdownHandler(PythonEngine.ShutdownHandler(_shutdown_readers))
        _shutdown_registered = True
