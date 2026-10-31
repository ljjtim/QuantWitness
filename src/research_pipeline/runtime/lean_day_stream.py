"""按交易日交接内存行情，计算与下一交易日读取重叠。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import socket
import struct
from threading import Event, Thread


BAR_DTYPE_FIELDS = (
    ("instrument", "<i4"), ("start", "<i8"), ("end", "<i8"),
    ("open", "<i8"), ("high", "<i8"), ("low", "<i8"),
    ("close", "<i8"), ("volume", "<i8"),
)


def pack_day_bars(table, symbols):
    """只传输引擎实际使用的列；时间为UTC微秒，价格为整数单位。"""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    rows = np.empty(table.num_rows, dtype=np.dtype(list(BAR_DTYPE_FIELDS)))
    codes = pc.index_in(table["instrument_id"], value_set=pa.array(symbols))
    if codes.null_count:
        raise ValueError("日行情含未声明证券")
    rows["instrument"] = codes.to_numpy(zero_copy_only=False)
    for target, source in (("start", "bar_start"), ("end", "bar_end")):
        rows[target] = pc.cast(table[source], pa.int64()).to_numpy(zero_copy_only=False)
    for name in ("open", "high", "low", "close"):
        rows[name] = table[name + "_units"].to_numpy(zero_copy_only=False)
    rows["volume"] = table["volume"].to_numpy(zero_copy_only=False)
    # 月源按证券与时间排序；经日分组后保留该顺序。
    return rows.tobytes()


def _receive_exact(connection, size):
    result = bytearray(size)
    view = memoryview(result)
    offset = 0
    while offset < size:
        count = connection.recv_into(view[offset:])
        if not count:
            raise EOFError("LEAN日数据通道已关闭")
        offset += count
    return result


class LeanDayStream:
    """一个读取线程预备下一日；一个连接向引擎交付当天数据。"""

    def __init__(self, dates, loader, *, frequency, memory_bytes):
        self.dates = tuple(dates)
        if not self.dates or tuple(sorted(set(self.dates))) != self.dates:
            raise ValueError("日数据流须声明有序且唯一的交易日")
        if frequency not in {"1m", "1d"}:
            raise ValueError("行情频率仅支持1m或1d")
        self.loader = loader
        self.frequency = frequency
        self.memory_bytes = memory_bytes
        self.counts = {}
        self.signal_rows = 0
        self.failure = None
        self._stop = Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lean-day-read")
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self._listener.settimeout(0.2)
        self.endpoint = {"host": "127.0.0.1", "port": self._listener.getsockname()[1]}
        self._connection = None
        self._future = self._executor.submit(self._load, self.dates[0])
        self._thread = Thread(target=self._serve, name="lean-day-delivery", daemon=True)
        self._thread.start()

    def _load(self, day):
        rules, content, signals = self.loader(day)
        row_size = 60
        if len(content) % row_size:
            raise ValueError("日行情二进制长度无效")
        if len(content) > self.memory_bytes:
            raise ValueError("单日行情超过已声明内存额度")
        header = {"date": day, "frequency": self.frequency,
                  "rules": rules, "row_count": len(content) // row_size, "signals": signals}
        if self.frequency == "1d":
            header["availability"] = "next_session_open"
        return header, content

    @staticmethod
    def _send(connection, header, content=b""):
        raw = json.dumps(header, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        connection.sendall(struct.pack("<I", len(raw)))
        connection.sendall(raw)
        if content:
            connection.sendall(content)

    def _serve(self):
        try:
            while not self._stop.is_set():
                try:
                    self._connection, _ = self._listener.accept()
                    break
                except socket.timeout:
                    continue
            if self._connection is None:
                return
            connection = self._connection
            for index, day in enumerate(self.dates):
                if self._stop.is_set():
                    return
                length = struct.unpack("<I", _receive_exact(connection, 4))[0]
                if length > 4096:
                    raise ValueError("日数据请求超过日期消息长度")
                request = json.loads(_receive_exact(connection, length))
                if request != {"date": day}:
                    raise ValueError(f"日数据请求顺序无效，期望{day}")
                header, content = self._future.result()
                # 下一日读取与本日传输及执行重叠，不预读整个研究窗口。
                if index + 1 < len(self.dates):
                    self._future = self._executor.submit(self._load, self.dates[index + 1])
                else:
                    self._future = None
                self._send(connection, header, content)
                self.counts[day] = header["row_count"]
                self.signal_rows += len(header["signals"]["selection"])
                del header, content
        except BaseException as exc:
            if not self._stop.is_set():
                self.failure = exc
                if self._connection is not None:
                    try:
                        self._send(self._connection, {"error": str(exc)})
                    except OSError:
                        pass
        finally:
            if self._connection is not None:
                self._connection.close()

    def require_complete(self):
        if self.failure is not None:
            raise RuntimeError("LEAN日行情交接失败") from self.failure
        if set(self.counts) != set(self.dates):
            raise RuntimeError("LEAN未消费全部声明交易日")

    def close(self):
        self._stop.set()
        self._listener.close()
        if self._connection is not None:
            try:
                self._connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._connection.close()
        self._thread.join()
        if self._future is not None:
            self._future.cancel()
        self._executor.shutdown(wait=True, cancel_futures=True)
        close = getattr(self.loader, "close", None)
        if close is not None:
            close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
