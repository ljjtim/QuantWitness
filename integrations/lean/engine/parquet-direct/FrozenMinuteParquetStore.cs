using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using ParquetSharp;
using QuantConnect;
using QuantConnect.Data;
using QuantWitness.Lean.China;

namespace QuantWitness.LeanInput;

internal sealed partial class FrozenMinuteParquetStore : IDisposable
{
    private sealed record Partition(string Month, string Path, DateTime Start, DateTime End,
        long Size, long MtimeTicks, long Rows, int GroupCount, bool TimestampUtc, int[] Groups);

    private readonly record struct Segment(DateTime Day, DateTime Start, DateTime End);
    private readonly record struct Minute(DateTime Day, DateTime End, double Open, double High,
        double Low, double Close, long Volume);
    private readonly record struct RowRange(int Group, long Offset, int Count);
    private sealed class SymbolRows
    {
        public readonly List<RowRange> Ranges = new();
        public long Count;
        public uint ExecutionDays;
        public Minute[] Bars;
        public long LastAccess;
    }
    private sealed record MonthIndex(Dictionary<string, SymbolRows> Symbols, long Bytes,
        long SelectedRows, Dictionary<DateTime, long> DailyCounts);
    private sealed record RowGroupData(DateTime[] Ends, double[][] Columns, long LastAccess)
    {
        public long Bytes => Ends.LongLength * 48;
    }
    private sealed record MonthData(MonthIndex Index, ParquetFileReader Reader, Dictionary<string, int> Columns)
    {
        public readonly Dictionary<int, RowGroupData> Groups = new();
    }
    private const int ChunkSize = 8192;
    private const int BarBytes = 56;
    private const int SymbolIndexBytes = 256;
    private const int RangeIndexBytes = 48;
    private const int CachedMonths = 2;
    private static readonly TimeSpan ShanghaiOffset = TimeSpan.FromHours(8);
    private readonly object _gate = new();
    private readonly Dictionary<string, Segment[]> _schedules = new(StringComparer.Ordinal);
    private readonly Dictionary<string, decimal> _tradeDivisors;
    private readonly Dictionary<DateTime, SpotExecutionWindow> _executionWindows;
    private readonly HashSet<DateTime> _dates;
    private readonly Partition[] _partitions;
    private readonly string[] _numericColumns;
    private readonly long _memory;
    private readonly string _indexDirectory;
    private readonly byte[] _requestIdentity;
    private readonly Dictionary<string, int> _indexHits = new();
    private readonly HashSet<string> _countedMonths = new();
    private readonly Dictionary<string, MonthData> _cache = new();
    private readonly LinkedList<string> _recent = new();
    private readonly Dictionary<string, int> _loads = new();
    private readonly Dictionary<string, int> _partitionOpens = new();
    private readonly Dictionary<string, int> _groupDecodes = new();
    private readonly Dictionary<string, (int Count, long Rows)> _symbolReads = new();
    private readonly List<object> _reads = new();
    private readonly Dictionary<DateTime, long> _dailyCounts = new();
    private long _access;
    private long _cachedBytes;
    private long _peakCachedBytes;
    private long _sourceRows;
    private long _selectedRows;
    private long _emittedRows;

    public FrozenMinuteParquetStore(JsonElement root, string indexDirectory = null)
    {
        var source = root.GetProperty("minute_parquet");
        _memory = source.GetProperty("memory_bytes").GetInt64();
        _dates = root.GetProperty("dates").EnumerateArray().Select(item => Day(item.GetString())).ToHashSet();
        var future = root.TryGetProperty("asset_class", out var asset) && asset.GetString() == "cn_future";
        var quality = source.GetProperty("quality_columns").EnumerateArray().Select(item => item.GetString());
        _numericColumns = new[] { "open", "high", "low", "close", "volume", "money" }.Concat(quality).ToArray();
        var commonDivisor = Divisor(root.GetProperty("price_scale").GetInt32());
        _tradeDivisors = root.GetProperty("symbols").EnumerateArray().Select(item => item.GetString())
            .ToDictionary(code => code, code => future
                ? Divisor(root.GetProperty("contracts").GetProperty(code).GetProperty("price_scale").GetInt32())
                : commonDivisor, StringComparer.Ordinal);
        _executionWindows = future ? null : SpotExecutionWindow.Read(root, false);
        _partitions = source.GetProperty("partitions").EnumerateArray().Select(item => new Partition(
            item.GetProperty("month").GetString(), item.GetProperty("path").GetString(),
            Local(item.GetProperty("start_at").GetString()), Local(item.GetProperty("end_at").GetString()),
            item.GetProperty("size").GetInt64(), item.GetProperty("mtime_ticks").GetInt64(),
            item.GetProperty("row_count").GetInt64(), item.GetProperty("row_groups").GetInt32(),
            item.GetProperty("timestamp_utc").GetBoolean(),
            item.GetProperty("groups").EnumerateArray().Select(group => group.GetInt32()).ToArray()))
            .OrderBy(item => item.Start).ToArray();
        var scheduleSource = source.GetProperty("schedules");
        var templates = scheduleSource.GetProperty("templates").EnumerateArray()
            .Select(item => (StartOffset: item[0].GetInt32(), Start: item[1].GetString(),
                EndOffset: item[2].GetInt32(), End: item[3].GetString())).ToArray();
        var shared = scheduleSource.GetProperty("segments").EnumerateArray().Select(item =>
        {
            var day = Day(item[0].GetString());
            var template = templates[item[1].GetInt32()];
            DateTime At(int offset, string clock) =>
                Local(day.AddDays(offset).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "T" + clock);
            return new Segment(day, At(template.StartOffset, template.Start), At(template.EndOffset, template.End));
        }).ToArray();
        foreach (var group in scheduleSource.GetProperty("groups").EnumerateArray())
        {
            var ranges = group.GetProperty("ranges").EnumerateArray()
                .Select(range => (Start: range[0].GetInt32(), End: range[1].GetInt32()))
                .ToArray();
            var segments = new Segment[ranges.Sum(range => range.End - range.Start)];
            var offset = 0;
            foreach (var (start, end) in ranges)
            {
                Array.Copy(shared, start, segments, offset, end - start);
                offset += end - start;
            }
            foreach (var code in group.GetProperty("symbols").EnumerateArray())
                _schedules.Add(code.GetString(), segments);
        }
        var overrideDirectory = Environment.GetEnvironmentVariable("QUANTWITNESS_MINUTE_INDEX_DIRECTORY");
        var localData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        _indexDirectory = indexDirectory ?? (!string.IsNullOrWhiteSpace(overrideDirectory)
            ? Path.GetFullPath(overrideDirectory)
            : string.IsNullOrEmpty(localData) ? null : Path.Combine(localData, "QuantWitness", "minute-index-v1"));
        _requestIdentity = RequestIdentity(future);
    }

    private byte[] RequestIdentity(bool future)
    {
        using var digest = SHA256.Create();
        using var stream = new CryptoStream(Stream.Null, digest, CryptoStreamMode.Write);
        using var output = new BinaryWriter(stream);
        output.Write(future);
        output.Write(_numericColumns.Length);
        foreach (var name in _numericColumns) output.Write(name);
        output.Write(_dates.Count);
        foreach (var day in _dates.Order()) output.Write(day.Ticks);
        output.Write(_executionWindows != null);
        if (_executionWindows != null)
        {
            output.Write(_executionWindows.Count);
            foreach (var (day, window) in _executionWindows.OrderBy(pair => pair.Key))
            {
                output.Write(day.Ticks);
                output.Write(window.StartAt.Ticks);
                output.Write(window.EndAt.Ticks);
            }
        }
        output.Write(_tradeDivisors.Count);
        foreach (var (code, divisor) in _tradeDivisors.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            output.Write(code);
            output.Write(divisor);
        }
        var schedules = _schedules.OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToLookup(pair => pair.Value, pair => pair.Key);
        output.Write(schedules.Count);
        foreach (var schedule in schedules)
        {
            output.Write(schedule.Count());
            foreach (var code in schedule) output.Write(code);
            output.Write(schedule.Key.Length);
            foreach (var segment in schedule.Key)
            {
                output.Write(segment.Day.Ticks);
                output.Write(segment.Start.Ticks);
                output.Write(segment.End.Ticks);
            }
        }
        output.Flush();
        stream.FlushFinalBlock();
        return digest.Hash;
    }

    public IEnumerable<BaseData> Read(SubscriptionDataConfig config, string code, DateTime startUtc, DateTime endUtc,
        DateTime consumerUtc)
    {
        var start = startUtc.Add(ShanghaiOffset);
        var end = (consumerUtc < endUtc ? consumerUtc : endUtc).Add(ShanghaiOffset);
        if (end <= start || !_schedules.TryGetValue(code, out var segments) || segments.Length == 0) yield break;
        foreach (var partition in _partitions)
        {
            if (partition.End <= start || partition.Start > end ||
                segments[^1].End < partition.Start || segments[0].Start >= partition.End) continue;
            var cursor = start;
            while (cursor < end)
            {
                Minute[] batch;
                lock (_gate)
                {
                    var rows = Rows(partition, code);
                    if (rows == null) break;
                    var first = FirstAfter(rows.Bars, cursor);
                    var stop = Math.Min(FirstAfter(rows.Bars, end), first + 64);
                    if (stop <= first) break;
                    batch = rows.Bars[first..stop];
                }
                foreach (var row in batch)
                {
                    Interlocked.Increment(ref _emittedRows);
                    yield return new ResearchTradeBar(row.End.AddMinutes(-1), row.End, row.End, config.Symbol,
                        (decimal)row.Open, (decimal)row.High, (decimal)row.Low, (decimal)row.Close, row.Volume);
                }
                cursor = batch[^1].End;
            }
        }
    }

    public void ValidateSpotRules(DateTime day, Func<string, bool> covers)
    {
        if (_executionWindows == null || !_executionWindows.ContainsKey(day)) return;
        var partition = _partitions.FirstOrDefault(partition => partition.Month == Month(day));
        if (partition == null) return;
        lock (_gate)
        {
            var dayBit = 1u << (day.Day - 1);
            foreach (var (code, rows) in GetMonth(partition).Index.Symbols)
                if ((rows.ExecutionDays & dayBit) != 0 && !covers(code))
                    throw new InvalidDataException($"正式行情缺少当日历史规则：{code} {day:yyyy-MM-dd}");
        }
    }

    private SymbolRows Rows(Partition partition, string code)
    {
        var month = GetMonth(partition);
        if (!month.Index.Symbols.TryGetValue(code, out var rows)) return null;
        rows.LastAccess = ++_access;
        if (rows.Bars != null) return rows;
        var bytes = rows.Count * BarBytes;
        CheckMemory(bytes + ChunkSize * 192L, partition.Month);
        rows.Bars = LoadSymbol(partition, month, code, rows);
        _cachedBytes += bytes;
        _peakCachedBytes = Math.Max(_peakCachedBytes, _cachedBytes);
        var read = _symbolReads.GetValueOrDefault(partition.Month);
        _symbolReads[partition.Month] = (read.Count + 1, read.Rows + rows.Count);
        return rows;
    }

    private MonthData GetMonth(Partition partition)
    {
        _recent.Remove(partition.Month);
        if (!_cache.TryGetValue(partition.Month, out var result))
        {
            if (_cache.Count == CachedMonths) EvictOldest();
            var reader = OpenPartition(partition);
            try
            {
                using var metadata = reader.FileMetaData;
                var columns = Columns(partition, metadata);
                var indexPath = IndexPath(partition);
                var index = ReadIndex(indexPath, partition);
                if (index == null)
                {
                    index = Validate(partition, reader, columns);
                    WriteIndex(indexPath, index);
                }
                else _indexHits[partition.Month] = _indexHits.GetValueOrDefault(partition.Month) + 1;
                if (_countedMonths.Add(partition.Month))
                {
                    _selectedRows += index.SelectedRows;
                    foreach (var (date, count) in index.DailyCounts)
                        _dailyCounts[date] = _dailyCounts.GetValueOrDefault(date) + count;
                }
                result = new MonthData(index, reader, columns);
                _cache.Add(partition.Month, result);
                _cachedBytes += index.Bytes;
                _peakCachedBytes = Math.Max(_peakCachedBytes, _cachedBytes);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }
        _recent.AddLast(partition.Month);
        return result;
    }

    private static void VerifyPartitionFile(Partition partition)
    {
        var file = new FileInfo(partition.Path);
        if (!file.Exists || file.Length != partition.Size || file.LastWriteTimeUtc.Ticks != partition.MtimeTicks)
            throw new InvalidDataException($"分钟 Parquet 文件身份变化：{partition.Path}");
    }

    private ParquetFileReader OpenPartition(Partition partition)
    {
        VerifyPartitionFile(partition);
        var reader = new ParquetFileReader(partition.Path);
        _partitionOpens[partition.Month] = _partitionOpens.GetValueOrDefault(partition.Month) + 1;
        return reader;
    }

    private static Dictionary<string, int> Columns(Partition partition, FileMetaData metadata)
    {
        if (metadata.NumRows != partition.Rows || metadata.NumRowGroups != partition.GroupCount)
            throw new InvalidDataException($"分钟 Parquet 文件页脚变化：{partition.Path}");
        var schema = metadata.Schema;
        return Enumerable.Range(0, schema.NumColumns).ToDictionary(index => schema.Column(index).Name);
    }

    private string IndexPath(Partition partition)
    {
        if (_indexDirectory == null) return null;
        // 版本随扫描校验、行段或索引格式的语义变化递增。
        var identity = JsonSerializer.SerializeToUtf8Bytes(new { Version = 1, Request = _requestIdentity, Partition = partition });
        return Path.Combine(_indexDirectory, Convert.ToHexString(SHA256.HashData(identity)) + ".bin");
    }

    private MonthIndex ReadIndex(string path, Partition partition)
    {
        const int digestSize = 32;
        if (path == null) return null;
        try
        {
            using var file = File.OpenRead(path);
            if (file.Length <= digestSize || file.Length > Math.Min(_memory / 2, int.MaxValue)) return null;
            CheckMemory(file.Length, partition.Month);
            var bytes = new byte[(int)file.Length];
            file.ReadExactly(bytes);
            var length = bytes.Length - digestSize;
            if (!SHA256.HashData(bytes.AsSpan(0, length)).AsSpan().SequenceEqual(bytes.AsSpan(length))) return null;
            using var input = new BinaryReader(new MemoryStream(bytes, 0, length, writable: false));
            if (input.ReadString() != Path.GetFileNameWithoutExtension(path)) return null;
            var indexBytes = input.ReadInt64();
            if (indexBytes < 0 || indexBytes > _memory - file.Length) return null;
            CheckMemory(indexBytes + file.Length, partition.Month);
            var selected = input.ReadInt64();
            var daily = new Dictionary<DateTime, long>();
            var dates = input.ReadInt32();
            for (var i = 0; i < dates; i++) daily.Add(new DateTime(input.ReadInt64()), input.ReadInt64());
            var symbols = new Dictionary<string, SymbolRows>(StringComparer.Ordinal);
            var count = input.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var code = input.ReadString();
                var rows = new SymbolRows { Count = input.ReadInt64(), ExecutionDays = input.ReadUInt32() };
                var ranges = input.ReadInt32();
                for (var j = 0; j < ranges; j++)
                    rows.Ranges.Add(new RowRange(input.ReadInt32(), input.ReadInt64(), input.ReadInt32()));
                symbols.Add(code, rows);
            }
            if (input.BaseStream.Position != length) return null;
            return new MonthIndex(symbols, indexBytes, selected, daily);
        }
        catch (IOException) { return null; }
        catch (UnauthorizedAccessException) { return null; }
    }

    private void WriteIndex(string path, MonthIndex data)
    {
        if (path == null) return;
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            Directory.CreateDirectory(_indexDirectory);
            using (var file = new FileStream(temporary, FileMode.CreateNew, FileAccess.ReadWrite))
            using (var output = new BinaryWriter(file))
            {
                output.Write(Path.GetFileNameWithoutExtension(path));
                output.Write(data.Bytes);
                output.Write(data.SelectedRows);
                output.Write(data.DailyCounts.Count);
                foreach (var (day, count) in data.DailyCounts)
                {
                    output.Write(day.Ticks);
                    output.Write(count);
                }
                output.Write(data.Symbols.Count);
                foreach (var (code, rows) in data.Symbols)
                {
                    output.Write(code);
                    output.Write(rows.Count);
                    output.Write(rows.ExecutionDays);
                    output.Write(rows.Ranges.Count);
                    foreach (var range in rows.Ranges)
                    {
                        output.Write(range.Group);
                        output.Write(range.Offset);
                        output.Write(range.Count);
                    }
                }
                output.Flush();
                file.Position = 0;
                output.Write(SHA256.HashData(file));
            }
            File.Move(temporary, path, overwrite: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
        finally
        {
            try { if (File.Exists(temporary)) File.Delete(temporary); }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }

    private MonthIndex Validate(Partition partition, ParquetFileReader reader, Dictionary<string, int> columns)
    {
        var collected = new Dictionary<string, SymbolRows>(StringComparer.Ordinal);
        var dailyCounts = new Dictionary<DateTime, long>();
        var watch = Stopwatch.StartNew();
        long bytes = 0, sourceRows = 0, selectedRows = 0;
        string previousCode = null;
        DateTime previousEnd = default;
        string activeCode = null;
        Segment[] segments = null;
        SymbolRows rows = null;
        int activeSegment = -1;
        decimal divisor = 0;
        double safeLow = 0, safeHigh = 0;
        DateTime executionDay = default, executionStart = default, executionEnd = default;
        var isExecutionDay = false;
        long dailyCount = 0;
        var bufferBytes = ChunkSize * 192L;
        var codeBuffer = new string[ChunkSize];
        var endBuffer = new DateTime?[ChunkSize];
        var buffers = _numericColumns.Select(_ => new double?[ChunkSize]).ToArray();
        CheckMemory(bufferBytes, partition.Month);
        foreach (var index in partition.Groups)
        {
            using var group = reader.RowGroup(index);
            using var codes = group.Column(columns["code"]).LogicalReader<string>();
            using var ends = group.Column(columns["dt"]).LogicalReader<DateTime?>();
            var values = _numericColumns.Select(name => group.Column(columns[name]).LogicalReader<double?>()).ToArray();
            try
            {
                var remaining = group.MetaData.NumRows;
                sourceRows += remaining;
                long offset = 0;
                while (remaining > 0)
                {
                    var count = (int)Math.Min(remaining, ChunkSize);
                    if (codes.ReadBatch(codeBuffer.AsSpan(0, count)) != count)
                        throw new EndOfStreamException("分钟 Parquet 未交付完整证券列");
                    if (!ContainsScheduledCode(codeBuffer.AsSpan(0, count)))
                    {
                        if (ends.Skip(count) != count || values.Any(column => column.Skip(count) != count))
                            throw new EndOfStreamException("分钟 Parquet 未交付完整物理行段");
                        remaining -= count;
                        offset += count;
                        continue;
                    }
                    if (ends.ReadBatch(endBuffer.AsSpan(0, count)) != count)
                        throw new EndOfStreamException("分钟 Parquet 未交付完整结束时间");
                    for (var column = 0; column < values.Length; column++)
                        if (values[column].ReadBatch(buffers[column].AsSpan(0, count)) != count)
                            throw new EndOfStreamException("分钟 Parquet 未交付完整行情列");
                    for (var i = 0; i < count; i++)
                    {
                        var code = codeBuffer[i];
                        if (activeCode != code)
                        {
                            activeCode = code;
                            _schedules.TryGetValue(code, out segments);
                            collected.TryGetValue(code, out rows);
                            activeSegment = -1;
                            _tradeDivisors.TryGetValue(code, out divisor);
                            if (divisor != 0)
                            {
                                safeLow = 1d / (double)divisor;
                                safeHigh = (double)(long.MaxValue / 2) / (double)divisor;
                            }
                        }
                        if (segments == null) continue;
                        var end = endBuffer[i] ?? throw new InvalidDataException("原始分钟结束时间为空");
                        end = LocalEnd(end, partition.TimestampUtc);
                        if (end < partition.Start || end >= partition.End) continue;
                        if (activeSegment < 0 || end <= segments[activeSegment].Start || end > segments[activeSegment].End)
                            activeSegment = SegmentAt(segments, end);
                        if (activeSegment < 0)
                        {
                            var next = ~activeSegment;
                            if (segments.Length == 0 || _executionWindows != null &&
                                (next == 0 || segments[next - 1].End.Date != end.Date) &&
                                (next == segments.Length || segments[next].Start.Date != end.Date))
                                continue;
                            throw new InvalidDataException($"原始分钟不在冻结交易时段：{code} {end:O}");
                        }
                        var segment = segments[activeSegment];
                        if (segment.Day != executionDay)
                        {
                            if (dailyCount != 0)
                                dailyCounts[executionDay] = dailyCounts.GetValueOrDefault(executionDay) + dailyCount;
                            dailyCount = 0;
                            executionDay = segment.Day;
                            executionStart = DateTime.MinValue;
                            executionEnd = DateTime.MaxValue;
                            isExecutionDay = _dates.Contains(executionDay);
                            if (isExecutionDay && _executionWindows != null)
                            {
                                isExecutionDay = _executionWindows.TryGetValue(executionDay, out var window);
                                if (isExecutionDay) (executionStart, executionEnd) = (window.StartAt, window.EndAt);
                            }
                        }
                        var elapsedTicks = end.Ticks - segment.Start.Ticks;
                        if (elapsedTicks < TimeSpan.TicksPerMinute || elapsedTicks % TimeSpan.TicksPerMinute != 0)
                            throw new InvalidDataException($"原始行情不在完整分钟边界：{code} {end:O}");
                        if (previousCode != null && (StringComparer.Ordinal.Compare(previousCode, code) > 0 ||
                            previousCode == code && previousEnd >= end))
                            throw new InvalidDataException("原始行情未按证券、时间稳定排序或存在重复");
                        var open = Finite(buffers[0][i]);
                        var high = Finite(buffers[1][i]);
                        var low = Finite(buffers[2][i]);
                        var close = Finite(buffers[3][i]);
                        var volume = Finite(buffers[4][i]);
                        if (low > Math.Min(open, close) || high < Math.Max(open, close) ||
                            volume < 0 || volume != Math.Truncate(volume) || Finite(buffers[5][i]) < 0)
                            throw new InvalidDataException($"原始分钟 OHLCV 或成交金额无效：{code} {end:O}");
                        for (var column = 6; column < buffers.Length; column++)
                            if (buffers[column][i] is double value && !double.IsFinite(value) &&
                                (_numericColumns[column] != "avg" || volume > 0))
                                throw new InvalidDataException($"原始分钟 {_numericColumns[column]} 无效：{code} {end:O}");
                        _ = checked((long)volume);
                        if (rows == null)
                        {
                            bytes += SymbolIndexBytes;
                            CheckMemory(bytes + bufferBytes, partition.Month);
                            collected.Add(code, rows = new());
                        }
                        if (divisor != 0 && isExecutionDay && end >= executionStart && end <= executionEnd)
                        {
                            // 边界附近沿用原执行报价换算，保留舍入和溢出行为。
                            if ((low < safeLow || low > safeHigh) && Units(low, divisor) <= 0)
                                throw new InvalidDataException("分钟价格在执行报价单位下必须为正");
                            dailyCount++;
                            if (_executionWindows != null) rows.ExecutionDays |= 1u << (segment.Day.Day - 1);
                        }
                        var position = offset + i;
                        var last = rows.Ranges.Count == 0 ? default : rows.Ranges[^1];
                        if (last.Count > 0 && last.Group == index && last.Offset + last.Count == position)
                            rows.Ranges[^1] = last with { Count = last.Count + 1 };
                        else
                        {
                            bytes += RangeIndexBytes;
                            CheckMemory(bytes + bufferBytes, partition.Month);
                            rows.Ranges.Add(new RowRange(index, position, 1));
                        }
                        rows.Count++;
                        previousCode = code;
                        previousEnd = end;
                        selectedRows++;
                    }
                    remaining -= count;
                    offset += count;
                }
            }
            finally { foreach (var value in values) value.Dispose(); }
        }
        if (dailyCount != 0) dailyCounts[executionDay] = dailyCounts.GetValueOrDefault(executionDay) + dailyCount;
        _sourceRows += sourceRows;
        _loads[partition.Month] = _loads.GetValueOrDefault(partition.Month) + 1;
        _reads.Add(new { month = partition.Month, source_rows = sourceRows, selected_rows = selectedRows,
            row_groups = partition.Groups.Length, read_seconds = watch.Elapsed.TotalSeconds });
        return new MonthIndex(collected, bytes, selectedRows, dailyCounts);
    }

    private bool ContainsScheduledCode(ReadOnlySpan<string> codes)
    {
        string previous = null;
        var selected = false;
        foreach (var code in codes)
        {
            if (code == null) throw new InvalidDataException("原始分钟证券为空");
            if (code == previous) continue;
            previous = code;
            selected |= _schedules.ContainsKey(code);
        }
        return selected;
    }

    private Minute[] LoadSymbol(Partition partition, MonthData month, string code, SymbolRows rows)
    {
        var bars = new Minute[checked((int)rows.Count)];
        var columns = month.Columns;
        var ends = new DateTime?[ChunkSize];
        var values = new double?[5][];
        for (var i = 0; i < values.Length; i++) values[i] = new double?[ChunkSize];
        var segments = _schedules[code];
        var output = 0;
        var segmentIndex = -1;
        var rangeIndex = 0;
        while (rangeIndex < rows.Ranges.Count)
        {
            var groupIndex = rows.Ranges[rangeIndex].Group;
            using var group = month.Reader.RowGroup(groupIndex);
            var decoded = LoadGroup(partition, month, groupIndex, group, rows.Count * BarBytes);
            if (decoded != null)
            {
                do
                {
                    var range = rows.Ranges[rangeIndex++];
                    var limit = checked((int)(range.Offset + range.Count));
                    for (var i = checked((int)range.Offset); i < limit; i++)
                        Append(decoded.Ends[i], decoded.Columns[0][i], decoded.Columns[1][i], decoded.Columns[2][i],
                            decoded.Columns[3][i], decoded.Columns[4][i]);
                } while (rangeIndex < rows.Ranges.Count && rows.Ranges[rangeIndex].Group == groupIndex);
                continue;
            }
            using var times = group.Column(columns["dt"]).LogicalReader<DateTime?>();
            var prices = _numericColumns.Take(5)
                .Select(name => group.Column(columns[name]).LogicalReader<double?>()).ToArray();
            try
            {
                long offset = 0;
                do
                {
                    var range = rows.Ranges[rangeIndex++];
                    var skip = range.Offset - offset;
                    // 使用已校验的物理行偏移，仅解码该证券的时间和成交所需列。
                    if (times.Skip(skip) != skip || prices.Any(column => column.Skip(skip) != skip))
                        throw new EndOfStreamException("分钟 Parquet 未到达已校验的证券行段");
                    var remaining = range.Count;
                    while (remaining > 0)
                    {
                        var count = Math.Min(remaining, ChunkSize);
                        if (times.ReadBatch(ends.AsSpan(0, count)) != count)
                            throw new EndOfStreamException("分钟 Parquet 未交付完整结束时间");
                        for (var i = 0; i < prices.Length; i++)
                            if (prices[i].ReadBatch(values[i].AsSpan(0, count)) != count)
                                throw new EndOfStreamException("分钟 Parquet 未交付完整行情列");
                        for (var i = 0; i < count; i++)
                            Append(ends[i].Value, values[0][i].Value, values[1][i].Value, values[2][i].Value,
                                values[3][i].Value, values[4][i].Value);
                        remaining -= count;
                    }
                    offset = range.Offset + range.Count;
                } while (rangeIndex < rows.Ranges.Count && rows.Ranges[rangeIndex].Group == groupIndex);
            }
            finally { foreach (var column in prices) column.Dispose(); }
        }
        return bars;

        void Append(DateTime rawEnd, double open, double high, double low, double close, double volume)
        {
            if (rawEnd == default || !double.IsFinite(open) || !double.IsFinite(high) || !double.IsFinite(low)
                || !double.IsFinite(close) || !double.IsFinite(volume))
                throw new InvalidDataException("已选分钟行的时间或报价无效");
            var end = LocalEnd(rawEnd, partition.TimestampUtc);
            if (segmentIndex < 0 || end <= segments[segmentIndex].Start || end > segments[segmentIndex].End)
                segmentIndex = SegmentAt(segments, end);
            bars[output++] = new Minute(segments[segmentIndex].Day, end,
                open, high, low, close, checked((long)volume));
        }
    }

    private RowGroupData LoadGroup(Partition partition, MonthData month, int index, RowGroupReader group, long barsBytes)
    {
        if (month.Groups.TryGetValue(index, out var cached))
        {
            month.Groups[index] = cached with { LastAccess = ++_access };
            return cached;
        }
        var count = group.MetaData.NumRows;
        var bytes = checked(count * 48L);
        var scratch = ChunkSize * 192L;
        // 保留索引与现有证券行情；首先淘汰近期不用的行组解码。
        if (count > int.MaxValue || bytes > _memory / 2 || count < ChunkSize) return null;
        while (_cachedBytes + bytes + barsBytes + scratch > _memory && ReleaseOldestGroup()) { }
        if (_cachedBytes + bytes + barsBytes + scratch > _memory) return null;
        var ends = new DateTime[checked((int)count)];
        var columns = new double[5][];
        var timeBuffer = new DateTime?[ChunkSize];
        var valueBuffer = new double?[ChunkSize];
        using (var times = group.Column(month.Columns["dt"]).LogicalReader<DateTime?>())
        {
            for (var offset = 0; offset < ends.Length; offset += ChunkSize)
            {
                var length = Math.Min(ChunkSize, ends.Length - offset);
                if (times.ReadBatch(timeBuffer.AsSpan(0, length)) != length)
                    throw new EndOfStreamException("分钟 Parquet 未交付完整行组时间");
                for (var i = 0; i < length; i++) ends[offset + i] = timeBuffer[i].GetValueOrDefault();
            }
        }
        for (var i = 0; i < columns.Length; i++)
        {
            var values = new double[ends.Length];
            using var column = group.Column(month.Columns[_numericColumns[i]]).LogicalReader<double?>();
            for (var offset = 0; offset < ends.Length; offset += ChunkSize)
            {
                var length = Math.Min(ChunkSize, ends.Length - offset);
                if (column.ReadBatch(valueBuffer.AsSpan(0, length)) != length)
                    throw new EndOfStreamException("分钟 Parquet 未交付完整行组行情");
                for (var j = 0; j < length; j++) values[offset + j] = valueBuffer[j] ?? double.NaN;
            }
            columns[i] = values;
        }
        var decoded = new RowGroupData(ends, columns, ++_access);
        month.Groups.Add(index, decoded);
        _cachedBytes += bytes;
        _peakCachedBytes = Math.Max(_peakCachedBytes, _cachedBytes);
        _groupDecodes[partition.Month] = _groupDecodes.GetValueOrDefault(partition.Month) + 1;
        return decoded;
    }

    private bool ReleaseOldestGroup()
    {
        MonthData selected = null;
        var key = 0;
        var oldest = long.MaxValue;
        foreach (var month in _cache.Values)
            foreach (var (index, group) in month.Groups)
                if (group.LastAccess < oldest)
                {
                    oldest = group.LastAccess;
                    selected = month;
                    key = index;
                }
        if (selected == null) return false;
        _cachedBytes -= selected.Groups[key].Bytes;
        selected.Groups.Remove(key);
        return true;
    }

    private void CheckMemory(long extraBytes, string month)
    {
        while (_cachedBytes + extraBytes > _memory)
        {
            if (ReleaseOldestGroup()) continue;
            if (_recent.First != null && _recent.First.Value != month)
            {
                EvictOldest();
                continue;
            }
            if (!_cache.TryGetValue(month, out var data)) break;
            var oldest = data.Index.Symbols.Values.Where(rows => rows.Bars != null).MinBy(rows => rows.LastAccess);
            if (oldest == null) break;
            Release(oldest);
        }
        if (_cachedBytes + extraBytes > _memory)
            throw new InvalidDataException("已选分钟行情超过月缓存内存额度");
    }

    private void Release(SymbolRows rows)
    {
        if (rows.Bars == null) return;
        _cachedBytes -= rows.Count * BarBytes;
        rows.Bars = null;
    }

    private void EvictOldest()
    {
        var oldest = _recent.First.Value;
        var month = _cache[oldest];
        foreach (var rows in month.Index.Symbols.Values) Release(rows);
        foreach (var group in month.Groups.Values) _cachedBytes -= group.Bytes;
        month.Groups.Clear();
        month.Reader.Dispose();
        _cachedBytes -= month.Index.Bytes;
        _cache.Remove(oldest);
        _recent.RemoveFirst();
    }

    public void Dispose()
    {
        lock (_gate)
            while (_recent.Count > 0) EvictOldest();
    }

    private static int FirstAfter(IReadOnlyList<Minute> rows, DateTime at)
    {
        var lower = 0;
        var upper = rows.Count;
        while (lower < upper)
        {
            var middle = lower + (upper - lower) / 2;
            if (rows[middle].End <= at) lower = middle + 1;
            else upper = middle;
        }
        return lower;
    }

    private static int SegmentAt(Segment[] segments, DateTime end)
    {
        var lower = 0;
        var upper = segments.Length;
        while (lower < upper)
        {
            var middle = lower + (upper - lower) / 2;
            if (segments[middle].End < end) lower = middle + 1;
            else upper = middle;
        }
        return lower < segments.Length && segments[lower].Start < end ? lower : ~lower;
    }

    private static DateTime Local(string value) => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture)
        .ToOffset(ShanghaiOffset).DateTime;
    private static DateTime Day(string value) => DateTime.ParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture);
    private static string Month(DateTime at) => at.ToString("yyyy-MM", CultureInfo.InvariantCulture);
    private static DateTime LocalEnd(DateTime at, bool timestampUtc) =>
        DateTime.SpecifyKind(timestampUtc ? at.Add(ShanghaiOffset) : at, DateTimeKind.Unspecified);
    private static decimal Divisor(int scale) => (decimal)Math.Pow(10, scale);
    private static long Units(double price, decimal divisor) =>
        checked((long)Math.Round((decimal)price * divisor, 0, MidpointRounding.AwayFromZero));
    private static double Finite(double? value) => value.HasValue && double.IsFinite(value.Value)
        ? value.Value : throw new InvalidDataException("原始分钟数值为空或非有限");
    public void Save(string path, Dictionary<string, int> dateLoads,
        IReadOnlyDictionary<string, object> rulesByDate = null, double? ruleSourceLoadSeconds = null,
        IReadOnlyDictionary<string, object> financialDays = null)
    {
        lock (_gate)
            File.WriteAllText(path, JsonSerializer.Serialize(new
            {
                source_kind = "jqdata_monthly_parquet", frequency = "1m", source_rows_read = _sourceRows,
                selected_rows = _selectedRows, emitted_rows = _emittedRows, month_load_counts = _loads,
                date_load_counts = dateLoads, month_index_hits = _indexHits, partition_open_counts = _partitionOpens,
                row_group_decodes = _groupDecodes,
                rules_by_date = rulesByDate,
                rule_source_load_seconds = ruleSourceLoadSeconds,
                financial_days = financialDays,
                daily_selected_rows = _dailyCounts.ToDictionary(
                    pair => pair.Key.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), pair => pair.Value),
                reads = _reads,
                symbol_parquet_reads = _symbolReads.ToDictionary(pair => pair.Key,
                    pair => new { reads = pair.Value.Count, rows = pair.Value.Rows }),
                cached_months = _recent, peak_cached_bytes = _peakCachedBytes,
                intermediate_market_files = 0, intermediate_rule_files = 0
            }));
    }
}
