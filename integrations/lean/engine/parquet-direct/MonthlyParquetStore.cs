using System.Diagnostics;
using System.Text.Json;
using ParquetSharp;

namespace QuantWitness.LeanInput;

// 原始文件按证券和月份排列，逐月共享读取；不会按交易日反复扫描整月。
internal sealed class MonthlyParquetStore : IBarStore
{
    private readonly Dictionary<string, string> _selected = new();
    private readonly HashSet<DateTime> _dates = new();
    private readonly Dictionary<string, (string Path, int[] Groups)> _months = new();
    private readonly Dictionary<DateTime, Dictionary<string, BarRow[]>> _cache = new();
    private readonly List<object> _reads = new();
    private readonly Dictionary<string, int> _loads = new();
    private readonly Dictionary<string, long> _dailyCounts = new();
    private string _loadedMonth;
    private long _sourceRows;
    private long _selectedRows;
    public long EmittedRows { get; set; }
    public decimal PriceDivisor { get; }

    public MonthlyParquetStore(string path)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var root = doc.RootElement;
        if (root.GetProperty("timestamp_semantics").GetString() != "completed_bar_end_asia_shanghai")
            throw new InvalidDataException("必须明确原始分钟结束时间及上海时区");
        var scale = root.GetProperty("price_scale").GetInt32();
        if (scale is not (2 or 3)) throw new NotSupportedException("现货输入仅支持二位或三位价格");
        PriceDivisor = (decimal)Math.Pow(10, scale);
        foreach (var item in root.GetProperty("symbols").EnumerateArray())
        {
            var code = item.GetString();
            _selected.Add(code, code.Split('.')[0]);
        }
        foreach (var item in root.GetProperty("dates").EnumerateArray())
            _dates.Add(DateTime.ParseExact(item.GetString(), "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture));
        foreach (var item in root.GetProperty("months").EnumerateObject())
            _months.Add(item.Name, (item.Value.GetProperty("path").GetString(),
                item.Value.GetProperty("row_groups").EnumerateArray().Select(x => x.GetInt32()).ToArray()));
    }

    public BarRow[] GetRows(DateTime date, string ticker)
    {
        if (!_dates.Contains(date)) throw new InvalidDataException("请求日期未冻结");
        var month = date.ToString("yyyy-MM");
        if (month != _loadedMonth)
        {
            if (_loads.ContainsKey(month)) throw new InvalidDataException("行情同步回退到已消费月份");
            _cache.Clear();
            Load(month);
            _loadedMonth = month;
        }
        return _cache.TryGetValue(date, out var batch) && batch.TryGetValue(ticker, out var rows)
            ? rows : Array.Empty<BarRow>();
    }

    private void Load(string month)
    {
        var watch = Stopwatch.StartNew();
        var plan = _months[month];
        var collected = new Dictionary<DateTime, Dictionary<string, List<BarRow>>>();
        using var reader = new ParquetFileReader(plan.Path);
        using var metadata = reader.FileMetaData;
        var schema = metadata.Schema;
        var columns = new Dictionary<string, int>();
        for (var i = 0; i < schema.NumColumns; i++) columns.Add(schema.Column(i).Name, i);
        long sourceRows = 0;
        foreach (var index in plan.Groups)
        {
            using var group = reader.RowGroup(index);
            var count = checked((int)group.MetaData.NumRows);
            var codes = Read<string>(group, columns["code"], count);
            var ends = Read<DateTime?>(group, columns["dt"], count);
            var opens = Read<double?>(group, columns["open"], count);
            var highs = Read<double?>(group, columns["high"], count);
            var lows = Read<double?>(group, columns["low"], count);
            var closes = Read<double?>(group, columns["close"], count);
            var volumes = Read<double?>(group, columns["volume"], count);
            sourceRows += count;
            for (var i = 0; i < count; i++)
            {
                if (!_selected.TryGetValue(codes[i], out var ticker)) continue;
                if (!ends[i].HasValue) throw new InvalidDataException("分钟时间为空");
                var end = DateTime.SpecifyKind(ends[i].Value, DateTimeKind.Unspecified);
                if (!_dates.Contains(end.Date)) continue;
                var start = end.AddMinutes(-1);
                var minute = start.TimeOfDay.TotalMinutes;
                if (!((minute >= 570 && minute < 690) || (minute >= 780 && minute < 900)))
                    throw new InvalidDataException("原始分钟不在已支持的连续交易时段");
                if (!collected.TryGetValue(end.Date, out var daily)) collected.Add(end.Date, daily = new());
                if (!daily.TryGetValue(ticker, out var list)) daily.Add(ticker, list = new List<BarRow>(240));
                if (list.Count > 0 && list[^1].Start >= start) throw new InvalidDataException("原始分钟重复或时间倒退");
                var open = Units(opens[i], PriceDivisor);
                var high = Units(highs[i], PriceDivisor);
                var low = Units(lows[i], PriceDivisor);
                var close = Units(closes[i], PriceDivisor);
                var volume = Units(volumes[i], 1m);
                if (low < 0 || low > Math.Min(open, close) || high < Math.Max(open, close) || volume < 0)
                    throw new InvalidDataException("原始行情价格或成交量无效");
                list.Add(new BarRow(start, open, high, low, close, volume));
            }
        }
        long selectedRows = 0;
        foreach (var pair in collected)
        {
            var daily = pair.Value.ToDictionary(x => x.Key, x => x.Value.ToArray());
            var rows = daily.Values.Sum(x => (long)x.Length);
            _cache.Add(pair.Key, daily);
            _dailyCounts.Add(pair.Key.ToString("yyyy-MM-dd"), rows);
            selectedRows += rows;
        }
        _sourceRows += sourceRows;
        _selectedRows += selectedRows;
        _loads.Add(month, 1);
        _reads.Add(new { month, source_rows = sourceRows, selected_rows = selectedRows,
            row_groups = plan.Groups.Length, read_seconds = watch.Elapsed.TotalSeconds });
    }

    private static long Units(double? value, decimal divisor)
    {
        if (!value.HasValue || !double.IsFinite(value.Value)) throw new InvalidDataException("原始行情包含缺失值");
        var units = (decimal)value.Value * divisor;
        if (units != decimal.Truncate(units)) throw new InvalidDataException("原始价格或成交量精度超出已声明单位");
        return checked((long)units);
    }

    private static T[] Read<T>(RowGroupReader group, int index, int count)
    {
        using var column = group.Column(index);
        using var logical = column.LogicalReader<T>();
        return logical.ReadAll(count);
    }

    public void Save(string path) => File.WriteAllText(path, JsonSerializer.Serialize(new
    {
        source_kind = "jqdata_monthly_parquet", source_rows_read = _sourceRows,
        selected_rows = _selectedRows, emitted_rows = EmittedRows, month_load_counts = _loads,
        daily_selected_rows = _dailyCounts, reads = _reads, intermediate_market_files = 0
    }, new JsonSerializerOptions { WriteIndented = true }));

    public void Clear() => _cache.Clear();
}
