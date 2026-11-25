using System.Text.Json;
using ParquetSharp;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantWitness.LeanInput;

internal static class FrozenMinuteContracts
{
    public static void Verify(string root, Symbol symbol, string code)
    {
        var january = WritePartition(root, "2024-01", "2024-01-31T20:00:00+08:00",
            "2024-02-01T00:00:00+08:00", code, new DateTime(2024, 1, 31, 21, 1, 0));
        var february = WritePartition(root, "2024-02", "2024-02-01T00:00:00+08:00",
            "2024-02-01T02:30:00.000001+08:00", code, new DateTime(2024, 2, 1, 0, 0, 0));
        var manifest = Path.Combine(root, "minute.json");
        var report = Path.Combine(root, "minute-report.json");
        File.WriteAllText(manifest, JsonSerializer.Serialize(new
        {
            asset_class = "cn_future",
            symbols = new[] { code },
            dates = new[] { "2024-02-01" },
            price_scale = 2,
            contracts = new Dictionary<string, object> { [code] = new { price_scale = 2,
                product = "IF", market = "cffex", expiry_date = "2024-03-15" } },
            minute_parquet = new
            {
                memory_bytes = 8 * 1024 * 1024,
                quality_columns = new[] { "avg", "open_interest" },
                partitions = new[] { january, february },
                schedules = new { segments = new[] { new object[] { "2024-02-01", 0 } },
                    templates = new[] { new object[] { -1, "21:00:00+08:00", 0, "02:30:00+08:00" } }, groups = new[] { new { symbols = new[] { code }, ranges = new[] { new[] { 0, 1 } } } } }
            },
            native_data = new { bindings = new[] { new { binding_id = "quotes", kind = "bars", resolution = "1m",
                symbols = new[] { code }, history_start_at = "2024-01-31T20:00:00+08:00",
                end_at = "2024-02-01T02:30:00+08:00", direct_minute = true } } }
        }));
        try
        {
            var config = new SubscriptionDataConfig(typeof(TradeBar), symbol, Resolution.Minute,
                TimeZones.Shanghai, TimeZones.Shanghai, false, false, false,
                dataNormalizationMode: DataNormalizationMode.Raw);
            var store = MonthlyParquetStore.Open(manifest, Path.Combine(root, "initial-index"));
            var native = new NativeDataStore(manifest);
            var start = new DateTime(2024, 1, 31, 12, 0, 0, DateTimeKind.Utc);
            var close = new DateTime(2024, 1, 31, 16, 0, 0, DateTimeKind.Utc);
            var first = native.Read(config, start, close, close.AddTicks(-1), "history").Cast<ResearchTradeBar>().Single();
            if (first.Close != 10.025m || first.Volume != 0 || first.ObservationEndTime.Hour != 21)
                throw new Exception("分钟历史丢失原始价格、零量或冻结时钟");
            var rows = store.Read(config, code, start, close, close).Cast<ResearchTradeBar>().ToArray();
            if (rows.Length != 2 || rows[0].Close != 10.025m || rows[1].ObservationEndTime != new DateTime(2024, 2, 1))
                throw new Exception("期货夜盘未按实际交易日合并跨月文件");
            var tail = native.Read(config, first.EndTime.AddHours(-8), close, close, "feed")
                .Cast<ResearchTradeBar>().Single();
            if (tail.ObservationTime != new DateTime(2024, 1, 31, 23, 59, 0) || tail.ObservationEndTime.Hour != 0)
                throw new Exception("分钟订阅应排除起点并保留已完成的午夜分钟");
            store.Save(report, new());
            using var profile = JsonDocument.Parse(File.ReadAllText(report));
            var loads = profile.RootElement.GetProperty("month_load_counts");
            if (loads.GetProperty("2024-01").GetInt32() != 1 || loads.GetProperty("2024-02").GetInt32() != 1)
                throw new Exception("订阅、History与执行未共享原始分区读取");
            var opens = profile.RootElement.GetProperty("partition_open_counts");
            if (opens.GetProperty("2024-01").GetInt32() != 1 || opens.GetProperty("2024-02").GetInt32() != 1)
                throw new Exception("同一月的页脚校验和证券解码须共用读句柄");
            var symbolReads = profile.RootElement.GetProperty("symbol_parquet_reads");
            if (symbolReads.GetProperty("2024-01").GetProperty("reads").GetInt32() != 1
                || symbolReads.GetProperty("2024-02").GetProperty("reads").GetInt32() != 1)
                throw new Exception("订阅、History与执行重复读取同一证券行段");
        }
        finally { MonthlyParquetStore.Close(manifest); }
        VerifyGroupDecode(root);
        VerifyIntradayRules(root);
        VerifyMonthEviction(root, symbol, code);
    }

    private static void VerifyGroupDecode(string root)
    {
        const string day = "2024-06-03";
        var when = DateTime.Parse(day).AddHours(9).AddMinutes(31);
        var codes = Enumerable.Range(0, 9000).Select(index => $"{index:000000}.XSHE").ToArray();
        var partition = WriteGroups(root, "2024-06", day + "T09:30:00+08:00", day + "T15:00:00+08:00",
            codes.Select(code => (code, when)).ToArray());
        var trading = new[] { codes[^1], codes[^2] };
        using var store = new FrozenMinuteParquetStore(JsonSerializer.SerializeToElement(new {
            symbols = trading, dates = new[] { day }, price_scale = 2,
            execution_windows = new {
                @default = new[] { "T09:30:00+08:00", "T15:00:00+08:00", "T09:30:00+08:00", "T15:00:00+08:00" },
                overrides = new Dictionary<string, object>() },
            minute_parquet = new { memory_bytes = 8 * 1024 * 1024, quality_columns = new[] { "avg" },
                partitions = new[] { partition }, schedules = new {
                    segments = new[] { new object[] { day, 0 } },
                    templates = new[] { new object[] { 0, "09:30:00+08:00", 0, "15:00:00+08:00" } },
                    groups = new[] { new { symbols = trading, ranges = new[] { new[] { 0, 1 } } } } } }
        }));
        var startUtc = new DateTime(2024, 6, 3, 1, 30, 0, DateTimeKind.Utc);
        var endUtc = new DateTime(2024, 6, 3, 7, 0, 0, DateTimeKind.Utc);
        foreach (var code in trading)
        {
            var cfg = new SubscriptionDataConfig(typeof(TradeBar),
                new Symbol(SecurityIdentifier.GenerateEquity(code, Market.USA, mapSymbol: false), code), Resolution.Minute,
                TimeZones.Shanghai, TimeZones.Shanghai, false, false, false,
                dataNormalizationMode: DataNormalizationMode.Raw);
            if (store.Read(cfg, code, startUtc, endUtc, endUtc).Cast<ResearchTradeBar>().Single().Close != 10.025m)
                throw new Exception("行组批量解码不能更改报价精度");
        }
        var profilePath = Path.Combine(root, "group-decode-profile.json");
        store.Save(profilePath, new());
        using var report = JsonDocument.Parse(File.ReadAllText(profilePath));
        if (report.RootElement.GetProperty("row_group_decodes").GetProperty("2024-06").GetInt32() != 1)
            throw new Exception("同一行组内逆序请求两只证券也只解码一次");
    }

    private static void VerifyMonthEviction(string root, Symbol symbol, string code)
    {
        var dates = new[] { "2024-03-01", "2024-04-01", "2024-05-01" };
        var starts = dates.Select(day => day + "T09:30:00+08:00").ToArray();
        var ends = dates.Select(day => day + "T15:00:00+08:00").ToArray();
        var times = dates.Select(day => DateTime.Parse(day).AddHours(9).AddMinutes(31)).ToArray();
        var partitions = dates.Select((day, index) => WritePartition(root, day[..7], starts[index],
            ends[index], code, times[index])).ToArray();
        var cache = Path.Combine(root, "eviction-index");
        FrozenMinuteParquetStore Create(long budget, int scale = 2) => new(JsonSerializer.SerializeToElement(new {
            asset_class = "cn_future", symbols = new[] { code }, dates, price_scale = scale,
            contracts = new Dictionary<string, object> { [code] = new { price_scale = scale } },
            minute_parquet = new { memory_bytes = budget, quality_columns = new[] { "avg" }, partitions,
                schedules = new {
                    segments = new[] { new object[] { dates[0], 0 }, new object[] { dates[0], 1 } }
                        .Concat(dates.Skip(1).Select(day => new object[] { day, 0 })).ToArray(),
                    templates = new[] { new object[] { 0, starts[0][11..], 0, ends[0][11..] },
                        new object[] { 0, "15:30:00+08:00", 0, "16:00:00+08:00" } },
                    groups = new[] { new { symbols = new[] { code },
                        ranges = new[] { new[] { 0, 1 }, new[] { 2, dates.Length + 1 } } } } } }
        }), cache);
        const long budget = 8 * 1024 * 1024;
        using var store = Create(budget);
        var config = new SubscriptionDataConfig(typeof(TradeBar), symbol, Resolution.Minute,
            TimeZones.Shanghai, TimeZones.Shanghai, false, false, false,
            dataNormalizationMode: DataNormalizationMode.Raw);
        ResearchTradeBar ReadFrom(FrozenMinuteParquetStore target, int index, bool checkEarly = false)
        {
            var end = DateTime.SpecifyKind(times[index].AddHours(-8), DateTimeKind.Utc);
            if (checkEarly && target.Read(config, code, end.AddMinutes(-1), end, end.AddTicks(-1)).Any())
                throw new Exception("缓存或回载不能提前交付未完成分钟");
            return target.Read(config, code, end.AddMinutes(-1), end, end).Cast<ResearchTradeBar>().Single();
        }
        var first = ReadFrom(store, 0, checkEarly: true);
        ReadFrom(store, 1, checkEarly: true);
        ReadFrom(store, 2, checkEarly: true);
        var report = Path.Combine(root, "minute-eviction-report.json");
        store.Save(report, new());
        using (var profile = JsonDocument.Parse(File.ReadAllText(report)))
        {
            var cached = profile.RootElement.GetProperty("cached_months").EnumerateArray().Select(x => x.GetString());
            if (!cached.SequenceEqual(new[] { "2024-04", "2024-05" }) ||
                profile.RootElement.GetProperty("peak_cached_bytes").GetInt64() > budget)
                throw new Exception("月缓存须限制为当前相邻活跃月份和冻结字节额度");
        }
        var reloaded = ReadFrom(store, 0, checkEarly: true);
        store.Save(report, new());
        using var reloadedProfile = JsonDocument.Parse(File.ReadAllText(report));
        var result = reloadedProfile.RootElement;
        if (first.Close != reloaded.Close || first.ObservationEndTime != reloaded.ObservationEndTime
            || result.GetProperty("month_load_counts").GetProperty("2024-03").GetInt32() != 1
            || result.GetProperty("month_index_hits").GetProperty("2024-03").GetInt32() != 1
            || result.GetProperty("selected_rows").GetInt64() != 3)
            throw new Exception("历史回载须交付相同完成分钟且不重复统计入选行");
        using var warm = Create(budget);
        if (ReadFrom(warm, 0).Close != 10.025m)
            throw new Exception("新读取器复用索引时必须交付相同报价");
        warm.Save(report, new());
        using (var warmProfile = JsonDocument.Parse(File.ReadAllText(report)))
        {
            var stats = warmProfile.RootElement;
            if (stats.GetProperty("source_rows_read").GetInt64() != 0
                || stats.GetProperty("month_index_hits").GetProperty("2024-03").GetInt32() != 1
                || stats.GetProperty("daily_selected_rows").GetProperty(dates[0]).GetInt64() != 1)
                throw new Exception("暖读取必须免扫描并恢复逐日统计");
        }
        using var otherScale = Create(budget, 3);
        ReadFrom(otherScale, 0);
        otherScale.Save(report, new());
        using (var changedProfile = JsonDocument.Parse(File.ReadAllText(report)))
            if (changedProfile.RootElement.GetProperty("source_rows_read").GetInt64() != 1)
                throw new Exception("执行报价精度变化必须重新校验");
        foreach (var index in Directory.GetFiles(cache, "*.bin"))
        {
            var damaged = File.ReadAllBytes(index);
            damaged[0] ^= 0x01;
            File.WriteAllBytes(index, damaged);
        }
        using var repaired = Create(budget);
        ReadFrom(repaired, 0);
        repaired.Save(report, new());
        using (var damagedProfile = JsonDocument.Parse(File.ReadAllText(report)))
            if (damagedProfile.RootElement.GetProperty("source_rows_read").GetInt64() != 1)
                throw new Exception("损坏索引必须重新扫描原始分区");
        // 额度只容纳一份分区索引及其行情，第二个月应释放第一个月。
        using var small = Create(8192 * 192L + 400);
        ReadFrom(small, 0);
        ReadFrom(small, 1);
        small.Save(report, new());
        using var smallProfile = JsonDocument.Parse(File.ReadAllText(report));
        if (smallProfile.RootElement.GetProperty("cached_months")[0].GetString() != "2024-04"
            || smallProfile.RootElement.GetProperty("cached_months").GetArrayLength() != 1)
            throw new Exception("加载第二个月时必须遵守冻结字节额度");

        store.Dispose();
        var monthFile = Path.Combine(root, "2024-05.parquet");
        File.SetLastWriteTimeUtc(monthFile, File.GetLastWriteTimeUtc(monthFile).AddSeconds(2));
        using var changed = Create(budget);
        try { ReadFrom(changed, 2); }
        catch (InvalidDataException error) when (error.Message.Contains("文件身份变化")) { return; }
        throw new Exception("即使侧车已存在，冻结文件变化仍必须拒绝");
    }

    private static void VerifyIntradayRules(string root)
    {
        const string day = "2024-02-02", code = "000001.XSHE", neighbor = "000002.XSHE";
        var date = new DateTime(2024, 2, 2);
        var first = date.AddHours(9).AddMinutes(31);
        var active = date.AddHours(10).AddMinutes(1);
        var next = active.AddMinutes(1);
        var partition = WriteGroups(root, "2024-02", day + "T09:30:00+08:00", day + "T15:00:00+08:00",
            new[] { (code, first), (code, active), (neighbor, first) }, new[] { (neighbor, next) });
        var window = new { start_at = day + "T10:00:00+08:00", end_at = day + "T14:00:00+08:00",
            session_open_at = day + "T09:30:00+08:00", session_close_at = day + "T15:00:00+08:00" };
        FrozenMinuteParquetStore Create(long budget) => new(JsonSerializer.SerializeToElement(new {
            symbols = new[] { code, neighbor }, dates = new[] { day }, price_scale = 2,
            execution_windows = new {
                @default = new[] { "T10:00:00+08:00", "T14:00:00+08:00", "T09:30:00+08:00", "T15:00:00+08:00" },
                overrides = new Dictionary<string, object>() },
            minute_parquet = new { memory_bytes = budget, quality_columns = new[] { "avg" },
                partitions = new[] { partition }, schedules = new {
                    segments = new[] { new object[] { day, 0 } },
                    templates = new[] { new object[] { 0, window.session_open_at[11..],
                        0, window.session_close_at[11..] } },
                    groups = new[] { new { symbols = new[] { code, neighbor }, ranges = new[] { new[] { 0, 1 } } } } } }
        }), Path.Combine(root, "intraday-index", budget.ToString()));
        SubscriptionDataConfig Config(string instrument) => new(typeof(TradeBar),
            new Symbol(SecurityIdentifier.GenerateEquity(instrument, Market.USA, mapSymbol: false), instrument), Resolution.Minute,
            TimeZones.Shanghai, TimeZones.Shanghai, false, false, false,
            dataNormalizationMode: DataNormalizationMode.Raw);
        var winStart = new DateTime(2024, 2, 2, 2, 0, 0, DateTimeKind.Utc);
        var winEnd = new DateTime(2024, 2, 2, 6, 0, 0, DateTimeKind.Utc);
        using var store = Create(8 * 1024L * 1024);
        store.ValidateSpotRules(date, symbolCode => symbolCode == code || symbolCode == neighbor);
        var rows = store.Read(Config(neighbor), neighbor, winStart, winEnd, winEnd).Cast<ResearchTradeBar>().ToArray();
        if (rows.Length != 1 || rows[0].ObservationEndTime != next || rows[0].Close != 10.025m)
            throw new Exception("指定证券需沿物理偏移合并跨行组记录，并排除盘中窗口外的分钟");
        var report = Path.Combine(root, "minute-rules-report.json");
        store.Save(report, new());
        using var profile = JsonDocument.Parse(File.ReadAllText(report));
        var statistics = profile.RootElement;
        var reads = statistics.GetProperty("symbol_parquet_reads").GetProperty("2024-02");
        if (statistics.GetProperty("selected_rows").GetInt64() != 4
            || statistics.GetProperty("daily_selected_rows").GetProperty(day).GetInt64() != 2
            || reads.GetProperty("reads").GetInt32() != 1 || reads.GetProperty("rows").GetInt64() != 2)
            throw new Exception("规则检查和完整校验应覆盖全部入选行，只读取实际请求证券的明细");
        store.Read(Config(code), code, winStart, winEnd, winEnd).ToArray();
        var sharedReport = Path.Combine(root, "shared-minutes.json");
        store.Save(sharedReport, new());
        using (var sharedProfile = JsonDocument.Parse(File.ReadAllText(sharedReport)))
            if (sharedProfile.RootElement.GetProperty("partition_open_counts").GetProperty("2024-02").GetInt32() != 1)
                throw new Exception("同月两只证券必须复用分区读句柄");
        // 同月索引保留，行情额度只能容纳其中一份证券数组。
        const long smallBudget = 8192 * 192L + 790;
        using var small = Create(smallBudget);
        small.Read(Config(code), code, winStart, winEnd, winEnd).ToArray();
        small.Read(Config(neighbor), neighbor, winStart, winEnd, winEnd).ToArray();
        var reloaded = small.Read(Config(code), code, winStart, winEnd, winEnd).Cast<ResearchTradeBar>().Single();
        small.Save(report, new());
        using var smallProfile = JsonDocument.Parse(File.ReadAllText(report));
        var smallStats = smallProfile.RootElement;
        if (reloaded.ObservationEndTime != active || smallStats.GetProperty("peak_cached_bytes").GetInt64() > smallBudget
            || smallStats.GetProperty("month_load_counts").GetProperty("2024-02").GetInt32() != 1
            || smallStats.GetProperty("symbol_parquet_reads").GetProperty("2024-02").GetProperty("reads").GetInt32() != 3)
            throw new Exception("同月超预算需淘汰证券行情，保留规则及行段索引以便回载");
        try { store.ValidateSpotRules(date, symbolCode => symbolCode == neighbor); }
        catch (InvalidDataException error) when (error.Message.Contains(code)) { return; }
        throw new Exception("未订阅证券在盘中窗口内存在已完成分钟时仍需检查历史规则");
    }

    private static object WritePartition(string root, string month, string start, string end, string code, params DateTime[] times)
        => WriteGroups(root, month, start, end, times.Select(at => (code, at)).ToArray());

    private static object WriteGroups(string root, string month, string start, string end,
        params (string Code, DateTime Time)[][] groups)
    {
        var path = Path.Combine(root, month + ".parquet");
        var numeric = new[] { "open", "high", "low", "close", "volume", "money", "avg", "open_interest" };
        var columns = new Column[] { new Column<string>("code"), new Column<DateTime?>("dt") }
            .Concat(numeric.Select(name => (Column)new Column<double?>(name))).ToArray();
        using (var file = new ParquetFileWriter(path, columns))
        {
            foreach (var rows in groups)
            {
                using var group = file.AppendRowGroup();
                using (var values = group.NextColumn().LogicalWriter<string>()) values.WriteBatch(rows.Select(row => row.Code).ToArray());
                using (var values = group.NextColumn().LogicalWriter<DateTime?>()) values.WriteBatch(rows.Select(row => (DateTime?)row.Time).ToArray());
                foreach (var item in new double?[] { 10, 11, 9, 10.025, 0, 0, null, null })
                {
                    using var values = group.NextColumn().LogicalWriter<double?>();
                    values.WriteBatch(Enumerable.Repeat(item, rows.Length).ToArray());
                }
            }
            file.Close();
        }
        var info = new FileInfo(path);
        return new { path, month, start_at = start, end_at = end, size = info.Length,
            mtime_ticks = info.LastWriteTimeUtc.Ticks, row_count = groups.Sum(group => group.Length),
            row_groups = groups.Length, timestamp_utc = false, groups = Enumerable.Range(0, groups.Length).ToArray() };
    }
}
