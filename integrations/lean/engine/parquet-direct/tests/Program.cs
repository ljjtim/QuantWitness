using System.Text.Json;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using QuantWitness.LeanInput;

// 真实 LEAN SID 在 AddFutureContract 后不保留供应商 ticker，Feed 和 History 必须取回同一冻结合约。
Config.Set("data-folder", args[0]);
var customSymbol = new Symbol(SecurityIdentifier.GenerateBase(typeof(ResearchData), "600000.XSHG", Market.USA, false), "600000.XSHG");
var customHours = QuantConnect.Securities.MarketHoursDatabase.FromDataFolder().GetEntry(customSymbol, new[] { typeof(ResearchData) });
#pragma warning disable CS0618
Symbol implicitUnderlying = "599999.XSHG";
#pragma warning restore CS0618
var implicitCustom = Symbol.CreateBase(typeof(ResearchData), implicitUnderlying, implicitUnderlying.ID.Market);
NativeClockSubscription.ConfigureTimeZones();
var implicitHours = QuantConnect.Securities.MarketHoursDatabase.FromDataFolder().GetEntry(implicitCustom, new[] { typeof(ResearchData) });
if (customHours.DataTimeZone != TimeZones.Shanghai || implicitHours.DataTimeZone != TimeZones.Shanghai
    || implicitHours.ExchangeHours.TimeZone != TimeZones.Shanghai) throw new Exception("自定义订阅时区必须为上海");
var config = new SubscriptionDataConfig(typeof(ResearchData), implicitCustom, Resolution.Daily,
    implicitHours.DataTimeZone, implicitHours.ExchangeHours.TimeZone, false, true, true);
var point = new DateTime(2024, 1, 9, 9, 30, 0);
var offset = new TimeZoneOffsetProvider(config.ExchangeTimeZone, point.AddDays(-1), point.AddDays(1));
var emitted = QuantConnect.Lean.Engine.DataFeeds.SubscriptionData.Create(false, config, implicitHours.ExchangeHours,
    offset, new ResearchData { Symbol = implicitCustom, Time = point }, DataNormalizationMode.Raw);
if (emitted.EmitTimeUtc != new DateTime(2024, 1, 9, 1, 30, 0)) throw new Exception("自定义订阅发射时点偏移");
Console.WriteLine("自定义数据默认重载：上海09:30对应UTC01:30通过");
var root = Path.Combine(Path.GetTempPath(), "native-symbol-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
var code = "IF2403.CCFX";
var expiry = new DateTime(2024, 3, 15);
if (!Market.Encode("cffex").HasValue) Market.Add("cffex", 810);
var symbol = new Symbol(SecurityIdentifier.GenerateFuture(expiry, "IF", "cffex"), "IF15H24");
if (symbol.Value == code) throw new Exception("测试必须使用原生期货别名");
try
{
    FrozenMinuteContracts.Verify(root, symbol, code);
    FrozenSpotRuleContracts.Verify(root);
    foreach (var resolution in new[] { Resolution.Daily, Resolution.Minute })
    {
        var input = new
        {
            contracts = new Dictionary<string, object> { [code] = new { product = "IF", market = "cffex", expiry_date = "2024-03-15" } },
            native_data = new
            {
                bindings = new[] { new { binding_id = "bars", kind = "bars", resolution = resolution == Resolution.Daily ? "1d" : "1m",
                    symbols = new[] { code }, history_start_at = "2024-01-01T00:00:00Z", end_at = "2024-01-05T00:00:00Z" } }
            }
        };
        var path = Path.Combine(root, $"input-{resolution}.json");
        File.WriteAllText(path, JsonSerializer.Serialize(input));
        var store = new NativeDataStore(path);
        if (!store.Manifest.Equals(new NativeDataStore(Path.Combine(root, ".", Path.GetFileName(path))).Manifest))
            throw new Exception("同一冻结输入应共用一份只读文档");
        SubscriptionDataConfig Config(Symbol value) => new(typeof(TradeBar), value, resolution, TimeZones.Shanghai,
            TimeZones.Shanghai, false, false, false, dataNormalizationMode: DataNormalizationMode.Raw);
        if (store.Binding(Config(symbol)).GetProperty("binding_id").GetString() != "bars") throw new Exception("绑定失败");
        try
        {
            store.Binding(Config(new Symbol(SecurityIdentifier.GenerateFuture(expiry.AddMonths(1), "IF", "cffex"), "IF15J24")));
            throw new Exception("错误到期月不能匹配");
        }
        catch (InvalidDataException) { }
    }
    var clockManifest = Path.Combine(root, "clock.json");
    File.WriteAllText(clockManifest, JsonSerializer.Serialize(new
    {
        dates = new[] { "2024-01-08", "2024-01-09" },
        execution_windows = new
        {
            @default = new[] { "T09:30:00+08:00", "T15:00:00+08:00", "T09:30:00+08:00", "T15:00:00+08:00" },
            overrides = new Dictionary<string, string[]> {
                ["2024-01-09"] = new[] { "T10:00:00+08:00", "T10:02:00+08:00", "T09:30:00+08:00", "T15:00:00+08:00" },
            },
        },
        native_data = new
        {
            bindings = Array.Empty<object>(),
            spot_execution_clock = true,
            spot_minute_clock = true,
            calendar_events = new[] { "2024-01-09T10:00:30+08:00", "2024-01-08T12:00:00+08:00" },
        },
    }));
    var clock = new NativeDataStore(clockManifest);
    var clockConfig = new SubscriptionDataConfig(typeof(ResearchClock), implicitCustom, Resolution.Minute,
        TimeZones.Shanghai, TimeZones.Shanghai, false, true, true);
    var start = new DateTime(2024, 1, 9, 1, 30, 0, DateTimeKind.Utc);
    var end = start.AddHours(1);
    var startLocal = new DateTime(2024, 1, 9, 10, 0, 0);
    var expectedClock = new[] { startLocal, startLocal.AddSeconds(30), startLocal.AddMinutes(1), startLocal.AddMinutes(2) };
    if (!clock.Read(clockConfig, start, end, end, "feed").Select(item => item.Time).SequenceEqual(expectedClock))
        throw new Exception("紧凑时钟应保留盘中边界和独立事件");
    var morning = new DateTime(2024, 1, 8, 1, 29, 0, DateTimeKind.Utc);
    var close = morning.Date.AddHours(7);
    var fullDay = clock.Read(clockConfig, morning, close, close, "feed").Select(item => item.Time).ToArray();
    if (fullDay.Length != 243 || fullDay.Count(point => point.TimeOfDay > TimeSpan.FromHours(11.5)
        && point.TimeOfDay < TimeSpan.FromHours(13)) != 1)
        throw new Exception("午休仅保留显式时钟事件，交易时段两端均可见");

    var row = new ResearchData { Fields = new() { ["quantity"] = JsonSerializer.SerializeToElement(9007199254740993L) } };
    var copy = (ResearchData)row.Clone();
    copy.Fields["quantity"] = JsonSerializer.SerializeToElement(42);
    if (row.Fields["quantity"].GetInt64() != 9007199254740993L || copy.Fields["quantity"].GetInt64() != 42)
        throw new Exception("克隆研究字段必须保留整数精度和独立修改");
}
finally
{
    Directory.Delete(root, true);
}
