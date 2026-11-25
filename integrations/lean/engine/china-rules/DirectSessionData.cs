using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using System.Text.Json;

namespace QuantWitness.Lean.China;

/// <summary>日线订阅中的内部开盘事件，仅含当时可见开盘价，不是已完成日线。</summary>
public sealed class DailyOpenBar : TradeBar
{
    public DailyOpenBar(DateTime time, Symbol symbol, decimal open)
        : base(time, symbol, open, open, open, open, 0, TimeSpan.Zero) { }
    public override BaseData Clone() => new DailyOpenBar(Time, Symbol, Open);
}

/// <summary>现货运行时钟与完整交易会话分别保存，盘中终点不代表收盘。</summary>
public sealed record SpotExecutionWindow(DateTime StartAt, DateTime EndAt, DateTime SessionOpenAt, DateTime SessionCloseAt)
{
    public static Dictionary<DateTime, SpotExecutionWindow> Read(JsonElement manifest, bool daily)
    {
        var result = new Dictionary<DateTime, SpotExecutionWindow>();
        var windows = daily ? default : manifest.GetProperty("execution_windows");
        foreach (var date in manifest.GetProperty("dates").EnumerateArray())
        {
            var day = DateTime.ParseExact(date.GetString()!, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            if (daily)
            {
                result.Add(day, new(day.AddHours(9.5), day.AddHours(15), day.AddHours(9.5), day.AddHours(15)));
                continue;
            }
            var current = date.GetString()!;
            var value = windows.GetProperty("overrides").TryGetProperty(current, out var entry)
                ? entry : windows.GetProperty("default");
            DateTime At(int index)
            {
                var at = DateTimeOffset.Parse(current + value[index].GetString()!, System.Globalization.CultureInfo.InvariantCulture);
                if (at.Offset != TimeSpan.FromHours(8) || at.Date != day)
                    throw new InvalidDataException("现货执行窗口必须为同一交易日的+08时区时间");
                return at.DateTime;
            }
            var window = new SpotExecutionWindow(At(0), At(1), At(2), At(3));
            if (window.StartAt < window.SessionOpenAt || window.EndAt > window.SessionCloseAt || window.EndAt < window.StartAt)
                throw new InvalidDataException("现货执行窗口必须位于交易会话内且起点不晚于终点");
            result.Add(day, window);
        }
        return result;
    }
}
