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

/// <summary>前一交易日日线在下一交易日开盘可见；原始区间与可见时点分别保存。</summary>
public sealed class CompletedDailyData : BaseData
{
    public TradeBar SourceBar { get; }
    public CompletedDailyData(TradeBar sourceBar, DateTime visibleAt)
    {
        if (sourceBar.EndTime >= visibleAt) throw new ArgumentException("日线必须在后续交易日开盘交付");
        SourceBar = sourceBar; Time = visibleAt; Symbol = sourceBar.Symbol;
        DataType = MarketDataType.Auxiliary;
    }
    public override BaseData Clone() => new CompletedDailyData(SourceBar, Time);
}

/// <summary>当日无开盘行情时，用刚可见的前日收盘估值；不授予开盘成交资格。</summary>
public sealed class DailyCloseMark : TradeBar
{
    public DateTime SourceTime { get; }
    public DailyCloseMark(DateTime availableAt, DateTime sourceTime, Symbol symbol, decimal close)
        : base(availableAt, symbol, close, close, close, close, 0, TimeSpan.Zero) => SourceTime = sourceTime;
    public override BaseData Clone() => new DailyCloseMark(Time, SourceTime, Symbol, Close);
}

/// <summary>无价格的会话时钟，无行情证券仍按声明窗口参与同步。</summary>
public sealed class SessionBoundary : BaseData
{
    public SessionBoundary(DateTime time, Symbol symbol)
    {
        Time = time; Symbol = symbol; DataType = MarketDataType.Auxiliary;
    }
    public override BaseData Clone() => new SessionBoundary(Time, Symbol);
}

/// <summary>现货运行时钟与完整交易会话分别保存，盘中终点不代表收盘。</summary>
public sealed record SpotExecutionWindow(DateTime StartAt, DateTime EndAt, DateTime SessionOpenAt, DateTime SessionCloseAt)
{
    public static Dictionary<DateTime, SpotExecutionWindow> Read(JsonElement manifest, bool daily)
    {
        var result = new Dictionary<DateTime, SpotExecutionWindow>();
        foreach (var date in manifest.GetProperty("dates").EnumerateArray())
        {
            var day = DateTime.ParseExact(date.GetString()!, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            if (daily)
            {
                result.Add(day, new(day.AddHours(9.5), day.AddHours(15), day.AddHours(9.5), day.AddHours(15)));
                continue;
            }
            var value = manifest.GetProperty("execution_windows").GetProperty(date.GetString()!);
            DateTime At(string name)
            {
                var at = DateTimeOffset.Parse(value.GetProperty(name).GetString()!, System.Globalization.CultureInfo.InvariantCulture);
                if (at.Offset != TimeSpan.FromHours(8) || at.Date != day)
                    throw new InvalidDataException("现货执行窗口必须为同一交易日的+08时区时间");
                return at.DateTime;
            }
            var window = new SpotExecutionWindow(At("start_at"), At("end_at"), At("session_open_at"), At("session_close_at"));
            if (window.StartAt < window.SessionOpenAt || window.EndAt > window.SessionCloseAt || window.EndAt < window.StartAt)
                throw new InvalidDataException("现货执行窗口必须位于交易会话内且起点不晚于终点");
            result.Add(day, window);
        }
        return result;
    }
}

/// <summary>期货交易日和结算时钟，不携带可交易价格。</summary>
public sealed class FutureSessionEvent : BaseData
{
    public string Kind { get; }
    public string TradingDate { get; }
    public FutureSessionEvent(DateTime time, Symbol symbol, string kind, string tradingDate)
    {
        Time = time; Symbol = symbol; Kind = kind; TradingDate = tradingDate;
        DataType = MarketDataType.Auxiliary;
    }
    public override BaseData Clone() => new FutureSessionEvent(Time, Symbol, Kind, TradingDate);
}
