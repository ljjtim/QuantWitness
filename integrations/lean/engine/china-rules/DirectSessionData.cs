using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;

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

/// <summary>无行情证券仍推进到当日收盘，防止其枚举器无限领先其他证券。</summary>
public sealed class SessionBoundary : BaseData
{
    public SessionBoundary(DateTime time, Symbol symbol)
    {
        Time = time; Symbol = symbol; DataType = MarketDataType.Auxiliary;
    }
    public override BaseData Clone() => new SessionBoundary(Time, Symbol);
}
