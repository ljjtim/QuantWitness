using QuantConnect.Interfaces;
using QuantConnect.Orders;

namespace QuantWitness.Lean.China;

/// <summary>人民币每股保护价；不改变原生 Market／MOO 的订单类型和撮合价格。</summary>
public sealed class ChinaOrderProperties : OrderProperties
{
    public decimal? ProtectionPrice { get; set; }
    /// <summary>买单累计成交额及费用的人民币硬上限；随委托固定。</summary>
    public decimal? BudgetCny { get; set; }
    /// <summary>已可见前日日线的开盘决策，可在同一开盘的后续执行阶段成交。</summary>
    public bool DailyOpenDecision { get; set; }

    public override IOrderProperties Clone() => (ChinaOrderProperties)MemberwiseClone();
}
