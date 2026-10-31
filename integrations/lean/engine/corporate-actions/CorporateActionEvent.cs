using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace QuantWitness.Lean.CorporateActions;

public sealed record CorporateActionEvent
{
    public required string ActionId { get; init; }
    public required int Revision { get; init; }
    public required string Kind { get; init; }
    public required string Symbol { get; init; }
    public required DateTimeOffset AvailableAt { get; init; }
    public required DateTimeOffset RecordAt { get; init; }
    public required DateTimeOffset ExAt { get; init; }
    public DateTimeOffset? CashPayAt { get; init; }
    public DateTimeOffset? SharesArrivalAt { get; init; }
    public DateTimeOffset? SharesSellableAt { get; init; }
    public decimal CashPerShare { get; init; }
    public long RatioNumerator { get; init; } = 1;
    public long RatioDenominator { get; init; } = 1;
    public decimal? ExReferencePrice { get; init; }
    public required string SourceRef { get; init; }

    [JsonIgnore]
    public bool IsCash => Kind == "cash_dividend";
    [JsonIgnore]
    public decimal BonusRatio => IsCash ? 0m : (decimal)RatioNumerator / RatioDenominator - 1m;

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ActionId) || string.IsNullOrWhiteSpace(Symbol)
            || string.IsNullOrWhiteSpace(SourceRef) || Revision < 1)
            throw new ArgumentException("公司行动必须有身份、证券、正版本和来源");
        if (Kind is not ("cash_dividend" or "stock_dividend" or "split"))
            throw new NotSupportedException($"不支持公司行动类型：{Kind}");
        if (RecordAt > ExAt || RatioDenominator < 1 || RatioNumerator < 0
            || CashPerShare < 0 || ExReferencePrice is <= 0)
            throw new ArgumentException("公司行动日期、金额或比例无效");
        if (IsCash)
        {
            if (CashPayAt is null || CashPayAt < ExAt || CashPerShare <= 0
                || SharesArrivalAt is not null || SharesSellableAt is not null)
                throw new ArgumentException("现金分红必须声明真实到账时点和正每股金额，不能带股份日期");
        }
        else if (RatioNumerator <= RatioDenominator || CashPerShare != 0 || CashPayAt is not null
            || SharesArrivalAt is null || SharesSellableAt is null
            || SharesArrivalAt < ExAt || SharesSellableAt < SharesArrivalAt)
            throw new ArgumentException("送转必须有大于一的总股份倍率及真实到账、可卖时点");
    }
}

public static class CorporateActionJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        WriteIndented = true
    };

    public static IReadOnlyList<CorporateActionEvent> Load(string filePath) => Parse(File.ReadAllText(filePath));

    public static IReadOnlyList<CorporateActionEvent> Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Array)
            throw new ArgumentException("事件 JSON 顶层必须为数组");
        foreach (var row in doc.RootElement.EnumerateArray())
            foreach (var field in row.EnumerateObject())
                if (field.Name.EndsWith("_at", StringComparison.Ordinal) && field.Value.ValueKind != JsonValueKind.Null)
                {
                    var text = field.Value.GetString() ?? "";
                    if (!Regex.IsMatch(text, @"T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,7})?(Z|[+-][0-9]{2}:[0-9]{2})$"))
                        throw new ArgumentException($"{field.Name} 必须含时间与显式时区偏移");
                }
        var result = JsonSerializer.Deserialize<CorporateActionEvent[]>(json, Options)!;
        foreach (var action in result) action.Validate();
        return result;
    }
}

public sealed record InitialEntitlement(string ActionId, int Revision, decimal Quantity,
    DateTimeOffset RecordAt, string SourceRef);

public sealed record ActionTransition(string ActionId, int Revision, string Phase,
    DateTimeOffset At, decimal RegisteredQuantity, decimal CashDelta, decimal ShareDelta);

public sealed record EntitlementValuation(DateTimeOffset AsOf, decimal CashReceivable,
    decimal PendingBonusValue, decimal PendingBonusQuantity, decimal LockedBonusValue,
    decimal LeanPortfolioValue)
{
    public decimal AdditionalEquity => CashReceivable + PendingBonusValue;
    public decimal TotalEquity => LeanPortfolioValue + AdditionalEquity;
}
