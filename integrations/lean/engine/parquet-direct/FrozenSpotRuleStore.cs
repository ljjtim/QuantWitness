using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using ParquetSharp;
using QuantWitness.Lean.China;

namespace QuantWitness.LeanInput;

internal sealed class FrozenSpotRuleStore
{
    private static readonly TimeSpan ShanghaiOffset = TimeSpan.FromHours(8);
    private const int BatchSize = 2048;
    private const int RetainedTemplateLimit = 4096;
    private sealed record InstrumentRules(string Code, RuleRange[] Rules);

    private readonly record struct Rule(int From, int To, long VisibleTicks, long Revision, int Parameters, int Unsupported,
        int DailyIndex)
    {
        public bool Covers(int day) => From <= day && day <= To;
    }

    private sealed class RuleRange
    {
        private readonly Rule[] _rows;
        private readonly int[] _maxEnd;

        public RuleRange(List<Rule> rows)
        {
            rows.Sort((left, right) => left.From.CompareTo(right.From));
            _rows = rows.ToArray();
            _maxEnd = new int[_rows.Length];
            var maxEnd = 0;
            for (var i = 0; i < _rows.Length; i++)
                _maxEnd[i] = maxEnd = Math.Max(maxEnd, _rows[i].To);
        }

        public int Locate(int day)
        {
            var low = 0;
            var high = _rows.Length;
            while (low < high)
            {
                var middle = low + (high - low) / 2;
                if (_rows[middle].From <= day) low = middle + 1;
                else high = middle;
            }
            return low - 1;
        }

        public bool Reaches(int index, int day) => index >= 0 && _maxEnd[index] >= day;
        public Rule this[int index] => _rows[index];
    }

    private readonly Dictionary<string, InstrumentRules> _instruments;
    private readonly bool _validatedDailyPrices;
    private readonly record struct TemplateKey(int Lifecycle, int Quantity, int Prices, int Settlement, int Suspension, int Fee);
    private readonly record struct DailyRows(int Lifecycle, int Prices, int Suspension);

    private sealed class Template(Dictionary<string, JsonElement> fields, DailyColumn[] overlaps)
        : ChinaRule.FrozenTemplate(fields)
    {
        public readonly DailyColumn[] Overlaps = overlaps;
    }

    private abstract class DailyColumn(string name, int section)
    {
        private readonly List<int> _rows = new();
        public string Name { get; } = name;

        public ChinaRule.ParameterValue At(DailyRows rows)
        {
            var index = section switch { 0 => rows.Lifecycle, 2 => rows.Prices, _ => rows.Suspension };
            return index < 0 || index >= _rows.Count || _rows[index] == 0 ? default : Value(_rows[index] - 1);
        }

        protected void AddRow(int id) => _rows.Add(id);
        protected abstract ChinaRule.ParameterValue Value(int index);
        public abstract ColumnBatch Open(RowGroupReader group, Dictionary<string, int> columns);
        public abstract void Seal();
    }

    private abstract class ColumnBatch : IDisposable
    {
        public abstract void ReadBatch(int[] slots, int count, int keptInBatch);
        public abstract void Dispose();
    }

    private sealed class DailyColumn<TColumn, TValue>(string name, string physical, int section,
        Func<TColumn, TValue> pack, Func<TValue, ChinaRule.ParameterValue> convert) : DailyColumn(name, section)
    {
        private Dictionary<TValue, int> _ids = new();
        private TValue[] _values;

        protected override ChinaRule.ParameterValue Value(int index) => convert(_values[index]);
        private TValue Pack(TColumn value) => pack(value);
        public override ColumnBatch Open(RowGroupReader group, Dictionary<string, int> columns) =>
            new Batch(this, group.Column(columns[physical]).LogicalReader<TColumn>());

        public override void Seal()
        {
            _values = new TValue[_ids.Count];
            foreach (var (value, id) in _ids) _values[id - 1] = value;
            _ids = null;
        }

        private sealed class Batch(DailyColumn<TColumn, TValue> column, LogicalColumnReader<TColumn> reader) : ColumnBatch
        {
            private readonly TColumn[] _buffer = new TColumn[BatchSize];

            public override void ReadBatch(int[] slots, int count, int keptInBatch)
            {
                Read(reader, _buffer, count);
                if (keptInBatch == 0) return;
                for (var i = 0; i < count; i++)
                {
                    if (slots[i] < 0) continue;
                    var source = _buffer[i];
                    if (source == null)
                    {
                        column.AddRow(0);
                        continue;
                    }
                    var value = column.Pack(source);
                    if (!column._ids.TryGetValue(value, out var id))
                        column._ids.Add(value, id = column._ids.Count + 1);
                    column.AddRow(id);
                }
            }

            public override void Dispose() => reader.Dispose();
        }
    }

    private sealed class DailyParameters(Dictionary<string, DailyColumn> columns, DailyRows rows)
        : ChinaRule.FrozenOverrides
    {
        public override ChinaRule.ParameterValue Find(string name) =>
            columns.TryGetValue(name, out var column) ? column.At(rows) : default;

        public override IEnumerable<KeyValuePair<string, ChinaRule.ParameterValue>> Fields()
        {
            foreach (var column in columns.Values)
            {
                var value = column.At(rows);
                if (value.ValueKind != JsonValueKind.Undefined)
                    yield return new(column.Name, value);
            }
        }
    }

    private sealed class RuleBatch : IDisposable
    {
        public int Kind { get; }
        public string[] RuleId { get; } = new string[BatchSize];
        public long?[] Revision { get; } = new long?[BatchSize];
        public string[] Status { get; } = new string[BatchSize];
        public string[] Available { get; } = new string[BatchSize];
        public long?[] Parameter { get; } = new long?[BatchSize];
        public string[] Unsupported { get; } = new string[BatchSize];
        private readonly LogicalColumnReader<string> _id, _status, _available, _unsupported;
        private readonly LogicalColumnReader<long?> _revision, _parameter;

        public RuleBatch(RowGroupReader group, Dictionary<string, int> columns, string prefix, int kind, bool verified)
        {
            Kind = kind;
            int Column(string name) => columns[prefix + name];
            _id = verified ? null : group.Column(Column("rule_id")).LogicalReader<string>();
            _revision = group.Column(Column("revision")).LogicalReader<long?>();
            _status = verified ? null : group.Column(Column("status")).LogicalReader<string>();
            _available = group.Column(Column("available_at")).LogicalReader<string>();
            _parameter = group.Column(Column("parameter_index")).LogicalReader<long?>();
            _unsupported = verified ? null : group.Column(Column("unsupported_reason")).LogicalReader<string>();
        }

        public void ReadBatch(int count)
        {
            if (_id != null) Read(_id, RuleId, count);
            Read(_revision, Revision, count);
            if (_status != null) Read(_status, Status, count);
            Read(_available, Available, count);
            Read(_parameter, Parameter, count);
            if (_unsupported != null) Read(_unsupported, Unsupported, count);
        }

        public void Dispose()
        {
            _id?.Dispose();
            _revision.Dispose();
            _status?.Dispose();
            _available.Dispose();
            _parameter.Dispose();
            _unsupported?.Dispose();
        }
    }

    private readonly byte[][] _templates;
    private readonly Dictionary<int, JsonElement> _parsed = new();
    private readonly Dictionary<TemplateKey, Template> _combined = new();
    private readonly Dictionary<string, DailyColumn> _dailyColumns = new(StringComparer.Ordinal);
    private readonly string[] _unsupported;

    public double LoadSeconds { get; }

    public FrozenSpotRuleStore(JsonElement source, IEnumerable<string> codes, string asset)
    {
        var watch = Stopwatch.StartNew();
        var rulesPath = FrozenPath(source, "rules_path", "rules.parquet");
        var parametersPath = FrozenPath(source, "parameters_path", "parameters.parquet");
        var dailyPath = source.TryGetProperty("daily_reference_path", out _)
            ? FrozenPath(source, "daily_reference_path", "daily-reference.parquet") : null;
        _validatedDailyPrices = dailyPath != null && source.TryGetProperty("validated_daily_prices", out var validated)
            && validated.GetBoolean();
        if (Path.GetDirectoryName(rulesPath) != Path.GetDirectoryName(parametersPath)
            || dailyPath != null && Path.GetDirectoryName(rulesPath) != Path.GetDirectoryName(dailyPath)
            || Path.GetFileName(Path.GetDirectoryName(rulesPath)) != source.GetProperty("scope").GetProperty("name").GetString())
            throw new InvalidDataException("现货冻结规则文件必须来自同一已准入分钟范围");
        var prefix = asset switch
        {
            "cn_stock" => "cn_stock",
            "cn_etf" => "cn_fund",
            _ => throw new InvalidDataException("现货分钟规则仅支持股票和 ETF")
        };
        var kinds = new[] { "instrument_lifecycle", "lot_size", "price_limit", "settlement", "suspension", "trading_fee" }
            .Select((kind, index) => (Id: $"rule.{prefix}.{kind}.v1", index))
            .ToDictionary(entry => entry.Id, entry => entry.index, StringComparer.Ordinal);
        var indices = codes.Select((code, index) => (code, index))
            .ToDictionary(entry => entry.code, entry => entry.index, StringComparer.Ordinal);
        _templates = LoadParameters(parametersPath);
        var reasons = new Dictionary<string, int>(StringComparer.Ordinal);
        var rows = new List<Rule>[indices.Count, kinds.Count];
        LoadRules(rulesPath, asset, indices, kinds, rows, reasons, new[] { ("", -1) });
        if (dailyPath != null)
            LoadRules(dailyPath, asset, indices, kinds, rows, reasons, new[] {
                ("lifecycle_", kinds[$"rule.{prefix}.instrument_lifecycle.v1"]),
                ("prices_", kinds[$"rule.{prefix}.price_limit.v1"]),
                ("suspension_", kinds[$"rule.{prefix}.suspension.v1"]),
            });
        _unsupported = reasons.Keys.ToArray();
        _instruments = indices.Select(entry =>
        {
            var rules = new RuleRange[kinds.Count];
            for (var kind = 0; kind < rules.Length; kind++)
                if (rows[entry.Value, kind] is { } versions)
                {
                    rules[kind] = new RuleRange(versions);
                    rows[entry.Value, kind] = null;
                }
            return new InstrumentRules(entry.Key, rules);
        }).ToDictionary(instrument => instrument.Code, StringComparer.Ordinal);
        LoadSeconds = watch.Elapsed.TotalSeconds;
    }

    private void LoadRules(string path, string asset, Dictionary<string, int> indices,
        Dictionary<string, int> kinds, List<Rule>[,] rows, Dictionary<string, int> reasons,
        (string Prefix, int Kind)[] streams)
    {
        using var reader = new ParquetFileReader(path);
        using var metadata = reader.FileMetaData;
        var columns = Columns(metadata);
        var daily = streams.Length > 1;
        var verified = daily && _validatedDailyPrices;
        if (daily) AddDailyColumns();

        var keptRowCount = 0;
        var keptSlots = daily ? new int[BatchSize] : null;
        var code = new string[BatchSize];
        var rowAsset = new string[BatchSize];
        var from = new string[BatchSize];
        var to = new string[BatchSize];
        var dates = new Dictionary<string, int>(StringComparer.Ordinal);
        var times = new Dictionary<string, long>(StringComparer.Ordinal);
        for (var groupIndex = 0; groupIndex < metadata.NumRowGroups; groupIndex++)
        {
            using var group = reader.RowGroup(groupIndex);
            using var codeColumn = group.Column(columns["instrument_id"]).LogicalReader<string>();
            using var assetColumn = verified ? null : group.Column(columns["asset_class"]).LogicalReader<string>();
            using var fromColumn = group.Column(columns[daily ? "trading_date" : "effective_from"]).LogicalReader<string>();
            using var toColumn = daily ? null : group.Column(columns["effective_to"]).LogicalReader<string>();
            var rules = new List<RuleBatch>(streams.Length);
            var values = new List<ColumnBatch>();
            try
            {
                foreach (var (prefix, kind) in streams) rules.Add(new RuleBatch(group, columns, prefix, kind, verified));
                if (daily)
                    foreach (var column in _dailyColumns.Values) values.Add(column.Open(group, columns));
                for (var remaining = group.MetaData.NumRows; remaining > 0;)
                {
                    var count = (int)Math.Min(remaining, BatchSize);
                    Read(codeColumn, code, count);
                    if (assetColumn != null) Read(assetColumn, rowAsset, count);
                    Read(fromColumn, from, count);
                    if (!daily) Read(toColumn, to, count);
                    var keptInBatch = 0;
                    if (daily)
                    {
                        for (var i = 0; i < count; i++)
                        {
                            if (code[i] != null && indices.ContainsKey(code[i]))
                            {
                                keptSlots[i] = keptRowCount++;
                                keptInBatch++;
                            }
                            else keptSlots[i] = -1;
                        }
                    }
                    foreach (var column in values) column.ReadBatch(keptSlots, count, keptInBatch);
                    foreach (var rule in rules)
                    {
                        rule.ReadBatch(count);
                        for (var i = 0; i < count; i++)
                        {
                            if (daily && rule.Revision[i] == null) continue;
                            if (code[i] == null || (!verified && rule.RuleId[i] == null))
                                throw new InvalidDataException("冻结规则缺少证券或规则身份");
                            if (!indices.TryGetValue(code[i], out var instrument)) continue;
                            var kind = rule.Kind;
                            if (!verified && !kinds.TryGetValue(rule.RuleId[i], out kind)) continue;
                            if (daily && kind != rule.Kind)
                                throw new InvalidDataException($"逐日参考使用错误规则分组：{code[i]} {rule.RuleId[i]}");
                            var start = RuleDate(dates, from[i]);
                            var stop = daily ? start : RuleDate(dates, to[i]);
                            if ((!verified && rowAsset[i] != asset) || rule.Revision[i] is not long version || version < 1 || start > stop
                                || rule.Parameter[i] is not long template || template < 0 || template >= _templates.Length)
                                throw new InvalidDataException($"冻结现货规则字段无效：{code[i]} {rule.RuleId[i]}");
                            var reason = -1;
                            long ticks = 0;
                            var available = rule.Available[i];
                            var unsupported = verified && available == null ? "冻结逐日规则不可用" : rule.Unsupported[i];
                            if ((verified || rule.Status[i] == "supported") && unsupported == null && available != null)
                            {
                                if (!times.TryGetValue(available, out ticks))
                                {
                                    if (!(available.EndsWith('Z') || available.Length >= 6 && (available[^6] is '+' or '-')))
                                        throw new InvalidDataException("冻结规则可见时点缺少时区");
                                    ticks = DateTimeOffset.Parse(available, CultureInfo.InvariantCulture).UtcTicks;
                                    times.Add(available, ticks);
                                }
                            }
                            else if ((verified || rule.Status[i] == "unsupported") && !string.IsNullOrWhiteSpace(unsupported) && available == null)
                            {
                                if (!reasons.TryGetValue(unsupported, out reason))
                                    reasons.Add(unsupported, reason = reasons.Count);
                            }
                            else throw new InvalidDataException($"冻结规则状态与可见时点不一致：{code[i]} {rule.RuleId[i]}");
                            (rows[instrument, kind] ??= new List<Rule>()).Add(
                                new Rule(start, stop, ticks, version, (int)template, reason, daily ? keptSlots[i] : -1));
                        }
                    }
                    remaining -= count;
                }
            }
            finally
            {
                foreach (var column in values) column.Dispose();
                foreach (var rule in rules) rule.Dispose();
            }
        }
        if (daily)
            foreach (var column in _dailyColumns.Values) column.Seal();
    }

    private void AddDailyColumns()
    {
        void Text(string prefix, int section, string name) =>
            _dailyColumns.Add(name, new DailyColumn<string, string>(name, prefix + name, section, value => value, value => new(value)));
        void Flag(string prefix, int section, string name) =>
            _dailyColumns.Add(name, new DailyColumn<bool?, bool>(name, prefix + name, section, value => value.Value, value => new(value)));

        foreach (var name in new[] { "price_scale", "price_tick_units", "high_limit_units", "low_limit_units",
            "reference_previous_close_units", "reference_price_units", "price_limit_ratio_ppm",
            "price_limit_up_ratio_ppm", "price_limit_down_ratio_ppm" })
            _dailyColumns.Add(name, new DailyColumn<long?, long>(name, "prices_" + name, 2, value => value.Value, value => new(value)));
        foreach (var name in new[] { "price_limit_mode", "reference_price_kind", "reference_price_available_at" })
            Text("prices_", 2, name);
        foreach (var name in new[] { "stock_board", "listing_phase", "trading_status" })
            Text("lifecycle_", 0, name);
        Flag("lifecycle_", 0, "is_st");
        Flag("suspension_", 4, "paused");
    }

    public bool Covers(string code, DateOnly day)
    {
        if (!_instruments.TryGetValue(code, out var instrument)) return false;
        foreach (var rules in instrument.Rules)
        {
            if (rules == null) return false;
            var covered = false;
            for (var i = rules.Locate(day.DayNumber); rules.Reaches(i, day.DayNumber); i--)
                if (rules[i].Covers(day.DayNumber))
                {
                    covered = true;
                    break;
                }
            if (!covered) return false;
        }
        return true;
    }

    public ChinaRule[] ReadInstrument(string code, DateOnly day, SpotExecutionWindow window)
    {
        if (!_instruments.TryGetValue(code, out var instrument)) return Array.Empty<ChinaRule>();
        if (_combined.Count > RetainedTemplateLimit) _combined.Clear();
        if (_parsed.Count > RetainedTemplateLimit) _parsed.Clear();

        var start = Shanghai(window.StartAt).UtcTicks;
        var close = Shanghai(window.EndAt).UtcTicks;
        var end = Shanghai(window.EndAt == window.SessionCloseAt
            ? day.AddDays(1).ToDateTime(TimeOnly.MinValue) : window.EndAt.AddTicks(10)).UtcTicks;
        var points = new SortedSet<long> { start };
        Span<int> cursors = stackalloc int[6];
        var hasCandidate = false;
        for (var kind = 0; kind < instrument.Rules.Length; kind++)
        {
            var rules = instrument.Rules[kind];
            cursors[kind] = rules?.Locate(day.DayNumber) ?? -1;
            if (rules == null) continue;
            for (var i = cursors[kind]; rules.Reaches(i, day.DayNumber); i--)
            {
                var rule = rules[i];
                if (!rule.Covers(day.DayNumber)) continue;
                hasCandidate = true;
                if (rule.VisibleTicks > start && rule.VisibleTicks <= close) points.Add(rule.VisibleTicks);
            }
        }
        if (!hasCandidate) return Array.Empty<ChinaRule>();

        var cuts = points.ToArray();
        var output = new ChinaRule[cuts.Length];
        Span<Rule> selected = stackalloc Rule[6];
        for (var i = 0; i < cuts.Length; i++)
        {
            var at = cuts[i];
            var until = i + 1 < cuts.Length ? cuts[i + 1] : end;
            long available = 0;
            for (var kind = 0; kind < instrument.Rules.Length; kind++)
            {
                selected[kind] = Select(instrument, kind, cursors[kind], day, at);
                available = Math.Max(available, selected[kind].VisibleTicks);
            }
            var template = GetTemplate(selected, code, day);
            var rows = new DailyRows(selected[0].DailyIndex, selected[2].DailyIndex, selected[4].DailyIndex);
            foreach (var column in template.Overlaps)
            {
                var value = column.At(rows);
                if (value.ValueKind != JsonValueKind.Undefined && !value.Matches(template.Parameters[column.Name]))
                    throw ConflictingParameter(code, day, column.Name);
            }
            var overrides = rows.Lifecycle >= 0 || rows.Prices >= 0 || rows.Suspension >= 0
                ? new DailyParameters(_dailyColumns, rows) : null;
            output[i] = ChinaRule.FromFrozenDay(code, day, At(available), At(at), At(until),
                template, overrides, _validatedDailyPrices && rows.Prices >= 0 && selected[2].VisibleTicks <= start);
        }
        return output;
    }

    private Template GetTemplate(ReadOnlySpan<Rule> rules, string code, DateOnly day)
    {
        var key = new TemplateKey(rules[0].Parameters, rules[1].Parameters, rules[2].Parameters,
            rules[3].Parameters, rules[4].Parameters, rules[5].Parameters);
        if (_combined.TryGetValue(key, out var combined)) return combined;

        var fields = new Dictionary<string, JsonElement>(32, StringComparer.Ordinal);
        foreach (var rule in rules)
        {
            if (!_parsed.TryGetValue(rule.Parameters, out var template))
            {
                template = JsonSerializer.Deserialize<JsonElement>(_templates[rule.Parameters]);
                if (template.ValueKind != JsonValueKind.Object)
                    throw new InvalidDataException("冻结现货规则参数必须是对象");
                _parsed.Add(rule.Parameters, template);
            }
            foreach (var property in template.EnumerateObject())
            {
                if (!fields.TryAdd(property.Name, property.Value)
                    && !JsonElement.DeepEquals(fields[property.Name], property.Value))
                    throw ConflictingParameter(code, day, property.Name);
            }
        }
        combined = new Template(fields, _dailyColumns.Values.Where(column => fields.ContainsKey(column.Name)).ToArray());
        _combined.Add(key, combined);
        return combined;
    }

    private static InvalidDataException ConflictingParameter(string code, DateOnly day, string name) =>
        new($"冻结规则参数冲突：{code} {day:yyyy-MM-dd} {name}");

    private Rule Select(InstrumentRules instrument, int kind, int index, DateOnly day, long at)
    {
        var rows = instrument.Rules[kind];
        Rule selected = default;
        var hasCandidate = false;
        for (var i = index; rows != null && rows.Reaches(i, day.DayNumber); i--)
        {
            var rule = rows[i];
            if (!rule.Covers(day.DayNumber)) continue;
            hasCandidate = true;
            if (rule.VisibleTicks > at) continue;
            if (selected.Revision > 0 && (rule.From != selected.From || rule.To != selected.To))
                throw new InvalidDataException($"rule_snapshot_missing_or_conflicting：{instrument.Code} {day:yyyy-MM-dd}");
            if (rule.Revision > selected.Revision) selected = rule;
        }
        if (selected.Revision == 0)
            throw new InvalidDataException(hasCandidate
                ? $"rule_snapshot_unavailable_at_decision：{instrument.Code} {day:yyyy-MM-dd} {At(at):O}"
                : $"rule_snapshot_missing_or_conflicting：{instrument.Code} {day:yyyy-MM-dd}");
        if (selected.Unsupported >= 0)
            throw new InvalidDataException($"rule_snapshot_unsupported:{_unsupported[selected.Unsupported]}：{instrument.Code} {day:yyyy-MM-dd}");
        return selected;
    }

    private static string FrozenPath(JsonElement source, string name, string filename)
    {
        var path = source.GetProperty(name).GetString();
        if (path == null || !Path.IsPathFullyQualified(path) || Path.GetFileName(path) != filename)
            throw new InvalidDataException($"冻结现货规则路径无效：{name}");
        return Path.GetFullPath(path);
    }

    private static byte[][] LoadParameters(string path)
    {
        using var reader = new ParquetFileReader(path);
        using var metadata = reader.FileMetaData;
        var column = Columns(metadata)["parameters"];
        var values = new byte[checked((int)metadata.NumRows)][];
        var buffer = new string[BatchSize];
        var index = 0;
        for (var i = 0; i < metadata.NumRowGroups; i++)
        {
            using var group = reader.RowGroup(i);
            using var rows = group.Column(column).LogicalReader<string>();
            for (var remaining = group.MetaData.NumRows; remaining > 0;)
            {
                var count = (int)Math.Min(remaining, BatchSize);
                Read(rows, buffer, count);
                for (var j = 0; j < count; j++)
                    values[index++] = System.Text.Encoding.UTF8.GetBytes(buffer[j]
                        ?? throw new InvalidDataException("冻结规则参数模板不能为空"));
                remaining -= count;
            }
        }
        if (index != values.Length) throw new EndOfStreamException("冻结规则参数行数与页脚不符");
        return values;
    }

    private static Dictionary<string, int> Columns(FileMetaData metadata)
    {
        var schema = metadata.Schema;
        // 来源证据的列表叶节点不参与选版，只有顶层标量列用于引擎读取。
        return Enumerable.Range(0, schema.NumColumns)
            .GroupBy(i => schema.Column(i).Name)
            .Where(columns => columns.Count() == 1)
            .ToDictionary(columns => columns.Key, columns => columns.Single());
    }

    private static int RuleDate(Dictionary<string, int> dates, string value)
    {
        if (value == null) throw new InvalidDataException("冻结规则缺少有效日期");
        if (!dates.TryGetValue(value, out var result))
            dates.Add(value, result = DateOnly.ParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture).DayNumber);
        return result;
    }

    private static void Read<T>(LogicalColumnReader<T> reader, T[] buffer, int count)
    {
        if (reader.ReadBatch(buffer.AsSpan(0, count)) != count)
            throw new EndOfStreamException("冻结现货规则未交付完整列");
    }

    private static DateTimeOffset Shanghai(DateTime value) => new(value, ShanghaiOffset);
    private static DateTimeOffset At(long utcTicks) => new DateTimeOffset(utcTicks, TimeSpan.Zero).ToOffset(ShanghaiOffset);
}
