"""为固定 LEAN 安装原生研究生命周期接点。"""
from pathlib import Path
import argparse


def replace(root: Path, relative: str, old: str, new: str) -> None:
    path = root / relative
    content = path.read_text(encoding="utf-8-sig")
    if new in content:
        return
    if content.count(old) != 1:
        raise ValueError(f"固定 LEAN 接点不匹配：{relative}")
    path.write_text(content.replace(old, new), encoding="utf-8", newline="\n")


def apply(root: Path) -> None:
    # 所有订单请求响应在此更新，包括未纳入交易管理器票据的请求。
    order_request = "Common/Orders/OrderRequest.cs"
    replace(root, order_request, "    public abstract class OrderRequest\n    {",
            "    public abstract class OrderRequest\n    {\n"
            "        /// <summary>报告订单请求状态及响应的实际变化。</summary>\n"
            "        public static event Action<OrderRequest> ResponseChanged;\n")
    replace(root, order_request, '''            // if the response is an error, ignore the input status
            Status = response.IsError ? OrderRequestStatus.Error : status;
            Response = response;''', '''            var previousStatus = Status;
            var previousResponse = Response;
            Status = response.IsError ? OrderRequestStatus.Error : status;
            Response = response;
            if (previousStatus != Status || previousResponse.ErrorCode != response.ErrorCode
                || previousResponse.ErrorMessage != response.ErrorMessage)
            {
                ResponseChanged?.Invoke(this);
            }''')
    replace(root, "Common/Orders/OrderTicket.cs",
            '''                    return OrderResponse.Error(request, OrderResponseErrorCode.RequestCanceled,
                        Messages.OrderTicket.CancelRequestAlreadySubmitted(this));''',
            '''                    request.SetResponse(OrderResponse.Error(request, OrderResponseErrorCode.RequestCanceled,
                        Messages.OrderTicket.CancelRequestAlreadySubmitted(this)));
                    return request.Response;''')
    replace(root, "Common/Securities/SecurityTransactionManager.cs",
            "                throw new Exception(OrderResponse.WarmingUp(request).ToString());",
            "                request.SetResponse(OrderResponse.WarmingUp(request));\n"
            "                throw new Exception(request.Response.ToString());")
    security = "Common/Securities/Security.cs"
    # 原生研究在证券初始化后冻结成交及费用模型，拦截 C# 属性和 Python setter。
    for model in ("Fee", "Fill"):
        field = "_" + model.lower() + "Model"
        property_comment = {
            "Fee": "Fee model used to compute order fees for this security",
            "Fill": "Fill model used to produce fill events for this security",
        }[model]
        declaration = f"        /// <summary>\n        /// {property_comment}\n        /// </summary>\n"
        replace(root, security, declaration + f"""        public I{model}Model {model}Model
        {{
            get;
            set;
        }}""", f"        private I{model}Model {field};\n" + declaration + f"""        public I{model}Model {model}Model
        {{
            get => {field};
            set
            {{
                if (_researchTradingModelsFrozen && !ReferenceEquals(value, {field}))
                    throw new InvalidOperationException("原生研究不能更换已冻结的成交或费用模型");
                {field} = value;
            }}
        }}""")
    marker = "        public void SetFeeModel(IFeeModel feelModel)"
    replace(root, security, "        /// <summary>\n        /// Sets the fee model\n        /// </summary>\n"
            '        /// <param name="feelModel">Model that represents a fee model</param>\n' + marker,
            "        private bool _researchTradingModelsFrozen;\n"
            "        public void FreezeResearchTradingModels() => _researchTradingModelsFrozen = true;\n\n"
            "        /// <summary>\n        /// Sets the fee model\n        /// </summary>\n"
            '        /// <param name="feelModel">Model that represents a fee model</param>\n' + marker)
    algorithm = "Algorithm/QCAlgorithm.cs"
    replace(root, algorithm, "        public void SetBrokerageModel(IBrokerageModel model)\n        {",
            "        public void SetBrokerageModel(IBrokerageModel model)\n        {\n"
            "            if (_researchBrokerageModelFrozen)\n"
            "                throw new InvalidOperationException(\"原生研究不能重新设置冻结券商模型\");")
    brokerage_doc = "        /// <summary>\n        /// Gets the brokerage model - used to model interactions with specific brokerages."
    replace(root, algorithm, brokerage_doc,
            "        private bool _researchBrokerageModelFrozen;\n"
            "        public void FreezeResearchBrokerageModel() => _researchBrokerageModelFrozen = true;\n\n"
            + brokerage_doc)
    # 本地 Launcher 由外层节点监督。退出前等桥接层清理完成，异常沿退出码交给宿主。
    replace(root, "Launcher/Program.cs", '''                var isolator = new Isolator();
                isolator.ExecuteWithTimeLimit(TimeSpan.FromSeconds(10), PythonInitializer.Shutdown, -1);''',
            '''                PythonInitializer.Shutdown();''')
    replace(root, "Launcher/Program.cs", '''                Log.Error(ex, $"Failed to shutdown python");''',
            '''                exitCode = 1;
                Log.Error(ex, $"Failed to shutdown python");''')
    initializer = "Common/Python/PythonInitializer.cs"
    gil_line = "var pyLock = Py.GIL();"
    if gil_line not in (root / initializer).read_text(encoding="utf-8-sig"):
        gil_line = "// Python.NET关闭时管理GIL。"
    replace(root, initializer, '''                try
                {
                    GIL_LINE
                    Log.Trace($"PythonInitializer.Shutdown(): calling engine shutdown...");
                    PythonEngine.Shutdown();
                }
                catch (Exception ex)
                {
                    Log.Error(ex);
                }'''.replace("GIL_LINE", gil_line),
            '''                Log.Trace($"PythonInitializer.Shutdown(): calling engine shutdown...");
                PythonEngine.Shutdown();''')
    target = root / "Engine/Research/ResearchLifecycle.cs"
    target.parent.mkdir(exist_ok=True)
    lifecycle_source = '''using System;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Util;
using System.ComponentModel.Composition;

namespace QuantConnect.Lean.Engine.Research
{
    [InheritedExport(typeof(IResearchLifecycle))]
    public interface IResearchLifecycle
    {
        void PrepareTime(IAlgorithm algorithm, DateTime utc);
        void AfterData(IAlgorithm algorithm);
        void AfterTimeStep(IAlgorithm algorithm);
        void AfterInitialize(IAlgorithm algorithm);
        void BeforeInitialize(IAlgorithm algorithm);
        void Complete(IAlgorithm algorithm);
    }

    public static class ResearchLifecycle
    {
        private static readonly Lazy<IResearchLifecycle> Handler = new Lazy<IResearchLifecycle>(() =>
        {
            var type = Config.Get("research-lifecycle-handler");
            return string.IsNullOrEmpty(type) ? null : Composer.Instance.GetExportedValueByTypeName<IResearchLifecycle>(type);
        });
        public static IResearchLifecycle Current => Handler.Value;
    }
}
'''
    if not target.exists() or target.read_text(encoding="utf-8") != lifecycle_source:
        target.write_text(lifecycle_source, encoding="utf-8", newline="\n")
    lifecycle = "QuantConnect.Lean.Engine.Research.ResearchLifecycle.Current"
    replace(root, "Engine/AlgorithmManager.cs", "                algorithm.SetDateTime(time);",
            f"                algorithm.SetDateTime(time);\n                {lifecycle}?.PrepareTime(algorithm, time);")
    replace(root, "Engine/AlgorithmManager.cs", "                // security prices got updated",
            f"                {lifecycle}?.AfterData(algorithm);\n\n                // security prices got updated")
    replace(root, "Engine/AlgorithmManager.cs", "                algorithm.OnEndOfTimeStep();",
            f"                algorithm.OnEndOfTimeStep();\n                {lifecycle}?.AfterTimeStep(algorithm);")
    replace(root, "Engine/AlgorithmManager.cs", "                    CheckWarmupFinished();\n                    continue;",
            f"                    CheckWarmupFinished();\n                    {lifecycle}?.AfterTimeStep(algorithm);\n                    continue;")
    replace(root, "Engine/RealTime/BacktestingRealTimeHandler.cs", "                Algorithm.SetDateTime(nextEventUtcTime);",
            f"                Algorithm.SetDateTime(nextEventUtcTime);\n                {lifecycle}?.PrepareTime(Algorithm, nextEventUtcTime);")
    replace(root, "Engine/Setup/BacktestingSetupHandler.cs", "                    algorithm.Initialize();",
            f"                    {lifecycle}?.BeforeInitialize(algorithm);\n                    algorithm.Initialize();\n                    {lifecycle}?.AfterInitialize(algorithm);")
    replace(root, "Engine/Engine.cs", "                    SystemHandlers.LeanManager.OnAlgorithmEnd();",
            f"                    {lifecycle}?.Complete(algorithm);\n                    SystemHandlers.LeanManager.OnAlgorithmEnd();")
    replace(root, "Engine/Engine.cs", "                AlgorithmHandlers.Results.Exit();\n                AlgorithmHandlers.DataFeed.Exit();",
            f"                try {{ {lifecycle}?.Complete(algorithm); }}\n"
            "                catch (Exception completionError) { Log.Error(completionError); }\n"
            "                AlgorithmHandlers.Results.Exit();\n                AlgorithmHandlers.DataFeed.Exit();")
    replace(root, "AlgorithmFactory/Python/Wrappers/AlgorithmPythonWrapper.cs",
            "                            typeof(QCAlgorithm).IsAssignableFrom(type))",
            "                            typeof(QCAlgorithm).IsAssignableFrom(type)\n"
            "                            && name.ToString() == QuantConnect.Configuration.Config.Get(\"algorithm-type-name\").Split('.')[^1])")
    replace(root, "Common/Orders/OrderProperties.cs", "        public TimeInForce TimeInForce { get; set; }",
            "        public TimeInForce TimeInForce { get; set; }\n\n"
            "        /// <summary>自定义日线执行模型保留原生市价请求，由模型选择真实开盘机会。</summary>\n"
            "        public virtual bool PreserveMarketOrder => false;")
    replace(root, "Algorithm/QCAlgorithm.Trading.ContingentOrders.cs",
            "            // For futures and FOPs, market orders can be submitted on extended hours, so we let them through.",
            "            if (!LiveMode && IsDailyResolutionOnly(security.Symbol)\n"
            "                && request.OrderProperties is OrderProperties { PreserveMarketOrder: true }) return null;\n\n"
            "            // For futures and FOPs, market orders can be submitted on extended hours, so we let them through.")
    replace(root, "Common/Brokerages/DefaultBrokerageModel.cs",
            "    public class DefaultBrokerageModel : IBrokerageModel\n    {",
            "    public class DefaultBrokerageModel : IBrokerageModel\n    {\n"
            "        /// <summary>由自定义经纪模型按原始市价委托审批。</summary>\n"
            "        public virtual bool PreserveMarketOrder(Security security) => false;")
    replace(root, "Algorithm/QCAlgorithm.Trading.ContingentOrders.cs",
            "            // When the market is closed the order is converted to fill at the next open (MarketOnOpen),",
            "            if (!LiveMode && BrokerageModel is QuantConnect.Brokerages.DefaultBrokerageModel model\n"
            "                && model.PreserveMarketOrder(security)) return null;\n\n"
            "            // When the market is closed the order is converted to fill at the next open (MarketOnOpen),")
    replace(root, "Algorithm/QCAlgorithm.History.cs",
            "                    if ((type == null || type == typeof(Tick)) && !configs.Any(config => config.TickType == TickType.Quote))",
            "                    if ((type == null || type == typeof(Tick))\n"
            "                        && SubscriptionManager.AvailableDataTypes.TryGetValue(symbol.SecurityType, out var availableTickTypes)\n"
            "                        && availableTickTypes.Contains(TickType.Quote)\n"
            "                        && !configs.Any(config => config.TickType == TickType.Quote))")
    analyzer = "Engine/Results/Analysis/ResultsAnalyzer.cs"
    replace(root, analyzer, '''            // ── 2. Benchmark from SPY history ─────────────────────────────────────
            var spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
            var exchangeTimeZone = MarketHoursDatabase.FromDataFolder()
                .GetExchangeHours(spy.ID.Market, spy, spy.ID.SecurityType)
                .TimeZone;

            var benchmarkSeries = new SortedList<DateTime, decimal>();
            var historyStart = algorithm.StartDate - TimeSpan.FromDays(3);
            var historyEnd = algorithm.EndDate + TimeSpan.FromDays(1);
            foreach (var bar in algorithm.History(spy, historyStart, historyEnd, Resolution.Daily))
            {
                var time = algorithm.Settings.DailyPreciseEndTime ? bar.EndTime.AddDays(1).Date : bar.EndTime;
                benchmarkSeries.Add(time.Date.ConvertToUtc(exchangeTimeZone), bar.Close);
            }

            if (benchmarkSeries.Count == 0)
            {
                return (new(), new());
            }''', '''            // 分析使用本次实际采样的基准，不请求额外市场来源。
            if (!result.Charts.TryGetValue("Benchmark", out var benchmarkChart)
                || !benchmarkChart.Series.TryGetValue("Benchmark", out var benchmarkValues)
                || benchmarkValues.Values.Count == 0)
            {
                return (new(), new());
            }
            var benchmarkSeries = new SortedList<DateTime, decimal>(benchmarkValues.Values
                .Where(point => point is ChartPoint chartPoint && chartPoint.Y.HasValue)
                .GroupBy(point => point.Time)
                .ToDictionary(group => group.Key, group => ((ChartPoint)group.Last()).Y.Value));
            if (benchmarkSeries.Count == 0) return (new(), new());''')
    replace(root, analyzer, "            var sampledEquity = sampler.Sample(equitySeries, start, stop);",
            '''            var sampledEquity = sampler.Sample(equitySeries, start, stop);
            var sampledBenchmark = sampler.Sample(benchmarkValues, start, stop);
            benchmarkSeries = new SortedList<DateTime, decimal>(sampledBenchmark.Values
                .Where(point => point is ChartPoint chartPoint && chartPoint.Y.HasValue)
                .GroupBy(point => point.Time)
                .ToDictionary(group => group.Key, group => ((ChartPoint)group.Last()).Y.Value));''')
    sharpe = "Engine/Results/Analysis/Analyses/CrisisEventsAnalysis.cs"
    replace(root, sharpe, "            var riskFreeRate = (double)riskFreeInterestRateModel.GetRiskFreeRate(backtest.First().Key, backtest.Last().Key);",
            '''            // 常数基准或不足两个收益观测不能计算相对夏普，不产生数值结论。
            if (backtestReturns.Length < 2 || benchmarkReturns.Length < 2
                || backtestReturns.StandardDeviation() == 0 || benchmarkReturns.StandardDeviation() == 0)
                return (double.NaN, double.NaN);
            var riskFreeRate = (double)riskFreeInterestRateModel.GetRiskFreeRate(backtest.First().Key, backtest.Last().Key);''')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    apply(parser.parse_args().source)
