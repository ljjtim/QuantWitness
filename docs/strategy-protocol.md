# 原生LEAN算法声明

项目策略直接使用Python或C#的`QCAlgorithm`。ResearchPackage冻结算法源码、数据来源、可见规则、中国金融模型与资源预算，由本地LEAN执行并发布Result。

## 算法与数据绑定

声明版本为`research-lean-native-strategy-v1`。`LeanAlgorithm`包含`language`、`entry_file`、`type_name`，对应封存源码中的实际算法类；Python入口为`.py`，C#入口为已构建的`.dll`。项目bundle通过`lean_algorithm`绑定入口。

`LeanStrategyDeclaration`包含算法、bundle身份、资产、频率、币种、`input_bindings`和`limits`。`LeanDataBinding`保存`binding_id`、`input_port`、`request_id`、`kind`、`resolution`和公开`columns`；日期来源另绑定经Catalog核对的`daily_time`与冻结交易日历。kind为bars、event、universe或targets。初始化与单时间步分别使用`initialize_seconds`和`time_step_seconds`预算，节点继续受总资源预算监管。

算法实现原生Initialize、OnData、OnOrderEvent、Scheduled Events及其他LEAN生命周期方法，使用原生指标、Consolidator、Universe、History、SetWarmUp、Portfolio、OrderTicket、Chart和ObjectStore。选股可在算法内部运行，也可消费正式上游研究输出；准入不运行选股或交易。

普通数据绑定的`request_id`引用实际QueryIR。`kind=targets`直接绑定正式组合目标输入工件，`request_id`为null；其身份由真实上游节点和工件依赖冻结。

## 统一可见性

订阅、History、预热、Universe和证券初始化共享已冻结来源及消费者时钟。完整日线按Catalog的next_session_open在下一交易日开盘可见；ResearchTradeBar保留原始ObservationTime和ObservationEndTime，EndTime表示可见时刻。原生History索引使用可见时刻，不能据原始报告期提前读取数据。

财报、公告和业务数据使用ResearchData，BindingId标识来源，Fields仅包含声明公开列。多来源订阅使用`binding_id:完整证券代码`区分同证券同频率数据。Universe读取相同可见服务，动态候选退出不删除真实持仓和已登记权益。独立研究时钟在冻结日历窗口推进，空证券池也继续处理账户与权益。

Python 策略的 `ReadOnlyPIT` 对只含一个日期可见时钟的直读来源按日期块复用查询，按当前 LEAN 时钟、交易日历与冻结查询上限切片后返回声明列。原始公告和更正各自保留，跨日期或超出块预算的请求可直接精确查询。算法结束时的 `PIT read statistics` 日志按绑定记录实际查询、块命中和读数耗时。

现货算法调用 `NativeMarketLifecycle.AccountView(True)` 时，`unsettled_registration_codes` 列出已有正登记数量、现金尚未支付或股份尚未解锁的证券。算法可在调整分钟订阅时保留这些证券，覆盖卖清原股后仍有待交付权益的日期。

Python Universe 回调可从 `research_pipeline.lean_data` 导入 `read_fields`，一次读取同一来源、同一可见时点的全部业务字段。调用 `records, available_at = read_fields(rows, binding_id)` 后，按传入顺序读取字典列表。可见时间为带 `Asia/Shanghai` 时区的 Python `datetime`，保留微秒。空批次返回 `([], None)`。

需要对多行使用同一字段布局时，可调用 `groups, available_at = read_field_groups(rows, binding_id)`。每组包含字段名元组和对应的行值数组列表，行内值按字段名位置对应。分组沿用传入顺序，同一字段布局可以在多个分组中出现。可见时间和空批次规则与 `read_fields` 相同。

独立开盘来源只提供当时已知的Open，不包含当日尚未知的高、低、收和成交量。预读和缓存不改变消费者时间。AUDIT来源不能进入特征、信号或交易。

## 订单与中国市场模型

| 范围 | 当前金融模型 |
| --- | --- |
| 股票、ETF一分钟 | Market、Limit、MarketOnOpen；原生撤单和受规则检查的改单 |
| 股票、ETF日线 | 已可见日线决策后的Market开盘执行 |
| 人民币期货 | Market、Limit，DAY、IOC；显式open、close、close_today、close_yesterday |
| 股票数量与资金 | 历史申报格点、T+1；ETF按历史T+0或T+1；费用、保护价、涨跌停、滑点和容量 |
| 期货账户 | 真实Future、多空与今昨四桶、共享保证金、真实成交腿及独立结算 |

Order、MarketOrder、SetHoldings以及Framework执行模型生成的订单均经过中国Brokerage、Fill、Fee和BuyingPower模型。Ticket更新重新检查历史规则；期货改单要求先撤单再提交。ChinaOrderProperties支持人民币ProtectionPrice和含费BudgetCny；ChinaFutureOrderProperties须明确开平、期限及真实交易日截止时刻。

股票、ETF一分钟的普通市价单与限价单只在常规交易时段接受申报和改单，改单使用请求发生时的市场时钟。午休及收盘边界（例如北京时间 11:30 和 15:00）提交时，经纪模型直接拒绝，并保留原委托类型。需要盘前提交开盘单时，显式调用 MarketOnOpenOrder。

金融状态由引擎生命周期推进，不要求用户回调调用父类来更新T+1、费用或权益。公司行动在真实登记、除权、到账及可卖时点处理。应收股份不提供现金购买力或可卖量；真实持仓与应收同证券只占一个持仓名额。登记后卖出原股不取消权益。

项目算子可在 OperatorSpec 声明 JSON 参数 `strategy_worker_projection`，组包时用 `{"parameters": ["参数名"]}` 指定交给算法的项目参数。历史规则版本由引擎按当前决策时钟解析。现货算法可用 `NativeMarketLifecycle.QuantityForBudget(symbol, budgetCny)` 按当前可见报价和适用的历史数量、费用规则计算含费预算内的申报数量。

现货和期货算法可在正式执行期间查询已准入证券的订单规则。现货视图提供报价格点、人民币涨跌停价、市价保护要求、停牌、交收和四组申报数量规则。无涨跌幅限制时上下界为空。缺少当时适用规则或参考价尚不可见时，调用会报错。期货视图提供当前会话的交易日、报价格点、价格范围、保证金和费用。夜盘交易日按冻结会话解析，缺少会话或规则时调用会报错。

```python
from QuantWitness.Lean.Research import NativeMarketLifecycle

spot = NativeMarketLifecycle.CurrentSpotOrderRules(stock_symbol)
buy_step = spot.MarketBuy.Step
upper_limit = spot.UpperLimit

future = NativeMarketLifecycle.CurrentFutureOrderRules(future_symbol)
trading_date = future.TradingDate
tick = future.Rule.Tick
```

```csharp
using QuantWitness.Lean.Research;

var spot = NativeMarketLifecycle.CurrentSpotOrderRules(stockSymbol);
var buyStep = spot.MarketBuy.Step;
var upperLimit = spot.UpperLimit;

var future = NativeMarketLifecycle.CurrentFutureOrderRules(futureSymbol);
var tradingDate = future.TradingDate;
var tick = future.Rule.Tick;
```

确认退市生效后的剩余股份按零估值，保留数量和时点，不产生卖出现金；已登记现金权益继续交付。普通停牌和缺价不触发归零。

## 结果与恢复

正式输出为`research.lean-simulation.v4`。orders、intents、daily、trace、fills、positions、account_events、lifecycle八表使用v4，metrics继续使用v2。原生算法没有策略意图时intents为空；trace保存真实提交、更新、撤销请求及响应，不能从订单反推虚构意图。现货明细表保存真实成交、可卖量和权益，期货保留成交腿和四桶账户。

Result封存账户终态、algorithm-state、runtime-lock、输入声明和引擎完成摘要，以及完整原生输出、Chart、ObjectStore、配置与算法源码。来源和规则保留实际持久依赖。报告直接读取Result，详见[原生结果](lean-native-results.md)。

成功节点按冻结身份复用checkpoint；未完成LEAN节点整体重新执行。旧项目函数回调和逐事件JSON状态不提供兼容入口。历史Result及实际依赖保持只读，旧计划、安装与checkpoint不跨当前合同恢复。

## 已知限制

- 原生API可调用不代表所有订单类型已定义中国市场语义。Stop、StopLimit、TrailingStop及全部TIF不属于当前支持承诺。
- 分钟按后续完成分钟Close加声明滑点成交，日线按已声明开盘模型执行；保真度为bar_level_research_only。日线最新可见估值不是当日收盘净值。
- 当前金融模型分别支持人民币股票、ETF或期货，不能据此推断股票期货混合账户、其他资产、live或QMT已经接入。
- 公司行动支持毛额现金和整数送转，税款及其他分配类型需要明确金融合同。
- 原生完整接入仍在实施验收中；编译、组包与内存模型测试不替代从安装到正式Result的真实引擎验收。
