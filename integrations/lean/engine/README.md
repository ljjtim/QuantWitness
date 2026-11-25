# LEAN 引擎安装与策略桥接

构建 LEAN 插件并理解策略桥接接口。

## 开始前

- 输入：已安装的 SDK、Launcher、Python 环境和元数据。
- 输出：安装资产、桥接输入及正式引擎输出。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 按上级文档完成干净安装。
2. 执行 `build.ps1` 构建插件。
3. 用正式研究主链冻结安装。
4. 按本页接口检查输入和事件。
5. 核对引擎终态及实际输出。

中国规则、公司行动和直接数据馈送将已准入策略接到LEAN原生订单与账户。产物类型为 `research.lean-simulation.v4`。一分钟执行模型为 `lean_next_completed_close_v1`，日线为 `lean_next_session_open_v1`。

现货有界价格支持前收对称简写或显式参考类型及独立上下比例，发行价仅用于股票IPO阶段。上下限按整数报价单位四舍五入，参考价必须在实际消费时可见；公司行动仍要求前收参考。字段和示例见[现货价格规则](../../../docs/minute_market_rules.md#现货价格参考与独立上下比例)。

桥接不连接数据库。干净安装使用[上级README](../README.md)说明的Python.NET关闭和Windows虚拟环境路径初始化兼容补丁。

本地构建使用与 LEAN 依赖相同的 QuantConnect.pythonnet 2.0.67 源码。离线安装可将 `pythonnet-ba1b2eae84a34ea98301e47732c2a56c59a52821.zip` 放入下载缓存。关闭时每轮执行两次 CLR 回收、终结器等待、Python 回收及跨语言引用队列清理。整轮两侧均无回收后结束当前阶段，仍有工作时继续执行原有的重试轮数。日志中的 `Python.NET shutdown GC` 给出达到稳定状态的轮数。[对应上游源码](https://github.com/QuantConnect/pythonnet/blob/ba1b2eae84a34ea98301e47732c2a56c59a52821/src/runtime/Runtime.cs)

## 编译与安装

在包含 `pyproject.toml` 的 QuantWitness 公开源码根目录执行：

```powershell
& ./integrations/lean/engine/build.ps1 -LeanHome <安装目录> -Install
```

不带 `-Install` 时构建并准备插件资产。脚本使用 `LeanHome/sdk/dotnet.exe`、`python-env/Scripts/python.exe` 和 `upstream-revision.txt` 指向的固定源码，应用原生生命周期接点并增量编译 Launcher。构建不保留后台编译进程。

要求已有 .NET 10 SDK 与 Launcher Release 依赖。干净安装由上级 `install.ps1` 完成。已有安装沿用 `installation.json` 记录的元数据目录；首次构建使用 `LeanHome/input-metadata`。也可用 `-MetadataSource` 显式指定，仅复制market-hours和symbol-properties两个目录。

jqchina 的基础 LotSize 和最小数量设为 1，历史申报数量由中国规则限制。

构建、包缓存、日志和临时文件全部位于 `LeanHome/rp-integration/`。当前安装布局：

```text
<安装目录>/rp-integration/
  config-template.json
  installation.json
  metadata/
    market-hours/market-hours-database.json
    symbol-properties/security-database.csv
    symbol-properties/symbol-properties-database.csv
    equity/{jqchina,usa}/{map_files,factor_files}/
  assets/
    QuantWitnessLeanHost.dll
    ChinaRules.dll
    CorporateActions.dll
    ParquetInput.dll
    ParquetSharp.dll
    Apache.Arrow.dll
    ParquetSharpNative.dll
  bin/
  obj/
  cache/
  logs/
  temp/
```

`-Install` 把本页的七个 DLL 复制到 `src/Lean-<upstream-revision>/Launcher/bin/Release/`。Host 的程序集名是 `QuantWitnessLeanHost`，类型为 `QuantWitness.Lean.Research.LeanStrategyHost`。中国规则、公司行动和Parquet插件与宿主一起构建。

`installation.json`记录dotnet、launcher、Python环境、配置模板、元数据、插件和安装状态。实际项目算法入口由冻结bundle及运行配置绑定。

## 原生算法与引擎生命周期

主运行时按冻结LeanAlgorithm入口装载项目Python或C# QCAlgorithm，配置实际algorithm-type-name、algorithm-language与algorithm-location。NativeSetupHandler在用户Initialize之前安装冻结账户、市场元数据及金融模型；NativeMarketLifecycle通过引擎阶段推进市场状态、证券初始化、订单反馈与收盘权益，不依赖用户调用父类回调。

引擎补丁把市场准备放在过去Scheduled Events消费之前，按事件自身时点推进。数据更新后处理可见价格与原生订单，时间步结束后登记真实收盘持仓。独立 ResearchClock 按冻结交易时段推进，现货午休保留显式边界与独立事件，动态Universe为空也不跳过权益或账户结束。

中国Brokerage、Fill、Fee、BuyingPower及公司行动处理器直接约束原生Order、SetHoldings、Framework执行模型与Ticket操作。现货证券仅在原生订阅时初始化；动态加入仍需属于冻结输入集合。人民币期货保留真实Future及四桶账户，不以现货净仓替代。

## 数据与PIT

ParquetDataFeed、NativeHistoryProvider、Universe及预热共用统一原生数据服务。输入清单固定订阅绑定、原始来源、可见规则和日历。ResearchData以BindingId与Fields交付全部声明公开列，多源名称采用binding_id:完整代码；ResearchTradeBar保留原始ObservationTime/ObservationEndTime，原生EndTime为可见时间。

日线完整OHLCV按下一会话开盘可见；独立Open只提供当前开盘价格。Raw、非填充数据使用同一时点证明。预读不授予提前可见性。实现与有界读取见[数据插件](parquet-direct/README.md)。

## 交易支持范围

| 模型 | 当前支持 |
| --- | --- |
| 股票、ETF一分钟 | Market、Limit、MarketOnOpen；历史规则校验的提交、改单与撤单 |
| 股票、ETF日线 | 已可见日线决策后的原生Market开盘执行 |
| 人民币期货 | Market、Limit、DAY、IOC；显式开平和交易日有效期；改单须先撤再下 |

保护价、历史数量格点、T+1或ETF历史结算、现金及保证金、最低佣金、税费、容量和滑点由模型执行。ChinaOrderProperties支持ProtectionPrice与含费BudgetCny；NativeMarketLifecycle.QuantityForBudget按当前可见价和历史规则求量，AccountView提供只读金融账户。

现货 `AccountView` 从 LEAN 的当前非零持仓和开放委托生成持仓、可卖量和可用现金。空委托、空未结权益的账户在持仓数量、可卖量、退市事实及现金相同时复用当日视图，报告记录命中次数。未结登记、应收和锁定股份独立跟随公司行动账簿，卖光原股后仍按实际权益阶段交付。

原生现货日线的MarketOrder和SetHoldings保留Market订单，由中国开盘模型等待真实开盘机会，不转换为收盘单。宿主默认订单属性在日频将DailyOpenDecision设为true，允许已可见前日日线触发的订单在同刻开盘的后续成交阶段执行。自建ChinaOrderProperties默认false：同刻提交须等待后续开盘，早于开盘提交可在该开盘执行。日频Limit和显式MarketOnOpen不在当前支持范围，完整日线OHLCV不能作为开盘成交价格。

Stop、StopLimit、TrailingStop及所有TIF不属于当前支持承诺。日线开盘缺少竞价量，不提供竞价容量估计；日线估值使用当时最新可见价格，不冒充本日收盘净值。股票期货混合账户、其他资产、live与QMT没有因原生API接入而自动获得支持。

公司行动按登记、除权、到账、可卖推进，普通送转可显式采用sellable_date_receivable。应收不提供现金或可卖量，原股卖出不取消登记权益。确认退市后保留股份零估值，普通停牌与缺价不归零。

公司行动和退市按生效及可见时点推进。正式生命周期保留实际登记权益、后续处理，以及对账户有影响的除权和退市记录。账户估值只检查当前持仓与未结清权益。现货成功运行的 `lean/native-output/spot-account-view.json` 按交易月记录账户视图的调用次数和引擎内累计秒数。

## 资源与正式输出

初始化initialize_seconds、单时间步time_step_seconds与节点总预算分别监管。完整进程树的内存、CPU、进程槽、临时磁盘及累计时长沿ResearchPackage冻结；未完成或超额不能发布成功。

NativeResultRecorder记录真实原生请求与响应。LeanStrategyHost负责金融事实，NativeSpotResult保存现货成交、可卖量与权益，期货保留真实成交腿和四桶状态。正式simulation.v4包含orders、intents、daily、trace、fills、positions、account_events、lifecycle八表；无显式意图时intents为空，metrics仍为v2。

订单请求追踪在 LEAN 更新响应时记录。预交易拒绝、终态后改单和撤单、无效订单号和重复撤单均保留真实响应。同一请求的处理状态或响应变化各记一行。常规运行在交易日执行窗口结束及引擎退出时刷新追踪文件，严格审计逐行刷新。

账户终态、algorithm-state、runtime-lock、strategy-input、引擎完成摘要以及原生结果、Chart、ObjectStore、算法源码和配置进入正式Result。algorithm-state为算法身份和终态元数据，不是另一套策略JSON状态。详见[原生结果](../../../docs/lean-native-results.md)。

旧项目entry回调和逐事件JSON状态不提供兼容入口。历史Result及实际依赖只读保留，旧安装、计划、checkpoint不跨新合同恢复。失败LEAN节点从冻结输入整体重跑。

## 实施状态与临时文件

原生接入仍在实施验收中。源码编译、内存模型案例与合成组包不能代替从独立安装到正式Result及报告的完整引擎验收。

安装不运行策略，不访问数据库。依赖路径由QW_ENGINE_TEST_INSTALLATION与QW_ENGINE_TEST_BUILD显式提供，构建与测试使用独立输出目录，不覆盖运行中安装。

rp-integration下bin、obj、cache、logs、temp为构建材料；assets、模板、metadata和installation.json为冻结安装依赖。callback-events.jsonl为监管记录。正式Result及其源码、安装与输入依赖必须保留。

## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和预算的完整声明 |
| Catalog Lock | 已确认的字段、来源和可见性说明 |
| PIT（按时点可见） | 过去某时刻只使用当时已知的信息 |
| bar（周期行情） | 一个日线或分钟区间的行情记录 |
| OHLCV（开高低收量） | 开盘价、最高价、最低价、收盘价和成交量 |
| Feature（特征） | 决策时已知的研究输入 |
| Label（标签） | 用于评价预测的未来目标 |
| bundle（封存包） | 完整代码、依赖或数据声明及其内容身份 |
| checkpoint（检查点） | 通过验收的节点输出和恢复依据 |
| Result（正式结果包） | 正式表、运行摘要及配置、实现和输入引用 |
| minute_scope（分钟范围引用） | 指向同一冻结日历和历史规则的小引用 |
| Supervisor | 监督运行和提交结果的主进程 |
| Worker | 执行节点计算的独立进程 |
