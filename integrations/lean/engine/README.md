# LEAN 引擎安装与策略桥接

中国规则、公司行动和直接数据馈送将已准入策略接到LEAN原生订单与账户。产物类型为 `research.lean-simulation.v1`；一分钟执行模型为 `lean_next_completed_close_v1`，日线为 `lean_next_session_open_v1`。桥接不连接数据库；干净安装使用[上级README](../README.md)说明的Python.NET关闭和Windows虚拟环境路径初始化兼容补丁。

## 编译与安装

在包含 `pyproject.toml` 的 QuantWitness 公开源码根目录执行：

```powershell
& ./integrations/lean/engine/build.ps1 -LeanHome <安装目录> -Install
```

不带 `-Install` 时只构建并准备安装资产。脚本使用 `LeanHome/sdk/dotnet.exe`、`python-env/Scripts/python.exe` 和 `upstream-revision.txt` 指向的现有 Launcher 产物；不重新编译上游。要求已有 .NET 10 SDK 与 Launcher Release 依赖。干净安装由上级 `install.ps1` 完成；元数据默认来自其生成的 `LeanHome/input-metadata`，也可用 `-MetadataSource` 显式指定，仅复制market-hours和symbol-properties两个目录；jqchina 的基础 LotSize 和最小数量设为 1，历史申报数量由中国规则限制。

构建、包缓存、日志和临时文件全部位于 `LeanHome/rp-integration/`。当前安装布局：

```text
<安装目录>/rp-integration/
  ResearchLeanAlgorithm.py
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

`-Install` 把上述七个 DLL 复制到 `src/Lean-<upstream-revision>/Launcher/bin/Release/`。Host 的程序集名是 `QuantWitnessLeanHost`，类型为 `QuantWitness.Lean.Research.LeanStrategyHost`。中国规则、公司行动和Parquet插件与宿主一起构建。

`installation.json` 提供 `dotnet`、`launcher`、`python`、`python_venv`、`algorithm`、`algorithm_relative`、`config_template`、`metadata`、`assets` 和安装状态。`algorithm_relative` 固定为 `ResearchLeanAlgorithm.py`，相对于 `rp-integration`。

## 主运行时接线

Python 桥接显式加载 `QCAlgorithm`、`OrderStatus`、`Config` 和 `LeanStrategyHost` 所需程序集与类型，启动时不加载通用 `AlgorithmImports` 的绘图库及字体缓存。策略源码按自身声明导入依赖。

主运行时从正式 admitted inputs 准备独立 run 目录，并封存安装资产。配置模板逐字复制原 Launcher 的 JSONC 配置；主运行时解析后设置：

| 配置 | 值 |
| --- | --- |
| `algorithm-type-name` | `ResearchLeanAlgorithm` |
| `algorithm-language` | `Python` |
| `algorithm-location` | 已封存 `ResearchLeanAlgorithm.py` 的绝对路径 |
| `environments.backtesting.data-feed-handler` | `QuantWitness.LeanInput.ParquetDataFeed` |
| `environment` | `backtesting` |
| `results-destination-folder` | 本次 run 绝对路径 |
| `parquet-input-manifest` | 本次 run/input.json 绝对路径 |
| `data-folder` | 本次 run 的 metadata 目录 |
| `python-venv` | 安装清单的 python_venv |
| `object-store-root` | 本次 run/storage |
| `close-automatically` | `true` |

Launcher 的工作目录使用安装清单的 `launcher`；主运行时负责 Python 原生环境、进程资源与回调超时监管。主进程调用形式：

```powershell
& '<installation.dotnet>' '<installation.launcher>/QuantConnect.Lean.Launcher.dll' --config '<run>/config.json'
```

这条命令属于研究运行，由主运行时执行；安装脚本只编译和安装资产。正式准入后不能再次安装并覆盖该计划绑定的资产。

## 输入与支持范围

run内保留`input.json`、`strategy-input.json`、`actions.json`、metadata和`strategy-source/`。默认行情与规则通过本机TCP按日内存交接，不生成行情或规则中间文件。生产者负责读取来源和下一交易日预读；引擎共享单连接，并在锁内顺序请求，全证券共用每天同一数组。

- `input.json`声明 `source_kind="direct_daily_stream"`、`endpoint={"host":"127.0.0.1","port":整数}`、`frequency="1m"|"1d"`、`symbols=[完整证券代码]`、`dates=["YYYY-MM-DD"]`、`price_scale=2|3`。dates与已冻结交易日历一致且按时间递增；当前支持A股及ETF日线与一分钟，不接受X分钟。价格除数为10的price_scale次方，Price.scale跟随输入；现金和预算Money.scale固定为2，与价格精度分开。
- 每次请求为4字节little endian JSON字节长度，再接UTF-8 JSON `{"date":"YYYY-MM-DD"}`。响应为同样的4字节头长度及JSON `{date,frequency,rules,signals,row_count,error?}`，随后紧接row_count条60字节little endian记录 `<iqqqqqqq`：int32证券索引、int64开始/结束UTC Unix微秒、按price_scale计量的整数open/high/low/close、整数volume。证券索引引用symbols；行按完整代码、开始时间排序。error非null时终止。signals为 `{available_at,selection}`，空信号同样提供selection空数组，与规则共用当日日缓存生命周期。
- 日线响应头必须有 `availability="next_session_open"`。binary保持原始交易日09:30至15:00区间；完整日线在冻结的下一session开盘才可见。最后一天没有后续session时不交付该日完整行情。规则来自当天header，ChinaRuleBook按历史有效区间和可见时间解析；估费保留最近已可见费率，不能用旧费率替代当日交易资格。
- `jqdata_monthly_parquet`和日Parquet读取器保留显式文件输入接口；它们仍只支持原始一分钟。直接输入不读取strategy-input中的rule_files。
- `strategy-input.entry={"module":"包.模块","function":"run"}` 从本次 `strategy-source/` 加载准入 bundle 源码，支持包内相对导入。未提供该目录时，可显式提供绝对已冻结 `execution_source`，仍须声明 entry；没有项目源码路径默认值。
- 现金使用 `initial_cash_units/100` 人民币；当前沿用初始空仓。clock 证券为 `input.json.symbols[0]`，主运行时需将最早具备有效日历的证券排在首位。
- declaration必须只有一个 `source=bars,delivery=columnar` binding；columns为open/high/low/close/volume的非空子集，lookback_bars为正整数。至多一个 `source=signals,lookback_bars=1,delivery=inline` binding。binding_id从declaration读取。schedules必须为空。
- 直接输入通过Host.DailySignals读取当天header中的signals；strategy-input仅保存静态声明，不复制整窗信号。每行的binding_id和列须符合declaration，批次及每行available_at都不得晚于当前模拟时点，只交付当时最新可见完整批次，不重算候选。Host报价请求包含全部可见信号证券及实际持仓证券。显式非direct文件输入保留strategy-input.daily接口。
- 策略调用为 `entry(context,event,state)`。`context.market.quote(binding_id,instrument_id)`返回最新已可见完整bar；`history(binding_id,instrument_id,count)`返回不超过声明窗口的已可见历史，不足时返回实际条数。values只包含声明columns，OHLC为声明价格精度下的整数、volume为证券份数。bar_start/bar_end保留原始区间，available_at独立表示可见时间。实际请求进入同一trace的quotes/histories并计入上下文额度；预读数组不作为策略历史来源。

ETF在策略上下文保留asset_class=cn_etf及contract_kind=etf，LEAN内使用Equity证券。数量格点、结算、费率、价格精度及价格格点由逐日rules消费；未单列price_tick_units的现货规则使用一个声明报价单位。股票要求T+1，ETF接受规则明确的T+0或T+1。手续费最低额及现金记账单位继续使用人民币分；公司行动参考价格使用已转换的人民币每份小数，不以现金精度截断ETF价格。

原生cn_fund合并规则的buy_lot_units作为买卖数量步长及最低量，保留sell_remainder_allowed；存在显式数量字段时使用显式值。基金价格上下限使用其price_scale及既有四舍五入口径，不要求股票专有的市价保护字段。min_commission_units始终是现金分，不能按三位价格再次缩放。
- 仅支持 `amount` 买入、`quantity` 买卖和 `cancel`。新委托仅为 `market`、`DAY`；目标数量、目标金额、目标权重、限价及其他类型明确拒绝。金额买单沿用可用现金裁剪与含费预算，数量单沿用原生购买力和中国规则。

## 时序、限额与成功状态

支持订阅 `initialize`、`before_trading`、`bar`、`execution_feedback`、`after_trading`。首个交易日09:30推进公司行动后交付initialize及before_trading；后续每日09:30交付before_trading。一分钟使用原生Resolution.Minute，完成时刻进入LEAN；新委托等待之后开始的完整分钟，成交使用Close加冻结滑点，容量来自已完成分钟。

日线使用原生Resolution.Daily订阅。D+1开盘由独立可见事件交付D日完整bar，再调用策略，随后执行原生Market订单。公开SubmitOrderRequest保留LEAN前置检查、购买力、brokerage、账户及成交回调；DailyOpenDecision标识限定同开盘先决策后执行的模型。内部DailyOpenBar仅含D+1开盘价，零周期、零成交量，不进入策略报价、bar回调或历史。开盘执行使用当时可见规则、现金、可卖量、保护价、涨跌停、费用和声明滑点，禁止读取D+1高低收量，也不用全天量或分钟参与率限制开盘成交。09:30:01内部时钟撤销未成交余量，不代表一分钟行情。尾日日线没有下一冻结session时不会额外延长研究窗口。

15:00取消DAY挂单，交付after_trading，随后记录账户及公司行动登记。日线在当日收盘尚不可见当天Close，账户按当时最新可见市场标记估值。daily.jsonl与account.json的 `equity_semantics="latest_visible_mark_not_session_close"`明确这不是通常意义的当日收盘净值；`valuation_at`为记录时点，`valuation_marks`逐证券保存price、source、price_source_at、price_available_at、is_current_session和is_current_session_close。source为session_open、completed_daily_close、corporate_action_reference或completed_minute_close。当前日有Open时使用当日Open；没有Open时使用刚可见的前日Close，并分别记录原收盘时点与本次可见时点；跨除权则按该历史价格的原时点处理，形成明确的公司行动参考价。持仓条目同样保存valuation_mark。分钟收盘记录使用 `equity_semantics="session_close_mark"`，其价格来源为已完成收盘分钟。

期末保留实际持仓。无行情证券使用内部session边界参与同步，不制造价格或填充bar。每次成功回调的trace保存事件、报价、历史请求、完整response、state和回调前order_event_count；订单同名计数连续递增。同步反馈在当前调用结束后顺序交付。

回调account.corporate_entitlements保存cash_receivable、pending_bonus_value、pending_bonus_quantity、locked_bonus_value及additional_equity。存在待到账或锁定股份时，即使原股已经卖出，也封存对应证券的可见报价。权益估值使用当前回调时点，不提前推进收盘登记；除权形成的零周期参考价不能作为完成分钟报价交付。

桥接检查declaration的 `max_state_bytes`、`max_response_bytes`、`max_context_bytes`、`max_intents_per_event`；字节按UTF-8 JSON计量。context计入声明、参数、event、可见报价和历史请求。每次实际调用策略前后，向持续打开的 `callback-events.jsonl`追加完整记录并flush：

```jsonl
{"status":"started","pid":1234,"event_seq":1,"kind":"initialize","started_at_unix":1793260800.0,"wall_seconds":60}
{"status":"finished","event_seq":1}
```

`started_at_unix=time.time()`，`wall_seconds=declaration.limits.callback_wall_seconds`。回调同步执行，返回或抛异常后追加结束记录。主运行时增量读取完整行，按未结束回调的时间和限额终止超时进程。

run 输出 `strategy-trace.jsonl`、`intents.jsonl`、`orders.jsonl`、`daily.jsonl`、`account.json`、`strategy-state.json`、`parquet-read.json` 和 `engine-status.json`。成功标志为：

```json
{"status":"completed","host_finished":true,"event_seq":123,"execution_model":"lean_next_completed_close_v1","artifact_type":"research.lean-simulation.v1","error":null}
```

该文件仅在最终 state 写好、全部冻结交易日日终完成且 Host.Finish 正常返回后发布。失败写 `status=failed`；进程被终止可能仍保留 running 和未结束的回调记录。主运行时必须另行核对 Launcher 退出、引擎终态和完整证据，不能只凭进程退出码发布 Result。

## 验证与临时文件

本目录负责可复用安装资产。构建验收覆盖四个 C# 项目的 Release 编译与 Python 语法，不代表正式 ResearchPackage 的金融验收；正式执行和 Result 发布由主运行时完成。安装不运行策略，不访问数据库。

`test_visible_market.py`检查字段投影、历史窗口、未来数据隔离、上下文预算与回调日志。`tests/EngineContracts.csproj`使用内存案例检查TCP帧、共享日缓存、可见规则、日线开盘及分钟容量。`tests/test_direct_launcher.py`使用合成TCP生产者验证真实Launcher的日线/分钟订单、尾日日线不回调、缺行情证券同步、历史窗口、持仓估值和现金红利；依赖路径通过QW_ENGINE_TEST_INSTALLATION与QW_ENGINE_TEST_BUILD显式提供，pytest的--basetemp必须指向仓库外独立目录。测试复制Launcher到该目录，现有安装仅供只读依赖。

`rp-integration/bin`、`obj`、`cache`、`logs`、`temp` 是可重建构建产物；assets、算法、模板、metadata 和 installation.json 为安装资产，需由准入计划冻结。运行中的 `.json.tmp` 为小型原子发布暂存文件，成功替换后消失；`callback-events.jsonl` 是运行监管日志。仓库目录不存 bin、obj、数据或缓存。

## 已知限制

日线开盘模型没有开盘竞价成交量，因而不提供容量估计；资金、数量格点、T+1、涨跌停和保护价约束仍生效。日线权益基于最新可见价格标记，不能解释为当日收盘净值。报告和指标消费者须保留equity_semantics及价格来源时点。




