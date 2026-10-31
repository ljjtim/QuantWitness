# LEAN 日线与一分钟策略执行

项目策略声明 `execution_backend=lean`，在正式 ResearchPackage 主链中使用本地 LEAN。频率为 `1d` 或 `1m`。数据经过同一 Catalog、PIT 与交易范围准入，选股继续由已准入的因果 Feature 产生。LEAN 负责账户、订单和撮合；项目策略通过冻结的 Python bundle 接收事件并返回交易意图。

安装与完整包转换命令见[LEAN 接入](../integrations/lean/README.md)。外部插件、宿主和安装脚本位于 `integrations/lean/engine/`，使用固定的本地 LEAN 安装；运行不依赖 Docker。

Windows x64 和 PowerShell 7 的首次安装可从[合成回测示例](../examples/lean_quickstart/README.md)开始，先生成策略包与 Parquet 输入，再安装并执行。使用完整公开源码或解压的 sdist；wheel 只提供 Python 框架。`run.ps1` 的 `DataDb` 与 `InputSnapshotManifest` 必须二选一，归档分支不连接数据库。

## 声明与输入

策略节点同时声明 `execution_backend=lean`、`claim_ceiling=bar_level_research_only` 和完整 `lean_runtime`。一分钟使用 `execution_model=lean_next_completed_close_v1`，日线使用 `lean_next_session_open_v1`，声明频率须与 bars 端口一致。策略声明的 `bundle_id` 必须对应已准入 bundle，输出端口为 `simulation: research.lean-simulation.v1`。

人民币股票使用 `asset_class=cn_stock`、`contract_kind=stock`、`price_scale=2`；ETF 使用 `asset_class=cn_etf`、`contract_kind=etf`、`price_scale=3`。现金金额均按人民币分记账。ETF 的数量、结算和费用来自冻结的 `cn_fund` 规则；日线与独立开盘来源须在所用 Catalog 中声明并准入。

输入为正式 `bars`、逐证券 `snapshot` 和可选 `signals`。行情必须原始未复权、已完成且质量合格。一分钟在分钟结束时可见；日线按 Catalog 的 `next_session_open` 规则在下一交易日开盘可见。日线直接绑定列式数据请求，以 `bars_request_id` 指定 OHLCV 来源，以 `open_request_id` 指定同证券、同日期的 `session_open` 开盘价来源；两者由同一 data bundle 交接。历史规则来自包内冻结的 `minute_scope`，公司行动来自正式快照。每条信号保留核心可见时间，同一输入只交付当时最新的完整批次。

行情、逐日规则和当日信号通过本机日数据通道交给引擎。当前交易日执行时，单个读取线程预读下一交易日；预读数据不进入策略当前上下文。分钟月文件按证券、时间和字段裁剪，每月只扫描一次并在内存中按天组织，已消费日期释放，跨月释放旧月。内存超过声明额度时停止，不生成磁盘交换副本。价格按不同值进行十进制 half-up 转换后按原索引展开。节点摘要记录输入准备、引擎执行及提交前结果写出耗时，完整流程验收还计入准入、正式提交、Result 封存与报告。

接口接受一个 OHLCV 子集、`lookback_bars>=1` 的行情绑定及至多一个 `lookback_bars=1` 的信号绑定。历史窗口仅包含模拟时钟之前已可见的完整周期。订阅可包含初始化、盘前、完成周期、订单反馈与盘后事件。策略可以按需读取冻结证券集合内的当时可见报价，实际读取的报价计入上下文额度。策略读取只读账户，提交金额买入、数量买卖或撤单意图；订单为 DAY 市价单。

## 执行与恢复

Workspace execute 合并准备与准入后运行并报告；独立 package lint 保留为主动诊断入口。

项目策略只接受显式声明的 `execution_backend=lean`。历史 RP 策略 Result 保持只读；当前研究使用已准入 LEAN 会话。

独立命令流程使用 `package admit → run → report`。安装锁将实际引擎、插件、策略宿主、配置模板、市场元数据及 .NET/Python 执行依赖的内容身份放入节点参数。正式计划加载和执行前复验这些文件，文件缺失、增加或内容变化均拒绝使用原计划。

已有 LEAN 声明的准备阶段只检查安装锁结构及安装根绑定，不逐个访问安装文件。实际内容检查在 LEAN 节点启动前、跨进程计划加载及恢复时执行：一次调用内复用已解析路径，枚举完整执行闭包并读取全部内容，链接核对实际目标，不跨调用沿用内容验证结论。

节点至少预留三个进程槽：Supervisor、固定启动宿主和 LEAN；Windows 虚拟环境还须为 Python 启动进程增加一个槽。整个进程树受声明的内存、临时空间、CPU、累计时间及单次策略回调额度约束。Windows Job 在宿主退出时清理后代。引擎退出码、正常完成状态和正式输出均满足后，节点才提交工件。

成功节点沿用现有 checkpoint 恢复合同。中断或失败的 LEAN 节点从该节点重新执行，先前成功的数据与选股节点可按原身份复用。修改引擎安装或桥接实现后须重新准入，不能继续使用旧成功检查点。

切换后运行故障先查看run目录的节点诊断和LEAN日志。需要撤回版本时，使用独立安装目录恢复已保存的框架与引擎版本，并以该版本重新准入到新运行目录；不能手改原计划、覆盖历史Result或混用不同版本checkpoint。当前版本没有旧项目策略执行开关。

## 结果与金融计算

正式 Result 保存订单、交易意图、日终账户、必要的策略决策和状态、金融指标，以及配置、代码、安装身份和输入版本引用。输入行情与来源保留持久位置；迁移时一并携带引用依赖。报告直接读取正式表。

日线按最新已可见价格估值：当日 Open 可用时使用 Open，缺 Open 时使用已可见历史价格，除权参考价沿用公司行动规则。每日账户及最终账户保存 `equity_semantics`、`valuation_at` 和逐证券 `valuation_marks`，记录价格来源和可见时间；该序列不是当日收盘净值。日线指标定义使用 `daily_latest_visible_mark`，报告注明相同口径。一分钟使用已完成行情的日终估值。

LEAN的simulation端口交付四张执行表和一张指标表：

| 表目录 | 内容 |
| --- | --- |
| `lean/orders` | 订单事件、成交、费用及对应账户变化 |
| `lean/intents` | 策略交易意图与实际提交数量 |
| `lean/daily` | 逐交易日日终账户与持仓 |
| `lean/trace` | 实际回调、可见报价、应答和状态 |
| `lean/metrics` | 金融指标、定义、单位及样本范围 |

支持文件包括`lean/account.json`、`lean/strategy-state.json`、`lean/runtime-lock.json`、`lean/strategy-input.json`，以及引擎终态与Launcher摘要。行情、历史规则、公司行动和信号输入通过持久工件与计划引用保留；迁移、归档或清理时同时保留这些依赖。项目绑定signals时，ResultSpec继续选择实际Feature producer的正式源表，该表属于上游输出。

输入准备检查正式 PIT 快照并保存小型来源清单、策略输入、公司行动及市场元数据。行情与逐日规则在执行时读取和传输；必要金融事实按执行需要投影，来源身份保留在原准入输入及持久工件中。

研究DAG由实际数据、模型或策略计算节点组成，成功后发布Result。模型最终评价继续记录持久holdout账本，实际读取前登记消费；报告直接使用正式指标和执行表。

LEAN 与冻结的中国市场规则在下单、成交及账本转换处检查信号和行情可见性、后续分钟成交、滑点、费用、参与率、数量格点、保护价、T+1、可用现金及日终权益。回调账户区分持仓、可卖数量、活动订单与扣除金额买单预占后的现金。

现金红利按登记日持仓确认权益，付款后进入现金；整数送转分别按股份到账和可卖时点处理。应收红利、待到账和锁定股份使用当时可见价格估值。开发验收用手算案例检查登记后卖出、跨日到账、费用和购买力边界。

金融指标包含收益、回撤、波动、夏普、换手、费用及交易数量。收益含首日相对期初资金的变化，回撤使用含期初资金的运行峰值；年化使用252个交易日，夏普采用零无风险利率。定义、单位和样本随 Result 保存，算法更改使用确定性案例验收。

HTML报告展示净值、回撤、成交和费用。将正式Result ID与表ID写入请求文件：

```json
{
  "contract_version": "lean-research-report-v1",
  "result_id": "<正式Result ID>",
  "tables": {"daily": "lean_daily", "orders": "lean_orders", "metrics": "lean_metrics"},
  "budget": {"max_rows": 100000, "memory_bytes": 268435456}
}
```

```powershell
python -m research_pipeline report --result '<Result目录>' `
  --result-store '<Result存储根>' --format html --request '<报告请求JSON>' `
  --output '<报告HTML>' --summary
```

Markdown和JSON报告沿用同一正式指标入口，不需要HTML请求文件。

## 已知限制

- 一分钟按后续完成分钟 Close 加声明滑点成交；日线按下一交易日已可见开盘价成交。日线账户估值记录价格来源和时间，不等同于当日收盘净值。
- 当前支持人民币股票与 ETF 的日线和一分钟；不接受其他周期或定时事件。行情历史窗口必须显式声明，订单范围为 DAY 市价单。
- 不保存可跨进程接续的 LEAN 内部状态；节点失败时重新执行整个节点。
- 公司行动支持毛额现金分红和整数送转；个人红利税、非整数股份分配及其他行动类型需要独立合同。
- 日行情与逐日规则通过内存数据流交接。日志和构建缓存按生命周期规则处理；正式 Result、模型、holdout 账本、冻结声明与恢复工件须保留。直接来源运行依赖原数据库和行情文件。
