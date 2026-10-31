# 金融执行、账户与正式结果

本页说明 ResearchPackage 中股票和 ETF 策略的当前执行边界，以及正式 Result 中保存的金融事实。当前生产策略统一使用本地 LEAN：日线频率为 `1d`，分钟频率为 `1m`，声明 `execution_backend=lean`。股票和 ETF 不再通过旧的 `finance.simulation.daily-cash`、`finance.simulation.intraday` 或其他旧回测入口运行。

完整的安装、策略声明、准入、恢复和资源约束见[LEAN 执行](lean-execution.md)。研究主链仍是 ResearchPackage → 准入 → 执行 → Result；使用 `report`、`analysis` 或 `export-result` 时直接读取正式 Result，不从运行目录猜测账户事实。

## LEAN 执行

策略输入必须通过 Catalog、PIT 和交易范围准入。行情使用原始、已完成且合格的 bars；公司行动、交易日历、停牌、价格限制、T+1、费用和其他市场规则使用当时可见的冻结来源。策略只能读取模拟时钟前已经可见的行情、信号和账户状态。

日线使用 `lean_next_session_open_v1`：收盘后形成的信号在下一交易日开盘执行，日线独立开盘价来源必须在 Catalog 中声明。分钟使用 `lean_next_completed_close_v1`：策略在分钟完成后获得该 bar，成交按后续完成分钟 Close 加声明滑点。执行模型、频率和 bars 端口必须一致。

LEAN 负责事件驱动、订单、成交、账户和指标。项目策略通过冻结的 Python bundle 接收事件并返回交易意图；当前订单类型为 DAY 市价单，策略可以提交金额买入、数量买卖或撤单意图。股票价格按 `price_scale=2` 记账，ETF 按 `price_scale=3` 记账，现金统一按人民币分记账。

执行期间，系统检查可见性、后续完成 bar、滑点、费用、数量格点、保护价、T+1、可用现金、可卖数量和日终权益。现金红利按登记日持仓确认，到账后进入现金；送股和转增按股份到账及可卖时点处理。缺少必要规则、来源时间晚于使用时点或输入发生冲突时，节点失败关闭。

## 账户和金融规则

`research_pipeline.domain` 与 `research_pipeline.simulation` 仍保存金额、费用、订单、成交、结算、公司行动、现货账本和期货账本等纯金融类型。它们服务于历史 Result 的解释、公共事实合同和 LEAN 边界，类型存在不等于存在可运行的旧算子或 CLI 入口。

当前股票和 ETF 账户从 ResearchPackage 声明的期初现金和持仓开始。账户状态区分现金、持仓、可卖数量、活动订单和预占金额；账户估值记录 `equity_semantics`、`valuation_at`、价格来源和可见时间。日线估值优先使用当日可见 Open；缺少 Open 时使用已知历史价格，并保留估值来源。分钟结果使用已完成行情进行日终估值。

费用和滑点必须由 `research_cost_assumption` 显式声明币种、单位、数值和适用区间。该假设不是历史真实费率；研究窗口未被完整覆盖时，准入失败。交易规则、公司行动和费用分开记录，不能用费用假设代替市场规则来源。

所有信号、规则、公司行动和估值均按可见时间使用。开盘成交不能使用当日收盘价或全天成交量；分钟决策不能使用未完成的 bar；连续合约只可作为历史信号来源，不能直接成为成交标的。

## LEAN 正式输出

LEAN simulation 端口的类型为 `research.lean-simulation.v1`，Result 至少保存以下执行表：

| 路径 | 内容 |
| --- | --- |
| `lean/orders` | 订单事件、成交、费用及对应账户变化 |
| `lean/intents` | 策略交易意图与实际提交数量 |
| `lean/daily` | 逐交易日日终账户与持仓 |
| `lean/trace` | 实际回调、可见报价、应答和状态 |
| `lean/metrics` | 金融指标、定义、单位及样本范围 |

同时保存 `lean/account.json`、`lean/strategy-state.json`、`lean/runtime-lock.json`、`lean/strategy-input.json`、引擎终态和 Launcher 摘要。行情、规则、公司行动和信号以计划引用或持久工件保留；归档和迁移时必须保留这些依赖。

报告使用正式 Result 的订单、成交、日终账户、指标和配置引用。HTML 报告可通过：

```powershell
python -m research_pipeline report --result '<Result目录>' `
  --result-store '<Result存储根>' --format html --request '<报告请求JSON>' `
  --output '<报告HTML>' --summary
```

旧 `finance.simulation.*` 参数、旧 daily-cash/futures-daily CLI 和旧引擎开关不属于当前用户支持面。历史 Result 继续只读保存，不能用旧入口重跑或覆盖。

## 当前限制

- 当前策略执行范围是已准入的中国股票和 ETF 日线、分钟 LEAN 会话。
- 结果是 bar-level research；不表示 tick、盘口、实盘成交质量或全市场覆盖。
- LEAN 节点失败时按节点重新执行；不能接续跨进程的 LEAN 内部状态。
- 期货历史数据、连续合约换月、共享资金和多品种组合的来源语义见本目录对应历史文档，不构成当前可运行的期货策略入口。
