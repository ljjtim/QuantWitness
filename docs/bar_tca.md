# 交易成本：收益花在了哪里

策略想成交的价格与实际模拟成交价格可能不同。交易成本分析把正式成交对应的费用、滑点等影响分开，让你知道差异来自哪里。它分析已有成交，不再运行第二套撮合器。

## 参数与行为说明

Bar TCA 是正式仿真成交的诊断归因，不是第二个成交引擎。它支持股票、ETF 和期货的日频正式成交；
分钟路径只有在能提供逐笔正式 fill 与 ledger 身份后才能启用。当前能力保持 `local_only`。

## 唯一成交事实

日频 TCA 通过 `execute_simulation_result_bar_tca()` 只消费已复验的公共 `SimulationResultContract` orders/fills、显式决策基准、费用以及 ledger/settlement 身份。历史原始现金表与期货结算表的测试转译入口不属于发行包：

- TCA 不决定成交数量、成交时点、成交价格、拒单、顺延或强平；
- 每条 TCA fill 必须一一绑定 `source_fill_id`、`source_fill_hash` 和正式 ledger hash；
- 股票 block、ETF 和期货都从 `simulation/result-contract/orders|fills`（block 为 `result-contract/`）读取，不再按模拟器表名分支；
- 正式费用直接来自成交账本，TCA policy 不能再生成另一份“正式费用”。

分钟仿真现在直接输出公共六表；510300 ETF 与 AG2406 冻结参考包的 TCA 均逐笔绑定正式 fill、
账本、决策基准和已完成执行 bar 容量。缺少其中任一直接输入时仍失败关闭，不能退回 Bar 重模拟成交。

## 时间可见性

- decision benchmark 必须在订单 decision time 之前可见；
- ETF 日频使用决策时已知的前收基准，不读取当日 close 或全天 volume；
- 期货只使用决策时已经发布的最近结算价。缺失、重复或发布晚于决策时直接失败，绝不回退当日 open；
- 当日 close 只能用于明确标记的事后观察，不能改变正式 fill 或实现差额。

日频全天 volume 通常收盘后才完整，因此不能冒充开盘容量。只有显式容量值及其 `available_at`
不晚于 fill time 时，才能计算 participation 和 modeled impact。

## 输出

四层 Parquet 工件继续是：

- `tca/orders`：正式订单状态、正式成交数量和观察成本汇总；
- `tca/fills`：逐笔 source fill lineage、正式价格、正式费用和观察实现差额；
- `tca/daily`：按成交日汇总；
- `tca/research`：窗口汇总与 source simulation/ledger/fill manifest 身份。

观察实现差额为：

```text
有符号(正式成交价 - 决策基准) × 正式数量 × 合约乘数 + 正式费用
```

价格整数按 policy 的 `price_scale` 解释。日频的现金与费用单位统一为人民币分：价差乘总数量和合约乘数后换算到分，观察价差按四舍五入、模型价差成本按向上取整，再与正式费用相加。分钟沿用该路径原有的价格整数单位。旧日频两位报价的数值保持一致；使用其他价格精度的日频输入同样必须把正式费用声明为分，不能按报价整数单位填写。

无可见容量时，`liquidity_attribution_status=not_computable`，participation、modeled spread 和
modeled impact 为 null，不能用 0 冒充已计算；结论上限自动收紧到 `analysis_only`。

## 正式结果与开发验收

TCA 消费规范订单、成交、持仓、现金、费用、估值，以及声明的基准和政策，不二次撮合。ResultSpec 选择正式表，报告直接读取 Result。生产路径检查 source fill 对应关系、金额单位及费用口径。

日频与分钟 TCA 工件保存 orders、fills、daily、research 四张正式表，以及 manifest.json 和 COMMITTED。清单记录政策、输入和来源身份、各表行数与内容摘要。Result 随正式表封存仿真及 TCA 的清单和提交标记；决策基准及成交观察在生产归因时消费。

开发用例用手算订单和成交检查滑点、实施差额、参与率与聚合，覆盖零成交、部分成交、缺失基准和不同价格精度。核对失败修对应生产实现，不以状态字符串代替数值案例。

项目通过 ResultSpec 选择账本与 TCA 表，按实际使用范围保留订单、成交、持仓、现金、费用、
估值和成本归因。开发测试将 source fill、数量、价格、费用与手算对照，覆盖新增、删除、
重复和错误关联。研究报告使用实际生产输出及声明的单位和样本范围。

## 能力边界

当前范围不包含 Tick/LOB、日内容量预测或实盘路由。能力状态以 `capabilities.json` 为准，
本地开发测试不构成全市场或实盘成交认证。
