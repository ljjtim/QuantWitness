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

## 独立复核

```powershell
python research_pipeline/tools/bar_tca_oracle.py `
  --artifact-root <simulation-tca-dir> `
  --input-json <simulation-tca-dir>/oracle-input.json `
  --output <oracle-result.json>
```

oracle 不导入生产 TCA 实现。它从正式 orders/fills、决策基准、policy 和 ledger 身份独立重建逐笔
观察实现差额，并比较 source fill 集合、数量、价格、费用及汇总。新增、删除、重复或修改正式成交映射
都会失败。

进入 Evidence v3 的 Bar TCA 结果必须在同一 ResultSpec 中同时公开 canonical
`orders/fills/positions/cash/costs/valuations` 六表和 TCA `orders/fills/daily/research` 四表。
独立 verifier 从显式的自包含 ResultStore 读取实际 Parquet、manifest、policy 和 oracle input，
先复验字节、Arrow schema、行数与 lineage，再独立检查现金、持仓、费用、NAV
守恒及 source fill 一一对应。只写一个形似 SHA-256 的值、`reconciliation_delta_units=0` 或
`status=pass` 都不能通过。

## 能力边界

本次修复关闭了第二套成交事实与期货开盘价回退，但没有提供 Tick/LOB、日内容量预测、实盘路由或
外部信任域验收。能力状态仍以 `capabilities.json` 为准，不得从本地测试外推为 sealed TCA。
