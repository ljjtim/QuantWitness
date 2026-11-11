# LEAN 期货结果与研究收益

读取期货及现货的正式执行表和研究收益。

## 开始前

- 输入：同一 simulation 工件的 8 张执行表及指标。
- 输出：收益、成交、保证金和真实估值时点。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 取得正式 Result 的实际表标识。
2. 按资产类型选择成交和持仓来源。
3. 读取日终账户及必要终态观察。
4. 按本页口径汇总指标。
5. 绑定 9 张表生成正式 HTML 报告。

当前执行工件为 `research.lean-simulation.v2`。股票、ETF和期货共用八张执行表，正式 Result 使用当前版本重新准入和发布。每张执行表的 Arrow schema 均为非空 `payload_json:string`，每行保存一个完整 JSON 事实。空表仍保存同一 schema。

| 表 | schema ID | 内容 |
| --- | --- | --- |
| orders | `research.lean.orders.v2` | 原生订单与订单事件，保留订单ID、状态、实际时间和数量 |
| intents | `research.lean.intents.v2` | 策略请求及其原生订单关联 |
| daily | `research.lean.daily.v2` | 每个交易日的实际账户快照、估值时间与已结算范围 |
| trace | `research.lean.trace.v2` | 策略回调、可见输入与策略状态 |
| fills | `research.lean.fills.v2` | 实际成交腿、今昨桶、费用、已实现盈亏和规则引用 |
| positions | `research.lean.positions.v2` | 多空方向×今昨桶的持仓快照 |
| account_events | `research.lean.account_events.v2` | 实际账户变化、原因、所属交易日和可见时间 |
| lifecycle | `research.lean.lifecycle.v2` | 资金预占与释放、结算、风险、换月及订单/合约/计划引用 |

八表来自同一策略节点的 `simulation` 端口和同一已提交工件，路径为 `lean/{表名}`。现货的 fills、positions 两张专用表为空。实际分笔成交读取 orders 中非零 quantity 的 Filled/PartiallyFilled 事件，持仓读取 daily.positions 及正式终态账户。

不能把空 fills 表解释为没有成交，也不能把空 positions 表解释为空仓。原生 trace 文件名为 `strategy-trace.jsonl`，其他表为 `{表名}.jsonl`。

现货 lifecycle 保存已确认退市后的零估值记录。account_events 可保存 terminal 尾部账户观察，不保证为空。开盘涨幅前两名2025-10-01至2026-09-30正式Result保存23条退市lifecycle记录，均为零持仓数量，account_events为0条。这两个数量是该Result的事实，不是所有现货运行的固定表形状。

指标表为 `research.lean.metrics.v2`，路径 `lean/metrics`，指标引用为 `project.lean.{指标名}@2.0.0`。指标表保持显式列 `metric_ref/value/unit/sample_start/sample_end/sample_size/status/definition_json`。指标定义和实际生产端口随计划冻结。

## 账户和成交字段

期货 daily 包含 `account_id`、`asset_class=cn_future`、`date`、`trading_date`、`cash`、`unrealized_pnl`、`equity`、`margin`、`frozen_funds`、`available_funds`、`valuation_at`、`settled_through` 和 `equity_semantics`。其中 C 为现金，U 为浮盈亏，E=C+U 为账户权益，M 为保证金。F 为活动委托冻结资金，A=E−M−F 为可用资金。

当前期货 daily 与终态账户使用 `equity_semantics=latest_visible_mark_not_session_close`。估值使用当时已可见的行情或结算事件。字段不能解读为同交易日收盘价净值。

fills 保存 `account_id/order_id/fill_id/leg_id/instrument_id/direction/bucket/position_effect/quantity/price/price_scale/multiplier/fee/realized_pnl/rule_reference/at/trading_date/event_sequence`。quantity 是正整数手。买卖与开平方向由 direction、position_effect 表达。

price 是报价数值，price_scale 保存价格精度，不能把 price 再按尺度除一次。名义成交额为 `quantity × price × multiplier`。

同一笔原生成交可拆成多个今昨仓腿：费用与名义金额按各腿相加。成交次数按唯一 fill_id 计，leg_id 必须唯一。订单数按原生订单ID计，不能从净持仓变化推算费用或成交数量。夜盘成交按显式 trading_date 归属，不按 at 的自然日截断。

account_events 保存本页的六个账户值和 `reason/available_at/trading_date/event_sequence`。positions 按 `instrument_id/direction/bucket/event_sequence` 保存当次真实持仓，direction 为 long/short，bucket 为 today/yesterday。净仓为零不抵消双向保证金。lifecycle 保留 kind、实际时间及 detail 中的订单、合约和计划引用。

现货 orders 的 quantity 是本次事件的带方向成交增量，price、fee 分别是实际成交价格和该次费用。同一 order_id 可有多次成交，随后以 Invalid 或 Canceled 结束余量。汇总同时依据累计非零成交和最终原因分类。分类包含 6 类：

- 部分成交后剩余含费预算不足。
- 完全未成交且预算不足。
- 历史规则拒单。
- 其他无效终止。
- 撤销。
- 到期撤销。

原生终态不撤销此前成交，不应把所有 Invalid 统称非法单。

## 期末结算观察与收益归属

daily、account_events 和 `lean/account.json` 保存真实发生时点，不改写历史资金。末交易日的结算可在下一实际会话才可见。这个仅结算观察尾段不增加收益样本交易日，也不开放策略和新成交。

研究消费者从末交易日的 settlement/terminal 账户事件或正式终态账户提取最终 E，构造研究权益投影：

- research_equity：研究收益使用的权益，末日包含观察尾段最终 E。
- research_valuation_at：该研究权益的真实估值时刻，可晚于收益归属日。
- research_margin、research_frozen_funds、research_available_funds、research_settled_through：同一研究估值时点的资金与已结算范围投影。

投影只供指标和报告消费，不改原始 equity、cash、available_funds、valuation_at 或 settled_through，不向策略交付。终态账户必须仍归属末交易日。终态文件与 terminal 事件权益冲突时拒绝消费。

现货的退市尾部观察使用独立terminal时钟。只推进已可见的生命周期和公司行动，不开放策略、新委托或成交。研究投影承接正式终态账户的research_equity、research_cash、research_valuation_at与估值来源，原daily不改写。

分钟尾部消费截止遵守end_at排他、as_of包含端点。日线日期范围的结束日包含当日。指标和报告使用同一研究权益及同时点现金，保留实际估值时间，观察尾段不增加交易日数。

例如，期初100000元，首日原日终E=100098，次日原日终E=100097，下一会话结算后E=100117。两日研究收益序列使用100098、100117，累计收益为0.117%。原次日现金100107、可用资金99990及其15:00时点保持原值，研究权益实际估值时间保存为次日09:30。

## 指标口径

收益、回撤、波动与夏普基于 E，包含首日收益。年化按252个交易日，夏普无风险利率为0，收益平坦时记0。期末观察不增加交易日数。

权益归零后，无外部注资的零权益日继续计入样本。存在保证金或冻结资金占用的零权益期货状态不能形成有效资金占比。

| 指标 | 期货口径 |
| --- | --- |
| fee_total | fills 的实际成交腿费用之和 |
| gross_traded_notional | 各成交腿正手数×价格×乘数之和 |
| fill_count | 唯一fill_id数量，多腿拆分不重复计次 |
| filled_order_count | 有实际成交腿的唯一order_id数量 |
| order_count | orders与成交腿所关联的唯一原生订单ID数量 |
| turnover_mean | 按trading_date汇总的双边名义成交额÷2÷当日期初研究权益，再求日均 |
| margin_utilization_mean | 日均M/E，使用对应研究估值时点的保证金与权益 |
| capital_utilization_mean | 日均(M+F)/E，包含委托资金冻结 |

现货沿用原生 orders 的实际成交事件，成交额为绝对成交数量×价格，capital_utilization_mean 继续表示日均非现金权益占比。现货没有期货保证金使用率指标。期货现有14项共同指标加保证金使用率，共15项。现货14项。

## 正式报告

HTML报告请求使用 `lean-research-report-v2`，显式绑定八表和metrics，预算覆盖读取总行数和字节。表ID以实际Result为准：

`integrations/lean/accept.py`按实际Result中的当前schema唯一匹配九张表，再生成请求，不依赖示例表ID。公开ETF、期货和Qlib组合示例的LEAN指标均引用`@2.0.0`。ETF手算脚本读取八张v2执行表，其合成案例的fills、positions、account_events、lifecycle为空，不构成所有现货Result的空表要求。

```json
{
  "contract_version": "lean-research-report-v2",
  "result_id": "实际Result ID",
  "tables": {
    "orders": "lean_orders",
    "intents": "lean_intents",
    "daily": "lean_daily",
    "trace": "lean_trace",
    "fills": "lean_fills",
    "positions": "lean_positions",
    "account_events": "lean_account_events",
    "lifecycle": "lean_lifecycle",
    "metrics": "lean_metrics"
  },
  "budget": {"max_rows": 100000, "memory_bytes": 268435456}
}
```

报告直接读取正式Result。报告展示研究权益净值、回撤、成交和费用。报告还展示保证金、可用资金、收益归属和真实估值时刻。四桶持仓、成交腿、结算、风险、换月和账户事件也进入报告。JSON和Markdown标准报告继续读取正式指标及其冻结定义。报告不会重跑策略、撮合或结算。

现货收益归因使用正式 orders、daily.positions、终态 account 和公司行动快照。计算公式为：

```text
净收益 = 卖出金额 - 买入金额 + 持仓市值变化 + 现金分红到账
       + 应收及待到账股份权益变化 - 显式费用
```
逐日、月度与证券贡献按同一恒等式汇总，尾部账户实际估值时间单独保存。扣显式费用前的贡献仍含实际成交价格中的滑点，不能称为无滑点收益。拒单与撤销只报告事实数量，不推算反事实机会损失。


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
