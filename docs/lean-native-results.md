# 原生LEAN正式结果

原生算法直接使用LEAN订单、账户和算法对象。正式输出为`research.lean-simulation.v4`，八张执行表使用v4，金融指标继续使用`research.lean.metrics.v2`。

| 表 | 记录内容 |
| --- | --- |
| orders | 原生订单状态事件、订单类型、有效期、申报数量、限价及实际成交 |
| intents | 未声明策略意图时为空，不从原生订单反推意图 |
| daily | 声明交易窗口终点的现金、权益、估值时间和价格来源 |
| trace | 原生订单提交、更新、撤销请求及其处理响应、订单事件和算法生命周期 |
| fills | 原生真实成交；期货按开平成交腿保存 |
| positions | 原生持仓、可卖量及锁定股份；期货保留今昨仓方向桶 |
| account_events | 交易、权益和账户观察后的真实账户事实 |
| lifecycle | 有登记权益或账户影响的公司行动、退市以及期货合约和风险生命周期 |

trace 保留请求时间、请求状态和响应变化，包括预交易拒绝、终态后改单和撤单、无效订单号以及重复撤单。是否被拒绝以 `response_is_error` 为准。费用和交易次数来自实际成交，订单次数按真实 OrderId 去重。

公司行动登记数量为零且对账户无影响时，该阶段不进入正式 lifecycle。有登记权益的登记、除权、应收、到账及可卖阶段均保留。零登记后发生的除权如果影响当时持仓估值、股份应收估值或开放委托，同样保留。

正式表使用 Parquet 业务列。交易日期、事件时间、数量、价格和金额可直接用 SQL 选择、过滤及汇总。现货日终持仓按 `instrument_id` 关联同一行的 `valuation_marks`，每只证券的估值来源只保存一次。分钟与日线都使用这组结果列。文件路径可从正式 Result 的 `result.json` 中按表的 `files` 取得。

费用按分保存。持仓成本、浮盈亏和账户权益等计算结果根据整列的实际整数位和小数位以 Decimal128 无损保存。若同一列同时含有大额和极小尾数，所需总位数超过 38 位，该列使用 Decimal256。DuckDB 1.5.5 将这种宽列映射为 DOUBLE，精确对账时用 PyArrow 读取该列。其余列继续使用自身的类型。

```sql
SELECT trading_date, instrument_id, sum(quantity) AS net_quantity, sum(fee) AS fees
FROM read_parquet('tables/lean_fills/*.parquet')
GROUP BY trading_date, instrument_id
ORDER BY trading_date, instrument_id;
```

正式Result同时封存`lean/native-output/`下的LEAN结果、图表、运行配置和日志，以及`lean/storage/`中的ObjectStore、`lean/strategy-source/`中的算法源码。`lean/native-support.json`列出实际封存入口。原生算法终态写入`algorithm-state.json`，内容为算法身份与终态信息；持久算法数据使用实际ObjectStore，不构造另一套Python策略状态。

报告和Dashboard直接读取正式Result。复制或归档时保留Result及其实际依赖，不能仅复制HTML。历史Result保持只读，当前计划、安装与检查点按当前合同重新准入。
