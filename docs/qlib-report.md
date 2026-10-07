# Qlib 模型与组合研究报告

HTML 报告从已封存 Result 的预测表或正式组合表生成。必须提供与 Result 关联的合法 VerificationResult；报告成功不改变研究验证状态。读取只有列投影和有界批次，不访问数据库或重新训练模型。

```powershell
python -m research_pipeline report --verification-result I:/research/verification.json --result-store I:/research/results --format html --request I:/research/model-report.yaml --output I:/research/model-report.html
```

包级入口使用相同服务：`package report --package <研究包目录>` 加上上述报告参数。`markdown` 与 `json` 继续生成验证摘要，不接受图形 request。

## 模型请求

```yaml
contract_version: qlib-research-report-v1
result_id: <实际 Result ID>
table_id: <实际预测表 table_id>
selection:
  candidate_id: ridge
  stage: validation
  fold_id: fold-1
  horizon_sessions: 1
window:
  start: '2020-01-01'
  end: '2020-03-31'
columns:
  instrument: entity_id
  datetime: observation_session
  score: prediction
  label: actual
method:
  graphs: [group_return, pred_ic, pred_autocorr]
  groups: 5
  ic_methods: [IC, Rank IC]
  lag: 1
  reverse: false
budget:
  max_rows: 100000
  memory_bytes: 268435456
```

选择范围同时约束候选、阶段、fold 和预测周期。单个报告不能混入其他候选或周期。`stage: test` 配合 `fold_id: '*'` 只用于 selection 已产生的唯一滚动输出；重复证券日期仍报错，并列出来源，不采用最后一条覆盖。

预测表使用统一模型 v2 字段，其中 `observation_session` 必须是 Arrow date32，`score_semantics` 明确 `raw_return_prediction` 或 `ranking_score`。`actual` 始终是原始未排名标签。报告使用 instrument/datetime MultiIndex；排名分数不计算原始收益 MSE。

## 图形方法

- 分组收益使用 Qlib 的截面排序、每组 floor(n/N) 和算术累计方法。余数样本不进入分组，实际数量随报告列出。这是诊断曲线，不是复利资金净值。
- IC/Rank IC 分别使用 Pearson 和 Spearman。Qlib 接口显式接收 `methods=(IC, Rank IC)`。
- 预测自相关按证券对分数平移 lag 个观察值，计算截面排名相关。
- 样本不足、常量分数、常量标签导致的未定义统计保留为缺失，并显示对应会话数。常量 IC 或单日结果仍可展示有效统计，省略无法拟合的分布图。
- HTML 显示样本数、缺失数、MSE（仅原始收益预测）、平均 IC/Rank IC、Result/Verification 引用和验证摘要。

结果表按需要的列分批读取；筛选后先检查行数和内存预算，再转换 pandas。预算保留 Arrow、pandas 和图形计算的工作空间。超限要求缩小窗口或增大预算，不自动抽样。

## 文件与依赖

输出是单个 HTML，内嵌 Plotly 脚本一次，可以离线打开。采用排他原子写入，已有路径不覆盖，也不写回封存 Result。模型图使用 Qlib、Plotly、statsmodels、matplotlib；由项目的 ML 可选依赖提供。

## 组合请求

组合报告沿用同一 `report --format html --request` 与 `package report` 入口。请求显式绑定日频现金仿真的六张规范表和组合指标表，七表必须来自同一节点与输出端口，并对应唯一组合。报告保留完整样本窗口，不筛选部分交易日后套用全样本指标。

```yaml
contract_version: qlib-portfolio-report-v1
result_id: <实际 Result ID>
portfolio_id: default
tables:
  cash: canonical_cash
  valuations: canonical_valuations
  orders: canonical_orders
  fills: canonical_fills
  positions: canonical_positions
  costs: canonical_costs
  metrics: portfolio_metrics
budget:
  max_rows: 100000
  memory_bytes: 268435456
```

表 ID 以实际 Result 为准。七表合计共享行数和内存预算，只投影展示必需的列，超限停止，不自动抽样。所需表通过与模型报告相同的 ResultStore 完整性和 VerificationResult 绑定门禁读取。

组合 HTML 包含：

- 含费用净值：直接使用正式估值表的 `nav_units / opening_cash_units`，初始资金参考线为 1。现金与估值已经包含费用，不再次扣除。
- 收盘回撤：当日收盘估值除以截至当日最高收盘估值，再减 1。与正式最大回撤指标相同，首个收盘会话为 0；首日相对初始资金的损益留在净值曲线中。
- 资金与持仓：正式净资产、总现金、可用现金和每日持仓市值。
- 交易费用：从正式费用表汇总每日和累计费用。
- 成交金额与订单终态：按实际买卖成交额和订单状态展示；双边成交金额不冒充换手率。
- 正式组合指标原值及其单位、样本起止和计算状态，逐日资金表、成交明细、订单明细、费用明细及期末持仓。

现金、估值、成交额和费用的 `*_units` 金额按分换算为人民币元。成交价单独使用 `execution_price_units / 10**price_scale`，支持三位小数的 ETF 价格。数量以股或份展示。正式指标保留原单位：`decimal_return` 为小数收益，`ratio` 为比率，`CNY` 为人民币元。

组合图使用现有 Plotly 依赖，图形文字显式使用微软雅黑，不重新调用 Qlib 回测、交易仿真或数据库。HTML 内嵌脚本一次，可离线打开；输出依旧不可覆盖。原 `qlib-research-report-v1` 模型请求保持兼容。

## 已知限制

报告展示实际 VerificationResult 的验证状态，允许展示身份合法的 `fail` 结果用于诊断；生成图形不改变验证范围，也不代表投资有效。组合报告当前支持日频 CNY 现金组合，不补造没有封存的基准、超额收益或假设成本曲线。
