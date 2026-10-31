# 读懂模型与策略报告

模型预测准不准，与按这个预测交易能不能得到预期结果，是两个不同问题。模型报告看预测与样本外评价；组合报告看净值、回撤、仓位、交易和费用。两类图表都从正式 Result读取数据。

## 参数与行为说明

HTML 报告从已封存 Result 的预测表或正式组合表生成。请求绑定实际 Result ID、表和窗口；报告不提高声明结论级别。读取只有列投影和有界批次，不访问数据库或重新训练模型。

```powershell
python -m research_pipeline report --result I:/research/results/<project>/<run>/<result> --result-store I:/research/results --format html --request I:/research/model-report.yaml --output I:/research/model-report.html
```

包级入口使用相同服务：`package report --package <研究包目录>` 加上上述报告参数。`markdown` 与 `json` 继续生成正式指标摘要，不接受图形 request。

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
- HTML 显示样本数、缺失数、MSE（仅原始收益预测）、平均 IC/Rank IC、Result 引用、声明限制与指标摘要。

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

表 ID 以实际 Result 为准。七表合计共享行数和内存预算，只投影展示必需的列，超限停止，不自动抽样。所需表通过与模型报告相同的 ResultStore 正式表与输入身份合同读取。

组合 HTML 包含：

- 含费用净值：直接使用正式估值表的 `nav_units / opening_cash_units`，初始资金参考线为 1。现金与估值已经包含费用，不再次扣除。
- 收盘回撤：当日收盘估值除以截至当日最高收盘估值，再减 1。与正式最大回撤指标相同，首个收盘会话为 0；首日相对初始资金的损益留在净值曲线中。
- 资金与持仓：正式净资产、总现金、可用现金和每日持仓市值。
- 交易费用：从正式费用表汇总每日和累计费用。
- 成交金额与订单终态：按实际买卖成交额和订单状态展示；双边成交金额不冒充换手率。
- 正式组合指标原值及其单位、样本起止和计算状态，逐日资金表、成交明细、订单明细、费用明细及期末持仓。

现金、估值、成交额和费用的 `*_units` 金额按分换算为人民币元。成交价单独使用 `execution_price_units / 10**price_scale`，支持三位小数的 ETF 价格。数量以股或份展示。正式指标保留原单位：`decimal_return` 为小数收益，`ratio` 为比率，`CNY` 为人民币元。

组合图使用现有 Plotly 依赖，图形文字显式使用微软雅黑，不重新调用 Qlib 回测、交易仿真或数据库。HTML 内嵌脚本一次，可离线打开；输出依旧不可覆盖。原 `qlib-research-report-v1` 模型请求保持兼容。

## 账户、资金流与信用报告数据

日频生产输出通过 simulation/portfolio-report.json 保存报告专用投影，合同为 research-daily-cash-report-v1。它与七张正式表属于同一个已提交仿真工件，由 ResultAssembler 封存；导出后仍可离线报告。

净资产、现金、持仓、成交、费用和正式指标直接读取金融表。额外投影只包含报告无法从这些表取得的字段：

- 期初净资产，区分期初现金与已有持仓、权益和信用负债。
- 账户日终负债与待登记权益，以及税款核定和扣收事件。
- 生产已算好的资金流调整收益序列与生效前后估值；入出金改变资产规模，不构成收益。
- 信用日终本金、利息、保证金和风险状态，以及利息确认、还本和付息的展示金额与协议引用。

投影不包含完整账户输入、税务book、原始行情、规则历史或事后验证上下文。报告不会从当前数据库补取缺失材料，不重新计算交易或金融账本。必需投影缺失、会话或工件身份不符时返回错误，不以现金起点代替权益或信用净资产。

投影和正式表共同受报告行数、内存预算约束。税款核定与扣收分别展示；利息确认与现金偿还分开，均不重复计入交易费用或扣减正式净资产。

## 已知限制

报告展示正式指标和声明限制；生成图形不代表预测有效或可交易收益。组合报告当前支持日频 CNY 现金组合，不补造没有封存的基准、超额收益或假设成本曲线。
