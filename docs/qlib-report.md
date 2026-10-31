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

## 组合与策略请求

Qlib组合和项目策略统一使用LEAN结果报告。请求通过同一 `report --format html --request` 入口绑定正式 `daily`、`orders`、`metrics` 三表，三表必须来自同一simulation工件。

```yaml
contract_version: lean-research-report-v1
result_id: <实际 Result ID>
tables:
  daily: <实际 daily table_id>
  orders: <实际 orders table_id>
  metrics: <实际 metrics table_id>
budget:
  max_rows: 100000
  memory_bytes: 268435456
```

表ID以实际Result为准。三表合计共享行数和内存预算；超过预算时停止，不自动抽样。报告从已封存表读取结果，不连接数据库、不执行交易或重新训练。

HTML展示收益与回撤、资金使用、双边成交额、费用、逐日账户和订单明细。正式指标保留实际单位、样本区间和计算状态；成交费用已计入账户权益，不重复扣除。报告使用实际初始权益和逐日权益展示净值。

LEAN日线与一分钟的执行时点、资金及交易规则见[LEAN执行](lean-execution.md)。报告输出为单个可离线打开的HTML，已有文件不覆盖。

## 已知限制

报告展示正式指标和声明限制；生成图形不代表预测有效或可交易收益。报告不补造没有封存的基准、超额收益或假设成本曲线。框架只读取当前合同的Result和报告请求。
