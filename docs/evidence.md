# 读懂 Result 与研究结论

读取正式结果，并判断结果能支持什么结论。

## 开始前

- 输入：已经原子发布的正式 Result。
- 输出：报告、比较、分析结果或导出的 Result 副本。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 取得正式 Result 目录。
2. 用 `load_result_context` 或命令行加载。
3. 按实际表和窗口消费结果。
4. 结合指标定义、样本和限制解释结论。
5. 保存 Result 和实际输入依赖。

Result 保存一次研究的正式计算输出、运行摘要、配置、代码和输入版本引用。报告、比较、分析、导出和 AI 开发反馈直接消费它。

## Result 保存什么

- 研究包与计划身份、固定时点、种子、实现及依赖身份。
- 正式表、字段、单位、样本范围、指标定义和计算状态。
- 实际需要的模型文件、策略状态、订单、成交、资金、持仓、费用与公司行动记录。
- 输入来源和持久位置，以及恢复所需的引用。
- 模型开发范围、选择结果及最终留出消费账本。

LEAN项目的正式执行表为 orders、intents、daily、trace、fills、positions、account_events 和 lifecycle。金融指标单独保存在metrics。行情、历史规则、公司行动和信号输入使用持久工件与计划引用。

结果同时保留实际需要的账户、策略state、运行锁与策略输入。报告读取所需正式表，归档携带对应引用依赖。

研究数据库只读。Result 原子发布后保持不可变，不能编辑表或状态来改变研究结论。恢复使用原计划与合格 checkpoint。报告从 Result 读取所需表，不重跑交易或训练。

## 成功与结论级别

Runtime 节点成功后还需正式发布 Result。`run` 返回 `result_finalized`，Workspace 完整流程成功返回 `status=succeeded` 与 `execution_status=succeeded`。报告失败保留已发布的 Result，单独重新生成报告即可。

`claim_level` 和 `limitations` 是结果中的声明事实。`local_only` 等能力状态表示已记录的适用范围，不能仅凭命令成功、图表生成或某个收益数字扩大范围。

研究节点直接生成正式表与指标，运行状态描述执行和发布是否成功。数据可见性、标签、资金规则及holdout在相应消费者处处理。研究结论依据声明、实际样本和生产结果解释。

## 直接消费接口

```python
from research_pipeline.results import (
    ResultContext,
    load_result_context,
    build_result_report,
    render_result_report,
    compare_results,
    export_result,
)

context = load_result_context(result_directory, result_store=result_store)
markdown = render_result_report(context)
report = build_result_report(context).to_dict()
```

`load_result_context(result, *, result_store=None, additional_table_ids=())` 接收 Result 目录或正式清单路径。额外表通过 table_id 显式指定。`ResultContext` 包含 `snapshot`、`metrics`、`claim_level`、`limitations`、`research_cost_assumption`。运行摘要位于 `context.snapshot.bundle.run`。

`ResultSnapshot.iter_table_batches(schema_id, *, columns=None, batch_size=8192, filters=())` 按文件清单顺序返回 Arrow RecordBatch。`columns` 只投影返回列。`filters` 为 `DatasetFilter` 元组，多项条件取交集，筛选字段可以不在返回列中，但必须存在于该表。

`filters=()` 保留无过滤读取，`columns=None` 返回全部列。`batch_size` 必须为正整数，每批行数不超过它。空表或没有命中行时总返回行数为零，迭代中可能出现空批次。

```python
from datetime import date
from research_pipeline.results import DatasetFilter

context = load_result_context(
    result_directory, result_store=result_store,
    additional_table_ids=("predictions",),
)
for batch in context.snapshot.iter_table_batches(
    "research.model-predictions.v2",
    columns=("entity_id", "observation_session", "prediction", "actual"),
    batch_size=4096,
    filters=(
        DatasetFilter("candidate_id", "eq", "ridge"),
        DatasetFilter("stage", "eq", "validation"),
        DatasetFilter("fold_id", "eq", "fold-1"),
        DatasetFilter("horizon_sessions", "eq", 1),
        DatasetFilter("observation_session", "ge", date(2020, 1, 1)),
        DatasetFilter("observation_session", "le", date(2020, 3, 31)),
    ),
):
    consume(batch)
```

示例中的 table_id、schema_id 和选择值须使用实际 Result 声明。筛选在已验证快照上执行。Parquet 行组统计可跳过无关数据，没有统计时仍逐行筛选。

读取保持文件内原顺序，不另做全局排序。已请求表的完整文件摘要、schema 和行数仍按原入口验证，未命中行组不能免除文件验证。未请求的 holdout 表不会因此打开。

批次大小只限制单批行数，累计行数、内存及后续计算预算由消费者检查。模型报告沿用自己的预算门禁。

```powershell
python -m research_pipeline report --result <Result目录> --output <报告.md> --format markdown --json
python -m research_pipeline compare --left-result <左Result目录> --right-result <右Result目录> --json
python -m research_pipeline export-result --result <Result目录> --output <新导出目录> --json
```

包级消费者额外核对 ResearchPackage、计划、metric/claim 合同。直接 compare 的范围是 `formal_metric_facts`，包级比较是 `package_contract_and_metric_facts`。定义、单位、样本、状态或声明不一致时给出不可比较原因，不输出部分排名或部分 delta。

报告文件不覆盖已有文件，JSON 文档版本为 `research-result-report-v1`。HTML 模型、组合与 LEAN 请求见[报告](qlib-report.md)和[LEAN 执行](lean-execution.md)。

## 接入自定义指标

[Arrow自定义指标最小示例](../examples/custom_metric/README.md)提供完整可执行代码：生成合成观察、声明指标、发布正式Result。再由`load_result_context`和报告读取。示例使用合成Arrow和Parquet归档，不连接数据库，实际执行ResearchPackage准入、Runtime及正式Result发布。

项目算子只输出指标表。事件、可达性证明、检查点和发布材料由框架生成。

项目在`spec/research.yaml`的`metric_definitions`中保存`MetricDefinition.to_dict()`。`measurement_semantics`必须完整包含`quantity`、`numerator`、`denominator`、`observation_timing`和`aggregation`。分母为1也必须明确填写。定义中的实现引用、实现摘要、单位、频率、年化及空值政策要对应实际生产算法。

正式指标行使用`metric_ref:string`、`value:float64`、`unit:string`、`sample_start:string`、`sample_end:string`、`sample_size:int64`和`status:string`。样本边界写ISO日期或时间字符串，不直接输出Arrow date/timestamp。样本数为正整数，数值有限，单位匹配定义，完成状态为`computed`。

每个已声明指标在其正式生产表中恰好一行。`ResultSpec`与可达性证明共同绑定生产节点、端口、工件及schema。

发布入口在原子发布前检查正式指标行。直接读取、报告及比较沿用同一行合同。失败时异常指明指标引用、字段、实际值或类型和期望，不能把报告无法消费的表当成已完成指标。具体运行命令及Python、CLI消费者验收见示例README。

## 时间序列分析

analysis 生成独立 AnalysisResult，不修改 Result 或提高结论级别。请求显式指定 Result ID、日期列、数值列、日期窗口、频率、单位、费用和缺失值政策。

```powershell
python -m research_pipeline analysis run --result <Result目录> --request <AnalysisRequest.yaml> --output <新AnalysisResult.json> --json
python -m research_pipeline analysis compare --analysis-result <左AnalysisResult.json> --analysis-result <右AnalysisResult.json> --measure overall.compounded_return --direction higher_is_better --output <新AnalysisComparison.json> --json
```

只对明确声明的非重叠期间收益计算复利、几何年化与最大回撤。水平值只计算水平与绝对变化。比较要求分析规格、实际窗口和声明事实一致，不满足时返回稳定原因和空排名。

## 正确性由哪里保证

数据读取时检查公告可见时间、completed bar、历史规则及公司行动版本。Feature 不能读取未来 Label，处理器和模型只拟合允许的训练范围。最终 test/holdout 不进入开发反馈，留出访问登记在读取前完成，失败也不重置已打开区间。

下单和账本转换检查可用现金、可卖数量、T+1、费用、成交顺序和权益到账。订单在决策后允许的执行时点成交。指标使用实际金融账本及其声明的单位、费用和样本范围。

算法开发验收使用手算案例、确定性用例、错误输入及必要的差分对照。公式复现先确认原文定义，再测试公式边界。开发测试针对具体失败，不加入每轮研究的事后复算阶段。计算正确、预测关系成立和扣费后策略收益是不同研究问题。

## 归档与迁移

保存正式 Result、其依赖的持久输入、配置、模型和实现引用。需要恢复时保留原计划、invocation、checkpoint 与工件。研究系列和 holdout 账本是持久锚点，不能随失败轮次一起删除。

export-result 复制 Result 本体。输入位置仍被引用时应一同迁移，不能仅复制一份 Markdown 或安装目录来替代研究材料。工件释放与清理见[生命周期](artifact-lifecycle.md)。

## 已知限制

声明可见性依赖真实来源和正确字段语义。生产约束与开发测试不能证明供应商保存了全部历史版本。也不能把合成案例当作真实市场收益证据。

历史 Result、离线报告和历史验收记录保持原样。新研究以当前声明、接口和生产约束执行。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和资源的完整研究声明 |
| Catalog Lock | 已确认的数据字段、来源和可见性说明 |
| PIT（按时点可见） | 在过去某时刻只使用当时已知的信息 |
| QueryIR | 限定字段、范围、排序和预算的数据请求 |
| DAG（计算步骤图） | 节点之间的先后依赖 |
| Artifact（工件） | 带类型、内容身份和来源引用的产物 |
| checkpoint（检查点） | 通过正式验收的节点输出及恢复依据 |
| Result（正式结果包） | 正式表、运行摘要和配置、实现、输入引用 |
| holdout（最终留出集） | 开发选择之外的最终评价区间 |
| Supervisor | 监督执行、发布检查点和记录事件的主进程 |
| Worker | 执行节点计算的独立进程 |
| manifest（清单） | 记录文件、类型、范围和内容身份的文件 |
| schema（结构） | 表的字段名称、类型和约束 |
