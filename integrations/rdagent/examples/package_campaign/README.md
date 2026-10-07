# 正式研究包的两轮参数研究

本例在2400条虚构分钟数据上比较两日与三日成交量集中度窗口。每轮使用一个独立ResearchPackage，通过原有Workspace完成准入、执行、Result、独立VerificationResult和报告。RD-Agent读取已验证的开发指标后进入下一轮，不调用模型或数据库。

研究目标是两个虚构证券在2025年1月8日至10日的滚动集中度均值；方向设为最小化，仅用于演示候选选择。较小集中度不代表更高收益、更低风险或更优投资策略。前两日输入用于窗口预热，不进入目标均值；两候选目标样本均为六条。

## 执行

使用安装了QuantWitness核心、可选RD集成（含`materials`依赖）及固定上游RD源码的同一Linux环境。环境准备见集成安装指南。本例路径均由当前运行主机解释，不使用Windows/Linux执行桥；项目公式复现入口仍支持跨系统桥。

```bash
python integrations/rdagent/examples/package_campaign/prepare.py --output "$RESEARCH_OUTPUT"
python -m quantwitness_rdagent campaign-run --request "$RESEARCH_OUTPUT/request.json"
python -m quantwitness_rdagent campaign-inspect --request "$RESEARCH_OUTPUT/request.json"
python -m quantwitness_rdagent campaign-resume --request "$RESEARCH_OUTPUT/request.json"
```

`RESEARCH_OUTPUT`必须是尚不存在的目录。`prepare`生成教学Parquet、Catalog、来源材料、ResearchPackage和固定算子/Verifier bundle。没有数据库文件或联网步骤，合成来源无需真实论文审阅。

本例参考目标值为三日窗口`0.004609909162671379`、两日窗口`0.004617528112477554`；按声明的最小化目标选择三日窗口。

结果在`session/outcome.json`，两轮Result与独立验证位于`session/candidates/<候选>/workspace/.research/executions/`。`selected_result_ref`和`selected_verification_ref`指向选定开发候选。恢复会继续原execution，已发布结果不会重新计算。技术中断保留已预约评价，诊断写入候选目录的`execution-diagnostic.json`；按Runtime建议等待或恢复，不会另开运行。独立验证失败或开发指标不合格记录为失败候选。

## 研究声明

请求使用`research_kind=package`，候选只覆盖基包已有节点参数。基包、归档、Catalog、扩展和Verifier在会话开始时冻结。目标明确声明表、schema、数值列、日期列、可见时点列、固定筛选、聚合与方向；提案器只接收目标值和候选状态。

完整Qlib六节点图中的最终test/holdout计算不能作为自动循环候选。开发循环与最终一次性评估分别执行。新增研究复用自己的已批准ResearchPackage和Verifier；本例证明参数研究及恢复行为，不证明任意论文或任意项目可以无人审核运行。

## Qlib模型参数研究

安装核心`ml`依赖后，可将上述准备命令替换为：

```bash
python integrations/rdagent/examples/package_campaign/prepare_qlib.py --output "$RESEARCH_OUTPUT"
```

该起点使用十只虚构ETF、最终评估边界前79个价格会话。两个候选分别重新训练Ridge模型，正则化强度为0.1与1.0；各候选独立封存模型、处理器、正式Result和VerificationResult。开发目标是相同validation样本的均方误差，只有验证通过的指标进入下一轮。模型调用预算为零，RD-Agent使用固定提案器完成调度。

模型开发图只允许2.0.0版本的split、fit、predict和fold-metrics，split必须显式声明`evaluation_scope=development`。selection和locked-holdout继续留在最终研究中。每个变体同时声明模型参数、研究设计与身份，不能复用其他候选的训练结果。
