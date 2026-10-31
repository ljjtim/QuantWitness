# 文档导航

研究主链是声明 → 准备与准入 → 执行 → Result → 报告或开发反馈。普通 Workspace execute 完成一次准备与准入，独立 package lint 用于主动诊断。

## 开始研究

1. [第一项合成研究](workspace-quickstart.md)：准备输入、执行、报告、参数变体与恢复。
2. [创建自己的研究](getting-started.md)：问题、样本、数据可见性、算法与指标。
3. [基本数据接入](catalog.md)：字段、物理来源、Catalog Lock 和可见日期。
4. [Result 与研究结论](evidence.md)：正式输出、声明限制、直接报告、比较与导出。
5. [LEAN 首次回测](../examples/lean_quickstart/README.md)：Windows 独立安装、合成日线与一分钟、正式结果与报告。

## AI 研究

[AI 研究指南](ai_workflow.md)说明人的决定、提案器和正式执行的职责。

| 目标 | 文档 |
| --- | --- |
| PDF 公式复现 | [公式复现](../integrations/rdagent/docs/formula-reproduction.md) |
| 合成教学案例 | [成交量集中度](../integrations/rdagent/examples/volume_concentration/README.md) |
| 受控参数循环 | [开发研究循环](../integrations/rdagent/docs/research-campaign.md) |
| 因子、模型和联合探索 | [因子](../integrations/rdagent/docs/factor-research.md) · [模型](../integrations/rdagent/docs/model-research.md) · [联合](../integrations/rdagent/docs/joint-research.md) |
| 可选环境 | [RD-Agent](../integrations/rdagent/README.md) · [安装验收](../integrations/rdagent/docs/installation.md) |

开发反馈来自成功 Result 的开发指标，最终 test/holdout 不参与循环选择。固定公式先确认定义并进行算法测试。

## 模型、组合与交易

| 问题 | 文档 |
| --- | --- |
| 训练、选择和最终评价 | [模型研究](walk_forward_model.md) · [研究系列与留出集](holdout-series.md) |
| 特征表达式与基线 | [Qlib 表达式](qlib-expressions.md) · [量价与选定 Alpha](qlib-factor-baselines.md) |
| 模型交付与报告 | [模型文件](qlib-model-results.md) · [模型和组合报告](qlib-report.md) |
| 风险约束组合 | [组合风险目标](qlib-portfolio-risk.md) |
| 项目分钟策略 | [LEAN 执行](lean-execution.md) · [策略协议](strategy-protocol.md) |
| 历史分钟范围 | [项目范围](minute-project-scope.md) · [规则来源](minute_rule_provenance.md) |
| 订单、成交和账户 | [金融计算](financial_simulation.md) · [显式订单](explicit_orders.md) |
| 公司行动和信用账户 | [现货与信用](spot_account.md) |
| 期货资金共享 | [共享期货](shared_futures.md) |
| 统计与选择偏差 | [稳健研究](robust_research.md) |

数据可见性、标签、holdout、可用资金和交易规则在生产读取及状态转换处执行。算法正确性在开发时通过确定性案例、手算与必要差分验收。

## 系统、恢复与资源

- [命令行参考](cli.md)、[运行维护](operations.md)。
- [模板和执行接续](workspace-continuation.md)、[Runtime](runtime.md)、[真实进度](runtime-progress.md)。
- [资源预算](project_resource_budgets.md)、[资源历史](resource-history.md)与[分钟预算](minute-resource-budget.md)。
- [数据平面](data_plane.md)、[快照布局](snapshot-layout.md)、[工件生命周期](artifact-lifecycle.md)。
- [实现身份](implementation-identity-scope.md)、[执行与交付](research-execution-improvements.md)。
- [研究包](research_package.md)、[框架约定](framework-contracts.md)、[架构](architecture.md)与[六张图](architecture/README.md)。

## 扩展与交付

- [项目扩展](../EXTENSIONS.md)、[扩展接口](../project_extensions/README.md)、[贡献](../CONTRIBUTING.md)。
- [安装与发布](release.md)、[独立使用验收](external-acceptance.md)。
- [第三方许可](../THIRD_PARTY_NOTICES.md)、[许可证](../LICENSE)与[安全报告](../SECURITY.md)。

能力以当前 capabilities 命令和机器清单为准。local_only 表示已记录的本地适用范围，不能扩展成全市场、全历史或任意环境认证。

## 常用名称

| 名称 | 含义 |
| --- | --- |
| PIT | 在过去某一时刻只使用当时已知的信息 |
| Catalog Lock | 认可的数据字段、来源及可见性版本 |
| ResearchPackage | 数据、计算、样本、指标和预算的完整声明 |
| DAG | 步骤的先后依赖 |
| checkpoint | 可按身份与内容继续使用的已完成步骤 |
| Result | 正式计算输出、运行摘要及配置、实现和输入引用 |
| holdout | 开发循环之外的最终评价区间 |
