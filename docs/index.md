# 文档导航

第一次使用 QuantWitness，不必从接口表开始。先完成一项小研究，再按自己的目标选择数据、AI、模型或回测文档。

## 从这里开始

1. [入门教程：第一项可验证研究](workspace-quickstart.md)：不依赖真实行情，跑通第一份结果、独立验证与报告。
2. [创建自己的研究](getting-started.md)：把问题、样本、计算步骤和评价标准写成研究包。
3. [基本数据接入](catalog.md)：说明字段含义和可见日期，让框架知道过去的某个时点能用什么。
4. [读懂结果和检查单](evidence.md)：分清“程序跑完”“结果已保存”和“独立检查通过”。

## 我想用 AI 研究

[AI 研究指南](ai_workflow.md)解释人、RD-Agent 与框架分别做什么。

| 目标 | 指南 |
| --- | --- |
| 给一篇 PDF，复现其中的公式 | [公式复现](../integrations/rdagent/docs/formula-reproduction.md) |
| 不付费调用模型，先体验流程 | [教学 PDF 与成交量集中度](../integrations/rdagent/examples/volume_concentration/README.md) |
| 比较已有研究方案 | [开发研究循环](../integrations/rdagent/docs/research-campaign.md) |
| 让 AI 提出新因子 | [因子研究](../integrations/rdagent/docs/factor-research.md) |
| 让 AI 提出模型结构 | [生成式模型研究](../integrations/rdagent/docs/model-research.md) |
| 联合探索因子与模型 | [联合研究](../integrations/rdagent/docs/joint-research.md) |
| 准备独立的 AI 调度环境 | [RD-Agent 安装与场景](../integrations/rdagent/README.md) · [安装验收](../integrations/rdagent/docs/installation.md) |

## 我想做因子、模型与策略研究

| 问题 | 文档 |
| --- | --- |
| 怎么避免拿最终样本反复挑模型 | [滚动训练与样本外选择](walk_forward_model.md) |
| 怎么写价格、成交量等表达式 | [Qlib 因子表达式](qlib-expressions.md) |
| 有哪些现成特征可作基线 | [量价与选定 Alpha 特征](qlib-factor-baselines.md) |
| 预测如何转为带风险约束的组合 | [组合与风险目标](qlib-portfolio-risk.md) |
| 训练好的模型怎样保存和恢复 | [模型文件交付](qlib-model-results.md) |
| 怎么看预测或策略图表 | [模型与组合报告](qlib-report.md) |
| 订单、持仓和资金如何模拟 | [交易与账户](explicit_orders.md) |
| 分红、红利税、换股或融资如何处理 | [现货与信用账户](spot_account.md) |
| 订单什么时候成交，资金怎样预占 | [显式订单](explicit_orders.md) |
| 多个期货合约怎样共享资金 | [共享期货账户](shared_futures.md) |
| 分钟级策略有哪些额外要求 | [分钟成交与订单](explicit_orders.md) · [分钟规则来源](minute_rule_provenance.md) |
| 目前有哪些真实场景验收 | [真实覆盖与限制](native_backtest_acceptance.md) |

## 我想理解系统，或解决运行问题

- [六张交互图](architecture/README.md)：总体概览、总体架构、数据流、工作流、调用时序与运行状态。
- [模块职责与边界](architecture.md)：从问题找到代码位置。
- [研究包格式](research_package.md)：哪些约定需要固定下来。
- [数据读取与输入归档](data_plane.md)：同一研究怎样使用数据库或已封存行情。
- [运行与恢复](runtime.md)：什么时候用 resume，什么时候用 retry-node。
- [资源预算](project_resource_budgets.md)：运行和独立验证如何分配内存与进程。
- [命令行参考](cli.md)与[运维](operations.md)：查找、诊断、清理与恢复入口。
- [完整框架约定](framework-contracts.md)：需要实现新接入或深查行为时再读。

## 我想扩展或发布

- [项目扩展](../EXTENSIONS.md)与[多文件计算接口](../project_extensions/README.md)：把自己的算法接入已有流程。
- [参与贡献](../CONTRIBUTING.md)、[安全报告](../SECURITY.md)。
- [安装与发布](release.md)、[独立使用验收](external-acceptance.md)。
- [第三方代码与数据许可](../THIRD_PARTY_NOTICES.md)、[Apache-2.0 许可证](../LICENSE)。

## 看懂几个常见名称

| 名称 | 用人话解释 |
| --- | --- |
| PIT / 时点一致性 | 在过去的某一时刻，只使用那时已经可获得的信息 |
| Catalog Lock | 本次认可的数据说明版本：字段、来源及可见性约定 |
| ResearchPackage | 一份可执行研究说明，记录要用什么数据、怎么算、怎么判断 |
| DAG | 计算步骤的先后依赖关系；不是额外一种研究方法 |
| checkpoint | 已完成步骤的保存点，通过检查后可以继续使用 |
| Result | 一次研究的正式结果包及其来源与实现信息 |
| VerificationResult | 绑定这份结果的独立检查单 |
| holdout | 开发时留在一边，最后才用于评价的数据 |

## 精确能力状态

日常选功能先看上面的指南；做验收或集成时，再核对机器能力表。`local_only` 表示本地范围内已有实现与验收，不等于全市场、全历史或任意环境均已验证。

<details>
<summary>展开能力清单</summary>

<!-- CAPABILITIES_TABLE:START -->
| capability | 状态 | 命令 | 证据 / 信任 | 边界说明 |
| --- | --- | --- | --- | --- |
| `catalog.lock` | `local_only` | `catalog` | `local_acceptance` / `local_only` | Catalog 编译、锁定和漂移检查由显式来源文件驱动；公开仓库不附带个人 Catalog 或独立发布验收记录。 |
| `research_package.plan` | `local_only` | `package lint`<br>`package admit` | `local_acceptance` / `local_only` | ResearchPackage 只能声明受控合同，不接受自由 SQL、动态模块或项目 runner。lint 一次返回声明、算子、指标、结果和准入缺口；admit 从显式只读数据源生成漂移/PIT 闭包并在发布/加载时校验准入事实。 |
| `runtime.recovery` | `local_only` | `resume`<br>`retry-node`<br>`inspect`<br>`rerun-from`<br>`run --reuse-run-root`<br>`run --require-reused-node`<br>`run --reuse-failed-run-root`<br>`workspace run --reuse-run-root`<br>`workspace run --require-reused-node`<br>`workspace run --reuse-failed-run-root` | `local_acceptance` / `local_only` | resume、retry-node、inspect 和 rerun-from 消费当前 invocation、计划内算子闭包和已验证 checkpoint；run 与 workspace run 共用复用参数和正式运行校验；--reuse-run-root 只按显式顺序接受现行节点局部身份下已成功发布 Result 的完成态 run，并机会式复用 pure/cacheable 节点。配合 --require-reused-node 时，指定节点及其必要上游允许不是 pure/cacheable，但必须在 Worker 启动前全部通过节点局部身份、输入、checkpoint、typed 输出和 ExternalArtifact 内容复验，且不得回退执行。run --reuse-failed-run-root 只接受一个显式失败终态 run，要求新旧 DAG、节点身份环境、clock 和 seed 完全一致，只把状态为成功且完整复验通过的 checkpoint 复制进目标 run，再从首个未成功节点继续。两种跨运行来源不能混用，不扫描磁盘或依赖裸 ResultStore。同一次 execute 复用 Supervisor 已冻结的工件验证结果，新的 resume 进程、rerun child 或新 run 必须重新完整验证。多请求 data 节点另以 run 内 partial index 逐项复验已提交 DatasetArtifactRef，只重做失效 request；partial 不进入正式输出。旧计划仍可 inspect/resume/retry，但不能开启跨运行复用。该边界仍为 local_only。 |
| `evidence.consume` | `local_only` | `verify`<br>`report`<br>`export-result`<br>`compare`<br>`analysis run`<br>`analysis compare` | `local_acceptance` / `local_only` | verify 直接从自包含 Result 生成结构化 VerificationResult；report、compare、analysis、export-result 和 Dashboard 不依赖 run-root。analysis run 只消费 status=pass 的 VerificationResult，要求外部请求显式声明表列、窗口、值语义、频率、单位、费用口径和处理政策，只投影日期和值两列并生成独立 AnalysisResult，不修改 Result、VerificationResult 或 claim。analysis compare 只对同规格、同实际窗口和同 claim 事实的 AnalysisResult 排名，任一必要事实不一致时不输出部分排名。直接 compare 仍只比较已验证指标事实并明示未检查 package 合同；package compare 由 delivery 唯一检查 metric/claim 合同。export-result 仅复制并复核已验证 Result，不重新执行研究。该合同尚未通过独立发布验收，因此保持 local_only。 |
| `evidence.validity_recompute` | `local_only` | `verify` | `local_acceptance` / `local_only` | 独立 verify 从已封存的 canonical 六表与 Bar TCA 四表复核金融守恒、费用与 lineage；金融 oracle 使用有界 Arrow 批次和受配额 DuckDB 扫描，篡改或资源不足均阻止生成 VerificationResult。公开源码提供合同测试，不附带个人真实数据 Result 或独立发布验收；能力保持 local_only，不代表策略盈利、实盘成交或可交易性。 |
| `operator_graph.generic_run` | `local_only` | `run`<br>`resume`<br>`retry-node`<br>`inspect` | `local_acceptance` / `local_only` | 正式 run 由 Runtime v2 调度，节点返回按端口索引的 typed refs，checkpoint 绑定全部端口；成功后唯一 finalize 自包含 Result，再由独立 verify 生成 VerificationResult。该边界尚缺独立发布验收，因此仍是 local_only。 |
| `research.walk_forward_model` | `local_only` | `package admit`<br>`run` | `local_acceptance` / `local_only` | Qlib 六节点负责日频回归模型：Linear、LightGBM、XGBoost；Processor 与 Model 按候选/fold 一起训练保存。保留 purge/embargo、开发区 validation 选择及隔离 holdout；模型文件随 Result 封存。只接受 v2 模型工件，不恢复旧七阶段 ML checkpoint。本地技术验收不代表真实策略、可交易性或样本外盈利。 |
| `minute_line.complete` | `local_only` | `run` | `local_acceptance` / `local_only` | 四类中国市场资产可按当前 Catalog、已完成分钟 bar、PIT 快照与历史规则进入同一研究主链；随包规则仅覆盖文档列明的参考标的及窗口，范围外无默认规则。公开源码提供合同与合成示例，不附带真实分钟数据或研究结果；能力保持 local_only，不代表全历史、全品种或实盘可交易。 |
| `minute_line.real_data_smoke` | `local_only` | `run` | `local_acceptance` / `local_only` | 公开包提供四资产分钟输入和只读 Result/VerificationResult 合同，不附带供应商原始数据或个人真实运行收据。真实数据观察须由使用者在自己的来源、窗口和规则下重新执行并独立验证；当前能力只到 local_only，不宣称供应商历史版本精确重放或分钟交易仿真。 |
| `simulation.bar_tca` | `local_only` | `package admit`<br>`run` | `local_acceptance` / `local_only` | Bar TCA 只消费统一 SimulationResult 的正式订单与成交和账本身份，不二次撮合。分钟路径要求决策时可见基准、已完成执行 bar、可见容量与正式 fill，缺任何必要事实均失败关闭。随包参考规则只有有界中国市场标的窗口；公开源码不包含个人真实研究结果，能力保持 local_only。 |
| `capability.discovery` | `local_only` | `capabilities --format json`<br>`operator list/describe/scaffold/validate/build`<br>`artifact describe`<br>`recipe list/describe/scaffold`<br>`catalog dataset/field search`<br>`package lint`<br>`package expand-variants` | `local_acceptance` / `local_only` | capabilities 命令逐字段读取本清单；operator、artifact、catalog 和 package lint 的发现结果来自正式 registry、schema、Catalog Lock 或 package compiler。package expand-variants 只覆盖基包中已存在的 node 参数，原子生成普通完整包，不接受深层 YAML 合并、模板、Python 或 SQL。当前没有获准的公共 recipe，使用 package init 创建通用起点；项目完整拓扑由 ResearchPackage 声明。operator scaffold 生成可验证的最小项目算子；正式 Feature/Label 需必填 causal_plan，且来源必须可由已准入请求证明。validate/build 只复验显式源码闭包，不扫描目录或自动安装。 |
| `resource.governance` | `local_only` | `run --resource-state-dir` | `local_acceptance` / `local_only` | 数据节点预算先编译为 DuckDB、batch/writer 与进程余量；当前 provider 支持包络下限为 256 MiB，低于下限在对象统计和扫描前拒绝。完整矩阵消费者用 footer 做数据页前的明显超界拒绝；内部 worker 默认 1 个，显式指定不能超过 CPU 或 max_workers 容量，并共用节点总预算。全新隔离进程中的真实 DuckDB/Parquet/Arrow 探针回归整个进程树 RSS、读取量与 temp 峰值，不把局部公式称为任意 allocator 的硬上界。多个 CLI/worker 的 FIFO 租约与父子令牌仍共用总容量；资源不足不改变样本、频率、参数或 seed。合成探针不进入正式运行校准总体。 |
<!-- CAPABILITIES_TABLE:END -->

</details>
