# QuantWitness

QuantWitness 帮助个人研究者和 AI 协作者把量化研究问题变成可重复运行、可独立验证的结果，只读使用研究数据，并按当时可见的信息开展研究。

## 能得到什么

股票横截面示例用四只虚构证券，在同一决策时点按已知收益排序，再观察之后的收益差。完成入门后，可以得到：

- **Result**：封存研究数值表、数据来源和运行身份。
- **VerificationResult**：独立复核结果，明确验证状态是否为 `status=pass`。
- **报告**：展示验证结论；具体研究数值保存在 Result 的表中。

示例中 A 与 C 的未来收益差，手算值约为 **0.0472222**（小数收益单位）；改变样本范围后可生成第二份结果并比较。这是合成数据上的算法示例，不含交易费用或实盘成交模型。

## 快速入门与安装

从 [Workspace 合成研究入门](docs/workspace-quickstart.md) 开始：使用填写完整的股票横截面研究包，用 `workspace execute` 完成运行、独立验证和报告，再改变样本范围比较两次结果。该指南的准备步骤会创建一次性合成数据库，执行前须按所在环境的规则取得写库授权。

需要 Python 3.10 或更新版本。首次使用请取得 [QuantWitness 完整公开源码](https://github.com/ljjtim/QuantWitness)，可下载 Code → Download ZIP，或在 PowerShell 执行：

```powershell
git clone https://github.com/ljjtim/QuantWitness.git
Set-Location QuantWitness
python -m pip install -e ".[dev]" "pyarrow==21.0.0"
python -m research_pipeline --help
```

源码、文档和 `examples/` 使用同一 checkout。已有本单仓库源码时，先进入 `research_pipeline/` 再安装；离线使用交付的完整公开源码目录。运行前记录 `git rev-parse HEAD`，离线目录记录交付清单中的源码提交号。

只安装框架可使用 `python -m pip install quantwitness`，之后运行 `quantwitness --help`；Python import 名仍为 `research_pipeline`。wheel 不包含示例，`research_pipeline-source.zip` 也不是完整示例包。分发范围见[安装与发布构建](docs/release.md)。

## 使用自己的数据

先按 [Catalog 与 PIT 准入](docs/catalog.md) 描述数据字段、物理表、可见日期和来源，通过现有的 `catalog validate/compile` 生成自己的持久 Catalog Lock。公开发行包不附带供应商或个人数据库的 Catalog，也不会猜测本机数据库路径；搜索、lint 和 admit 都复用显式 Lock 和只读数据源。

日频 Qlib 研究可以从[模型与组合示例](examples/qlib_portfolio/README.md)开始；自有冻结行情通过显式输入配置复用相同研究包主链。组合结果可用[Qlib 研究报告](docs/qlib-report.md)生成离线净值、回撤、费用和成交诊断图表。

再按[创建自己的研究](docs/getting-started.md)填写研究问题、样本、指标、数据请求和算子图。`package init` 只创建四份中性声明草稿，补齐后才可依次 `package lint` → `package admit` → `run` → `verify` → `report`。AI 协作者按 [AI 工作协议](docs/ai_workflow.md)确认研究口径、选择已有能力并读取诊断。

## 范围限制

- 当前没有公共 Recipe；完整研究由项目声明，已有算法可复用，缺少的算法按[项目扩展合同](project_extensions/README.md)提供可信本地代码。扩展执行环境不是安全沙箱。
- 研究数据库始终只读。财报、成分、行业、ST、停牌、复权和市场规则必须按当时可见的日期对齐；盘中研究只使用已完成的 bar。
- 运行成功不等于研究结论可信，需要读取独立 VerificationResult 的状态；金融口径、准确性与验证边界见[结果与独立验证](docs/evidence.md)。框架不是交易执行平台，不承诺策略盈利或实盘可交易。
- 能力适用范围以机器清单和[分钟参考规则](docs/minute_rule_provenance.md)为准。发布验收与逐项晋级条件见[安装与发布构建](docs/release.md)。`planned` 只能发现，`local_only` 不代表已通过独立发布验收；不承诺全历史、全品种覆盖。

### 能力状态

<details>
<summary>展开完整能力表</summary>

能力状态以 [`src/research_pipeline/capabilities.json`](src/research_pipeline/capabilities.json) 为唯一机器来源。

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

## 深入文档

- [框架合同](docs/framework-contracts.md)：完整流程、数据与金融口径、校验职责、扩展与恢复边界。
- [ResearchPackage](docs/research_package.md)：项目声明、输入和结果约定。
- [Runtime 与恢复](docs/runtime.md)：节点诊断、中断恢复和跨运行复用。
- [资源预算](docs/project_resource_budgets.md)：项目执行与独立复核的内存、临时空间和进程槽。
- [命令行](docs/cli.md)与[运维](docs/operations.md)：具体参数和日常操作。
- [现行文档索引](docs/index.md)：按主题查阅其余文档。
- [架构边界](ARCHITECTURE.md)、[参与贡献](CONTRIBUTING.md)、[安全报告](SECURITY.md)、[第三方材料](THIRD_PARTY_NOTICES.md)、[许可证](LICENSE)与[随附声明](NOTICE)。

### Qlib 模型与研究图表

使用 `pip install -e ".[ml]"` 安装 Qlib 模型与 Plotly 图形依赖。日频回归采用 Qlib 六节点链，模型与处理器共同封存；见[模型合同](docs/walk_forward_model.md)和[模型文件交付](docs/qlib-model-results.md)。`report --format html --request <请求文件> --output <报告.html>` 和 `package report` 使用同一 Qlib 研究图形服务。HTML 展示模型诊断及验证状态，不提升原结果的验证范围；完整请求见 [Qlib 报告](docs/qlib-report.md)。

公开[Qlib 研究起点](examples/qlib_portfolio/README.md)支持 `development`、`model` 和 `portfolio`。组合模式把 test 预测经固定规则转为目标，调用 `finance.simulation.daily-cash@1.0.0` 完成下一会话开盘成交、现金和费用账本、TCA 与独立金融复核。该节点按[精确人工准入记录](release/daily-cash-local-admission.md)保持 `local_only`，不代表通用晋级或实盘交易能力。

### 归档输入与 RD-Agent

已提交的行情快照可通过 `--input-snapshot-manifest` 进入同一准入、运行和恢复流程；与数据库
来源互斥，当前 Catalog 与 PIT 规则仍适用。详见 [封存输入](docs/data_plane.md#封存输入)。
RD-Agent 可选集成包位于 `integrations/rdagent`，使用独立环境调用 RP 正式主链；其请求引用
ResearchPackage，候选代码通过项目 bundle 执行，反馈引用 Result 与 VerificationResult。
多轮开发研究既可比较已有预测，也可实际执行研究包参数变体；每轮独立运行和验证，开发循环不打开最终 holdout。见[研究循环](integrations/rdagent/docs/research-campaign.md)。
