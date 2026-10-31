# QuantWitness

QuantWitness 是面向个人、由 AI 协作开展量化研究与回测的框架。研究包声明问题、数据、算法和范围，框架负责按当时可见的信息取数、执行算法、保存结果并支持中断恢复。

主链为：**研究声明 → 准备与准入 → 计算或回测 → Result → 报告与开发反馈**。项目日线与一分钟策略使用本地 LEAN，日频模型使用 Qlib；RD-Agent 是可选的因子与模型提案层。报告、比较、分析、导出和 Dashboard 直接读取正式 Result。

[入门教程](docs/workspace-quickstart.md) · [数据接入](docs/catalog.md) · [研究包](docs/research_package.md) · [LEAN 执行](docs/lean-execution.md) · [RD-Agent](integrations/rdagent/README.md) · [文档导航](docs/index.md) · [模块职责](docs/architecture.md) · [框架约定](docs/framework-contracts.md)

## 一次研究怎样完成

1. 用 `package init` 创建草稿，填写研究问题、来源、证券、日期、时点和指标。已有完整研究包可通过 Workspace 复制并修改已声明参数。
2. 用 `package lint` 主动检查声明，或直接通过 Workspace `execute` 进入准备与准入。准入在重数据计算前检查物理来源、数据时点、算子输入和资源声明。
3. 执行算子图，保存可恢复检查点。项目日线与一分钟策略由 LEAN 撮合和记账，日频模型按冻结训练窗口拟合。
4. 成功后原子发布 Result，直接生成 Markdown／HTML 报告，或交给 AI 读取开发指标。

`package init` 生成的是中性草稿，补齐前不能准入。目前没有公共 Recipe。命令及参数以 `python -m research_pipeline --help` 和各子命令的 `--help` 为准。

查询输入默认直接读取已准入的只读数据库或既有归档，保留小型来源描述。`run`、`workspace run` 和 `workspace execute` 使用 `--persist-inputs` 才保存查询输入。LEAN 按天消费行情并预读下一交易日，分钟月文件由读取层按天整理，不生成日行情和逐日规则副本。日线策略通过同一列式bundle内的 `bars_request_id` 和 `open_request_id` 分别绑定完整OHLCV及独立开盘价；上一日完整行情在下一交易日开盘可见，开盘执行只使用当时可见的开盘来源。正式 Result、模型、运行计划及 holdout 账本继续保存。直接来源变化后拒绝原运行恢复；迁移运行时须同时保留引用来源。详见[直接输入](docs/direct-inputs.md)。

```powershell
python -m pip install -e ".[dev]" "pyarrow==21.0.0"
python -m research_pipeline package init work/my_research --json
python -m research_pipeline package lint --package work/my_research --catalog-lock <Catalog目录> --json
python -m research_pipeline package admit --package work/my_research --catalog-lock <Catalog目录> --data-db <只读数据库> --output work/plan --json
python -m research_pipeline run --plan work/plan --data-db <只读数据库> --artifact-root work/artifacts --run-root work/run --result-store work/results --clock <声明时钟> --root-seed <声明种子> --json
python -m research_pipeline report --result <Result目录> --result-store work/results --output work/report.md --format markdown --json
```

核心要求 Python 3.10+。RD-Agent 集成要求 Python 3.11+；Qlib 与 LEAN 按各自指南安装。公开包不包含个人数据库或真实行情，第一次运行可使用[合成研究示例](examples/README.md)。

Windows x64 用户可从[LEAN 合成日线与一分钟回测](examples/lean_quickstart/README.md)开始：先生成完整包和 Parquet 输入，再创建独立 LEAN 安装，最后执行正式准入、回测与报告。该流程使用完整公开源码；wheel 只提供 Python 框架，不包含独立 LEAN 引擎。当前安装脚本要求 PowerShell 7。

## 数据、计算与恢复

- **数据只读。** 数据库输入与输出目录分离；研究执行不会发布或改写数据库。在聚宽数据库仓库中，`data_factor.duckdb` 默认只读，研究主链不导入 `FactorPublisher`；正式因子库仍只能由 `factor_calc.publish` 写入，因子发布必须走独立授权流程。
- **只使用当时已知的信息。** 公告按可见日期对齐，分钟行情必须已完成，规则按历史版本选择。股票分红送股采用供应商当前历史事件，实施公告后下一交易日可见，源维护时间不作为事件可见性；ETF沿用版本选择合同。Feature 不得读取未来 Label；训练处理器只拟合允许的训练样本。
- **交易规则由执行实现负责。** 可用资金、可卖数量、T+1、数量格点、费用、成交顺序和权益到账在实际状态转换处处理。生产算法使用确定性案例、手算与开发期差分测试验收。
- **开发与最终评价隔离。** RD-Agent 只读取成功运行的开发指标；test／holdout 不进入研究循环。同系列变体共享持久留出消费账本，失败消费不重置。
- **中断可以继续。** 已提交检查点只在输入、配置和实现相符时复用。Result 组装失败可以接续组装；发布成功的结果保持只读。Workspace `execute --execution-id ID` 继续原 execution。

正式 DAG 默认 `deterministic_serial`、`workers=1`；显式并行仍受总预算限制。运行记录保存阶段进度和资源观测，资源不足不会自动改变样本、参数或种子。详见[运行与恢复](docs/runtime.md)、[进度](docs/runtime-progress.md)和[资源预算](docs/project_resource_budgets.md)。

## Result 与项目算法

Result 保存正式表、指标、运行摘要、配置与实现及输入引用。`ResultContext` 提供 `snapshot`、`metrics`、`claim_level` 和 `limitations`；结论级别描述研究声明及数据范围。报告按实际需要读取表与列。

LEAN的simulation输出为orders、intents、daily、trace四张执行表和metrics指标表；账户终态、策略state、运行锁与策略输入作为必要支持文件保存。行情、规则、公司行动及信号来源使用持久工件和计划引用，归档与清理时同时保留这些依赖。模型最终holdout账本继续长期保存。

项目算法通过算子 bundle 接入。自定义指标在 `spec/research.yaml` 的 `metric_definitions` 中声明，格式为 `MetricDefinition.to_dict()`，实现引用绑定实际计算算法。项目保留相应公式、边界和时点测试。

归档或迁移时应同时保留 Result 所引用的持久输入、模型和工件；恢复仍以原计划、invocation 和检查点为准。布局与生命周期见[快照布局](docs/snapshot-layout.md)、[工件保留与回收](docs/artifact-lifecycle.md)。

## AI 研究循环

```text
个人提出问题并冻结开发范围
  → RD-Agent 提出因子或模型
  → ResearchPackage / Workspace 执行
  → Result 中的开发指标反馈
  → 下一轮候选
  → 选定方案后单独进行最终评价
```

[因子研究](integrations/rdagent/docs/factor-research.md)、[模型研究](integrations/rdagent/docs/model-research.md)和[联合研究](integrations/rdagent/docs/joint-research.md)各有明确的数据与算法范围。失败运行只反馈诊断，不能伪装成金融指标。

## 当前范围

日线与一分钟策略统一由LEAN撮合和记账，Qlib组合使用同一日频执行路径，冻结目标在执行开盘交付。Python层保留当前金融规则、类型与历史Result读取；旧现货和期货撮合执行入口不再导出。公开分钟行情、特征及标签只接受 `interval_minutes=1`，日线直接使用日频源。历史Result按其冻结合同读取，旧计划和checkpoint不跨实现身份恢复。

框架支持声明式研究、日频模型和已准入的中国股票／ETF 日线与一分钟 LEAN 策略。期货文档保留历史结果与来源语义，当前没有可运行的期货策略入口。期权、完整融券、组合保证金优惠和实物交割尚不属于通用能力。

合成示例用于理解与检查工程行为，不构成投资效果证据。供应商历史数据的准确性和策略盈利需要研究者根据实际来源和研究设计判断。`local_only` 表示本地实现范围，不能解读为覆盖全部市场和机器的发行认证。

<details>
<summary>精确能力状态</summary>

状态来自 `src/research_pipeline/capabilities.json`。

<!-- CAPABILITIES_TABLE:START -->

| capability | 状态 | 命令 | 证据 / 信任 | 边界说明 |
| --- | --- | --- | --- | --- |
| `catalog.lock` | `local_only` | `catalog` | `local_acceptance` / `local_only` | Catalog 编译、锁定和漂移检查由显式来源文件驱动；公开仓库不附带个人 Catalog 或独立发布验收记录。 |
| `research_package.plan` | `local_only` | `package lint`<br>`package admit` | `local_acceptance` / `local_only` | ResearchPackage 只能声明受控合同，不接受自由 SQL、动态模块或项目 runner。lint 一次返回声明、算子、指标、结果和准入缺口；admit 从显式只读数据源生成漂移/PIT 闭包并在发布/加载时校验准入事实。 |
| `runtime.recovery` | `local_only` | `resume`<br>`retry-node`<br>`inspect`<br>`rerun-from`<br>`run --reuse-run-root`<br>`run --require-reused-node`<br>`run --reuse-failed-run-root`<br>`workspace run --reuse-run-root`<br>`workspace run --require-reused-node`<br>`workspace run --reuse-failed-run-root` | `local_acceptance` / `local_only` | resume、retry-node、inspect 和 rerun-from 消费当前 invocation、计划内算子闭包和已验证 checkpoint；run 与 workspace run 共用复用参数和正式运行校验；--reuse-run-root 只按显式顺序接受现行节点局部身份下已成功发布 Result 的完成态 run，并机会式复用 pure/cacheable 节点。配合 --require-reused-node 时，指定节点及其必要上游允许不是 pure/cacheable，但必须在 Worker 启动前全部通过节点局部身份、输入、checkpoint、typed 输出和 ExternalArtifact 内容复验，且不得回退执行。run --reuse-failed-run-root 只接受一个显式失败终态 run，要求新旧 DAG、节点身份环境、clock 和 seed 完全一致，只把状态为成功且完整复验通过的 checkpoint 复制进目标 run，再从首个未成功节点继续。两种跨运行来源不能混用，不扫描磁盘或依赖裸 ResultStore。同一次 execute 复用 Supervisor 已冻结的工件验证结果，新的 resume 进程、rerun child 或新 run 必须重新完整验证。多请求 data 节点另以 run 内 partial index 逐项复验已提交 DatasetArtifactRef，只重做失效 request；partial 不进入正式输出。旧计划仍可 inspect/resume/retry，但不能开启跨运行复用。该边界仍为 local_only。 |
| `result.consume` | `local_only` | `report`<br>`export-result`<br>`compare`<br>`analysis run`<br>`analysis compare` | `local_acceptance` / `local_only` | report、compare、analysis、export-result 和 Dashboard 直接消费正式 Result，不依赖 run-root。报告只投影需要的表与列。analysis 的请求显式声明窗口、值语义、单位、费用和处理政策；只有规格与实际窗口相同的结果才能排名。package compare 另检查研究包的指标与结论合同。Result 中的结论级别是研究声明及输入范围，不是独立认证。 |
| `operator_graph.generic_run` | `local_only` | `run`<br>`resume`<br>`retry-node`<br>`inspect` | `local_acceptance` / `local_only` | 正式 run 由统一 Runtime 调度，节点返回按端口索引的类型化工件，checkpoint 绑定全部端口；成功后原子发布 Result，报告与开发反馈直接读取结果。恢复继续核对输入、配置、实现和检查点，失败不能作为成功研究反馈。当前范围保持 local_only。 |
| `research.walk_forward_model` | `local_only` | `package admit`<br>`run` | `local_acceptance` / `local_only` | Qlib 六节点负责日频回归模型：Linear、LightGBM、XGBoost；Processor 与 Model 按候选/fold 一起训练保存。保留 purge/embargo、开发区 validation 选择及隔离 holdout；模型文件随 Result 封存。只接受 v2 模型工件，不恢复旧七阶段 ML checkpoint。本地技术验收不代表真实策略、可交易性或样本外盈利。 |
| `minute_line.complete` | `local_only` | `run` | `local_acceptance` / `local_only` | 股票与ETF的一分钟策略使用LEAN原生订单与账户，按已准入来源、完成bar和历史可见规则执行。读取层按月整理、按天消费并预读下一交易日；预读不改变策略可见时点。日线使用独立开盘来源。当前策略频率为1m或1d，能力保持local_only。 |
| `minute_line.real_data_smoke` | `local_only` | `run` | `local_acceptance` / `local_only` | 公开包提供四资产分钟输入和只读 Result 合同，不附带供应商原始数据或个人运行记录。真实数据观察由使用者在自己的来源、窗口和规则下执行；当前能力为 local_only，不代表供应商历史版本精确重放或分钟交易仿真。 |
| `simulation.bar_tca` | `local_only` | `package admit`<br>`run` | `local_acceptance` / `local_only` | Bar TCA 只消费统一 SimulationResult 的正式订单与成交和账本身份，不二次撮合。分钟路径要求决策时可见基准、已完成执行 bar、可见容量与正式 fill，缺任何必要事实均失败关闭。随包参考规则只有有界中国市场标的窗口；公开源码不包含个人真实研究结果，能力保持 local_only。 |
| `capability.discovery` | `local_only` | `capabilities --format json`<br>`operator list/describe/scaffold/validate/build`<br>`artifact describe`<br>`recipe list/describe/scaffold`<br>`catalog dataset/field search`<br>`package lint`<br>`package expand-variants` | `local_acceptance` / `local_only` | capabilities 命令逐字段读取本清单；operator、artifact、catalog 和 package lint 的发现结果来自正式 registry、schema、Catalog Lock 或 package compiler。package expand-variants 只覆盖基包中已存在的 node 参数，原子生成普通完整包，不接受深层 YAML 合并、模板、Python 或 SQL。当前没有获准的公共 recipe，使用 package init 创建通用起点；项目完整拓扑由 ResearchPackage 声明。operator scaffold 生成可验证的最小项目算子；正式 Feature/Label 需必填 causal_plan，且来源必须可由已准入请求证明。validate/build 只复验显式源码闭包，不扫描目录或自动安装。 |
| `resource.governance` | `local_only` | `run --resource-state-dir` | `local_acceptance` / `local_only` | 数据节点预算先编译为 DuckDB、batch/writer 与进程余量；当前 provider 支持包络下限为 256 MiB，低于下限在对象统计和扫描前拒绝。完整矩阵消费者用 footer 做数据页前的明显超界拒绝；内部 worker 默认 1 个，显式指定不能超过 CPU 或 max_workers 容量，并共用节点总预算。全新隔离进程中的真实 DuckDB/Parquet/Arrow 探针回归整个进程树 RSS、读取量与 temp 峰值，不把局部公式称为任意 allocator 的硬上界。多个 CLI/worker 的 FIFO 租约与父子令牌仍共用总容量；资源不足不改变样本、频率、参数或 seed。合成探针不进入正式运行校准总体。 |

<!-- CAPABILITIES_TABLE:END -->

</details>
