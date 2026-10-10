# 命令行参考

最常用的路径是：检查研究包、确认数据可用、运行、独立验证、生成报告。Workspace 可以替你串起同一流程。遇到错误先看输出中的阶段与下一步建议，不必从所有命令里猜。

下面列出命令入口。可执行参数以当前安装版本的 --help 为准。

## 参数与行为说明

正式顶层命令：

```text
catalog, package, run, resume, retry-node, inspect, rerun-from,
verify, report, compare, analysis, export-result, doctor, gc, capabilities,
operator, artifact, recipe, workspace
```

以 `python -m research_pipeline --help` 和子命令 `--help` 为准。

## 发现与创建

```powershell
python -m research_pipeline capabilities --format json
python -m research_pipeline operator list --format json
python -m research_pipeline operator describe <operator-id> --format json
python -m research_pipeline artifact describe <artifact-type> --format json
python -m research_pipeline recipe list --format json
python -m research_pipeline catalog dataset search <关键词> --catalog-lock <持久Catalog-Lock目录> --format json
python -m research_pipeline package init <package目录> --json
python -m research_pipeline package expand-variants --base <基包> --manifest <变体清单.yaml> --output-root <输出目录> --json
```

QuantWitness 公开发行包不附带供应商或个人数据库的 Catalog。先用自己的声明和审批文件执行
`catalog validate/compile --definition ... --approvals ...` 生成持久 Catalog Lock，再显式传给
搜索、lint 和 admit。当前公共 Recipe 清单为空，以 `package init` 创建未填写研究事实的中性草稿，补齐四份声明后才能通过正式准入。Catalog dataset/field 搜索机器结果统一返回
`catalog_context`：其中的 `lock_reference` 是本次实际
加载且可直接传给 `--catalog-lock` 的稳定目录，`source_profiles` 列出正式 profile/environment。
dataset 命中项另含当前 `bindings`；`next_commands` 会带入 lock，并为每个命中 profile 保留显式
只读路径占位符。field 命中项不重复大块 binding 数据。

## lint 与准入

```powershell
python -m research_pipeline package lint --package <package目录> --catalog-lock <搜索结果.catalog_context.lock_reference> --json
python -m research_pipeline package admit --package <package目录> --catalog-lock <搜索结果.catalog_context.lock_reference> --data-db <source:prod只读DuckDB> --output <计划目录> --json
```

`package lint` 不打开研究数据库；草稿 lint 报出当前阻断字段，完整包 lint 返回编译结果、资源声明和 `required_inputs`。数据源、Catalog Lock 或输出目录仍需操作者选择时，不返回带尖括号占位符的 `next_command`。`package admit` 只接收完整包，根据显式只读数据源自动生成漂移/PIT 闭包。

`workspace init` 同样创建中性草稿；`workspace validate` 检查工作区路径、四份 YAML 的结构与文件边界，不代替检查研究内容的 `package lint`。草稿补齐前不得运行 `package admit`。

使用已提交的数据快照时，`package admit`、`run`、`workspace run/execute` 可传
`--input-snapshot-manifest <清单.json>`，与 `--data-db/--source-db` 互斥。分钟仍显式传入
`--minute-data-root`。清单引用原准入和工件，但新研究须使用当前批准的归档 Catalog 绑定；
运行与恢复核对计划冻结的清单内容。格式与边界见 [数据平面](data_plane.md#封存输入)。

多个数据源使用可重复的 `--source-db PROFILE=PATH`。来源声明为 `archived_snapshot` 时，lint、admit 和 package 消费命令必须提供 `--source-archive-root`。

直接采集的 raw 分钟查询没有额外来源版本参数。admit 从当前 Catalog binding 判断来源是否为
`raw + collected + pit_allowed`，并检查 completed bar、实际 schema、有界时区范围和稳定排序。
复权分钟或历史截面状态缺少各自 PIT 快照时仍拒绝。分钟 `AUDIT` 可用于只读观察，但若其输出
进入 Feature、Label、Signal、Statistics、Validity 或交易仿真，准入会明确拒绝。

## 项目算子

```powershell
python -m research_pipeline operator scaffold --output <新算子目录> --project-id <项目ID> --operator-id <算子ID> --format json
python -m research_pipeline operator validate --spec <operator.yaml> --source <源码目录> --format json
python -m research_pipeline operator build --spec <operator.yaml> --source <源码目录> --output <bundle父目录> --format json
python -m research_pipeline package lint --package <package目录> --extension-bundle <bundle目录> --json
python -m research_pipeline package admit --package <package目录> --extension-bundle <bundle目录> --catalog-lock <Catalog-Lock> --data-db <只读DuckDB> --output <计划目录> --json
```

scaffold 目标必须不存在，生成 `operator.yaml`、`source/operator.py` 和 README；声明由现行
`ProjectOperatorDeclaration` 序列化，不维护第二套 schema。入口是可由 Worker 以三个位置参数调用
的同步函数 `run(context, inputs, output_root)`。正式工件写入 `output_root`，返回每个声明端口对应的
typed commit；缺少 commit、端口/Artifact 不符、未声明文件或字节/hash 漂移均失败。

`--source` 是唯一源码闭包。CLI 不扫描当前目录、不自动安装依赖。bundle 在 admit 后进入计划闭包，run 和恢复不再接受 bundle 路径。

## 运行与恢复

```powershell
python -m research_pipeline run --plan <计划目录> --data-db <只读DuckDB> --artifact-root <工件目录> --handoff-out <handoff.json> --run-root <run目录> --result-store <ResultStore> --clock <带时区ISO时间> --root-seed 0 --json

# 可重复提供；只机会式复用 pure/cacheable 节点，未命中时正常执行
python -m research_pipeline run --plan <新计划目录> --reuse-run-root <已完成run目录> --data-db <只读DuckDB> --artifact-root <工件目录> --handoff-out <handoff.json> --run-root <新run目录> --result-store <ResultStore> --clock <带时区ISO时间> --root-seed 0 --json

# 指定节点及其必要上游必须复用；可非 pure/cacheable，完整复验失败时不得回退执行
python -m research_pipeline run --plan <新计划目录> --reuse-run-root <已完成run目录> --require-reused-node <节点ID> --data-db <只读DuckDB> --artifact-root <工件目录> --handoff-out <handoff.json> --run-root <新run目录> --result-store <ResultStore> --clock <带时区ISO时间> --root-seed 0 --json

# 修正不改变 DAG 的计划后，从一个显式失败 run 的成功 checkpoint 继续
python -m research_pipeline run --plan <新计划目录> --reuse-failed-run-root <失败run目录> --data-db <只读DuckDB> --artifact-root <工件目录> --handoff-out <handoff.json> --run-root <新run目录> --result-store <ResultStore> --clock <带时区ISO时间> --root-seed 0 --json
python -m research_pipeline inspect --run-root <run目录> --json
python -m research_pipeline resume --run-root <run目录> --json
python -m research_pipeline retry-node --run-root <run目录> --node <node-id> --json
python -m research_pipeline rerun-from --run-root <父run目录> --output-run-root <子run目录> --node <node-id> --json
```

不提供资源参数时，单次 CLI 使用 16 GiB 总内存、当前进程可用的全部逻辑核心和 64 GiB scratch 上限。内部 worker 默认 1 个；显式 `--workers` 不得超过 CPU 或 `max_workers` 容量，项目 Worker 在节点总资源预算内自行管理任务并行。`--resource-memory-bytes`、`--resource-cpu-slots` 和 `--resource-scratch-bytes` 可以覆盖本次进程内容量。

只有同时运行多个独立 CLI 时才需要仓库外 `--resource-state-dir`，并额外提供 process 总额度和 timeout。它只等待或拒绝，不改变样本、分区、seed 或金融语义。

`inspect --json` 返回 Runtime/节点/attempt 状态，并把现有事件投影为 `waiting_for_dependencies`、
`ready`、`waiting_for_resources`、`executing`、`checkpointing`、`finalizing` 或终态。每个节点同时包含
开始时间、阶段时间、已运行时长、最近心跳、进程健康、资源申请和预留；启用共享资源池时还会
列出各 run 的 FIFO 请求与活跃租约，且读取不会清理或改写治理状态。输出保留唯一
`recommended_action`，完整命令同时提供 `next_command` 和 `next_command_argv`。
只有 retry policy 允许且仍有余额的错误才建议 `retry-node`。需要人工修改 package 时，
`next_command` 只给可直接执行的 `package lint --help`，不会伪造仓库未保存的 package 路径。
finalize 失败但 `result_published=true` 时，`next_command` 使用真实 Result 路径进入 verify；该字段会实际改变下一步，不是只展示的回执。

## 结果验证与消费

```powershell
python -m research_pipeline verify --result <Result目录> --result-store <ResultStore> --output <VerificationResult.json> --verification-memory-bytes <字节> --verification-temp-bytes <字节> --verification-process-slots <进程槽> --verification-scratch-root <临时目录> --json
python -m research_pipeline report --verification-result <VerificationResult.json> --result-store <ResultStore> --output <报告.md> --format markdown --json
python -m research_pipeline compare --left-verification-result <左.json> --left-result-store <左Store> --right-verification-result <右.json> --right-result-store <右Store> --json
python -m research_pipeline analysis run --verification-result <VerificationResult.json> --result-store <ResultStore> --request <AnalysisRequest.yaml> --output <AnalysisResult.json> --analysis-memory-bytes <字节> --json
python -m research_pipeline analysis compare --analysis-result <左AnalysisResult.json> --analysis-result <右AnalysisResult.json> --measure overall.compounded_return --direction higher_is_better --output <AnalysisComparison.json> --json
python -m research_pipeline export-result --verification-result <VerificationResult.json> --result-store <ResultStore> --output <导出目录> --json
```

`--verification-memory-bytes`、`--verification-temp-bytes` 和 `--verification-scratch-root` 用于独立金融 oracle，并作为项目 Verifier 的默认内存、临时空间和工作目录预算。默认内存为 1 GiB、临时盘为 8 GiB；未指定 scratch root 时使用系统临时目录。`--verification-process-slots` 单独约束项目 Verifier 的 Supervisor 与 Worker 树，默认 2，Windows venv 可显式声明 3，见[资源预算](project_resource_budgets.md)。大型 canonical/TCA 使用批次扫描和受限 DuckDB，不按固定 Result 大小跳过；真实额度不足会使 `verify` 执行失败且不产生输出文件。数值复核发现研究错误时则会生成 `status=fail` 的 VerificationResult，命令退出码不替代研究验证状态。

新项目 Result 已包含 Plan 冻结的 Verifier bundle，`verify` 不再要求外部路径；历史 Result 仍可用
`--verifier-bundle` 提供同一身份的旧闭包。`report --output` 按 UTF-8 原子发布，目标已存在时拒绝
覆盖；`--format json` 写版本化结构对象，不把 Markdown 正文再包成字符串。省略 `--output` 时保留
原 stdout 报告行为。

`report --format html --request <Qlib报告请求.yaml> --output <报告.html>` 从 Result 的模型预测表或日频组合正式表生成离线图形。模型请求显式选择候选、fold、阶段、周期和日期窗口；组合请求以 `qlib-portfolio-report-v1` 绑定六张规范表及组合指标表，展示完整组合窗口的净值、回撤、费用和成交诊断，见 [Qlib 报告](qlib-report.md)。`package report` 使用相同参数和服务。HTML 保留原 VerificationResult 状态，不重新训练、不重跑交易、不修改 Result。

package 也提供绑定 ResearchPackage 的 report、compare 和 export-result 入口。直接 `compare`
返回 `verified_metric_facts_only`，明确说明未检查 package 合同；它只在已验证的指标单位、方向、
窗口、样本量、状态和实际 claim 事实一致时输出逐指标 delta。完整研究语义比较使用
`package compare`，由 `packages.delivery` 唯一检查两侧 metric/claim 合同并返回
`package_contract_and_verified_metric_facts`。不同 plan 只作为说明，不再单独否决比较。
`export-result` 只复制并复核已验证 Result；真正重跑仍使用 package → admit → run → verify。

`analysis run` 只接受 `status=pass` 的 VerificationResult。AnalysisRequest 必须显式绑定
Result ID、Verification hash、表、日期列、数值列、窗口、值语义、频率、单位、费用口径、
缺失值、不完整年度政策和聚合集。只有声明为非重叠期间收益的 `simple_return` 或
`log_return` 才能计算复合收益、几何年化和最大回撤；`level` 只计算水平值与绝对变化，
`observation` 不套收益公式。`--analysis-memory-bytes` 约束两个投影列的 Arrow 批次预算，
不是整个 Python 进程的绝对内存上限。

`analysis compare` 至少需要两份规范 AnalysisResult，排名指标和方向必须显式提供。分析规格、
实际窗口、Arrow 类型、claim policy、claim level 或 claim ceiling 不一致时，输出
`comparable=false`、稳定原因码和空排名，不生成部分结论。

## 运维

```powershell
python -m research_pipeline doctor --run-root <run目录> --json
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --json
```

`gc` 默认只给计划；真正应用需要显式 `--apply`。

## 发现指标与诊断目标

`artifact describe` 从正式 Metric registry 返回可绑定的 `metrics` 与 `schema.result_tables`，包括指标引用、单位、测量语义、结果 schema、字段类型和 `path_prefix`。ResultSpec 使用发现的 producer 端口和结果表声明；字段来源于查询的数据表仍由 Catalog 提供，Catalog 搜索必须显式传入持久 `--catalog-lock`。指标表匹配失败时显示预期节点、端口、schema 和实际表声明。

`doctor --run-root <目录> --json` 未通过时返回非零退出码和 `doctor_failed`，`data.findings` 保留每个目标的状态、错误码、对象、说明与下一步建议。多个同名运行目录用完整路径定位失败对象。

## Workspace 运行与复用

完整合成研究流程见 [Workspace 入门](workspace-quickstart.md)。`workspace run` 自动管理工件、handoff、run 和 ResultStore 路径，与普通 `run` 共用复用选项及校验：

- `--reuse-run-root <完成态run目录>` 可重复传入，按显式顺序尝试复用。
- `--require-reused-node <节点ID>` 可重复传入；必须同时提供完成态来源，任一指定节点不可复用时在计算前拒绝。
- `--reuse-failed-run-root <失败run目录>` 复用终态失败 run 中通过完整复验的成功 checkpoint，不能与完成态复用组合使用。

Workspace 不自动执行 lint、admit、verify 或 report；clock 和 root-seed 必须与分配 execution 时一致。

## 状态摘要与完整流程

支持 `--json` 的操作型命令也支持互斥的 `--summary`：前者保留完整 data 并附带 summary，后者只返回摘要、错误和下一步参数。默认文本输出展示主要状态、结果位置和诊断。

`summary.command_status` 只描述命令是否完成；`execution_status` 需要 Runtime 与 Result 封存共同成功；`verification_status` 来自独立验证结论。尚未执行或不适用的状态为 null。单独 verify 的退出码含义保持不变，验证 fail 不能因命令退出 0 被解释为通过。

```powershell
python -m research_pipeline workspace init <新工作区> --from-package <完整研究包> --json
python -m research_pipeline workspace execute --workspace <工作区> --catalog-lock <Lock目录> --data-db <只读DuckDB> --extension-bundle <算子bundle> --verifier-bundle <验证bundle> --summary
python -m research_pipeline package lint --package <研究包> --summary
```

`workspace execute` 沿用现有 lint、admit、run、verify、report；时点和种子从包读取，显式值必须一致。它每次分配新 execution，失败返回 `failed_stage`、`stages` 和已生成的结果位置。验证 fail 仍生成失败报告，但完整流程退出非零。恢复使用 inspect/resume/retry-node 后独立 verify/report，不以新的 execute 替代恢复。来源与资源参数见各命令 --help。
