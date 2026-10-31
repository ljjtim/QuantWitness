# 命令行参考

正式流程是研究声明 → 准备与准入 → 运行 → Result → 报告。Workspace 串联同一应用服务；独立 `package lint` 用于主动诊断声明。

`run`、`workspace run` 和 `workspace execute` 默认直接读取已准入数据库或既有归档。添加 `--persist-inputs` 才保存查询输入；选择随首次调用冻结，恢复沿用原设置。来源引用、依赖保留和日数据预读见[直接输入](direct-inputs.md)。

## 命令入口

```text
catalog, package, run, resume, retry-node, inspect, rerun-from,
report, compare, analysis, export-result, doctor, gc, capabilities,
operator, artifact, recipe, workspace
```

参数以 `python -m research_pipeline --help` 及各子命令 `--help` 为准。操作型命令提供互斥的 `--json` 与 `--summary`：前者返回完整 data 和 summary，后者返回摘要、错误和下一条命令。机器输出采用 UTF-8。

## 发现与声明

```powershell
python -m research_pipeline capabilities --format json
python -m research_pipeline operator list --format json
python -m research_pipeline artifact describe data.columnar-bundle.v1 --format json
python -m research_pipeline recipe list --format json
python -m research_pipeline catalog dataset search daily --catalog-lock <持久Catalog-Lock目录> --format json
python -m research_pipeline package init <新研究包目录> --json
python -m research_pipeline package lint --package <研究包目录> --catalog-lock <持久Catalog-Lock目录> --json
```

当前公共 Recipe 清单为空。`package init` 生成待填写的中性草稿；补齐四份声明后才能准入。`package lint` 不打开研究数据库，报告声明、算子端口、指标、结果表、来源与资源缺口。

Catalog 搜索返回本次实际加载的 `catalog_context.lock_reference`、来源 profile 和物理 bindings。按这些事实提供数据库路径，不猜测本机位置。公开源码不附带个人或供应商 Catalog；使用自己的定义与审批文件编译持久 Lock。

项目指标可在 `spec/research.yaml` 的可选 `metric_definitions` 列表中声明，每项使用 `MetricDefinition.to_dict()` 格式。定义必须绑定实际生产输出、单位和测量语义。

## 项目算子与准入

```powershell
python -m research_pipeline operator scaffold --output <新算子目录> --project-id <项目ID> --operator-id <算子ID> --format json
python -m research_pipeline operator validate --spec <operator.yaml> --source <源码目录> --format json
python -m research_pipeline operator build --spec <operator.yaml> --source <源码目录> --output <bundle父目录> --format json
python -m research_pipeline package admit --package <研究包目录> --extension-bundle <bundle目录> --catalog-lock <持久Catalog-Lock目录> --data-db <只读DuckDB> --output <新计划目录> --json
```

`--source` 是显式源码闭包。扩展入口为同步函数 `run(context, inputs, output_root)`，输出必须符合声明端口和 typed commit。准入后 bundle 随计划冻结，运行和恢复使用该计划。

多个数据源通过可重复的 `--source-db PROFILE=PATH` 指定。归档输入使用 `--input-snapshot-manifest`，与 `--data-db/--source-db` 互斥；分钟输入显式提供 `--minute-data-root`。来源正文采用 `archived_snapshot` 时，提供 `--source-archive-root`。

准入只读检查物理 schema、来源、时区、排序和可见日期。直接采集的 raw 分钟按 `raw + collected + pit_allowed` 及 completed bar 语义准入；复权、财报、成分、行业、ST 和历史规则各自需要当时可见的来源。分钟 AUDIT 仅作观察，不能进入特征、标签、信号、统计或交易仿真。

## 运行与恢复

```powershell
python -m research_pipeline run --plan <计划目录> --data-db <只读DuckDB> --artifact-root <工件目录> --run-root <新run目录> --result-store <ResultStore> --clock <带时区ISO时间> --root-seed 0 --json
python -m research_pipeline inspect --run-root <run目录> --json
python -m research_pipeline resume --run-root <run目录> --json
python -m research_pipeline retry-node --run-root <run目录> --node <节点ID> --json
python -m research_pipeline rerun-from --run-root <父run目录> --output-run-root <新子run目录> --node <节点ID> --json
```

`inspect` 只读展示节点、attempt、checkpoint、心跳、真实进度和 Result 发布状态，并给出唯一 `recommended_action` 及 `next_command_argv`。同身份中断使用 resume；允许重试且仍有余额的错误使用 retry-node。输入、实现或配置变化后重新准入，不能修改原计划或手改成功状态。

所有节点成功但 Result 尚未发布时，原 resume 可以只重试组装。Result 已发布时建议 `report --result`；报告失败不重跑研究。

跨运行复用参数由 run、workspace run 和 workspace execute 共用：

| 参数 | 行为 |
| --- | --- |
| `--reuse-run-root` | 可重复，按显式顺序机会式复用完成态 run 的合格节点 |
| `--require-reused-node` | 与完成态来源组合；指定节点及必要上游必须复用，不合格即拒绝启动 |
| `--reuse-failed-run-root` | 使用一个终态失败 run 中合格的成功 checkpoint；不能与完成态来源混用 |

恢复与复用核对节点身份、输入、工件和冻结时点，不重置已消费的 holdout。完整规则见[运行与恢复](runtime.md)。

## 资源

正式主 DAG 默认 `deterministic_serial`、workers=1。并行显式选择 `bounded_parallel` 或 `partitioned_batch`，通过 `--workers N` 设置并发上限；总预算不随 workers 增加。

`--resource-memory-bytes`、`--resource-cpu-slots`、`--resource-scratch-bytes` 设置运行容量。多个独立 CLI 共用资源时，显式提供仓库外 `--resource-state-dir`、进程总额度与等待超时。`run --no-node-timeout` 取消主 DAG 节点累计时长限制；内存、磁盘、进程与单次回调限时继续生效。详见[资源预算](project_resource_budgets.md)。

## Result 报告、比较与导出

```powershell
python -m research_pipeline report --result <Result目录> --output <新报告.md> --format markdown --json
python -m research_pipeline report --result <Result目录> --output <新报告.json> --format json --json
python -m research_pipeline report --result <Result目录> --format html --request <报告请求.yaml> --output <新报告.html> --json
python -m research_pipeline compare --left-result <左Result目录> --right-result <右Result目录> --json
python -m research_pipeline export-result --result <Result目录> --output <新导出目录> --json
```

可选 `--result-store` 显式指定存储根；compare 对应 `--left-result-store/--right-result-store`。结果消费者读取 Result，报告不连接研究数据库。省略 report 的 output 时将正文返回 stdout；文件输出采用 UTF-8 排他原子写入，不覆盖已有文件。JSON 报告版本为 `research-result-report-v1`；HTML 请求格式见[报告说明](qlib-report.md)与[LEAN 报告](lean-execution.md)。

直接 compare 比较正式指标事实，返回 `formal_metric_facts`；指标定义、单位、样本、状态或声明结论不一致时不给部分 delta。包级语义比较使用 `package compare`，返回 `package_contract_and_metric_facts`，额外检查两侧 metric/claim 合同。包级 report、compare、export-result 同样使用 Result 参数，并显式指定 `--package`。

export-result 复制正式 Result，不重新执行研究。迁移时同时保留 Result 引用的持久输入依赖。

## 时间序列分析

```powershell
python -m research_pipeline analysis run --result <Result目录> --request <AnalysisRequest.yaml> --output <新AnalysisResult.json> --analysis-memory-bytes <字节> --json
python -m research_pipeline analysis compare --analysis-result <左AnalysisResult.json> --analysis-result <右AnalysisResult.json> --measure overall.compounded_return --direction higher_is_better --output <新AnalysisComparison.json> --json
```

AnalysisRequest 显式绑定 Result ID、表列、窗口、值语义、频率、单位、费用口径、缺失值与年度处理政策。只读取日期和值两列；memory 参数约束批次工作集。非重叠期间收益才能计算复合收益、几何年化和回撤；水平值与普通观察不套收益公式。analysis compare 要求同规格、实际窗口和声明事实，条件不一致时返回原因及空排名。

## Workspace

```powershell
python -m research_pipeline workspace init <新工作区> --from-package <完整研究包> --json
python -m research_pipeline workspace execute --workspace <工作区> --catalog-lock <Lock目录> --data-db <只读DuckDB> --extension-bundle <算子bundle> --summary
python -m research_pipeline workspace execute --workspace <工作区> --execution-id <原编号> --catalog-lock <Lock目录> --summary
python -m research_pipeline workspace dashboard --workspace <工作区> --json
```

普通 execute 合并准备与准入，同次解析和编译后直接交接，不先完整调用独立 lint。主动诊断仍使用 package lint。成功返回 `status=succeeded`、`execution_status=succeeded`、`result_directory`、`result_id`、`result_store` 和 `report_path`。`command_status` 表示命令是否完成；执行状态需要 Result 已正式发布。报告失败保留执行成功与 Result 位置。

未提供 execution-id 时建立新 execution；指定原编号时沿用原 plan、invocation、Result 和 holdout 记录。详见[模板与接续](workspace-continuation.md)。

## 运维

```powershell
python -m research_pipeline doctor --run-root <run目录> --json
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --json
```

doctor 只读诊断；gc 默认只列清理计划，`--apply` 仅隔离候选。数据库写入和历史研究删除须另行授权，见[运维](operations.md)。
